// syncrep.c — synchronous replication (PG 18.3). Backend commit waits
// (SyncRepWaitForLSN over the per-mode LSN-ordered proc queues in WalSndCtl),
// the walsender release side (SyncRepInitConfig / SyncRepReleaseWaiters /
// SyncRepGetCandidateStandbys), the checkpointer's
// SyncRepUpdateSyncStandbysDefined, and the synchronous_standby_names GUC
// hooks with the syncrep_gram.y/scanner.l parser (config.rs).
//
// Divergence recorded: C caches SyncRepWaitMode per backend via
// assign_synchronous_commit; here the mode is computed from the
// synchronous_commit GUC on each use (same mapping, no cache).
#![allow(non_snake_case)]

pub mod config;

use std::sync::atomic::Ordering::Relaxed;
use pgsync::RwLock;

use elog::ereport;
use types_core::{InvalidXLogRecPtr, ProcNumber, XLogRecPtr};
use types_error::{
    ErrorLocation, PgResult, DEBUG1, ERRCODE_ADMIN_SHUTDOWN, ERRCODE_SYNTAX_ERROR, LOG, WARNING,
};
use types_storage::storage::{proclist_head, proclist_node, PGPROC, SYNC_REP_LOCK};
use types_storage::waiteventset::{WL_LATCH_SET, WL_POSTMASTER_DEATH};

use config::{SyncRepConfigData, SYNC_REP_PRIORITY};

const SRC: &str = "src/backend/replication/syncrep.c";

fn loc(line: i32, func: &'static str) -> ErrorLocation {
    ErrorLocation::new(SRC, line, func)
}

// syncrep.h wait states / modes.
pub const SYNC_REP_NOT_WAITING: i32 = 0;
pub const SYNC_REP_WAITING: i32 = 1;
pub const SYNC_REP_WAIT_COMPLETE: i32 = 2;

pub const SYNC_REP_NO_WAIT: i32 = -1;
pub const SYNC_REP_WAIT_WRITE: i32 = 0;
pub const SYNC_REP_WAIT_FLUSH: i32 = 1;
pub const SYNC_REP_WAIT_APPLY: i32 = 2;
pub const NUM_SYNC_REP_WAIT_MODE: usize = walsender::NUM_SYNC_REP_WAIT_MODE;

// walsender_private.h sync_standbys_status bits.
pub const SYNC_STANDBY_INIT: u32 = 1 << 0;
pub const SYNC_STANDBY_DEFINED: u32 = 1 << 1;

// PG_WAIT_IPC | index of "SyncRep" in the IPC wait-event table.
const WAIT_EVENT_SYNC_REP: u32 = 0x0800_0000 | 52;

// SyncRepConfig (the parsed synchronous_standby_names). C is per-process via
// the GUC assign hook; the process-shared RwLock matches the thread model
// (the GUC itself is PGC_SIGHUP, one value per cluster).
pgsync::process_global! {
    static SYNC_REP_CONFIG: RwLock<Option<SyncRepConfigData>> = RwLock::new(None);
}

pub fn sync_rep_config() -> Option<SyncRepConfigData> {
    SYNC_REP_CONFIG.read().expect("SYNC_REP_CONFIG poisoned").clone()
}

// SyncStandbysDefined() (syncrep.h): GUC set and non-empty.
fn sync_standbys_defined() -> bool {
    guc_tables::vars::SyncRepStandbyNames
        .read()
        .map(|v| !v.is_empty())
        .unwrap_or(false)
}

// SyncRepRequested() (syncrep.h) + C's cached SyncRepWaitMode
// (assign_synchronous_commit), computed on the fly here.
fn sync_rep_wait_mode() -> i32 {
    match guc_tables::vars::synchronous_commit.read() {
        types_core::xact::SYNCHRONOUS_COMMIT_REMOTE_WRITE => SYNC_REP_WAIT_WRITE,
        types_core::xact::SYNCHRONOUS_COMMIT_REMOTE_FLUSH => SYNC_REP_WAIT_FLUSH,
        types_core::xact::SYNCHRONOUS_COMMIT_REMOTE_APPLY => SYNC_REP_WAIT_APPLY,
        _ => SYNC_REP_NO_WAIT,
    }
}

fn my_proc() -> Option<(&'static PGPROC, ProcNumber)> {
    let procno = lmgr_proc::MyProc()?;
    Some((lmgr_proc::GetPGProcByNumber(procno), procno))
}

fn lock_sync_rep() -> PgResult<()> {
    lwlock::LWLockAcquire(
        lwlock::main_lock(SYNC_REP_LOCK),
        lwlock::LW_EXCLUSIVE,
        init_small::globals::MyProcNumber(),
    )
    .map(|_| ())
}

fn unlock_sync_rep() -> PgResult<()> {
    lwlock::LWLockRelease(lwlock::main_lock(SYNC_REP_LOCK))
}

// ---------------------------------------------------------------------------
// dlist ops over PGPROC.syncRepLinks (C uses dlist_* on WalSndCtl->SyncRepQueue).
// All require SyncRepLock exclusive.
// ---------------------------------------------------------------------------

fn queue(mode: i32) -> &'static types_storage::storage::SyncCell<proclist_head> {
    &walsender::WalSndCtl().sync_rep_queue[mode as usize]
}

fn links_of(procno: ProcNumber) -> &'static types_storage::storage::SyncCell<proclist_node> {
    &lmgr_proc::GetPGProcByNumber(procno).syncRepLinks
}

fn q_delete(mode: i32, procno: ProcNumber) {
    let list = queue(mode);
    let mut head = list.get();
    let node = links_of(procno).get();
    if node.prev == types_core::INVALID_PROC_NUMBER {
        head.head = node.next;
    } else {
        let prev = links_of(node.prev);
        let mut p = prev.get();
        p.next = node.next;
        prev.set(p);
    }
    if node.next == types_core::INVALID_PROC_NUMBER {
        head.tail = node.prev;
    } else {
        let next = links_of(node.next);
        let mut n = next.get();
        n.prev = node.prev;
        next.set(n);
    }
    links_of(procno).set(proclist_node::detached());
    list.set(head);
}

// dlist_delete_thoroughly on whichever queue holds us: the queues never share
// a proc, and C deletes via the node without knowing the queue; scan modes.
fn q_delete_any(procno: ProcNumber) {
    for mode in 0..NUM_SYNC_REP_WAIT_MODE as i32 {
        let mut cur = queue(mode).get().head;
        while cur != types_core::INVALID_PROC_NUMBER {
            if cur == procno {
                q_delete(mode, procno);
                return;
            }
            cur = links_of(cur).get().next;
        }
    }
    // Not found: already removed; mark detached for the invariant.
    links_of(procno).set(proclist_node::detached());
}

// SyncRepQueueInsert: keep the queue ordered by waitLSN, scanning from the
// tail (we usually belong there).
fn queue_insert(mode: i32, procno: ProcNumber) {
    let list = queue(mode);
    let my_lsn = lmgr_proc::GetPGProcByNumber(procno).waitLSN.load(Relaxed);

    let mut cur = list.get().tail;
    while cur != types_core::INVALID_PROC_NUMBER {
        let cur_lsn = lmgr_proc::GetPGProcByNumber(cur).waitLSN.load(Relaxed);
        if cur_lsn < my_lsn {
            // dlist_insert_after(cur, procno)
            let cur_node_cell = links_of(cur);
            let mut cur_node = cur_node_cell.get();
            let next = cur_node.next;
            links_of(procno).set(proclist_node { prev: cur, next });
            cur_node.next = procno;
            cur_node_cell.set(cur_node);
            let mut head = list.get();
            if next == types_core::INVALID_PROC_NUMBER {
                head.tail = procno;
            } else {
                let next_cell = links_of(next);
                let mut n = next_cell.get();
                n.prev = procno;
                next_cell.set(n);
            }
            list.set(head);
            return;
        }
        cur = links_of(cur).get().prev;
    }

    // Empty list, or we belong at the head.
    let mut head = list.get();
    links_of(procno).set(proclist_node {
        prev: types_core::INVALID_PROC_NUMBER,
        next: head.head,
    });
    if head.head == types_core::INVALID_PROC_NUMBER {
        head.tail = procno;
    } else {
        let old_head = links_of(head.head);
        let mut h = old_head.get();
        h.prev = procno;
        old_head.set(h);
    }
    head.head = procno;
    list.set(head);
}

#[cfg(debug_assertions)]
fn queue_is_ordered_by_lsn(mode: i32) -> bool {
    let mut last: XLogRecPtr = 0;
    let mut cur = queue(mode).get().head;
    while cur != types_core::INVALID_PROC_NUMBER {
        let lsn = lmgr_proc::GetPGProcByNumber(cur).waitLSN.load(Relaxed);
        if lsn <= last {
            return false;
        }
        last = lsn;
        cur = links_of(cur).get().next;
    }
    true
}

// ---------------------------------------------------------------------------
// Backend side
// ---------------------------------------------------------------------------

/// SyncRepWaitForLSN(lsn, commit) (syncrep.c:148).
pub fn SyncRepWaitForLSN(lsn: XLogRecPtr, commit: bool) -> PgResult<()> {
    use init_small::globals as g;

    // Fast exit: no sync replication requested (SyncRepRequested()), or the
    // checkpointer has initialized the status with no sync standbys defined.
    if walsender_config::max_wal_senders() <= 0 || sync_rep_wait_mode() == SYNC_REP_NO_WAIT {
        return Ok(());
    }
    let ctl = walsender::WalSndCtl();
    if ctl.sync_standbys_status.load(Relaxed) & (SYNC_STANDBY_INIT | SYNC_STANDBY_DEFINED)
        == SYNC_STANDBY_INIT
    {
        return Ok(());
    }

    // Cap the level for anything other than commit to remote flush only.
    let mode = if commit {
        sync_rep_wait_mode()
    } else {
        sync_rep_wait_mode().min(SYNC_REP_WAIT_FLUSH)
    };

    let Some((proc, procno)) = my_proc() else {
        return Ok(());
    };
    debug_assert!(proc.syncRepLinks.get().is_detached());

    lock_sync_rep()?;
    debug_assert_eq!(proc.syncRepState.get(), SYNC_REP_NOT_WAITING);

    let status = ctl.sync_standbys_status.load(Relaxed);
    if status & SYNC_STANDBY_INIT != 0 {
        if status & SYNC_STANDBY_DEFINED == 0
            || lsn <= ctl.sync_rep_lsn[mode as usize].load(Relaxed)
        {
            unlock_sync_rep()?;
            return Ok(());
        }
    } else if lsn <= ctl.sync_rep_lsn[mode as usize].load(Relaxed) {
        // Status not initialized yet, but the LSN alone proves there is no
        // point in waiting.
        unlock_sync_rep()?;
        return Ok(());
    } else if !sync_standbys_defined() {
        // Status not initialized and the GUC is empty: never wait (matters at
        // cluster initialization; no sync standbys is the default).
        unlock_sync_rep()?;
        return Ok(());
    }

    // Set our waitLSN so the walsender knows when to wake us, and enqueue.
    proc.waitLSN.store(lsn, Relaxed);
    proc.syncRepState.set(SYNC_REP_WAITING);
    queue_insert(mode, procno);
    #[cfg(debug_assertions)]
    debug_assert!(queue_is_ordered_by_lsn(mode));
    unlock_sync_rep()?;

    if guc_tables::vars::update_process_title.read() {
        ps_status_seams::set_ps_display_suffix::call(&format!(
            "waiting for {:X}/{:X}",
            (lsn >> 32) as u32,
            lsn as u32
        ));
    }

    // Wait for the specified LSN to be confirmed (own-latch loop).
    loop {
        latch::ResetLatch(init_small::globals::MyLatch().expect("SyncRepWaitForLSN: no MyLatch"));

        if proc.syncRepState.get() == SYNC_REP_WAIT_COMPLETE {
            break;
        }

        if g::ProcDiePending() {
            // The transaction has already committed locally; we can neither
            // ack the commit nor raise ERROR/FATAL. WARN and shut off output.
            ereport(WARNING)
                .errcode(ERRCODE_ADMIN_SHUTDOWN)
                .errmsg(
                    "canceling the wait for synchronous replication and terminating \
                     connection due to administrator command",
                )
                .errdetail(
                    "The transaction has already committed locally, but might not \
                     have been replicated to the standby.",
                )
                .finish(loc(305, "SyncRepWaitForLSN"))?;
            elog::config::set_where_to_send_output(types_dest::CommandDest::None);
            SyncRepCancelWait(procno)?;
            break;
        }

        if g::QueryCancelPending() {
            g::SetQueryCancelPending(false);
            ereport(WARNING)
                .errmsg("canceling wait for synchronous replication due to user request")
                .errdetail(
                    "The transaction has already committed locally, but might not \
                     have been replicated to the standby.",
                )
                .finish(loc(322, "SyncRepWaitForLSN"))?;
            SyncRepCancelWait(procno)?;
            break;
        }

        let rc = latch::WaitLatch(
            init_small::globals::MyLatch(),
            WL_LATCH_SET | WL_POSTMASTER_DEATH,
            -1,
            WAIT_EVENT_SYNC_REP,
        )?;

        if rc & WL_POSTMASTER_DEATH != 0 {
            // All walsenders exit with the postmaster: no ack will ever come.
            g::SetProcDiePending(true);
            elog::config::set_where_to_send_output(types_dest::CommandDest::None);
            SyncRepCancelWait(procno)?;
            break;
        }
    }

    // The walsender removed us from the queue (or we canceled). Reset state.
    std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);
    debug_assert!(proc.syncRepLinks.get().is_detached());
    proc.syncRepState.set(SYNC_REP_NOT_WAITING);
    proc.waitLSN.store(0, Relaxed);

    if guc_tables::vars::update_process_title.read() {
        ps_status_seams::set_ps_display_remove_suffix::call();
    }
    Ok(())
}

// SyncRepCancelWait (syncrep.c:406).
fn SyncRepCancelWait(procno: ProcNumber) -> PgResult<()> {
    lock_sync_rep()?;
    if !links_of(procno).get().is_detached() {
        q_delete_any(procno);
    }
    lmgr_proc::GetPGProcByNumber(procno).syncRepState.set(SYNC_REP_NOT_WAITING);
    unlock_sync_rep()
}

/// SyncRepCleanupAtProcExit (syncrep.c:416).
pub fn SyncRepCleanupAtProcExit() {
    let Some(procno) = lmgr_proc::MyProc() else { return };
    if !links_of(procno).get().is_detached() {
        let _ = lock_sync_rep();
        if !links_of(procno).get().is_detached() {
            q_delete_any(procno);
        }
        let _ = unlock_sync_rep();
    }
}

// ---------------------------------------------------------------------------
// Walsender side
// ---------------------------------------------------------------------------

thread_local! {
    // announce_next_takeover (syncrep.c file-static, per walsender).
    static ANNOUNCE_NEXT_TAKEOVER: std::cell::Cell<bool> = const { std::cell::Cell::new(true) };
}

fn application_name() -> String {
    guc_tables::vars::application_name.read().unwrap_or_default()
}

/// SyncRepInitConfig (syncrep.c:445).
pub fn SyncRepInitConfig() -> PgResult<()> {
    let priority = SyncRepGetStandbyPriority();
    let i = walsender::my_walsnd_index();
    debug_assert!(i >= 0);
    let slot = &walsender::WalSndCtl().walsnds[i as usize];
    let mut w = slot.lock().expect("walsnd mutex");
    if w.sync_standby_priority != priority {
        w.sync_standby_priority = priority;
        drop(w);
        ereport(DEBUG1)
            .errmsg_internal(format!(
                "standby \"{}\" now has synchronous standby priority {}",
                application_name(),
                priority
            ))
            .finish(loc(462, "SyncRepInitConfig"))?;
    }
    Ok(())
}

/// SyncRepStandbyData (syncrep.h), the candidate snapshot.
#[derive(Clone, Copy, Debug)]
pub struct SyncRepStandbyData {
    pub pid: i32,
    pub write: XLogRecPtr,
    pub flush: XLogRecPtr,
    pub apply: XLogRecPtr,
    pub sync_standby_priority: i32,
    pub walsnd_index: i32,
    pub is_me: bool,
}

/// SyncRepGetCandidateStandbys (syncrep.c:754).
pub fn SyncRepGetCandidateStandbys() -> Vec<SyncRepStandbyData> {
    let Some(cfg) = sync_rep_config() else {
        return Vec::new();
    };
    let ctl = walsender::WalSndCtl();
    let my_index = walsender::my_walsnd_index();
    let mut out: Vec<SyncRepStandbyData> = Vec::with_capacity(ctl.walsnds.len());

    for (i, slot) in ctl.walsnds.iter().enumerate() {
        let w = slot.lock().expect("walsnd mutex");
        let (pid, state, write, flush, apply, priority) =
            (w.pid, w.state, w.write, w.flush, w.apply, w.sync_standby_priority);
        drop(w);

        if pid == 0 {
            continue;
        }
        if state != walsender::WalSndState::Streaming && state != walsender::WalSndState::Stopping
        {
            continue;
        }
        if priority == 0 {
            continue;
        }
        if flush == InvalidXLogRecPtr {
            continue;
        }
        out.push(SyncRepStandbyData {
            pid,
            write,
            flush,
            apply,
            sync_standby_priority: priority,
            walsnd_index: i as i32,
            is_me: i as i32 == my_index,
        });
    }

    // Priority mode with too many candidates: keep the num_sync of highest
    // priority (ties broken by walsnd array position, C's comparator).
    if cfg.syncrep_method == SYNC_REP_PRIORITY && out.len() as i32 > cfg.num_sync {
        out.sort_by_key(|s| (s.sync_standby_priority, s.walsnd_index));
        out.truncate(cfg.num_sync as usize);
    }
    out
}

// SyncRepGetStandbyPriority (syncrep.c:860).
fn SyncRepGetStandbyPriority() -> i32 {
    // Synchronous cascade replication is not allowed.
    if walsender::am_cascading_walsender_now() {
        return 0;
    }
    if !sync_standbys_defined() {
        return 0;
    }
    let Some(cfg) = sync_rep_config() else { return 0 };

    let appname = application_name();
    for (idx, member) in cfg.members.iter().enumerate() {
        if member.eq_ignore_ascii_case(&appname) || member == "*" {
            // Quorum mode: all listed standbys have priority one.
            return if cfg.syncrep_method == SYNC_REP_PRIORITY { idx as i32 + 1 } else { 1 };
        }
    }
    0
}

// SyncRepGetSyncRecPtr (syncrep.c:586): (write, flush, apply, am_sync) or None
// when there are not enough sync standbys / we are not one.
fn SyncRepGetSyncRecPtr() -> (Option<(XLogRecPtr, XLogRecPtr, XLogRecPtr)>, bool) {
    let Some(cfg) = sync_rep_config() else {
        return (None, false);
    };
    let standbys = SyncRepGetCandidateStandbys();
    let am_sync = standbys.iter().any(|s| s.is_me);

    if !am_sync || (standbys.len() as i32) < cfg.num_sync {
        return (None, am_sync);
    }

    if cfg.syncrep_method == SYNC_REP_PRIORITY {
        // Oldest positions among sync standbys.
        let mut write = InvalidXLogRecPtr;
        let mut flush = InvalidXLogRecPtr;
        let mut apply = InvalidXLogRecPtr;
        for s in &standbys {
            if write == InvalidXLogRecPtr || write > s.write {
                write = s.write;
            }
            if flush == InvalidXLogRecPtr || flush > s.flush {
                flush = s.flush;
            }
            if apply == InvalidXLogRecPtr || apply > s.apply {
                apply = s.apply;
            }
        }
        (Some((write, flush, apply)), true)
    } else {
        // Nth latest positions (quorum).
        let nth = cfg.num_sync as usize;
        let mut writes: Vec<_> = standbys.iter().map(|s| s.write).collect();
        let mut flushes: Vec<_> = standbys.iter().map(|s| s.flush).collect();
        let mut applies: Vec<_> = standbys.iter().map(|s| s.apply).collect();
        writes.sort_unstable_by(|a, b| b.cmp(a));
        flushes.sort_unstable_by(|a, b| b.cmp(a));
        applies.sort_unstable_by(|a, b| b.cmp(a));
        (Some((writes[nth - 1], flushes[nth - 1], applies[nth - 1])), true)
    }
}

/// SyncRepReleaseWaiters (syncrep.c:474).
pub fn SyncRepReleaseWaiters() -> PgResult<()> {
    let i = walsender::my_walsnd_index();
    debug_assert!(i >= 0);
    let ctl = walsender::WalSndCtl();
    let (priority, state, flush) = {
        let w = ctl.walsnds[i as usize].lock().expect("walsnd mutex");
        (w.sync_standby_priority, w.state, w.flush)
    };

    // Nothing to do unless we are a potential sync standby that is streaming
    // or stopping with a valid flush position.
    if priority == 0
        || (state != walsender::WalSndState::Streaming
            && state != walsender::WalSndState::Stopping)
        || flush == InvalidXLogRecPtr
    {
        ANNOUNCE_NEXT_TAKEOVER.set(true);
        return Ok(());
    }

    lock_sync_rep()?;
    let (recptrs, am_sync) = SyncRepGetSyncRecPtr();

    if ANNOUNCE_NEXT_TAKEOVER.get() && am_sync {
        ANNOUNCE_NEXT_TAKEOVER.set(false);
        let method_is_priority = sync_rep_config()
            .map(|c| c.syncrep_method == SYNC_REP_PRIORITY)
            .unwrap_or(true);
        if method_is_priority {
            ereport(LOG)
                .errmsg(format!(
                    "standby \"{}\" is now a synchronous standby with priority {}",
                    application_name(),
                    priority
                ))
                .finish(loc(528, "SyncRepReleaseWaiters"))?;
        } else {
            ereport(LOG)
                .errmsg(format!(
                    "standby \"{}\" is now a candidate for quorum synchronous standby",
                    application_name()
                ))
                .finish(loc(533, "SyncRepReleaseWaiters"))?;
        }
    }

    let Some((write_ptr, flush_ptr, apply_ptr)) = recptrs else {
        unlock_sync_rep()?;
        ANNOUNCE_NEXT_TAKEOVER.set(!am_sync);
        return Ok(());
    };

    // Set the lsn first so woken backends release up to this location.
    for (mode, ptr) in [
        (SYNC_REP_WAIT_WRITE, write_ptr),
        (SYNC_REP_WAIT_FLUSH, flush_ptr),
        (SYNC_REP_WAIT_APPLY, apply_ptr),
    ] {
        if ctl.sync_rep_lsn[mode as usize].load(Relaxed) < ptr {
            ctl.sync_rep_lsn[mode as usize].store(ptr, Relaxed);
            SyncRepWakeQueue(false, mode);
        }
    }
    unlock_sync_rep()
}

// SyncRepWakeQueue (syncrep.c:907): caller holds SyncRepLock exclusive.
fn SyncRepWakeQueue(all: bool, mode: i32) -> i32 {
    let ctl = walsender::WalSndCtl();
    #[cfg(debug_assertions)]
    debug_assert!(queue_is_ordered_by_lsn(mode));

    let mut numprocs = 0;
    let mut cur = queue(mode).get().head;
    while cur != types_core::INVALID_PROC_NUMBER {
        let next = links_of(cur).get().next;
        let proc = lmgr_proc::GetPGProcByNumber(cur);

        // The queue is ordered by LSN.
        if !all && ctl.sync_rep_lsn[mode as usize].load(Relaxed) < proc.waitLSN.load(Relaxed) {
            return numprocs;
        }

        q_delete(mode, cur);

        // The waiter reads syncRepState without the lock: make sure the queue
        // unlink is visible before the state change.
        std::sync::atomic::fence(std::sync::atomic::Ordering::Release);
        proc.syncRepState.set(SYNC_REP_WAIT_COMPLETE);

        latch::SetLatch(types_storage::latch::LatchHandle::proc(cur));
        numprocs += 1;
        cur = next;
    }
    numprocs
}

/// SyncRepUpdateSyncStandbysDefined (syncrep.c:964); checkpointer-only caller.
pub fn SyncRepUpdateSyncStandbysDefined() {
    let ctl = walsender::WalSndCtl();
    let defined = sync_standbys_defined();
    let status = ctl.sync_standbys_status.load(Relaxed);

    if defined != (status & SYNC_STANDBY_DEFINED != 0) {
        let _ = lock_sync_rep();
        // If the GUC was reset to empty, waiting is futile: wake everyone.
        if !defined {
            for mode in 0..NUM_SYNC_REP_WAIT_MODE as i32 {
                SyncRepWakeQueue(true, mode);
            }
        }
        ctl.sync_standbys_status.store(
            SYNC_STANDBY_INIT | if defined { SYNC_STANDBY_DEFINED } else { 0 },
            Relaxed,
        );
        let _ = unlock_sync_rep();
    } else if status & SYNC_STANDBY_INIT == 0 {
        let _ = lock_sync_rep();
        debug_assert!(!sync_standbys_defined());
        ctl.sync_standbys_status.fetch_or(SYNC_STANDBY_INIT, Relaxed);
        let _ = unlock_sync_rep();
    }
}

// ---------------------------------------------------------------------------
// GUC hooks
// ---------------------------------------------------------------------------

// check_synchronous_standby_names (syncrep.c:1057).
fn check_synchronous_standby_names(
    newval: &mut Option<String>,
    extra: &mut Option<guc_tables::GucHookExtra>,
    _source: types_guc::GucSource,
) -> PgResult<bool> {
    match newval.as_deref() {
        Some(v) if !v.is_empty() => {
            let parsed = match config::parse_synchronous_standby_names(v) {
                Ok(p) => p,
                Err(msg) => {
                    guc::GUC_check_errcode(ERRCODE_SYNTAX_ERROR);
                    guc::GUC_check_errdetail(msg);
                    return Ok(false);
                }
            };
            if parsed.num_sync <= 0 {
                guc::GUC_check_errmsg(format!(
                    "number of synchronous standbys ({}) must be greater than zero",
                    parsed.num_sync
                ));
                return Ok(false);
            }
            *extra = Some(Box::new(parsed));
        }
        _ => {
            *extra = None;
        }
    }
    Ok(true)
}

// assign_synchronous_standby_names (syncrep.c:1118).
fn assign_synchronous_standby_names(
    _newval: Option<&str>,
    extra: Option<&guc_tables::GucHookExtra>,
) {
    let parsed = extra.and_then(|e| e.downcast_ref::<SyncRepConfigData>()).cloned();
    *SYNC_REP_CONFIG.write().expect("SYNC_REP_CONFIG poisoned") = parsed;
}

// synchronous_standby_names' backing storage (C: the SyncRepStandbyNames
// char* owned by syncrep). PGC_SIGHUP — one cluster-wide value, so a
// process-shared static matches the thread model.
pgsync::process_global! {
    static STANDBY_NAMES: RwLock<Option<String>> = RwLock::new(None);
}

fn standby_names_get() -> Option<String> {
    STANDBY_NAMES.read().expect("STANDBY_NAMES poisoned").clone()
}

fn standby_names_set(v: Option<String>) {
    *STANDBY_NAMES.write().expect("STANDBY_NAMES poisoned") = v;
}

pub fn init_seams() {
    guc_tables::vars::SyncRepStandbyNames.install_if_absent(guc_tables::GucVarAccessors {
        get: standby_names_get,
        set: standby_names_set,
    });
    guc_tables::hooks::check_synchronous_standby_names.install(check_synchronous_standby_names);
    guc_tables::hooks::assign_synchronous_standby_names.install(assign_synchronous_standby_names);

    syncrep_seams::sync_rep_wait_for_lsn::set(SyncRepWaitForLSN);
    syncrep_seams::sync_rep_cleanup_at_proc_exit::set(SyncRepCleanupAtProcExit);
    syncrep_seams::sync_rep_update_sync_standbys_defined::set(SyncRepUpdateSyncStandbysDefined);
    syncrep_seams::sync_rep_init_config::set(SyncRepInitConfig);
    syncrep_seams::sync_rep_release_waiters::set(SyncRepReleaseWaiters);
    syncrep_seams::sync_rep_candidate_indexes::set(|| {
        Ok(SyncRepGetCandidateStandbys()
            .into_iter()
            .map(|s| (s.walsnd_index, s.pid))
            .collect())
    });
    syncrep_seams::sync_rep_method_is_priority::set(|| {
        sync_rep_config()
            .map(|c| c.syncrep_method == SYNC_REP_PRIORITY)
            .unwrap_or(true)
    });
}

#[cfg(test)]
mod tests;
