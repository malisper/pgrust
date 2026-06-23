//! `syncrep.c` — synchronous replication wait + standby ordering.
//!
//! Synchronous replication makes transaction commits wait until their commit
//! LSN is acknowledged by the synchronous standbys.  All code in this module
//! executes on the primary; the streaming transport stays in the
//! WALreceiver/WALsender modules.  The design isolates all logic about
//! waiting/releasing onto the primary, which defines (via
//! `synchronous_standby_names`) which standbys it wishes to wait for.
//!
//! This is a function-for-function port of `src/backend/replication/syncrep.c`.
//!
//! The `WalSndCtl` shared-memory control block (the released-LSN array, the
//! status flag, and the three `SyncRepQueue` heads) and the per-`WalSnd` slot
//! data are owned by `backend-replication-walsender`; this crate reaches them
//! through that crate's accessors (the SyncRepQueue heads via
//! `with_sync_rep_queue`).  The per-`PGPROC` sync-rep fields
//! (`syncRepState`/`waitLSN`/`syncRepLinks`) are reached through
//! `backend-storage-lmgr-proc-seams`.  `SyncRepLock` is the built-in LWLock at
//! offset `SYNC_REP_LOCK` in lwlock.c's `MainLWLockArray`.
//!
//! The module-static C globals `announce_next_takeover` and `SyncRepWaitMode`
//! become per-backend thread-locals; `SyncRepConfig` is the parsed GUC "extra"
//! threaded through the assign hook; `SyncRepStandbyNames` is the
//! `synchronous_standby_names` GUC string, read straight from the GUC tables.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::cell::Cell;

use utils_error::{elog, ereport};
use types_error::{
    ErrorLocation, PgResult, DEBUG1, DEBUG3, ERRCODE_ADMIN_SHUTDOWN, LOG, WARNING,
};
use ::types_core::primitive::XLogRecPtr;
use ::types_core::xact::{
    InvalidXLogRecPtr, SYNCHRONOUS_COMMIT_LOCAL_FLUSH, SYNCHRONOUS_COMMIT_REMOTE_APPLY,
    SYNCHRONOUS_COMMIT_REMOTE_FLUSH, SYNCHRONOUS_COMMIT_REMOTE_WRITE,
};
use types_core::{ProcNumber, INVALID_PROC_NUMBER};
use ::replication::walsender::{SyncRepStandbyData, WalSndState};
use ::types_storage::storage::{proclist_node, LWLockMode, SYNC_REP_LOCK};
use ::types_storage::waiteventset::{WL_LATCH_SET, WL_POSTMASTER_DEATH};

use syncrep_gram::{syncrep_yyparse, SYNC_REP_PRIORITY};
use ::walsender::core as wsctl;

use latch_seams as latch;
use lwlock_seams as lwlock;
use lmgr_proc_seams as proc_s;
use init_small_seams as misc;
use ::guc_tables::vars;
use ps_status_seams as ps;

mod seams;
#[cfg(test)]
mod tests;

pub use seams::init_seams;

// ===========================================================================
// Constants (replication/syncrep.h + storage/proc.h + walsender_private.h).
// ===========================================================================

/// `syncRepState` — initial / cleared state (syncrep.h).
pub const SYNC_REP_NOT_WAITING: i32 = 0;
/// `syncRepState` — on the queue, awaiting confirmation (syncrep.h).
pub const SYNC_REP_WAITING: i32 = 1;
/// `syncRepState` — walsender has confirmed; the backend may proceed (syncrep.h).
pub const SYNC_REP_WAIT_COMPLETE: i32 = 2;

/// `SYNC_REP_NO_WAIT` (syncrep.h) — `SyncRepWaitMode` sentinel for async commit.
pub const SYNC_REP_NO_WAIT: i32 = -1;
/// `SYNC_REP_WAIT_WRITE` (syncrep.h).
pub const SYNC_REP_WAIT_WRITE: i32 = 0;
/// `SYNC_REP_WAIT_FLUSH` (syncrep.h).
pub const SYNC_REP_WAIT_FLUSH: i32 = 1;
/// `SYNC_REP_WAIT_APPLY` (syncrep.h).
pub const SYNC_REP_WAIT_APPLY: i32 = 2;
/// `NUM_SYNC_REP_WAIT_MODE` (syncrep.h).
pub const NUM_SYNC_REP_WAIT_MODE: i32 = 3;

/// `SYNC_STANDBY_INIT` (walsender_private.h).
pub const SYNC_STANDBY_INIT: u8 = 1 << 0;
/// `SYNC_STANDBY_DEFINED` (walsender_private.h).
pub const SYNC_STANDBY_DEFINED: u8 = 1 << 1;

/// `WAIT_EVENT_SYNC_REP` (`utils/wait_event.h`): `PG_WAIT_IPC | 0x34`
/// (the 0-based index 52 of `SYNC_REP` within the alphabetically-sorted IPC
/// wait-event class). "Waiting for confirmation from a remote server during
/// synchronous replication."
const PG_WAIT_IPC: u32 = 0x0800_0000;
const WAIT_EVENT_SYNC_REP: u32 = PG_WAIT_IPC | 0x34;

// ===========================================================================
// Module statics (syncrep.c top-of-file), as owned per-backend thread-locals.
// ===========================================================================

/// One parsed `synchronous_standby_names` configuration, the owned GUC "extra"
/// the check hook produces and the assign hook installs (replaces the raw
/// `SyncRepConfigData *` pointer).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncRepConfig {
    /// `config_size`.
    pub config_size: i32,
    /// `num_sync` — number of sync standbys to wait for.
    pub num_sync: i32,
    /// `syncrep_method` — `SYNC_REP_PRIORITY` or `SYNC_REP_QUORUM`.
    pub syncrep_method: u8,
    /// `nmembers` — number of names in `member_names`.
    pub nmembers: i32,
    /// `member_names` — the configured standby names, in list order.
    pub member_names: Vec<String>,
}

std::thread_local! {
    /// `static bool announce_next_takeover = true;`
    static ANNOUNCE_NEXT_TAKEOVER: Cell<bool> = const { Cell::new(true) };

    /// `SyncRepConfigData *SyncRepConfig = NULL;` — the parsed, owned config.
    pub(crate) static SYNC_REP_CONFIG: core::cell::RefCell<Option<SyncRepConfig>> =
        const { core::cell::RefCell::new(None) };

    /// `static int SyncRepWaitMode = SYNC_REP_NO_WAIT;`
    static SYNC_REP_WAIT_MODE: Cell<i32> = const { Cell::new(SYNC_REP_NO_WAIT) };

    /// `char *SyncRepStandbyNames;` (syncrep.c) — the runtime storage backing
    /// the `synchronous_standby_names` GUC. In C this `char *` is the GUC's
    /// `conf->variable`: the GUC machinery owns the string and writes it
    /// directly (the assign hook only stashes the parsed config). Boot value is
    /// the empty string (guc_tables.c boot_val `""`), matching a non-NULL
    /// pointer to an empty C string.
    static SYNC_REP_STANDBY_NAMES: core::cell::RefCell<Option<String>> =
        core::cell::RefCell::new(Some(String::new()));
}

/// Read `SyncRepStandbyNames` (the `synchronous_standby_names` GUC storage) —
/// `*conf->variable` for the GUC var accessor.
pub fn sync_rep_standby_names() -> Option<String> {
    SYNC_REP_STANDBY_NAMES.with(|cell| cell.borrow().clone())
}

/// Write `SyncRepStandbyNames` (the GUC machinery's `*conf->variable = newval`).
pub fn set_sync_rep_standby_names(value: Option<String>) {
    SYNC_REP_STANDBY_NAMES.with(|cell| *cell.borrow_mut() = value);
}

/// Read `SyncRepConfig` (clone of the owned config), or `None` when unset.
fn sync_rep_config() -> Option<SyncRepConfig> {
    SYNC_REP_CONFIG.with(|cell| cell.borrow().clone())
}

// ===========================================================================
// Inline helpers reproduced from PostgreSQL headers.
// ===========================================================================

/// `SyncStandbysDefined()` (syncrep.c macro) — `SyncRepStandbyNames != NULL &&
/// SyncRepStandbyNames[0] != '\0'`.  `SyncRepStandbyNames` is the
/// `synchronous_standby_names` GUC string.
fn SyncStandbysDefined() -> bool {
    match vars::SyncRepStandbyNames.read() {
        Some(s) => !s.is_empty(),
        None => false,
    }
}

/// `SyncRepRequested()` (syncrep.h):
/// `max_wal_senders > 0 && synchronous_commit > SYNCHRONOUS_COMMIT_LOCAL_FLUSH`.
fn SyncRepRequested() -> bool {
    vars::max_wal_senders.read() > 0 && vars::synchronous_commit.read() > SYNCHRONOUS_COMMIT_LOCAL_FLUSH
}

/// `XLogRecPtrIsInvalid(ptr)` (access/xlogdefs.h) — `ptr == InvalidXLogRecPtr`.
fn XLogRecPtrIsInvalid(ptr: XLogRecPtr) -> bool {
    ptr == InvalidXLogRecPtr
}

/// `"%X/%X"` / `LSN_FORMAT_ARGS(lsn)`.
fn lsn_format(lsn: XLogRecPtr) -> String {
    alloc::format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32)
}

/// `Min(a, b)` for `i32`.
fn min_int(a: i32, b: i32) -> i32 {
    if a < b {
        a
    } else {
        b
    }
}

fn syncrep_location(funcname: &'static str) -> ErrorLocation {
    ErrorLocation {
        filename: Some("syncrep.c".to_string()),
        lineno: 0,
        funcname: Some(funcname.to_string()),
    }
}

/// `application_name` GUC as an owned `String` (empty when unset), matching the
/// byte compare `pg_strcasecmp` performs and the `%s` formatting in messages.
fn application_name_string() -> String {
    vars::application_name.read().unwrap_or_default()
}

// SyncRepLock helpers — the built-in lock at offset SYNC_REP_LOCK.
fn sync_rep_lock_acquire() -> PgResult<lwlock::MainLWLockGuard> {
    lwlock::lwlock_acquire_main::call(SYNC_REP_LOCK, LWLockMode::LW_EXCLUSIVE)
}

// ===========================================================================
// Synchronous Replication functions for normal user backends
// ===========================================================================

/// Wait for synchronous replication, if requested by user.
///
/// `'lsn'` represents the LSN to wait for.  `'commit'` indicates whether this
/// LSN represents a commit record.
pub fn SyncRepWaitForLSN(lsn: XLogRecPtr, commit: bool) -> PgResult<()> {
    let mode: i32;

    // This should be called while holding interrupts during a transaction
    // commit to prevent the follow-up shared memory queue cleanups to be
    // influenced by external interruptions.
    debug_assert!(misc::interrupt_holdoff_count::call() > 0);

    // Fast exit if user has not requested sync replication, or there are no sync
    // replication standby names defined.
    //
    // We check WalSndCtl->sync_standbys_status flag without the lock and exit
    // immediately if SYNC_STANDBY_INIT is set but SYNC_STANDBY_DEFINED is
    // missing.
    if !SyncRepRequested()
        || (wsctl::wal_snd_ctl().sync_standbys_status & (SYNC_STANDBY_INIT | SYNC_STANDBY_DEFINED))
            == SYNC_STANDBY_INIT
    {
        return Ok(());
    }

    // Cap the level for anything other than commit to remote flush only.
    if commit {
        mode = SYNC_REP_WAIT_MODE.with(Cell::get);
    } else {
        mode = min_int(SYNC_REP_WAIT_MODE.with(Cell::get), SYNC_REP_WAIT_FLUSH);
    }

    debug_assert!(my_proc_links_detached());
    debug_assert!(wsctl::wal_snd_ctl_is_set());

    let guard = sync_rep_lock_acquire()?;
    debug_assert_eq!(proc_s::my_proc_sync_rep_state::call(), SYNC_REP_NOT_WAITING);

    // We don't wait for sync rep if SYNC_STANDBY_DEFINED is not set.  Also check
    // that the standby hasn't already replied.  If the sync standby data has not
    // been initialized yet (SYNC_STANDBY_INIT is not set), fall back to a check
    // based on the LSN, then a direct GUC check.
    let status = wsctl::wal_snd_ctl().sync_standbys_status;
    if status & SYNC_STANDBY_INIT != 0 {
        if status & SYNC_STANDBY_DEFINED == 0 || lsn <= wsctl::ctl_lsn(mode as usize) {
            guard.release()?;
            return Ok(());
        }
    } else if lsn <= wsctl::ctl_lsn(mode as usize) {
        // The LSN is older than what we need to wait for.  The sync standby data
        // has not been initialized yet, but we are OK to not wait.
        guard.release()?;
        return Ok(());
    } else if !SyncStandbysDefined() {
        // The sync standby data has not been initialized yet, and the LSN is
        // newer than what need to wait for, so we have fallen back to a check on
        // SyncStandbysDefined().
        guard.release()?;
        return Ok(());
    }

    // Set our waitLSN so WALSender will know when to wake us, and add ourselves
    // to the queue.
    proc_s::set_my_proc_wait_lsn::call(lsn);
    proc_s::set_my_proc_sync_rep_state::call(SYNC_REP_WAITING);
    SyncRepQueueInsert(mode);
    debug_assert!(SyncRepQueueIsOrderedByLSN(mode));
    guard.release()?;

    // Alter ps display to show waiting for sync rep.
    if vars::update_process_title.read() {
        let buffer = alloc::format!("waiting for {}", lsn_format(lsn));
        ps::set_ps_display_suffix::call(&buffer);
    }

    // Wait for specified LSN to be confirmed.  Each proc has its own wait latch,
    // so we perform a normal latch check/wait loop here.
    loop {
        // Must reset the latch before testing state.
        latch::reset_latch_my_latch::call();

        // Acquiring the lock is not needed, the latch ensures proper barriers.
        // If it looks like we're done, we must really be done.
        if proc_s::my_proc_sync_rep_state::call() == SYNC_REP_WAIT_COMPLETE {
            break;
        }

        // If a wait for synchronous replication is pending, we can neither
        // acknowledge the commit nor raise ERROR or FATAL.  So in this case we
        // issue a WARNING and shut off further output.  We do NOT reset
        // ProcDiePending, so that the process will die after the commit is
        // cleaned up.
        if misc::proc_die_pending::call() {
            ereport(WARNING)
                .errcode(ERRCODE_ADMIN_SHUTDOWN)
                .errmsg("canceling the wait for synchronous replication and terminating connection due to administrator command")
                .errdetail("The transaction has already committed locally, but might not have been replicated to the standby.")
                .finish(syncrep_location("SyncRepWaitForLSN"))?;
            postgres_seams::set_where_to_send_output_none::call();
            SyncRepCancelWait()?;
            break;
        }

        // It's unclear what to do if a query cancel interrupt arrives.  We can't
        // actually abort at this point, but ignoring the interrupt altogether is
        // not helpful, so we just terminate the wait with a suitable warning.
        if misc::query_cancel_pending::call() {
            misc::set_query_cancel_pending::call(false);
            ereport(WARNING)
                .errmsg("canceling wait for synchronous replication due to user request")
                .errdetail("The transaction has already committed locally, but might not have been replicated to the standby.")
                .finish(syncrep_location("SyncRepWaitForLSN"))?;
            SyncRepCancelWait()?;
            break;
        }

        // Wait on latch.  Any condition that should wake us up will set the
        // latch, so no need for timeout.
        let rc = latch::wait_latch_my_latch::call(
            WL_LATCH_SET | WL_POSTMASTER_DEATH,
            -1,
            WAIT_EVENT_SYNC_REP,
        )?;

        // If the postmaster dies, we'll probably never get an acknowledgment,
        // because all the wal sender processes will exit.  So just bail out.
        if rc & WL_POSTMASTER_DEATH != 0 {
            misc::set_proc_die_pending::call(true);
            postgres_seams::set_where_to_send_output_none::call();
            SyncRepCancelWait()?;
            break;
        }
    }

    // WalSender has checked our LSN and has removed us from queue.  Clean up
    // state and leave.  (The C pg_read_barrier() here is subsumed by the proc
    // accessor read.)
    debug_assert!(my_proc_links_detached());
    proc_s::set_my_proc_sync_rep_state::call(SYNC_REP_NOT_WAITING);
    proc_s::set_my_proc_wait_lsn::call(0);

    // reset ps display to remove the suffix
    if vars::update_process_title.read() {
        ps::set_ps_display_remove_suffix::call();
    }

    Ok(())
}

/// Insert `MyProc` into the specified `SyncRepQueue`, maintaining the sorted
/// (by `waitLSN`) invariant.  We will usually go at the tail, though it's
/// possible to arrive out of order, so start at the tail and work back to the
/// insertion point.
///
/// The caller holds `SyncRepLock`.
fn SyncRepQueueInsert(mode: i32) {
    debug_assert!(mode >= 0 && mode < NUM_SYNC_REP_WAIT_MODE);
    let me = proc_s::my_proc_number::call();
    let my_wait_lsn = proc_s::my_proc_wait_lsn::call();

    wsctl::with_sync_rep_queue(mode as usize, |queue| {
        // dlist_reverse_foreach: walk from the tail toward the head.
        let mut cur = queue.tail;
        while cur != INVALID_PROC_NUMBER {
            let node = proc_s::proc_sync_rep_links::call(cur);
            // Stop at the queue element we should insert after to keep the queue
            // ordered by LSN.
            if proc_s::proc_wait_lsn::call(cur) < my_wait_lsn {
                proclist_insert_after(queue, cur, me);
                return;
            }
            cur = link_prev(node);
        }

        // The list was either empty, or this process needs to be at the head.
        proclist_push_head(queue, me);
    });
}

/// Acquire `SyncRepLock` and cancel any wait currently in progress.
fn SyncRepCancelWait() -> PgResult<()> {
    let guard = sync_rep_lock_acquire()?;
    if !my_proc_links_detached() {
        proclist_delete_my_proc_all_modes();
    }
    proc_s::set_my_proc_sync_rep_state::call(SYNC_REP_NOT_WAITING);
    guard.release()
}

/// Called at backend exit (`ProcKill`) to remove this backend from the queue if
/// still on it.
pub fn SyncRepCleanupAtProcExit() -> PgResult<()> {
    // First check if we are removed from the queue without the lock to not slow
    // down backend exit.
    if !my_proc_links_detached() {
        let guard = sync_rep_lock_acquire()?;

        // maybe we have just been removed, so recheck
        if !my_proc_links_detached() {
            proclist_delete_my_proc_all_modes();
        }

        guard.release()?;
    }
    Ok(())
}

// ===========================================================================
// Synchronous Replication functions for wal sender processes
// ===========================================================================

/// Take any action required to initialise sync rep state from config data.
/// Called at WALSender startup and after each SIGHUP.
pub fn SyncRepInitConfig() -> PgResult<()> {
    // Determine if we are a potential sync standby and remember the result for
    // handling replies from standby.
    let priority = SyncRepGetStandbyPriority();
    if wsctl::my_sync_standby_priority() != priority {
        wsctl::set_my_sync_standby_priority(priority);

        ereport(DEBUG1)
            .errmsg_internal(alloc::format!(
                "standby \"{}\" now has synchronous standby priority {}",
                application_name_string(),
                priority
            ))
            .finish(syncrep_location("SyncRepInitConfig"))?;
    }
    Ok(())
}

/// Update the LSNs on each queue based upon our latest state.  This implements
/// a simple policy of first-valid-sync-standby-releases-waiter.
pub fn SyncRepReleaseWaiters() -> PgResult<()> {
    let mut writePtr: XLogRecPtr = 0;
    let mut flushPtr: XLogRecPtr = 0;
    let mut applyPtr: XLogRecPtr = 0;
    let mut am_sync = false;
    let mut numwrite: i32 = 0;
    let mut numflush: i32 = 0;
    let mut numapply: i32 = 0;

    // If this WALSender is serving a standby that is not on the list of potential
    // sync standbys then we have nothing to do.  If we are still starting up,
    // still running base backup or the current flush position is still invalid,
    // then leave quickly also.  Streaming or stopping WAL senders are allowed to
    // release waiters.
    let state = wsctl::WalSndGetState();
    if wsctl::my_sync_standby_priority() == 0
        || (state != WalSndState::WALSNDSTATE_STREAMING && state != WalSndState::WALSNDSTATE_STOPPING)
        || XLogRecPtrIsInvalid(wsctl::WalSndGetFlush())
    {
        ANNOUNCE_NEXT_TAKEOVER.with(|c| c.set(true));
        return Ok(());
    }

    // We're a potential sync standby.  Release waiters if there are enough sync
    // standbys and we are considered as sync.
    let guard = sync_rep_lock_acquire()?;

    // Check whether we are a sync standby or not, and calculate the synced
    // positions among all sync standbys.
    let got_recptr =
        SyncRepGetSyncRecPtr(&mut writePtr, &mut flushPtr, &mut applyPtr, &mut am_sync);

    // If we are managing a sync standby, though we weren't prior to this, then
    // announce we are now a sync standby.
    if ANNOUNCE_NEXT_TAKEOVER.with(Cell::get) && am_sync {
        ANNOUNCE_NEXT_TAKEOVER.with(|c| c.set(false));

        // SyncRepConfig != NULL here (got_recptr/am_sync imply it).
        if sync_rep_method() == SYNC_REP_PRIORITY {
            ereport(LOG)
                .errmsg(alloc::format!(
                    "standby \"{}\" is now a synchronous standby with priority {}",
                    application_name_string(),
                    wsctl::my_sync_standby_priority()
                ))
                .finish(syncrep_location("SyncRepReleaseWaiters"))?;
        } else {
            ereport(LOG)
                .errmsg(alloc::format!(
                    "standby \"{}\" is now a candidate for quorum synchronous standby",
                    application_name_string()
                ))
                .finish(syncrep_location("SyncRepReleaseWaiters"))?;
        }
    }

    // If the number of sync standbys is less than requested or we aren't managing
    // a sync standby then just leave.
    if !got_recptr || !am_sync {
        guard.release()?;
        ANNOUNCE_NEXT_TAKEOVER.with(|c| c.set(!am_sync));
        return Ok(());
    }

    // Set the lsn first so that when we wake backends they will release up to
    // this location.
    if wsctl::ctl_lsn(SYNC_REP_WAIT_WRITE as usize) < writePtr {
        wsctl::set_ctl_lsn(SYNC_REP_WAIT_WRITE as usize, writePtr);
        numwrite = SyncRepWakeQueue(false, SYNC_REP_WAIT_WRITE);
    }
    if wsctl::ctl_lsn(SYNC_REP_WAIT_FLUSH as usize) < flushPtr {
        wsctl::set_ctl_lsn(SYNC_REP_WAIT_FLUSH as usize, flushPtr);
        numflush = SyncRepWakeQueue(false, SYNC_REP_WAIT_FLUSH);
    }
    if wsctl::ctl_lsn(SYNC_REP_WAIT_APPLY as usize) < applyPtr {
        wsctl::set_ctl_lsn(SYNC_REP_WAIT_APPLY as usize, applyPtr);
        numapply = SyncRepWakeQueue(false, SYNC_REP_WAIT_APPLY);
    }

    guard.release()?;

    elog(
        DEBUG3,
        alloc::format!(
            "released {} procs up to write {}, {} procs up to flush {}, {} procs up to apply {}",
            numwrite,
            lsn_format(writePtr),
            numflush,
            lsn_format(flushPtr),
            numapply,
            lsn_format(applyPtr)
        ),
    )?;
    Ok(())
}

/// Calculate the synced Write, Flush and Apply positions among sync standbys.
///
/// Return false if the number of sync standbys is less than
/// `synchronous_standby_names` specifies.  Otherwise return true and store the
/// positions.  On return, `*am_sync` is set to true if this walsender is
/// connecting to a sync standby.
fn SyncRepGetSyncRecPtr(
    writePtr: &mut XLogRecPtr,
    flushPtr: &mut XLogRecPtr,
    applyPtr: &mut XLogRecPtr,
    am_sync: &mut bool,
) -> bool {
    // Initialize default results
    *writePtr = InvalidXLogRecPtr;
    *flushPtr = InvalidXLogRecPtr;
    *applyPtr = InvalidXLogRecPtr;
    *am_sync = false;

    // Quick out if not even configured to be synchronous
    let config = match sync_rep_config() {
        Some(config) => config,
        None => return false,
    };

    // Get standbys that are considered as synchronous at this moment
    let sync_standbys = SyncRepGetCandidateStandbys(&config);
    let num_standbys = sync_standbys.len() as i32;

    // Am I among the candidate sync standbys?
    let mut i = 0;
    while i < num_standbys {
        if sync_standbys[i as usize].is_me {
            *am_sync = true;
            break;
        }
        i += 1;
    }

    // Nothing more to do if we are not managing a sync standby or there are not
    // enough synchronous standbys.
    if !(*am_sync) || num_standbys < config.num_sync {
        return false;
    }

    // In a priority-based sync replication, the synced positions are the oldest
    // ones among sync standbys.  In a quorum-based, they are the Nth latest ones.
    if config.syncrep_method == SYNC_REP_PRIORITY {
        SyncRepGetOldestSyncRecPtr(writePtr, flushPtr, applyPtr, &sync_standbys);
    } else {
        SyncRepGetNthLatestSyncRecPtr(
            writePtr,
            flushPtr,
            applyPtr,
            &sync_standbys,
            config.num_sync as u8,
        );
    }

    true
}

/// Calculate the oldest Write, Flush and Apply positions among sync standbys.
fn SyncRepGetOldestSyncRecPtr(
    writePtr: &mut XLogRecPtr,
    flushPtr: &mut XLogRecPtr,
    applyPtr: &mut XLogRecPtr,
    sync_standbys: &[SyncRepStandbyData],
) {
    for stby in sync_standbys {
        let write = stby.write;
        let flush = stby.flush;
        let apply = stby.apply;

        if XLogRecPtrIsInvalid(*writePtr) || *writePtr > write {
            *writePtr = write;
        }
        if XLogRecPtrIsInvalid(*flushPtr) || *flushPtr > flush {
            *flushPtr = flush;
        }
        if XLogRecPtrIsInvalid(*applyPtr) || *applyPtr > apply {
            *applyPtr = apply;
        }
    }
}

/// Calculate the Nth latest Write, Flush and Apply positions among sync standbys.
fn SyncRepGetNthLatestSyncRecPtr(
    writePtr: &mut XLogRecPtr,
    flushPtr: &mut XLogRecPtr,
    applyPtr: &mut XLogRecPtr,
    sync_standbys: &[SyncRepStandbyData],
    nth: u8,
) {
    let num_standbys = sync_standbys.len() as i32;

    // Should have enough candidates, or somebody messed up
    debug_assert!(nth > 0 && (nth as i32) <= num_standbys);

    let mut write_array: Vec<XLogRecPtr> = sync_standbys.iter().map(|s| s.write).collect();
    let mut flush_array: Vec<XLogRecPtr> = sync_standbys.iter().map(|s| s.flush).collect();
    let mut apply_array: Vec<XLogRecPtr> = sync_standbys.iter().map(|s| s.apply).collect();

    // Sort each array in descending order
    write_array.sort_by(cmp_lsn);
    flush_array.sort_by(cmp_lsn);
    apply_array.sort_by(cmp_lsn);

    // Get Nth latest Write, Flush, Apply positions
    *writePtr = write_array[(nth - 1) as usize];
    *flushPtr = flush_array[(nth - 1) as usize];
    *applyPtr = apply_array[(nth - 1) as usize];
}

/// Compare lsn in order to sort array in descending order
/// (`pg_cmp_u64(lsn2, lsn1)`).
fn cmp_lsn(a: &XLogRecPtr, b: &XLogRecPtr) -> core::cmp::Ordering {
    b.cmp(a)
}

/// Return data about walsenders that are candidates to be sync standbys.
///
/// The raw collection over `WalSndCtl->walsnds[]` (active / streaming-or-stopping
/// / synchronous / valid-flush filtering with `walsnd_index` / `is_me` filled
/// in) reads the walsender shmem array directly under each slot's spinlock; the
/// priority-mode trim to `num_sync` is done here.
pub fn SyncRepGetCandidateStandbys(config: &SyncRepConfig) -> Vec<SyncRepStandbyData> {
    let max_wal_senders = vars::max_wal_senders.read();
    let mut standbys: Vec<SyncRepStandbyData> = Vec::with_capacity(max_wal_senders.max(0) as usize);

    // Collect raw data from shared memory
    let mut i: i32 = 0;
    while i < max_wal_senders {
        // Snapshot one slot under its spinlock (C reads pid/state/write/flush/
        // apply/sync_standby_priority of WalSndCtl->walsnds[i]).
        let snap = wsctl::walsnd_candidate_snapshot(i);

        let mut stby = SyncRepStandbyData {
            pid: snap.pid,
            walsnd_index: 0,
            is_me: false,
            sync_standby_priority: snap.sync_standby_priority,
            write: snap.write,
            flush: snap.flush,
            apply: snap.apply,
        };

        // Must be active
        if stby.pid == 0 {
            i += 1;
            continue;
        }

        // Must be streaming or stopping
        if snap.state != WalSndState::WALSNDSTATE_STREAMING
            && snap.state != WalSndState::WALSNDSTATE_STOPPING
        {
            i += 1;
            continue;
        }

        // Must be synchronous
        if stby.sync_standby_priority == 0 {
            i += 1;
            continue;
        }

        // Must have a valid flush position
        if XLogRecPtrIsInvalid(stby.flush) {
            i += 1;
            continue;
        }

        // OK, it's a candidate
        stby.walsnd_index = i;
        stby.is_me = snap.is_me;
        standbys.push(stby);
        i += 1;
    }

    let mut n = standbys.len() as i32;

    // In quorum mode, we return all the candidates.  In priority mode, if we have
    // too many candidates then return only the num_sync ones of highest priority.
    if config.syncrep_method == SYNC_REP_PRIORITY && n > config.num_sync {
        // Sort by priority ...
        standbys.sort_by(standby_priority_comparator);
        // ... then report just the first num_sync ones
        n = config.num_sync;
    }

    standbys.truncate(n as usize);
    standbys
}

/// qsort comparator to sort `SyncRepStandbyData` entries by priority.
fn standby_priority_comparator(
    sa: &SyncRepStandbyData,
    sb: &SyncRepStandbyData,
) -> core::cmp::Ordering {
    // First, sort by increasing priority value
    if sa.sync_standby_priority != sb.sync_standby_priority {
        return sa.sync_standby_priority.cmp(&sb.sync_standby_priority);
    }

    // We might have equal priority values; arbitrarily break ties by position in
    // the WalSnd array.
    sa.walsnd_index.cmp(&sb.walsnd_index)
}

/// Check if we are in the list of sync standbys, and if so, determine priority
/// sequence.  Return priority if set, or zero to indicate that we are not a
/// potential sync standby.
fn SyncRepGetStandbyPriority() -> i32 {
    let mut found = false;

    // Since synchronous cascade replication is not allowed, we always set the
    // priority of cascading walsender to zero.
    if wsctl::am_cascading_walsender() {
        return 0;
    }

    if !SyncStandbysDefined() {
        return 0;
    }
    let config = match sync_rep_config() {
        Some(config) => config,
        None => return 0,
    };

    let application_name = application_name_string();
    let mut priority: i32 = 1;
    let mut idx: usize = 0;
    while priority <= config.nmembers {
        let standby_name = &config.member_names[idx];
        if pg_strcasecmp(standby_name, &application_name) == 0 || standby_name == "*" {
            found = true;
            break;
        }
        idx += 1;
        priority += 1;
    }

    if !found {
        return 0;
    }

    // In quorum-based sync replication, all the standbys in the list have the
    // same priority, one.
    if config.syncrep_method == SYNC_REP_PRIORITY {
        priority
    } else {
        1
    }
}

/// Walk the specified queue from the head.  Set the state of any backends that
/// need to be woken, remove them from the queue, and then wake them.  Pass
/// `all = true` to wake the whole queue; otherwise just wake up to the
/// walsender's LSN.
///
/// The caller holds `SyncRepLock` in exclusive mode.
fn SyncRepWakeQueue(all: bool, mode: i32) -> i32 {
    let mut numprocs = 0;

    debug_assert!(mode >= 0 && mode < NUM_SYNC_REP_WAIT_MODE);
    debug_assert!(lwlock::lwlock_held_by_me_in_mode_main::call(SYNC_REP_LOCK, LWLockMode::LW_EXCLUSIVE));
    debug_assert!(SyncRepQueueIsOrderedByLSN(mode));

    let walsndctl_lsn = wsctl::ctl_lsn(mode as usize);

    // dlist_foreach_modify over WalSndCtl->SyncRepQueue[mode].
    let mut cur = wsctl::with_sync_rep_queue(mode as usize, |q| q.head);
    while cur != INVALID_PROC_NUMBER {
        // Cache the next link before we (possibly) remove the current node.
        let node = proc_s::proc_sync_rep_links::call(cur);
        let next = link_next(node);

        // Assume the queue is ordered by LSN.
        if !all && walsndctl_lsn < proc_s::proc_wait_lsn::call(cur) {
            return numprocs;
        }

        // Remove from queue.
        wsctl::with_sync_rep_queue(mode as usize, |q| proclist_delete(q, cur));

        // SyncRepWaitForLSN() reads syncRepState without holding the lock, so
        // make sure that it sees the queue link being removed before the
        // syncRepState change.  (The C pg_write_barrier() is subsumed by the
        // ordering of the seam writes here.)

        // Set state to complete.
        proc_s::set_proc_sync_rep_state::call(cur, SYNC_REP_WAIT_COMPLETE);

        // Wake only when we have set state and removed from queue.
        latch::set_latch_for_procno::call(cur);

        numprocs += 1;
        cur = next;
    }

    numprocs
}

/// The checkpointer calls this as needed to update the shared
/// `sync_standbys_status` flag, so that backends don't remain permanently wedged
/// if `synchronous_standby_names` is unset.
pub fn SyncRepUpdateSyncStandbysDefined() -> PgResult<()> {
    let sync_standbys_defined = SyncStandbysDefined();

    if sync_standbys_defined
        != ((wsctl::wal_snd_ctl().sync_standbys_status & SYNC_STANDBY_DEFINED) != 0)
    {
        let guard = sync_rep_lock_acquire()?;

        // If synchronous_standby_names has been reset to empty, it's futile for
        // backends to continue waiting.  Since the user no longer wants
        // synchronous replication, we'd better wake them up.
        if !sync_standbys_defined {
            let mut i: i32 = 0;
            while i < NUM_SYNC_REP_WAIT_MODE {
                SyncRepWakeQueue(true, i);
                i += 1;
            }
        }

        // Only allow people to join the queue when there are synchronous standbys
        // defined.  Without this interlock, there's a race condition.
        wsctl::set_ctl_sync_standbys_status(
            SYNC_STANDBY_INIT
                | (if sync_standbys_defined {
                    SYNC_STANDBY_DEFINED
                } else {
                    0
                }),
        );

        guard.release()?;
    } else if (wsctl::wal_snd_ctl().sync_standbys_status & SYNC_STANDBY_INIT) == 0 {
        let guard = sync_rep_lock_acquire()?;

        // Note that there is no need to wake up the queues here.
        debug_assert!(!SyncStandbysDefined());

        // Even if there is no sync standby defined, let the readers of this
        // information know that the sync standby data has been initialized.
        wsctl::set_ctl_sync_standbys_status(
            wsctl::wal_snd_ctl().sync_standbys_status | SYNC_STANDBY_INIT,
        );

        guard.release()?;
    }
    Ok(())
}

/// `SyncRepQueueIsOrderedByLSN(mode)` (USE_ASSERT_CHECKING) — the queue is
/// ordered by LSN and no two procs share an LSN.
fn SyncRepQueueIsOrderedByLSN(mode: i32) -> bool {
    debug_assert!(mode >= 0 && mode < NUM_SYNC_REP_WAIT_MODE);

    let mut last_lsn: XLogRecPtr = 0;
    let mut cur = wsctl::with_sync_rep_queue(mode as usize, |q| q.head);
    while cur != INVALID_PROC_NUMBER {
        let wait_lsn = proc_s::proc_wait_lsn::call(cur);
        // Check the queue is ordered by LSN and that multiple procs don't have
        // matching LSNs.
        if wait_lsn <= last_lsn {
            return false;
        }
        last_lsn = wait_lsn;
        cur = link_next(proc_s::proc_sync_rep_links::call(cur));
    }

    true
}

// ===========================================================================
// Synchronous Replication functions executed by any process (GUC hooks)
// ===========================================================================

/// Outcome of [`check_synchronous_standby_names`], mirroring the C `bool`
/// return plus the produced `*extra`.
pub enum CheckResult {
    /// C `return true`: the value is accepted, with this parsed "extra"
    /// (`None` mirrors the C `*extra = NULL` for an empty value).
    Ok(Option<SyncRepConfig>),
    /// C `return false`: the value is rejected (the `GUC_check_*` reporting has
    /// already run).
    Reject,
}

/// GUC check hook for `synchronous_standby_names` (`check_synchronous_standby_names`).
pub fn check_synchronous_standby_names(newval: Option<&str>) -> PgResult<CheckResult> {
    if let Some(s) = newval {
        if !s.is_empty() {
            // Parse the synchronous_standby_names string in a short-lived context
            // (the C comment: any cruft is freed when the context is deleted).
            let scratch = mcx::MemoryContext::new("check_synchronous_standby_names");
            let mcx = scratch.mcx();

            let scanner =
                scanner_seams::syncrep_scanner_init::call(mcx, s)?;
            let parse_outcome = syncrep_yyparse(mcx, scanner);
            scanner_seams::syncrep_scanner_finish::call(scanner);

            let parsed = match parse_outcome {
                Ok(parsed) => parsed,
                Err(err) => {
                    use misc_guc::{GUC_check_errcode, GUC_check_errdetail};
                    use ::types_error::ERRCODE_SYNTAX_ERROR;
                    GUC_check_errcode(ERRCODE_SYNTAX_ERROR);
                    let msg = err.message();
                    if !msg.is_empty() {
                        GUC_check_errdetail(msg.to_string());
                    } else {
                        // translator: %s is a GUC name
                        GUC_check_errdetail(alloc::format!(
                            "\"{}\" parser failed.",
                            "synchronous_standby_names"
                        ));
                    }
                    return Ok(CheckResult::Reject);
                }
            };

            if parsed.num_sync() <= 0 {
                use ::misc_guc::GUC_check_errmsg;
                GUC_check_errmsg(alloc::format!(
                    "number of synchronous standbys ({}) must be greater than zero",
                    parsed.num_sync()
                ));
                return Ok(CheckResult::Reject);
            }

            // The C hook guc_malloc's a flat copy as the GUC "extra"; here we
            // materialize the owned parsed config directly.
            let config = SyncRepConfig {
                config_size: parsed.config_size(),
                num_sync: parsed.num_sync(),
                syncrep_method: parsed.syncrep_method(),
                nmembers: parsed.nmembers(),
                member_names: parsed.member_names().map(|name| name.to_string()).collect(),
            };
            return Ok(CheckResult::Ok(Some(config)));
        }
    }
    Ok(CheckResult::Ok(None))
}

/// GUC assign hook for `synchronous_standby_names`.
pub fn assign_synchronous_standby_names(extra: Option<SyncRepConfig>) {
    SYNC_REP_CONFIG.with(|cell| *cell.borrow_mut() = extra);
}

/// GUC assign hook for `synchronous_commit`.
pub fn assign_synchronous_commit(newval: i32) {
    let mode = match newval {
        x if x == SYNCHRONOUS_COMMIT_REMOTE_WRITE => SYNC_REP_WAIT_WRITE,
        x if x == SYNCHRONOUS_COMMIT_REMOTE_FLUSH => SYNC_REP_WAIT_FLUSH,
        x if x == SYNCHRONOUS_COMMIT_REMOTE_APPLY => SYNC_REP_WAIT_APPLY,
        _ => SYNC_REP_NO_WAIT,
    };
    SYNC_REP_WAIT_MODE.with(|c| c.set(mode));
}

// ===========================================================================
// Helpers consumed by the inward seam adapters (`mod seams`).
// ===========================================================================

/// `SyncRepConfig->syncrep_method`, or `SYNC_REP_PRIORITY` when unset (the C
/// code reaches this only when `SyncRepConfig != NULL`).
pub fn sync_rep_method() -> u8 {
    sync_rep_config()
        .map(|c| c.syncrep_method)
        .unwrap_or(SYNC_REP_PRIORITY)
}

/// `pg_strcasecmp(a, b)` — case-insensitive ASCII compare returning the sign of
/// the first differing (lowercased) byte difference, matching libpgport.
fn pg_strcasecmp(a: &str, b: &str) -> i32 {
    let ab = a.as_bytes();
    let bb = b.as_bytes();
    let mut i = 0;
    loop {
        let ca = if i < ab.len() { ab[i] } else { 0 };
        let cb = if i < bb.len() { bb[i] } else { 0 };
        let la = ascii_tolower(ca);
        let lb = ascii_tolower(cb);
        if la != lb {
            return la as i32 - lb as i32;
        }
        if ca == 0 {
            return 0;
        }
        i += 1;
    }
}

/// ASCII `tolower` matching `pg_tolower` for the C-locale path `pg_strcasecmp`
/// uses.
fn ascii_tolower(c: u8) -> u8 {
    if c.is_ascii_uppercase() {
        c + (b'a' - b'A')
    } else {
        c
    }
}

// ===========================================================================
// `proclist`-style intrusive queue ops over `syncRepLinks` (pgprocno links).
//
// The SyncRepQueue heads live in WalSndCtl (walsender); the per-PGPROC links
// are in `syncRepLinks`, reached via proc-seams.  These mirror the C `dlist`
// operations (`dlist_push_head`, `dlist_insert_after`, `dlist_delete_thoroughly`,
// `dlist_node_is_detached`) over the shmem-safe pgprocno representation, exactly
// like lwlock.c's `proclist` helpers over `lwWaitLink`.  All run with
// `SyncRepLock` held (except the lock-free detached read).
// ===========================================================================

type Queue = ::types_storage::storage::proclist_head;

#[inline]
fn link_next(node: proclist_node) -> ProcNumber {
    node.next
}

#[inline]
fn link_prev(node: proclist_node) -> ProcNumber {
    node.prev
}

/// `dlist_node_is_detached(&GetPGProcByNumber(procno)->syncRepLinks)` — a node
/// not on any list has the `{0,0}` zero-init marker (the LWLock/CV convention).
fn proc_links_detached(procno: ProcNumber) -> bool {
    let node = proc_s::proc_sync_rep_links::call(procno);
    node.next == 0 && node.prev == 0
}

fn my_proc_links_detached() -> bool {
    proc_links_detached(proc_s::my_proc_number::call())
}

/// `dlist_push_head(queue, &proc->syncRepLinks)`.
fn proclist_push_head(queue: &mut Queue, procno: ProcNumber) {
    debug_assert!(proc_links_detached(procno));
    let mut node = proclist_node {
        next: INVALID_PROC_NUMBER,
        prev: INVALID_PROC_NUMBER,
    };
    if queue.head == INVALID_PROC_NUMBER {
        debug_assert!(queue.tail == INVALID_PROC_NUMBER);
        node.next = INVALID_PROC_NUMBER;
        queue.tail = procno;
    } else {
        node.next = queue.head;
        let mut head_node = proc_s::proc_sync_rep_links::call(queue.head);
        head_node.prev = procno;
        proc_s::set_proc_sync_rep_links::call(queue.head, head_node);
    }
    proc_s::set_proc_sync_rep_links::call(procno, node);
    queue.head = procno;
}

/// `dlist_insert_after(&after->syncRepLinks, &proc->syncRepLinks)` — splice
/// `procno` immediately after `after` in the queue.
fn proclist_insert_after(queue: &mut Queue, after: ProcNumber, procno: ProcNumber) {
    debug_assert!(proc_links_detached(procno));
    let mut after_node = proc_s::proc_sync_rep_links::call(after);
    let next = after_node.next;

    let node = proclist_node {
        prev: after,
        next,
    };
    proc_s::set_proc_sync_rep_links::call(procno, node);

    after_node.next = procno;
    proc_s::set_proc_sync_rep_links::call(after, after_node);

    if next == INVALID_PROC_NUMBER {
        // `after` was the tail.
        queue.tail = procno;
    } else {
        let mut next_node = proc_s::proc_sync_rep_links::call(next);
        next_node.prev = procno;
        proc_s::set_proc_sync_rep_links::call(next, next_node);
    }
}

/// `dlist_delete_thoroughly(&proc->syncRepLinks)` — unlink `procno` from
/// `queue` and reset its links to the detached `{0,0}` marker.
fn proclist_delete(queue: &mut Queue, procno: ProcNumber) {
    let node = proc_s::proc_sync_rep_links::call(procno);

    if node.prev == INVALID_PROC_NUMBER {
        queue.head = node.next;
    } else {
        let mut prev_node = proc_s::proc_sync_rep_links::call(node.prev);
        prev_node.next = node.next;
        proc_s::set_proc_sync_rep_links::call(node.prev, prev_node);
    }

    if node.next == INVALID_PROC_NUMBER {
        queue.tail = node.prev;
    } else {
        let mut next_node = proc_s::proc_sync_rep_links::call(node.next);
        next_node.prev = node.prev;
        proc_s::set_proc_sync_rep_links::call(node.next, next_node);
    }

    // dlist_delete_thoroughly: mark detached.
    proc_s::set_proc_sync_rep_links::call(procno, proclist_node { next: 0, prev: 0 });
}

/// Remove `MyProc` from whichever sync-rep queue it is on.  C's
/// `dlist_delete_thoroughly(&MyProc->syncRepLinks)` doesn't name the mode (the
/// links carry the membership); with the head/tail in `WalSndCtl` we find the
/// containing queue by checking each mode's membership and unlink there.
fn proclist_delete_my_proc_all_modes() {
    let me = proc_s::my_proc_number::call();
    let mut mode = 0usize;
    while mode < NUM_SYNC_REP_WAIT_MODE as usize {
        let found = wsctl::with_sync_rep_queue(mode, |q| {
            if queue_contains(q, me) {
                proclist_delete(q, me);
                true
            } else {
                false
            }
        });
        if found {
            return;
        }
        mode += 1;
    }
    // Not found on any queue: still reset the links to the detached marker, the
    // dlist_delete_thoroughly post-condition.
    proc_s::set_proc_sync_rep_links::call(me, proclist_node { next: 0, prev: 0 });
}

/// Whether `procno` is currently a member of `queue`.
fn queue_contains(queue: &Queue, procno: ProcNumber) -> bool {
    let mut cur = queue.head;
    while cur != INVALID_PROC_NUMBER {
        if cur == procno {
            return true;
        }
        cur = link_next(proc_s::proc_sync_rep_links::call(cur));
    }
    false
}
