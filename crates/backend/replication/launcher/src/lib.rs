// launcher.c: the logical replication launcher and the LogicalRepWorker slot
// pool. Thread-model divergences (same style as bgworker.c's port): the
// LogicalRepCtx shmem struct + LogicalRepWorkerLock LWLock collapse into one
// std Mutex over a plain Vec (cold supervisor path); worker->proc becomes the
// worker's ProcNumber + pid; the last-start-times dsa/dshash becomes a HashMap
// inside the same Mutex. C code that holds the LWLock across latch waits
// releases/reacquires per iteration — mirrored here.
#![allow(non_snake_case)]

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use pgsync::Mutex;

use elog::{elog as log_report, ereport};
use guc_tables::{vars, GucVarAccessors};
use init_small::globals as g;
use types_core::primitive::XLogRecPtr;
use types_core::{pid_t, InvalidOid, Oid, ProcNumber, TimestampTz};
use types_error::{
    ErrorLocation, PgResult, DEBUG1, ERRCODE_CONFIGURATION_LIMIT_EXCEEDED,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR, LOG, WARNING,
};
use types_storage::latch::LatchHandle;
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};

#[cfg(test)]
mod tests;

const SRC: &str = "src/backend/replication/logical/launcher.c";
const InvalidPid: pid_t = -1;
const InvalidXLogRecPtr: XLogRecPtr = 0;

// DEFAULT_NAPTIME_PER_CYCLE (launcher.c:52), ms.
const DEFAULT_NAPTIME_PER_CYCLE: i64 = 180_000;

const PG_WAIT_IPC: u32 = 0x0800_0000;
const WAIT_EVENT_BGWORKER_STARTUP: u32 = PG_WAIT_IPC + 6;
const WAIT_EVENT_BGWORKER_SHUTDOWN: u32 = PG_WAIT_IPC + 5;
// LOGICAL_LAUNCHER_MAIN's index in wait_event_names.txt's Activity section is
// 8, not 5 — 5 is CHECKPOINTER_SHUTDOWN. GL-SYNCWEDGE-1 (second instance of
// the same class); scripts/lint-waitevent-tags.sh pins it.
const WAIT_EVENT_LOGICAL_LAUNCHER_MAIN: u32 = 0x0500_0000 | 8;

fn loc(line: i32, func: &'static str) -> ErrorLocation {
    ErrorLocation::new(SRC, line, func)
}

static MAX_LOGICAL_REPLICATION_WORKERS: AtomicI32 = AtomicI32::new(4);
static MAX_SYNC_WORKERS_PER_SUBSCRIPTION: AtomicI32 = AtomicI32::new(2);
static MAX_PARALLEL_APPLY_WORKERS_PER_SUBSCRIPTION: AtomicI32 = AtomicI32::new(2);

thread_local! {
    static ON_COMMIT_LAUNCHER_WAKEUP: Cell<bool> = const { Cell::new(false) };
    // on_commit_wakeup_workers_subids (worker.c): subscriptions whose workers
    // want a wakeup at commit. C allocates the list in TopTransactionContext;
    // a plain TLS Vec here — AtEOXact_LogicalRepWorkers clears it on every
    // transaction-end path, matching C's unconditional list reset.
    static ON_COMMIT_WAKEUP_WORKERS_SUBIDS: RefCell<Vec<Oid>> = const { RefCell::new(Vec::new()) };
    // MyLogicalRepWorker: this worker thread's slot index.
    static MY_WORKER_SLOT: Cell<Option<usize>> = const { Cell::new(None) };
}

pub fn max_logical_replication_workers() -> i32 {
    MAX_LOGICAL_REPLICATION_WORKERS.load(Ordering::Relaxed)
}
pub fn max_sync_workers_per_subscription() -> i32 {
    MAX_SYNC_WORKERS_PER_SUBSCRIPTION.load(Ordering::Relaxed)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LogicalRepWorkerType {
    Unknown,
    TableSync,
    Apply,
    ParallelApply,
}

// LogicalRepWorker (worker_internal.h).
// pg_subscription_rel.h srsubstate values (shared with pg_subscription).
pub const SUBREL_STATE_SYNCWAIT: u8 = b'w';
pub const SUBREL_STATE_CATCHUP: u8 = b'c';

#[derive(Clone)]
pub struct LogicalRepWorker {
    pub wtype: LogicalRepWorkerType,
    pub in_use: bool,
    pub generation: u16,
    // C's `proc`: the attached worker's PGPROC. (pid 0 = not attached.)
    pub proc_pid: pid_t,
    pub proc_no: Option<ProcNumber>,
    pub dbid: Oid,
    pub userid: Oid,
    pub subid: Oid,
    pub relid: Oid,
    pub relstate: u8,
    pub relstate_lsn: XLogRecPtr,
    pub leader_pid: pid_t,
    pub parallel_apply: bool,
    pub launch_time: TimestampTz,
    pub last_lsn: XLogRecPtr,
    pub last_send_time: TimestampTz,
    pub last_recv_time: TimestampTz,
    pub reply_lsn: XLogRecPtr,
    pub reply_time: TimestampTz,
}

impl LogicalRepWorker {
    fn empty() -> Self {
        LogicalRepWorker {
            wtype: LogicalRepWorkerType::Unknown,
            in_use: false,
            generation: 0,
            proc_pid: 0,
            proc_no: None,
            dbid: InvalidOid,
            userid: InvalidOid,
            subid: InvalidOid,
            relid: InvalidOid,
            relstate: 0,
            relstate_lsn: InvalidXLogRecPtr,
            leader_pid: InvalidPid,
            parallel_apply: false,
            launch_time: 0,
            last_lsn: InvalidXLogRecPtr,
            last_send_time: 0,
            last_recv_time: 0,
            reply_lsn: InvalidXLogRecPtr,
            reply_time: 0,
        }
    }
    pub fn is_parallel_apply(&self) -> bool {
        self.wtype == LogicalRepWorkerType::ParallelApply
    }
    pub fn is_tablesync(&self) -> bool {
        self.wtype == LogicalRepWorkerType::TableSync
    }
}

// LogicalRepCtxStruct: launcher pid + worker slots + last-start times (C: dshash).
struct LogicalRepCtx {
    launcher_pid: pid_t,
    launcher_proc: Option<ProcNumber>,
    workers: Vec<LogicalRepWorker>,
    last_start_times: HashMap<Oid, TimestampTz>,
}

pgsync::process_global! {
    static CTX: Mutex<Option<LogicalRepCtx>> = Mutex::new(None);
}

fn with_ctx<R>(f: impl FnOnce(&mut LogicalRepCtx) -> R) -> R {
    let mut guard = CTX.lock().unwrap_or_else(|e| e.into_inner());
    let ctx = guard
        .as_mut()
        .unwrap_or_else(|| panic!("LogicalRepCtx accessed before ApplyLauncherShmemInit"));
    f(ctx)
}

// ApplyLauncherShmemInit (launcher.c:964).
pub fn ApplyLauncherShmemInit() {
    let mut guard = CTX.lock().unwrap_or_else(|e| e.into_inner());
    *guard = Some(LogicalRepCtx {
        launcher_pid: 0,
        launcher_proc: None,
        workers: (0..max_logical_replication_workers().max(0) as usize)
            .map(|_| LogicalRepWorker::empty())
            .collect(),
        last_start_times: HashMap::new(),
    });
}

// ApplyLauncherRegister (launcher.c:930): static bgworker registration.
pub fn ApplyLauncherRegister() {
    if max_logical_replication_workers() == 0 || g::IsBinaryUpgrade() {
        return;
    }
    let bgw = bgworker::BackgroundWorker {
        bgw_name: "logical replication launcher".to_string(),
        bgw_type: "logical replication launcher".to_string(),
        bgw_flags: bgworker::BGWORKER_SHMEM_ACCESS | bgworker::BGWORKER_BACKEND_DATABASE_CONNECTION,
        bgw_start_time: bgworker::BgWorkerStartTime::RecoveryFinished,
        bgw_restart_time: 5,
        bgw_main: launcher_bgw_main,
        bgw_main_arg: 0,
        bgw_extra: [0; bgworker::BGW_EXTRALEN],
        bgw_notify_pid: 0,
    };
    bgworker::RegisterBackgroundWorker(&bgw);
}

fn launcher_bgw_main(main_arg: u64) -> PgResult<()> {
    ApplyLauncherMain(main_arg)
}

// The apply/tablesync worker's bgw_main: dispatch through the seam; a clean
// LOG + exit while the worker port hasn't landed.
fn logicalrep_worker_bgw_main(main_arg: u64) -> PgResult<()> {
    if logical_worker_seams::apply_worker_main::is_installed() {
        logical_worker_seams::apply_worker_main::call(main_arg)
    } else {
        let _ = log_report(LOG, "logical replication apply worker unported; exiting".to_string());
        Ok(())
    }
}

// Read-side helper: contexts that never launched workers (single-user, no
// postmaster shmem pass) see an empty pool rather than a panic — in C the
// zeroed shmem struct exists unconditionally.
fn with_ctx_opt<R>(default: R, f: impl FnOnce(&mut LogicalRepCtx) -> R) -> R {
    let mut guard = CTX.lock().unwrap_or_else(|e| e.into_inner());
    match guard.as_mut() {
        Some(ctx) => f(ctx),
        None => default,
    }
}

// logicalrep_worker_find (launcher.c:247). Returns the slot index.
pub fn logicalrep_worker_find(subid: Oid, relid: Oid, only_running: bool) -> Option<usize> {
    with_ctx_opt(None, |ctx| find_locked(ctx, subid, relid, only_running))
}

fn find_locked(ctx: &LogicalRepCtx, subid: Oid, relid: Oid, only_running: bool) -> Option<usize> {
    ctx.workers.iter().position(|w| {
        !w.is_parallel_apply()
            && w.in_use
            && w.subid == subid
            && w.relid == relid
            && (!only_running || w.proc_pid != 0)
    })
}

// logicalrep_workers_find (launcher.c:279): all workers for a subscription.
pub fn logicalrep_workers_find(subid: Oid, only_running: bool) -> Vec<usize> {
    with_ctx_opt(Vec::new(), |ctx| {
        (0..ctx.workers.len())
            .filter(|&i| {
                let w = &ctx.workers[i];
                w.in_use && w.subid == subid && (!only_running || w.proc_pid != 0)
            })
            .collect()
    })
}

pub fn worker_snapshot(slot: usize) -> Option<LogicalRepWorker> {
    with_ctx_opt(None, |ctx| ctx.workers.get(slot).cloned())
}

fn logicalrep_worker_cleanup_locked(w: &mut LogicalRepWorker) {
    w.wtype = LogicalRepWorkerType::Unknown;
    w.in_use = false;
    w.proc_pid = 0;
    w.proc_no = None;
    w.dbid = InvalidOid;
    w.userid = InvalidOid;
    w.subid = InvalidOid;
    w.relid = InvalidOid;
    w.leader_pid = InvalidPid;
    w.parallel_apply = false;
}

// logicalrep_worker_launch (launcher.c:310), WORKERTYPE_APPLY/TABLESYNC arms
// (parallel apply needs the DSM error queue — refused until that port).
pub fn logicalrep_worker_launch(
    wtype: LogicalRepWorkerType,
    dbid: Oid,
    subid: Oid,
    subname: &str,
    userid: Oid,
    relid: Oid,
) -> PgResult<bool> {
    use LogicalRepWorkerType::*;
    let is_tablesync = wtype == TableSync;
    debug_assert!(wtype != Unknown);
    debug_assert_eq!(is_tablesync, relid != InvalidOid);
    if wtype == ParallelApply {
        panic!("logicalrep_worker_launch: parallel apply workers unported (DSM error queue)");
    }

    let _ = log_report(
        DEBUG1,
        format!("starting logical replication worker for subscription \"{subname}\""),
    );

    if guc_tables::vars::max_active_replication_origins.read() == 0 {
        ereport(ERROR)
            .errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED)
            .errmsg("cannot start logical replication workers when \"max_active_replication_origins\" is 0")
            .finish(loc(343, "logicalrep_worker_launch"))?;
        unreachable!();
    }

    let now = timestamp_seams::get_current_timestamp::call();
    let wal_receiver_timeout = guc_tables::vars::wal_receiver_timeout.read();

    enum Pick {
        NoSlot,
        SyncLimit,
        Slot(usize, u16),
    }
    let picked = with_ctx(|ctx| {
        loop {
            let free = ctx.workers.iter().position(|w| !w.in_use);
            let nsyncworkers =
                ctx.workers.iter().filter(|w| w.is_tablesync() && w.subid == subid).count() as i32;

            // Garbage-collect workers that never managed to attach.
            if free.is_none() || nsyncworkers >= max_sync_workers_per_subscription() {
                let mut did_cleanup = false;
                for w in ctx.workers.iter_mut() {
                    if w.in_use
                        && w.proc_pid == 0
                        && now - w.launch_time > (wal_receiver_timeout as i64) * 1000
                    {
                        let _ = log_report(
                            WARNING,
                            format!(
                                "logical replication worker for subscription {} took too long to start; canceled",
                                w.subid
                            ),
                        );
                        logicalrep_worker_cleanup_locked(w);
                        did_cleanup = true;
                    }
                }
                if did_cleanup {
                    continue;
                }
            }

            if is_tablesync && nsyncworkers >= max_sync_workers_per_subscription() {
                return Pick::SyncLimit;
            }
            let Some(slot) = free else {
                return Pick::NoSlot;
            };

            let w = &mut ctx.workers[slot];
            w.wtype = wtype;
            w.launch_time = now;
            w.in_use = true;
            w.generation = w.generation.wrapping_add(1);
            w.proc_pid = 0;
            w.proc_no = None;
            w.dbid = dbid;
            w.userid = userid;
            w.subid = subid;
            w.relid = relid;
            w.relstate = 0;
            w.relstate_lsn = InvalidXLogRecPtr;
            w.leader_pid = InvalidPid;
            w.parallel_apply = false;
            w.last_lsn = InvalidXLogRecPtr;
            w.last_send_time = 0;
            w.last_recv_time = 0;
            w.reply_lsn = InvalidXLogRecPtr;
            w.reply_time = 0;
            return Pick::Slot(slot, w.generation);
        }
    });

    let (slot, generation) = match picked {
        Pick::SyncLimit => return Ok(false),
        Pick::NoSlot => {
            let _ = ereport(WARNING)
                .errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED)
                .errmsg("out of logical replication worker slots")
                .errhint("You might need to increase \"max_logical_replication_workers\".")
                .finish(loc(434, "logicalrep_worker_launch"));
            return Ok(false);
        }
        Pick::Slot(s, gen) => (s, gen),
    };

    let (name, btype) = match wtype {
        Apply => (
            format!(
                "logical replication apply worker for subscription {}",
                subid
            ),
            "logical replication apply worker",
        ),
        TableSync => (
            format!(
                "logical replication tablesync worker for subscription {} sync {}",
                subid,
                relid
            ),
            "logical replication tablesync worker",
        ),
        _ => unreachable!(),
    };

    let bgw = bgworker::BackgroundWorker {
        bgw_name: name,
        bgw_type: btype.to_string(),
        bgw_flags: bgworker::BGWORKER_SHMEM_ACCESS | bgworker::BGWORKER_BACKEND_DATABASE_CONNECTION,
        bgw_start_time: bgworker::BgWorkerStartTime::RecoveryFinished,
        bgw_restart_time: bgworker::BGW_NEVER_RESTART,
        bgw_main: logicalrep_worker_bgw_main,
        bgw_main_arg: slot as u64,
        bgw_extra: [0; bgworker::BGW_EXTRALEN],
        bgw_notify_pid: g::MyProcPid(),
    };

    let handle = bgworker::RegisterDynamicBackgroundWorker(bgw)?;
    let Some(handle) = handle else {
        with_ctx(|ctx| {
            debug_assert_eq!(generation, ctx.workers[slot].generation);
            logicalrep_worker_cleanup_locked(&mut ctx.workers[slot]);
        });
        let _ = ereport(WARNING)
            .errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED)
            .errmsg("out of background worker slots")
            .errhint("You might need to increase \"max_worker_processes\".")
            .finish(loc(521, "logicalrep_worker_launch"));
        return Ok(false);
    };

    WaitForReplicationWorkerAttach(slot, generation, &handle)
}

// WaitForReplicationWorkerAttach (launcher.c:175).
fn WaitForReplicationWorkerAttach(
    slot: usize,
    generation: u16,
    handle: &bgworker::BackgroundWorkerHandle,
) -> PgResult<bool> {
    let mut dropped_latch = false;
    let result;
    loop {
        postgres_seams::check_for_interrupts::call()?;

        let state = with_ctx(|ctx| {
            let w = &ctx.workers[slot];
            if !w.in_use || w.proc_pid != 0 {
                Some(w.in_use)
            } else {
                None
            }
        });
        if let Some(r) = state {
            result = r;
            break;
        }

        let (status, _pid) = bgworker::GetBackgroundWorkerPid(handle);
        if status == bgworker::BgwHandleStatus::BGWH_STOPPED {
            with_ctx(|ctx| {
                if generation == ctx.workers[slot].generation {
                    logicalrep_worker_cleanup_locked(&mut ctx.workers[slot]);
                }
            });
            result = false;
            break;
        }

        let rc = latch::WaitLatch(
            g::MyLatch(),
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            10,
            WAIT_EVENT_BGWORKER_STARTUP,
        )?;
        if rc & WL_LATCH_SET != 0 {
            if let Some(l) = g::MyLatch() {
                latch::ResetLatch(l);
            }
            postgres_seams::check_for_interrupts::call()?;
            dropped_latch = true;
        }
    }

    if dropped_latch {
        if let Some(l) = g::MyLatch() {
            latch::SetLatch(l);
        }
    }
    Ok(result)
}

// logicalrep_worker_stop (launcher.c:619) + _internal (:537).
pub fn logicalrep_worker_stop(subid: Oid, relid: Oid) -> PgResult<()> {
    let Some(slot) = logicalrep_worker_find(subid, relid, false) else {
        return Ok(());
    };
    logicalrep_worker_stop_internal(slot, procsignal::signums::SIGTERM)
}

fn logicalrep_worker_stop_internal(slot: usize, signo: i32) -> PgResult<()> {
    let generation = with_ctx(|ctx| ctx.workers[slot].generation);

    // Still starting up: wait for attach, then kill.
    loop {
        let st = with_ctx(|ctx| {
            let w = &ctx.workers[slot];
            if !w.in_use || w.generation != generation {
                Some(None) // gone or replaced
            } else if w.proc_pid != 0 {
                Some(Some(w.proc_pid))
            } else {
                None // in_use, not attached yet
            }
        });
        match st {
            Some(None) => return Ok(()),
            Some(Some(pid)) => {
                let _ = procsignal::SendThreadSignal(pid, signo);
                break;
            }
            None => {
                let rc = latch::WaitLatch(
                    g::MyLatch(),
                    WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                    10,
                    WAIT_EVENT_BGWORKER_STARTUP,
                )?;
                if rc & WL_LATCH_SET != 0 {
                    if let Some(l) = g::MyLatch() {
                        latch::ResetLatch(l);
                    }
                    postgres_seams::check_for_interrupts::call()?;
                }
            }
        }
    }

    // ... and wait for it to detach.
    loop {
        let gone = with_ctx(|ctx| {
            let w = &ctx.workers[slot];
            w.proc_pid == 0 || w.generation != generation
        });
        if gone {
            return Ok(());
        }
        let rc = latch::WaitLatch(
            g::MyLatch(),
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            10,
            WAIT_EVENT_BGWORKER_SHUTDOWN,
        )?;
        if rc & WL_LATCH_SET != 0 {
            if let Some(l) = g::MyLatch() {
                latch::ResetLatch(l);
            }
            postgres_seams::check_for_interrupts::call()?;
        }
    }
}

// --- tablesync shared-state accessors (C: worker->relstate under relmutex;
// the pool Mutex is the spinlock rendering) ---

// My own slot's relstate write (tablesync worker side).
pub fn my_worker_set_relstate(state: u8, lsn: XLogRecPtr) {
    if let Some(slot) = my_worker_slot() {
        with_ctx(|ctx| {
            ctx.workers[slot].relstate = state;
            ctx.workers[slot].relstate_lsn = lsn;
        });
    }
}

pub fn my_worker_relstate() -> (u8, XLogRecPtr) {
    match my_worker_slot() {
        Some(slot) => with_ctx(|ctx| (ctx.workers[slot].relstate, ctx.workers[slot].relstate_lsn)),
        None => (0, InvalidXLogRecPtr),
    }
}

// The apply side's SYNCWAIT->CATCHUP handshake (tablesync.c:507): read the
// sync worker's state; if SYNCWAIT, promote to CATCHUP with
// max(relstate_lsn, current_lsn) and wake it. Returns the state READ (pre-
// promotion) + its lsn, None when no worker exists for (subid, relid).
pub fn sync_worker_read_and_maybe_catchup(
    subid: Oid,
    relid: Oid,
    current_lsn: XLogRecPtr,
) -> Option<(u8, XLogRecPtr)> {
    let (st, proc_no) = with_ctx_opt(None, |ctx| {
        let i = find_locked(ctx, subid, relid, false)?;
        let w = &mut ctx.workers[i];
        let read = (w.relstate, w.relstate_lsn);
        if w.relstate == SUBREL_STATE_SYNCWAIT {
            w.relstate = SUBREL_STATE_CATCHUP;
            w.relstate_lsn = w.relstate_lsn.max(current_lsn);
        }
        Some((read, w.proc_no))
    })?;
    if st.0 == SUBREL_STATE_SYNCWAIT {
        if let Some(p) = proc_no {
            latch::SetLatch(LatchHandle::proc(p));
        }
    }
    Some(st)
}

// logicalrep_sync_worker_count (launcher.c:868).
pub fn logicalrep_sync_worker_count(subid: Oid) -> usize {
    with_ctx_opt(0, |ctx| {
        ctx.workers
            .iter()
            .filter(|w| w.in_use && w.subid == subid && w.is_tablesync())
            .count()
    })
}

// Tablesync start-time throttle (tablesync.c last_start_times; the launcher
// ctx HashMap doubles as C's worker-local HTAB — keyed per relid).
pub fn tablesync_start_time_check_and_set(relid: Oid, now: TimestampTz, interval_ms: i32) -> bool {
    with_ctx_opt(false, |ctx| {
        let due = match ctx.last_start_times.get(&relid) {
            // TimestampDifferenceExceeds: timestamps are microseconds.
            Some(&last) => now.saturating_sub(last) >= interval_ms as i64 * 1000,
            None => true,
        };
        if due {
            ctx.last_start_times.insert(relid, now);
        }
        due
    })
}

// logicalrep_worker_wakeup (launcher.c:686).
pub fn logicalrep_worker_wakeup(subid: Oid, relid: Oid) {
    let proc_no =
        with_ctx(|ctx| find_locked(ctx, subid, relid, true).and_then(|i| ctx.workers[i].proc_no));
    if let Some(p) = proc_no {
        latch::SetLatch(LatchHandle::proc(p));
    }
}

// logicalrep_worker_attach (launcher.c:717). Called by the worker itself.
pub fn logicalrep_worker_attach(slot: usize) -> PgResult<()> {
    let attached = with_ctx(|ctx| {
        let w = &mut ctx.workers[slot];
        if !w.in_use {
            return Err("empty");
        }
        if w.proc_pid != 0 {
            return Err("used");
        }
        w.proc_pid = g::MyProcPid();
        w.proc_no = lmgr_proc::MyProc();
        Ok(())
    });
    match attached {
        Ok(()) => {
            MY_WORKER_SLOT.with(|c| c.set(Some(slot)));
            // logicalrep_worker_onexit (launcher.c:825, via before_shmem_exit
            // at attach): the slot must clear on EVERY exit path — a SIGTERM
            // FATAL tears the worker thread down without unwinding through
            // ApplyWorkerMain's normal-return detach, and a stuck slot makes
            // logicalrep_worker_stop (DROP SUBSCRIPTION) wait forever.
            ipc::on_shmem_exit(logicalrep_worker_onexit, 0);
            Ok(())
        }
        Err(kind) => ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(if kind == "empty" {
                format!("logical replication worker slot {slot} is empty, cannot attach")
            } else {
                format!(
                    "logical replication worker slot {slot} is already used by another worker, cannot attach"
                )
            })
            .finish(loc(729, "logicalrep_worker_attach")),
    }
}

pub fn my_worker_slot() -> Option<usize> {
    MY_WORKER_SLOT.with(|c| c.get())
}

// logicalrep_worker_onexit (launcher.c:825): shmem-exit callback form.
fn logicalrep_worker_onexit(_code: i32, _arg: usize) {
    logicalrep_worker_detach();
}

// logicalrep_worker_detach + onexit's launcher wakeup (launcher.c:754/825).
// Runs from the worker's normal exit path AND the shmem-exit callback; the
// MY_WORKER_SLOT take() makes it idempotent.
pub fn logicalrep_worker_detach() {
    if let Some(slot) = my_worker_slot() {
        with_ctx(|ctx| logicalrep_worker_cleanup_locked(&mut ctx.workers[slot]));
        MY_WORKER_SLOT.with(|c| c.set(None));
    }
    ApplyLauncherWakeup();
}

// ApplyLauncherWakeupAtCommit / AtEOXact_ApplyLauncher / ApplyLauncherWakeup
// (launcher.c:1096-1129).
pub fn ApplyLauncherWakeupAtCommit() {
    ON_COMMIT_LAUNCHER_WAKEUP.with(|c| c.set(true));
}

pub fn AtEOXact_ApplyLauncher(is_commit: bool) {
    if is_commit && ON_COMMIT_LAUNCHER_WAKEUP.with(|c| c.get()) {
        ApplyLauncherWakeup();
    }
    ON_COMMIT_LAUNCHER_WAKEUP.with(|c| c.set(false));
}

// LogicalRepWorkersWakeupAtCommit / AtEOXact_LogicalRepWorkers (worker.c):
// request wakeup of a subscription's workers at commit of the transaction
// that changed it (ALTER SUBSCRIPTION, RENAME, OWNER TO), so the workers
// process the change quickly. C hosts the pair in worker.c; the slot pool
// and latch plumbing live here, so it sits with its ApplyLauncher siblings.
pub fn LogicalRepWorkersWakeupAtCommit(subid: Oid) {
    ON_COMMIT_WAKEUP_WORKERS_SUBIDS.with(|l| {
        let mut subids = l.borrow_mut();
        // list_append_unique_oid.
        if !subids.contains(&subid) {
            subids.push(subid);
        }
    });
}

pub fn AtEOXact_LogicalRepWorkers(is_commit: bool) {
    // Take-and-clear on every path (C resets the list unconditionally).
    let subids = ON_COMMIT_WAKEUP_WORKERS_SUBIDS.with(|l| std::mem::take(&mut *l.borrow_mut()));
    if !is_commit || subids.is_empty() {
        return;
    }
    // One ctx pass = C's LogicalRepWorkerLock LW_SHARED span over
    // logicalrep_workers_find(subid, only_running) per queued subid; the
    // latch pokes run outside the lock (logicalrep_worker_wakeup's pattern).
    let proc_nos: Vec<ProcNumber> = with_ctx_opt(Vec::new(), |ctx| {
        ctx.workers
            .iter()
            .filter(|w| w.in_use && w.proc_pid != 0 && subids.contains(&w.subid))
            .filter_map(|w| w.proc_no)
            .collect()
    });
    for p in proc_nos {
        latch::SetLatch(LatchHandle::proc(p));
    }
}

fn ApplyLauncherWakeup() {
    // C signals SIGUSR1; the latch is what the launcher sleeps on.
    let proc_no = CTX
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
        .and_then(|ctx| if ctx.launcher_pid != 0 { ctx.launcher_proc } else { None });
    if let Some(p) = proc_no {
        latch::SetLatch(LatchHandle::proc(p));
    }
}

// ApplyLauncherGetWorkerStartTime / SetWorkerStartTime (launcher.c:1043/1059);
// the dsa/dshash collapses into the ctx HashMap.
fn ApplyLauncherGetWorkerStartTime(subid: Oid) -> TimestampTz {
    with_ctx(|ctx| ctx.last_start_times.get(&subid).copied().unwrap_or(0))
}
fn ApplyLauncherSetWorkerStartTime(subid: Oid, t: TimestampTz) {
    with_ctx(|ctx| {
        ctx.last_start_times.insert(subid, t);
    });
}
// ApplyLauncherForgetWorkerStartTime: lets a worker restart without waiting
// out wal_retrieve_retry_interval.
pub fn ApplyLauncherForgetWorkerStartTime(subid: Oid) {
    with_ctx_opt((), |ctx| {
        ctx.last_start_times.remove(&subid);
    });
}

// IsLogicalLauncher (launcher.c:1262).
pub fn IsLogicalLauncher() -> bool {
    CTX.lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
        .is_some_and(|ctx| ctx.launcher_pid == g::MyProcPid())
}

// GetLeaderApplyWorkerPid (launcher.c:1270).
pub fn GetLeaderApplyWorkerPid(pid: i32) -> i32 {
    CTX.lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
        .and_then(|ctx| {
            ctx.workers
                .iter()
                .find(|w| w.is_parallel_apply() && w.proc_pid == pid && pid != 0)
                .map(|w| w.leader_pid as i32)
        })
        .unwrap_or(-1)
}

// ApplyLauncherMain (launcher.c:1132).
pub fn ApplyLauncherMain(_main_arg: u64) -> PgResult<()> {
    use procsignal::ThreadSignalHandler::{Fallible, Simple};

    let _ = log_report(DEBUG1, "logical replication launcher started".to_string());

    with_ctx(|ctx| {
        debug_assert_eq!(ctx.launcher_pid, 0);
        ctx.launcher_pid = g::MyProcPid();
        ctx.launcher_proc = lmgr_proc::MyProc();
    });
    // logicalrep_launcher_onexit: clear launcher_pid however we leave.
    struct OnExit;
    impl Drop for OnExit {
        fn drop(&mut self) {
            let mut guard = CTX.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(ctx) = guard.as_mut() {
                ctx.launcher_pid = 0;
                ctx.launcher_proc = None;
            }
        }
    }
    let _onexit = OnExit;

    // procsignal::signums, not libc::SIG*: the wasi libc crate exposes no
    // SIG* names (thread-signal emulation numbering, signums law).
    procsignal::pqsignal_thread(
        procsignal::signums::SIGHUP,
        Simple(interrupt::SignalHandlerForConfigReload),
    );
    procsignal::pqsignal_thread(procsignal::signums::SIGTERM, Fallible(postgres::die));
    bgworker::BackgroundWorkerUnblockSignals();

    // Connection to nailed catalogs (we only ever access pg_subscription).
    bgworker::BackgroundWorkerInitializeConnection(None, None, 0)?;

    loop {
        let mut wait_time: i64 = DEFAULT_NAPTIME_PER_CYCLE;

        postgres_seams::check_for_interrupts::call()?;

        // Start any missing workers for enabled subscriptions.
        let sublist = {
            let cx = mcx::MemoryContext::new("Logical Replication Launcher sublist");
            xact::StartTransactionCommand()?;
            let list = pg_subscription::GetSubscriptionList(cx.mcx())?;
            xact::CommitTransactionCommand()?;
            list
        };
        let wal_retrieve_retry_interval =
            guc_tables::vars::wal_retrieve_retry_interval.read() as i64;

        for sub in &sublist {
            if !sub.enabled {
                continue;
            }
            if logicalrep_worker_find(sub.oid, InvalidOid, false).is_some() {
                continue; // worker is running already
            }

            let last_start = ApplyLauncherGetWorkerStartTime(sub.oid);
            let now = timestamp_seams::get_current_timestamp::call();
            let elapsed_ms = (now - last_start) / 1000;
            if last_start == 0 || elapsed_ms >= wal_retrieve_retry_interval {
                ApplyLauncherSetWorkerStartTime(sub.oid, now);
                let launched = logicalrep_worker_launch(
                    LogicalRepWorkerType::Apply,
                    sub.dbid,
                    sub.oid,
                    &sub.name,
                    sub.owner,
                    InvalidOid,
                )?;
                if !launched {
                    wait_time = wait_time.min(wal_retrieve_retry_interval);
                }
            } else {
                wait_time = wait_time.min(wal_retrieve_retry_interval - elapsed_ms);
            }
        }

        let rc = latch::WaitLatch(
            g::MyLatch(),
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            // WaitLatch takes i64, not c_long: c_long is i32 on wasm32
            // (ILP32) — identical on LP64 native.
            wait_time as i64,
            WAIT_EVENT_LOGICAL_LAUNCHER_MAIN,
        )?;
        if rc & WL_LATCH_SET != 0 {
            if let Some(l) = g::MyLatch() {
                latch::ResetLatch(l);
            }
            postgres_seams::check_for_interrupts::call()?;
        }

        if interrupt::ConfigReloadPending() {
            interrupt::SetConfigReloadPending(false);
            guc_file::ProcessConfigFile(types_guc::GucContext::PGC_SIGHUP)?;
        }
    }
}

pub fn init_seams() {
    vars::max_logical_replication_workers.install(GucVarAccessors {
        get: max_logical_replication_workers,
        set: |v| MAX_LOGICAL_REPLICATION_WORKERS.store(v, Ordering::Relaxed),
    });
    vars::max_sync_workers_per_subscription.install(GucVarAccessors {
        get: || MAX_SYNC_WORKERS_PER_SUBSCRIPTION.load(Ordering::Relaxed),
        set: |v| MAX_SYNC_WORKERS_PER_SUBSCRIPTION.store(v, Ordering::Relaxed),
    });
    vars::max_parallel_apply_workers_per_subscription.install(GucVarAccessors {
        get: || MAX_PARALLEL_APPLY_WORKERS_PER_SUBSCRIPTION.load(Ordering::Relaxed),
        set: |v| MAX_PARALLEL_APPLY_WORKERS_PER_SUBSCRIPTION.store(v, Ordering::Relaxed),
    });
    launcher_seams::apply_launcher_register::set(ApplyLauncherRegister);
    launcher_seams::apply_launcher_shmem_init::set(ApplyLauncherShmemInit);
    launcher_seams::at_eoxact_apply_launcher::set(AtEOXact_ApplyLauncher);
    logical_worker_seams::at_eoxact_logical_rep_workers::set(AtEOXact_LogicalRepWorkers);
    launcher_seams::get_leader_apply_worker_pid::set(GetLeaderApplyWorkerPid);
}
