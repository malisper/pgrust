// launcher.c: the logical replication launcher and the LogicalRepWorker slot
// pool. Thread-model divergences (same style as bgworker.c's port): the
// LogicalRepCtx shmem struct + LogicalRepWorkerLock LWLock collapse into one
// std Mutex over a plain Vec (cold supervisor path); worker->proc becomes the
// worker's ProcNumber + pid; the last-start-times dsa/dshash becomes a HashMap
// inside the same Mutex (and tablesync.c's per-apply-worker HTAB of tablesync
// start times a second, (subid, relid)-keyed map beside it). C code that holds
// the LWLock across latch waits releases/reacquires per iteration — mirrored
// here. The shmem-index entry ("Logical Replication Launcher Data") is still
// registered with C's size so pg_shmem_allocations lists it.
#![allow(non_snake_case)]

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use pgsync::Mutex;

use datum::Datum;
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
use types_storage::lock::DEFAULT_LOCKMETHOD;
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};

mod funcs;
#[cfg(test)]
mod tests;

const SRC: &str = "src/backend/replication/logical/launcher.c";
#[allow(non_upper_case_globals)] // C-parity name
const InvalidPid: pid_t = -1;
#[allow(non_upper_case_globals)] // C-parity name
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
    // InitializingApplyWorker (worker.c:312): true while ApplyWorkerMain /
    // ParallelApplyWorkerMain initialize the worker; logicalrep_worker_onexit
    // skips LockReleaseAll then (the locks are only acquired once the worker
    // is initialized). Hosted here, where the exit callback reads it.
    static INITIALIZING_APPLY_WORKER: Cell<bool> = const { Cell::new(false) };
}

pub fn set_initializing_apply_worker(v: bool) {
    INITIALIZING_APPLY_WORKER.with(|c| c.set(v));
}
fn initializing_apply_worker() -> bool {
    INITIALIZING_APPLY_WORKER.with(|c| c.get())
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
    // tablesync.c's last_start_times: one private HTAB per apply worker,
    // keyed by relid (tablesync.c:425). Rendered as one map keyed by
    // (subid, relid) — an apply worker is its subscription — so a relation
    // whose OID equals a subscription OID never touches the apply throttle
    // above, and each worker's table dies with it (or when every table is
    // READY, tablesync.c:455).
    tablesync_last_start_times: HashMap<(Oid, Oid), TimestampTz>,
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

// ApplyLauncherShmemSize (launcher.c:909): MAXALIGN(sizeof(LogicalRepCtxStruct))
// + max_logical_replication_workers * sizeof(LogicalRepWorker), with the C
// struct sizes on LP64: LogicalRepCtxStruct = pid_t + dsa_handle +
// dshash_table_handle = 16 (MAXALIGNed 16; the flexible array adds nothing);
// LogicalRepWorker (worker_internal.h) = 128 (type 0, launch_time 8, in_use
// 16, generation 18, proc 24, dbid/userid/subid/relid 32..48, relstate 48,
// relstate_lsn 56, relmutex 64, stream_fileset 72, leader_pid 80,
// parallel_apply 84, last_lsn 88, last_send_time 96, last_recv_time 104,
// reply_lsn 112, reply_time 120). 528 bytes at the default 4 workers —
// C 18.6's pg_shmem_allocations row.
const C_SIZEOF_LOGICAL_REP_CTX_STRUCT: usize = 16;
const C_SIZEOF_LOGICAL_REP_WORKER: usize = 128;
pub fn ApplyLauncherShmemSize() -> PgResult<usize> {
    let size = C_SIZEOF_LOGICAL_REP_CTX_STRUCT;
    shmem::add_size(
        size,
        shmem::mul_size(
            max_logical_replication_workers().max(0) as usize,
            C_SIZEOF_LOGICAL_REP_WORKER,
        )?,
    )
}

// ApplyLauncherShmemInit (launcher.c:964). The shmem-index entry carries
// C's name and size (pg_shmem_allocations); the state itself is the CTX
// Mutex (thread model), re-created on every call — C's `found` guard only
// matters for a re-attaching EXEC_BACKEND child.
pub fn ApplyLauncherShmemInit() {
    // launcher.c:969 ShmemInitStruct(...): C's ereport(ERROR) here aborts
    // postmaster startup; the seam has no error channel, so the same abort.
    let _ = ApplyLauncherShmemSize()
        .and_then(|size| shmem::ShmemInitStruct("Logical Replication Launcher Data", size))
        .unwrap_or_else(|e| panic!("ApplyLauncherShmemInit: {}", e.message()));
    let mut guard = CTX.lock().unwrap_or_else(|e| e.into_inner());
    *guard = Some(LogicalRepCtx {
        launcher_pid: 0,
        launcher_proc: None,
        workers: (0..max_logical_replication_workers().max(0) as usize)
            .map(|_| LogicalRepWorker::empty())
            .collect(),
        last_start_times: HashMap::new(),
        tablesync_last_start_times: HashMap::new(),
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

// ParallelApplyWorkerMain dispatch (launcher.c bgw_function_name arm).
fn logicalrep_pa_worker_bgw_main(main_arg: u64) -> PgResult<()> {
    logical_worker_seams::parallel_apply_worker_main::call(main_arg)
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

// pg_stat_get_subscription's consistent view (C: memcpy of every slot under
// LogicalRepWorkerLock shared).
pub fn workers_snapshot() -> Vec<LogicalRepWorker> {
    with_ctx_opt(Vec::new(), |ctx| ctx.workers.clone())
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

// logicalrep_worker_launch (launcher.c:310). `dsm_handle` is C's
// subworker_dsm: the parallel-apply shared-state handle (0 = none), carried
// to the worker in bgw_extra.
pub fn logicalrep_worker_launch(
    wtype: LogicalRepWorkerType,
    dbid: Oid,
    subid: Oid,
    subname: &str,
    userid: Oid,
    relid: Oid,
    dsm_handle: u64,
) -> PgResult<bool> {
    use LogicalRepWorkerType::*;
    let is_tablesync = wtype == TableSync;
    let is_parallel_apply = wtype == ParallelApply;
    debug_assert!(wtype != Unknown);
    debug_assert_eq!(is_tablesync, relid != InvalidOid);
    debug_assert_eq!(is_parallel_apply, dsm_handle != 0);

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
                        && worker_attach_timed_out(w.launch_time, now, wal_receiver_timeout)
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
            // Return false once parallel apply workers reached the per-
            // subscription limit (launcher.c:421).
            let npaworkers = ctx
                .workers
                .iter()
                .filter(|w| w.in_use && w.subid == subid && w.is_parallel_apply())
                .count() as i32;
            if is_parallel_apply
                && npaworkers
                    >= MAX_PARALLEL_APPLY_WORKERS_PER_SUBSCRIPTION.load(Ordering::Relaxed)
            {
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
            w.leader_pid = if is_parallel_apply { g::MyProcPid() } else { InvalidPid };
            w.parallel_apply = is_parallel_apply;
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

    let (name, btype, bgw_main) = worker_bgw_identity(wtype, subid, relid)?;

    let mut bgw_extra = [0u8; bgworker::BGW_EXTRALEN];
    bgw_extra[..8].copy_from_slice(&dsm_handle.to_ne_bytes());

    let bgw = bgworker::BackgroundWorker {
        bgw_name: name,
        bgw_type: btype.to_string(),
        bgw_flags: bgworker::BGWORKER_SHMEM_ACCESS | bgworker::BGWORKER_BACKEND_DATABASE_CONNECTION,
        bgw_start_time: bgworker::BgWorkerStartTime::RecoveryFinished,
        bgw_restart_time: bgworker::BGW_NEVER_RESTART,
        bgw_main,
        bgw_main_arg: slot as u64,
        bgw_extra,
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

// The slot-GC threshold of logicalrep_worker_launch (launcher.c:388): a
// worker still unattached this long after its launch is cleaned up.
fn worker_attach_timed_out(
    launch_time: TimestampTz,
    now: TimestampTz,
    wal_receiver_timeout: i32,
) -> bool {
    // TimestampDifferenceExceeds (timestamp.c:1785): diff >= msec * 1000.
    adt_timestamp::TimestampDifferenceExceeds(launch_time, now, wal_receiver_timeout)
}

// The per-type bgworker identity of logicalrep_worker_launch's switch
// (launcher.c:470-508): bgw_name, bgw_type, and the main entry.
fn worker_bgw_identity(
    wtype: LogicalRepWorkerType,
    subid: Oid,
    relid: Oid,
) -> PgResult<(String, &'static str, fn(u64) -> PgResult<()>)> {
    use LogicalRepWorkerType::*;
    Ok(match wtype {
        Apply => (
            format!(
                "logical replication apply worker for subscription {}",
                subid
            ),
            "logical replication apply worker",
            logicalrep_worker_bgw_main as fn(u64) -> PgResult<()>,
        ),
        ParallelApply => (
            format!(
                "logical replication parallel apply worker for subscription {}",
                subid
            ),
            "logical replication parallel worker",
            logicalrep_pa_worker_bgw_main,
        ),
        TableSync => (
            format!(
                "logical replication tablesync worker for subscription {} sync {}",
                subid,
                relid
            ),
            "logical replication tablesync worker",
            logicalrep_worker_bgw_main,
        ),
        // launcher.c:504-506: "Should never happen", but an ERROR, not an abort.
        Unknown => {
            log_report(ERROR, "unknown worker type".to_string())?;
            unreachable!("elog(ERROR) returned");
        }
    })
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

// Tablesync start-time throttle (tablesync.c:618-630): the apply worker of
// `subid` may launch a sync worker for `relid` when its private table has no
// entry for the relation or the entry is at least wal_retrieve_retry_interval
// old; the entry is set even if the launch then fails. Keyed by the calling
// apply worker's subscription — C's table is that process's own HTAB.
pub fn tablesync_start_time_check_and_set(
    subid: Oid,
    relid: Oid,
    now: TimestampTz,
    interval_ms: i32,
) -> bool {
    with_ctx_opt(false, |ctx| {
        let due = match ctx.tablesync_last_start_times.get(&(subid, relid)) {
            Some(&last) => adt_timestamp::TimestampDifferenceExceeds(last, now, interval_ms),
            None => true,
        };
        if due {
            ctx.tablesync_last_start_times.insert((subid, relid), now);
        }
        due
    })
}

// tablesync.c:455 hash_destroy(last_start_times): the apply worker of `subid`
// drops its table once every relation is READY (a later REFRESH starts from
// an empty table); also the process-death release of a leaving apply worker.
pub fn tablesync_start_times_destroy(subid: Oid) {
    with_ctx_opt((), |ctx| {
        ctx.tablesync_last_start_times.retain(|&(s, _), _| s != subid);
    });
}

// logicalrep_worker_wakeup (launcher.c:686). Over C's zeroed shmem struct
// (no launcher: single-user mode) the search finds nothing and returns.
pub fn logicalrep_worker_wakeup(subid: Oid, relid: Oid) {
    let proc_no = with_ctx_opt(None, |ctx| {
        find_locked(ctx, subid, relid, true).and_then(|i| ctx.workers[i].proc_no)
    });
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
            // logicalrep_worker_onexit (launcher.c:744, before_shmem_exit at
            // attach): the slot must clear on EVERY exit path — a SIGTERM
            // FATAL tears the worker thread down without unwinding through
            // ApplyWorkerMain's normal-return detach, and a stuck slot makes
            // logicalrep_worker_stop (DROP SUBSCRIPTION) wait forever. The
            // before_shmem_exit stage (LIFO: after ShutdownPostgres, which
            // the later BackgroundWorkerInitializeConnection registers)
            // runs it while locks and shmem communication are still up,
            // ahead of ReplicationOriginExitCleanup / ProcKill.
            ipc::before_shmem_exit(logicalrep_worker_onexit, Datum::null())?;
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

// logicalrep_worker_onexit (launcher.c:825-850): detach, then release every
// session-level lock — parallel apply mode takes them outside any
// transaction, so nothing else releases them (launcher.c:840-848) — then
// wake the launcher. The walrcv disconnect and the stream fileset removal
// are the worker crate's own exit path.
fn logicalrep_worker_onexit(_code: i32, _arg: Datum) -> PgResult<()> {
    logicalrep_worker_detach_slot();
    if !initializing_apply_worker() {
        lock_seams::lock_release_all::call(DEFAULT_LOCKMETHOD, true)?;
    }
    ApplyLauncherWakeup();
    Ok(())
}

// logicalrep_worker_detach + onexit's launcher wakeup (launcher.c:754/825).
// Runs from the worker's normal exit path AND the exit callback; the
// MY_WORKER_SLOT take() makes it idempotent.
pub fn logicalrep_worker_detach() {
    logicalrep_worker_detach_slot();
    ApplyLauncherWakeup();
}

// logicalrep_worker_detach (launcher.c:754): stop my parallel apply workers,
// clear my slot.
fn logicalrep_worker_detach_slot() {
    if let Some(slot) = my_worker_slot() {
        // A dying leader apply worker stops its parallel apply workers first
        // (launcher.c:754): C detaches the error queues (pa_detach_all_error_mq,
        // done by the worker crate's own exit path) then SIGTERMs each one.
        let pa_slots: Vec<usize> = with_ctx(|ctx| {
            let me = &ctx.workers[slot];
            if me.wtype != LogicalRepWorkerType::Apply {
                return Vec::new();
            }
            let subid = me.subid;
            (0..ctx.workers.len())
                .filter(|&i| {
                    let w = &ctx.workers[i];
                    w.in_use && w.subid == subid && w.is_parallel_apply() && w.proc_pid != 0
                })
                .collect()
        });
        for pa in pa_slots {
            let _ = logicalrep_worker_stop_internal(pa, procsignal::signums::SIGTERM);
        }
        with_ctx(|ctx| {
            // A leaving apply worker's private tablesync start-times table
            // dies with it (tablesync.c:425 is a process static).
            if ctx.workers[slot].wtype == LogicalRepWorkerType::Apply {
                let subid = ctx.workers[slot].subid;
                ctx.tablesync_last_start_times.retain(|&(s, _), _| s != subid);
            }
            logicalrep_worker_cleanup_locked(&mut ctx.workers[slot]);
        });
        MY_WORKER_SLOT.with(|c| c.set(None));
    }
}

// logicalrep_pa_worker_stop (launcher.c:643): SIGUSR2 so the parallel apply
// worker exits cleanly; identified by (slot, generation) recorded in the
// shared state at attach.
pub fn logicalrep_pa_worker_stop(slot: usize, generation: u16) -> PgResult<()> {
    let alive = with_ctx(|ctx| {
        let w = &ctx.workers[slot];
        debug_assert!(w.is_parallel_apply() || !w.in_use || w.generation != generation);
        w.generation == generation && w.proc_pid != 0
    });
    if alive {
        logicalrep_worker_stop_internal(slot, procsignal::signums::SIGUSR2)?;
    }
    Ok(())
}

// set_stream_options' MyLogicalRepWorker->parallel_apply write (worker.c:4465).
pub fn my_worker_set_parallel_apply(v: bool) {
    if let Some(slot) = my_worker_slot() {
        with_ctx(|ctx| ctx.workers[slot].parallel_apply = v);
    }
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
            match apply_worker_restart_wait_ms(last_start, now, wal_retrieve_retry_interval) {
                None => {
                    ApplyLauncherSetWorkerStartTime(sub.oid, now);
                    let launched = logicalrep_worker_launch(
                        LogicalRepWorkerType::Apply,
                        sub.dbid,
                        sub.oid,
                        &sub.name,
                        sub.owner,
                        InvalidOid,
                        0,
                    )?;
                    if !launched {
                        wait_time = wait_time.min(wal_retrieve_retry_interval);
                    }
                }
                Some(wait) => wait_time = wait_time.min(wait),
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

// ApplyLauncherMain's restart throttle (launcher.c:1207): None = the apply
// worker may start now; Some(ms) = how long until it may.
fn apply_worker_restart_wait_ms(
    last_start: TimestampTz,
    now: TimestampTz,
    wal_retrieve_retry_interval: i64,
) -> Option<i64> {
    // TimestampDifferenceMilliseconds (timestamp.c:1761): 0 when now <=
    // last_start (a clock step back waits the full interval, never longer),
    // fractional milliseconds rounded up.
    let elapsed_ms = adt_timestamp::TimestampDifferenceMilliseconds(last_start, now);
    if last_start == 0 || elapsed_ms >= wal_retrieve_retry_interval {
        None
    } else {
        Some(wal_retrieve_retry_interval - elapsed_ms)
    }
}

pub fn init_seams() {
    funcs::register_builtins();
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
