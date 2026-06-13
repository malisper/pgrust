//! `replication/logical/launcher.c` — the logical-replication worker launcher.
//!
//! The supervisor daemon ([`ApplyLauncherMain`]) that starts an apply worker
//! for every enabled subscription, plus the worker-slot registry every logical
//! replication worker uses (launch / find / stop / wakeup / attach / detach /
//! cleanup), the shmem sizing/init, the last-start-times dshash orchestration,
//! and the `pg_stat_get_subscription` set-returning function.
//!
//! # Shared state
//!
//! The C `LogicalRepCtxStruct` control block and its flexible
//! `LogicalRepWorker[]` slot array live in shared memory: every logical-rep
//! backend reads/writes its slot and the launcher scans all of them. Per
//! AGENTS.md "Backend-global state", genuinely cross-backend state is ported as
//! an explicitly shared, synchronized type — here a process-global [`Mutex`]ed
//! [`LogicalRepCtx`] that this crate owns. C's `LWLock(LogicalRepWorkerLock)`
//! is still acquired (via the lwlock seam, offset 43) where C acquires it, for
//! cross-backend exclusion; the `Mutex` additionally gives Rust data-race
//! freedom for the single-process model.
//!
//! GUCs (`max_logical_replication_workers`, ...) and the per-backend flags
//! (`on_commit_launcher_wakeup`, `MyLogicalRepWorker` slot, the backend-local
//! dsa/dshash mappings) are `thread_local!` — per-backend state, never shared
//! statics.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

use std::cell::Cell;

use backend_utils_error::ereport;
use types_error::{DEBUG1, WARNING};

use types_core::primitive::{InvalidOid, Oid, OidIsValid, Size, TimestampTz, XLogRecPtr};
use types_core::xact::InvalidXLogRecPtr;
use types_datum::Datum;
use types_storage::storage::{
    dsa_handle as DsaHandle, dshash_table_handle as DshashTableHandle, DsaArea, DshashKeyKind,
    DshashParameters, DshashTable, DSM_HANDLE_INVALID as DSA_HANDLE_INVALID,
    INVALID_DSA_POINTER as DSHASH_HANDLE_INVALID,
};
use types_storage::storage::LWTRANCHE_LAUNCHER_HASH;
use types_error::{PgError, PgResult};
use types_guc::PGC_SIGHUP;
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_pgstat::wait_event::{
    WAIT_EVENT_BGWORKER_SHUTDOWN, WAIT_EVENT_BGWORKER_STARTUP, WAIT_EVENT_LOGICAL_LAUNCHER_MAIN,
};
use types_signal::SigHandler;
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};
use types_storage::LWLockMode;

use types_bgworker::{
    BackgroundWorker, BgWorkerStartTime, BgwHandleStatus, BGWORKER_BACKEND_DATABASE_CONNECTION,
    BGWORKER_SHMEM_ACCESS, BGW_NEVER_RESTART,
};
use types_replication_applyparallel::ParallelApplyWorkerInfo;
use types_replication_launcher::{
    LauncherLastStartTimesEntry, LogicalRepWorker, LogicalRepWorkerType, Subscription,
    DEFAULT_NAPTIME_PER_CYCLE, DSM_HANDLE_INVALID, SUBREL_STATE_UNKNOWN,
};

use backend_postmaster_bgworker_seams as bgworker;
use backend_replication_logical_applyparallelworker_seams as pa;
use backend_replication_logical_origin_seams as origin;
use backend_replication_logical_worker_seams as worker;
use backend_replication_walreceiver_seams as walrcv;
use backend_storage_ipc_latch_seams as latch;
use backend_storage_ipc_procarray_seams as procarray;
use backend_storage_ipc_seams as ipc;
use backend_storage_lmgr_lock_seams as lockmgr;
use backend_storage_lmgr_lwlock_seams as lwlock;
use backend_tcop_postgres_seams as tcop;
use backend_utils_adt_timestamp_seams as timestamp;
use backend_utils_fmgr_funcapi_seams as funcapi;
use backend_utils_init_small_seams as globals;
use backend_utils_misc_guc_file_seams as guc_file;
use backend_lib_dshash_seams as dshash;
use backend_utils_mmgr_dsa_seams as dsa;
use backend_catalog_pg_subscription_seams as subscription;

mod state;
use state::{my_logical_rep_worker_slot, with_ctx, with_workers};

#[cfg(test)]
mod tests;

// ===========================================================================
// Constants (PostgreSQL 18.3 headers).
// ===========================================================================

const MAXIMUM_ALIGNOF: usize = 8;

/// `MAXALIGN(len)` (c.h).
#[inline]
const fn MAXALIGN(len: usize) -> usize {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `sizeof(LogicalRepCtxStruct)` — the fixed shmem header: `pid_t launcher_pid`
/// (4) + `dsa_handle last_start_dsa` (4) + `dshash_table_handle last_start_dsh`
/// (8) = 16 bytes (already 8-aligned). The flexible `LogicalRepWorker[]` tail
/// contributes nothing to `sizeof`.
const SIZEOF_LOGICAL_REP_CTX_STRUCT: usize = 16;

/// `sizeof(LogicalRepWorker)` — the per-slot shmem record (worker_internal.h),
/// 8-aligned: type(4)+launch_time(8)+in_use(1)+generation(2)[+pad]+proc(8)+
/// dbid(4)+userid(4)+subid(4)+relid(4)+relstate(1)[+pad]+relstate_lsn(8)+
/// relmutex(4)[+pad]+stream_fileset(8)+leader_pid(4)+parallel_apply(1)[+pad]+
/// last_lsn(8)+last_send_time(8)+last_recv_time(8)+reply_lsn(8)+reply_time(8)
/// = 128 bytes.
const SIZEOF_LOGICAL_REP_WORKER: usize = 128;

/// `LWTRANCHE_LAUNCHER_DSA` — DSA tranche for the last-start-times area.
const LWTRANCHE_LAUNCHER_DSA: i32 = types_storage::storage::LWTRANCHE_LAUNCHER_DSA;

/// `LogicalRepWorkerLock` — individual built-in LWLock #43 (lwlocklist.h).
const LOGICAL_REP_WORKER_LOCK: usize = 43;

/// `InvalidPid` (miscadmin.h): `-1`.
const InvalidPid: i32 = -1;

/// `SIGHUP` / `SIGTERM` / `SIGUSR1` / `SIGUSR2` (platform values via libc, so
/// they match the OS where `kill(2)` is delivered).
const SIGHUP: i32 = libc::SIGHUP;
const SIGTERM: i32 = libc::SIGTERM;
const SIGUSR1: i32 = libc::SIGUSR1;
const SIGUSR2: i32 = libc::SIGUSR2;

/// `PG_STAT_GET_SUBSCRIPTION_COLS` (launcher.c).
const PG_STAT_GET_SUBSCRIPTION_COLS: usize = 10;

// ===========================================================================
// Small C macro/inline helpers.
// ===========================================================================

/// `XLogRecPtrIsInvalid(r)` (xlogdefs.h): `(r) == InvalidXLogRecPtr`.
#[inline]
fn XLogRecPtrIsInvalid(r: XLogRecPtr) -> bool {
    r == InvalidXLogRecPtr
}

/// `Min(a, b)` (c.h).
#[inline]
fn Min(a: i64, b: i64) -> i64 {
    if a < b {
        a
    } else {
        b
    }
}

/// `TIMESTAMP_NOBEGIN(j)` (timestamp.h): `j = DT_NOBEGIN` (`PG_INT64_MIN`).
const DT_NOBEGIN: TimestampTz = i64::MIN;

#[inline]
fn error_location() -> types_error::ErrorLocation {
    types_error::ErrorLocation::new(file!(), line!() as i32, "launcher")
}

// ===========================================================================
// LWLock helpers (LogicalRepWorkerLock).
// ===========================================================================

#[inline]
fn worker_lock_acquire(mode: LWLockMode) -> PgResult<()> {
    lwlock::lwlock_acquire_main::call(LOGICAL_REP_WORKER_LOCK, mode).map(|_| ())
}

#[inline]
fn worker_lock_release() -> PgResult<()> {
    lwlock::lwlock_release_main::call(LOGICAL_REP_WORKER_LOCK)
}

#[inline]
fn worker_lock_held_by_me() -> bool {
    lwlock::lwlock_held_by_me_main::call(LOGICAL_REP_WORKER_LOCK)
}

#[inline]
fn worker_lock_held_by_me_in_mode(mode: LWLockMode) -> bool {
    lwlock::lwlock_held_by_me_in_mode_main::call(LOGICAL_REP_WORKER_LOCK, mode)
}

// ===========================================================================
// GUCs (defined in launcher.c) — per-backend, thread_local.
// ===========================================================================

thread_local! {
    /// `int max_logical_replication_workers = 4;`
    static MAX_LOGICAL_REPLICATION_WORKERS: Cell<i32> = const { Cell::new(4) };
    /// `int max_sync_workers_per_subscription = 2;`
    static MAX_SYNC_WORKERS_PER_SUBSCRIPTION: Cell<i32> = const { Cell::new(2) };
    /// `int max_parallel_apply_workers_per_subscription = 2;`
    static MAX_PARALLEL_APPLY_WORKERS_PER_SUBSCRIPTION: Cell<i32> = const { Cell::new(2) };
    /// `static bool on_commit_launcher_wakeup = false;`
    static ON_COMMIT_LAUNCHER_WAKEUP: Cell<bool> = const { Cell::new(false) };
}

/// `max_logical_replication_workers` GUC.
pub fn max_logical_replication_workers() -> i32 {
    MAX_LOGICAL_REPLICATION_WORKERS.with(Cell::get)
}

/// Assign hook for `max_logical_replication_workers`.
pub fn set_max_logical_replication_workers(v: i32) {
    MAX_LOGICAL_REPLICATION_WORKERS.with(|c| c.set(v));
}

/// `max_sync_workers_per_subscription` GUC.
pub fn max_sync_workers_per_subscription() -> i32 {
    MAX_SYNC_WORKERS_PER_SUBSCRIPTION.with(Cell::get)
}

/// Assign hook for `max_sync_workers_per_subscription`.
pub fn set_max_sync_workers_per_subscription(v: i32) {
    MAX_SYNC_WORKERS_PER_SUBSCRIPTION.with(|c| c.set(v));
}

/// `max_parallel_apply_workers_per_subscription` GUC.
pub fn max_parallel_apply_workers_per_subscription() -> i32 {
    MAX_PARALLEL_APPLY_WORKERS_PER_SUBSCRIPTION.with(Cell::get)
}

/// Assign hook for `max_parallel_apply_workers_per_subscription`.
pub fn set_max_parallel_apply_workers_per_subscription(v: i32) {
    MAX_PARALLEL_APPLY_WORKERS_PER_SUBSCRIPTION.with(|c| c.set(v));
}

#[inline]
fn on_commit_launcher_wakeup_get() -> bool {
    ON_COMMIT_LAUNCHER_WAKEUP.with(Cell::get)
}

#[inline]
fn on_commit_launcher_wakeup_set(v: bool) {
    ON_COMMIT_LAUNCHER_WAKEUP.with(|c| c.set(v));
}

// ===========================================================================
// Worker-slot mutators (launcher's own logic over the shared array).
// ===========================================================================

/// `logicalrep_worker_cleanup(worker)` (launcher.c) — clean up worker info.
/// Caller holds `LogicalRepWorkerLock` in EXCLUSIVE mode.
fn logicalrep_worker_cleanup(w: &mut LogicalRepWorker) {
    debug_assert!(worker_lock_held_by_me_in_mode(LWLockMode::LW_EXCLUSIVE));

    w.wtype = LogicalRepWorkerType::Unknown;
    w.in_use = false;
    w.proc_pid = None;
    w.dbid = InvalidOid;
    w.userid = InvalidOid;
    w.subid = InvalidOid;
    w.relid = InvalidOid;
    w.leader_pid = InvalidPid;
    w.parallel_apply = false;
}

// ===========================================================================
// get_subscription_list (static) — seamed catalog scan.
// ===========================================================================

/// `get_subscription_list()` (launcher.c). The catalog transaction +
/// `pg_subscription` scan live in the heapam/tableam/xact subsystems; the
/// launcher consumes the resulting list, allocated in `mcx` (the per-cycle
/// sublist context).
fn get_subscription_list(mcx: mcx::Mcx<'_>) -> PgResult<mcx::PgVec<'_, Subscription>> {
    subscription::get_subscription_list::call(mcx)
}

// ===========================================================================
// WaitForReplicationWorkerAttach (static).
// ===========================================================================

/// `WaitForReplicationWorkerAttach(worker, generation, handle)` (launcher.c).
/// `slot` is the slot index; returns whether the attach succeeded.
fn WaitForReplicationWorkerAttach(
    slot: i32,
    generation: u16,
    handle: types_bgworker::BackgroundWorkerHandle,
) -> PgResult<bool> {
    let mut result = false;
    let mut dropped_latch = false;

    loop {
        tcop::check_for_interrupts::call()?;

        worker_lock_acquire(LWLockMode::LW_SHARED)?;

        // Worker either died or has started. Return false if died.
        let (in_use, proc_attached) =
            with_workers(|ws| (ws[slot as usize].in_use, ws[slot as usize].proc_pid.is_some()));
        if !in_use || proc_attached {
            result = in_use;
            worker_lock_release()?;
            break;
        }

        worker_lock_release()?;

        // Check if worker has died before attaching, and clean up after it.
        let (status, _pid) = bgworker::get_background_worker_pid::call(handle);

        if status == BgwHandleStatus::Stopped {
            worker_lock_acquire(LWLockMode::LW_EXCLUSIVE)?;
            // Ensure that this was indeed the worker we waited for.
            let cur_generation = with_workers(|ws| ws[slot as usize].generation);
            if generation == cur_generation {
                with_workers(|ws| logicalrep_worker_cleanup(&mut ws[slot as usize]));
            }
            worker_lock_release()?;
            break; // result is already false
        }

        // We need timeout because we generally don't get notified via latch
        // about the worker attach. But we don't expect to have to wait long.
        let rc = latch::wait_latch_my_latch::call(
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            10,
            WAIT_EVENT_BGWORKER_STARTUP,
        )?;

        if (rc & WL_LATCH_SET) != 0 {
            reset_my_latch()?;
            tcop::check_for_interrupts::call()?;
            dropped_latch = true;
        }
    }

    // If we had to clear a latch event in order to wait, be sure to restore it
    // before exiting. Otherwise caller may miss events.
    if dropped_latch {
        latch::set_latch_my_latch::call();
    }

    Ok(result)
}

/// `ResetLatch(MyLatch)` — the latch unit resolves `MyLatch`; there is no
/// `LatchHandle` to thread through this process's own loop, so reset via the
/// my-latch helper. (The launcher only ever resets its own latch.)
fn reset_my_latch() -> PgResult<()> {
    latch::reset_latch_my_latch::call();
    Ok(())
}

// ===========================================================================
// logicalrep_worker_find / logicalrep_workers_find.
// ===========================================================================

/// `logicalrep_worker_find(subid, relid, only_running)` (launcher.c). Returns
/// the matching slot index, or `None` (C NULL). Only the leader apply or table
/// sync worker is considered. Caller holds `LogicalRepWorkerLock`.
pub fn logicalrep_worker_find(subid: Oid, relid: Oid, only_running: bool) -> Option<i32> {
    debug_assert!(worker_lock_held_by_me());

    with_workers(|ws| {
        for (i, w) in ws.iter().enumerate() {
            // Skip parallel apply workers.
            if w.is_parallel_apply_worker() {
                continue;
            }
            if w.in_use
                && w.subid == subid
                && w.relid == relid
                && (!only_running || w.proc_pid.is_some())
            {
                return Some(i as i32);
            }
        }
        None
    })
}

/// `logicalrep_workers_find(subid, only_running, acquire_lock)` (launcher.c).
/// Returns the list of matching slot indices.
pub fn logicalrep_workers_find(
    subid: Oid,
    only_running: bool,
    acquire_lock: bool,
) -> PgResult<Vec<i32>> {
    let mut res: Vec<i32> = Vec::new();

    if acquire_lock {
        worker_lock_acquire(LWLockMode::LW_SHARED)?;
    }

    debug_assert!(worker_lock_held_by_me());

    with_workers(|ws| {
        for (i, w) in ws.iter().enumerate() {
            if w.in_use && w.subid == subid && (!only_running || w.proc_pid.is_some()) {
                res.push(i as i32);
            }
        }
    });

    if acquire_lock {
        worker_lock_release()?;
    }

    Ok(res)
}

// ===========================================================================
// logicalrep_worker_launch.
// ===========================================================================

/// `logicalrep_worker_launch(...)` (launcher.c). Returns true on success.
pub fn logicalrep_worker_launch(
    wtype: LogicalRepWorkerType,
    dbid: Oid,
    subid: Oid,
    subname: &str,
    userid: Oid,
    relid: Oid,
    subworker_dsm: u32,
) -> PgResult<bool> {
    let generation: u16;
    let mut worker_slot: Option<i32>;
    let mut nsyncworkers: i32;
    let nparallelapplyworkers: i32;
    let mut now: TimestampTz;
    let is_tablesync_worker = wtype == LogicalRepWorkerType::Tablesync;
    let is_parallel_apply_worker = wtype == LogicalRepWorkerType::ParallelApply;

    // Sanity checks.
    debug_assert!(wtype != LogicalRepWorkerType::Unknown);
    debug_assert!(is_tablesync_worker == OidIsValid(relid));
    debug_assert!(is_parallel_apply_worker == (subworker_dsm != DSM_HANDLE_INVALID));

    ereport(DEBUG1)
        .errmsg_internal(format!(
            "starting logical replication worker for subscription \"{subname}\""
        ))
        .finish(error_location())?;

    // Report this after the initial starting message for consistency.
    if origin::max_active_replication_origins::call() == 0 {
        return Err(PgError::error(
            "cannot start logical replication workers when \"max_active_replication_origins\" is 0",
        )
        .with_sqlstate(types_error::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED));
    }

    // We need to do the modification of the shared memory under lock so that we
    // have a consistent view.
    worker_lock_acquire(LWLockMode::LW_EXCLUSIVE)?;

    'retry: loop {
        // Find unused worker slot.
        worker_slot = with_workers(|ws| {
            for (i, w) in ws.iter().enumerate() {
                if !w.in_use {
                    return Some(i as i32);
                }
            }
            None
        });

        nsyncworkers = logicalrep_sync_worker_count(subid);

        now = timestamp::get_current_timestamp::call();

        // If we didn't find a free slot, try garbage collection: a worker that
        // failed to start (its parent crashed while waiting) may have left
        // in_use set.
        if worker_slot.is_none() || nsyncworkers >= max_sync_workers_per_subscription() {
            let mut did_cleanup = false;
            let wal_receiver_timeout = walrcv::wal_receiver_timeout::call();

            let n = max_logical_replication_workers();
            for i in 0..n {
                let (in_use, no_proc, launch_time, w_subid) = with_workers(|ws| {
                    let w = &ws[i as usize];
                    (w.in_use, w.proc_pid.is_none(), w.launch_time, w.subid)
                });

                if in_use
                    && no_proc
                    && timestamp::timestamp_difference_exceeds::call(
                        launch_time,
                        now,
                        wal_receiver_timeout,
                    )
                {
                    ereport(WARNING)
                        .errmsg_internal(format!(
                            "logical replication worker for subscription {w_subid} took too long to start; canceled"
                        ))
                        .finish(error_location())?;

                    with_workers(|ws| logicalrep_worker_cleanup(&mut ws[i as usize]));
                    did_cleanup = true;
                }
            }

            if did_cleanup {
                continue 'retry;
            }
        }

        break 'retry;
    }

    // Don't invoke more sync workers once the per-subscription sync limit is
    // reached; return silently (possible harmless race).
    if is_tablesync_worker && nsyncworkers >= max_sync_workers_per_subscription() {
        worker_lock_release()?;
        return Ok(false);
    }

    nparallelapplyworkers = logicalrep_pa_worker_count(subid);

    // Return false if the parallel-apply-worker limit per subscription is
    // reached.
    if is_parallel_apply_worker
        && nparallelapplyworkers >= max_parallel_apply_workers_per_subscription()
    {
        worker_lock_release()?;
        return Ok(false);
    }

    // No free worker slots: inform the user before exiting.
    let slot = match worker_slot {
        Some(s) => s,
        None => {
            worker_lock_release()?;
            ereport(WARNING)
                .errcode(types_error::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED)
                .errmsg("out of logical replication worker slots")
                .errhint("You might need to increase \"max_logical_replication_workers\".")
                .finish(error_location())?;
            return Ok(false);
        }
    };

    // Prepare the worker slot (worker->generation++ and every field assignment).
    let leader_pid = if is_parallel_apply_worker {
        globals::my_proc_pid::call()
    } else {
        InvalidPid
    };
    generation = with_workers(|ws| {
        let w = &mut ws[slot as usize];
        w.wtype = wtype;
        w.launch_time = now;
        w.in_use = true;
        w.generation = w.generation.wrapping_add(1);
        w.proc_pid = None;
        w.dbid = dbid;
        w.userid = userid;
        w.subid = subid;
        w.relid = relid;
        w.relstate = SUBREL_STATE_UNKNOWN;
        w.relstate_lsn = InvalidXLogRecPtr;
        w.leader_pid = leader_pid;
        w.parallel_apply = is_parallel_apply_worker;
        w.last_lsn = InvalidXLogRecPtr;
        w.last_send_time = DT_NOBEGIN;
        w.last_recv_time = DT_NOBEGIN;
        w.reply_lsn = InvalidXLogRecPtr;
        w.reply_time = DT_NOBEGIN;
        w.generation
    });

    worker_lock_release()?;

    // Register the new dynamic worker (memset(&bgw, 0, ...) == ::zeroed()).
    let mut bgw = BackgroundWorker::zeroed();
    bgw.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    bgw.bgw_start_time = BgWorkerStartTime::RecoveryFinished;
    types_bgworker::snprintf_cstr(&mut bgw.bgw_library_name, "postgres");

    match wtype {
        LogicalRepWorkerType::Apply => {
            types_bgworker::snprintf_cstr(&mut bgw.bgw_function_name, "ApplyWorkerMain");
            types_bgworker::snprintf_cstr(
                &mut bgw.bgw_name,
                &format!("logical replication apply worker for subscription {subid}"),
            );
            types_bgworker::snprintf_cstr(&mut bgw.bgw_type, "logical replication apply worker");
        }
        LogicalRepWorkerType::ParallelApply => {
            types_bgworker::snprintf_cstr(&mut bgw.bgw_function_name, "ParallelApplyWorkerMain");
            types_bgworker::snprintf_cstr(
                &mut bgw.bgw_name,
                &format!("logical replication parallel apply worker for subscription {subid}"),
            );
            types_bgworker::snprintf_cstr(
                &mut bgw.bgw_type,
                "logical replication parallel worker",
            );
            // memcpy(bgw.bgw_extra, &subworker_dsm, sizeof(dsm_handle)).
            bgw.bgw_extra[..4].copy_from_slice(&subworker_dsm.to_ne_bytes());
        }
        LogicalRepWorkerType::Tablesync => {
            types_bgworker::snprintf_cstr(&mut bgw.bgw_function_name, "TablesyncWorkerMain");
            types_bgworker::snprintf_cstr(
                &mut bgw.bgw_name,
                &format!(
                    "logical replication tablesync worker for subscription {subid} sync {relid}"
                ),
            );
            types_bgworker::snprintf_cstr(&mut bgw.bgw_type, "logical replication tablesync worker");
        }
        LogicalRepWorkerType::Unknown => {
            // Should never happen.
            return Err(PgError::error("unknown worker type"));
        }
    }

    bgw.bgw_restart_time = BGW_NEVER_RESTART;
    bgw.bgw_notify_pid = globals::my_proc_pid::call();
    bgw.bgw_main_arg = Datum::from_i32(slot); // Int32GetDatum(slot)

    let bgw_handle = match bgworker::register_dynamic_background_worker::call(&bgw)? {
        Some(h) => h,
        None => {
            // Failed to start worker, so clean up the worker slot.
            worker_lock_acquire(LWLockMode::LW_EXCLUSIVE)?;
            debug_assert!(generation == with_workers(|ws| ws[slot as usize].generation));
            with_workers(|ws| logicalrep_worker_cleanup(&mut ws[slot as usize]));
            worker_lock_release()?;

            ereport(WARNING)
                .errcode(types_error::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED)
                .errmsg("out of background worker slots")
                .errhint("You might need to increase \"max_worker_processes\".")
                .finish(error_location())?;
            return Ok(false);
        }
    };

    // Now wait until it attaches.
    WaitForReplicationWorkerAttach(slot, generation, bgw_handle)
}

// ===========================================================================
// logicalrep_worker_stop_internal (static).
// ===========================================================================

/// `logicalrep_worker_stop_internal(worker, signo)` (launcher.c).
fn logicalrep_worker_stop_internal(slot: i32, signo: i32) -> PgResult<()> {
    debug_assert!(worker_lock_held_by_me_in_mode(LWLockMode::LW_SHARED));

    // Remember the generation so we can detect a different worker taking the
    // slot.
    let generation = with_workers(|ws| ws[slot as usize].generation);

    // If we found a worker without proc set, it is still starting up; wait for
    // it to finish starting and then kill it.
    loop {
        let (in_use, proc_attached) =
            with_workers(|ws| (ws[slot as usize].in_use, ws[slot as usize].proc_pid.is_some()));
        if !(in_use && !proc_attached) {
            break;
        }

        worker_lock_release()?;

        // Wait a bit --- we don't expect to have to wait long.
        let rc = latch::wait_latch_my_latch::call(
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            10,
            WAIT_EVENT_BGWORKER_STARTUP,
        )?;

        if (rc & WL_LATCH_SET) != 0 {
            reset_my_latch()?;
            tcop::check_for_interrupts::call()?;
        }

        worker_lock_acquire(LWLockMode::LW_SHARED)?;

        // Recheck: slot freed (worker exited) or generation differs (a
        // different worker took the slot).
        let (in_use, cur_gen, proc_attached) = with_workers(|ws| {
            let w = &ws[slot as usize];
            (w.in_use, w.generation, w.proc_pid.is_some())
        });
        if !in_use || cur_gen != generation {
            return Ok(());
        }

        // Worker has assigned proc, so it has started.
        if proc_attached {
            break;
        }
    }

    // Now terminate the worker ...
    {
        let pid = with_workers(|ws| ws[slot as usize].proc_pid.unwrap_or(0));
        pg_kill(pid, signo);
    }

    // ... and wait for it to die.
    loop {
        let (proc_attached, cur_gen) =
            with_workers(|ws| (ws[slot as usize].proc_pid.is_some(), ws[slot as usize].generation));
        if !proc_attached || cur_gen != generation {
            break;
        }

        worker_lock_release()?;

        let rc = latch::wait_latch_my_latch::call(
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            10,
            WAIT_EVENT_BGWORKER_SHUTDOWN,
        )?;

        if (rc & WL_LATCH_SET) != 0 {
            reset_my_latch()?;
            tcop::check_for_interrupts::call()?;
        }

        worker_lock_acquire(LWLockMode::LW_SHARED)?;
    }

    Ok(())
}

/// `kill(pid, signo)` — deliver a signal to another backend by PID, the direct
/// libc syscall C uses (`kill((*(*worker).proc).pid, signo)` /
/// `kill(LogicalRepCtx->launcher_pid, SIGUSR1)`). SIGTERM/SIGUSR2 terminate the
/// worker; SIGUSR1 is consumed by the target's procsignal handler and sets its
/// latch.
fn pg_kill(pid: i32, signo: i32) {
    // SAFETY: a plain `kill(2)` syscall; the launcher passes only real backend
    // PIDs (its own `launcher_pid`, or a worker slot's `proc->pid`).
    unsafe {
        libc::kill(pid, signo);
    }
}

// ===========================================================================
// logicalrep_worker_stop / logicalrep_pa_worker_stop.
// ===========================================================================

/// `logicalrep_worker_stop(subid, relid)` (launcher.c).
pub fn logicalrep_worker_stop(subid: Oid, relid: Oid) -> PgResult<()> {
    worker_lock_acquire(LWLockMode::LW_SHARED)?;

    let w = logicalrep_worker_find(subid, relid, false);

    if let Some(slot) = w {
        debug_assert!(!with_workers(|ws| ws[slot as usize].is_parallel_apply_worker()));
        logicalrep_worker_stop_internal(slot, SIGTERM)?;
    }

    worker_lock_release()?;
    Ok(())
}

/// `logicalrep_pa_worker_stop(winfo)` (launcher.c). Sends SIGUSR2 (not SIGTERM)
/// so the parallel apply worker exits cleanly.
pub fn logicalrep_pa_worker_stop(winfo: &mut ParallelApplyWorkerInfo) -> PgResult<()> {
    // SpinLockAcquire(&winfo->shared->mutex); generation/slot_no;
    // SpinLockRelease(...).
    let (generation, slot_no) = pa::pa_read_winfo_slot::call(winfo)?;

    debug_assert!(slot_no >= 0 && slot_no < max_logical_replication_workers());

    // Detach from the error_mq_handle for the parallel apply worker before
    // stopping it, so the leader does not try to receive from a queue the
    // worker may already have detached.
    if pa::pa_winfo_has_error_mq::call(winfo)? {
        pa::pa_winfo_detach_error_mq::call(winfo)?;
    }

    worker_lock_acquire(LWLockMode::LW_SHARED)?;

    debug_assert!(with_workers(|ws| ws[slot_no as usize].is_parallel_apply_worker()));

    // Only stop the worker if the generation matches and it is alive.
    let (cur_gen, proc_attached) = with_workers(|ws| {
        let w = &ws[slot_no as usize];
        (w.generation, w.proc_pid.is_some())
    });
    if cur_gen == generation && proc_attached {
        logicalrep_worker_stop_internal(slot_no, SIGUSR2)?;
    }

    worker_lock_release()?;
    Ok(())
}

// ===========================================================================
// logicalrep_worker_wakeup / _ptr.
// ===========================================================================

/// `logicalrep_worker_wakeup(subid, relid)` (launcher.c).
pub fn logicalrep_worker_wakeup(subid: Oid, relid: Oid) -> PgResult<()> {
    worker_lock_acquire(LWLockMode::LW_SHARED)?;

    let w = logicalrep_worker_find(subid, relid, true);

    if let Some(slot) = w {
        logicalrep_worker_wakeup_ptr(slot)?;
    }

    worker_lock_release()?;
    Ok(())
}

/// `logicalrep_worker_wakeup_ptr(worker)` (launcher.c). Caller must hold the
/// lock, else worker->proc could change under us.
pub fn logicalrep_worker_wakeup_ptr(slot: i32) -> PgResult<()> {
    debug_assert!(worker_lock_held_by_me());

    // SetLatch(&worker->proc->procLatch).
    let pid = with_workers(|ws| ws[slot as usize].proc_pid);
    if let Some(pid) = pid {
        latch::set_latch_for_proc_pid::call(pid);
    }
    Ok(())
}

// ===========================================================================
// logicalrep_worker_attach.
// ===========================================================================

/// `logicalrep_worker_attach(slot)` (launcher.c).
pub fn logicalrep_worker_attach(slot: i32) -> PgResult<()> {
    // Block concurrent access.
    worker_lock_acquire(LWLockMode::LW_EXCLUSIVE)?;

    debug_assert!(slot >= 0 && slot < max_logical_replication_workers());

    let (in_use, proc_attached) =
        with_workers(|ws| (ws[slot as usize].in_use, ws[slot as usize].proc_pid.is_some()));

    if !in_use {
        worker_lock_release()?;
        return Err(PgError::error(format!(
            "logical replication worker slot {slot} is empty, cannot attach"
        ))
        .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE));
    }

    if proc_attached {
        worker_lock_release()?;
        return Err(PgError::error(format!(
            "logical replication worker slot {slot} is already used by another worker, cannot attach"
        ))
        .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE));
    }

    // MyLogicalRepWorker = &LogicalRepCtx->workers[slot]; worker->proc = MyProc;
    my_logical_rep_worker_slot().set(Some(slot));
    let my_pid = globals::my_proc_pid::call();
    with_workers(|ws| ws[slot as usize].proc_pid = Some(my_pid));
    ipc::before_shmem_exit::call(logicalrep_worker_onexit, Datum::from_usize(0))?;

    worker_lock_release()?;
    Ok(())
}

// ===========================================================================
// logicalrep_worker_detach (static).
// ===========================================================================

/// `logicalrep_worker_detach()` (launcher.c). Stop the parallel apply workers
/// if any, and detach the leader apply worker (cleans up the worker info).
fn logicalrep_worker_detach() -> PgResult<()> {
    // Stop the parallel apply workers.
    if worker::am_leader_apply_worker::call()? {
        // Detach from every parallel worker's error_mq_handle before
        // terminating them, so the leader doesn't log a termination message the
        // parallel worker already logged.
        pa::pa_detach_all_error_mq::call()?;

        worker_lock_acquire(LWLockMode::LW_SHARED)?;

        let subid = my_worker_subid()?;
        let workers = logicalrep_workers_find(subid, true, false)?;
        for slot in workers {
            if with_workers(|ws| ws[slot as usize].is_parallel_apply_worker()) {
                logicalrep_worker_stop_internal(slot, SIGTERM)?;
            }
        }

        worker_lock_release()?;
    }

    // Block concurrent access.
    worker_lock_acquire(LWLockMode::LW_EXCLUSIVE)?;

    let slot = my_worker_slot()?;
    with_workers(|ws| logicalrep_worker_cleanup(&mut ws[slot as usize]));

    worker_lock_release()?;
    Ok(())
}

/// The slot index of `MyLogicalRepWorker` (set at attach), or an error if no
/// slot is attached (would be a NULL-deref in C, which is a bug).
fn my_worker_slot() -> PgResult<i32> {
    my_logical_rep_worker_slot()
        .get()
        .ok_or_else(|| PgError::error("MyLogicalRepWorker is not set"))
}

/// `MyLogicalRepWorker->subid` (read from this backend's slot).
fn my_worker_subid() -> PgResult<Oid> {
    let slot = my_worker_slot()?;
    Ok(with_workers(|ws| ws[slot as usize].subid))
}

// ===========================================================================
// logicalrep_launcher_onexit / logicalrep_worker_onexit (static).
// ===========================================================================

/// `logicalrep_launcher_onexit(code, arg)` (launcher.c).
fn logicalrep_launcher_onexit(_code: i32, _arg: Datum) -> PgResult<()> {
    // LogicalRepCtx->launcher_pid = 0;
    with_ctx(|c| c.launcher_pid = 0);
    Ok(())
}

/// `logicalrep_worker_onexit(code, arg)` (launcher.c).
fn logicalrep_worker_onexit(_code: i32, _arg: Datum) -> PgResult<()> {
    // Disconnect gracefully from the remote side.
    if worker::have_walrcv_conn::call() {
        worker::walrcv_disconnect::call()?;
    }

    logicalrep_worker_detach()?;

    // Cleanup fileset used for streaming transactions.
    if worker::have_stream_fileset::call() {
        worker::fileset_delete_all::call()?;
    }

    // Session level locks may be acquired outside of a transaction in parallel
    // apply mode and will not be released when the worker terminates, so
    // manually release all locks before the worker exits. They are reacquired
    // once the worker is initialized.
    if !worker::initializing_apply_worker::call() {
        lockmgr::lock_release_all::call(types_storage::lock::DEFAULT_LOCKMETHOD, true)?;
    }

    ApplyLauncherWakeup()
}

// ===========================================================================
// logicalrep_sync_worker_count / logicalrep_pa_worker_count.
// ===========================================================================

/// `logicalrep_sync_worker_count(subid)` (launcher.c). Count the registered
/// (not necessarily running) sync workers for a subscription. Caller holds the
/// lock.
pub fn logicalrep_sync_worker_count(subid: Oid) -> i32 {
    debug_assert!(worker_lock_held_by_me());

    with_workers(|ws| {
        let mut res = 0;
        for w in ws.iter() {
            if w.is_tablesync_worker() && w.subid == subid {
                res += 1;
            }
        }
        res
    })
}

/// `logicalrep_pa_worker_count(subid)` (static, launcher.c). Count the
/// registered parallel apply workers. Caller holds the lock.
fn logicalrep_pa_worker_count(subid: Oid) -> i32 {
    debug_assert!(worker_lock_held_by_me());

    with_workers(|ws| {
        let mut res = 0;
        for w in ws.iter() {
            if w.is_parallel_apply_worker() && w.subid == subid {
                res += 1;
            }
        }
        res
    })
}

// ===========================================================================
// ApplyLauncherShmemSize / Register / ShmemInit.
// ===========================================================================

/// `ApplyLauncherShmemSize()` (launcher.c).
pub fn ApplyLauncherShmemSize() -> Size {
    // size = sizeof(LogicalRepCtxStruct); size = MAXALIGN(size);
    let mut size: Size = MAXALIGN(SIZEOF_LOGICAL_REP_CTX_STRUCT);
    // size = add_size(size, mul_size(max_logical_replication_workers,
    //                                sizeof(LogicalRepWorker)));
    size += (max_logical_replication_workers() as Size) * SIZEOF_LOGICAL_REP_WORKER;
    size
}

/// `ApplyLauncherRegister()` (launcher.c).
pub fn ApplyLauncherRegister() -> PgResult<()> {
    // Disabled during binary upgrades, to prevent logical replication workers
    // from running on the source cluster.
    if max_logical_replication_workers() == 0 || globals::is_binary_upgrade::call() {
        return Ok(());
    }

    // memset(&bgw, 0, sizeof(bgw)) — BackgroundWorker::zeroed().
    let mut bgw = BackgroundWorker::zeroed();
    bgw.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    bgw.bgw_start_time = BgWorkerStartTime::RecoveryFinished;
    types_bgworker::snprintf_cstr(&mut bgw.bgw_library_name, "postgres");
    types_bgworker::snprintf_cstr(&mut bgw.bgw_function_name, "ApplyLauncherMain");
    types_bgworker::snprintf_cstr(&mut bgw.bgw_name, "logical replication launcher");
    types_bgworker::snprintf_cstr(&mut bgw.bgw_type, "logical replication launcher");
    bgw.bgw_restart_time = 5;
    bgw.bgw_notify_pid = 0;
    bgw.bgw_main_arg = Datum::null(); // (Datum) 0

    bgworker::register_background_worker::call(&bgw)
}

/// `ApplyLauncherShmemInit()` (launcher.c).
pub fn ApplyLauncherShmemInit() -> PgResult<()> {
    // LogicalRepCtx = ShmemInitStruct(...); on !found, zero the region, set the
    // last_start handles to INVALID, and init each slot's spinlock. The owned
    // shared state lives in `state`; here we lazily create the slot array sized
    // to max_logical_replication_workers, exactly as C lays out the flexible
    // member from ApplyLauncherShmemSize().
    let n = max_logical_replication_workers() as usize;
    state::initialize(n);
    with_ctx(|c| {
        c.last_start_dsa = DSA_HANDLE_INVALID;
        c.last_start_dsh = DSHASH_HANDLE_INVALID;
    });
    Ok(())
}

// ===========================================================================
// logicalrep_launcher_attach_dshmem (static) + last-start-time accessors.
// ===========================================================================

thread_local! {
    /// `static dsa_area *last_start_times_dsa = NULL;` — backend-local mapping
    /// (the real `dsa_area *` the dsa substrate hands back, never dereferenced
    /// here; `None` == C NULL).
    static LAST_START_TIMES_DSA: Cell<Option<*mut DsaArea>> = const { Cell::new(None) };
    /// `static dshash_table *last_start_times = NULL;` — backend-local mapping
    /// (the real `dshash_table *`; `None` == C NULL).
    static LAST_START_TIMES: Cell<Option<*mut DshashTable>> = const { Cell::new(None) };
}

/// `static const dshash_parameters dsh_params` (launcher.c:78) — the
/// last-start-times table's key/entry sizes, the `dshash_memcmp`/`dshash_memhash`
/// /`dshash_memcpy` binary-key helper set, and the partition-lock tranche. Key
/// is `sizeof(Oid)`; the value is `LauncherLastStartTimesEntry`.
fn dsh_params() -> DshashParameters {
    DshashParameters {
        key_size: core::mem::size_of::<Oid>(),
        entry_size: core::mem::size_of::<LauncherLastStartTimesEntry>(),
        key_kind: DshashKeyKind::Binary,
        tranche_id: LWTRANCHE_LAUNCHER_HASH,
    }
}

/// The `const void *key` byte image for a subscription OID — `&subid` in C.
#[inline]
fn subid_key(subid: Oid) -> [u8; core::mem::size_of::<Oid>()] {
    subid.to_ne_bytes()
}

/// `logicalrep_launcher_attach_dshmem()` (launcher.c). Initialize or attach to
/// the dynamic shared hash table that stores the last-start times.
fn logicalrep_launcher_attach_dshmem() -> PgResult<()> {
    // Quick exit if we already did this.
    let dsh_handle = with_ctx(|c| c.last_start_dsh);
    if dsh_handle != DSHASH_HANDLE_INVALID && LAST_START_TIMES.with(Cell::get).is_some() {
        return Ok(());
    }

    // Otherwise, use a lock to ensure only one process creates the table.
    worker_lock_acquire(LWLockMode::LW_EXCLUSIVE)?;

    // (DSA-allocated local memory must be persistent; the seamed create/attach
    // calls run in the right context.)
    let dsh_handle = with_ctx(|c| c.last_start_dsh);
    if dsh_handle == DSHASH_HANDLE_INVALID {
        // Initialize the dynamic shared hash table and publish handles.
        let area = dsa::dsa_create::call(LWTRANCHE_LAUNCHER_DSA)?;
        dsa::dsa_pin::call(area)?;
        dsa::dsa_pin_mapping::call(area)?;
        let table = dshash::dshash_create::call(area, dsh_params())?;

        LAST_START_TIMES_DSA.with(|c| c.set(Some(area)));
        LAST_START_TIMES.with(|c| c.set(Some(table)));

        let dsa_handle = dsa::dsa_get_handle::call(area);
        let dsh = dshash::dshash_get_hash_table_handle::call(table);
        with_ctx(|c| {
            c.last_start_dsa = dsa_handle;
            c.last_start_dsh = dsh;
        });
    } else if LAST_START_TIMES.with(Cell::get).is_none() {
        // Attach to the existing dynamic shared hash table.
        let dsa_handle = with_ctx(|c| c.last_start_dsa);
        let area = dsa::dsa_attach::call(dsa_handle)?;
        dsa::dsa_pin_mapping::call(area)?;
        let table = dshash::dshash_attach::call(area, dsh_params(), dsh_handle)?;

        LAST_START_TIMES_DSA.with(|c| c.set(Some(area)));
        LAST_START_TIMES.with(|c| c.set(Some(table)));
    }

    worker_lock_release()?;
    Ok(())
}

/// The backend-local `last_start_times` table handle, valid after
/// `logicalrep_launcher_attach_dshmem()`.
fn last_start_times_table() -> PgResult<*mut DshashTable> {
    LAST_START_TIMES
        .with(Cell::get)
        .ok_or_else(|| PgError::error("last_start_times dshash table is not attached"))
}

/// `ApplyLauncherSetWorkerStartTime(subid, start_time)` (static, launcher.c).
fn ApplyLauncherSetWorkerStartTime(subid: Oid, start_time: TimestampTz) -> PgResult<()> {
    logicalrep_launcher_attach_dshmem()?;
    // entry = dshash_find_or_insert(last_start_times, &subid, &found);
    // entry->last_start_time = start_time;
    // dshash_release_lock(last_start_times, entry);
    let guard = dshash::dshash_find_or_insert::call(last_start_times_table()?, &subid_key(subid))?;
    let entry = guard.entry_ptr() as *mut LauncherLastStartTimesEntry;
    // SAFETY: `entry` points at the freshly found/inserted entry in the table's
    // DSA-shared memory, sized `sizeof(LauncherLastStartTimesEntry)`; the
    // partition lock is held by `guard`.
    unsafe {
        (*entry).last_start_time = start_time;
    }
    guard.release();
    Ok(())
}

/// `ApplyLauncherGetWorkerStartTime(subid)` (static, launcher.c). Return the
/// last-start time, or 0 if there isn't one.
fn ApplyLauncherGetWorkerStartTime(subid: Oid) -> PgResult<TimestampTz> {
    logicalrep_launcher_attach_dshmem()?;
    // entry = dshash_find(last_start_times, &subid, false);
    // if (entry == NULL) return 0;
    // ret = entry->last_start_time; dshash_release_lock(...); return ret;
    match dshash::dshash_find::call(last_start_times_table()?, &subid_key(subid), false)? {
        None => Ok(0),
        Some(guard) => {
            let entry = guard.entry_ptr() as *const LauncherLastStartTimesEntry;
            // SAFETY: present entry in DSA-shared memory; lock held by `guard`.
            let ret = unsafe { (*entry).last_start_time };
            guard.release();
            Ok(ret)
        }
    }
}

/// `ApplyLauncherForgetWorkerStartTime(subid)` (launcher.c).
pub fn ApplyLauncherForgetWorkerStartTime(subid: Oid) -> PgResult<()> {
    logicalrep_launcher_attach_dshmem()?;
    let _ = dshash::dshash_delete_key::call(last_start_times_table()?, &subid_key(subid))?;
    Ok(())
}

// ===========================================================================
// AtEOXact_ApplyLauncher / ApplyLauncherWakeupAtCommit / ApplyLauncherWakeup.
// ===========================================================================

/// `AtEOXact_ApplyLauncher(isCommit)` (launcher.c).
pub fn AtEOXact_ApplyLauncher(is_commit: bool) -> PgResult<()> {
    if is_commit && on_commit_launcher_wakeup_get() {
        ApplyLauncherWakeup()?;
    }
    on_commit_launcher_wakeup_set(false);
    Ok(())
}

/// `ApplyLauncherWakeupAtCommit()` (launcher.c).
pub fn ApplyLauncherWakeupAtCommit() {
    if !on_commit_launcher_wakeup_get() {
        on_commit_launcher_wakeup_set(true);
    }
}

/// `ApplyLauncherWakeup()` (static, launcher.c).
fn ApplyLauncherWakeup() -> PgResult<()> {
    let launcher_pid = with_ctx(|c| c.launcher_pid);
    if launcher_pid != 0 {
        pg_kill(launcher_pid, SIGUSR1);
    }
    Ok(())
}

// ===========================================================================
// ApplyLauncherMain — the supervisor main loop.
// ===========================================================================

/// `ApplyLauncherMain(main_arg)` (launcher.c). Main loop for the apply
/// launcher process.
pub fn ApplyLauncherMain(_main_arg: Datum) -> PgResult<()> {
    ereport(DEBUG1)
        .errmsg_internal("logical replication launcher started")
        .finish(error_location())?;

    ipc::before_shmem_exit::call(logicalrep_launcher_onexit, Datum::from_usize(0))?;

    debug_assert!(with_ctx(|c| c.launcher_pid) == 0);
    with_ctx(|c| c.launcher_pid = globals::my_proc_pid::call());

    // Establish signal handlers.
    interfaces_libpq_legacy_pqsignal::pqsignal(SIGHUP, SigHandler::Handler(config_reload_handler));
    interfaces_libpq_legacy_pqsignal::pqsignal(
        SIGTERM,
        SigHandler::Handler(tcop::die_signal_handler::call()),
    );
    bgworker::background_worker_unblock_signals::call();

    // Establish connection to nailed catalogs (we only access pg_subscription).
    bgworker::background_worker_initialize_connection::call(None, None, 0)?;

    // The launcher's long-lived context; the per-cycle sublist context is a
    // child, dropped at cycle end (MemoryContextDelete).
    let top = mcx::MemoryContext::new("Logical Replication Launcher");

    // Enter main loop.
    loop {
        let mut wait_time: i64 = DEFAULT_NAPTIME_PER_CYCLE;

        tcop::check_for_interrupts::call()?;

        // Use a temporary context to avoid leaking memory across cycles.
        let subctx = top.new_child("Logical Replication Launcher sublist");

        // Start any missing workers for enabled subscriptions.
        let sublist = get_subscription_list(subctx.mcx())?;
        for sub in sublist.iter() {
            if !sub.enabled {
                continue;
            }

            worker_lock_acquire(LWLockMode::LW_SHARED)?;
            let w = logicalrep_worker_find(sub.oid, InvalidOid, false);
            worker_lock_release()?;

            if w.is_some() {
                continue; // worker is running already
            }

            // If the worker is eligible to start now, launch it; otherwise
            // adjust wait_time so we wake when it can be started.
            let last_start = ApplyLauncherGetWorkerStartTime(sub.oid)?;
            let now = timestamp::get_current_timestamp::call();
            let wal_retrieve_retry_interval = walrcv::wal_retrieve_retry_interval::call();

            // C: `last_start == 0 || (elapsed = TimestampDifferenceMilliseconds(
            // last_start, now)) >= wal_retrieve_retry_interval`. The short-
            // circuit assigns `elapsed` only when last_start != 0.
            let mut elapsed: i64 = 0;
            let eligible = if last_start == 0 {
                true
            } else {
                elapsed = timestamp::timestamp_difference_milliseconds::call(last_start, now);
                elapsed >= wal_retrieve_retry_interval as i64
            };

            if eligible {
                ApplyLauncherSetWorkerStartTime(sub.oid, now)?;
                if !logicalrep_worker_launch(
                    LogicalRepWorkerType::Apply,
                    sub.dbid,
                    sub.oid,
                    &sub.name,
                    sub.owner,
                    InvalidOid,
                    DSM_HANDLE_INVALID,
                )? {
                    // Failed to launch (resource exhaustion) or launched-and-
                    // quit; retry after wal_retrieve_retry_interval.
                    wait_time = Min(wait_time, wal_retrieve_retry_interval as i64);
                }
            } else {
                wait_time = Min(wait_time, (wal_retrieve_retry_interval as i64) - elapsed);
            }
        }

        // Clean the temporary memory (MemoryContextDelete).
        drop(sublist);
        drop(subctx);

        // Wait for more work.
        let rc = latch::wait_latch_my_latch::call(
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            wait_time,
            WAIT_EVENT_LOGICAL_LAUNCHER_MAIN,
        )?;

        if (rc & WL_LATCH_SET) != 0 {
            reset_my_latch()?;
            tcop::check_for_interrupts::call()?;
        }

        if backend_postmaster_interrupt::ConfigReloadPending() {
            backend_postmaster_interrupt::SetConfigReloadPending(false);
            guc_file::process_config_file::call(PGC_SIGHUP)?;
        }
    }

    // Not reachable.
}

/// `SignalHandlerForConfigReload(SIGNAL_ARGS)` adapter — the ported interrupt
/// handler is argless; `pqsignal` installs a `fn(i32)`.
fn config_reload_handler(_signo: i32) {
    backend_postmaster_interrupt::SignalHandlerForConfigReload();
}

// ===========================================================================
// IsLogicalLauncher / GetLeaderApplyWorkerPid.
// ===========================================================================

/// `IsLogicalLauncher()` (launcher.c).
pub fn IsLogicalLauncher() -> bool {
    with_ctx(|c| c.launcher_pid) == globals::my_proc_pid::call()
}

/// `GetLeaderApplyWorkerPid(pid)` (launcher.c). The leader apply worker's PID
/// if `pid` is a parallel apply worker, else `InvalidPid`.
pub fn GetLeaderApplyWorkerPid(pid: i32) -> PgResult<i32> {
    let mut leader_pid: i32 = InvalidPid;

    worker_lock_acquire(LWLockMode::LW_SHARED)?;

    with_workers(|ws| {
        for w in ws.iter() {
            if w.is_parallel_apply_worker() && w.proc_pid == Some(pid) {
                leader_pid = w.leader_pid;
                break;
            }
        }
    });

    worker_lock_release()?;
    Ok(leader_pid)
}

// ===========================================================================
// pg_stat_get_subscription (SRF).
// ===========================================================================

/// `pg_stat_get_subscription(PG_FUNCTION_ARGS)` (launcher.c). Returns the state
/// of the subscriptions. `mcx` is the call's memory context (for the text
/// column and the materialized tuplestore the funcapi seam fills).
pub fn pg_stat_get_subscription(
    mcx: mcx::Mcx<'_>,
    fcinfo: &mut FunctionCallInfoBaseData<'_>,
) -> PgResult<Datum> {
    // Oid subid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
    let subid: Oid = funcapi::srf_arg0_oid::call(fcinfo).unwrap_or(InvalidOid);

    // InitMaterializedSRF(fcinfo, 0).
    funcapi::InitMaterializedSRF::call(fcinfo, 0)?;

    // Make sure we get a consistent view of the workers.
    worker_lock_acquire(LWLockMode::LW_SHARED)?;

    let n = max_logical_replication_workers();
    for i in 0..n {
        // memcpy(&worker, &LogicalRepCtx->workers[i], sizeof(LogicalRepWorker)).
        let w = with_workers(|ws| ws[i as usize]);

        // if (!worker.proc || !IsBackendPid(worker.proc->pid)) continue;
        let worker_pid = match w.proc_pid {
            Some(p) => p,
            None => continue,
        };
        if !procarray::is_backend_pid::call(worker_pid) {
            continue;
        }

        // if (OidIsValid(subid) && worker.subid != subid) continue;
        if OidIsValid(subid) && w.subid != subid {
            continue;
        }

        let mut values: [Datum; PG_STAT_GET_SUBSCRIPTION_COLS] =
            [Datum::from_usize(0); PG_STAT_GET_SUBSCRIPTION_COLS];
        let mut nulls: [bool; PG_STAT_GET_SUBSCRIPTION_COLS] =
            [false; PG_STAT_GET_SUBSCRIPTION_COLS];

        // Column 0: subid.
        values[0] = Datum::from_oid(w.subid);
        // Column 1: relid (only for tablesync workers, else null).
        if w.is_tablesync_worker() {
            values[1] = Datum::from_oid(w.relid);
        } else {
            nulls[1] = true;
        }
        // Column 2: worker pid (always present).
        values[2] = Datum::from_i32(worker_pid);
        // Column 3: leader_pid (only for parallel apply workers, else null).
        if w.is_parallel_apply_worker() {
            values[3] = Datum::from_i32(w.leader_pid);
        } else {
            nulls[3] = true;
        }
        // Column 4: last_lsn (null if invalid).
        if XLogRecPtrIsInvalid(w.last_lsn) {
            nulls[4] = true;
        } else {
            values[4] = Datum::from_u64(w.last_lsn);
        }
        // Column 5: last_send_time (null if 0).
        if w.last_send_time == 0 {
            nulls[5] = true;
        } else {
            values[5] = Datum::from_i64(w.last_send_time);
        }
        // Column 6: last_recv_time (null if 0).
        if w.last_recv_time == 0 {
            nulls[6] = true;
        } else {
            values[6] = Datum::from_i64(w.last_recv_time);
        }
        // Column 7: reply_lsn (null if invalid).
        if XLogRecPtrIsInvalid(w.reply_lsn) {
            nulls[7] = true;
        } else {
            values[7] = Datum::from_u64(w.reply_lsn);
        }
        // Column 8: reply_time (null if 0).
        if w.reply_time == 0 {
            nulls[8] = true;
        } else {
            values[8] = Datum::from_i64(w.reply_time);
        }
        // Column 9: worker type text.
        let worker_type = match w.wtype {
            LogicalRepWorkerType::Apply => "apply",
            LogicalRepWorkerType::ParallelApply => "parallel apply",
            LogicalRepWorkerType::Tablesync => "table synchronization",
            LogicalRepWorkerType::Unknown => {
                // Should never happen.
                worker_lock_release()?;
                return Err(PgError::error("unknown worker type"));
            }
        };
        values[9] = funcapi::cstring_get_text_datum::call(mcx, worker_type)?;

        let rsinfo = fcinfo
            .resultinfo
            .as_mut()
            .expect("InitMaterializedSRF set fcinfo->resultinfo");
        funcapi::materialized_srf_putvalues::call(rsinfo, &values, &nulls)?;

        // If only a single subscription was requested and we found it, break.
        if OidIsValid(subid) {
            break;
        }
    }

    worker_lock_release()?;

    Ok(Datum::from_usize(0)) // (Datum) 0
}

// ===========================================================================
// Seam installation.
// ===========================================================================

/// Install every seam in `backend-replication-logical-launcher-seams`.
pub fn init_seams() {
    use backend_replication_logical_launcher_seams as s;
    s::logicalrep_worker_launch::set(logicalrep_worker_launch);
    s::logicalrep_worker_stop::set(logicalrep_worker_stop);
    s::logicalrep_pa_worker_stop::set(logicalrep_pa_worker_stop);
    s::logicalrep_worker_wakeup::set(logicalrep_worker_wakeup);
    s::logicalrep_worker_wakeup_ptr::set(logicalrep_worker_wakeup_ptr);
    s::logicalrep_worker_attach::set(logicalrep_worker_attach);
    s::logicalrep_worker_find::set(logicalrep_worker_find);
    s::logicalrep_sync_worker_count::set(logicalrep_sync_worker_count);
    s::GetLeaderApplyWorkerPid::set(GetLeaderApplyWorkerPid);
    s::IsLogicalLauncher::set(IsLogicalLauncher);
    s::ApplyLauncherWakeupAtCommit::set(ApplyLauncherWakeupAtCommit);
    s::ApplyLauncherForgetWorkerStartTime::set(ApplyLauncherForgetWorkerStartTime);
    // The inward seam (consumed by xact's AtEOXact) is void per the C return;
    // `AtEOXact_ApplyLauncher`'s only fallible call (`ApplyLauncherWakeup` ->
    // `kill(2)`) is in fact infallible, so unwrap the always-`Ok` result here.
    s::at_eoxact_apply_launcher::set(|is_commit| {
        AtEOXact_ApplyLauncher(is_commit).expect("AtEOXact_ApplyLauncher is infallible")
    });
    // Pure-wiring install (assemble/seam-wiring-guard): owner body matches.
    s::apply_launcher_shmem_init::set(ApplyLauncherShmemInit);
}
