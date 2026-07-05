#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

mod acquire;
mod fastpath;
mod locallock;
mod shared;
mod status;
mod twophase;
mod waitqueue;

#[cfg(test)]
mod tests;

pub use acquire::{
    GetLockConflicts, LockAcquire, LockAcquireExtended, LockHasWaiters, LockRelease,
    LockReassignCurrentOwner, LockReleaseAll, LockReleaseCurrentOwner, LockReleaseSession,
    LockWaiterCount,
};
pub use fastpath::{
    FastPathLockSlotsPerBackend, VirtualXactLock, VirtualXactLockTableCleanup,
    VirtualXactLockTableInsert,
};
pub use locallock::{
    AbortStrongLockAcquire, GetAwaitedLockHashcode, GrantAwaitedLock, InitLockManagerAccess,
    LockHeldByMe, MarkLockClear, ResetAwaitedLock,
};
pub use shared::{
    foreach_proclock_on_lock, GetRunningTransactionLocks, GrantLock, LockCheckConflicts,
    LockManagerShmemInit, LockManagerShmemResetAfterCrash, LockManagerShmemSize, LockTagHashCode,
};
pub use status::{BlockedProcData, BlockedProcsData, GetBlockerStatusData, GetLockStatusData};
pub use waitqueue::{
    wq_foreach, CheckDeadLock, GetLockHoldersAndWaiters, ProcLockWakeup, ProcSleep, ProcWakeup,
    RemoveFromWaitQueue, SetWaitQueueOrder,
};

use std::cell::Cell;

use types_core::ProcNumber;
use types_error::{PgError, PgResult, ERROR};
use types_storage::lock::{
    AccessExclusiveLock, AccessShareLock, ExclusiveLock, LockMethod, LockMethodData, MaxLockMode,
    RowExclusiveLock, RowShareLock, ShareLock, ShareRowExclusiveLock, ShareUpdateExclusiveLock,
    LOCKBIT_ON, LOCKMASK, LOCKMETHODID, LOCKMODE, LOCKTAG,
};

thread_local! {
    static MAX_LOCKS_PER_XACT: Cell<i32> = const { Cell::new(64) };
    static LOG_LOCK_FAILURES: Cell<bool> = const { Cell::new(false) };
}

pub fn max_locks_per_xact() -> i32 {
    MAX_LOCKS_PER_XACT.get()
}

pub fn set_max_locks_per_xact(v: i32) {
    MAX_LOCKS_PER_XACT.set(v);
}

pub fn log_lock_failures() -> bool {
    LOG_LOCK_FAILURES.get()
}

pub fn set_log_lock_failures(v: bool) {
    LOG_LOCK_FAILURES.set(v);
}

static LOCK_CONFLICTS: [LOCKMASK; MaxLockMode as usize + 1] = [
    0,
    LOCKBIT_ON(AccessExclusiveLock),
    LOCKBIT_ON(ExclusiveLock) | LOCKBIT_ON(AccessExclusiveLock),
    LOCKBIT_ON(ShareLock)
        | LOCKBIT_ON(ShareRowExclusiveLock)
        | LOCKBIT_ON(ExclusiveLock)
        | LOCKBIT_ON(AccessExclusiveLock),
    LOCKBIT_ON(ShareUpdateExclusiveLock)
        | LOCKBIT_ON(ShareLock)
        | LOCKBIT_ON(ShareRowExclusiveLock)
        | LOCKBIT_ON(ExclusiveLock)
        | LOCKBIT_ON(AccessExclusiveLock),
    LOCKBIT_ON(RowExclusiveLock)
        | LOCKBIT_ON(ShareUpdateExclusiveLock)
        | LOCKBIT_ON(ShareRowExclusiveLock)
        | LOCKBIT_ON(ExclusiveLock)
        | LOCKBIT_ON(AccessExclusiveLock),
    LOCKBIT_ON(RowExclusiveLock)
        | LOCKBIT_ON(ShareUpdateExclusiveLock)
        | LOCKBIT_ON(ShareLock)
        | LOCKBIT_ON(ShareRowExclusiveLock)
        | LOCKBIT_ON(ExclusiveLock)
        | LOCKBIT_ON(AccessExclusiveLock),
    LOCKBIT_ON(RowShareLock)
        | LOCKBIT_ON(RowExclusiveLock)
        | LOCKBIT_ON(ShareUpdateExclusiveLock)
        | LOCKBIT_ON(ShareLock)
        | LOCKBIT_ON(ShareRowExclusiveLock)
        | LOCKBIT_ON(ExclusiveLock)
        | LOCKBIT_ON(AccessExclusiveLock),
    LOCKBIT_ON(AccessShareLock)
        | LOCKBIT_ON(RowShareLock)
        | LOCKBIT_ON(RowExclusiveLock)
        | LOCKBIT_ON(ShareUpdateExclusiveLock)
        | LOCKBIT_ON(ShareLock)
        | LOCKBIT_ON(ShareRowExclusiveLock)
        | LOCKBIT_ON(ExclusiveLock)
        | LOCKBIT_ON(AccessExclusiveLock),
];

static LOCK_MODE_NAMES: [&str; 9] = [
    "INVALID",
    "AccessShareLock",
    "RowShareLock",
    "RowExclusiveLock",
    "ShareUpdateExclusiveLock",
    "ShareLock",
    "ShareRowExclusiveLock",
    "ExclusiveLock",
    "AccessExclusiveLock",
];

static DEFAULT_LOCKMETHOD_DATA: LockMethodData = LockMethodData {
    numLockModes: MaxLockMode,
    conflictTab: &LOCK_CONFLICTS,
    lockModeNames: &LOCK_MODE_NAMES,
    trace_flag: false,
};

static USER_LOCKMETHOD_DATA: LockMethodData = LockMethodData {
    numLockModes: MaxLockMode,
    conflictTab: &LOCK_CONFLICTS,
    lockModeNames: &LOCK_MODE_NAMES,
    trace_flag: false,
};

fn lock_method_by_id(lockmethodid: LOCKMETHODID) -> PgResult<LockMethod> {
    match lockmethodid {
        1 => Ok(&DEFAULT_LOCKMETHOD_DATA),
        2 => Ok(&USER_LOCKMETHOD_DATA),
        _ => Err(Box::new(PgError::new(
            ERROR,
            format!("unrecognized lock method: {lockmethodid}"),
        ))),
    }
}

fn check_lockmode(table: LockMethod, lockmode: LOCKMODE) -> PgResult<()> {
    if lockmode <= 0 || lockmode > table.numLockModes {
        return Err(Box::new(PgError::new(
            ERROR,
            format!("unrecognized lock mode: {lockmode}"),
        )));
    }
    Ok(())
}

fn lock_method_checked(lockmethodid: LOCKMETHODID, lockmode: LOCKMODE) -> PgResult<LockMethod> {
    let table = lock_method_by_id(lockmethodid)?;
    check_lockmode(table, lockmode)?;
    Ok(table)
}

pub fn GetLocksMethodTable(lock: &types_storage::lock::LOCK) -> LockMethod {
    lock_method_by_id(lock.lock_method()).expect("bad lockmethodid in shared lock table")
}

pub fn GetLockTagsMethodTable(locktag: &LOCKTAG) -> LockMethod {
    lock_method_by_id(locktag.locktag_lockmethodid as LOCKMETHODID)
        .expect("bad lockmethodid in locktag")
}

pub fn GetLockmodeName(lockmethodid: LOCKMETHODID, mode: LOCKMODE) -> &'static str {
    debug_assert!(lockmethodid == 1 || lockmethodid == 2);
    debug_assert!(mode > 0 && mode <= MaxLockMode);
    LOCK_MODE_NAMES[mode as usize]
}

pub fn DoLockModesConflict(mode1: LOCKMODE, mode2: LOCKMODE) -> bool {
    LOCK_CONFLICTS[mode1 as usize] & LOCKBIT_ON(mode2) != 0
}

fn my_procno() -> ProcNumber {
    lmgr_proc::MyProc().expect("lock manager entered without a PGPROC")
}

pub use twophase::{
    lock_twophase_postabort, lock_twophase_postcommit, lock_twophase_recover,
    lock_twophase_standby_recover, AtPrepare_Locks, PostPrepare_Locks,
};

pub fn init_seams() {
    use lock_seams as s;
    use types_storage::lock::{DEFAULT_LOCKMETHOD, USER_LOCKMETHOD};

    s::lock_acquire_extended::set(|tag, mode, session, dont_wait, report_mem, log_fail| {
        LockAcquireExtended(&tag, mode, session, dont_wait, report_mem, log_fail)
    });
    s::lock_release::set(|tag, mode, session| LockRelease(&tag, mode, session));
    s::mark_lock_clear::set(|tag, mode| MarkLockClear(&tag, mode));
    s::lock_held_by_me::set(|tag, mode, orstronger| LockHeldByMe(&tag, mode, orstronger));
    s::lock_has_waiters::set(|tag, mode| LockHasWaiters(&tag, mode, false));
    s::do_lock_modes_conflict::set(DoLockModesConflict);
    s::lock_release_all::set(|lockmethodid, all_locks| {
        let id = if lockmethodid == DEFAULT_LOCKMETHOD {
            1
        } else if lockmethodid == USER_LOCKMETHOD {
            2
        } else {
            lockmethodid as LOCKMETHODID
        };
        LockReleaseAll(id, all_locks)
    });
    s::abort_strong_lock_acquire::set(AbortStrongLockAcquire);
    s::get_awaited_lock_hashcode::set(GetAwaitedLockHashcode);
    s::grant_awaited_lock::set(GrantAwaitedLock);
    s::reset_awaited_lock::set(ResetAwaitedLock);
    s::remove_from_wait_queue::set(RemoveFromWaitQueue);
    s::lock_reassign_current_owner::set(LockReassignCurrentOwner);
    s::lock_release_current_owner::set(LockReleaseCurrentOwner);

    use guc_tables::{vars, GucVarAccessors};
    vars::max_locks_per_xact.install(GucVarAccessors {
        get: max_locks_per_xact,
        set: set_max_locks_per_xact,
    });
    vars::log_lock_failures.install(GucVarAccessors {
        get: log_lock_failures,
        set: set_log_lock_failures,
    });
}
