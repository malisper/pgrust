use std::sync::atomic::Ordering::Relaxed;

use mcx::{Mcx, PgVec};
use types_core::{ProcNumber, VirtualTransactionId, InvalidLocalTransactionId};
use types_error::{
    PgError, PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_OUT_OF_MEMORY, ERROR, LOG,
};
use types_resowner::ResourceOwner;
use types_storage::lock::{
    AccessExclusiveLock, LOCALLOCKTAG, LOCK, LOCKBIT_ON, LOCKMASK, LOCKMETHODID,
    LOCKMODE, LOCKTAG, LOCKTAG_OBJECT, LOCKTAG_RELATION,
    LOCKACQUIRE_ALREADY_CLEAR, LOCKACQUIRE_ALREADY_HELD, LOCKACQUIRE_NOT_AVAIL, LOCKACQUIRE_OK,
    LockAcquireResult, RowExclusiveLock,
};
use types_storage::storage::{
    NUM_LOCK_PARTITIONS, PROC_WAIT_STATUS_ERROR, PROC_WAIT_STATUS_OK, PROC_WAIT_STATUS_WAITING,
};

use crate::fastpath::{
    fast_path_local_can_try, fp_info_lock, strong_lock_count, ConflictsWithRelationFastPath,
    FastPathGrantRelationLock, FastPathStrongLockHashPartition, FastPathTransferRelationLocks,
    FastPathUnGrantRelationLock, eligible_for_relation_fast_path, VirtualXactLockTableCleanup,
};
use crate::locallock;
use crate::locallock::{
    assert_no_relation_extension_lock_held, grant_locallock_after_fastpath,
    prepare_or_grant_locallock, warn_not_owned, with_local, AbortStrongLockAcquire,
    BeginStrongLockAcquire, FinishStrongLockAcquire, GrantLockLocal, LocalGrant, RemoveLocalLock,
};
use crate::shared::{
    dlist_delete, dlist_is_empty, find_lock, find_proclock, foreach_proclock_on_lock,
    foreach_proclock_on_proc_partition, grant_lock_raw, lock_check_conflicts_raw, CleanUpLock,
    LockHashPartition, LockHashPartitionLock, LockHashPartitionLockByIndex, LockRefindAndRelease,
    LockTagHashCode, ProcLockHashCode, SetupLockInTable, UnGrantLock,
};
use crate::waitqueue::{GetLockHoldersAndWaiters, JoinWaitQueue, WaitOnLock};
use crate::{lock_method_by_id, lock_method_checked, my_procno};

#[cold]
pub(crate) fn out_of_shmem_error() -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, "out of shared memory")
            .with_sqlstate(ERRCODE_OUT_OF_MEMORY)
            .with_hint("You might need to increase \"max_locks_per_transaction\"."),
    )
}

// Snapshot the locallock tags into the retained scratch vector;
// RemoveLocalLock mutates the table during the sweeps.
pub(crate) fn snapshot_locallock_tags() -> PgVec<'static, LOCALLOCKTAG> {
    with_local(|state| {
        let mut scratch = std::mem::replace(&mut state.scratch, PgVec::new_in(state.mcx));
        scratch.clear();
        scratch.extend(state.table.keys().copied());
        scratch
    })
}

pub(crate) fn return_scratch(scratch: PgVec<'static, LOCALLOCKTAG>) {
    with_local(|state| state.scratch = scratch);
}

fn remove_locallock_if_unused(tag: &LOCALLOCKTAG) {
    let unused = with_local(|state| state.table.get(tag).is_some_and(|ll| ll.nLocks == 0));
    if unused {
        RemoveLocalLock(tag);
    }
}

pub fn LockAcquire(
    locktag: &LOCKTAG,
    lockmode: LOCKMODE,
    sessionLock: bool,
    dontWait: bool,
) -> PgResult<LockAcquireResult> {
    LockAcquireExtended(locktag, lockmode, sessionLock, dontWait, true, false)
}

pub fn LockAcquireExtended(
    locktag: &LOCKTAG,
    lockmode: LOCKMODE,
    sessionLock: bool,
    dontWait: bool,
    reportMemoryError: bool,
    logLockFailure: bool,
) -> PgResult<LockAcquireResult> {
    let lockmethodid = locktag.locktag_lockmethodid as LOCKMETHODID;
    let lockMethodTable = lock_method_checked(lockmethodid, lockmode)?;

    // Field tests run before the seam call (C tests RecoveryInProgress
    // first, but it is a memoized read with no ordering requirement) so the
    // hot path skips the indirect call. !InRecovery exempts the startup
    // process taking standby AELs, as C.
    if (locktag.locktag_type == LOCKTAG_OBJECT || locktag.locktag_type == LOCKTAG_RELATION)
        && lockmode > RowExclusiveLock
        && transam_xlog_seams::recovery_in_progress::call()
        && !xlogutils_seams::in_recovery::call()
    {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "cannot acquire lock mode {} on database objects while recovery is in progress",
                    lockMethodTable.lockModeNames[lockmode as usize]
                ),
            )
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_hint(
                "Only RowExclusiveLock or less can be acquired on database objects during recovery.",
            ),
        ));
    }

    let owner = if sessionLock {
        ResourceOwner::NULL
    } else {
        resowner::CurrentResourceOwner()
    };

    let localtag = LOCALLOCKTAG {
        lock: *locktag,
        mode: lockmode,
    };
    let (hashcode, ll_ptr) = match prepare_or_grant_locallock(&localtag, owner) {
        LocalGrant::Held { cleared, .. } => {
            return Ok(if cleared {
                LOCKACQUIRE_ALREADY_CLEAR
            } else {
                LOCKACQUIRE_ALREADY_HELD
            });
        }
        LocalGrant::NotHeld { hashcode, ll } => (hashcode, ll),
    };

    assert_no_relation_extension_lock_held();

    let mut log_lock = false;
    if lockmode >= AccessExclusiveLock
        && locktag.locktag_type == LOCKTAG_RELATION
        && !transam_xlog_seams::recovery_in_progress::call()
        && transam_xlog_seams::xlog_standby_info_active::call()
    {
        standby_seams::log_access_exclusive_lock_prepare::call()?;
        log_lock = true;
    }

    if fast_path_local_can_try(locktag, lockmode) {
        let fasthashcode = FastPathStrongLockHashPartition(hashcode);
        let procno = my_procno();
        let proc = lmgr_proc::GetPGProcByNumber(procno);
        // The LWLock is the sequencing point for the unlocked strong-count read.
        lwlock::LWLockAcquire(fp_info_lock(proc), lwlock::LW_EXCLUSIVE, procno)?;
        let acquired = if strong_lock_count(fasthashcode) != 0 {
            false
        } else {
            // SAFETY: fpInfoLock held exclusive.
            unsafe { FastPathGrantRelationLock(locktag.locktag_field2, lockmode) }
        };
        lwlock::LWLockRelease(fp_info_lock(proc))?;
        if acquired {
            // SAFETY: ll_ptr from prepare_or_grant_locallock above; the
            // fast-path arm makes no LOCALLOCK-table access in between.
            unsafe { grant_locallock_after_fastpath(&localtag, owner, ll_ptr) };
            return Ok(LOCKACQUIRE_OK);
        }
    }

    if ConflictsWithRelationFastPath(locktag, lockmode) {
        let fasthashcode = FastPathStrongLockHashPartition(hashcode);
        BeginStrongLockAcquire(&localtag, fasthashcode);
        if !FastPathTransferRelationLocks(lockMethodTable, locktag, hashcode)? {
            AbortStrongLockAcquire();
            remove_locallock_if_unused(&localtag);
            if reportMemoryError {
                return Err(out_of_shmem_error());
            }
            return Ok(LOCKACQUIRE_NOT_AVAIL);
        }
    }

    let partition_lock = LockHashPartitionLock(hashcode);
    lwlock::LWLockAcquire(partition_lock, lwlock::LW_EXCLUSIVE, my_procno())?;

    // SAFETY: partition lock held exclusive from here to its release.
    let proclock = unsafe {
        SetupLockInTable(lockMethodTable, my_procno(), locktag, hashcode, lockmode)?
    };
    if proclock.is_null() {
        AbortStrongLockAcquire();
        lwlock::LWLockRelease(partition_lock)?;
        remove_locallock_if_unused(&localtag);
        if reportMemoryError {
            return Err(out_of_shmem_error());
        }
        return Ok(LOCKACQUIRE_NOT_AVAIL);
    }
    let lock = unsafe { (*proclock).tag.myLock };
    with_local(|state| {
        let ll = state.table.get_mut(&localtag).expect("missing LOCALLOCK");
        ll.proclock = proclock;
        ll.lock = lock;
    });

    let found_conflict = unsafe {
        if lockMethodTable.conflictTab[lockmode as usize] & (*lock).waitMask != 0 {
            true
        } else {
            lock_check_conflicts_raw(lockMethodTable, lockmode, lock, proclock, my_procno())
        }
    };

    let mut wait_result = if !found_conflict {
        unsafe { grant_lock_raw(lock, proclock, lockmode) };
        PROC_WAIT_STATUS_OK
    } else {
        // Even for dontWait: JoinWaitQueue may grant immediately after all.
        unsafe { JoinWaitQueue(&localtag, lockMethodTable, dontWait) }
    };

    if wait_result == PROC_WAIT_STATUS_ERROR {
        // Immediate deadlock, or would have to wait with dontWait: undo the
        // shared-state changes before releasing the partition lock.
        AbortStrongLockAcquire();
        unsafe {
            if (*proclock).holdMask == 0 {
                let proclock_hashcode = ProcLockHashCode(&(*proclock).tag, hashcode);
                let partition = LockHashPartition(hashcode) as usize;
                let proc = lmgr_proc::GetPGProcByNumber(my_procno());
                dlist_delete(&raw mut (*lock).procLocks, &raw mut (*proclock).lockLink);
                dlist_delete(proc.myProcLocks[partition].ptr(), &raw mut (*proclock).procLink);
                let tag = (*proclock).tag;
                let removed = dynahash::hash_search_with_hash_value(
                    crate::shared::shared().proclock_hash,
                    &tag as *const _ as *const u8,
                    proclock_hashcode,
                    types_hash::hsearch::HASH_REMOVE,
                    None,
                )?;
                assert!(!removed.is_null(), "proclock table corrupted");
            }
            (*lock).nRequested -= 1;
            (*lock).requested[lockmode as usize] -= 1;
            debug_assert!((*lock).nRequested > 0 && (*lock).requested[lockmode as usize] >= 0);
            debug_assert!((*lock).nGranted <= (*lock).nRequested);
        }
        lwlock::LWLockRelease(partition_lock)?;
        remove_locallock_if_unused(&localtag);

        if dontWait {
            if logLockFailure {
                let modename = crate::GetLockmodeName(lockmethodid, lockmode);
                let tag_desc = lmgr_seams::describe_lock_tag::call(*locktag);
                lwlock::LWLockAcquire(partition_lock, lwlock::LW_SHARED, my_procno())?;
                let (holders, waiters, holders_num) =
                    GetLockHoldersAndWaiters(&localtag, hashcode);
                lwlock::LWLockRelease(partition_lock)?;
                let noun = if holders_num == 1 {
                    "Process holding the lock"
                } else {
                    "Processes holding the lock"
                };
                elog_seams::ereport_msg::call(
                    LOG,
                    format!(
                        "process {} could not obtain {modename} on {tag_desc}",
                        init_small::globals::MyProcPid()
                    ),
                    Some(format!("{noun}: {holders}, Wait queue: {waiters}.")),
                )?;
            }
            return Ok(LOCKACQUIRE_NOT_AVAIL);
        }
        deadlock_seams::dead_lock_report::call()?;
        unreachable!("DeadLockReport returned");
    }

    if wait_result == PROC_WAIT_STATUS_WAITING {
        debug_assert!(!dontWait);
        lwlock::LWLockRelease(partition_lock)?;

        wait_result = WaitOnLock(&localtag, hashcode, owner)?;

        // No material state change between here and return: the grantor (or
        // CheckDeadLock) fully updated the lock table on our behalf.
        if wait_result == PROC_WAIT_STATUS_ERROR {
            debug_assert!(!dontWait);
            deadlock_seams::dead_lock_report::call()?;
            unreachable!("DeadLockReport returned");
        }
    } else {
        lwlock::LWLockRelease(partition_lock)?;
    }
    debug_assert_eq!(wait_result, PROC_WAIT_STATUS_OK);

    debug_assert!(unsafe { (*proclock).holdMask & LOCKBIT_ON(lockmode) != 0 });
    GrantLockLocal(&localtag, owner);

    FinishStrongLockAcquire();

    if log_lock {
        standby_seams::log_access_exclusive_lock::call(
            locktag.locktag_field1,
            locktag.locktag_field2,
        )?;
    }

    Ok(LOCKACQUIRE_OK)
}

pub fn LockRelease(locktag: &LOCKTAG, lockmode: LOCKMODE, sessionLock: bool) -> PgResult<bool> {
    let lockmethodid = locktag.locktag_lockmethodid as LOCKMETHODID;
    let lockMethodTable = lock_method_checked(lockmethodid, lockmode)?;

    let localtag = LOCALLOCKTAG {
        lock: *locktag,
        mode: lockmode,
    };

    let owner = if sessionLock {
        ResourceOwner::NULL
    } else {
        resowner::CurrentResourceOwner()
    };

    // One probe: held check + owner bookkeeping (C's hash_search pointer).
    let (owned, forget_owner, still_held, hashcode, mut lock, proclock) =
        with_local(|state| {
            let Some(ll) = state.table.get_mut(&localtag).filter(|ll| ll.nLocks > 0) else {
                return (false, None, false, 0, std::ptr::null_mut(), std::ptr::null_mut());
            };
            let mut owned = false;
            let mut forget = None;
            for i in (0..ll.lockOwners.len()).rev() {
                if ll.lockOwners[i].owner == owner {
                    owned = true;
                    debug_assert!(ll.lockOwners[i].nLocks > 0);
                    ll.lockOwners[i].nLocks -= 1;
                    if ll.lockOwners[i].nLocks == 0 {
                        if !owner.is_null() {
                            forget = Some(owner);
                        }
                        ll.lockOwners.swap_remove(i);
                    }
                    break;
                }
            }
            if !owned {
                return (false, None, false, 0, std::ptr::null_mut(), std::ptr::null_mut());
            }
            ll.nLocks -= 1;
            let still_held = ll.nLocks > 0;
            if !still_held {
                // We may error out before deleting the entry; don't keep
                // claiming sinval clearance.
                ll.lockCleared = false;
            }
            (true, forget, still_held, ll.hashcode, ll.lock, ll.proclock)
        });
    if let Some(o) = forget_owner {
        resowner::ResourceOwnerForgetLock(o, localtag).expect("ResourceOwnerForgetLock");
    }
    if !owned {
        warn_not_owned(lockMethodTable.lockModeNames[lockmode as usize])?;
        return Ok(false);
    }
    if still_held {
        return Ok(true);
    }

    // Attempt fast release of any lock eligible for the fast path.
    if eligible_for_relation_fast_path(locktag, lockmode)
        && crate::fastpath::fast_path_group_in_use(locktag.locktag_field2)
    {
        let procno = my_procno();
        let proc = lmgr_proc::GetPGProcByNumber(procno);
        lwlock::LWLockAcquire(fp_info_lock(proc), lwlock::LW_EXCLUSIVE, procno)?;
        // SAFETY: fpInfoLock held exclusive.
        let released =
            unsafe { FastPathUnGrantRelationLock(locktag.locktag_field2, lockmode) };
        lwlock::LWLockRelease(fp_info_lock(proc))?;
        if released {
            RemoveLocalLock(&localtag);
            return Ok(true);
        }
    }

    let partition_lock = LockHashPartitionLock(hashcode);
    lwlock::LWLockAcquire(partition_lock, lwlock::LW_EXCLUSIVE, my_procno())?;

    // SAFETY: partition lock held exclusive.
    unsafe {
        // A fast-path lock moved to the main table by another backend has no
        // cached pointers; re-find them.
        let proclock = if lock.is_null() {
            debug_assert!(eligible_for_relation_fast_path(locktag, lockmode));
            lock = find_lock(locktag, hashcode)?;
            if lock.is_null() {
                lwlock::LWLockRelease(partition_lock)?;
                return Err(Box::new(PgError::new(
                    ERROR,
                    "failed to re-find shared lock object",
                )));
            }
            let pl = find_proclock(lock, my_procno(), hashcode)?;
            if pl.is_null() {
                lwlock::LWLockRelease(partition_lock)?;
                return Err(Box::new(PgError::new(
                    ERROR,
                    "failed to re-find shared proclock object",
                )));
            }
            with_local(|state| {
                let ll = state.table.get_mut(&localtag).expect("missing LOCALLOCK");
                ll.lock = lock;
                ll.proclock = pl;
            });
            pl
        } else {
            proclock
        };

        if (*proclock).holdMask & LOCKBIT_ON(lockmode) == 0 {
            lwlock::LWLockRelease(partition_lock)?;
            warn_not_owned(lockMethodTable.lockModeNames[lockmode as usize])?;
            RemoveLocalLock(&localtag);
            return Ok(false);
        }

        let wakeup_needed = UnGrantLock(lock, lockmode, proclock, lockMethodTable);
        CleanUpLock(lock, proclock, lockMethodTable, hashcode, wakeup_needed)?;
    }

    lwlock::LWLockRelease(partition_lock)?;
    RemoveLocalLock(&localtag);
    Ok(true)
}

pub fn LockReleaseAll(lockmethodid: LOCKMETHODID, allLocks: bool) -> PgResult<()> {
    let lockMethodTable = lock_method_by_id(lockmethodid)?;
    let numLockModes = lockMethodTable.numLockModes;

    // The fast-path VXID lock is only ever released here (top-level xact end).
    if lockmethodid == 1 {
        VirtualXactLockTableCleanup()?;
    }

    // ONE table pass (C's single dynahash walk): drain matching entries,
    // then run the deferred per-entry work outside the with_local borrow.
    let mut kept_forgets: Vec<(ResourceOwner, LOCALLOCKTAG)> = Vec::new();
    let mut removed = locallock::drain_release_all(lockmethodid, allLocks, &mut kept_forgets);
    for (owner, tag) in kept_forgets {
        resowner::ResourceOwnerForgetLock(owner, tag).expect("ResourceOwnerForgetLock");
    }

    let mut have_fast_path_lwlock = false;
    let my_proc = lmgr_proc::GetPGProcByNumber(my_procno());
    for r in removed.iter_mut() {
        if r.fastpath {
            let lockmode = r.tag.mode;
            assert!(
                eligible_for_relation_fast_path(&r.tag.lock, lockmode),
                "locallock table corrupted"
            );
            if !have_fast_path_lwlock {
                lwlock::LWLockAcquire(fp_info_lock(my_proc), lwlock::LW_EXCLUSIVE, my_procno())?;
                have_fast_path_lwlock = true;
            }
            let relid = r.tag.lock.locktag_field2;
            // SAFETY: fpInfoLock held exclusive.
            if unsafe { !FastPathUnGrantRelationLock(relid, lockmode) } {
                // Transferred to the main table; drop the fast-path lock
                // before the extra partition-lock cycle.
                lwlock::LWLockRelease(fp_info_lock(my_proc))?;
                have_fast_path_lwlock = false;
                LockRefindAndRelease(lockMethodTable, my_procno(), &r.tag.lock, lockmode, false)?;
            }
        }
        locallock::finish_removed_lock(r);
    }

    if have_fast_path_lwlock {
        lwlock::LWLockRelease(fp_info_lock(my_proc))?;
    }
    drop(removed);

    for partition in 0..NUM_LOCK_PARTITIONS as usize {
        let partition_lock = LockHashPartitionLockByIndex(partition);
        let proc_locks = my_proc.myProcLocks[partition].ptr();

        // Safe to skip without the partition lock: a concurrent fast-path
        // promotion can only add entries we already chose not to delete.
        // SAFETY: emptiness probe on our own list head.
        if unsafe { dlist_is_empty(&*proc_locks) } {
            continue;
        }

        lwlock::LWLockAcquire(partition_lock, lwlock::LW_EXCLUSIVE, my_procno())?;

        let mut failure: Option<Box<PgError>> = None;
        // SAFETY: partition lock held exclusive.
        unsafe {
            foreach_proclock_on_proc_partition(proc_locks, |proclock| {
                debug_assert_eq!((*proclock).tag.myProc, my_procno());
                let lock: *mut LOCK = (*proclock).tag.myLock;

                if (*lock).tag.locktag_lockmethodid as LOCKMETHODID != lockmethodid {
                    return true;
                }

                if allLocks {
                    (*proclock).releaseMask = (*proclock).holdMask;
                } else {
                    debug_assert!(((*proclock).releaseMask & !(*proclock).holdMask) == 0);
                }

                if (*proclock).releaseMask == 0 && (*proclock).holdMask != 0 {
                    return true;
                }

                debug_assert!((*lock).nRequested >= 0 && (*lock).nGranted >= 0);
                debug_assert!((*lock).nGranted <= (*lock).nRequested);
                debug_assert!(((*proclock).holdMask & !(*lock).grantMask) == 0);

                let mut wakeup_needed = false;
                for i in 1..=numLockModes {
                    if (*proclock).releaseMask & LOCKBIT_ON(i) != 0 {
                        wakeup_needed |= UnGrantLock(lock, i, proclock, lockMethodTable);
                    }
                }
                (*proclock).releaseMask = 0;

                let hashcode = LockTagHashCode(&(*lock).tag);
                if let Err(e) =
                    CleanUpLock(lock, proclock, lockMethodTable, hashcode, wakeup_needed)
                {
                    failure = Some(e);
                    return false;
                }
                true
            });
        }

        lwlock::LWLockRelease(partition_lock)?;
        if let Some(e) = failure {
            return Err(e);
        }
    }
    Ok(())
}

pub fn LockReleaseSession(lockmethodid: LOCKMETHODID) -> PgResult<()> {
    lock_method_by_id(lockmethodid)?;
    let tags = snapshot_locallock_tags();
    for tag in tags.iter() {
        if tag.lock.locktag_lockmethodid as LOCKMETHODID != lockmethodid {
            continue;
        }
        ReleaseLockIfHeld(tag, true)?;
    }
    return_scratch(tags);
    Ok(())
}

pub fn LockReleaseCurrentOwner(locallocks: Option<&[LOCALLOCKTAG]>) -> PgResult<()> {
    match locallocks {
        None => {
            let tags = snapshot_locallock_tags();
            for tag in tags.iter() {
                ReleaseLockIfHeld(tag, false)?;
            }
            return_scratch(tags);
        }
        Some(tags) => {
            for tag in tags.iter().rev() {
                ReleaseLockIfHeld(tag, false)?;
            }
        }
    }
    Ok(())
}

fn ReleaseLockIfHeld(tag: &LOCALLOCKTAG, sessionLock: bool) -> PgResult<()> {
    let owner = if sessionLock {
        ResourceOwner::NULL
    } else {
        resowner::CurrentResourceOwner()
    };

    enum Outcome {
        None,
        PartialForget(Option<ResourceOwner>),
        FullRelease,
    }
    let outcome = with_local(|state| {
        let Some(ll) = state.table.get_mut(tag) else {
            return Outcome::None;
        };
        for i in (0..ll.lockOwners.len()).rev() {
            if ll.lockOwners[i].owner == owner {
                debug_assert!(ll.lockOwners[i].nLocks > 0);
                if ll.lockOwners[i].nLocks < ll.nLocks {
                    // We will still hold this lock after forgetting this owner.
                    ll.nLocks -= ll.lockOwners[i].nLocks;
                    let forget = (!owner.is_null()).then_some(owner);
                    ll.lockOwners.swap_remove(i);
                    return Outcome::PartialForget(forget);
                }
                debug_assert_eq!(ll.lockOwners[i].nLocks, ll.nLocks);
                // Call LockRelease just once.
                ll.lockOwners[i].nLocks = 1;
                ll.nLocks = 1;
                return Outcome::FullRelease;
            }
        }
        Outcome::None
    });
    match outcome {
        Outcome::None => Ok(()),
        Outcome::PartialForget(forget) => {
            if let Some(o) = forget {
                resowner::ResourceOwnerForgetLock(o, *tag).expect("ResourceOwnerForgetLock");
            }
            Ok(())
        }
        Outcome::FullRelease => {
            if !LockRelease(&tag.lock, tag.mode, sessionLock)? {
                elog_seams::ereport_msg::call(
                    types_error::WARNING,
                    "ReleaseLockIfHeld: failed??".to_string(),
                    None,
                )?;
            }
            Ok(())
        }
    }
}

pub fn LockReassignCurrentOwner(locallocks: Option<&[LOCALLOCKTAG]>) -> PgResult<()> {
    let current = resowner::CurrentResourceOwner();
    let parent = resowner::ResourceOwnerGetParent(current);
    assert!(!parent.is_null());

    match locallocks {
        None => {
            let tags = snapshot_locallock_tags();
            for tag in tags.iter() {
                LockReassignOwner(tag, current, parent);
            }
            return_scratch(tags);
        }
        Some(tags) => {
            for tag in tags.iter().rev() {
                LockReassignOwner(tag, current, parent);
            }
        }
    }
    Ok(())
}

fn LockReassignOwner(tag: &LOCALLOCKTAG, current: ResourceOwner, parent: ResourceOwner) {
    enum Change {
        None,
        GaveSlotToParent,
        MergedIntoParent,
    }
    let change = with_local(|state| {
        let Some(ll) = state.table.get_mut(tag) else {
            return Change::None;
        };
        let mut ic = None;
        let mut ip = None;
        for i in (0..ll.lockOwners.len()).rev() {
            if ll.lockOwners[i].owner == current {
                ic = Some(i);
            } else if ll.lockOwners[i].owner == parent {
                ip = Some(i);
            }
        }
        let Some(ic) = ic else { return Change::None };
        match ip {
            None => {
                ll.lockOwners[ic].owner = parent;
                Change::GaveSlotToParent
            }
            Some(ip) => {
                ll.lockOwners[ip].nLocks += ll.lockOwners[ic].nLocks;
                ll.lockOwners.swap_remove(ic);
                Change::MergedIntoParent
            }
        }
    });
    match change {
        Change::None => {}
        Change::GaveSlotToParent => {
            resowner::ResourceOwnerRememberLock(parent, *tag);
            resowner::ResourceOwnerForgetLock(current, *tag).expect("ResourceOwnerForgetLock");
        }
        Change::MergedIntoParent => {
            resowner::ResourceOwnerForgetLock(current, *tag).expect("ResourceOwnerForgetLock");
        }
    }
}

pub fn LockHasWaiters(
    locktag: &LOCKTAG,
    lockmode: LOCKMODE,
    _sessionLock: bool,
) -> PgResult<bool> {
    let lockmethodid = locktag.locktag_lockmethodid as LOCKMETHODID;
    let lockMethodTable = lock_method_checked(lockmethodid, lockmode)?;

    let localtag = LOCALLOCKTAG {
        lock: *locktag,
        mode: lockmode,
    };
    let found = with_local(|state| {
        state
            .table
            .get(&localtag)
            .filter(|ll| ll.nLocks > 0)
            .map(|ll| (ll.hashcode, ll.lock, ll.proclock))
    });
    let Some((hashcode, lock, proclock)) = found else {
        warn_not_owned(lockMethodTable.lockModeNames[lockmode as usize])?;
        return Ok(false);
    };

    let partition_lock = LockHashPartitionLock(hashcode);
    lwlock::LWLockAcquire(partition_lock, lwlock::LW_SHARED, my_procno())?;
    // SAFETY: partition lock held; pointers cached in the locallock stay
    // valid while we hold the lock.
    let has_waiters = unsafe {
        if (*proclock).holdMask & LOCKBIT_ON(lockmode) == 0 {
            lwlock::LWLockRelease(partition_lock)?;
            warn_not_owned(lockMethodTable.lockModeNames[lockmode as usize])?;
            RemoveLocalLock(&localtag);
            return Ok(false);
        }
        lockMethodTable.conflictTab[lockmode as usize] & (*lock).waitMask != 0
    };
    lwlock::LWLockRelease(partition_lock)?;
    Ok(has_waiters)
}

pub fn GetLockConflicts<'mcx>(
    mcx: Mcx<'mcx>,
    locktag: &LOCKTAG,
    lockmode: LOCKMODE,
) -> PgResult<PgVec<'mcx, VirtualTransactionId>> {
    let lockmethodid = locktag.locktag_lockmethodid as LOCKMETHODID;
    let lockMethodTable = lock_method_checked(lockmethodid, lockmode)?;

    let mut vxids: PgVec<'mcx, VirtualTransactionId> = PgVec::new_in(mcx);
    let hashcode = LockTagHashCode(locktag);
    let partition_lock = LockHashPartitionLock(hashcode);
    let conflictMask: LOCKMASK = lockMethodTable.conflictTab[lockmode as usize];
    let hdr = lmgr_proc::ProcGlobal();

    if ConflictsWithRelationFastPath(locktag, lockmode) {
        let relid = locktag.locktag_field2;
        let group = crate::fastpath::fast_path_rel_group(relid);
        for i in 0..hdr.allProcCount as usize {
            if i as ProcNumber == my_procno() {
                continue;
            }
            let proc = &hdr.allProcs[i];
            lwlock::LWLockAcquire(fp_info_lock(proc), lwlock::LW_SHARED, my_procno())?;
            // SAFETY: fpInfoLock held.
            unsafe {
                let conflicting = crate::fastpath::fp_group_conflicts(
                    proc,
                    group,
                    relid,
                    locktag.locktag_field1,
                    conflictMask,
                );
                if conflicting {
                    let vxid = VirtualTransactionId {
                        procNumber: proc.vxid.procNumber.load(Relaxed),
                        localTransactionId: proc.vxid.lxid.load(Relaxed),
                    };
                    if vxid.localTransactionId != InvalidLocalTransactionId {
                        vxids.push(vxid);
                    }
                }
            }
            lwlock::LWLockRelease(fp_info_lock(proc))?;
        }
    }
    let fast_count = vxids.len();

    lwlock::LWLockAcquire(partition_lock, lwlock::LW_SHARED, my_procno())?;
    // SAFETY: partition lock held.
    unsafe {
        let lock = find_lock(locktag, hashcode)?;
        if !lock.is_null() {
            foreach_proclock_on_lock(lock, |proclock| {
                if conflictMask & (*proclock).holdMask != 0 {
                    let procno = (*proclock).tag.myProc;
                    if procno != my_procno() {
                        let proc = lmgr_proc::GetPGProcByNumber(procno);
                        let vxid = VirtualTransactionId {
                            procNumber: proc.vxid.procNumber.load(Relaxed),
                            localTransactionId: proc.vxid.lxid.load(Relaxed),
                        };
                        if vxid.localTransactionId != InvalidLocalTransactionId
                            && !vxids[..fast_count].contains(&vxid)
                        {
                            vxids.push(vxid);
                        }
                    }
                }
                true
            });
        }
    }
    lwlock::LWLockRelease(partition_lock)?;

    Ok(vxids)
}

pub fn LockWaiterCount(locktag: &LOCKTAG) -> PgResult<i32> {
    lock_method_by_id(locktag.locktag_lockmethodid as LOCKMETHODID)?;
    let hashcode = LockTagHashCode(locktag);
    let partition_lock = LockHashPartitionLock(hashcode);
    lwlock::LWLockAcquire(partition_lock, lwlock::LW_EXCLUSIVE, my_procno())?;
    // SAFETY: partition lock held.
    let waiters = unsafe {
        let lock = find_lock(locktag, hashcode)?;
        if lock.is_null() {
            0
        } else {
            (*lock).nRequested
        }
    };
    lwlock::LWLockRelease(partition_lock)?;
    Ok(waiters)
}
