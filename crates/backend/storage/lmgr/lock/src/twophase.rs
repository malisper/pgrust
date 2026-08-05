use std::sync::atomic::Ordering::Relaxed;

use mcx::MemoryContext;
use types_core::{ProcNumber, TransactionId, INVALID_PROC_NUMBER};
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
use types_storage::lock::{
    LOCKBIT_ON, LOCKMODE, LOCKTAG, LOCKTAG_RELATION, LOCKTAG_VIRTUALTRANSACTION, PROCLOCK,
    PROCLOCKTAG,
};
use types_storage::storage::NUM_LOCK_PARTITIONS;

use crate::acquire::{out_of_shmem_error, snapshot_locallock_tags};
use crate::fastpath::{
    ConflictsWithRelationFastPath, FastPathGetRelationLockEntry, FastPathStrongLockHashPartition,
};
use crate::locallock::{with_local, RemoveLocalLock};
use crate::my_procno;
use crate::shared::{
    dlist_delete, dlist_is_empty, dlist_push_tail, foreach_proclock_on_proc_partition, shared,
    GrantLock, LockHashPartitionLock, LockHashPartitionLockByIndex, LockRefindAndRelease,
    LockTagHashCode, SetupLockInTable,
};

pub const TWOPHASE_RM_LOCK_ID: u8 = 1;

// SAFETY contract: proclock live under the partition lock.
unsafe fn lock_ptr(proclock: *mut PROCLOCK) -> *mut types_storage::lock::LOCK {
    (*proclock).tag.myLock
}

pub const SIZEOF_TWOPHASE_LOCK_RECORD: usize = 20;

fn lock_record_bytes(locktag: &LOCKTAG, lockmode: LOCKMODE) -> [u8; SIZEOF_TWOPHASE_LOCK_RECORD] {
    let mut b = [0u8; SIZEOF_TWOPHASE_LOCK_RECORD];
    b[0..4].copy_from_slice(&locktag.locktag_field1.to_ne_bytes());
    b[4..8].copy_from_slice(&locktag.locktag_field2.to_ne_bytes());
    b[8..12].copy_from_slice(&locktag.locktag_field3.to_ne_bytes());
    b[12..14].copy_from_slice(&locktag.locktag_field4.to_ne_bytes());
    b[14] = locktag.locktag_type;
    b[15] = locktag.locktag_lockmethodid;
    b[16..20].copy_from_slice(&lockmode.to_ne_bytes());
    b
}

fn decode_lock_record(recdata: &[u8]) -> (LOCKTAG, LOCKMODE) {
    assert_eq!(recdata.len(), SIZEOF_TWOPHASE_LOCK_RECORD);
    let rd_u32 = |o: usize| u32::from_ne_bytes(recdata[o..o + 4].try_into().unwrap());
    let locktag = LOCKTAG {
        locktag_field1: rd_u32(0),
        locktag_field2: rd_u32(4),
        locktag_field3: rd_u32(8),
        locktag_field4: u16::from_ne_bytes(recdata[12..14].try_into().unwrap()),
        locktag_type: recdata[14],
        locktag_lockmethodid: recdata[15],
    };
    (locktag, rd_u32(16) as LOCKMODE)
}

#[track_caller]
#[cold]
fn session_and_xact_error() -> Box<PgError> {
    Box::new(
        PgError::new(
            ERROR,
            "cannot PREPARE while holding both session-level and transaction-level locks on the same object",
        )
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

fn check_for_session_and_xact_locks() -> PgResult<()> {
    let ws = MemoryContext::new("CheckForSessionAndXactLocks table");
    let mcx = ws.mcx();
    let mut lockhtab: mcx::PgFxHashMap<'_, LOCKTAG, (bool, bool)> =
        mcx::PgFxHashMap::with_hasher_in(Default::default(), mcx);

    with_local(|state| -> PgResult<()> {
        for (tag, ll) in state.table.iter() {
            if tag.lock.locktag_type == LOCKTAG_VIRTUALTRANSACTION {
                continue;
            }
            if ll.nLocks <= 0 {
                continue;
            }
            let entry = lockhtab.entry(tag.lock).or_insert((false, false));
            for o in ll.lockOwners.iter() {
                if o.owner.is_null() {
                    entry.0 = true;
                } else {
                    entry.1 = true;
                }
            }
            if entry.0 && entry.1 {
                return Err(session_and_xact_error());
            }
        }
        Ok(())
    })
}

pub fn AtPrepare_Locks() -> PgResult<()> {
    check_for_session_and_xact_locks()?;

    let tags = snapshot_locallock_tags();
    let mut result = Ok(());
    for tag in tags.iter() {
        if tag.lock.locktag_type == LOCKTAG_VIRTUALTRANSACTION {
            continue;
        }
        enum Action {
            Skip,
            SessionOnly,
            Both,
            Transfer { needs_proclock: bool },
        }
        let action = with_local(|state| {
            let Some(ll) = state.table.get(tag) else {
                return Action::Skip;
            };
            if ll.nLocks <= 0 {
                return Action::Skip;
            }
            let mut have_session = false;
            let mut have_xact = false;
            for o in ll.lockOwners.iter() {
                if o.owner.is_null() {
                    have_session = true;
                } else {
                    have_xact = true;
                }
            }
            if !have_xact {
                return Action::SessionOnly;
            }
            if have_session {
                return Action::Both;
            }
            Action::Transfer {
                needs_proclock: ll.proclock.is_null(),
            }
        });
        match action {
            Action::Skip | Action::SessionOnly => continue,
            Action::Both => {
                result = Err(session_and_xact_error());
                break;
            }
            Action::Transfer { needs_proclock } => {
                if needs_proclock {
                    let proclock = FastPathGetRelationLockEntry(tag)?;
                    with_local(|state| {
                        let ll = state.table.get_mut(tag).expect("missing LOCALLOCK");
                        ll.proclock = proclock;
                        // SAFETY: proclock returned live under the partition
                        // lock; its myLock tag field is immutable.
                        ll.lock = unsafe { (*proclock).tag.myLock };
                    });
                }
                with_local(|state| {
                    state
                        .table
                        .get_mut(tag)
                        .expect("missing LOCALLOCK")
                        .holdsStrongLockCount = false;
                });
                let record = lock_record_bytes(&tag.lock, tag.mode);
                if let Err(e) =
                    twophase_seams::register_two_phase_record::call(TWOPHASE_RM_LOCK_ID, 0, &record)
                {
                    result = Err(e);
                    break;
                }
            }
        }
    }
    crate::acquire::return_scratch(tags);
    result
}

pub fn PostPrepare_Locks(xid: TransactionId) -> PgResult<()> {
    let new_procno = twophase_seams::two_phase_get_dummy_proc_number::call(xid, false)?;
    let my_procno = my_procno();
    let my_proc = lmgr_proc::GetPGProcByNumber(my_procno);
    let new_proc = lmgr_proc::GetPGProcByNumber(new_procno);

    let leader = my_proc.lockGroupLeader.load(Relaxed);
    assert!(leader == INVALID_PROC_NUMBER || leader == my_procno);

    init_small::globals::StartCriticalSection();

    let tags = snapshot_locallock_tags();
    for tag in tags.iter() {
        enum Action {
            Skip,
            RemoveOnly,
            Mark(*mut PROCLOCK, bool),
        }
        let action = with_local(|state| {
            let Some(ll) = state.table.get(tag) else {
                return Action::Skip;
            };
            if ll.proclock.is_null() || ll.lock.is_null() {
                debug_assert!(ll.nLocks == 0);
                return Action::RemoveOnly;
            }
            if tag.lock.locktag_type == LOCKTAG_VIRTUALTRANSACTION {
                return Action::Skip;
            }
            let mut have_session = false;
            let mut have_xact = false;
            for o in ll.lockOwners.iter() {
                if o.owner.is_null() {
                    have_session = true;
                } else {
                    have_xact = true;
                }
            }
            if !have_xact {
                return Action::Skip;
            }
            if have_session {
                panic!("cannot PREPARE while holding both session-level and transaction-level locks on the same object");
            }
            Action::Mark(ll.proclock, ll.nLocks > 0)
        });
        match action {
            Action::Skip => continue,
            Action::RemoveOnly => RemoveLocalLock(tag),
            Action::Mark(proclock, mark) => {
                if mark {
                    // SAFETY: releaseMask is only ever touched by the owning
                    // backend (LockReleaseAll relies on the same).
                    unsafe {
                        (*proclock).releaseMask |= LOCKBIT_ON(tag.mode);
                    }
                }
                RemoveLocalLock(tag);
            }
        }
    }
    crate::acquire::return_scratch(tags);

    for partition in 0..NUM_LOCK_PARTITIONS as usize {
        let proc_locks = my_proc.myProcLocks[partition].ptr();

        // Safe to skip unlocked: AtPrepare_Locks moved every fast-path lock to
        // the main table, so no other backend is adding to our list now.
        // SAFETY: emptiness probe on our own list head.
        if unsafe { dlist_is_empty(&*proc_locks) } {
            continue;
        }

        let partition_lock = LockHashPartitionLockByIndex(partition);
        lwlock::LWLockAcquire(partition_lock, lwlock::LW_EXCLUSIVE, my_procno)
            .expect("PostPrepare_Locks: partition lock");

        // SAFETY: partition lock held exclusive; list nodes and hash entries
        // are only mutated under it.
        unsafe {
            foreach_proclock_on_proc_partition(proc_locks, |proclock| {
                debug_assert_eq!((*proclock).tag.myProc, my_procno);
                let lock = (*proclock).tag.myLock;

                if (*lock).tag.locktag_type == LOCKTAG_VIRTUALTRANSACTION {
                    return true;
                }

                debug_assert!((*lock).nRequested >= 0 && (*lock).nGranted >= 0);
                debug_assert!((*lock).nGranted <= (*lock).nRequested);
                debug_assert!(((*proclock).holdMask & !(*lock).grantMask) == 0);

                if (*proclock).releaseMask == 0 {
                    return true;
                }
                if (*proclock).releaseMask != (*proclock).holdMask {
                    panic!("we seem to have dropped a bit somewhere");
                }

                dlist_delete(proc_locks, &raw mut (*proclock).procLink);

                let new_tag = PROCLOCKTAG::new(lock, new_procno);

                debug_assert_eq!((*proclock).groupLeader, (*proclock).tag.myProc);
                (*proclock).groupLeader = new_procno;

                let updated = dynahash::hash_update_hash_key(
                    shared().proclock_hash,
                    proclock as *mut u8,
                    &new_tag as *const PROCLOCKTAG as *const u8,
                )
                .expect("PostPrepare_Locks: hash_update_hash_key");
                if !updated {
                    panic!("duplicate entry found while reassigning a prepared transaction's locks");
                }

                dlist_push_tail(
                    new_proc.myProcLocks[partition].ptr(),
                    &raw mut (*proclock).procLink,
                );
                true
            });
        }

        lwlock::LWLockRelease(partition_lock).expect("PostPrepare_Locks: partition unlock");
    }

    init_small::globals::EndCriticalSection();
    Ok(())
}

pub fn lock_twophase_recover(xid: TransactionId, _info: u16, recdata: &[u8]) -> PgResult<()> {
    let (locktag, lockmode) = decode_lock_record(recdata);
    let procno: ProcNumber = twophase_seams::two_phase_get_dummy_proc_number::call(xid, false)?;
    let lock_method_table = crate::lock_method_by_id(locktag.locktag_lockmethodid as _)?;

    let hashcode = LockTagHashCode(&locktag);
    let partition_lock = LockHashPartitionLock(hashcode);

    lwlock::LWLockAcquire(partition_lock, lwlock::LW_EXCLUSIVE, my_procno())?;

    // SAFETY: partition lock held exclusive.
    let setup = unsafe { SetupLockInTable(lock_method_table, procno, &locktag, hashcode, lockmode) };
    let proclock = match setup {
        Ok(p) if p.is_null() => {
            lwlock::LWLockRelease(partition_lock)?;
            return Err(out_of_shmem_error());
        }
        Ok(p) => p,
        Err(e) => {
            lwlock::LWLockRelease(partition_lock)?;
            return Err(e);
        }
    };

    // Conflicts are ignored: grant unconditionally, as C does, to avoid
    // deadlocks when switching from standby to normal mode.
    // SAFETY: partition lock held exclusive.
    unsafe { GrantLock(lock_ptr(proclock), proclock, lockmode) };

    if ConflictsWithRelationFastPath(&locktag, lockmode) {
        crate::fastpath::increment_strong_lock_count_partition(FastPathStrongLockHashPartition(hashcode));
    }

    lwlock::LWLockRelease(partition_lock)?;
    Ok(())
}

pub fn lock_twophase_postcommit(xid: TransactionId, _info: u16, recdata: &[u8]) -> PgResult<()> {
    let (locktag, lockmode) = decode_lock_record(recdata);
    let procno: ProcNumber = twophase_seams::two_phase_get_dummy_proc_number::call(xid, true)?;
    let lock_method_table = crate::lock_method_by_id(locktag.locktag_lockmethodid as _)?;
    LockRefindAndRelease(lock_method_table, procno, &locktag, lockmode, true)
}

pub fn lock_twophase_postabort(xid: TransactionId, info: u16, recdata: &[u8]) -> PgResult<()> {
    lock_twophase_postcommit(xid, info, recdata)
}

pub fn lock_twophase_standby_recover(xid: TransactionId, _info: u16, recdata: &[u8]) -> PgResult<()> {
    let (locktag, lockmode) = decode_lock_record(recdata);
    if lockmode == crate::AccessExclusiveLock
        && locktag.locktag_type == LOCKTAG_RELATION
    {
        standby_seams::standby_acquire_access_exclusive_lock::call(
            xid,
            locktag.locktag_field1,
            locktag.locktag_field2,
        )?;
    }
    Ok(())
}
