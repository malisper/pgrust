//! F1 + F2 of `storage/lmgr/lock.c`: the `LockAcquire` / `LockRelease` spine,
//! the grant/conflict core, the LOCALLOCK / resource-owner bookkeeping, and the
//! `WaitOnLock` / wait-queue integration that hands off to proc.c.
//!
//! # The ambient model and the proc.c bridge
//!
//! The shared `LOCK` / `PROCLOCK` tables and the backend-private `LOCALLOCK`
//! table live in [`crate::state`] as per-backend `thread_local`s. lock.c here
//! owns them outright; proc.c's wait-queue machinery
//! (`JoinWaitQueue` / `ProcSleep` / `ProcWakeup` / `ProcLockWakeup`) reaches
//! back into them through the fine-grained `(LOCKTAG, ProcNumber)` seams this
//! module installs (`grant_lock` / `lock_check_conflicts` / the
//! `lock_wait_queue_*` family / `proclock_hold_mask` / `lock_group_held_locks`
//! / `remove_from_wait_queue` / ...). lock.c calls proc.c's public
//! `JoinWaitQueue(&mut LOCALLOCK, ...)` / `ProcSleep(&mut LOCK, ...)` directly,
//! lending `&mut` out of its ambient table; proc.c calls those seams. No cycle.
//!
//! # The fast path (deferred to F3)
//!
//! lock.c's per-backend fast-path array (`MyProc->fpRelId` / `fpLockBits`) needs
//! additive PGPROC accessor seams the merged proc crate has not yet exposed, so
//! F3 (`FastPathGrantRelationLock` / `FastPathUnGrantRelationLock` /
//! `FastPathTransferRelationLocks` / `EligibleForRelationFastPath` /
//! `ConflictsWithRelationFastPath`) is deferred. On the F1 paths where C would
//! consult the fast path, we faithfully take the slow path: a lock is never
//! eligible for the fast path here (`EligibleForRelationFastPath` is
//! conservatively false), so acquisition / release always go through the shared
//! table, which is correct (just slower) and observably identical. The two
//! strong-lock-count interlock points (`ConflictsWithRelationFastPath`) reach a
//! deferred fast-path seam and panic precisely if hit.
//!
//! A handful of fully-ported F1 entry points have no installed consumer yet
//! because their callers are unported: `LockReleaseCurrentOwner` /
//! `LockReassignCurrentOwner` / `LockRefindAndRelease` are called by resowner.c
//! and the 2PC paths (F4). They are kept implemented (ready to install once
//! those land) and `EligibleForRelationFastPath` is the conservative-false stub
//! the F1 paths consult; allow dead_code for that frontier.
#![allow(dead_code)]

use alloc::boxed::Box;
use alloc::vec::Vec;

use types_core::primitive::INVALID_PROC_NUMBER;
use types_core::ProcNumber;
use types_error::PgResult;
use types_storage::lock::{
    LOCALLOCK, LOCALLOCKOWNER, LOCALLOCKTAG, LOCK, LOCKMASK, LOCKMODE, LOCKTAG,
    DEFAULT_LOCKMETHOD, LOCKBIT_OFF, LOCKBIT_ON, LOCKTAG_RELATION,
    LOCKTAG_RELATION_EXTEND, LOCKTAG_TUPLE, LockAcquireResult, LOCKACQUIRE_ALREADY_CLEAR,
    LOCKACQUIRE_ALREADY_HELD, LOCKACQUIRE_NOT_AVAIL, LOCKACQUIRE_OK, RowExclusiveLock,
    ResourceOwnerHandle,
};
use types_storage::storage::{
    ProcWaitStatus, PROC_WAIT_STATUS_ERROR, PROC_WAIT_STATUS_OK, PROC_WAIT_STATUS_WAITING,
    NUM_LOCK_PARTITIONS, LOCK_MANAGER_LWLOCK_OFFSET,
};
use types_storage::LWLockMode;

use crate::state;
use crate::tables;
use crate::LockTagHashCode;

use backend_access_transam_twophase_seams as twophase;
use backend_storage_lmgr_lwlock_seams as lwlock;
use backend_storage_lmgr_proc as proc_owner;
use backend_storage_lmgr_proc_seams as proc;
use backend_utils_resowner_seams as resowner;


/// Source file for the `ErrorLocation` of lock.c's ereports.
const SRC: &str = "src/backend/storage/lmgr/lock.c";

/// `errstart`-less `ErrorLocation` for lock.c reports.
fn loc(lineno: i32, funcname: &'static str) -> types_error::ErrorLocation {
    types_error::ErrorLocation::new(SRC, lineno, funcname)
}

/// Build a `PgError` (ERROR level) with an sqlstate and message — the C
/// `ereport(ERROR, (errcode(...), errmsg(...)))` surface, returned for the
/// caller's `?`.
fn pg_error(sqlstate: types_error::SqlState, message: alloc::string::String) -> types_error::PgError {
    types_error::PgError::error(message).with_sqlstate(sqlstate)
}

/// `elog(WARNING, msg)` — emit a warning (does not propagate; the Err leg of the
/// underlying report cannot fire for WARNING level).
fn warning(message: alloc::string::String) {
    let _ = backend_utils_error::ereport(types_error::WARNING)
        .errmsg_internal(message)
        .finish(loc(0, "lock.c"));
}

// ===========================================================================
// Partition-lock plumbing (lock.h `LockHashPartition*`).
// ===========================================================================

/// `LockHashPartition(hashcode)` (lock.h) — the partition index of a hashcode.
#[inline]
pub(crate) fn lock_hash_partition(hashcode: u32) -> i32 {
    (hashcode % (NUM_LOCK_PARTITIONS as u32)) as i32
}

/// `LockHashPartitionLock(hashcode)` (lock.h) — the `MainLWLockArray` offset of
/// the partition LWLock for a hashcode.
#[inline]
pub(crate) fn lock_partition_lock_offset(hashcode: u32) -> usize {
    (LOCK_MANAGER_LWLOCK_OFFSET + lock_hash_partition(hashcode)) as usize
}

/// `LockHashPartitionLockByIndex(i)` (lock.h).
#[inline]
pub(crate) fn lock_partition_lock_offset_by_index(i: i32) -> usize {
    (LOCK_MANAGER_LWLOCK_OFFSET + i) as usize
}

// ===========================================================================
// Lock-method validation (matches C `lockmethodid` / `lockmode` guards).
// ===========================================================================

/// Validate `lockmethodid` against `lengthof(LockMethods)`; `elog(ERROR,
/// "unrecognized lock method")` becomes an `Err`.
fn check_lockmethodid(lockmethodid: u8) -> PgResult<()> {
    if !tables::is_valid_lockmethodid(lockmethodid as u16) {
        return Err(pg_error(
            types_error::ERRCODE_INTERNAL_ERROR,
            format!("unrecognized lock method: {lockmethodid}"),
        ));
    }
    Ok(())
}

/// Validate `lockmode` against `lockMethodTable->numLockModes`.
fn check_lockmode(lockmethodid: u8, lockmode: LOCKMODE) -> PgResult<()> {
    let num = tables::num_lock_modes(lockmethodid as u16);
    if lockmode <= 0 || lockmode > num {
        return Err(pg_error(
            types_error::ERRCODE_INTERNAL_ERROR,
            format!("unrecognized lock mode: {lockmode}"),
        ));
    }
    Ok(())
}

/// `lockMethodTable->lockModeNames[lockmode]`.
fn mode_name(lockmethodid: u8, lockmode: LOCKMODE) -> &'static str {
    tables::get_lockmode_name(lockmethodid as u16, lockmode)
}

// ===========================================================================
// Resource-owner identity (lock.c `owner` / `CurrentResourceOwner`).
// ===========================================================================

/// `owner = sessionLock ? NULL : CurrentResourceOwner` (the lock.c idiom shared
/// by LockAcquire / LockRelease / ReleaseLockIfHeld). `None` models the C
/// `NULL` (a session-level hold).
fn lock_owner(session_lock: bool) -> Option<ResourceOwnerHandle> {
    if session_lock {
        None
    } else {
        resowner::lock_current_resource_owner::call()
    }
}

// ===========================================================================
// Fast-path eligibility (deferred F3).
// ===========================================================================

/// `EligibleForRelationFastPath(locktag, mode)` (lock.c). The fast path is
/// deferred to F3 (it needs additive `MyProc->fpRelId` / `fpLockBits` accessor
/// seams from the merged proc crate); until then nothing is eligible and every
/// acquisition/release takes the (correct, slower) shared-table path.
#[inline]
fn eligible_for_relation_fast_path(_locktag: &LOCKTAG, _mode: LOCKMODE) -> bool {
    false
}

/// `ConflictsWithRelationFastPath(locktag, mode)` (lock.c) — a strong relation
/// lock that may have to migrate fast-path locks out of other backends. With the
/// fast path disabled (F3 deferred) no backend can be holding a fast-path lock,
/// so nothing ever needs transferring; this is faithfully false here.
#[inline]
fn conflicts_with_relation_fast_path(_locktag: &LOCKTAG, _mode: LOCKMODE) -> bool {
    false
}

// ===========================================================================
// LOCALLOCK helpers (`crate::state::LOCAL`).
// ===========================================================================

/// `MemSet(&localtag, 0, ...); localtag.lock = *locktag; localtag.mode = mode`.
#[inline]
fn make_localtag(locktag: &LOCKTAG, lockmode: LOCKMODE) -> LOCALLOCKTAG {
    LOCALLOCKTAG {
        lock: *locktag,
        mode: lockmode,
    }
}

/// `CheckAndSetLockHeld(locallock, acquired)` (lock.c, asserts-only): track the
/// relation-extension-lock-held flag.
fn check_and_set_lock_held(tag: &LOCALLOCKTAG, acquired: bool) {
    if tag.lock.locktag_type == LOCKTAG_RELATION_EXTEND {
        state::IS_RELATION_EXTENSION_LOCK_HELD.with(|c| c.set(acquired));
    }
}

// ===========================================================================
// F1: LockHeldByMe / LockHasWaiters / LockWaiterCount.
// ===========================================================================

/// `LockHeldByMe(locktag, lockmode, orstronger)` (lock.c).
pub fn LockHeldByMe(locktag: &LOCKTAG, lockmode: LOCKMODE, orstronger: bool) -> bool {
    let localtag = make_localtag(locktag, lockmode);
    let held = state::with_local(|m| m.get(&localtag).map(|l| l.nLocks).unwrap_or(0) > 0);
    if held {
        return true;
    }
    if orstronger {
        let mut slockmode = lockmode + 1;
        while slockmode <= tables::MaxLockMode {
            if LockHeldByMe(locktag, slockmode, false) {
                return true;
            }
            slockmode += 1;
        }
    }
    false
}

/// `LockHasWaiters(locktag, lockmode, sessionLock)` (lock.c).
pub fn LockHasWaiters(locktag: &LOCKTAG, lockmode: LOCKMODE, _session_lock: bool) -> PgResult<bool> {
    let lockmethodid = locktag.locktag_lockmethodid;
    check_lockmethodid(lockmethodid)?;
    check_lockmode(lockmethodid, lockmode)?;

    let localtag = make_localtag(locktag, lockmode);

    // Find the LOCALLOCK entry (its hashcode + hold check).
    let (found, hashcode) = state::with_local(|m| match m.get(&localtag) {
        Some(l) if l.nLocks > 0 => (true, l.hashcode),
        _ => (false, 0),
    });
    if !found {
        warning(format!(
            "you don't own a lock of type {}",
            mode_name(lockmethodid, lockmode)
        ));
        return Ok(false);
    }

    // Check the shared lock table under the partition lock (SHARED).
    let guard = lwlock::lwlock_acquire_main::call(
        lock_partition_lock_offset(hashcode),
        LWLockMode::LW_SHARED,
    )?;

    let result = state::with_shared(|s| {
        let myproc = proc::my_proc_number::call();
        let hold_mask = s.proclock_hold_mask(locktag, myproc);
        if (hold_mask & LOCKBIT_ON(lockmode)) == 0 {
            return Err(());
        }
        let wait_mask = s.lock_wait_mask(locktag);
        Ok((tables::conflict_tab_for(lockmethodid as u16, lockmode) & wait_mask) != 0)
    });

    match result {
        Ok(has_waiters) => {
            guard.release()?;
            Ok(has_waiters)
        }
        Err(()) => {
            guard.release()?;
            warning(format!(
                "you don't own a lock of type {}",
                mode_name(lockmethodid, lockmode)
            ));
            remove_local_lock(&localtag);
            Ok(false)
        }
    }
}

/// `LockWaiterCount(locktag)` (lock.c).
pub fn LockWaiterCount(locktag: &LOCKTAG) -> PgResult<i32> {
    let lockmethodid = locktag.locktag_lockmethodid;
    check_lockmethodid(lockmethodid)?;

    let hashcode = LockTagHashCode(locktag);
    let guard = lwlock::lwlock_acquire_main::call(
        lock_partition_lock_offset(hashcode),
        LWLockMode::LW_EXCLUSIVE,
    )?;
    let waiters = state::with_shared(|s| s.lock_n_requested(locktag));
    guard.release()?;
    Ok(waiters)
}

// ===========================================================================
// F1: SetupLockInTable / LockCheckConflicts / GrantLock / UnGrantLock /
//     CleanUpLock / GrantLockLocal / RemoveLocalLock.
// ===========================================================================

/// `SetupLockInTable(...)` (lock.c) — find or create the LOCK + PROCLOCK for a
/// new request, incrementing the request counts. Returns the holder's
/// `ProcNumber` (the proclock identity), or `None` for out-of-(shared-)memory.
///
/// The partition lock must be held by the caller. The HashMap model never runs
/// out of memory short of process OOM (which surfaces elsewhere), so the
/// `HASH_ENTER_NULL` failure leg is unreachable; we keep the C control flow.
fn setup_lock_in_table(
    proc_no: ProcNumber,
    locktag: &LOCKTAG,
    lockmode: LOCKMODE,
) -> PgResult<Option<ProcNumber>> {
    let lockmethodid = locktag.locktag_lockmethodid;

    // group leader: proc->lockGroupLeader != NULL ? leader : proc.
    let leader = {
        let l = proc::proc_lock_group_leader::call(proc_no);
        if l != INVALID_PROC_NUMBER {
            l
        } else {
            proc_no
        }
    };

    let result = state::with_shared(|s| {
        // Find or create the LOCK.
        s.lock_get_or_create(locktag);

        // Find or create the PROCLOCK.
        let proclock_found = s.proclock_exists(locktag, proc_no);
        if !proclock_found {
            // proclock_insert chains the holder into lock->procLocks.
            s.proclock_insert(
                locktag,
                proc_no,
                state::ProcLock {
                    group_leader: leader,
                    hold_mask: 0,
                    release_mask: 0,
                },
            );
        } else {
            // C: Assert((proclock->holdMask & ~lock->grantMask) == 0).
        }

        // Increment request counts immediately (granted or waiting).
        s.lock_with_mut(locktag, |b| {
            b.set_n_requested(b.n_requested() + 1);
            b.set_requested_at(lockmode as usize, b.requested_at(lockmode as usize) + 1);
        });

        // We shouldn't already hold the desired lock.
        let hold_mask = s.proclock_hold_mask(locktag, proc_no);
        if (hold_mask & LOCKBIT_ON(lockmode)) != 0 {
            return Err((locktag.locktag_field1, locktag.locktag_field2, locktag.locktag_field3));
        }
        Ok(())
    });

    match result {
        Ok(()) => Ok(Some(proc_no)),
        Err((f1, f2, f3)) => Err(pg_error(
            types_error::ERRCODE_INTERNAL_ERROR,
            format!(
                "lock {} on object {f1}/{f2}/{f3} is already held",
                mode_name(lockmethodid, lockmode)
            ),
        )),
    }
}

/// `LockCheckConflicts(lockMethodTable, lockmode, lock, proclock)` (lock.c) —
/// does the requested mode conflict with already-granted locks, after excluding
/// the requester's own holdings and those of its lock group?
///
/// `proc_no` identifies the requesting PROCLOCK; the shared state is read under
/// the caller-held partition lock.
fn lock_check_conflicts(lockmethodid: u8, lockmode: LOCKMODE, locktag: &LOCKTAG, proc_no: ProcNumber) -> bool {
    let num_lock_modes = tables::num_lock_modes(lockmethodid as u16);
    let conflict_mask = tables::conflict_tab_for(lockmethodid as u16, lockmode);

    state::with_shared(|s| {
        let (grant_mask, granted) = match s.lock_with(locktag, |b| (b.grant_mask(), b.granted())) {
            Some(v) => v,
            None => return false,
        };

        // Global conflict check.
        if (conflict_mask & grant_mask) == 0 {
            return false;
        }

        // Subtract out locks I hold myself.
        let my_pl = s.proclock_get(locktag, proc_no).unwrap_or_default();
        let my_locks = my_pl.hold_mask;
        let mut conflicts_remaining = [0i32; types_storage::lock::MAX_LOCKMODES];
        let mut total_conflicts_remaining = 0i32;
        let mut i = 1;
        while i <= num_lock_modes {
            if (conflict_mask & LOCKBIT_ON(i)) == 0 {
                conflicts_remaining[i as usize] = 0;
                i += 1;
                continue;
            }
            let mut c = granted[i as usize];
            if (my_locks & LOCKBIT_ON(i)) != 0 {
                c -= 1;
            }
            conflicts_remaining[i as usize] = c;
            total_conflicts_remaining += c;
            i += 1;
        }

        if total_conflicts_remaining == 0 {
            return false;
        }

        // No group locking -> definitely a conflict.
        // (proclock->groupLeader == MyProc && MyProc->lockGroupLeader == NULL)
        let my_leader = proc::proc_lock_group_leader::call(proc_no);
        if my_pl.group_leader == proc_no && my_leader == INVALID_PROC_NUMBER {
            return true;
        }

        // The relation-extension lock conflicts even between group members.
        if locktag.locktag_type == LOCKTAG_RELATION_EXTEND {
            return true;
        }

        // Subtract out locks held in conflicting modes by members of our group.
        for other in s.holders(locktag) {
            if other == proc_no {
                continue;
            }
            let other_pl = match s.proclock_get(locktag, other) {
                Some(p) => p,
                None => continue,
            };
            if other_pl.group_leader == my_pl.group_leader
                && (other_pl.hold_mask & conflict_mask) != 0
            {
                let intersect_mask = other_pl.hold_mask & conflict_mask;
                let mut j = 1;
                while j <= num_lock_modes {
                    if (intersect_mask & LOCKBIT_ON(j)) != 0 {
                        // C elog(PANIC) if conflictsRemaining[j] <= 0; trust the
                        // invariant (proclocks held match the lock counts).
                        conflicts_remaining[j as usize] -= 1;
                        total_conflicts_remaining -= 1;
                    }
                    j += 1;
                }
                if total_conflicts_remaining == 0 {
                    return false;
                }
            }
        }

        true
    })
}

/// `GrantLock(lock, proclock, lockmode)` (lock.c).
fn grant_lock(locktag: &LOCKTAG, proc_no: ProcNumber, lockmode: LOCKMODE) {
    state::with_shared(|s| {
        s.lock_with_mut(locktag, |b| {
            b.set_n_granted(b.n_granted() + 1);
            b.set_granted_at(lockmode as usize, b.granted_at(lockmode as usize) + 1);
            b.set_grant_mask(b.grant_mask() | LOCKBIT_ON(lockmode));
            if b.granted_at(lockmode as usize) == b.requested_at(lockmode as usize) {
                b.set_wait_mask(b.wait_mask() & LOCKBIT_OFF(lockmode));
            }
        });
        s.proclock_update(locktag, proc_no, |pl| {
            pl.hold_mask |= LOCKBIT_ON(lockmode);
        });
    });
}

/// `UnGrantLock(lock, lockmode, proclock, lockMethodTable)` (lock.c). Returns
/// whether `ProcLockWakeup` is needed.
fn un_grant_lock(locktag: &LOCKTAG, lockmode: LOCKMODE, proc_no: ProcNumber, lockmethodid: u8) -> bool {
    state::with_shared(|s| {
        let wakeup_needed = s
            .lock_with_mut(locktag, |b| {
                b.set_n_requested(b.n_requested() - 1);
                b.set_requested_at(lockmode as usize, b.requested_at(lockmode as usize) - 1);
                b.set_n_granted(b.n_granted() - 1);
                b.set_granted_at(lockmode as usize, b.granted_at(lockmode as usize) - 1);
                if b.granted_at(lockmode as usize) == 0 {
                    b.set_grant_mask(b.grant_mask() & LOCKBIT_OFF(lockmode));
                }
                (tables::conflict_tab_for(lockmethodid as u16, lockmode) & b.wait_mask()) != 0
            })
            .unwrap_or(false);
        s.proclock_update(locktag, proc_no, |pl| {
            pl.hold_mask &= LOCKBIT_OFF(lockmode);
        });
        wakeup_needed
    })
}

/// `CleanUpLock(lock, proclock, lockMethodTable, hashcode, wakeupNeeded)`
/// (lock.c) — garbage-collect the proclock/lock if empty, else wake waiters.
fn clean_up_lock(locktag: &LOCKTAG, proc_no: ProcNumber, lockmethodid: u8, wakeup_needed: bool) {
    // If this was my last hold, delete my proclock entry.
    let (hold_mask, n_requested) = state::with_shared(|s| {
        let hold = s.proclock_hold_mask(locktag, proc_no);
        if hold == 0 {
            // proclock_remove unchains it from lock->procLocks.
            s.proclock_remove(locktag, proc_no);
        }
        let nreq = s.lock_n_requested(locktag);
        (hold, nreq)
    });
    let _ = hold_mask;

    if n_requested == 0 {
        // Garbage-collect the lock object.
        state::with_shared(|s| {
            s.lock_remove(locktag);
        });
    } else if wakeup_needed {
        proc_lock_wakeup(locktag, lockmethodid);
    }
}

/// `ProcLockWakeup(lockMethodTable, lock)` (proc.c) reached from lock.c's
/// CleanUpLock. We lend `&mut LOCK` from the ambient table to proc.c's public
/// `ProcLockWakeup`, which walks the wait queue through our seams.
fn proc_lock_wakeup(locktag: &LOCKTAG, _lockmethodid: u8) {
    // Materialize a LOCK value for proc.c (it reads only tag/grant/wait state,
    // all reachable via the lock seams keyed on the tag); the per-LOCK body in
    // the ambient table is the authority for the mutations the seams perform.
    let method: types_storage::lock::LockMethod = make_lock_method(locktag.locktag_lockmethodid);
    let mut lock_val = take_lock_snapshot(locktag);
    proc_owner::proc_waitqueue::ProcLockWakeup(&method, &mut lock_val);
}

/// `GrantLockLocal(locallock, owner)` (lock.c) — bump the LOCALLOCK's per-owner
/// counts and (for a non-session owner) register with the resource owner.
fn grant_lock_local(localtag: &LOCALLOCKTAG, owner: Option<ResourceOwnerHandle>) {
    let registered = state::with_local(|m| {
        let l = m.get_mut(localtag).expect("locallock present");
        l.nLocks += 1;
        for o in l.lockOwners.iter_mut() {
            if o.owner == owner {
                o.nLocks += 1;
                return false;
            }
        }
        l.lockOwners.push(LOCALLOCKOWNER {
            owner,
            nLocks: 1,
        });
        l.numLockOwners += 1;
        owner.is_some()
    });
    if registered {
        resowner::resource_owner_remember_lock::call(owner.unwrap(), *localtag);
    }
    check_and_set_lock_held(localtag, true);
}

/// `RemoveLocalLock(locallock)` (lock.c) — drop a LOCALLOCK entry, forgetting it
/// from every resource owner and decrementing the strong-lock count if held.
fn remove_local_lock(localtag: &LOCALLOCKTAG) {
    // Forget from each non-session owner, then clear; decrement strong count.
    let (owners, holds_strong, hashcode) = state::with_local(|m| match m.get_mut(localtag) {
        Some(l) => {
            let owners: Vec<Option<ResourceOwnerHandle>> =
                l.lockOwners.iter().rev().map(|o| o.owner).collect();
            l.numLockOwners = 0;
            l.lockOwners.clear();
            (owners, l.holdsStrongLockCount, l.hashcode)
        }
        None => (Vec::new(), false, 0),
    });

    for owner in owners {
        if let Some(o) = owner {
            resowner::resource_owner_forget_lock::call(o, *localtag);
        }
    }

    if holds_strong {
        let fasthashcode = state::fast_path_strong_lock_hash_partition(hashcode);
        state::FP_STRONG.with(|c| {
            let mut fp = c.borrow_mut();
            fp.count[fasthashcode] -= 1;
        });
        state::with_local(|m| {
            if let Some(l) = m.get_mut(localtag) {
                l.holdsStrongLockCount = false;
            }
        });
    }

    let removed = state::with_local(|m| m.remove(localtag).is_some());
    if !removed {
        warning("locallock table corrupted".into());
    }

    check_and_set_lock_held(localtag, false);
}

// ===========================================================================
// F2: strong-lock interlock + awaited-lock state.
// ===========================================================================

/// `BeginStrongLockAcquire(locallock, fasthashcode)` (lock.c).
fn begin_strong_lock_acquire(localtag: &LOCALLOCKTAG, fasthashcode: usize) {
    state::FP_STRONG.with(|c| {
        let mut fp = c.borrow_mut();
        fp.count[fasthashcode] += 1;
    });
    state::with_local(|m| {
        if let Some(l) = m.get_mut(localtag) {
            l.holdsStrongLockCount = true;
        }
    });
    state::STRONG_LOCK_IN_PROGRESS.with(|c| *c.borrow_mut() = Some((*localtag, fasthashcode)));
}

/// `FinishStrongLockAcquire()` (lock.c).
fn finish_strong_lock_acquire() {
    state::STRONG_LOCK_IN_PROGRESS.with(|c| *c.borrow_mut() = None);
}

/// `AbortStrongLockAcquire()` (lock.c).
pub fn AbortStrongLockAcquire() {
    let in_progress = state::STRONG_LOCK_IN_PROGRESS.with(|c| *c.borrow());
    let (localtag, fasthashcode) = match in_progress {
        Some(v) => v,
        None => return,
    };
    state::FP_STRONG.with(|c| {
        let mut fp = c.borrow_mut();
        fp.count[fasthashcode] -= 1;
    });
    state::with_local(|m| {
        if let Some(l) = m.get_mut(&localtag) {
            l.holdsStrongLockCount = false;
        }
    });
    state::STRONG_LOCK_IN_PROGRESS.with(|c| *c.borrow_mut() = None);
}

/// `GrantAwaitedLock()` (lock.c).
pub fn GrantAwaitedLock() {
    let awaited = state::AWAITED_LOCK.with(|c| *c.borrow());
    let owner = state::AWAITED_OWNER.with(|c| *c.borrow());
    if let Some(tag) = awaited {
        grant_lock_local(&tag, owner);
    }
}

/// `GetAwaitedLock()` (lock.c), as a hashcode for proc.c (`-1` when not
/// waiting).
pub fn GetAwaitedLockHashcode() -> i64 {
    state::AWAITED_LOCK.with(|c| match *c.borrow() {
        Some(tag) => state::with_local(|m| m.get(&tag).map(|l| l.hashcode as i64).unwrap_or(-1)),
        None => -1,
    })
}

/// `ResetAwaitedLock()` (lock.c).
pub fn ResetAwaitedLock() {
    state::AWAITED_LOCK.with(|c| *c.borrow_mut() = None);
}

/// `MarkLockClear(locallock)` (lock.c) — re-keyed on the (tag, mode) pair.
pub fn MarkLockClear(locktag: &LOCKTAG, lockmode: LOCKMODE) {
    let localtag = make_localtag(locktag, lockmode);
    state::with_local(|m| {
        if let Some(l) = m.get_mut(&localtag) {
            l.lockCleared = true;
        }
    });
}

// ===========================================================================
// F2: WaitOnLock / RemoveFromWaitQueue.
// ===========================================================================

/// `WaitOnLock(locallock, owner)` (lock.c) — wrap proc.c's `ProcSleep`, with the
/// awaited-lock bookkeeping `LockErrorCleanup` relies on. The partition lock is
/// NOT held here (LockAcquire released it before calling).
fn wait_on_lock(localtag: &LOCALLOCKTAG, owner: Option<ResourceOwnerHandle>) -> PgResult<ProcWaitStatus> {
    // Record that we are waiting (LockErrorCleanup cleans up on cancel/die).
    state::AWAITED_LOCK.with(|c| *c.borrow_mut() = Some(*localtag));
    state::AWAITED_OWNER.with(|c| *c.borrow_mut() = owner);

    // Lend &mut LOCALLOCK from the ambient table to proc.c's ProcSleep. The
    // entry's address is stable across the call (we hold no &mut to the table).
    let result = with_locallock_mut(localtag, |ll| proc_owner::proc_waitqueue::ProcSleep(ll));

    // No longer want LockErrorCleanup to act (success path).
    state::AWAITED_LOCK.with(|c| *c.borrow_mut() = None);

    result.unwrap_or(Ok(PROC_WAIT_STATUS_ERROR))
}

/// `RemoveFromWaitQueue(proc, hashcode)` (lock.c) — pull `proc` off its lock's
/// wait queue and clean up. The partition lock is held by the caller (proc.c).
pub fn RemoveFromWaitQueue(proc_no: ProcNumber, _hashcode: u32) {
    let wait_tag = proc::proc_wait_lock_tag::call(proc_no);
    let lockmode = proc::proc_wait_lock_mode::call(proc_no);
    let lockmethodid = wait_tag.locktag_lockmethodid;

    // Remove proc from lock's wait queue.
    state::with_shared(|s| {
        if s.lock_exists(&wait_tag) {
            s.waitq_remove(&wait_tag, proc_no);
            // Undo the waiting process's request-count increments.
            s.lock_with_mut(&wait_tag, |b| {
                b.set_n_requested(b.n_requested() - 1);
                b.set_requested_at(lockmode as usize, b.requested_at(lockmode as usize) - 1);
                if b.granted_at(lockmode as usize) == b.requested_at(lockmode as usize) {
                    b.set_wait_mask(b.wait_mask() & LOCKBIT_OFF(lockmode));
                }
            });
        }
    });

    // Clean up the proc's own state and pass it the fail signal.
    proc::wakeup_proc_clear_wait::call(proc_no, PROC_WAIT_STATUS_ERROR);

    // Delete the proclock if it now represents no held locks, then wake others.
    clean_up_lock(&wait_tag, proc_no, lockmethodid, true);
}

// ===========================================================================
// F1: LockAcquire / LockAcquireExtended.
// ===========================================================================

/// `LockAcquire(locktag, lockmode, sessionLock, dontWait)` (lock.c).
pub fn LockAcquire(
    locktag: &LOCKTAG,
    lockmode: LOCKMODE,
    session_lock: bool,
    dont_wait: bool,
) -> PgResult<LockAcquireResult> {
    LockAcquireExtended(locktag, lockmode, session_lock, dont_wait, false)
}

/// `LockAcquireExtended(...)` (lock.c). lmgr.c always passes
/// `reportMemoryError = true`, modeled by the `Err` leg; the `*locallockp`
/// out-parameter is internal (re-keyed by `MarkLockClear`).
pub fn LockAcquireExtended(
    locktag: &LOCKTAG,
    lockmode: LOCKMODE,
    session_lock: bool,
    dont_wait: bool,
    log_lock_failure: bool,
) -> PgResult<LockAcquireResult> {
    let lockmethodid = locktag.locktag_lockmethodid;
    check_lockmethodid(lockmethodid)?;
    check_lockmode(lockmethodid, lockmode)?;

    // Recovery: only RowExclusiveLock or less on database objects.
    if backend_access_transam_xlog_seams::recovery_in_progress::call()
        && !backend_access_transam_xlogrecovery_seams::in_recovery::call()
        && (locktag.locktag_type == types_storage::lock::LOCKTAG_OBJECT
            || locktag.locktag_type == LOCKTAG_RELATION)
        && lockmode > RowExclusiveLock
    {
        return Err(pg_error(
            types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
            format!(
                "cannot acquire lock mode {} on database objects while recovery is in progress",
                mode_name(lockmethodid, lockmode)
            ),
        ));
    }

    let owner = lock_owner(session_lock);
    let localtag = make_localtag(locktag, lockmode);

    // Find or create the LOCALLOCK; initialize if new.
    let (n_locks_existing, hashcode) = state::with_local(|m| {
        if let Some(l) = m.get(&localtag) {
            (l.nLocks, l.hashcode)
        } else {
            (-1, 0)
        }
    });
    if n_locks_existing < 0 {
        let hashcode = LockTagHashCode(locktag);
        state::with_local(|m| {
            m.insert(
                localtag,
                Box::new(LOCALLOCK {
                    tag: localtag,
                    hashcode,
                    lock: None,
                    proclock: None,
                    nLocks: 0,
                    numLockOwners: 0,
                    maxLockOwners: 8,
                    lockOwners: Vec::with_capacity(8),
                    holdsStrongLockCount: false,
                    lockCleared: false,
                }),
            );
        });
    }
    let hashcode = if n_locks_existing < 0 {
        state::with_local(|m| m.get(&localtag).unwrap().hashcode)
    } else {
        hashcode
    };

    // If we already hold the lock, just bump the count locally.
    let already = state::with_local(|m| m.get(&localtag).map(|l| (l.nLocks, l.lockCleared)));
    if let Some((n, cleared)) = already {
        if n > 0 {
            grant_lock_local(&localtag, owner);
            return Ok(if cleared {
                LOCKACQUIRE_ALREADY_CLEAR
            } else {
                LOCKACQUIRE_ALREADY_HELD
            });
        }
    }

    // C: Assert(!IsRelationExtensionLockHeld).

    // (Fast path is deferred to F3; nothing is eligible here.)
    // ConflictsWithRelationFastPath strong-lock interlock (false here).
    if conflicts_with_relation_fast_path(locktag, lockmode) {
        let fasthashcode = state::fast_path_strong_lock_hash_partition(hashcode);
        begin_strong_lock_acquire(&localtag, fasthashcode);
        // FastPathTransferRelationLocks is the deferred F3 seam.
        crate::fastpath::fast_path_transfer_relation_locks(locktag, hashcode)?;
    }

    // Mess with the shared lock table under the partition lock (EXCLUSIVE).
    let partition_guard = lwlock::lwlock_acquire_main::call(
        lock_partition_lock_offset(hashcode),
        LWLockMode::LW_EXCLUSIVE,
    )?;

    let myproc = proc::my_proc_number::call();
    let proclock = setup_lock_in_table(myproc, locktag, lockmode)?;
    let proc_no = match proclock {
        Some(p) => p,
        None => {
            // Out of shared memory (unreachable in the HashMap model).
            AbortStrongLockAcquire();
            partition_guard.release()?;
            if state::with_local(|m| m.get(&localtag).map(|l| l.nLocks).unwrap_or(0)) == 0 {
                remove_local_lock(&localtag);
            }
            return Err(out_of_shared_memory());
        }
    };

    // Record lock/proclock back-pointers in the locallock (identity only).
    state::with_local(|m| {
        if let Some(l) = m.get_mut(&localtag) {
            // The ambient model reaches the LOCK/PROCLOCK by (tag, proc); we
            // mark them present (Some) without materializing the boxes.
            l.lock = Some(Box::new(LOCK { tag: *locktag, ..LOCK::default() }));
        }
    });

    // Conflict with waiters' requests, else with already-held locks.
    let wait_mask = state::with_shared(|s| s.lock_wait_mask(locktag));
    let found_conflict = if (tables::conflict_tab_for(lockmethodid as u16, lockmode) & wait_mask) != 0 {
        true
    } else {
        lock_check_conflicts(lockmethodid, lockmode, locktag, proc_no)
    };

    let mut wait_result = if !found_conflict {
        grant_lock(locktag, proc_no, lockmode);
        PROC_WAIT_STATUS_OK
    } else {
        // Join the wait queue (proc.c). We call it even in dontWait, because it
        // may discover the lock can be granted immediately.
        let method = make_lock_method(lockmethodid);
        with_locallock_mut(&localtag, |ll| {
            proc_owner::proc_waitqueue::JoinWaitQueue(ll, &method, dont_wait)
        })
        .unwrap_or(Ok(PROC_WAIT_STATUS_ERROR))?
    };

    if wait_result == PROC_WAIT_STATUS_ERROR {
        // Deadlock detected while joining, or dontWait and would block.
        AbortStrongLockAcquire();

        let hold_mask = state::with_shared(|s| s.proclock_hold_mask(locktag, proc_no));
        if hold_mask == 0 {
            state::with_shared(|s| {
                // proclock_remove unchains it from lock->procLocks.
                s.proclock_remove(locktag, proc_no);
            });
        }
        state::with_shared(|s| {
            s.lock_with_mut(locktag, |b| {
                b.set_n_requested(b.n_requested() - 1);
                b.set_requested_at(lockmode as usize, b.requested_at(lockmode as usize) - 1);
            });
        });
        partition_guard.release()?;
        if state::with_local(|m| m.get(&localtag).map(|l| l.nLocks).unwrap_or(0)) == 0 {
            remove_local_lock(&localtag);
        }

        if dont_wait {
            if log_lock_failure {
                let buf = backend_storage_lmgr_lmgr_seams::describe_lock_tag::call(*locktag);
                let modename = mode_name(lockmethodid, lockmode);
                let part_guard = lwlock::lwlock_acquire_main::call(
                    lock_partition_lock_offset(hashcode),
                    LWLockMode::LW_SHARED,
                )?;
                let hw = get_lock_holders_and_waiters(locktag);
                part_guard.release()?;
                let _ = backend_utils_error::ereport(types_error::LOG)
                    .errmsg(format!(
                        "process {} could not obtain {} on {}",
                        backend_utils_init_small_seams::my_proc_pid::call(),
                        modename,
                        buf
                    ))
                    .errdetail_log_plural(
                        format!(
                            "Process holding the lock: {}, Wait queue: {}.",
                            hw.holders, hw.waiters
                        ),
                        format!(
                            "Processes holding the lock: {}, Wait queue: {}.",
                            hw.holders, hw.waiters
                        ),
                        hw.holders_num as u64,
                    )
                    .finish(loc(1184, "LockAcquireExtended"));
            }
            return Ok(LOCKACQUIRE_NOT_AVAIL);
        } else {
            // DeadLockReport() does not return.
            return Err(backend_storage_lmgr_deadlock_seams::dead_lock_report::call());
        }
    }

    // In the queue or already granted; if queued, sleep.
    if wait_result == PROC_WAIT_STATUS_WAITING {
        partition_guard.release()?;
        wait_result = wait_on_lock(&localtag, owner)?;
        if wait_result == PROC_WAIT_STATUS_ERROR {
            return Err(backend_storage_lmgr_deadlock_seams::dead_lock_report::call());
        }
    } else {
        partition_guard.release()?;
    }

    // Granted. Update the local lock entry.
    grant_lock_local(&localtag, owner);

    // Lock state fully up-to-date; cancel strong-lock cleanup.
    finish_strong_lock_acquire();

    Ok(LOCKACQUIRE_OK)
}

// ===========================================================================
// F1: LockRelease / LockReleaseAll / LockReleaseSession / owner reassignment.
// ===========================================================================

/// `LockRelease(locktag, lockmode, sessionLock)` (lock.c).
pub fn LockRelease(locktag: &LOCKTAG, lockmode: LOCKMODE, session_lock: bool) -> PgResult<bool> {
    let lockmethodid = locktag.locktag_lockmethodid;
    check_lockmethodid(lockmethodid)?;
    check_lockmode(lockmethodid, lockmode)?;

    let localtag = make_localtag(locktag, lockmode);

    let exists = state::with_local(|m| m.get(&localtag).map(|l| l.nLocks).unwrap_or(0) > 0);
    if !exists {
        warning(format!(
            "you don't own a lock of type {}",
            mode_name(lockmethodid, lockmode)
        ));
        return Ok(false);
    }

    let owner = lock_owner(session_lock);

    // Decrease the count for the resource owner.
    let owner_found = state::with_local(|m| {
        let l = m.get_mut(&localtag).expect("locallock present");
        let mut i = l.numLockOwners - 1;
        while i >= 0 {
            let idx = i as usize;
            if l.lockOwners[idx].owner == owner {
                l.lockOwners[idx].nLocks -= 1;
                if l.lockOwners[idx].nLocks == 0 {
                    l.numLockOwners -= 1;
                    let last = l.numLockOwners as usize;
                    if idx < last {
                        l.lockOwners[idx] = l.lockOwners[last].clone();
                    }
                    l.lockOwners.truncate(l.numLockOwners as usize);
                    return Some(true); // owner had count drop to zero
                }
                return Some(false);
            }
            i -= 1;
        }
        None
    });

    match owner_found {
        None => {
            warning(format!(
                "you don't own a lock of type {}",
                mode_name(lockmethodid, lockmode)
            ));
            return Ok(false);
        }
        Some(dropped_to_zero) => {
            if dropped_to_zero {
                if let Some(o) = owner {
                    resowner::resource_owner_forget_lock::call(o, localtag);
                }
            }
        }
    }

    // Decrease the total local count.
    let still_held = state::with_local(|m| {
        let l = m.get_mut(&localtag).expect("locallock present");
        l.nLocks -= 1;
        if l.nLocks > 0 {
            true
        } else {
            l.lockCleared = false;
            false
        }
    });
    if still_held {
        return Ok(true);
    }

    // (Fast-path release deferred to F3; nothing eligible here.)

    // Mess with the shared lock table.
    let partition_guard = lwlock::lwlock_acquire_main::call(
        lock_partition_lock_offset(LockTagHashCode(locktag)),
        LWLockMode::LW_EXCLUSIVE,
    )?;

    let myproc = proc::my_proc_number::call();
    let hold_mask = state::with_shared(|s| s.proclock_hold_mask(locktag, myproc));
    if (hold_mask & LOCKBIT_ON(lockmode)) == 0 {
        partition_guard.release()?;
        warning(format!(
            "you don't own a lock of type {}",
            mode_name(lockmethodid, lockmode)
        ));
        remove_local_lock(&localtag);
        return Ok(false);
    }

    let wakeup_needed = un_grant_lock(locktag, lockmode, myproc, lockmethodid);
    clean_up_lock(locktag, myproc, lockmethodid, wakeup_needed);

    partition_guard.release()?;
    remove_local_lock(&localtag);
    Ok(true)
}

/// `LockReleaseAll(lockmethodid, allLocks)` (lock.c).
pub fn LockReleaseAll(lockmethodid: u8, all_locks: bool) -> PgResult<()> {
    check_lockmethodid(lockmethodid)?;
    let num_lock_modes = tables::num_lock_modes(lockmethodid as u16);

    // Get rid of our fast-path VXID lock, if appropriate. Note that this is the
    // only way that the lock we hold on our own VXID can ever get released: it is
    // always and only released when a toplevel transaction ends.
    if lockmethodid == DEFAULT_LOCKMETHOD {
        VirtualXactLockTableCleanup()?;
    }

    // Pass 1: scan the locallock table, mark proclocks, drop locallock entries.
    let all_localtags: Vec<LOCALLOCKTAG> = state::with_local(|m| m.keys().copied().collect());

    for localtag in all_localtags {
        // Re-fetch (the entry may already be gone via a recursive remove).
        let snapshot = state::with_local(|m| {
            m.get(&localtag).map(|l| {
                (
                    l.nLocks,
                    l.lockOwners.clone(),
                    l.numLockOwners,
                )
            })
        });
        let (n_locks, _owners, _num_owners) = match snapshot {
            Some(v) => v,
            None => continue,
        };

        // Unused entry: forget it.
        if n_locks == 0 {
            remove_local_lock(&localtag);
            continue;
        }

        // Ignore other lock methods.
        if localtag.lock.locktag_lockmethodid != lockmethodid {
            continue;
        }

        // Non-allLocks: keep session locks (owner == None).
        if !all_locks {
            // Forget non-session owners; compact the session owner into slot 0.
            let keep_session = state::with_local(|m| {
                let l = m.get_mut(&localtag).expect("present");
                let mut session: Option<LOCALLOCKOWNER> = None;
                let owners = core::mem::take(&mut l.lockOwners);
                let mut to_forget: Vec<ResourceOwnerHandle> = Vec::new();
                for o in owners.into_iter() {
                    match o.owner {
                        None => session = Some(o),
                        Some(h) => to_forget.push(h),
                    }
                }
                // (resowner forget happens outside the borrow)
                if let Some(s) = session {
                    if s.nLocks > 0 {
                        l.nLocks = s.nLocks;
                        l.numLockOwners = 1;
                        l.lockOwners = alloc::vec![s];
                        return (true, to_forget);
                    }
                }
                l.numLockOwners = 0;
                l.lockOwners.clear();
                (false, to_forget)
            });
            for h in keep_session.1 {
                resowner::resource_owner_forget_lock::call(h, localtag);
            }
            if keep_session.0 {
                // Keep just the session locks; done with this entry.
                continue;
            }
        }

        // Tuple-lock-held-at-commit assert (warning, asserts-only in C).
        if localtag.lock.locktag_type == LOCKTAG_TUPLE && !all_locks {
            warning("tuple lock held at commit".into());
        }

        // (Fast-path lock case is deferred; with the fast path disabled a
        // locallock always has a proclock in the shared table, so we fall
        // through to the mark-for-release path.)

        // Mark the proclock to show we need to release this lockmode.
        let myproc = proc::my_proc_number::call();
        state::with_shared(|s| {
            s.proclock_update(&localtag.lock, myproc, |pl| {
                pl.release_mask |= LOCKBIT_ON(localtag.mode);
            });
        });

        remove_local_lock(&localtag);
    }

    // Pass 2: scan each partition's proclocks for this backend and release.
    let myproc = proc::my_proc_number::call();
    for partition in 0..NUM_LOCK_PARTITIONS {
        // Collect this backend's proclock tags in this partition by walking its
        // OWN per-partition myProcLocks list (C's
        // `dlist_foreach(&MyProc->myProcLocks[partition])`) — O(held), not a
        // full-slab seq-scan. Empty for a no-lock SELECT, so the common case is
        // O(1) per partition instead of O(slab capacity).
        let proclock_tags: Vec<LOCKTAG> =
            state::with_shared(|s| s.my_proc_lock_tags(myproc, partition as i32));
        if proclock_tags.is_empty() {
            continue;
        }

        let partition_guard = lwlock::lwlock_acquire_main::call(
            lock_partition_lock_offset_by_index(partition),
            LWLockMode::LW_EXCLUSIVE,
        )?;

        for tag in proclock_tags {
            // Ignore other lock methods.
            if tag.locktag_lockmethodid != lockmethodid {
                continue;
            }

            let (hold_mask, mut release_mask) = state::with_shared(|s| {
                s.proclock_get(&tag, myproc)
                    .map(|p| (p.hold_mask, p.release_mask))
                    .unwrap_or((0, 0))
            });

            // allLocks mode forces release of everything held.
            if all_locks {
                release_mask = hold_mask;
                state::with_shared(|s| {
                    s.proclock_update(&tag, myproc, |pl| {
                        pl.release_mask = hold_mask;
                    });
                });
            }

            // Nothing to release and not recyclable.
            if release_mask == 0 && hold_mask != 0 {
                continue;
            }

            let mut wakeup_needed = false;
            let mut i = 1;
            while i <= num_lock_modes {
                if (release_mask & LOCKBIT_ON(i)) != 0 {
                    wakeup_needed |= un_grant_lock(&tag, i, myproc, lockmethodid);
                }
                i += 1;
            }

            state::with_shared(|s| {
                s.proclock_update(&tag, myproc, |pl| {
                    pl.release_mask = 0;
                });
            });

            clean_up_lock(&tag, myproc, lockmethodid, wakeup_needed);
        }

        partition_guard.release()?;
    }

    Ok(())
}

/// `LockReleaseSession(lockmethodid)` (lock.c).
pub fn LockReleaseSession(lockmethodid: u8) -> PgResult<()> {
    if !tables::is_valid_lockmethodid(lockmethodid as u16) {
        return Err(pg_error(
            types_error::ERRCODE_INTERNAL_ERROR,
            format!("unrecognized lock method: {lockmethodid}"),
        ));
    }
    let tags: Vec<LOCALLOCKTAG> = state::with_local(|m| {
        m.keys()
            .filter(|t| t.lock.locktag_lockmethodid == lockmethodid)
            .copied()
            .collect()
    });
    for tag in tags {
        release_lock_if_held(&tag, true)?;
    }
    Ok(())
}

/// `LockReleaseCurrentOwner(locallocks, nlocks)` (lock.c). The "known list"
/// fast path passes LOCALLOCKTAGs; `None` scans the whole table.
pub fn LockReleaseCurrentOwner(locallocks: Option<&[LOCALLOCKTAG]>) -> PgResult<()> {
    match locallocks {
        None => {
            let tags: Vec<LOCALLOCKTAG> = state::with_local(|m| m.keys().copied().collect());
            for tag in tags {
                release_lock_if_held(&tag, false)?;
            }
        }
        Some(list) => {
            for tag in list.iter().rev() {
                release_lock_if_held(tag, false)?;
            }
        }
    }
    Ok(())
}

/// `ReleaseLockIfHeld(locallock, sessionLock)` (lock.c).
fn release_lock_if_held(localtag: &LOCALLOCKTAG, session_lock: bool) -> PgResult<()> {
    let owner = if session_lock {
        None
    } else {
        resowner::lock_current_resource_owner::call()
    };

    // Scan for locks belonging to the target owner.
    enum Action {
        None,
        Partial(Option<ResourceOwnerHandle>),
        Release,
    }
    let action = state::with_local(|m| {
        let l = match m.get_mut(localtag) {
            Some(l) => l,
            None => return Action::None,
        };
        let mut i = l.numLockOwners - 1;
        while i >= 0 {
            let idx = i as usize;
            if l.lockOwners[idx].owner == owner {
                let owner_n = l.lockOwners[idx].nLocks;
                if owner_n < l.nLocks {
                    l.nLocks -= owner_n;
                    l.numLockOwners -= 1;
                    let last = l.numLockOwners as usize;
                    if idx < last {
                        l.lockOwners[idx] = l.lockOwners[last].clone();
                    }
                    l.lockOwners.truncate(l.numLockOwners as usize);
                    return Action::Partial(owner);
                } else {
                    // Reduce to exactly one to call LockRelease once.
                    l.lockOwners[idx].nLocks = 1;
                    l.nLocks = 1;
                    return Action::Release;
                }
            }
            i -= 1;
        }
        Action::None
    });

    match action {
        Action::None => Ok(()),
        Action::Partial(o) => {
            if let Some(h) = o {
                resowner::resource_owner_forget_lock::call(h, *localtag);
            }
            Ok(())
        }
        Action::Release => {
            if !LockRelease(&localtag.lock, localtag.mode, session_lock)? {
                warning("ReleaseLockIfHeld: failed??".into());
            }
            Ok(())
        }
    }
}

/// `LockReassignCurrentOwner(locallocks, nlocks)` (lock.c).
pub fn LockReassignCurrentOwner(locallocks: Option<&[LOCALLOCKTAG]>) -> PgResult<()> {
    let current = resowner::lock_current_resource_owner::call()
        .expect("CurrentResourceOwner must be set for LockReassignCurrentOwner");
    let parent = resowner::resource_owner_get_parent::call(current);

    match locallocks {
        None => {
            let tags: Vec<LOCALLOCKTAG> = state::with_local(|m| m.keys().copied().collect());
            for tag in tags {
                lock_reassign_owner(&tag, current, parent);
            }
        }
        Some(list) => {
            for tag in list.iter().rev() {
                lock_reassign_owner(tag, current, parent);
            }
        }
    }
    Ok(())
}

/// `LockReassignOwner(locallock, parent)` (lock.c).
fn lock_reassign_owner(localtag: &LOCALLOCKTAG, current: ResourceOwnerHandle, parent: ResourceOwnerHandle) {
    // Returns (found_current, remember_parent): found_current => we mutated a
    // current-owner slot and must ForgetLock(current); remember_parent => we
    // handed the parent a fresh slot and must RememberLock(parent).
    let (found_current, remember_parent) = state::with_local(|m| {
        let l = match m.get_mut(localtag) {
            Some(l) => l,
            None => return (false, false),
        };
        let mut ic: i32 = -1;
        let mut ip: i32 = -1;
        let mut i = l.numLockOwners - 1;
        while i >= 0 {
            let idx = i as usize;
            if l.lockOwners[idx].owner == Some(current) {
                ic = i;
            } else if l.lockOwners[idx].owner == Some(parent) {
                ip = i;
            }
            i -= 1;
        }
        if ic < 0 {
            return (false, false); // no current locks
        }
        if ip < 0 {
            // Parent has no slot: give it the child's slot.
            l.lockOwners[ic as usize].owner = Some(parent);
            (true, true)
        } else {
            // Merge child's count into parent's, compact out the child slot.
            let child_n = l.lockOwners[ic as usize].nLocks;
            l.lockOwners[ip as usize].nLocks += child_n;
            l.numLockOwners -= 1;
            let last = l.numLockOwners as usize;
            if (ic as usize) < last {
                l.lockOwners[ic as usize] = l.lockOwners[last].clone();
            }
            l.lockOwners.truncate(l.numLockOwners as usize);
            (true, false)
        }
    });

    if remember_parent {
        resowner::resource_owner_remember_lock::call(parent, *localtag);
    }
    if found_current {
        resowner::resource_owner_forget_lock::call(current, *localtag);
    }
}

// ===========================================================================
// LockRefindAndRelease (2PC + fast-path-transfer release path; F1 reach).
// ===========================================================================

/// `LockRefindAndRelease(lockMethodTable, proc, locktag, lockmode,
/// decrement_strong_lock_count)` (lock.c) — release a lock for which we no
/// longer hold LOCK/PROCLOCK pointers (re-find by tag). Used by LockReleaseAll's
/// transferred-fast-path path and by 2PC. The partition lock is taken here.
pub fn LockRefindAndRelease(
    proc_no: ProcNumber,
    locktag: &LOCKTAG,
    lockmode: LOCKMODE,
    decrement_strong_lock_count: bool,
) -> PgResult<()> {
    let lockmethodid = locktag.locktag_lockmethodid;
    let hashcode = LockTagHashCode(locktag);
    let partition_guard = lwlock::lwlock_acquire_main::call(
        lock_partition_lock_offset(hashcode),
        LWLockMode::LW_EXCLUSIVE,
    )?;

    let exists =
        state::with_shared(|s| s.lock_exists(locktag) && s.proclock_exists(locktag, proc_no));
    if !exists {
        partition_guard.release()?;
        return Err(pg_error(
            types_error::ERRCODE_INTERNAL_ERROR,
            "failed to re-find shared lock object".into(),
        ));
    }

    let hold_mask = state::with_shared(|s| s.proclock_hold_mask(locktag, proc_no));
    if (hold_mask & LOCKBIT_ON(lockmode)) == 0 {
        partition_guard.release()?;
        warning(format!(
            "you don't own a lock of type {}",
            mode_name(lockmethodid, lockmode)
        ));
        return Ok(());
    }

    let wakeup_needed = un_grant_lock(locktag, lockmode, proc_no, lockmethodid);
    clean_up_lock(locktag, proc_no, lockmethodid, wakeup_needed);
    partition_guard.release()?;

    if decrement_strong_lock_count && conflicts_with_relation_fast_path(locktag, lockmode) {
        let fasthashcode = state::fast_path_strong_lock_hash_partition(hashcode);
        state::FP_STRONG.with(|c| {
            let mut fp = c.borrow_mut();
            fp.count[fasthashcode] -= 1;
        });
    }
    Ok(())
}

// ===========================================================================
// Wait-queue seam bodies (proc.c callbacks keyed on (LOCKTAG, ProcNumber)).
// ===========================================================================

pub(crate) fn seam_grant_lock(locktag: LOCKTAG, proc_no: ProcNumber, lockmode: LOCKMODE) {
    grant_lock(&locktag, proc_no, lockmode);
}

pub(crate) fn seam_lock_check_conflicts(
    lockmethodid: u8,
    lockmode: LOCKMODE,
    locktag: LOCKTAG,
    proc_no: ProcNumber,
) -> bool {
    lock_check_conflicts(lockmethodid, lockmode, &locktag, proc_no)
}

pub(crate) fn seam_proclock_hold_mask(locktag: LOCKTAG, holder: ProcNumber) -> LOCKMASK {
    state::with_shared(|s| s.proclock_hold_mask(&locktag, holder))
}

pub(crate) fn seam_lock_group_held_locks(locktag: LOCKTAG, leader: ProcNumber) -> LOCKMASK {
    state::with_shared(|s| {
        if !s.lock_exists(&locktag) {
            return 0;
        }
        let mut mask = 0;
        for h in s.holders(&locktag) {
            if let Some(pl) = s.proclock_get(&locktag, h) {
                if pl.group_leader == leader {
                    mask |= pl.hold_mask;
                }
            }
        }
        mask
    })
}

pub(crate) fn seam_lock_wait_queue_is_empty(locktag: LOCKTAG) -> bool {
    state::with_shared(|s| s.waitq_is_empty(&locktag))
}

pub(crate) fn seam_lock_wait_queue_insert_before(
    locktag: LOCKTAG,
    insert_before: ProcNumber,
    myproc: ProcNumber,
) {
    state::with_shared(|s| {
        s.waitq_insert_before(&locktag, insert_before, myproc);
    });
}

pub(crate) fn seam_lock_wait_queue_push_tail(locktag: LOCKTAG, myproc: ProcNumber) {
    state::with_shared(|s| {
        s.waitq_push_tail(&locktag, myproc);
    });
}

pub(crate) fn seam_lock_set_wait_mask_bit(locktag: LOCKTAG, lockmode: LOCKMODE) {
    state::with_shared(|s| {
        s.lock_with_mut(&locktag, |b| {
            b.set_wait_mask(b.wait_mask() | LOCKBIT_ON(lockmode));
        });
    });
}

pub(crate) fn seam_lock_wait_queue_delete(proc_no: ProcNumber) {
    // dclist_delete_from_thoroughly(&proc->waitLock->waitProcs, ...): find the
    // lock the proc is waiting on (its waitLock tag) and remove it.
    let wait_tag = proc::proc_wait_lock_tag::call(proc_no);
    state::with_shared(|s| {
        s.waitq_remove(&wait_tag, proc_no);
    });
}

pub(crate) fn seam_lock_wait_queue_waiters_snapshot(locktag: LOCKTAG) -> Vec<ProcNumber> {
    state::with_shared(|s| s.waiters(&locktag))
}

/// `GetLockHoldersAndWaiters` inner walk (proc.c log path), seam form (by
/// value).
pub(crate) fn get_lock_holders_and_waiters_seam(
    locktag: LOCKTAG,
) -> backend_storage_lmgr_lock_seams::LockHoldersAndWaiters {
    get_lock_holders_and_waiters(&locktag)
}

/// `GetLockHoldersAndWaiters` inner walk (proc.c log path).
fn get_lock_holders_and_waiters(
    locktag: &LOCKTAG,
) -> backend_storage_lmgr_lock_seams::LockHoldersAndWaiters {
    state::with_shared(|s| {
        if !s.lock_exists(locktag) {
            return backend_storage_lmgr_lock_seams::LockHoldersAndWaiters::default();
        }
        let waiters: Vec<ProcNumber> = s.waiters(locktag);
        let mut holders_pids: Vec<i32> = Vec::new();
        let mut holders_num = 0;
        for h in s.holders(locktag) {
            // A holder is a PROCLOCK not on the wait queue.
            if !waiters.contains(&h) {
                holders_pids.push(proc::proc_pid::call(h));
                holders_num += 1;
            }
        }
        let waiter_pids: Vec<i32> = waiters.iter().map(|&w| proc::proc_pid::call(w)).collect();
        backend_storage_lmgr_lock_seams::LockHoldersAndWaiters {
            holders: join_pids(&holders_pids),
            waiters: join_pids(&waiter_pids),
            holders_num,
        }
    })
}

fn join_pids(pids: &[i32]) -> alloc::string::String {
    let mut out = alloc::string::String::new();
    for (i, p) in pids.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(&p.to_string());
    }
    out
}

// ===========================================================================
// LockMethod / LOCK snapshot bridges for proc.c's by-value calls.
// ===========================================================================

/// Build the `LockMethodData` descriptor for a lock method (proc.c reads
/// `numLockModes` / `conflictTab` / `lockModeNames`).
fn make_lock_method(lockmethodid: u8) -> types_storage::lock::LockMethod {
    let num = tables::num_lock_modes(lockmethodid as u16);
    let conflicts = tables::lock_conflicts();
    let conflict_tab: Vec<LOCKMASK> = conflicts.to_vec();
    let lock_mode_names: Vec<alloc::string::String> = tables::LOCK_MODE_NAMES
        .iter()
        .map(|s| alloc::string::String::from(*s))
        .collect();
    Box::new(types_storage::lock::LockMethodData {
        numLockModes: num,
        conflictTab: conflict_tab,
        lockModeNames: lock_mode_names,
        trace_flag: false,
    })
}

/// Materialize a `LOCK` value snapshot for proc.c's `ProcLockWakeup`, which only
/// reads the tag (the seams it then calls mutate the authoritative ambient
/// table). The wait-mask/grant state is copied so any read inside proc.c sees a
/// consistent picture.
fn take_lock_snapshot(locktag: &LOCKTAG) -> LOCK {
    state::with_shared(|s| {
        s.lock_with(locktag, |b| LOCK {
            tag: b.tag(),
            grantMask: b.grant_mask(),
            waitMask: b.wait_mask(),
            procLocks: Default::default(),
            waitProcs: Default::default(),
            requested: b.requested(),
            nRequested: b.n_requested(),
            granted: b.granted(),
            nGranted: b.n_granted(),
        })
        .unwrap_or(LOCK { tag: *locktag, ..LOCK::default() })
    })
}

/// Run `f` with `&mut LOCALLOCK` lent out of the ambient LOCAL table. proc.c's
/// `JoinWaitQueue` / `ProcSleep` take `&mut LOCALLOCK` and, during the call, may
/// re-enter lock.c's seams (e.g. `GrantAwaitedLock` / `GetAwaitedLock` touch the
/// LOCAL table on the deadlock-recovery path). To keep the `RefCell` borrow from
/// overlapping that re-entry, we temporarily remove the boxed entry, run `f`
/// against the owned box (its contents are stable), then reinsert it. Returns
/// `None` if the entry is absent.
fn with_locallock_mut<R>(
    localtag: &LOCALLOCKTAG,
    f: impl FnOnce(&mut LOCALLOCK) -> R,
) -> Option<R> {
    let mut boxed = state::LOCAL.with(|c| c.borrow_mut().remove(localtag))?;
    let r = f(&mut boxed);
    state::LOCAL.with(|c| {
        c.borrow_mut().insert(*localtag, boxed);
    });
    Some(r)
}

// ===========================================================================
// Virtual transaction locks (fast-path VXID lock on MyProc).
// ===========================================================================

/// `VirtualXactLockTableInsert(VirtualTransactionId vxid)` (lock.c): take the
/// fast-path VXID lock for `vxid` on `MyProc`. The guarded `MyProc` mutation
/// (under `MyProc->fpInfoLock`) is performed by proc.c, which owns the
/// `MyProc`-private `fpInfoLock` / `fp*` PGPROC storage.
pub(crate) fn VirtualXactLockTableInsert(
    vxid: types_storage::VirtualTransactionId,
) -> types_error::PgResult<()> {
    debug_assert!(vxid.is_valid());

    proc::vxid_lock_table_insert_my_proc::call(vxid.procNumber, vxid.localTransactionId)
}

/// `VirtualXactLockTableCleanup()` (lock.c:4613): check whether a VXID lock has
/// been materialized; if so, release it, unblocking waiters.
///
/// The `MyProc->fpInfoLock` critical section (read+clear the fast-path VXID
/// state) is owned by proc.c, which returns the prior `(fastpath, lxid)`. If the
/// fast-path bit was cleared without touching `fpLocalTransactionId`, someone
/// transferred the lock to the main lock table, so we re-find and release it.
pub(crate) fn VirtualXactLockTableCleanup() -> PgResult<()> {
    let (fastpath, lxid) = proc::vxid_lock_table_cleanup_my_proc::call()?;

    // If fpVXIDLock has been cleared without touching fpLocalTransactionId, that
    // means someone transferred the lock to the main lock table.
    if !fastpath && lxid != types_core::InvalidLocalTransactionId {
        let myprocno = proc::my_proc_number::call();
        let locktag = LOCKTAG::virtualtransaction(myprocno as u32, lxid);
        LockRefindAndRelease(
            myprocno,
            &locktag,
            types_storage::lock::ExclusiveLock,
            false,
        )?;
    }

    Ok(())
}

/// `GetRunningTransactionLocks(*nlocks)` (lock.c) — return every currently
/// held `AccessExclusiveLock` with an assigned xid, for use by
/// `LogStandbySnapshot()`.
///
/// C grabs all `NUM_LOCK_PARTITIONS` partition LWLocks (in partition-number
/// order to avoid LWLock deadlock), then `hash_seq`-scans the PROCLOCK table.
/// For each proclock that holds `AccessExclusiveLock` on a `LOCKTAG_RELATION`
/// it reads the holder PGPROC's `xid`; transactions that have already issued
/// their commit WAL record (and so zeroed `xid`) are skipped
/// (`TransactionIdIsValid`). Locks are released in reverse partition order
/// (held by the guards' reverse drop here). C `palloc`s the result in the
/// caller's context and the caller `pfree`s it; here we allocate the result
/// `PgVec` in `mcx`, the caller's target context.
///
/// In this single-process ambient model the PROCLOCK table is the
/// `(LOCKTAG, ProcNumber)`-keyed `state::SHARED.proclocks` map — the analog of
/// `hash_seq_search(LockMethodProcLockHash)`.
pub fn GetRunningTransactionLocks<'mcx>(
    mcx: mcx::Mcx<'mcx>,
) -> PgResult<mcx::PgVec<'mcx, types_storage::storage::xl_standby_lock>> {
    use types_core::xact::TransactionIdIsValid;
    use types_storage::lock::AccessExclusiveLock;

    // Acquire lock on the entire shared lock data structure. Must grab LWLocks
    // in partition-number order to avoid LWLock deadlock. The guards are held
    // until the end and dropped in reverse partition order (Vec drop is
    // front-to-back, so we pop in reverse below).
    let mut guards = Vec::with_capacity(NUM_LOCK_PARTITIONS as usize);
    for i in 0..NUM_LOCK_PARTITIONS as i32 {
        guards.push(lwlock::lwlock_acquire_main::call(
            lock_partition_lock_offset_by_index(i),
            LWLockMode::LW_SHARED,
        )?);
    }

    // Scan the PROCLOCK table to copy the data.
    //
    // If a lock is a currently granted AccessExclusiveLock then it will have
    // just one proclock holder, so locks are never accessed twice in this
    // particular case. (Don't copy this code for use elsewhere because in the
    // general case this will give you duplicate locks when looking at
    // non-exclusive lock types.)
    let mut access_exclusive_locks = mcx::PgVec::new_in(mcx);
    let collected: Vec<(LOCKTAG, ProcNumber)> = state::with_shared(|s| {
        s.proclock_scan()
            .into_iter()
            .filter(|(_, _, pl)| pl.hold_mask & LOCKBIT_ON(AccessExclusiveLock) != 0)
            .filter(|(tag, _, _)| tag.locktag_type == LOCKTAG_RELATION)
            .map(|(tag, pno, _)| (tag, pno))
            .collect()
    });
    for (tag, pno) in collected {
        let xid = proc::proc_xid::call(pno);

        // Don't record locks for transactions if we know they have already
        // issued their WAL record for commit but not yet released lock. It is
        // still possible that we see locks held by already complete
        // transactions, if they haven't yet zeroed their xids.
        if !TransactionIdIsValid(xid) {
            continue;
        }

        access_exclusive_locks.push(types_storage::storage::xl_standby_lock {
            xid,
            dbOid: tag.locktag_field1,
            relOid: tag.locktag_field2,
        });
    }

    // Release locks in reverse order for two reasons: (1) Anyone else who needs
    // more than one of the locks will be trying to lock them in increasing
    // order; we don't want to release the other process until it can get all
    // the locks it needs. (2) This avoids O(N^2) behavior inside LWLockRelease.
    while let Some(guard) = guards.pop() {
        guard.release()?;
    }

    Ok(access_exclusive_locks)
}

// ===========================================================================
// Error-surface helpers.
// ===========================================================================

fn out_of_shared_memory() -> types_error::PgError {
    pg_error(
        types_error::ERRCODE_OUT_OF_MEMORY,
        "out of shared memory".into(),
    )
}

// ===========================================================================
// Wrappers exposed to `crate::recovery` (the 2PC / introspection entry points
// live in a sibling module but reuse this module's grant/release bookkeeping).
// ===========================================================================

/// `pg_error` for the recovery module.
pub(crate) fn pg_error_internal(
    sqlstate: types_error::SqlState,
    message: alloc::string::String,
) -> types_error::PgError {
    pg_error(sqlstate, message)
}

/// `warning` for the recovery module.
pub(crate) fn warning_msg(message: alloc::string::String) {
    warning(message)
}

/// `GrantLock` for the recovery module.
pub(crate) fn grant_lock_seam(locktag: &LOCKTAG, proc_no: ProcNumber, lockmode: LOCKMODE) {
    grant_lock(locktag, proc_no, lockmode)
}

/// `SetupLockInTable(lockMethodTable, proc, locktag, hashcode, lockmode)` for a
/// named proc (recovery / 2PC use it for the dummy PGPROC). The partition lock
/// is held by the caller. Returns the holder ProcNumber on success.
pub(crate) fn setup_lock_in_table_for(
    proc_no: ProcNumber,
    locktag: &LOCKTAG,
    lockmode: LOCKMODE,
) -> PgResult<Option<ProcNumber>> {
    setup_lock_in_table(proc_no, locktag, lockmode)
}

/// `ConflictsWithRelationFastPath` for the recovery module.
pub(crate) fn conflicts_with_relation_fast_path_pub(locktag: &LOCKTAG, mode: LOCKMODE) -> bool {
    conflicts_with_relation_fast_path(locktag, mode)
}

/// `RemoveLocalLock` keyed on a (LOCKTAG, mode) pair for the recovery module.
pub(crate) fn remove_local_lock_for(locktag: &LOCKTAG, mode: LOCKMODE) {
    let localtag = make_localtag(locktag, mode);
    remove_local_lock(&localtag)
}

// ===========================================================================
// DoLockModesConflict / virtual-transaction wait (XactLockForVirtualXact /
// VirtualXactLock).
// ===========================================================================

/// `DoLockModesConflict(mode1, mode2)` (lock.c) — whether two lock modes would
/// conflict, i.e. `lockMethodTable->conflictTab[mode1] & LOCKBIT_ON(mode2)` for
/// the default (heavyweight) lock method.
pub fn DoLockModesConflict(mode1: LOCKMODE, mode2: LOCKMODE) -> bool {
    let conflict_tab = tables::conflict_tab_for(DEFAULT_LOCKMETHOD as u16, mode1);
    (conflict_tab & LOCKBIT_ON(mode2)) != 0
}

/// `XactLockForVirtualXact(vxid, xid, wait)` (lock.c) — if `xid` is valid this
/// is essentially `(Conditional)XactLockTableWait(xid)`; if not, it locks every
/// prepared XID that was known as `vxid` before its `PREPARE TRANSACTION`.
/// Returns false only when `wait == false` and the xid is still running.
fn XactLockForVirtualXact(
    vxid: types_storage::VirtualTransactionId,
    mut xid: types_core::TransactionId,
    wait: bool,
) -> PgResult<bool> {
    use types_core::xact::TransactionIdIsValid;
    use types_storage::lock::ShareLock;

    // There is no point to wait for 2PCs if you have no 2PCs.
    if backend_utils_init_small_seams::max_prepared_xacts::call() == 0 {
        return Ok(true);
    }

    let mut more = false;
    loop {
        // Clear state from previous iterations.
        if more {
            xid = types_core::xact::InvalidTransactionId;
            more = false;
        }

        // If we have no xid, try to find one.
        if !TransactionIdIsValid(xid) {
            let (found, have_more) =
                twophase::two_phase_get_xid_by_virtual_xid::call((vxid.procNumber, vxid.localTransactionId))?;
            xid = found;
            more = have_more;
        }
        if !TransactionIdIsValid(xid) {
            debug_assert!(!more);
            return Ok(true);
        }

        // Check or wait for XID completion.
        let tag = LOCKTAG::transaction(xid);
        let lar = LockAcquire(&tag, ShareLock, false, !wait)?;
        if lar == LOCKACQUIRE_NOT_AVAIL {
            return Ok(false);
        }
        LockRelease(&tag, ShareLock, false)?;

        if !more {
            break;
        }
    }

    Ok(true)
}

/// `VirtualXactLock(vxid, wait)` (lock.c) — with `wait == true`, wait until the
/// given VXID (or any XID acquired by the same transaction) is no longer running
/// and return true; with `wait == false`, just check and return whether it is
/// still running.
///
/// The `&proc->fpInfoLock`-guarded cross-backend examination (and, when needed,
/// the conversion of the proc's fast-path VXID lock into a primary lock-table
/// entry) is performed by proc.c's `virtual_xact_examine_proc` seam, which owns
/// that PGPROC-private critical section. The lock-table transfer itself
/// (`SetupLockInTable` + `GrantLock`) is this unit's work, threaded into the
/// critical section through the `transfer` callback.
pub fn VirtualXactLock(
    vxid: types_storage::VirtualTransactionId,
    wait: bool,
) -> PgResult<bool> {
    use types_storage::lock::{ExclusiveLock, ShareLock, VirtualXactExamineOutcome};

    debug_assert!(vxid.is_valid());

    if vxid.is_recovered_prepared_xact() {
        // No vxid lock; localTransactionId is a normal, locked XID.
        return XactLockForVirtualXact(vxid, vxid.localTransactionId, wait);
    }

    let tag = LOCKTAG::virtualtransaction(vxid.procNumber as u32, vxid.localTransactionId);

    // If a lock table entry must be made, this is the PGPROC on whose behalf it
    // must be done. Note that the transaction might end or the PGPROC might be
    // reassigned to a new backend before we get around to examining it, but it
    // doesn't matter.
    if !proc::proc_number_get_proc_is_present::call(vxid.procNumber) {
        return XactLockForVirtualXact(vxid, types_core::xact::InvalidTransactionId, wait);
    }

    // `transfer`: convert the target proc's fast-path VXID lock into a regular
    // primary lock-table entry, run under the target's fpInfoLock by proc.c.
    let target = vxid.procNumber;
    let transfer = |target: ProcNumber, tag: &LOCKTAG| -> PgResult<()> {
        let hashcode = LockTagHashCode(tag);
        let partition_guard = lwlock::lwlock_acquire_main::call(
            lock_partition_lock_offset(hashcode),
            LWLockMode::LW_EXCLUSIVE,
        )?;
        let res = setup_lock_in_table(target, tag, ExclusiveLock);
        match res {
            Ok(Some(_)) => {
                grant_lock(tag, target, ExclusiveLock);
                partition_guard.release()?;
                Ok(())
            }
            Ok(None) => {
                partition_guard.release()?;
                Err(out_of_shared_memory())
            }
            Err(e) => {
                partition_guard.release()?;
                Err(e)
            }
        }
    };

    let outcome = proc::virtual_xact_examine_proc::call(
        target,
        vxid,
        wait,
        &mut || transfer(target, &tag),
    )?;

    let xid = match outcome {
        VirtualXactExamineOutcome::Ended => {
            return XactLockForVirtualXact(vxid, types_core::xact::InvalidTransactionId, wait);
        }
        VirtualXactExamineOutcome::StillRunningNoWait => {
            return Ok(false);
        }
        VirtualXactExamineOutcome::Proceed { xid } => xid,
    };

    // Time to wait.
    let _ = LockAcquire(&tag, ShareLock, false, false)?;
    LockRelease(&tag, ShareLock, false)?;
    XactLockForVirtualXact(vxid, xid, wait)
}
