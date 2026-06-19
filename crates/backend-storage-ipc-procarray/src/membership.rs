//! F1 — ProcArray slot add/remove + end-of-xact membership (procarray.c).
//!
//! `ProcArrayAdd`/`ProcArrayRemove` (PREPARE dummy-proc entry/exit),
//! `ProcArrayEndTransaction` + its internal helper, `ProcArrayClearTransaction`
//! (PREPARE variant), the `MaintainLatestCompletedXid*` advance, and the
//! lock-batching group-clear path (`ProcArrayGroupClearXid`) with its
//! `pg_atomic_*` CAS over `ProcGlobal->procArrayGroupFirst`.
//!
//! Builds on the F0 shmem model + `ProcArrayLock`/`XidGenLock` (lwlock) and
//! reaches the dense `ProcGlobal->{xids,subxidStates,statusFlags}` arrays + the
//! per-`PGPROC` xact fields + `procArrayGroupFirst` through the proc seam crate.
//! `TransamVariables->{latestCompletedXid,xactCompletionCount}` are reached
//! through the varsup seam crate (varsup owns that singleton).

use types_core::{
    FirstNormalTransactionId, FullTransactionId, InvalidLocalTransactionId, InvalidTransactionId,
    ProcNumber, TransactionId, TransactionIdIsValid, INVALID_PROC_NUMBER,
};
use types_error::PgResult;
use types_storage::storage::{PROC_AFFECTS_ALL_HORIZONS, PROC_VACUUM_STATE_MASK};
use types_storage::LWLockMode::LW_EXCLUSIVE;

use backend_access_transam_transam_seams as transam;
use backend_access_transam_varsup_seams as varsup;
use backend_storage_lmgr_lwlock_seams as lwlock;
use backend_storage_lmgr_proc_seams as proc;

use crate::shmem_model::{FullXidRelativeTo, PROC_ARRAY};

/// `FullTransactionIdIsNormal(fxid)` (`access/transam.h`) — the `FullTransactionId`
/// holds a normal (>= `FirstNormalTransactionId`) XID.
#[inline]
fn full_transaction_id_is_normal(fxid: FullTransactionId) -> bool {
    fxid.xid() >= FirstNormalTransactionId
}

/// `ProcArrayAdd(PGPROC *proc)` (procarray.c) — enter `proc` (a dummy
/// prepared-xact PGPROC, identified here by its `ProcNumber`) into the global
/// ProcArray under `ProcArrayLock`+`XidGenLock`. `ereport(FATAL)` past
/// `maxProcs` carried on `Err`.
pub fn ProcArrayAdd(pgprocno: ProcNumber) -> PgResult<()> {
    // See ProcGlobal comment explaining why both locks are held.
    lwlock::lwlock_acquire_proc_array::call(LW_EXCLUSIVE)?;
    lwlock::lwlock_acquire_xid_gen::call(LW_EXCLUSIVE)?;

    let result = proc_array_add_locked(pgprocno);

    // Release in reversed acquisition order, to reduce frequency of having to
    // wait for XidGenLock while holding ProcArrayLock.
    let r1 = lwlock::lwlock_release_xid_gen::call();
    let r2 = lwlock::lwlock_release_proc_array::call();
    result?;
    r1?;
    r2?;
    Ok(())
}

fn proc_array_add_locked(pgprocno: ProcNumber) -> PgResult<()> {
    PROC_ARRAY.with(|pa| {
        let mut borrow = pa.borrow_mut();
        let array_p = borrow
            .as_mut()
            .expect("ProcArray accessed before ProcArrayShmemInit");

        if array_p.numProcs >= array_p.maxProcs {
            // Oops, no room.  (This really shouldn't happen, since there is a
            // fixed supply of PGPROC structs too, and so we should have failed
            // earlier.)
            return Err(types_error::PgError::error(
                "sorry, too many clients already",
            ));
        }

        // Keep the procs array sorted by (PGPROC *) so that we can utilize
        // locality of references much better. Since the occurrence of
        // adding/removing a proc is much lower than the access to the ProcArray
        // itself, the overhead should be marginal.
        let mut index = 0;
        while index < array_p.numProcs {
            let this_procno = array_p.pgprocnos()[index as usize];
            debug_assert_eq!(proc::proc_pgxactoff::call(this_procno), index);
            // If we have found our right position in the array, break.
            if this_procno > pgprocno {
                break;
            }
            index += 1;
        }

        let movecount = array_p.numProcs - index;
        // memmove the pgprocnos array (procarray-owned) ...
        array_p.pgprocnos_mut().copy_within(
            index as usize..(index + movecount) as usize,
            (index + 1) as usize,
        );
        // ... and the dense ProcGlobal arrays (proc-owned).
        proc::proc_array_xids_memmove::call(index + 1, index, movecount);
        proc::proc_array_subxid_states_memmove::call(index + 1, index, movecount);
        proc::proc_array_status_flags_memmove::call(index + 1, index, movecount);

        array_p.pgprocnos_mut()[index as usize] = pgprocno;
        proc::set_proc_pgxactoff::call(pgprocno, index);
        proc::set_proc_array_xid::call(index, proc::proc_xid::call(pgprocno));
        let (count, overflowed) = proc::proc_subxid_status::call(pgprocno);
        proc::set_proc_array_subxid_state::call(index, count, overflowed);
        proc::set_proc_array_status_flags::call(index, proc::proc_status_flags::call(pgprocno));

        array_p.numProcs += 1;

        // adjust pgxactoff for all following PGPROCs.
        index += 1;
        while index < array_p.numProcs {
            let procno = array_p.pgprocnos()[index as usize];
            debug_assert_eq!(proc::proc_pgxactoff::call(procno), index - 1);
            proc::set_proc_pgxactoff::call(procno, index);
            index += 1;
        }

        Ok(())
    })
}

/// `ProcArrayRemove(PGPROC *proc, TransactionId latestXid)` (procarray.c) —
/// remove the proc from the global ProcArray on COMMIT/ABORT PREPARED,
/// advancing latest-completed to `latest_xid`, under `ProcArrayLock`+`XidGenLock`.
pub fn ProcArrayRemove(pgprocno: ProcNumber, latest_xid: TransactionId) -> PgResult<()> {
    // See ProcGlobal comment explaining why both locks are held.
    lwlock::lwlock_acquire_proc_array::call(LW_EXCLUSIVE)?;
    lwlock::lwlock_acquire_xid_gen::call(LW_EXCLUSIVE)?;

    let result = proc_array_remove_locked(pgprocno, latest_xid);

    // Release in reversed acquisition order.
    let r1 = lwlock::lwlock_release_xid_gen::call();
    let r2 = lwlock::lwlock_release_proc_array::call();
    result?;
    r1?;
    r2?;
    Ok(())
}

fn proc_array_remove_locked(pgprocno: ProcNumber, latest_xid: TransactionId) -> PgResult<()> {
    let myoff = proc::proc_pgxactoff::call(pgprocno);

    PROC_ARRAY.with(|pa| {
        let mut borrow = pa.borrow_mut();
        let array_p = borrow
            .as_mut()
            .expect("ProcArray accessed before ProcArrayShmemInit");

        debug_assert!(myoff >= 0 && myoff < array_p.numProcs);
        debug_assert_eq!(
            proc::proc_pgxactoff::call(array_p.pgprocnos()[myoff as usize]),
            myoff
        );

        if TransactionIdIsValid(latest_xid) {
            debug_assert!(TransactionIdIsValid(proc::proc_array_xid::call(myoff)));

            // Advance global latestCompletedXid while holding the lock.
            MaintainLatestCompletedXid(latest_xid);

            // Same with xactCompletionCount.
            varsup::increment_xact_completion_count::call();

            proc::set_proc_array_xid::call(myoff, InvalidTransactionId);
            proc::set_proc_array_subxid_state::call(myoff, 0, false);
        } else {
            // Shouldn't be trying to remove a live transaction here.
            debug_assert!(!TransactionIdIsValid(proc::proc_array_xid::call(myoff)));
        }

        debug_assert!(!TransactionIdIsValid(proc::proc_array_xid::call(myoff)));
        debug_assert_eq!(proc::proc_array_subxid_state::call(myoff), (0, false));

        proc::set_proc_array_status_flags::call(myoff, 0);

        // Keep the PGPROC array sorted. See notes above.
        let movecount = array_p.numProcs - myoff - 1;
        array_p.pgprocnos_mut().copy_within(
            (myoff + 1) as usize..(myoff + 1 + movecount) as usize,
            myoff as usize,
        );
        proc::proc_array_xids_memmove::call(myoff, myoff + 1, movecount);
        proc::proc_array_subxid_states_memmove::call(myoff, myoff + 1, movecount);
        proc::proc_array_status_flags_memmove::call(myoff, myoff + 1, movecount);

        let last = (array_p.numProcs - 1) as usize;
        array_p.pgprocnos_mut()[last] = -1; // for debugging
        array_p.numProcs -= 1;

        // Adjust pgxactoff of following procs for removed PGPROC (note that
        // numProcs already has been decremented).
        let mut index = myoff;
        while index < array_p.numProcs {
            let procno = array_p.pgprocnos()[index as usize];
            debug_assert_eq!(proc::proc_pgxactoff::call(procno) - 1, index);
            proc::set_proc_pgxactoff::call(procno, index);
            index += 1;
        }
    });

    Ok(())
}

/// `ProcArrayEndTransaction(PGPROC *proc, TransactionId latestXid)`
/// (procarray.c) — advertise no transaction in progress for `MyProc`. Takes the
/// group-clear fast path when `ProcArrayLock` is contended. (The C `proc`
/// argument is always `MyProc`; this seam takes only `latest_xid`.)
pub fn ProcArrayEndTransaction(latest_xid: TransactionId) -> PgResult<()> {
    let pgprocno = proc::my_proc_number::call();

    if TransactionIdIsValid(latest_xid) {
        // We must lock ProcArrayLock while clearing our advertised XID, so that
        // we do not exit the set of "running" transactions while someone else is
        // taking a snapshot.  See discussion in
        // src/backend/access/transam/README.
        debug_assert!(TransactionIdIsValid(proc::proc_xid::call(pgprocno)));

        // If we can immediately acquire ProcArrayLock, we clear our own XID and
        // release the lock.  If not, use group XID clearing to improve
        // efficiency.
        if lwlock::lwlock_conditional_acquire_proc_array::call(LW_EXCLUSIVE)? {
            ProcArrayEndTransactionInternal(pgprocno, latest_xid);
            lwlock::lwlock_release_proc_array::call()?;
        } else {
            ProcArrayGroupClearXid(pgprocno, latest_xid);
        }
    } else {
        // If we have no XID, we don't need to lock, since we won't affect
        // anyone else's calculation of a snapshot.  We might change their
        // estimate of global xmin, but that's OK.
        debug_assert!(!TransactionIdIsValid(proc::proc_xid::call(pgprocno)));
        debug_assert_eq!(proc::proc_subxid_status::call(pgprocno), (0, false));

        proc::set_proc_lxid::call(pgprocno, InvalidLocalTransactionId);
        proc::set_proc_xmin::call(pgprocno, InvalidTransactionId);

        // be sure this is cleared in abort.
        proc::set_proc_delay_chkpt_flags::call(pgprocno, 0);

        proc::set_proc_recovery_conflict_pending::call(pgprocno, false);

        // must be cleared with xid/xmin: avoid unnecessarily dirtying shared
        // cachelines.
        let status_flags = proc::proc_status_flags::call(pgprocno);
        if status_flags & PROC_VACUUM_STATE_MASK != 0 {
            debug_assert!(!lwlock::lwlock_held_by_me_proc_array::call());
            lwlock::lwlock_acquire_proc_array::call(LW_EXCLUSIVE)?;
            let pgxactoff = proc::proc_pgxactoff::call(pgprocno);
            debug_assert_eq!(
                status_flags,
                proc::proc_global_status_flags::call(pgxactoff)
            );
            let new_flags = status_flags & !PROC_VACUUM_STATE_MASK;
            proc::set_proc_status_flags::call(pgprocno, new_flags);
            proc::set_proc_array_status_flags::call(pgxactoff, new_flags);
            lwlock::lwlock_release_proc_array::call()?;
        }
    }

    Ok(())
}

/// `ProcArrayEndTransactionInternal(PGPROC *proc, TransactionId latestXid)`
/// (procarray.c, static) — the actual per-proc clear of `xid`/`xmin`/subxids
/// run with `ProcArrayLock` held (directly, or batched on behalf of the group
/// leader). Maintains `latestCompletedXid`.
pub fn ProcArrayEndTransactionInternal(pgprocno: ProcNumber, latest_xid: TransactionId) {
    let pgxactoff = proc::proc_pgxactoff::call(pgprocno);

    // Note: we need exclusive lock here because we're going to change other
    // processes' PGPROC entries.
    debug_assert!(lwlock::lwlock_held_by_me_in_mode_main::call(
        types_storage::PROC_ARRAY_LOCK,
        LW_EXCLUSIVE
    ));
    debug_assert!(TransactionIdIsValid(proc::proc_array_xid::call(pgxactoff)));
    debug_assert_eq!(
        proc::proc_array_xid::call(pgxactoff),
        proc::proc_xid::call(pgprocno)
    );

    proc::set_proc_array_xid::call(pgxactoff, InvalidTransactionId);
    proc::set_proc_xid::call(pgprocno, InvalidTransactionId);
    proc::set_proc_lxid::call(pgprocno, InvalidLocalTransactionId);
    proc::set_proc_xmin::call(pgprocno, InvalidTransactionId);

    // be sure this is cleared in abort.
    proc::set_proc_delay_chkpt_flags::call(pgprocno, 0);

    proc::set_proc_recovery_conflict_pending::call(pgprocno, false);

    // must be cleared with xid/xmin: avoid unnecessarily dirtying shared
    // cachelines.
    let status_flags = proc::proc_status_flags::call(pgprocno);
    if status_flags & PROC_VACUUM_STATE_MASK != 0 {
        let new_flags = status_flags & !PROC_VACUUM_STATE_MASK;
        proc::set_proc_status_flags::call(pgprocno, new_flags);
        proc::set_proc_array_status_flags::call(pgxactoff, new_flags);
    }

    // Clear the subtransaction-XID cache too while holding the lock.
    let dense_sub = proc::proc_array_subxid_state::call(pgxactoff);
    let proc_sub = proc::proc_subxid_status::call(pgprocno);
    debug_assert_eq!(dense_sub, proc_sub);
    if proc_sub.0 > 0 || proc_sub.1 {
        proc::set_proc_array_subxid_state::call(pgxactoff, 0, false);
        proc::set_proc_subxid_status::call(pgprocno, 0, false);
    }

    // Also advance global latestCompletedXid while holding the lock.
    MaintainLatestCompletedXid(latest_xid);

    // Same with xactCompletionCount.
    varsup::increment_xact_completion_count::call();
}

/// `ProcArrayGroupClearXid(PGPROC *proc, TransactionId latestXid)`
/// (procarray.c, static) — the lock-batching group-clear path: CAS this proc
/// onto `ProcGlobal->procArrayGroupFirst`, then either become the leader who
/// clears the whole batch under `ProcArrayLock` or sleep on the semaphore until
/// the leader clears us.
pub fn ProcArrayGroupClearXid(pgprocno: ProcNumber, latest_xid: TransactionId) {
    // We should definitely have an XID to clear.
    debug_assert!(TransactionIdIsValid(proc::proc_xid::call(pgprocno)));

    // Add ourselves to the list of processes needing a group XID clear.
    proc::set_proc_array_group_member_data::call(pgprocno, true, latest_xid);
    let mut nextidx = proc::proc_array_group_first_read::call();
    loop {
        proc::set_proc_array_group_next::call(pgprocno, nextidx);

        let (succeeded, seen) =
            proc::proc_array_group_first_compare_exchange::call(nextidx, pgprocno as u32);
        if succeeded {
            break;
        }
        nextidx = seen;
    }

    // If the list was not empty, the leader will clear our XID.  It is
    // impossible to have followers without a leader because the first process
    // that has added itself to the list will always have nextidx as
    // INVALID_PROC_NUMBER.
    if nextidx != INVALID_PROC_NUMBER as u32 {
        let mut extra_waits = 0i32;

        // Sleep until the leader clears our XID.
        backend_utils_activity_waitevent_seams::pgstat_report_wait_start::call(
            types_pgstat::wait_event::WAIT_EVENT_PROCARRAY_GROUP_UPDATE,
        );
        loop {
            // acts as a read barrier.
            proc::pg_semaphore_lock::call(pgprocno);
            if !proc::proc_array_group_member::call(pgprocno) {
                break;
            }
            extra_waits += 1;
        }
        backend_utils_activity_waitevent_seams::pgstat_report_wait_end::call();

        debug_assert_eq!(
            proc::proc_array_group_next::call(pgprocno),
            INVALID_PROC_NUMBER as u32
        );

        // Fix semaphore count for any absorbed wakeups.
        while extra_waits > 0 {
            extra_waits -= 1;
            proc::pg_semaphore_unlock::call(pgprocno);
        }
        return;
    }

    // We are the leader.  Acquire the lock on behalf of everyone.
    lwlock::lwlock_acquire_proc_array::call(LW_EXCLUSIVE)
        .expect("ProcArrayGroupClearXid: ProcArrayLock acquire");

    // Now that we've got the lock, clear the list of processes waiting for group
    // XID clearing, saving a pointer to the head of the list.  Trying to pop
    // elements one at a time could lead to an ABA problem.
    let mut nextidx = proc::proc_array_group_first_exchange::call(INVALID_PROC_NUMBER as u32);

    // Remember head of list so we can perform wakeups after dropping lock.
    let mut wakeidx = nextidx;

    // Walk the list and clear all XIDs.
    while nextidx != INVALID_PROC_NUMBER as u32 {
        let nextproc = nextidx as ProcNumber;
        ProcArrayEndTransactionInternal(
            nextproc,
            proc::proc_array_group_member_xid::call(nextproc),
        );
        // Move to next proc in list.
        nextidx = proc::proc_array_group_next::call(nextproc);
    }

    // We're done with the lock now.
    lwlock::lwlock_release_proc_array::call()
        .expect("ProcArrayGroupClearXid: ProcArrayLock release");

    // Now that we've released the lock, go back and wake everybody up.  We don't
    // do this under the lock so as to keep lock hold times to a minimum.
    while wakeidx != INVALID_PROC_NUMBER as u32 {
        let nextproc = wakeidx as ProcNumber;

        wakeidx = proc::proc_array_group_next::call(nextproc);
        proc::set_proc_array_group_next::call(nextproc, INVALID_PROC_NUMBER as u32);

        // ensure all previous writes are visible before follower continues.
        // (The atomic store of procArrayGroupMember below uses SeqCst ordering,
        // which provides the C `pg_write_barrier()`.)
        proc::set_proc_array_group_member::call(nextproc, false);

        if !proc::proc_is_my_proc::call(nextproc) {
            proc::pg_semaphore_unlock::call(nextproc);
        }
    }
}

/// `ProcArrayClearTransaction(PGPROC *proc)` (procarray.c) — PREPARE's variant:
/// clear the xid/xmin bookkeeping without ending the proc's ProcArray presence.
/// (The C `proc` argument is always `MyProc`.)
pub fn ProcArrayClearTransaction() -> PgResult<()> {
    let pgprocno = proc::my_proc_number::call();

    // Currently we need to lock ProcArrayLock exclusively here, as we increment
    // xactCompletionCount below. We also need it at least in shared mode for
    // pgproc->pgxactoff to stay the same below.
    lwlock::lwlock_acquire_proc_array::call(LW_EXCLUSIVE)?;

    let pgxactoff = proc::proc_pgxactoff::call(pgprocno);

    proc::set_proc_array_xid::call(pgxactoff, InvalidTransactionId);
    proc::set_proc_xid::call(pgprocno, InvalidTransactionId);

    proc::set_proc_lxid::call(pgprocno, InvalidLocalTransactionId);
    proc::set_proc_xmin::call(pgprocno, InvalidTransactionId);
    proc::set_proc_recovery_conflict_pending::call(pgprocno, false);

    debug_assert_eq!(proc::proc_status_flags::call(pgprocno) & PROC_VACUUM_STATE_MASK, 0);
    debug_assert_eq!(proc::proc_delay_chkpt_flags::call(pgprocno), 0);

    // Need to increment completion count even though transaction hasn't really
    // committed yet. The reason is that GetSnapshotData() omits the xid of the
    // current transaction, thus without the increment we otherwise could end up
    // reusing the snapshot later. Which would be bad, because it might not count
    // the prepared transaction as running.
    varsup::increment_xact_completion_count::call();

    // Clear the subtransaction-XID cache too.
    let dense_sub = proc::proc_array_subxid_state::call(pgxactoff);
    let proc_sub = proc::proc_subxid_status::call(pgprocno);
    debug_assert_eq!(dense_sub, proc_sub);
    if proc_sub.0 > 0 || proc_sub.1 {
        proc::set_proc_array_subxid_state::call(pgxactoff, 0, false);
        proc::set_proc_subxid_status::call(pgprocno, 0, false);
    }

    lwlock::lwlock_release_proc_array::call()?;
    Ok(())
}

/// `MaintainLatestCompletedXid(TransactionId latestXid)` (procarray.c, static) —
/// advance `TransamVariables->latestCompletedXid` to include `latestXid`
/// (normal-running path).
pub fn MaintainLatestCompletedXid(latest_xid: TransactionId) {
    let cur_latest = varsup::get_latest_completed_xid::call();

    debug_assert!(cur_latest.is_valid());
    debug_assert!(!backend_access_transam_xlog_seams::recovery_in_progress::call());
    debug_assert!(lwlock::lwlock_held_by_me_proc_array::call());

    if transam::transaction_id_precedes::call(cur_latest.xid(), latest_xid) {
        varsup::set_latest_completed_xid::call(FullXidRelativeTo(cur_latest, latest_xid));
    }

    debug_assert!(
        backend_utils_init_miscinit_seams::is_bootstrap_processing_mode::call()
            || full_transaction_id_is_normal(varsup::get_latest_completed_xid::call())
    );
}

/// `MaintainLatestCompletedXidRecovery(TransactionId latestXid)` (procarray.c,
/// static) — the hot-standby recovery-side variant.
pub fn MaintainLatestCompletedXidRecovery(latest_xid: TransactionId) {
    let cur_latest = varsup::get_latest_completed_xid::call();

    debug_assert!(lwlock::lwlock_held_by_me_proc_array::call());

    // Need a FullTransactionId to compare latestXid with. Can't rely on
    // latestCompletedXid to be initialized in recovery. But in recovery it's
    // safe to access nextXid without a lock for the startup process.
    let rel = read_next_full_transaction_id();
    debug_assert!(rel.is_valid());

    if !cur_latest.is_valid()
        || transam::transaction_id_precedes::call(cur_latest.xid(), latest_xid)
    {
        varsup::set_latest_completed_xid::call(FullXidRelativeTo(rel, latest_xid));
    }

    debug_assert!(full_transaction_id_is_normal(
        varsup::get_latest_completed_xid::call()
    ));
}

/// `TransamVariables->nextXid` (`ReadNextFullTransactionId`, used by
/// `MaintainLatestCompletedXidRecovery`).
#[inline]
fn read_next_full_transaction_id() -> FullTransactionId {
    varsup::read_next_full_transaction_id::call()
}

/// `MyProc->xmin` (walsender.c `InitWalSender` assertion) — the backend's
/// advertised xmin in the proc array. The procarray owner reads it through the
/// PGPROC accessor seam.
pub fn MyProcXmin() -> TransactionId {
    proc::proc_xmin::call(proc::my_proc_number::call())
}

/// `MyProc->statusFlags |= PROC_AFFECTS_ALL_HORIZONS;
/// ProcGlobal->statusFlags[MyProc->pgxactoff] = MyProc->statusFlags;` under
/// `ProcArrayLock` (walsender.c `InitWalSender`) — a database-less (physical)
/// walsender's xmin must hold back vacuum in all databases.
pub fn SetProcAffectsAllHorizons() {
    let pgprocno = proc::my_proc_number::call();

    lwlock::lwlock_acquire_proc_array::call(LW_EXCLUSIVE)
        .expect("SetProcAffectsAllHorizons: ProcArrayLock acquire");
    let new_flags = proc::proc_status_flags::call(pgprocno) | PROC_AFFECTS_ALL_HORIZONS;
    proc::set_proc_status_flags::call(pgprocno, new_flags);
    let pgxactoff = proc::proc_pgxactoff::call(pgprocno);
    proc::set_proc_array_status_flags::call(pgxactoff, new_flags);
    lwlock::lwlock_release_proc_array::call()
        .expect("SetProcAffectsAllHorizons: ProcArrayLock release");
}

/// Install the F1-owned inward seams: the membership + end-of-xact seams
/// consumed by twophase / xact.
pub fn init_seams() {
    use backend_storage_ipc_procarray_seams as seams;

    seams::proc_array_add::set(ProcArrayAdd);
    seams::proc_array_remove::set(ProcArrayRemove);
    seams::proc_array_end_transaction::set(ProcArrayEndTransaction);
    seams::proc_array_clear_transaction::set(ProcArrayClearTransaction);
    seams::my_proc_xmin::set(MyProcXmin);
    seams::set_proc_affects_all_horizons::set(SetProcAffectsAllHorizons);
}
