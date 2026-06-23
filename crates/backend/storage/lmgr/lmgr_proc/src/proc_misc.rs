//! Signal/wait helpers and lock-group membership (`storage/lmgr/proc.c`).
//!
//! `ProcWaitForSignal`/`ProcSendSignal` are the latch-based "wait until poked"
//! primitive used outside the lock manager. `BecomeLockGroupLeader` /
//! `BecomeLockGroupMember` build the parallel-query lock groups whose members
//! share lock ownership for deadlock purposes.
//!
//! RECLAIMED here: `lock_group_held_locks` — the real walk over a lock
//! group's members' `myProcLocks` partitions.
//!
//! OUTWARD seams: latch (`WaitLatch`/`ResetLatch`/`SetLatch`), pgstat
//! wait-event, lock.c (lock-group LWLock partition).

use latch_seams as latch_seams;
use lwlock_seams as lwlock_seams;
use ::postgres_seams::check_for_interrupts;
use ::types_core::ProcNumber;
use types_error::{PgError, PgResult};
use ::types_storage::lock::LOCKMASK;
use ::types_storage::storage::{
    LOCK_MANAGER_LWLOCK_OFFSET, NUM_LOCK_PARTITIONS, PGPROC,
};
use ::types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET};
use ::types_storage::LWLockMode;

use crate::proc_lifecycle;
use crate::proc_shmem;

/// `MyProc->recoveryConflictPending = value` (proc.c / postgres.c) — set this
/// backend's hot-standby recovery-conflict-pending flag.
/// `ProcessRecoveryConflictInterrupt` (tcop/postgres.c) sets it when a
/// buffer-pin conflict forces error handling.
pub fn set_my_proc_recovery_conflict_pending(value: bool) {
    proc_shmem::with_my_proc(|p| p.recoveryConflictPending = value);
}

/// `MyProc->statusFlags |= PROC_IN_VACUUM [| PROC_VACUUM_FOR_WRAPAROUND]`
/// (commands/vacuum.c:2066-2070) — the lazy-VACUUM `vacuum_rel` path setting the
/// PROC flags so concurrent VACUUMs may ignore this backend when computing their
/// `OldestXmin`. Done under `ProcArrayLock` (exclusive), and the dense
/// `ProcGlobal->statusFlags[MyProc->pgxactoff]` mirror is kept in sync, exactly
/// as C does.
pub fn set_my_proc_in_vacuum_flags(is_wraparound: bool) -> PgResult<()> {
    use ::types_storage::storage::{PROC_IN_VACUUM, PROC_VACUUM_FOR_WRAPAROUND};

    // LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
    lwlock_seams::lwlock_acquire_proc_array::call(LWLockMode::LW_EXCLUSIVE)?;

    // MyProc->statusFlags is the genuinely-shared per-proc word (cross-process
    // visible). Read-modify-write it: OR in PROC_IN_VACUUM (+wraparound).
    let my_procno = proc_shmem::my_proc_number();
    let mut flags = proc_shmem::proc_status_flags_shared(my_procno);
    flags |= PROC_IN_VACUUM;
    if is_wraparound {
        flags |= PROC_VACUUM_FOR_WRAPAROUND;
    }
    proc_shmem::set_proc_status_flags_shared(my_procno, flags);
    // MyProc->pgxactoff — the canonical shared offset (renumbered cross-process
    // by ProcArrayAdd/Remove), not the fork-private PGPROC field.
    let pgxactoff = proc_shmem::my_proc_pgxactoff();

    // ProcGlobal->statusFlags[MyProc->pgxactoff] = MyProc->statusFlags;
    proc_shmem::set_proc_array_status_flags(pgxactoff, flags);

    // LWLockRelease(ProcArrayLock);
    lwlock_seams::lwlock_release_proc_array::call()?;
    Ok(())
}

/// `set_indexsafe_procflags(void)` (commands/indexcmds.c) — set
/// `MyProc->statusFlags |= PROC_IN_SAFE_IC` so concurrent index builds (and
/// `WaitForOlderSnapshots`) may ignore this backend. Called during a
/// CONCURRENTLY build for an index that is neither expressional nor partial,
/// before any xid/xmin is installed in MyProc. Done under `ProcArrayLock`
/// (exclusive), with the dense `ProcGlobal->statusFlags[MyProc->pgxactoff]`
/// mirror kept in sync — the exact shape of [`set_my_proc_in_vacuum_flags`].
///
/// ```c
/// LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
/// MyProc->statusFlags |= PROC_IN_SAFE_IC;
/// ProcGlobal->statusFlags[MyProc->pgxactoff] = MyProc->statusFlags;
/// LWLockRelease(ProcArrayLock);
/// ```
pub fn set_indexsafe_procflags() -> PgResult<()> {
    use ::types_storage::storage::PROC_IN_SAFE_IC;

    // This should only be called before installing xid or xmin in MyProc;
    // otherwise concurrent processes could see an Xmin that moves backwards.
    let my_procno = proc_shmem::my_proc_number();
    debug_assert!(
        proc_shmem::with_my_proc(|p| p.xid == 0) && proc_shmem::proc_xmin_shared(my_procno) == 0
    );

    // LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
    lwlock_seams::lwlock_acquire_proc_array::call(LWLockMode::LW_EXCLUSIVE)?;

    // MyProc->statusFlags |= PROC_IN_SAFE_IC; (the genuinely-shared per-proc word).
    let flags = proc_shmem::proc_status_flags_shared(my_procno) | PROC_IN_SAFE_IC;
    proc_shmem::set_proc_status_flags_shared(my_procno, flags);

    // ProcGlobal->statusFlags[MyProc->pgxactoff] = MyProc->statusFlags;
    let pgxactoff = proc_shmem::my_proc_pgxactoff();
    proc_shmem::set_proc_array_status_flags(pgxactoff, flags);

    // LWLockRelease(ProcArrayLock);
    lwlock_seams::lwlock_release_proc_array::call()?;
    Ok(())
}

/// `MyProc->statusFlags == PROC_IN_VACUUM` (commands/vacuumparallel.c:1007) — a
/// parallel-vacuum worker must carry ONLY the `PROC_IN_VACUUM` flag (we don't
/// support parallel vacuum for autovacuum). Used in the worker entry `Assert`.
pub fn my_proc_status_flags_is_in_vacuum_only() -> bool {
    use ::types_storage::storage::PROC_IN_VACUUM;
    proc_shmem::proc_status_flags_shared(proc_shmem::my_proc_number()) == PROC_IN_VACUUM
}

/// `MyProc->recoveryConflictPending` (proc.c / postgres.c) — read this
/// backend's recovery-conflict-pending flag. `errdetail_abort`
/// (tcop/postgres.c) reads it to phrase the abort reason.
pub fn my_proc_recovery_conflict_pending() -> bool {
    proc_shmem::with_my_proc_ref(|p| p.recoveryConflictPending)
}

/// `LockHashPartitionLockByProc(proc)` (`storage/lock.h`): the
/// `MainLWLockArray` offset of the lock-hash partition lock that guards
/// `proc`'s lock-group fields. C:
/// `&MainLWLockArray[LOCK_MANAGER_LWLOCK_OFFSET +
///  (GetNumberFromPGProc(proc) % NUM_LOCK_PARTITIONS)].lock`.
pub(crate) fn lock_hash_partition_lock_offset_by_proc(procno: ProcNumber) -> usize {
    (LOCK_MANAGER_LWLOCK_OFFSET as i64 + (procno as i64 % NUM_LOCK_PARTITIONS as i64))
        as usize
}

/// `ProcWaitForSignal(uint32 wait_event_info)` — wait on the process latch
/// until signalled (or interrupted). `Err` carries the `CHECK_FOR_INTERRUPTS`
/// `ereport(ERROR)` path.
///
/// As this uses the generic process latch the caller has to be robust against
/// unrelated wakeups: always check that the desired state has occurred, and
/// wait again if not.
pub fn ProcWaitForSignal(wait_event_info: u32) -> PgResult<()> {
    // (void) WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0,
    //                  wait_event_info);
    let _ = latch_seams::wait_latch_my_latch::call(
        WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
        0,
        wait_event_info,
    )?;
    // ResetLatch(MyLatch);
    latch_seams::reset_latch_my_latch::call();
    // CHECK_FOR_INTERRUPTS();
    check_for_interrupts::call()?;
    Ok(())
}

/// `ProcSendSignal(ProcNumber procNumber)` — set the latch of the backend
/// owning the given `PGPROC` slot.
pub fn ProcSendSignal(procNumber: ProcNumber) -> PgResult<()> {
    if procNumber < 0 || procNumber as u32 >= proc_shmem::all_proc_count() {
        return Err(PgError::error("procNumber out of range"));
    }

    // SetLatch(&ProcGlobal->allProcs[procNumber].procLatch);
    latch_seams::set_latch::call(proc_shmem::proc_latch_handle(procNumber));
    Ok(())
}

/// `BecomeLockGroupLeader(void)` — make this backend the leader of a new lock
/// group (idempotent; sets `MyProc->lockGroupLeader = MyProc`).
///
/// Once this function has returned, other processes can join the lock group by
/// calling [`BecomeLockGroupMember`].
pub fn BecomeLockGroupLeader() -> PgResult<()> {
    let my_procno = proc_lifecycle::my_proc_number();

    // If we already did it, we don't need to do it again.
    //   if (MyProc->lockGroupLeader == MyProc) return;
    if proc_lifecycle::proc_lock_group_leader_is(my_procno, my_procno) {
        return Ok(());
    }

    // We had better not be a follower.
    //   Assert(MyProc->lockGroupLeader == NULL);
    debug_assert!(proc_lifecycle::proc_lock_group_leader_is_none(my_procno));

    // Create single-member group, containing only ourselves.
    let leader_lwlock_offset = lock_hash_partition_lock_offset_by_proc(my_procno);
    let guard = lwlock_seams::lwlock_acquire_main::call(leader_lwlock_offset, LWLockMode::LW_EXCLUSIVE)?;

    // MyProc->lockGroupLeader = MyProc;
    proc_lifecycle::set_my_proc_lock_group_leader(my_procno);
    // dlist_push_head(&MyProc->lockGroupMembers, &MyProc->lockGroupLink);
    proc_lifecycle::lock_group_members_push_head(my_procno, my_procno);

    guard.release()?;
    Ok(())
}

/// `BecomeLockGroupMember(PGPROC *leader, int pid)` — join the lock group led
/// by `leader`, verifying the leader's pid. Returns `false` if the leader has
/// already exited.
///
/// This is pretty straightforward except for the possibility that the leader
/// whose group we're trying to join might exit before we manage to do so; and
/// the `PGPROC` might get recycled for an unrelated process. To avoid that, we
/// require the caller to pass the PID of the intended `PGPROC` as an interlock.
pub fn BecomeLockGroupMember(leader: &mut PGPROC, pid: i32) -> PgResult<bool> {
    let mut ok = false;

    // Group leader can't become member of group / can't already be a member.
    let my_procno = proc_lifecycle::my_proc_number();
    let leader_procno = proc_lifecycle::proc_number_of(leader);
    debug_assert!(my_procno != leader_procno);
    debug_assert!(proc_lifecycle::proc_lock_group_leader_is_none(my_procno));
    // PID must be valid.
    debug_assert!(pid != 0);

    // Get lock protecting the group fields. Note LockHashPartitionLockByProc
    // calculates the proc number based on the PGPROC slot without looking at
    // its contents, so we will acquire the correct lock even if the leader
    // PGPROC is in process of being recycled.
    let leader_lwlock_offset = lock_hash_partition_lock_offset_by_proc(leader_procno);
    let guard = lwlock_seams::lwlock_acquire_main::call(leader_lwlock_offset, LWLockMode::LW_EXCLUSIVE)?;

    // Is this the leader we're looking for?
    //   if (leader->pid == pid && leader->lockGroupLeader == leader)
    if leader.pid == pid && proc_lifecycle::proc_lock_group_leader_is(leader_procno, leader_procno)
    {
        // OK, join the group.
        ok = true;
        // MyProc->lockGroupLeader = leader;
        proc_lifecycle::set_my_proc_lock_group_leader(leader_procno);
        // dlist_push_tail(&leader->lockGroupMembers, &MyProc->lockGroupLink);
        proc_lifecycle::lock_group_members_push_tail(leader_procno, my_procno);
    }

    guard.release()?;
    Ok(ok)
}

/// `BecomeLockGroupMember` addressed by the leader's `ProcNumber` instead of a
/// raw `PGPROC *`.
///
/// This repo identifies a proc by its slot index (`ProcNumber`), not by a
/// process-local `PGPROC *` (which is meaningless across the
/// leader→DSM→worker hand-off, since each process maps the proc array at its
/// own address). `access/transam/parallel.c` carries the leader's identity in
/// `FixedParallelState::parallel_leader_proc_number`; the worker passes that
/// slot index here. The body is identical to [`BecomeLockGroupMember`] with the
/// two `leader->` field reads (`leader->pid`, `leader->lockGroupLeader`) routed
/// through the by-number `PGPROC` accessors. `GetPGProcByNumber(procno)` always
/// names the correct slot even while that slot is being recycled (the partition
/// lock is computed from the slot index alone), preserving the C interlock.
pub fn BecomeLockGroupMemberByNumber(leader_procno: ProcNumber, pid: i32) -> PgResult<bool> {
    let mut ok = false;

    // Group leader can't become member of group / can't already be a member.
    let my_procno = proc_lifecycle::my_proc_number();
    debug_assert!(my_procno != leader_procno);
    debug_assert!(proc_lifecycle::proc_lock_group_leader_is_none(my_procno));
    // PID must be valid.
    debug_assert!(pid != 0);

    // Get lock protecting the group fields. LockHashPartitionLockByProc
    // calculates the partition from the slot index alone (not the slot's
    // contents), so the correct lock is taken even while the leader PGPROC is
    // being recycled.
    let leader_lwlock_offset = lock_hash_partition_lock_offset_by_proc(leader_procno);
    let guard = lwlock_seams::lwlock_acquire_main::call(leader_lwlock_offset, LWLockMode::LW_EXCLUSIVE)?;

    // Is this the leader we're looking for?
    //   if (leader->pid == pid && leader->lockGroupLeader == leader)
    if proc_lifecycle::proc_pid_of(leader_procno) == pid
        && proc_lifecycle::proc_lock_group_leader_is(leader_procno, leader_procno)
    {
        // OK, join the group.
        ok = true;
        // MyProc->lockGroupLeader = leader;
        proc_lifecycle::set_my_proc_lock_group_leader(leader_procno);
        // dlist_push_tail(&leader->lockGroupMembers, &MyProc->lockGroupLink);
        proc_lifecycle::lock_group_members_push_tail(leader_procno, my_procno);
    }

    guard.release()?;
    Ok(ok)
}

/// Reclaimed helper (`lock.c`-adjacent, lives with the lock group logic in
/// proc.c): the union of lock-mode masks held on a lock partition by every
/// member of `leader`'s lock group, by walking each member's `myProcLocks`
/// partition.
#[allow(dead_code)]
pub(crate) fn lock_group_held_locks(leader: &PGPROC, partition: usize) -> LOCKMASK {
    let leader_procno = proc_lifecycle::proc_number_of(leader);

    let mut held: LOCKMASK = 0;
    // dlist_foreach(member of leader->lockGroupMembers): OR together the
    // holdMask of every PROCLOCK on member->myProcLocks[partition].
    for member_procno in proc_lifecycle::lock_group_members_iter(leader_procno) {
        for hold_mask in proc_lifecycle::my_proc_locks_hold_masks(member_procno, partition) {
            held |= hold_mask;
        }
    }
    held
}
