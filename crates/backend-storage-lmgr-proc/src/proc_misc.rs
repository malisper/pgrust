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
//!
//! SCAFFOLD STAGE: bodies are `todo!()`.

use types_core::ProcNumber;
use types_error::PgResult;
use types_storage::lock::LOCKMASK;
use types_storage::storage::PGPROC;

/// `ProcWaitForSignal(uint32 wait_event_info)` — wait on the process latch
/// until signalled (or interrupted). `Err` carries the `CHECK_FOR_INTERRUPTS`
/// `ereport(ERROR)` path.
pub fn ProcWaitForSignal(_wait_event_info: u32) -> PgResult<()> {
    todo!("proc.c:ProcWaitForSignal")
}

/// `ProcSendSignal(ProcNumber procNumber)` — set the latch of the backend
/// owning the given `PGPROC` slot.
pub fn ProcSendSignal(_procNumber: ProcNumber) {
    todo!("proc.c:ProcSendSignal")
}

/// `BecomeLockGroupLeader(void)` — make this backend the leader of a new lock
/// group (idempotent; sets `MyProc->lockGroupLeader = MyProc`).
pub fn BecomeLockGroupLeader() -> PgResult<()> {
    todo!("proc.c:BecomeLockGroupLeader")
}

/// `BecomeLockGroupMember(PGPROC *leader, int pid)` — join the lock group led
/// by `leader`, verifying the leader's pid. Returns `false` if the leader has
/// already exited.
pub fn BecomeLockGroupMember(_leader: &mut PGPROC, _pid: i32) -> PgResult<bool> {
    todo!("proc.c:BecomeLockGroupMember")
}

/// Reclaimed helper (`lock.c`-adjacent, lives with the lock group logic in
/// proc.c): the union of lock-mode masks held on `lock` by every member of
/// `leader`'s lock group, by walking each member's `myProcLocks` partition.
#[allow(dead_code)]
pub(crate) fn lock_group_held_locks(
    _leader: &PGPROC,
    _partition: usize,
) -> LOCKMASK {
    todo!("proc.c:lock_group_held_locks (lockGroupLeader member walk)")
}
