//! Heavyweight-lock wait queue, sleeping, and wakeups (`storage/lmgr/proc.c`).
//!
//! When a backend cannot get a heavyweight lock immediately, `JoinWaitQueue`
//! inserts its `PGPROC` into the `LOCK`'s `waitProcs` queue at the position
//! dictated by lock-mode conflicts, then `ProcSleep` blocks on the process
//! semaphore until granted or a deadlock/timeout fires. `ProcWakeup` /
//! `ProcLockWakeup` move granted waiters off the queue and signal them.
//! `CheckDeadLock` runs on the deadlock timer; `LockErrorCleanup` unwinds a
//! partially-entered wait on error.
//!
//! RECLAIMED here: the wait-queue priority insertion (the conflict-ordered
//! placement of `MyProc` in the `dclist` of waiters).
//!
//! OUTWARD seams: lock.c (`LockCheckConflicts`/`GrantLock`/
//! `RemoveFromWaitQueue`/`GetLockmodeName`), the deadlock checker
//! (`DeadLockCheck`/`DeadLockReport`/`InitDeadLockChecking`), syncrep, latch,
//! pgstat wait-event, timeout.
//!
//! SCAFFOLD STAGE: bodies are `todo!()`.

use types_core::ProcNumber;
use types_error::PgResult;
use types_storage::lock::{LockMethod, LOCALLOCK, LOCK};
use types_storage::storage::{ProcWaitStatus, PGPROC};
use types_stringinfo::StringInfo;

/// `JoinWaitQueue(LOCALLOCK *locallock, LockMethod lockMethodTable, bool
/// dontWait)` — insert `MyProc` into the lock's wait queue (or report it can
/// be granted immediately / would deadlock). Returns the resulting wait
/// status.
pub fn JoinWaitQueue(
    _locallock: &mut LOCALLOCK,
    _lockMethodTable: &LockMethod,
    _dontWait: bool,
) -> PgResult<ProcWaitStatus> {
    todo!("proc.c:JoinWaitQueue")
}

/// `ProcSleep(LOCALLOCK *locallock)` — block on the process semaphore until
/// the awaited lock is granted, a deadlock is detected, or a timeout fires.
pub fn ProcSleep(_locallock: &mut LOCALLOCK) -> PgResult<ProcWaitStatus> {
    todo!("proc.c:ProcSleep")
}

/// `ProcWakeup(PGPROC *proc, ProcWaitStatus waitStatus)` — remove `proc` from
/// its wait queue, stamp its final status, and signal it.
pub fn ProcWakeup(_proc: &mut PGPROC, _waitStatus: ProcWaitStatus) {
    todo!("proc.c:ProcWakeup")
}

/// `ProcLockWakeup(LockMethod lockMethodTable, LOCK *lock)` — wake every
/// waiter on `lock` that can now be granted.
pub fn ProcLockWakeup(_lockMethodTable: &LockMethod, _lock: &mut LOCK) {
    todo!("proc.c:ProcLockWakeup")
}

/// `CheckDeadLock(void)` — deadlock-timer handler: run the deadlock checker
/// and, if a cycle is found, arrange for the victim to error out.
pub fn CheckDeadLock() -> PgResult<()> {
    todo!("proc.c:CheckDeadLock")
}

/// `CheckDeadLockAlert(void)` — SIGALRM handler that sets the
/// deadlock-check-needed flag and the latch.
pub fn CheckDeadLockAlert() {
    todo!("proc.c:CheckDeadLockAlert")
}

/// `LockErrorCleanup(void)` — unwind a partially-entered lock wait when the
/// waiting backend errors out (remove `MyProc` from the wait queue, disable
/// the timers, reset wait state).
pub fn LockErrorCleanup() -> PgResult<()> {
    todo!("proc.c:LockErrorCleanup")
}

/// `ProcReleaseLocks(bool isCommit)` — release all locks at transaction end
/// (calls `LockReleaseAll` for the default lock method, then resets the
/// fast-path VXID lock).
pub fn ProcReleaseLocks(_isCommit: bool) -> PgResult<()> {
    todo!("proc.c:ProcReleaseLocks")
}

/// `GetLockHoldersAndWaiters(LOCALLOCK *locallock, StringInfo
/// lock_holders_sbuf, StringInfo lock_waiters_sbuf, int *lockHoldersNum)` —
/// build the human-readable holder/waiter PID lists for a lock-wait log
/// message.
pub fn GetLockHoldersAndWaiters(
    _locallock: &LOCALLOCK,
    _lock_holders_sbuf: &mut StringInfo<'_>,
    _lock_waiters_sbuf: &mut StringInfo<'_>,
    _lockHoldersNum: &mut i32,
) -> PgResult<()> {
    todo!("proc.c:GetLockHoldersAndWaiters")
}

/// Reclaimed helper (`proc.c` static): the conflict-ordered insertion point of
/// `MyProc` in a lock's `dclist` of waiters, computed from the held/awaited
/// lock-mode masks. Returns the `ProcNumber` to insert before, or `None` to
/// append at the tail.
#[allow(dead_code)]
pub(crate) fn wait_queue_insert_before(
    _lock: &LOCK,
    _lockMethodTable: &LockMethod,
) -> Option<ProcNumber> {
    todo!("proc.c: JoinWaitQueue priority-insertion scan")
}
