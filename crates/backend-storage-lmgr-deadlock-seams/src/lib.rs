//! Seam declarations for the `backend-storage-lmgr-deadlock` unit
//! (`storage/lmgr/deadlock.c`), the deadlock checker that `proc.c`'s
//! `CheckDeadLock` invokes once `DEADLOCK_TIMEOUT` fires.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `DeadLockCheck(proc)` — run the deadlock check rooted at `proc`'s wait,
    /// rearranging wait queues to break soft deadlocks where possible; returns
    /// the resulting [`DeadLockState`](types_storage::lock::DeadLockState).
    pub fn deadlock_check(
        procno: types_core::ProcNumber,
    ) -> types_storage::lock::DeadLockState
);

seam_core::seam!(
    /// `DeadLockReport()` — raise the `ereport(ERROR)` describing the deadlock
    /// found by the last `DeadLockCheck`. (`proc.c` itself does not call this;
    /// `lock.c` does on the `PROC_WAIT_STATUS_ERROR` path, but it is part of
    /// this owner's surface.)
    pub fn deadlock_report() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `RememberSimpleDeadLock(proc1, lockmode, lock, proc2)` — record an
    /// already-detected (non-cyclic-search) deadlock for the eventual error
    /// message; used by `JoinWaitQueue`'s early-deadlock branch.
    pub fn remember_simple_deadlock(
        proc1: types_core::ProcNumber,
        lockmode: types_storage::lock::LOCKMODE,
        lock: types_storage::lock::LOCKTAG,
        proc2: types_core::ProcNumber,
    )
);

seam_core::seam!(
    /// `GetBlockingAutoVacuumPgproc()` — the autovacuum worker found by the last
    /// `DeadLockCheck` to be blocking us (set when the result is
    /// `DS_BLOCKED_BY_AUTOVACUUM`), as a `ProcNumber`.
    pub fn get_blocking_autovacuum_pgproc() -> types_core::ProcNumber
);
