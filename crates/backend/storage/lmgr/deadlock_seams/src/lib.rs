//! Seam declarations for the `backend-storage-lmgr-deadlock` unit
//! (`storage/lmgr/deadlock.c`): the deadlock detector's public interface, called
//! by `proc.c` (`ProcSleep`/`CheckDeadLock`) across the proc <-> deadlock cycle.
//!
//! The owning unit installs these from its `init_seams()`; until proc.c lands and
//! actually calls them, the slots are installed but simply unused.

use types_deadlock::{DeadLockState, LockId, LockSpace, ProcId};
use types_error::{PgError, PgResult};
use types_storage::lock::LOCKMODE;

seam_core::seam!(
    /// `InitDeadLockChecking()` — per-backend allocation of the detector's
    /// working memory at backend startup. The C `palloc`s can hit
    /// `ERRCODE_OUT_OF_MEMORY`, so the allocation is fallible.
    pub fn init_dead_lock_checking() -> PgResult<()>
);

seam_core::seam!(
    /// `DeadLockCheck(proc)` — check for deadlocks involving `proc`, rearranging
    /// wait queues to resolve soft cycles where possible. Caller holds all lock
    /// partition locks (modeled by `&mut LockSpace`). `my_proc` is the caller's
    /// `MyProc` (passed explicitly, narrowest capability — not an ambient seam):
    /// it is matched against directly-blocking autovacuum workers. `Err` carries
    /// the C `elog(FATAL)` consistency-check failures.
    pub fn dead_lock_check(
        space: &mut LockSpace,
        proc: ProcId,
        my_proc: Option<ProcId>,
    ) -> PgResult<DeadLockState>
);

seam_core::seam!(
    /// `GetBlockingAutoVacuumPgproc()` — the proc of the autovacuum blocking a
    /// process (cleared as it is returned), or `None`.
    pub fn get_blocking_auto_vacuum_pgproc() -> Option<ProcId>
);

seam_core::seam!(
    /// `DeadLockReport()` — build the deadlock `ereport(ERROR)`. C is
    /// `pg_noreturn`; the seam returns the constructed `PgError` for the caller
    /// to raise.
    pub fn dead_lock_report() -> PgError
);

seam_core::seam!(
    /// `RememberSimpleDeadLock(proc1, lockmode, lock, proc2)` — record the
    /// info for a trivial two-way deadlock detected by `ProcSleep`.
    pub fn remember_simple_dead_lock(
        space: &LockSpace,
        proc1: ProcId,
        lockmode: LOCKMODE,
        lock: LockId,
        proc2: ProcId,
    )
);

