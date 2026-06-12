//! Seam declarations for the `backend-storage-lmgr-lwlock` unit
//! (`storage/lmgr/lwlock.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_error::PgResult;
use types_storage::{LWLock, LWLockMode};

seam_core::seam!(
    /// `LWLockInitialize(LWLock *lock, int tranche_id)`.
    pub fn lwlock_initialize(lock: &mut LWLock, tranche_id: i32)
);

seam_core::seam!(
    /// `LWLockAcquire(LWLock *lock, LWLockMode mode)` — returns true if the
    /// lock was free, false if it had to wait. `Err` carries the C
    /// `elog(ERROR, "too many LWLocks taken")`.
    pub fn lwlock_acquire(lock: &mut LWLock, mode: LWLockMode) -> PgResult<bool>
);

seam_core::seam!(
    /// `LWLockRelease(LWLock *lock)`. `Err` carries the C
    /// `elog(ERROR, "lock %s is not held")`.
    pub fn lwlock_release(lock: &mut LWLock) -> PgResult<()>
);

seam_core::seam!(
    /// `LWLockRelease(&MainLWLockArray[lock_id].lock)` — release one of the
    /// individual (named) LWLocks by its `lwlocklist.h` index (e.g.
    /// `types_storage::LWLOCK_PROC_ARRAY`, `types_storage::LWLOCK_XID_GEN`).
    pub fn lwlock_release_builtin(lock_id: i32) -> PgResult<()>
);
