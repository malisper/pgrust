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
    /// `LWLockAcquire(&MainLWLockArray[lock_offset].lock, mode)` — acquire one
    /// of the individual built-in locks (`lwlocklist.h` offsets, e.g.
    /// `types_storage::DYNAMIC_SHARED_MEMORY_CONTROL_LOCK`). `MainLWLockArray`
    /// lives in main shared memory owned by `lwlock.c`, so the lock is named
    /// by offset rather than by reference.
    pub fn lwlock_acquire_main(lock_offset: usize, mode: LWLockMode) -> PgResult<bool>
);

seam_core::seam!(
    /// `LWLockRelease(&MainLWLockArray[lock_offset].lock)` — release a
    /// built-in lock previously taken via [`lwlock_acquire_main`].
    pub fn lwlock_release_main(lock_offset: usize) -> PgResult<()>
);

seam_core::seam!(
    /// `LWLockReleaseAll()` — release all LWLocks held by this backend; used
    /// during error recovery and at shmem exit.
    pub fn lwlock_release_all()
);
