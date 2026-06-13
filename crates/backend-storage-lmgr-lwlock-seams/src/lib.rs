//! Seam declarations for the `backend-storage-lmgr-lwlock` unit
//! (`storage/lmgr/lwlock.c`).
//!
//! The owner (`backend-storage-lmgr-lwlock`) installs these from its
//! `init_seams()`. Acquisition returns an [`LWLockGuard`] (AGENTS.md "Locks
//! and held resources": a lock may never be held across a `?` without a
//! `Drop` guard — C survives an `ereport` between acquire/release via
//! `LWLockReleaseAll()` in abort cleanup; here the guard's `Drop` is that
//! backstop). Release where C releases mid-function is the explicit
//! [`LWLockGuard::release`].
//!
//! The main-array (by-offset) surface — `LWLockAcquireMain`,
//! `LWLockReleaseAll` — is a direct dependency on the ported owner crate, not
//! a seam (no dependency cycle requires one).

use types_core::ProcNumber;
use types_error::PgResult;
use types_storage::{LWLock, LWLockMode};

seam_core::seam!(
    /// `LWLockInitialize(LWLock *lock, int tranche_id)`.
    pub fn lwlock_initialize(lock: &mut LWLock, tranche_id: i32)
);

seam_core::seam!(
    /// `LWLockAcquire(LWLock *lock, LWLockMode mode)` — acquire the lock,
    /// returning a guard that releases it on drop (`was_free` carries the C
    /// return value: true if the lock was free, false if it had to wait).
    /// `my_proc_number` is the caller's `MyProcNumber` (the C ambient
    /// per-backend global, passed explicitly per the no-ambient-seams rule).
    /// `Err` carries the C `elog(ERROR, "too many LWLocks taken")`.
    pub fn lwlock_acquire<'l>(
        lock: &'l LWLock,
        mode: LWLockMode,
        my_proc_number: ProcNumber,
    ) -> PgResult<LWLockGuard<'l>>
);

seam_core::seam!(
    /// `LWLockRelease(LWLock *lock)`. `Err` carries the C
    /// `elog(ERROR, "lock %s is not held")`. Reached only through
    /// [`LWLockGuard`] (`release()` or `Drop`); consumers never call it
    /// directly.
    pub fn lwlock_release(lock: &LWLock) -> PgResult<()>
);

/// The held-lock token returned by [`lwlock_acquire`]: `Drop` releases the
/// lock (the silent abort path, C's `LWLockReleaseAll`); [`Self::release`]
/// is the explicit release at the point where C calls `LWLockRelease`,
/// surfacing its error.
#[derive(Debug)]
pub struct LWLockGuard<'l> {
    lock: Option<&'l LWLock>,
    /// The C `LWLockAcquire` return value: true if the lock was free.
    pub was_free: bool,
}

impl<'l> LWLockGuard<'l> {
    /// Wrap a just-acquired lock. Called by the owner's installed
    /// implementation (and test fixtures); consumers only ever receive one.
    pub fn new(lock: &'l LWLock, was_free: bool) -> Self {
        LWLockGuard {
            lock: Some(lock),
            was_free,
        }
    }

    /// `LWLockRelease(lock)` at the C call site, consuming the guard.
    pub fn release(mut self) -> PgResult<()> {
        let lock = self.lock.take().expect("LWLockGuard released twice");
        lwlock_release::call(lock)
    }
}

impl Drop for LWLockGuard<'_> {
    fn drop(&mut self) {
        if let Some(lock) = self.lock.take() {
            // The abort path: release silently ("lock not held" cannot fire
            // for a guard-held lock; C's error-recovery LWLockReleaseAll is
            // likewise non-reporting).
            let _ = lwlock_release::call(lock);
        }
    }
}
