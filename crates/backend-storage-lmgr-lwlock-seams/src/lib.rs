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
    /// `Err` carries the C `elog(ERROR, "too many LWLocks taken")`.
    pub fn lwlock_acquire<'l>(
        lock: &'l LWLock,
        mode: LWLockMode,
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

seam_core::seam!(
    /// `LWLockAcquire(&MainLWLockArray[lock_offset].lock, mode)` — acquire one
    /// of the individual built-in locks (`lwlocklist.h` offsets, e.g.
    /// `types_storage::DYNAMIC_SHARED_MEMORY_CONTROL_LOCK`). `MainLWLockArray`
    /// lives in main shared memory owned by `lwlock.c`, so the lock is named
    /// by offset rather than by reference. Returns a [`MainLWLockGuard`] that
    /// releases the lock on drop (AGENTS.md "Locks and held resources": a lock
    /// held across a `?` needs a `Drop` backstop, matching C's
    /// `LWLockReleaseAll()` in abort cleanup); `was_free` carries the C return
    /// value. `Err` carries the C `elog(ERROR, "too many LWLocks taken")`.
    pub fn lwlock_acquire_main(lock_offset: usize, mode: LWLockMode) -> PgResult<MainLWLockGuard>
);

seam_core::seam!(
    /// `LWLockRelease(&MainLWLockArray[lock_offset].lock)` — release a
    /// built-in lock previously taken via [`lwlock_acquire_main`]. Reached only
    /// through [`MainLWLockGuard`] (`release()` or `Drop`); consumers never
    /// call it directly.
    pub fn lwlock_release_main(lock_offset: usize) -> PgResult<()>
);

/// The held-lock token returned by [`lwlock_acquire_main`]: `Drop` releases
/// the built-in lock (the silent abort path, C's `LWLockReleaseAll`);
/// [`Self::release`] is the explicit release at the point where C calls
/// `LWLockRelease`, surfacing its error.
#[derive(Debug)]
pub struct MainLWLockGuard {
    lock_offset: Option<usize>,
    /// The C `LWLockAcquire` return value: true if the lock was free.
    pub was_free: bool,
}

impl MainLWLockGuard {
    /// Wrap a just-acquired built-in lock. Called by the owner's installed
    /// implementation (and test fixtures); consumers only ever receive one.
    pub fn new(lock_offset: usize, was_free: bool) -> Self {
        MainLWLockGuard {
            lock_offset: Some(lock_offset),
            was_free,
        }
    }

    /// `LWLockRelease(&MainLWLockArray[offset].lock)` at the C call site,
    /// consuming the guard and surfacing any release error.
    pub fn release(mut self) -> PgResult<()> {
        let offset = self
            .lock_offset
            .take()
            .expect("MainLWLockGuard released twice");
        lwlock_release_main::call(offset)
    }
}

impl Drop for MainLWLockGuard {
    fn drop(&mut self) {
        if let Some(offset) = self.lock_offset.take() {
            let _ = lwlock_release_main::call(offset);
        }
    }
}

seam_core::seam!(
    /// `LWLockReleaseAll()` — release all LWLocks held by this backend; used
    /// during error recovery and at shmem exit.
    pub fn lwlock_release_all()
);
