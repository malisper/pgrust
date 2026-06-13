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

// ---- TwoPhaseStateLock (the named LWLock guarding the 2PC shared state) ----
//
// twophase.c acquires `TwoPhaseStateLock` in LW_SHARED or LW_EXCLUSIVE and
// releases it mid-function at points C chooses; the lock object itself lives
// in shmem stood up by `TwoPhaseShmemInit` (deferred). These model that
// acquire/release on the named lock. A guard form is preferred per AGENTS.md
// "Locks and held resources"; until the 2PC shmem lock owner lands and can
// hand back a guard, the explicit acquire/release pair is recorded in
// DESIGN_DEBT.

seam_core::seam!(
    /// `LWLockAcquire(TwoPhaseStateLock, exclusive ? LW_EXCLUSIVE : LW_SHARED)`.
    pub fn lock_twophase_state(exclusive: bool) -> PgResult<()>
);
seam_core::seam!(
    /// `LWLockRelease(TwoPhaseStateLock)`.
    pub fn unlock_twophase_state() -> PgResult<()>
);
seam_core::seam!(
    /// `LWLockHeldByMeInMode(TwoPhaseStateLock, LW_EXCLUSIVE)` — the assertion
    /// predicate guarding the redo/scan entry points. Pure read.
    pub fn twophase_state_held_exclusive() -> bool
);
