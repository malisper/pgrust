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

use ::types_core::ProcNumber;
use ::types_error::PgResult;
use ::types_storage::{LWLock, LWLockMode};

seam_core::seam!(
    /// `LWLockInitialize(LWLock *lock, int tranche_id)`.
    pub fn lwlock_initialize(lock: &mut LWLock, tranche_id: i32)
);

seam_core::seam!(
    /// `GetLWLockIdentifier(uint32 classId, uint16 eventId)` — the tranche name
    /// for an LWLock wait event. Returns a `'static` tranche name owned by
    /// `lwlock.c`'s built-in/registered tranche tables.
    pub fn get_lwlock_identifier(class_id: ::types_core::uint32, event_id: ::types_core::uint16) -> &'static str
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
    /// `LWLockConditionalAcquire(LWLock *lock, LWLockMode mode)` — try to
    /// acquire the lock without blocking. Returns `Some(guard)` if the lock was
    /// obtained (C `true`) and `None` if it could not be (C `false`). The guard
    /// releases the lock on drop (or via [`LWLockGuard::release`]).
    pub fn lwlock_conditional_acquire<'l>(
        lock: &'l LWLock,
        mode: LWLockMode,
    ) -> PgResult<Option<LWLockGuard<'l>>>
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
    /// `::types_storage::DYNAMIC_SHARED_MEMORY_CONTROL_LOCK`). `MainLWLockArray`
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

// ---- RelationMappingLock (the named LWLock interlocking pg_filenode.map) ----
//
// relmapper.c acquires `RelationMappingLock` in LW_SHARED (read) or
// LW_EXCLUSIVE (write/checkpoint) and releases it mid-function at points C
// chooses. The lock lives in `MainLWLockArray` owned by lwlock.c. As with
// `TwoPhaseStateLock`, a guard form is preferred per AGENTS.md "Locks and held
// resources"; until lwlock.c hands back a guard for the named locks, the
// explicit acquire/release pair is recorded in DESIGN_DEBT.

seam_core::seam!(
    /// `LWLockAcquire(RelationMappingLock, exclusive ? LW_EXCLUSIVE : LW_SHARED)`.
    pub fn lock_relation_mapping(exclusive: bool) -> PgResult<()>
);
seam_core::seam!(
    /// `LWLockRelease(RelationMappingLock)`.
    pub fn unlock_relation_mapping() -> PgResult<()>
);
seam_core::seam!(
    /// `LWLockHeldByMeInMode(RelationMappingLock, LW_EXCLUSIVE)` — the
    /// assertion predicate at the top of `write_relmap_file`. Pure read.
    pub fn relation_mapping_lock_held_by_me_exclusive() -> bool
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

seam_core::seam!(
    /// `LWLockAcquire(ProcArrayLock, mode)` — acquire the built-in ProcArrayLock
    /// (its `MainLWLockArray` offset is owned by lwlock; the named slot avoids
    /// transcribing the individual-lock offset here). `Err` carries the C
    /// `elog(ERROR, "too many LWLocks taken")`.
    pub fn lwlock_acquire_proc_array(mode: LWLockMode) -> PgResult<()>
);

seam_core::seam!(
    /// `LWLockRelease(ProcArrayLock)`.
    pub fn lwlock_release_proc_array() -> PgResult<()>
);

seam_core::seam!(
    /// `LWLockConditionalAcquire(ProcArrayLock, mode)` — acquire the built-in
    /// `ProcArrayLock` if it is immediately available, returning `true`;
    /// otherwise leave it untaken and return `false`. `Err` carries the C
    /// `elog(ERROR, "too many LWLocks taken")`.
    pub fn lwlock_conditional_acquire_proc_array(mode: LWLockMode) -> PgResult<bool>
);

seam_core::seam!(
    /// `LWLockHeldByMe(ProcArrayLock)` — does this backend hold the built-in
    /// `ProcArrayLock` in any mode? Used in `Assert`s.
    pub fn lwlock_held_by_me_proc_array() -> bool
);

seam_core::seam!(
    /// `LWLockAcquire(XidGenLock, mode)` — acquire the built-in `XidGenLock`
    /// (held alongside `ProcArrayLock` while adding/removing a proc so the
    /// dense-array slot order stays consistent with xid generation). `Err`
    /// carries the C `elog(ERROR, "too many LWLocks taken")`.
    pub fn lwlock_acquire_xid_gen(mode: LWLockMode) -> PgResult<()>
);

seam_core::seam!(
    /// `LWLockRelease(XidGenLock)`.
    pub fn lwlock_release_xid_gen() -> PgResult<()>
);

seam_core::seam!(
    /// `LWLockAcquire(WrapLimitsVacuumLock, mode)` — `vac_truncate_clog()`
    /// restricts the wrap-limit advance to one backend per cluster (see
    /// `SimpleLruTruncate`). Held across the pg_database scan and the CLOG /
    /// CommitTs / MultiXact truncation; released by
    /// [`lwlock_release_wrap_limits_vacuum`] (an `ereport` between the two
    /// unwinds through `LWLockReleaseAll`). `Err` carries the C
    /// `elog(ERROR, "too many LWLocks taken")`.
    pub fn lwlock_acquire_wrap_limits_vacuum(mode: LWLockMode) -> PgResult<()>
);

seam_core::seam!(
    /// `LWLockRelease(WrapLimitsVacuumLock)`.
    pub fn lwlock_release_wrap_limits_vacuum() -> PgResult<()>
);

seam_core::seam!(
    /// `LWLockHeldByMe(&MainLWLockArray[lock_offset].lock)` — does this backend
    /// hold the built-in lock at `lock_offset` (in any mode)? Used in
    /// `Assert`s; the lock is named by offset since `MainLWLockArray` is
    /// lwlock.c-owned shared memory.
    pub fn lwlock_held_by_me_main(lock_offset: usize) -> bool
);

seam_core::seam!(
    /// `LWLockHeldByMeInMode(&MainLWLockArray[lock_offset].lock, mode)` — does
    /// this backend hold the built-in lock at `lock_offset` in exactly `mode`?
    pub fn lwlock_held_by_me_in_mode_main(lock_offset: usize, mode: LWLockMode) -> bool
);

seam_core::seam!(
    /// `LWLockShmemSize()` (`storage/lmgr/lwlock.c`) — shared-memory bytes for
    /// the LWLock arrays/tranches; summed by ipci.c `CalculateShmemSize`.
    /// `Err` carries the `add_size`/`mul_size` overflow `ereport`. Owner
    /// unported; scaffolded slot.
    pub fn lwlock_shmem_size() -> ::types_error::PgResult<::types_core::Size>
);

seam_core::seam!(
    /// `CreateLWLocks()` (lwlock.c) — allocate the LWLocks (must run first in
    /// `CreateOrAttachShmemStructs`, before `InitShmemIndex`). `Err` carries
    /// the out-of-shmem `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn create_lwlocks() -> ::types_error::PgResult<()>
);

seam_core::seam!(
    /// `InitLWLockAccess()` (lwlock.c) — initialize a backend's lock-statistics
    /// state at process start (`InitProcess` / `InitAuxiliaryProcess`). A no-op
    /// unless `LWLOCK_STATS` is compiled in.
    pub fn init_lwlock_access()
);
