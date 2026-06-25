//! Seam declarations for the `backend-storage-ipc-ipci` unit
//! (`storage/ipc/ipci.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `CreateSharedMemoryAndSemaphores()` (ipci.c): create and initialize the
    /// main shared-memory segment and semaphores. The C path `ereport(FATAL)`s
    /// if it cannot create the segment (never a recoverable ERROR), so it is
    /// modeled infallible.
    pub fn create_shared_memory_and_semaphores()
);

seam_core::seam!(
    /// Reset the transient cross-process shared state a SIGQUIT/SIGKILL-killed
    /// backend left behind, on the postmaster's crash-restart reinitialization.
    /// Stands in for C's fresh, zeroed shared segment (which this tree reuses
    /// across the restart): re-zeroes the lock manager's LOCK/PROCLOCK arena so a
    /// lock held by a crash-killed in-progress transaction does not survive into,
    /// and deadlock, the new generation. `Err` carries any subsystem
    /// `ereport(ERROR)`.
    pub fn reset_shared_state_after_crash() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// Re-register the `ReleaseSemaphores` `on_shmem_exit` callback after the
    /// postmaster's crash-reinit `shmem_exit(1)` consumed the on-exit list.
    ///
    /// In C, crash reinit re-runs `CreateSharedMemoryAndSemaphores` →
    /// `PGReserveSemaphores`, which re-registers the callback. This tree reuses
    /// the existing semaphore batch (and main shmem segment) for the cluster's
    /// lifetime and deliberately skips that re-create (see
    /// `PostmasterStateMachine`'s `if !pm().shmem_created` guard), so the
    /// `on_shmem_exit` registration that the reinit `shmem_exit(1)` just consumed
    /// must be re-established here. Otherwise the persistent SysV semaphore sets
    /// would leak at the postmaster's eventual genuine final exit (`SEMMNI`
    /// exhaustion), one crash-reinit later. Delegates to the pg_sema seam.
    /// `Err` carries the on-exit-list-full `ereport(FATAL)`.
    pub fn reregister_release_semaphores() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `CalculateShmemSize(int *num_semaphores)` (ipci.c): estimate the total
    /// size of the main shared-memory segment by summing every subsystem's
    /// `*ShmemSize`, plus the add-in request. The C `num_semaphores`
    /// out-parameter is folded into the returned tuple (`(size, num_semaphores)`).
    /// `Err` carries the `add_size`/`mul_size` overflow `ereport(ERROR)`.
    /// Used by both `CreateSharedMemoryAndSemaphores` and `InitializeShmemGUCs`.
    pub fn calculate_shmem_size() -> types_error::PgResult<(usize, i32)>
);
