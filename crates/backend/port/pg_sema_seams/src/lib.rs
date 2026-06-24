//! Seam declarations for the OS-semaphore owner
//! (`src/backend/port/{sysv,posix,win32}_sema.c`). ipci.c reserves
//! semaphores during `CreateSharedMemoryAndSemaphores` and sizes them in
//! `CalculateShmemSize`. The owning unit installs these from its
//! `init_seams()`; until then a call panics loudly.

seam_core::seam!(
    /// `PGSemaphoreShmemSize(int maxSemas)` (pg_sema) — shared-memory bytes for
    /// `maxSemas` semaphores; summed by ipci.c `CalculateShmemSize`. `Err`
    /// carries the `add_size`/`mul_size` overflow `ereport`. Scaffolded slot.
    pub fn pg_semaphore_shmem_size(max_semas: i32) -> types_error::PgResult<types_core::Size>
);

seam_core::seam!(
    /// `PGReserveSemaphores(int maxSemas)` (pg_sema) — reserve the OS
    /// semaphores at postmaster startup (`CreateSharedMemoryAndSemaphores`).
    /// `Err` carries the `ereport(FATAL)` when the OS cannot create them.
    /// Scaffolded slot.
    pub fn pg_reserve_semaphores(max_semas: i32) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// Re-register the `ReleaseSemaphores` `on_shmem_exit` callback (the tail of
    /// `PGReserveSemaphores`) without re-creating any semaphores. The postmaster
    /// crash-reinit path calls this after `shmem_exit(1)`, which consumes the
    /// whole `on_shmem_exit` list: this tree reuses the existing semaphore batch
    /// and skips the C `CreateSharedMemoryAndSemaphores` re-create that would
    /// otherwise re-register the callback, so without re-registering here the
    /// persistent sets would leak at the postmaster's eventual final exit (the
    /// SEMMNI-exhaustion leak, one crash-reinit later). `Err` carries the
    /// `ereport(FATAL)` if the on-exit list is full.
    pub fn pg_reregister_release_semaphores() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `PGSemaphoreReset(GetPGProcByNumber(procno)->sem)` (pg_sema) — reset the
    /// named PGPROC's wait semaphore to zero. Called from `InitProcess` /
    /// `InitAuxiliaryProcess` to ensure the slot's semaphore starts at zero.
    pub fn pg_semaphore_reset(procno: types_core::ProcNumber)
);

seam_core::seam!(
    /// `PGSemaphoreLock(GetPGProcByNumber(procno)->sem)` (pg_sema) — block on
    /// the named PGPROC's wait semaphore (the proc-sleep wait primitive).
    pub fn pg_semaphore_lock(procno: types_core::ProcNumber)
);

seam_core::seam!(
    /// `PGSemaphoreUnlock(GetPGProcByNumber(procno)->sem)` (pg_sema) — signal
    /// the named PGPROC's wait semaphore to wake a sleeping waiter.
    pub fn pg_semaphore_unlock(procno: types_core::ProcNumber)
);
