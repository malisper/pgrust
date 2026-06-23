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
