//! Seam declarations for the `backend-storage-ipc-dsm-core` unit, which ports
//! both `src/backend/storage/ipc/dsm.c` and `src/backend/storage/ipc/ipc.c`.
//! The owning unit installs these from its `init_seams()`; until then a call
//! panics loudly.

seam_core::seam!(
    /// `dsm_detach_all()` (`dsm.c`): detach every dynamic shared memory
    /// segment, including the control segment.
    pub fn dsm_detach_all()
);

seam_core::seam!(
    /// `dsm_estimate_size()` (`dsm.c`) — shared-memory bytes for the DSM
    /// control segment; summed by ipci.c `CalculateShmemSize`. `Err` carries
    /// the `add_size`/`mul_size` overflow `ereport`. Scaffolded slot.
    pub fn dsm_estimate_size() -> types_error::PgResult<types_core::Size>
);

seam_core::seam!(
    /// `dsm_shmem_init()` (`dsm.c`) — initialize the DSM state in main shared
    /// memory (called from `CreateOrAttachShmemStructs`). `Err` carries the
    /// out-of-shmem `ereport(ERROR)`. Scaffolded slot.
    pub fn dsm_shmem_init() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `dsm_postmaster_startup(PGShmemHeader *shim)` (`dsm.c`) — set up the DSM
    /// control segment at postmaster startup. The `shim` header is genuinely
    /// shared memory (raw pointer, opacity inherited). `Err` carries the
    /// `ereport(ERROR)`. Scaffolded slot.
    pub fn dsm_postmaster_startup(shim: *mut types_storage::PGShmemHeader) -> types_error::PgResult<()>
);

// ---------------------------------------------------------------------------
// `storage/ipc/ipc.c` seams. These belong to the same `dsm-core` unit (which
// also ports ipc.c); they live here so the seam-install owner derives to
// `backend-storage-ipc-dsm-core`, the crate whose `init_seams()` installs
// them.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `proc_exit(code, my_pid)` (`storage/ipc/ipc.c`) — run the on_proc_exit
    /// callbacks and terminate the process. Never returns. `my_pid` is the
    /// caller's `MyProcPid` (globals.c), passed explicitly per the
    /// no-ambient-global rule; it backs the C "called in child process"
    /// PANIC check against `getpid()`.
    pub fn proc_exit(code: i32, my_pid: i32) -> !
);

seam_core::seam!(
    /// `on_proc_exit(function, arg)` (`storage/ipc/ipc.c`) — register a
    /// callback to run inside `proc_exit`. The `Err` is the C
    /// `ereport(FATAL)` past `MAX_ON_EXITS`. Callbacks carry the same
    /// `PgResult` failure surface (the C callbacks may `ereport`).
    pub fn on_proc_exit(
        function: fn(i32, types_datum::Datum) -> types_error::PgResult<()>,
        arg: types_datum::Datum,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `on_shmem_exit(function, arg)` (`storage/ipc/ipc.c`): register a
    /// callback to run while shared memory is still accessible during
    /// `shmem_exit`. The `Err` is the C `ereport(FATAL)` past
    /// `MAX_ON_EXITS`. Callbacks carry the same `PgResult` failure surface
    /// (the C callbacks may `ereport`).
    pub fn on_shmem_exit(
        callback: fn(code: i32, arg: types_datum::Datum) -> types_error::PgResult<()>,
        arg: types_datum::Datum,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `before_shmem_exit(function, arg)` (ipc.c): register a callback to run
    /// early in shmem exit. C `ereport(FATAL)`s when the callback table is
    /// full, carried on `Err`. The callback's `PgResult` mirrors a C callback
    /// that can `ereport(ERROR)`.
    pub fn before_shmem_exit(
        callback: fn(code: i32, arg: types_datum::Datum) -> types_error::PgResult<()>,
        arg: types_datum::Datum,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `on_exit_reset()` (`storage/ipc/ipc.c`) — clear the on_proc_exit /
    /// before_shmem_exit / on_shmem_exit callback arrays inherited from the
    /// postmaster (a forked child must not run the parent's handlers).
    pub fn on_exit_reset()
);

seam_core::seam!(
    /// `check_on_shmem_exit_lists_are_empty()` (`storage/ipc/ipc.c`) — assert
    /// that no `on_shmem_exit` handlers have been registered yet (the
    /// startup-packet safety check). `ereport(FATAL)` on violation, carried on
    /// `Err` so the caller propagates the process-terminating failure.
    pub fn check_on_shmem_exit_lists_are_empty() -> types_error::PgResult<()>
);
