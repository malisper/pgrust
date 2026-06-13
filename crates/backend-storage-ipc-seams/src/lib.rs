//! Seam declarations for `storage/ipc/ipc.c`, installed by
//! `backend-storage-ipc-dsm-core` (which owns the ipc.c port) from its
//! `init_seams()`.

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
