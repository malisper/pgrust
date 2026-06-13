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
    /// callback to run inside `proc_exit`. Callbacks are
    /// `pg_on_exit_callback`s and may `ereport(ERROR/FATAL)` (the C longjmp
    /// surface), hence their `PgResult`. The registration `Err` is the
    /// `ereport(FATAL, "out_of_on_proc_exit_slots")` past `MAX_ON_EXITS`.
    pub fn on_proc_exit(
        function: fn(i32, types_datum::Datum) -> types_error::PgResult<()>,
        arg: types_datum::Datum,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `on_shmem_exit(function, arg)` (`storage/ipc/ipc.c`): register a
    /// callback to run while shared memory is still accessible during
    /// `shmem_exit`. Callbacks are `pg_on_exit_callback`s and may
    /// `ereport(ERROR/FATAL)`, hence their `PgResult`. The registration
    /// `Err` is the `ereport(FATAL, "out_of_on_shmem_exit_slots")` past
    /// `MAX_ON_EXITS`.
    pub fn on_shmem_exit(
        function: fn(i32, types_datum::Datum) -> types_error::PgResult<()>,
        arg: types_datum::Datum,
    ) -> types_error::PgResult<()>
);
