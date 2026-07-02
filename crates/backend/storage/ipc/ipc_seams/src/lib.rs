seam_core::seam!(
    pub fn proc_exit(code: i32, my_pid: i32) -> !
);

seam_core::seam!(
    // `before_shmem_exit(function, arg)` (storage/ipc/ipc.c).
    pub fn before_shmem_exit(
        function: fn(code: i32, arg: datum::Datum) -> types_error::PgResult<()>,
        arg: datum::Datum,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    pub fn on_shmem_exit(callback: fn(code: i32, arg: usize), arg: usize)
);

seam_core::seam!(
    // `on_proc_exit(function, arg)` (storage/ipc/ipc.c).
    pub fn on_proc_exit(callback: fn(code: i32, arg: usize), arg: usize)
);

seam_core::seam!(
    // check_on_shmem_exit_lists_are_empty (ipc.c): FATAL if any registered.
    pub fn check_on_shmem_exit_lists_are_empty() -> types_error::PgResult<()>
);
