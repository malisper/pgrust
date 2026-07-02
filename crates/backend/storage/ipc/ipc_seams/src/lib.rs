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
