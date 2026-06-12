//! Seam declarations for `storage/ipc/ipc.c`, installed by
//! `backend-storage-ipc-dsm-core` (which owns the ipc.c port) from its
//! `init_seams()`.

seam_core::seam!(
    /// `proc_exit(code)` (`storage/ipc/ipc.c`) — run the on_proc_exit
    /// callbacks and terminate the process. Never returns.
    pub fn proc_exit(code: i32) -> !
);

seam_core::seam!(
    /// `on_proc_exit(function, arg)` (`storage/ipc/ipc.c`) — register a
    /// callback to run inside `proc_exit`. `elog(FATAL)`s past
    /// `MAX_ON_EXITS`; that path diverges through the FATAL exit, so the
    /// registration surface stays bare.
    pub fn on_proc_exit(function: fn(i32, types_datum::Datum), arg: types_datum::Datum)
);

seam_core::seam!(
    /// `on_shmem_exit(function, arg)` (`storage/ipc/ipc.c`): register a
    /// callback to run while shared memory is still accessible during
    /// `shmem_exit`. The C `ereport(FATAL, "out_of_on_shmem_exit_slots")`
    /// overflow path exits the process rather than returning, so the
    /// signature is infallible.
    pub fn on_shmem_exit(callback: fn(code: i32, arg: types_datum::Datum), arg: types_datum::Datum)
);
