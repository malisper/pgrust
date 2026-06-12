//! Seam declarations for the `backend-storage-ipc` unit (`storage/ipc/ipc.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `proc_exit(code)` (`storage/ipc/ipc.c`) — run the on_proc_exit
    /// callbacks and terminate the process. Never returns.
    pub fn proc_exit(code: i32) -> !
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
