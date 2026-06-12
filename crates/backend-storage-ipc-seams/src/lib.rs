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
    /// `on_shmem_exit(function, arg)` (`storage/ipc/ipc.c`) — register a
    /// callback to run at shared-memory detach time, latest-registered
    /// first. Slot exhaustion is `ereport(FATAL)` (process exit, not an
    /// `Err`), so the signature is infallible.
    pub fn on_shmem_exit(function: fn(i32, types_datum::Datum), arg: types_datum::Datum)
);
