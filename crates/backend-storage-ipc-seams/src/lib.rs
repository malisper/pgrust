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
    /// `on_proc_exit(function, arg)` (`storage/ipc/ipc.c`) — register a
    /// callback to run inside `proc_exit`. `elog(FATAL)`s past
    /// `MAX_ON_EXITS`; that path diverges through the FATAL exit, so the
    /// registration surface stays bare.
    pub fn on_proc_exit(function: fn(i32, types_datum::Datum), arg: types_datum::Datum)
);
