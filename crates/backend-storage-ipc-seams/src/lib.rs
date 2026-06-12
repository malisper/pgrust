//! Seam declarations for the `backend-storage-ipc` unit (`storage/ipc/ipc.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `proc_exit(code)` (`storage/ipc/ipc.c`) — run the on_proc_exit
    /// callbacks and terminate the process. Never returns.
    pub fn proc_exit(code: i32) -> !
);
