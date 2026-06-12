//! Seam declarations for `storage/ipc/ipc.c`, installed by
//! `backend-storage-ipc-dsm-core` (which owns the ipc.c port) from its
//! `init_seams()`.

seam_core::seam!(
    /// `proc_exit(code)` (`storage/ipc/ipc.c`) — run the on_proc_exit
    /// callbacks and terminate the process. Never returns.
    pub fn proc_exit(code: i32) -> !
);
