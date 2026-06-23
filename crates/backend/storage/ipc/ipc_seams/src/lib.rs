//! Seam declarations for the `backend-storage-ipc-ipc` unit
//! (`storage/ipc/ipc.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `proc_exit(code)` (ipc.c): run the on-exit callbacks and terminate the
    /// process. Does not return.
    pub fn proc_exit(code: i32) -> !
);
