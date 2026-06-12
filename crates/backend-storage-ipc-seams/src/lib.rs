//! Seam declarations for the `storage/ipc/ipc.c` unit.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `proc_exit(int code)` (ipc.c): run all on_proc_exit callbacks and exit
    /// the process. Never returns.
    pub fn proc_exit(code: i32) -> !
);
