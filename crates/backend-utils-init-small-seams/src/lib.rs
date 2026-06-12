//! Seam declarations for the `backend-utils-init-small` unit
//! (`utils/init/globals.c`, `utils/init/usercontext.c`): backend-global
//! variable reads.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `work_mem` (globals.c): the `work_mem` GUC — per-operation memory
    /// budget in kilobytes.
    pub fn work_mem() -> i32
);

seam_core::seam!(
    /// `NBuffers` (globals.c): the `shared_buffers` GUC — number of shared
    /// buffers. Pure read of backend-local state.
    pub fn nbuffers() -> i32
);
