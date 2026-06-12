//! Seam declarations for the `backend-storage-buffer-bufmgr` unit
//! (`storage/buffer/bufmgr.c`): reads of its GUC-assigned globals.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `effective_io_concurrency` (bufmgr.c): the GUC of the same name.
    pub fn effective_io_concurrency() -> i32
);

seam_core::seam!(
    /// `maintenance_io_concurrency` (bufmgr.c): the GUC of the same name.
    pub fn maintenance_io_concurrency() -> i32
);
