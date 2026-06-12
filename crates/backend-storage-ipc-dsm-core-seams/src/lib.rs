//! Seam declarations for the `backend-storage-ipc-dsm-core` unit
//! (`src/backend/storage/ipc/dsm.c`). The owning unit installs these from its
//! `init_seams()`; until then a call panics loudly.

seam_core::seam!(
    /// `dsm_detach_all()` (`dsm.c`): detach every dynamic shared memory
    /// segment, including the control segment.
    pub fn dsm_detach_all()
);
