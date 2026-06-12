//! Seam declarations for the `backend-utils-init-miscinit` unit
//! (`src/backend/utils/init/miscinit.c`). The owning unit installs these from
//! its `init_seams()`; until then a call panics loudly.

seam_core::seam!(
    /// `InitPostmasterChild()` (`miscinit.c`): initialization common to all
    /// postmaster children — detangle the child from the postmaster (signal
    /// handling, process group, postmaster-death watch, etc.).
    pub fn init_postmaster_child()
);
