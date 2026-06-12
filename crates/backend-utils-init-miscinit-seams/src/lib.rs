//! Seam declarations for the `backend-utils-init-miscinit` unit
//! (`utils/init/miscinit.c`), which owns the `Mode` processing-mode
//! backend-global.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `IsBootstrapProcessingMode()` (miscadmin.h): `Mode ==
    /// BootstrapProcessing`. A plain global read — infallible.
    pub fn is_bootstrap_processing_mode() -> bool
);
