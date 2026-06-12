//! Seam declarations for the `backend-utils-init-miscinit` unit
//! (`utils/init/miscinit.c`: backend identity and processing-mode reads).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;

seam_core::seam!(
    /// `GetUserId()` (miscinit.c): the current effective user id. Pure
    /// global read (asserts validity in C); cannot `ereport`.
    pub fn get_user_id() -> Oid
);

seam_core::seam!(
    /// `IsBootstrapProcessingMode()` (miscadmin.h macro over miscinit.c's
    /// `Mode`): true during initdb bootstrap. Pure global read.
    pub fn is_bootstrap_processing_mode() -> bool
);
