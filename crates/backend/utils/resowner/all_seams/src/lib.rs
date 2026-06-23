//! Seam declarations for the `backend-utils-resowner-all` unit
//! (`utils/resowner/resowner.c`).
//!
//! Only the auxiliary-process resource-owner teardown the archiver's
//! error-recovery path needs. The owning unit installs this from its
//! `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `ReleaseAuxProcessResources(isCommit)` (`utils/resowner/resowner.c`) —
    /// release everything held by `AuxProcessResourceOwner` during an aux
    /// process's error recovery. Infallible.
    pub fn release_aux_process_resources(is_commit: bool)
);
