//! Seam declarations for `utils/resowner/resowner.c`, the aux-process resource
//! owner used by the standalone/bootstrap XLOG startup path.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_error::PgResult;

seam_core::seam!(
    /// `CreateAuxProcessResourceOwner()` (resowner.c): create the aux-process
    /// resource owner and set `CurrentResourceOwner` to it, registering a
    /// cleanup callback. `Err` carries its `ereport` surface.
    pub fn create_aux_process_resource_owner() -> PgResult<()>
);

seam_core::seam!(
    /// `ReleaseAuxProcessResources(isCommit)` (resowner.c): release everything
    /// held by the aux-process resource owner (warning about leaked buffer
    /// pins). `Err` carries its `ereport` surface.
    pub fn release_aux_process_resources(is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `CurrentResourceOwner = NULL` (resowner.c global): reset the current
    /// resource owner to nothing.
    pub fn reset_current_resource_owner()
);
