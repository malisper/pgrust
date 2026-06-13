//! Seam declarations for `utils/resowner/resowner.c` functions.
//!
//! Resource owners dissolve into RAII owner values (docs/query-lifecycle-raii.md);
//! until that owner lands, callers thread the C `ResourceOwner` pointer as the
//! shared [`ResourceOwner`] handle (`types_portal`, the same value
//! `portal->resowner` holds). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_portal::{ResourceOwner, ResourceReleasePhase};

seam_core::seam!(
    /// `ResourceOwnerCreate(parent, name)` for a portal — returns the new
    /// owner. portalmem always passes `CurrentResourceOwner` as the parent and
    /// the name "Portal", so the seam takes no arguments.
    pub fn resource_owner_create_portal() -> ResourceOwner
);

seam_core::seam!(
    /// `ResourceOwnerRelease(owner, phase, isCommit, isTopLevel)`.
    pub fn resource_owner_release(
        owner: ResourceOwner,
        phase: ResourceReleasePhase,
        is_commit: bool,
        is_top_level: bool,
    )
);

seam_core::seam!(
    /// `ResourceOwnerDelete(owner)`.
    pub fn resource_owner_delete(owner: ResourceOwner)
);

seam_core::seam!(
    /// `ResourceOwnerNewParent(owner, newparent)`.
    pub fn resource_owner_new_parent(owner: ResourceOwner, new_parent: ResourceOwner)
);

seam_core::seam!(
    /// `ReleaseAuxProcessResources(isCommit)` (resowner.c) — release all
    /// resources held by `AuxProcessResourceOwner`. Called from auxiliary
    /// processes' error-recovery cleanup with `isCommit = false`. `Err`
    /// carries any `ereport` from a release callback.
    pub fn release_aux_process_resources(is_commit: bool) -> types_error::PgResult<()>
);
