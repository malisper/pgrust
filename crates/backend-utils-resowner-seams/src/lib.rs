//! Seam declarations for `utils/resowner/resowner.c` functions portalmem calls
//! to manage a portal's `ResourceOwner`. Resource owners dissolve into RAII
//! owner values (docs/query-lifecycle-raii.md); until that owner lands portalmem
//! threads the C `ResourceOwner` pointer as the shared [`ResourceOwner`] handle
//! (`types_portal`, the same value `portal->resowner` holds). Calls panic
//! loudly until the owner installs them.

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
