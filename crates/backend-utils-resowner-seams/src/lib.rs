//! Seam declarations for `utils/resowner/resowner.c` functions portalmem calls
//! to manage a portal's `ResourceOwner`. Resource owners dissolve into RAII
//! owner values (docs/query-lifecycle-raii.md); until that owner lands portalmem
//! threads the C `ResourceOwner` pointer as a [`ResourceOwnerHandle`] token.
//! Calls panic loudly until the owner installs them.

use types_portal::{ResourceOwnerHandle, ResourceReleasePhase};

seam_core::seam!(
    /// `ResourceOwnerCreate(parent, name)` for a portal — returns the new
    /// owner. portalmem always passes `CurrentResourceOwner` as the parent and
    /// the name "Portal", so the seam takes no arguments.
    pub fn resource_owner_create_portal() -> ResourceOwnerHandle
);

seam_core::seam!(
    /// `ResourceOwnerRelease(owner, phase, isCommit, isTopLevel)`.
    pub fn resource_owner_release(
        owner: ResourceOwnerHandle,
        phase: ResourceReleasePhase,
        is_commit: bool,
        is_top_level: bool,
    )
);

seam_core::seam!(
    /// `ResourceOwnerDelete(owner)`.
    pub fn resource_owner_delete(owner: ResourceOwnerHandle)
);

seam_core::seam!(
    /// `ResourceOwnerNewParent(owner, newparent)`.
    pub fn resource_owner_new_parent(owner: ResourceOwnerHandle, new_parent: ResourceOwnerHandle)
);
