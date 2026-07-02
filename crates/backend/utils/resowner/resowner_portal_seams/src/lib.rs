// Portal-facing resowner.c slice (cf. resowner_seams, the xact-facing slice);
// the portal's owner crosses as the types_resowner handle.

use types_resowner::{ResourceOwner, ResourceReleasePhase};

seam_core::seam!(
    // ResourceOwnerCreate(CurTransactionResourceOwner, "Portal") (resowner.c);
    // the owner reads its own CurTransactionResourceOwner global.
    pub fn resource_owner_create_portal() -> ResourceOwner
);

seam_core::seam!(
    pub fn resource_owner_release(
        owner: ResourceOwner,
        phase: ResourceReleasePhase,
        is_commit: bool,
        is_top_level: bool,
    )
);

seam_core::seam!(
    pub fn resource_owner_delete(owner: ResourceOwner)
);

seam_core::seam!(
    pub fn resource_owner_new_parent(owner: ResourceOwner, new_parent: ResourceOwner)
);
