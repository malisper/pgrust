//! Install every seam declared in `backend-utils-mmgr-portalmem-seams` to this
//! crate's real functions. Called once from `seams-init`'s `init_all()`.
//!
//! The inward seams xact (the cyclic caller) invokes drop the parent
//! `ResourceOwner` arguments — resource owners dissolve into RAII owner values
//! (docs/query-lifecycle-raii.md), so the parent-owner reparenting in
//! `AtSubCommit`/`AtSubAbort` threads `ResourceOwnerHandle::NULL` for the absent
//! owners; the reparent seam is a no-op until the resowner side lands.

use types_core::SubTransactionId;
use types_error::PgResult;
use types_portal::ResourceOwnerHandle;

use backend_utils_mmgr_portalmem_seams as seams;

pub fn init_seams() {
    seams::pre_commit_portals::set(crate::PreCommit_Portals);
    seams::at_abort_portals::set(crate::AtAbort_Portals);
    seams::at_cleanup_portals::set(crate::AtCleanup_Portals);
    seams::at_subcommit_portals::set(at_subcommit_portals);
    seams::at_subabort_portals::set(at_subabort_portals);
    seams::at_subcleanup_portals::set(crate::AtSubCleanup_Portals);
}

/// `AtSubCommit_Portals(mySubid, parentSubid, parentLevel, parentXactOwner)`
/// with the owner argument dissolved.
fn at_subcommit_portals(
    my_subid: SubTransactionId,
    parent_subid: SubTransactionId,
    parent_level: i32,
) -> PgResult<()> {
    crate::AtSubCommit_Portals(my_subid, parent_subid, parent_level, ResourceOwnerHandle::NULL)
}

/// `AtSubAbort_Portals(mySubid, parentSubid, myXactOwner, parentXactOwner)`
/// with the owner arguments dissolved.
fn at_subabort_portals(my_subid: SubTransactionId, parent_subid: SubTransactionId) -> PgResult<()> {
    crate::AtSubAbort_Portals(
        my_subid,
        parent_subid,
        ResourceOwnerHandle::NULL,
        ResourceOwnerHandle::NULL,
    )
}
