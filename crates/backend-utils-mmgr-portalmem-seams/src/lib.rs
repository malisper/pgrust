//! Seam declarations for the `backend-utils-mmgr-portalmem` unit
//! (`utils/mmgr/portalmem.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.
//!
//! C's `AtSubCommit_Portals` / `AtSubAbort_Portals` also receive the parent's
//! ResourceOwner; resource owners dissolve into RAII owner values here
//! (docs/query-lifecycle-raii.md), so those parameters drop out.

use types_core::SubTransactionId;
use types_error::PgResult;
use types_nodes::nodeindexscan::PlannedStmt;
use types_nodes::parsestmt::{
    CachedPlanHandle, CommandTag, MemoryContextHandle, PortalHandle,
};

seam_core::seam!(
    /// `CreateNewPortal()` (portalmem.c) — create an unnamed portal with a
    /// generated name. Allocates / can `ereport(ERROR)`.
    pub fn create_new_portal() -> PgResult<PortalHandle>
);

seam_core::seam!(
    /// `portal->visible = value`.
    pub fn portal_set_visible(portal: &PortalHandle, value: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `portal->portalContext`.
    pub fn portal_get_portal_context(portal: &PortalHandle) -> PgResult<MemoryContextHandle>
);

seam_core::seam!(
    /// `PortalDefineQuery(portal, NULL, query_string, commandTag, stmts,
    /// cplan)` (portalmem.c). `plan_list` is the cplan's statement list;
    /// `cplan`'s refcount transfers to the portal.
    pub fn portal_define_query<'mcx>(
        portal: &PortalHandle,
        query_string: &str,
        command_tag: CommandTag,
        plan_list: &[PlannedStmt<'mcx>],
        cplan: CachedPlanHandle,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `PortalDrop(portal, isTopCommit=false)` (portalmem.c).
    pub fn portal_drop(portal: &PortalHandle) -> PgResult<()>
);

seam_core::seam!(
    /// `PreCommit_Portals(isPrepare)` — close open portals before commit;
    /// returns true if it did anything (the caller loops). Runs user code:
    /// can `ereport(ERROR)`.
    pub fn pre_commit_portals(is_prepare: bool) -> PgResult<bool>
);

seam_core::seam!(
    /// `AtAbort_Portals()`.
    pub fn at_abort_portals() -> PgResult<()>
);

seam_core::seam!(
    /// `AtCleanup_Portals()` — now safe to release portal memory.
    pub fn at_cleanup_portals() -> PgResult<()>
);

seam_core::seam!(
    /// `AtSubCommit_Portals(mySubid, parentSubid, parentLevel, parentXactOwner)`
    /// (owner parameter dissolved).
    pub fn at_subcommit_portals(
        my_subid: SubTransactionId,
        parent_subid: SubTransactionId,
        parent_level: i32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `AtSubAbort_Portals(mySubid, parentSubid, myXactOwner, parentXactOwner)`
    /// (owner parameters dissolved).
    pub fn at_subabort_portals(
        my_subid: SubTransactionId,
        parent_subid: SubTransactionId,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `AtSubCleanup_Portals(mySubid)`.
    pub fn at_subcleanup_portals(my_subid: SubTransactionId) -> PgResult<()>
);
