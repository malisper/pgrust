//! PREPARE/EXECUTE's consumer slice of the `backend-utils-mmgr-portalmem` unit
//! (`utils/mmgr/portalmem.c`).
//!
//! The base `backend-utils-mmgr-portalmem-seams` crate is portalcmds' slice
//! (it models the portal machinery against the real `types_portal::Portal`).
//! PREPARE/EXECUTE carries the live `Portal`/`MemoryContext` it creates and
//! runs as the parsestmt opaque handle newtypes (inherited opacity,
//! docs/types.md rule 6), so it gets its own slice here.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_error::PgResult;
use types_nodes::nodeindexscan::PlannedStmt;
use types_nodes::parsestmt::{CachedPlanHandle, CommandTag, MemoryContextHandle, PortalHandle};

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
