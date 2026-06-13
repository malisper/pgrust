//! PREPARE/EXECUTE's consumer slice of the portal-execution surface of
//! `tcop/pquery.c` (`PortalStart`/`PortalRun`).
//!
//! The base `backend-tcop-pquery-seams` crate is portalcmds' slice (it models
//! the cursor `FETCH`/`MOVE` surface against the real `types_portal` types).
//! PREPARE/EXECUTE carries the live `Portal`/`ParamListInfo`/`DestReceiver`/
//! `QueryCompletion` values as the parsestmt opaque handle newtypes
//! (inherited opacity, docs/types.md rule 6), so it gets its own slice here.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_error::PgResult;
use types_nodes::parsestmt::{
    DestReceiverHandle, ParamListInfoHandle, PortalHandle, QueryCompletionHandle,
};
use types_execparallel::SnapshotHandle;

seam_core::seam!(
    /// `PortalStart(portal, params, eflags, snapshot)` (pquery.c). `snapshot`
    /// is `GetActiveSnapshot()` (the C NULL for none is `None`). Can
    /// `ereport(ERROR)`.
    pub fn portal_start(
        portal: &PortalHandle,
        params: ParamListInfoHandle,
        eflags: i32,
        snapshot: Option<SnapshotHandle>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `PortalRun(portal, count, isTopLevel=false, dest, altdest=dest, qc)`
    /// (pquery.c). Runs the query; can `ereport(ERROR)`.
    pub fn portal_run(
        portal: &PortalHandle,
        count: i64,
        dest: DestReceiverHandle,
        qc: QueryCompletionHandle,
    ) -> PgResult<()>
);
