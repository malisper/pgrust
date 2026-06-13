//! Seam declarations for the `backend-tcop-pquery` unit (`tcop/pquery.c`):
//! the portal-execution operations portalcmds (cursor commands) calls.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_error::PgResult;
use types_nodes::portalcmds::ParamListInfo;
use types_portal::{DestReceiver, FetchDirection, Portal};
use types_snapshot::SnapshotData;

seam_core::seam!(
    /// `PortalStart(portal, params, eflags, snapshot)` (pquery.c) — set up a
    /// portal for execution (chooses the portal strategy, runs the executor's
    /// `ExecutorStart`). `snapshot` is `GetActiveSnapshot()` (may be the C
    /// NULL). Can `ereport(ERROR)`.
    pub fn portal_start(
        portal: &Portal,
        params: ParamListInfo,
        eflags: i32,
        snapshot: Option<std::rc::Rc<SnapshotData>>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `PortalRunFetch(portal, fdirection, count, dest)` (pquery.c) — run a
    /// `FETCH`/`MOVE` against the portal, returning the number of rows
    /// processed. Runs the executor; can `ereport(ERROR)`.
    pub fn portal_run_fetch(
        portal: &Portal,
        fdirection: FetchDirection,
        count: i64,
        dest: DestReceiver,
    ) -> PgResult<u64>
);
