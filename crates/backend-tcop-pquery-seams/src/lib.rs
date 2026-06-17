//! Seam declarations for the `backend-tcop-pquery` unit (`tcop/pquery.c`):
//! the portal-execution operations portalcmds (cursor commands) calls.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_error::PgResult;
use types_nodes::copy_query::Query;
use types_nodes::parsestmt::DestReceiverHandle;
use types_nodes::portalcmds::ParamListInfo;
use types_portal::{FetchDirection, Portal, PortalStrategy};
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
    /// processed. `dest` is the router-keyed [`DestReceiverHandle`]. Runs the
    /// executor; can `ereport(ERROR)`.
    pub fn portal_run_fetch(
        portal: &Portal,
        fdirection: FetchDirection,
        count: i64,
        dest: DestReceiverHandle,
    ) -> PgResult<u64>
);

seam_core::seam!(
    /// `ChoosePortalStrategy(stmt_list)` (pquery.c:210) over the OWNED `Query`
    /// value tree — the leg `plancache.c` uses (its cached `stmt_list` is a list
    /// of `Query` nodes, not `PlannedStmt`s). Selects the [`PortalStrategy`] for
    /// the querytree list. `Err` carries the `UtilityReturnsTuples` lookup
    /// surface. This is the VALUE counterpart of the handle-based
    /// `backend_tcop_pquery_pc_seams::choose_portal_strategy` that plancache's F0
    /// de-handle will switch to.
    pub fn choose_portal_strategy_queries(stmts: &[Query<'_>]) -> PgResult<PortalStrategy>
);
