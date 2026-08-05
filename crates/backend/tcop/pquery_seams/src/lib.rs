use mcx::{Mcx, PgVec};
use types_core::{AttrNumber, Oid};
use types_error::PgResult;
use types_nodes::Node;
use types_portal::{PortalData, StmtListHandle};

// A TargetEntry projected to the fields row-description senders read.
#[derive(Clone, Copy, Debug, Default)]
pub struct TargetEntrySummary {
    pub resjunk: bool,
    pub resorigtbl: Oid,
    pub resorigcol: AttrNumber,
}

seam_core::seam!(
    // FetchPortalTargetList (pquery.c); NIL comes back as an empty vec.
    pub fn fetch_portal_target_list<'a, 'mcx>(
        mcx: Mcx<'mcx>,
        portal: &'a PortalData<'a>,
    ) -> PgResult<PgVec<'mcx, TargetEntrySummary>>
);

seam_core::seam!(
    // stmt_list::free — PortalDrop releases the portal's registry handle
    // (idempotent; C's portal->stmts dies with the portal contexts).
    pub fn stmt_list_free(h: StmtListHandle)
);

seam_core::seam!(
    // EnsurePortalSnapshotExists (pquery.c).
    pub fn ensure_portal_snapshot_exists() -> PgResult<()>
);

seam_core::seam!(
    // FetchStatementTargetList (pquery.c) utilityStmt tail, called from
    // plancache::CachedPlanGetTargetList: FETCH recurses into the referenced
    // portal, EXECUTE recurses into the named prepared statement's plan.
    pub fn fetch_utility_statement_target_list<'mcx>(
        mcx: Mcx<'mcx>,
        utility_stmt: Option<Node<'mcx>>,
    ) -> PgResult<PgVec<'mcx, TargetEntrySummary>>
);
