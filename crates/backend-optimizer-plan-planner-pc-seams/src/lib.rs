//! plancache's slice of the planner (`optimizer/plan/planner.c`,
//! `tcop/postgres.c`'s `pg_plan_queries`, `optimizer/path/costsize.c`) plus the
//! `PlannedStmt`-node field reads plancache performs (`nodes/plannodes.h`).
//! The owning planner unit installs these; until then a call panics loudly.

extern crate alloc;
use alloc::vec::Vec;

use types_core::primitive::Oid;
use types_error::PgResult;
use types_plancache::{
    InvalItemKey, ParamListInfoHandle, PlannedStmtHandle, PlannedStmtListHandle, QueryListHandle,
    RteFields,
};

seam_core::seam!(
    /// `pg_plan_queries(querytree_list, query_string, cursor_options, boundParams)`.
    pub fn plan_queries(
        querytree_list: QueryListHandle,
        query_string: &str,
        cursor_options: i32,
        bound_params: ParamListInfoHandle,
    ) -> PgResult<PlannedStmtListHandle>
);

seam_core::seam!(
    /// `cpu_operator_cost` (the planner GUC).
    pub fn cpu_operator_cost() -> PgResult<f64>
);

/* ---- PlannedStmt-node field reads ----------------------------------------- */

seam_core::seam!(
    /// `plannedstmt->commandType == CMD_UTILITY`.
    pub fn pstmt_command_type_is_utility(stmt: PlannedStmtHandle) -> PgResult<bool>
);

seam_core::seam!(
    /// `plannedstmt->transientPlan`.
    pub fn pstmt_transient_plan(stmt: PlannedStmtHandle) -> PgResult<bool>
);

seam_core::seam!(
    /// `plannedstmt->dependsOnRole`.
    pub fn pstmt_depends_on_role(stmt: PlannedStmtHandle) -> PgResult<bool>
);

seam_core::seam!(
    /// `plannedstmt->planTree->total_cost`.
    pub fn pstmt_plantree_total_cost(stmt: PlannedStmtHandle) -> PgResult<f64>
);

seam_core::seam!(
    /// `list_length(plannedstmt->rtable)`.
    pub fn pstmt_rtable_length(stmt: PlannedStmtHandle) -> PgResult<i32>
);

seam_core::seam!(
    /// `plannedstmt->rtable` as `RteFields`, in order.
    pub fn pstmt_rtable_fields(stmt: PlannedStmtHandle) -> PgResult<Vec<RteFields>>
);

seam_core::seam!(
    /// `plannedstmt->relationOids`.
    pub fn pstmt_relation_oids(stmt: PlannedStmtHandle) -> PgResult<Vec<Oid>>
);

seam_core::seam!(
    /// `plannedstmt->invalItems` as `(cacheId, hashValue)` keys.
    pub fn pstmt_inval_items(stmt: PlannedStmtHandle) -> PgResult<Vec<InvalItemKey>>
);

seam_core::seam!(
    /// `plannedstmt->utilityStmt` (a `Node *`; NULL if none).
    pub fn pstmt_utility_stmt(stmt: PlannedStmtHandle) -> PgResult<types_plancache::UtilityStmtHandle>
);
