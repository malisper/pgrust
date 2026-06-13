//! plancache's slice of parse analysis (`parser/analyze.c`,
//! `parser/parse_node.c`) plus the `Query`-node field reads plancache performs
//! (`nodes/parsenodes.h`). The owning parser unit installs these; until then a
//! call panics loudly.

extern crate alloc;
use alloc::vec::Vec;

use types_error::PgResult;
use types_plancache::{
    AnalyzedQueryHandle, ParserSetupHandle, QueryEnvHandle, QueryHandle, QueryListHandle,
    RawStmtHandle, RteFields, TargetListHandle, UtilityStmtHandle,
};

seam_core::seam!(
    /// `stmt_requires_parse_analysis(raw_parse_tree)`.
    pub fn stmt_requires_parse_analysis(raw: RawStmtHandle) -> PgResult<bool>
);

seam_core::seam!(
    /// `analyze_requires_snapshot(raw_parse_tree)`.
    pub fn analyze_requires_snapshot(raw: RawStmtHandle) -> PgResult<bool>
);

seam_core::seam!(
    /// `query_requires_rewrite_plan(query)`.
    pub fn query_requires_rewrite_plan(q: AnalyzedQueryHandle) -> PgResult<bool>
);

seam_core::seam!(
    /// `pg_analyze_and_rewrite_withcb(rawtree, query_string, parserSetup,
    /// parserSetupArg, queryEnv)`.
    pub fn analyze_and_rewrite_withcb(
        rawtree: RawStmtHandle,
        query_string: &str,
        parser_setup: ParserSetupHandle,
        query_env: QueryEnvHandle,
    ) -> PgResult<QueryListHandle>
);

seam_core::seam!(
    /// `pg_analyze_and_rewrite_fixedparams(rawtree, query_string, param_types,
    /// num_params, queryEnv)`.
    pub fn analyze_and_rewrite_fixedparams(
        rawtree: RawStmtHandle,
        query_string: &str,
        param_types: &[types_core::primitive::Oid],
        query_env: QueryEnvHandle,
    ) -> PgResult<QueryListHandle>
);

/* ---- Query-node field reads ----------------------------------------------- */

seam_core::seam!(
    /// `query->commandType == CMD_UTILITY`.
    pub fn query_command_type_is_utility(q: QueryHandle) -> PgResult<bool>
);

seam_core::seam!(
    /// `query->canSetTag`.
    pub fn query_can_set_tag(q: QueryHandle) -> PgResult<bool>
);

seam_core::seam!(
    /// `query->utilityStmt`.
    pub fn query_utility_stmt(q: QueryHandle) -> PgResult<UtilityStmtHandle>
);

seam_core::seam!(
    /// `query->rtable` as `RteFields`, in order.
    pub fn query_rtable_fields(q: QueryHandle) -> PgResult<Vec<RteFields>>
);

seam_core::seam!(
    /// `query->rtable != NIL`.
    pub fn query_has_rtable(q: QueryHandle) -> PgResult<bool>
);

seam_core::seam!(
    /// `query->cteList` — the `castNode(Query, cte->ctequery)` of each CTE.
    pub fn query_cte_queries(q: QueryHandle) -> PgResult<Vec<QueryHandle>>
);

seam_core::seam!(
    /// `query->cteList != NIL`.
    pub fn query_has_cte_list(q: QueryHandle) -> PgResult<bool>
);

seam_core::seam!(
    /// `query->hasSubLinks`.
    pub fn query_has_sublinks(q: QueryHandle) -> PgResult<bool>
);

seam_core::seam!(
    /// `query->targetList`.
    pub fn query_target_list(q: QueryHandle) -> PgResult<TargetListHandle>
);

seam_core::seam!(
    /// `query->returningList`.
    pub fn query_returning_list(q: QueryHandle) -> PgResult<TargetListHandle>
);

seam_core::seam!(
    /// `query_tree_walker(query, ScanQueryWalker, &acquire,
    /// QTW_IGNORE_RC_SUBQUERIES)` — the `SubLink` subselect `Query*`s, in walk
    /// order, that `ScanQueryForLocks` must recurse into.
    pub fn walk_query_sublinks_for_locks(query: QueryHandle) -> PgResult<Vec<QueryHandle>>
);
