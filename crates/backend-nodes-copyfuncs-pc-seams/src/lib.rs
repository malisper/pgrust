//! plancache's slice of the node toolkit (`nodes/copyfuncs.c`,
//! `nodes/list.c`, `optimizer/plan/setrefs.c`/`plancat.c`'s
//! `extract_query_dependencies`, `optimizer/util/clauses.c`'s
//! `expression_planner_with_deps`). plancache copies querytree / plan / parse
//! trees through `copyObject`, walks lists, and extracts plan dependencies.
//! The owning node/optimizer units install these; until then a call panics.
//!
//! The querytree/plan/parse identities are the opaque tokens defined in
//! `types-plancache` (the storage is owned by those subsystems).

extern crate alloc;
use alloc::vec::Vec;

use types_core::primitive::Oid;
use types_error::PgResult;
use types_plancache::{
    AnalyzedQueryHandle, ExprHandle, InvalItemKey, PlannedStmtListHandle, QueryHandle,
    QueryListHandle, RawStmtHandle,
};

/// The three dependency out-params `extract_query_dependencies` writes.
///
/// The owned `Vec`s land in plancache's backend-lifetime thread-local source
/// state (mcx-design decision 5: backend-global cells use owned collections).
#[derive(Clone, Debug, Default)]
pub struct QueryDependencies {
    /// `relationOids`.
    pub relation_oids: Vec<Oid>,
    /// `invalItems` (`(cacheId, hashValue)` keys).
    pub inval_items: Vec<InvalItemKey>,
    /// `dependsOnRLS`.
    pub depends_on_rls: bool,
}

seam_core::seam!(
    /// `copyObject` of a querytree `List*` (allocated in the current context).
    pub fn copy_query_list(list: QueryListHandle) -> PgResult<QueryListHandle>
);

seam_core::seam!(
    /// `copyObject` of a plan `List*` (allocated in the current context).
    pub fn copy_plan_list(list: PlannedStmtListHandle) -> PgResult<PlannedStmtListHandle>
);

seam_core::seam!(
    /// `copyObject` of a raw parse tree (`RawStmt *`); NULL copies to NULL.
    pub fn copy_raw_stmt(raw: RawStmtHandle) -> PgResult<RawStmtHandle>
);

seam_core::seam!(
    /// `copyObject` of an analyzed query (`Query *`); NULL copies to NULL.
    pub fn copy_analyzed_query(q: AnalyzedQueryHandle) -> PgResult<AnalyzedQueryHandle>
);

seam_core::seam!(
    /// `copyObject` of an expression node (`Node *`).
    pub fn copy_expr(expr: ExprHandle) -> PgResult<ExprHandle>
);

seam_core::seam!(
    /// `list_length(query_list)`.
    pub fn query_list_length(list: QueryListHandle) -> PgResult<i32>
);

seam_core::seam!(
    /// The `Query *` elements of a querytree list, in order (`lfirst` walk).
    pub fn query_list_elements(list: QueryListHandle) -> PgResult<Vec<QueryHandle>>
);

seam_core::seam!(
    /// The `PlannedStmt *` elements of a plan list, in order.
    pub fn plan_list_elements(list: PlannedStmtListHandle) -> PgResult<Vec<types_plancache::PlannedStmtHandle>>
);

seam_core::seam!(
    /// `list_member_oid(relationOids, oid)` over a relation-OID `List`.
    pub fn list_member_oid(list: &[Oid], oid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `extract_query_dependencies((Node *) query_list, &relationOids,
    /// &invalItems, &dependsOnRLS)`.
    pub fn extract_query_dependencies(query_list: QueryListHandle) -> PgResult<QueryDependencies>
);

seam_core::seam!(
    /// `expression_planner_with_deps(expr, &relationOids, &invalItems)` —
    /// the planned expression plus its relation-OID and inval-item deps.
    pub fn expression_planner_with_deps(
        expr: ExprHandle,
    ) -> PgResult<(ExprHandle, Vec<Oid>, Vec<InvalItemKey>)>
);
