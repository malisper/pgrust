//! Subquery / CTE pathlist machinery — Query-keystone-blocked.
//!
//! `set_subquery_pathlist` (allpaths.c:2528) plans an `RTE_SUBQUERY` by running
//! `subquery_planner` over the `rte->subquery` and building SubqueryScan paths;
//! its pushdown-safety cluster (`subquery_is_pushdown_safe` /
//! `qual_is_pushdown_safe` / `remove_unused_subquery_outputs` /
//! `check_and_push_window_quals` / …) reads the `Query` subtrees (`targetList`,
//! `setOperations`, `windowClause`, `distinctClause`, …). `set_cte_pathlist`
//! (2906) and `set_worktable_pathlist` (3039) resolve a CTE by name out of
//! `parse->cteList`.
//!
//! `types_pathnodes` carries no `Query` *value* — only the opaque
//! [`types_pathnodes::QueryId`] handle and the scalar RTE projections in
//! `backend-optimizer-rte-seams` — because the real `Query<'mcx>` is owned by
//! the (unported) planner-entry crate that runs `subquery_planner`. So these
//! three functions cannot be ported faithfully here: they route through
//! planner-entry-owned seams (registered in seams-init's
//! `CONTRACT_RECONCILE_PENDING`) until that keystone lands. This is the honest
//! seam-and-panic-until-owner pattern (never a silent stub).

use types_core::primitive::Index;
use types_error::PgResult;
use types_pathnodes::{PlannerInfo, RelId};

/// `set_subquery_pathlist` (allpaths.c:2528) — SubqueryScan access paths for a
/// subquery RTE. Routes to the planner-entry owner (it runs `subquery_planner`
/// over the owned `Query` subtree and applies the pushdown-safety cluster).
pub fn set_subquery_pathlist(root: &mut PlannerInfo, rel: RelId, rti: Index) -> PgResult<()> {
    seams::set_subquery_pathlist::call(root, rel, rti)
}

/// `set_cte_pathlist` (allpaths.c:2906) — the access path for a non-self-ref CTE
/// RTE. Resolves the CTE by name out of `cteroot->parse->cteList` (a `Query`
/// subtree); routed to the planner-entry owner.
pub fn set_cte_pathlist(root: &mut PlannerInfo, rel: RelId, rti: Index) -> PgResult<()> {
    seams::set_cte_pathlist::call(root, rel, rti)
}

/// `set_worktable_pathlist` (allpaths.c:3039) — the access path for a
/// self-reference (recursive) CTE RTE. Reads `cteroot->non_recursive_path` after
/// resolving the CTE by name; routed to the planner-entry owner.
pub fn set_worktable_pathlist(root: &mut PlannerInfo, rel: RelId, rti: Index) -> PgResult<()> {
    seams::set_worktable_pathlist::call(root, rel, rti)
}

/// Planner-entry-owned seams for the subquery/CTE vertical (Query-value
/// keystone). Installed by the planner-entry crate (`subquery_planner` /
/// `planner.c`) once it lands; registered in `CONTRACT_RECONCILE_PENDING`
/// meanwhile.
pub mod seams {
    use types_core::primitive::Index;
    use types_error::PgResult;
    use types_pathnodes::{PlannerInfo, RelId};

    seam_core::seam!(
        /// `set_subquery_pathlist(root, rel, rti, rte)` (allpaths.c) — runs
        /// `subquery_planner` over `rte->subquery` and builds SubqueryScan paths.
        pub fn set_subquery_pathlist(root: &mut PlannerInfo, rel: RelId, rti: Index) -> PgResult<()>
    );
    seam_core::seam!(
        /// `set_cte_pathlist(root, rel, rti, rte)` (allpaths.c).
        pub fn set_cte_pathlist(root: &mut PlannerInfo, rel: RelId, rti: Index) -> PgResult<()>
    );
    seam_core::seam!(
        /// `set_worktable_pathlist(root, rel, rti, rte)` (allpaths.c).
        pub fn set_worktable_pathlist(root: &mut PlannerInfo, rel: RelId, rti: Index) -> PgResult<()>
    );
}
