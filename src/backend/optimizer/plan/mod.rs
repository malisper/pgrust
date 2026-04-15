mod planner;
mod subselect;

use crate::backend::parser::CatalogLookup;
use crate::include::nodes::parsenodes::Query;
use crate::include::nodes::pathnodes::{PathTarget, PlannerInfo, RelOptInfo};
use crate::include::nodes::plannodes::{Plan, PlannedStmt};
use crate::include::nodes::primnodes::{Expr, TargetEntry};

pub(crate) fn planner(query: Query, catalog: &dyn CatalogLookup) -> PlannedStmt {
    planner::planner(query, catalog)
}

pub(super) fn finalize_expr_subqueries(
    expr: Expr,
    catalog: &dyn CatalogLookup,
    subplans: &mut Vec<Plan>,
) -> Expr {
    subselect::finalize_expr_subqueries(expr, catalog, subplans)
}

pub(super) fn finalize_plan_subqueries(
    plan: Plan,
    catalog: &dyn CatalogLookup,
    subplans: &mut Vec<Plan>,
) -> Plan {
    subselect::finalize_plan_subqueries(plan, catalog, subplans)
}

pub(super) fn grouping_planner(
    root: &mut PlannerInfo,
    scanjoin_rel: RelOptInfo,
    catalog: &dyn CatalogLookup,
) -> RelOptInfo {
    planner::grouping_planner(root, scanjoin_rel, catalog)
}

pub(super) fn make_pathtarget_projection_rel(
    root: &PlannerInfo,
    input_rel: RelOptInfo,
    reltarget: &PathTarget,
    catalog: &dyn CatalogLookup,
    allow_identity_elision: bool,
) -> RelOptInfo {
    planner::make_pathtarget_projection_rel(
        root,
        input_rel,
        reltarget,
        catalog,
        allow_identity_elision,
    )
}
