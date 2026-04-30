#![allow(dead_code)]

mod planner;
mod subselect;

use crate::backend::parser::CatalogLookup;
use crate::include::nodes::parsenodes::Query;
use crate::include::nodes::pathnodes::{PathTarget, PlannerConfig, PlannerInfo, RelOptInfo};
use crate::include::nodes::plannodes::{Plan, PlannedStmt};
use crate::include::nodes::primnodes::Expr;

pub(crate) fn planner(
    query: Query,
    catalog: &dyn CatalogLookup,
) -> Result<PlannedStmt, crate::backend::parser::ParseError> {
    planner::planner(query, catalog)
}

pub(crate) fn planner_with_config(
    query: Query,
    catalog: &dyn CatalogLookup,
    config: PlannerConfig,
) -> Result<PlannedStmt, crate::backend::parser::ParseError> {
    planner::planner_with_config(query, catalog, config)
}

pub(crate) fn planner_with_param_base(
    query: Query,
    catalog: &dyn CatalogLookup,
    next_param_id: usize,
) -> Result<(PlannedStmt, usize), crate::backend::parser::ParseError> {
    planner::planner_with_param_base(query, catalog, next_param_id)
}

pub(crate) fn planner_with_param_base_and_config(
    query: Query,
    catalog: &dyn CatalogLookup,
    next_param_id: usize,
    config: PlannerConfig,
) -> Result<(PlannedStmt, usize), crate::backend::parser::ParseError> {
    planner::planner_with_param_base_and_config(query, catalog, next_param_id, config)
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

pub(super) fn append_planned_subquery(
    planned_stmt: PlannedStmt,
    subplans: &mut Vec<Plan>,
) -> usize {
    subselect::append_planned_subquery(planned_stmt, subplans)
}

pub(super) fn append_uncorrelated_planned_subquery(
    planned_stmt: PlannedStmt,
    subplans: &mut Vec<Plan>,
) -> usize {
    subselect::append_uncorrelated_planned_subquery(planned_stmt, subplans)
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
