mod allpaths;
mod costsize;

use crate::RelFileLocator;
use crate::backend::optimizer::{AccessCandidate, IndexPathSpec, RelationStats};
use crate::backend::parser::BoundIndexRelation;
use crate::backend::parser::CatalogLookup;
use crate::include::nodes::pathnodes::{Path, PlannerInfo, RelOptInfo};
use crate::include::nodes::plannodes::PlanEstimate;
use crate::include::nodes::primnodes::ToastRelationRef;
use crate::include::nodes::primnodes::{Expr, JoinType, OrderByEntry, QueryColumn, RelationDesc};

pub(super) fn query_planner(root: &mut PlannerInfo, catalog: &dyn CatalogLookup) -> RelOptInfo {
    allpaths::query_planner(root, catalog)
}

pub(super) fn make_one_rel(root: &mut PlannerInfo, catalog: &dyn CatalogLookup) -> RelOptInfo {
    allpaths::make_one_rel(root, catalog)
}

pub(super) fn residual_where_qual(root: &PlannerInfo) -> Option<Expr> {
    allpaths::residual_where_qual(root)
}

pub(super) fn optimize_path(plan: Path, catalog: &dyn CatalogLookup) -> Path {
    costsize::optimize_path(plan, catalog)
}

pub(super) fn rewrite_join_input_expr(
    root: Option<&PlannerInfo>,
    expr: Expr,
    path: &Path,
    layout: &[Expr],
) -> Expr {
    costsize::rewrite_join_input_expr(root, expr, path, layout)
}

pub(super) fn flatten_and_conjuncts(expr: &Expr) -> Vec<Expr> {
    costsize::flatten_and_conjuncts(expr)
}

pub(super) fn and_exprs(exprs: Vec<Expr>) -> Option<Expr> {
    costsize::and_exprs(exprs)
}

pub(super) fn estimate_sql_type_width(sql_type: crate::backend::parser::SqlType) -> usize {
    costsize::estimate_sql_type_width(sql_type)
}

pub(super) fn predicate_cost(expr: &Expr) -> f64 {
    costsize::predicate_cost(expr)
}

pub(super) fn clamp_rows(rows: f64) -> f64 {
    costsize::clamp_rows(rows)
}

pub(super) fn relation_stats(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    desc: &RelationDesc,
) -> RelationStats {
    costsize::relation_stats(catalog, relation_oid, desc)
}

pub(super) fn estimate_seqscan_candidate(
    source_id: usize,
    rel: RelFileLocator,
    relation_oid: u32,
    toast: Option<ToastRelationRef>,
    desc: RelationDesc,
    stats: &RelationStats,
    filter: Option<Expr>,
    order_items: Option<Vec<OrderByEntry>>,
) -> AccessCandidate {
    costsize::estimate_seqscan_candidate(
        source_id,
        rel,
        relation_oid,
        toast,
        desc,
        stats,
        filter,
        order_items,
    )
}

pub(super) fn estimate_index_candidate(
    source_id: usize,
    rel: RelFileLocator,
    toast: Option<ToastRelationRef>,
    desc: RelationDesc,
    stats: &RelationStats,
    spec: crate::backend::optimizer::IndexPathSpec,
    order_items: Option<Vec<OrderByEntry>>,
    catalog: &dyn CatalogLookup,
) -> AccessCandidate {
    costsize::estimate_index_candidate(
        source_id,
        rel,
        toast,
        desc,
        stats,
        spec,
        order_items,
        catalog,
    )
}

pub(super) fn build_join_paths_with_root(
    root: &PlannerInfo,
    left: Path,
    right: Path,
    left_relids: &[usize],
    right_relids: &[usize],
    kind: JoinType,
    on: Expr,
) -> Vec<Path> {
    costsize::build_join_paths_with_root(root, left, right, left_relids, right_relids, kind, on)
}

pub(super) fn build_join_paths(
    left: Path,
    right: Path,
    left_relids: &[usize],
    right_relids: &[usize],
    kind: JoinType,
    on: Expr,
) -> Vec<Path> {
    costsize::build_join_paths(left, right, left_relids, right_relids, kind, on)
}

pub(super) fn restore_join_output_order(
    join: Path,
    left_columns: &[QueryColumn],
    right_columns: &[QueryColumn],
    left_vars: &[Expr],
    right_vars: &[Expr],
) -> Path {
    costsize::restore_join_output_order(join, left_columns, right_columns, left_vars, right_vars)
}

pub(super) fn extract_hash_join_clauses(
    on: &Expr,
    left_relids: &[usize],
    right_relids: &[usize],
) -> Option<crate::backend::optimizer::HashJoinClauses> {
    costsize::extract_hash_join_clauses(on, left_relids, right_relids)
}

pub(super) fn rewrite_semantic_expr_for_join_inputs(
    root: Option<&PlannerInfo>,
    expr: Expr,
    left: &Path,
    right: &Path,
    join_layout: &[Expr],
) -> Expr {
    costsize::rewrite_semantic_expr_for_join_inputs(root, expr, left, right, join_layout)
}

pub(super) fn build_index_path_spec(
    filter: Option<&Expr>,
    order_items: Option<&[OrderByEntry]>,
    index: &BoundIndexRelation,
) -> Option<IndexPathSpec> {
    costsize::build_index_path_spec(filter, order_items, index)
}
