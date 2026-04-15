mod tlist;

use crate::backend::parser::CatalogLookup;
use crate::include::nodes::pathnodes::{Path, PathKey, PathTarget, PlannerInfo, RelOptInfo};
use crate::include::nodes::primnodes::{AggAccum, Expr, QueryColumn, RelationDesc, TargetEntry};

pub(super) fn build_aggregate_output_columns(
    group_by: &[Expr],
    accumulators: &[AggAccum],
) -> Vec<QueryColumn> {
    tlist::build_aggregate_output_columns(group_by, accumulators)
}

pub(super) fn project_to_slot_layout(
    slot_id: usize,
    desc: &RelationDesc,
    input: Path,
    target_exprs: Vec<Expr>,
    catalog: &dyn CatalogLookup,
) -> Path {
    tlist::project_to_slot_layout(slot_id, desc, input, target_exprs, catalog)
}

pub(super) fn project_to_slot_layout_internal(
    root: Option<&PlannerInfo>,
    slot_id: usize,
    desc: &RelationDesc,
    input: Path,
    target_exprs: Vec<Expr>,
    catalog: &dyn CatalogLookup,
) -> Path {
    tlist::project_to_slot_layout_internal(root, slot_id, desc, input, target_exprs, catalog)
}

pub(super) fn normalize_rte_path(
    rtindex: usize,
    desc: &RelationDesc,
    input: Path,
    catalog: &dyn CatalogLookup,
) -> Path {
    tlist::normalize_rte_path(rtindex, desc, input, catalog)
}

pub(super) fn lower_targets_for_path(
    root: &PlannerInfo,
    path: &Path,
    targets: &[TargetEntry],
) -> Vec<TargetEntry> {
    tlist::lower_targets_for_path(root, path, targets)
}

pub(super) fn lower_pathkeys_for_path(
    root: &PlannerInfo,
    path: &Path,
    pathkeys: &[PathKey],
) -> Vec<PathKey> {
    tlist::lower_pathkeys_for_path(root, path, pathkeys)
}

pub(super) fn lower_pathkeys_for_rel(
    root: &PlannerInfo,
    rel: &RelOptInfo,
    pathkeys: &[PathKey],
) -> Vec<PathKey> {
    tlist::lower_pathkeys_for_rel(root, rel, pathkeys)
}

pub(super) fn pathkeys_to_order_items(
    pathkeys: &[PathKey],
) -> Vec<crate::include::nodes::primnodes::OrderByEntry> {
    tlist::pathkeys_to_order_items(pathkeys)
}

pub(super) fn projection_is_identity(path: &Path, targets: &[TargetEntry]) -> bool {
    tlist::projection_is_identity(path, targets)
}

pub(super) fn rewrite_semantic_expr_for_path(expr: Expr, path: &Path, layout: &[Expr]) -> Expr {
    tlist::rewrite_semantic_expr_for_path(expr, path, layout)
}

pub(super) fn rewrite_expr_for_path(expr: Expr, path: &Path, layout: &[Expr]) -> Expr {
    tlist::rewrite_expr_for_path(expr, path, layout)
}

pub(super) fn rewrite_semantic_expr_for_path_or_expand_join_vars(
    root: &PlannerInfo,
    expr: Expr,
    path: &Path,
    layout: &[Expr],
) -> Expr {
    tlist::rewrite_semantic_expr_for_path_or_expand_join_vars(root, expr, path, layout)
}

pub(super) fn layout_candidate_for_expr(
    root: &PlannerInfo,
    expr: &Expr,
    layout: &[Expr],
) -> Option<Expr> {
    tlist::layout_candidate_for_expr(root, expr, layout)
}

pub(super) fn aggregate_group_by(path: &Path) -> Option<&[Expr]> {
    tlist::aggregate_group_by(path)
}
