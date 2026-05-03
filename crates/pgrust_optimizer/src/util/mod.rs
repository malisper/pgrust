#![allow(dead_code)]

mod indexed_pathtarget;
mod tlist;

use pgrust_analyze::CatalogLookup;
use pgrust_nodes::pathnodes::{Path, PathKey, PathTarget, PlannerInfo, RelOptInfo};
use pgrust_nodes::primnodes::{AggAccum, Expr, QueryColumn, RelationDesc, TargetEntry};

pub(super) use indexed_pathtarget::{
    IndexedPathTarget, simple_var_key, strip_binary_coercible_casts,
};

pub(super) fn build_aggregate_output_columns(
    group_by: &[Expr],
    passthrough_exprs: &[Expr],
    accumulators: &[AggAccum],
) -> Vec<QueryColumn> {
    tlist::build_aggregate_output_columns(group_by, passthrough_exprs, accumulators)
}

pub(super) fn project_to_slot_layout(
    slot_id: usize,
    desc: &RelationDesc,
    input: Path,
    target: PathTarget,
    catalog: &dyn CatalogLookup,
) -> Path {
    tlist::project_to_slot_layout(slot_id, desc, input, target, catalog)
}

pub fn project_to_slot_layout_internal(
    root: Option<&PlannerInfo>,
    slot_id: usize,
    desc: &RelationDesc,
    input: Path,
    target: PathTarget,
    catalog: &dyn CatalogLookup,
) -> Path {
    tlist::project_to_slot_layout_internal(root, slot_id, desc, input, target, catalog)
}

pub fn normalize_rte_path(
    rtindex: usize,
    desc: &RelationDesc,
    input: Path,
    catalog: &dyn CatalogLookup,
) -> Path {
    tlist::normalize_rte_path(rtindex, desc, input, catalog)
}

pub(super) fn annotate_targets_for_input(
    root: Option<&PlannerInfo>,
    path: &Path,
    targets: &[TargetEntry],
) -> Vec<TargetEntry> {
    tlist::annotate_targets_for_input(root, path, targets)
}

pub fn lower_pathkeys_for_path(
    root: &PlannerInfo,
    path: &Path,
    pathkeys: &[PathKey],
) -> Vec<PathKey> {
    tlist::lower_pathkeys_for_path(root, path, pathkeys)
}

pub fn lower_pathkeys_for_rel(
    root: &PlannerInfo,
    rel: &RelOptInfo,
    pathkeys: &[PathKey],
) -> Vec<PathKey> {
    tlist::lower_pathkeys_for_rel(root, rel, pathkeys)
}

pub(super) fn pathkeys_are_fully_identified(pathkeys: &[PathKey]) -> bool {
    tlist::pathkeys_are_fully_identified(pathkeys)
}

pub fn required_query_pathkeys_for_path(root: &PlannerInfo, path: &Path) -> Vec<PathKey> {
    tlist::required_query_pathkeys_for_path(root, path)
}

pub fn required_query_pathkeys_for_rel(root: &PlannerInfo, rel: &RelOptInfo) -> Vec<PathKey> {
    tlist::required_query_pathkeys_for_rel(root, rel)
}

pub fn path_exposes_required_pathkey_identity(path: &Path, pathkeys: &[PathKey]) -> bool {
    tlist::path_exposes_required_pathkey_identity(path, pathkeys)
}

pub fn rel_exposes_required_pathkey_identity(rel: &RelOptInfo, pathkeys: &[PathKey]) -> bool {
    tlist::rel_exposes_required_pathkey_identity(rel, pathkeys)
}

pub(super) fn pathkeys_to_order_items(
    pathkeys: &[PathKey],
) -> Vec<pgrust_nodes::primnodes::OrderByEntry> {
    tlist::pathkeys_to_order_items(pathkeys)
}

pub(super) fn projection_is_identity(path: &Path, targets: &[TargetEntry]) -> bool {
    tlist::projection_is_identity(path, targets)
}

pub(super) fn aggregate_group_by(path: &Path) -> Option<&[Expr]> {
    tlist::aggregate_group_by(path)
}
