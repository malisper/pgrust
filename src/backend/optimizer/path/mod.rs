#![allow(dead_code)]

mod allpaths;
mod costsize;
mod gistcost;
mod regex_prefix;

use crate::RelFileLocator;
use crate::backend::optimizer::{AccessCandidate, IndexPathSpec, RelationStats};
use crate::backend::parser::BoundIndexRelation;
use crate::backend::parser::CatalogLookup;
use crate::include::nodes::pathnodes::{
    Path, PathKey, PlannerConfig, PlannerInfo, RelOptInfo, RestrictInfo,
};
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

pub(super) fn optimize_path_with_config(
    plan: Path,
    catalog: &dyn CatalogLookup,
    config: PlannerConfig,
) -> Path {
    costsize::optimize_path_with_config(plan, catalog, config)
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
    relation_name: String,
    relation_oid: u32,
    relkind: char,
    relispopulated: bool,
    toast: Option<ToastRelationRef>,
    desc: RelationDesc,
    stats: &RelationStats,
    filter: Option<Expr>,
    order_items: Option<Vec<OrderByEntry>>,
    catalog: &dyn CatalogLookup,
) -> AccessCandidate {
    costsize::estimate_seqscan_candidate(
        source_id,
        rel,
        relation_name,
        relation_oid,
        relkind,
        relispopulated,
        toast,
        desc,
        stats,
        filter,
        order_items,
        None,
        catalog,
        false,
    )
}

pub(super) fn estimate_index_candidate(
    source_id: usize,
    rel: RelFileLocator,
    relation_name: String,
    relation_oid: u32,
    toast: Option<ToastRelationRef>,
    desc: RelationDesc,
    stats: &RelationStats,
    spec: crate::backend::optimizer::IndexPathSpec,
    order_items: Option<Vec<OrderByEntry>>,
    target_index_only: bool,
    config: PlannerConfig,
    catalog: &dyn CatalogLookup,
) -> AccessCandidate {
    costsize::estimate_index_candidate(
        source_id,
        rel,
        relation_name,
        relation_oid,
        toast,
        desc,
        stats,
        spec,
        order_items,
        None,
        target_index_only,
        config,
        catalog,
    )
}

pub(super) fn index_supports_index_only_attrs(
    index: &BoundIndexRelation,
    required_attrs: &[usize],
) -> bool {
    costsize::index_supports_index_only_attrs(index, required_attrs)
}

pub(super) fn estimate_bitmap_candidate(
    source_id: usize,
    rel: RelFileLocator,
    relation_name: String,
    relation_oid: u32,
    toast: Option<ToastRelationRef>,
    desc: RelationDesc,
    stats: &RelationStats,
    spec: crate::backend::optimizer::IndexPathSpec,
    order_items: Option<Vec<OrderByEntry>>,
    catalog: &dyn CatalogLookup,
) -> AccessCandidate {
    costsize::estimate_bitmap_candidate(
        source_id,
        rel,
        relation_name,
        relation_oid,
        toast,
        desc,
        stats,
        spec,
        order_items,
        None,
        catalog,
    )
}

pub(super) fn build_join_paths_with_root(
    root: &PlannerInfo,
    catalog: &dyn CatalogLookup,
    left: Path,
    right: Path,
    left_relids: &[usize],
    right_relids: &[usize],
    kind: JoinType,
    restrict_clauses: Vec<RestrictInfo>,
    pathtarget: crate::include::nodes::pathnodes::PathTarget,
    output_columns: Vec<QueryColumn>,
) -> Vec<Path> {
    costsize::build_join_paths_with_root(
        root,
        catalog,
        left,
        right,
        left_relids,
        right_relids,
        kind,
        restrict_clauses,
        pathtarget,
        output_columns,
    )
}

pub(super) fn build_join_paths(
    left: Path,
    right: Path,
    left_relids: &[usize],
    right_relids: &[usize],
    kind: JoinType,
    restrict_clauses: Vec<RestrictInfo>,
    pathtarget: crate::include::nodes::pathnodes::PathTarget,
    output_columns: Vec<QueryColumn>,
) -> Vec<Path> {
    costsize::build_join_paths(
        left,
        right,
        left_relids,
        right_relids,
        kind,
        restrict_clauses,
        pathtarget,
        output_columns,
    )
}

pub(super) fn extract_hash_join_clauses(
    restrict_clauses: &[RestrictInfo],
    left_relids: &[usize],
    right_relids: &[usize],
) -> Option<crate::backend::optimizer::HashJoinClauses> {
    costsize::extract_hash_join_clauses(restrict_clauses, left_relids, right_relids)
}

pub(super) fn extract_merge_join_clauses(
    restrict_clauses: &[RestrictInfo],
    left_relids: &[usize],
    right_relids: &[usize],
) -> Option<crate::backend::optimizer::MergeJoinClauses> {
    costsize::extract_merge_join_clauses(restrict_clauses, left_relids, right_relids)
}

pub(super) fn build_index_path_spec(
    filter: Option<&Expr>,
    order_items: Option<&[OrderByEntry]>,
    index: &BoundIndexRelation,
) -> Option<IndexPathSpec> {
    costsize::build_index_path_spec(filter, order_items, index)
}

pub(super) fn relation_ordered_index_paths(
    root: &PlannerInfo,
    rtindex: usize,
    pathkeys: &[PathKey],
    catalog: &dyn CatalogLookup,
) -> Vec<Path> {
    allpaths::relation_ordered_index_paths(root, rtindex, pathkeys, catalog)
}

pub(super) fn relation_index_only_full_scan_paths(
    root: &PlannerInfo,
    rtindex: usize,
    catalog: &dyn CatalogLookup,
) -> Vec<Path> {
    allpaths::relation_index_only_full_scan_paths(root, rtindex, catalog)
}
