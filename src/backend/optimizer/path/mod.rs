#![allow(dead_code)]

mod allpaths;
mod costsize;
mod gistcost;

use crate::RelFileLocator;
use crate::backend::optimizer::{AccessCandidate, IndexPathSpec, RelationStats};
use crate::backend::parser::BoundIndexRelation;
use crate::backend::parser::CatalogLookup;
use crate::include::nodes::pathnodes::{Path, PlannerInfo, RelOptInfo, RestrictInfo};
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
    toast: Option<ToastRelationRef>,
    desc: RelationDesc,
    stats: &RelationStats,
    filter: Option<Expr>,
    order_items: Option<Vec<OrderByEntry>>,
) -> AccessCandidate {
    costsize::estimate_seqscan_candidate(
        source_id,
        rel,
        relation_name,
        relation_oid,
        relkind,
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
    relation_oid: u32,
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
        relation_oid,
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
    restrict_clauses: Vec<RestrictInfo>,
    pathtarget: crate::include::nodes::pathnodes::PathTarget,
    output_columns: Vec<QueryColumn>,
) -> Vec<Path> {
    costsize::build_join_paths_with_root(
        root,
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

pub(super) fn build_index_path_spec(
    filter: Option<&Expr>,
    order_items: Option<&[OrderByEntry]>,
    index: &BoundIndexRelation,
) -> Option<IndexPathSpec> {
    costsize::build_index_path_spec(filter, order_items, index)
}
