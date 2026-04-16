use std::collections::HashMap;

mod bestpath;
mod inherit;
mod joininfo;
mod path;
mod pathnodes;
mod plan;
mod root;
mod setrefs;
#[cfg(test)]
mod tests;
mod upperrels;
mod util;

use crate::RelFileLocator;
use crate::backend::executor::Value;
use crate::backend::parser::{BoundIndexRelation, CatalogLookup, SqlType, SqlTypeKind};
use crate::include::catalog::PgStatisticRow;
use crate::include::nodes::datum::ArrayValue;
use crate::include::nodes::parsenodes::{JoinTreeNode, Query, RangeTblEntryKind};
use crate::include::nodes::pathnodes::{Path, PlannerInfo, RestrictInfo, SpecialJoinInfo};
use crate::include::nodes::plannodes::{Plan, PlannedStmt};
use crate::include::nodes::primnodes::{
    AggAccum, Expr, JoinType, ProjectSetTarget, RelationDesc, SetReturningCall, SubLink, SubPlan,
    ToastRelationRef, Var,
};

const DEFAULT_EQ_SEL: f64 = 0.005;
const DEFAULT_INEQ_SEL: f64 = 1.0 / 3.0;
const DEFAULT_BOOL_SEL: f64 = 0.5;
const DEFAULT_NUM_ROWS: f64 = 1000.0;
const DEFAULT_NUM_PAGES: f64 = 10.0;

const SEQ_PAGE_COST: f64 = 1.0;
const RANDOM_PAGE_COST: f64 = 4.0;
const CPU_TUPLE_COST: f64 = 0.01;
const CPU_INDEX_TUPLE_COST: f64 = 0.005;
const CPU_OPERATOR_COST: f64 = 0.0025;

const STATISTIC_KIND_MCV: i16 = 1;
const STATISTIC_KIND_HISTOGRAM: i16 = 2;

#[derive(Debug, Clone)]
struct RelationStats {
    relpages: f64,
    reltuples: f64,
    width: usize,
    stats_by_attnum: HashMap<i16, PgStatisticRow>,
}

#[derive(Debug, Clone)]
struct IndexableQual {
    column: usize,
    strategy: u16,
    argument: Value,
    expr: Expr,
}

#[derive(Debug, Clone)]
struct IndexPathSpec {
    index: BoundIndexRelation,
    keys: Vec<crate::include::access::scankey::ScanKeyData>,
    residual: Option<Expr>,
    used_quals: Vec<Expr>,
    direction: crate::include::access::relscan::ScanDirection,
    removes_order: bool,
}

#[derive(Debug, Clone)]
struct AccessCandidate {
    total_cost: f64,
    plan: Path,
}

#[derive(Debug, Clone)]
struct JoinBuildSpec {
    kind: JoinType,
    reversed: bool,
    rtindex: Option<usize>,
    explicit_qual: Option<Expr>,
}

#[derive(Debug, Clone)]
struct HashJoinClauses {
    hash_clauses: Vec<RestrictInfo>,
    outer_hash_keys: Vec<Expr>,
    inner_hash_keys: Vec<Expr>,
    join_clauses: Vec<RestrictInfo>,
}

fn create_plan(root: &PlannerInfo, path: Path) -> Plan {
    setrefs::create_plan(root, path)
}

fn has_outer_joins(root: &PlannerInfo) -> bool {
    !root.join_info_list.is_empty()
}

fn has_grouping(root: &PlannerInfo) -> bool {
    !root.parse.group_by.is_empty()
        || !root.parse.accumulators.is_empty()
        || root.parse.having_qual.is_some()
}

fn relids_union(left: &[usize], right: &[usize]) -> Vec<usize> {
    joininfo::relids_union(left, right)
}

fn relids_subset(required: &[usize], available: &[usize]) -> bool {
    joininfo::relids_subset(required, available)
}

fn relids_overlap(left: &[usize], right: &[usize]) -> bool {
    joininfo::relids_overlap(left, right)
}

fn relids_disjoint(left: &[usize], right: &[usize]) -> bool {
    joininfo::relids_disjoint(left, right)
}

fn is_pushable_base_clause(root: &PlannerInfo, relids: &[usize]) -> bool {
    !has_outer_joins(root)
        && relids.len() == 1
        && root
            .simple_rel_array
            .get(relids[0])
            .and_then(Option::as_ref)
            .is_some()
        && root
            .parse
            .rtable
            .get(relids[0].saturating_sub(1))
            .is_some_and(|rte| {
                // :HACK: Non-relation base RTEs still leak semantic identity through
                // pushed-down filters for repeated VALUES/subquery/function shapes.
                // Keep those quals above the join until base-slot identity is carried
                // separately from Var equality.
                matches!(rte.kind, RangeTblEntryKind::Relation { .. })
            })
}

fn expr_relids(expr: &Expr) -> Vec<usize> {
    joininfo::expr_relids(expr)
}

fn path_relids(path: &Path) -> Vec<usize> {
    match path {
        Path::Result { .. } => Vec::new(),
        Path::Append { source_id, .. } => vec![*source_id],
        Path::SeqScan { source_id, .. } | Path::IndexScan { source_id, .. } => vec![*source_id],
        Path::Filter { input, .. }
        | Path::Projection { input, .. }
        | Path::OrderBy { input, .. }
        | Path::Limit { input, .. }
        | Path::Aggregate { input, .. }
        | Path::ProjectSet { input, .. } => path_relids(input),
        Path::Values { slot_id, .. }
        | Path::FunctionScan { slot_id, .. }
        | Path::WorkTableScan { slot_id, .. } => vec![*slot_id],
        Path::RecursiveUnion {
            anchor, recursive, ..
        } => relids_union(&path_relids(anchor), &path_relids(recursive)),
        Path::NestedLoopJoin { left, right, .. } | Path::HashJoin { left, right, .. } => {
            relids_union(&path_relids(left), &path_relids(right))
        }
    }
}

fn reverse_join_type(kind: JoinType) -> JoinType {
    match kind {
        JoinType::Left => JoinType::Right,
        JoinType::Right => JoinType::Left,
        other => other,
    }
}

fn exact_join_rtindex_for_relids(node: &JoinTreeNode, target_relids: &[usize]) -> Option<usize> {
    match node {
        JoinTreeNode::RangeTblRef(_) => None,
        JoinTreeNode::JoinExpr {
            left,
            right,
            rtindex,
            ..
        } => {
            let left_match = exact_join_rtindex_for_relids(left, target_relids);
            let right_match = exact_join_rtindex_for_relids(right, target_relids);
            let mut relids = jointree_relids(left);
            relids.extend(jointree_relids(right));
            relids.sort_unstable();
            relids.dedup();
            if relids == target_relids {
                Some(*rtindex)
            } else {
                left_match.or(right_match)
            }
        }
    }
}

fn jointree_relids(node: &JoinTreeNode) -> Vec<usize> {
    match node {
        JoinTreeNode::RangeTblRef(rtindex) => vec![*rtindex],
        JoinTreeNode::JoinExpr { left, right, .. } => {
            let mut relids = jointree_relids(left);
            relids.extend(jointree_relids(right));
            relids.sort_unstable();
            relids.dedup();
            relids
        }
    }
}

fn exact_join_rtindex(root: &PlannerInfo, relids: &[usize]) -> Option<usize> {
    root.parse
        .jointree
        .as_ref()
        .and_then(|jointree| exact_join_rtindex_for_relids(jointree, relids))
}

fn expand_join_rte_vars(root: &PlannerInfo, expr: Expr) -> Expr {
    joininfo::expand_join_rte_vars(root, expr)
}

fn flatten_join_alias_vars(root: &PlannerInfo, expr: Expr) -> Expr {
    joininfo::flatten_join_alias_vars(root, expr)
}

fn rewrite_semantic_expr_for_path(expr: Expr, path: &Path, layout: &[Expr]) -> Expr {
    util::rewrite_semantic_expr_for_path(expr, path, layout)
}

fn rewrite_semantic_expr_for_path_or_expand_join_vars(
    root: &PlannerInfo,
    expr: Expr,
    path: &Path,
    layout: &[Expr],
) -> Expr {
    util::rewrite_semantic_expr_for_path_or_expand_join_vars(root, expr, path, layout)
}

fn layout_candidate_for_expr(root: &PlannerInfo, expr: &Expr, layout: &[Expr]) -> Option<Expr> {
    util::layout_candidate_for_expr(root, expr, layout)
}

fn aggregate_group_by(path: &Path) -> Option<&[Expr]> {
    util::aggregate_group_by(path)
}

fn rewrite_join_input_expr(
    root: Option<&PlannerInfo>,
    expr: Expr,
    path: &Path,
    layout: &[Expr],
) -> Expr {
    path::rewrite_join_input_expr(root, expr, path, layout)
}

fn rewrite_semantic_expr_for_join_inputs(
    root: Option<&PlannerInfo>,
    expr: Expr,
    left: &Path,
    right: &Path,
    join_layout: &[Expr],
) -> Expr {
    path::rewrite_semantic_expr_for_join_inputs(root, expr, left, right, join_layout)
}

fn optimize_path(plan: Path, catalog: &dyn CatalogLookup) -> Path {
    path::optimize_path(plan, catalog)
}

fn flatten_and_conjuncts(expr: &Expr) -> Vec<Expr> {
    path::flatten_and_conjuncts(expr)
}

fn and_exprs(exprs: Vec<Expr>) -> Option<Expr> {
    path::and_exprs(exprs)
}

fn estimate_sql_type_width(sql_type: SqlType) -> usize {
    path::estimate_sql_type_width(sql_type)
}

fn predicate_cost(expr: &Expr) -> f64 {
    path::predicate_cost(expr)
}

fn clamp_rows(rows: f64) -> f64 {
    path::clamp_rows(rows)
}

fn build_join_paths(
    left: Path,
    right: Path,
    left_relids: &[usize],
    right_relids: &[usize],
    kind: JoinType,
    restrict_clauses: Vec<RestrictInfo>,
) -> Vec<Path> {
    path::build_join_paths(
        left,
        right,
        left_relids,
        right_relids,
        kind,
        restrict_clauses,
    )
}

fn extract_hash_join_clauses(
    restrict_clauses: &[RestrictInfo],
    left_relids: &[usize],
    right_relids: &[usize],
) -> Option<HashJoinClauses> {
    path::extract_hash_join_clauses(restrict_clauses, left_relids, right_relids)
}

pub(crate) fn planner(query: Query, catalog: &dyn CatalogLookup) -> PlannedStmt {
    plan::planner(query, catalog)
}

pub(crate) fn finalize_expr_subqueries(
    expr: Expr,
    catalog: &dyn CatalogLookup,
    subplans: &mut Vec<Plan>,
) -> Expr {
    plan::finalize_expr_subqueries(expr, catalog, subplans)
}

pub(crate) fn finalize_plan_subqueries(
    plan: Plan,
    catalog: &dyn CatalogLookup,
    subplans: &mut Vec<Plan>,
) -> Plan {
    plan::finalize_plan_subqueries(plan, catalog, subplans)
}
