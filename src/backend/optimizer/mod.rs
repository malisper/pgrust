#![allow(dead_code)]

use std::collections::HashMap;

mod bestpath;
mod constfold;
mod inherit;
mod joininfo;
mod partition_prune;
mod partitionwise;
mod path;
mod pathnodes;
mod plan;
mod rewrite;
mod root;
mod setrefs;
mod sublink_pullup;
#[cfg(test)]
mod tests;
mod upperrels;
mod util;

use crate::backend::parser::{BoundIndexRelation, CatalogLookup, SqlType};
use crate::include::catalog::PgStatisticRow;
use crate::include::nodes::parsenodes::{JoinTreeNode, Query};
use crate::include::nodes::pathnodes::{Path, PlannerConfig, PlannerInfo, RestrictInfo};
use crate::include::nodes::plannodes::{IndexScanKey, IndexScanKeyArgument, Plan, PlannedStmt};
use crate::include::nodes::primnodes::{Expr, JoinType, OpExprKind};

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
const STATISTIC_KIND_CORRELATION: i16 = 3;

#[derive(Debug, Clone)]
struct RelationStats {
    relpages: f64,
    reltuples: f64,
    width: usize,
    stats_by_attnum: HashMap<i16, PgStatisticRow>,
}

#[derive(Debug, Clone)]
enum IndexStrategyLookup {
    Operator { oid: u32, kind: OpExprKind },
    Proc(u32),
}

#[derive(Debug, Clone)]
struct IndexableQual {
    column: Option<usize>,
    key_expr: Expr,
    lookup: IndexStrategyLookup,
    argument: IndexScanKeyArgument,
    expr: Expr,
    is_not_null: bool,
}

#[derive(Debug, Clone)]
struct IndexPathSpec {
    index: BoundIndexRelation,
    keys: Vec<IndexScanKey>,
    order_by_keys: Vec<IndexScanKey>,
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

#[derive(Debug, Clone)]
struct MergeJoinClauses {
    merge_clauses: Vec<RestrictInfo>,
    outer_merge_keys: Vec<Expr>,
    inner_merge_keys: Vec<Expr>,
    join_clauses: Vec<RestrictInfo>,
}

fn create_plan(
    root: &PlannerInfo,
    path: Path,
    catalog: &dyn CatalogLookup,
    subplans: &mut Vec<Plan>,
) -> (Plan, Vec<crate::include::nodes::plannodes::ExecParamSource>) {
    setrefs::create_plan(root, path, catalog, subplans)
}

fn create_plan_with_param_base(
    root: &PlannerInfo,
    path: Path,
    catalog: &dyn CatalogLookup,
    subplans: &mut Vec<Plan>,
    next_param_id: usize,
) -> (
    Plan,
    Vec<crate::include::nodes::plannodes::ExecParamSource>,
    usize,
) {
    setrefs::create_plan_with_param_base(root, path, catalog, subplans, next_param_id)
}

fn has_outer_joins(root: &PlannerInfo) -> bool {
    root.join_info_list.iter().any(|sjinfo| {
        matches!(
            sjinfo.jointype,
            JoinType::Left | JoinType::Right | JoinType::Full
        )
    })
}

fn nullable_relids_by_outer_joins(root: &PlannerInfo) -> Vec<usize> {
    let mut relids = Vec::new();
    for sjinfo in &root.join_info_list {
        match sjinfo.jointype {
            JoinType::Left => relids.extend(sjinfo.syn_righthand.iter().copied()),
            JoinType::Right => relids.extend(sjinfo.syn_lefthand.iter().copied()),
            JoinType::Full => {
                relids.extend(sjinfo.syn_lefthand.iter().copied());
                relids.extend(sjinfo.syn_righthand.iter().copied());
            }
            JoinType::Inner | JoinType::Cross | JoinType::Semi | JoinType::Anti => {}
        }
    }
    relids.sort_unstable();
    relids.dedup();
    relids
}

fn base_rel_is_nullable_by_outer_join(root: &PlannerInfo, relid: usize) -> bool {
    nullable_relids_by_outer_joins(root).contains(&relid)
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
    relids.len() == 1
        && !base_rel_is_nullable_by_outer_join(root, relids[0])
        && root
            .simple_rel_array
            .get(relids[0])
            .and_then(Option::as_ref)
            .is_some()
}

fn expr_relids(expr: &Expr) -> Vec<usize> {
    joininfo::expr_relids(expr)
}

fn path_relids(path: &Path) -> Vec<usize> {
    let slot_relid = |slot_id: usize| pathnodes::rte_slot_varno(slot_id).unwrap_or(slot_id);
    match path {
        Path::Result { .. } => Vec::new(),
        Path::Append { relids, .. } => relids.clone(),
        Path::SetOp { slot_id, .. } => vec![*slot_id],
        Path::SeqScan { source_id, .. }
        | Path::IndexScan { source_id, .. }
        | Path::BitmapIndexScan { source_id, .. }
        | Path::BitmapHeapScan { source_id, .. } => vec![*source_id],
        Path::Filter { input, .. }
        | Path::Projection { input, .. }
        | Path::OrderBy { input, .. }
        | Path::Limit { input, .. }
        | Path::LockRows { input, .. }
        | Path::Aggregate { input, .. }
        | Path::WindowAgg { input, .. }
        | Path::ProjectSet { input, .. } => path_relids(input),
        Path::SubqueryScan { rtindex, .. } => vec![*rtindex],
        Path::Values { slot_id, .. }
        | Path::FunctionScan { slot_id, .. }
        | Path::CteScan { slot_id, .. }
        | Path::WorkTableScan { slot_id, .. } => vec![slot_relid(*slot_id)],
        Path::RecursiveUnion {
            anchor, recursive, ..
        } => relids_union(&path_relids(anchor), &path_relids(recursive)),
        Path::NestedLoopJoin { left, right, .. }
        | Path::HashJoin { left, right, .. }
        | Path::MergeJoin { left, right, .. } => {
            relids_union(&path_relids(left), &path_relids(right))
        }
    }
}

fn reverse_join_type(kind: JoinType) -> JoinType {
    match kind {
        JoinType::Left => JoinType::Right,
        JoinType::Right => JoinType::Left,
        JoinType::Semi | JoinType::Anti => kind,
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

#[cfg(test)]
fn make_restrict_info(clause: Expr) -> RestrictInfo {
    joininfo::make_restrict_info(clause)
}

fn aggregate_group_by(path: &Path) -> Option<&[Expr]> {
    util::aggregate_group_by(path)
}

fn optimize_path(plan: Path, catalog: &dyn CatalogLookup) -> Path {
    path::optimize_path(plan, catalog)
}

fn pull_up_sublinks(query: Query) -> Query {
    sublink_pullup::pull_up_sublinks(query)
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
    let mut output_columns = left.columns();
    if !matches!(kind, JoinType::Semi | JoinType::Anti) {
        output_columns.extend(right.columns());
    }
    let mut exprs = left.semantic_output_target().exprs;
    let mut sortgrouprefs = left.semantic_output_target().sortgrouprefs;
    if !matches!(kind, JoinType::Semi | JoinType::Anti) {
        exprs.extend(right.semantic_output_target().exprs);
        sortgrouprefs.extend(right.semantic_output_target().sortgrouprefs);
    }
    path::build_join_paths(
        left,
        right,
        left_relids,
        right_relids,
        kind,
        restrict_clauses,
        crate::include::nodes::pathnodes::PathTarget::with_sortgrouprefs(exprs, sortgrouprefs),
        output_columns,
    )
}

fn extract_hash_join_clauses(
    restrict_clauses: &[RestrictInfo],
    left_relids: &[usize],
    right_relids: &[usize],
) -> Option<HashJoinClauses> {
    path::extract_hash_join_clauses(restrict_clauses, left_relids, right_relids)
}

fn extract_merge_join_clauses(
    restrict_clauses: &[RestrictInfo],
    left_relids: &[usize],
    right_relids: &[usize],
) -> Option<MergeJoinClauses> {
    path::extract_merge_join_clauses(restrict_clauses, left_relids, right_relids)
}

pub(crate) fn planner(
    query: Query,
    catalog: &dyn CatalogLookup,
) -> Result<PlannedStmt, crate::backend::parser::ParseError> {
    plan::planner(query, catalog)
}

pub(crate) fn planner_with_config(
    query: Query,
    catalog: &dyn CatalogLookup,
    config: PlannerConfig,
) -> Result<PlannedStmt, crate::backend::parser::ParseError> {
    plan::planner_with_config(query, catalog, config)
}

pub(crate) fn fold_query_constants(
    query: Query,
) -> Result<Query, crate::backend::parser::ParseError> {
    constfold::fold_query_constants(query)
}

pub(crate) fn planner_with_param_base(
    query: Query,
    catalog: &dyn CatalogLookup,
    next_param_id: usize,
) -> Result<(PlannedStmt, usize), crate::backend::parser::ParseError> {
    plan::planner_with_param_base(query, catalog, next_param_id)
}

pub(crate) fn planner_with_param_base_and_config(
    query: Query,
    catalog: &dyn CatalogLookup,
    next_param_id: usize,
    config: PlannerConfig,
) -> Result<(PlannedStmt, usize), crate::backend::parser::ParseError> {
    plan::planner_with_param_base_and_config(query, catalog, next_param_id, config)
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
