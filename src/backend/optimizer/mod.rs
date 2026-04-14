use std::cmp::Ordering;
use std::collections::HashMap;

mod bestpath;
mod joininfo;
mod pathnodes;
mod root;
#[cfg(test)]
mod tests;
mod upperrels;

use crate::RelFileLocator;
use crate::backend::executor::{Value, compare_order_values};
use crate::backend::parser::{BoundIndexRelation, CatalogLookup, SqlType, SqlTypeKind};
use crate::include::catalog::{BTREE_AM_OID, PgStatisticRow};
use crate::include::nodes::datum::ArrayValue;
use crate::include::nodes::parsenodes::{JoinTreeNode, Query, RangeTblEntryKind};
use crate::include::nodes::pathnodes::{
    Path, PathKey, PathTarget, PlannerGlobal, PlannerInfo, RelOptInfo, RelOptKind, RestrictInfo,
    SpecialJoinInfo, UpperRelKind,
};
use crate::include::nodes::plannodes::{Plan, PlanEstimate, PlannedStmt};
use crate::include::nodes::primnodes::{
    AggAccum, BoolExprType, Expr, ExprArraySubscript, JoinType, OpExprKind, OrderByEntry,
    ProjectSetTarget, QueryColumn, RelationDesc, SetReturningCall, SubLink, SubPlan, TargetEntry,
    ToastRelationRef, Var,
};
use pathnodes::{
    aggregate_output_vars, expr_sql_type, lower_agg_output_expr, next_synthetic_slot_id,
    rewrite_expr_against_layout, rewrite_project_set_target_against_layout,
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

fn create_plan(path: Path) -> Plan {
    path.into_plan()
}

fn pathkeys_to_order_items(pathkeys: &[PathKey]) -> Vec<OrderByEntry> {
    pathkeys
        .iter()
        .map(|key| OrderByEntry {
            expr: key.expr.clone(),
            descending: key.descending,
            nulls_first: key.nulls_first,
        })
        .collect()
}

fn pathkeys_satisfy(actual: &[PathKey], required: &[PathKey]) -> bool {
    actual.len() >= required.len()
        && actual
            .iter()
            .zip(required.iter())
            .all(|(actual, required)| actual == required)
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
}

fn expr_relids(expr: &Expr) -> Vec<usize> {
    joininfo::expr_relids(expr)
}

#[derive(Debug, Clone)]
struct JoinBuildSpec {
    kind: JoinType,
    rtindex: Option<usize>,
    explicit_qual: Option<Expr>,
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

fn collect_inner_join_clauses(root: &PlannerInfo) -> Vec<RestrictInfo> {
    fn walk(root: &PlannerInfo, node: &JoinTreeNode, clauses: &mut Vec<RestrictInfo>) {
        if let JoinTreeNode::JoinExpr {
            left,
            right,
            kind,
            quals,
            ..
        } = node
        {
            walk(root, left, clauses);
            walk(root, right, clauses);
            if matches!(kind, JoinType::Inner | JoinType::Cross) {
                clauses.push(joininfo::make_restrict_info(expand_join_rte_vars(root, quals.clone())));
            }
        }
    }

    let mut clauses = Vec::new();
    if let Some(jointree) = root.parse.jointree.as_ref() {
        walk(root, jointree, &mut clauses);
    }
    if !has_outer_joins(root) {
        if let Some(where_qual) = root.parse.where_qual.as_ref() {
            clauses.extend(
                flatten_and_conjuncts(where_qual)
                    .into_iter()
                    .map(|clause| expand_join_rte_vars(root, clause))
                    .filter(|clause| expr_relids(clause).len() > 1)
                    .map(joininfo::make_restrict_info),
            );
        }
    }
    clauses
}

fn residual_where_qual(root: &PlannerInfo) -> Option<Expr> {
    let Some(where_qual) = root.parse.where_qual.as_ref() else {
        return None;
    };
    let clauses = flatten_and_conjuncts(where_qual)
        .into_iter()
        .map(|clause| expand_join_rte_vars(root, clause))
        .filter(|clause| {
            let relids = expr_relids(clause);
            has_outer_joins(root) || !is_pushable_base_clause(root, &relids)
        })
        .collect();
    and_exprs(clauses)
}

fn assign_base_restrictinfo(root: &mut PlannerInfo) {
    for rel in root.simple_rel_array.iter_mut().flatten() {
        rel.baserestrictinfo.clear();
        rel.joininfo.clear();
    }
    if has_outer_joins(root) {
        return;
    }
    let Some(where_qual) = root.parse.where_qual.as_ref() else {
        return;
    };
    for clause in flatten_and_conjuncts(where_qual) {
        let restrict = joininfo::make_restrict_info(expand_join_rte_vars(root, clause));
        if !is_pushable_base_clause(root, &restrict.required_relids) {
            continue;
        }
        let relid = restrict.required_relids[0];
        if let Some(rel) = root
            .simple_rel_array
            .get_mut(relid)
            .and_then(Option::as_mut)
        {
            rel.baserestrictinfo.push(restrict);
        }
    }
}

fn base_filter_expr(rel: &RelOptInfo) -> Option<Expr> {
    and_exprs(
        rel.baserestrictinfo
            .iter()
            .map(|restrict| restrict.clause.clone())
            .collect(),
    )
}

fn build_aggregate_output_columns(
    group_by: &[Expr],
    accumulators: &[AggAccum],
) -> Vec<QueryColumn> {
    let mut output_columns = Vec::with_capacity(group_by.len() + accumulators.len());
    for (index, expr) in group_by.iter().enumerate() {
        output_columns.push(QueryColumn {
            name: format!("group{}", index + 1),
            sql_type: expr_sql_type(expr),
        });
    }
    for (index, accum) in accumulators.iter().enumerate() {
        output_columns.push(QueryColumn {
            name: format!("agg{}", index + 1),
            sql_type: accum.sql_type,
        });
    }
    output_columns
}

fn query_order_items_for_base_rel(root: &PlannerInfo, rtindex: usize) -> Option<Vec<OrderByEntry>> {
    if root.query_pathkeys.is_empty() {
        return None;
    }
    let expanded_pathkeys = root
        .query_pathkeys
        .iter()
        .cloned()
        .map(|key| PathKey {
            expr: expand_join_rte_vars(root, key.expr),
            descending: key.descending,
            nulls_first: key.nulls_first,
        })
        .collect::<Vec<_>>();
    if expanded_pathkeys
        .iter()
        .all(|key| expr_relids(&key.expr).iter().all(|relid| *relid == rtindex))
    {
        Some(pathkeys_to_order_items(&expanded_pathkeys))
    } else {
        None
    }
}

fn project_to_slot_layout(
    slot_id: usize,
    desc: &RelationDesc,
    input: Path,
    target_exprs: Vec<Expr>,
    catalog: &dyn CatalogLookup,
) -> Path {
    let layout = input.output_vars();
    let rewritten_targets = desc
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let expr = target_exprs
                .get(index)
                .cloned()
                .or_else(|| layout.get(index).cloned())
                .unwrap_or_else(|| Expr::Column(index));
            TargetEntry::new(
                column.name.clone(),
                rewrite_semantic_expr_for_path(expr, &input, &layout),
                column.sql_type,
                index + 1,
            )
        })
        .collect();
    optimize_path(
        Path::Projection {
            plan_info: PlanEstimate::default(),
            slot_id,
            input: Box::new(input),
            targets: rewritten_targets,
        },
        catalog,
    )
}

fn normalize_rte_path(
    rtindex: usize,
    desc: &RelationDesc,
    input: Path,
    catalog: &dyn CatalogLookup,
) -> Path {
    let desired_layout = PathTarget::new(
        desc.columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                Expr::Var(Var {
                    varno: rtindex,
                    varattno: index + 1,
                    varlevelsup: 0,
                    vartype: column.sql_type,
                })
            })
            .collect(),
    )
    .exprs;
    if input.output_vars() == desired_layout {
        input
    } else {
        project_to_slot_layout(rtindex, desc, input.clone(), input.output_vars(), catalog)
    }
}

fn lower_targets_for_path(
    root: &PlannerInfo,
    path: &Path,
    targets: &[TargetEntry],
) -> Vec<TargetEntry> {
    let layout = path.output_vars();
    match aggregate_group_by(path) {
        Some(group_by) => targets
            .iter()
            .cloned()
            .map(|target| TargetEntry {
                expr: lower_agg_output_expr(
                    expand_join_rte_vars(root, target.expr),
                    group_by,
                    &layout,
                ),
                ..target
            })
            .collect(),
        None => targets
            .iter()
            .cloned()
            .map(|target| TargetEntry {
                expr: rewrite_semantic_expr_for_path(
                    expand_join_rte_vars(root, target.expr),
                    path,
                    &layout,
                ),
                ..target
            })
            .collect(),
    }
}

fn lower_pathkeys_for_path(root: &PlannerInfo, path: &Path, pathkeys: &[PathKey]) -> Vec<PathKey> {
    let layout = path.output_vars();
    match aggregate_group_by(path) {
        Some(group_by) => pathkeys
            .iter()
            .cloned()
            .map(|key| PathKey {
                expr: lower_agg_output_expr(
                    expand_join_rte_vars(root, key.expr),
                    group_by,
                    &layout,
                ),
                descending: key.descending,
                nulls_first: key.nulls_first,
            })
            .collect(),
        None => pathkeys
            .iter()
            .cloned()
            .map(|key| PathKey {
                expr: rewrite_semantic_expr_for_path(
                    expand_join_rte_vars(root, key.expr),
                    path,
                    &layout,
                ),
                descending: key.descending,
                nulls_first: key.nulls_first,
            })
            .collect(),
    }
}

fn lower_pathkeys_for_rel(root: &PlannerInfo, rel: &RelOptInfo, pathkeys: &[PathKey]) -> Vec<PathKey> {
    rel.pathlist
        .first()
        .map(|path| lower_pathkeys_for_path(root, path, pathkeys))
        .unwrap_or_else(|| pathkeys.to_vec())
}

fn projection_is_identity(path: &Path, targets: &[TargetEntry]) -> bool {
    let input_columns = path.columns();
    let layout = path.output_vars();
    targets.len() == input_columns.len()
        && targets.iter().enumerate().all(|(index, target)| {
            target.expr == layout[index] && target.name == input_columns[index].name
        })
}

fn projection_slot_var(slot_id: usize, attno: usize, vartype: SqlType) -> Expr {
    Expr::Var(Var {
        varno: slot_id,
        varattno: attno,
        varlevelsup: 0,
        vartype,
    })
}

fn rewrite_expr_for_path(expr: Expr, path: &Path, layout: &[Expr]) -> Expr {
    match path {
        Path::Projection {
            slot_id,
            input,
            targets,
            ..
        } => {
            if let Some((index, target)) = targets
                .iter()
                .enumerate()
                .find(|(_, target)| target.expr == expr)
            {
                projection_slot_var(*slot_id, index + 1, target.sql_type)
            } else {
                let rewritten_input_expr =
                    rewrite_expr_for_path(expr.clone(), input, &input.output_vars());
                if let Some((index, target)) = targets
                    .iter()
                    .enumerate()
                    .find(|(_, target)| target.expr == rewritten_input_expr)
                {
                    projection_slot_var(*slot_id, index + 1, target.sql_type)
                } else {
                    rewrite_expr_against_layout(expr, layout)
                }
            }
        }
        Path::Filter { input, .. } | Path::OrderBy { input, .. } | Path::Limit { input, .. } => {
            rewrite_expr_for_path(expr, input, layout)
        }
        Path::NestedLoopJoin { left, right, .. } => {
            let left_layout = left.output_vars();
            let rewritten_left = rewrite_expr_for_path(expr.clone(), left, &left_layout);
            if rewritten_left != expr || left_layout.contains(&expr) {
                return rewritten_left;
            }
            let right_layout = right.output_vars();
            let rewritten_right = rewrite_expr_for_path(expr.clone(), right, &right_layout);
            if rewritten_right != expr || right_layout.contains(&expr) {
                return rewritten_right;
            }
            rewrite_expr_against_layout(expr, layout)
        }
        _ => rewrite_expr_against_layout(expr, layout),
    }
}

fn rewrite_semantic_expr_for_path(expr: Expr, path: &Path, layout: &[Expr]) -> Expr {
    let rewritten = rewrite_expr_for_path(expr.clone(), path, layout);
    if rewritten != expr {
        return rewritten;
    }
    match expr {
        Expr::Aggref(aggref) => Expr::Aggref(Box::new(crate::include::nodes::primnodes::Aggref {
            args: aggref
                .args
                .into_iter()
                .map(|arg| rewrite_semantic_expr_for_path(arg, path, layout))
                .collect(),
            ..*aggref
        })),
        Expr::Op(op) => Expr::Op(Box::new(crate::include::nodes::primnodes::OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| rewrite_semantic_expr_for_path(arg, path, layout))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(crate::include::nodes::primnodes::BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| rewrite_semantic_expr_for_path(arg, path, layout))
                .collect(),
            ..*bool_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(crate::include::nodes::primnodes::FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| rewrite_semantic_expr_for_path(arg, path, layout))
                .collect(),
            ..*func
        })),
        Expr::SubLink(sublink) => {
            Expr::SubLink(Box::new(crate::include::nodes::primnodes::SubLink {
                testexpr: sublink
                    .testexpr
                    .map(|expr| Box::new(rewrite_semantic_expr_for_path(*expr, path, layout))),
                ..*sublink
            }))
        }
        Expr::SubPlan(subplan) => {
            Expr::SubPlan(Box::new(crate::include::nodes::primnodes::SubPlan {
                testexpr: subplan
                    .testexpr
                    .map(|expr| Box::new(rewrite_semantic_expr_for_path(*expr, path, layout))),
                ..*subplan
            }))
        }
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(
            crate::include::nodes::primnodes::ScalarArrayOpExpr {
                left: Box::new(rewrite_semantic_expr_for_path(*saop.left, path, layout)),
                right: Box::new(rewrite_semantic_expr_for_path(*saop.right, path, layout)),
                ..*saop
            },
        )),
        Expr::Cast(inner, ty) => Expr::Cast(
            Box::new(rewrite_semantic_expr_for_path(*inner, path, layout)),
            ty,
        ),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            expr: Box::new(rewrite_semantic_expr_for_path(*expr, path, layout)),
            pattern: Box::new(rewrite_semantic_expr_for_path(*pattern, path, layout)),
            escape: escape
                .map(|expr| Box::new(rewrite_semantic_expr_for_path(*expr, path, layout))),
            case_insensitive,
            negated,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(rewrite_semantic_expr_for_path(*expr, path, layout)),
            pattern: Box::new(rewrite_semantic_expr_for_path(*pattern, path, layout)),
            escape: escape
                .map(|expr| Box::new(rewrite_semantic_expr_for_path(*expr, path, layout))),
            negated,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(rewrite_semantic_expr_for_path(
            *inner, path, layout,
        ))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(rewrite_semantic_expr_for_path(
            *inner, path, layout,
        ))),
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(rewrite_semantic_expr_for_path(*left, path, layout)),
            Box::new(rewrite_semantic_expr_for_path(*right, path, layout)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(rewrite_semantic_expr_for_path(*left, path, layout)),
            Box::new(rewrite_semantic_expr_for_path(*right, path, layout)),
        ),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|element| rewrite_semantic_expr_for_path(element, path, layout))
                .collect(),
            array_type,
        },
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(rewrite_semantic_expr_for_path(*left, path, layout)),
            Box::new(rewrite_semantic_expr_for_path(*right, path, layout)),
        ),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(rewrite_semantic_expr_for_path(*array, path, layout)),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript
                        .lower
                        .map(|expr| rewrite_semantic_expr_for_path(expr, path, layout)),
                    upper: subscript
                        .upper
                        .map(|expr| rewrite_semantic_expr_for_path(expr, path, layout)),
                })
                .collect(),
        },
        other => other,
    }
}

fn aggregate_group_by(path: &Path) -> Option<&[Expr]> {
    match path {
        Path::Aggregate { group_by, .. } => Some(group_by),
        Path::Filter { input, .. } | Path::OrderBy { input, .. } | Path::Limit { input, .. } => {
            aggregate_group_by(input)
        }
        _ => None,
    }
}

fn set_base_rel_pathlist(root: &mut PlannerInfo, rtindex: usize, catalog: &dyn CatalogLookup) {
    let Some(rte) = root.parse.rtable.get(rtindex.saturating_sub(1)).cloned() else {
        return;
    };
    let query_order_items = query_order_items_for_base_rel(root, rtindex);
    let Some(rel) = root
        .simple_rel_array
        .get_mut(rtindex)
        .and_then(Option::as_mut)
    else {
        return;
    };
    if !rel.pathlist.is_empty() {
        return;
    }

    match rte.kind {
        RangeTblEntryKind::Result => rel.add_path(optimize_path(
            Path::Result {
                plan_info: PlanEstimate::default(),
            },
            catalog,
        )),
        RangeTblEntryKind::Relation {
            rel: heap_rel,
            relation_oid,
            relkind: _,
            toast,
        } => {
            let filter = base_filter_expr(rel);
            let stats = relation_stats(catalog, relation_oid, &rte.desc);
            rel.add_path(
                estimate_seqscan_candidate(
                    rtindex,
                    heap_rel,
                    relation_oid,
                    toast,
                    rte.desc.clone(),
                    &stats,
                    filter.clone(),
                    None,
                )
                .plan,
            );
            if let Some(order_items) = query_order_items.clone() {
                rel.add_path(
                    estimate_seqscan_candidate(
                        rtindex,
                        heap_rel,
                        relation_oid,
                        toast,
                        rte.desc.clone(),
                        &stats,
                        filter.clone(),
                        Some(order_items),
                    )
                    .plan,
                );
            }
            for index in catalog
                .index_relations_for_heap(relation_oid)
                .iter()
                .filter(|index| {
                    index.index_meta.indisvalid
                        && index.index_meta.indisready
                        && !index.index_meta.indkey.is_empty()
                        && index.index_meta.am_oid == BTREE_AM_OID
                })
            {
                let Some(spec) = build_index_path_spec(filter.as_ref(), None, index) else {
                    continue;
                };
                rel.add_path(
                    estimate_index_candidate(
                        rtindex,
                        heap_rel,
                        toast,
                        rte.desc.clone(),
                        &stats,
                        spec,
                        None,
                        catalog,
                    )
                    .plan,
                );
                if let Some(order_items) = query_order_items.as_ref()
                    && let Some(spec) =
                        build_index_path_spec(filter.as_ref(), Some(order_items), index)
                {
                    rel.add_path(
                        estimate_index_candidate(
                            rtindex,
                            heap_rel,
                            toast,
                            rte.desc.clone(),
                            &stats,
                            spec,
                            Some(order_items.clone()),
                            catalog,
                        )
                        .plan,
                    );
                }
            }
        }
        RangeTblEntryKind::Values {
            rows,
            output_columns,
        } => {
            let mut path = optimize_path(
                Path::Values {
                    plan_info: PlanEstimate::default(),
                    slot_id: rtindex,
                    rows,
                    output_columns,
                },
                catalog,
            );
            if let Some(filter) = base_filter_expr(rel) {
                path = optimize_path(
                    Path::Filter {
                        plan_info: PlanEstimate::default(),
                        predicate: rewrite_expr_against_layout(filter, &path.output_vars()),
                        input: Box::new(path),
                    },
                    catalog,
                );
            }
            rel.add_path(path);
        }
        RangeTblEntryKind::Function { call } => {
            let mut path = optimize_path(
                Path::FunctionScan {
                    plan_info: PlanEstimate::default(),
                    slot_id: rtindex,
                    call,
                },
                catalog,
            );
            if let Some(filter) = base_filter_expr(rel) {
                path = optimize_path(
                    Path::Filter {
                        plan_info: PlanEstimate::default(),
                        predicate: rewrite_expr_against_layout(filter, &path.output_vars()),
                        input: Box::new(path),
                    },
                    catalog,
                );
            }
            rel.add_path(path);
        }
        RangeTblEntryKind::Subquery { query } => {
            let mut subroot = PlannerInfo::new(*query);
            let scanjoin_rel = query_planner(&mut subroot, catalog);
            let final_rel = grouping_planner(&mut subroot, scanjoin_rel, catalog);
            let required_pathkeys =
                lower_pathkeys_for_rel(&subroot, &final_rel, &subroot.query_pathkeys);
            let mut path = bestpath::choose_final_path(&final_rel, &required_pathkeys)
                .cloned()
                .unwrap_or(Path::Result {
                    plan_info: PlanEstimate::default(),
                });
            path = normalize_rte_path(rtindex, &rte.desc, path, catalog);
            if let Some(filter) = base_filter_expr(rel) {
                path = optimize_path(
                    Path::Filter {
                        plan_info: PlanEstimate::default(),
                        predicate: rewrite_expr_against_layout(filter, &path.output_vars()),
                        input: Box::new(path),
                    },
                    catalog,
                );
            }
            rel.add_path(path);
        }
        RangeTblEntryKind::Join { .. } => unreachable!("join RTEs are not base relations"),
    }
    bestpath::set_cheapest(rel);
}

fn set_base_rel_pathlists(root: &mut PlannerInfo, catalog: &dyn CatalogLookup) {
    let max_rtindex = root.simple_rel_array.len().saturating_sub(1);
    for rtindex in 1..=max_rtindex {
        if root
            .simple_rel_array
            .get(rtindex)
            .and_then(Option::as_ref)
            .is_some()
        {
            set_base_rel_pathlist(root, rtindex, catalog);
        }
    }
}

fn build_join_qual(
    kind: JoinType,
    explicit_qual: Option<Expr>,
    left_relids: &[usize],
    right_relids: &[usize],
    inner_join_clauses: &[RestrictInfo],
) -> Expr {
    let join_relids = relids_union(left_relids, right_relids);
    let mut clauses = Vec::new();
    if let Some(explicit_qual) = explicit_qual {
        clauses.push(explicit_qual);
    }
    if matches!(kind, JoinType::Inner | JoinType::Cross) {
        for restrict in inner_join_clauses {
            let clause = &restrict.clause;
            let clause_relids = &restrict.required_relids;
            if clause_relids.len() <= 1 {
                continue;
            }
            if relids_subset(&clause_relids, &join_relids)
                && !relids_subset(&clause_relids, left_relids)
                && !relids_subset(&clause_relids, right_relids)
                && !clauses.contains(clause)
            {
                clauses.push(clause.clone());
            }
        }
    }
    and_exprs(clauses).unwrap_or(Expr::Const(Value::Bool(true)))
}

fn maybe_project_join_alias(
    rtindex: usize,
    input: Path,
    root: &PlannerInfo,
    catalog: &dyn CatalogLookup,
) -> Path {
    let Some(rte) = root.parse.rtable.get(rtindex.saturating_sub(1)) else {
        return input;
    };
    let RangeTblEntryKind::Join { joinaliasvars, .. } = &rte.kind else {
        return input;
    };
    let layout = input.output_vars();
    let desired_layout = PathTarget::from_rte(rtindex, rte).exprs;
    let target_exprs = joinaliasvars
        .iter()
        .cloned()
        .map(|expr| expand_join_rte_vars(root, expr))
        .map(|expr| rewrite_semantic_expr_for_path(expr, &input, &layout))
        .collect::<Vec<_>>();
    if layout == desired_layout {
        return input;
    }
    project_to_slot_layout(rtindex, &rte.desc, input, target_exprs, catalog)
}

fn top_join_rtindex(root: &PlannerInfo) -> Option<usize> {
    match root.parse.jointree.as_ref() {
        Some(JoinTreeNode::JoinExpr { rtindex, .. }) => root
            .parse
            .rtable
            .get(rtindex.saturating_sub(1))
            .filter(|rte| matches!(rte.kind, RangeTblEntryKind::Join { .. }))
            .map(|_| *rtindex),
        _ => None,
    }
}

fn normalize_join_output_rel(
    root: &PlannerInfo,
    input_rel: RelOptInfo,
    rtindex: usize,
    catalog: &dyn CatalogLookup,
) -> RelOptInfo {
    let Some(rte) = root.parse.rtable.get(rtindex.saturating_sub(1)) else {
        return input_rel;
    };
    let mut rel = RelOptInfo::new(
        input_rel.relids.clone(),
        input_rel.reloptkind,
        PathTarget::from_rte(rtindex, rte),
    );
    for path in input_rel.pathlist {
        rel.add_path(maybe_project_join_alias(rtindex, path, root, catalog));
    }
    bestpath::set_cheapest(&mut rel);
    rel
}

fn base_rels_at_level(root: &PlannerInfo, level: usize) -> Vec<RelOptInfo> {
    if level == 1 {
        root.simple_rel_array
            .iter()
            .skip(1)
            .filter_map(|rel| rel.clone())
            .collect()
    } else {
        root.join_rel_list
            .iter()
            .filter(|rel| rel.relids.len() == level)
            .cloned()
            .collect()
    }
}

fn find_join_rel_index(root: &PlannerInfo, relids: &[usize]) -> Option<usize> {
    root.join_rel_list
        .iter()
        .position(|rel| rel.relids == relids)
}

fn join_reltarget(
    root: &PlannerInfo,
    relids: &[usize],
    left_rel: &RelOptInfo,
    right_rel: &RelOptInfo,
) -> PathTarget {
    if let Some(sjinfo) = root
        .join_info_list
        .iter()
        .find(|sjinfo| relids_union(&sjinfo.syn_lefthand, &sjinfo.syn_righthand) == relids)
    {
        if let Some(rte) = root.parse.rtable.get(sjinfo.rtindex.saturating_sub(1)) {
            if matches!(rte.kind, RangeTblEntryKind::Join { .. }) {
                return PathTarget::from_rte(sjinfo.rtindex, rte);
            }
        }
    }
    if let Some(rtindex) = exact_join_rtindex(root, relids)
        && let Some(rte) = root.parse.rtable.get(rtindex.saturating_sub(1))
    {
        if matches!(rte.kind, RangeTblEntryKind::Join { .. }) {
            return PathTarget::from_rte(rtindex, rte);
        }
    }
    let mut exprs = left_rel.reltarget.exprs.clone();
    exprs.extend(right_rel.reltarget.exprs.clone());
    PathTarget::new(exprs)
}

fn join_spec_for_special_join(
    sjinfo: &SpecialJoinInfo,
    reversed: bool,
) -> JoinBuildSpec {
    JoinBuildSpec {
        kind: if reversed {
            reverse_join_type(sjinfo.jointype)
        } else {
            sjinfo.jointype
        },
        rtindex: Some(sjinfo.rtindex),
        explicit_qual: Some(sjinfo.join_quals.clone()),
    }
}

fn special_join_relids(sjinfo: &SpecialJoinInfo) -> Vec<usize> {
    relids_union(&sjinfo.syn_lefthand, &sjinfo.syn_righthand)
}

fn rel_contains_special_join(relids: &[usize], sjinfo: &SpecialJoinInfo) -> bool {
    relids_subset(&sjinfo.min_lefthand, relids) && relids_subset(&sjinfo.min_righthand, relids)
}

fn rel_matches_special_join(
    sjinfo: &SpecialJoinInfo,
    left_relids: &[usize],
    right_relids: &[usize],
) -> Option<bool> {
    if relids_subset(&sjinfo.min_lefthand, left_relids)
        && relids_subset(&sjinfo.min_righthand, right_relids)
    {
        Some(false)
    } else if relids_subset(&sjinfo.min_lefthand, right_relids)
        && relids_subset(&sjinfo.min_righthand, left_relids)
    {
        Some(true)
    } else {
        None
    }
}

fn relids_match_ojrelid(root: &PlannerInfo, relids: &[usize], ojrelid: usize) -> bool {
    root.join_info_list.iter().any(|sjinfo| {
        sjinfo.ojrelid == Some(ojrelid) && special_join_relids(sjinfo) == relids
    })
}

fn input_crosses_rhs_boundary(input_relids: &[usize], sjinfo: &SpecialJoinInfo) -> bool {
    relids_overlap(input_relids, &sjinfo.min_righthand)
        && !relids_subset(input_relids, &sjinfo.min_righthand)
}

fn input_can_commute_past_special_join(
    root: &PlannerInfo,
    input_relids: &[usize],
    sjinfo: &SpecialJoinInfo,
) -> bool {
    if !input_crosses_rhs_boundary(input_relids, sjinfo) {
        return true;
    }
    sjinfo
        .commute_below_l
        .iter()
        .chain(sjinfo.commute_below_r.iter())
        .any(|ojrelid| relids_match_ojrelid(root, input_relids, *ojrelid))
}

fn violates_full_join_barrier(
    root: &PlannerInfo,
    sjinfo: &SpecialJoinInfo,
    left_relids: &[usize],
    right_relids: &[usize],
    joinrelids: &[usize],
) -> bool {
    if sjinfo.jointype != JoinType::Full {
        return false;
    }
    let full_relids = special_join_relids(sjinfo);
    relids_overlap(&full_relids, joinrelids)
        && full_relids != joinrelids
        && full_relids != left_relids
        && full_relids != right_relids
        && (relids_overlap(&full_relids, left_relids) || relids_overlap(&full_relids, right_relids))
        && !rel_contains_special_join(left_relids, sjinfo)
        && !rel_contains_special_join(right_relids, sjinfo)
        && !input_can_commute_past_special_join(root, left_relids, sjinfo)
        && !input_can_commute_past_special_join(root, right_relids, sjinfo)
}

fn join_is_legal(
    root: &PlannerInfo,
    left_rel: &RelOptInfo,
    right_rel: &RelOptInfo,
) -> Option<JoinBuildSpec> {
    let joinrelids = relids_union(&left_rel.relids, &right_rel.relids);
    let mut matched_sj: Option<(&SpecialJoinInfo, bool)> = None;
    let mut must_be_leftjoin = false;

    for sjinfo in &root.join_info_list {
        if violates_full_join_barrier(
            root,
            sjinfo,
            &left_rel.relids,
            &right_rel.relids,
            &joinrelids,
        ) {
            return None;
        }
        if !relids_overlap(&sjinfo.min_righthand, &joinrelids) {
            continue;
        }
        if relids_subset(&joinrelids, &sjinfo.min_righthand) {
            continue;
        }
        if rel_contains_special_join(&left_rel.relids, sjinfo) {
            continue;
        }
        if rel_contains_special_join(&right_rel.relids, sjinfo) {
            continue;
        }

        if let Some(reversed) = rel_matches_special_join(sjinfo, &left_rel.relids, &right_rel.relids) {
            if matched_sj.is_some() {
                return None;
            }
            matched_sj = Some((sjinfo, reversed));
            continue;
        }

        if relids_overlap(&left_rel.relids, &sjinfo.min_righthand)
            && relids_overlap(&right_rel.relids, &sjinfo.min_righthand)
        {
            if input_can_commute_past_special_join(root, &left_rel.relids, sjinfo)
                && input_can_commute_past_special_join(root, &right_rel.relids, sjinfo)
            {
                continue;
            }
            return None;
        }
        if sjinfo.jointype != JoinType::Left || relids_overlap(&joinrelids, &sjinfo.min_lefthand) {
            return None;
        }
        must_be_leftjoin = true;
    }

    if must_be_leftjoin
        && !matched_sj
            .is_some_and(|(sjinfo, _)| sjinfo.jointype == JoinType::Left && sjinfo.lhs_strict)
    {
        return None;
    }

    if let Some((sjinfo, reversed)) = matched_sj {
        return Some(join_spec_for_special_join(sjinfo, reversed));
    }

    Some(JoinBuildSpec {
        kind: JoinType::Inner,
        rtindex: None,
        explicit_qual: None,
    })
}

fn make_join_rel(
    root: &mut PlannerInfo,
    left_rel: &RelOptInfo,
    right_rel: &RelOptInfo,
    catalog: &dyn CatalogLookup,
) -> Option<RelOptInfo> {
    if !relids_disjoint(&left_rel.relids, &right_rel.relids) {
        return None;
    }
    let relids = relids_union(&left_rel.relids, &right_rel.relids);
    let spec = join_is_legal(root, left_rel, right_rel)?;
    let reltarget = join_reltarget(root, &relids, left_rel, right_rel);
    let join_qual = build_join_qual(
        spec.kind,
        spec.explicit_qual.clone(),
        &left_rel.relids,
        &right_rel.relids,
        &root.inner_join_clauses,
    );
    let join_rel_index = match find_join_rel_index(root, &relids) {
        Some(index) => index,
        None => {
            root.join_rel_list.push(RelOptInfo::new(
                relids.clone(),
                RelOptKind::JoinRel,
                reltarget,
            ));
            root.join_rel_list.len() - 1
        }
    };
    let mut candidate_paths = Vec::new();
    let output_rtindex = spec.rtindex.or_else(|| exact_join_rtindex(root, &relids));
    for left_path in &left_rel.pathlist {
        for right_path in &right_rel.pathlist {
            let path = choose_join_plan(
                left_path.clone(),
                right_path.clone(),
                spec.kind,
                join_qual.clone(),
            );
            let path = match output_rtindex {
                Some(rtindex) => maybe_project_join_alias(rtindex, path, root, catalog),
                None => path,
            };
            candidate_paths.push(path);
        }
    }
    let join_rel = root
        .join_rel_list
        .get_mut(join_rel_index)
        .expect("join rel just inserted or found");
    if !join_rel.joininfo.iter().any(|info| info.clause == join_qual) {
        join_rel.joininfo.push(joininfo::make_restrict_info(join_qual.clone()));
    }
    for path in candidate_paths {
        join_rel.add_path(path);
    }
    bestpath::set_cheapest(join_rel);
    Some(join_rel.clone())
}

fn join_search_one_level(root: &mut PlannerInfo, level: usize, catalog: &dyn CatalogLookup) {
    for left_level in 1..level {
        let right_level = level - left_level;
        if left_level > right_level {
            continue;
        }
        let left_rels = base_rels_at_level(root, left_level);
        let right_rels = base_rels_at_level(root, right_level);
        for left_rel in &left_rels {
            for right_rel in &right_rels {
                if left_level == right_level && left_rel.relids >= right_rel.relids {
                    continue;
                }
                let _ = make_join_rel(root, left_rel, right_rel, catalog);
            }
        }
    }
}

fn make_one_rel(root: &mut PlannerInfo, catalog: &dyn CatalogLookup) -> RelOptInfo {
    assign_base_restrictinfo(root);
    root.inner_join_clauses = collect_inner_join_clauses(root);
    set_base_rel_pathlists(root, catalog);
    let query_relids = root.all_query_relids();
    if query_relids.is_empty() {
        let mut rel = RelOptInfo::new(
            Vec::new(),
            RelOptKind::UpperRel,
            PathTarget::new(Vec::new()),
        );
        rel.add_path(optimize_path(
            Path::Result {
                plan_info: PlanEstimate::default(),
            },
            catalog,
        ));
        bestpath::set_cheapest(&mut rel);
        return rel;
    }
    if query_relids.len() == 1 {
        return root.simple_rel_array[query_relids[0]]
            .clone()
            .expect("single base relation reloptinfo");
    }
    for level in 2..=query_relids.len() {
        join_search_one_level(root, level, catalog);
    }
    root.join_rel_list
        .iter()
        .find(|rel| rel.relids == query_relids)
        .cloned()
        .unwrap_or_else(|| panic!("failed to build join rel for relids {:?}", query_relids))
}

fn query_planner(root: &mut PlannerInfo, catalog: &dyn CatalogLookup) -> RelOptInfo {
    let mut rel = make_one_rel(root, catalog);
    if let Some(rtindex) = top_join_rtindex(root) {
        rel = normalize_join_output_rel(root, rel, rtindex, catalog);
    }
    if has_grouping(root) && rel.relids.len() > 1 && rel.reltarget != root.scanjoin_target {
        rel = make_pathtarget_projection_rel(root, rel, &root.scanjoin_target, catalog, false);
    }
    rel
}

fn make_pathtarget_projection_rel(
    root: &PlannerInfo,
    input_rel: RelOptInfo,
    reltarget: &PathTarget,
    catalog: &dyn CatalogLookup,
    allow_identity_elision: bool,
) -> RelOptInfo {
    let targets = root::build_projection_targets_for_pathtarget(reltarget);
    let slot_id = next_synthetic_slot_id();
    let mut rel = RelOptInfo::new(
        input_rel.relids.clone(),
        RelOptKind::UpperRel,
        reltarget.clone(),
    );
    for path in input_rel.pathlist {
        let lowered_targets = lower_targets_for_path(root, &path, &targets);
        if allow_identity_elision && projection_is_identity(&path, &lowered_targets) {
            rel.add_path(path);
            continue;
        }
        rel.add_path(optimize_path(
            Path::Projection {
                plan_info: PlanEstimate::default(),
                slot_id,
                input: Box::new(path),
                targets: lowered_targets,
            },
            catalog,
        ));
    }
    bestpath::set_cheapest(&mut rel);
    rel
}

fn make_filter_rel(
    _root: &PlannerInfo,
    input_rel: RelOptInfo,
    predicate: Expr,
    catalog: &dyn CatalogLookup,
) -> RelOptInfo {
    let mut rel = RelOptInfo::new(
        input_rel.relids.clone(),
        RelOptKind::UpperRel,
        input_rel.reltarget.clone(),
    );
    for path in input_rel.pathlist {
        rel.add_path(optimize_path(
            Path::Filter {
                plan_info: PlanEstimate::default(),
                predicate: rewrite_semantic_expr_for_path(
                    predicate.clone(),
                    &path,
                    &path.output_vars(),
                ),
                input: Box::new(path),
            },
            catalog,
        ));
    }
    bestpath::set_cheapest(&mut rel);
    rel
}

fn make_aggregate_rel(
    root: &mut PlannerInfo,
    input_rel: RelOptInfo,
    catalog: &dyn CatalogLookup,
) -> RelOptInfo {
    let upper_rel_index = upperrels::ensure_upper_rel_index(
        root,
        UpperRelKind::GroupAgg,
        &input_rel.relids,
        root.grouped_target.clone(),
    );
    if !root.upper_rels[upper_rel_index].rel.pathlist.is_empty() {
        return root.upper_rels[upper_rel_index].rel.clone();
    }
    let slot_id = next_synthetic_slot_id();
    let mut rel = RelOptInfo::new(
        input_rel.relids.clone(),
        RelOptKind::UpperRel,
        root.grouped_target.clone(),
    );
    for path in input_rel.pathlist {
        let group_by = root
            .parse
            .group_by
            .iter()
            .cloned()
            .map(|expr| expand_join_rte_vars(root, expr))
            .collect::<Vec<_>>();
        let accumulators = root
            .parse
            .accumulators
            .iter()
            .cloned()
            .map(|mut accum| {
                accum.args = accum
                    .args
                    .into_iter()
                    .map(|arg| expand_join_rte_vars(root, arg))
                    .collect();
                accum
            })
            .collect::<Vec<_>>();
        let agg_output_layout = aggregate_output_vars(slot_id, &group_by, &accumulators);
        let having = root.parse.having_qual.clone().map(|expr| {
            lower_agg_output_expr(
                expand_join_rte_vars(root, expr),
                &group_by,
                &agg_output_layout,
            )
        });
        rel.add_path(optimize_path(
            Path::Aggregate {
                plan_info: PlanEstimate::default(),
                slot_id,
                input: Box::new(path),
                group_by: group_by.clone(),
                accumulators: accumulators.clone(),
                having,
                output_columns: build_aggregate_output_columns(
                    &group_by,
                    &accumulators,
                ),
            },
            catalog,
        ));
    }
    bestpath::set_cheapest(&mut rel);
    root.upper_rels[upper_rel_index].rel = rel.clone();
    rel
}

fn make_project_set_rel(
    root: &mut PlannerInfo,
    input_rel: RelOptInfo,
    targets: &[ProjectSetTarget],
    catalog: &dyn CatalogLookup,
) -> RelOptInfo {
    let upper_rel_index = upperrels::ensure_upper_rel_index(
        root,
        UpperRelKind::ProjectSet,
        &input_rel.relids,
        input_rel.reltarget.clone(),
    );
    if !root.upper_rels[upper_rel_index].rel.pathlist.is_empty() {
        return root.upper_rels[upper_rel_index].rel.clone();
    }
    let slot_id = next_synthetic_slot_id();
    let mut rel = RelOptInfo::new(
        input_rel.relids.clone(),
        RelOptKind::UpperRel,
        input_rel.reltarget.clone(),
    );
    for path in input_rel.pathlist {
        let layout = path.output_vars();
        rel.add_path(optimize_path(
            Path::ProjectSet {
                plan_info: PlanEstimate::default(),
                slot_id,
                input: Box::new(path),
                targets: targets
                    .iter()
                    .cloned()
                    .map(|target| rewrite_project_set_target_against_layout(target, &layout))
                    .collect(),
            },
            catalog,
        ));
    }
    bestpath::set_cheapest(&mut rel);
    root.upper_rels[upper_rel_index].rel = rel.clone();
    rel
}

fn make_ordered_rel(root: &mut PlannerInfo, input_rel: RelOptInfo, catalog: &dyn CatalogLookup) -> RelOptInfo {
    let upper_rel_index = upperrels::ensure_upper_rel_index(
        root,
        UpperRelKind::Ordered,
        &input_rel.relids,
        input_rel.reltarget.clone(),
    );
    if !root.upper_rels[upper_rel_index].rel.pathlist.is_empty() {
        return root.upper_rels[upper_rel_index].rel.clone();
    }
    let required_pathkeys = lower_pathkeys_for_rel(root, &input_rel, &root.query_pathkeys);
    let mut rel = RelOptInfo::new(
        input_rel.relids.clone(),
        RelOptKind::UpperRel,
        input_rel.reltarget.clone(),
    );
    if let Some(path) = bestpath::get_cheapest_path_for_pathkeys(
        &input_rel,
        &required_pathkeys,
        bestpath::CostSelector::Total,
    ) {
        rel.add_path(path.clone());
    }
    if let Some(path) = input_rel.cheapest_total_path() {
        if !bestpath::pathkeys_satisfy(&path.pathkeys(), &required_pathkeys) {
            rel.add_path(optimize_path(
                Path::OrderBy {
                    plan_info: PlanEstimate::default(),
                    items: pathkeys_to_order_items(&required_pathkeys),
                    input: Box::new(path.clone()),
                },
                catalog,
            ));
        } else if rel.pathlist.is_empty() {
            rel.add_path(path.clone());
        }
    }
    bestpath::set_cheapest(&mut rel);
    root.upper_rels[upper_rel_index].rel = rel.clone();
    rel
}

fn make_limit_rel(
    _root: &PlannerInfo,
    input_rel: RelOptInfo,
    limit: Option<usize>,
    offset: usize,
    catalog: &dyn CatalogLookup,
) -> RelOptInfo {
    let mut rel = RelOptInfo::new(
        input_rel.relids.clone(),
        RelOptKind::UpperRel,
        input_rel.reltarget.clone(),
    );
    for path in input_rel.pathlist {
        rel.add_path(optimize_path(
            Path::Limit {
                plan_info: PlanEstimate::default(),
                input: Box::new(path),
                limit,
                offset,
            },
            catalog,
        ));
    }
    bestpath::set_cheapest(&mut rel);
    rel
}

fn make_projection_rel(
    root: &mut PlannerInfo,
    input_rel: RelOptInfo,
    targets: &[TargetEntry],
    catalog: &dyn CatalogLookup,
    allow_identity_elision: bool,
) -> RelOptInfo {
    let reltarget = PathTarget::from_target_list(targets);
    let upper_rel_index = upperrels::ensure_upper_rel_index(
        root,
        UpperRelKind::Final,
        &input_rel.relids,
        reltarget.clone(),
    );
    if !root.upper_rels[upper_rel_index].rel.pathlist.is_empty() {
        return root.upper_rels[upper_rel_index].rel.clone();
    }
    let slot_id = next_synthetic_slot_id();
    let mut rel = RelOptInfo::new(
        input_rel.relids.clone(),
        RelOptKind::UpperRel,
        reltarget,
    );
    for path in input_rel.pathlist {
        let lowered_targets = lower_targets_for_path(root, &path, targets);
        if allow_identity_elision && projection_is_identity(&path, &lowered_targets) {
            rel.add_path(path);
            continue;
        }
        rel.add_path(optimize_path(
            Path::Projection {
                plan_info: PlanEstimate::default(),
                slot_id,
                input: Box::new(path),
                targets: lowered_targets,
            },
            catalog,
        ));
    }
    bestpath::set_cheapest(&mut rel);
    root.upper_rels[upper_rel_index].rel = rel.clone();
    rel
}

fn grouping_planner(
    root: &mut PlannerInfo,
    scanjoin_rel: RelOptInfo,
    catalog: &dyn CatalogLookup,
) -> RelOptInfo {
    let mut current_rel = scanjoin_rel;
    if let Some(predicate) = residual_where_qual(root) {
        current_rel = make_filter_rel(root, current_rel, predicate, catalog);
    }

    let has_grouping = has_grouping(root);
    let mut projection_done = false;
    let final_targets = root.parse.target_list.clone();
    if has_grouping {
        current_rel = make_aggregate_rel(root, current_rel, catalog);
    } else if let Some(project_set) = root.parse.project_set.clone() {
        current_rel = make_project_set_rel(root, current_rel, &project_set, catalog);
        current_rel = make_projection_rel(
            root,
            current_rel,
            &final_targets,
            catalog,
            false,
        );
        projection_done = true;
    }

    if !root.query_pathkeys.is_empty() {
        current_rel = make_ordered_rel(root, current_rel, catalog);
    }

    if root.parse.limit_count.is_some() || root.parse.limit_offset != 0 {
        current_rel = make_limit_rel(
            root,
            current_rel,
            root.parse.limit_count,
            root.parse.limit_offset,
            catalog,
        );
    }

    if has_grouping {
        current_rel = make_projection_rel(root, current_rel, &final_targets, catalog, false);
    } else if !projection_done {
        current_rel = make_projection_rel(root, current_rel, &final_targets, catalog, true);
    }

    root.final_rel = Some(current_rel.clone());
    current_rel
}

fn standard_planner(query: Query, catalog: &dyn CatalogLookup) -> PlannedStmt {
    let mut glob = PlannerGlobal::new();
    let mut root = PlannerInfo::new(query);
    let command_type = root.parse.command_type;
    let scanjoin_rel = query_planner(&mut root, catalog);
    let final_rel = grouping_planner(&mut root, scanjoin_rel, catalog);
    let required_pathkeys = lower_pathkeys_for_rel(&root, &final_rel, &root.query_pathkeys);
    let best_path = bestpath::choose_final_path(&final_rel, &required_pathkeys)
        .cloned()
        .unwrap_or(Path::Result {
            plan_info: PlanEstimate::default(),
        });
    PlannedStmt {
        command_type,
        plan_tree: finalize_plan_subqueries(create_plan(best_path), catalog, &mut glob.subplans),
        subplans: glob.subplans,
    }
}

pub(crate) fn planner(query: Query, catalog: &dyn CatalogLookup) -> PlannedStmt {
    standard_planner(query, catalog)
}

fn append_planned_subquery(planned_stmt: PlannedStmt, subplans: &mut Vec<Plan>) -> usize {
    let base = subplans.len();
    subplans.extend(
        planned_stmt
            .subplans
            .into_iter()
            .map(|plan| rebase_plan_subplan_ids(plan, base)),
    );
    let plan_id = subplans.len();
    subplans.push(rebase_plan_subplan_ids(planned_stmt.plan_tree, base));
    plan_id
}

fn lower_sublink_to_subplan(
    sublink: SubLink,
    catalog: &dyn CatalogLookup,
    subplans: &mut Vec<Plan>,
) -> Expr {
    let testexpr = sublink
        .testexpr
        .map(|expr| Box::new(finalize_expr_subqueries(*expr, catalog, subplans)));
    let first_col_type = sublink
        .subselect
        .target_list
        .first()
        .map(|target| target.sql_type);
    let plan_id = append_planned_subquery(planner(*sublink.subselect, catalog), subplans);
    Expr::SubPlan(Box::new(SubPlan {
        sublink_type: sublink.sublink_type,
        testexpr,
        first_col_type,
        plan_id,
    }))
}

pub(crate) fn finalize_expr_subqueries(
    expr: Expr,
    catalog: &dyn CatalogLookup,
    subplans: &mut Vec<Plan>,
) -> Expr {
    match expr {
        other @ (Expr::Var(_)
        | Expr::Column(_)
        | Expr::OuterColumn { .. }
        | Expr::Const(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. }) => other,
        Expr::Op(op) => Expr::Op(Box::new(crate::include::nodes::primnodes::OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(crate::include::nodes::primnodes::BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
                .collect(),
            ..*bool_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(crate::include::nodes::primnodes::FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
                .collect(),
            ..*func
        })),
        Expr::SubLink(sublink) => lower_sublink_to_subplan(*sublink, catalog, subplans),
        Expr::SubPlan(subplan) => Expr::SubPlan(Box::new(SubPlan {
            testexpr: subplan
                .testexpr
                .map(|expr| Box::new(finalize_expr_subqueries(*expr, catalog, subplans))),
            ..*subplan
        })),
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(
            crate::include::nodes::primnodes::ScalarArrayOpExpr {
                left: Box::new(finalize_expr_subqueries(*saop.left, catalog, subplans)),
                right: Box::new(finalize_expr_subqueries(*saop.right, catalog, subplans)),
                ..*saop
            },
        )),
        Expr::Cast(inner, ty) => Expr::Cast(
            Box::new(finalize_expr_subqueries(*inner, catalog, subplans)),
            ty,
        ),
        Expr::IsNull(inner) => Expr::IsNull(Box::new(finalize_expr_subqueries(
            *inner, catalog, subplans,
        ))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(finalize_expr_subqueries(
            *inner, catalog, subplans,
        ))),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            expr: Box::new(finalize_expr_subqueries(*expr, catalog, subplans)),
            pattern: Box::new(finalize_expr_subqueries(*pattern, catalog, subplans)),
            escape: escape.map(|expr| Box::new(finalize_expr_subqueries(*expr, catalog, subplans))),
            case_insensitive,
            negated,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(finalize_expr_subqueries(*expr, catalog, subplans)),
            pattern: Box::new(finalize_expr_subqueries(*pattern, catalog, subplans)),
            escape: escape.map(|expr| Box::new(finalize_expr_subqueries(*expr, catalog, subplans))),
            negated,
        },
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|element| finalize_expr_subqueries(element, catalog, subplans))
                .collect(),
            array_type,
        },
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(finalize_expr_subqueries(*left, catalog, subplans)),
            Box::new(finalize_expr_subqueries(*right, catalog, subplans)),
        ),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(finalize_expr_subqueries(*array, catalog, subplans)),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript
                        .lower
                        .map(|expr| finalize_expr_subqueries(expr, catalog, subplans)),
                    upper: subscript
                        .upper
                        .map(|expr| finalize_expr_subqueries(expr, catalog, subplans)),
                })
                .collect(),
        },
        other => other,
    }
}

fn finalize_set_returning_call(
    call: SetReturningCall,
    catalog: &dyn CatalogLookup,
    subplans: &mut Vec<Plan>,
) -> SetReturningCall {
    match call {
        SetReturningCall::GenerateSeries {
            func_oid,
            func_variadic,
            start,
            stop,
            step,
            output,
        } => SetReturningCall::GenerateSeries {
            func_oid,
            func_variadic,
            start: finalize_expr_subqueries(start, catalog, subplans),
            stop: finalize_expr_subqueries(stop, catalog, subplans),
            step: finalize_expr_subqueries(step, catalog, subplans),
            output,
        },
        SetReturningCall::Unnest {
            func_oid,
            func_variadic,
            args,
            output_columns,
        } => SetReturningCall::Unnest {
            func_oid,
            func_variadic,
            args: args
                .into_iter()
                .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
                .collect(),
            output_columns,
        },
        SetReturningCall::JsonTableFunction {
            func_oid,
            func_variadic,
            kind,
            args,
            output_columns,
        } => SetReturningCall::JsonTableFunction {
            func_oid,
            func_variadic,
            kind,
            args: args
                .into_iter()
                .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
                .collect(),
            output_columns,
        },
        SetReturningCall::RegexTableFunction {
            func_oid,
            func_variadic,
            kind,
            args,
            output_columns,
        } => SetReturningCall::RegexTableFunction {
            func_oid,
            func_variadic,
            kind,
            args: args
                .into_iter()
                .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
                .collect(),
            output_columns,
        },
        SetReturningCall::TextSearchTableFunction {
            kind,
            args,
            output_columns,
        } => SetReturningCall::TextSearchTableFunction {
            kind,
            args: args
                .into_iter()
                .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
                .collect(),
            output_columns,
        },
    }
}

fn finalize_agg_accum(
    accum: AggAccum,
    catalog: &dyn CatalogLookup,
    subplans: &mut Vec<Plan>,
) -> AggAccum {
    let AggAccum {
        aggfnoid,
        agg_variadic,
        args,
        distinct,
        sql_type,
    } = accum;
    AggAccum {
        aggfnoid,
        agg_variadic,
        args: args
            .into_iter()
            .map(|arg| finalize_expr_subqueries(arg, catalog, subplans))
            .collect(),
        distinct,
        sql_type,
    }
}

fn rebase_expr_subplan_ids(expr: Expr, base: usize) -> Expr {
    match expr {
        other @ (Expr::Var(_)
        | Expr::Column(_)
        | Expr::OuterColumn { .. }
        | Expr::Const(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. }) => other,
        Expr::Op(op) => Expr::Op(Box::new(crate::include::nodes::primnodes::OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| rebase_expr_subplan_ids(arg, base))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(crate::include::nodes::primnodes::BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| rebase_expr_subplan_ids(arg, base))
                .collect(),
            ..*bool_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(crate::include::nodes::primnodes::FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| rebase_expr_subplan_ids(arg, base))
                .collect(),
            ..*func
        })),
        Expr::SubLink(sublink) => Expr::SubLink(Box::new(SubLink {
            testexpr: sublink
                .testexpr
                .map(|expr| Box::new(rebase_expr_subplan_ids(*expr, base))),
            ..*sublink
        })),
        Expr::SubPlan(subplan) => Expr::SubPlan(Box::new(SubPlan {
            testexpr: subplan
                .testexpr
                .map(|expr| Box::new(rebase_expr_subplan_ids(*expr, base))),
            first_col_type: subplan.first_col_type,
            plan_id: subplan.plan_id + base,
            sublink_type: subplan.sublink_type,
        })),
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(
            crate::include::nodes::primnodes::ScalarArrayOpExpr {
                left: Box::new(rebase_expr_subplan_ids(*saop.left, base)),
                right: Box::new(rebase_expr_subplan_ids(*saop.right, base)),
                ..*saop
            },
        )),
        Expr::Cast(inner, ty) => Expr::Cast(Box::new(rebase_expr_subplan_ids(*inner, base)), ty),
        Expr::IsNull(inner) => Expr::IsNull(Box::new(rebase_expr_subplan_ids(*inner, base))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(rebase_expr_subplan_ids(*inner, base))),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            expr: Box::new(rebase_expr_subplan_ids(*expr, base)),
            pattern: Box::new(rebase_expr_subplan_ids(*pattern, base)),
            escape: escape.map(|expr| Box::new(rebase_expr_subplan_ids(*expr, base))),
            case_insensitive,
            negated,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(rebase_expr_subplan_ids(*expr, base)),
            pattern: Box::new(rebase_expr_subplan_ids(*pattern, base)),
            escape: escape.map(|expr| Box::new(rebase_expr_subplan_ids(*expr, base))),
            negated,
        },
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|expr| rebase_expr_subplan_ids(expr, base))
                .collect(),
            array_type,
        },
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(rebase_expr_subplan_ids(*left, base)),
            Box::new(rebase_expr_subplan_ids(*right, base)),
        ),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(rebase_expr_subplan_ids(*array, base)),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript
                        .lower
                        .map(|expr| rebase_expr_subplan_ids(expr, base)),
                    upper: subscript
                        .upper
                        .map(|expr| rebase_expr_subplan_ids(expr, base)),
                })
                .collect(),
        },
        other => other,
    }
}

fn rebase_set_returning_call_subplan_ids(call: SetReturningCall, base: usize) -> SetReturningCall {
    match call {
        SetReturningCall::GenerateSeries {
            func_oid,
            func_variadic,
            start,
            stop,
            step,
            output,
        } => SetReturningCall::GenerateSeries {
            func_oid,
            func_variadic,
            start: rebase_expr_subplan_ids(start, base),
            stop: rebase_expr_subplan_ids(stop, base),
            step: rebase_expr_subplan_ids(step, base),
            output,
        },
        SetReturningCall::Unnest {
            func_oid,
            func_variadic,
            args,
            output_columns,
        } => SetReturningCall::Unnest {
            func_oid,
            func_variadic,
            args: args
                .into_iter()
                .map(|arg| rebase_expr_subplan_ids(arg, base))
                .collect(),
            output_columns,
        },
        SetReturningCall::JsonTableFunction {
            func_oid,
            func_variadic,
            kind,
            args,
            output_columns,
        } => SetReturningCall::JsonTableFunction {
            func_oid,
            func_variadic,
            kind,
            args: args
                .into_iter()
                .map(|arg| rebase_expr_subplan_ids(arg, base))
                .collect(),
            output_columns,
        },
        SetReturningCall::RegexTableFunction {
            func_oid,
            func_variadic,
            kind,
            args,
            output_columns,
        } => SetReturningCall::RegexTableFunction {
            func_oid,
            func_variadic,
            kind,
            args: args
                .into_iter()
                .map(|arg| rebase_expr_subplan_ids(arg, base))
                .collect(),
            output_columns,
        },
        SetReturningCall::TextSearchTableFunction {
            kind,
            args,
            output_columns,
        } => SetReturningCall::TextSearchTableFunction {
            kind,
            args: args
                .into_iter()
                .map(|arg| rebase_expr_subplan_ids(arg, base))
                .collect(),
            output_columns,
        },
    }
}

fn rebase_agg_accum_subplan_ids(accum: AggAccum, base: usize) -> AggAccum {
    AggAccum {
        args: accum
            .args
            .into_iter()
            .map(|arg| rebase_expr_subplan_ids(arg, base))
            .collect(),
        ..accum
    }
}

fn rebase_plan_subplan_ids(plan: Plan, base: usize) -> Plan {
    match plan {
        Plan::Result { .. } | Plan::SeqScan { .. } | Plan::IndexScan { .. } => plan,
        Plan::Hash {
            plan_info,
            input,
            hash_keys,
        } => Plan::Hash {
            plan_info,
            input: Box::new(rebase_plan_subplan_ids(*input, base)),
            hash_keys: hash_keys
                .into_iter()
                .map(|expr| rebase_expr_subplan_ids(expr, base))
                .collect(),
        },
        Plan::NestedLoopJoin {
            plan_info,
            left,
            right,
            kind,
            on,
        } => Plan::NestedLoopJoin {
            plan_info,
            left: Box::new(rebase_plan_subplan_ids(*left, base)),
            right: Box::new(rebase_plan_subplan_ids(*right, base)),
            kind,
            on: rebase_expr_subplan_ids(on, base),
        },
        Plan::HashJoin {
            plan_info,
            left,
            right,
            kind,
            hash_clauses,
            hash_keys,
            join_qual,
        } => Plan::HashJoin {
            plan_info,
            left: Box::new(rebase_plan_subplan_ids(*left, base)),
            right: Box::new(rebase_plan_subplan_ids(*right, base)),
            kind,
            hash_clauses: hash_clauses
                .into_iter()
                .map(|expr| rebase_expr_subplan_ids(expr, base))
                .collect(),
            hash_keys: hash_keys
                .into_iter()
                .map(|expr| rebase_expr_subplan_ids(expr, base))
                .collect(),
            join_qual: join_qual.map(|expr| rebase_expr_subplan_ids(expr, base)),
        },
        Plan::Filter {
            plan_info,
            input,
            predicate,
        } => Plan::Filter {
            plan_info,
            input: Box::new(rebase_plan_subplan_ids(*input, base)),
            predicate: rebase_expr_subplan_ids(predicate, base),
        },
        Plan::OrderBy {
            plan_info,
            input,
            items,
        } => Plan::OrderBy {
            plan_info,
            input: Box::new(rebase_plan_subplan_ids(*input, base)),
            items: items
                .into_iter()
                .map(|item| crate::include::nodes::primnodes::OrderByEntry {
                    expr: rebase_expr_subplan_ids(item.expr, base),
                    descending: item.descending,
                    nulls_first: item.nulls_first,
                })
                .collect(),
        },
        Plan::Limit {
            plan_info,
            input,
            limit,
            offset,
        } => Plan::Limit {
            plan_info,
            input: Box::new(rebase_plan_subplan_ids(*input, base)),
            limit,
            offset,
        },
        Plan::Projection {
            plan_info,
            input,
            targets,
        } => Plan::Projection {
            plan_info,
            input: Box::new(rebase_plan_subplan_ids(*input, base)),
            targets: targets
                .into_iter()
                .map(|target| crate::include::nodes::primnodes::TargetEntry {
                    expr: rebase_expr_subplan_ids(target.expr, base),
                    ..target
                })
                .collect(),
        },
        Plan::Aggregate {
            plan_info,
            input,
            group_by,
            accumulators,
            having,
            output_columns,
        } => Plan::Aggregate {
            plan_info,
            input: Box::new(rebase_plan_subplan_ids(*input, base)),
            group_by: group_by
                .into_iter()
                .map(|expr| rebase_expr_subplan_ids(expr, base))
                .collect(),
            accumulators: accumulators
                .into_iter()
                .map(|accum| rebase_agg_accum_subplan_ids(accum, base))
                .collect(),
            having: having.map(|expr| rebase_expr_subplan_ids(expr, base)),
            output_columns,
        },
        Plan::FunctionScan { plan_info, call } => Plan::FunctionScan {
            plan_info,
            call: rebase_set_returning_call_subplan_ids(call, base),
        },
        Plan::Values {
            plan_info,
            rows,
            output_columns,
        } => Plan::Values {
            plan_info,
            rows: rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|expr| rebase_expr_subplan_ids(expr, base))
                        .collect()
                })
                .collect(),
            output_columns,
        },
        Plan::ProjectSet {
            plan_info,
            input,
            targets,
        } => Plan::ProjectSet {
            plan_info,
            input: Box::new(rebase_plan_subplan_ids(*input, base)),
            targets: targets
                .into_iter()
                .map(|target| match target {
                    ProjectSetTarget::Scalar(entry) => {
                        ProjectSetTarget::Scalar(crate::include::nodes::primnodes::TargetEntry {
                            expr: rebase_expr_subplan_ids(entry.expr, base),
                            ..entry
                        })
                    }
                    ProjectSetTarget::Set {
                        name,
                        call,
                        sql_type,
                        column_index,
                    } => ProjectSetTarget::Set {
                        name,
                        call: rebase_set_returning_call_subplan_ids(call, base),
                        sql_type,
                        column_index,
                    },
                })
                .collect(),
        },
    }
}

pub(crate) fn finalize_plan_subqueries(
    plan: Plan,
    catalog: &dyn CatalogLookup,
    subplans: &mut Vec<Plan>,
) -> Plan {
    match plan {
        Plan::Result { .. } | Plan::SeqScan { .. } | Plan::IndexScan { .. } => plan,
        Plan::Hash {
            plan_info,
            input,
            hash_keys,
        } => Plan::Hash {
            plan_info,
            input: Box::new(finalize_plan_subqueries(*input, catalog, subplans)),
            hash_keys: hash_keys
                .into_iter()
                .map(|expr| finalize_expr_subqueries(expr, catalog, subplans))
                .collect(),
        },
        Plan::NestedLoopJoin {
            plan_info,
            left,
            right,
            kind,
            on,
        } => Plan::NestedLoopJoin {
            plan_info,
            left: Box::new(finalize_plan_subqueries(*left, catalog, subplans)),
            right: Box::new(finalize_plan_subqueries(*right, catalog, subplans)),
            kind,
            on: finalize_expr_subqueries(on, catalog, subplans),
        },
        Plan::HashJoin {
            plan_info,
            left,
            right,
            kind,
            hash_clauses,
            hash_keys,
            join_qual,
        } => Plan::HashJoin {
            plan_info,
            left: Box::new(finalize_plan_subqueries(*left, catalog, subplans)),
            right: Box::new(finalize_plan_subqueries(*right, catalog, subplans)),
            kind,
            hash_clauses: hash_clauses
                .into_iter()
                .map(|expr| finalize_expr_subqueries(expr, catalog, subplans))
                .collect(),
            hash_keys: hash_keys
                .into_iter()
                .map(|expr| finalize_expr_subqueries(expr, catalog, subplans))
                .collect(),
            join_qual: join_qual.map(|expr| finalize_expr_subqueries(expr, catalog, subplans)),
        },
        Plan::Filter {
            plan_info,
            input,
            predicate,
        } => Plan::Filter {
            plan_info,
            input: Box::new(finalize_plan_subqueries(*input, catalog, subplans)),
            predicate: finalize_expr_subqueries(predicate, catalog, subplans),
        },
        Plan::OrderBy {
            plan_info,
            input,
            items,
        } => Plan::OrderBy {
            plan_info,
            input: Box::new(finalize_plan_subqueries(*input, catalog, subplans)),
            items: items
                .into_iter()
                .map(|item| crate::include::nodes::primnodes::OrderByEntry {
                    expr: finalize_expr_subqueries(item.expr, catalog, subplans),
                    descending: item.descending,
                    nulls_first: item.nulls_first,
                })
                .collect(),
        },
        Plan::Limit {
            plan_info,
            input,
            limit,
            offset,
        } => Plan::Limit {
            plan_info,
            input: Box::new(finalize_plan_subqueries(*input, catalog, subplans)),
            limit,
            offset,
        },
        Plan::Projection {
            plan_info,
            input,
            targets,
        } => Plan::Projection {
            plan_info,
            input: Box::new(finalize_plan_subqueries(*input, catalog, subplans)),
            targets: targets
                .into_iter()
                .map(|target| crate::include::nodes::primnodes::TargetEntry {
                    name: target.name,
                    expr: finalize_expr_subqueries(target.expr, catalog, subplans),
                    sql_type: target.sql_type,
                    resno: target.resno,
                    ressortgroupref: target.ressortgroupref,
                    resjunk: target.resjunk,
                })
                .collect(),
        },
        Plan::Aggregate {
            plan_info,
            input,
            group_by,
            accumulators,
            having,
            output_columns,
        } => Plan::Aggregate {
            plan_info,
            input: Box::new(finalize_plan_subqueries(*input, catalog, subplans)),
            group_by: group_by
                .into_iter()
                .map(|expr| finalize_expr_subqueries(expr, catalog, subplans))
                .collect(),
            accumulators: accumulators
                .into_iter()
                .map(|accum| finalize_agg_accum(accum, catalog, subplans))
                .collect(),
            having: having.map(|expr| finalize_expr_subqueries(expr, catalog, subplans)),
            output_columns,
        },
        Plan::FunctionScan { plan_info, call } => Plan::FunctionScan {
            plan_info,
            call: finalize_set_returning_call(call, catalog, subplans),
        },
        Plan::Values {
            plan_info,
            rows,
            output_columns,
        } => Plan::Values {
            plan_info,
            rows: rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|expr| finalize_expr_subqueries(expr, catalog, subplans))
                        .collect()
                })
                .collect(),
            output_columns,
        },
        Plan::ProjectSet {
            plan_info,
            input,
            targets,
        } => Plan::ProjectSet {
            plan_info,
            input: Box::new(finalize_plan_subqueries(*input, catalog, subplans)),
            targets: targets
                .into_iter()
                .map(|target| match target {
                    ProjectSetTarget::Scalar(entry) => {
                        ProjectSetTarget::Scalar(crate::include::nodes::primnodes::TargetEntry {
                            name: entry.name,
                            expr: finalize_expr_subqueries(entry.expr, catalog, subplans),
                            sql_type: entry.sql_type,
                            resno: entry.resno,
                            ressortgroupref: entry.ressortgroupref,
                            resjunk: entry.resjunk,
                        })
                    }
                    ProjectSetTarget::Set {
                        name,
                        call,
                        sql_type,
                        column_index,
                    } => ProjectSetTarget::Set {
                        name,
                        call: finalize_set_returning_call(call, catalog, subplans),
                        sql_type,
                        column_index,
                    },
                })
                .collect(),
        },
    }
}

pub(super) fn optimize_path(plan: Path, catalog: &dyn CatalogLookup) -> Path {
    if plan.plan_info() != PlanEstimate::default() {
        return plan;
    }
    match try_optimize_access_subtree(plan, catalog) {
        Ok(plan) => plan,
        Err(plan) => match plan {
            Path::Result { .. } => Path::Result {
                plan_info: PlanEstimate::new(0.0, 0.0, 1.0, 0),
            },
            Path::SeqScan {
                source_id,
                rel,
                relation_oid,
                toast,
                desc,
                ..
            } => {
                let stats = relation_stats(catalog, relation_oid, &desc);
                let base = seq_scan_estimate(&stats);
                Path::SeqScan {
                    plan_info: base,
                    source_id,
                    rel,
                    relation_oid,
                    toast,
                    desc,
                }
            }
            Path::IndexScan {
                source_id,
                rel,
                index_rel,
                am_oid,
                toast,
                desc,
                index_meta,
                keys,
                direction,
                pathkeys,
                ..
            } => {
                let stats = relation_stats(catalog, index_meta.indrelid, &desc);
                let rows = clamp_rows(stats.reltuples * DEFAULT_EQ_SEL);
                let pages = catalog
                    .class_row_by_oid(index_meta.indrelid)
                    .map(|row| row.relpages.max(1) as f64)
                    .unwrap_or(DEFAULT_NUM_PAGES);
                let plan_info = PlanEstimate::new(
                    CPU_OPERATOR_COST,
                    RANDOM_PAGE_COST + pages.min(rows.max(1.0)) + rows * CPU_INDEX_TUPLE_COST,
                    rows,
                    stats.width,
                );
                Path::IndexScan {
                    plan_info,
                    source_id,
                    rel,
                    index_rel,
                    am_oid,
                    toast,
                    desc,
                    index_meta,
                    keys,
                    direction,
                    pathkeys,
                }
            }
            Path::Filter {
                input, predicate, ..
            } => {
                let input = optimize_path(*input, catalog);
                let input_info = input.plan_info();
                let input_rows = input_info.plan_rows.as_f64();
                let selectivity = clause_selectivity(&predicate, None, input_rows);
                let rows = clamp_rows(input_rows * selectivity);
                let qual_cost = predicate_cost(&predicate) * input_rows * CPU_OPERATOR_COST;
                Path::Filter {
                    plan_info: PlanEstimate::new(
                        input_info.startup_cost.as_f64(),
                        input_info.total_cost.as_f64() + qual_cost,
                        rows,
                        input_info.plan_width,
                    ),
                    input: Box::new(input),
                    predicate,
                }
            }
            Path::OrderBy { input, items, .. } => {
                let input = optimize_path(*input, catalog);
                let input_info = input.plan_info();
                let sort_cost = estimate_sort_cost(input_info.plan_rows.as_f64(), items.len());
                Path::OrderBy {
                    plan_info: PlanEstimate::new(
                        input_info.total_cost.as_f64(),
                        input_info.total_cost.as_f64() + sort_cost,
                        input_info.plan_rows.as_f64(),
                        input_info.plan_width,
                    ),
                    input: Box::new(input),
                    items,
                }
            }
            Path::Limit {
                input,
                limit,
                offset,
                ..
            } => {
                let input = optimize_path(*input, catalog);
                let input_info = input.plan_info();
                let input_rows = input_info.plan_rows.as_f64();
                let requested = limit
                    .map(|limit| limit.saturating_add(offset) as f64)
                    .unwrap_or(input_rows);
                let fraction = if input_rows <= 0.0 {
                    1.0
                } else {
                    (requested / input_rows).clamp(0.0, 1.0)
                };
                let rows = limit
                    .map(|limit| {
                        clamp_rows((input_rows - offset as f64).max(0.0).min(limit as f64))
                    })
                    .unwrap_or_else(|| clamp_rows((input_rows - offset as f64).max(0.0)));
                let total = input_info.startup_cost.as_f64()
                    + (input_info.total_cost.as_f64() - input_info.startup_cost.as_f64())
                        * fraction;
                Path::Limit {
                    plan_info: PlanEstimate::new(
                        input_info.startup_cost.as_f64(),
                        total,
                        rows,
                        input_info.plan_width,
                    ),
                    input: Box::new(input),
                    limit,
                    offset,
                }
            }
            Path::Projection {
                input,
                targets,
                slot_id,
                ..
            } => {
                let input = optimize_path(*input, catalog);
                let input_info = input.plan_info();
                let width = targets
                    .iter()
                    .map(|target| estimate_sql_type_width(target.sql_type))
                    .sum();
                Path::Projection {
                    plan_info: PlanEstimate::new(
                        input_info.startup_cost.as_f64(),
                        input_info.total_cost.as_f64()
                            + input_info.plan_rows.as_f64() * CPU_OPERATOR_COST,
                        input_info.plan_rows.as_f64(),
                        width,
                    ),
                    slot_id,
                    input: Box::new(input),
                    targets,
                }
            }
            Path::Aggregate {
                input,
                group_by,
                accumulators,
                having,
                output_columns,
                slot_id,
                ..
            } => {
                let input = optimize_path(*input, catalog);
                let input_info = input.plan_info();
                let rows = if group_by.is_empty() {
                    1.0
                } else {
                    clamp_rows((input_info.plan_rows.as_f64() * 0.1).max(1.0))
                };
                let width = output_columns
                    .iter()
                    .map(|col| estimate_sql_type_width(col.sql_type))
                    .sum();
                let total = input_info.total_cost.as_f64()
                    + input_info.plan_rows.as_f64()
                        * (accumulators.len().max(1) as f64)
                        * CPU_OPERATOR_COST;
                Path::Aggregate {
                    plan_info: PlanEstimate::new(total, total, rows, width),
                    slot_id,
                    input: Box::new(input),
                    group_by,
                    accumulators,
                    having,
                    output_columns,
                }
            }
            Path::NestedLoopJoin {
                left,
                right,
                kind,
                on,
                ..
            } => {
                let left = optimize_path(*left, catalog);
                let right = optimize_path(*right, catalog);
                choose_join_plan(left, right, kind, on)
            }
            Path::FunctionScan { call, slot_id, .. } => {
                let output_columns = call.output_columns();
                let width = output_columns
                    .iter()
                    .map(|col| estimate_sql_type_width(col.sql_type))
                    .sum();
                Path::FunctionScan {
                    plan_info: PlanEstimate::new(0.0, 10.0, 1000.0, width),
                    slot_id,
                    call,
                }
            }
            Path::Values {
                rows,
                output_columns,
                slot_id,
                ..
            } => {
                let width = output_columns
                    .iter()
                    .map(|col| estimate_sql_type_width(col.sql_type))
                    .sum();
                let row_count = rows.len().max(1) as f64;
                Path::Values {
                    plan_info: PlanEstimate::new(0.0, row_count * CPU_TUPLE_COST, row_count, width),
                    slot_id,
                    rows,
                    output_columns,
                }
            }
            Path::ProjectSet {
                input,
                targets,
                slot_id,
                ..
            } => {
                let input = optimize_path(*input, catalog);
                let input_info = input.plan_info();
                let rows = clamp_rows(input_info.plan_rows.as_f64() * 10.0);
                let width = targets
                    .iter()
                    .map(|target| match target {
                        ProjectSetTarget::Scalar(entry) => estimate_sql_type_width(entry.sql_type),
                        ProjectSetTarget::Set { sql_type, .. } => {
                            estimate_sql_type_width(*sql_type)
                        }
                    })
                    .sum();
                Path::ProjectSet {
                    plan_info: PlanEstimate::new(
                        input_info.startup_cost.as_f64(),
                        input_info.total_cost.as_f64()
                            + input_info.plan_rows.as_f64() * CPU_OPERATOR_COST,
                        rows,
                        width,
                    ),
                    slot_id,
                    input: Box::new(input),
                    targets,
                }
            }
        },
    }
}

fn try_optimize_access_subtree(plan: Path, catalog: &dyn CatalogLookup) -> Result<Path, Path> {
    let (source_id, rel, relation_oid, toast, desc, filter, order_items) = match plan {
        Path::SeqScan {
            source_id,
            rel,
            relation_oid,
            toast,
            desc,
            ..
        } => (source_id, rel, relation_oid, toast, desc, None, None),
        Path::Filter {
            input, predicate, ..
        } => match *input {
            Path::SeqScan {
                source_id,
                rel,
                relation_oid,
                toast,
                desc,
                ..
            } => (
                source_id,
                rel,
                relation_oid,
                toast,
                desc,
                Some(predicate),
                None,
            ),
            other => {
                return Err(Path::Filter {
                    plan_info: PlanEstimate::default(),
                    input: Box::new(other),
                    predicate,
                });
            }
        },
        Path::OrderBy { input, items, .. } => match *input {
            Path::SeqScan {
                source_id,
                rel,
                relation_oid,
                toast,
                desc,
                ..
            } => (source_id, rel, relation_oid, toast, desc, None, Some(items)),
            Path::Filter {
                input, predicate, ..
            } => match *input {
                Path::SeqScan {
                    source_id,
                    rel,
                    relation_oid,
                    toast,
                    desc,
                    ..
                } => (
                    source_id,
                    rel,
                    relation_oid,
                    toast,
                    desc,
                    Some(predicate),
                    Some(items),
                ),
                other => {
                    return Err(Path::OrderBy {
                        plan_info: PlanEstimate::default(),
                        input: Box::new(Path::Filter {
                            plan_info: PlanEstimate::default(),
                            input: Box::new(other),
                            predicate,
                        }),
                        items,
                    });
                }
            },
            other => {
                return Err(Path::OrderBy {
                    plan_info: PlanEstimate::default(),
                    input: Box::new(other),
                    items,
                });
            }
        },
        other => return Err(other),
    };

    let filter = filter;
    let order_items = order_items;

    let stats = relation_stats(catalog, relation_oid, &desc);
    let mut best = estimate_seqscan_candidate(
        source_id,
        rel,
        relation_oid,
        toast,
        desc.clone(),
        &stats,
        filter.clone(),
        order_items.clone(),
    );
    let indexes = catalog.index_relations_for_heap(relation_oid);
    for index in indexes.iter().filter(|index| {
        index.index_meta.indisvalid
            && index.index_meta.indisready
            && !index.index_meta.indkey.is_empty()
            && index.index_meta.am_oid == BTREE_AM_OID
    }) {
        let Some(spec) = build_index_path_spec(filter.as_ref(), order_items.as_deref(), index)
        else {
            continue;
        };
        let candidate = estimate_index_candidate(
            source_id,
            rel,
            toast,
            desc.clone(),
            &stats,
            spec,
            order_items.clone(),
            catalog,
        );
        if candidate.total_cost < best.total_cost {
            best = candidate;
        }
    }
    Ok(best.plan)
}

fn relation_stats(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    desc: &RelationDesc,
) -> RelationStats {
    let class_row = catalog.class_row_by_oid(relation_oid);
    let relpages = class_row
        .as_ref()
        .map(|row| row.relpages.max(1) as f64)
        .unwrap_or(DEFAULT_NUM_PAGES);
    let reltuples = class_row
        .as_ref()
        .map(|row| {
            if row.reltuples > 0.0 {
                row.reltuples
            } else {
                DEFAULT_NUM_ROWS
            }
        })
        .unwrap_or(DEFAULT_NUM_ROWS);
    let stats = catalog
        .statistic_rows_for_relation(relation_oid)
        .into_iter()
        .map(|row| (row.staattnum, row))
        .collect::<HashMap<_, _>>();
    RelationStats {
        relpages,
        reltuples,
        width: estimate_relation_width(desc, &stats),
        stats_by_attnum: stats,
    }
}

fn estimate_seqscan_candidate(
    source_id: usize,
    rel: RelFileLocator,
    relation_oid: u32,
    toast: Option<ToastRelationRef>,
    desc: RelationDesc,
    stats: &RelationStats,
    filter: Option<Expr>,
    order_items: Option<Vec<OrderByEntry>>,
) -> AccessCandidate {
    let scan_info = seq_scan_estimate(stats);
    let mut total_cost = scan_info.total_cost.as_f64();
    let mut plan = Path::SeqScan {
        plan_info: scan_info,
        source_id,
        rel,
        relation_oid,
        toast,
        desc,
    };
    let mut current_rows = scan_info.plan_rows.as_f64();
    let width = scan_info.plan_width;

    if let Some(predicate) = filter {
        let selectivity = clause_selectivity(&predicate, Some(stats), stats.reltuples);
        current_rows = clamp_rows(stats.reltuples * selectivity);
        total_cost += stats.reltuples * predicate_cost(&predicate) * CPU_OPERATOR_COST;
        plan = Path::Filter {
            plan_info: PlanEstimate::new(
                scan_info.startup_cost.as_f64(),
                total_cost,
                current_rows,
                width,
            ),
            input: Box::new(plan),
            predicate,
        };
    }

    if let Some(items) = order_items {
        total_cost += estimate_sort_cost(current_rows, items.len());
        plan = Path::OrderBy {
            plan_info: PlanEstimate::new(
                total_cost - estimate_sort_cost(current_rows, items.len()),
                total_cost,
                current_rows,
                width,
            ),
            input: Box::new(plan),
            items,
        };
    }

    AccessCandidate { total_cost, plan }
}

fn estimate_index_candidate(
    source_id: usize,
    rel: RelFileLocator,
    toast: Option<ToastRelationRef>,
    desc: RelationDesc,
    stats: &RelationStats,
    spec: IndexPathSpec,
    order_items: Option<Vec<OrderByEntry>>,
    catalog: &dyn CatalogLookup,
) -> AccessCandidate {
    let index_class = catalog.class_row_by_oid(spec.index.relation_oid);
    let index_pages = index_class
        .as_ref()
        .map(|row| row.relpages.max(1) as f64)
        .unwrap_or(DEFAULT_NUM_PAGES);

    let used_sel = spec
        .used_quals
        .iter()
        .map(|expr| clause_selectivity(expr, Some(stats), stats.reltuples))
        .product::<f64>()
        .clamp(0.0, 1.0);
    let index_rows = clamp_rows(stats.reltuples * used_sel);
    let base_cost = RANDOM_PAGE_COST
        + index_pages.min(index_rows.max(1.0)) * RANDOM_PAGE_COST
        + index_rows * (CPU_INDEX_TUPLE_COST + CPU_TUPLE_COST);
    let scan_info = PlanEstimate::new(CPU_OPERATOR_COST, base_cost, index_rows, stats.width);
    let mut total_cost = scan_info.total_cost.as_f64();
    let mut current_rows = scan_info.plan_rows.as_f64();
    let native_pathkeys = if spec.removes_order {
        order_items
            .as_ref()
            .map(|items| {
                items
                    .iter()
                    .map(|item| PathKey {
                        expr: item.expr.clone(),
                        descending: item.descending,
                        nulls_first: item.nulls_first,
                    })
                    .collect()
            })
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let mut plan = Path::IndexScan {
        plan_info: scan_info,
        source_id,
        rel,
        index_rel: spec.index.rel,
        am_oid: spec.index.index_meta.am_oid,
        toast,
        desc,
        index_meta: spec.index.index_meta,
        keys: spec.keys,
        direction: spec.direction,
        pathkeys: native_pathkeys,
    };

    if let Some(predicate) = spec.residual {
        let selectivity = clause_selectivity(&predicate, Some(stats), stats.reltuples);
        current_rows = clamp_rows(current_rows * selectivity);
        total_cost += current_rows * predicate_cost(&predicate) * CPU_OPERATOR_COST;
        plan = Path::Filter {
            plan_info: PlanEstimate::new(
                scan_info.startup_cost.as_f64(),
                total_cost,
                current_rows,
                stats.width,
            ),
            input: Box::new(plan),
            predicate,
        };
    }

    if !spec.removes_order
        && let Some(items) = order_items
    {
        let sort_cost = estimate_sort_cost(current_rows, items.len());
        total_cost += sort_cost;
        plan = Path::OrderBy {
            plan_info: PlanEstimate::new(
                total_cost - sort_cost,
                total_cost,
                current_rows,
                stats.width,
            ),
            input: Box::new(plan),
            items,
        };
    }

    AccessCandidate { total_cost, plan }
}

fn seq_scan_estimate(stats: &RelationStats) -> PlanEstimate {
    let total_cost = stats.relpages * SEQ_PAGE_COST + stats.reltuples * CPU_TUPLE_COST;
    PlanEstimate::new(0.0, total_cost, clamp_rows(stats.reltuples), stats.width)
}

fn choose_join_plan(left: Path, right: Path, kind: JoinType, on: Expr) -> Path {
    let original = estimate_nested_loop_join(left.clone(), right.clone(), kind, on.clone());
    if !matches!(kind, JoinType::Inner | JoinType::Cross) {
        return original;
    }

    let left_columns = left.columns();
    let right_columns = right.columns();
    let left_vars = left.output_vars();
    let right_vars = right.output_vars();
    let swapped_join = estimate_nested_loop_join(right, left, kind, on.clone());
    let swapped = restore_join_output_order(
        swapped_join,
        &left_columns,
        &right_columns,
        &left_vars,
        &right_vars,
    );
    if swapped.plan_info().total_cost.as_f64() < original.plan_info().total_cost.as_f64() {
        swapped
    } else {
        original
    }
}

fn rewrite_semantic_expr_for_join_inputs(
    expr: Expr,
    left: &Path,
    right: &Path,
    join_layout: &[Expr],
) -> Expr {
    if let Some(index) = join_layout.iter().position(|candidate| *candidate == expr) {
        return join_layout[index].clone();
    }
    let left_layout = left.output_vars();
    let right_layout = right.output_vars();
    match expr {
        Expr::Var(_) | Expr::Column(_) => {
            let rewritten_left = rewrite_semantic_expr_for_path(expr.clone(), left, &left_layout);
            if rewritten_left != expr || left_layout.contains(&expr) {
                return rewritten_left;
            }
            let rewritten_right =
                rewrite_semantic_expr_for_path(expr.clone(), right, &right_layout);
            if rewritten_right != expr || right_layout.contains(&expr) {
                return rewritten_right;
            }
            expr
        }
        Expr::Aggref(aggref) => Expr::Aggref(Box::new(crate::include::nodes::primnodes::Aggref {
            args: aggref
                .args
                .into_iter()
                .map(|arg| rewrite_semantic_expr_for_join_inputs(arg, left, right, join_layout))
                .collect(),
            ..*aggref
        })),
        Expr::Op(op) => Expr::Op(Box::new(crate::include::nodes::primnodes::OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| rewrite_semantic_expr_for_join_inputs(arg, left, right, join_layout))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(crate::include::nodes::primnodes::BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| rewrite_semantic_expr_for_join_inputs(arg, left, right, join_layout))
                .collect(),
            ..*bool_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(crate::include::nodes::primnodes::FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| rewrite_semantic_expr_for_join_inputs(arg, left, right, join_layout))
                .collect(),
            ..*func
        })),
        Expr::SubLink(sublink) => {
            Expr::SubLink(Box::new(crate::include::nodes::primnodes::SubLink {
                testexpr: sublink.testexpr.map(|expr| {
                    Box::new(rewrite_semantic_expr_for_join_inputs(
                        *expr,
                        left,
                        right,
                        join_layout,
                    ))
                }),
                ..*sublink
            }))
        }
        Expr::SubPlan(subplan) => {
            Expr::SubPlan(Box::new(crate::include::nodes::primnodes::SubPlan {
                testexpr: subplan.testexpr.map(|expr| {
                    Box::new(rewrite_semantic_expr_for_join_inputs(
                        *expr,
                        left,
                        right,
                        join_layout,
                    ))
                }),
                ..*subplan
            }))
        }
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(
            crate::include::nodes::primnodes::ScalarArrayOpExpr {
                left: Box::new(rewrite_semantic_expr_for_join_inputs(
                    *saop.left,
                    left,
                    right,
                    join_layout,
                )),
                right: Box::new(rewrite_semantic_expr_for_join_inputs(
                    *saop.right,
                    left,
                    right,
                    join_layout,
                )),
                ..*saop
            },
        )),
        Expr::Cast(inner, ty) => Expr::Cast(
            Box::new(rewrite_semantic_expr_for_join_inputs(
                *inner,
                left,
                right,
                join_layout,
            )),
            ty,
        ),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            expr: Box::new(rewrite_semantic_expr_for_join_inputs(
                *expr,
                left,
                right,
                join_layout,
            )),
            pattern: Box::new(rewrite_semantic_expr_for_join_inputs(
                *pattern,
                left,
                right,
                join_layout,
            )),
            escape: escape.map(|expr| {
                Box::new(rewrite_semantic_expr_for_join_inputs(
                    *expr,
                    left,
                    right,
                    join_layout,
                ))
            }),
            case_insensitive,
            negated,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(rewrite_semantic_expr_for_join_inputs(
                *expr,
                left,
                right,
                join_layout,
            )),
            pattern: Box::new(rewrite_semantic_expr_for_join_inputs(
                *pattern,
                left,
                right,
                join_layout,
            )),
            escape: escape.map(|expr| {
                Box::new(rewrite_semantic_expr_for_join_inputs(
                    *expr,
                    left,
                    right,
                    join_layout,
                ))
            }),
            negated,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(rewrite_semantic_expr_for_join_inputs(
            *inner,
            left,
            right,
            join_layout,
        ))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(rewrite_semantic_expr_for_join_inputs(
            *inner,
            left,
            right,
            join_layout,
        ))),
        Expr::IsDistinctFrom(left_expr, right_expr) => Expr::IsDistinctFrom(
            Box::new(rewrite_semantic_expr_for_join_inputs(
                *left_expr,
                left,
                right,
                join_layout,
            )),
            Box::new(rewrite_semantic_expr_for_join_inputs(
                *right_expr,
                left,
                right,
                join_layout,
            )),
        ),
        Expr::IsNotDistinctFrom(left_expr, right_expr) => Expr::IsNotDistinctFrom(
            Box::new(rewrite_semantic_expr_for_join_inputs(
                *left_expr,
                left,
                right,
                join_layout,
            )),
            Box::new(rewrite_semantic_expr_for_join_inputs(
                *right_expr,
                left,
                right,
                join_layout,
            )),
        ),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|element| {
                    rewrite_semantic_expr_for_join_inputs(element, left, right, join_layout)
                })
                .collect(),
            array_type,
        },
        Expr::Coalesce(left_expr, right_expr) => Expr::Coalesce(
            Box::new(rewrite_semantic_expr_for_join_inputs(
                *left_expr,
                left,
                right,
                join_layout,
            )),
            Box::new(rewrite_semantic_expr_for_join_inputs(
                *right_expr,
                left,
                right,
                join_layout,
            )),
        ),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(rewrite_semantic_expr_for_join_inputs(
                *array,
                left,
                right,
                join_layout,
            )),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript.lower.map(|expr| {
                        rewrite_semantic_expr_for_join_inputs(expr, left, right, join_layout)
                    }),
                    upper: subscript.upper.map(|expr| {
                        rewrite_semantic_expr_for_join_inputs(expr, left, right, join_layout)
                    }),
                })
                .collect(),
        },
        other => other,
    }
}

fn estimate_nested_loop_join(left: Path, right: Path, kind: JoinType, on: Expr) -> Path {
    let mut join_layout = left.output_vars();
    join_layout.extend(right.output_vars());
    let rewritten_on =
        rewrite_semantic_expr_for_join_inputs(on.clone(), &left, &right, &join_layout);
    let left_info = left.plan_info();
    let right_info = right.plan_info();
    let join_sel = clause_selectivity(&on, None, left_info.plan_rows.as_f64());
    let rows = clamp_rows(left_info.plan_rows.as_f64() * right_info.plan_rows.as_f64() * join_sel);
    let total = left_info.total_cost.as_f64()
        + left_info.plan_rows.as_f64() * right_info.total_cost.as_f64()
        + left_info.plan_rows.as_f64()
            * right_info.plan_rows.as_f64()
            * predicate_cost(&on)
            * CPU_OPERATOR_COST;
    Path::NestedLoopJoin {
        plan_info: PlanEstimate::new(
            left_info.startup_cost.as_f64() + right_info.startup_cost.as_f64(),
            total,
            rows,
            left_info.plan_width + right_info.plan_width,
        ),
        left: Box::new(left),
        right: Box::new(right),
        kind,
        on: rewritten_on,
    }
}

fn restore_join_output_order(
    join: Path,
    left_columns: &[QueryColumn],
    right_columns: &[QueryColumn],
    left_vars: &[Expr],
    right_vars: &[Expr],
) -> Path {
    let join_info = join.plan_info();
    let join_layout = join.output_vars();
    let mut targets = Vec::with_capacity(left_columns.len() + right_columns.len());
    for (column, expr) in left_columns.iter().zip(left_vars.iter()) {
        targets.push(TargetEntry {
            name: column.name.clone(),
            expr: rewrite_semantic_expr_for_path(expr.clone(), &join, &join_layout),
            sql_type: column.sql_type,
            resno: targets.len() + 1,
            ressortgroupref: 0,
            resjunk: false,
        });
    }
    for (column, expr) in right_columns.iter().zip(right_vars.iter()) {
        targets.push(TargetEntry {
            name: column.name.clone(),
            expr: rewrite_semantic_expr_for_path(expr.clone(), &join, &join_layout),
            sql_type: column.sql_type,
            resno: targets.len() + 1,
            ressortgroupref: 0,
            resjunk: false,
        });
    }
    let width = targets
        .iter()
        .map(|target| estimate_sql_type_width(target.sql_type))
        .sum();
    Path::Projection {
        plan_info: PlanEstimate::new(
            join_info.startup_cost.as_f64(),
            join_info.total_cost.as_f64() + join_info.plan_rows.as_f64() * CPU_OPERATOR_COST,
            join_info.plan_rows.as_f64(),
            width,
        ),
        slot_id: next_synthetic_slot_id(),
        input: Box::new(join),
        targets,
    }
}

fn build_index_path_spec(
    filter: Option<&Expr>,
    order_items: Option<&[OrderByEntry]>,
    index: &BoundIndexRelation,
) -> Option<IndexPathSpec> {
    let conjuncts = filter.map(flatten_and_conjuncts).unwrap_or_default();
    let parsed_quals = conjuncts
        .iter()
        .filter_map(indexable_qual)
        .collect::<Vec<_>>();
    let mut used = vec![false; parsed_quals.len()];
    let mut keys = Vec::new();
    let mut used_quals = Vec::new();
    let mut equality_prefix = 0usize;

    for attnum in &index.index_meta.indkey {
        let column = attnum.saturating_sub(1) as usize;
        if let Some((qual_idx, qual)) = parsed_quals
            .iter()
            .enumerate()
            .find(|(idx, qual)| !used[*idx] && qual.column == column && qual.strategy == 3)
        {
            used[qual_idx] = true;
            used_quals.push(qual.expr.clone());
            equality_prefix += 1;
            keys.push(crate::include::access::scankey::ScanKeyData {
                attribute_number: equality_prefix as i16,
                strategy: qual.strategy,
                argument: qual.argument.clone(),
            });
            continue;
        }
        if let Some((qual_idx, qual)) = parsed_quals
            .iter()
            .enumerate()
            .find(|(idx, qual)| !used[*idx] && qual.column == column)
        {
            used[qual_idx] = true;
            used_quals.push(qual.expr.clone());
            keys.push(crate::include::access::scankey::ScanKeyData {
                attribute_number: (equality_prefix + 1) as i16,
                strategy: qual.strategy,
                argument: qual.argument.clone(),
            });
        }
        break;
    }

    let order_match =
        order_items.and_then(|items| index_order_match(items, index, equality_prefix));
    if keys.is_empty() && order_match.is_none() {
        return None;
    }

    let used_exprs = parsed_quals
        .iter()
        .enumerate()
        .filter_map(|(idx, qual)| {
            used.get(idx)
                .copied()
                .unwrap_or(false)
                .then_some(&qual.expr)
        })
        .collect::<Vec<_>>();
    let residual = and_exprs(
        conjuncts
            .iter()
            .filter(|expr| !used_exprs.iter().any(|used_expr| *used_expr == *expr))
            .cloned()
            .collect(),
    );

    Some(IndexPathSpec {
        index: index.clone(),
        keys,
        residual,
        used_quals,
        direction: order_match
            .as_ref()
            .map(|(_, direction)| *direction)
            .unwrap_or(crate::include::access::relscan::ScanDirection::Forward),
        removes_order: order_match.is_some(),
    })
}

fn clause_selectivity(expr: &Expr, stats: Option<&RelationStats>, reltuples: f64) -> f64 {
    match expr {
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::And => bool_expr
            .args
            .iter()
            .fold(1.0, |acc, arg| {
                acc * clause_selectivity(arg, stats, reltuples)
            })
            .clamp(0.0, 1.0),
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::Or => {
            let mut result = 0.0;
            for arg in &bool_expr.args {
                let selectivity = clause_selectivity(arg, stats, reltuples);
                result = result + selectivity - result * selectivity;
            }
            result.clamp(0.0, 1.0)
        }
        Expr::IsNull(inner) => {
            column_selectivity(inner, stats, |row, _| row.stanullfrac).unwrap_or(DEFAULT_EQ_SEL)
        }
        Expr::IsNotNull(inner) => column_selectivity(inner, stats, |row, _| 1.0 - row.stanullfrac)
            .unwrap_or(1.0 - DEFAULT_EQ_SEL),
        Expr::Op(op) if matches!(op.op, OpExprKind::Eq) && op.args.len() == 2 => {
            eq_selectivity(&op.args[0], &op.args[1], stats, reltuples)
        }
        Expr::Op(op) if matches!(op.op, OpExprKind::NotEq) && op.args.len() == 2 => {
            1.0 - eq_selectivity(&op.args[0], &op.args[1], stats, reltuples)
        }
        Expr::Op(op) if matches!(op.op, OpExprKind::Lt) && op.args.len() == 2 => {
            ineq_selectivity(&op.args[0], &op.args[1], stats, reltuples, Ordering::Less)
        }
        Expr::Op(op) if matches!(op.op, OpExprKind::LtEq) && op.args.len() == 2 => {
            ineq_selectivity(&op.args[0], &op.args[1], stats, reltuples, Ordering::Less)
                .max(eq_selectivity(&op.args[0], &op.args[1], stats, reltuples))
        }
        Expr::Op(op) if matches!(op.op, OpExprKind::Gt) && op.args.len() == 2 => ineq_selectivity(
            &op.args[0],
            &op.args[1],
            stats,
            reltuples,
            Ordering::Greater,
        ),
        Expr::Op(op) if matches!(op.op, OpExprKind::GtEq) && op.args.len() == 2 => {
            ineq_selectivity(
                &op.args[0],
                &op.args[1],
                stats,
                reltuples,
                Ordering::Greater,
            )
            .max(eq_selectivity(&op.args[0], &op.args[1], stats, reltuples))
        }
        _ => DEFAULT_BOOL_SEL,
    }
    .clamp(0.0, 1.0)
}

fn eq_selectivity(left: &Expr, right: &Expr, stats: Option<&RelationStats>, reltuples: f64) -> f64 {
    let Some((column, constant)) = column_const_pair(left, right) else {
        return DEFAULT_EQ_SEL;
    };
    let Some(stats) = stats else {
        return DEFAULT_EQ_SEL;
    };
    let Some(row) = stats.stats_by_attnum.get(&((column + 1) as i16)) else {
        return DEFAULT_EQ_SEL;
    };
    if let Some((values, freqs)) = slot_values_and_numbers(row, STATISTIC_KIND_MCV) {
        for (value, freq) in values.elements.iter().zip(freqs.elements.iter()) {
            if values_equal(value, &constant) {
                return float_value(freq).unwrap_or(DEFAULT_EQ_SEL).clamp(0.0, 1.0);
            }
        }
    }

    let ndistinct = effective_ndistinct(row, reltuples).unwrap_or(200.0);
    let mcv_count = slot_values(row, STATISTIC_KIND_MCV)
        .map(|array| array.elements.len() as f64)
        .unwrap_or(0.0);
    let mcv_total = slot_numbers(row, STATISTIC_KIND_MCV)
        .map(|array| array.elements.iter().filter_map(float_value).sum::<f64>())
        .unwrap_or(0.0);
    let remaining = (1.0 - row.stanullfrac - mcv_total).max(0.0);
    let distinct_remaining = (ndistinct - mcv_count).max(1.0);
    (remaining / distinct_remaining).clamp(0.0, 1.0)
}

fn ineq_selectivity(
    left: &Expr,
    right: &Expr,
    stats: Option<&RelationStats>,
    _reltuples: f64,
    wanted: Ordering,
) -> f64 {
    let Some((column, constant, flipped)) = ordered_column_const_pair(left, right) else {
        return DEFAULT_INEQ_SEL;
    };
    let Some(stats) = stats else {
        return DEFAULT_INEQ_SEL;
    };
    let Some(row) = stats.stats_by_attnum.get(&((column + 1) as i16)) else {
        return DEFAULT_INEQ_SEL;
    };
    let Some(hist) = slot_values(row, STATISTIC_KIND_HISTOGRAM) else {
        return DEFAULT_INEQ_SEL;
    };
    let fraction = histogram_fraction(&hist, &constant);
    let lt_fraction = fraction * (1.0 - row.stanullfrac);
    let gt_fraction = (1.0 - fraction) * (1.0 - row.stanullfrac);
    match (wanted, flipped) {
        (Ordering::Less, false) => lt_fraction,
        (Ordering::Greater, false) => gt_fraction,
        (Ordering::Less, true) => gt_fraction,
        (Ordering::Greater, true) => lt_fraction,
        _ => DEFAULT_INEQ_SEL,
    }
}

fn column_selectivity(
    expr: &Expr,
    stats: Option<&RelationStats>,
    f: impl FnOnce(&PgStatisticRow, f64) -> f64,
) -> Option<f64> {
    let column = expr_column_index(expr)?;
    let stats = stats?;
    let row = stats.stats_by_attnum.get(&((column + 1) as i16))?;
    Some(f(row, stats.reltuples))
}

fn column_const_pair<'a>(left: &'a Expr, right: &'a Expr) -> Option<(usize, Value)> {
    match (left, right) {
        (expr, Expr::Const(value)) => Some((expr_column_index(expr)?, value.clone())),
        (Expr::Const(value), expr) => Some((expr_column_index(expr)?, value.clone())),
        _ => None,
    }
}

fn ordered_column_const_pair<'a>(left: &'a Expr, right: &'a Expr) -> Option<(usize, Value, bool)> {
    match (left, right) {
        (expr, Expr::Const(value)) => Some((expr_column_index(expr)?, value.clone(), false)),
        (Expr::Const(value), expr) => Some((expr_column_index(expr)?, value.clone(), true)),
        _ => None,
    }
}

fn histogram_fraction(hist: &ArrayValue, constant: &Value) -> f64 {
    if hist.elements.len() < 2 {
        return DEFAULT_INEQ_SEL;
    }
    let bins = (hist.elements.len() - 1) as f64;
    for (idx, value) in hist.elements.iter().enumerate() {
        match compare_order_values(value, constant, None, false) {
            Ordering::Greater => {
                return (idx.saturating_sub(1) as f64 / bins).clamp(0.0, 1.0);
            }
            Ordering::Equal => return (idx as f64 / bins).clamp(0.0, 1.0),
            Ordering::Less => {}
        }
    }
    1.0
}

fn effective_ndistinct(row: &PgStatisticRow, reltuples: f64) -> Option<f64> {
    if row.stadistinct > 0.0 {
        Some(row.stadistinct)
    } else if row.stadistinct < 0.0 && reltuples > 0.0 {
        Some((-row.stadistinct) * reltuples)
    } else {
        None
    }
}

fn slot_values_and_numbers(row: &PgStatisticRow, kind: i16) -> Option<(ArrayValue, ArrayValue)> {
    let idx = row.stakind.iter().position(|entry| *entry == kind)?;
    Some((row.stavalues[idx].clone()?, row.stanumbers[idx].clone()?))
}

fn slot_values(row: &PgStatisticRow, kind: i16) -> Option<ArrayValue> {
    let idx = row.stakind.iter().position(|entry| *entry == kind)?;
    row.stavalues[idx].clone()
}

fn slot_numbers(row: &PgStatisticRow, kind: i16) -> Option<ArrayValue> {
    let idx = row.stakind.iter().position(|entry| *entry == kind)?;
    row.stanumbers[idx].clone()
}

fn values_equal(left: &Value, right: &Value) -> bool {
    compare_order_values(left, right, None, false) == Ordering::Equal
}

fn float_value(value: &Value) -> Option<f64> {
    match value {
        Value::Float64(v) => Some(*v),
        Value::Int16(v) => Some(*v as f64),
        Value::Int32(v) => Some(*v as f64),
        Value::Int64(v) => Some(*v as f64),
        _ => None,
    }
}

fn estimate_relation_width(desc: &RelationDesc, stats: &HashMap<i16, PgStatisticRow>) -> usize {
    desc.columns
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            stats
                .get(&((idx + 1) as i16))
                .map(|row| row.stawidth.max(1) as usize)
                .unwrap_or_else(|| {
                    if column.storage.attlen > 0 {
                        column.storage.attlen as usize
                    } else {
                        estimate_sql_type_width(column.sql_type)
                    }
                })
        })
        .sum::<usize>()
        .max(1)
}

fn estimate_sql_type_width(sql_type: SqlType) -> usize {
    match sql_type.kind {
        SqlTypeKind::Bool => 1,
        SqlTypeKind::Int2 => 2,
        SqlTypeKind::Int4 | SqlTypeKind::Oid | SqlTypeKind::Date | SqlTypeKind::Float4 => 4,
        SqlTypeKind::Int8
        | SqlTypeKind::Timestamp
        | SqlTypeKind::TimestampTz
        | SqlTypeKind::Time
        | SqlTypeKind::TimeTz
        | SqlTypeKind::Float8 => 8,
        SqlTypeKind::Numeric => 16,
        SqlTypeKind::Bit | SqlTypeKind::VarBit | SqlTypeKind::Bytea => 16,
        SqlTypeKind::Text
        | SqlTypeKind::Char
        | SqlTypeKind::Varchar
        | SqlTypeKind::Name
        | SqlTypeKind::Json
        | SqlTypeKind::Jsonb
        | SqlTypeKind::JsonPath
        | SqlTypeKind::TsVector
        | SqlTypeKind::TsQuery
        | SqlTypeKind::RegConfig
        | SqlTypeKind::RegDictionary
        | SqlTypeKind::AnyArray
        | SqlTypeKind::Point
        | SqlTypeKind::Lseg
        | SqlTypeKind::Path
        | SqlTypeKind::Line
        | SqlTypeKind::Box
        | SqlTypeKind::Polygon
        | SqlTypeKind::Circle
        | SqlTypeKind::InternalChar
        | SqlTypeKind::Int2Vector
        | SqlTypeKind::OidVector
        | SqlTypeKind::PgNodeTree
        | SqlTypeKind::Record
        | SqlTypeKind::Composite => 32,
    }
}

fn estimate_sort_cost(rows: f64, keys: usize) -> f64 {
    if rows <= 1.0 {
        0.0
    } else {
        rows * rows.log2().max(1.0) * (keys.max(1) as f64) * CPU_OPERATOR_COST
    }
}

fn predicate_cost(expr: &Expr) -> f64 {
    match expr {
        Expr::Op(op) => 1.0 + op.args.iter().map(predicate_cost).sum::<f64>(),
        Expr::Bool(bool_expr) => 1.0 + bool_expr.args.iter().map(predicate_cost).sum::<f64>(),
        Expr::Coalesce(left, right) => 1.0 + predicate_cost(left) + predicate_cost(right),
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => 1.0 + predicate_cost(inner),
        _ => 1.0,
    }
}

fn clamp_rows(rows: f64) -> f64 {
    if !rows.is_finite() {
        1.0
    } else {
        rows.max(1.0)
    }
}

fn flatten_and_conjuncts(expr: &Expr) -> Vec<Expr> {
    match expr {
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::And => bool_expr
            .args
            .iter()
            .flat_map(flatten_and_conjuncts)
            .collect(),
        other => vec![other.clone()],
    }
}

fn indexable_qual(expr: &Expr) -> Option<IndexableQual> {
    fn mk(column: usize, strategy: u16, argument: &Value, expr: &Expr) -> Option<IndexableQual> {
        Some(IndexableQual {
            column,
            strategy,
            argument: argument.clone(),
            expr: expr.clone(),
        })
    }

    match expr {
        Expr::Op(op) if op.args.len() == 2 && matches!(op.op, OpExprKind::Eq) => {
            match (&op.args[0], &op.args[1]) {
                (arg, Expr::Const(value)) => mk(expr_column_index(arg)?, 3, value, expr),
                (Expr::Const(value), arg) => mk(expr_column_index(arg)?, 3, value, expr),
                _ => None,
            }
        }
        Expr::Op(op) if op.args.len() == 2 && matches!(op.op, OpExprKind::Lt) => {
            match (&op.args[0], &op.args[1]) {
                (arg, Expr::Const(value)) => mk(expr_column_index(arg)?, 1, value, expr),
                (Expr::Const(value), arg) => mk(expr_column_index(arg)?, 5, value, expr),
                _ => None,
            }
        }
        Expr::Op(op) if op.args.len() == 2 && matches!(op.op, OpExprKind::LtEq) => {
            match (&op.args[0], &op.args[1]) {
                (arg, Expr::Const(value)) => mk(expr_column_index(arg)?, 2, value, expr),
                (Expr::Const(value), arg) => mk(expr_column_index(arg)?, 4, value, expr),
                _ => None,
            }
        }
        Expr::Op(op) if op.args.len() == 2 && matches!(op.op, OpExprKind::Gt) => {
            match (&op.args[0], &op.args[1]) {
                (arg, Expr::Const(value)) => mk(expr_column_index(arg)?, 5, value, expr),
                (Expr::Const(value), arg) => mk(expr_column_index(arg)?, 1, value, expr),
                _ => None,
            }
        }
        Expr::Op(op) if op.args.len() == 2 && matches!(op.op, OpExprKind::GtEq) => {
            match (&op.args[0], &op.args[1]) {
                (arg, Expr::Const(value)) => mk(expr_column_index(arg)?, 4, value, expr),
                (Expr::Const(value), arg) => mk(expr_column_index(arg)?, 2, value, expr),
                _ => None,
            }
        }
        _ => None,
    }
}

fn and_exprs(mut exprs: Vec<Expr>) -> Option<Expr> {
    if exprs.is_empty() {
        return None;
    }
    let first = exprs.remove(0);
    Some(exprs.into_iter().fold(first, Expr::and))
}

fn index_order_match(
    items: &[OrderByEntry],
    index: &BoundIndexRelation,
    equality_prefix: usize,
) -> Option<(usize, crate::include::access::relscan::ScanDirection)> {
    if items.is_empty() {
        return None;
    }
    let mut direction = None;
    let mut matched = 0usize;
    for (idx, item) in items.iter().enumerate() {
        let Some(column) = expr_column_index(&item.expr) else {
            break;
        };
        let Some(attnum) = index.index_meta.indkey.get(equality_prefix + idx) else {
            break;
        };
        if *attnum as usize != column + 1 {
            break;
        }
        let item_direction = if item.descending {
            crate::include::access::relscan::ScanDirection::Backward
        } else {
            crate::include::access::relscan::ScanDirection::Forward
        };
        if let Some(existing) = direction {
            if existing != item_direction {
                return None;
            }
        } else {
            direction = Some(item_direction);
        }
        matched += 1;
    }
    (matched == items.len()).then_some((
        matched,
        direction.unwrap_or(crate::include::access::relscan::ScanDirection::Forward),
    ))
}

fn expr_column_index(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Var(var) if var.varlevelsup == 0 => Some(var.varattno.saturating_sub(1)),
        Expr::Column(column) => Some(*column),
        _ => None,
    }
}
