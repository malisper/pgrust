use std::cmp::Ordering;
use std::collections::HashMap;

mod pathnodes;

use crate::RelFileLocator;
use crate::backend::executor::{Value, compare_order_values};
use crate::backend::parser::{BoundIndexRelation, CatalogLookup, SqlType, SqlTypeKind};
use crate::include::catalog::{BTREE_AM_OID, PgStatisticRow};
use crate::include::nodes::datum::ArrayValue;
use crate::include::nodes::parsenodes::{JoinTreeNode, Query, RangeTblEntry, RangeTblEntryKind};
use crate::include::nodes::pathnodes::{
    PathTarget, PlannerGlobal, PlannerInfo, PlannerPath, RelOptInfo, RelOptKind, RestrictInfo,
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
    plan: PlannerPath,
}

fn create_plan(path: PlannerPath) -> Plan {
    path.into_plan()
}

fn sort_clause_to_order_items(
    sort_clause: &[crate::include::nodes::primnodes::SortGroupClause],
) -> Vec<OrderByEntry> {
    sort_clause
        .iter()
        .map(|clause| OrderByEntry {
            expr: clause.expr.clone(),
            descending: clause.descending,
            nulls_first: clause.nulls_first,
        })
        .collect()
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
    let mut relids = left.to_vec();
    relids.extend(right.iter().copied());
    relids.sort_unstable();
    relids.dedup();
    relids
}

fn relids_subset(required: &[usize], available: &[usize]) -> bool {
    required.iter().all(|relid| available.contains(relid))
}

fn relids_overlap(left: &[usize], right: &[usize]) -> bool {
    left.iter().any(|relid| right.contains(relid))
}

fn relids_disjoint(left: &[usize], right: &[usize]) -> bool {
    !relids_overlap(left, right)
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
    fn collect(expr: &Expr, relids: &mut Vec<usize>) {
        match expr {
            Expr::Var(var) if var.varlevelsup == 0 => relids.push(var.varno),
            Expr::Aggref(aggref) => {
                for arg in &aggref.args {
                    collect(arg, relids);
                }
            }
            Expr::Op(op) => {
                for arg in &op.args {
                    collect(arg, relids);
                }
            }
            Expr::Bool(bool_expr) => {
                for arg in &bool_expr.args {
                    collect(arg, relids);
                }
            }
            Expr::Func(func) => {
                for arg in &func.args {
                    collect(arg, relids);
                }
            }
            Expr::SubLink(sublink) => {
                if let Some(testexpr) = &sublink.testexpr {
                    collect(testexpr, relids);
                }
            }
            Expr::SubPlan(subplan) => {
                if let Some(testexpr) = &subplan.testexpr {
                    collect(testexpr, relids);
                }
            }
            Expr::ScalarArrayOp(saop) => {
                collect(&saop.left, relids);
                collect(&saop.right, relids);
            }
            Expr::Cast(inner, _) | Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
                collect(inner, relids)
            }
            Expr::Like {
                expr,
                pattern,
                escape,
                ..
            }
            | Expr::Similar {
                expr,
                pattern,
                escape,
                ..
            } => {
                collect(expr, relids);
                collect(pattern, relids);
                if let Some(escape) = escape {
                    collect(escape, relids);
                }
            }
            Expr::IsDistinctFrom(left, right)
            | Expr::IsNotDistinctFrom(left, right)
            | Expr::Coalesce(left, right) => {
                collect(left, relids);
                collect(right, relids);
            }
            Expr::ArrayLiteral { elements, .. } => {
                for element in elements {
                    collect(element, relids);
                }
            }
            Expr::ArraySubscript { array, subscripts } => {
                collect(array, relids);
                for subscript in subscripts {
                    if let Some(lower) = &subscript.lower {
                        collect(lower, relids);
                    }
                    if let Some(upper) = &subscript.upper {
                        collect(upper, relids);
                    }
                }
            }
            Expr::Column(_)
            | Expr::OuterColumn { .. }
            | Expr::Const(_)
            | Expr::Random
            | Expr::CurrentDate
            | Expr::CurrentTime { .. }
            | Expr::CurrentTimestamp { .. }
            | Expr::LocalTime { .. }
            | Expr::LocalTimestamp { .. } => {}
            Expr::Var(_) => {}
        }
    }

    let mut relids = Vec::new();
    collect(expr, &mut relids);
    relids.sort_unstable();
    relids.dedup();
    relids
}

#[derive(Debug, Clone)]
struct JoinNodeInfo {
    rtindex: usize,
    kind: JoinType,
    left_relids: Vec<usize>,
    right_relids: Vec<usize>,
    relids: Vec<usize>,
    quals: Expr,
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

fn collect_join_node_infos(node: &JoinTreeNode, infos: &mut Vec<JoinNodeInfo>) -> Vec<usize> {
    match node {
        JoinTreeNode::RangeTblRef(rtindex) => vec![*rtindex],
        JoinTreeNode::JoinExpr {
            left,
            right,
            kind,
            quals,
            rtindex,
        } => {
            let left_relids = collect_join_node_infos(left, infos);
            let right_relids = collect_join_node_infos(right, infos);
            let relids = relids_union(&left_relids, &right_relids);
            infos.push(JoinNodeInfo {
                rtindex: *rtindex,
                kind: *kind,
                left_relids: left_relids.clone(),
                right_relids: right_relids.clone(),
                relids: relids.clone(),
                quals: quals.clone(),
            });
            relids
        }
    }
}

fn join_node_infos(root: &PlannerInfo) -> Vec<JoinNodeInfo> {
    let mut infos = Vec::new();
    if let Some(jointree) = root.parse.jointree.as_ref() {
        collect_join_node_infos(jointree, &mut infos);
    }
    infos
}

fn expand_join_rte_vars(root: &PlannerInfo, expr: Expr) -> Expr {
    match expr {
        Expr::Var(var) if var.varlevelsup == 0 => {
            let Some(rte) = root.parse.rtable.get(var.varno.saturating_sub(1)) else {
                return Expr::Var(var);
            };
            let RangeTblEntryKind::Join { joinaliasvars, .. } = &rte.kind else {
                return Expr::Var(var);
            };
            joinaliasvars
                .get(var.varattno.saturating_sub(1))
                .cloned()
                .map(|expr| expand_join_rte_vars(root, expr))
                .unwrap_or(Expr::Var(var))
        }
        Expr::Aggref(aggref) => Expr::Aggref(Box::new(crate::include::nodes::primnodes::Aggref {
            args: aggref
                .args
                .into_iter()
                .map(|arg| expand_join_rte_vars(root, arg))
                .collect(),
            ..*aggref
        })),
        Expr::Op(op) => Expr::Op(Box::new(crate::include::nodes::primnodes::OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| expand_join_rte_vars(root, arg))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(crate::include::nodes::primnodes::BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| expand_join_rte_vars(root, arg))
                .collect(),
            ..*bool_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(crate::include::nodes::primnodes::FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| expand_join_rte_vars(root, arg))
                .collect(),
            ..*func
        })),
        Expr::SubLink(sublink) => {
            Expr::SubLink(Box::new(crate::include::nodes::primnodes::SubLink {
                testexpr: sublink
                    .testexpr
                    .map(|expr| Box::new(expand_join_rte_vars(root, *expr))),
                ..*sublink
            }))
        }
        Expr::SubPlan(subplan) => {
            Expr::SubPlan(Box::new(crate::include::nodes::primnodes::SubPlan {
                testexpr: subplan
                    .testexpr
                    .map(|expr| Box::new(expand_join_rte_vars(root, *expr))),
                ..*subplan
            }))
        }
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(
            crate::include::nodes::primnodes::ScalarArrayOpExpr {
                left: Box::new(expand_join_rte_vars(root, *saop.left)),
                right: Box::new(expand_join_rte_vars(root, *saop.right)),
                ..*saop
            },
        )),
        Expr::Cast(inner, ty) => Expr::Cast(Box::new(expand_join_rte_vars(root, *inner)), ty),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            expr: Box::new(expand_join_rte_vars(root, *expr)),
            pattern: Box::new(expand_join_rte_vars(root, *pattern)),
            escape: escape.map(|expr| Box::new(expand_join_rte_vars(root, *expr))),
            case_insensitive,
            negated,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(expand_join_rte_vars(root, *expr)),
            pattern: Box::new(expand_join_rte_vars(root, *pattern)),
            escape: escape.map(|expr| Box::new(expand_join_rte_vars(root, *expr))),
            negated,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(expand_join_rte_vars(root, *inner))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(expand_join_rte_vars(root, *inner))),
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(expand_join_rte_vars(root, *left)),
            Box::new(expand_join_rte_vars(root, *right)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(expand_join_rte_vars(root, *left)),
            Box::new(expand_join_rte_vars(root, *right)),
        ),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|element| expand_join_rte_vars(root, element))
                .collect(),
            array_type,
        },
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(expand_join_rte_vars(root, *left)),
            Box::new(expand_join_rte_vars(root, *right)),
        ),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(expand_join_rte_vars(root, *array)),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript
                        .lower
                        .map(|expr| expand_join_rte_vars(root, expr)),
                    upper: subscript
                        .upper
                        .map(|expr| expand_join_rte_vars(root, expr)),
                })
                .collect(),
        },
        other => other,
    }
}

fn collect_inner_join_clauses(root: &PlannerInfo, join_infos: &[JoinNodeInfo]) -> Vec<Expr> {
    let mut clauses = join_infos
        .iter()
        .filter(|info| matches!(info.kind, JoinType::Inner | JoinType::Cross))
        .map(|info| expand_join_rte_vars(root, info.quals.clone()))
        .collect::<Vec<_>>();
    if !has_outer_joins(root) {
        if let Some(where_qual) = root.parse.where_qual.as_ref() {
            clauses.extend(
                flatten_and_conjuncts(where_qual)
                    .into_iter()
                    .map(|clause| expand_join_rte_vars(root, clause))
                    .filter(|clause| expr_relids(clause).len() > 1),
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
        let restrict = RestrictInfo::new(expand_join_rte_vars(root, clause));
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

fn build_aggregate_output_columns(group_by: &[Expr], accumulators: &[AggAccum]) -> Vec<QueryColumn> {
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

fn project_to_slot_layout(
    slot_id: usize,
    desc: &RelationDesc,
    input: PlannerPath,
    target_exprs: Vec<Expr>,
    catalog: &dyn CatalogLookup,
) -> PlannerPath {
    optimize_path(
        PlannerPath::Projection {
            plan_info: PlanEstimate::default(),
            slot_id,
            input: Box::new(input),
            targets: desc
                .columns
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    TargetEntry::new(
                        column.name.clone(),
                        target_exprs
                            .get(index)
                            .cloned()
                            .unwrap_or_else(|| Expr::Column(index)),
                        column.sql_type,
                        index + 1,
                    )
                })
                .collect(),
        },
        catalog,
    )
}

fn normalize_rte_path(
    rtindex: usize,
    desc: &RelationDesc,
    input: PlannerPath,
    catalog: &dyn CatalogLookup,
) -> PlannerPath {
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
    path: &PlannerPath,
    targets: &[TargetEntry],
) -> Vec<TargetEntry> {
    let layout = path.output_vars();
    match aggregate_group_by(path) {
        Some(group_by) => targets
            .iter()
            .cloned()
            .map(|target| TargetEntry {
                expr: lower_agg_output_expr(expand_join_rte_vars(root, target.expr), group_by, &layout),
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

fn lower_order_items_for_path(
    root: &PlannerInfo,
    path: &PlannerPath,
    items: &[OrderByEntry],
) -> Vec<OrderByEntry> {
    let layout = path.output_vars();
    match aggregate_group_by(path) {
        Some(group_by) => items
            .iter()
            .cloned()
            .map(|item| OrderByEntry {
                expr: lower_agg_output_expr(expand_join_rte_vars(root, item.expr), group_by, &layout),
                descending: item.descending,
                nulls_first: item.nulls_first,
            })
            .collect(),
        None => items
            .iter()
            .cloned()
            .map(|item| OrderByEntry {
                expr: rewrite_semantic_expr_for_path(
                    expand_join_rte_vars(root, item.expr),
                    path,
                    &layout,
                ),
                descending: item.descending,
                nulls_first: item.nulls_first,
            })
            .collect(),
    }
}

fn projection_is_identity(path: &PlannerPath, targets: &[TargetEntry]) -> bool {
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

fn rewrite_expr_for_path(expr: Expr, path: &PlannerPath, layout: &[Expr]) -> Expr {
    match path {
        PlannerPath::Projection {
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
        PlannerPath::Filter { input, .. }
        | PlannerPath::OrderBy { input, .. }
        | PlannerPath::Limit { input, .. } => rewrite_expr_for_path(expr, input, layout),
        PlannerPath::NestedLoopJoin { left, right, .. } => {
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

fn rewrite_semantic_expr_for_path(expr: Expr, path: &PlannerPath, layout: &[Expr]) -> Expr {
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
        Expr::SubLink(sublink) => Expr::SubLink(Box::new(crate::include::nodes::primnodes::SubLink {
            testexpr: sublink
                .testexpr
                .map(|expr| Box::new(rewrite_semantic_expr_for_path(*expr, path, layout))),
            ..*sublink
        })),
        Expr::SubPlan(subplan) => Expr::SubPlan(Box::new(crate::include::nodes::primnodes::SubPlan {
            testexpr: subplan
                .testexpr
                .map(|expr| Box::new(rewrite_semantic_expr_for_path(*expr, path, layout))),
            ..*subplan
        })),
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
            escape: escape.map(|expr| Box::new(rewrite_semantic_expr_for_path(*expr, path, layout))),
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
            escape: escape.map(|expr| Box::new(rewrite_semantic_expr_for_path(*expr, path, layout))),
            negated,
        },
        Expr::IsNull(inner) => {
            Expr::IsNull(Box::new(rewrite_semantic_expr_for_path(*inner, path, layout)))
        }
        Expr::IsNotNull(inner) => {
            Expr::IsNotNull(Box::new(rewrite_semantic_expr_for_path(*inner, path, layout)))
        }
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

fn aggregate_group_by(path: &PlannerPath) -> Option<&[Expr]> {
    match path {
        PlannerPath::Aggregate { group_by, .. } => Some(group_by),
        PlannerPath::Filter { input, .. }
        | PlannerPath::OrderBy { input, .. }
        | PlannerPath::Limit { input, .. } => aggregate_group_by(input),
        _ => None,
    }
}

fn set_base_rel_pathlist(root: &mut PlannerInfo, rtindex: usize, catalog: &dyn CatalogLookup) {
    let Some(rte) = root.parse.rtable.get(rtindex.saturating_sub(1)).cloned() else {
        return;
    };
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
            PlannerPath::Result {
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
            for index in catalog.index_relations_for_heap(relation_oid).iter().filter(|index| {
                index.index_meta.indisvalid
                    && index.index_meta.indisready
                    && !index.index_meta.indkey.is_empty()
                    && index.index_meta.am_oid == BTREE_AM_OID
            }) {
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
            }
        }
        RangeTblEntryKind::Values {
            rows,
            output_columns,
        } => {
            let mut path = optimize_path(
                PlannerPath::Values {
                    plan_info: PlanEstimate::default(),
                    slot_id: rtindex,
                    rows,
                    output_columns,
                },
                catalog,
            );
            if let Some(filter) = base_filter_expr(rel) {
                path = optimize_path(
                    PlannerPath::Filter {
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
                PlannerPath::FunctionScan {
                    plan_info: PlanEstimate::default(),
                    slot_id: rtindex,
                    call,
                },
                catalog,
            );
            if let Some(filter) = base_filter_expr(rel) {
                path = optimize_path(
                    PlannerPath::Filter {
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
            let mut path = final_rel
                .cheapest_total_path()
                .cloned()
                .unwrap_or(PlannerPath::Result {
                    plan_info: PlanEstimate::default(),
                });
            path = normalize_rte_path(rtindex, &rte.desc, path, catalog);
            if let Some(filter) = base_filter_expr(rel) {
                path = optimize_path(
                    PlannerPath::Filter {
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
    inner_join_clauses: &[Expr],
) -> Expr {
    let join_relids = relids_union(left_relids, right_relids);
    let mut clauses = Vec::new();
    if let Some(explicit_qual) = explicit_qual {
        clauses.push(explicit_qual);
    }
    if matches!(kind, JoinType::Inner | JoinType::Cross) {
        for clause in inner_join_clauses {
            let clause_relids = expr_relids(clause);
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

fn join_rte_requires_alias_projection(rte: &RangeTblEntry) -> bool {
    match &rte.kind {
        RangeTblEntryKind::Join {
            joinmergedcols, ..
        } => *joinmergedcols > 0,
        _ => false,
    }
}

fn maybe_project_join_alias(
    rtindex: usize,
    input: PlannerPath,
    root: &PlannerInfo,
    catalog: &dyn CatalogLookup,
) -> PlannerPath {
    let Some(rte) = root.parse.rtable.get(rtindex.saturating_sub(1)) else {
        return input;
    };
    if !join_rte_requires_alias_projection(rte) {
        return input;
    }
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
            .filter(|rte| join_rte_requires_alias_projection(rte))
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
    root.join_rel_list.iter().position(|rel| rel.relids == relids)
}

fn join_reltarget(
    root: &PlannerInfo,
    join_infos: &[JoinNodeInfo],
    relids: &[usize],
    left_rel: &RelOptInfo,
    right_rel: &RelOptInfo,
) -> PathTarget {
    if let Some(info) = join_infos.iter().find(|info| info.relids == relids) {
        if let Some(rte) = root.parse.rtable.get(info.rtindex.saturating_sub(1)) {
            if join_rte_requires_alias_projection(rte) {
                return PathTarget::from_rte(info.rtindex, rte);
            }
        }
    }
    let mut exprs = left_rel.reltarget.exprs.clone();
    exprs.extend(right_rel.reltarget.exprs.clone());
    PathTarget::new(exprs)
}

fn join_spec_for_relids(
    root: &PlannerInfo,
    join_infos: &[JoinNodeInfo],
    left_relids: &[usize],
    right_relids: &[usize],
) -> Option<JoinBuildSpec> {
    let joinrelids = relids_union(left_relids, right_relids);
    for sjinfo in &root.join_info_list {
        let full_relids = relids_union(&sjinfo.min_lefthand, &sjinfo.min_righthand);
        if relids_overlap(&joinrelids, &sjinfo.min_righthand)
            && !relids_subset(&joinrelids, &sjinfo.min_righthand)
            && joinrelids != full_relids
        {
            return None;
        }
    }

    if let Some(info) = join_infos.iter().find(|info| info.relids == joinrelids) {
        if relids_subset(left_relids, &info.left_relids) && relids_subset(right_relids, &info.right_relids)
        {
            let explicit_qual =
                (!matches!(info.kind, JoinType::Inner | JoinType::Cross)).then(|| info.quals.clone());
            return Some(JoinBuildSpec {
                kind: info.kind,
                rtindex: Some(info.rtindex),
                explicit_qual,
            });
        }
        if relids_subset(left_relids, &info.right_relids) && relids_subset(right_relids, &info.left_relids)
        {
            let explicit_qual =
                (!matches!(info.kind, JoinType::Inner | JoinType::Cross)).then(|| info.quals.clone());
            return Some(JoinBuildSpec {
                kind: reverse_join_type(info.kind),
                rtindex: Some(info.rtindex),
                explicit_qual,
            });
        }
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
    join_infos: &[JoinNodeInfo],
    inner_join_clauses: &[Expr],
    catalog: &dyn CatalogLookup,
) -> Option<RelOptInfo> {
    if !relids_disjoint(&left_rel.relids, &right_rel.relids) {
        return None;
    }
    let relids = relids_union(&left_rel.relids, &right_rel.relids);
    let spec = join_spec_for_relids(root, join_infos, &left_rel.relids, &right_rel.relids)?;
    let reltarget = join_reltarget(root, join_infos, &relids, left_rel, right_rel);
    let join_qual = build_join_qual(
        spec.kind,
        spec.explicit_qual.clone(),
        &left_rel.relids,
        &right_rel.relids,
        inner_join_clauses,
    );
    let join_rel_index = match find_join_rel_index(root, &relids) {
        Some(index) => index,
        None => {
            root.join_rel_list
                .push(RelOptInfo::new(relids.clone(), RelOptKind::JoinRel, reltarget));
            root.join_rel_list.len() - 1
        }
    };
    let mut candidate_paths = Vec::new();
    for left_path in &left_rel.pathlist {
        for right_path in &right_rel.pathlist {
            let path = choose_join_plan(
                left_path.clone(),
                right_path.clone(),
                spec.kind,
                join_qual.clone(),
            );
            let path = match spec.rtindex {
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
        join_rel.joininfo.push(RestrictInfo::new(join_qual.clone()));
    }
    for path in candidate_paths {
        join_rel.add_path(path);
    }
    Some(join_rel.clone())
}

fn join_search_one_level(
    root: &mut PlannerInfo,
    level: usize,
    join_infos: &[JoinNodeInfo],
    inner_join_clauses: &[Expr],
    catalog: &dyn CatalogLookup,
) {
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
                let _ = make_join_rel(
                    root,
                    left_rel,
                    right_rel,
                    join_infos,
                    inner_join_clauses,
                    catalog,
                );
            }
        }
    }
}

fn make_one_rel(root: &mut PlannerInfo, catalog: &dyn CatalogLookup) -> RelOptInfo {
    assign_base_restrictinfo(root);
    set_base_rel_pathlists(root, catalog);
    let query_relids = root.all_query_relids();
    if query_relids.is_empty() {
        let mut rel = RelOptInfo::new(Vec::new(), RelOptKind::UpperRel, PathTarget::new(Vec::new()));
        rel.add_path(optimize_path(
            PlannerPath::Result {
                plan_info: PlanEstimate::default(),
            },
            catalog,
        ));
        return rel;
    }
    if query_relids.len() == 1 {
        return root.simple_rel_array[query_relids[0]]
            .clone()
            .expect("single base relation reloptinfo");
    }
    let join_infos = join_node_infos(root);
    let inner_join_clauses = collect_inner_join_clauses(root, &join_infos);
    for level in 2..=query_relids.len() {
        join_search_one_level(root, level, &join_infos, &inner_join_clauses, catalog);
    }
    root.join_rel_list
        .iter()
        .find(|rel| rel.relids == query_relids)
        .cloned()
        .unwrap_or_else(|| {
            panic!("failed to build join rel for relids {:?}", query_relids)
        })
}

fn query_planner(root: &mut PlannerInfo, catalog: &dyn CatalogLookup) -> RelOptInfo {
    let rel = make_one_rel(root, catalog);
    match top_join_rtindex(root) {
        Some(rtindex) => normalize_join_output_rel(root, rel, rtindex, catalog),
        None => rel,
    }
}

fn make_filter_rel(
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
            PlannerPath::Filter {
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
    rel
}

fn make_aggregate_rel(
    root: &PlannerInfo,
    input_rel: RelOptInfo,
    catalog: &dyn CatalogLookup,
) -> RelOptInfo {
    let mut rel = RelOptInfo::new(
        input_rel.relids.clone(),
        RelOptKind::UpperRel,
        root.final_target.clone(),
    );
    for path in input_rel.pathlist {
        let input_layout = path.output_vars();
        let group_by = root
            .parse
            .group_by
            .iter()
            .cloned()
            .map(|expr| rewrite_semantic_expr_for_path(expr, &path, &input_layout))
            .collect::<Vec<_>>();
        let slot_id = next_synthetic_slot_id();
        let agg_output_layout =
            aggregate_output_vars(slot_id, &group_by, &root.parse.accumulators);
        let having = root.parse.having_qual.clone().map(|expr| {
            lower_agg_output_expr(
                rewrite_semantic_expr_for_path(expr, &path, &input_layout),
                &group_by,
                &agg_output_layout,
            )
        });
        rel.add_path(optimize_path(
            PlannerPath::Aggregate {
                plan_info: PlanEstimate::default(),
                slot_id,
                input: Box::new(path),
                group_by: group_by.clone(),
                accumulators: root.parse.accumulators.clone(),
                having,
                output_columns: build_aggregate_output_columns(
                    &group_by,
                    &root.parse.accumulators,
                ),
            },
            catalog,
        ));
    }
    rel
}

fn make_project_set_rel(
    input_rel: RelOptInfo,
    targets: &[ProjectSetTarget],
    catalog: &dyn CatalogLookup,
) -> RelOptInfo {
    let mut rel = RelOptInfo::new(
        input_rel.relids.clone(),
        RelOptKind::UpperRel,
        input_rel.reltarget.clone(),
    );
    for path in input_rel.pathlist {
        let layout = path.output_vars();
        rel.add_path(optimize_path(
            PlannerPath::ProjectSet {
                plan_info: PlanEstimate::default(),
                slot_id: next_synthetic_slot_id(),
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
    rel
}

fn maybe_add_ordered_base_paths(
    root: &PlannerInfo,
    input_rel: &RelOptInfo,
    output_rel: &mut RelOptInfo,
    catalog: &dyn CatalogLookup,
) {
    if input_rel.reloptkind != RelOptKind::BaseRel || input_rel.relids.len() != 1 {
        return;
    }
    let rtindex = input_rel.relids[0];
    let Some(rte) = root.parse.rtable.get(rtindex.saturating_sub(1)) else {
        return;
    };
    let RangeTblEntryKind::Relation {
        rel,
        relation_oid,
        relkind: _,
        toast,
    } = rte.kind
    else {
        return;
    };
    let order_items = sort_clause_to_order_items(&root.parse.sort_clause);
    let filter = base_filter_expr(input_rel);
    let stats = relation_stats(catalog, relation_oid, &rte.desc);
    output_rel.add_path(
        estimate_seqscan_candidate(
            rtindex,
            rel,
            relation_oid,
            toast,
            rte.desc.clone(),
            &stats,
            filter.clone(),
            Some(order_items.clone()),
        )
        .plan,
    );
    for index in catalog.index_relations_for_heap(relation_oid).iter().filter(|index| {
        index.index_meta.indisvalid
            && index.index_meta.indisready
            && !index.index_meta.indkey.is_empty()
            && index.index_meta.am_oid == BTREE_AM_OID
    }) {
        let Some(spec) = build_index_path_spec(filter.as_ref(), Some(&order_items), index) else {
            continue;
        };
        output_rel.add_path(
            estimate_index_candidate(
                rtindex,
                rel,
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

fn make_ordered_rel(
    root: &PlannerInfo,
    input_rel: RelOptInfo,
    order_items: &[OrderByEntry],
    catalog: &dyn CatalogLookup,
) -> RelOptInfo {
    let mut rel = RelOptInfo::new(
        input_rel.relids.clone(),
        RelOptKind::UpperRel,
        input_rel.reltarget.clone(),
    );
    maybe_add_ordered_base_paths(root, &input_rel, &mut rel, catalog);
    for path in input_rel.pathlist {
        rel.add_path(optimize_path(
            PlannerPath::OrderBy {
                plan_info: PlanEstimate::default(),
                items: lower_order_items_for_path(root, &path, order_items),
                input: Box::new(path),
            },
            catalog,
        ));
    }
    rel
}

fn make_limit_rel(
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
            PlannerPath::Limit {
                plan_info: PlanEstimate::default(),
                input: Box::new(path),
                limit,
                offset,
            },
            catalog,
        ));
    }
    rel
}

fn make_projection_rel(
    root: &PlannerInfo,
    input_rel: RelOptInfo,
    targets: &[TargetEntry],
    catalog: &dyn CatalogLookup,
    allow_identity_elision: bool,
) -> RelOptInfo {
    let mut rel = RelOptInfo::new(
        input_rel.relids.clone(),
        RelOptKind::UpperRel,
        PathTarget::from_target_list(targets),
    );
    for path in input_rel.pathlist {
        let lowered_targets = lower_targets_for_path(root, &path, targets);
        if allow_identity_elision && projection_is_identity(&path, &lowered_targets) {
            rel.add_path(path);
            continue;
        }
        rel.add_path(optimize_path(
            PlannerPath::Projection {
                plan_info: PlanEstimate::default(),
                slot_id: next_synthetic_slot_id(),
                input: Box::new(path),
                targets: lowered_targets,
            },
            catalog,
        ));
    }
    rel
}

fn grouping_planner(
    root: &mut PlannerInfo,
    scanjoin_rel: RelOptInfo,
    catalog: &dyn CatalogLookup,
) -> RelOptInfo {
    let mut current_rel = scanjoin_rel;
    if let Some(predicate) = residual_where_qual(root) {
        current_rel = make_filter_rel(current_rel, predicate, catalog);
    }

    let has_grouping = has_grouping(root);
    let mut projection_done = false;
    if has_grouping {
        current_rel = make_aggregate_rel(root, current_rel, catalog);
    } else if let Some(project_set) = root.parse.project_set.clone() {
        current_rel = make_project_set_rel(current_rel, &project_set, catalog);
        current_rel = make_projection_rel(
            root,
            current_rel,
            &root.parse.target_list,
            catalog,
            false,
        );
        projection_done = true;
    }

    let order_items = sort_clause_to_order_items(&root.parse.sort_clause);
    if !order_items.is_empty() {
        current_rel = make_ordered_rel(root, current_rel, &order_items, catalog);
    }

    if root.parse.limit_count.is_some() || root.parse.limit_offset != 0 {
        current_rel = make_limit_rel(
            current_rel,
            root.parse.limit_count,
            root.parse.limit_offset,
            catalog,
        );
    }

    if has_grouping {
        current_rel = make_projection_rel(root, current_rel, &root.parse.target_list, catalog, false);
    } else if !projection_done {
        current_rel = make_projection_rel(root, current_rel, &root.parse.target_list, catalog, true);
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
    let best_path = final_rel
        .cheapest_total_path()
        .cloned()
        .unwrap_or(PlannerPath::Result {
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

pub(super) fn optimize_path(plan: PlannerPath, catalog: &dyn CatalogLookup) -> PlannerPath {
    match try_optimize_access_subtree(plan, catalog) {
        Ok(plan) => plan,
        Err(plan) => match plan {
            PlannerPath::Result { .. } => PlannerPath::Result {
                plan_info: PlanEstimate::new(0.0, 0.0, 1.0, 0),
            },
            PlannerPath::SeqScan {
                source_id,
                rel,
                relation_oid,
                toast,
                desc,
                ..
            } => {
                let stats = relation_stats(catalog, relation_oid, &desc);
                let base = seq_scan_estimate(&stats);
                PlannerPath::SeqScan {
                    plan_info: base,
                    source_id,
                    rel,
                    relation_oid,
                    toast,
                    desc,
                }
            }
            PlannerPath::IndexScan {
                source_id,
                rel,
                index_rel,
                am_oid,
                toast,
                desc,
                index_meta,
                keys,
                direction,
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
                PlannerPath::IndexScan {
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
                }
            }
            PlannerPath::Filter {
                input, predicate, ..
            } => {
                let input = optimize_path(*input, catalog);
                let input_info = input.plan_info();
                let input_rows = input_info.plan_rows.as_f64();
                let selectivity = clause_selectivity(&predicate, None, input_rows);
                let rows = clamp_rows(input_rows * selectivity);
                let qual_cost = predicate_cost(&predicate) * input_rows * CPU_OPERATOR_COST;
                PlannerPath::Filter {
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
            PlannerPath::OrderBy { input, items, .. } => {
                let input = optimize_path(*input, catalog);
                let input_info = input.plan_info();
                let sort_cost = estimate_sort_cost(input_info.plan_rows.as_f64(), items.len());
                PlannerPath::OrderBy {
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
            PlannerPath::Limit {
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
                PlannerPath::Limit {
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
            PlannerPath::Projection {
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
                PlannerPath::Projection {
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
            PlannerPath::Aggregate {
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
                PlannerPath::Aggregate {
                    plan_info: PlanEstimate::new(total, total, rows, width),
                    slot_id,
                    input: Box::new(input),
                    group_by,
                    accumulators,
                    having,
                    output_columns,
                }
            }
            PlannerPath::NestedLoopJoin {
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
            PlannerPath::FunctionScan { call, slot_id, .. } => {
                let output_columns = call.output_columns();
                let width = output_columns
                    .iter()
                    .map(|col| estimate_sql_type_width(col.sql_type))
                    .sum();
                PlannerPath::FunctionScan {
                    plan_info: PlanEstimate::new(0.0, 10.0, 1000.0, width),
                    slot_id,
                    call,
                }
            }
            PlannerPath::Values {
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
                PlannerPath::Values {
                    plan_info: PlanEstimate::new(0.0, row_count * CPU_TUPLE_COST, row_count, width),
                    slot_id,
                    rows,
                    output_columns,
                }
            }
            PlannerPath::ProjectSet {
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
                PlannerPath::ProjectSet {
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

fn try_optimize_access_subtree(
    plan: PlannerPath,
    catalog: &dyn CatalogLookup,
) -> Result<PlannerPath, PlannerPath> {
    let (source_id, rel, relation_oid, toast, desc, filter, order_items) = match plan {
        PlannerPath::SeqScan {
            source_id,
            rel,
            relation_oid,
            toast,
            desc,
            ..
        } => (source_id, rel, relation_oid, toast, desc, None, None),
        PlannerPath::Filter {
            input, predicate, ..
        } => match *input {
            PlannerPath::SeqScan {
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
                return Err(PlannerPath::Filter {
                    plan_info: PlanEstimate::default(),
                    input: Box::new(other),
                    predicate,
                });
            }
        },
        PlannerPath::OrderBy { input, items, .. } => match *input {
            PlannerPath::SeqScan {
                source_id,
                rel,
                relation_oid,
                toast,
                desc,
                ..
            } => (source_id, rel, relation_oid, toast, desc, None, Some(items)),
            PlannerPath::Filter {
                input, predicate, ..
            } => match *input {
                PlannerPath::SeqScan {
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
                    return Err(PlannerPath::OrderBy {
                        plan_info: PlanEstimate::default(),
                        input: Box::new(PlannerPath::Filter {
                            plan_info: PlanEstimate::default(),
                            input: Box::new(other),
                            predicate,
                        }),
                        items,
                    });
                }
            },
            other => {
                return Err(PlannerPath::OrderBy {
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
    let mut plan = PlannerPath::SeqScan {
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
        plan = PlannerPath::Filter {
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
        plan = PlannerPath::OrderBy {
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
    let mut plan = PlannerPath::IndexScan {
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
    };

    if let Some(predicate) = spec.residual {
        let selectivity = clause_selectivity(&predicate, Some(stats), stats.reltuples);
        current_rows = clamp_rows(current_rows * selectivity);
        total_cost += current_rows * predicate_cost(&predicate) * CPU_OPERATOR_COST;
        plan = PlannerPath::Filter {
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
        plan = PlannerPath::OrderBy {
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

fn choose_join_plan(
    left: PlannerPath,
    right: PlannerPath,
    kind: JoinType,
    on: Expr,
) -> PlannerPath {
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
    left: &PlannerPath,
    right: &PlannerPath,
    join_layout: &[Expr],
) -> Expr {
    if let Some(index) = join_layout.iter().position(|candidate| *candidate == expr) {
        return join_layout[index].clone();
    }
    let left_layout = left.output_vars();
    let right_layout = right.output_vars();
    match expr {
        Expr::Var(_) | Expr::Column(_) => {
            let rewritten_left =
                rewrite_semantic_expr_for_path(expr.clone(), left, &left_layout);
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
        Expr::SubLink(sublink) => Expr::SubLink(Box::new(crate::include::nodes::primnodes::SubLink {
            testexpr: sublink.testexpr.map(|expr| {
                Box::new(rewrite_semantic_expr_for_join_inputs(
                    *expr,
                    left,
                    right,
                    join_layout,
                ))
            }),
            ..*sublink
        })),
        Expr::SubPlan(subplan) => Expr::SubPlan(Box::new(crate::include::nodes::primnodes::SubPlan {
            testexpr: subplan.testexpr.map(|expr| {
                Box::new(rewrite_semantic_expr_for_join_inputs(
                    *expr,
                    left,
                    right,
                    join_layout,
                ))
            }),
            ..*subplan
        })),
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
                *inner, left, right, join_layout,
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
                *expr, left, right, join_layout,
            )),
            pattern: Box::new(rewrite_semantic_expr_for_join_inputs(
                *pattern, left, right, join_layout,
            )),
            escape: escape.map(|expr| {
                Box::new(rewrite_semantic_expr_for_join_inputs(
                    *expr, left, right, join_layout,
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
                *expr, left, right, join_layout,
            )),
            pattern: Box::new(rewrite_semantic_expr_for_join_inputs(
                *pattern, left, right, join_layout,
            )),
            escape: escape.map(|expr| {
                Box::new(rewrite_semantic_expr_for_join_inputs(
                    *expr, left, right, join_layout,
                ))
            }),
            negated,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(rewrite_semantic_expr_for_join_inputs(
            *inner, left, right, join_layout,
        ))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(rewrite_semantic_expr_for_join_inputs(
            *inner, left, right, join_layout,
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
                *array, left, right, join_layout,
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

fn estimate_nested_loop_join(
    left: PlannerPath,
    right: PlannerPath,
    kind: JoinType,
    on: Expr,
) -> PlannerPath {
    let mut join_layout = left.output_vars();
    join_layout.extend(right.output_vars());
    let rewritten_on = rewrite_semantic_expr_for_join_inputs(on.clone(), &left, &right, &join_layout);
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
    PlannerPath::NestedLoopJoin {
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
    join: PlannerPath,
    left_columns: &[QueryColumn],
    right_columns: &[QueryColumn],
    left_vars: &[Expr],
    right_vars: &[Expr],
) -> PlannerPath {
    let join_info = join.plan_info();
    let mut targets = Vec::with_capacity(left_columns.len() + right_columns.len());
    for (column, expr) in left_columns.iter().zip(left_vars.iter()) {
        targets.push(TargetEntry {
            name: column.name.clone(),
            expr: expr.clone(),
            sql_type: column.sql_type,
            resno: targets.len() + 1,
            ressortgroupref: 0,
            resjunk: false,
        });
    }
    for (column, expr) in right_columns.iter().zip(right_vars.iter()) {
        targets.push(TargetEntry {
            name: column.name.clone(),
            expr: expr.clone(),
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
    PlannerPath::Projection {
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
        | SqlTypeKind::PgNodeTree => 32,
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
