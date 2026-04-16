use crate::backend::parser::{CatalogLookup, SqlType};
use crate::include::nodes::pathnodes::{Path, PathKey, PathTarget, PlannerInfo, RelOptInfo};
use crate::include::nodes::plannodes::PlanEstimate;
use crate::include::nodes::primnodes::{
    AggAccum, Expr, ExprArraySubscript, OrderByEntry, QueryColumn, RelationDesc, TargetEntry, Var,
    attrno_index, user_attrno,
};

use super::super::inherit::append_translation;
use super::super::optimize_path;
use super::super::pathnodes::{
    expr_sql_type, is_synthetic_slot_id, lower_agg_output_expr, rewrite_expr_against_layout,
    rewrite_semantic_expr_for_input_path,
};
use super::super::{expand_join_rte_vars, expr_relids, flatten_join_alias_vars};
use crate::include::nodes::pathnodes::AppendRelInfo;

pub(super) fn pathkeys_to_order_items(pathkeys: &[PathKey]) -> Vec<OrderByEntry> {
    pathkeys
        .iter()
        .map(|key| OrderByEntry {
            expr: key.expr.clone(),
            descending: key.descending,
            nulls_first: key.nulls_first,
        })
        .collect()
}

pub(super) fn build_aggregate_output_columns(
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

pub(super) fn project_to_slot_layout_internal(
    root: Option<&PlannerInfo>,
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
            let expr = match root {
                Some(root) => {
                    rewrite_semantic_expr_for_path_or_expand_join_vars(root, expr, &input, &layout)
                }
                None => rewrite_semantic_expr_for_input_path(expr, &input, &layout),
            };
            TargetEntry::new(column.name.clone(), expr, column.sql_type, index + 1)
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

pub(super) fn project_to_slot_layout(
    slot_id: usize,
    desc: &RelationDesc,
    input: Path,
    target_exprs: Vec<Expr>,
    catalog: &dyn CatalogLookup,
) -> Path {
    project_to_slot_layout_internal(None, slot_id, desc, input, target_exprs, catalog)
}

pub(super) fn normalize_rte_path(
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
                    varattno: user_attrno(index),
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

pub(super) fn lower_targets_for_path(
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
            .map(|target| {
                let expr = rewrite_semantic_expr_for_path_or_expand_join_vars(
                    root,
                    target.expr.clone(),
                    path,
                    &layout,
                );
                TargetEntry { expr, ..target }
            })
            .collect(),
    }
}

pub(super) fn lower_pathkeys_for_path(
    root: &PlannerInfo,
    path: &Path,
    pathkeys: &[PathKey],
) -> Vec<PathKey> {
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
            .map(|key| {
                let expr = rewrite_semantic_expr_for_path_or_expand_join_vars(
                    root,
                    key.expr.clone(),
                    path,
                    &layout,
                );
                PathKey {
                    expr,
                    descending: key.descending,
                    nulls_first: key.nulls_first,
                }
            })
            .collect(),
    }
}

pub(super) fn lower_pathkeys_for_rel(
    root: &PlannerInfo,
    rel: &RelOptInfo,
    pathkeys: &[PathKey],
) -> Vec<PathKey> {
    rel.pathlist
        .first()
        .map(|path| lower_pathkeys_for_path(root, path, pathkeys))
        .unwrap_or_else(|| pathkeys.to_vec())
}

pub(super) fn projection_is_identity(path: &Path, targets: &[TargetEntry]) -> bool {
    let input_columns = path.columns();
    let layout = path.output_vars();
    targets.len() == input_columns.len()
        && targets.iter().enumerate().all(|(index, target)| {
            target.expr == layout[index] && target.name == input_columns[index].name
        })
}

fn projection_slot_var(
    slot_id: usize,
    attno: crate::include::nodes::primnodes::AttrNumber,
    vartype: SqlType,
) -> Expr {
    Expr::Var(Var {
        varno: slot_id,
        varattno: attno,
        varlevelsup: 0,
        vartype,
    })
}

fn projection_is_passthrough_boundary(input: &Path, targets: &[TargetEntry]) -> bool {
    let input_layout = input.output_vars();
    targets.len() == input_layout.len()
        && targets
            .iter()
            .zip(input_layout.iter())
            .all(|(target, expr)| target.expr == *expr)
}

fn projection_target_semantic_expr(target: &TargetEntry, input_layout: &[Expr]) -> Expr {
    rewrite_expr_against_layout(target.expr.clone(), input_layout)
}

fn projection_target_index_for_semantic_expr(
    targets: &[TargetEntry],
    input_layout: &[Expr],
    expr: &Expr,
) -> Option<usize> {
    targets.iter().enumerate().find_map(|(index, target)| {
        (projection_target_semantic_expr(target, input_layout) == *expr).then_some(index)
    })
}

fn projection_target_matches_expr(target_expr: &Expr, candidate: &Expr) -> bool {
    if target_expr == candidate {
        return true;
    }
    matches!(
        target_expr,
        Expr::Coalesce(left, right) if left.as_ref() == candidate || right.as_ref() == candidate
    )
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
        } => {
            let mut relids = path_relids(anchor);
            relids.extend(path_relids(recursive));
            relids.sort_unstable();
            relids.dedup();
            relids
        }
        Path::NestedLoopJoin { left, right, .. } | Path::HashJoin { left, right, .. } => {
            let mut relids = path_relids(left);
            relids.extend(path_relids(right));
            relids.sort_unstable();
            relids.dedup();
            relids
        }
    }
}

fn rewrite_appendrel_expr_for_path(root: &PlannerInfo, expr: Expr, path: &Path) -> Expr {
    let relids = path_relids(path);
    if relids.len() != 1 {
        return expr;
    }
    append_translation(root, relids[0])
        .map(|info| rewrite_expr_for_append_rel(expr.clone(), info))
        .unwrap_or(expr)
}

fn rewrite_expr_for_append_rel(expr: Expr, info: &AppendRelInfo) -> Expr {
    match expr {
        Expr::Var(var) if var.varlevelsup == 0 && var.varno == info.parent_relid => info
            .translated_vars
            .get(attrno_index(var.varattno).unwrap_or(usize::MAX))
            .cloned()
            .unwrap_or(Expr::Var(var)),
        Expr::Op(op) => Expr::Op(Box::new(crate::include::nodes::primnodes::OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| rewrite_expr_for_append_rel(arg, info))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(crate::include::nodes::primnodes::BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| rewrite_expr_for_append_rel(arg, info))
                .collect(),
            ..*bool_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(crate::include::nodes::primnodes::FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| rewrite_expr_for_append_rel(arg, info))
                .collect(),
            ..*func
        })),
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(
            crate::include::nodes::primnodes::ScalarArrayOpExpr {
                left: Box::new(rewrite_expr_for_append_rel(*saop.left, info)),
                right: Box::new(rewrite_expr_for_append_rel(*saop.right, info)),
                ..*saop
            },
        )),
        Expr::Cast(inner, ty) => {
            Expr::Cast(Box::new(rewrite_expr_for_append_rel(*inner, info)), ty)
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            expr: Box::new(rewrite_expr_for_append_rel(*expr, info)),
            pattern: Box::new(rewrite_expr_for_append_rel(*pattern, info)),
            escape: escape.map(|expr| Box::new(rewrite_expr_for_append_rel(*expr, info))),
            case_insensitive,
            negated,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(rewrite_expr_for_append_rel(*expr, info)),
            pattern: Box::new(rewrite_expr_for_append_rel(*pattern, info)),
            escape: escape.map(|expr| Box::new(rewrite_expr_for_append_rel(*expr, info))),
            negated,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(rewrite_expr_for_append_rel(*inner, info))),
        Expr::IsNotNull(inner) => {
            Expr::IsNotNull(Box::new(rewrite_expr_for_append_rel(*inner, info)))
        }
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(rewrite_expr_for_append_rel(*left, info)),
            Box::new(rewrite_expr_for_append_rel(*right, info)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(rewrite_expr_for_append_rel(*left, info)),
            Box::new(rewrite_expr_for_append_rel(*right, info)),
        ),
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(rewrite_expr_for_append_rel(*left, info)),
            Box::new(rewrite_expr_for_append_rel(*right, info)),
        ),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|element| rewrite_expr_for_append_rel(element, info))
                .collect(),
            array_type,
        },
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(rewrite_expr_for_append_rel(*array, info)),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript
                        .lower
                        .map(|expr| rewrite_expr_for_append_rel(expr, info)),
                    upper: subscript
                        .upper
                        .map(|expr| rewrite_expr_for_append_rel(expr, info)),
                })
                .collect(),
        },
        other => other,
    }
}

pub(super) fn rewrite_expr_for_path(expr: Expr, path: &Path, layout: &[Expr]) -> Expr {
    if layout.contains(&expr) {
        return expr;
    }
    match path {
        Path::Projection {
            slot_id,
            input,
            targets,
            ..
        } => {
            let input_layout = input.output_vars();
            let passthrough_boundary = projection_is_passthrough_boundary(input, targets);
            if let Some(index) =
                projection_target_index_for_semantic_expr(targets, &input_layout, &expr)
            {
                let target = &targets[index];
                projection_slot_var(*slot_id, user_attrno(index), target.sql_type)
            } else {
                // :HACK: Identity projections on synthetic slots are planner-only boundaries.
                // Map semantic input Vars onto the synthetic output slot by ordinal there, but
                // do not chase real parse-time rtindex Vars back through normalization layers.
                let rewritten_input_expr =
                    rewrite_expr_for_path(expr.clone(), input, &input_layout);
                if passthrough_boundary
                    && is_synthetic_slot_id(*slot_id)
                    && let Some(index) = input_layout
                        .iter()
                        .position(|candidate| *candidate == rewritten_input_expr)
                {
                    return projection_slot_var(
                        *slot_id,
                        user_attrno(index),
                        targets[index].sql_type,
                    );
                }
                if matches!(expr, Expr::Column(_)) && passthrough_boundary {
                    return rewrite_expr_against_layout(expr, layout);
                }
                if let Some(index) = projection_target_index_for_semantic_expr(
                    targets,
                    &input_layout,
                    &rewritten_input_expr,
                ) {
                    let target = &targets[index];
                    projection_slot_var(*slot_id, user_attrno(index), target.sql_type)
                } else {
                    rewrite_expr_against_layout(expr, layout)
                }
            }
        }
        Path::Filter { input, .. } | Path::OrderBy { input, .. } | Path::Limit { input, .. } => {
            rewrite_expr_for_path(expr, input, layout)
        }
        Path::NestedLoopJoin { left, right, .. } | Path::HashJoin { left, right, .. } => {
            let left_layout = left.output_vars();
            if left_layout.contains(&expr) {
                return rewrite_expr_for_path(expr, left, &left_layout);
            }
            let right_layout = right.output_vars();
            if right_layout.contains(&expr) {
                return rewrite_expr_for_path(expr, right, &right_layout);
            }
            let rewritten_left = rewrite_expr_for_path(expr.clone(), left, &left_layout);
            if rewritten_left != expr {
                return rewritten_left;
            }
            let rewritten_right = rewrite_expr_for_path(expr.clone(), right, &right_layout);
            if rewritten_right != expr {
                return rewritten_right;
            }
            rewrite_expr_against_layout(expr, layout)
        }
        _ => rewrite_expr_against_layout(expr, layout),
    }
}

pub(super) fn rewrite_semantic_expr_for_path(expr: Expr, path: &Path, layout: &[Expr]) -> Expr {
    let original = expr;
    let rewritten = rewrite_expr_for_path(original.clone(), path, layout);
    if rewritten != original {
        return rewritten;
    }
    let rebuilt = match original.clone() {
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
    };
    let rewritten = rewrite_expr_for_path(rebuilt.clone(), path, layout);
    if rewritten != rebuilt {
        rewritten
    } else {
        rebuilt
    }
}

pub(super) fn rewrite_semantic_expr_for_path_or_expand_join_vars(
    root: &PlannerInfo,
    expr: Expr,
    path: &Path,
    layout: &[Expr],
) -> Expr {
    let expr = rewrite_appendrel_expr_for_path(root, expr, path);
    if let Some(candidate) = layout_candidate_for_expr(root, &expr, layout) {
        return candidate;
    }
    let rewritten = rewrite_semantic_expr_for_input_path(expr.clone(), path, layout);
    if rewritten != expr || layout.contains(&rewritten) {
        return rewritten;
    }
    let expanded = expand_join_rte_vars(root, expr.clone());
    if expanded != expr {
        if let Some(candidate) = layout_candidate_for_expr(root, &expanded, layout) {
            return candidate;
        }
        rewrite_semantic_expr_for_input_path(expanded, path, layout)
    } else {
        rewritten
    }
}

pub(super) fn layout_candidate_for_expr(
    root: &PlannerInfo,
    expr: &Expr,
    layout: &[Expr],
) -> Option<Expr> {
    let expanded_expr = flatten_join_alias_vars(root, expr.clone());
    layout.iter().find_map(|candidate| {
        if candidate == expr {
            return Some(candidate.clone());
        }
        let expanded_candidate = flatten_join_alias_vars(root, candidate.clone());
        (expanded_candidate == *expr || expanded_candidate == expanded_expr)
            .then(|| candidate.clone())
    })
}

pub(super) fn aggregate_group_by(path: &Path) -> Option<&[Expr]> {
    match path {
        Path::Aggregate { group_by, .. } => Some(group_by),
        Path::Filter { input, .. } | Path::OrderBy { input, .. } | Path::Limit { input, .. } => {
            aggregate_group_by(input)
        }
        _ => None,
    }
}
