#![allow(dead_code)]

use crate::backend::parser::SqlType;
use crate::include::nodes::pathnodes::{AppendRelInfo, Path, PlannerInfo};
use crate::include::nodes::primnodes::{
    Expr, ExprArraySubscript, TargetEntry, Var, attrno_index, user_attrno,
};

use super::inherit::append_translation;
use super::pathnodes::{is_synthetic_slot_id, rte_slot_varno};
use super::{expand_join_rte_vars, flatten_join_alias_vars};

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
    let input_layout = input.semantic_output_vars();
    targets.len() == input_layout.len()
        && targets
            .iter()
            .zip(input_layout.iter())
            .enumerate()
            .all(|(index, (target, expr))| {
                target.expr == *expr || target.input_resno == Some(index + 1)
            })
}

fn projection_target_semantic_expr(target: &TargetEntry, input_layout: &[Expr]) -> Expr {
    target
        .input_resno
        .and_then(|input_resno| input_layout.get(input_resno.saturating_sub(1)).cloned())
        .unwrap_or_else(|| target.expr.clone())
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

fn path_relids(path: &Path) -> Vec<usize> {
    let slot_relid = |slot_id: usize| rte_slot_varno(slot_id).unwrap_or(slot_id);
    match path {
        Path::Result { .. } => Vec::new(),
        Path::Append { source_id, .. } | Path::MergeAppend { source_id, .. } => vec![*source_id],
        Path::SetOp { slot_id, .. } => vec![*slot_id],
        Path::SeqScan { source_id, .. }
        | Path::IndexOnlyScan { source_id, .. }
        | Path::IndexScan { source_id, .. }
        | Path::BitmapIndexScan { source_id, .. }
        | Path::BitmapHeapScan { source_id, .. } => vec![*source_id],
        Path::Unique { input, .. }
        | Path::Filter { input, .. }
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
        } => {
            let mut relids = path_relids(anchor);
            relids.extend(path_relids(recursive));
            relids.sort_unstable();
            relids.dedup();
            relids
        }
        Path::NestedLoopJoin { left, right, .. }
        | Path::HashJoin { left, right, .. }
        | Path::MergeJoin { left, right, .. } => {
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

pub(super) fn rewrite_expr_for_append_rel(expr: Expr, info: &AppendRelInfo) -> Expr {
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
        Expr::Case(case_expr) => Expr::Case(Box::new(crate::include::nodes::primnodes::CaseExpr {
            arg: case_expr
                .arg
                .map(|arg| Box::new(rewrite_expr_for_append_rel(*arg, info))),
            args: case_expr
                .args
                .into_iter()
                .map(|arm| crate::include::nodes::primnodes::CaseWhen {
                    expr: rewrite_expr_for_append_rel(arm.expr, info),
                    result: rewrite_expr_for_append_rel(arm.result, info),
                })
                .collect(),
            defresult: Box::new(rewrite_expr_for_append_rel(*case_expr.defresult, info)),
            ..*case_expr
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
            collation_oid,
        } => Expr::Like {
            expr: Box::new(rewrite_expr_for_append_rel(*expr, info)),
            pattern: Box::new(rewrite_expr_for_append_rel(*pattern, info)),
            escape: escape.map(|expr| Box::new(rewrite_expr_for_append_rel(*expr, info))),
            case_insensitive,
            negated,
            collation_oid,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
            collation_oid,
        } => Expr::Similar {
            expr: Box::new(rewrite_expr_for_append_rel(*expr, info)),
            pattern: Box::new(rewrite_expr_for_append_rel(*pattern, info)),
            escape: escape.map(|expr| Box::new(rewrite_expr_for_append_rel(*expr, info))),
            negated,
            collation_oid,
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
    if !matches!(
        path,
        Path::Projection { .. }
            | Path::ProjectSet { .. }
            | Path::NestedLoopJoin { .. }
            | Path::HashJoin { .. }
            | Path::MergeJoin { .. }
    ) && layout.contains(&expr)
    {
        return expr;
    }
    match path {
        Path::Projection {
            slot_id,
            input,
            targets,
            ..
        } => {
            let input_layout = input.semantic_output_vars();
            let passthrough_boundary = projection_is_passthrough_boundary(input, targets);
            if let Some(index) =
                projection_target_index_for_semantic_expr(targets, &input_layout, &expr)
            {
                let target = &targets[index];
                projection_slot_var(*slot_id, user_attrno(index), target.sql_type)
            } else {
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
                if targets
                    .iter()
                    .any(|target| target.expr == rewritten_input_expr)
                {
                    return rewritten_input_expr;
                }
                if let Some(index) = projection_target_index_for_semantic_expr(
                    targets,
                    &input_layout,
                    &rewritten_input_expr,
                ) {
                    let target = &targets[index];
                    projection_slot_var(*slot_id, user_attrno(index), target.sql_type)
                } else {
                    expr
                }
            }
        }
        Path::Filter { input, .. }
        | Path::OrderBy { input, .. }
        | Path::Limit { input, .. }
        | Path::LockRows { input, .. } => rewrite_expr_for_path(expr, input, layout),
        Path::NestedLoopJoin { left, right, .. }
        | Path::HashJoin { left, right, .. }
        | Path::MergeJoin { left, right, .. } => {
            let left_layout = left.semantic_output_vars();
            if left_layout.contains(&expr) {
                return rewrite_expr_for_path(expr, left, &left_layout);
            }
            let right_layout = right.semantic_output_vars();
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
            expr
        }
        Path::RecursiveUnion { .. } => expr,
        _ => expr,
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
            aggorder: aggref
                .aggorder
                .into_iter()
                .map(|item| crate::include::nodes::primnodes::OrderByEntry {
                    expr: rewrite_semantic_expr_for_path(item.expr, path, layout),
                    ..item
                })
                .collect(),
            aggfilter: aggref
                .aggfilter
                .map(|expr| rewrite_semantic_expr_for_path(expr, path, layout)),
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
        Expr::Case(case_expr) => Expr::Case(Box::new(crate::include::nodes::primnodes::CaseExpr {
            arg: case_expr
                .arg
                .map(|arg| Box::new(rewrite_semantic_expr_for_path(*arg, path, layout))),
            args: case_expr
                .args
                .into_iter()
                .map(|arm| crate::include::nodes::primnodes::CaseWhen {
                    expr: rewrite_semantic_expr_for_path(arm.expr, path, layout),
                    result: rewrite_semantic_expr_for_path(arm.result, path, layout),
                })
                .collect(),
            defresult: Box::new(rewrite_semantic_expr_for_path(
                *case_expr.defresult,
                path,
                layout,
            )),
            ..*case_expr
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
            collation_oid,
        } => Expr::Like {
            expr: Box::new(rewrite_semantic_expr_for_path(*expr, path, layout)),
            pattern: Box::new(rewrite_semantic_expr_for_path(*pattern, path, layout)),
            escape: escape
                .map(|expr| Box::new(rewrite_semantic_expr_for_path(*expr, path, layout))),
            case_insensitive,
            negated,
            collation_oid,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
            collation_oid,
        } => Expr::Similar {
            expr: Box::new(rewrite_semantic_expr_for_path(*expr, path, layout)),
            pattern: Box::new(rewrite_semantic_expr_for_path(*pattern, path, layout)),
            escape: escape
                .map(|expr| Box::new(rewrite_semantic_expr_for_path(*expr, path, layout))),
            negated,
            collation_oid,
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
    let rewritten = rewrite_semantic_expr_for_path(expr.clone(), path, layout);
    if rewritten != expr || layout.contains(&rewritten) {
        return rewritten;
    }
    let expanded = expand_join_rte_vars(root, expr.clone());
    if expanded != expr {
        if let Some(candidate) = layout_candidate_for_expr(root, &expanded, layout) {
            return candidate;
        }
        rewrite_semantic_expr_for_path(expanded, path, layout)
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
