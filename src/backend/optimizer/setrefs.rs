use super::pathnodes::{
    aggregate_output_vars, lower_agg_accum_to_plan_layout, lower_agg_output_expr,
    lower_expr_to_plan_layout, lower_order_by_entry_to_plan_layout,
    lower_project_set_target_to_plan_layout, lower_set_returning_call_to_plan_layout,
    lower_target_entry_to_plan_layout, rewrite_expr_against_layout,
    rewrite_semantic_expr_for_input_path,
};
use super::{
    aggregate_group_by, expand_join_rte_vars, expr_relids, path_relids, relids_subset,
    rewrite_join_input_expr, rewrite_semantic_expr_for_path,
    rewrite_semantic_expr_for_path_or_expand_join_vars,
};
use crate::include::nodes::pathnodes::{Path, PlannerInfo};
use crate::include::nodes::plannodes::{Plan, PlanEstimate};
use crate::include::nodes::primnodes::{
    Aggref, BoolExpr, Expr, ExprArraySubscript, FuncExpr, OpExpr, OrderByEntry,
    ScalarArrayOpExpr, TargetEntry, Var,
};

pub(super) fn create_plan(root: &PlannerInfo, path: Path) -> Plan {
    set_plan_refs(Some(root), path)
}

pub(super) fn create_plan_without_root(path: Path) -> Plan {
    set_plan_refs(None, path)
}

fn set_plan_refs(root: Option<&PlannerInfo>, path: Path) -> Plan {
    match path {
        Path::Result { plan_info } => Plan::Result { plan_info },
        Path::SeqScan {
            plan_info,
            source_id: _,
            rel,
            relation_oid,
            toast,
            desc,
        } => Plan::SeqScan {
            plan_info,
            rel,
            relation_oid,
            toast,
            desc,
        },
        Path::IndexScan {
            plan_info,
            source_id: _,
            rel,
            index_rel,
            am_oid,
            toast,
            desc,
            index_meta,
            keys,
            direction,
            pathkeys: _,
        } => Plan::IndexScan {
            plan_info,
            rel,
            index_rel,
            am_oid,
            toast,
            desc,
            index_meta,
            keys,
            direction,
        },
        Path::Filter {
            plan_info,
            input,
            predicate,
        } => {
            let layout = input.output_vars();
            let predicate = fix_upper_expr(root, predicate, &input, &layout);
            Plan::Filter {
                plan_info,
                input: Box::new(set_plan_refs(root, *input)),
                predicate: lower_expr_to_plan_layout(predicate, &layout),
            }
        }
        Path::NestedLoopJoin {
            plan_info,
            left,
            right,
            kind,
            on,
        } => {
            let left_layout = left.output_vars();
            let right_layout = right.output_vars();
            let on = fix_join_expr(root, on, &left, &left_layout, &right, &right_layout);
            let mut join_layout = left_layout;
            join_layout.extend(right_layout);
            Plan::NestedLoopJoin {
                plan_info,
                left: Box::new(set_plan_refs(root, *left)),
                right: Box::new(set_plan_refs(root, *right)),
                kind,
                on: lower_expr_to_plan_layout(on, &join_layout),
            }
        }
        Path::HashJoin {
            plan_info,
            left,
            right,
            kind,
            hash_clauses: _,
            outer_hash_keys,
            inner_hash_keys,
            join_qual,
        } => {
            let left_layout = left.output_vars();
            let right_layout = right.output_vars();
            let mut join_layout = left_layout.clone();
            join_layout.extend(right_layout.clone());

            let outer_hash_keys = outer_hash_keys
                .into_iter()
                .map(|expr| fix_upper_expr(root, expr, &left, &left_layout))
                .collect::<Vec<_>>();
            let inner_hash_keys = inner_hash_keys
                .into_iter()
                .map(|expr| fix_upper_expr(root, expr, &right, &right_layout))
                .collect::<Vec<_>>();
            let hash_clauses = outer_hash_keys
                .iter()
                .cloned()
                .zip(inner_hash_keys.iter().cloned())
                .map(|(outer, inner)| {
                    Expr::op_auto(
                        crate::include::nodes::primnodes::OpExprKind::Eq,
                        vec![
                            lower_expr_to_plan_layout(outer, &join_layout),
                            lower_expr_to_plan_layout(inner, &join_layout),
                        ],
                    )
                })
                .collect::<Vec<_>>();
            let outer_hash_keys = outer_hash_keys
                .into_iter()
                .map(|expr| lower_expr_to_plan_layout(expr, &left_layout))
                .collect::<Vec<_>>();
            let inner_hash_keys = inner_hash_keys
                .into_iter()
                .map(|expr| lower_expr_to_plan_layout(expr, &right_layout))
                .collect::<Vec<_>>();
            let join_qual = join_qual
                .map(|expr| fix_join_expr(root, expr, &left, &left_layout, &right, &right_layout))
                .map(|expr| lower_expr_to_plan_layout(expr, &join_layout));

            let left_plan = set_plan_refs(root, *left);
            let right_plan = set_plan_refs(root, *right);
            let right_plan_info = right_plan.plan_info();

            Plan::HashJoin {
                plan_info,
                left: Box::new(left_plan),
                right: Box::new(Plan::Hash {
                    // :HACK: Keep the synthetic Hash node's displayed cost aligned with the
                    // inner path cost until EXPLAIN has a planner-native hash costing display.
                    plan_info: PlanEstimate::new(
                        right_plan_info.total_cost.as_f64(),
                        right_plan_info.total_cost.as_f64(),
                        right_plan_info.plan_rows.as_f64(),
                        right_plan_info.plan_width,
                    ),
                    input: Box::new(right_plan),
                    hash_keys: inner_hash_keys,
                }),
                kind,
                hash_clauses,
                hash_keys: outer_hash_keys,
                join_qual,
            }
        }
        Path::Projection {
            plan_info,
            input,
            targets,
            ..
        } => {
            let layout = input.output_vars();
            let targets = targets
                .into_iter()
                .map(|target| {
                    let expr = fix_upper_expr(root, target.expr, &input, &layout);
                    TargetEntry { expr, ..target }
                })
                .map(|target| lower_target_entry_to_plan_layout(target, &layout))
                .collect();
            Plan::Projection {
                plan_info,
                input: Box::new(set_plan_refs(root, *input)),
                targets,
            }
        }
        Path::OrderBy {
            plan_info,
            input,
            items,
        } => {
            let layout = input.output_vars();
            let items = match (root, aggregate_group_by(&input)) {
                (Some(root), Some(group_by)) => items
                    .into_iter()
                    .map(|item| OrderByEntry {
                        expr: lower_agg_output_expr(
                            expand_join_rte_vars(root, item.expr),
                            group_by,
                            &layout,
                        ),
                        ..item
                    })
                    .collect::<Vec<_>>(),
                (None, Some(group_by)) => items
                    .into_iter()
                    .map(|item| OrderByEntry {
                        expr: lower_agg_output_expr(item.expr, group_by, &layout),
                        ..item
                    })
                    .collect::<Vec<_>>(),
                (_, None) => items
                    .into_iter()
                    .map(|item| OrderByEntry {
                        expr: fix_upper_expr(root, item.expr, &input, &layout),
                        ..item
                    })
                    .collect::<Vec<_>>(),
            };
            Plan::OrderBy {
                plan_info,
                input: Box::new(set_plan_refs(root, *input)),
                items: items
                    .into_iter()
                    .map(|item| lower_order_by_entry_to_plan_layout(item, &layout))
                    .collect(),
            }
        }
        Path::Limit {
            plan_info,
            input,
            limit,
            offset,
        } => Plan::Limit {
            plan_info,
            input: Box::new(set_plan_refs(root, *input)),
            limit,
            offset,
        },
        Path::Aggregate {
            plan_info,
            slot_id,
            input,
            group_by,
            accumulators,
            having,
            output_columns,
        } => {
            let layout = input.output_vars();
            let aggregate_layout = aggregate_output_vars(slot_id, &group_by, &accumulators);
            let group_by = group_by
                .into_iter()
                .map(|expr| rewrite_semantic_expr_for_input_path(expr, &input, &layout))
                .map(|expr| lower_expr_to_plan_layout(expr, &layout))
                .collect();
            let accumulators = accumulators
                .into_iter()
                .map(|accum| lower_agg_accum_to_plan_layout(accum, &input, &layout))
                .collect();
            let having = having.map(|expr| lower_expr_to_plan_layout(expr, &aggregate_layout));
            Plan::Aggregate {
                plan_info,
                input: Box::new(set_plan_refs(root, *input)),
                group_by,
                accumulators,
                having,
                output_columns,
            }
        }
        Path::Values {
            plan_info,
            rows,
            output_columns,
            ..
        } => Plan::Values {
            plan_info,
            rows: rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|expr| lower_expr_to_plan_layout(expr, &[]))
                        .collect()
                })
                .collect(),
            output_columns,
        },
        Path::FunctionScan {
            plan_info, call, ..
        } => Plan::FunctionScan {
            plan_info,
            call: lower_set_returning_call_to_plan_layout(call, &[]),
        },
        Path::ProjectSet {
            plan_info,
            input,
            targets,
            ..
        } => {
            let layout = input.output_vars();
            Plan::ProjectSet {
                plan_info,
                input: Box::new(set_plan_refs(root, *input)),
                targets: targets
                    .into_iter()
                    .map(|target| lower_project_set_target_to_plan_layout(target, &layout))
                    .collect(),
            }
        }
    }
}

fn fix_upper_expr(root: Option<&PlannerInfo>, expr: Expr, path: &Path, layout: &[Expr]) -> Expr {
    if let Some(rewritten) = find_exposed_output_match(root, &expr, path) {
        return rewritten;
    }
    match root {
        Some(root) => rewrite_semantic_expr_for_path_or_expand_join_vars(root, expr, path, layout),
        None => rewrite_semantic_expr_for_path(expr, path, layout),
    }
}

#[derive(Debug)]
struct PathRewrite {
    expr: Expr,
    exposed: bool,
}

fn exprs_equivalent(root: Option<&PlannerInfo>, left: &Expr, right: &Expr) -> bool {
    if left == right {
        return true;
    }
    let Some(root) = root else {
        return false;
    };
    let expanded_left = expand_join_rte_vars(root, left.clone());
    let expanded_right = expand_join_rte_vars(root, right.clone());
    expanded_left == *right || expanded_right == *left || expanded_left == expanded_right
}

fn slot_var(slot_id: usize, attno: usize, vartype: crate::backend::parser::SqlType) -> Expr {
    Expr::Var(Var {
        varno: slot_id,
        varattno: attno,
        varlevelsup: 0,
        vartype,
    })
}

fn fully_expand_output_expr(expr: Expr, path: &Path) -> Expr {
    match expr {
        Expr::Var(var) if var.varlevelsup == 0 => expand_output_var(var, path),
        Expr::Aggref(aggref) => Expr::Aggref(Box::new(Aggref {
            args: aggref
                .args
                .into_iter()
                .map(|arg| fully_expand_output_expr(arg, path))
                .collect(),
            ..*aggref
        })),
        Expr::Op(op) => Expr::Op(Box::new(OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| fully_expand_output_expr(arg, path))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| fully_expand_output_expr(arg, path))
                .collect(),
            ..*bool_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| fully_expand_output_expr(arg, path))
                .collect(),
            ..*func
        })),
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            left: Box::new(fully_expand_output_expr(*saop.left, path)),
            right: Box::new(fully_expand_output_expr(*saop.right, path)),
            ..*saop
        })),
        Expr::Cast(inner, ty) => Expr::Cast(Box::new(fully_expand_output_expr(*inner, path)), ty),
        Expr::IsNull(inner) => Expr::IsNull(Box::new(fully_expand_output_expr(*inner, path))),
        Expr::IsNotNull(inner) => {
            Expr::IsNotNull(Box::new(fully_expand_output_expr(*inner, path)))
        }
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(fully_expand_output_expr(*left, path)),
            Box::new(fully_expand_output_expr(*right, path)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(fully_expand_output_expr(*left, path)),
            Box::new(fully_expand_output_expr(*right, path)),
        ),
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(fully_expand_output_expr(*left, path)),
            Box::new(fully_expand_output_expr(*right, path)),
        ),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            expr: Box::new(fully_expand_output_expr(*expr, path)),
            pattern: Box::new(fully_expand_output_expr(*pattern, path)),
            escape: escape.map(|expr| Box::new(fully_expand_output_expr(*expr, path))),
            case_insensitive,
            negated,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(fully_expand_output_expr(*expr, path)),
            pattern: Box::new(fully_expand_output_expr(*pattern, path)),
            escape: escape.map(|expr| Box::new(fully_expand_output_expr(*expr, path))),
            negated,
        },
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|element| fully_expand_output_expr(element, path))
                .collect(),
            array_type,
        },
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(fully_expand_output_expr(*array, path)),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript
                        .lower
                        .map(|expr| fully_expand_output_expr(expr, path)),
                    upper: subscript
                        .upper
                        .map(|expr| fully_expand_output_expr(expr, path)),
                })
                .collect(),
        },
        other => other,
    }
}

fn expand_output_var(var: Var, path: &Path) -> Expr {
    match path {
        Path::Projection {
            slot_id,
            input,
            targets,
            ..
        } if var.varno == *slot_id && var.varattno >= 1 && var.varattno <= targets.len() => {
            fully_expand_output_expr(targets[var.varattno - 1].expr.clone(), input)
        }
        Path::Filter { input, .. } | Path::OrderBy { input, .. } | Path::Limit { input, .. } => {
            expand_output_var(var, input)
        }
        Path::NestedLoopJoin { left, right, .. } | Path::HashJoin { left, right, .. } => {
            let expr = Expr::Var(var.clone());
            if left.output_vars().contains(&expr) {
                fully_expand_output_expr(expr, left)
            } else if right.output_vars().contains(&expr) {
                fully_expand_output_expr(expr, right)
            } else {
                Expr::Var(var)
            }
        }
        _ => Expr::Var(var),
    }
}

fn find_exposed_output_match(
    root: Option<&PlannerInfo>,
    expr: &Expr,
    path: &Path,
) -> Option<Expr> {
    match path {
        Path::Projection {
            slot_id,
            input,
            targets,
            ..
        } => {
            let input_layout = input.output_vars();
            targets.iter().enumerate().find_map(|(index, target)| {
                let semantic = rewrite_expr_against_layout(target.expr.clone(), &input_layout);
                let rewritten_semantic =
                    rewrite_semantic_expr_for_path(target.expr.clone(), input, &input_layout);
                let expanded_semantic = fully_expand_output_expr(target.expr.clone(), input);
                (exprs_equivalent(root, expr, &target.expr)
                    || exprs_equivalent(root, expr, &semantic)
                    || exprs_equivalent(root, expr, &rewritten_semantic)
                    || exprs_equivalent(root, expr, &expanded_semantic))
                .then(|| slot_var(*slot_id, index + 1, target.sql_type))
            })
        }
        Path::Filter { input, .. } | Path::OrderBy { input, .. } | Path::Limit { input, .. } => {
            find_exposed_output_match(root, expr, input)
        }
        _ => None,
    }
}

fn maybe_rewrite_against_path(
    root: Option<&PlannerInfo>,
    expr: &Expr,
    path: &Path,
    layout: &[Expr],
) -> Option<PathRewrite> {
    if let Some(rewritten) = find_exposed_output_match(root, expr, path) {
        return Some(PathRewrite {
            expr: rewritten,
            exposed: true,
        });
    }
    let rewritten = rewrite_join_input_expr(root, expr.clone(), path, layout);
    let exposed = layout.contains(expr) || layout.contains(&rewritten);
    (rewritten != *expr || exposed).then_some(PathRewrite {
        expr: rewritten,
        exposed,
    })
}

fn rewrite_join_whole_expr(
    root: Option<&PlannerInfo>,
    expr: &Expr,
    left: &Path,
    left_layout: &[Expr],
    right: &Path,
    right_layout: &[Expr],
) -> Option<Expr> {
    let expr_relids = expr_relids(expr);
    let left_relids = path_relids(left);
    let right_relids = path_relids(right);

    if !expr_relids.is_empty() {
        if relids_subset(&expr_relids, &left_relids) {
            return maybe_rewrite_against_path(root, expr, left, left_layout).map(|m| m.expr);
        }
        if relids_subset(&expr_relids, &right_relids) {
            return maybe_rewrite_against_path(root, expr, right, right_layout).map(|m| m.expr);
        }
    }

    match (
        maybe_rewrite_against_path(root, expr, left, left_layout),
        maybe_rewrite_against_path(root, expr, right, right_layout),
    ) {
        (Some(left), None) => Some(left.expr),
        (None, Some(right)) => Some(right.expr),
        (Some(left), Some(right)) if left.exposed && !right.exposed => Some(left.expr),
        (Some(left), Some(right)) if !left.exposed && right.exposed => Some(right.expr),
        (Some(left), Some(right)) if left.expr == right.expr => Some(left.expr),
        _ => None,
    }
}

fn fix_join_expr(
    root: Option<&PlannerInfo>,
    expr: Expr,
    left: &Path,
    left_layout: &[Expr],
    right: &Path,
    right_layout: &[Expr],
) -> Expr {
    if let Some(rewritten) =
        rewrite_join_whole_expr(root, &expr, left, left_layout, right, right_layout)
    {
        return if rewritten == expr {
            rewritten
        } else {
            fix_join_expr(root, rewritten, left, left_layout, right, right_layout)
        };
    }

    let rebuilt = match expr {
        Expr::Var(_) | Expr::Column(_) => expr,
        Expr::Aggref(aggref) => Expr::Aggref(Box::new(Aggref {
            args: aggref
                .args
                .into_iter()
                .map(|arg| fix_join_expr(root, arg, left, left_layout, right, right_layout))
                .collect(),
            ..*aggref
        })),
        Expr::Op(op) => Expr::Op(Box::new(OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| fix_join_expr(root, arg, left, left_layout, right, right_layout))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| fix_join_expr(root, arg, left, left_layout, right, right_layout))
                .collect(),
            ..*bool_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| fix_join_expr(root, arg, left, left_layout, right, right_layout))
                .collect(),
            ..*func
        })),
        Expr::SubLink(sublink) => {
            Expr::SubLink(Box::new(crate::include::nodes::primnodes::SubLink {
                testexpr: sublink.testexpr.map(|expr| {
                    Box::new(fix_join_expr(
                        root,
                        *expr,
                        left,
                        left_layout,
                        right,
                        right_layout,
                    ))
                }),
                ..*sublink
            }))
        }
        Expr::SubPlan(subplan) => {
            Expr::SubPlan(Box::new(crate::include::nodes::primnodes::SubPlan {
                testexpr: subplan.testexpr.map(|expr| {
                    Box::new(fix_join_expr(
                        root,
                        *expr,
                        left,
                        left_layout,
                        right,
                        right_layout,
                    ))
                }),
                ..*subplan
            }))
        }
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            left: Box::new(fix_join_expr(
                root,
                *saop.left,
                left,
                left_layout,
                right,
                right_layout,
            )),
            right: Box::new(fix_join_expr(
                root,
                *saop.right,
                left,
                left_layout,
                right,
                right_layout,
            )),
            ..*saop
        })),
        Expr::Cast(inner, ty) => Expr::Cast(
            Box::new(fix_join_expr(
                root,
                *inner,
                left,
                left_layout,
                right,
                right_layout,
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
            expr: Box::new(fix_join_expr(
                root,
                *expr,
                left,
                left_layout,
                right,
                right_layout,
            )),
            pattern: Box::new(fix_join_expr(
                root,
                *pattern,
                left,
                left_layout,
                right,
                right_layout,
            )),
            escape: escape.map(|expr| {
                Box::new(fix_join_expr(
                    root,
                    *expr,
                    left,
                    left_layout,
                    right,
                    right_layout,
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
            expr: Box::new(fix_join_expr(
                root,
                *expr,
                left,
                left_layout,
                right,
                right_layout,
            )),
            pattern: Box::new(fix_join_expr(
                root,
                *pattern,
                left,
                left_layout,
                right,
                right_layout,
            )),
            escape: escape.map(|expr| {
                Box::new(fix_join_expr(
                    root,
                    *expr,
                    left,
                    left_layout,
                    right,
                    right_layout,
                ))
            }),
            negated,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(fix_join_expr(
            root,
            *inner,
            left,
            left_layout,
            right,
            right_layout,
        ))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(fix_join_expr(
            root,
            *inner,
            left,
            left_layout,
            right,
            right_layout,
        ))),
        Expr::IsDistinctFrom(left_expr, right_expr) => Expr::IsDistinctFrom(
            Box::new(fix_join_expr(
                root,
                *left_expr,
                left,
                left_layout,
                right,
                right_layout,
            )),
            Box::new(fix_join_expr(
                root,
                *right_expr,
                left,
                left_layout,
                right,
                right_layout,
            )),
        ),
        Expr::IsNotDistinctFrom(left_expr, right_expr) => Expr::IsNotDistinctFrom(
            Box::new(fix_join_expr(
                root,
                *left_expr,
                left,
                left_layout,
                right,
                right_layout,
            )),
            Box::new(fix_join_expr(
                root,
                *right_expr,
                left,
                left_layout,
                right,
                right_layout,
            )),
        ),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|element| fix_join_expr(root, element, left, left_layout, right, right_layout))
                .collect(),
            array_type,
        },
        Expr::Coalesce(left_expr, right_expr) => Expr::Coalesce(
            Box::new(fix_join_expr(
                root,
                *left_expr,
                left,
                left_layout,
                right,
                right_layout,
            )),
            Box::new(fix_join_expr(
                root,
                *right_expr,
                left,
                left_layout,
                right,
                right_layout,
            )),
        ),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(fix_join_expr(
                root,
                *array,
                left,
                left_layout,
                right,
                right_layout,
            )),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript.lower.map(|expr| {
                        fix_join_expr(root, expr, left, left_layout, right, right_layout)
                    }),
                    upper: subscript.upper.map(|expr| {
                        fix_join_expr(root, expr, left, left_layout, right, right_layout)
                    }),
                })
                .collect(),
        },
        other => other,
    };

    match rewrite_join_whole_expr(root, &rebuilt, left, left_layout, right, right_layout) {
        Some(rewritten) if rewritten != rebuilt => {
            fix_join_expr(root, rewritten, left, left_layout, right, right_layout)
        }
        Some(rewritten) => rewritten,
        None => rebuilt,
    }
}
