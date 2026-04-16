use std::sync::atomic::{AtomicUsize, Ordering};

use super::rewrite_semantic_expr_for_path;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::nodes::datum::Value;
use crate::include::nodes::pathnodes::{Path, PathKey, PathTarget};
use crate::include::nodes::plannodes::{Plan, PlanEstimate};
use crate::include::nodes::primnodes::{
    AggAccum, Aggref, BoolExpr, Expr, ExprArraySubscript, FuncExpr, JoinType, OpExpr, OrderByEntry,
    ProjectSetTarget, QueryColumn, ScalarArrayOpExpr, SetReturningCall, SubLinkType, TargetEntry,
    Var, attrno_index, is_system_attr, user_attrno,
};

// :HACK: Planner-generated slot Vars still share the same Var identity space as parse-time
// rtindex Vars, so keep synthetic slots in a disjoint high range until slot identity is split
// from relation identity more cleanly.
const SYNTHETIC_SLOT_ID_BASE: usize = 1_000_000;
const RTE_SLOT_ID_BASE: usize = 2_000_000;

static NEXT_SYNTHETIC_SLOT_ID: AtomicUsize = AtomicUsize::new(SYNTHETIC_SLOT_ID_BASE);

pub(crate) fn next_synthetic_slot_id() -> usize {
    NEXT_SYNTHETIC_SLOT_ID.fetch_add(1, Ordering::Relaxed)
}

pub(crate) fn is_synthetic_slot_id(slot_id: usize) -> bool {
    slot_id >= SYNTHETIC_SLOT_ID_BASE
}

pub(crate) fn rte_slot_id(rtindex: usize) -> usize {
    RTE_SLOT_ID_BASE + rtindex
}

pub(crate) fn rte_slot_varno(slot_id: usize) -> Option<usize> {
    if slot_id >= RTE_SLOT_ID_BASE {
        Some(slot_id - RTE_SLOT_ID_BASE)
    } else {
        None
    }
}

impl Path {
    pub fn into_plan(self) -> Plan {
        super::setrefs::create_plan_without_root(self)
    }

    pub fn plan_info(&self) -> PlanEstimate {
        match self {
            Self::Result { plan_info }
            | Self::Append { plan_info, .. }
            | Self::SeqScan { plan_info, .. }
            | Self::IndexScan { plan_info, .. }
            | Self::Filter { plan_info, .. }
            | Self::NestedLoopJoin { plan_info, .. }
            | Self::HashJoin { plan_info, .. }
            | Self::Projection { plan_info, .. }
            | Self::OrderBy { plan_info, .. }
            | Self::Limit { plan_info, .. }
            | Self::Aggregate { plan_info, .. }
            | Self::CteScan { plan_info, .. }
            | Self::WorkTableScan { plan_info, .. }
            | Self::RecursiveUnion { plan_info, .. }
            | Self::Values { plan_info, .. }
            | Self::FunctionScan { plan_info, .. }
            | Self::ProjectSet { plan_info, .. } => *plan_info,
        }
    }

    pub fn columns(&self) -> Vec<QueryColumn> {
        match self {
            Self::Result { .. } => Vec::new(),
            Self::Append { desc, .. } => desc
                .columns
                .iter()
                .map(|c| QueryColumn {
                    name: c.name.clone(),
                    sql_type: c.sql_type,
                })
                .collect(),
            Self::SeqScan { desc, .. } | Self::IndexScan { desc, .. } => desc
                .columns
                .iter()
                .map(|c| QueryColumn {
                    name: c.name.clone(),
                    sql_type: c.sql_type,
                })
                .collect(),
            Self::Filter { input, .. }
            | Self::OrderBy { input, .. }
            | Self::Limit { input, .. } => input.columns(),
            Self::Projection { targets, .. } => targets
                .iter()
                .map(|t| QueryColumn {
                    name: t.name.clone(),
                    sql_type: t.sql_type,
                })
                .collect(),
            Self::Aggregate { output_columns, .. } => output_columns.clone(),
            Self::CteScan { output_columns, .. } => output_columns.clone(),
            Self::WorkTableScan { output_columns, .. }
            | Self::RecursiveUnion { output_columns, .. } => output_columns.clone(),
            Self::NestedLoopJoin { left, right, .. } | Self::HashJoin { left, right, .. } => {
                let mut cols = left.columns();
                cols.extend(right.columns());
                cols
            }
            Self::FunctionScan { call, .. } => call.output_columns().to_vec(),
            Self::Values { output_columns, .. } => output_columns.clone(),
            Self::ProjectSet { targets, .. } => targets
                .iter()
                .map(|target| match target {
                    ProjectSetTarget::Scalar(entry) => QueryColumn {
                        name: entry.name.clone(),
                        sql_type: entry.sql_type,
                    },
                    ProjectSetTarget::Set { name, sql_type, .. } => QueryColumn {
                        name: name.clone(),
                        sql_type: *sql_type,
                    },
                })
                .collect(),
        }
    }

    pub fn output_vars(&self) -> Vec<Expr> {
        match self {
            Self::Result { .. } => Vec::new(),
            Self::Append {
                source_id, desc, ..
            } => slot_output_vars(*source_id, &desc.columns, |column| column.sql_type),
            Self::SeqScan {
                source_id, desc, ..
            }
            | Self::IndexScan {
                source_id, desc, ..
            } => slot_output_vars(*source_id, &desc.columns, |column| column.sql_type),
            Self::Filter { input, .. }
            | Self::OrderBy { input, .. }
            | Self::Limit { input, .. } => input.output_vars(),
            Self::Projection {
                slot_id, targets, ..
            } => targets
                .iter()
                .enumerate()
                .map(|(index, target)| slot_var(*slot_id, user_attrno(index), target.sql_type))
                .collect(),
            Self::Aggregate {
                slot_id,
                group_by,
                accumulators,
                ..
            } => aggregate_output_vars(*slot_id, group_by, accumulators),
            Self::Values {
                slot_id,
                output_columns,
                ..
            } => slot_output_vars(*slot_id, output_columns, |column| column.sql_type),
            Self::CteScan {
                slot_id,
                output_columns,
                ..
            } => slot_output_vars(*slot_id, output_columns, |column| column.sql_type),
            Self::WorkTableScan {
                slot_id,
                output_columns,
                ..
            }
            | Self::RecursiveUnion {
                slot_id,
                output_columns,
                ..
            } => slot_output_vars(*slot_id, output_columns, |column| column.sql_type),
            Self::FunctionScan { slot_id, call, .. } => {
                slot_output_vars(*slot_id, call.output_columns(), |column| column.sql_type)
            }
            Self::ProjectSet {
                slot_id, targets, ..
            } => targets
                .iter()
                .enumerate()
                .map(|(index, target)| match target {
                    ProjectSetTarget::Scalar(entry) => {
                        slot_var(*slot_id, user_attrno(index), entry.sql_type)
                    }
                    ProjectSetTarget::Set { sql_type, .. } => {
                        slot_var(*slot_id, user_attrno(index), *sql_type)
                    }
                })
                .collect(),
            Self::NestedLoopJoin { left, right, .. } => {
                let mut vars = left.output_vars();
                vars.extend(right.output_vars());
                vars
            }
            Self::HashJoin { left, right, .. } => {
                let mut vars = left.output_vars();
                vars.extend(right.output_vars());
                vars
            }
        }
    }

    pub fn output_target(&self) -> PathTarget {
        match self {
            Self::Filter { input, .. } | Self::OrderBy { input, .. } | Self::Limit { input, .. } => {
                input.output_target()
            }
            Self::Projection {
                slot_id, targets, ..
            } => PathTarget::with_sortgrouprefs(
                targets
                    .iter()
                    .enumerate()
                    .map(|(index, target)| slot_var(*slot_id, user_attrno(index), target.sql_type))
                    .collect(),
                targets
                    .iter()
                    .map(|target| target.ressortgroupref)
                    .collect(),
            ),
            _ => PathTarget::new(self.output_vars()),
        }
    }

    pub fn pathkeys(&self) -> Vec<PathKey> {
        match self {
            Self::Result { .. }
            | Self::Append { .. }
            | Self::SeqScan { .. }
            | Self::Aggregate { .. }
            | Self::CteScan { .. }
            | Self::WorkTableScan { .. }
            | Self::RecursiveUnion { .. }
            | Self::Values { .. }
            | Self::FunctionScan { .. }
            | Self::ProjectSet { .. } => Vec::new(),
            Self::IndexScan { pathkeys, .. } => pathkeys.clone(),
            Self::Filter { input, .. } | Self::Limit { input, .. } => input.pathkeys(),
            Self::Projection {
                slot_id,
                targets,
                input,
                ..
            } => project_pathkeys(*slot_id, input, targets, &input.pathkeys()),
            Self::OrderBy { items, .. } => items
                .iter()
                .map(|item| PathKey {
                    expr: item.expr.clone(),
                    ressortgroupref: item.ressortgroupref,
                    descending: item.descending,
                    nulls_first: item.nulls_first,
                })
                .collect(),
            Self::NestedLoopJoin { left, kind, .. }
                if matches!(kind, JoinType::Inner | JoinType::Cross | JoinType::Left) =>
            {
                left.pathkeys()
            }
            Self::HashJoin { .. } => Vec::new(),
            Self::NestedLoopJoin { .. } => Vec::new(),
        }
    }
}

fn slot_output_vars<T>(
    slot_id: usize,
    columns: &[T],
    sql_type: impl Fn(&T) -> SqlType,
) -> Vec<Expr> {
    columns
        .iter()
        .enumerate()
        .map(|(index, column)| slot_var(slot_id, user_attrno(index), sql_type(column)))
        .collect()
}

fn slot_var(
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

fn project_pathkeys(
    slot_id: usize,
    input: &Path,
    targets: &[TargetEntry],
    input_pathkeys: &[PathKey],
) -> Vec<PathKey> {
    let input_layout = input.output_vars();
    let passthrough_boundary = targets.len() == input_layout.len()
        && targets
            .iter()
            .zip(input_layout.iter())
            .all(|(target, expr)| target.expr == *expr);
    input_pathkeys
        .iter()
        .map(|key| {
            let expr = targets
                .iter()
                .enumerate()
                .find(|(_, target)| {
                    key.ressortgroupref != 0 && target.ressortgroupref == key.ressortgroupref
                })
                .map(|(index, target)| slot_var(slot_id, user_attrno(index), target.sql_type))
                .or_else(|| {
                    passthrough_boundary.then(|| {
                        input_layout
                            .iter()
                            .position(|expr| *expr == key.expr)
                            .map(|index| {
                                slot_var(slot_id, user_attrno(index), targets[index].sql_type)
                            })
                    })?
                })
                .or_else(|| {
                    targets
                        .iter()
                        .enumerate()
                        .find(|(_, target)| target.expr == key.expr)
                        .map(|(index, target)| slot_var(slot_id, user_attrno(index), target.sql_type))
                })
                .unwrap_or_else(|| key.expr.clone());
            PathKey {
                expr,
                ressortgroupref: key.ressortgroupref,
                descending: key.descending,
                nulls_first: key.nulls_first,
            }
        })
        .collect()
}

pub(super) fn aggregate_output_vars(
    slot_id: usize,
    group_by: &[Expr],
    accumulators: &[AggAccum],
) -> Vec<Expr> {
    let mut vars = Vec::with_capacity(group_by.len() + accumulators.len());
    for (index, expr) in group_by.iter().enumerate() {
        vars.push(slot_var(slot_id, user_attrno(index), expr_sql_type(expr)));
    }
    for (index, accum) in accumulators.iter().enumerate() {
        vars.push(slot_var(
            slot_id,
            user_attrno(group_by.len() + index),
            accum.sql_type,
        ));
    }
    vars
}

pub(super) fn rewrite_target_entry_against_layout(
    target: TargetEntry,
    layout: &[Expr],
) -> TargetEntry {
    TargetEntry {
        expr: rewrite_expr_against_layout(target.expr, layout),
        ..target
    }
}

pub(super) fn lower_target_entry_to_plan_layout(
    target: TargetEntry,
    layout: &[Expr],
) -> TargetEntry {
    TargetEntry {
        expr: lower_expr_to_plan_layout(target.expr, layout),
        ..target
    }
}

pub(super) fn lower_order_by_entry_to_plan_layout(
    item: OrderByEntry,
    layout: &[Expr],
) -> OrderByEntry {
    OrderByEntry {
        expr: lower_expr_to_plan_layout(item.expr, layout),
        ..item
    }
}

pub(super) fn rewrite_project_set_target_against_layout(
    target: ProjectSetTarget,
    layout: &[Expr],
) -> ProjectSetTarget {
    match target {
        ProjectSetTarget::Scalar(entry) => {
            ProjectSetTarget::Scalar(rewrite_target_entry_against_layout(entry, layout))
        }
        ProjectSetTarget::Set {
            name,
            call,
            sql_type,
            column_index,
        } => ProjectSetTarget::Set {
            name,
            call: rewrite_set_returning_call_against_layout(call, layout),
            sql_type,
            column_index,
        },
    }
}

pub(super) fn lower_project_set_target_to_plan_layout(
    target: ProjectSetTarget,
    layout: &[Expr],
) -> ProjectSetTarget {
    match target {
        ProjectSetTarget::Scalar(entry) => {
            ProjectSetTarget::Scalar(lower_target_entry_to_plan_layout(entry, layout))
        }
        ProjectSetTarget::Set {
            name,
            call,
            sql_type,
            column_index,
        } => ProjectSetTarget::Set {
            name,
            call: lower_set_returning_call_to_plan_layout(call, layout),
            sql_type,
            column_index,
        },
    }
}

pub(super) fn rewrite_set_returning_call_against_layout(
    call: SetReturningCall,
    layout: &[Expr],
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
            start: rewrite_expr_against_layout(start, layout),
            stop: rewrite_expr_against_layout(stop, layout),
            step: rewrite_expr_against_layout(step, layout),
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
                .map(|arg| rewrite_expr_against_layout(arg, layout))
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
                .map(|arg| rewrite_expr_against_layout(arg, layout))
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
                .map(|arg| rewrite_expr_against_layout(arg, layout))
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
                .map(|arg| rewrite_expr_against_layout(arg, layout))
                .collect(),
            output_columns,
        },
        SetReturningCall::UserDefined {
            proc_oid,
            func_variadic,
            args,
            output_columns,
        } => SetReturningCall::UserDefined {
            proc_oid,
            func_variadic,
            args: args
                .into_iter()
                .map(|arg| rewrite_expr_against_layout(arg, layout))
                .collect(),
            output_columns,
        },
    }
}

pub(super) fn lower_set_returning_call_to_plan_layout(
    call: SetReturningCall,
    layout: &[Expr],
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
            start: lower_expr_to_plan_layout(start, layout),
            stop: lower_expr_to_plan_layout(stop, layout),
            step: lower_expr_to_plan_layout(step, layout),
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
                .map(|arg| lower_expr_to_plan_layout(arg, layout))
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
                .map(|arg| lower_expr_to_plan_layout(arg, layout))
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
                .map(|arg| lower_expr_to_plan_layout(arg, layout))
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
                .map(|arg| lower_expr_to_plan_layout(arg, layout))
                .collect(),
            output_columns,
        },
        SetReturningCall::UserDefined {
            proc_oid,
            func_variadic,
            args,
            output_columns,
        } => SetReturningCall::UserDefined {
            proc_oid,
            func_variadic,
            args: args
                .into_iter()
                .map(|arg| lower_expr_to_plan_layout(arg, layout))
                .collect(),
            output_columns,
        },
    }
}

pub(super) fn lower_agg_accum_to_plan_layout(
    accum: AggAccum,
    path: &Path,
    layout: &[Expr],
) -> AggAccum {
    AggAccum {
        args: accum
            .args
            .into_iter()
            .map(|arg| {
                lower_expr_to_plan_layout(
                    rewrite_semantic_expr_for_input_path(arg, path, layout),
                    layout,
                )
            })
            .collect(),
        ..accum
    }
}

fn rewrite_expr_for_input_path(expr: Expr, path: &Path, layout: &[Expr]) -> Expr {
    match path {
        Path::Projection {
            slot_id,
            input,
            targets,
            ..
        } => {
            let input_layout = input.output_vars();
            let passthrough_boundary = targets.len() == input_layout.len()
                && targets
                    .iter()
                    .zip(input_layout.iter())
                    .all(|(target, expr)| target.expr == *expr);
            if let Some((index, target)) = targets
                .iter()
                .enumerate()
                .find(|(_, target)| target.expr == expr)
            {
                slot_var(*slot_id, user_attrno(index), target.sql_type)
            } else {
                let rewritten_input_expr =
                    rewrite_semantic_expr_for_path(expr.clone(), input, &input_layout);
                if passthrough_boundary
                    && is_synthetic_slot_id(*slot_id)
                    && let Some(index) = input_layout
                        .iter()
                        .position(|candidate| *candidate == rewritten_input_expr)
                {
                    return slot_var(*slot_id, user_attrno(index), targets[index].sql_type);
                }
                if let Some((index, target)) = targets.iter().enumerate().find(|(_, target)| {
                    target.expr == rewritten_input_expr
                        || rewrite_semantic_expr_for_path(target.expr.clone(), input, &input_layout)
                            == rewritten_input_expr
                }) {
                    slot_var(*slot_id, user_attrno(index), target.sql_type)
                } else {
                    rewrite_expr_against_layout(expr, layout)
                }
            }
        }
        Path::Filter { input, .. } | Path::OrderBy { input, .. } | Path::Limit { input, .. } => {
            rewrite_expr_for_input_path(expr, input, layout)
        }
        Path::NestedLoopJoin { left, right, .. } | Path::HashJoin { left, right, .. } => {
            let left_layout = left.output_vars();
            let rewritten_left = rewrite_expr_for_input_path(expr.clone(), left, &left_layout);
            if rewritten_left != expr || left_layout.contains(&expr) {
                return rewritten_left;
            }
            let right_layout = right.output_vars();
            let rewritten_right = rewrite_expr_for_input_path(expr.clone(), right, &right_layout);
            if rewritten_right != expr || right_layout.contains(&expr) {
                return rewritten_right;
            }
            rewrite_expr_against_layout(expr, layout)
        }
        Path::RecursiveUnion { .. } => {
            if let Expr::Var(var) = &expr
                && var.varlevelsup == 0
                && let Some(index) = attrno_index(var.varattno)
                && let Some(candidate) = layout.get(index)
                && expr_sql_type(candidate) == var.vartype
            {
                return candidate.clone();
            }
            rewrite_expr_against_layout(expr, layout)
        }
        _ => rewrite_expr_against_layout(expr, layout),
    }
}

pub(crate) fn rewrite_semantic_expr_for_input_path(
    expr: Expr,
    path: &Path,
    layout: &[Expr],
) -> Expr {
    let rewritten = rewrite_expr_for_input_path(expr.clone(), path, layout);
    if rewritten != expr {
        return rewritten;
    }
    match expr {
        Expr::Aggref(aggref) => Expr::Aggref(Box::new(Aggref {
            args: aggref
                .args
                .into_iter()
                .map(|arg| rewrite_semantic_expr_for_input_path(arg, path, layout))
                .collect(),
            ..*aggref
        })),
        Expr::Op(op) => Expr::Op(Box::new(OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| rewrite_semantic_expr_for_input_path(arg, path, layout))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| rewrite_semantic_expr_for_input_path(arg, path, layout))
                .collect(),
            ..*bool_expr
        })),
        Expr::Case(case_expr) => Expr::Case(Box::new(crate::include::nodes::primnodes::CaseExpr {
            arg: case_expr
                .arg
                .map(|arg| Box::new(rewrite_semantic_expr_for_input_path(*arg, path, layout))),
            args: case_expr
                .args
                .into_iter()
                .map(|arm| crate::include::nodes::primnodes::CaseWhen {
                    expr: rewrite_semantic_expr_for_input_path(arm.expr, path, layout),
                    result: rewrite_semantic_expr_for_input_path(arm.result, path, layout),
                })
                .collect(),
            defresult: Box::new(rewrite_semantic_expr_for_input_path(
                *case_expr.defresult,
                path,
                layout,
            )),
            ..*case_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| rewrite_semantic_expr_for_input_path(arg, path, layout))
                .collect(),
            ..*func
        })),
        Expr::SubLink(sublink) => {
            Expr::SubLink(Box::new(crate::include::nodes::primnodes::SubLink {
                testexpr: sublink.testexpr.map(|expr| {
                    Box::new(rewrite_semantic_expr_for_input_path(*expr, path, layout))
                }),
                ..*sublink
            }))
        }
        Expr::SubPlan(subplan) => {
            Expr::SubPlan(Box::new(crate::include::nodes::primnodes::SubPlan {
                testexpr: subplan.testexpr.map(|expr| {
                    Box::new(rewrite_semantic_expr_for_input_path(*expr, path, layout))
                }),
                ..*subplan
            }))
        }
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            left: Box::new(rewrite_semantic_expr_for_input_path(
                *saop.left, path, layout,
            )),
            right: Box::new(rewrite_semantic_expr_for_input_path(
                *saop.right,
                path,
                layout,
            )),
            ..*saop
        })),
        Expr::Cast(inner, ty) => Expr::Cast(
            Box::new(rewrite_semantic_expr_for_input_path(*inner, path, layout)),
            ty,
        ),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            expr: Box::new(rewrite_semantic_expr_for_input_path(*expr, path, layout)),
            pattern: Box::new(rewrite_semantic_expr_for_input_path(*pattern, path, layout)),
            escape: escape
                .map(|expr| Box::new(rewrite_semantic_expr_for_input_path(*expr, path, layout))),
            case_insensitive,
            negated,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(rewrite_semantic_expr_for_input_path(*expr, path, layout)),
            pattern: Box::new(rewrite_semantic_expr_for_input_path(*pattern, path, layout)),
            escape: escape
                .map(|expr| Box::new(rewrite_semantic_expr_for_input_path(*expr, path, layout))),
            negated,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(rewrite_semantic_expr_for_input_path(
            *inner, path, layout,
        ))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(rewrite_semantic_expr_for_input_path(
            *inner, path, layout,
        ))),
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(rewrite_semantic_expr_for_input_path(*left, path, layout)),
            Box::new(rewrite_semantic_expr_for_input_path(*right, path, layout)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(rewrite_semantic_expr_for_input_path(*left, path, layout)),
            Box::new(rewrite_semantic_expr_for_input_path(*right, path, layout)),
        ),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|element| rewrite_semantic_expr_for_input_path(element, path, layout))
                .collect(),
            array_type,
        },
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(rewrite_semantic_expr_for_input_path(*left, path, layout)),
            Box::new(rewrite_semantic_expr_for_input_path(*right, path, layout)),
        ),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(rewrite_semantic_expr_for_input_path(*array, path, layout)),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript
                        .lower
                        .map(|expr| rewrite_semantic_expr_for_input_path(expr, path, layout)),
                    upper: subscript
                        .upper
                        .map(|expr| rewrite_semantic_expr_for_input_path(expr, path, layout)),
                })
                .collect(),
        },
        other => other,
    }
}

pub(super) fn lower_agg_output_expr(
    expr: Expr,
    group_by: &[Expr],
    agg_output_layout: &[Expr],
) -> Expr {
    if let Some(index) = group_by.iter().position(|group_expr| *group_expr == expr) {
        return agg_output_layout[index].clone();
    }
    match expr {
        Expr::Aggref(aggref) => agg_output_layout
            .get(group_by.len() + aggref.aggno)
            .cloned()
            .unwrap_or_else(|| panic!("aggregate output slot {} missing", aggref.aggno)),
        Expr::Op(op) => Expr::Op(Box::new(OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| lower_agg_output_expr(arg, group_by, agg_output_layout))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| lower_agg_output_expr(arg, group_by, agg_output_layout))
                .collect(),
            ..*bool_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| lower_agg_output_expr(arg, group_by, agg_output_layout))
                .collect(),
            ..*func
        })),
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            left: Box::new(lower_agg_output_expr(
                *saop.left,
                group_by,
                agg_output_layout,
            )),
            right: Box::new(lower_agg_output_expr(
                *saop.right,
                group_by,
                agg_output_layout,
            )),
            ..*saop
        })),
        Expr::Cast(inner, ty) => Expr::Cast(
            Box::new(lower_agg_output_expr(*inner, group_by, agg_output_layout)),
            ty,
        ),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            expr: Box::new(lower_agg_output_expr(*expr, group_by, agg_output_layout)),
            pattern: Box::new(lower_agg_output_expr(*pattern, group_by, agg_output_layout)),
            escape: escape
                .map(|expr| Box::new(lower_agg_output_expr(*expr, group_by, agg_output_layout))),
            case_insensitive,
            negated,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(lower_agg_output_expr(*expr, group_by, agg_output_layout)),
            pattern: Box::new(lower_agg_output_expr(*pattern, group_by, agg_output_layout)),
            escape: escape
                .map(|expr| Box::new(lower_agg_output_expr(*expr, group_by, agg_output_layout))),
            negated,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(lower_agg_output_expr(
            *inner,
            group_by,
            agg_output_layout,
        ))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(lower_agg_output_expr(
            *inner,
            group_by,
            agg_output_layout,
        ))),
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(lower_agg_output_expr(*left, group_by, agg_output_layout)),
            Box::new(lower_agg_output_expr(*right, group_by, agg_output_layout)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(lower_agg_output_expr(*left, group_by, agg_output_layout)),
            Box::new(lower_agg_output_expr(*right, group_by, agg_output_layout)),
        ),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|element| lower_agg_output_expr(element, group_by, agg_output_layout))
                .collect(),
            array_type,
        },
        Expr::SubLink(sublink) => {
            Expr::SubLink(Box::new(crate::include::nodes::primnodes::SubLink {
                testexpr: sublink.testexpr.map(|expr| {
                    Box::new(lower_agg_output_expr(*expr, group_by, agg_output_layout))
                }),
                ..*sublink
            }))
        }
        Expr::SubPlan(subplan) => {
            Expr::SubPlan(Box::new(crate::include::nodes::primnodes::SubPlan {
                testexpr: subplan.testexpr.map(|expr| {
                    Box::new(lower_agg_output_expr(*expr, group_by, agg_output_layout))
                }),
                ..*subplan
            }))
        }
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(lower_agg_output_expr(*left, group_by, agg_output_layout)),
            Box::new(lower_agg_output_expr(*right, group_by, agg_output_layout)),
        ),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(lower_agg_output_expr(*array, group_by, agg_output_layout)),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript
                        .lower
                        .map(|expr| lower_agg_output_expr(expr, group_by, agg_output_layout)),
                    upper: subscript
                        .upper
                        .map(|expr| lower_agg_output_expr(expr, group_by, agg_output_layout)),
                })
                .collect(),
        },
        other => other,
    }
}

pub(super) fn rewrite_expr_against_layout(expr: Expr, layout: &[Expr]) -> Expr {
    match expr {
        Expr::Column(index) => layout.get(index).cloned().unwrap_or(Expr::Column(index)),
        Expr::Var(var) => Expr::Var(var),
        Expr::Aggref(aggref) => Expr::Aggref(Box::new(Aggref {
            args: aggref
                .args
                .into_iter()
                .map(|arg| rewrite_expr_against_layout(arg, layout))
                .collect(),
            ..*aggref
        })),
        Expr::Op(op) => Expr::Op(Box::new(OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| rewrite_expr_against_layout(arg, layout))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| rewrite_expr_against_layout(arg, layout))
                .collect(),
            ..*bool_expr
        })),
        Expr::Case(case_expr) => Expr::Case(Box::new(crate::include::nodes::primnodes::CaseExpr {
            arg: case_expr
                .arg
                .map(|arg| Box::new(rewrite_expr_against_layout(*arg, layout))),
            args: case_expr
                .args
                .into_iter()
                .map(|arm| crate::include::nodes::primnodes::CaseWhen {
                    expr: rewrite_expr_against_layout(arm.expr, layout),
                    result: rewrite_expr_against_layout(arm.result, layout),
                })
                .collect(),
            defresult: Box::new(rewrite_expr_against_layout(*case_expr.defresult, layout)),
            ..*case_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| rewrite_expr_against_layout(arg, layout))
                .collect(),
            ..*func
        })),
        Expr::SubLink(sublink) => {
            Expr::SubLink(Box::new(crate::include::nodes::primnodes::SubLink {
                testexpr: sublink
                    .testexpr
                    .map(|expr| Box::new(rewrite_expr_against_layout(*expr, layout))),
                ..*sublink
            }))
        }
        Expr::SubPlan(subplan) => {
            Expr::SubPlan(Box::new(crate::include::nodes::primnodes::SubPlan {
                testexpr: subplan
                    .testexpr
                    .map(|expr| Box::new(rewrite_expr_against_layout(*expr, layout))),
                ..*subplan
            }))
        }
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            left: Box::new(rewrite_expr_against_layout(*saop.left, layout)),
            right: Box::new(rewrite_expr_against_layout(*saop.right, layout)),
            ..*saop
        })),
        Expr::Cast(inner, ty) => {
            Expr::Cast(Box::new(rewrite_expr_against_layout(*inner, layout)), ty)
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            expr: Box::new(rewrite_expr_against_layout(*expr, layout)),
            pattern: Box::new(rewrite_expr_against_layout(*pattern, layout)),
            escape: escape.map(|expr| Box::new(rewrite_expr_against_layout(*expr, layout))),
            case_insensitive,
            negated,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(rewrite_expr_against_layout(*expr, layout)),
            pattern: Box::new(rewrite_expr_against_layout(*pattern, layout)),
            escape: escape.map(|expr| Box::new(rewrite_expr_against_layout(*expr, layout))),
            negated,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(rewrite_expr_against_layout(*inner, layout))),
        Expr::IsNotNull(inner) => {
            Expr::IsNotNull(Box::new(rewrite_expr_against_layout(*inner, layout)))
        }
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(rewrite_expr_against_layout(*left, layout)),
            Box::new(rewrite_expr_against_layout(*right, layout)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(rewrite_expr_against_layout(*left, layout)),
            Box::new(rewrite_expr_against_layout(*right, layout)),
        ),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|element| rewrite_expr_against_layout(element, layout))
                .collect(),
            array_type,
        },
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(rewrite_expr_against_layout(*left, layout)),
            Box::new(rewrite_expr_against_layout(*right, layout)),
        ),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(rewrite_expr_against_layout(*array, layout)),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript
                        .lower
                        .map(|expr| rewrite_expr_against_layout(expr, layout)),
                    upper: subscript
                        .upper
                        .map(|expr| rewrite_expr_against_layout(expr, layout)),
                })
                .collect(),
        },
        other => other,
    }
}

pub(super) fn lower_expr_to_plan_layout(expr: Expr, layout: &[Expr]) -> Expr {
    if let Some(index) = layout.iter().position(|candidate| *candidate == expr) {
        return Expr::Column(index);
    }
    if let Expr::Var(var) = &expr
        && var.varlevelsup == 0
        && !is_system_attr(var.varattno)
        && let Some(index) = layout.iter().position(|candidate| match candidate {
            Expr::Var(candidate_var) => {
                candidate_var.varlevelsup == 0
                    && candidate_var.varattno == var.varattno
                    && rte_slot_varno(candidate_var.varno) == Some(var.varno)
            }
            _ => false,
        })
    {
        return Expr::Column(index);
    }
    match expr {
        Expr::Var(var) if var.varlevelsup > 0 && !is_system_attr(var.varattno) => {
            Expr::OuterColumn {
                depth: var.varlevelsup - 1,
                index: attrno_index(var.varattno)
                    .expect("outer Vars lowered to plan layout must reference user columns"),
            }
        }
        Expr::Var(var) if is_system_attr(var.varattno) => Expr::Var(var),
        Expr::Var(var) => panic!(
            "semantic Var {var:?} was not rewritten against path layout {layout:?} before create_plan"
        ),
        Expr::Aggref(_) => {
            panic!("Aggref should be lowered to aggregate output vars before create_plan")
        }
        Expr::Op(op) => Expr::Op(Box::new(OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| lower_expr_to_plan_layout(arg, layout))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| lower_expr_to_plan_layout(arg, layout))
                .collect(),
            ..*bool_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| lower_expr_to_plan_layout(arg, layout))
                .collect(),
            ..*func
        })),
        Expr::SubLink(sublink) => {
            Expr::SubLink(Box::new(crate::include::nodes::primnodes::SubLink {
                testexpr: sublink
                    .testexpr
                    .map(|expr| Box::new(lower_expr_to_plan_layout(*expr, layout))),
                ..*sublink
            }))
        }
        Expr::SubPlan(subplan) => {
            Expr::SubPlan(Box::new(crate::include::nodes::primnodes::SubPlan {
                testexpr: subplan
                    .testexpr
                    .map(|expr| Box::new(lower_expr_to_plan_layout(*expr, layout))),
                ..*subplan
            }))
        }
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            left: Box::new(lower_expr_to_plan_layout(*saop.left, layout)),
            right: Box::new(lower_expr_to_plan_layout(*saop.right, layout)),
            ..*saop
        })),
        Expr::Cast(inner, ty) => {
            Expr::Cast(Box::new(lower_expr_to_plan_layout(*inner, layout)), ty)
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            expr: Box::new(lower_expr_to_plan_layout(*expr, layout)),
            pattern: Box::new(lower_expr_to_plan_layout(*pattern, layout)),
            escape: escape.map(|expr| Box::new(lower_expr_to_plan_layout(*expr, layout))),
            case_insensitive,
            negated,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(lower_expr_to_plan_layout(*expr, layout)),
            pattern: Box::new(lower_expr_to_plan_layout(*pattern, layout)),
            escape: escape.map(|expr| Box::new(lower_expr_to_plan_layout(*expr, layout))),
            negated,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(lower_expr_to_plan_layout(*inner, layout))),
        Expr::IsNotNull(inner) => {
            Expr::IsNotNull(Box::new(lower_expr_to_plan_layout(*inner, layout)))
        }
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(lower_expr_to_plan_layout(*left, layout)),
            Box::new(lower_expr_to_plan_layout(*right, layout)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(lower_expr_to_plan_layout(*left, layout)),
            Box::new(lower_expr_to_plan_layout(*right, layout)),
        ),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|element| lower_expr_to_plan_layout(element, layout))
                .collect(),
            array_type,
        },
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(lower_expr_to_plan_layout(*left, layout)),
            Box::new(lower_expr_to_plan_layout(*right, layout)),
        ),
        Expr::Case(case_expr) => Expr::Case(Box::new(crate::include::nodes::primnodes::CaseExpr {
            arg: case_expr
                .arg
                .map(|arg| Box::new(lower_expr_to_plan_layout(*arg, layout))),
            args: case_expr
                .args
                .into_iter()
                .map(|arm| crate::include::nodes::primnodes::CaseWhen {
                    expr: lower_expr_to_plan_layout(arm.expr, layout),
                    result: lower_expr_to_plan_layout(arm.result, layout),
                })
                .collect(),
            defresult: Box::new(lower_expr_to_plan_layout(*case_expr.defresult, layout)),
            ..*case_expr
        })),
        Expr::CaseTest(case_test) => Expr::CaseTest(case_test),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(lower_expr_to_plan_layout(*array, layout)),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript
                        .lower
                        .map(|expr| lower_expr_to_plan_layout(expr, layout)),
                    upper: subscript
                        .upper
                        .map(|expr| lower_expr_to_plan_layout(expr, layout)),
                })
                .collect(),
        },
        other => other,
    }
}

pub(super) fn expr_sql_type(expr: &Expr) -> SqlType {
    match expr {
        Expr::Var(var) => var.vartype,
        Expr::Param(param) => param.paramtype,
        Expr::Aggref(aggref) => aggref.aggtype,
        Expr::Op(op) => op.opresulttype,
        Expr::Func(func) => func
            .funcresulttype
            .unwrap_or(SqlType::new(SqlTypeKind::Text)),
        Expr::Bool(_)
        | Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::IsDistinctFrom(_, _)
        | Expr::IsNotDistinctFrom(_, _)
        | Expr::Like { .. }
        | Expr::Similar { .. }
        | Expr::ScalarArrayOp(_) => SqlType::new(SqlTypeKind::Bool),
        Expr::Cast(_, ty) => *ty,
        Expr::ArrayLiteral { array_type, .. } => *array_type,
        Expr::Coalesce(left, right) => expr_sql_type_maybe(left)
            .or_else(|| expr_sql_type_maybe(right))
            .unwrap_or(SqlType::new(SqlTypeKind::Text)),
        Expr::Case(case_expr) => case_expr.casetype,
        Expr::CaseTest(case_test) => case_test.type_id,
        Expr::SubLink(sublink) => match sublink.sublink_type {
            SubLinkType::ExistsSubLink
            | SubLinkType::AnySubLink(_)
            | SubLinkType::AllSubLink(_) => SqlType::new(SqlTypeKind::Bool),
            SubLinkType::ExprSubLink => sublink
                .subselect
                .target_list
                .first()
                .map(|target| target.sql_type)
                .unwrap_or(SqlType::new(SqlTypeKind::Text)),
        },
        Expr::SubPlan(subplan) => match subplan.sublink_type {
            SubLinkType::ExistsSubLink
            | SubLinkType::AnySubLink(_)
            | SubLinkType::AllSubLink(_) => SqlType::new(SqlTypeKind::Bool),
            SubLinkType::ExprSubLink => subplan
                .first_col_type
                .unwrap_or(SqlType::new(SqlTypeKind::Text)),
        },
        Expr::Const(value) => value_sql_type_hint(value),
        Expr::Random => SqlType::new(SqlTypeKind::Float8),
        Expr::CurrentDate => SqlType::new(SqlTypeKind::Date),
        Expr::CurrentTime { .. } => SqlType::new(SqlTypeKind::TimeTz),
        Expr::CurrentTimestamp { .. } => SqlType::new(SqlTypeKind::TimestampTz),
        Expr::LocalTime { .. } => SqlType::new(SqlTypeKind::Time),
        Expr::LocalTimestamp { .. } => SqlType::new(SqlTypeKind::Timestamp),
        Expr::Column(_) | Expr::OuterColumn { .. } | Expr::ArraySubscript { .. } => {
            SqlType::new(SqlTypeKind::Text)
        }
    }
}

fn expr_sql_type_maybe(expr: &Expr) -> Option<SqlType> {
    match expr {
        Expr::Column(_) | Expr::OuterColumn { .. } | Expr::ArraySubscript { .. } => None,
        Expr::Param(param) => Some(param.paramtype),
        other => Some(expr_sql_type(other)),
    }
}

fn value_sql_type_hint(value: &Value) -> SqlType {
    match value {
        Value::Int16(_) => SqlType::new(SqlTypeKind::Int2),
        Value::Int32(_) => SqlType::new(SqlTypeKind::Int4),
        Value::Int64(_) => SqlType::new(SqlTypeKind::Int8),
        Value::Money(_) => SqlType::new(SqlTypeKind::Money),
        Value::Date(_) => SqlType::new(SqlTypeKind::Date),
        Value::Time(_) => SqlType::new(SqlTypeKind::Time),
        Value::TimeTz(_) => SqlType::new(SqlTypeKind::TimeTz),
        Value::Timestamp(_) => SqlType::new(SqlTypeKind::Timestamp),
        Value::TimestampTz(_) => SqlType::new(SqlTypeKind::TimestampTz),
        Value::Bit(_) => SqlType::new(SqlTypeKind::Bit),
        Value::Bytea(_) => SqlType::new(SqlTypeKind::Bytea),
        Value::Point(_) => SqlType::new(SqlTypeKind::Point),
        Value::Lseg(_) => SqlType::new(SqlTypeKind::Lseg),
        Value::Path(_) => SqlType::new(SqlTypeKind::Path),
        Value::Line(_) => SqlType::new(SqlTypeKind::Line),
        Value::Box(_) => SqlType::new(SqlTypeKind::Box),
        Value::Polygon(_) => SqlType::new(SqlTypeKind::Polygon),
        Value::Circle(_) => SqlType::new(SqlTypeKind::Circle),
        Value::Float64(_) => SqlType::new(SqlTypeKind::Float8),
        Value::Numeric(_) => SqlType::new(SqlTypeKind::Numeric),
        Value::Json(_) => SqlType::new(SqlTypeKind::Json),
        Value::Jsonb(_) => SqlType::new(SqlTypeKind::Jsonb),
        Value::JsonPath(_) => SqlType::new(SqlTypeKind::JsonPath),
        Value::TsVector(_) => SqlType::new(SqlTypeKind::TsVector),
        Value::TsQuery(_) => SqlType::new(SqlTypeKind::TsQuery),
        Value::Text(_) | Value::TextRef(_, _) => SqlType::new(SqlTypeKind::Text),
        Value::InternalChar(_) => SqlType::new(SqlTypeKind::InternalChar),
        Value::Bool(_) => SqlType::new(SqlTypeKind::Bool),
        Value::Array(_) | Value::PgArray(_) | Value::Null => SqlType::new(SqlTypeKind::Text),
    }
}
