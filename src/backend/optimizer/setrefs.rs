use super::pathnodes::{
    aggregate_output_vars, expr_sql_type, lower_agg_output_expr, rewrite_expr_against_layout,
    rewrite_semantic_expr_for_input_path, rte_slot_varno,
};
use super::plan::append_planned_subquery;
use super::{
    aggregate_group_by, expand_join_rte_vars, flatten_join_alias_vars, planner_with_param_base,
    rewrite_semantic_expr_for_path, rewrite_semantic_expr_for_path_or_expand_join_vars,
};
use crate::backend::parser::CatalogLookup;
use crate::include::nodes::parsenodes::RangeTblEntryKind;
use crate::include::nodes::pathnodes::{Path, PlannerInfo, RestrictInfo};
use crate::include::nodes::plannodes::{ExecParamSource, Plan, PlanEstimate};
use crate::include::nodes::primnodes::{
    Aggref, BoolExpr, Expr, ExprArraySubscript, FuncExpr, OpExpr, OrderByEntry, Param, ParamKind,
    ScalarArrayOpExpr, SubPlan, TargetEntry, Var, attrno_index, is_system_attr, user_attrno,
    INNER_VAR, OUTER_VAR,
};

#[derive(Clone, Copy, Debug)]
enum LowerMode<'a> {
    Scalar,
    Input {
        path: &'a Path,
        layout: &'a [Expr],
    },
    Aggregate {
        group_by: &'a [Expr],
        layout: &'a [Expr],
    },
    Join {
        left_path: &'a Path,
        left: &'a [Expr],
        right_path: &'a Path,
        right: &'a [Expr],
    },
}

struct SetRefsContext<'a> {
    root: Option<&'a PlannerInfo>,
    catalog: Option<&'a dyn CatalogLookup>,
    subplans: &'a mut Vec<Plan>,
    next_param_id: usize,
    ext_params: Vec<ExecParamSource>,
}

pub(super) fn create_plan(
    root: &PlannerInfo,
    path: Path,
    catalog: &dyn CatalogLookup,
    subplans: &mut Vec<Plan>,
) -> (Plan, Vec<ExecParamSource>) {
    let (plan, ext_params, _) = create_plan_with_param_base(root, path, catalog, subplans, 0);
    (plan, ext_params)
}

pub(super) fn create_plan_with_param_base(
    root: &PlannerInfo,
    path: Path,
    catalog: &dyn CatalogLookup,
    subplans: &mut Vec<Plan>,
    next_param_id: usize,
) -> (Plan, Vec<ExecParamSource>, usize) {
    let mut ctx = SetRefsContext {
        root: Some(root),
        catalog: Some(catalog),
        subplans,
        next_param_id,
        ext_params: Vec::new(),
    };
    let plan = set_plan_refs(&mut ctx, path);
    (plan, ctx.ext_params, ctx.next_param_id)
}

pub(super) fn create_plan_without_root(path: Path) -> Plan {
    let mut subplans = Vec::new();
    let mut ctx = SetRefsContext {
        root: None,
        catalog: None,
        subplans: &mut subplans,
        next_param_id: 0,
        ext_params: Vec::new(),
    };
    let plan = set_plan_refs(&mut ctx, path);
    assert!(ctx.ext_params.is_empty());
    assert!(ctx.subplans.is_empty());
    plan
}

fn layout_index_for_expr(expr: &Expr, layout: &[Expr]) -> Option<usize> {
    if let Some(index) = layout.iter().position(|candidate| candidate == expr) {
        return Some(index);
    }
    let Expr::Var(var) = expr else {
        return None;
    };
    if var.varlevelsup > 0 || is_system_attr(var.varattno) {
        return None;
    }
    layout.iter().position(|candidate| match candidate {
        Expr::Var(candidate_var) => {
            candidate_var.varlevelsup == 0
                && candidate_var.varattno == var.varattno
                && rte_slot_varno(candidate_var.varno) == Some(var.varno)
        }
        _ => false,
    })
}

fn special_slot_var(varno: usize, index: usize, source_expr: &Expr) -> Expr {
    Expr::Var(Var {
        varno,
        varattno: user_attrno(index),
        varlevelsup: 0,
        vartype: expr_sql_type(source_expr),
    })
}

fn lower_direct_ref(expr: &Expr, mode: LowerMode<'_>) -> Option<Expr> {
    match mode {
        LowerMode::Scalar => None,
        LowerMode::Input { layout, .. } => match expr {
            Expr::Column(index) => layout
                .get(*index)
                .map(|source| special_slot_var(OUTER_VAR, *index, source)),
            _ => layout_index_for_expr(expr, layout)
                .map(|index| special_slot_var(OUTER_VAR, index, &layout[index])),
        },
        LowerMode::Aggregate { layout, .. } => match expr {
            Expr::Column(index) => layout
                .get(*index)
                .map(|source| special_slot_var(OUTER_VAR, *index, source)),
            _ => layout_index_for_expr(expr, layout)
                .map(|index| special_slot_var(OUTER_VAR, index, &layout[index])),
        },
        LowerMode::Join { left, right, .. } => match expr {
            Expr::Column(index) if *index < left.len() => left
                .get(*index)
                .map(|source| special_slot_var(OUTER_VAR, *index, source)),
            Expr::Column(index) => {
                let right_index = index.saturating_sub(left.len());
                right
                    .get(right_index)
                    .map(|source| special_slot_var(INNER_VAR, right_index, source))
            }
            _ => layout_index_for_expr(expr, left)
                .map(|index| special_slot_var(OUTER_VAR, index, &left[index]))
                .or_else(|| {
                    layout_index_for_expr(expr, right)
                        .map(|index| special_slot_var(INNER_VAR, index, &right[index]))
                }),
        },
    }
}

fn exec_param_for_outer_var(ctx: &mut SetRefsContext<'_>, var: Var) -> Expr {
    let parent_expr = Expr::Var(Var {
        varlevelsup: var.varlevelsup - 1,
        ..var
    });
    if let Some(existing) = ctx.ext_params.iter().find(|param| param.expr == parent_expr) {
        return Expr::Param(Param {
            paramkind: ParamKind::Exec,
            paramid: existing.paramid,
            paramtype: var.vartype,
        });
    }
    let paramid = ctx.next_param_id;
    ctx.next_param_id += 1;
    ctx.ext_params.push(ExecParamSource {
        paramid,
        expr: parent_expr,
    });
    Expr::Param(Param {
        paramkind: ParamKind::Exec,
        paramid,
        paramtype: var.vartype,
    })
}

fn lower_target_entry(
    ctx: &mut SetRefsContext<'_>,
    target: TargetEntry,
    mode: LowerMode<'_>,
) -> TargetEntry {
    TargetEntry {
        expr: lower_expr(ctx, target.expr, mode),
        ..target
    }
}

fn lower_order_by_entry(
    ctx: &mut SetRefsContext<'_>,
    item: OrderByEntry,
    mode: LowerMode<'_>,
) -> OrderByEntry {
    OrderByEntry {
        expr: lower_expr(ctx, item.expr, mode),
        ..item
    }
}

fn lower_set_returning_call(
    ctx: &mut SetRefsContext<'_>,
    call: crate::include::nodes::primnodes::SetReturningCall,
    mode: LowerMode<'_>,
) -> crate::include::nodes::primnodes::SetReturningCall {
    use crate::include::nodes::primnodes::SetReturningCall;

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
            start: lower_expr(ctx, start, mode),
            stop: lower_expr(ctx, stop, mode),
            step: lower_expr(ctx, step, mode),
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
                .map(|arg| lower_expr(ctx, arg, mode))
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
                .map(|arg| lower_expr(ctx, arg, mode))
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
                .map(|arg| lower_expr(ctx, arg, mode))
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
                .map(|arg| lower_expr(ctx, arg, mode))
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
                .map(|arg| lower_expr(ctx, arg, mode))
                .collect(),
            output_columns,
        },
    }
}

fn fix_set_returning_call_upper_exprs(
    root: Option<&PlannerInfo>,
    call: crate::include::nodes::primnodes::SetReturningCall,
    path: &Path,
    layout: &[Expr],
) -> crate::include::nodes::primnodes::SetReturningCall {
    use crate::include::nodes::primnodes::SetReturningCall;

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
            start: fix_upper_expr(root, start, path, layout),
            stop: fix_upper_expr(root, stop, path, layout),
            step: fix_upper_expr(root, step, path, layout),
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
                .map(|arg| fix_upper_expr(root, arg, path, layout))
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
                .map(|arg| fix_upper_expr(root, arg, path, layout))
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
                .map(|arg| fix_upper_expr(root, arg, path, layout))
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
                .map(|arg| fix_upper_expr(root, arg, path, layout))
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
                .map(|arg| fix_upper_expr(root, arg, path, layout))
                .collect(),
            output_columns,
        },
    }
}

fn lower_project_set_target(
    ctx: &mut SetRefsContext<'_>,
    target: crate::include::nodes::primnodes::ProjectSetTarget,
    mode: LowerMode<'_>,
) -> crate::include::nodes::primnodes::ProjectSetTarget {
    use crate::include::nodes::primnodes::ProjectSetTarget;

    match target {
        ProjectSetTarget::Scalar(entry) => ProjectSetTarget::Scalar(lower_target_entry(ctx, entry, mode)),
        ProjectSetTarget::Set {
            name,
            call,
            sql_type,
            column_index,
        } => ProjectSetTarget::Set {
            name,
            call: lower_set_returning_call(ctx, call, mode),
            sql_type,
            column_index,
        },
    }
}

fn lower_agg_accum(
    ctx: &mut SetRefsContext<'_>,
    accum: crate::include::nodes::primnodes::AggAccum,
    path: &Path,
    layout: &[Expr],
) -> crate::include::nodes::primnodes::AggAccum {
    crate::include::nodes::primnodes::AggAccum {
        args: accum
            .args
            .into_iter()
            .map(|arg| {
                lower_expr(
                    ctx,
                    rewrite_semantic_expr_for_input_path(arg, path, layout),
                    LowerMode::Input { path, layout },
                )
            })
            .collect(),
        ..accum
    }
}

fn lower_sublink(
    ctx: &mut SetRefsContext<'_>,
    sublink: crate::include::nodes::primnodes::SubLink,
    mode: LowerMode<'_>,
) -> Expr {
    let catalog = ctx
        .catalog
        .expect("SubLink lowering requires a catalog-backed planner context");
    let first_col_type = sublink
        .subselect
        .target_list
        .first()
        .map(|target| target.sql_type);
    let (planned_stmt, next_param_id) =
        planner_with_param_base(*sublink.subselect, catalog, ctx.next_param_id);
    ctx.next_param_id = next_param_id;
    let par_param = planned_stmt
        .ext_params
        .iter()
        .map(|param| param.paramid)
        .collect::<Vec<_>>();
    let args = planned_stmt
        .ext_params
        .iter()
        .map(|param| {
            let expr = match mode {
                LowerMode::Scalar => param.expr.clone(),
                LowerMode::Input { path, layout } => {
                    fix_upper_expr(ctx.root, param.expr.clone(), path, layout)
                }
                LowerMode::Aggregate { group_by, layout } => match ctx.root {
                    Some(root) => lower_agg_output_expr(
                        expand_join_rte_vars(root, param.expr.clone()),
                        group_by,
                        layout,
                    ),
                    None => lower_agg_output_expr(param.expr.clone(), group_by, layout),
                },
                LowerMode::Join {
                    left_path,
                    left,
                    right_path,
                    right,
                } => fix_join_expr(ctx.root, param.expr.clone(), left_path, left, right_path, right),
            };
            lower_expr(ctx, expr, mode)
        })
        .collect::<Vec<_>>();
    let plan_id = append_planned_subquery(planned_stmt, ctx.subplans);
    Expr::SubPlan(Box::new(SubPlan {
        sublink_type: sublink.sublink_type,
        testexpr: sublink
            .testexpr
            .map(|expr| Box::new(lower_expr(ctx, *expr, mode))),
        first_col_type,
        plan_id,
        par_param,
        args,
    }))
}

fn lower_expr(ctx: &mut SetRefsContext<'_>, expr: Expr, mode: LowerMode<'_>) -> Expr {
    if let Some(lowered) = lower_direct_ref(&expr, mode) {
        return lowered;
    }
    match expr {
        Expr::Var(var) if var.varlevelsup > 0 => exec_param_for_outer_var(ctx, var),
        Expr::Var(var) => {
            if is_system_attr(var.varattno) {
                Expr::Var(var)
            } else {
                panic!("unresolved semantic Var {var:?} survived setrefs in mode {mode:?}")
            }
        }
        Expr::Param(param) => Expr::Param(param),
        Expr::Column(index) => panic!("unresolved Column({index}) survived setrefs"),
        Expr::OuterColumn { depth, index } => {
            panic!("unresolved OuterColumn(depth={depth}, index={index}) survived setrefs")
        }
        Expr::Aggref(_) => {
            panic!("Aggref should be lowered before executable plan creation")
        }
        Expr::Op(op) => Expr::Op(Box::new(OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| lower_expr(ctx, arg, mode))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| lower_expr(ctx, arg, mode))
                .collect(),
            ..*bool_expr
        })),
        Expr::Case(case_expr) => Expr::Case(Box::new(crate::include::nodes::primnodes::CaseExpr {
            arg: case_expr
                .arg
                .map(|arg| Box::new(lower_expr(ctx, *arg, mode))),
            args: case_expr
                .args
                .into_iter()
                .map(|arm| crate::include::nodes::primnodes::CaseWhen {
                    expr: lower_expr(ctx, arm.expr, mode),
                    result: lower_expr(ctx, arm.result, mode),
                })
                .collect(),
            defresult: Box::new(lower_expr(ctx, *case_expr.defresult, mode)),
            ..*case_expr
        })),
        Expr::CaseTest(case_test) => Expr::CaseTest(case_test),
        Expr::Func(func) => Expr::Func(Box::new(FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| lower_expr(ctx, arg, mode))
                .collect(),
            ..*func
        })),
        Expr::SubLink(sublink) => lower_sublink(ctx, *sublink, mode),
        Expr::SubPlan(subplan) => Expr::SubPlan(Box::new(SubPlan {
            sublink_type: subplan.sublink_type,
            testexpr: subplan
                .testexpr
                .map(|expr| Box::new(lower_expr(ctx, *expr, mode))),
            first_col_type: subplan.first_col_type,
            plan_id: subplan.plan_id,
            par_param: subplan.par_param,
            args: subplan
                .args
                .into_iter()
                .map(|expr| lower_expr(ctx, expr, mode))
                .collect(),
        })),
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            left: Box::new(lower_expr(ctx, *saop.left, mode)),
            right: Box::new(lower_expr(ctx, *saop.right, mode)),
            ..*saop
        })),
        Expr::Cast(inner, ty) => Expr::Cast(Box::new(lower_expr(ctx, *inner, mode)), ty),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            expr: Box::new(lower_expr(ctx, *expr, mode)),
            pattern: Box::new(lower_expr(ctx, *pattern, mode)),
            escape: escape.map(|expr| Box::new(lower_expr(ctx, *expr, mode))),
            case_insensitive,
            negated,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(lower_expr(ctx, *expr, mode)),
            pattern: Box::new(lower_expr(ctx, *pattern, mode)),
            escape: escape.map(|expr| Box::new(lower_expr(ctx, *expr, mode))),
            negated,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(lower_expr(ctx, *inner, mode))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(lower_expr(ctx, *inner, mode))),
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(lower_expr(ctx, *left, mode)),
            Box::new(lower_expr(ctx, *right, mode)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(lower_expr(ctx, *left, mode)),
            Box::new(lower_expr(ctx, *right, mode)),
        ),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|expr| lower_expr(ctx, expr, mode))
                .collect(),
            array_type,
        },
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(lower_expr(ctx, *left, mode)),
            Box::new(lower_expr(ctx, *right, mode)),
        ),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(lower_expr(ctx, *array, mode)),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript.lower.map(|expr| lower_expr(ctx, expr, mode)),
                    upper: subscript.upper.map(|expr| lower_expr(ctx, expr, mode)),
                })
                .collect(),
        },
        other => other,
    }
}

fn set_plan_refs(ctx: &mut SetRefsContext<'_>, path: Path) -> Plan {
    match path {
        Path::Result { plan_info } => Plan::Result { plan_info },
        Path::Append {
            plan_info,
            source_id,
            desc,
            children,
        } => Plan::Append {
            plan_info,
            source_id,
            desc,
            children: children
                .into_iter()
                .map(|child| set_plan_refs(ctx, child))
                .collect(),
        },
        Path::SeqScan {
            plan_info,
            source_id,
            rel,
            relation_name,
            relation_oid,
            toast,
            desc,
        } => Plan::SeqScan {
            plan_info,
            source_id,
            rel,
            relation_name,
            relation_oid,
            toast,
            desc,
        },
        Path::IndexScan {
            plan_info,
            source_id,
            rel,
            relation_oid,
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
            source_id,
            rel,
            relation_oid,
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
            let predicate = fix_upper_expr(ctx.root, predicate, &input, &layout);
            let predicate = lower_expr(
                ctx,
                predicate,
                LowerMode::Input {
                    path: &input,
                    layout: &layout,
                },
            );
            let input_plan = Box::new(set_plan_refs(ctx, *input));
            Plan::Filter {
                plan_info,
                input: input_plan,
                predicate,
            }
        }
        Path::NestedLoopJoin {
            plan_info,
            left,
            right,
            kind,
            restrict_clauses,
        } => {
            let left_layout = left.output_vars();
            let right_layout = right.output_vars();
            let (join_restrict_clauses, other_restrict_clauses) =
                split_join_restrict_clauses(kind, &restrict_clauses);
            let join_qual = lower_join_clause_list(
                ctx,
                join_restrict_clauses,
                &left,
                &right,
                &left_layout,
                &right_layout,
            );
            let qual = lower_join_clause_list(
                ctx,
                other_restrict_clauses,
                &left,
                &right,
                &left_layout,
                &right_layout,
            );
            let (right_plan, nest_params) = {
                let mut right_ctx = SetRefsContext {
                    root: ctx.root,
                    catalog: ctx.catalog,
                    subplans: ctx.subplans,
                    next_param_id: ctx.next_param_id,
                    ext_params: Vec::new(),
                };
                let plan = set_plan_refs(&mut right_ctx, *right);
                ctx.next_param_id = right_ctx.next_param_id;
                let params = right_ctx
                    .ext_params
                    .into_iter()
                    .map(|param| ExecParamSource {
                        paramid: param.paramid,
                        expr: lower_expr(
                            ctx,
                            fix_upper_expr(ctx.root, param.expr, &left, &left_layout),
                            LowerMode::Input {
                                path: &left,
                                layout: &left_layout,
                            },
                        ),
                    })
                    .collect::<Vec<_>>();
                (plan, params)
            };
            let left_plan = set_plan_refs(ctx, *left);
            Plan::NestedLoopJoin {
                plan_info,
                left: Box::new(left_plan),
                right: Box::new(right_plan),
                kind,
                nest_params,
                join_qual,
                qual,
            }
        }
        Path::HashJoin {
            plan_info,
            left,
            right,
            kind,
            hash_clauses,
            outer_hash_keys,
            inner_hash_keys,
            restrict_clauses,
        } => {
            let left_layout = left.output_vars();
            let right_layout = right.output_vars();
            let mut join_layout = left_layout.clone();
            join_layout.extend(right_layout.clone());
            let hash_restrict_clauses = hash_clauses.clone();

            let outer_hash_keys = outer_hash_keys
                .into_iter()
                .map(|expr| fix_upper_expr(ctx.root, expr, &left, &left_layout))
                .collect::<Vec<_>>();
            let inner_hash_keys = inner_hash_keys
                .into_iter()
                .map(|expr| fix_upper_expr(ctx.root, expr, &right, &right_layout))
                .collect::<Vec<_>>();
            let lowered_hash_clauses = hash_clauses
                .into_iter()
                .map(|restrict| {
                    let expr = fix_join_expr(
                        ctx.root,
                        restrict.clause,
                        &left,
                        &left_layout,
                        &right,
                        &right_layout,
                    );
                    lower_expr(
                        ctx,
                        expr,
                        LowerMode::Join {
                            left_path: &left,
                            left: &left_layout,
                            right_path: &right,
                            right: &right_layout,
                        },
                    )
                })
                .collect::<Vec<_>>();
            let outer_hash_keys = outer_hash_keys
                .into_iter()
                .map(|expr| {
                    lower_expr(
                        ctx,
                        expr,
                        LowerMode::Input {
                            path: &left,
                            layout: &left_layout,
                        },
                    )
                })
                .collect::<Vec<_>>();
            let inner_hash_keys = inner_hash_keys
                .into_iter()
                .map(|expr| {
                    lower_expr(
                        ctx,
                        expr,
                        LowerMode::Input {
                            path: &right,
                            layout: &right_layout,
                        },
                    )
                })
                .collect::<Vec<_>>();
            let (join_restrict_clauses, other_restrict_clauses) =
                split_join_restrict_clauses(kind, &restrict_clauses);
            let join_restrict_clauses =
                remove_hash_clauses(join_restrict_clauses, &hash_restrict_clauses);
            let join_qual = lower_join_clause_list(
                ctx,
                &join_restrict_clauses,
                &left,
                &right,
                &left_layout,
                &right_layout,
            );
            let qual = lower_join_clause_list(
                ctx,
                other_restrict_clauses,
                &left,
                &right,
                &left_layout,
                &right_layout,
            );

            let left_plan = set_plan_refs(ctx, *left);
            let right_plan = set_plan_refs(ctx, *right);
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
                hash_clauses: lowered_hash_clauses,
                hash_keys: outer_hash_keys,
                join_qual,
                qual,
            }
        }
        Path::Projection {
            plan_info,
            input,
            targets,
            ..
        } => {
            let layout = input.output_vars();
            let root = ctx.root;
            let mut lowered_targets = Vec::with_capacity(targets.len());
            for target in targets {
                let expr = fix_upper_expr(root, target.expr, &input, &layout);
                lowered_targets.push(lower_target_entry(
                    ctx,
                    TargetEntry { expr, ..target },
                    LowerMode::Input {
                        path: &input,
                        layout: &layout,
                    },
                ));
            }
            Plan::Projection {
                plan_info,
                input: Box::new(set_plan_refs(ctx, *input)),
                targets: lowered_targets,
            }
        }
        Path::OrderBy {
            plan_info,
            input,
            items,
        } => {
            let layout = input.output_vars();
            let items = match (ctx.root, aggregate_group_by(&input)) {
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
                        expr: fix_upper_expr(ctx.root, item.expr, &input, &layout),
                        ..item
                    })
                    .collect::<Vec<_>>(),
            };
            let lowered_items = items
                .into_iter()
                .map(|item| {
                    lower_order_by_entry(
                        ctx,
                        item,
                        LowerMode::Input {
                            path: &input,
                            layout: &layout,
                        },
                    )
                })
                .collect();
            let input_plan = Box::new(set_plan_refs(ctx, *input));
            Plan::OrderBy {
                plan_info,
                input: input_plan,
                items: lowered_items,
            }
        }
        Path::Limit {
            plan_info,
            input,
            limit,
            offset,
        } => Plan::Limit {
            plan_info,
            input: Box::new(set_plan_refs(ctx, *input)),
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
            let semantic_group_by = group_by.clone();
            let group_by = group_by
                .into_iter()
                .map(|expr| rewrite_semantic_expr_for_input_path(expr, &input, &layout))
                .map(|expr| {
                    lower_expr(
                        ctx,
                        expr,
                        LowerMode::Input {
                            path: &input,
                            layout: &layout,
                        },
                    )
                })
                .collect();
            let accumulators = accumulators
                .into_iter()
                .map(|accum| lower_agg_accum(ctx, accum, &input, &layout))
                .collect();
            let having = having.map(|expr| {
                let expr = match ctx.root {
                    Some(root) => lower_agg_output_expr(
                        expand_join_rte_vars(root, expr),
                        &semantic_group_by,
                        &aggregate_layout,
                    ),
                    None => lower_agg_output_expr(expr, &semantic_group_by, &aggregate_layout),
                };
                lower_expr(
                    ctx,
                    expr,
                    LowerMode::Aggregate {
                        group_by: &semantic_group_by,
                        layout: &aggregate_layout,
                    },
                )
            });
            Plan::Aggregate {
                plan_info,
                input: Box::new(set_plan_refs(ctx, *input)),
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
                        .map(|expr| lower_expr(ctx, expr, LowerMode::Scalar))
                        .collect()
                })
                .collect(),
            output_columns,
        },
        Path::FunctionScan {
            plan_info, call, ..
        } => Plan::FunctionScan {
            plan_info,
            call: lower_set_returning_call(ctx, call, LowerMode::Scalar),
        },
        Path::CteScan {
            plan_info,
            cte_id,
            cte_plan,
            output_columns,
            ..
        } => Plan::CteScan {
            plan_info,
            cte_id,
            cte_plan: Box::new(set_plan_refs(ctx, *cte_plan)),
            output_columns,
        },
        Path::WorkTableScan {
            plan_info,
            worktable_id,
            output_columns,
            ..
        } => Plan::WorkTableScan {
            plan_info,
            worktable_id,
            output_columns,
        },
        Path::RecursiveUnion {
            plan_info,
            worktable_id,
            distinct,
            output_columns,
            anchor,
            recursive,
            ..
        } => Plan::RecursiveUnion {
            plan_info,
            worktable_id,
            distinct,
            output_columns,
            anchor: Box::new(set_plan_refs(ctx, *anchor)),
            recursive: Box::new(set_plan_refs(ctx, *recursive)),
        },
        Path::ProjectSet {
            plan_info,
            input,
            targets,
            ..
        } => {
            let layout = input.output_vars();
            let lowered_targets = targets
                .into_iter()
                .map(|target| {
                    let target = match target {
                        crate::include::nodes::primnodes::ProjectSetTarget::Scalar(entry) => {
                            crate::include::nodes::primnodes::ProjectSetTarget::Scalar(TargetEntry {
                                expr: fix_upper_expr(ctx.root, entry.expr, &input, &layout),
                                ..entry
                            })
                        }
                        crate::include::nodes::primnodes::ProjectSetTarget::Set {
                            name,
                            call,
                            sql_type,
                            column_index,
                        } => crate::include::nodes::primnodes::ProjectSetTarget::Set {
                            name,
                            call: fix_set_returning_call_upper_exprs(
                                ctx.root,
                                call,
                                &input,
                                &layout,
                            ),
                            sql_type,
                            column_index,
                        },
                    };
                    lower_project_set_target(ctx, target, LowerMode::Input { path: &input, layout: &layout })
                })
                .collect();
            let input_plan = Box::new(set_plan_refs(ctx, *input));
            Plan::ProjectSet {
                plan_info,
                input: input_plan,
                targets: lowered_targets,
            }
        }
    }
}

fn split_join_restrict_clauses<'a>(
    kind: crate::include::nodes::primnodes::JoinType,
    restrict_clauses: &'a [RestrictInfo],
) -> (&'a [RestrictInfo], &'a [RestrictInfo]) {
    if matches!(
        kind,
        crate::include::nodes::primnodes::JoinType::Inner
            | crate::include::nodes::primnodes::JoinType::Cross
    ) {
        return (restrict_clauses, &[]);
    }
    let split_at = restrict_clauses
        .iter()
        .position(|restrict| restrict.is_pushed_down)
        .unwrap_or(restrict_clauses.len());
    restrict_clauses.split_at(split_at)
}

fn remove_hash_clauses<'a>(
    restrict_clauses: &'a [RestrictInfo],
    hash_clauses: &[RestrictInfo],
) -> Vec<RestrictInfo> {
    restrict_clauses
        .iter()
        .filter(|restrict| {
            !hash_clauses
                .iter()
                .any(|hash_clause| hash_clause.clause == restrict.clause)
        })
        .cloned()
        .collect()
}

fn lower_join_clause_list(
    ctx: &mut SetRefsContext<'_>,
    restrict_clauses: &[RestrictInfo],
    left: &Path,
    right: &Path,
    left_layout: &[Expr],
    right_layout: &[Expr],
) -> Vec<Expr> {
    let root = ctx.root;
    let mut lowered = Vec::with_capacity(restrict_clauses.len());
    for restrict in restrict_clauses {
        let expr = fix_join_expr(
            root,
            restrict.clause.clone(),
            left,
            left_layout,
            right,
            right_layout,
        );
        lowered.push(lower_expr(
            ctx,
            expr,
            LowerMode::Join {
                left_path: left,
                left: left_layout,
                right_path: right,
                right: right_layout,
            },
        ));
    }
    lowered
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
}

fn exprs_equivalent(root: Option<&PlannerInfo>, left: &Expr, right: &Expr) -> bool {
    if left == right {
        return true;
    }
    let Some(root) = root else {
        return false;
    };
    flatten_join_alias_vars(root, left.clone()) == flatten_join_alias_vars(root, right.clone())
}

fn slot_var(
    slot_id: usize,
    attno: crate::include::nodes::primnodes::AttrNumber,
    vartype: crate::backend::parser::SqlType,
) -> Expr {
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
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(fully_expand_output_expr(*inner, path))),
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

fn fully_expand_output_expr_with_root(root: Option<&PlannerInfo>, expr: Expr, path: &Path) -> Expr {
    let expr = match root {
        Some(root) => flatten_join_alias_vars(root, expr),
        None => expr,
    };
    fully_expand_output_expr(expr, path)
}

fn expand_output_var(var: Var, path: &Path) -> Expr {
    match path {
        Path::Projection {
            slot_id,
            input,
            targets,
            ..
        } if var.varno == *slot_id => attrno_index(var.varattno)
            .filter(|index| *index < targets.len())
            .map(|index| fully_expand_output_expr(targets[index].expr.clone(), input))
            .unwrap_or(Expr::Var(var)),
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

fn find_exposed_output_match(root: Option<&PlannerInfo>, expr: &Expr, path: &Path) -> Option<Expr> {
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
                let expanded_semantic =
                    fully_expand_output_expr_with_root(root, target.expr.clone(), input);
                (exprs_equivalent(root, expr, &target.expr)
                    || exprs_equivalent(root, expr, &semantic)
                    || exprs_equivalent(root, expr, &rewritten_semantic)
                    || exprs_equivalent(root, expr, &expanded_semantic))
                .then(|| slot_var(*slot_id, user_attrno(index), target.sql_type))
            })
        }
        Path::Filter { input, .. } | Path::OrderBy { input, .. } | Path::Limit { input, .. } => {
            find_exposed_output_match(root, expr, input)
        }
        _ => None,
    }
}

fn match_join_input_output(expr: &Expr, path: &Path, layout: &[Expr]) -> Option<PathRewrite> {
    if let Some(index) = layout.iter().position(|candidate| candidate == expr) {
        return Some(PathRewrite {
            expr: layout[index].clone(),
        });
    }
    match path {
        Path::Projection {
            slot_id, targets, ..
        } => targets.iter().enumerate().find_map(|(index, target)| {
            (target.expr == *expr).then(|| PathRewrite {
                expr: slot_var(*slot_id, user_attrno(index), target.sql_type),
            })
        }),
        Path::Filter { input, .. } | Path::OrderBy { input, .. } | Path::Limit { input, .. } => {
            match_join_input_output(expr, input, layout)
        }
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
    if let Some(rewritten) = match_join_input_output(&expr, left, left_layout)
        .or_else(|| match_join_input_output(&expr, right, right_layout))
        .map(|rewrite| rewrite.expr)
    {
        return if rewritten == expr {
            rewritten
        } else {
            fix_join_expr(root, rewritten, left, left_layout, right, right_layout)
        };
    }
    if let (Expr::Var(var), Some(root)) = (&expr, root)
        && root
            .parse
            .rtable
            .get(var.varno.saturating_sub(1))
            .is_some_and(|rte| matches!(rte.kind, RangeTblEntryKind::Join { .. }))
    {
        let flattened = flatten_join_alias_vars(root, expr.clone());
        if flattened != expr {
            return fix_join_expr(
                Some(root),
                flattened,
                left,
                left_layout,
                right,
                right_layout,
            );
        }
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

    match match_join_input_output(&rebuilt, left, left_layout)
        .or_else(|| match_join_input_output(&rebuilt, right, right_layout))
        .map(|rewrite| rewrite.expr)
    {
        Some(rewritten) if rewritten != rebuilt => {
            fix_join_expr(root, rewritten, left, left_layout, right, right_layout)
        }
        Some(rewritten) => rewritten,
        None => rebuilt,
    }
}
