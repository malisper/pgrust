use super::pathnodes::{
    aggregate_output_vars, expr_sql_type, lower_agg_output_expr, rewrite_semantic_expr_for_input_path,
    rte_slot_varno,
};
use super::plan::append_planned_subquery;
use super::{
    aggregate_group_by, expand_join_rte_vars, flatten_join_alias_vars, planner_with_param_base,
    rewrite_semantic_expr_for_join_inputs, rewrite_semantic_expr_for_path,
    rewrite_semantic_expr_for_path_or_expand_join_vars,
};
use crate::backend::parser::CatalogLookup;
use crate::include::nodes::parsenodes::RangeTblEntryKind;
use crate::include::nodes::pathnodes::{Path, PlannerInfo, RestrictInfo};
use crate::include::nodes::plannodes::{ExecParamSource, Plan, PlanEstimate};
use crate::include::nodes::primnodes::{
    Aggref, BoolExpr, Expr, ExprArraySubscript, FuncExpr, OpExpr, OrderByEntry, Param, ParamKind,
    ScalarArrayOpExpr, SubPlan, TargetEntry, Var, attrno_index, is_special_varno, is_system_attr,
    user_attrno, INNER_VAR, OUTER_VAR,
};

#[derive(Clone, Debug)]
struct IndexedTlistEntry {
    index: usize,
    sql_type: crate::backend::parser::SqlType,
    match_exprs: Vec<Expr>,
}

#[derive(Clone, Debug, Default)]
struct IndexedTlist {
    entries: Vec<IndexedTlistEntry>,
}

impl IndexedTlist {
    fn search_expr(&self, root: Option<&PlannerInfo>, expr: &Expr) -> Option<&IndexedTlistEntry> {
        match expr {
            Expr::Var(var) => self.entries.iter().find(|entry| {
                entry
                    .match_exprs
                    .iter()
                    .any(|candidate| match candidate {
                        Expr::Var(candidate_var) => {
                            candidate_var == var
                                || root.is_some_and(|root| {
                                    flatten_join_alias_vars(root, Expr::Var(candidate_var.clone()))
                                        == flatten_join_alias_vars(root, expr.clone())
                                })
                        }
                        _ => false,
                    })
            }),
            _ => self.entries.iter().find(|entry| {
                entry
                    .match_exprs
                    .iter()
                    .any(|candidate| exprs_equivalent(root, candidate, expr))
            }),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum LowerMode<'a> {
    Scalar,
    Input {
        tlist: &'a IndexedTlist,
    },
    Aggregate {
        group_by: &'a [Expr],
        layout: &'a [Expr],
        tlist: &'a IndexedTlist,
    },
    Join {
        outer_tlist: &'a IndexedTlist,
        inner_tlist: &'a IndexedTlist,
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

fn special_slot_var(
    varno: usize,
    index: usize,
    sql_type: crate::backend::parser::SqlType,
) -> Expr {
    Expr::Var(Var {
        varno,
        varattno: user_attrno(index),
        varlevelsup: 0,
        vartype: sql_type,
    })
}

fn build_simple_tlist(output_vars: &[Expr]) -> IndexedTlist {
    IndexedTlist {
        entries: output_vars
            .iter()
            .enumerate()
            .map(|(index, expr)| IndexedTlistEntry {
                index,
                sql_type: expr_sql_type(expr),
                match_exprs: vec![expr.clone()],
            })
            .collect(),
    }
}

fn aggregate_output_expr(accum: &crate::include::nodes::primnodes::AggAccum, aggno: usize) -> Expr {
    Expr::Aggref(Box::new(Aggref {
        aggfnoid: accum.aggfnoid,
        aggtype: accum.sql_type,
        aggvariadic: accum.agg_variadic,
        aggdistinct: accum.distinct,
        args: accum.args.clone(),
        agglevelsup: 0,
        aggno,
    }))
}

fn dedup_match_exprs(exprs: Vec<Expr>) -> Vec<Expr> {
    let mut deduped = Vec::new();
    for expr in exprs {
        if !deduped.contains(&expr) {
            deduped.push(expr);
        }
    }
    deduped
}

fn build_projection_tlist(
    _root: Option<&PlannerInfo>,
    slot_id: usize,
    _input: &Path,
    targets: &[TargetEntry],
) -> IndexedTlist {
    IndexedTlist {
        entries: targets
            .iter()
            .enumerate()
            .map(|(index, target)| IndexedTlistEntry {
                index,
                sql_type: target.sql_type,
                match_exprs: dedup_match_exprs(vec![
                    slot_var(slot_id, user_attrno(index), target.sql_type),
                    target.expr.clone(),
                ]),
            })
            .collect(),
    }
}

fn build_aggregate_tlist(
    root: Option<&PlannerInfo>,
    slot_id: usize,
    group_by: &[Expr],
    accumulators: &[crate::include::nodes::primnodes::AggAccum],
) -> IndexedTlist {
    let mut entries = Vec::with_capacity(group_by.len() + accumulators.len());
    for (index, expr) in group_by.iter().enumerate() {
        let mut match_exprs = vec![
            slot_var(slot_id, user_attrno(index), expr_sql_type(expr)),
            expr.clone(),
        ];
        if let Some(root) = root {
            match_exprs.push(flatten_join_alias_vars(root, expr.clone()));
        }
        entries.push(IndexedTlistEntry {
            index,
            sql_type: expr_sql_type(expr),
            match_exprs: dedup_match_exprs(match_exprs),
        });
    }
    for (aggno, accum) in accumulators.iter().enumerate() {
        let index = group_by.len() + aggno;
        entries.push(IndexedTlistEntry {
            index,
            sql_type: accum.sql_type,
            match_exprs: dedup_match_exprs(vec![
                slot_var(slot_id, user_attrno(index), accum.sql_type),
                aggregate_output_expr(accum, aggno),
            ]),
        });
    }
    IndexedTlist { entries }
}

fn build_join_tlist(root: Option<&PlannerInfo>, left: &Path, right: &Path) -> IndexedTlist {
    let left_tlist = build_path_tlist(root, left);
    let right_tlist = build_path_tlist(root, right);
    let left_len = left_tlist.entries.len();
    let mut entries = left_tlist.entries;
    entries.extend(right_tlist.entries.into_iter().map(|entry| IndexedTlistEntry {
        index: left_len + entry.index,
        ..entry
    }));
    IndexedTlist { entries }
}

fn build_path_tlist(root: Option<&PlannerInfo>, path: &Path) -> IndexedTlist {
    match path {
        Path::Projection {
            slot_id,
            input,
            targets,
            ..
        } => build_projection_tlist(root, *slot_id, input, targets),
        Path::Filter { input, .. } | Path::OrderBy { input, .. } | Path::Limit { input, .. } => {
            build_path_tlist(root, input)
        }
        Path::Aggregate {
            slot_id,
            group_by,
            accumulators,
            ..
        } => build_aggregate_tlist(root, *slot_id, group_by, accumulators),
        Path::NestedLoopJoin { left, right, .. } | Path::HashJoin { left, right, .. } => {
            build_join_tlist(root, left, right)
        }
        _ => build_simple_tlist(&path.output_vars()),
    }
}

fn search_tlist_entry<'a>(
    root: Option<&PlannerInfo>,
    expr: &Expr,
    tlist: &'a IndexedTlist,
) -> Option<&'a IndexedTlistEntry> {
    tlist.search_expr(root, expr).or_else(|| {
        let Expr::Var(var) = expr else {
            return None;
        };
        if var.varlevelsup > 0 || is_system_attr(var.varattno) {
            return None;
        }
        tlist.entries.iter().find(|entry| {
            entry.match_exprs.iter().any(|candidate| match candidate {
                Expr::Var(candidate_var) => {
                    candidate_var.varlevelsup == 0
                        && candidate_var.varattno == var.varattno
                        && rte_slot_varno(candidate_var.varno) == Some(var.varno)
                }
                _ => false,
            })
        })
    })
}

fn lower_direct_ref(expr: &Expr, mode: LowerMode<'_>) -> Option<Expr> {
    match mode {
        LowerMode::Scalar => None,
        LowerMode::Input { tlist } | LowerMode::Aggregate { tlist, .. } => match expr {
            Expr::Column(index) => tlist
                .entries
                .get(*index)
                .map(|entry| special_slot_var(OUTER_VAR, entry.index, entry.sql_type)),
            _ => search_tlist_entry(None, expr, tlist)
                .map(|entry| special_slot_var(OUTER_VAR, entry.index, entry.sql_type)),
        },
        LowerMode::Join {
            outer_tlist,
            inner_tlist,
        } => match expr {
            Expr::Column(index) if *index < outer_tlist.entries.len() => outer_tlist
                .entries
                .get(*index)
                .map(|entry| special_slot_var(OUTER_VAR, entry.index, entry.sql_type)),
            Expr::Column(index) => inner_tlist
                .entries
                .get(index.saturating_sub(outer_tlist.entries.len()))
                .map(|entry| special_slot_var(INNER_VAR, entry.index, entry.sql_type)),
            _ => search_tlist_entry(None, expr, outer_tlist)
                .map(|entry| special_slot_var(OUTER_VAR, entry.index, entry.sql_type))
                .or_else(|| {
                    search_tlist_entry(None, expr, inner_tlist)
                        .map(|entry| special_slot_var(INNER_VAR, entry.index, entry.sql_type))
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
            start: fix_upper_expr_for_path(root, start, path),
            stop: fix_upper_expr_for_path(root, stop, path),
            step: fix_upper_expr_for_path(root, step, path),
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
                .map(|arg| fix_upper_expr_for_path(root, arg, path))
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
                .map(|arg| fix_upper_expr_for_path(root, arg, path))
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
                .map(|arg| fix_upper_expr_for_path(root, arg, path))
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
                .map(|arg| fix_upper_expr_for_path(root, arg, path))
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
                .map(|arg| fix_upper_expr_for_path(root, arg, path))
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
    input_tlist: &IndexedTlist,
) -> crate::include::nodes::primnodes::AggAccum {
    crate::include::nodes::primnodes::AggAccum {
        args: accum
            .args
            .into_iter()
            .map(|arg| {
                lower_expr(
                    ctx,
                    rewrite_semantic_expr_for_input_path(arg, path, layout),
                    LowerMode::Input { tlist: input_tlist },
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
                LowerMode::Input { tlist } => fix_upper_expr(ctx.root, param.expr.clone(), tlist),
                LowerMode::Aggregate {
                    group_by, layout, ..
                } => match ctx.root {
                    Some(root) => lower_agg_output_expr(
                        expand_join_rte_vars(root, param.expr.clone()),
                        group_by,
                        layout,
                    ),
                    None => lower_agg_output_expr(param.expr.clone(), group_by, layout),
                },
                LowerMode::Join {
                    outer_tlist,
                    inner_tlist,
                } => fix_join_expr(ctx.root, param.expr.clone(), outer_tlist, inner_tlist),
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
        Expr::Var(var) if is_special_varno(var.varno) => Expr::Var(var),
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
            let input_tlist = build_path_tlist(ctx.root, &input);
            let predicate = fix_upper_expr_for_path(ctx.root, predicate, &input);
            let predicate = lower_expr(ctx, predicate, LowerMode::Input { tlist: &input_tlist });
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
            let left_tlist = build_path_tlist(ctx.root, &left);
            let (join_restrict_clauses, other_restrict_clauses) =
                split_join_restrict_clauses(kind, &restrict_clauses);
            let join_qual = lower_join_clause_list(ctx, join_restrict_clauses, &left, &right);
            let qual = lower_join_clause_list(ctx, other_restrict_clauses, &left, &right);
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
                        expr: lower_expr(ctx, fix_upper_expr_for_path(ctx.root, param.expr, &left), LowerMode::Input { tlist: &left_tlist }),
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
            let left_tlist = build_path_tlist(ctx.root, &left);
            let right_tlist = build_path_tlist(ctx.root, &right);
            let hash_restrict_clauses = hash_clauses.clone();

            let outer_hash_keys = outer_hash_keys
                .into_iter()
                .map(|expr| fix_upper_expr_for_path(ctx.root, expr, &left))
                .collect::<Vec<_>>();
            let inner_hash_keys = inner_hash_keys
                .into_iter()
                .map(|expr| fix_upper_expr_for_path(ctx.root, expr, &right))
                .collect::<Vec<_>>();
            let lowered_hash_clauses = hash_clauses
                .into_iter()
                .map(|restrict| {
                    let expr = fix_join_expr_for_paths(ctx.root, restrict.clause, &left, &right);
                    lower_expr(ctx, expr, LowerMode::Join { outer_tlist: &left_tlist, inner_tlist: &right_tlist })
                })
                .collect::<Vec<_>>();
            let outer_hash_keys = outer_hash_keys
                .into_iter()
                .map(|expr| lower_expr(ctx, expr, LowerMode::Input { tlist: &left_tlist }))
                .collect::<Vec<_>>();
            let inner_hash_keys = inner_hash_keys
                .into_iter()
                .map(|expr| lower_expr(ctx, expr, LowerMode::Input { tlist: &right_tlist }))
                .collect::<Vec<_>>();
            let (join_restrict_clauses, other_restrict_clauses) =
                split_join_restrict_clauses(kind, &restrict_clauses);
            let join_restrict_clauses =
                remove_hash_clauses(join_restrict_clauses, &hash_restrict_clauses);
            let join_qual = lower_join_clause_list(ctx, &join_restrict_clauses, &left, &right);
            let qual = lower_join_clause_list(ctx, other_restrict_clauses, &left, &right);

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
            let input_tlist = build_path_tlist(ctx.root, &input);
            let root = ctx.root;
            let mut lowered_targets = Vec::with_capacity(targets.len());
            for target in targets {
                let expr = fix_upper_expr_for_path(root, target.expr, &input);
                lowered_targets.push(lower_target_entry(
                    ctx,
                    TargetEntry { expr, ..target },
                    LowerMode::Input { tlist: &input_tlist },
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
            let input_tlist = build_path_tlist(ctx.root, &input);
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
                        expr: fix_upper_expr_for_path(ctx.root, item.expr, &input),
                        ..item
                    })
                    .collect::<Vec<_>>(),
            };
            let lowered_items = items
                .into_iter()
                .map(|item| {
                    lower_order_by_entry(ctx, item, LowerMode::Input { tlist: &input_tlist })
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
            let input_tlist = build_path_tlist(ctx.root, &input);
            let aggregate_layout = aggregate_output_vars(slot_id, &group_by, &accumulators);
            let aggregate_tlist = build_aggregate_tlist(ctx.root, slot_id, &group_by, &accumulators);
            let semantic_group_by = group_by.clone();
            let group_by = group_by
                .into_iter()
                .map(|expr| rewrite_semantic_expr_for_input_path(expr, &input, &layout))
                .map(|expr| lower_expr(ctx, expr, LowerMode::Input { tlist: &input_tlist }))
                .collect();
            let accumulators = accumulators
                .into_iter()
                .map(|accum| lower_agg_accum(ctx, accum, &input, &layout, &input_tlist))
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
                        tlist: &aggregate_tlist,
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
            let input_tlist = build_path_tlist(ctx.root, &input);
            let lowered_targets = targets
                .into_iter()
                .map(|target| {
                    let target = match target {
                        crate::include::nodes::primnodes::ProjectSetTarget::Scalar(entry) => {
                            crate::include::nodes::primnodes::ProjectSetTarget::Scalar(TargetEntry {
                                expr: fix_upper_expr_for_path(ctx.root, entry.expr, &input),
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
                            call: fix_set_returning_call_upper_exprs(ctx.root, call, &input),
                            sql_type,
                            column_index,
                        },
                    };
                    lower_project_set_target(ctx, target, LowerMode::Input { tlist: &input_tlist })
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
) -> Vec<Expr> {
    let root = ctx.root;
    let outer_tlist = build_path_tlist(root, left);
    let inner_tlist = build_path_tlist(root, right);
    let mut lowered = Vec::with_capacity(restrict_clauses.len());
    for restrict in restrict_clauses {
        let expr = fix_join_expr_for_paths(root, restrict.clause.clone(), left, right);
        lowered.push(lower_expr(
            ctx,
            expr,
            LowerMode::Join {
                outer_tlist: &outer_tlist,
                inner_tlist: &inner_tlist,
            },
        ));
    }
    lowered
}

fn fix_upper_expr(root: Option<&PlannerInfo>, expr: Expr, tlist: &IndexedTlist) -> Expr {
    if let Some(entry) = search_tlist_entry(root, &expr, tlist) {
        return special_slot_var(OUTER_VAR, entry.index, entry.sql_type);
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
            return fix_upper_expr(Some(root), flattened, tlist);
        }
    }
    rebuild_setrefs_expr(root, expr, |inner| fix_upper_expr(root, inner, tlist))
}

fn fix_upper_expr_for_path(root: Option<&PlannerInfo>, expr: Expr, path: &Path) -> Expr {
    let layout = path.output_vars();
    let tlist = build_path_tlist(root, path);
    if let Some(entry) = search_tlist_entry(root, &expr, &tlist) {
        return special_slot_var(OUTER_VAR, entry.index, entry.sql_type);
    }
    let rewritten = match root {
        Some(root) => rewrite_semantic_expr_for_path_or_expand_join_vars(root, expr.clone(), path, &layout),
        None => rewrite_semantic_expr_for_path(expr.clone(), path, &layout),
    };
    if rewritten != expr {
        if let Some(entry) = search_tlist_entry(root, &rewritten, &tlist) {
            return special_slot_var(OUTER_VAR, entry.index, entry.sql_type);
        }
        return fix_upper_expr_for_path(root, rewritten, path);
    }
    rebuild_setrefs_expr(root, expr, |inner| fix_upper_expr_for_path(root, inner, path))
}

fn fix_join_expr_for_paths(
    root: Option<&PlannerInfo>,
    expr: Expr,
    left: &Path,
    right: &Path,
) -> Expr {
    let mut join_layout = left.output_vars();
    join_layout.extend(right.output_vars());
    rewrite_semantic_expr_for_join_inputs(root, expr, left, right, &join_layout)
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

fn rebuild_setrefs_expr(
    root: Option<&PlannerInfo>,
    expr: Expr,
    recurse: impl Copy + Fn(Expr) -> Expr,
) -> Expr {
    match expr {
        Expr::Var(_) | Expr::Column(_) | Expr::OuterColumn { .. } | Expr::Param(_) => expr,
        Expr::Aggref(aggref) => Expr::Aggref(Box::new(Aggref {
            args: aggref.args.into_iter().map(recurse).collect(),
            ..*aggref
        })),
        Expr::Op(op) => Expr::Op(Box::new(OpExpr {
            args: op.args.into_iter().map(recurse).collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(BoolExpr {
            args: bool_expr.args.into_iter().map(recurse).collect(),
            ..*bool_expr
        })),
        Expr::Case(case_expr) => Expr::Case(Box::new(crate::include::nodes::primnodes::CaseExpr {
            arg: case_expr.arg.map(|arg| Box::new(recurse(*arg))),
            args: case_expr
                .args
                .into_iter()
                .map(|arm| crate::include::nodes::primnodes::CaseWhen {
                    expr: recurse(arm.expr),
                    result: recurse(arm.result),
                })
                .collect(),
            defresult: Box::new(recurse(*case_expr.defresult)),
            ..*case_expr
        })),
        Expr::CaseTest(case_test) => Expr::CaseTest(case_test),
        Expr::Func(func) => Expr::Func(Box::new(FuncExpr {
            args: func.args.into_iter().map(recurse).collect(),
            ..*func
        })),
        Expr::SubLink(sublink) => Expr::SubLink(Box::new(crate::include::nodes::primnodes::SubLink {
            testexpr: sublink.testexpr.map(|expr| Box::new(recurse(*expr))),
            ..*sublink
        })),
        Expr::SubPlan(subplan) => Expr::SubPlan(Box::new(SubPlan {
            testexpr: subplan.testexpr.map(|expr| Box::new(recurse(*expr))),
            args: subplan.args.into_iter().map(recurse).collect(),
            ..*subplan
        })),
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            left: Box::new(recurse(*saop.left)),
            right: Box::new(recurse(*saop.right)),
            ..*saop
        })),
        Expr::Cast(inner, ty) => Expr::Cast(Box::new(recurse(*inner)), ty),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            expr: Box::new(recurse(*expr)),
            pattern: Box::new(recurse(*pattern)),
            escape: escape.map(|expr| Box::new(recurse(*expr))),
            case_insensitive,
            negated,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(recurse(*expr)),
            pattern: Box::new(recurse(*pattern)),
            escape: escape.map(|expr| Box::new(recurse(*expr))),
            negated,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(recurse(*inner))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(recurse(*inner))),
        Expr::IsDistinctFrom(left, right) => {
            Expr::IsDistinctFrom(Box::new(recurse(*left)), Box::new(recurse(*right)))
        }
        Expr::IsNotDistinctFrom(left, right) => {
            Expr::IsNotDistinctFrom(Box::new(recurse(*left)), Box::new(recurse(*right)))
        }
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements.into_iter().map(recurse).collect(),
            array_type,
        },
        Expr::Coalesce(left, right) => {
            Expr::Coalesce(Box::new(recurse(*left)), Box::new(recurse(*right)))
        }
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(recurse(*array)),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript.lower.map(recurse),
                    upper: subscript.upper.map(recurse),
                })
                .collect(),
        },
        other => {
            let _ = root;
            other
        }
    }
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

fn fix_join_expr(
    root: Option<&PlannerInfo>,
    expr: Expr,
    outer_tlist: &IndexedTlist,
    inner_tlist: &IndexedTlist,
) -> Expr {
    if let Some(entry) = search_tlist_entry(root, &expr, outer_tlist) {
        return special_slot_var(OUTER_VAR, entry.index, entry.sql_type);
    }
    if let Some(entry) = search_tlist_entry(root, &expr, inner_tlist) {
        return special_slot_var(INNER_VAR, entry.index, entry.sql_type);
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
            return fix_join_expr(Some(root), flattened, outer_tlist, inner_tlist);
        }
    }
    rebuild_setrefs_expr(root, expr, |inner| fix_join_expr(root, inner, outer_tlist, inner_tlist))
}
