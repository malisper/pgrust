use super::inherit::append_translation;
use super::pathnodes::{
    aggregate_output_vars, expr_sql_type, lower_agg_output_expr, rte_slot_id, rte_slot_varno,
};
use super::plan::append_planned_subquery;
use super::{expand_join_rte_vars, flatten_join_alias_vars, planner_with_param_base};
use crate::backend::parser::CatalogLookup;
use crate::include::nodes::parsenodes::{Query, RangeTblEntryKind};
use crate::include::nodes::pathnodes::{Path, PlannerInfo, PlannerSubroot, RestrictInfo};
use crate::include::nodes::plannodes::{ExecParamSource, Plan, PlanEstimate};
use crate::include::nodes::primnodes::{
    AggAccum, Aggref, BoolExpr, Expr, ExprArraySubscript, FuncExpr, INNER_VAR, OUTER_VAR, OpExpr,
    OrderByEntry, Param, ParamKind, QueryColumn, ScalarArrayOpExpr, SubPlan, TargetEntry, Var,
    attrno_index, is_executor_special_varno, is_system_attr, user_attrno,
};

#[derive(Clone, Debug)]
struct IndexedTlistEntry {
    index: usize,
    sql_type: crate::backend::parser::SqlType,
    ressortgroupref: usize,
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
                entry.match_exprs.iter().any(|candidate| match candidate {
                    Expr::Var(candidate_var) => {
                        candidate_var == var
                            || root.is_some_and(|root| {
                                flatten_join_alias_vars(root, Expr::Var(candidate_var.clone()))
                                    == flatten_join_alias_vars(root, expr.clone())
                            })
                    }
                    _ => output_component_matches_expr(candidate, expr),
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
    validate_planner_path(&path);
    let plan = set_plan_refs(&mut ctx, path);
    validate_executable_plan(&plan);
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
    validate_planner_path(&path);
    let plan = set_plan_refs(&mut ctx, path);
    validate_executable_plan(&plan);
    assert!(ctx.ext_params.is_empty());
    assert!(ctx.subplans.is_empty());
    plan
}

fn recurse_with_root(ctx: &mut SetRefsContext<'_>, root: Option<&PlannerInfo>, path: Path) -> Plan {
    let ext_params = std::mem::take(&mut ctx.ext_params);
    let mut nested = SetRefsContext {
        root,
        catalog: ctx.catalog,
        subplans: ctx.subplans,
        next_param_id: ctx.next_param_id,
        ext_params,
    };
    let plan = set_plan_refs(&mut nested, path);
    ctx.next_param_id = nested.next_param_id;
    ctx.ext_params = nested.ext_params;
    plan
}

fn special_slot_var(varno: usize, index: usize, sql_type: crate::backend::parser::SqlType) -> Expr {
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
                ressortgroupref: 0,
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
        aggfilter: accum.filter.clone(),
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
    root: Option<&PlannerInfo>,
    slot_id: usize,
    input: &Path,
    targets: &[TargetEntry],
) -> IndexedTlist {
    let input_target = input.semantic_output_target();
    IndexedTlist {
        entries: targets
            .iter()
            .enumerate()
            .map(|(index, target)| {
                let mut match_exprs = vec![slot_var(slot_id, user_attrno(index), target.sql_type)];
                if let Some(input_resno) = target.input_resno {
                    if let Some(input_expr) = input_target.exprs.get(input_resno.saturating_sub(1))
                    {
                        match_exprs.push(input_expr.clone());
                        match_exprs.push(fully_expand_output_expr_with_root(
                            root,
                            input_expr.clone(),
                            input,
                        ));
                        if let Some(root) = root {
                            match_exprs.push(flatten_join_alias_vars(root, input_expr.clone()));
                            match_exprs.push(flatten_join_alias_vars(
                                root,
                                fully_expand_output_expr_with_root(
                                    Some(root),
                                    input_expr.clone(),
                                    input,
                                ),
                            ));
                        }
                    }
                }
                match_exprs.push(target.expr.clone());
                match_exprs.push(fully_expand_output_expr_with_root(
                    root,
                    target.expr.clone(),
                    input,
                ));
                if let Some(root) = root {
                    match_exprs.push(flatten_join_alias_vars(root, target.expr.clone()));
                    match_exprs.push(flatten_join_alias_vars(
                        root,
                        fully_expand_output_expr_with_root(Some(root), target.expr.clone(), input),
                    ));
                }
                IndexedTlistEntry {
                    index,
                    sql_type: target.sql_type,
                    ressortgroupref: target.ressortgroupref,
                    match_exprs: dedup_match_exprs(match_exprs),
                }
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
            ressortgroupref: 0,
            match_exprs: dedup_match_exprs(match_exprs),
        });
    }
    for (aggno, accum) in accumulators.iter().enumerate() {
        let index = group_by.len() + aggno;
        entries.push(IndexedTlistEntry {
            index,
            sql_type: accum.sql_type,
            ressortgroupref: 0,
            match_exprs: dedup_match_exprs(vec![
                slot_var(slot_id, user_attrno(index), accum.sql_type),
                aggregate_output_expr(accum, aggno),
            ]),
        });
    }
    IndexedTlist { entries }
}

fn build_project_set_tlist(
    root: Option<&PlannerInfo>,
    slot_id: usize,
    input: &Path,
    targets: &[crate::include::nodes::primnodes::ProjectSetTarget],
) -> IndexedTlist {
    let input_target = input.semantic_output_target();
    IndexedTlist {
        entries: targets
            .iter()
            .enumerate()
            .map(|(index, target)| {
                let (sql_type, ressortgroupref, mut match_exprs) = match target {
                    crate::include::nodes::primnodes::ProjectSetTarget::Scalar(entry) => {
                        let mut match_exprs =
                            vec![slot_var(slot_id, user_attrno(index), entry.sql_type)];
                        if let Some(input_resno) = entry.input_resno {
                            if let Some(input_expr) =
                                input_target.exprs.get(input_resno.saturating_sub(1))
                            {
                                match_exprs.push(input_expr.clone());
                                match_exprs.push(fully_expand_output_expr_with_root(
                                    root,
                                    input_expr.clone(),
                                    input,
                                ));
                                if let Some(root) = root {
                                    match_exprs
                                        .push(flatten_join_alias_vars(root, input_expr.clone()));
                                    match_exprs.push(flatten_join_alias_vars(
                                        root,
                                        fully_expand_output_expr_with_root(
                                            Some(root),
                                            input_expr.clone(),
                                            input,
                                        ),
                                    ));
                                }
                            }
                        }
                        match_exprs.push(entry.expr.clone());
                        match_exprs.push(fully_expand_output_expr_with_root(
                            root,
                            entry.expr.clone(),
                            input,
                        ));
                        if let Some(root) = root {
                            match_exprs.push(flatten_join_alias_vars(root, entry.expr.clone()));
                            match_exprs.push(flatten_join_alias_vars(
                                root,
                                fully_expand_output_expr_with_root(
                                    Some(root),
                                    entry.expr.clone(),
                                    input,
                                ),
                            ));
                        }
                        (entry.sql_type, entry.ressortgroupref, match_exprs)
                    }
                    crate::include::nodes::primnodes::ProjectSetTarget::Set {
                        sql_type, ..
                    } => (
                        *sql_type,
                        0,
                        vec![slot_var(slot_id, user_attrno(index), *sql_type)],
                    ),
                };
                IndexedTlistEntry {
                    index,
                    sql_type,
                    ressortgroupref,
                    match_exprs: dedup_match_exprs(std::mem::take(&mut match_exprs)),
                }
            })
            .collect(),
    }
}

fn build_join_tlist(root: Option<&PlannerInfo>, left: &Path, right: &Path) -> IndexedTlist {
    let left_tlist = build_path_tlist(root, left);
    let right_tlist = build_path_tlist(root, right);
    let left_len = left_tlist.entries.len();
    let mut entries = left_tlist.entries;
    entries.extend(
        right_tlist
            .entries
            .into_iter()
            .map(|entry| IndexedTlistEntry {
                index: left_len + entry.index,
                ..entry
            }),
    );
    IndexedTlist { entries }
}

fn build_subquery_tlist(
    rtindex: usize,
    _query: &Query,
    output_columns: &[QueryColumn],
) -> IndexedTlist {
    IndexedTlist {
        entries: output_columns
            .iter()
            .enumerate()
            .map(|(index, column)| IndexedTlistEntry {
                index,
                sql_type: column.sql_type,
                ressortgroupref: 0,
                match_exprs: dedup_match_exprs(vec![
                    Expr::Var(Var {
                        varno: rtindex,
                        varattno: user_attrno(index),
                        varlevelsup: 0,
                        vartype: column.sql_type,
                    }),
                    slot_var(rte_slot_id(rtindex), user_attrno(index), column.sql_type),
                ]),
            })
            .collect(),
    }
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
        Path::ProjectSet {
            slot_id,
            input,
            targets,
            ..
        } => build_project_set_tlist(root, *slot_id, input, targets),
        Path::SubqueryScan {
            rtindex,
            query,
            output_columns,
            ..
        } => build_subquery_tlist(*rtindex, query, output_columns),
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

fn search_input_tlist_entry<'a>(
    root: Option<&PlannerInfo>,
    expr: &Expr,
    input: &Path,
    tlist: &'a IndexedTlist,
) -> Option<&'a IndexedTlistEntry> {
    let flattened_expr = root.map(|root| flatten_join_alias_vars(root, expr.clone()));
    search_tlist_entry(root, expr, tlist).or_else(|| {
        let mut matched_index = None;
        for entry in &tlist.entries {
            let entry_matches = entry.match_exprs.iter().any(|candidate| {
                exprs_equivalent(root, candidate, expr)
                    || output_component_matches_expr(candidate, expr)
                    || flattened_expr.as_ref().is_some_and(|flattened_expr| {
                        exprs_equivalent(root, candidate, flattened_expr)
                            || output_component_matches_expr(candidate, flattened_expr)
                    })
                    || exprs_equivalent(
                        root,
                        &fully_expand_output_expr_with_root(root, candidate.clone(), input),
                        expr,
                    )
                    || flattened_expr.as_ref().is_some_and(|flattened_expr| {
                        exprs_equivalent(
                            root,
                            &fully_expand_output_expr_with_root(root, candidate.clone(), input),
                            flattened_expr,
                        )
                    })
            });
            if !entry_matches {
                continue;
            }
            match matched_index {
                Some(index) if index != entry.index => return None,
                Some(_) => {}
                None => matched_index = Some(entry.index),
            }
        }
        matched_index.and_then(|index| tlist.entries.iter().find(|entry| entry.index == index))
    })
}

fn output_component_matches_expr(candidate: &Expr, expr: &Expr) -> bool {
    if candidate == expr {
        return true;
    }
    match candidate {
        Expr::Coalesce(left, right) => {
            output_component_matches_expr(left, expr) || output_component_matches_expr(right, expr)
        }
        _ => false,
    }
}

fn search_tlist_entry_by_sortgroupref(
    ressortgroupref: usize,
    tlist: &IndexedTlist,
) -> Option<&IndexedTlistEntry> {
    if ressortgroupref == 0 {
        return None;
    }
    tlist
        .entries
        .iter()
        .find(|entry| entry.ressortgroupref == ressortgroupref)
}

fn lower_top_level_input_var(
    root: Option<&PlannerInfo>,
    expr: Expr,
    input: &Path,
    tlist: &IndexedTlist,
) -> Expr {
    match expr {
        Expr::Var(var)
            if var.varlevelsup == 0
                && !is_executor_special_varno(var.varno)
                && !is_system_attr(var.varattno) =>
        {
            search_input_tlist_entry(root, &Expr::Var(var.clone()), input, tlist)
                .filter(|entry| entry.sql_type == var.vartype)
                .map(|entry| special_slot_var(OUTER_VAR, entry.index, entry.sql_type))
                .unwrap_or(Expr::Var(var))
        }
        other => other,
    }
}

fn lower_projection_expr_by_input_target(
    root: Option<&PlannerInfo>,
    expr: Expr,
    input: &Path,
    input_tlist: &IndexedTlist,
) -> Expr {
    if let Some(entry) = search_input_tlist_entry(root, &expr, input, input_tlist)
        && entry.sql_type == expr_sql_type(&expr)
    {
        return special_slot_var(OUTER_VAR, entry.index, entry.sql_type);
    }
    let map_var = |var: Var| {
        if var.varlevelsup != 0
            || is_executor_special_varno(var.varno)
            || is_system_attr(var.varattno)
        {
            return Expr::Var(var);
        }
        let expr = Expr::Var(var.clone());
        search_input_tlist_entry(root, &expr, input, input_tlist)
            .filter(|entry| entry.sql_type == var.vartype)
            .map(|entry| special_slot_var(OUTER_VAR, entry.index, entry.sql_type))
            .unwrap_or(Expr::Var(var))
    };
    match expr {
        Expr::Var(var) => map_var(var),
        Expr::Aggref(aggref) => Expr::Aggref(Box::new(Aggref {
            args: aggref
                .args
                .into_iter()
                .map(|arg| lower_projection_expr_by_input_target(root, arg, input, input_tlist))
                .collect(),
            aggfilter: aggref.aggfilter.map(|expr| {
                lower_projection_expr_by_input_target(root, expr, input, input_tlist)
            }),
            ..*aggref
        })),
        Expr::Op(op) => Expr::Op(Box::new(OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| lower_projection_expr_by_input_target(root, arg, input, input_tlist))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| lower_projection_expr_by_input_target(root, arg, input, input_tlist))
                .collect(),
            ..*bool_expr
        })),
        Expr::Case(case_expr) => Expr::Case(Box::new(crate::include::nodes::primnodes::CaseExpr {
            arg: case_expr.arg.map(|arg| {
                Box::new(lower_projection_expr_by_input_target(
                    root,
                    *arg,
                    input,
                    input_tlist,
                ))
            }),
            args: case_expr
                .args
                .into_iter()
                .map(|arm| crate::include::nodes::primnodes::CaseWhen {
                    expr: lower_projection_expr_by_input_target(root, arm.expr, input, input_tlist),
                    result: lower_projection_expr_by_input_target(
                        root,
                        arm.result,
                        input,
                        input_tlist,
                    ),
                })
                .collect(),
            defresult: Box::new(lower_projection_expr_by_input_target(
                root,
                *case_expr.defresult,
                input,
                input_tlist,
            )),
            ..*case_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| lower_projection_expr_by_input_target(root, arg, input, input_tlist))
                .collect(),
            ..*func
        })),
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            left: Box::new(lower_projection_expr_by_input_target(
                root,
                *saop.left,
                input,
                input_tlist,
            )),
            right: Box::new(lower_projection_expr_by_input_target(
                root,
                *saop.right,
                input,
                input_tlist,
            )),
            ..*saop
        })),
        Expr::Cast(inner, ty) => Expr::Cast(
            Box::new(lower_projection_expr_by_input_target(
                root,
                *inner,
                input,
                input_tlist,
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
            expr: Box::new(lower_projection_expr_by_input_target(
                root,
                *expr,
                input,
                input_tlist,
            )),
            pattern: Box::new(lower_projection_expr_by_input_target(
                root,
                *pattern,
                input,
                input_tlist,
            )),
            escape: escape.map(|expr| {
                Box::new(lower_projection_expr_by_input_target(
                    root,
                    *expr,
                    input,
                    input_tlist,
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
            expr: Box::new(lower_projection_expr_by_input_target(
                root,
                *expr,
                input,
                input_tlist,
            )),
            pattern: Box::new(lower_projection_expr_by_input_target(
                root,
                *pattern,
                input,
                input_tlist,
            )),
            escape: escape.map(|expr| {
                Box::new(lower_projection_expr_by_input_target(
                    root,
                    *expr,
                    input,
                    input_tlist,
                ))
            }),
            negated,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(lower_projection_expr_by_input_target(
            root,
            *inner,
            input,
            input_tlist,
        ))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(lower_projection_expr_by_input_target(
            root,
            *inner,
            input,
            input_tlist,
        ))),
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(lower_projection_expr_by_input_target(
                root,
                *left,
                input,
                input_tlist,
            )),
            Box::new(lower_projection_expr_by_input_target(
                root,
                *right,
                input,
                input_tlist,
            )),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(lower_projection_expr_by_input_target(
                root,
                *left,
                input,
                input_tlist,
            )),
            Box::new(lower_projection_expr_by_input_target(
                root,
                *right,
                input,
                input_tlist,
            )),
        ),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|element| {
                    lower_projection_expr_by_input_target(root, element, input, input_tlist)
                })
                .collect(),
            array_type,
        },
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(lower_projection_expr_by_input_target(
                root,
                *left,
                input,
                input_tlist,
            )),
            Box::new(lower_projection_expr_by_input_target(
                root,
                *right,
                input,
                input_tlist,
            )),
        ),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(lower_projection_expr_by_input_target(
                root,
                *array,
                input,
                input_tlist,
            )),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript.lower.map(|expr| {
                        lower_projection_expr_by_input_target(root, expr, input, input_tlist)
                    }),
                    upper: subscript.upper.map(|expr| {
                        lower_projection_expr_by_input_target(root, expr, input, input_tlist)
                    }),
                })
                .collect(),
        },
        other => other,
    }
}

fn lower_order_by_expr_for_input(
    root: Option<&PlannerInfo>,
    item: OrderByEntry,
    input: &Path,
    input_tlist: &IndexedTlist,
) -> OrderByEntry {
    if let Some(entry) = search_tlist_entry_by_sortgroupref(item.ressortgroupref, input_tlist) {
        return OrderByEntry {
            expr: special_slot_var(OUTER_VAR, entry.index, entry.sql_type),
            ..item
        };
    }
    OrderByEntry {
        expr: lower_top_level_input_var(
            root,
            fix_upper_expr_for_input(root, item.expr, input, input_tlist),
            input,
            input_tlist,
        ),
        ..item
    }
}

fn expr_contains_local_semantic_var(expr: &Expr) -> bool {
    match expr {
        Expr::Var(var) => {
            var.varlevelsup == 0
                && !is_executor_special_varno(var.varno)
                && !is_system_attr(var.varattno)
        }
        Expr::Aggref(aggref) => {
            aggref.args.iter().any(expr_contains_local_semantic_var)
                || aggref
                    .aggfilter
                    .as_ref()
                    .is_some_and(expr_contains_local_semantic_var)
        }
        Expr::Op(op) => op.args.iter().any(expr_contains_local_semantic_var),
        Expr::Bool(bool_expr) => bool_expr.args.iter().any(expr_contains_local_semantic_var),
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_deref()
                .is_some_and(expr_contains_local_semantic_var)
                || case_expr.args.iter().any(|arm| {
                    expr_contains_local_semantic_var(&arm.expr)
                        || expr_contains_local_semantic_var(&arm.result)
                })
                || expr_contains_local_semantic_var(&case_expr.defresult)
        }
        Expr::Func(func) => func.args.iter().any(expr_contains_local_semantic_var),
        Expr::SubLink(sublink) => sublink
            .testexpr
            .as_deref()
            .is_some_and(expr_contains_local_semantic_var),
        Expr::SubPlan(subplan) => {
            subplan
                .testexpr
                .as_deref()
                .is_some_and(expr_contains_local_semantic_var)
                || subplan.args.iter().any(expr_contains_local_semantic_var)
        }
        Expr::ScalarArrayOp(saop) => {
            expr_contains_local_semantic_var(&saop.left)
                || expr_contains_local_semantic_var(&saop.right)
        }
        Expr::Cast(inner, _) => expr_contains_local_semantic_var(inner),
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_contains_local_semantic_var(expr)
                || expr_contains_local_semantic_var(pattern)
                || escape
                    .as_deref()
                    .is_some_and(expr_contains_local_semantic_var)
        }
        Expr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_contains_local_semantic_var(expr)
                || expr_contains_local_semantic_var(pattern)
                || escape
                    .as_deref()
                    .is_some_and(expr_contains_local_semantic_var)
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => expr_contains_local_semantic_var(inner),
        Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
            expr_contains_local_semantic_var(left) || expr_contains_local_semantic_var(right)
        }
        Expr::ArrayLiteral { elements, .. } => {
            elements.iter().any(expr_contains_local_semantic_var)
        }
        Expr::Coalesce(left, right) => {
            expr_contains_local_semantic_var(left) || expr_contains_local_semantic_var(right)
        }
        Expr::ArraySubscript { array, subscripts } => {
            expr_contains_local_semantic_var(array)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_some_and(expr_contains_local_semantic_var)
                        || subscript
                            .upper
                            .as_ref()
                            .is_some_and(expr_contains_local_semantic_var)
                })
        }
        _ => false,
    }
}

fn path_single_relid(path: &Path) -> Option<usize> {
    match path {
        Path::Append { source_id, .. }
        | Path::SeqScan { source_id, .. }
        | Path::IndexScan { source_id, .. } => Some(*source_id),
        Path::Filter { input, .. }
        | Path::Projection { input, .. }
        | Path::OrderBy { input, .. }
        | Path::Limit { input, .. }
        | Path::Aggregate { input, .. }
        | Path::ProjectSet { input, .. } => path_single_relid(input),
        _ => None,
    }
}

fn rewrite_expr_for_append_rel(
    expr: Expr,
    info: &crate::include::nodes::pathnodes::AppendRelInfo,
) -> Expr {
    match expr {
        Expr::Var(var) if var.varlevelsup == 0 && var.varno == info.parent_relid => info
            .translated_vars
            .get(attrno_index(var.varattno).unwrap_or(usize::MAX))
            .cloned()
            .unwrap_or(Expr::Var(var)),
        Expr::Aggref(aggref) => Expr::Aggref(Box::new(Aggref {
            args: aggref
                .args
                .into_iter()
                .map(|arg| rewrite_expr_for_append_rel(arg, info))
                .collect(),
            aggfilter: aggref
                .aggfilter
                .map(|expr| rewrite_expr_for_append_rel(expr, info)),
            ..*aggref
        })),
        Expr::Op(op) => Expr::Op(Box::new(OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| rewrite_expr_for_append_rel(arg, info))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(BoolExpr {
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
        Expr::Func(func) => Expr::Func(Box::new(FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| rewrite_expr_for_append_rel(arg, info))
                .collect(),
            ..*func
        })),
        Expr::SubLink(sublink) => {
            Expr::SubLink(Box::new(crate::include::nodes::primnodes::SubLink {
                testexpr: sublink
                    .testexpr
                    .map(|expr| Box::new(rewrite_expr_for_append_rel(*expr, info))),
                ..*sublink
            }))
        }
        Expr::SubPlan(subplan) => {
            Expr::SubPlan(Box::new(crate::include::nodes::primnodes::SubPlan {
                testexpr: subplan
                    .testexpr
                    .map(|expr| Box::new(rewrite_expr_for_append_rel(*expr, info))),
                ..*subplan
            }))
        }
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            left: Box::new(rewrite_expr_for_append_rel(*saop.left, info)),
            right: Box::new(rewrite_expr_for_append_rel(*saop.right, info)),
            ..*saop
        })),
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
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(rewrite_expr_for_append_rel(*left, info)),
            Box::new(rewrite_expr_for_append_rel(*right, info)),
        ),
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

fn rewrite_appendrel_expr_for_input_path(root: &PlannerInfo, expr: Expr, path: &Path) -> Expr {
    path_single_relid(path)
        .and_then(|relid| append_translation(root, relid))
        .map(|info| rewrite_expr_for_append_rel(expr.clone(), info))
        .unwrap_or(expr)
}

fn fix_upper_expr_for_input(
    root: Option<&PlannerInfo>,
    expr: Expr,
    input: &Path,
    input_tlist: &IndexedTlist,
) -> Expr {
    let rewritten = fix_upper_expr(root, expr.clone(), input_tlist);
    if rewritten != expr {
        return rewritten;
    }
    if let Some(root) = root {
        let translated = rewrite_appendrel_expr_for_input_path(root, expr.clone(), input);
        if translated != expr {
            let translated_rewritten = fix_upper_expr(Some(root), translated.clone(), input_tlist);
            if translated_rewritten != translated {
                return translated_rewritten;
            }
        }
    }
    expr
}

fn lower_direct_ref(expr: &Expr, mode: LowerMode<'_>) -> Option<Expr> {
    match mode {
        LowerMode::Scalar => None,
        LowerMode::Input { tlist } => search_tlist_entry(None, expr, tlist)
            .map(|entry| special_slot_var(OUTER_VAR, entry.index, entry.sql_type)),
        LowerMode::Aggregate { layout, tlist, .. } => search_tlist_entry(None, expr, tlist)
            .map(|entry| special_slot_var(OUTER_VAR, entry.index, entry.sql_type))
            .or_else(|| {
                layout.iter().enumerate().find_map(|(index, candidate)| {
                    (candidate == expr)
                        .then(|| special_slot_var(OUTER_VAR, index, expr_sql_type(candidate)))
                })
            }),
        LowerMode::Join {
            outer_tlist,
            inner_tlist,
        } => search_tlist_entry(None, expr, outer_tlist)
            .map(|entry| special_slot_var(OUTER_VAR, entry.index, entry.sql_type))
            .or_else(|| {
                search_tlist_entry(None, expr, inner_tlist)
                    .map(|entry| special_slot_var(INNER_VAR, entry.index, entry.sql_type))
            }),
    }
}

fn exec_param_for_outer_var(ctx: &mut SetRefsContext<'_>, var: Var) -> Expr {
    let parent_expr = Expr::Var(Var {
        varlevelsup: var.varlevelsup - 1,
        ..var
    });
    if let Some(existing) = ctx
        .ext_params
        .iter()
        .find(|param| param.expr == parent_expr)
    {
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

fn inline_exec_params(expr: Expr, params: &[ExecParamSource], consumed: &mut Vec<usize>) -> Expr {
    match expr {
        Expr::Param(param) if matches!(param.paramkind, ParamKind::Exec) => params
            .iter()
            .find(|candidate| candidate.paramid == param.paramid)
            .map(|candidate| {
                if !consumed.contains(&param.paramid) {
                    consumed.push(param.paramid);
                }
                inline_exec_params(candidate.expr.clone(), params, consumed)
            })
            .unwrap_or(Expr::Param(param)),
        Expr::Op(op) => Expr::Op(Box::new(OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| inline_exec_params(arg, params, consumed))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| inline_exec_params(arg, params, consumed))
                .collect(),
            ..*bool_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| inline_exec_params(arg, params, consumed))
                .collect(),
            ..*func
        })),
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            left: Box::new(inline_exec_params(*saop.left, params, consumed)),
            right: Box::new(inline_exec_params(*saop.right, params, consumed)),
            ..*saop
        })),
        Expr::Cast(inner, ty) => {
            Expr::Cast(Box::new(inline_exec_params(*inner, params, consumed)), ty)
        }
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(inline_exec_params(*left, params, consumed)),
            Box::new(inline_exec_params(*right, params, consumed)),
        ),
        Expr::IsNull(inner) => Expr::IsNull(Box::new(inline_exec_params(*inner, params, consumed))),
        Expr::IsNotNull(inner) => {
            Expr::IsNotNull(Box::new(inline_exec_params(*inner, params, consumed)))
        }
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(inline_exec_params(*left, params, consumed)),
            Box::new(inline_exec_params(*right, params, consumed)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(inline_exec_params(*left, params, consumed)),
            Box::new(inline_exec_params(*right, params, consumed)),
        ),
        other => other,
    }
}

fn decrement_outer_var_levels(expr: Expr) -> Expr {
    match expr {
        Expr::Var(mut var) if var.varlevelsup > 0 => {
            var.varlevelsup -= 1;
            Expr::Var(var)
        }
        Expr::Op(op) => Expr::Op(Box::new(OpExpr {
            args: op
                .args
                .into_iter()
                .map(decrement_outer_var_levels)
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(decrement_outer_var_levels)
                .collect(),
            ..*bool_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(FuncExpr {
            args: func
                .args
                .into_iter()
                .map(decrement_outer_var_levels)
                .collect(),
            ..*func
        })),
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            left: Box::new(decrement_outer_var_levels(*saop.left)),
            right: Box::new(decrement_outer_var_levels(*saop.right)),
            ..*saop
        })),
        Expr::Cast(inner, ty) => Expr::Cast(Box::new(decrement_outer_var_levels(*inner)), ty),
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(decrement_outer_var_levels(*left)),
            Box::new(decrement_outer_var_levels(*right)),
        ),
        Expr::IsNull(inner) => Expr::IsNull(Box::new(decrement_outer_var_levels(*inner))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(decrement_outer_var_levels(*inner))),
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(decrement_outer_var_levels(*left)),
            Box::new(decrement_outer_var_levels(*right)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(decrement_outer_var_levels(*left)),
            Box::new(decrement_outer_var_levels(*right)),
        ),
        other => other,
    }
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
    input_tlist: &IndexedTlist,
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
            start: fix_upper_expr_for_input(root, start, path, input_tlist),
            stop: fix_upper_expr_for_input(root, stop, path, input_tlist),
            step: fix_upper_expr_for_input(root, step, path, input_tlist),
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
                .map(|arg| fix_upper_expr_for_input(root, arg, path, input_tlist))
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
                .map(|arg| fix_upper_expr_for_input(root, arg, path, input_tlist))
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
                .map(|arg| fix_upper_expr_for_input(root, arg, path, input_tlist))
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
                .map(|arg| fix_upper_expr_for_input(root, arg, path, input_tlist))
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
                .map(|arg| fix_upper_expr_for_input(root, arg, path, input_tlist))
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
        ProjectSetTarget::Scalar(entry) => {
            ProjectSetTarget::Scalar(lower_target_entry(ctx, entry, mode))
        }
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
    input_tlist: &IndexedTlist,
) -> crate::include::nodes::primnodes::AggAccum {
    crate::include::nodes::primnodes::AggAccum {
        args: accum
            .args
            .into_iter()
            .map(|arg| {
                let arg = fix_upper_expr_for_input(ctx.root, arg, path, input_tlist);
                lower_expr(ctx, arg, LowerMode::Input { tlist: input_tlist })
            })
            .collect(),
        filter: accum.filter.map(|filter| {
            let filter = fix_upper_expr_for_input(ctx.root, filter, path, input_tlist);
            lower_expr(
                ctx,
                filter,
                LowerMode::Input { tlist: input_tlist },
            )
        }),
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
        Expr::Var(var) if is_executor_special_varno(var.varno) => Expr::Var(var),
        Expr::Var(var) => {
            if is_system_attr(var.varattno) {
                Expr::Var(var)
            } else if let Some(root) = ctx.root {
                let flattened = flatten_join_alias_vars(root, Expr::Var(var.clone()));
                if flattened != Expr::Var(var.clone()) {
                    lower_expr(ctx, flattened, mode)
                } else {
                    panic!(
                        "unresolved semantic Var {var:?} survived setrefs in mode {mode:?}; \
                         executable plans should only contain executor-facing refs or allowed scan/system Vars"
                    )
                }
            } else {
                panic!(
                    "unresolved semantic Var {var:?} survived setrefs in mode {mode:?}; \
                     executable plans should only contain executor-facing refs or allowed scan/system Vars"
                )
            }
        }
        Expr::Param(param) => Expr::Param(param),
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
        Expr::Row { fields } => Expr::Row {
            fields: fields
                .into_iter()
                .map(|(name, expr)| (name, lower_expr(ctx, expr, mode)))
                .collect(),
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

fn validate_executable_expr(expr: &Expr, plan_node: &str, field: &str) {
    match expr {
        Expr::Var(var) if var.varlevelsup > 0 => {
            panic!("executable plan contains outer-level Var in {plan_node}.{field}: {var:?}")
        }
        Expr::Aggref(aggref) => {
            panic!("executable plan contains unresolved Aggref in {plan_node}.{field}: {aggref:?}")
        }
        Expr::SubLink(sublink) => panic!(
            "executable plan contains unresolved SubLink in {plan_node}.{field}: {sublink:?}"
        ),
        Expr::Op(op) => op
            .args
            .iter()
            .for_each(|arg| validate_executable_expr(arg, plan_node, field)),
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .for_each(|arg| validate_executable_expr(arg, plan_node, field)),
        Expr::Case(case_expr) => {
            if let Some(arg) = &case_expr.arg {
                validate_executable_expr(arg, plan_node, field);
            }
            for arm in &case_expr.args {
                validate_executable_expr(&arm.expr, plan_node, field);
                validate_executable_expr(&arm.result, plan_node, field);
            }
            validate_executable_expr(&case_expr.defresult, plan_node, field);
        }
        Expr::Func(func) => func
            .args
            .iter()
            .for_each(|arg| validate_executable_expr(arg, plan_node, field)),
        Expr::SubPlan(subplan) => {
            if let Some(testexpr) = &subplan.testexpr {
                validate_executable_expr(testexpr, plan_node, field);
            }
            subplan
                .args
                .iter()
                .for_each(|arg| validate_executable_expr(arg, plan_node, field));
        }
        Expr::ScalarArrayOp(saop) => {
            validate_executable_expr(&saop.left, plan_node, field);
            validate_executable_expr(&saop.right, plan_node, field);
        }
        Expr::Cast(inner, _) => validate_executable_expr(inner, plan_node, field),
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
            validate_executable_expr(expr, plan_node, field);
            validate_executable_expr(pattern, plan_node, field);
            if let Some(escape) = escape {
                validate_executable_expr(escape, plan_node, field);
            }
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            validate_executable_expr(inner, plan_node, field);
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            validate_executable_expr(left, plan_node, field);
            validate_executable_expr(right, plan_node, field);
        }
        Expr::ArrayLiteral { elements, .. } => elements
            .iter()
            .for_each(|element| validate_executable_expr(element, plan_node, field)),
        Expr::Row { fields } => fields
            .iter()
            .for_each(|(_, expr)| validate_executable_expr(expr, plan_node, field)),
        Expr::ArraySubscript { array, subscripts } => {
            validate_executable_expr(array, plan_node, field);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    validate_executable_expr(lower, plan_node, field);
                }
                if let Some(upper) = &subscript.upper {
                    validate_executable_expr(upper, plan_node, field);
                }
            }
        }
        Expr::Var(_)
        | Expr::Param(_)
        | Expr::Const(_)
        | Expr::CaseTest(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => {}
    }
}

fn validate_set_returning_call(
    call: &crate::include::nodes::primnodes::SetReturningCall,
    plan_node: &str,
    field: &str,
) {
    use crate::include::nodes::primnodes::SetReturningCall;

    match call {
        SetReturningCall::GenerateSeries {
            start, stop, step, ..
        } => {
            validate_executable_expr(start, plan_node, field);
            validate_executable_expr(stop, plan_node, field);
            validate_executable_expr(step, plan_node, field);
        }
        SetReturningCall::Unnest { args, .. }
        | SetReturningCall::JsonTableFunction { args, .. }
        | SetReturningCall::RegexTableFunction { args, .. }
        | SetReturningCall::TextSearchTableFunction { args, .. }
        | SetReturningCall::UserDefined { args, .. } => args
            .iter()
            .for_each(|arg| validate_executable_expr(arg, plan_node, field)),
    }
}

fn validate_agg_accum(
    accum: &crate::include::nodes::primnodes::AggAccum,
    plan_node: &str,
    field: &str,
) {
    accum
        .args
        .iter()
        .for_each(|arg| validate_executable_expr(arg, plan_node, field));
}

fn validate_executable_plan(plan: &Plan) {
    match plan {
        Plan::Result { .. } | Plan::SeqScan { .. } => {}
        Plan::Append { children, .. } | Plan::SetOp { children, .. } => {
            for child in children {
                validate_executable_plan(child);
            }
        }
        Plan::IndexScan { .. } => {}
        Plan::Hash {
            input, hash_keys, ..
        } => {
            hash_keys
                .iter()
                .for_each(|expr| validate_executable_expr(expr, "Hash", "hash_keys"));
            validate_executable_plan(input);
        }
        Plan::NestedLoopJoin {
            left,
            right,
            nest_params,
            join_qual,
            qual,
            ..
        } => {
            for param in nest_params {
                validate_executable_expr(&param.expr, "NestedLoopJoin", "nest_params");
            }
            join_qual
                .iter()
                .for_each(|expr| validate_executable_expr(expr, "NestedLoopJoin", "join_qual"));
            qual.iter()
                .for_each(|expr| validate_executable_expr(expr, "NestedLoopJoin", "qual"));
            validate_executable_plan(left);
            validate_executable_plan(right);
        }
        Plan::HashJoin {
            left,
            right,
            hash_clauses,
            hash_keys,
            join_qual,
            qual,
            ..
        } => {
            hash_clauses
                .iter()
                .for_each(|expr| validate_executable_expr(expr, "HashJoin", "hash_clauses"));
            hash_keys
                .iter()
                .for_each(|expr| validate_executable_expr(expr, "HashJoin", "hash_keys"));
            join_qual
                .iter()
                .for_each(|expr| validate_executable_expr(expr, "HashJoin", "join_qual"));
            qual.iter()
                .for_each(|expr| validate_executable_expr(expr, "HashJoin", "qual"));
            validate_executable_plan(left);
            validate_executable_plan(right);
        }
        Plan::Filter {
            input, predicate, ..
        } => {
            validate_executable_expr(predicate, "Filter", "predicate");
            validate_executable_plan(input);
        }
        Plan::OrderBy { input, items, .. } => {
            items
                .iter()
                .for_each(|item| validate_executable_expr(&item.expr, "OrderBy", "items"));
            validate_executable_plan(input);
        }
        Plan::Limit { input, .. } => validate_executable_plan(input),
        Plan::Projection { input, targets, .. } => {
            targets
                .iter()
                .for_each(|target| validate_executable_expr(&target.expr, "Projection", "targets"));
            validate_executable_plan(input);
        }
        Plan::Aggregate {
            input,
            group_by,
            accumulators,
            having,
            ..
        } => {
            group_by
                .iter()
                .for_each(|expr| validate_executable_expr(expr, "Aggregate", "group_by"));
            accumulators
                .iter()
                .for_each(|accum| validate_agg_accum(accum, "Aggregate", "accumulators"));
            if let Some(having) = having {
                validate_executable_expr(having, "Aggregate", "having");
            }
            validate_executable_plan(input);
        }
        Plan::FunctionScan { call, .. } => {
            validate_set_returning_call(call, "FunctionScan", "call");
        }
        Plan::SubqueryScan { input, .. } => validate_executable_plan(input),
        Plan::CteScan { cte_plan, .. } => validate_executable_plan(cte_plan),
        Plan::WorkTableScan { .. } => {}
        Plan::RecursiveUnion {
            anchor, recursive, ..
        } => {
            validate_executable_plan(anchor);
            validate_executable_plan(recursive);
        }
        Plan::Values { rows, .. } => {
            for row in rows {
                row.iter()
                    .for_each(|expr| validate_executable_expr(expr, "Values", "rows"));
            }
        }
        Plan::ProjectSet { input, targets, .. } => {
            for target in targets {
                match target {
                    crate::include::nodes::primnodes::ProjectSetTarget::Scalar(entry) => {
                        validate_executable_expr(&entry.expr, "ProjectSet", "targets");
                    }
                    crate::include::nodes::primnodes::ProjectSetTarget::Set { call, .. } => {
                        validate_set_returning_call(call, "ProjectSet", "targets");
                    }
                }
            }
            validate_executable_plan(input);
        }
    }
}

fn validate_planner_expr(expr: &Expr, path_node: &str, field: &str) {
    match expr {
        Expr::Var(var) if is_executor_special_varno(var.varno) => {
            panic!("planner path contains executor-only Var in {path_node}.{field}: {var:?}")
        }
        Expr::Param(Param {
            paramkind: ParamKind::Exec,
            ..
        }) => panic!("planner path contains PARAM_EXEC in {path_node}.{field}: {expr:?}"),
        Expr::Op(op) => op
            .args
            .iter()
            .for_each(|arg| validate_planner_expr(arg, path_node, field)),
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .for_each(|arg| validate_planner_expr(arg, path_node, field)),
        Expr::Case(case_expr) => {
            if let Some(arg) = &case_expr.arg {
                validate_planner_expr(arg, path_node, field);
            }
            for arm in &case_expr.args {
                validate_planner_expr(&arm.expr, path_node, field);
                validate_planner_expr(&arm.result, path_node, field);
            }
            validate_planner_expr(&case_expr.defresult, path_node, field);
        }
        Expr::Func(func) => func
            .args
            .iter()
            .for_each(|arg| validate_planner_expr(arg, path_node, field)),
        Expr::SubLink(sublink) => {
            if let Some(testexpr) = &sublink.testexpr {
                validate_planner_expr(testexpr, path_node, field);
            }
        }
        Expr::SubPlan(subplan) => {
            if let Some(testexpr) = &subplan.testexpr {
                validate_planner_expr(testexpr, path_node, field);
            }
            subplan
                .args
                .iter()
                .for_each(|arg| validate_planner_expr(arg, path_node, field));
        }
        Expr::ScalarArrayOp(saop) => {
            validate_planner_expr(&saop.left, path_node, field);
            validate_planner_expr(&saop.right, path_node, field);
        }
        Expr::Cast(inner, _) => validate_planner_expr(inner, path_node, field),
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
            validate_planner_expr(expr, path_node, field);
            validate_planner_expr(pattern, path_node, field);
            if let Some(escape) = escape {
                validate_planner_expr(escape, path_node, field);
            }
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            validate_planner_expr(inner, path_node, field);
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            validate_planner_expr(left, path_node, field);
            validate_planner_expr(right, path_node, field);
        }
        Expr::ArrayLiteral { elements, .. } => elements
            .iter()
            .for_each(|element| validate_planner_expr(element, path_node, field)),
        Expr::Row { fields } => fields
            .iter()
            .for_each(|(_, expr)| validate_planner_expr(expr, path_node, field)),
        Expr::ArraySubscript { array, subscripts } => {
            validate_planner_expr(array, path_node, field);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    validate_planner_expr(lower, path_node, field);
                }
                if let Some(upper) = &subscript.upper {
                    validate_planner_expr(upper, path_node, field);
                }
            }
        }
        Expr::Var(_)
        | Expr::Const(_)
        | Expr::Aggref(_)
        | Expr::CaseTest(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => {}
    }
}

fn validate_planner_set_returning_call(
    call: &crate::include::nodes::primnodes::SetReturningCall,
    path_node: &str,
    field: &str,
) {
    use crate::include::nodes::primnodes::SetReturningCall;

    match call {
        SetReturningCall::GenerateSeries {
            start, stop, step, ..
        } => {
            validate_planner_expr(start, path_node, field);
            validate_planner_expr(stop, path_node, field);
            validate_planner_expr(step, path_node, field);
        }
        SetReturningCall::Unnest { args, .. }
        | SetReturningCall::JsonTableFunction { args, .. }
        | SetReturningCall::RegexTableFunction { args, .. }
        | SetReturningCall::TextSearchTableFunction { args, .. }
        | SetReturningCall::UserDefined { args, .. } => args
            .iter()
            .for_each(|arg| validate_planner_expr(arg, path_node, field)),
    }
}

fn validate_planner_agg_accum(
    accum: &crate::include::nodes::primnodes::AggAccum,
    path_node: &str,
    field: &str,
) {
    accum
        .args
        .iter()
        .for_each(|arg| validate_planner_expr(arg, path_node, field));
}

fn validate_planner_path(path: &Path) {
    match path {
        Path::Result { .. } | Path::SeqScan { .. } | Path::IndexScan { .. } => {}
        Path::Append { children, .. } | Path::SetOp { children, .. } => {
            for child in children {
                validate_planner_path(child);
            }
        }
        Path::Filter {
            input, predicate, ..
        } => {
            validate_planner_expr(predicate, "Filter", "predicate");
            validate_planner_path(input);
        }
        Path::NestedLoopJoin {
            left,
            right,
            restrict_clauses,
            ..
        } => {
            for restrict in restrict_clauses {
                validate_planner_expr(&restrict.clause, "Join", "restrict_clauses");
            }
            validate_planner_path(left);
            validate_planner_path(right);
        }
        Path::HashJoin {
            left,
            right,
            restrict_clauses,
            hash_clauses,
            outer_hash_keys,
            inner_hash_keys,
            ..
        } => {
            for restrict in restrict_clauses {
                validate_planner_expr(&restrict.clause, "HashJoin", "restrict_clauses");
            }
            for restrict in hash_clauses {
                validate_planner_expr(&restrict.clause, "HashJoin", "hash_clauses");
            }
            for expr in outer_hash_keys {
                validate_planner_expr(expr, "HashJoin", "outer_hash_keys");
            }
            for expr in inner_hash_keys {
                validate_planner_expr(expr, "HashJoin", "inner_hash_keys");
            }
            validate_planner_path(left);
            validate_planner_path(right);
        }
        Path::Projection { input, targets, .. } => {
            for target in targets {
                validate_planner_expr(&target.expr, "Projection", "targets");
            }
            validate_planner_path(input);
        }
        Path::OrderBy { input, items, .. } => {
            for item in items {
                validate_planner_expr(&item.expr, "OrderBy", "items");
            }
            validate_planner_path(input);
        }
        Path::Limit { input, .. } => validate_planner_path(input),
        Path::Aggregate {
            input,
            group_by,
            accumulators,
            having,
            ..
        } => {
            for expr in group_by {
                validate_planner_expr(expr, "Aggregate", "group_by");
            }
            for accum in accumulators {
                validate_planner_agg_accum(accum, "Aggregate", "accumulators");
            }
            if let Some(having) = having {
                validate_planner_expr(having, "Aggregate", "having");
            }
            validate_planner_path(input);
        }
        Path::Values { rows, .. } => {
            for row in rows {
                for expr in row {
                    validate_planner_expr(expr, "Values", "rows");
                }
            }
        }
        Path::FunctionScan { call, .. } => {
            validate_planner_set_returning_call(call, "FunctionScan", "call");
        }
        Path::SubqueryScan { input, .. } => validate_planner_path(input),
        Path::CteScan { cte_plan, .. } => validate_planner_path(cte_plan),
        Path::WorkTableScan { .. } => {}
        Path::RecursiveUnion {
            anchor, recursive, ..
        } => {
            validate_planner_path(anchor);
            validate_planner_path(recursive);
        }
        Path::ProjectSet { input, targets, .. } => {
            for target in targets {
                match target {
                    crate::include::nodes::primnodes::ProjectSetTarget::Scalar(entry) => {
                        validate_planner_expr(&entry.expr, "ProjectSet", "targets");
                    }
                    crate::include::nodes::primnodes::ProjectSetTarget::Set { call, .. } => {
                        validate_planner_set_returning_call(call, "ProjectSet", "targets");
                    }
                }
            }
            validate_planner_path(input);
        }
    }
}

#[cfg(test)]
pub(super) fn validate_executable_plan_for_tests(plan: &Plan) {
    validate_executable_plan(plan);
}

#[cfg(test)]
pub(super) fn validate_planner_path_for_tests(path: &Path) {
    validate_planner_path(path);
}

fn set_filter_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    input: Box<Path>,
    predicate: Expr,
) -> Plan {
    let input_tlist = build_path_tlist(ctx.root, &input);
    let predicate = fix_upper_expr_for_input(ctx.root, predicate, &input, &input_tlist);
    let predicate = lower_expr(
        ctx,
        predicate,
        LowerMode::Input {
            tlist: &input_tlist,
        },
    );
    let input_plan = Box::new(set_plan_refs(ctx, *input));
    Plan::Filter {
        plan_info,
        input: input_plan,
        predicate,
    }
}

fn set_append_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    source_id: usize,
    desc: crate::include::nodes::primnodes::RelationDesc,
    children: Vec<Path>,
) -> Plan {
    Plan::Append {
        plan_info,
        source_id,
        desc,
        children: children
            .into_iter()
            .map(|child| set_plan_refs(ctx, child))
            .collect(),
    }
}

fn set_set_op_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    op: crate::include::nodes::parsenodes::SetOperator,
    output_columns: Vec<QueryColumn>,
    children: Vec<Path>,
) -> Plan {
    Plan::SetOp {
        plan_info,
        op,
        output_columns,
        children: children
            .into_iter()
            .map(|child| set_plan_refs(ctx, child))
            .collect(),
    }
}

fn set_seq_scan_references(
    plan_info: PlanEstimate,
    source_id: usize,
    rel: crate::RelFileLocator,
    relation_name: String,
    relation_oid: u32,
    toast: Option<crate::include::nodes::primnodes::ToastRelationRef>,
    desc: crate::include::nodes::primnodes::RelationDesc,
) -> Plan {
    Plan::SeqScan {
        plan_info,
        source_id,
        rel,
        relation_name,
        relation_oid,
        toast,
        desc,
    }
}

#[allow(clippy::too_many_arguments)]
fn set_index_scan_references(
    plan_info: PlanEstimate,
    source_id: usize,
    rel: crate::RelFileLocator,
    relation_oid: u32,
    index_rel: crate::RelFileLocator,
    am_oid: u32,
    toast: Option<crate::include::nodes::primnodes::ToastRelationRef>,
    desc: crate::include::nodes::primnodes::RelationDesc,
    index_meta: crate::backend::utils::cache::relcache::IndexRelCacheEntry,
    keys: Vec<crate::include::access::scankey::ScanKeyData>,
    direction: crate::include::access::relscan::ScanDirection,
) -> Plan {
    Plan::IndexScan {
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
    }
}

fn set_nested_loop_join_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    left: Box<Path>,
    right: Box<Path>,
    kind: crate::include::nodes::primnodes::JoinType,
    restrict_clauses: Vec<RestrictInfo>,
) -> Plan {
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
        if matches!(
            kind,
            crate::include::nodes::primnodes::JoinType::Right
                | crate::include::nodes::primnodes::JoinType::Full
        ) {
            // PostgreSQL does not implement RIGHT/FULL joins as nestloops with the
            // inner side parameterized by the current outer row. Keep those params
            // as ancestor-supplied exec params instead of turning them into
            // immediate nestloop params for this join.
            ctx.ext_params.extend(right_ctx.ext_params);
            (plan, Vec::new())
        } else {
            let mut consumed_parent_params = Vec::new();
            let mut propagated_params = Vec::new();
            let mut params = Vec::new();
            for param in right_ctx.ext_params {
                let mut param_consumed_parent_params = Vec::new();
                let rebased_expr = inline_exec_params(
                    decrement_outer_var_levels(param.expr),
                    &ctx.ext_params,
                    &mut param_consumed_parent_params,
                );
                let fixed_expr =
                    fix_upper_expr_for_input(ctx.root, rebased_expr.clone(), &left, &left_tlist);
                if expr_contains_local_semantic_var(&rebased_expr)
                    && !expr_contains_local_semantic_var(&fixed_expr)
                {
                    consumed_parent_params.extend(param_consumed_parent_params);
                    params.push(ExecParamSource {
                        paramid: param.paramid,
                        expr: lower_expr(ctx, fixed_expr, LowerMode::Input { tlist: &left_tlist }),
                    });
                } else {
                    propagated_params.push(ExecParamSource {
                        paramid: param.paramid,
                        expr: rebased_expr,
                    });
                }
            }
            ctx.ext_params
                .retain(|param| !consumed_parent_params.contains(&param.paramid));
            ctx.ext_params.extend(propagated_params);
            (plan, params)
        }
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

fn set_hash_join_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    left: Box<Path>,
    right: Box<Path>,
    kind: crate::include::nodes::primnodes::JoinType,
    hash_clauses: Vec<RestrictInfo>,
    outer_hash_keys: Vec<Expr>,
    inner_hash_keys: Vec<Expr>,
    restrict_clauses: Vec<RestrictInfo>,
) -> Plan {
    let left_tlist = build_path_tlist(ctx.root, &left);
    let right_tlist = build_path_tlist(ctx.root, &right);
    let hash_restrict_clauses = hash_clauses.clone();

    let outer_hash_keys = outer_hash_keys
        .into_iter()
        .map(|expr| fix_upper_expr_for_input(ctx.root, expr, &left, &left_tlist))
        .collect::<Vec<_>>();
    let inner_hash_keys = inner_hash_keys
        .into_iter()
        .map(|expr| fix_upper_expr_for_input(ctx.root, expr, &right, &right_tlist))
        .collect::<Vec<_>>();
    let lowered_hash_clauses = hash_clauses
        .into_iter()
        .map(|restrict| {
            let expr = fix_join_expr_for_inputs(
                ctx.root,
                restrict.clause,
                &left,
                &right,
                &left_tlist,
                &right_tlist,
            );
            lower_expr(
                ctx,
                expr,
                LowerMode::Join {
                    outer_tlist: &left_tlist,
                    inner_tlist: &right_tlist,
                },
            )
        })
        .collect::<Vec<_>>();
    let outer_hash_keys = outer_hash_keys
        .into_iter()
        .map(|expr| lower_expr(ctx, expr, LowerMode::Input { tlist: &left_tlist }))
        .collect::<Vec<_>>();
    let inner_hash_keys = inner_hash_keys
        .into_iter()
        .map(|expr| {
            lower_expr(
                ctx,
                expr,
                LowerMode::Input {
                    tlist: &right_tlist,
                },
            )
        })
        .collect::<Vec<_>>();
    let (join_restrict_clauses, other_restrict_clauses) =
        split_join_restrict_clauses(kind, &restrict_clauses);
    let join_restrict_clauses = remove_hash_clauses(join_restrict_clauses, &hash_restrict_clauses);
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

fn set_projection_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    input: Box<Path>,
    targets: Vec<TargetEntry>,
) -> Plan {
    let input_tlist = build_path_tlist(ctx.root, &input);
    let root = ctx.root;
    let mut lowered_targets = Vec::with_capacity(targets.len());
    for target in targets {
        let expr = target
            .input_resno
            .and_then(|input_resno| input_tlist.entries.get(input_resno.saturating_sub(1)))
            .map(|entry| special_slot_var(OUTER_VAR, entry.index, entry.sql_type))
            .unwrap_or_else(|| {
                let lowered = lower_projection_expr_by_input_target(
                    root,
                    target.expr.clone(),
                    &input,
                    &input_tlist,
                );
                if expr_contains_local_semantic_var(&lowered) {
                    let rewritten =
                        fix_upper_expr_for_input(root, target.expr.clone(), &input, &input_tlist);
                    rewritten
                } else {
                    fix_upper_expr_for_input(root, lowered, &input, &input_tlist)
                }
            });
        lowered_targets.push(lower_target_entry(
            ctx,
            TargetEntry { expr, ..target },
            LowerMode::Input {
                tlist: &input_tlist,
            },
        ));
    }
    Plan::Projection {
        plan_info,
        input: Box::new(set_plan_refs(ctx, *input)),
        targets: lowered_targets,
    }
}

fn set_order_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    input: Box<Path>,
    items: Vec<OrderByEntry>,
) -> Plan {
    let input_tlist = build_path_tlist(ctx.root, &input);
    let items = items
        .into_iter()
        .map(|item| lower_order_by_expr_for_input(ctx.root, item, &input, &input_tlist))
        .collect::<Vec<_>>();
    let lowered_items = items
        .into_iter()
        .map(|item| {
            lower_order_by_entry(
                ctx,
                item,
                LowerMode::Input {
                    tlist: &input_tlist,
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

fn set_limit_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    input: Box<Path>,
    limit: Option<usize>,
    offset: usize,
) -> Plan {
    Plan::Limit {
        plan_info,
        input: Box::new(set_plan_refs(ctx, *input)),
        limit,
        offset,
    }
}

fn set_aggregate_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    slot_id: usize,
    input: Box<Path>,
    group_by: Vec<Expr>,
    accumulators: Vec<AggAccum>,
    having: Option<Expr>,
    output_columns: Vec<QueryColumn>,
) -> Plan {
    let input_tlist = build_path_tlist(ctx.root, &input);
    let aggregate_layout = aggregate_output_vars(slot_id, &group_by, &accumulators);
    let aggregate_tlist = build_aggregate_tlist(ctx.root, slot_id, &group_by, &accumulators);
    let semantic_group_by = group_by.clone();
    let root = ctx.root;
    let group_by = group_by
        .into_iter()
        .map(|expr| fix_upper_expr_for_input(root, expr, &input, &input_tlist))
        .map(|expr| {
            lower_expr(
                ctx,
                expr,
                LowerMode::Input {
                    tlist: &input_tlist,
                },
            )
        })
        .collect();
    let accumulators = accumulators
        .into_iter()
        .map(|accum| lower_agg_accum(ctx, accum, &input, &input_tlist))
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

fn set_values_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    rows: Vec<Vec<Expr>>,
    output_columns: Vec<QueryColumn>,
) -> Plan {
    Plan::Values {
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
    }
}

fn set_function_scan_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    call: crate::include::nodes::primnodes::SetReturningCall,
) -> Plan {
    Plan::FunctionScan {
        plan_info,
        call: lower_set_returning_call(ctx, call, LowerMode::Scalar),
    }
}

fn set_cte_scan_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    cte_id: usize,
    subroot: PlannerSubroot,
    cte_plan: Box<Path>,
    output_columns: Vec<QueryColumn>,
) -> Plan {
    Plan::CteScan {
        plan_info,
        cte_id,
        cte_plan: Box::new(recurse_with_root(ctx, Some(subroot.as_ref()), *cte_plan)),
        output_columns,
    }
}

fn set_subquery_scan_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    subroot: PlannerSubroot,
    input: Box<Path>,
    output_columns: Vec<QueryColumn>,
) -> Plan {
    let input = recurse_with_root(ctx, Some(subroot.as_ref()), *input);
    if input.columns() == output_columns {
        input
    } else {
        Plan::SubqueryScan {
            plan_info,
            input: Box::new(input),
            output_columns,
        }
    }
}

fn set_worktable_scan_references(
    plan_info: PlanEstimate,
    worktable_id: usize,
    output_columns: Vec<QueryColumn>,
) -> Plan {
    Plan::WorkTableScan {
        plan_info,
        worktable_id,
        output_columns,
    }
}

fn set_recursive_union_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    worktable_id: usize,
    distinct: bool,
    anchor_root: PlannerSubroot,
    recursive_root: PlannerSubroot,
    output_columns: Vec<QueryColumn>,
    anchor: Box<Path>,
    recursive: Box<Path>,
) -> Plan {
    Plan::RecursiveUnion {
        plan_info,
        worktable_id,
        distinct,
        output_columns,
        anchor: Box::new(recurse_with_root(ctx, Some(anchor_root.as_ref()), *anchor)),
        recursive: Box::new(recurse_with_root(
            ctx,
            Some(recursive_root.as_ref()),
            *recursive,
        )),
    }
}

fn set_project_set_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    input: Box<Path>,
    targets: Vec<crate::include::nodes::primnodes::ProjectSetTarget>,
) -> Plan {
    let input_tlist = build_path_tlist(ctx.root, &input);
    let lowered_targets = targets
        .into_iter()
        .map(|target| {
            let target = match target {
                crate::include::nodes::primnodes::ProjectSetTarget::Scalar(entry) => {
                    crate::include::nodes::primnodes::ProjectSetTarget::Scalar(TargetEntry {
                        expr: fix_upper_expr_for_input(ctx.root, entry.expr, &input, &input_tlist),
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
                    call: fix_set_returning_call_upper_exprs(ctx.root, call, &input, &input_tlist),
                    sql_type,
                    column_index,
                },
            };
            lower_project_set_target(
                ctx,
                target,
                LowerMode::Input {
                    tlist: &input_tlist,
                },
            )
        })
        .collect();
    let input_plan = Box::new(set_plan_refs(ctx, *input));
    Plan::ProjectSet {
        plan_info,
        input: input_plan,
        targets: lowered_targets,
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
        } => set_append_references(ctx, plan_info, source_id, desc, children),
        Path::SetOp {
            plan_info,
            op,
            output_columns,
            children,
            ..
        } => set_set_op_references(ctx, plan_info, op, output_columns, children),
        Path::SeqScan {
            plan_info,
            source_id,
            rel,
            relation_name,
            relation_oid,
            toast,
            desc,
        } => set_seq_scan_references(
            plan_info,
            source_id,
            rel,
            relation_name,
            relation_oid,
            toast,
            desc,
        ),
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
        } => set_index_scan_references(
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
        ),
        Path::Filter {
            plan_info,
            input,
            predicate,
        } => set_filter_references(ctx, plan_info, input, predicate),
        Path::NestedLoopJoin {
            plan_info,
            left,
            right,
            kind,
            restrict_clauses,
        } => set_nested_loop_join_references(ctx, plan_info, left, right, kind, restrict_clauses),
        Path::HashJoin {
            plan_info,
            left,
            right,
            kind,
            hash_clauses,
            outer_hash_keys,
            inner_hash_keys,
            restrict_clauses,
        } => set_hash_join_references(
            ctx,
            plan_info,
            left,
            right,
            kind,
            hash_clauses,
            outer_hash_keys,
            inner_hash_keys,
            restrict_clauses,
        ),
        Path::Projection {
            plan_info,
            input,
            targets,
            ..
        } => set_projection_references(ctx, plan_info, input, targets),
        Path::OrderBy {
            plan_info,
            input,
            items,
        } => set_order_references(ctx, plan_info, input, items),
        Path::Limit {
            plan_info,
            input,
            limit,
            offset,
        } => set_limit_references(ctx, plan_info, input, limit, offset),
        Path::Aggregate {
            plan_info,
            slot_id,
            input,
            group_by,
            accumulators,
            having,
            output_columns,
        } => set_aggregate_references(
            ctx,
            plan_info,
            slot_id,
            input,
            group_by,
            accumulators,
            having,
            output_columns,
        ),
        Path::Values {
            plan_info,
            rows,
            output_columns,
            ..
        } => set_values_references(ctx, plan_info, rows, output_columns),
        Path::FunctionScan {
            plan_info, call, ..
        } => set_function_scan_references(ctx, plan_info, call),
        Path::SubqueryScan {
            plan_info,
            subroot,
            query,
            input,
            output_columns,
            ..
        } => {
            let _ = query;
            set_subquery_scan_references(ctx, plan_info, subroot, input, output_columns)
        }
        Path::CteScan {
            plan_info,
            cte_id,
            subroot,
            query,
            cte_plan,
            output_columns,
            ..
        } => {
            let _ = query;
            set_cte_scan_references(ctx, plan_info, cte_id, subroot, cte_plan, output_columns)
        }
        Path::WorkTableScan {
            plan_info,
            worktable_id,
            output_columns,
            ..
        } => set_worktable_scan_references(plan_info, worktable_id, output_columns),
        Path::RecursiveUnion {
            plan_info,
            worktable_id,
            distinct,
            anchor_root,
            recursive_root,
            anchor_query,
            recursive_query,
            output_columns,
            anchor,
            recursive,
            ..
        } => set_recursive_union_references(
            ctx,
            plan_info,
            worktable_id,
            distinct,
            {
                let _ = anchor_query;
                anchor_root
            },
            {
                let _ = recursive_query;
                recursive_root
            },
            output_columns,
            anchor,
            recursive,
        ),
        Path::ProjectSet {
            plan_info,
            input,
            targets,
            ..
        } => set_project_set_references(ctx, plan_info, input, targets),
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
        let expr = fix_join_expr_for_inputs(
            root,
            restrict.clause.clone(),
            left,
            right,
            &outer_tlist,
            &inner_tlist,
        );
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

fn fix_join_expr_for_inputs(
    root: Option<&PlannerInfo>,
    expr: Expr,
    left: &Path,
    right: &Path,
    outer_tlist: &IndexedTlist,
    inner_tlist: &IndexedTlist,
) -> Expr {
    let rewritten = fix_join_expr(root, expr.clone(), outer_tlist, inner_tlist);
    if rewritten != expr {
        return rewritten;
    }
    expr
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
        Expr::Var(_) | Expr::Param(_) => expr,
        Expr::Aggref(aggref) => Expr::Aggref(Box::new(Aggref {
            args: aggref.args.into_iter().map(recurse).collect(),
            aggfilter: aggref.aggfilter.map(recurse),
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
        Expr::SubLink(sublink) => {
            Expr::SubLink(Box::new(crate::include::nodes::primnodes::SubLink {
                testexpr: sublink.testexpr.map(|expr| Box::new(recurse(*expr))),
                ..*sublink
            }))
        }
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
            aggfilter: aggref
                .aggfilter
                .map(|expr| fully_expand_output_expr(expr, path)),
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
        Path::SubqueryScan { .. } => Expr::Var(var),
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
    rebuild_setrefs_expr(root, expr, |inner| {
        fix_join_expr(root, inner, outer_tlist, inner_tlist)
    })
}
