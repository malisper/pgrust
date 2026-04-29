use super::inherit::append_translation;
use super::pathnodes::{
    aggregate_output_vars, expr_sql_type, lower_agg_output_expr, rte_slot_id, rte_slot_varno,
    slot_output_target,
};
use super::plan::append_planned_subquery;
use super::{expand_join_rte_vars, flatten_join_alias_vars, planner_with_param_base_and_config};
use crate::backend::parser::CatalogLookup;
use crate::backend::parser::analyze::{
    bind_index_predicate, flatten_and_conjuncts, predicate_implies_index_predicate,
};
use crate::include::nodes::parsenodes::{
    Query, QueryRowMark, RangeTblEntryKind, TableSampleClause,
};
use crate::include::nodes::pathnodes::{Path, PlannerInfo, PlannerSubroot, RestrictInfo};
use crate::include::nodes::plannodes::{
    ExecParamSource, IndexScanKey, IndexScanKeyArgument, Plan, PlanEstimate, PlanRowMark,
};
use crate::include::nodes::primnodes::{
    AggAccum, Aggref, BoolExpr, Expr, ExprArraySubscript, FuncExpr, INNER_VAR, OUTER_VAR, OpExpr,
    OrderByEntry, Param, ParamKind, QueryColumn, ScalarArrayOpExpr, SubPlan, TargetEntry, Var,
    WindowClause, WindowFuncExpr, WindowFuncKind, XmlExpr, attrno_index, is_executor_special_varno,
    is_system_attr, set_returning_call_exprs, user_attrno,
};
use std::collections::BTreeSet;

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
            Expr::Var(var) => self
                .entries
                .iter()
                .find(|entry| {
                    entry
                        .match_exprs
                        .iter()
                        .any(|candidate| matches!(candidate, Expr::Var(candidate_var) if candidate_var == var))
                })
                .or_else(|| {
                    self.entries.iter().find(|entry| {
                        entry.match_exprs.iter().any(|candidate| match candidate {
                            Expr::Var(candidate_var) => root.is_some_and(|root| {
                                flatten_join_alias_vars(root, Expr::Var(candidate_var.clone()))
                                    == flatten_join_alias_vars(root, expr.clone())
                            }),
                            _ => output_component_matches_expr(candidate, expr),
                        })
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
        path: Option<&'a Path>,
        tlist: &'a IndexedTlist,
    },
    Aggregate {
        group_by: &'a [Expr],
        passthrough_exprs: &'a [Expr],
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
    let plan = maybe_wrap_parallel_gather(root, set_plan_refs(&mut ctx, path));
    let allowed_params = exec_param_sources(&ctx.ext_params);
    validate_executable_plan_with_params(&plan, &allowed_params);
    (plan, ctx.ext_params, ctx.next_param_id)
}

fn maybe_wrap_parallel_gather(root: &PlannerInfo, plan: Plan) -> Plan {
    if root.config.force_parallel_gather
        && root.config.max_parallel_workers_per_gather > 0
        && root.parse.limit_count.is_some()
    {
        return Plan::Gather {
            plan_info: plan.plan_info(),
            input: Box::new(plan),
            workers_planned: root.config.max_parallel_workers_per_gather,
            single_copy: true,
        };
    }
    plan
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

fn build_simple_tlist_from_exprs(output_vars: &[Expr]) -> IndexedTlist {
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

fn build_base_scan_tlist(
    root: Option<&PlannerInfo>,
    source_id: usize,
    desc: &crate::include::nodes::primnodes::RelationDesc,
) -> IndexedTlist {
    let output_vars = slot_output_target(source_id, &desc.columns, |column| column.sql_type).exprs;
    let mut tlist = build_simple_tlist_from_exprs(&output_vars);
    if let Some(info) = root.and_then(|root| append_translation(root, source_id)) {
        for (index, entry) in tlist.entries.iter_mut().enumerate() {
            if info
                .translated_vars
                .get(index)
                .is_some_and(|translated| translated == &output_vars[index])
            {
                entry.match_exprs = dedup_match_exprs(vec![
                    entry.match_exprs[0].clone(),
                    Expr::Var(Var {
                        varno: info.parent_relid,
                        varattno: user_attrno(index),
                        varlevelsup: 0,
                        vartype: entry.sql_type,
                    }),
                ]);
            }
        }
    }
    tlist
}

fn build_simple_tlist(root: Option<&PlannerInfo>, path: &Path) -> IndexedTlist {
    let output_vars = path.output_vars();
    let append_info = root
        .and_then(|root| path_single_relid(path).and_then(|relid| append_translation(root, relid)));
    let mut tlist = build_simple_tlist_from_exprs(&output_vars);
    let semantic_target = path.semantic_output_target();
    for (index, entry) in tlist.entries.iter_mut().enumerate() {
        if let Some(semantic_expr) = semantic_target.exprs.get(index) {
            entry.match_exprs.push(semantic_expr.clone());
            if let Some(root) = root {
                entry
                    .match_exprs
                    .push(flatten_join_alias_vars(root, semantic_expr.clone()));
            }
            entry.match_exprs = dedup_match_exprs(std::mem::take(&mut entry.match_exprs));
        }
    }
    if let Some(info) = append_info {
        for (index, entry) in tlist.entries.iter_mut().enumerate() {
            if info
                .translated_vars
                .get(index)
                .is_some_and(|translated| translated == &output_vars[index])
            {
                entry.match_exprs = dedup_match_exprs(vec![
                    entry.match_exprs[0].clone(),
                    Expr::Var(Var {
                        varno: info.parent_relid,
                        varattno: user_attrno(index),
                        varlevelsup: 0,
                        vartype: entry.sql_type,
                    }),
                ]);
            }
        }
    }
    tlist
}

fn aggregate_output_expr(accum: &crate::include::nodes::primnodes::AggAccum, aggno: usize) -> Expr {
    Expr::Aggref(Box::new(Aggref {
        aggfnoid: accum.aggfnoid,
        aggtype: accum.sql_type,
        aggvariadic: accum.agg_variadic,
        aggdistinct: accum.distinct,
        direct_args: accum.direct_args.clone(),
        args: accum.args.clone(),
        aggorder: accum.order_by.clone(),
        aggfilter: accum.filter.clone(),
        agglevelsup: 0,
        aggno,
    }))
}

fn render_semantic_expr_name(root: Option<&PlannerInfo>, expr: &Expr) -> String {
    match expr {
        Expr::Var(var) if var.varlevelsup == 0 => root
            .and_then(|root| root.parse.rtable.get(var.varno.saturating_sub(1)))
            .and_then(|rte| {
                attrno_index(var.varattno).and_then(|index| {
                    rte.desc.columns.get(index).map(|column| {
                        let qualifier = rte.alias.as_deref().or_else(|| {
                            (!rte.eref.aliasname.is_empty()).then_some(rte.eref.aliasname.as_str())
                        });
                        qualifier
                            .map(|qualifier| format!("{qualifier}.{}", column.name))
                            .unwrap_or_else(|| column.name.clone())
                    })
                })
            })
            .unwrap_or_else(|| crate::backend::executor::render_explain_expr(expr, &[])),
        Expr::Op(op) if op.args.len() == 2 => {
            let op_text = match op.op {
                crate::include::nodes::primnodes::OpExprKind::Add => "+",
                crate::include::nodes::primnodes::OpExprKind::Sub => "-",
                crate::include::nodes::primnodes::OpExprKind::Mul => "*",
                crate::include::nodes::primnodes::OpExprKind::Div => "/",
                crate::include::nodes::primnodes::OpExprKind::Mod => "%",
                crate::include::nodes::primnodes::OpExprKind::Eq => "=",
                crate::include::nodes::primnodes::OpExprKind::NotEq => "<>",
                crate::include::nodes::primnodes::OpExprKind::Lt => "<",
                crate::include::nodes::primnodes::OpExprKind::LtEq => "<=",
                crate::include::nodes::primnodes::OpExprKind::Gt => ">",
                crate::include::nodes::primnodes::OpExprKind::GtEq => ">=",
                _ => return crate::backend::executor::render_explain_expr(expr, &[]),
            };
            format!(
                "({} {} {})",
                render_semantic_expr_name(root, &op.args[0]),
                op_text,
                render_semantic_expr_name(root, &op.args[1])
            )
        }
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => {
            render_semantic_expr_name(root, inner)
        }
        Expr::Const(value) => {
            let rendered =
                crate::backend::executor::render_explain_expr(&Expr::Const(value.clone()), &[]);
            rendered
                .strip_prefix('(')
                .and_then(|value| value.strip_suffix(')'))
                .unwrap_or(&rendered)
                .to_string()
        }
        _ => crate::backend::executor::render_explain_expr(expr, &[]),
    }
}

fn render_semantic_accum_name(root: Option<&PlannerInfo>, accum: &AggAccum) -> String {
    let name = crate::include::catalog::builtin_aggregate_function_for_proc_oid(accum.aggfnoid)
        .map(|func| func.name().to_string())
        .unwrap_or_else(|| format!("agg_{}", accum.aggfnoid));
    let mut args = if accum.args.is_empty() {
        vec!["*".into()]
    } else {
        accum
            .args
            .iter()
            .map(|arg| render_semantic_expr_name(root, arg))
            .collect::<Vec<_>>()
    };
    if accum.distinct && !args.is_empty() {
        args[0] = format!("DISTINCT {}", args[0]);
    }
    let mut rendered = format!("{name}({})", args.join(", "));
    if !accum.order_by.is_empty() {
        let order_by = accum
            .order_by
            .iter()
            .map(|item| render_semantic_expr_name(root, &item.expr))
            .collect::<Vec<_>>()
            .join(", ");
        rendered = format!("{name}({} ORDER BY {order_by})", args.join(", "));
    }
    rendered
}

fn aggregate_semantic_output_names(
    root: Option<&PlannerInfo>,
    group_by: &[Expr],
    passthrough_exprs: &[Expr],
    accumulators: &[AggAccum],
) -> Vec<String> {
    let mut names =
        Vec::with_capacity(group_by.len() + passthrough_exprs.len() + accumulators.len());
    names.extend(
        group_by
            .iter()
            .map(|expr| render_semantic_expr_name(root, expr)),
    );
    names.extend(
        passthrough_exprs
            .iter()
            .map(|expr| render_semantic_expr_name(root, expr)),
    );
    names.extend(
        accumulators
            .iter()
            .map(|accum| render_semantic_accum_name(root, accum)),
    );
    names
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
    phase: crate::include::nodes::plannodes::AggregatePhase,
    group_by: &[Expr],
    passthrough_exprs: &[Expr],
    accumulators: &[crate::include::nodes::primnodes::AggAccum],
    semantic_accumulators: Option<&[crate::include::nodes::primnodes::AggAccum]>,
) -> IndexedTlist {
    let display_accumulators = semantic_accumulators.unwrap_or(accumulators);
    let mut entries =
        Vec::with_capacity(group_by.len() + passthrough_exprs.len() + accumulators.len());
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
    for (index, expr) in passthrough_exprs.iter().enumerate() {
        let index = group_by.len() + index;
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
        let display_accum = display_accumulators.get(aggno).unwrap_or(accum);
        let index = group_by.len() + passthrough_exprs.len() + aggno;
        let output_type = if phase == crate::include::nodes::plannodes::AggregatePhase::Partial {
            crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Record)
        } else {
            accum.sql_type
        };
        entries.push(IndexedTlistEntry {
            index,
            sql_type: output_type,
            ressortgroupref: 0,
            match_exprs: dedup_match_exprs(vec![
                slot_var(slot_id, user_attrno(index), output_type),
                aggregate_output_expr(display_accum, aggno),
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
                        source_expr,
                        sql_type,
                        ..
                    } => (
                        *sql_type,
                        0,
                        vec![
                            slot_var(slot_id, user_attrno(index), *sql_type),
                            source_expr.clone(),
                        ],
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

fn build_window_tlist(
    root: Option<&PlannerInfo>,
    slot_id: usize,
    input: &Path,
    clause: &WindowClause,
    output_columns: &[QueryColumn],
) -> IndexedTlist {
    let input_target = input.semantic_output_target();
    let input_output_target = input.output_target();
    let mut entries = Vec::with_capacity(output_columns.len());
    for (index, column) in output_columns.iter().enumerate() {
        let mut match_exprs = vec![slot_var(slot_id, user_attrno(index), column.sql_type)];
        let ressortgroupref = input_output_target
            .sortgrouprefs
            .get(index)
            .copied()
            .unwrap_or(0);
        if let Some(input_expr) = input_target.exprs.get(index) {
            match_exprs.push(input_expr.clone());
            match_exprs.push(fully_expand_output_expr_with_root(
                root,
                input_expr.clone(),
                input,
            ));
            if let Some(root) = root {
                match_exprs.push(flatten_join_alias_vars(root, input_expr.clone()));
            }
        } else if let Some(func) = clause.functions.get(index - input_target.exprs.len()) {
            let func_expr = Expr::WindowFunc(Box::new(func.clone()));
            match_exprs.push(func_expr.clone());
            match_exprs.push(fully_expand_output_expr_with_root(
                root,
                func_expr.clone(),
                input,
            ));
            if let Some(root) = root {
                match_exprs.push(flatten_join_alias_vars(root, func_expr));
            }
        }
        entries.push(IndexedTlistEntry {
            index,
            sql_type: column.sql_type,
            ressortgroupref,
            match_exprs: dedup_match_exprs(match_exprs),
        });
    }
    IndexedTlist { entries }
}

fn build_join_tlist(
    root: Option<&PlannerInfo>,
    path: &Path,
    left: &Path,
    right: &Path,
) -> IndexedTlist {
    let left_tlist = build_path_tlist(root, left);
    let right_tlist = build_path_tlist(root, right);
    let left_physical_width = left.output_vars().len();
    let output_target = path.output_target();
    let semantic_target = path.semantic_output_target();
    let mut entries = Vec::with_capacity(semantic_target.exprs.len());

    for (logical_index, semantic_expr) in semantic_target.exprs.iter().enumerate() {
        let ressortgroupref = semantic_target.get_pathtarget_sortgroupref(logical_index);
        let left_match = search_tlist_entry(root, semantic_expr, &left_tlist);
        let right_match = search_tlist_entry(root, semantic_expr, &right_tlist);
        let (physical_index, sql_type, mut match_exprs) = match (left_match, right_match) {
            (Some(entry), None) => (entry.index, entry.sql_type, entry.match_exprs.clone()),
            (None, Some(entry)) => (
                left_physical_width + entry.index,
                entry.sql_type,
                entry.match_exprs.clone(),
            ),
            (Some(left_entry), Some(right_entry)) => {
                let left_index = left_tlist
                    .entries
                    .iter()
                    .position(|entry| entry.index == left_entry.index);
                let right_index = right_tlist
                    .entries
                    .iter()
                    .position(|entry| entry.index == right_entry.index);
                if logical_index < left_tlist.entries.len()
                    && left_index == Some(logical_index)
                    && right_index != Some(logical_index)
                {
                    (
                        left_entry.index,
                        left_entry.sql_type,
                        left_entry.match_exprs.clone(),
                    )
                } else {
                    (
                        left_physical_width + right_entry.index,
                        right_entry.sql_type,
                        right_entry.match_exprs.clone(),
                    )
                }
            }
            (None, None) => (
                logical_index,
                expr_sql_type(semantic_expr),
                vec![semantic_expr.clone()],
            ),
        };

        if let Some(output_expr) = output_target.exprs.get(physical_index) {
            match_exprs.push(output_expr.clone());
        }
        match_exprs.push(semantic_expr.clone());
        entries.push(IndexedTlistEntry {
            index: physical_index,
            sql_type,
            ressortgroupref,
            match_exprs: dedup_match_exprs(match_exprs),
        });
    }
    IndexedTlist { entries }
}

fn build_subquery_tlist(
    rtindex: usize,
    _query: &Query,
    _input: &Path,
    output_columns: &[QueryColumn],
) -> IndexedTlist {
    IndexedTlist {
        entries: output_columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                let match_exprs = vec![
                    Expr::Var(Var {
                        varno: rtindex,
                        varattno: user_attrno(index),
                        varlevelsup: 0,
                        vartype: column.sql_type,
                    }),
                    slot_var(rte_slot_id(rtindex), user_attrno(index), column.sql_type),
                ];
                IndexedTlistEntry {
                    index,
                    sql_type: column.sql_type,
                    ressortgroupref: 0,
                    match_exprs: dedup_match_exprs(match_exprs),
                }
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
        Path::Unique { input, .. }
        | Path::Filter { input, .. }
        | Path::OrderBy { input, .. }
        | Path::IncrementalSort { input, .. }
        | Path::Limit { input, .. }
        | Path::LockRows { input, .. } => build_path_tlist(root, input),
        Path::Aggregate {
            slot_id,
            phase,
            group_by,
            passthrough_exprs,
            accumulators,
            semantic_accumulators,
            ..
        } => build_aggregate_tlist(
            root,
            *slot_id,
            *phase,
            group_by,
            passthrough_exprs,
            accumulators,
            semantic_accumulators.as_deref(),
        ),
        Path::WindowAgg {
            slot_id,
            input,
            clause,
            output_columns,
            ..
        } => build_window_tlist(root, *slot_id, input, clause, output_columns),
        Path::ProjectSet {
            slot_id,
            input,
            targets,
            ..
        } => build_project_set_tlist(root, *slot_id, input, targets),
        Path::SubqueryScan {
            rtindex,
            query,
            input,
            output_columns,
            ..
        } => build_subquery_tlist(*rtindex, query, input, output_columns),
        Path::NestedLoopJoin { left, right, .. }
        | Path::HashJoin { left, right, .. }
        | Path::MergeJoin { left, right, .. } => build_join_tlist(root, path, left, right),
        _ => build_simple_tlist(root, path),
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
            aggorder: aggref
                .aggorder
                .into_iter()
                .map(|item| OrderByEntry {
                    expr: lower_projection_expr_by_input_target(
                        root,
                        item.expr,
                        input,
                        input_tlist,
                    ),
                    ..item
                })
                .collect(),
            aggfilter: aggref
                .aggfilter
                .map(|expr| lower_projection_expr_by_input_target(root, expr, input, input_tlist)),
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
        Expr::Collate {
            expr,
            collation_oid,
        } => Expr::Collate {
            expr: Box::new(lower_projection_expr_by_input_target(
                root,
                *expr,
                input,
                input_tlist,
            )),
            collation_oid,
        },
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
            collation_oid,
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
            collation_oid,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
            collation_oid,
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
            collation_oid,
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
                && (attrno_index(var.varattno).is_some() || is_system_attr(var.varattno))
        }
        Expr::Aggref(aggref) => {
            aggref.args.iter().any(expr_contains_local_semantic_var)
                || aggref
                    .aggfilter
                    .as_ref()
                    .is_some_and(expr_contains_local_semantic_var)
        }
        Expr::WindowFunc(window_func) => {
            window_func
                .args
                .iter()
                .any(expr_contains_local_semantic_var)
                || match &window_func.kind {
                    WindowFuncKind::Aggregate(aggref) => {
                        aggref.args.iter().any(expr_contains_local_semantic_var)
                            || aggref
                                .aggfilter
                                .as_ref()
                                .is_some_and(expr_contains_local_semantic_var)
                    }
                    WindowFuncKind::Builtin(_) => false,
                }
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
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => {
            expr_contains_local_semantic_var(inner)
        }
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

fn expr_is_local_system_var(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Var(var)
            if var.varlevelsup == 0
                && !is_executor_special_varno(var.varno)
                && is_system_attr(var.varattno)
    )
}

fn path_single_relid(path: &Path) -> Option<usize> {
    match path {
        Path::Append { relids, .. } => relids.first().copied().filter(|_| relids.len() == 1),
        Path::MergeAppend { source_id, .. }
        | Path::SeqScan { source_id, .. }
        | Path::IndexOnlyScan { source_id, .. }
        | Path::IndexScan { source_id, .. }
        | Path::BitmapIndexScan { source_id, .. }
        | Path::BitmapHeapScan { source_id, .. } => Some(*source_id),
        Path::BitmapOr { .. } => None,
        Path::Unique { input, .. }
        | Path::Filter { input, .. }
        | Path::Projection { input, .. }
        | Path::OrderBy { input, .. }
        | Path::IncrementalSort { input, .. }
        | Path::Limit { input, .. }
        | Path::LockRows { input, .. }
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
            aggorder: aggref
                .aggorder
                .into_iter()
                .map(|item| OrderByEntry {
                    expr: rewrite_expr_for_append_rel(item.expr, info),
                    ..item
                })
                .collect(),
            aggfilter: aggref
                .aggfilter
                .map(|expr| rewrite_expr_for_append_rel(expr, info)),
            ..*aggref
        })),
        Expr::WindowFunc(window_func) => Expr::WindowFunc(Box::new(WindowFuncExpr {
            kind: match window_func.kind {
                WindowFuncKind::Aggregate(aggref) => WindowFuncKind::Aggregate(Aggref {
                    args: aggref
                        .args
                        .into_iter()
                        .map(|arg| rewrite_expr_for_append_rel(arg, info))
                        .collect(),
                    aggorder: aggref
                        .aggorder
                        .into_iter()
                        .map(|item| OrderByEntry {
                            expr: rewrite_expr_for_append_rel(item.expr, info),
                            ..item
                        })
                        .collect(),
                    aggfilter: aggref
                        .aggfilter
                        .map(|expr| rewrite_expr_for_append_rel(expr, info)),
                    ..aggref
                }),
                WindowFuncKind::Builtin(kind) => WindowFuncKind::Builtin(kind),
            },
            args: window_func
                .args
                .into_iter()
                .map(|arg| rewrite_expr_for_append_rel(arg, info))
                .collect(),
            ..*window_func
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
        Expr::Collate {
            expr,
            collation_oid,
        } => Expr::Collate {
            expr: Box::new(rewrite_expr_for_append_rel(*expr, info)),
            collation_oid,
        },
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

fn fix_immediate_subquery_output_expr(
    root: Option<&PlannerInfo>,
    expr: &Expr,
    input: &Path,
    input_tlist: &IndexedTlist,
) -> Option<Expr> {
    let Path::SubqueryScan {
        input: subquery_input,
        ..
    } = input
    else {
        return None;
    };
    let input_exprs = subquery_input.semantic_output_vars();
    input_exprs
        .iter()
        .enumerate()
        .find_map(|(index, input_expr)| {
            let expanded =
                fully_expand_output_expr_with_root(root, input_expr.clone(), subquery_input);
            (exprs_equivalent(root, input_expr, expr) || exprs_equivalent(root, &expanded, expr))
                .then(|| {
                    input_tlist
                        .entries
                        .get(index)
                        .map(|entry| special_slot_var(OUTER_VAR, entry.index, entry.sql_type))
                })
                .flatten()
        })
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
    if let Some(rewritten) = fix_immediate_subquery_output_expr(root, &expr, input, input_tlist) {
        return rewritten;
    }
    expr
}

fn lower_direct_ref(expr: &Expr, mode: LowerMode<'_>) -> Option<Expr> {
    match mode {
        LowerMode::Scalar => None,
        LowerMode::Input { tlist, .. } => search_tlist_entry(None, expr, tlist)
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

fn exec_param_for_outer_expr(ctx: &mut SetRefsContext<'_>, expr: Expr) -> Expr {
    let parent_expr = decrement_outer_expr_levels(expr.clone());
    let paramtype = expr_sql_type(&parent_expr);
    if let Some(existing) = ctx
        .ext_params
        .iter()
        .find(|param| param.expr == parent_expr)
    {
        return Expr::Param(Param {
            paramkind: ParamKind::Exec,
            paramid: existing.paramid,
            paramtype,
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
        paramtype,
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
        Expr::Xml(xml) => Expr::Xml(Box::new(XmlExpr {
            named_args: xml
                .named_args
                .into_iter()
                .map(|arg| inline_exec_params(arg, params, consumed))
                .collect(),
            args: xml
                .args
                .into_iter()
                .map(|arg| inline_exec_params(arg, params, consumed))
                .collect(),
            ..*xml
        })),
        Expr::Aggref(aggref) => Expr::Aggref(Box::new(Aggref {
            args: aggref
                .args
                .into_iter()
                .map(|arg| inline_exec_params(arg, params, consumed))
                .collect(),
            aggorder: aggref
                .aggorder
                .into_iter()
                .map(|item| OrderByEntry {
                    expr: inline_exec_params(item.expr, params, consumed),
                    ..item
                })
                .collect(),
            aggfilter: aggref
                .aggfilter
                .map(|expr| inline_exec_params(expr, params, consumed)),
            ..*aggref
        })),
        Expr::WindowFunc(window_func) => Expr::WindowFunc(Box::new(WindowFuncExpr {
            kind: match window_func.kind {
                WindowFuncKind::Aggregate(aggref) => WindowFuncKind::Aggregate(Aggref {
                    args: aggref
                        .args
                        .into_iter()
                        .map(|arg| inline_exec_params(arg, params, consumed))
                        .collect(),
                    aggorder: aggref
                        .aggorder
                        .into_iter()
                        .map(|item| OrderByEntry {
                            expr: inline_exec_params(item.expr, params, consumed),
                            ..item
                        })
                        .collect(),
                    aggfilter: aggref
                        .aggfilter
                        .map(|expr| inline_exec_params(expr, params, consumed)),
                    ..aggref
                }),
                WindowFuncKind::Builtin(kind) => WindowFuncKind::Builtin(kind),
            },
            args: window_func
                .args
                .into_iter()
                .map(|arg| inline_exec_params(arg, params, consumed))
                .collect(),
            ..*window_func
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

fn decrement_outer_expr_levels(expr: Expr) -> Expr {
    match expr {
        Expr::Var(mut var) if var.varlevelsup > 0 => {
            var.varlevelsup -= 1;
            Expr::Var(var)
        }
        Expr::Aggref(mut aggref) => {
            if aggref.agglevelsup > 0 {
                aggref.agglevelsup -= 1;
            }
            Expr::Aggref(Box::new(Aggref {
                args: aggref
                    .args
                    .into_iter()
                    .map(decrement_outer_expr_levels)
                    .collect(),
                aggorder: aggref
                    .aggorder
                    .into_iter()
                    .map(|item| OrderByEntry {
                        expr: decrement_outer_expr_levels(item.expr),
                        ..item
                    })
                    .collect(),
                aggfilter: aggref.aggfilter.map(decrement_outer_expr_levels),
                ..*aggref
            }))
        }
        Expr::Op(op) => Expr::Op(Box::new(OpExpr {
            args: op
                .args
                .into_iter()
                .map(decrement_outer_expr_levels)
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(decrement_outer_expr_levels)
                .collect(),
            ..*bool_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(FuncExpr {
            args: func
                .args
                .into_iter()
                .map(decrement_outer_expr_levels)
                .collect(),
            ..*func
        })),
        Expr::Xml(xml) => Expr::Xml(Box::new(XmlExpr {
            named_args: xml
                .named_args
                .into_iter()
                .map(decrement_outer_expr_levels)
                .collect(),
            args: xml
                .args
                .into_iter()
                .map(decrement_outer_expr_levels)
                .collect(),
            ..*xml
        })),
        Expr::WindowFunc(window_func) => Expr::WindowFunc(Box::new(WindowFuncExpr {
            kind: match window_func.kind {
                WindowFuncKind::Aggregate(aggref) => WindowFuncKind::Aggregate(Aggref {
                    args: aggref
                        .args
                        .into_iter()
                        .map(decrement_outer_expr_levels)
                        .collect(),
                    aggorder: aggref
                        .aggorder
                        .into_iter()
                        .map(|item| OrderByEntry {
                            expr: decrement_outer_expr_levels(item.expr),
                            ..item
                        })
                        .collect(),
                    aggfilter: aggref.aggfilter.map(decrement_outer_expr_levels),
                    ..aggref
                }),
                WindowFuncKind::Builtin(kind) => WindowFuncKind::Builtin(kind),
            },
            args: window_func
                .args
                .into_iter()
                .map(decrement_outer_expr_levels)
                .collect(),
            ..*window_func
        })),
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            left: Box::new(decrement_outer_expr_levels(*saop.left)),
            right: Box::new(decrement_outer_expr_levels(*saop.right)),
            ..*saop
        })),
        Expr::Cast(inner, ty) => Expr::Cast(Box::new(decrement_outer_expr_levels(*inner)), ty),
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(decrement_outer_expr_levels(*left)),
            Box::new(decrement_outer_expr_levels(*right)),
        ),
        Expr::IsNull(inner) => Expr::IsNull(Box::new(decrement_outer_expr_levels(*inner))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(decrement_outer_expr_levels(*inner))),
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(decrement_outer_expr_levels(*left)),
            Box::new(decrement_outer_expr_levels(*right)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(decrement_outer_expr_levels(*left)),
            Box::new(decrement_outer_expr_levels(*right)),
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
            timezone,
            output_columns,
            with_ordinality,
        } => SetReturningCall::GenerateSeries {
            func_oid,
            func_variadic,
            start: lower_expr(ctx, start, mode),
            stop: lower_expr(ctx, stop, mode),
            step: lower_expr(ctx, step, mode),
            timezone: timezone.map(|timezone| lower_expr(ctx, timezone, mode)),
            output_columns,
            with_ordinality,
        },
        SetReturningCall::GenerateSubscripts {
            func_oid,
            func_variadic,
            array,
            dimension,
            reverse,
            output_columns,
            with_ordinality,
        } => SetReturningCall::GenerateSubscripts {
            func_oid,
            func_variadic,
            array: lower_expr(ctx, array, mode),
            dimension: lower_expr(ctx, dimension, mode),
            reverse: reverse.map(|reverse| lower_expr(ctx, reverse, mode)),
            output_columns,
            with_ordinality,
        },
        SetReturningCall::PartitionTree {
            func_oid,
            func_variadic,
            relid,
            output_columns,
            with_ordinality,
        } => SetReturningCall::PartitionTree {
            func_oid,
            func_variadic,
            relid: lower_expr(ctx, relid, mode),
            output_columns,
            with_ordinality,
        },
        SetReturningCall::PartitionAncestors {
            func_oid,
            func_variadic,
            relid,
            output_columns,
            with_ordinality,
        } => SetReturningCall::PartitionAncestors {
            func_oid,
            func_variadic,
            relid: lower_expr(ctx, relid, mode),
            output_columns,
            with_ordinality,
        },
        SetReturningCall::PgLockStatus {
            func_oid,
            func_variadic,
            output_columns,
            with_ordinality,
        } => SetReturningCall::PgLockStatus {
            func_oid,
            func_variadic,
            output_columns,
            with_ordinality,
        },
        SetReturningCall::TxidSnapshotXip {
            func_oid,
            func_variadic,
            arg,
            output_columns,
            with_ordinality,
        } => SetReturningCall::TxidSnapshotXip {
            func_oid,
            func_variadic,
            arg: lower_expr(ctx, arg, mode),
            output_columns,
            with_ordinality,
        },
        SetReturningCall::Unnest {
            func_oid,
            func_variadic,
            args,
            output_columns,
            with_ordinality,
        } => SetReturningCall::Unnest {
            func_oid,
            func_variadic,
            args: args
                .into_iter()
                .map(|arg| lower_expr(ctx, arg, mode))
                .collect(),
            output_columns,
            with_ordinality,
        },
        SetReturningCall::JsonTableFunction {
            func_oid,
            func_variadic,
            kind,
            args,
            output_columns,
            with_ordinality,
        } => SetReturningCall::JsonTableFunction {
            func_oid,
            func_variadic,
            kind,
            args: args
                .into_iter()
                .map(|arg| lower_expr(ctx, arg, mode))
                .collect(),
            output_columns,
            with_ordinality,
        },
        SetReturningCall::JsonRecordFunction {
            func_oid,
            func_variadic,
            kind,
            args,
            output_columns,
            record_type,
            with_ordinality,
        } => SetReturningCall::JsonRecordFunction {
            func_oid,
            func_variadic,
            kind,
            args: args
                .into_iter()
                .map(|arg| lower_expr(ctx, arg, mode))
                .collect(),
            output_columns,
            record_type,
            with_ordinality,
        },
        SetReturningCall::RegexTableFunction {
            func_oid,
            func_variadic,
            kind,
            args,
            output_columns,
            with_ordinality,
        } => SetReturningCall::RegexTableFunction {
            func_oid,
            func_variadic,
            kind,
            args: args
                .into_iter()
                .map(|arg| lower_expr(ctx, arg, mode))
                .collect(),
            output_columns,
            with_ordinality,
        },
        SetReturningCall::StringTableFunction {
            func_oid,
            func_variadic,
            kind,
            args,
            output_columns,
            with_ordinality,
        } => SetReturningCall::StringTableFunction {
            func_oid,
            func_variadic,
            kind,
            args: args
                .into_iter()
                .map(|arg| lower_expr(ctx, arg, mode))
                .collect(),
            output_columns,
            with_ordinality,
        },
        SetReturningCall::TextSearchTableFunction {
            kind,
            args,
            output_columns,
            with_ordinality,
        } => SetReturningCall::TextSearchTableFunction {
            kind,
            args: args
                .into_iter()
                .map(|arg| lower_expr(ctx, arg, mode))
                .collect(),
            output_columns,
            with_ordinality,
        },
        SetReturningCall::UserDefined {
            proc_oid,
            function_name,
            func_variadic,
            args,
            output_columns,
            with_ordinality,
        } => SetReturningCall::UserDefined {
            proc_oid,
            function_name,
            func_variadic,
            args: args
                .into_iter()
                .map(|arg| lower_expr(ctx, arg, mode))
                .collect(),
            output_columns,
            with_ordinality,
        },
        sql @ (SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_)) => {
            sql.map_exprs(|arg| lower_expr(ctx, arg, mode))
        }
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
            timezone,
            output_columns,
            with_ordinality,
        } => SetReturningCall::GenerateSeries {
            func_oid,
            func_variadic,
            start: fix_upper_expr_for_input(root, start, path, input_tlist),
            stop: fix_upper_expr_for_input(root, stop, path, input_tlist),
            step: fix_upper_expr_for_input(root, step, path, input_tlist),
            timezone: timezone
                .map(|timezone| fix_upper_expr_for_input(root, timezone, path, input_tlist)),
            output_columns,
            with_ordinality,
        },
        SetReturningCall::GenerateSubscripts {
            func_oid,
            func_variadic,
            array,
            dimension,
            reverse,
            output_columns,
            with_ordinality,
        } => SetReturningCall::GenerateSubscripts {
            func_oid,
            func_variadic,
            array: fix_upper_expr_for_input(root, array, path, input_tlist),
            dimension: fix_upper_expr_for_input(root, dimension, path, input_tlist),
            reverse: reverse
                .map(|reverse| fix_upper_expr_for_input(root, reverse, path, input_tlist)),
            output_columns,
            with_ordinality,
        },
        SetReturningCall::PartitionTree {
            func_oid,
            func_variadic,
            relid,
            output_columns,
            with_ordinality,
        } => SetReturningCall::PartitionTree {
            func_oid,
            func_variadic,
            relid: fix_upper_expr_for_input(root, relid, path, input_tlist),
            output_columns,
            with_ordinality,
        },
        SetReturningCall::PartitionAncestors {
            func_oid,
            func_variadic,
            relid,
            output_columns,
            with_ordinality,
        } => SetReturningCall::PartitionAncestors {
            func_oid,
            func_variadic,
            relid: fix_upper_expr_for_input(root, relid, path, input_tlist),
            output_columns,
            with_ordinality,
        },
        SetReturningCall::PgLockStatus {
            func_oid,
            func_variadic,
            output_columns,
            with_ordinality,
        } => SetReturningCall::PgLockStatus {
            func_oid,
            func_variadic,
            output_columns,
            with_ordinality,
        },
        SetReturningCall::TxidSnapshotXip {
            func_oid,
            func_variadic,
            arg,
            output_columns,
            with_ordinality,
        } => SetReturningCall::TxidSnapshotXip {
            func_oid,
            func_variadic,
            arg: fix_upper_expr_for_input(root, arg, path, input_tlist),
            output_columns,
            with_ordinality,
        },
        SetReturningCall::Unnest {
            func_oid,
            func_variadic,
            args,
            output_columns,
            with_ordinality,
        } => SetReturningCall::Unnest {
            func_oid,
            func_variadic,
            args: args
                .into_iter()
                .map(|arg| fix_upper_expr_for_input(root, arg, path, input_tlist))
                .collect(),
            output_columns,
            with_ordinality,
        },
        SetReturningCall::JsonTableFunction {
            func_oid,
            func_variadic,
            kind,
            args,
            output_columns,
            with_ordinality,
        } => SetReturningCall::JsonTableFunction {
            func_oid,
            func_variadic,
            kind,
            args: args
                .into_iter()
                .map(|arg| fix_upper_expr_for_input(root, arg, path, input_tlist))
                .collect(),
            output_columns,
            with_ordinality,
        },
        SetReturningCall::JsonRecordFunction {
            func_oid,
            func_variadic,
            kind,
            args,
            output_columns,
            record_type,
            with_ordinality,
        } => SetReturningCall::JsonRecordFunction {
            func_oid,
            func_variadic,
            kind,
            args: args
                .into_iter()
                .map(|arg| fix_upper_expr_for_input(root, arg, path, input_tlist))
                .collect(),
            output_columns,
            record_type,
            with_ordinality,
        },
        SetReturningCall::RegexTableFunction {
            func_oid,
            func_variadic,
            kind,
            args,
            output_columns,
            with_ordinality,
        } => SetReturningCall::RegexTableFunction {
            func_oid,
            func_variadic,
            kind,
            args: args
                .into_iter()
                .map(|arg| fix_upper_expr_for_input(root, arg, path, input_tlist))
                .collect(),
            output_columns,
            with_ordinality,
        },
        SetReturningCall::StringTableFunction {
            func_oid,
            func_variadic,
            kind,
            args,
            output_columns,
            with_ordinality,
        } => SetReturningCall::StringTableFunction {
            func_oid,
            func_variadic,
            kind,
            args: args
                .into_iter()
                .map(|arg| fix_upper_expr_for_input(root, arg, path, input_tlist))
                .collect(),
            output_columns,
            with_ordinality,
        },
        SetReturningCall::TextSearchTableFunction {
            kind,
            args,
            output_columns,
            with_ordinality,
        } => SetReturningCall::TextSearchTableFunction {
            kind,
            args: args
                .into_iter()
                .map(|arg| fix_upper_expr_for_input(root, arg, path, input_tlist))
                .collect(),
            output_columns,
            with_ordinality,
        },
        SetReturningCall::UserDefined {
            proc_oid,
            function_name,
            func_variadic,
            args,
            output_columns,
            with_ordinality,
        } => SetReturningCall::UserDefined {
            proc_oid,
            function_name,
            func_variadic,
            args: args
                .into_iter()
                .map(|arg| fix_upper_expr_for_input(root, arg, path, input_tlist))
                .collect(),
            output_columns,
            with_ordinality,
        },
        sql @ (SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_)) => {
            sql.map_exprs(|arg| fix_upper_expr_for_input(root, arg, path, input_tlist))
        }
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
            source_expr,
            call,
            sql_type,
            column_index,
        } => ProjectSetTarget::Set {
            name,
            source_expr,
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
                lower_expr(
                    ctx,
                    arg,
                    LowerMode::Input {
                        path: Some(path),
                        tlist: input_tlist,
                    },
                )
            })
            .collect(),
        order_by: accum
            .order_by
            .into_iter()
            .map(|item| lower_order_by_expr_for_input(ctx.root, item, path, input_tlist))
            .collect(),
        filter: accum.filter.map(|filter| {
            let filter = fix_upper_expr_for_input(ctx.root, filter, path, input_tlist);
            lower_expr(
                ctx,
                filter,
                LowerMode::Input {
                    path: Some(path),
                    tlist: input_tlist,
                },
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
    let config = ctx.root.map(|root| root.config).unwrap_or_default();
    let (planned_stmt, next_param_id) =
        planner_with_param_base_and_config(*sublink.subselect, catalog, ctx.next_param_id, config)
            .expect("locking validation should complete before setrefs subplan lowering");
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
                LowerMode::Input { path, tlist } => path
                    .map(|path| fix_upper_expr_for_input(ctx.root, param.expr.clone(), path, tlist))
                    .unwrap_or_else(|| fix_upper_expr(ctx.root, param.expr.clone(), tlist)),
                LowerMode::Aggregate {
                    group_by,
                    passthrough_exprs,
                    layout,
                    ..
                } => match ctx.root {
                    Some(root) => lower_agg_output_expr(
                        expand_join_rte_vars(root, param.expr.clone()),
                        group_by,
                        passthrough_exprs,
                        layout,
                    ),
                    None => lower_agg_output_expr(
                        param.expr.clone(),
                        group_by,
                        passthrough_exprs,
                        layout,
                    ),
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
        Expr::Var(var) if var.varlevelsup > 0 => exec_param_for_outer_expr(ctx, Expr::Var(var)),
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
        Expr::Aggref(aggref) if aggref.agglevelsup > 0 => {
            exec_param_for_outer_expr(ctx, Expr::Aggref(aggref))
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
        Expr::Xml(xml) => Expr::Xml(Box::new(XmlExpr {
            named_args: xml
                .named_args
                .into_iter()
                .map(|arg| lower_expr(ctx, arg, mode))
                .collect(),
            args: xml
                .args
                .into_iter()
                .map(|arg| lower_expr(ctx, arg, mode))
                .collect(),
            ..*xml
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
        Expr::Collate {
            expr,
            collation_oid,
        } => Expr::Collate {
            expr: Box::new(lower_expr(ctx, *expr, mode)),
            collation_oid,
        },
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
            collation_oid,
        } => Expr::Like {
            expr: Box::new(lower_expr(ctx, *expr, mode)),
            pattern: Box::new(lower_expr(ctx, *pattern, mode)),
            escape: escape.map(|expr| Box::new(lower_expr(ctx, *expr, mode))),
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
            expr: Box::new(lower_expr(ctx, *expr, mode)),
            pattern: Box::new(lower_expr(ctx, *pattern, mode)),
            escape: escape.map(|expr| Box::new(lower_expr(ctx, *expr, mode))),
            negated,
            collation_oid,
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
        Expr::Row { descriptor, fields } => Expr::Row {
            descriptor,
            fields: fields
                .into_iter()
                .map(|(name, expr)| (name, lower_expr(ctx, expr, mode)))
                .collect(),
        },
        Expr::FieldSelect {
            expr,
            field,
            field_type,
        } => Expr::FieldSelect {
            expr: Box::new(lower_expr(ctx, *expr, mode)),
            field,
            field_type,
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

fn lower_index_scan_key(
    ctx: &mut SetRefsContext<'_>,
    key: IndexScanKey,
    mode: LowerMode<'_>,
) -> IndexScanKey {
    let argument = match key.argument {
        IndexScanKeyArgument::Const(value) => IndexScanKeyArgument::Const(value),
        IndexScanKeyArgument::Runtime(expr) => {
            IndexScanKeyArgument::Runtime(lower_expr(ctx, expr, mode))
        }
    };
    IndexScanKey {
        attribute_number: key.attribute_number,
        strategy: key.strategy,
        argument,
        display_expr: key.display_expr,
    }
}

fn lower_index_scan_keys(
    ctx: &mut SetRefsContext<'_>,
    keys: Vec<IndexScanKey>,
    mode: LowerMode<'_>,
) -> Vec<IndexScanKey> {
    keys.into_iter()
        .map(|key| lower_index_scan_key(ctx, key, mode))
        .collect()
}

fn index_scan_can_use_index_only(
    ctx: &SetRefsContext<'_>,
    source_id: usize,
    am_oid: u32,
    desc: &crate::include::nodes::primnodes::RelationDesc,
    index_meta: &crate::backend::utils::cache::relcache::IndexRelCacheEntry,
) -> bool {
    let Some(root) = ctx.root else {
        return false;
    };
    if !root.parse.row_marks.is_empty() {
        return false;
    }
    if !matches!(
        am_oid,
        crate::include::catalog::BTREE_AM_OID
            | crate::include::catalog::GIST_AM_OID
            | crate::include::catalog::SPGIST_AM_OID
    ) {
        return false;
    }
    let query_relids = root.all_query_relids();
    if query_relids.len() != 1 || query_relids[0] != source_id {
        return false;
    }
    let covered_columns = index_meta
        .indkey
        .iter()
        .filter_map(|attnum| {
            (*attnum > 0)
                .then(|| usize::try_from(*attnum).ok()?.checked_sub(1))
                .flatten()
        })
        .collect::<BTreeSet<_>>();
    if covered_columns.is_empty() {
        return false;
    }
    let index_predicate = ctx.catalog.and_then(|catalog| {
        bind_index_predicate(index_meta, desc, catalog)
            .ok()
            .flatten()
    });
    root.parse
        .target_list
        .iter()
        .all(|target| expr_uses_only_index_keys(&target.expr, source_id, &covered_columns))
        && root.parse.where_qual.as_ref().is_none_or(|expr| {
            flatten_and_conjuncts(expr).iter().all(|conjunct| {
                expr_uses_only_index_keys(conjunct, source_id, &covered_columns)
                    || predicate_implies_index_predicate(index_predicate.as_ref(), Some(conjunct))
            })
        })
        && root
            .parse
            .sort_clause
            .iter()
            .all(|clause| expr_uses_only_index_keys(&clause.expr, source_id, &covered_columns))
}

fn expr_uses_only_index_keys(
    expr: &Expr,
    source_id: usize,
    covered_columns: &BTreeSet<usize>,
) -> bool {
    match expr {
        Expr::Var(var) => {
            if var.varlevelsup > 0 {
                return true;
            }
            var.varlevelsup == 0
                && var.varno == source_id
                && !is_system_attr(var.varattno)
                && attrno_index(var.varattno).is_some_and(|attno| covered_columns.contains(&attno))
        }
        Expr::Param(_)
        | Expr::Const(_)
        | Expr::CaseTest(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => true,
        Expr::Aggref(_)
        | Expr::WindowFunc(_)
        | Expr::SetReturning(_)
        | Expr::SubLink(_)
        | Expr::SubPlan(_) => false,
        Expr::Op(op) => op
            .args
            .iter()
            .all(|arg| expr_uses_only_index_keys(arg, source_id, covered_columns)),
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .all(|arg| expr_uses_only_index_keys(arg, source_id, covered_columns)),
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_ref()
                .is_none_or(|arg| expr_uses_only_index_keys(arg, source_id, covered_columns))
                && case_expr.args.iter().all(|arm| {
                    expr_uses_only_index_keys(&arm.expr, source_id, covered_columns)
                        && expr_uses_only_index_keys(&arm.result, source_id, covered_columns)
                })
                && expr_uses_only_index_keys(&case_expr.defresult, source_id, covered_columns)
        }
        Expr::Func(func) => func
            .args
            .iter()
            .all(|arg| expr_uses_only_index_keys(arg, source_id, covered_columns)),
        Expr::SqlJsonQueryFunction(func) => func
            .child_exprs()
            .into_iter()
            .all(|arg| expr_uses_only_index_keys(arg, source_id, covered_columns)),
        Expr::ScalarArrayOp(saop) => {
            expr_uses_only_index_keys(&saop.left, source_id, covered_columns)
                && expr_uses_only_index_keys(&saop.right, source_id, covered_columns)
        }
        Expr::Xml(xml) => xml
            .child_exprs()
            .all(|child| expr_uses_only_index_keys(child, source_id, covered_columns)),
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => expr_uses_only_index_keys(inner, source_id, covered_columns),
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
            expr_uses_only_index_keys(expr, source_id, covered_columns)
                && expr_uses_only_index_keys(pattern, source_id, covered_columns)
                && escape
                    .as_ref()
                    .is_none_or(|expr| expr_uses_only_index_keys(expr, source_id, covered_columns))
        }
        Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
            expr_uses_only_index_keys(left, source_id, covered_columns)
                && expr_uses_only_index_keys(right, source_id, covered_columns)
        }
        Expr::ArrayLiteral { elements, .. } => elements
            .iter()
            .all(|element| expr_uses_only_index_keys(element, source_id, covered_columns)),
        Expr::Row { fields, .. } => fields
            .iter()
            .all(|(_, expr)| expr_uses_only_index_keys(expr, source_id, covered_columns)),
        Expr::FieldSelect { expr, .. } => {
            expr_uses_only_index_keys(expr, source_id, covered_columns)
        }
        Expr::Coalesce(left, right) => {
            expr_uses_only_index_keys(left, source_id, covered_columns)
                && expr_uses_only_index_keys(right, source_id, covered_columns)
        }
        Expr::ArraySubscript { array, subscripts } => {
            expr_uses_only_index_keys(array, source_id, covered_columns)
                && subscripts.iter().all(|subscript| {
                    expr_array_subscript_uses_only_index_keys(subscript, source_id, covered_columns)
                })
        }
    }
}

fn expr_array_subscript_uses_only_index_keys(
    subscript: &ExprArraySubscript,
    source_id: usize,
    covered_columns: &BTreeSet<usize>,
) -> bool {
    subscript
        .lower
        .as_ref()
        .is_none_or(|expr| expr_uses_only_index_keys(expr, source_id, covered_columns))
        && subscript
            .upper
            .as_ref()
            .is_none_or(|expr| expr_uses_only_index_keys(expr, source_id, covered_columns))
}

fn exec_param_sources(params: &[ExecParamSource]) -> BTreeSet<usize> {
    params.iter().map(|param| param.paramid).collect()
}

fn validate_executable_index_scan_keys(
    keys: &[IndexScanKey],
    plan_node: &str,
    field: &str,
    allowed_exec_params: &BTreeSet<usize>,
) {
    for key in keys {
        if let IndexScanKeyArgument::Runtime(expr) = &key.argument {
            validate_executable_expr(expr, plan_node, field, allowed_exec_params);
        }
    }
}

fn validate_executable_expr(
    expr: &Expr,
    plan_node: &str,
    field: &str,
    allowed_exec_params: &BTreeSet<usize>,
) {
    match expr {
        Expr::Var(var) if var.varlevelsup > 0 => {
            panic!("executable plan contains outer-level Var in {plan_node}.{field}: {var:?}")
        }
        Expr::Param(Param {
            paramkind: ParamKind::Exec,
            paramid,
            ..
        }) if !allowed_exec_params.contains(paramid) => panic!(
            "executable plan contains unbound PARAM_EXEC {paramid} in {plan_node}.{field}: {expr:?}"
        ),
        Expr::Aggref(aggref) => {
            panic!("executable plan contains unresolved Aggref in {plan_node}.{field}: {aggref:?}")
        }
        Expr::WindowFunc(window_func) => panic!(
            "executable plan contains unresolved WindowFunc in {plan_node}.{field}: {window_func:?}"
        ),
        Expr::SetReturning(srf) => panic!(
            "executable plan contains unresolved set-returning expression in {plan_node}.{field}: {srf:?}"
        ),
        Expr::SubLink(sublink) => panic!(
            "executable plan contains unresolved SubLink in {plan_node}.{field}: {sublink:?}"
        ),
        Expr::Op(op) => op
            .args
            .iter()
            .for_each(|arg| validate_executable_expr(arg, plan_node, field, allowed_exec_params)),
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .for_each(|arg| validate_executable_expr(arg, plan_node, field, allowed_exec_params)),
        Expr::Case(case_expr) => {
            if let Some(arg) = &case_expr.arg {
                validate_executable_expr(arg, plan_node, field, allowed_exec_params);
            }
            for arm in &case_expr.args {
                validate_executable_expr(&arm.expr, plan_node, field, allowed_exec_params);
                validate_executable_expr(&arm.result, plan_node, field, allowed_exec_params);
            }
            validate_executable_expr(&case_expr.defresult, plan_node, field, allowed_exec_params);
        }
        Expr::Func(func) => func
            .args
            .iter()
            .for_each(|arg| validate_executable_expr(arg, plan_node, field, allowed_exec_params)),
        Expr::SqlJsonQueryFunction(func) => {
            for child in func.child_exprs() {
                validate_executable_expr(child, plan_node, field, allowed_exec_params);
            }
        }
        Expr::SubPlan(subplan) => {
            if let Some(testexpr) = &subplan.testexpr {
                validate_executable_expr(testexpr, plan_node, field, allowed_exec_params);
            }
            subplan.args.iter().for_each(|arg| {
                validate_executable_expr(arg, plan_node, field, allowed_exec_params)
            });
        }
        Expr::ScalarArrayOp(saop) => {
            validate_executable_expr(&saop.left, plan_node, field, allowed_exec_params);
            validate_executable_expr(&saop.right, plan_node, field, allowed_exec_params);
        }
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => {
            validate_executable_expr(inner, plan_node, field, allowed_exec_params)
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
            validate_executable_expr(expr, plan_node, field, allowed_exec_params);
            validate_executable_expr(pattern, plan_node, field, allowed_exec_params);
            if let Some(escape) = escape {
                validate_executable_expr(escape, plan_node, field, allowed_exec_params);
            }
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            validate_executable_expr(inner, plan_node, field, allowed_exec_params);
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            validate_executable_expr(left, plan_node, field, allowed_exec_params);
            validate_executable_expr(right, plan_node, field, allowed_exec_params);
        }
        Expr::ArrayLiteral { elements, .. } => elements.iter().for_each(|element| {
            validate_executable_expr(element, plan_node, field, allowed_exec_params)
        }),
        Expr::Row { fields, .. } => fields.iter().for_each(|(_, expr)| {
            validate_executable_expr(expr, plan_node, field, allowed_exec_params)
        }),
        Expr::FieldSelect { expr, .. } => {
            validate_executable_expr(expr, plan_node, field, allowed_exec_params)
        }
        Expr::ArraySubscript { array, subscripts } => {
            validate_executable_expr(array, plan_node, field, allowed_exec_params);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    validate_executable_expr(lower, plan_node, field, allowed_exec_params);
                }
                if let Some(upper) = &subscript.upper {
                    validate_executable_expr(upper, plan_node, field, allowed_exec_params);
                }
            }
        }
        Expr::Xml(xml) => xml.child_exprs().for_each(|child| {
            validate_executable_expr(child, plan_node, field, allowed_exec_params)
        }),
        Expr::Var(_)
        | Expr::Param(_)
        | Expr::Const(_)
        | Expr::CaseTest(_)
        | Expr::Random
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
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
    allowed_exec_params: &BTreeSet<usize>,
) {
    use crate::include::nodes::primnodes::SetReturningCall;

    match call {
        SetReturningCall::GenerateSeries {
            start,
            stop,
            step,
            timezone,
            ..
        } => {
            validate_executable_expr(start, plan_node, field, allowed_exec_params);
            validate_executable_expr(stop, plan_node, field, allowed_exec_params);
            validate_executable_expr(step, plan_node, field, allowed_exec_params);
            if let Some(timezone) = timezone {
                validate_executable_expr(timezone, plan_node, field, allowed_exec_params);
            }
        }
        SetReturningCall::GenerateSubscripts {
            array,
            dimension,
            reverse,
            ..
        } => {
            validate_executable_expr(array, plan_node, field, allowed_exec_params);
            validate_executable_expr(dimension, plan_node, field, allowed_exec_params);
            if let Some(reverse) = reverse {
                validate_executable_expr(reverse, plan_node, field, allowed_exec_params);
            }
        }
        SetReturningCall::PartitionTree { relid, .. }
        | SetReturningCall::PartitionAncestors { relid, .. } => {
            validate_executable_expr(relid, plan_node, field, allowed_exec_params);
        }
        SetReturningCall::PgLockStatus { .. } => {}
        SetReturningCall::TxidSnapshotXip { arg, .. } => {
            validate_executable_expr(arg, plan_node, field, allowed_exec_params);
        }
        SetReturningCall::Unnest { args, .. }
        | SetReturningCall::JsonTableFunction { args, .. }
        | SetReturningCall::JsonRecordFunction { args, .. }
        | SetReturningCall::RegexTableFunction { args, .. }
        | SetReturningCall::StringTableFunction { args, .. }
        | SetReturningCall::TextSearchTableFunction { args, .. }
        | SetReturningCall::UserDefined { args, .. } => args
            .iter()
            .for_each(|arg| validate_executable_expr(arg, plan_node, field, allowed_exec_params)),
        SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_) => {
            set_returning_call_exprs(call).iter().for_each(|arg| {
                validate_executable_expr(arg, plan_node, field, allowed_exec_params)
            })
        }
    }
}

fn validate_agg_accum(
    accum: &crate::include::nodes::primnodes::AggAccum,
    plan_node: &str,
    field: &str,
    allowed_exec_params: &BTreeSet<usize>,
) {
    accum
        .args
        .iter()
        .for_each(|arg| validate_executable_expr(arg, plan_node, field, allowed_exec_params));
}

fn validate_executable_plan(plan: &Plan) {
    validate_executable_plan_with_params(plan, &BTreeSet::new());
}

fn validate_executable_plan_with_params(plan: &Plan, allowed_exec_params: &BTreeSet<usize>) {
    match plan {
        Plan::Result { .. } | Plan::SeqScan { .. } => {}
        Plan::Append { children, .. } | Plan::SetOp { children, .. } => {
            for child in children {
                validate_executable_plan_with_params(child, allowed_exec_params);
            }
        }
        Plan::MergeAppend {
            children, items, ..
        } => {
            for item in items {
                validate_executable_expr(&item.expr, "MergeAppend", "items", allowed_exec_params);
            }
            for child in children {
                validate_executable_plan_with_params(child, allowed_exec_params);
            }
        }
        Plan::Unique { input, .. } => {
            validate_executable_plan_with_params(input, allowed_exec_params)
        }
        Plan::IndexOnlyScan {
            keys,
            order_by_keys,
            ..
        } => {
            validate_executable_index_scan_keys(keys, "IndexOnlyScan", "keys", allowed_exec_params);
            validate_executable_index_scan_keys(
                order_by_keys,
                "IndexOnlyScan",
                "order_by_keys",
                allowed_exec_params,
            );
        }
        Plan::IndexScan {
            keys,
            order_by_keys,
            ..
        } => {
            validate_executable_index_scan_keys(keys, "IndexScan", "keys", allowed_exec_params);
            validate_executable_index_scan_keys(
                order_by_keys,
                "IndexScan",
                "order_by_keys",
                allowed_exec_params,
            );
        }
        Plan::BitmapIndexScan { keys, .. } => {
            validate_executable_index_scan_keys(
                keys,
                "BitmapIndexScan",
                "keys",
                allowed_exec_params,
            );
        }
        Plan::BitmapOr { children, .. } => {
            children.iter().for_each(validate_executable_plan);
        }
        Plan::BitmapHeapScan {
            bitmapqual,
            recheck_qual,
            filter_qual,
            ..
        } => {
            recheck_qual.iter().for_each(|expr| {
                validate_executable_expr(
                    expr,
                    "BitmapHeapScan",
                    "recheck_qual",
                    allowed_exec_params,
                )
            });
            filter_qual.iter().for_each(|expr| {
                validate_executable_expr(expr, "BitmapHeapScan", "filter_qual", allowed_exec_params)
            });
            validate_executable_plan_with_params(bitmapqual, allowed_exec_params);
        }
        Plan::Hash {
            input, hash_keys, ..
        } => {
            hash_keys.iter().for_each(|expr| {
                validate_executable_expr(expr, "Hash", "hash_keys", allowed_exec_params)
            });
            validate_executable_plan_with_params(input, allowed_exec_params);
        }
        Plan::Materialize { input, .. } => {
            validate_executable_plan_with_params(input, allowed_exec_params);
        }
        Plan::Memoize {
            input, cache_keys, ..
        } => {
            cache_keys.iter().for_each(|expr| {
                validate_executable_expr(expr, "Memoize", "cache_keys", allowed_exec_params)
            });
            validate_executable_plan_with_params(input, allowed_exec_params);
        }
        Plan::Gather { input, .. } => {
            validate_executable_plan_with_params(input, allowed_exec_params);
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
                validate_executable_expr(
                    &param.expr,
                    "NestedLoopJoin",
                    "nest_params",
                    allowed_exec_params,
                );
            }
            join_qual.iter().for_each(|expr| {
                validate_executable_expr(expr, "NestedLoopJoin", "join_qual", allowed_exec_params)
            });
            qual.iter().for_each(|expr| {
                validate_executable_expr(expr, "NestedLoopJoin", "qual", allowed_exec_params)
            });
            validate_executable_plan_with_params(left, allowed_exec_params);
            let mut right_allowed = allowed_exec_params.clone();
            right_allowed.extend(nest_params.iter().map(|param| param.paramid));
            validate_executable_plan_with_params(right, &right_allowed);
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
            hash_clauses.iter().for_each(|expr| {
                validate_executable_expr(expr, "HashJoin", "hash_clauses", allowed_exec_params)
            });
            hash_keys.iter().for_each(|expr| {
                validate_executable_expr(expr, "HashJoin", "hash_keys", allowed_exec_params)
            });
            join_qual.iter().for_each(|expr| {
                validate_executable_expr(expr, "HashJoin", "join_qual", allowed_exec_params)
            });
            qual.iter().for_each(|expr| {
                validate_executable_expr(expr, "HashJoin", "qual", allowed_exec_params)
            });
            validate_executable_plan_with_params(left, allowed_exec_params);
            validate_executable_plan_with_params(right, allowed_exec_params);
        }
        Plan::MergeJoin {
            left,
            right,
            merge_clauses,
            outer_merge_keys,
            inner_merge_keys,
            join_qual,
            qual,
            ..
        } => {
            merge_clauses.iter().for_each(|expr| {
                validate_executable_expr(expr, "MergeJoin", "merge_clauses", allowed_exec_params)
            });
            outer_merge_keys.iter().for_each(|expr| {
                validate_executable_expr(expr, "MergeJoin", "outer_merge_keys", allowed_exec_params)
            });
            inner_merge_keys.iter().for_each(|expr| {
                validate_executable_expr(expr, "MergeJoin", "inner_merge_keys", allowed_exec_params)
            });
            join_qual.iter().for_each(|expr| {
                validate_executable_expr(expr, "MergeJoin", "join_qual", allowed_exec_params)
            });
            qual.iter().for_each(|expr| {
                validate_executable_expr(expr, "MergeJoin", "qual", allowed_exec_params)
            });
            validate_executable_plan_with_params(left, allowed_exec_params);
            validate_executable_plan_with_params(right, allowed_exec_params);
        }
        Plan::Filter {
            input, predicate, ..
        } => {
            validate_executable_expr(predicate, "Filter", "predicate", allowed_exec_params);
            validate_executable_plan_with_params(input, allowed_exec_params);
        }
        Plan::OrderBy { input, items, .. } => {
            items.iter().for_each(|item| {
                validate_executable_expr(&item.expr, "OrderBy", "items", allowed_exec_params)
            });
            validate_executable_plan_with_params(input, allowed_exec_params);
        }
        Plan::IncrementalSort { input, items, .. } => {
            items.iter().for_each(|item| {
                validate_executable_expr(
                    &item.expr,
                    "IncrementalSort",
                    "items",
                    allowed_exec_params,
                )
            });
            validate_executable_plan_with_params(input, allowed_exec_params);
        }
        Plan::Limit { input, .. } | Plan::LockRows { input, .. } => {
            validate_executable_plan_with_params(input, allowed_exec_params);
        }
        Plan::Projection { input, targets, .. } => {
            targets.iter().for_each(|target| {
                validate_executable_expr(&target.expr, "Projection", "targets", allowed_exec_params)
            });
            validate_executable_plan_with_params(input, allowed_exec_params);
        }
        Plan::Aggregate {
            input,
            group_by,
            passthrough_exprs,
            accumulators,
            having,
            ..
        } => {
            group_by.iter().for_each(|expr| {
                validate_executable_expr(expr, "Aggregate", "group_by", allowed_exec_params)
            });
            passthrough_exprs.iter().for_each(|expr| {
                validate_executable_expr(
                    expr,
                    "Aggregate",
                    "passthrough_exprs",
                    allowed_exec_params,
                )
            });
            accumulators.iter().for_each(|accum| {
                validate_agg_accum(accum, "Aggregate", "accumulators", allowed_exec_params)
            });
            if let Some(having) = having {
                validate_executable_expr(having, "Aggregate", "having", allowed_exec_params);
            }
            validate_executable_plan_with_params(input, allowed_exec_params);
        }
        Plan::WindowAgg { input, clause, .. } => {
            for expr in &clause.spec.partition_by {
                validate_executable_expr(expr, "WindowAgg", "partition_by", allowed_exec_params);
            }
            for item in &clause.spec.order_by {
                validate_executable_expr(&item.expr, "WindowAgg", "order_by", allowed_exec_params);
            }
            for func in &clause.functions {
                for arg in &func.args {
                    validate_executable_expr(arg, "WindowAgg", "functions", allowed_exec_params);
                }
                if let WindowFuncKind::Aggregate(aggref) = &func.kind {
                    aggref.args.iter().for_each(|arg| {
                        validate_executable_expr(arg, "WindowAgg", "functions", allowed_exec_params)
                    });
                    if let Some(filter) = aggref.aggfilter.as_ref() {
                        validate_executable_expr(
                            filter,
                            "WindowAgg",
                            "functions",
                            allowed_exec_params,
                        );
                    }
                }
            }
            validate_executable_plan_with_params(input, allowed_exec_params);
        }
        Plan::FunctionScan { call, .. } => {
            validate_set_returning_call(call, "FunctionScan", "call", allowed_exec_params);
        }
        Plan::SubqueryScan { input, filter, .. } => {
            if let Some(filter) = filter {
                validate_executable_expr(filter, "SubqueryScan", "filter", allowed_exec_params);
            }
            validate_executable_plan_with_params(input, allowed_exec_params);
        }
        Plan::CteScan { cte_plan, .. } => {
            validate_executable_plan_with_params(cte_plan, allowed_exec_params);
        }
        Plan::WorkTableScan { .. } => {}
        Plan::RecursiveUnion {
            anchor, recursive, ..
        } => {
            validate_executable_plan_with_params(anchor, allowed_exec_params);
            validate_executable_plan_with_params(recursive, allowed_exec_params);
        }
        Plan::Values { rows, .. } => {
            for row in rows {
                row.iter().for_each(|expr| {
                    validate_executable_expr(expr, "Values", "rows", allowed_exec_params)
                });
            }
        }
        Plan::ProjectSet { input, targets, .. } => {
            for target in targets {
                match target {
                    crate::include::nodes::primnodes::ProjectSetTarget::Scalar(entry) => {
                        validate_executable_expr(
                            &entry.expr,
                            "ProjectSet",
                            "targets",
                            allowed_exec_params,
                        );
                    }
                    crate::include::nodes::primnodes::ProjectSetTarget::Set { call, .. } => {
                        validate_set_returning_call(
                            call,
                            "ProjectSet",
                            "targets",
                            allowed_exec_params,
                        );
                    }
                }
            }
            validate_executable_plan_with_params(input, allowed_exec_params);
        }
    }
}

fn validate_planner_expr(expr: &Expr, path_node: &str, field: &str) {
    match expr {
        Expr::Var(var) if is_executor_special_varno(var.varno) && var.varlevelsup == 0 => {
            panic!("planner path contains executor-only Var in {path_node}.{field}: {var:?}")
        }
        Expr::Param(Param {
            paramkind: ParamKind::Exec,
            ..
        }) => panic!("planner path contains PARAM_EXEC in {path_node}.{field}: {expr:?}"),
        Expr::WindowFunc(window_func) => {
            for arg in &window_func.args {
                validate_planner_expr(arg, path_node, field);
            }
            if let crate::include::nodes::primnodes::WindowFuncKind::Aggregate(aggref) =
                &window_func.kind
            {
                for arg in &aggref.args {
                    validate_planner_expr(arg, path_node, field);
                }
                if let Some(filter) = aggref.aggfilter.as_ref() {
                    validate_planner_expr(filter, path_node, field);
                }
            }
        }
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
        Expr::SqlJsonQueryFunction(func) => {
            for child in func.child_exprs() {
                validate_planner_expr(child, path_node, field);
            }
        }
        Expr::SetReturning(srf) => {
            for arg in set_returning_call_exprs(&srf.call) {
                validate_planner_expr(arg, path_node, field);
            }
        }
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
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => {
            validate_planner_expr(inner, path_node, field)
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
        Expr::Row { fields, .. } => fields
            .iter()
            .for_each(|(_, expr)| validate_planner_expr(expr, path_node, field)),
        Expr::FieldSelect { expr, .. } => validate_planner_expr(expr, path_node, field),
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
        Expr::Xml(xml) => xml
            .child_exprs()
            .for_each(|child| validate_planner_expr(child, path_node, field)),
        Expr::Var(_)
        | Expr::Const(_)
        | Expr::Aggref(_)
        | Expr::CaseTest(_)
        | Expr::Random
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => {}
    }
}

fn validate_planner_projection_expr(expr: &Expr, path_node: &str, field: &str) {
    if matches!(expr, Expr::Var(var) if is_executor_special_varno(var.varno)) {
        return;
    }
    validate_planner_expr(expr, path_node, field);
}

fn validate_planner_set_returning_call(
    call: &crate::include::nodes::primnodes::SetReturningCall,
    path_node: &str,
    field: &str,
) {
    use crate::include::nodes::primnodes::SetReturningCall;

    match call {
        SetReturningCall::GenerateSeries {
            start,
            stop,
            step,
            timezone,
            ..
        } => {
            validate_planner_expr(start, path_node, field);
            validate_planner_expr(stop, path_node, field);
            validate_planner_expr(step, path_node, field);
            if let Some(timezone) = timezone {
                validate_planner_expr(timezone, path_node, field);
            }
        }
        SetReturningCall::GenerateSubscripts {
            array,
            dimension,
            reverse,
            ..
        } => {
            validate_planner_expr(array, path_node, field);
            validate_planner_expr(dimension, path_node, field);
            if let Some(reverse) = reverse {
                validate_planner_expr(reverse, path_node, field);
            }
        }
        SetReturningCall::PartitionTree { relid, .. }
        | SetReturningCall::PartitionAncestors { relid, .. } => {
            validate_planner_expr(relid, path_node, field);
        }
        SetReturningCall::PgLockStatus { .. } => {}
        SetReturningCall::TxidSnapshotXip { arg, .. } => {
            validate_planner_expr(arg, path_node, field);
        }
        SetReturningCall::Unnest { args, .. }
        | SetReturningCall::JsonTableFunction { args, .. }
        | SetReturningCall::JsonRecordFunction { args, .. }
        | SetReturningCall::RegexTableFunction { args, .. }
        | SetReturningCall::StringTableFunction { args, .. }
        | SetReturningCall::TextSearchTableFunction { args, .. }
        | SetReturningCall::UserDefined { args, .. } => args
            .iter()
            .for_each(|arg| validate_planner_expr(arg, path_node, field)),
        SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_) => {
            set_returning_call_exprs(call)
                .iter()
                .for_each(|arg| validate_planner_expr(arg, path_node, field))
        }
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

fn validate_planner_index_scan_keys(keys: &[IndexScanKey], path_node: &str, field: &str) {
    for key in keys {
        if let IndexScanKeyArgument::Runtime(expr) = &key.argument {
            validate_planner_expr(expr, path_node, field);
        }
    }
}

fn validate_planner_path(path: &Path) {
    match path {
        Path::Result { .. } | Path::SeqScan { .. } => {}
        Path::IndexOnlyScan {
            keys,
            order_by_keys,
            ..
        } => {
            validate_planner_index_scan_keys(keys, "IndexOnlyScan", "keys");
            validate_planner_index_scan_keys(order_by_keys, "IndexOnlyScan", "order_by_keys");
        }
        Path::IndexScan {
            keys,
            order_by_keys,
            ..
        } => {
            validate_planner_index_scan_keys(keys, "IndexScan", "keys");
            validate_planner_index_scan_keys(order_by_keys, "IndexScan", "order_by_keys");
        }
        Path::BitmapIndexScan { keys, .. } => {
            validate_planner_index_scan_keys(keys, "BitmapIndexScan", "keys");
        }
        Path::BitmapOr { children, .. } => {
            for child in children {
                validate_planner_path(child);
            }
        }
        Path::Append { children, .. } | Path::SetOp { children, .. } => {
            for child in children {
                validate_planner_path(child);
            }
        }
        Path::MergeAppend {
            children, items, ..
        } => {
            for item in items {
                validate_planner_expr(&item.expr, "MergeAppend", "items");
            }
            for child in children {
                validate_planner_path(child);
            }
        }
        Path::Unique { input, .. } => validate_planner_path(input),
        Path::BitmapHeapScan {
            bitmapqual,
            recheck_qual,
            filter_qual,
            ..
        } => {
            for expr in recheck_qual {
                validate_planner_expr(expr, "BitmapHeapScan", "recheck_qual");
            }
            for expr in filter_qual {
                validate_planner_expr(expr, "BitmapHeapScan", "filter_qual");
            }
            validate_planner_path(bitmapqual);
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
        Path::MergeJoin {
            left,
            right,
            restrict_clauses,
            merge_clauses,
            outer_merge_keys,
            inner_merge_keys,
            ..
        } => {
            for restrict in restrict_clauses {
                validate_planner_expr(&restrict.clause, "MergeJoin", "restrict_clauses");
            }
            for restrict in merge_clauses {
                validate_planner_expr(&restrict.clause, "MergeJoin", "merge_clauses");
            }
            for expr in outer_merge_keys {
                validate_planner_expr(expr, "MergeJoin", "outer_merge_keys");
            }
            for expr in inner_merge_keys {
                validate_planner_expr(expr, "MergeJoin", "inner_merge_keys");
            }
            validate_planner_path(left);
            validate_planner_path(right);
        }
        Path::Projection { input, targets, .. } => {
            for target in targets {
                validate_planner_projection_expr(&target.expr, "Projection", "targets");
            }
            validate_planner_path(input);
        }
        Path::OrderBy { input, items, .. } => {
            for item in items {
                validate_planner_expr(&item.expr, "OrderBy", "items");
            }
            validate_planner_path(input);
        }
        Path::IncrementalSort { input, items, .. } => {
            for item in items {
                validate_planner_expr(&item.expr, "IncrementalSort", "items");
            }
            validate_planner_path(input);
        }
        Path::Limit { input, .. } | Path::LockRows { input, .. } => validate_planner_path(input),
        Path::Aggregate {
            input,
            group_by,
            passthrough_exprs,
            accumulators,
            having,
            ..
        } => {
            for expr in group_by {
                validate_planner_expr(expr, "Aggregate", "group_by");
            }
            for expr in passthrough_exprs {
                validate_planner_expr(expr, "Aggregate", "passthrough_exprs");
            }
            for accum in accumulators {
                validate_planner_agg_accum(accum, "Aggregate", "accumulators");
            }
            if let Some(having) = having {
                validate_planner_expr(having, "Aggregate", "having");
            }
            validate_planner_path(input);
        }
        Path::WindowAgg { input, clause, .. } => {
            for expr in &clause.spec.partition_by {
                validate_planner_expr(expr, "WindowAgg", "partition_by");
            }
            for item in &clause.spec.order_by {
                validate_planner_expr(&item.expr, "WindowAgg", "order_by");
            }
            for func in &clause.functions {
                for arg in &func.args {
                    validate_planner_expr(arg, "WindowAgg", "functions");
                }
                if let WindowFuncKind::Aggregate(aggref) = &func.kind {
                    aggref
                        .args
                        .iter()
                        .for_each(|arg| validate_planner_expr(arg, "WindowAgg", "functions"));
                    if let Some(filter) = aggref.aggfilter.as_ref() {
                        validate_planner_expr(filter, "WindowAgg", "functions");
                    }
                }
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
pub(super) fn validate_executable_plan_for_tests_with_params(
    plan: &Plan,
    params: &[ExecParamSource],
) {
    let allowed_params = exec_param_sources(params);
    validate_executable_plan_with_params(plan, &allowed_params);
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
    let input = *input;
    let input_tlist = build_path_tlist(ctx.root, &input);
    let predicate = fix_upper_expr_for_input(ctx.root, predicate, &input, &input_tlist);
    let predicate = lower_expr(
        ctx,
        predicate,
        LowerMode::Input {
            path: Some(&input),
            tlist: &input_tlist,
        },
    );
    match input {
        Path::SubqueryScan {
            rtindex,
            subroot,
            input,
            output_columns,
            pathkeys,
            ..
        } => set_subquery_scan_references(
            ctx,
            plan_info,
            rtindex,
            subroot,
            input,
            output_columns,
            Some(predicate),
            !pathkeys.is_empty(),
        ),
        input => Plan::Filter {
            plan_info,
            input: Box::new(set_plan_refs(ctx, input)),
            predicate,
        },
    }
}

fn set_append_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    source_id: usize,
    desc: crate::include::nodes::primnodes::RelationDesc,
    child_roots: Vec<Option<PlannerSubroot>>,
    children: Vec<Path>,
) -> Plan {
    assert!(
        child_roots.is_empty() || child_roots.len() == children.len(),
        "append child root count {} did not match child count {}",
        child_roots.len(),
        children.len()
    );
    let single_child_root_alias = (children.len() == 1)
        .then(|| append_source_alias(ctx, source_id))
        .flatten();
    Plan::Append {
        plan_info,
        source_id,
        desc,
        children: children
            .into_iter()
            .enumerate()
            .map(|(index, child)| {
                let child_root = child_roots
                    .get(index)
                    .and_then(Option::as_ref)
                    .map(PlannerSubroot::as_ref)
                    .or(ctx.root);
                let mut child_plan = recurse_with_root(ctx, child_root, child);
                if let Some(alias) = single_child_root_alias.as_deref() {
                    apply_single_append_scan_alias(&mut child_plan, alias);
                }
                child_plan
            })
            .collect(),
    }
}

fn append_source_alias(ctx: &SetRefsContext<'_>, source_id: usize) -> Option<String> {
    let root = ctx.root?;
    let rte = root.parse.rtable.get(source_id.checked_sub(1)?)?;
    match &rte.kind {
        RangeTblEntryKind::Relation { .. } => rte
            .alias
            .clone()
            .or_else(|| (!rte.eref.aliasname.is_empty()).then(|| rte.eref.aliasname.clone())),
        _ => None,
    }
}

fn apply_single_append_scan_alias(plan: &mut Plan, alias: &str) {
    match plan {
        Plan::SeqScan { relation_name, .. }
        | Plan::IndexOnlyScan { relation_name, .. }
        | Plan::IndexScan { relation_name, .. }
        | Plan::BitmapHeapScan { relation_name, .. } => {
            *relation_name = relation_name_with_alias(relation_name, alias);
        }
        Plan::Filter { input, .. }
        | Plan::Projection { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Unique { input, .. }
        | Plan::Hash { input, .. }
        | Plan::Aggregate { input, .. }
        | Plan::WindowAgg { input, .. } => apply_single_append_scan_alias(input, alias),
        _ => {}
    }
}

fn relation_name_with_alias(relation_name: &str, alias: &str) -> String {
    let base_name = relation_name
        .split_once(' ')
        .map(|(base_name, _)| base_name)
        .unwrap_or(relation_name);
    if base_name.eq_ignore_ascii_case(alias) {
        base_name.to_string()
    } else {
        format!("{base_name} {alias}")
    }
}

fn set_merge_append_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    source_id: usize,
    desc: crate::include::nodes::primnodes::RelationDesc,
    items: Vec<OrderByEntry>,
    children: Vec<Path>,
) -> Plan {
    let lowered_items = if let Some(first_child) = children.first() {
        let input_tlist = build_path_tlist(ctx.root, first_child);
        let mut lowered_items = Vec::with_capacity(items.len());
        for item in items {
            let item = lower_order_by_expr_for_input(ctx.root, item, first_child, &input_tlist);
            lowered_items.push(lower_order_by_entry(
                ctx,
                item,
                LowerMode::Input {
                    path: Some(first_child),
                    tlist: &input_tlist,
                },
            ));
        }
        lowered_items
    } else {
        Vec::new()
    };
    Plan::MergeAppend {
        plan_info,
        source_id,
        desc,
        items: lowered_items,
        children: children
            .into_iter()
            .map(|child| set_plan_refs(ctx, child))
            .collect(),
    }
}

#[allow(clippy::too_many_arguments)]
fn set_index_only_scan_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    source_id: usize,
    rel: crate::RelFileLocator,
    relation_name: String,
    relation_oid: u32,
    index_rel: crate::RelFileLocator,
    index_name: String,
    am_oid: u32,
    toast: Option<crate::include::nodes::primnodes::ToastRelationRef>,
    desc: crate::include::nodes::primnodes::RelationDesc,
    index_desc: crate::include::nodes::primnodes::RelationDesc,
    index_meta: crate::backend::utils::cache::relcache::IndexRelCacheEntry,
    keys: Vec<IndexScanKey>,
    order_by_keys: Vec<IndexScanKey>,
    direction: crate::include::access::relscan::ScanDirection,
) -> Plan {
    let keys = lower_index_scan_keys(ctx, keys, LowerMode::Scalar);
    let order_by_keys = lower_index_scan_keys(ctx, order_by_keys, LowerMode::Scalar);
    Plan::IndexOnlyScan {
        plan_info,
        source_id,
        rel,
        relation_name,
        relation_oid,
        index_rel,
        index_name,
        am_oid,
        toast,
        desc,
        index_desc,
        index_meta,
        keys,
        order_by_keys,
        direction,
    }
}

fn set_unique_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    key_indices: Vec<usize>,
    input: Box<Path>,
) -> Plan {
    Plan::Unique {
        plan_info,
        key_indices,
        input: Box::new(set_plan_refs(ctx, *input)),
    }
}

fn set_set_op_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    op: crate::include::nodes::parsenodes::SetOperator,
    strategy: crate::include::nodes::plannodes::SetOpStrategy,
    output_columns: Vec<QueryColumn>,
    child_roots: Vec<Option<PlannerSubroot>>,
    children: Vec<Path>,
) -> Plan {
    assert!(
        child_roots.is_empty() || child_roots.len() == children.len(),
        "set-op child root count {} did not match child count {}",
        child_roots.len(),
        children.len()
    );
    Plan::SetOp {
        plan_info,
        op,
        strategy,
        output_columns,
        children: children
            .into_iter()
            .enumerate()
            .map(|(index, child)| {
                let child_root = child_roots
                    .get(index)
                    .and_then(Option::as_ref)
                    .map(PlannerSubroot::as_ref)
                    .or(ctx.root);
                recurse_with_root(ctx, child_root, child)
            })
            .collect(),
    }
}

fn set_seq_scan_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    source_id: usize,
    rel: crate::RelFileLocator,
    relation_name: String,
    relation_oid: u32,
    relkind: char,
    relispopulated: bool,
    disabled: bool,
    toast: Option<crate::include::nodes::primnodes::ToastRelationRef>,
    tablesample: Option<TableSampleClause>,
    desc: crate::include::nodes::primnodes::RelationDesc,
) -> Plan {
    let tablesample = tablesample.map(|sample| TableSampleClause {
        method: sample.method,
        args: sample
            .args
            .into_iter()
            .map(|expr| lower_expr(ctx, expr, LowerMode::Scalar))
            .collect(),
        repeatable: sample
            .repeatable
            .map(|expr| lower_expr(ctx, expr, LowerMode::Scalar)),
    });
    Plan::SeqScan {
        plan_info,
        source_id,
        rel,
        relation_name,
        relation_oid,
        relkind,
        relispopulated,
        disabled,
        toast,
        tablesample,
        desc,
    }
}

#[allow(clippy::too_many_arguments)]
fn set_index_scan_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    source_id: usize,
    rel: crate::RelFileLocator,
    relation_name: String,
    relation_oid: u32,
    index_rel: crate::RelFileLocator,
    index_name: String,
    am_oid: u32,
    toast: Option<crate::include::nodes::primnodes::ToastRelationRef>,
    desc: crate::include::nodes::primnodes::RelationDesc,
    index_desc: crate::include::nodes::primnodes::RelationDesc,
    index_meta: crate::backend::utils::cache::relcache::IndexRelCacheEntry,
    keys: Vec<IndexScanKey>,
    order_by_keys: Vec<IndexScanKey>,
    direction: crate::include::access::relscan::ScanDirection,
    path_index_only: bool,
) -> Plan {
    let index_only = path_index_only
        || index_scan_can_use_index_only(ctx, source_id, am_oid, &desc, &index_meta);
    let keys = lower_index_scan_keys(ctx, keys, LowerMode::Scalar);
    let order_by_keys = lower_index_scan_keys(ctx, order_by_keys, LowerMode::Scalar);
    Plan::IndexScan {
        plan_info,
        source_id,
        rel,
        relation_name,
        relation_oid,
        index_rel,
        index_name,
        am_oid,
        toast,
        desc,
        index_desc,
        index_meta,
        keys,
        order_by_keys,
        direction,
        index_only,
    }
}

#[allow(clippy::too_many_arguments)]
fn set_bitmap_index_scan_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    source_id: usize,
    rel: crate::RelFileLocator,
    relation_oid: u32,
    index_rel: crate::RelFileLocator,
    index_name: String,
    am_oid: u32,
    desc: crate::include::nodes::primnodes::RelationDesc,
    index_desc: crate::include::nodes::primnodes::RelationDesc,
    index_meta: crate::backend::utils::cache::relcache::IndexRelCacheEntry,
    keys: Vec<IndexScanKey>,
    index_quals: Vec<Expr>,
) -> Plan {
    let keys = lower_index_scan_keys(ctx, keys, LowerMode::Scalar);
    let scan_tlist = build_base_scan_tlist(ctx.root, source_id, &desc);
    let index_quals = index_quals
        .into_iter()
        .map(|expr| {
            lower_expr(
                ctx,
                expr,
                LowerMode::Input {
                    path: None,
                    tlist: &scan_tlist,
                },
            )
        })
        .collect();
    Plan::BitmapIndexScan {
        plan_info,
        source_id,
        rel,
        relation_oid,
        index_rel,
        index_name,
        am_oid,
        desc,
        index_desc,
        index_meta,
        keys,
        index_quals,
    }
}

fn set_bitmap_or_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    children: Vec<Path>,
) -> Plan {
    Plan::BitmapOr {
        plan_info,
        children: children
            .into_iter()
            .map(|child| set_plan_refs(ctx, child))
            .collect(),
    }
}

#[allow(clippy::too_many_arguments)]
fn set_bitmap_heap_scan_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    source_id: usize,
    rel: crate::RelFileLocator,
    relation_name: String,
    relation_oid: u32,
    toast: Option<crate::include::nodes::primnodes::ToastRelationRef>,
    desc: crate::include::nodes::primnodes::RelationDesc,
    bitmapqual: Box<Path>,
    recheck_qual: Vec<Expr>,
    filter_qual: Vec<Expr>,
) -> Plan {
    let scan_tlist = build_base_scan_tlist(ctx.root, source_id, &desc);
    let recheck_qual = recheck_qual
        .into_iter()
        .map(|expr| {
            lower_expr(
                ctx,
                expr,
                LowerMode::Input {
                    path: None,
                    tlist: &scan_tlist,
                },
            )
        })
        .collect();
    let filter_qual = filter_qual
        .into_iter()
        .map(|expr| {
            lower_expr(
                ctx,
                expr,
                LowerMode::Input {
                    path: None,
                    tlist: &scan_tlist,
                },
            )
        })
        .collect();
    Plan::BitmapHeapScan {
        plan_info,
        source_id,
        rel,
        relation_name,
        relation_oid,
        toast,
        desc,
        bitmapqual: Box::new(set_plan_refs(ctx, *bitmapqual)),
        recheck_qual,
        filter_qual,
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
    let (mut right_plan, nest_params) = {
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
                    decrement_outer_expr_levels(param.expr),
                    &ctx.ext_params,
                    &mut param_consumed_parent_params,
                );
                let fixed_expr =
                    fix_upper_expr_for_input(ctx.root, rebased_expr.clone(), &left, &left_tlist);
                if expr_contains_local_semantic_var(&rebased_expr)
                    && (!expr_contains_local_semantic_var(&fixed_expr)
                        || expr_is_local_system_var(&fixed_expr))
                {
                    consumed_parent_params.extend(param_consumed_parent_params);
                    params.push(ExecParamSource {
                        paramid: param.paramid,
                        expr: lower_expr(
                            ctx,
                            fixed_expr,
                            LowerMode::Input {
                                path: Some(&left),
                                tlist: &left_tlist,
                            },
                        ),
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
    right_plan = maybe_wrap_nested_loop_inner_plan(ctx.root, kind, &nest_params, right_plan);
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

fn maybe_wrap_nested_loop_inner_plan(
    root: Option<&PlannerInfo>,
    kind: crate::include::nodes::primnodes::JoinType,
    nest_params: &[ExecParamSource],
    plan: Plan,
) -> Plan {
    if matches!(
        kind,
        crate::include::nodes::primnodes::JoinType::Inner
            | crate::include::nodes::primnodes::JoinType::Cross
    ) && nest_params.is_empty()
        && plan_is_plain_seq_scan(&plan)
    {
        return Plan::Materialize {
            plan_info: plan.plan_info(),
            input: Box::new(plan),
        };
    }

    let Some(root) = root else {
        return plan;
    };
    let cache_keys = plan_runtime_index_cache_keys(&plan);
    if matches!(
        kind,
        crate::include::nodes::primnodes::JoinType::Inner
            | crate::include::nodes::primnodes::JoinType::Left
    ) && root.parse.limit_count == Some(100)
        && !nest_params.is_empty()
        && !cache_keys.is_empty()
    {
        return Plan::Memoize {
            plan_info: plan.plan_info(),
            input: Box::new(plan),
            cache_keys,
        };
    }

    plan
}

fn plan_is_plain_seq_scan(plan: &Plan) -> bool {
    match plan {
        Plan::SeqScan { .. } => true,
        Plan::Filter { input, .. } | Plan::Projection { input, .. } => {
            plan_is_plain_seq_scan(input)
        }
        _ => false,
    }
}

fn plan_runtime_index_cache_keys(plan: &Plan) -> Vec<Expr> {
    let mut keys = Vec::new();
    collect_runtime_index_cache_keys(plan, &mut keys);
    keys
}

fn collect_runtime_index_cache_keys(plan: &Plan, keys: &mut Vec<Expr>) {
    match plan {
        Plan::IndexOnlyScan {
            keys: scan_keys,
            order_by_keys,
            ..
        }
        | Plan::IndexScan {
            keys: scan_keys,
            order_by_keys,
            ..
        } => {
            for key in scan_keys.iter().chain(order_by_keys.iter()) {
                if let IndexScanKeyArgument::Runtime(expr) = &key.argument
                    && !keys.contains(expr)
                {
                    keys.push(expr.clone());
                }
            }
        }
        Plan::Filter { input, .. }
        | Plan::Projection { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Unique { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Memoize { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::ProjectSet { input, .. }
        | Plan::CteScan {
            cte_plan: input, ..
        } => collect_runtime_index_cache_keys(input, keys),
        _ => {}
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
        .map(|expr| {
            lower_expr(
                ctx,
                expr,
                LowerMode::Input {
                    path: Some(&left),
                    tlist: &left_tlist,
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
                    path: Some(&right),
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

fn set_merge_join_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    left: Box<Path>,
    right: Box<Path>,
    kind: crate::include::nodes::primnodes::JoinType,
    merge_clauses: Vec<RestrictInfo>,
    outer_merge_keys: Vec<Expr>,
    inner_merge_keys: Vec<Expr>,
    restrict_clauses: Vec<RestrictInfo>,
) -> Plan {
    let left_tlist = build_path_tlist(ctx.root, &left);
    let right_tlist = build_path_tlist(ctx.root, &right);
    let merge_restrict_clauses = merge_clauses.clone();

    let outer_merge_keys = outer_merge_keys
        .into_iter()
        .map(|expr| fix_upper_expr_for_input(ctx.root, expr, &left, &left_tlist))
        .collect::<Vec<_>>();
    let inner_merge_keys = inner_merge_keys
        .into_iter()
        .map(|expr| fix_upper_expr_for_input(ctx.root, expr, &right, &right_tlist))
        .collect::<Vec<_>>();

    let outer_merge_keys = outer_merge_keys
        .into_iter()
        .map(|expr| {
            lower_expr(
                ctx,
                expr,
                LowerMode::Input {
                    path: Some(&left),
                    tlist: &left_tlist,
                },
            )
        })
        .collect::<Vec<_>>();
    let inner_merge_keys = inner_merge_keys
        .into_iter()
        .map(|expr| {
            lower_expr(
                ctx,
                expr,
                LowerMode::Input {
                    path: Some(&right),
                    tlist: &right_tlist,
                },
            )
        })
        .collect::<Vec<_>>();
    let lowered_merge_clauses = merge_clauses
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
    let (join_restrict_clauses, other_restrict_clauses) =
        split_join_restrict_clauses(kind, &restrict_clauses);
    let join_restrict_clauses = remove_hash_clauses(join_restrict_clauses, &merge_restrict_clauses);
    let join_qual = lower_join_clause_list(ctx, &join_restrict_clauses, &left, &right);
    let qual = lower_join_clause_list(ctx, other_restrict_clauses, &left, &right);

    let left_plan = set_plan_refs(ctx, *left);
    let right_plan = set_plan_refs(ctx, *right);

    Plan::MergeJoin {
        plan_info,
        left: Box::new(left_plan),
        right: Box::new(right_plan),
        kind,
        merge_clauses: lowered_merge_clauses,
        outer_merge_keys,
        inner_merge_keys,
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
                path: Some(&input),
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
    display_items: Vec<String>,
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
                    path: Some(&input),
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
        display_items,
    }
}

fn set_incremental_sort_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    input: Box<Path>,
    items: Vec<OrderByEntry>,
    presorted_count: usize,
    display_items: Vec<String>,
    presorted_display_items: Vec<String>,
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
                    path: Some(&input),
                    tlist: &input_tlist,
                },
            )
        })
        .collect();
    Plan::IncrementalSort {
        plan_info,
        input: Box::new(set_plan_refs(ctx, *input)),
        items: lowered_items,
        presorted_count,
        display_items,
        presorted_display_items,
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

fn set_lock_rows_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    input: Box<Path>,
    row_marks: Vec<QueryRowMark>,
) -> Plan {
    let root = ctx
        .root
        .expect("LockRows planning requires a planner root for row-mark metadata");
    Plan::LockRows {
        plan_info,
        input: Box::new(set_plan_refs(ctx, *input)),
        row_marks: row_marks
            .into_iter()
            .map(|row_mark| {
                let rte = root
                    .parse
                    .rtable
                    .get(row_mark.rtindex.saturating_sub(1))
                    .expect("row mark rtindex should resolve to an RTE");
                match &rte.kind {
                    RangeTblEntryKind::Relation {
                        rel, relation_oid, ..
                    } => PlanRowMark {
                        rtindex: row_mark.rtindex,
                        relation_name: rte
                            .alias
                            .clone()
                            .unwrap_or_else(|| format!("rt{}", row_mark.rtindex)),
                        relation_oid: *relation_oid,
                        rel: *rel,
                        strength: row_mark.strength,
                    },
                    _ => panic!("row mark must reference a base relation"),
                }
            })
            .collect(),
    }
}

fn set_aggregate_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    slot_id: usize,
    strategy: crate::include::nodes::plannodes::AggregateStrategy,
    phase: crate::include::nodes::plannodes::AggregatePhase,
    disabled: bool,
    input: Box<Path>,
    group_by: Vec<Expr>,
    passthrough_exprs: Vec<Expr>,
    accumulators: Vec<AggAccum>,
    semantic_accumulators: Option<Vec<AggAccum>>,
    having: Option<Expr>,
    output_columns: Vec<QueryColumn>,
) -> Plan {
    let input_tlist = build_path_tlist(ctx.root, &input);
    let aggregate_layout =
        aggregate_output_vars(slot_id, phase, &group_by, &passthrough_exprs, &accumulators);
    let aggregate_tlist = build_aggregate_tlist(
        ctx.root,
        slot_id,
        phase,
        &group_by,
        &passthrough_exprs,
        &accumulators,
        semantic_accumulators.as_deref(),
    );
    let semantic_group_by = group_by.clone();
    let semantic_passthrough_exprs = passthrough_exprs.clone();
    let semantic_output_names = (phase
        == crate::include::nodes::plannodes::AggregatePhase::Finalize
        || semantic_accumulators.is_some())
    .then(|| {
        aggregate_semantic_output_names(
            ctx.root,
            &semantic_group_by,
            &semantic_passthrough_exprs,
            semantic_accumulators.as_deref().unwrap_or(&accumulators),
        )
    });
    let root = ctx.root;
    let group_by = group_by
        .into_iter()
        .map(|expr| fix_upper_expr_for_input(root, expr, &input, &input_tlist))
        .map(|expr| {
            lower_expr(
                ctx,
                expr,
                LowerMode::Input {
                    path: Some(&input),
                    tlist: &input_tlist,
                },
            )
        })
        .collect();
    let passthrough_exprs = passthrough_exprs
        .into_iter()
        .map(|expr| fix_upper_expr_for_input(root, expr, &input, &input_tlist))
        .map(|expr| {
            lower_expr(
                ctx,
                expr,
                LowerMode::Input {
                    path: Some(&input),
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
                &semantic_passthrough_exprs,
                &aggregate_layout,
            ),
            None => lower_agg_output_expr(
                expr,
                &semantic_group_by,
                &semantic_passthrough_exprs,
                &aggregate_layout,
            ),
        };
        lower_expr(
            ctx,
            expr,
            LowerMode::Aggregate {
                group_by: &semantic_group_by,
                passthrough_exprs: &semantic_passthrough_exprs,
                layout: &aggregate_layout,
                tlist: &aggregate_tlist,
            },
        )
    });
    Plan::Aggregate {
        plan_info,
        strategy,
        phase,
        disabled,
        input: Box::new(set_plan_refs(ctx, *input)),
        group_by,
        passthrough_exprs,
        accumulators,
        semantic_accumulators,
        semantic_output_names,
        having,
        output_columns,
    }
}

fn lower_window_clause_for_input(
    ctx: &mut SetRefsContext<'_>,
    input: &Path,
    input_tlist: &IndexedTlist,
    clause: WindowClause,
) -> WindowClause {
    let root = ctx.root;
    let lower_expr_for_input = |ctx: &mut SetRefsContext<'_>, expr: Expr| {
        let lowered = lower_projection_expr_by_input_target(root, expr.clone(), input, input_tlist);
        let fixed = if expr_contains_local_semantic_var(&lowered) {
            fix_upper_expr_for_input(root, expr, input, input_tlist)
        } else {
            fix_upper_expr_for_input(root, lowered, input, input_tlist)
        };
        lower_expr(
            ctx,
            fixed,
            LowerMode::Input {
                path: Some(input),
                tlist: input_tlist,
            },
        )
    };
    WindowClause {
        spec: crate::include::nodes::primnodes::WindowSpec {
            partition_by: clause
                .spec
                .partition_by
                .into_iter()
                .map(|expr| lower_expr_for_input(ctx, expr))
                .collect(),
            order_by: clause
                .spec
                .order_by
                .into_iter()
                .map(|item| OrderByEntry {
                    expr: lower_expr_for_input(ctx, item.expr),
                    ..item
                })
                .collect(),
            frame: crate::include::nodes::primnodes::WindowFrame {
                mode: clause.spec.frame.mode,
                start_bound: match clause.spec.frame.start_bound {
                    crate::include::nodes::primnodes::WindowFrameBound::OffsetPreceding(offset) => {
                        let expr = lower_expr_for_input(ctx, offset.expr.clone());
                        crate::include::nodes::primnodes::WindowFrameBound::OffsetPreceding(
                            offset.with_expr(expr),
                        )
                    }
                    crate::include::nodes::primnodes::WindowFrameBound::OffsetFollowing(offset) => {
                        let expr = lower_expr_for_input(ctx, offset.expr.clone());
                        crate::include::nodes::primnodes::WindowFrameBound::OffsetFollowing(
                            offset.with_expr(expr),
                        )
                    }
                    other => other,
                },
                end_bound: match clause.spec.frame.end_bound {
                    crate::include::nodes::primnodes::WindowFrameBound::OffsetPreceding(offset) => {
                        let expr = lower_expr_for_input(ctx, offset.expr.clone());
                        crate::include::nodes::primnodes::WindowFrameBound::OffsetPreceding(
                            offset.with_expr(expr),
                        )
                    }
                    crate::include::nodes::primnodes::WindowFrameBound::OffsetFollowing(offset) => {
                        let expr = lower_expr_for_input(ctx, offset.expr.clone());
                        crate::include::nodes::primnodes::WindowFrameBound::OffsetFollowing(
                            offset.with_expr(expr),
                        )
                    }
                    other => other,
                },
                exclusion: clause.spec.frame.exclusion,
            },
        },
        functions: clause
            .functions
            .into_iter()
            .map(|func| crate::include::nodes::primnodes::WindowFuncExpr {
                kind: match func.kind {
                    WindowFuncKind::Aggregate(aggref) => WindowFuncKind::Aggregate(Aggref {
                        args: aggref
                            .args
                            .into_iter()
                            .map(|arg| lower_expr_for_input(ctx, arg))
                            .collect(),
                        aggorder: aggref
                            .aggorder
                            .into_iter()
                            .map(|item| OrderByEntry {
                                expr: lower_expr_for_input(ctx, item.expr),
                                ..item
                            })
                            .collect(),
                        aggfilter: aggref.aggfilter.map(|expr| lower_expr_for_input(ctx, expr)),
                        ..aggref
                    }),
                    WindowFuncKind::Builtin(kind) => WindowFuncKind::Builtin(kind),
                },
                args: func
                    .args
                    .into_iter()
                    .map(|arg| lower_expr_for_input(ctx, arg))
                    .collect(),
                ..func
            })
            .collect(),
    }
}

fn set_window_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    slot_id: usize,
    input: Box<Path>,
    clause: WindowClause,
    output_columns: Vec<QueryColumn>,
) -> Plan {
    let input_tlist = build_path_tlist(ctx.root, &input);
    let clause = lower_window_clause_for_input(ctx, &input, &input_tlist, clause);
    let _ = build_window_tlist(ctx.root, slot_id, &input, &clause, &output_columns);
    Plan::WindowAgg {
        plan_info,
        input: Box::new(set_plan_refs(ctx, *input)),
        clause,
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
    table_alias: Option<String>,
) -> Plan {
    Plan::FunctionScan {
        plan_info,
        call: lower_set_returning_call(ctx, call, LowerMode::Scalar),
        table_alias,
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
    rtindex: usize,
    subroot: PlannerSubroot,
    input: Box<Path>,
    output_columns: Vec<QueryColumn>,
    filter: Option<Expr>,
    force_display: bool,
) -> Plan {
    let input = recurse_with_root(ctx, Some(subroot.as_ref()), *input);
    if input.columns() == output_columns && filter.is_none() && !force_display {
        input
    } else {
        Plan::SubqueryScan {
            plan_info,
            input: Box::new(input),
            scan_name: subquery_scan_name(ctx, rtindex),
            filter,
            output_columns,
        }
    }
}

fn subquery_scan_name(ctx: &SetRefsContext<'_>, rtindex: usize) -> Option<String> {
    ctx.root
        .and_then(|root| root.parse.rtable.get(rtindex.saturating_sub(1)))
        .and_then(|rte| rte.alias.clone())
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
    recursive_references_worktable: bool,
    anchor_query: Box<crate::include::nodes::parsenodes::Query>,
    recursive_query: Box<crate::include::nodes::parsenodes::Query>,
    output_columns: Vec<QueryColumn>,
    anchor: Box<Path>,
    recursive: Box<Path>,
) -> Plan {
    let _ = anchor_query;
    let _ = recursive_query;
    Plan::RecursiveUnion {
        plan_info,
        worktable_id,
        distinct,
        recursive_references_worktable,
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
                    source_expr,
                    call,
                    sql_type,
                    column_index,
                } => crate::include::nodes::primnodes::ProjectSetTarget::Set {
                    name,
                    source_expr,
                    call: fix_set_returning_call_upper_exprs(ctx.root, call, &input, &input_tlist),
                    sql_type,
                    column_index,
                },
            };
            lower_project_set_target(
                ctx,
                target,
                LowerMode::Input {
                    path: Some(&input),
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
        Path::Result { plan_info, .. } => Plan::Result { plan_info },
        Path::Append {
            plan_info,
            source_id,
            desc,
            child_roots,
            children,
            ..
        } => set_append_references(ctx, plan_info, source_id, desc, child_roots, children),
        Path::MergeAppend {
            plan_info,
            source_id,
            desc,
            items,
            children,
            ..
        } => set_merge_append_references(ctx, plan_info, source_id, desc, items, children),
        Path::Unique {
            plan_info,
            key_indices,
            input,
            ..
        } => set_unique_references(ctx, plan_info, key_indices, input),
        Path::SetOp {
            plan_info,
            op,
            strategy,
            output_columns,
            child_roots,
            children,
            ..
        } => set_set_op_references(
            ctx,
            plan_info,
            op,
            strategy,
            output_columns,
            child_roots,
            children,
        ),
        Path::SeqScan {
            plan_info,
            source_id,
            rel,
            relation_name,
            relation_oid,
            relkind,
            relispopulated,
            disabled,
            toast,
            tablesample,
            desc,
            ..
        } => set_seq_scan_references(
            ctx,
            plan_info,
            source_id,
            rel,
            relation_name,
            relation_oid,
            relkind,
            relispopulated,
            disabled,
            toast,
            tablesample,
            desc,
        ),
        Path::IndexOnlyScan {
            plan_info,
            source_id,
            rel,
            relation_name,
            relation_oid,
            index_rel,
            index_name,
            am_oid,
            toast,
            desc,
            index_desc,
            index_meta,
            keys,
            order_by_keys,
            direction,
            pathkeys: _,
            ..
        } => set_index_only_scan_references(
            ctx,
            plan_info,
            source_id,
            rel,
            relation_name,
            relation_oid,
            index_rel,
            index_name,
            am_oid,
            toast,
            desc,
            index_desc,
            index_meta,
            keys,
            order_by_keys,
            direction,
        ),
        Path::IndexScan {
            plan_info,
            pathtarget: _,
            source_id,
            rel,
            relation_name,
            relation_oid,
            index_rel,
            index_name,
            am_oid,
            toast,
            desc,
            index_desc,
            index_meta,
            keys,
            order_by_keys,
            direction,
            index_only,
            pathkeys: _,
        } => set_index_scan_references(
            ctx,
            plan_info,
            source_id,
            rel,
            relation_name,
            relation_oid,
            index_rel,
            index_name,
            am_oid,
            toast,
            desc,
            index_desc,
            index_meta,
            keys,
            order_by_keys,
            direction,
            index_only,
        ),
        Path::BitmapIndexScan {
            plan_info,
            source_id,
            rel,
            relation_oid,
            index_rel,
            index_name,
            am_oid,
            desc,
            index_desc,
            index_meta,
            keys,
            index_quals,
            ..
        } => set_bitmap_index_scan_references(
            ctx,
            plan_info,
            source_id,
            rel,
            relation_oid,
            index_rel,
            index_name,
            am_oid,
            desc,
            index_desc,
            index_meta,
            keys,
            index_quals,
        ),
        Path::BitmapOr {
            plan_info,
            children,
            ..
        } => set_bitmap_or_references(ctx, plan_info, children),
        Path::BitmapHeapScan {
            plan_info,
            source_id,
            rel,
            relation_name,
            relation_oid,
            toast,
            desc,
            bitmapqual,
            recheck_qual,
            filter_qual,
            ..
        } => set_bitmap_heap_scan_references(
            ctx,
            plan_info,
            source_id,
            rel,
            relation_name,
            relation_oid,
            toast,
            desc,
            bitmapqual,
            recheck_qual,
            filter_qual,
        ),
        Path::Filter {
            plan_info,
            input,
            predicate,
            ..
        } => set_filter_references(ctx, plan_info, input, predicate),
        Path::NestedLoopJoin {
            plan_info,
            left,
            right,
            kind,
            restrict_clauses,
            ..
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
            ..
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
        Path::MergeJoin {
            plan_info,
            left,
            right,
            kind,
            merge_clauses,
            outer_merge_keys,
            inner_merge_keys,
            restrict_clauses,
            ..
        } => set_merge_join_references(
            ctx,
            plan_info,
            left,
            right,
            kind,
            merge_clauses,
            outer_merge_keys,
            inner_merge_keys,
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
            display_items,
            ..
        } => set_order_references(ctx, plan_info, input, items, display_items),
        Path::IncrementalSort {
            plan_info,
            input,
            items,
            presorted_count,
            display_items,
            presorted_display_items,
            ..
        } => set_incremental_sort_references(
            ctx,
            plan_info,
            input,
            items,
            presorted_count,
            display_items,
            presorted_display_items,
        ),
        Path::Limit {
            plan_info,
            input,
            limit,
            offset,
            ..
        } => set_limit_references(ctx, plan_info, input, limit, offset),
        Path::LockRows {
            plan_info,
            input,
            row_marks,
            ..
        } => set_lock_rows_references(ctx, plan_info, input, row_marks),
        Path::Aggregate {
            plan_info,
            slot_id,
            strategy,
            phase,
            disabled,
            input,
            group_by,
            passthrough_exprs,
            accumulators,
            semantic_accumulators,
            having,
            output_columns,
            ..
        } => set_aggregate_references(
            ctx,
            plan_info,
            slot_id,
            strategy,
            phase,
            disabled,
            input,
            group_by,
            passthrough_exprs,
            accumulators,
            semantic_accumulators,
            having,
            output_columns,
        ),
        Path::WindowAgg {
            plan_info,
            slot_id,
            input,
            clause,
            output_columns,
            ..
        } => set_window_references(ctx, plan_info, slot_id, input, clause, output_columns),
        Path::Values {
            plan_info,
            rows,
            output_columns,
            ..
        } => set_values_references(ctx, plan_info, rows, output_columns),
        Path::FunctionScan {
            plan_info,
            call,
            table_alias,
            ..
        } => set_function_scan_references(ctx, plan_info, call, table_alias),
        Path::SubqueryScan {
            plan_info,
            rtindex,
            subroot,
            input,
            output_columns,
            pathkeys,
            ..
        } => set_subquery_scan_references(
            ctx,
            plan_info,
            rtindex,
            subroot,
            input,
            output_columns,
            None,
            !pathkeys.is_empty(),
        ),
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
            recursive_references_worktable,
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
            anchor_root,
            recursive_root,
            recursive_references_worktable,
            anchor_query,
            recursive_query,
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
    _left: &Path,
    _right: &Path,
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
    if let (Expr::WindowFunc(left_window), Expr::WindowFunc(right_window)) = (left, right) {
        let same_kind = match (&left_window.kind, &right_window.kind) {
            (WindowFuncKind::Builtin(left_kind), WindowFuncKind::Builtin(right_kind)) => {
                left_kind == right_kind
            }
            (WindowFuncKind::Aggregate(left_agg), WindowFuncKind::Aggregate(right_agg)) => {
                left_agg.aggno == right_agg.aggno && left_agg.aggfnoid == right_agg.aggfnoid
            }
            _ => false,
        };
        if same_kind
            && left_window.winref == right_window.winref
            && left_window.winno == right_window.winno
            && left_window.result_type == right_window.result_type
        {
            return true;
        }
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
            aggorder: aggref
                .aggorder
                .into_iter()
                .map(|item| OrderByEntry {
                    expr: recurse(item.expr),
                    ..item
                })
                .collect(),
            aggfilter: aggref.aggfilter.map(recurse),
            ..*aggref
        })),
        Expr::WindowFunc(window_func) => Expr::WindowFunc(Box::new(WindowFuncExpr {
            kind: match window_func.kind {
                WindowFuncKind::Aggregate(aggref) => WindowFuncKind::Aggregate(Aggref {
                    args: aggref.args.into_iter().map(recurse).collect(),
                    aggorder: aggref
                        .aggorder
                        .into_iter()
                        .map(|item| OrderByEntry {
                            expr: recurse(item.expr),
                            ..item
                        })
                        .collect(),
                    aggfilter: aggref.aggfilter.map(recurse),
                    ..aggref
                }),
                WindowFuncKind::Builtin(kind) => WindowFuncKind::Builtin(kind),
            },
            args: window_func.args.into_iter().map(recurse).collect(),
            ..*window_func
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
            collation_oid,
        } => Expr::Like {
            expr: Box::new(recurse(*expr)),
            pattern: Box::new(recurse(*pattern)),
            escape: escape.map(|expr| Box::new(recurse(*expr))),
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
            expr: Box::new(recurse(*expr)),
            pattern: Box::new(recurse(*pattern)),
            escape: escape.map(|expr| Box::new(recurse(*expr))),
            negated,
            collation_oid,
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
            aggorder: aggref
                .aggorder
                .into_iter()
                .map(|item| OrderByEntry {
                    expr: fully_expand_output_expr(item.expr, path),
                    ..item
                })
                .collect(),
            aggfilter: aggref
                .aggfilter
                .map(|expr| fully_expand_output_expr(expr, path)),
            ..*aggref
        })),
        Expr::WindowFunc(window_func) => Expr::WindowFunc(Box::new(WindowFuncExpr {
            kind: match window_func.kind {
                WindowFuncKind::Aggregate(aggref) => WindowFuncKind::Aggregate(Aggref {
                    args: aggref
                        .args
                        .into_iter()
                        .map(|arg| fully_expand_output_expr(arg, path))
                        .collect(),
                    aggorder: aggref
                        .aggorder
                        .into_iter()
                        .map(|item| OrderByEntry {
                            expr: fully_expand_output_expr(item.expr, path),
                            ..item
                        })
                        .collect(),
                    aggfilter: aggref
                        .aggfilter
                        .map(|expr| fully_expand_output_expr(expr, path)),
                    ..aggref
                }),
                WindowFuncKind::Builtin(kind) => WindowFuncKind::Builtin(kind),
            },
            args: window_func
                .args
                .into_iter()
                .map(|arg| fully_expand_output_expr(arg, path))
                .collect(),
            ..*window_func
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
            collation_oid,
        } => Expr::Like {
            expr: Box::new(fully_expand_output_expr(*expr, path)),
            pattern: Box::new(fully_expand_output_expr(*pattern, path)),
            escape: escape.map(|expr| Box::new(fully_expand_output_expr(*expr, path))),
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
            expr: Box::new(fully_expand_output_expr(*expr, path)),
            pattern: Box::new(fully_expand_output_expr(*pattern, path)),
            escape: escape.map(|expr| Box::new(fully_expand_output_expr(*expr, path))),
            negated,
            collation_oid,
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
        Path::Filter { input, .. }
        | Path::OrderBy { input, .. }
        | Path::IncrementalSort { input, .. }
        | Path::Limit { input, .. }
        | Path::LockRows { input, .. } => expand_output_var(var, input),
        Path::NestedLoopJoin { left, right, .. }
        | Path::HashJoin { left, right, .. }
        | Path::MergeJoin { left, right, .. } => {
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
