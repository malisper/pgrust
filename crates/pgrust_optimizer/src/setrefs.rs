use super::inherit::append_translation;
use super::pathnodes::{
    PathMethods, aggregate_output_vars, expr_sql_type, lower_agg_output_expr, rte_slot_id,
    rte_slot_varno,
};
use super::plan::append_uncorrelated_planned_subquery;
use super::{expand_join_rte_vars, flatten_join_alias_vars, planner_with_param_base_and_config};
use pgrust_analyze::CatalogLookup;
use pgrust_analyze::{
    bind_index_predicate, flatten_and_conjuncts, predicate_implies_index_predicate,
};
use pgrust_catalog_data::{BTREE_AM_OID, builtin_aggregate_function_for_proc_oid};
use pgrust_nodes::CommandType;
use pgrust_nodes::datum::{RecordDescriptor, Value};
use pgrust_nodes::parsenodes::{
    Query, QueryRowMark, RangeTblEntryKind, SqlType, SqlTypeKind, SubqueryComparisonOp,
    TableSampleClause,
};
use pgrust_nodes::pathnodes::{Path, PathTarget, PlannerInfo, PlannerSubroot, RestrictInfo};
use pgrust_nodes::plannodes::{
    AggregatePhase, AggregateStrategy, ExecParamSource, IndexScanKey, IndexScanKeyArgument,
    PartitionPruneChildDomain, PartitionPrunePlan, Plan, PlanEstimate, PlanRowMark,
    TidRangeScanCond, TidScanCond, TidScanSource,
};
use pgrust_nodes::primnodes::{
    AggAccum, AggFunc, Aggref, BoolExpr, BoolExprType, BuiltinScalarFunction, Expr,
    ExprArraySubscript, FuncExpr, INNER_VAR, JoinType, OUTER_VAR, OpExpr, OpExprKind, OrderByEntry,
    Param, ParamKind, ProjectSetTarget, QueryColumn, RowsFromSource, SELF_ITEM_POINTER_ATTR_NO,
    ScalarArrayOpExpr, ScalarFunctionImpl, SetReturningCall, SubLinkType, SubPlan, TargetEntry,
    Var, WHOLE_ROW_ATTR_NO, WindowClause, WindowFuncExpr, WindowFuncKind, XmlExpr, attrno_index,
    is_executor_special_varno, is_rule_pseudo_varno, is_special_varno, is_system_attr,
    set_returning_call_exprs, user_attrno,
};
use std::collections::{BTreeMap, BTreeSet};

const MEMOIZE_CPU_TUPLE_COST: f64 = 0.01;
const MEMOIZE_CPU_OPERATOR_COST: f64 = 0.0025;
const MEMOIZE_DEFAULT_HASH_MEM_BYTES: f64 = 4096.0 * 1024.0 * 2.0;
const MEMOIZE_ENTRY_OVERHEAD_BYTES: f64 = 64.0;
const MEMOIZE_TUPLE_OVERHEAD_BYTES: f64 = 16.0;

#[derive(Clone, Debug)]
struct IndexedTlistEntry {
    index: usize,
    sql_type: pgrust_nodes::parsenodes::SqlType,
    collation_oid: Option<u32>,
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
                    entry.match_exprs.iter().any(|candidate| {
                        matches!(candidate, Expr::Var(candidate_var) if vars_match_for_setrefs(candidate_var, var))
                            || matches!(candidate, Expr::GroupingKey(grouping_key)
                                if matches!(grouping_key.expr.as_ref(), Expr::Var(candidate_var)
                                    if vars_match_for_setrefs(candidate_var, var)))
                    })
                })
                .or_else(|| {
                    self.entries.iter().find(|entry| {
                        entry.match_exprs.iter().any(|candidate| match candidate {
                            Expr::Var(candidate_var) => root.is_some_and(|root| {
                                exprs_match_for_setrefs(
                                    &flatten_join_alias_vars(
                                        root,
                                        Expr::Var(candidate_var.clone()),
                                    ),
                                    &flatten_join_alias_vars(root, expr.clone()),
                                )
                            }),
                            Expr::GroupingKey(grouping_key) => {
                                exprs_match_for_setrefs(grouping_key.expr.as_ref(), expr)
                                    || root.is_some_and(|root| {
                                        exprs_match_for_setrefs(
                                            &flatten_join_alias_vars(
                                                root,
                                                *grouping_key.expr.clone(),
                                            ),
                                            &flatten_join_alias_vars(root, expr.clone()),
                                        )
                                    })
                            }
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ParallelSafety {
    Safe,
    Restricted,
    Unsafe,
}

impl ParallelSafety {
    fn combine(self, other: Self) -> Self {
        self.max(other)
    }

    fn is_unsafe(self) -> bool {
        matches!(self, ParallelSafety::Unsafe)
    }

    fn is_worker_safe(self) -> bool {
        matches!(self, ParallelSafety::Safe)
    }
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
    let plan = maybe_wrap_parallel_gather(root, catalog, set_plan_refs(&mut ctx, path));
    let allowed_params = exec_param_sources(&ctx.ext_params);
    validate_executable_plan_with_params(&plan, &allowed_params);
    (plan, ctx.ext_params, ctx.next_param_id)
}

fn maybe_wrap_parallel_gather(root: &PlannerInfo, catalog: &dyn CatalogLookup, plan: Plan) -> Plan {
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
    if root.config.max_parallel_workers_per_gather == 0
        || !parallel_query_requested(root)
        || matches!(plan, Plan::Gather { .. } | Plan::GatherMerge { .. })
    {
        return plan;
    }
    if let Some(plan) = maybe_parallelize_upper_node(root, catalog, &plan) {
        return plan;
    }
    if !plan_parallel_safe(&plan, catalog) {
        return plan;
    }
    if let Some(plan) = maybe_parallel_plain_aggregate(root, catalog, &plan) {
        return plan;
    }
    if !plan_has_complete_aggregate(&plan)
        && parallel_worker_safe_plan_shape(root, catalog, &plan)
        && plan_contains_seqscan(&plan)
    {
        let mut input = plan;
        mark_parallel_seq_scans(&mut input);
        if plan_contains_parallel_aware_seqscan(&input) {
            return Plan::Gather {
                plan_info: input.plan_info(),
                input: Box::new(input),
                workers_planned: root.config.max_parallel_workers_per_gather,
                single_copy: false,
            };
        }
        return input;
    }
    plan
}

fn maybe_parallelize_upper_node(
    root: &PlannerInfo,
    catalog: &dyn CatalogLookup,
    plan: &Plan,
) -> Option<Plan> {
    match plan {
        Plan::Projection {
            plan_info,
            input,
            targets,
        } if targets
            .iter()
            .all(|target| !expr_contains_parallel_global_unsafe(&target.expr)) =>
        {
            let input_plan = maybe_wrap_parallel_gather(root, catalog, (**input).clone());
            (input_plan != **input).then(|| Plan::Projection {
                plan_info: *plan_info,
                input: Box::new(input_plan),
                targets: targets.clone(),
            })
        }
        Plan::OrderBy {
            plan_info,
            input,
            items,
            display_items,
        } if items
            .iter()
            .all(|item| !expr_contains_parallel_global_unsafe(&item.expr)) =>
        {
            if items
                .iter()
                .all(|item| expr_parallel_safety(&item.expr, Some(catalog)).is_worker_safe())
                && plan_parallel_safe(input, catalog)
                && parallel_worker_safe_plan_shape(root, catalog, input)
                && plan_contains_seqscan(input)
            {
                let mut partial_input = (**input).clone();
                mark_parallel_seq_scans(&mut partial_input);
                if plan_contains_parallel_aware_seqscan(&partial_input) {
                    let worker_sort = Plan::OrderBy {
                        plan_info: *plan_info,
                        input: Box::new(partial_input),
                        items: items.clone(),
                        display_items: display_items.clone(),
                    };
                    return Some(Plan::GatherMerge {
                        plan_info: *plan_info,
                        input: Box::new(worker_sort),
                        workers_planned: root.config.max_parallel_workers_per_gather,
                        items: items.clone(),
                        display_items: display_items.clone(),
                    });
                }
            }
            let input_plan = maybe_wrap_parallel_gather(root, catalog, (**input).clone());
            (input_plan != **input).then(|| Plan::OrderBy {
                plan_info: *plan_info,
                input: Box::new(input_plan),
                items: items.clone(),
                display_items: display_items.clone(),
            })
        }
        Plan::Limit {
            plan_info,
            input,
            limit,
            offset,
        } if limit
            .as_ref()
            .is_none_or(|expr| !expr_contains_parallel_global_unsafe(expr))
            && offset
                .as_ref()
                .is_none_or(|expr| !expr_contains_parallel_global_unsafe(expr)) =>
        {
            let input_plan = maybe_parallelize_upper_node(root, catalog, input)
                .unwrap_or_else(|| maybe_wrap_parallel_gather(root, catalog, (**input).clone()));
            (input_plan != **input).then(|| Plan::Limit {
                plan_info: *plan_info,
                input: Box::new(input_plan),
                limit: limit.clone(),
                offset: offset.clone(),
            })
        }
        _ => None,
    }
}

fn parallel_query_requested(root: &PlannerInfo) -> bool {
    root.config.force_parallel_gather
        || root.config.min_parallel_table_scan_size == 0
        || root.config.parallel_setup_cost.as_f64() == 0.0
        || root.config.parallel_tuple_cost.as_f64() == 0.0
}

fn maybe_parallel_plain_aggregate(
    root: &PlannerInfo,
    catalog: &dyn CatalogLookup,
    plan: &Plan,
) -> Option<Plan> {
    if let Plan::Projection {
        plan_info,
        input,
        targets,
    } = plan
        && let Some(input) = maybe_parallel_plain_aggregate(root, catalog, input)
    {
        return Some(Plan::Projection {
            plan_info: *plan_info,
            input: Box::new(input),
            targets: targets.clone(),
        });
    }

    let Plan::Aggregate {
        plan_info,
        strategy,
        phase,
        disabled,
        input,
        group_by,
        group_by_refs,
        grouping_sets,
        passthrough_exprs,
        accumulators,
        semantic_accumulators,
        semantic_output_names,
        having,
        output_columns,
    } = plan
    else {
        return None;
    };
    if *phase != AggregatePhase::Complete
        || *disabled
        || !grouping_sets.is_empty()
        || !passthrough_exprs.is_empty()
        || having.is_some()
        || semantic_accumulators.is_some()
        || semantic_output_names.is_some()
        || aggregate_expressions_contain_parallel_unsafe(
            group_by,
            passthrough_exprs,
            accumulators,
            having.as_ref(),
            Some(catalog),
        )
    {
        return None;
    }

    let Some(partial_input) = parallelized_worker_input(root, catalog, input) else {
        return None;
    };

    if group_by.is_empty() != group_by_refs.is_empty()
        || !matches!(
            *strategy,
            AggregateStrategy::Plain | AggregateStrategy::Hashed | AggregateStrategy::Sorted
        )
        || !all_aggregates_are_partial_safe(accumulators, catalog)
        || !aggregate_expressions_worker_safe(
            group_by,
            passthrough_exprs,
            accumulators,
            having.as_ref(),
            Some(catalog),
        )
    {
        if matches!(
            *strategy,
            AggregateStrategy::Plain | AggregateStrategy::Hashed
        ) {
            return Some(Plan::Aggregate {
                plan_info: *plan_info,
                strategy: *strategy,
                phase: *phase,
                disabled: *disabled,
                input: Box::new(Plan::Gather {
                    plan_info: partial_input.plan_info(),
                    input: Box::new(partial_input),
                    workers_planned: root.config.max_parallel_workers_per_gather,
                    single_copy: false,
                }),
                group_by: group_by.clone(),
                group_by_refs: group_by_refs.clone(),
                grouping_sets: grouping_sets.clone(),
                passthrough_exprs: passthrough_exprs.clone(),
                accumulators: accumulators.clone(),
                semantic_accumulators: semantic_accumulators.clone(),
                semantic_output_names: semantic_output_names.clone(),
                having: having.clone(),
                output_columns: output_columns.clone(),
            });
        }
        return None;
    }

    let mut partial_output_columns = output_columns.clone();
    for column in partial_output_columns
        .iter_mut()
        .skip(group_by.len() + passthrough_exprs.len())
    {
        column.sql_type =
            pgrust_nodes::parsenodes::SqlType::new(pgrust_nodes::parsenodes::SqlTypeKind::Record);
        column.wire_type_oid = None;
    }
    let partial = Plan::Aggregate {
        plan_info: *plan_info,
        strategy: *strategy,
        phase: AggregatePhase::Partial,
        disabled: false,
        input: Box::new(partial_input),
        group_by: group_by.clone(),
        group_by_refs: group_by_refs.clone(),
        grouping_sets: Vec::new(),
        passthrough_exprs: Vec::new(),
        accumulators: accumulators.clone(),
        semantic_accumulators: None,
        semantic_output_names: None,
        having: None,
        output_columns: partial_output_columns,
    };
    let gather = if *strategy == AggregateStrategy::Sorted && !group_by.is_empty() {
        Plan::GatherMerge {
            plan_info: partial.plan_info(),
            input: Box::new(partial),
            workers_planned: root.config.max_parallel_workers_per_gather,
            items: gather_merge_items_for_partial_group_by(group_by),
            display_items: Vec::new(),
        }
    } else {
        Plan::Gather {
            plan_info: partial.plan_info(),
            input: Box::new(partial),
            workers_planned: root.config.max_parallel_workers_per_gather,
            single_copy: false,
        }
    };
    Some(Plan::Aggregate {
        plan_info: *plan_info,
        strategy: *strategy,
        phase: AggregatePhase::Finalize,
        disabled: false,
        input: Box::new(gather),
        group_by: final_group_by_for_partial(group_by),
        group_by_refs: group_by_refs.clone(),
        grouping_sets: Vec::new(),
        passthrough_exprs: Vec::new(),
        accumulators: final_accumulators_for_partial(accumulators),
        semantic_accumulators: Some(accumulators.clone()),
        semantic_output_names: None,
        having: None,
        output_columns: output_columns.clone(),
    })
}

fn parallelized_worker_input(
    root: &PlannerInfo,
    catalog: &dyn CatalogLookup,
    input: &Plan,
) -> Option<Plan> {
    if !plan_contains_seqscan(input) || !parallel_worker_safe_plan_shape(root, catalog, input) {
        return None;
    }
    let mut partial_input = input.clone();
    mark_parallel_seq_scans(&mut partial_input);
    plan_contains_parallel_aware_seqscan(&partial_input).then_some(partial_input)
}

fn gather_merge_items_for_partial_group_by(group_by: &[Expr]) -> Vec<OrderByEntry> {
    group_by
        .iter()
        .enumerate()
        .map(|(index, expr)| OrderByEntry {
            expr: slot_var_with_collation(
                0,
                user_attrno(index),
                expr_sql_type(expr),
                expr_collation_oid(expr),
            ),
            ressortgroupref: index + 1,
            descending: false,
            nulls_first: None,
            collation_oid: expr_collation_oid(expr),
        })
        .collect()
}

fn final_group_by_for_partial(group_by: &[Expr]) -> Vec<Expr> {
    group_by
        .iter()
        .enumerate()
        .map(|(index, expr)| {
            slot_var_with_collation(
                0,
                user_attrno(index),
                expr_sql_type(expr),
                expr_collation_oid(expr),
            )
        })
        .collect()
}

fn plan_has_complete_aggregate(plan: &Plan) -> bool {
    match plan {
        Plan::Aggregate {
            phase: AggregatePhase::Complete,
            ..
        } => true,
        Plan::Append { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::BitmapAnd { children, .. }
        | Plan::SetOp { children, .. } => children.iter().any(plan_has_complete_aggregate),
        Plan::Unique { input, .. }
        | Plan::Hash { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Memoize { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. }
        | Plan::Filter { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Projection { input, .. }
        | Plan::Aggregate { input, .. }
        | Plan::WindowAgg { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::ProjectSet { input, .. } => plan_has_complete_aggregate(input),
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::MergeJoin { left, right, .. } => {
            plan_has_complete_aggregate(left) || plan_has_complete_aggregate(right)
        }
        Plan::BitmapHeapScan { bitmapqual, .. } => plan_has_complete_aggregate(bitmapqual),
        Plan::CteScan { cte_plan, .. } => plan_has_complete_aggregate(cte_plan),
        Plan::RecursiveUnion {
            anchor, recursive, ..
        } => plan_has_complete_aggregate(anchor) || plan_has_complete_aggregate(recursive),
        Plan::Result { .. }
        | Plan::SeqScan { .. }
        | Plan::TidScan { .. }
        | Plan::TidRangeScan { .. }
        | Plan::IndexOnlyScan { .. }
        | Plan::IndexScan { .. }
        | Plan::BitmapIndexScan { .. }
        | Plan::FunctionScan { .. }
        | Plan::WorkTableScan { .. }
        | Plan::Values { .. } => false,
    }
}

fn parallel_worker_safe_plan_shape(
    root: &PlannerInfo,
    catalog: &dyn CatalogLookup,
    plan: &Plan,
) -> bool {
    match plan {
        Plan::SeqScan {
            relkind,
            tablesample,
            ..
        } => tablesample.is_none() && matches!(*relkind, 'r' | 'p' | 'm'),
        Plan::Filter {
            input, predicate, ..
        } => {
            expr_parallel_safety(predicate, Some(catalog)).is_worker_safe()
                && parallel_worker_safe_plan_shape(root, catalog, input)
        }
        Plan::Projection { input, targets, .. } => {
            targets
                .iter()
                .all(|target| expr_parallel_safety(&target.expr, Some(catalog)).is_worker_safe())
                && parallel_worker_safe_plan_shape(root, catalog, input)
        }
        Plan::SubqueryScan { input, filter, .. } => {
            filter
                .as_ref()
                .is_none_or(|expr| expr_parallel_safety(expr, Some(catalog)).is_worker_safe())
                && parallel_worker_safe_plan_shape(root, catalog, input)
        }
        Plan::OrderBy { input, items, .. } | Plan::IncrementalSort { input, items, .. } => {
            items
                .iter()
                .all(|item| expr_parallel_safety(&item.expr, Some(catalog)).is_worker_safe())
                && parallel_worker_safe_plan_shape(root, catalog, input)
        }
        Plan::Append { children, .. } if root.config.enable_parallel_append => children
            .iter()
            .all(|child| parallel_worker_safe_plan_shape(root, catalog, child)),
        Plan::Materialize { input, .. } => parallel_worker_safe_plan_shape(root, catalog, input),
        Plan::Hash {
            input, hash_keys, ..
        } => {
            hash_keys
                .iter()
                .all(|expr| expr_parallel_safety(expr, Some(catalog)).is_worker_safe())
                && parallel_worker_safe_plan_shape(root, catalog, input)
        }
        Plan::NestedLoopJoin {
            left,
            right,
            nest_params,
            join_qual,
            qual,
            ..
        } => {
            nest_params.is_empty()
                && join_qual
                    .iter()
                    .chain(qual.iter())
                    .all(|expr| expr_parallel_safety(expr, Some(catalog)).is_worker_safe())
                && parallel_worker_safe_plan_shape(root, catalog, left)
                && parallel_worker_safe_plan_shape(root, catalog, right)
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
                .chain(hash_keys.iter())
                .chain(join_qual.iter())
                .chain(qual.iter())
                .all(|expr| expr_parallel_safety(expr, Some(catalog)).is_worker_safe())
                && parallel_worker_safe_plan_shape(root, catalog, left)
                && parallel_worker_safe_plan_shape(root, catalog, right)
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
            merge_clauses
                .iter()
                .chain(outer_merge_keys.iter())
                .chain(inner_merge_keys.iter())
                .chain(join_qual.iter())
                .chain(qual.iter())
                .all(|expr| expr_parallel_safety(expr, Some(catalog)).is_worker_safe())
                && parallel_worker_safe_plan_shape(root, catalog, left)
                && parallel_worker_safe_plan_shape(root, catalog, right)
        }
        Plan::IndexOnlyScan { am_oid, .. }
        | Plan::IndexScan { am_oid, .. }
        | Plan::BitmapIndexScan { am_oid, .. } => *am_oid == BTREE_AM_OID,
        Plan::BitmapHeapScan { bitmapqual, .. } => {
            parallel_worker_safe_plan_shape(root, catalog, bitmapqual)
        }
        Plan::Result { .. }
        | Plan::Append { .. }
        | Plan::MergeAppend { .. }
        | Plan::Unique { .. }
        | Plan::TidScan { .. }
        | Plan::TidRangeScan { .. }
        | Plan::BitmapOr { .. }
        | Plan::BitmapAnd { .. }
        | Plan::Memoize { .. }
        | Plan::Gather { .. }
        | Plan::GatherMerge { .. }
        | Plan::Limit { .. }
        | Plan::LockRows { .. }
        | Plan::Aggregate { .. }
        | Plan::WindowAgg { .. }
        | Plan::FunctionScan { .. }
        | Plan::CteScan { .. }
        | Plan::WorkTableScan { .. }
        | Plan::RecursiveUnion { .. }
        | Plan::SetOp { .. }
        | Plan::ProjectSet { .. } => false,
        Plan::Values { rows, .. } => rows
            .iter()
            .flatten()
            .all(|expr| expr_parallel_safety(expr, Some(catalog)).is_worker_safe()),
    }
}

fn aggregate_is_partial_safe(accum: &AggAccum, catalog: &dyn CatalogLookup) -> bool {
    if accum.distinct || !accum.direct_args.is_empty() || !accum.order_by.is_empty() {
        return false;
    }
    let Some(aggregate) = catalog.aggregate_by_fnoid(accum.aggfnoid) else {
        return false;
    };
    aggregate.aggkind == 'n'
        && aggregate.aggcombinefn != 0
        && aggregate_partial_state_supported(accum)
}

fn aggregate_partial_state_supported(accum: &AggAccum) -> bool {
    match builtin_aggregate_function_for_proc_oid(accum.aggfnoid) {
        Some(AggFunc::Count | AggFunc::Sum | AggFunc::Avg | AggFunc::Min | AggFunc::Max) => true,
        Some(AggFunc::VarPop | AggFunc::VarSamp | AggFunc::StddevPop | AggFunc::StddevSamp) => {
            matches!(
                accum.sql_type.kind,
                SqlTypeKind::Float4 | SqlTypeKind::Float8
            )
        }
        Some(AggFunc::RegrCount) => true,
        Some(
            AggFunc::RegrSxx
            | AggFunc::RegrSyy
            | AggFunc::RegrSxy
            | AggFunc::RegrAvgX
            | AggFunc::RegrAvgY
            | AggFunc::RegrR2
            | AggFunc::RegrSlope
            | AggFunc::RegrIntercept
            | AggFunc::CovarPop
            | AggFunc::CovarSamp
            | AggFunc::Corr,
        ) => false,
        Some(_) => false,
        None => true,
    }
}

fn all_aggregates_are_partial_safe(accumulators: &[AggAccum], catalog: &dyn CatalogLookup) -> bool {
    accumulators
        .iter()
        .all(|accum| aggregate_is_partial_safe(accum, catalog))
}

fn aggregate_expressions_worker_safe(
    group_by: &[Expr],
    passthrough_exprs: &[Expr],
    accumulators: &[AggAccum],
    having: Option<&Expr>,
    catalog: Option<&dyn CatalogLookup>,
) -> bool {
    group_by
        .iter()
        .chain(passthrough_exprs.iter())
        .all(|expr| expr_parallel_safety(expr, catalog).is_worker_safe())
        && accumulators
            .iter()
            .all(|accum| accumulator_worker_safe(accum, catalog))
        && having.is_none_or(|expr| expr_parallel_safety(expr, catalog).is_worker_safe())
}

fn aggregate_expressions_contain_parallel_unsafe(
    group_by: &[Expr],
    passthrough_exprs: &[Expr],
    accumulators: &[AggAccum],
    having: Option<&Expr>,
    catalog: Option<&dyn CatalogLookup>,
) -> bool {
    group_by
        .iter()
        .chain(passthrough_exprs.iter())
        .any(|expr| expr_parallel_safety(expr, catalog).is_unsafe())
        || accumulators
            .iter()
            .any(|accum| accumulator_contains_parallel_unsafe(accum, catalog))
        || having.is_some_and(|expr| expr_parallel_safety(expr, catalog).is_unsafe())
}

fn final_accumulators_for_partial(accumulators: &[AggAccum]) -> Vec<AggAccum> {
    accumulators
        .iter()
        .cloned()
        .map(|mut accum| {
            accum.distinct = false;
            accum.direct_args.clear();
            accum.args.clear();
            accum.order_by.clear();
            accum.filter = None;
            accum.presorted = false;
            accum
        })
        .collect()
}

fn mark_parallel_seq_scans(plan: &mut Plan) {
    let mut next_scan_id = 1usize;
    mark_parallel_seq_scans_inner(plan, &mut next_scan_id);
}

fn mark_parallel_seq_scans_inner(plan: &mut Plan, next_scan_id: &mut usize) {
    match plan {
        Plan::SeqScan {
            relkind,
            tablesample,
            parallel_aware,
            parallel_scan_id,
            ..
        } if tablesample.is_none() && matches!(*relkind, 'r' | 'p' | 'm') => {
            *parallel_aware = true;
            *parallel_scan_id = Some(*next_scan_id);
            *next_scan_id += 1;
        }
        Plan::IndexOnlyScan {
            am_oid,
            parallel_aware,
            ..
        }
        | Plan::IndexScan {
            am_oid,
            parallel_aware,
            ..
        } if *am_oid == BTREE_AM_OID => {
            *parallel_aware = true;
        }
        Plan::BitmapHeapScan {
            bitmapqual,
            parallel_aware,
            ..
        } => {
            mark_parallel_seq_scans_inner(bitmapqual, next_scan_id);
            *parallel_aware = plan_contains_seqscan(bitmapqual);
        }
        Plan::Append {
            children,
            parallel_aware,
            ..
        } => {
            for child in children.iter_mut() {
                mark_parallel_seq_scans_inner(child, next_scan_id);
            }
            *parallel_aware =
                !children.is_empty() && children.iter().all(plan_contains_parallel_aware_seqscan);
        }
        Plan::MergeAppend { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::BitmapAnd { children, .. }
        | Plan::SetOp { children, .. } => {
            for child in children {
                mark_parallel_seq_scans_inner(child, next_scan_id);
            }
        }
        Plan::Unique { input, .. }
        | Plan::Hash { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Memoize { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. }
        | Plan::Filter { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Projection { input, .. }
        | Plan::Aggregate { input, .. }
        | Plan::WindowAgg { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::ProjectSet { input, .. } => mark_parallel_seq_scans_inner(input, next_scan_id),
        Plan::NestedLoopJoin { left, .. }
        | Plan::HashJoin { left, .. }
        | Plan::MergeJoin { left, .. } => {
            // :HACK: This v1 parallel rewrite only models partial joins by
            // dividing the outer side. The inner side stays worker-local and
            // serial to avoid duplicate/missing join pairs until real partial
            // path generation can choose both sides deliberately.
            mark_parallel_seq_scans_inner(left, next_scan_id);
        }
        Plan::CteScan { cte_plan, .. } => mark_parallel_seq_scans_inner(cte_plan, next_scan_id),
        Plan::RecursiveUnion {
            anchor, recursive, ..
        } => {
            mark_parallel_seq_scans_inner(anchor, next_scan_id);
            mark_parallel_seq_scans_inner(recursive, next_scan_id);
        }
        Plan::Result { .. }
        | Plan::SeqScan { .. }
        | Plan::TidScan { .. }
        | Plan::TidRangeScan { .. }
        | Plan::IndexOnlyScan { .. }
        | Plan::IndexScan { .. }
        | Plan::BitmapIndexScan { .. }
        | Plan::FunctionScan { .. }
        | Plan::WorkTableScan { .. }
        | Plan::Values { .. } => {}
    }
}

fn plan_contains_seqscan(plan: &Plan) -> bool {
    match plan {
        Plan::SeqScan {
            relkind,
            tablesample,
            ..
        } => tablesample.is_none() && matches!(*relkind, 'r' | 'p' | 'm'),
        Plan::IndexOnlyScan { am_oid, .. } | Plan::IndexScan { am_oid, .. } => {
            *am_oid == BTREE_AM_OID
        }
        Plan::BitmapIndexScan { am_oid, .. } => *am_oid == BTREE_AM_OID,
        Plan::Append { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::BitmapAnd { children, .. }
        | Plan::SetOp { children, .. } => children.iter().any(plan_contains_seqscan),
        Plan::Unique { input, .. }
        | Plan::Hash { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Memoize { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. }
        | Plan::Filter { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Projection { input, .. }
        | Plan::Aggregate { input, .. }
        | Plan::WindowAgg { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::ProjectSet { input, .. } => plan_contains_seqscan(input),
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::MergeJoin { left, right, .. } => {
            plan_contains_seqscan(left) || plan_contains_seqscan(right)
        }
        Plan::BitmapHeapScan { bitmapqual, .. } => plan_contains_seqscan(bitmapqual),
        Plan::CteScan { cte_plan, .. } => plan_contains_seqscan(cte_plan),
        Plan::RecursiveUnion {
            anchor, recursive, ..
        } => plan_contains_seqscan(anchor) || plan_contains_seqscan(recursive),
        Plan::Result { .. }
        | Plan::TidScan { .. }
        | Plan::TidRangeScan { .. }
        | Plan::FunctionScan { .. }
        | Plan::WorkTableScan { .. }
        | Plan::Values { .. } => false,
    }
}

fn plan_contains_parallel_aware_seqscan(plan: &Plan) -> bool {
    match plan {
        Plan::SeqScan {
            parallel_aware: true,
            ..
        } => true,
        Plan::IndexOnlyScan {
            parallel_aware: true,
            ..
        }
        | Plan::IndexScan {
            parallel_aware: true,
            ..
        }
        | Plan::BitmapHeapScan {
            parallel_aware: true,
            ..
        } => true,
        Plan::Append { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::BitmapAnd { children, .. }
        | Plan::SetOp { children, .. } => children.iter().any(plan_contains_parallel_aware_seqscan),
        Plan::Unique { input, .. }
        | Plan::Hash { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Memoize { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. }
        | Plan::Filter { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Projection { input, .. }
        | Plan::Aggregate { input, .. }
        | Plan::WindowAgg { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::ProjectSet { input, .. } => plan_contains_parallel_aware_seqscan(input),
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::MergeJoin { left, right, .. } => {
            plan_contains_parallel_aware_seqscan(left)
                || plan_contains_parallel_aware_seqscan(right)
        }
        Plan::BitmapHeapScan { bitmapqual, .. } => plan_contains_parallel_aware_seqscan(bitmapqual),
        Plan::CteScan { cte_plan, .. } => plan_contains_parallel_aware_seqscan(cte_plan),
        Plan::RecursiveUnion {
            anchor, recursive, ..
        } => {
            plan_contains_parallel_aware_seqscan(anchor)
                || plan_contains_parallel_aware_seqscan(recursive)
        }
        Plan::Result { .. }
        | Plan::SeqScan { .. }
        | Plan::TidScan { .. }
        | Plan::TidRangeScan { .. }
        | Plan::IndexOnlyScan { .. }
        | Plan::IndexScan { .. }
        | Plan::BitmapIndexScan { .. }
        | Plan::FunctionScan { .. }
        | Plan::WorkTableScan { .. }
        | Plan::Values { .. } => false,
    }
}

fn plan_parallel_safe(plan: &Plan, catalog: &dyn CatalogLookup) -> bool {
    match plan {
        Plan::WindowAgg { .. }
        | Plan::LockRows { .. }
        | Plan::FunctionScan { .. }
        | Plan::ProjectSet { .. }
        | Plan::CteScan { .. }
        | Plan::RecursiveUnion { .. }
        | Plan::WorkTableScan { .. } => false,
        Plan::Result { .. }
        | Plan::SeqScan { .. }
        | Plan::TidScan { filter: None, .. }
        | Plan::TidRangeScan { filter: None, .. }
        | Plan::IndexOnlyScan { .. }
        | Plan::IndexScan { .. }
        | Plan::BitmapIndexScan { .. } => true,
        Plan::TidScan {
            filter: Some(filter),
            ..
        } => !expr_parallel_safety(filter, Some(catalog)).is_unsafe(),
        Plan::TidRangeScan {
            tid_range_cond,
            filter,
            ..
        } => {
            !expr_parallel_safety(&tid_range_cond.display_expr, Some(catalog)).is_unsafe()
                && filter
                    .as_ref()
                    .is_none_or(|filter| !expr_parallel_safety(filter, Some(catalog)).is_unsafe())
        }
        Plan::Append { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::BitmapAnd { children, .. }
        | Plan::SetOp { children, .. } => children
            .iter()
            .all(|plan| plan_parallel_safe(plan, catalog)),
        Plan::Unique { input, .. }
        | Plan::Hash { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. } => plan_parallel_safe(input, catalog),
        Plan::Memoize {
            input, cache_keys, ..
        } => {
            plan_parallel_safe(input, catalog)
                && !cache_keys
                    .iter()
                    .any(|expr| expr_parallel_safety(expr, Some(catalog)).is_unsafe())
        }
        Plan::BitmapHeapScan {
            bitmapqual,
            recheck_qual,
            filter_qual,
            ..
        } => {
            plan_parallel_safe(bitmapqual, catalog)
                && !recheck_qual
                    .iter()
                    .any(|expr| expr_parallel_safety(expr, Some(catalog)).is_unsafe())
                && !filter_qual
                    .iter()
                    .any(|expr| expr_parallel_safety(expr, Some(catalog)).is_unsafe())
        }
        Plan::NestedLoopJoin {
            left,
            right,
            nest_params,
            join_qual,
            qual,
            ..
        } => {
            nest_params.is_empty()
                && plan_parallel_safe(left, catalog)
                && plan_parallel_safe(right, catalog)
                && !join_qual
                    .iter()
                    .any(|expr| expr_parallel_safety(expr, Some(catalog)).is_unsafe())
                && !qual
                    .iter()
                    .any(|expr| expr_parallel_safety(expr, Some(catalog)).is_unsafe())
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
            plan_parallel_safe(left, catalog)
                && plan_parallel_safe(right, catalog)
                && !hash_clauses
                    .iter()
                    .any(|expr| expr_parallel_safety(expr, Some(catalog)).is_unsafe())
                && !hash_keys
                    .iter()
                    .any(|expr| expr_parallel_safety(expr, Some(catalog)).is_unsafe())
                && !join_qual
                    .iter()
                    .any(|expr| expr_parallel_safety(expr, Some(catalog)).is_unsafe())
                && !qual
                    .iter()
                    .any(|expr| expr_parallel_safety(expr, Some(catalog)).is_unsafe())
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
            plan_parallel_safe(left, catalog)
                && plan_parallel_safe(right, catalog)
                && !merge_clauses
                    .iter()
                    .any(|expr| expr_parallel_safety(expr, Some(catalog)).is_unsafe())
                && !outer_merge_keys
                    .iter()
                    .any(|expr| expr_parallel_safety(expr, Some(catalog)).is_unsafe())
                && !inner_merge_keys
                    .iter()
                    .any(|expr| expr_parallel_safety(expr, Some(catalog)).is_unsafe())
                && !join_qual
                    .iter()
                    .any(|expr| expr_parallel_safety(expr, Some(catalog)).is_unsafe())
                && !qual
                    .iter()
                    .any(|expr| expr_parallel_safety(expr, Some(catalog)).is_unsafe())
        }
        Plan::Filter {
            input, predicate, ..
        } => {
            plan_parallel_safe(input, catalog)
                && !expr_parallel_safety(predicate, Some(catalog)).is_unsafe()
        }
        Plan::OrderBy { input, items, .. } | Plan::IncrementalSort { input, items, .. } => {
            plan_parallel_safe(input, catalog)
                && !items
                    .iter()
                    .any(|item| expr_parallel_safety(&item.expr, Some(catalog)).is_unsafe())
        }
        Plan::Limit {
            input,
            limit,
            offset,
            ..
        } => {
            plan_parallel_safe(input, catalog)
                && !limit
                    .as_ref()
                    .is_some_and(|expr| expr_parallel_safety(expr, Some(catalog)).is_unsafe())
                && !offset
                    .as_ref()
                    .is_some_and(|expr| expr_parallel_safety(expr, Some(catalog)).is_unsafe())
        }
        Plan::Projection { input, targets, .. } => {
            plan_parallel_safe(input, catalog)
                && !targets
                    .iter()
                    .any(|target| expr_parallel_safety(&target.expr, Some(catalog)).is_unsafe())
        }
        Plan::Aggregate {
            input,
            group_by,
            passthrough_exprs,
            accumulators,
            having,
            ..
        } => {
            plan_parallel_safe(input, catalog)
                && !group_by
                    .iter()
                    .any(|expr| expr_parallel_safety(expr, Some(catalog)).is_unsafe())
                && !passthrough_exprs
                    .iter()
                    .any(|expr| expr_parallel_safety(expr, Some(catalog)).is_unsafe())
                && !accumulators
                    .iter()
                    .any(|accum| accumulator_contains_parallel_unsafe(accum, Some(catalog)))
                && !having
                    .as_ref()
                    .is_some_and(|expr| expr_parallel_safety(expr, Some(catalog)).is_unsafe())
        }
        Plan::SubqueryScan { input, filter, .. } => {
            plan_parallel_safe(input, catalog)
                && !filter
                    .as_ref()
                    .is_some_and(|expr| expr_parallel_safety(expr, Some(catalog)).is_unsafe())
        }
        Plan::Values { rows, .. } => !rows
            .iter()
            .flatten()
            .any(|expr| expr_parallel_safety(expr, Some(catalog)).is_unsafe()),
    }
}

fn accumulator_contains_parallel_unsafe(
    accum: &AggAccum,
    catalog: Option<&dyn CatalogLookup>,
) -> bool {
    accum
        .direct_args
        .iter()
        .any(|expr| expr_parallel_safety(expr, catalog).is_unsafe())
        || accum
            .args
            .iter()
            .any(|expr| expr_parallel_safety(expr, catalog).is_unsafe())
        || accum
            .order_by
            .iter()
            .any(|item| expr_parallel_safety(&item.expr, catalog).is_unsafe())
        || accum
            .filter
            .as_ref()
            .is_some_and(|expr| expr_parallel_safety(expr, catalog).is_unsafe())
}

fn accumulator_worker_safe(accum: &AggAccum, catalog: Option<&dyn CatalogLookup>) -> bool {
    accum
        .direct_args
        .iter()
        .all(|expr| expr_parallel_safety(expr, catalog).is_worker_safe())
        && accum
            .args
            .iter()
            .all(|expr| expr_parallel_safety(expr, catalog).is_worker_safe())
        && accum
            .order_by
            .iter()
            .all(|item| expr_parallel_safety(&item.expr, catalog).is_worker_safe())
        && accum
            .filter
            .as_ref()
            .is_none_or(|expr| expr_parallel_safety(expr, catalog).is_worker_safe())
}

fn expr_contains_parallel_unsafe(expr: &Expr) -> bool {
    expr_parallel_safety(expr, None).is_unsafe()
}

fn expr_parallel_safety(expr: &Expr, catalog: Option<&dyn CatalogLookup>) -> ParallelSafety {
    match expr {
        Expr::Random | Expr::SetReturning(_) | Expr::SubLink(_) => ParallelSafety::Unsafe,
        Expr::SubPlan(subplan) => subplan_parallel_safety(subplan, catalog),
        Expr::Func(func) => {
            let mut safety = scalar_function_parallel_safety(func, catalog);
            for arg in &func.args {
                safety = safety.combine(expr_parallel_safety(arg, catalog));
            }
            if let Some(args) = &func.display_args {
                for arg in args {
                    safety = safety.combine(expr_parallel_safety(&arg.expr, catalog));
                }
            }
            safety
        }
        Expr::Aggref(aggref) => {
            let mut safety = ParallelSafety::Safe;
            for arg in aggref.direct_args.iter().chain(aggref.args.iter()) {
                safety = safety.combine(expr_parallel_safety(arg, catalog));
            }
            for entry in &aggref.aggorder {
                safety = safety.combine(expr_parallel_safety(&entry.expr, catalog));
            }
            if let Some(filter) = &aggref.aggfilter {
                safety = safety.combine(expr_parallel_safety(filter, catalog));
            }
            safety
        }
        Expr::GroupingKey(grouping_key) => expr_parallel_safety(&grouping_key.expr, catalog),
        Expr::GroupingFunc(grouping_func) => grouping_func
            .args
            .iter()
            .fold(ParallelSafety::Safe, |safety, arg| {
                safety.combine(expr_parallel_safety(arg, catalog))
            }),
        Expr::WindowFunc(_) => ParallelSafety::Unsafe,
        Expr::Op(op) => op.args.iter().fold(ParallelSafety::Safe, |safety, arg| {
            safety.combine(expr_parallel_safety(arg, catalog))
        }),
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .fold(ParallelSafety::Safe, |safety, arg| {
                safety.combine(expr_parallel_safety(arg, catalog))
            }),
        Expr::Case(case_expr) => {
            let mut safety = case_expr
                .arg
                .as_ref()
                .map(|arg| expr_parallel_safety(arg, catalog))
                .unwrap_or(ParallelSafety::Safe);
            for arm in &case_expr.args {
                safety = safety.combine(expr_parallel_safety(&arm.expr, catalog));
                safety = safety.combine(expr_parallel_safety(&arm.result, catalog));
            }
            safety.combine(expr_parallel_safety(&case_expr.defresult, catalog))
        }
        Expr::SqlJsonQueryFunction(func) => func
            .child_exprs()
            .into_iter()
            .fold(ParallelSafety::Safe, |safety, child| {
                safety.combine(expr_parallel_safety(child, catalog))
            }),
        Expr::ScalarArrayOp(op) => expr_parallel_safety(&op.left, catalog)
            .combine(expr_parallel_safety(&op.right, catalog)),
        Expr::Xml(xml) => xml
            .child_exprs()
            .fold(ParallelSafety::Safe, |safety, child| {
                safety.combine(expr_parallel_safety(child, catalog))
            }),
        Expr::Cast(inner, _) => expr_parallel_safety(inner, catalog),
        Expr::Collate { expr, .. } => expr_parallel_safety(expr, catalog),
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
            let mut safety =
                expr_parallel_safety(expr, catalog).combine(expr_parallel_safety(pattern, catalog));
            if let Some(escape) = escape {
                safety = safety.combine(expr_parallel_safety(escape, catalog));
            }
            safety
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => expr_parallel_safety(inner, catalog),
        Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
            expr_parallel_safety(left, catalog).combine(expr_parallel_safety(right, catalog))
        }
        Expr::ArrayLiteral { elements, .. } => elements
            .iter()
            .fold(ParallelSafety::Safe, |safety, element| {
                safety.combine(expr_parallel_safety(element, catalog))
            }),
        Expr::Row { fields, .. } => fields
            .iter()
            .fold(ParallelSafety::Safe, |safety, (_, field)| {
                safety.combine(expr_parallel_safety(field, catalog))
            }),
        Expr::FieldSelect { expr, .. } => expr_parallel_safety(expr, catalog),
        Expr::Coalesce(left, right) => {
            expr_parallel_safety(left, catalog).combine(expr_parallel_safety(right, catalog))
        }
        Expr::ArraySubscript { array, subscripts } => {
            let mut safety = expr_parallel_safety(array, catalog);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    safety = safety.combine(expr_parallel_safety(lower, catalog));
                }
                if let Some(upper) = &subscript.upper {
                    safety = safety.combine(expr_parallel_safety(upper, catalog));
                }
            }
            safety
        }
        Expr::Var(_)
        | Expr::Param(_)
        | Expr::Const(_)
        | Expr::CaseTest(_)
        | Expr::CurrentUser
        | Expr::User
        | Expr::SessionUser
        | Expr::SystemUser
        | Expr::CurrentRole
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => ParallelSafety::Safe,
    }
}

fn subplan_parallel_safety(
    subplan: &SubPlan,
    catalog: Option<&dyn CatalogLookup>,
) -> ParallelSafety {
    if !subplan.par_param.is_empty() || !subplan.args.is_empty() {
        return ParallelSafety::Unsafe;
    }
    subplan
        .testexpr
        .as_ref()
        .map(|expr| expr_parallel_safety(expr, catalog))
        .unwrap_or(ParallelSafety::Safe)
}

fn scalar_function_parallel_safety(
    func: &FuncExpr,
    catalog: Option<&dyn CatalogLookup>,
) -> ParallelSafety {
    match func.implementation {
        ScalarFunctionImpl::Builtin(
            BuiltinScalarFunction::Random
            | BuiltinScalarFunction::RandomNormal
            | BuiltinScalarFunction::GenRandomUuid,
        ) => ParallelSafety::Unsafe,
        ScalarFunctionImpl::UserDefined { proc_oid } => proc_parallel_safety(catalog, proc_oid),
        ScalarFunctionImpl::Builtin(_) => {
            if func.funcid == 0 || catalog.is_none() {
                ParallelSafety::Safe
            } else {
                proc_parallel_safety(catalog, func.funcid)
            }
        }
    }
}

fn proc_parallel_safety(catalog: Option<&dyn CatalogLookup>, proc_oid: u32) -> ParallelSafety {
    match catalog.and_then(|catalog| catalog.proc_row_by_oid(proc_oid)) {
        Some(row) => match row.proparallel {
            's' => ParallelSafety::Safe,
            'r' => ParallelSafety::Restricted,
            _ => ParallelSafety::Unsafe,
        },
        None => ParallelSafety::Unsafe,
    }
}

fn expr_contains_parallel_global_unsafe(expr: &Expr) -> bool {
    match expr {
        Expr::Random | Expr::SetReturning(_) | Expr::SubLink(_) => true,
        Expr::SubPlan(subplan) => subplan_parallel_safety(subplan, None).is_unsafe(),
        Expr::Func(func) => {
            matches!(
                func.implementation,
                ScalarFunctionImpl::Builtin(
                    BuiltinScalarFunction::Random
                        | BuiltinScalarFunction::RandomNormal
                        | BuiltinScalarFunction::GenRandomUuid
                )
            ) || func.args.iter().any(expr_contains_parallel_global_unsafe)
                || func.display_args.as_ref().is_some_and(|args| {
                    args.iter()
                        .any(|arg| expr_contains_parallel_global_unsafe(&arg.expr))
                })
        }
        Expr::Aggref(aggref) => {
            aggref
                .direct_args
                .iter()
                .any(expr_contains_parallel_global_unsafe)
                || aggref.args.iter().any(expr_contains_parallel_global_unsafe)
                || aggref
                    .aggorder
                    .iter()
                    .any(|entry| expr_contains_parallel_global_unsafe(&entry.expr))
                || aggref
                    .aggfilter
                    .as_ref()
                    .is_some_and(expr_contains_parallel_global_unsafe)
        }
        Expr::GroupingKey(grouping_key) => expr_contains_parallel_global_unsafe(&grouping_key.expr),
        Expr::GroupingFunc(grouping_func) => grouping_func
            .args
            .iter()
            .any(expr_contains_parallel_global_unsafe),
        Expr::WindowFunc(window_func) => window_func
            .args
            .iter()
            .any(expr_contains_parallel_global_unsafe),
        Expr::Op(op) => op.args.iter().any(expr_contains_parallel_global_unsafe),
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .any(expr_contains_parallel_global_unsafe),
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_ref()
                .is_some_and(|arg| expr_contains_parallel_global_unsafe(arg))
                || case_expr.args.iter().any(|arm| {
                    expr_contains_parallel_global_unsafe(&arm.expr)
                        || expr_contains_parallel_global_unsafe(&arm.result)
                })
                || expr_contains_parallel_global_unsafe(&case_expr.defresult)
        }
        Expr::SqlJsonQueryFunction(func) => func
            .child_exprs()
            .into_iter()
            .any(expr_contains_parallel_global_unsafe),
        Expr::ScalarArrayOp(op) => {
            expr_contains_parallel_global_unsafe(&op.left)
                || expr_contains_parallel_global_unsafe(&op.right)
        }
        Expr::Xml(xml) => xml.child_exprs().any(expr_contains_parallel_global_unsafe),
        Expr::Cast(inner, _) => expr_contains_parallel_global_unsafe(inner),
        Expr::Collate { expr, .. } => expr_contains_parallel_global_unsafe(expr),
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
            expr_contains_parallel_global_unsafe(expr)
                || expr_contains_parallel_global_unsafe(pattern)
                || escape
                    .as_ref()
                    .is_some_and(|escape| expr_contains_parallel_global_unsafe(escape))
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => expr_contains_parallel_global_unsafe(inner),
        Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
            expr_contains_parallel_global_unsafe(left)
                || expr_contains_parallel_global_unsafe(right)
        }
        Expr::ArrayLiteral { elements, .. } => {
            elements.iter().any(expr_contains_parallel_global_unsafe)
        }
        Expr::Row { fields, .. } => fields
            .iter()
            .any(|(_, field)| expr_contains_parallel_global_unsafe(field)),
        Expr::FieldSelect { expr, .. } => expr_contains_parallel_global_unsafe(expr),
        Expr::Coalesce(left, right) => {
            expr_contains_parallel_global_unsafe(left)
                || expr_contains_parallel_global_unsafe(right)
        }
        Expr::ArraySubscript { array, subscripts } => {
            expr_contains_parallel_global_unsafe(array)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_some_and(expr_contains_parallel_global_unsafe)
                        || subscript
                            .upper
                            .as_ref()
                            .is_some_and(expr_contains_parallel_global_unsafe)
                })
        }
        Expr::Var(_)
        | Expr::Param(_)
        | Expr::Const(_)
        | Expr::CaseTest(_)
        | Expr::CurrentUser
        | Expr::User
        | Expr::SessionUser
        | Expr::SystemUser
        | Expr::CurrentRole
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => false,
    }
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

fn expr_collation_oid(expr: &Expr) -> Option<u32> {
    match expr {
        Expr::Var(var) => var.collation_oid,
        Expr::Collate { collation_oid, .. } => Some(*collation_oid),
        Expr::Cast(inner, _) => expr_collation_oid(inner),
        Expr::GroupingKey(grouping_key) => expr_collation_oid(&grouping_key.expr),
        Expr::Func(func) => func
            .collation_oid
            .or_else(|| func.args.iter().find_map(expr_collation_oid)),
        _ => None,
    }
}

fn vars_match_for_setrefs(left: &Var, right: &Var) -> bool {
    // PostgreSQL's search_indexed_tlist_for_var matches Vars by varno/varattno
    // and copies the caller's Var into the replacement.  Do the same logical
    // match here: type and collation are expression metadata, not tlist lookup
    // keys, and can legitimately be missing from intermediate path targets.
    left.varno == right.varno
        && left.varattno == right.varattno
        && left.varlevelsup == right.varlevelsup
}

fn exprs_match_for_setrefs(left: &Expr, right: &Expr) -> bool {
    if left == right {
        return true;
    }
    match (left, right) {
        (Expr::Var(left_var), Expr::Var(right_var)) => vars_match_for_setrefs(left_var, right_var),
        (Expr::GroupingKey(left_key), Expr::GroupingKey(right_key)) => {
            left_key.ref_id == right_key.ref_id
                && exprs_match_for_setrefs(&left_key.expr, &right_key.expr)
        }
        _ => false,
    }
}

fn is_whole_row_var_expr(expr: &Expr) -> bool {
    matches!(expr, Expr::Var(var) if var.varattno == WHOLE_ROW_ATTR_NO)
}

fn special_slot_var(
    varno: usize,
    index: usize,
    sql_type: pgrust_nodes::parsenodes::SqlType,
    collation_oid: Option<u32>,
) -> Expr {
    Expr::Var(Var {
        varno,
        varattno: user_attrno(index),
        varlevelsup: 0,
        vartype: sql_type,
        collation_oid,
    })
}

fn special_slot_var_for_expr(varno: usize, entry: &IndexedTlistEntry, expr: &Expr) -> Expr {
    special_slot_var(
        varno,
        entry.index,
        entry.sql_type,
        expr_collation_oid(expr).or(entry.collation_oid),
    )
}

fn build_simple_tlist_from_exprs(output_vars: &[Expr]) -> IndexedTlist {
    IndexedTlist {
        entries: output_vars
            .iter()
            .enumerate()
            .map(|(index, expr)| IndexedTlistEntry {
                index,
                sql_type: expr_sql_type(expr),
                collation_oid: expr_collation_oid(expr),
                ressortgroupref: 0,
                match_exprs: vec![expr.clone()],
            })
            .collect(),
    }
}

fn build_base_scan_tlist(
    root: Option<&PlannerInfo>,
    source_id: usize,
    desc: &pgrust_nodes::primnodes::RelationDesc,
) -> IndexedTlist {
    let output_vars = desc
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            slot_var_with_collation(
                source_id,
                user_attrno(index),
                column.sql_type,
                (column.collation_oid != 0).then_some(column.collation_oid),
            )
        })
        .collect::<Vec<_>>();
    let mut tlist = build_simple_tlist_from_exprs(&output_vars);
    if let Some(rtindex) = rte_slot_varno(source_id) {
        for (index, entry) in tlist.entries.iter_mut().enumerate() {
            entry.match_exprs.push(Expr::Var(Var {
                varno: rtindex,
                varattno: user_attrno(index),
                varlevelsup: 0,
                vartype: entry.sql_type,
                collation_oid: entry.collation_oid,
            }));
            entry.match_exprs = dedup_match_exprs(std::mem::take(&mut entry.match_exprs));
        }
    }
    if let Some(info) = root.and_then(|root| append_translation(root, source_id)) {
        for (index, entry) in tlist.entries.iter_mut().enumerate() {
            if info
                .translated_vars
                .get(index)
                .is_some_and(|translated| exprs_match_for_setrefs(translated, &output_vars[index]))
            {
                entry.match_exprs = dedup_match_exprs(vec![
                    entry.match_exprs[0].clone(),
                    Expr::Var(Var {
                        varno: info.parent_relid,
                        varattno: user_attrno(index),
                        varlevelsup: 0,
                        vartype: entry.sql_type,
                        collation_oid: entry.collation_oid,
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
            entry.collation_oid = entry
                .collation_oid
                .or_else(|| expr_collation_oid(semantic_expr));
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
                .is_some_and(|translated| exprs_match_for_setrefs(translated, &output_vars[index]))
            {
                entry.match_exprs = dedup_match_exprs(vec![
                    entry.match_exprs[0].clone(),
                    Expr::Var(Var {
                        varno: info.parent_relid,
                        varattno: user_attrno(index),
                        varlevelsup: 0,
                        vartype: entry.sql_type,
                        collation_oid: None,
                    }),
                ]);
            }
        }
    }
    tlist
}

fn aggregate_output_expr(accum: &pgrust_nodes::primnodes::AggAccum, aggno: usize) -> Expr {
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
            .unwrap_or_else(|| crate::runtime::render_explain_expr(expr, &[])),
        Expr::Op(op) if op.args.len() == 2 => {
            let op_text = match op.op {
                pgrust_nodes::primnodes::OpExprKind::Add => "+",
                pgrust_nodes::primnodes::OpExprKind::Sub => "-",
                pgrust_nodes::primnodes::OpExprKind::Mul => "*",
                pgrust_nodes::primnodes::OpExprKind::Div => "/",
                pgrust_nodes::primnodes::OpExprKind::Mod => "%",
                pgrust_nodes::primnodes::OpExprKind::Eq => "=",
                pgrust_nodes::primnodes::OpExprKind::NotEq => "<>",
                pgrust_nodes::primnodes::OpExprKind::Lt => "<",
                pgrust_nodes::primnodes::OpExprKind::LtEq => "<=",
                pgrust_nodes::primnodes::OpExprKind::Gt => ">",
                pgrust_nodes::primnodes::OpExprKind::GtEq => ">=",
                _ => return crate::runtime::render_explain_expr(expr, &[]),
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
            let rendered = crate::runtime::render_explain_expr(&Expr::Const(value.clone()), &[]);
            rendered
                .strip_prefix('(')
                .and_then(|value| value.strip_suffix(')'))
                .unwrap_or(&rendered)
                .to_string()
        }
        _ => crate::runtime::render_explain_expr(expr, &[]),
    }
}

fn render_semantic_accum_name(root: Option<&PlannerInfo>, accum: &AggAccum) -> String {
    let name = pgrust_catalog_data::builtin_aggregate_function_for_proc_oid(accum.aggfnoid)
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
                    collation_oid: expr_collation_oid(&target.expr),
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
    phase: pgrust_nodes::plannodes::AggregatePhase,
    group_by: &[Expr],
    group_by_refs: &[usize],
    passthrough_exprs: &[Expr],
    accumulators: &[pgrust_nodes::primnodes::AggAccum],
    semantic_accumulators: Option<&[pgrust_nodes::primnodes::AggAccum]>,
) -> IndexedTlist {
    let display_accumulators = semantic_accumulators.unwrap_or(accumulators);
    let mut entries =
        Vec::with_capacity(group_by.len() + passthrough_exprs.len() + accumulators.len());
    for (index, expr) in group_by.iter().enumerate() {
        let output_var = slot_var(slot_id, user_attrno(index), expr_sql_type(expr));
        let mut match_exprs = vec![output_var.clone(), expr.clone()];
        if let Some(ref_id) = group_by_refs
            .get(index)
            .copied()
            .filter(|ref_id| *ref_id != 0)
        {
            match_exprs.push(Expr::GroupingKey(Box::new(
                pgrust_nodes::primnodes::GroupingKeyExpr {
                    expr: Box::new(expr.clone()),
                    ref_id,
                },
            )));
            match_exprs.push(Expr::GroupingKey(Box::new(
                pgrust_nodes::primnodes::GroupingKeyExpr {
                    expr: Box::new(output_var),
                    ref_id,
                },
            )));
        }
        if let Some(root) = root {
            match_exprs.push(flatten_join_alias_vars(root, expr.clone()));
        }
        entries.push(IndexedTlistEntry {
            index,
            sql_type: expr_sql_type(expr),
            collation_oid: expr_collation_oid(expr),
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
            collation_oid: expr_collation_oid(expr),
            ressortgroupref: 0,
            match_exprs: dedup_match_exprs(match_exprs),
        });
    }
    for (aggno, accum) in accumulators.iter().enumerate() {
        let display_accum = display_accumulators.get(aggno).unwrap_or(accum);
        let index = group_by.len() + passthrough_exprs.len() + aggno;
        let output_type = if phase == pgrust_nodes::plannodes::AggregatePhase::Partial {
            pgrust_nodes::parsenodes::SqlType::new(pgrust_nodes::parsenodes::SqlTypeKind::Record)
        } else {
            accum.sql_type
        };
        entries.push(IndexedTlistEntry {
            index,
            sql_type: output_type,
            collation_oid: None,
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
    targets: &[pgrust_nodes::primnodes::ProjectSetTarget],
) -> IndexedTlist {
    let input_target = input.semantic_output_target();
    IndexedTlist {
        entries: targets
            .iter()
            .enumerate()
            .map(|(index, target)| {
                let (sql_type, ressortgroupref, mut match_exprs) = match target {
                    pgrust_nodes::primnodes::ProjectSetTarget::Scalar(entry) => {
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
                    pgrust_nodes::primnodes::ProjectSetTarget::Set {
                        source_expr,
                        sql_type,
                        ressortgroupref,
                        ..
                    } => (
                        *sql_type,
                        *ressortgroupref,
                        vec![
                            slot_var(slot_id, user_attrno(index), *sql_type),
                            source_expr.clone(),
                        ],
                    ),
                };
                let match_exprs = dedup_match_exprs(std::mem::take(&mut match_exprs));
                let collation_oid = match_exprs.iter().find_map(expr_collation_oid);
                IndexedTlistEntry {
                    index,
                    sql_type,
                    collation_oid,
                    ressortgroupref,
                    match_exprs,
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
        let collation_oid = match_exprs.iter().find_map(expr_collation_oid);
        entries.push(IndexedTlistEntry {
            index,
            sql_type: column.sql_type,
            collation_oid,
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
        let (physical_index, sql_type, collation_oid, mut match_exprs) =
            match (left_match, right_match) {
                (Some(entry), None) => (
                    entry.index,
                    entry.sql_type,
                    entry.collation_oid,
                    entry.match_exprs.clone(),
                ),
                (None, Some(entry)) => (
                    left_physical_width + entry.index,
                    entry.sql_type,
                    entry.collation_oid,
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
                            left_entry.collation_oid,
                            left_entry.match_exprs.clone(),
                        )
                    } else {
                        (
                            left_physical_width + right_entry.index,
                            right_entry.sql_type,
                            right_entry.collation_oid,
                            right_entry.match_exprs.clone(),
                        )
                    }
                }
                (None, None) => continue,
            };

        if let Some(output_expr) = output_target.exprs.get(physical_index) {
            match_exprs.push(output_expr.clone());
        }
        match_exprs.push(semantic_expr.clone());
        entries.push(IndexedTlistEntry {
            index: physical_index,
            sql_type,
            collation_oid,
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
                        collation_oid: None,
                    }),
                    slot_var(rte_slot_id(rtindex), user_attrno(index), column.sql_type),
                ];
                IndexedTlistEntry {
                    index,
                    sql_type: column.sql_type,
                    collation_oid: None,
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
            group_by_refs,
            passthrough_exprs,
            accumulators,
            semantic_accumulators,
            ..
        } => build_aggregate_tlist(
            root,
            *slot_id,
            *phase,
            group_by,
            group_by_refs,
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
        Path::SeqScan {
            source_id, desc, ..
        }
        | Path::IndexOnlyScan {
            source_id, desc, ..
        }
        | Path::IndexScan {
            source_id, desc, ..
        }
        | Path::BitmapHeapScan {
            source_id, desc, ..
        } => build_base_scan_tlist(root, *source_id, desc),
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
    let mut search_exprs = vec![expr.clone()];
    if let Some(root) = root {
        if let Some(flattened) = maybe_flatten_join_alias_vars(root, expr) {
            push_unique_search_expr(&mut search_exprs, flattened);
        }
        for candidate in search_exprs.clone() {
            for translated in appendrel_search_exprs(root, &candidate) {
                if let Some(flattened) = maybe_flatten_join_alias_vars(root, &translated) {
                    push_unique_search_expr(&mut search_exprs, flattened);
                }
                push_unique_search_expr(&mut search_exprs, translated);
            }
        }
    }
    search_tlist_entry(root, expr, tlist)
        .or_else(|| {
            let mut matched_index = None;
            for entry in &tlist.entries {
                let entry_matches = entry.match_exprs.iter().any(|candidate| {
                    let expanded =
                        fully_expand_output_expr_with_root(root, candidate.clone(), input);
                    search_exprs.iter().any(|search_expr| {
                        exprs_equivalent(root, candidate, search_expr)
                            || output_component_matches_expr(candidate, search_expr)
                            || exprs_equivalent(root, &expanded, search_expr)
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
        .or_else(|| root.and_then(|root| search_partition_child_alias_var(root, expr, tlist)))
        .or_else(|| root.and_then(|root| search_partition_child_shape_var(root, expr, tlist)))
}

fn push_unique_search_expr(search_exprs: &mut Vec<Expr>, expr: Expr) {
    if !search_exprs.iter().any(|existing| existing == &expr) {
        search_exprs.push(expr);
    }
}

fn appendrel_search_exprs(root: &PlannerInfo, expr: &Expr) -> Vec<Expr> {
    root.append_rel_infos
        .iter()
        .flatten()
        .filter_map(|info| {
            let translated = rewrite_expr_for_append_rel(expr.clone(), info);
            (translated != *expr).then_some(translated)
        })
        .collect()
}

fn search_partition_child_alias_var<'a>(
    root: &PlannerInfo,
    expr: &Expr,
    tlist: &'a IndexedTlist,
) -> Option<&'a IndexedTlistEntry> {
    let Expr::Var(var) = expr else {
        return None;
    };
    if var.varlevelsup > 0 || is_executor_special_varno(var.varno) || is_system_attr(var.varattno) {
        return None;
    }
    let mut matched_index = None;
    for entry in &tlist.entries {
        let entry_matches = entry.match_exprs.iter().any(|candidate| {
            let Expr::Var(candidate_var) = candidate else {
                return false;
            };
            candidate_var.varlevelsup == 0
                && !is_executor_special_varno(candidate_var.varno)
                && candidate_var.varattno == var.varattno
                && candidate_var.vartype == var.vartype
                && appendrel_child_matches(root, var.varno, candidate_var.varno)
                && rte_alias_matches_partition_child(root, var.varno, candidate_var.varno)
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
}

fn search_partition_child_shape_var<'a>(
    root: &PlannerInfo,
    expr: &Expr,
    tlist: &'a IndexedTlist,
) -> Option<&'a IndexedTlistEntry> {
    let Expr::Var(var) = expr else {
        return None;
    };
    if var.varlevelsup > 0 || is_executor_special_varno(var.varno) || is_system_attr(var.varattno) {
        return None;
    }
    let parent_types = root
        .parse
        .rtable
        .get(var.varno.saturating_sub(1))?
        .desc
        .columns
        .iter()
        .map(|column| column.sql_type)
        .collect::<Vec<_>>();
    let mut matched_index = None;
    for entry in &tlist.entries {
        let entry_matches = entry.match_exprs.iter().any(|candidate| {
            let Expr::Var(candidate_var) = candidate else {
                return false;
            };
            candidate_var.varlevelsup == 0
                && !is_executor_special_varno(candidate_var.varno)
                && candidate_var.varattno == var.varattno
                && candidate_var.vartype == var.vartype
                && appendrel_child_matches(root, var.varno, candidate_var.varno)
                && tlist_varno_matches_desc_shape(tlist, candidate_var.varno, &parent_types)
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
}

fn tlist_varno_matches_desc_shape(
    tlist: &IndexedTlist,
    candidate_varno: usize,
    parent_types: &[pgrust_nodes::parsenodes::SqlType],
) -> bool {
    let mut candidate_types = vec![None; parent_types.len()];
    for entry in &tlist.entries {
        for candidate in &entry.match_exprs {
            let Expr::Var(var) = candidate else {
                continue;
            };
            if var.varlevelsup != 0 || var.varno != candidate_varno {
                continue;
            }
            let Some(index) = attrno_index(var.varattno) else {
                continue;
            };
            if index < candidate_types.len() {
                candidate_types[index] = Some(var.vartype);
            }
        }
    }
    candidate_types
        .into_iter()
        .zip(parent_types.iter().copied())
        .all(|(candidate, parent)| candidate == Some(parent))
}

fn appendrel_child_matches(root: &PlannerInfo, parent_varno: usize, child_varno: usize) -> bool {
    root.append_rel_infos
        .iter()
        .flatten()
        .any(|info| info.parent_relid == parent_varno && info.child_relid == child_varno)
}

fn rte_alias_matches_partition_child(
    root: &PlannerInfo,
    parent_varno: usize,
    candidate_varno: usize,
) -> bool {
    let Some(parent_alias) = rte_alias(root, parent_varno) else {
        return false;
    };
    let Some(candidate_alias) = rte_alias(root, candidate_varno) else {
        return false;
    };
    candidate_alias == parent_alias
        || candidate_alias
            .strip_prefix(&parent_alias)
            .and_then(|suffix| suffix.strip_prefix('_'))
            .is_some_and(|suffix| {
                !suffix.is_empty() && suffix.chars().all(|ch| ch.is_ascii_digit())
            })
}

fn rte_alias(root: &PlannerInfo, varno: usize) -> Option<String> {
    root.parse.rtable.get(varno.saturating_sub(1)).map(|rte| {
        rte.alias
            .clone()
            .unwrap_or_else(|| rte.eref.aliasname.clone())
    })
}

fn maybe_flatten_join_alias_vars(root: &PlannerInfo, expr: &Expr) -> Option<Expr> {
    expr_references_join_alias_var(root, expr).then(|| flatten_join_alias_vars(root, expr.clone()))
}

fn expr_references_join_alias_var(root: &PlannerInfo, expr: &Expr) -> bool {
    match expr {
        Expr::Var(var) if var.varlevelsup == 0 => root
            .parse
            .rtable
            .get(var.varno.saturating_sub(1))
            .is_some_and(|rte| matches!(rte.kind, RangeTblEntryKind::Join { .. })),
        Expr::GroupingKey(grouping_key) => expr_references_join_alias_var(root, &grouping_key.expr),
        Expr::GroupingFunc(grouping_func) => grouping_func
            .args
            .iter()
            .any(|expr| expr_references_join_alias_var(root, expr)),
        Expr::Aggref(aggref) => {
            aggref
                .direct_args
                .iter()
                .any(|expr| expr_references_join_alias_var(root, expr))
                || aggref
                    .args
                    .iter()
                    .any(|expr| expr_references_join_alias_var(root, expr))
                || aggref
                    .aggorder
                    .iter()
                    .any(|entry| expr_references_join_alias_var(root, &entry.expr))
                || aggref
                    .aggfilter
                    .as_ref()
                    .is_some_and(|expr| expr_references_join_alias_var(root, expr))
        }
        Expr::WindowFunc(window_func) => window_func
            .args
            .iter()
            .any(|expr| expr_references_join_alias_var(root, expr)),
        Expr::Op(op) => op
            .args
            .iter()
            .any(|expr| expr_references_join_alias_var(root, expr)),
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .any(|expr| expr_references_join_alias_var(root, expr)),
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_ref()
                .is_some_and(|expr| expr_references_join_alias_var(root, expr))
                || case_expr.args.iter().any(|arm| {
                    expr_references_join_alias_var(root, &arm.expr)
                        || expr_references_join_alias_var(root, &arm.result)
                })
                || expr_references_join_alias_var(root, &case_expr.defresult)
        }
        Expr::Func(func) => func
            .args
            .iter()
            .any(|expr| expr_references_join_alias_var(root, expr)),
        Expr::SubLink(sublink) => sublink
            .testexpr
            .as_ref()
            .is_some_and(|expr| expr_references_join_alias_var(root, expr)),
        Expr::SubPlan(subplan) => subplan
            .testexpr
            .as_ref()
            .is_some_and(|expr| expr_references_join_alias_var(root, expr)),
        Expr::ScalarArrayOp(op) => {
            expr_references_join_alias_var(root, &op.left)
                || expr_references_join_alias_var(root, &op.right)
        }
        Expr::Xml(xml) => xml
            .child_exprs()
            .any(|expr| expr_references_join_alias_var(root, expr)),
        Expr::Cast(inner, _) => expr_references_join_alias_var(root, inner),
        Expr::Collate { expr, .. } => expr_references_join_alias_var(root, expr),
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
            expr_references_join_alias_var(root, expr)
                || expr_references_join_alias_var(root, pattern)
                || escape
                    .as_ref()
                    .is_some_and(|expr| expr_references_join_alias_var(root, expr))
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => expr_references_join_alias_var(root, inner),
        Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
            expr_references_join_alias_var(root, left)
                || expr_references_join_alias_var(root, right)
        }
        Expr::ArrayLiteral { elements, .. } => elements
            .iter()
            .any(|expr| expr_references_join_alias_var(root, expr)),
        Expr::Row { fields, .. } => fields
            .iter()
            .any(|(_, expr)| expr_references_join_alias_var(root, expr)),
        Expr::FieldSelect { expr, .. } => expr_references_join_alias_var(root, expr),
        Expr::Coalesce(left, right) => {
            expr_references_join_alias_var(root, left)
                || expr_references_join_alias_var(root, right)
        }
        Expr::ArraySubscript { array, subscripts } => {
            expr_references_join_alias_var(root, array)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_some_and(|expr| expr_references_join_alias_var(root, expr))
                        || subscript
                            .upper
                            .as_ref()
                            .is_some_and(|expr| expr_references_join_alias_var(root, expr))
                })
        }
        Expr::SqlJsonQueryFunction(_)
        | Expr::SetReturning(_)
        | Expr::Var(_)
        | Expr::Param(_)
        | Expr::Const(_)
        | Expr::CaseTest(_)
        | Expr::Random
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::User
        | Expr::SystemUser
        | Expr::CurrentRole
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => false,
    }
}

fn output_component_matches_expr(candidate: &Expr, expr: &Expr) -> bool {
    if exprs_match_for_setrefs(candidate, expr) {
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
                && !is_special_varno(var.varno)
                && !is_system_attr(var.varattno) =>
        {
            let expr = Expr::Var(var.clone());
            search_input_tlist_entry(root, &Expr::Var(var.clone()), input, tlist)
                .filter(|entry| entry.sql_type == var.vartype)
                .map(|entry| special_slot_var_for_expr(OUTER_VAR, entry, &expr))
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
        return special_slot_var_for_expr(OUTER_VAR, entry, &expr);
    }
    let map_var = |var: Var| {
        if var.varlevelsup != 0 || is_special_varno(var.varno) || is_system_attr(var.varattno) {
            return Expr::Var(var);
        }
        let expr = Expr::Var(var.clone());
        search_input_tlist_entry(root, &expr, input, input_tlist)
            .filter(|entry| entry.sql_type == var.vartype)
            .map(|entry| special_slot_var_for_expr(OUTER_VAR, entry, &expr))
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
        Expr::Case(case_expr) => Expr::Case(Box::new(pgrust_nodes::primnodes::CaseExpr {
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
                .map(|arm| pgrust_nodes::primnodes::CaseWhen {
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
        Expr::Row { descriptor, fields } => Expr::Row {
            descriptor,
            fields: fields
                .into_iter()
                .map(|(name, expr)| {
                    (
                        name,
                        lower_projection_expr_by_input_target(root, expr, input, input_tlist),
                    )
                })
                .collect(),
        },
        Expr::FieldSelect {
            expr,
            field,
            field_type,
        } => Expr::FieldSelect {
            expr: Box::new(lower_projection_expr_by_input_target(
                root,
                *expr,
                input,
                input_tlist,
            )),
            field,
            field_type,
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
            expr: special_slot_var_for_expr(OUTER_VAR, entry, &item.expr),
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
                && !is_special_varno(var.varno)
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
                && !is_special_varno(var.varno)
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
        Path::Values { slot_id, .. } => rte_slot_varno(*slot_id),
        Path::BitmapOr { .. } | Path::BitmapAnd { .. } => None,
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

fn rewrite_expr_for_append_rel(expr: Expr, info: &pgrust_nodes::pathnodes::AppendRelInfo) -> Expr {
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
        Expr::Case(case_expr) => Expr::Case(Box::new(pgrust_nodes::primnodes::CaseExpr {
            arg: case_expr
                .arg
                .map(|arg| Box::new(rewrite_expr_for_append_rel(*arg, info))),
            args: case_expr
                .args
                .into_iter()
                .map(|arm| pgrust_nodes::primnodes::CaseWhen {
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
        Expr::SubLink(sublink) => Expr::SubLink(Box::new(pgrust_nodes::primnodes::SubLink {
            testexpr: sublink
                .testexpr
                .map(|expr| Box::new(rewrite_expr_for_append_rel(*expr, info))),
            ..*sublink
        })),
        Expr::SubPlan(subplan) => Expr::SubPlan(Box::new(pgrust_nodes::primnodes::SubPlan {
            testexpr: subplan
                .testexpr
                .map(|expr| Box::new(rewrite_expr_for_append_rel(*expr, info))),
            ..*subplan
        })),
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
                    input_tlist.entries.get(index).map(|entry| {
                        special_slot_var(
                            OUTER_VAR,
                            entry.index,
                            entry.sql_type,
                            entry.collation_oid,
                        )
                    })
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

fn fix_executor_join_var_for_input(expr: &Expr, input: &Path) -> Option<Expr> {
    let Expr::Var(var) = expr else {
        return None;
    };
    if var.varlevelsup > 0 || !is_executor_special_varno(var.varno) || is_system_attr(var.varattno)
    {
        return None;
    }
    let index = attrno_index(var.varattno)?;
    match input {
        Path::NestedLoopJoin { left, kind, .. }
        | Path::HashJoin { left, kind, .. }
        | Path::MergeJoin { left, kind, .. } => {
            let left_width = left.output_vars().len();
            let physical_index = if var.varno == OUTER_VAR {
                index
            } else if matches!(kind, JoinType::Semi | JoinType::Anti) {
                return None;
            } else {
                left_width + index
            };
            (physical_index < input.output_vars().len()).then(|| {
                special_slot_var(OUTER_VAR, physical_index, var.vartype, var.collation_oid)
            })
        }
        Path::Filter { input, .. }
        | Path::OrderBy { input, .. }
        | Path::IncrementalSort { input, .. }
        | Path::Limit { input, .. }
        | Path::LockRows { input, .. }
        | Path::Unique { input, .. } => fix_executor_join_var_for_input(expr, input),
        _ => None,
    }
}

fn fix_semantic_output_expr_for_input(
    root: Option<&PlannerInfo>,
    expr: &Expr,
    input: &Path,
) -> Option<Expr> {
    if is_whole_row_var_expr(expr) {
        return None;
    }
    input
        .semantic_output_target()
        .exprs
        .iter()
        .enumerate()
        .find_map(|(index, candidate)| {
            let direct = exprs_equivalent(root, candidate, expr);
            let flattened = root.is_some_and(|root| {
                exprs_equivalent(
                    Some(root),
                    &flatten_join_alias_vars(root, candidate.clone()),
                    &flatten_join_alias_vars(root, expr.clone()),
                )
            });
            (direct || flattened).then(|| {
                special_slot_var(
                    OUTER_VAR,
                    index,
                    expr_sql_type(expr),
                    expr_collation_oid(expr),
                )
            })
        })
}

fn fix_input_tlist_expr(
    root: Option<&PlannerInfo>,
    expr: &Expr,
    input: &Path,
    input_tlist: &IndexedTlist,
) -> Option<Expr> {
    if is_whole_row_var_expr(expr) {
        return None;
    }
    search_input_tlist_entry(root, expr, input, input_tlist)
        .filter(|entry| entry.sql_type == expr_sql_type(expr))
        .map(|entry| special_slot_var_for_expr(OUTER_VAR, entry, expr))
}

fn fix_semantic_position_var_for_input(
    root: Option<&PlannerInfo>,
    expr: &Expr,
    input: &Path,
    input_tlist: &IndexedTlist,
) -> Option<Expr> {
    let (Some(root), Expr::Var(var)) = (root, expr) else {
        return None;
    };
    if var.varlevelsup > 0 || is_executor_special_varno(var.varno) || is_system_attr(var.varattno) {
        return None;
    }
    let index = attrno_index(var.varattno)?;
    if root
        .parse
        .rtable
        .get(var.varno.saturating_sub(1))
        .is_some_and(|rte| index < rte.desc.columns.len())
    {
        return None;
    }
    let semantic_target = input.semantic_output_target();
    let semantic_expr = semantic_target.exprs.get(index)?;
    let entry = input_tlist.entries.get(index)?;
    // :HACK: Recursive CTE join paths can carry a Var whose varno still points
    // at a child RTE while varattno addresses the joined path's logical output
    // position. Treat only out-of-range child Vars as positional path refs, and
    // use the indexed tlist to recover the physical slot.
    (expr_sql_type(semantic_expr) == var.vartype && entry.sql_type == var.vartype)
        .then(|| special_slot_var(OUTER_VAR, entry.index, entry.sql_type, entry.collation_oid))
}

fn fix_join_rte_var_for_input(
    root: Option<&PlannerInfo>,
    expr: &Expr,
    input: &Path,
) -> Option<Expr> {
    let (Some(root), Expr::Var(var)) = (root, expr) else {
        return None;
    };
    if var.varlevelsup > 0 || is_special_varno(var.varno) || is_system_attr(var.varattno) {
        return None;
    }
    let index = attrno_index(var.varattno)?;
    if input
        .output_vars()
        .get(index)
        .is_none_or(|output| expr_sql_type(output) != var.vartype)
    {
        return None;
    }
    if path_single_relid(input) == Some(var.varno) {
        return Some(special_slot_var(
            OUTER_VAR,
            index,
            var.vartype,
            var.collation_oid,
        ));
    }
    if !root
        .parse
        .rtable
        .get(var.varno.saturating_sub(1))
        .is_some_and(|rte| matches!(rte.kind, RangeTblEntryKind::Join { .. }))
    {
        return None;
    }
    Some(special_slot_var(
        OUTER_VAR,
        index,
        var.vartype,
        var.collation_oid,
    ))
}

fn fix_upper_expr_for_input(
    root: Option<&PlannerInfo>,
    expr: Expr,
    input: &Path,
    input_tlist: &IndexedTlist,
) -> Expr {
    let rewritten = fix_upper_expr(root, expr.clone(), input_tlist);
    if rewritten != expr {
        if let Some(fixed) = fix_executor_join_var_for_input(&rewritten, input) {
            return fixed;
        }
        if expr_contains_local_semantic_var(&rewritten) {
            return fix_upper_expr_for_input(root, rewritten, input, input_tlist);
        }
        return rewritten;
    }
    if let Some(root) = root {
        let translated = rewrite_appendrel_expr_for_input_path(root, expr.clone(), input);
        if translated != expr {
            let translated_rewritten = fix_upper_expr(Some(root), translated.clone(), input_tlist);
            if translated_rewritten != translated {
                if let Some(fixed) = fix_executor_join_var_for_input(&translated_rewritten, input) {
                    return fixed;
                }
                if expr_contains_local_semantic_var(&translated_rewritten) {
                    return fix_upper_expr_for_input(
                        Some(root),
                        translated_rewritten,
                        input,
                        input_tlist,
                    );
                }
                return translated_rewritten;
            }
        }
    }
    if let Some(rewritten) = fix_input_tlist_expr(root, &expr, input, input_tlist) {
        return rewritten;
    }
    if let Some(rewritten) = fix_immediate_subquery_output_expr(root, &expr, input, input_tlist) {
        return rewritten;
    }
    if let Some(rewritten) = fix_executor_join_var_for_input(&expr, input) {
        return rewritten;
    }
    if let Some(rewritten) = fix_semantic_position_var_for_input(root, &expr, input, input_tlist) {
        return rewritten;
    }
    if let Some(rewritten) = fix_join_rte_var_for_input(root, &expr, input) {
        return rewritten;
    }
    if let Some(rewritten) = fix_semantic_output_expr_for_input(root, &expr, input) {
        return rewritten;
    }
    let rebuilt = rebuild_setrefs_expr(root, expr.clone(), |inner| {
        fix_upper_expr_for_input(root, inner, input, input_tlist)
    });
    if rebuilt != expr {
        return rebuilt;
    }
    expr
}

fn lower_direct_ref(expr: &Expr, mode: LowerMode<'_>) -> Option<Expr> {
    if is_whole_row_var_expr(expr) {
        return None;
    }
    match mode {
        LowerMode::Scalar => None,
        LowerMode::Input { tlist, .. } => search_tlist_entry(None, expr, tlist)
            .map(|entry| special_slot_var_for_expr(OUTER_VAR, entry, expr)),
        LowerMode::Aggregate { layout, tlist, .. } => search_tlist_entry(None, expr, tlist)
            .map(|entry| special_slot_var_for_expr(OUTER_VAR, entry, expr))
            .or_else(|| {
                layout.iter().enumerate().find_map(|(index, candidate)| {
                    exprs_match_for_setrefs(candidate, expr).then(|| {
                        special_slot_var(
                            OUTER_VAR,
                            index,
                            expr_sql_type(candidate),
                            expr_collation_oid(candidate),
                        )
                    })
                })
            }),
        LowerMode::Join {
            outer_tlist,
            inner_tlist,
        } => search_tlist_entry(None, expr, outer_tlist)
            .map(|entry| special_slot_var_for_expr(OUTER_VAR, entry, expr))
            .or_else(|| {
                search_tlist_entry(None, expr, inner_tlist)
                    .map(|entry| special_slot_var_for_expr(INNER_VAR, entry, expr))
            }),
    }
}

fn exec_param_for_outer_expr(ctx: &mut SetRefsContext<'_>, expr: Expr) -> Expr {
    let can_reuse_ancestor_param = expr_max_varlevelsup(&expr) > 1;
    let parent_expr = decrement_outer_expr_levels(expr.clone());
    let paramtype = expr_sql_type(&parent_expr);
    if can_reuse_ancestor_param
        && let Some(existing) = find_existing_exec_param(ctx, parent_expr.clone())
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
        label: label_for_expr(ctx, &parent_expr),
        expr: parent_expr,
    });
    Expr::Param(Param {
        paramkind: ParamKind::Exec,
        paramid,
        paramtype,
    })
}

fn expr_max_varlevelsup(expr: &Expr) -> usize {
    match expr {
        Expr::Var(var) => var.varlevelsup,
        Expr::GroupingKey(grouping_key) => expr_max_varlevelsup(&grouping_key.expr),
        Expr::GroupingFunc(grouping_func) => grouping_func
            .args
            .iter()
            .map(expr_max_varlevelsup)
            .max()
            .unwrap_or(0),
        Expr::Aggref(aggref) => aggref
            .args
            .iter()
            .map(expr_max_varlevelsup)
            .chain(
                aggref
                    .aggfilter
                    .as_ref()
                    .map(|expr| expr_max_varlevelsup(expr)),
            )
            .max()
            .unwrap_or(aggref.agglevelsup)
            .max(aggref.agglevelsup),
        Expr::WindowFunc(window_func) => window_func
            .args
            .iter()
            .map(expr_max_varlevelsup)
            .chain(match &window_func.kind {
                WindowFuncKind::Aggregate(aggref) => aggref
                    .aggfilter
                    .as_ref()
                    .map(|expr| expr_max_varlevelsup(expr)),
                WindowFuncKind::Builtin(_) => None,
            })
            .max()
            .unwrap_or(0),
        Expr::Op(op) => op.args.iter().map(expr_max_varlevelsup).max().unwrap_or(0),
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .map(expr_max_varlevelsup)
            .max()
            .unwrap_or(0),
        Expr::Case(case_expr) => case_expr
            .arg
            .as_deref()
            .map(expr_max_varlevelsup)
            .into_iter()
            .chain(case_expr.args.iter().flat_map(|arm| {
                [
                    expr_max_varlevelsup(&arm.expr),
                    expr_max_varlevelsup(&arm.result),
                ]
            }))
            .chain(std::iter::once(expr_max_varlevelsup(&case_expr.defresult)))
            .max()
            .unwrap_or(0),
        Expr::Func(func) => func
            .args
            .iter()
            .map(expr_max_varlevelsup)
            .max()
            .unwrap_or(0),
        Expr::SqlJsonQueryFunction(func) => func
            .child_exprs()
            .into_iter()
            .map(expr_max_varlevelsup)
            .max()
            .unwrap_or(0),
        Expr::SetReturning(srf) => set_returning_call_exprs(&srf.call)
            .into_iter()
            .map(expr_max_varlevelsup)
            .max()
            .unwrap_or(0),
        Expr::SubLink(sublink) => sublink
            .testexpr
            .as_deref()
            .map(expr_max_varlevelsup)
            .unwrap_or(0),
        Expr::SubPlan(subplan) => subplan
            .testexpr
            .as_deref()
            .map(expr_max_varlevelsup)
            .into_iter()
            .chain(subplan.args.iter().map(expr_max_varlevelsup))
            .max()
            .unwrap_or(0),
        Expr::ScalarArrayOp(saop) => {
            expr_max_varlevelsup(&saop.left).max(expr_max_varlevelsup(&saop.right))
        }
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::FieldSelect { expr: inner, .. } => expr_max_varlevelsup(inner),
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
        } => expr_max_varlevelsup(expr)
            .max(expr_max_varlevelsup(pattern))
            .max(escape.as_deref().map(expr_max_varlevelsup).unwrap_or(0)),
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            expr_max_varlevelsup(left).max(expr_max_varlevelsup(right))
        }
        Expr::ArrayLiteral { elements, .. } => {
            elements.iter().map(expr_max_varlevelsup).max().unwrap_or(0)
        }
        Expr::Row { fields, .. } => fields
            .iter()
            .map(|(_, expr)| expr_max_varlevelsup(expr))
            .max()
            .unwrap_or(0),
        Expr::ArraySubscript { array, subscripts } => subscripts
            .iter()
            .flat_map(|subscript| [subscript.lower.as_ref(), subscript.upper.as_ref()])
            .flatten()
            .map(expr_max_varlevelsup)
            .chain(std::iter::once(expr_max_varlevelsup(array)))
            .max()
            .unwrap_or(0),
        Expr::Xml(xml) => xml
            .child_exprs()
            .map(expr_max_varlevelsup)
            .max()
            .unwrap_or(0),
        Expr::Param(_)
        | Expr::Const(_)
        | Expr::CaseTest(_)
        | Expr::Random
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::User
        | Expr::SystemUser
        | Expr::CurrentRole
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => 0,
    }
}

fn find_existing_exec_param<'a>(
    ctx: &'a SetRefsContext<'_>,
    mut expr: Expr,
) -> Option<&'a ExecParamSource> {
    loop {
        if let Some(existing) = ctx
            .ext_params
            .iter()
            .find(|param| exprs_equivalent(ctx.root, &param.expr, &expr))
        {
            return Some(existing);
        }
        let decremented = decrement_outer_expr_levels(expr.clone());
        if decremented == expr {
            return None;
        }
        expr = decremented;
    }
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

fn label_for_runtime_expr(ctx: &SetRefsContext<'_>, expr: &Expr) -> Option<String> {
    label_for_expr(ctx, &decrement_outer_expr_levels(expr.clone()))
}

fn label_for_expr(ctx: &SetRefsContext<'_>, expr: &Expr) -> Option<String> {
    let root = ctx.root?;
    Some(render_param_label_expr(root, expr, ctx))
}

fn render_param_label_expr(root: &PlannerInfo, expr: &Expr, ctx: &SetRefsContext<'_>) -> String {
    match expr {
        Expr::Var(var) if var.varlevelsup > 0 => {
            let mut var = var.clone();
            var.varlevelsup -= 1;
            render_param_label_expr(root, &Expr::Var(var), ctx)
        }
        Expr::Var(var) if var.varlevelsup == 0 => root
            .parse
            .rtable
            .get(var.varno.saturating_sub(1))
            .and_then(|rte| {
                attrno_index(var.varattno).and_then(|index| {
                    if let RangeTblEntryKind::Join { joinaliasvars, .. } = &rte.kind
                        && let Some(alias_expr) = joinaliasvars.get(index)
                    {
                        return Some(render_param_label_expr(root, alias_expr, ctx));
                    }
                    rte.desc.columns.get(index).map(|column| {
                        if let Some(alias) = rte.alias.as_deref() {
                            format!("{alias}.{}", column.name)
                        } else {
                            column.name.clone()
                        }
                    })
                })
            })
            .unwrap_or_else(|| format!("{expr:?}")),
        Expr::Param(param) if matches!(param.paramkind, ParamKind::Exec) => ctx
            .ext_params
            .iter()
            .rev()
            .find(|source| source.paramid == param.paramid)
            .map(|source| {
                source
                    .label
                    .clone()
                    .unwrap_or_else(|| render_param_label_expr(root, &source.expr, ctx))
            })
            .unwrap_or_else(|| format!("${}", param.paramid)),
        Expr::Cast(inner, ty) => {
            let inner = render_param_label_expr(root, inner, ctx);
            format!("({inner})::{}", param_label_type_name(*ty))
        }
        Expr::Collate { expr: inner, .. } => render_param_label_expr(root, inner, ctx),
        Expr::Op(op) => {
            let op_text = match op.op {
                pgrust_nodes::primnodes::OpExprKind::Add => "+",
                pgrust_nodes::primnodes::OpExprKind::Sub => "-",
                pgrust_nodes::primnodes::OpExprKind::Mul => "*",
                pgrust_nodes::primnodes::OpExprKind::Div => "/",
                pgrust_nodes::primnodes::OpExprKind::Mod => "%",
                pgrust_nodes::primnodes::OpExprKind::Eq => "=",
                pgrust_nodes::primnodes::OpExprKind::NotEq => "<>",
                pgrust_nodes::primnodes::OpExprKind::Lt => "<",
                pgrust_nodes::primnodes::OpExprKind::LtEq => "<=",
                pgrust_nodes::primnodes::OpExprKind::Gt => ">",
                pgrust_nodes::primnodes::OpExprKind::GtEq => ">=",
                _ => {
                    return crate::runtime::render_explain_expr(expr, &[]);
                }
            };
            match op.args.as_slice() {
                [left, right] => format!(
                    "({} {op_text} {})",
                    render_param_label_expr(root, left, ctx),
                    render_param_label_expr(root, right, ctx)
                ),
                [inner] => format!("({op_text}{})", render_param_label_expr(root, inner, ctx)),
                _ => crate::runtime::render_explain_expr(expr, &[]),
            }
        }
        Expr::Func(func)
            if matches!(
                func.implementation,
                ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoPoint)
            ) =>
        {
            let name = func.funcname.as_deref().unwrap_or("point");
            let args = func
                .args
                .iter()
                .map(|arg| render_param_label_expr(root, arg, ctx))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{name}({args})")
        }
        Expr::Const(value) => {
            let rendered = crate::runtime::render_explain_expr(&Expr::Const(value.clone()), &[]);
            rendered
                .strip_prefix('(')
                .and_then(|value| value.strip_suffix(')'))
                .unwrap_or(&rendered)
                .to_string()
        }
        _ => crate::runtime::render_explain_expr(expr, &[]),
    }
}

fn param_label_type_name(ty: pgrust_nodes::parsenodes::SqlType) -> String {
    use pgrust_nodes::parsenodes::SqlTypeKind;
    let element = if ty.is_array { ty.element_type() } else { ty };
    let rendered = match element.kind {
        SqlTypeKind::Bool => "boolean".into(),
        SqlTypeKind::Int2 => "smallint".into(),
        SqlTypeKind::Int4 => "integer".into(),
        SqlTypeKind::Int8 => "bigint".into(),
        SqlTypeKind::Float4 => "real".into(),
        SqlTypeKind::Float8 => "double precision".into(),
        SqlTypeKind::Numeric => element
            .numeric_precision_scale()
            .map(|(precision, scale)| format!("numeric({precision},{scale})"))
            .unwrap_or_else(|| "numeric".into()),
        SqlTypeKind::Text => "text".into(),
        SqlTypeKind::Varchar => "character varying".into(),
        SqlTypeKind::Char => "character".into(),
        _ => format!("{:?}", element.kind).to_ascii_lowercase(),
    };
    if ty.is_array {
        format!("{rendered}[]")
    } else {
        rendered
    }
}

fn can_bind_as_nest_param(rebased_expr: &Expr, fixed_expr: &Expr) -> bool {
    let fixed_is_system_var = expr_is_local_system_var(fixed_expr);
    expr_contains_local_semantic_var(rebased_expr)
        && (fixed_expr != rebased_expr || fixed_is_system_var)
        && expr_sql_type(fixed_expr) == expr_sql_type(rebased_expr)
        && (!expr_contains_local_semantic_var(fixed_expr) || fixed_is_system_var)
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
    call: pgrust_nodes::primnodes::SetReturningCall,
    mode: LowerMode<'_>,
) -> pgrust_nodes::primnodes::SetReturningCall {
    use pgrust_nodes::primnodes::SetReturningCall;

    match call {
        SetReturningCall::RowsFrom {
            items,
            output_columns,
            with_ordinality,
        } => SetReturningCall::RowsFrom {
            items: items
                .into_iter()
                .map(|item| pgrust_nodes::primnodes::RowsFromItem {
                    source: match item.source {
                        RowsFromSource::Function(call) => {
                            RowsFromSource::Function(lower_set_returning_call(ctx, call, mode))
                        }
                        RowsFromSource::Project {
                            output_exprs,
                            output_columns,
                            display_sql,
                        } => RowsFromSource::Project {
                            output_exprs: output_exprs
                                .into_iter()
                                .map(|expr| lower_expr(ctx, expr, mode))
                                .collect(),
                            output_columns,
                            display_sql,
                        },
                    },
                    column_definitions: item.column_definitions,
                })
                .collect(),
            output_columns,
            with_ordinality,
        },
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
        SetReturningCall::PgStatProgressCopy {
            output_columns,
            with_ordinality,
        } => SetReturningCall::PgStatProgressCopy {
            output_columns,
            with_ordinality,
        },
        SetReturningCall::PgSequences {
            output_columns,
            with_ordinality,
        } => SetReturningCall::PgSequences {
            output_columns,
            with_ordinality,
        },
        SetReturningCall::InformationSchemaSequences {
            output_columns,
            with_ordinality,
        } => SetReturningCall::InformationSchemaSequences {
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
            inlined_expr,
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
            inlined_expr: inlined_expr.map(|expr| Box::new(lower_expr(ctx, *expr, mode))),
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
    call: pgrust_nodes::primnodes::SetReturningCall,
    path: &Path,
    input_tlist: &IndexedTlist,
) -> pgrust_nodes::primnodes::SetReturningCall {
    use pgrust_nodes::primnodes::SetReturningCall;

    match call {
        SetReturningCall::RowsFrom {
            items,
            output_columns,
            with_ordinality,
        } => SetReturningCall::RowsFrom {
            items: items
                .into_iter()
                .map(|item| pgrust_nodes::primnodes::RowsFromItem {
                    source: match item.source {
                        RowsFromSource::Function(call) => RowsFromSource::Function(
                            fix_set_returning_call_upper_exprs(root, call, path, input_tlist),
                        ),
                        RowsFromSource::Project {
                            output_exprs,
                            output_columns,
                            display_sql,
                        } => RowsFromSource::Project {
                            output_exprs: output_exprs
                                .into_iter()
                                .map(|expr| fix_upper_expr_for_input(root, expr, path, input_tlist))
                                .collect(),
                            output_columns,
                            display_sql,
                        },
                    },
                    column_definitions: item.column_definitions,
                })
                .collect(),
            output_columns,
            with_ordinality,
        },
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
        SetReturningCall::PgStatProgressCopy {
            output_columns,
            with_ordinality,
        } => SetReturningCall::PgStatProgressCopy {
            output_columns,
            with_ordinality,
        },
        SetReturningCall::PgSequences {
            output_columns,
            with_ordinality,
        } => SetReturningCall::PgSequences {
            output_columns,
            with_ordinality,
        },
        SetReturningCall::InformationSchemaSequences {
            output_columns,
            with_ordinality,
        } => SetReturningCall::InformationSchemaSequences {
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
            inlined_expr,
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
            inlined_expr: inlined_expr
                .map(|expr| Box::new(fix_upper_expr_for_input(root, *expr, path, input_tlist))),
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
    target: pgrust_nodes::primnodes::ProjectSetTarget,
    mode: LowerMode<'_>,
) -> pgrust_nodes::primnodes::ProjectSetTarget {
    use pgrust_nodes::primnodes::ProjectSetTarget;

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
            ressortgroupref,
        } => ProjectSetTarget::Set {
            name,
            source_expr,
            call: lower_set_returning_call(ctx, call, mode),
            sql_type,
            column_index,
            ressortgroupref,
        },
    }
}

fn lower_agg_accum(
    ctx: &mut SetRefsContext<'_>,
    accum: pgrust_nodes::primnodes::AggAccum,
    path: &Path,
    input_tlist: &IndexedTlist,
    semantic_group_by: &[Expr],
    semantic_passthrough_exprs: &[Expr],
    aggregate_layout: &[Expr],
    aggregate_tlist: &IndexedTlist,
) -> pgrust_nodes::primnodes::AggAccum {
    let direct_args = accum
        .direct_args
        .into_iter()
        .map(|arg| {
            let arg = match ctx.root {
                Some(root) => lower_agg_output_expr(
                    expand_join_rte_vars(root, arg),
                    semantic_group_by,
                    semantic_passthrough_exprs,
                    aggregate_layout,
                ),
                None => lower_agg_output_expr(
                    arg,
                    semantic_group_by,
                    semantic_passthrough_exprs,
                    aggregate_layout,
                ),
            };
            lower_expr(
                ctx,
                arg,
                LowerMode::Aggregate {
                    group_by: semantic_group_by,
                    passthrough_exprs: semantic_passthrough_exprs,
                    layout: aggregate_layout,
                    tlist: aggregate_tlist,
                },
            )
        })
        .collect();
    pgrust_nodes::primnodes::AggAccum {
        direct_args,
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
    sublink: pgrust_nodes::primnodes::SubLink,
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
    let target_width = sublink.subselect.target_list.len();
    let target_attnos = subplan_target_attnos(&sublink.subselect.target_list);
    let sublink_type = sublink.sublink_type;
    let plan_subquery_in_parallel = matches!(
        sublink_type,
        SubLinkType::ExprSubLink | SubLinkType::ArraySubLink
    ) && sublink.testexpr.is_none();
    let mut config = ctx.root.map(|root| root.config).unwrap_or_default();
    if !plan_subquery_in_parallel {
        config.max_parallel_workers_per_gather = 0;
        config.force_parallel_gather = false;
    }
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
    let plan_id = append_uncorrelated_planned_subquery(planned_stmt, ctx.subplans);
    let initplan_output_count = match sublink_type {
        SubLinkType::ExistsSubLink | SubLinkType::ExprSubLink | SubLinkType::ArraySubLink => 1,
        SubLinkType::RowCompareSubLink(_) => target_width,
        SubLinkType::AllSubLink(_) | SubLinkType::AnySubLink(_) => 0,
    };
    let is_initplan = par_param.is_empty() && initplan_output_count > 0;
    let set_params = if is_initplan {
        allocate_exec_params(ctx, initplan_output_count)
    } else {
        Vec::new()
    };
    Expr::SubPlan(Box::new(SubPlan {
        sublink_type,
        testexpr: sublink
            .testexpr
            .map(|expr| Box::new(lower_expr(ctx, *expr, mode))),
        comparison: sublink.comparison,
        first_col_type,
        target_width,
        target_attnos,
        plan_id,
        is_initplan,
        set_params,
        par_param,
        args,
    }))
}

fn allocate_exec_params(ctx: &mut SetRefsContext<'_>, count: usize) -> Vec<usize> {
    let start = ctx.next_param_id;
    ctx.next_param_id += count;
    (start..start + count).collect()
}

fn subplan_target_attnos(target_list: &[TargetEntry]) -> Vec<Option<usize>> {
    target_list
        .iter()
        .map(|target| match &target.expr {
            Expr::Var(var) if var.varlevelsup == 0 => attrno_index(var.varattno),
            _ => None,
        })
        .collect()
}

fn lower_expr(ctx: &mut SetRefsContext<'_>, expr: Expr, mode: LowerMode<'_>) -> Expr {
    if matches!(&expr, Expr::Var(var) if var.varattno == WHOLE_ROW_ATTR_NO) {
        let Expr::Var(var) = expr else {
            unreachable!();
        };
        return lower_whole_row_var(ctx, var, mode);
    }
    if let Some(lowered) = lower_direct_ref(&expr, mode) {
        return lowered;
    }
    match expr {
        Expr::Var(var) if var.varlevelsup > 0 => exec_param_for_outer_expr(ctx, Expr::Var(var)),
        Expr::Var(var) if is_rule_pseudo_varno(var.varno) => Expr::Var(var),
        Expr::Var(var) if is_executor_special_varno(var.varno) => {
            let expr = Expr::Var(var);
            if let LowerMode::Input {
                path: Some(input), ..
            } = mode
                && let Some(rewritten) = fix_executor_join_var_for_input(&expr, input)
            {
                return rewritten;
            }
            expr
        }
        Expr::Var(var) => {
            if is_system_attr(var.varattno) {
                Expr::Var(var)
            } else if let Some(root) = ctx.root {
                let expr = Expr::Var(var.clone());
                if let LowerMode::Input {
                    path: Some(input),
                    tlist,
                } = mode
                {
                    if let Some(rewritten) =
                        fix_semantic_position_var_for_input(ctx.root, &expr, input, tlist)
                    {
                        return rewritten;
                    }
                    if let Some(rewritten) = fix_join_rte_var_for_input(ctx.root, &expr, input) {
                        return rewritten;
                    }
                    if let Some(rewritten) =
                        fix_semantic_output_expr_for_input(ctx.root, &expr, input)
                    {
                        return rewritten;
                    }
                }
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
        Expr::GroupingKey(grouping_key) => {
            Expr::GroupingKey(Box::new(pgrust_nodes::primnodes::GroupingKeyExpr {
                expr: Box::new(lower_expr(ctx, *grouping_key.expr, mode)),
                ref_id: grouping_key.ref_id,
            }))
        }
        Expr::GroupingFunc(grouping_func) => {
            Expr::GroupingFunc(Box::new(pgrust_nodes::primnodes::GroupingFuncExpr {
                args: grouping_func
                    .args
                    .into_iter()
                    .map(|arg| lower_expr(ctx, arg, mode))
                    .collect(),
                ..*grouping_func
            }))
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
        Expr::Case(case_expr) => Expr::Case(Box::new(pgrust_nodes::primnodes::CaseExpr {
            arg: case_expr
                .arg
                .map(|arg| Box::new(lower_expr(ctx, *arg, mode))),
            args: case_expr
                .args
                .into_iter()
                .map(|arm| pgrust_nodes::primnodes::CaseWhen {
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
            comparison: subplan.comparison,
            first_col_type: subplan.first_col_type,
            target_width: subplan.target_width,
            target_attnos: subplan.target_attnos,
            plan_id: subplan.plan_id,
            is_initplan: subplan.is_initplan,
            set_params: subplan.set_params,
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

fn lower_whole_row_var(ctx: &mut SetRefsContext<'_>, var: Var, mode: LowerMode<'_>) -> Expr {
    if var.varlevelsup > 0 {
        return exec_param_for_outer_expr(ctx, Expr::Var(var));
    }
    if is_rule_pseudo_varno(var.varno) || is_executor_special_varno(var.varno) {
        return Expr::Var(var);
    }
    if let LowerMode::Input {
        path: Some(input), ..
    } = mode
        && path_preserves_base_whole_row_slot(input, var.varno)
    {
        return executor_whole_row_var(OUTER_VAR, &var);
    }
    if let LowerMode::Input {
        path: Some(input),
        tlist,
    } = mode
        && let Some(expr) = lower_whole_row_var_from_input_tlist(ctx, &var, input, tlist)
    {
        return expr;
    }
    if let LowerMode::Join {
        outer_tlist,
        inner_tlist,
    } = mode
    {
        if let Some(expr) = lower_whole_row_var_from_tlist(ctx, &var, OUTER_VAR, outer_tlist) {
            return expr;
        }
        if let Some(expr) = lower_whole_row_var_from_tlist(ctx, &var, INNER_VAR, inner_tlist) {
            return expr;
        }
    }
    if let Some(root) = ctx.root {
        let expr = Expr::Var(var.clone());
        let flattened = flatten_join_alias_vars(root, expr.clone());
        if flattened != expr {
            return lower_expr(ctx, flattened, mode);
        }
    }
    panic!(
        "unresolved whole-row Var {var:?} survived setrefs in mode {mode:?}; \
         executable plans should only contain executor-facing refs or allowed scan/system Vars"
    )
}

fn path_preserves_base_whole_row_slot(path: &Path, varno: usize) -> bool {
    match path {
        Path::SeqScan { source_id, .. }
        | Path::IndexOnlyScan { source_id, .. }
        | Path::IndexScan { source_id, .. }
        | Path::BitmapHeapScan { source_id, .. } => *source_id == varno,
        Path::Filter { input, .. }
        | Path::OrderBy { input, .. }
        | Path::IncrementalSort { input, .. }
        | Path::Limit { input, .. }
        | Path::LockRows { input, .. }
        | Path::Unique { input, .. } => path_preserves_base_whole_row_slot(input, varno),
        _ => false,
    }
}

fn executor_whole_row_var(varno: usize, var: &Var) -> Expr {
    Expr::Var(Var {
        varno,
        varattno: WHOLE_ROW_ATTR_NO,
        varlevelsup: 0,
        vartype: var.vartype,
        collation_oid: var.collation_oid,
    })
}

fn lower_whole_row_var_from_input_tlist(
    ctx: &SetRefsContext<'_>,
    var: &Var,
    input: &Path,
    tlist: &IndexedTlist,
) -> Option<Expr> {
    let descriptor = whole_row_descriptor_for_var(ctx, var)?;
    let mut fields = Vec::with_capacity(descriptor.fields.len());
    for (index, field) in descriptor.fields.iter().enumerate() {
        let attr_expr = Expr::Var(Var {
            varno: var.varno,
            varattno: user_attrno(index),
            varlevelsup: 0,
            vartype: field.sql_type,
            collation_oid: None,
        });
        let entry = search_input_tlist_entry(ctx.root, &attr_expr, input, tlist)?;
        fields.push((
            field.name.clone(),
            special_slot_var_for_expr(OUTER_VAR, entry, &attr_expr),
        ));
    }
    Some(null_preserving_whole_row_expr(descriptor, fields))
}

fn lower_whole_row_var_from_tlist(
    ctx: &SetRefsContext<'_>,
    var: &Var,
    slot_varno: usize,
    tlist: &IndexedTlist,
) -> Option<Expr> {
    let descriptor = whole_row_descriptor_for_var(ctx, var)?;
    let mut fields = Vec::with_capacity(descriptor.fields.len());
    for (index, field) in descriptor.fields.iter().enumerate() {
        let attr_expr = Expr::Var(Var {
            varno: var.varno,
            varattno: user_attrno(index),
            varlevelsup: 0,
            vartype: field.sql_type,
            collation_oid: None,
        });
        let entry = search_tlist_entry(ctx.root, &attr_expr, tlist)?;
        fields.push((
            field.name.clone(),
            special_slot_var_for_expr(slot_varno, entry, &attr_expr),
        ));
    }
    Some(null_preserving_whole_row_expr(descriptor, fields))
}

fn whole_row_descriptor_for_var(ctx: &SetRefsContext<'_>, var: &Var) -> Option<RecordDescriptor> {
    let mut sql_type = var.vartype;
    if (!matches!(sql_type.kind, SqlTypeKind::Composite) || sql_type.typrelid == 0)
        && let Some(catalog) = ctx.catalog
        && let Some(row) = catalog.type_by_oid(sql_type.type_oid)
        && row.typrelid != 0
    {
        sql_type = sql_type.with_identity(row.oid, row.typrelid);
    }
    if !matches!(sql_type.kind, SqlTypeKind::Composite) || sql_type.typrelid == 0 {
        return None;
    }
    let catalog_fields = ctx
        .catalog
        .and_then(|catalog| catalog.lookup_relation_by_oid(sql_type.typrelid))
        .map(|relation| {
            relation
                .desc
                .columns
                .iter()
                .filter(|column| !column.dropped)
                .map(|column| (column.name.clone(), column.sql_type))
                .collect::<Vec<_>>()
        });
    let catalog_field_count = catalog_fields.as_ref().map(Vec::len);
    let fields = ctx
        .root
        .and_then(|root| root.parse.rtable.get(var.varno.saturating_sub(1)))
        .map(|rte| {
            let mut fields = rte
                .desc
                .columns
                .iter()
                .filter(|column| !column.dropped)
                .map(|column| (column.name.clone(), column.sql_type))
                .collect::<Vec<_>>();
            if let Some(field_count) = catalog_field_count {
                fields.truncate(field_count);
            }
            fields
        })
        .filter(|fields| !fields.is_empty())
        .or(catalog_fields)?;
    Some(RecordDescriptor::named(
        sql_type.type_oid,
        sql_type.typrelid,
        sql_type.typmod,
        fields,
    ))
}

fn null_preserving_whole_row_expr(
    descriptor: RecordDescriptor,
    fields: Vec<(String, Expr)>,
) -> Expr {
    let row_expr = Expr::Row {
        descriptor: descriptor.clone(),
        fields: fields.clone(),
    };
    if fields.is_empty() {
        return row_expr;
    }
    Expr::Case(Box::new(pgrust_nodes::primnodes::CaseExpr {
        casetype: descriptor.sql_type(),
        arg: None,
        args: vec![pgrust_nodes::primnodes::CaseWhen {
            expr: Expr::Bool(Box::new(BoolExpr {
                boolop: BoolExprType::And,
                args: fields
                    .iter()
                    .map(|(_, expr)| Expr::IsNull(Box::new(expr.clone())))
                    .collect(),
            })),
            result: Expr::Const(Value::Null),
        }],
        defresult: Box::new(row_expr),
    }))
}

fn lower_index_scan_key(
    ctx: &mut SetRefsContext<'_>,
    key: IndexScanKey,
    mode: LowerMode<'_>,
) -> IndexScanKey {
    let runtime_label = match &key.argument {
        IndexScanKeyArgument::Runtime(expr) => key
            .runtime_label
            .clone()
            .or_else(|| label_for_runtime_expr(ctx, expr)),
        IndexScanKeyArgument::Const(_) => key.runtime_label.clone(),
    };
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
        runtime_label,
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
    desc: &pgrust_nodes::primnodes::RelationDesc,
    index_meta: &pgrust_nodes::relcache::IndexRelCacheEntry,
) -> bool {
    let Some(root) = ctx.root else {
        return false;
    };
    if !root.parse.row_marks.is_empty() {
        return false;
    }
    if !matches!(
        am_oid,
        pgrust_catalog_data::BTREE_AM_OID
            | pgrust_catalog_data::GIST_AM_OID
            | pgrust_catalog_data::SPGIST_AM_OID
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
        .enumerate()
        .filter_map(|(index_pos, attnum)| {
            if !index_scan_column_can_return(am_oid, index_meta, index_pos) {
                return None;
            }
            (*attnum > 0).then(|| usize::try_from(*attnum).ok()?.checked_sub(1))?
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

fn index_scan_column_can_return(
    am_oid: u32,
    index_meta: &pgrust_nodes::relcache::IndexRelCacheEntry,
    index_pos: usize,
) -> bool {
    if am_oid != pgrust_catalog_data::GIST_AM_OID {
        return true;
    }
    !matches!(
        index_meta.opfamily_oids.get(index_pos).copied(),
        Some(
            pgrust_catalog_data::GIST_POLY_FAMILY_OID | pgrust_catalog_data::GIST_CIRCLE_FAMILY_OID
        )
    ) && !matches!(
        index_meta.indclass.get(index_pos).copied(),
        Some(
            pgrust_catalog_data::POLY_GIST_OPCLASS_OID
                | pgrust_catalog_data::CIRCLE_GIST_OPCLASS_OID
        )
    ) && !matches!(
        index_meta.opcintype_oids.get(index_pos).copied(),
        Some(pgrust_catalog_data::POLYGON_TYPE_OID | pgrust_catalog_data::CIRCLE_TYPE_OID)
    )
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
        Expr::GroupingKey(grouping_key) => {
            expr_uses_only_index_keys(&grouping_key.expr, source_id, covered_columns)
        }
        Expr::GroupingFunc(grouping_func) => grouping_func
            .args
            .iter()
            .all(|arg| expr_uses_only_index_keys(arg, source_id, covered_columns)),
        Expr::Param(_)
        | Expr::Const(_)
        | Expr::CaseTest(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::User
        | Expr::SystemUser
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

fn collect_index_scan_key_exec_paramids(keys: &[IndexScanKey], out: &mut BTreeSet<usize>) {
    for key in keys {
        if let IndexScanKeyArgument::Runtime(expr) = &key.argument {
            collect_expr_exec_paramids(expr, out);
        }
        if let Some(display_expr) = &key.display_expr {
            collect_expr_exec_paramids(display_expr, out);
        }
    }
}

fn collect_order_by_exec_paramids(item: &OrderByEntry, out: &mut BTreeSet<usize>) {
    collect_expr_exec_paramids(&item.expr, out);
}

fn collect_aggref_exec_paramids(aggref: &Aggref, out: &mut BTreeSet<usize>) {
    aggref
        .direct_args
        .iter()
        .for_each(|arg| collect_expr_exec_paramids(arg, out));
    aggref
        .args
        .iter()
        .for_each(|arg| collect_expr_exec_paramids(arg, out));
    aggref
        .aggorder
        .iter()
        .for_each(|item| collect_order_by_exec_paramids(item, out));
    if let Some(filter) = &aggref.aggfilter {
        collect_expr_exec_paramids(filter, out);
    }
}

fn collect_agg_accum_exec_paramids(accum: &AggAccum, out: &mut BTreeSet<usize>) {
    accum
        .direct_args
        .iter()
        .for_each(|arg| collect_expr_exec_paramids(arg, out));
    accum
        .args
        .iter()
        .for_each(|arg| collect_expr_exec_paramids(arg, out));
    accum
        .order_by
        .iter()
        .for_each(|item| collect_order_by_exec_paramids(item, out));
    if let Some(filter) = &accum.filter {
        collect_expr_exec_paramids(filter, out);
    }
}

fn collect_window_func_exec_paramids(window_func: &WindowFuncExpr, out: &mut BTreeSet<usize>) {
    window_func
        .args
        .iter()
        .for_each(|arg| collect_expr_exec_paramids(arg, out));
    if let WindowFuncKind::Aggregate(aggref) = &window_func.kind {
        collect_aggref_exec_paramids(aggref, out);
    }
}

fn collect_window_clause_exec_paramids(clause: &WindowClause, out: &mut BTreeSet<usize>) {
    clause
        .spec
        .partition_by
        .iter()
        .for_each(|expr| collect_expr_exec_paramids(expr, out));
    clause
        .spec
        .order_by
        .iter()
        .for_each(|item| collect_order_by_exec_paramids(item, out));
    clause
        .functions
        .iter()
        .for_each(|func| collect_window_func_exec_paramids(func, out));
}

fn collect_set_returning_call_exec_paramids(
    call: &pgrust_nodes::primnodes::SetReturningCall,
    out: &mut BTreeSet<usize>,
) {
    set_returning_call_exprs(call)
        .into_iter()
        .for_each(|expr| collect_expr_exec_paramids(expr, out));
}

fn collect_set_returning_call_external_exec_paramids(
    call: &pgrust_nodes::primnodes::SetReturningCall,
    bound: &BTreeSet<usize>,
    out: &mut BTreeSet<usize>,
) {
    set_returning_call_exprs(call)
        .into_iter()
        .for_each(|expr| collect_external_expr_exec_paramids(expr, bound, out));
}

fn collect_expr_exec_paramids(expr: &Expr, out: &mut BTreeSet<usize>) {
    match expr {
        Expr::Param(Param {
            paramkind: ParamKind::Exec,
            paramid,
            ..
        }) => {
            out.insert(*paramid);
        }
        Expr::Aggref(aggref) => collect_aggref_exec_paramids(aggref, out),
        Expr::WindowFunc(window_func) => collect_window_func_exec_paramids(window_func, out),
        Expr::Op(op) => op
            .args
            .iter()
            .for_each(|arg| collect_expr_exec_paramids(arg, out)),
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .for_each(|arg| collect_expr_exec_paramids(arg, out)),
        Expr::Case(case_expr) => {
            if let Some(arg) = &case_expr.arg {
                collect_expr_exec_paramids(arg, out);
            }
            case_expr.args.iter().for_each(|arm| {
                collect_expr_exec_paramids(&arm.expr, out);
                collect_expr_exec_paramids(&arm.result, out);
            });
            collect_expr_exec_paramids(&case_expr.defresult, out);
        }
        Expr::Func(func) => func
            .args
            .iter()
            .for_each(|arg| collect_expr_exec_paramids(arg, out)),
        Expr::SqlJsonQueryFunction(func) => func
            .child_exprs()
            .into_iter()
            .for_each(|expr| collect_expr_exec_paramids(expr, out)),
        Expr::SetReturning(srf) => collect_set_returning_call_exec_paramids(&srf.call, out),
        Expr::SubLink(sublink) => {
            if let Some(testexpr) = &sublink.testexpr {
                collect_expr_exec_paramids(testexpr, out);
            }
        }
        Expr::SubPlan(subplan) => {
            // par_param ids are subplan-local slots populated from args at
            // execution; the containing plan only depends directly on args.
            if let Some(testexpr) = &subplan.testexpr {
                collect_expr_exec_paramids(testexpr, out);
            }
            subplan
                .args
                .iter()
                .for_each(|arg| collect_expr_exec_paramids(arg, out));
        }
        Expr::ScalarArrayOp(saop) => {
            collect_expr_exec_paramids(&saop.left, out);
            collect_expr_exec_paramids(&saop.right, out);
        }
        Expr::Xml(xml) => xml
            .child_exprs()
            .for_each(|expr| collect_expr_exec_paramids(expr, out)),
        Expr::Cast(inner, _)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::FieldSelect { expr: inner, .. } => collect_expr_exec_paramids(inner, out),
        Expr::Collate { expr: inner, .. } => collect_expr_exec_paramids(inner, out),
        Expr::Coalesce(left, right)
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right) => {
            collect_expr_exec_paramids(left, out);
            collect_expr_exec_paramids(right, out);
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
            collect_expr_exec_paramids(expr, out);
            collect_expr_exec_paramids(pattern, out);
            if let Some(escape) = escape {
                collect_expr_exec_paramids(escape, out);
            }
        }
        Expr::ArrayLiteral { elements, .. } => elements
            .iter()
            .for_each(|element| collect_expr_exec_paramids(element, out)),
        Expr::Row { fields, .. } => fields
            .iter()
            .for_each(|(_, expr)| collect_expr_exec_paramids(expr, out)),
        Expr::ArraySubscript { array, subscripts } => {
            collect_expr_exec_paramids(array, out);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    collect_expr_exec_paramids(lower, out);
                }
                if let Some(upper) = &subscript.upper {
                    collect_expr_exec_paramids(upper, out);
                }
            }
        }
        _ => {}
    }
}

fn collect_external_expr_exec_paramids(
    expr: &Expr,
    bound: &BTreeSet<usize>,
    out: &mut BTreeSet<usize>,
) {
    let mut params = BTreeSet::new();
    collect_expr_exec_paramids(expr, &mut params);
    out.extend(
        params
            .into_iter()
            .filter(|paramid| !bound.contains(paramid)),
    );
}

fn collect_index_scan_key_external_exec_paramids(
    keys: &[IndexScanKey],
    bound: &BTreeSet<usize>,
    out: &mut BTreeSet<usize>,
) {
    for key in keys {
        if let IndexScanKeyArgument::Runtime(expr) = &key.argument {
            collect_external_expr_exec_paramids(expr, bound, out);
        }
        if let Some(display_expr) = &key.display_expr {
            collect_external_expr_exec_paramids(display_expr, bound, out);
        }
    }
}

fn collect_tid_scan_external_exec_paramids(
    tid_cond: &TidScanCond,
    filter: Option<&Expr>,
    bound: &BTreeSet<usize>,
    out: &mut BTreeSet<usize>,
) {
    for source in &tid_cond.sources {
        match source {
            TidScanSource::Scalar(expr) | TidScanSource::Array(expr) => {
                collect_external_expr_exec_paramids(expr, bound, out);
            }
        }
    }
    collect_external_expr_exec_paramids(&tid_cond.display_expr, bound, out);
    if let Some(filter) = filter {
        collect_external_expr_exec_paramids(filter, bound, out);
    }
}

fn collect_plan_external_exec_paramids(
    plan: &Plan,
    bound: &BTreeSet<usize>,
    out: &mut BTreeSet<usize>,
) {
    match plan {
        Plan::Result { .. } | Plan::SeqScan { .. } | Plan::WorkTableScan { .. } => {}
        Plan::TidScan {
            tid_cond, filter, ..
        } => collect_tid_scan_external_exec_paramids(tid_cond, filter.as_ref(), bound, out),
        Plan::TidRangeScan {
            tid_range_cond,
            filter,
            ..
        } => {
            collect_external_expr_exec_paramids(&tid_range_cond.display_expr, bound, out);
            if let Some(filter) = filter {
                collect_external_expr_exec_paramids(filter, bound, out);
            }
        }
        Plan::Append { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::BitmapAnd { children, .. }
        | Plan::SetOp { children, .. } => {
            children
                .iter()
                .for_each(|child| collect_plan_external_exec_paramids(child, bound, out));
        }
        Plan::MergeAppend {
            items, children, ..
        } => {
            items
                .iter()
                .for_each(|item| collect_external_expr_exec_paramids(&item.expr, bound, out));
            children
                .iter()
                .for_each(|child| collect_plan_external_exec_paramids(child, bound, out));
        }
        Plan::Unique { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::CteScan {
            cte_plan: input, ..
        } => collect_plan_external_exec_paramids(input, bound, out),
        Plan::OrderBy { input, items, .. } | Plan::IncrementalSort { input, items, .. } => {
            collect_plan_external_exec_paramids(input, bound, out);
            items
                .iter()
                .for_each(|item| collect_external_expr_exec_paramids(&item.expr, bound, out));
        }
        Plan::SubqueryScan { input, filter, .. } => {
            collect_plan_external_exec_paramids(input, bound, out);
            if let Some(filter) = filter {
                collect_external_expr_exec_paramids(filter, bound, out);
            }
        }
        Plan::Filter {
            input, predicate, ..
        } => {
            collect_plan_external_exec_paramids(input, bound, out);
            collect_external_expr_exec_paramids(predicate, bound, out);
        }
        Plan::Projection { input, targets, .. } => {
            collect_plan_external_exec_paramids(input, bound, out);
            targets
                .iter()
                .for_each(|target| collect_external_expr_exec_paramids(&target.expr, bound, out));
        }
        Plan::ProjectSet { input, targets, .. } => {
            collect_plan_external_exec_paramids(input, bound, out);
            targets.iter().for_each(|target| {
                use pgrust_nodes::primnodes::ProjectSetTarget;
                match target {
                    ProjectSetTarget::Scalar(entry) => {
                        collect_external_expr_exec_paramids(&entry.expr, bound, out)
                    }
                    ProjectSetTarget::Set { call, .. } => {
                        collect_set_returning_call_external_exec_paramids(call, bound, out)
                    }
                }
            });
        }
        Plan::IndexOnlyScan {
            keys,
            order_by_keys,
            ..
        }
        | Plan::IndexScan {
            keys,
            order_by_keys,
            ..
        } => {
            collect_index_scan_key_external_exec_paramids(keys, bound, out);
            collect_index_scan_key_external_exec_paramids(order_by_keys, bound, out);
        }
        Plan::BitmapIndexScan {
            keys, index_quals, ..
        } => {
            collect_index_scan_key_external_exec_paramids(keys, bound, out);
            index_quals
                .iter()
                .for_each(|expr| collect_external_expr_exec_paramids(expr, bound, out));
        }
        Plan::BitmapHeapScan {
            bitmapqual,
            recheck_qual,
            filter_qual,
            ..
        } => {
            collect_plan_external_exec_paramids(bitmapqual, bound, out);
            recheck_qual
                .iter()
                .for_each(|expr| collect_external_expr_exec_paramids(expr, bound, out));
            filter_qual
                .iter()
                .for_each(|expr| collect_external_expr_exec_paramids(expr, bound, out));
        }
        Plan::Hash {
            input, hash_keys, ..
        } => {
            collect_plan_external_exec_paramids(input, bound, out);
            hash_keys
                .iter()
                .for_each(|expr| collect_external_expr_exec_paramids(expr, bound, out));
        }
        Plan::Memoize {
            input, cache_keys, ..
        } => {
            collect_plan_external_exec_paramids(input, bound, out);
            cache_keys
                .iter()
                .for_each(|expr| collect_external_expr_exec_paramids(expr, bound, out));
        }
        Plan::NestedLoopJoin {
            left,
            right,
            nest_params,
            join_qual,
            qual,
            ..
        } => {
            collect_plan_external_exec_paramids(left, bound, out);
            nest_params
                .iter()
                .for_each(|param| collect_external_expr_exec_paramids(&param.expr, bound, out));
            join_qual
                .iter()
                .for_each(|expr| collect_external_expr_exec_paramids(expr, bound, out));
            qual.iter()
                .for_each(|expr| collect_external_expr_exec_paramids(expr, bound, out));
            let mut right_bound = bound.clone();
            right_bound.extend(nest_params.iter().map(|param| param.paramid));
            collect_plan_external_exec_paramids(right, &right_bound, out);
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
            collect_plan_external_exec_paramids(left, bound, out);
            collect_plan_external_exec_paramids(right, bound, out);
            hash_clauses
                .iter()
                .for_each(|expr| collect_external_expr_exec_paramids(expr, bound, out));
            hash_keys
                .iter()
                .for_each(|expr| collect_external_expr_exec_paramids(expr, bound, out));
            join_qual
                .iter()
                .for_each(|expr| collect_external_expr_exec_paramids(expr, bound, out));
            qual.iter()
                .for_each(|expr| collect_external_expr_exec_paramids(expr, bound, out));
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
            collect_plan_external_exec_paramids(left, bound, out);
            collect_plan_external_exec_paramids(right, bound, out);
            merge_clauses
                .iter()
                .for_each(|expr| collect_external_expr_exec_paramids(expr, bound, out));
            outer_merge_keys
                .iter()
                .for_each(|expr| collect_external_expr_exec_paramids(expr, bound, out));
            inner_merge_keys
                .iter()
                .for_each(|expr| collect_external_expr_exec_paramids(expr, bound, out));
            join_qual
                .iter()
                .for_each(|expr| collect_external_expr_exec_paramids(expr, bound, out));
            qual.iter()
                .for_each(|expr| collect_external_expr_exec_paramids(expr, bound, out));
        }
        Plan::Aggregate {
            input,
            group_by,
            group_by_refs: _,
            grouping_sets: _,
            passthrough_exprs,
            accumulators,
            having,
            ..
        } => {
            collect_plan_external_exec_paramids(input, bound, out);
            group_by
                .iter()
                .for_each(|expr| collect_external_expr_exec_paramids(expr, bound, out));
            passthrough_exprs
                .iter()
                .for_each(|expr| collect_external_expr_exec_paramids(expr, bound, out));
            accumulators
                .iter()
                .for_each(|accum| collect_agg_accum_exec_paramids(accum, out));
            if let Some(having) = having {
                collect_external_expr_exec_paramids(having, bound, out);
            }
        }
        Plan::WindowAgg {
            input,
            clause,
            run_condition,
            top_qual,
            ..
        } => {
            collect_plan_external_exec_paramids(input, bound, out);
            collect_window_clause_exec_paramids(clause, out);
            if let Some(run_condition) = run_condition {
                collect_external_expr_exec_paramids(run_condition, bound, out);
            }
            if let Some(top_qual) = top_qual {
                collect_external_expr_exec_paramids(top_qual, bound, out);
            }
        }
        Plan::FunctionScan { call, .. } => {
            collect_set_returning_call_external_exec_paramids(call, bound, out)
        }
        Plan::Values { rows, .. } => rows
            .iter()
            .flatten()
            .for_each(|expr| collect_external_expr_exec_paramids(expr, bound, out)),
        Plan::RecursiveUnion {
            anchor, recursive, ..
        } => {
            collect_plan_external_exec_paramids(anchor, bound, out);
            collect_plan_external_exec_paramids(recursive, bound, out);
        }
    }
}

fn collect_plan_exec_paramids(plan: &Plan, out: &mut BTreeSet<usize>) {
    collect_plan_external_exec_paramids(plan, &BTreeSet::new(), out);
}

fn plan_contains_exec_param_id(plan: &Plan, target_paramid: usize) -> bool {
    let mut params = BTreeSet::new();
    collect_plan_exec_paramids(plan, &mut params);
    params.contains(&target_paramid)
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
        Expr::GroupingKey(grouping_key) => {
            validate_executable_expr(&grouping_key.expr, plan_node, field, allowed_exec_params);
        }
        Expr::GroupingFunc(grouping_func) => {
            for arg in &grouping_func.args {
                validate_executable_expr(arg, plan_node, field, allowed_exec_params);
            }
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
        | Expr::User
        | Expr::SystemUser
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
    call: &pgrust_nodes::primnodes::SetReturningCall,
    plan_node: &str,
    field: &str,
    allowed_exec_params: &BTreeSet<usize>,
) {
    use pgrust_nodes::primnodes::SetReturningCall;

    match call {
        SetReturningCall::RowsFrom { items, .. } => {
            for item in items {
                match &item.source {
                    RowsFromSource::Function(call) => {
                        validate_set_returning_call(call, plan_node, field, allowed_exec_params);
                    }
                    RowsFromSource::Project { output_exprs, .. } => {
                        for expr in output_exprs {
                            validate_executable_expr(expr, plan_node, field, allowed_exec_params);
                        }
                    }
                }
            }
        }
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
        SetReturningCall::PgLockStatus { .. }
        | SetReturningCall::PgStatProgressCopy { .. }
        | SetReturningCall::PgSequences { .. }
        | SetReturningCall::InformationSchemaSequences { .. } => {}
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
    accum: &pgrust_nodes::primnodes::AggAccum,
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
        Plan::TidScan {
            tid_cond, filter, ..
        } => {
            for source in &tid_cond.sources {
                match source {
                    TidScanSource::Scalar(expr) => {
                        validate_executable_expr(expr, "TidScan", "tid_cond", allowed_exec_params)
                    }
                    TidScanSource::Array(expr) => {
                        validate_executable_expr(expr, "TidScan", "tid_cond", allowed_exec_params)
                    }
                }
            }
            validate_executable_expr(
                &tid_cond.display_expr,
                "TidScan",
                "display_expr",
                allowed_exec_params,
            );
            if let Some(filter) = filter {
                validate_executable_expr(filter, "TidScan", "filter", allowed_exec_params);
            }
        }
        Plan::TidRangeScan {
            tid_range_cond,
            filter,
            ..
        } => {
            validate_executable_expr(
                &tid_range_cond.display_expr,
                "TidRangeScan",
                "display_expr",
                allowed_exec_params,
            );
            if let Some(filter) = filter {
                validate_executable_expr(filter, "TidRangeScan", "filter", allowed_exec_params);
            }
        }
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
        Plan::BitmapOr { children, .. } | Plan::BitmapAnd { children, .. } => {
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
        Plan::Gather { input, .. } | Plan::GatherMerge { input, .. } => {
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
        Plan::Limit {
            input,
            limit,
            offset,
            ..
        } => {
            if let Some(limit) = limit {
                validate_executable_expr(limit, "Limit", "limit", allowed_exec_params);
            }
            if let Some(offset) = offset {
                validate_executable_expr(offset, "Limit", "offset", allowed_exec_params);
            }
            validate_executable_plan_with_params(input, allowed_exec_params);
        }
        Plan::LockRows { input, .. } => {
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
            group_by_refs: _,
            grouping_sets: _,
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
        Plan::WindowAgg {
            input,
            clause,
            run_condition,
            top_qual,
            ..
        } => {
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
            if let Some(run_condition) = run_condition {
                validate_executable_expr(
                    run_condition,
                    "WindowAgg",
                    "run_condition",
                    allowed_exec_params,
                );
            }
            if let Some(top_qual) = top_qual {
                validate_executable_expr(top_qual, "WindowAgg", "top_qual", allowed_exec_params);
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
                    pgrust_nodes::primnodes::ProjectSetTarget::Scalar(entry) => {
                        validate_executable_expr(
                            &entry.expr,
                            "ProjectSet",
                            "targets",
                            allowed_exec_params,
                        );
                    }
                    pgrust_nodes::primnodes::ProjectSetTarget::Set { call, .. } => {
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
        Expr::GroupingKey(grouping_key) => {
            validate_planner_expr(&grouping_key.expr, path_node, field)
        }
        Expr::GroupingFunc(grouping_func) => {
            for arg in &grouping_func.args {
                validate_planner_expr(arg, path_node, field);
            }
        }
        Expr::Param(Param {
            paramkind: ParamKind::Exec,
            ..
        }) => panic!("planner path contains PARAM_EXEC in {path_node}.{field}: {expr:?}"),
        Expr::Param(Param {
            paramkind: ParamKind::External,
            ..
        }) => {}
        Expr::WindowFunc(window_func) => {
            for arg in &window_func.args {
                validate_planner_expr(arg, path_node, field);
            }
            if let pgrust_nodes::primnodes::WindowFuncKind::Aggregate(aggref) = &window_func.kind {
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
        | Expr::User
        | Expr::SystemUser
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
    call: &pgrust_nodes::primnodes::SetReturningCall,
    path_node: &str,
    field: &str,
) {
    use pgrust_nodes::primnodes::SetReturningCall;

    match call {
        SetReturningCall::RowsFrom { items, .. } => {
            for item in items {
                match &item.source {
                    RowsFromSource::Function(call) => {
                        validate_planner_set_returning_call(call, path_node, field);
                    }
                    RowsFromSource::Project { output_exprs, .. } => {
                        for expr in output_exprs {
                            validate_planner_expr(expr, path_node, field);
                        }
                    }
                }
            }
        }
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
        SetReturningCall::PgLockStatus { .. }
        | SetReturningCall::PgStatProgressCopy { .. }
        | SetReturningCall::PgSequences { .. }
        | SetReturningCall::InformationSchemaSequences { .. } => {}
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
    accum: &pgrust_nodes::primnodes::AggAccum,
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
        Path::BitmapOr { children, .. } | Path::BitmapAnd { children, .. } => {
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
        Path::WindowAgg {
            input,
            clause,
            run_condition,
            top_qual,
            ..
        } => {
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
            if let Some(run_condition) = run_condition {
                validate_planner_expr(run_condition, "WindowAgg", "run_condition");
            }
            if let Some(top_qual) = top_qual {
                validate_planner_expr(top_qual, "WindowAgg", "top_qual");
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
                    pgrust_nodes::primnodes::ProjectSetTarget::Scalar(entry) => {
                        validate_planner_expr(&entry.expr, "ProjectSet", "targets");
                    }
                    pgrust_nodes::primnodes::ProjectSetTarget::Set { call, .. } => {
                        validate_planner_set_returning_call(call, "ProjectSet", "targets");
                    }
                }
            }
            validate_planner_path(input);
        }
    }
}

pub fn validate_executable_plan_for_tests(plan: &Plan) {
    validate_executable_plan(plan);
}

pub fn validate_executable_plan_for_tests_with_params(plan: &Plan, params: &[ExecParamSource]) {
    let allowed_params = exec_param_sources(params);
    validate_executable_plan_with_params(plan, &allowed_params);
}

pub fn validate_planner_path_for_tests(path: &Path) {
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
        Path::SeqScan {
            plan_info: seq_plan_info,
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
            pathtarget: _,
        } => {
            if tablesample.is_none()
                && let Some(spec) = extract_tid_scan_spec(&predicate, source_id)
            {
                Plan::TidScan {
                    plan_info,
                    source_id,
                    rel,
                    relation_name,
                    relation_oid,
                    relkind,
                    relispopulated,
                    toast,
                    desc,
                    tid_cond: TidScanCond {
                        sources: spec.sources,
                        display_expr: spec.display_expr,
                    },
                    filter: spec.filter,
                }
            } else if tablesample.is_none()
                && let Some(spec) = extract_tid_range_scan_spec(&predicate, source_id)
            {
                Plan::TidRangeScan {
                    plan_info,
                    source_id,
                    rel,
                    relation_name,
                    relation_oid,
                    relkind,
                    relispopulated,
                    toast,
                    desc,
                    tid_range_cond: TidRangeScanCond {
                        display_expr: spec.display_expr,
                    },
                    filter: spec.filter,
                }
            } else {
                Plan::Filter {
                    plan_info,
                    input: Box::new(Plan::SeqScan {
                        plan_info: seq_plan_info,
                        source_id,
                        parallel_scan_id: None,
                        rel,
                        relation_name,
                        relation_oid,
                        relkind,
                        relispopulated,
                        disabled,
                        toast,
                        tablesample,
                        desc,
                        parallel_aware: false,
                    }),
                    predicate,
                }
            }
        }
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

#[derive(Debug)]
struct TidScanSpec {
    sources: Vec<TidScanSource>,
    display_expr: Expr,
    filter: Option<Expr>,
}

#[derive(Debug)]
struct TidScanBranch {
    sources: Vec<TidScanSource>,
    display_expr: Expr,
    residual: Option<Expr>,
}

#[derive(Debug)]
struct TidRangeScanSpec {
    display_expr: Expr,
    filter: Option<Expr>,
}

fn extract_tid_range_scan_spec(predicate: &Expr, source_id: usize) -> Option<TidRangeScanSpec> {
    let conjuncts = flatten_and_conjuncts(predicate);
    let mut range_exprs = Vec::new();
    let mut residual = Vec::new();
    for conjunct in conjuncts {
        if is_tid_range_clause(&conjunct, source_id) {
            range_exprs.push(conjunct);
        } else {
            residual.push(conjunct);
        }
    }
    if range_exprs.is_empty() {
        return None;
    }
    let display_expr = combine_bool_exprs(BoolExprType::And, range_exprs);
    let filter = if residual.is_empty() {
        None
    } else {
        Some(predicate.clone())
    };
    Some(TidRangeScanSpec {
        display_expr,
        filter,
    })
}

fn is_tid_range_clause(expr: &Expr, source_id: usize) -> bool {
    match expr {
        Expr::Op(op)
            if matches!(
                op.op,
                OpExprKind::Lt | OpExprKind::LtEq | OpExprKind::Gt | OpExprKind::GtEq
            ) && op.args.len() == 2 =>
        {
            is_ctid_var(&op.args[0], source_id) || is_ctid_var(&op.args[1], source_id)
        }
        _ => false,
    }
}

fn extract_tid_scan_spec(predicate: &Expr, source_id: usize) -> Option<TidScanSpec> {
    let disjuncts = flatten_or_disjuncts(predicate);
    if disjuncts.len() > 1 {
        let mut sources = Vec::new();
        let mut display_exprs = Vec::new();
        let mut needs_full_filter = false;
        for disjunct in &disjuncts {
            let branch = extract_tid_scan_branch(disjunct, source_id)?;
            sources.extend(branch.sources);
            display_exprs.push(branch.display_expr);
            needs_full_filter |= branch.residual.is_some();
        }
        return Some(TidScanSpec {
            sources,
            display_expr: combine_bool_exprs(BoolExprType::Or, display_exprs),
            filter: needs_full_filter.then(|| predicate.clone()),
        });
    }

    extract_tid_scan_branch(predicate, source_id).map(|branch| TidScanSpec {
        sources: branch.sources,
        display_expr: branch.display_expr,
        filter: branch.residual,
    })
}

fn extract_tid_scan_branch(expr: &Expr, source_id: usize) -> Option<TidScanBranch> {
    if let Some((source, display_expr)) = extract_tid_scan_source(expr, source_id) {
        return Some(TidScanBranch {
            sources: vec![source],
            display_expr,
            residual: None,
        });
    }

    let conjuncts = flatten_and_conjuncts(expr);
    if conjuncts.len() <= 1 {
        return None;
    }

    let mut tid_parts = Vec::new();
    let mut residual = Vec::new();
    for conjunct in conjuncts {
        if let Some((source, display_expr)) = extract_tid_scan_source(&conjunct, source_id) {
            tid_parts.push((source, display_expr));
        } else {
            residual.push(conjunct);
        }
    }
    if tid_parts.is_empty() {
        return None;
    }

    let mut sources = Vec::new();
    let mut display_exprs = Vec::new();
    for (source, display_expr) in tid_parts {
        sources.push(source);
        display_exprs.push(display_expr);
    }
    let residual = if display_exprs.len() > 1 {
        Some(expr.clone())
    } else if residual.is_empty() {
        None
    } else {
        Some(combine_bool_exprs(BoolExprType::And, residual))
    };

    Some(TidScanBranch {
        sources,
        display_expr: combine_bool_exprs(BoolExprType::And, display_exprs),
        residual,
    })
}

fn extract_tid_scan_source(expr: &Expr, source_id: usize) -> Option<(TidScanSource, Expr)> {
    match expr {
        Expr::Op(op) if op.op == OpExprKind::Eq && op.args.len() == 2 => {
            let left = &op.args[0];
            let right = &op.args[1];
            if is_ctid_var(left, source_id) {
                Some((TidScanSource::Scalar(right.clone()), expr.clone()))
            } else if is_ctid_var(right, source_id) {
                Some((TidScanSource::Scalar(left.clone()), expr.clone()))
            } else {
                None
            }
        }
        Expr::ScalarArrayOp(saop)
            if saop.use_or
                && saop.op == SubqueryComparisonOp::Eq
                && is_ctid_var(&saop.left, source_id) =>
        {
            Some((TidScanSource::Array((*saop.right).clone()), expr.clone()))
        }
        _ => None,
    }
}

fn is_ctid_var(expr: &Expr, source_id: usize) -> bool {
    matches!(
        expr,
        Expr::Var(var)
            if var.varlevelsup == 0
                && var.varno == source_id
                && var.varattno == SELF_ITEM_POINTER_ATTR_NO
    )
}

fn flatten_or_disjuncts(expr: &Expr) -> Vec<Expr> {
    match expr {
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::Or => bool_expr
            .args
            .iter()
            .flat_map(flatten_or_disjuncts)
            .collect(),
        other => vec![other.clone()],
    }
}

fn combine_bool_exprs(boolop: BoolExprType, mut exprs: Vec<Expr>) -> Expr {
    if exprs.len() == 1 {
        return exprs.remove(0);
    }
    Expr::Bool(Box::new(BoolExpr {
        boolop,
        args: exprs,
    }))
}

fn set_append_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    source_id: usize,
    desc: pgrust_nodes::primnodes::RelationDesc,
    relids: Vec<usize>,
    child_roots: Vec<Option<PlannerSubroot>>,
    partition_prune: Option<PartitionPrunePlan>,
    children: Vec<Path>,
) -> Plan {
    assert!(
        child_roots.is_empty() || child_roots.len() == children.len(),
        "append child root count {} did not match child count {}",
        child_roots.len(),
        children.len()
    );
    let relation_append_alias = (relids.as_slice() == [source_id])
        .then(|| append_source_alias(ctx, source_id))
        .flatten();
    let child_count = children.len();
    let partition_prune = lower_partition_prune_plan(ctx, partition_prune);
    let children = children
        .into_iter()
        .enumerate()
        .map(|(index, child)| {
            let child_root = child_roots
                .get(index)
                .and_then(Option::as_ref)
                .map(PlannerSubroot::as_ref)
                .or(ctx.root);
            let mut child_plan = recurse_with_root(ctx, child_root, child);
            if let Some(alias) = relation_append_alias.as_deref() {
                let child_alias = if child_count == 1 {
                    alias.to_string()
                } else {
                    format!("{}_{}", alias, index + 1)
                };
                apply_single_append_scan_alias(&mut child_plan, &child_alias);
            }
            child_plan
        })
        .collect();
    let (partition_prune, mut children) =
        flatten_partition_append_children(partition_prune, children);
    if partition_prune.is_some()
        && let Some(alias) = relation_append_alias.as_deref()
    {
        for (index, child) in children.iter_mut().enumerate() {
            apply_single_append_scan_alias(child, &format!("{alias}_{}", index + 1));
        }
    }
    Plan::Append {
        plan_info,
        source_id,
        desc,
        parallel_aware: false,
        partition_prune,
        children,
    }
}

fn partition_prune_child_domains(
    info: &PartitionPrunePlan,
    child_index: usize,
) -> Vec<PartitionPruneChildDomain> {
    info.child_domains
        .get(child_index)
        .filter(|domains| !domains.is_empty())
        .cloned()
        .unwrap_or_else(|| {
            vec![PartitionPruneChildDomain {
                spec: info.spec.clone(),
                sibling_bounds: info.sibling_bounds.clone(),
                bound: info.child_bounds.get(child_index).cloned().flatten(),
            }]
        })
}

fn flatten_partition_append_children(
    mut partition_prune: Option<PartitionPrunePlan>,
    children: Vec<Plan>,
) -> (Option<PartitionPrunePlan>, Vec<Plan>) {
    let Some(info) = partition_prune.as_mut() else {
        return (partition_prune, children);
    };
    if children.is_empty() {
        return (partition_prune, children);
    }

    let mut flattened_children = Vec::new();
    let mut flattened_bounds = Vec::new();
    let mut flattened_domains = Vec::new();
    let mut changed = false;

    for (index, child) in children.into_iter().enumerate() {
        let parent_domains = partition_prune_child_domains(info, index);
        let parent_bound = info.child_bounds.get(index).cloned().flatten();
        match child {
            Plan::Append {
                partition_prune: Some(child_prune),
                children: nested_children,
                ..
            } if !nested_children.is_empty() => {
                changed = true;
                for (nested_index, nested_child) in nested_children.into_iter().enumerate() {
                    let mut domains = parent_domains.clone();
                    domains.extend(partition_prune_child_domains(&child_prune, nested_index));
                    flattened_domains.push(domains);
                    flattened_bounds.push(parent_bound.clone());
                    flattened_children.push(nested_child);
                }
            }
            Plan::Projection {
                plan_info,
                input,
                targets,
            } => match *input {
                Plan::Append {
                    partition_prune: Some(child_prune),
                    children: nested_children,
                    ..
                } if !nested_children.is_empty() => {
                    changed = true;
                    for (nested_index, nested_child) in nested_children.into_iter().enumerate() {
                        let mut domains = parent_domains.clone();
                        domains.extend(translate_partition_prune_child_domains_through_projection(
                            partition_prune_child_domains(&child_prune, nested_index),
                            &targets,
                        ));
                        flattened_domains.push(domains);
                        flattened_bounds.push(parent_bound.clone());
                        flattened_children.push(Plan::Projection {
                            plan_info,
                            input: Box::new(nested_child),
                            targets: targets.clone(),
                        });
                    }
                }
                input => {
                    flattened_domains.push(parent_domains);
                    flattened_bounds.push(parent_bound);
                    flattened_children.push(Plan::Projection {
                        plan_info,
                        input: Box::new(input),
                        targets,
                    });
                }
            },
            other => {
                flattened_domains.push(parent_domains);
                flattened_bounds.push(parent_bound);
                flattened_children.push(other);
            }
        }
    }

    if changed {
        info.child_bounds = flattened_bounds;
        info.child_domains = flattened_domains;
    }
    (partition_prune, flattened_children)
}

fn translate_partition_prune_child_domains_through_projection(
    domains: Vec<PartitionPruneChildDomain>,
    targets: &[TargetEntry],
) -> Vec<PartitionPruneChildDomain> {
    domains
        .into_iter()
        .map(|mut domain| {
            // :HACK: flattened partition Appends use the parent output slot for
            // their single prune filter. Nested child partition specs are built
            // in child physical column order, so translate key Vars through the
            // projection that maps the child slot back to the parent slot until
            // path-level prune domains carry their own filter expression.
            domain.spec = translate_partition_spec_through_projection(domain.spec, targets);
            domain
        })
        .collect()
}

fn translate_partition_spec_through_projection(
    mut spec: pgrust_analyze::LoweredPartitionSpec,
    targets: &[TargetEntry],
) -> pgrust_analyze::LoweredPartitionSpec {
    spec.key_exprs = spec
        .key_exprs
        .iter()
        .map(|expr| translate_partition_key_expr_through_projection(expr, targets))
        .collect();
    spec.partattrs = spec
        .key_exprs
        .iter()
        .map(|expr| match expr {
            Expr::Var(var) => var.varattno as i16,
            _ => 0,
        })
        .collect();
    spec
}

fn translate_partition_key_expr_through_projection(expr: &Expr, targets: &[TargetEntry]) -> Expr {
    match expr {
        Expr::Var(var) if var.varlevelsup == 0 => {
            let Some((index, target)) = targets
                .iter()
                .filter(|target| !target.resjunk)
                .enumerate()
                .find(|(_, target)| {
                    matches!(
                        &target.expr,
                        Expr::Var(target_var)
                            if target_var.varlevelsup == 0
                                && target_var.varattno == var.varattno
                    )
                })
            else {
                return expr.clone();
            };
            Expr::Var(Var {
                varno: var.varno,
                varattno: user_attrno(index),
                varlevelsup: 0,
                vartype: target.sql_type,
                collation_oid: None,
            })
        }
        _ => expr.clone(),
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
        | Plan::TidScan { relation_name, .. }
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

fn lower_partition_prune_plan(
    ctx: &mut SetRefsContext<'_>,
    partition_prune: Option<PartitionPrunePlan>,
) -> Option<PartitionPrunePlan> {
    partition_prune.map(|mut info| {
        info.filter = lower_partition_prune_expr(ctx, info.filter);
        info
    })
}

fn lower_partition_prune_expr(ctx: &mut SetRefsContext<'_>, expr: Expr) -> Expr {
    match expr {
        Expr::Var(var) if var.varlevelsup > 0 => exec_param_for_outer_expr(ctx, Expr::Var(var)),
        Expr::SubLink(sublink) if partition_prune_sublink_has_local_testexpr(&sublink) => {
            // SubLink testexprs can reference partition keys as semantic Vars.
            // Keep those in planner form; pruning ignores them conservatively.
            Expr::SubLink(sublink)
        }
        Expr::SubLink(sublink) => lower_sublink(ctx, *sublink, LowerMode::Scalar),
        Expr::SubPlan(subplan) => Expr::SubPlan(Box::new(SubPlan {
            sublink_type: subplan.sublink_type,
            testexpr: subplan
                .testexpr
                .map(|expr| Box::new(lower_partition_prune_expr(ctx, *expr))),
            comparison: subplan.comparison,
            first_col_type: subplan.first_col_type,
            target_width: subplan.target_width,
            target_attnos: subplan.target_attnos,
            plan_id: subplan.plan_id,
            is_initplan: subplan.is_initplan,
            set_params: subplan.set_params,
            par_param: subplan.par_param,
            args: subplan
                .args
                .into_iter()
                .map(|expr| lower_partition_prune_expr(ctx, expr))
                .collect(),
        })),
        Expr::Op(op) => Expr::Op(Box::new(OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| lower_partition_prune_expr(ctx, arg))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| lower_partition_prune_expr(ctx, arg))
                .collect(),
            ..*bool_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| lower_partition_prune_expr(ctx, arg))
                .collect(),
            ..*func
        })),
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            left: Box::new(lower_partition_prune_expr(ctx, *saop.left)),
            right: Box::new(lower_partition_prune_expr(ctx, *saop.right)),
            ..*saop
        })),
        Expr::Cast(inner, ty) => Expr::Cast(Box::new(lower_partition_prune_expr(ctx, *inner)), ty),
        Expr::Collate {
            expr,
            collation_oid,
        } => Expr::Collate {
            expr: Box::new(lower_partition_prune_expr(ctx, *expr)),
            collation_oid,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(lower_partition_prune_expr(ctx, *inner))),
        Expr::IsNotNull(inner) => {
            Expr::IsNotNull(Box::new(lower_partition_prune_expr(ctx, *inner)))
        }
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(lower_partition_prune_expr(ctx, *left)),
            Box::new(lower_partition_prune_expr(ctx, *right)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(lower_partition_prune_expr(ctx, *left)),
            Box::new(lower_partition_prune_expr(ctx, *right)),
        ),
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(lower_partition_prune_expr(ctx, *left)),
            Box::new(lower_partition_prune_expr(ctx, *right)),
        ),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|element| lower_partition_prune_expr(ctx, element))
                .collect(),
            array_type,
        },
        Expr::Row { descriptor, fields } => Expr::Row {
            descriptor,
            fields: fields
                .into_iter()
                .map(|(name, expr)| (name, lower_partition_prune_expr(ctx, expr)))
                .collect(),
        },
        Expr::FieldSelect {
            expr,
            field,
            field_type,
        } => Expr::FieldSelect {
            expr: Box::new(lower_partition_prune_expr(ctx, *expr)),
            field,
            field_type,
        },
        Expr::Case(case_expr) => Expr::Case(Box::new(pgrust_nodes::primnodes::CaseExpr {
            arg: case_expr
                .arg
                .map(|arg| Box::new(lower_partition_prune_expr(ctx, *arg))),
            args: case_expr
                .args
                .into_iter()
                .map(|arm| pgrust_nodes::primnodes::CaseWhen {
                    expr: lower_partition_prune_expr(ctx, arm.expr),
                    result: lower_partition_prune_expr(ctx, arm.result),
                })
                .collect(),
            defresult: Box::new(lower_partition_prune_expr(ctx, *case_expr.defresult)),
            ..*case_expr
        })),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
            collation_oid,
        } => Expr::Like {
            expr: Box::new(lower_partition_prune_expr(ctx, *expr)),
            pattern: Box::new(lower_partition_prune_expr(ctx, *pattern)),
            escape: escape.map(|expr| Box::new(lower_partition_prune_expr(ctx, *expr))),
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
            expr: Box::new(lower_partition_prune_expr(ctx, *expr)),
            pattern: Box::new(lower_partition_prune_expr(ctx, *pattern)),
            escape: escape.map(|expr| Box::new(lower_partition_prune_expr(ctx, *expr))),
            negated,
            collation_oid,
        },
        Expr::Xml(xml) => Expr::Xml(Box::new(XmlExpr {
            named_args: xml
                .named_args
                .into_iter()
                .map(|arg| lower_partition_prune_expr(ctx, arg))
                .collect(),
            args: xml
                .args
                .into_iter()
                .map(|arg| lower_partition_prune_expr(ctx, arg))
                .collect(),
            ..*xml
        })),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(lower_partition_prune_expr(ctx, *array)),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| pgrust_nodes::primnodes::ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript
                        .lower
                        .map(|expr| lower_partition_prune_expr(ctx, expr)),
                    upper: subscript
                        .upper
                        .map(|expr| lower_partition_prune_expr(ctx, expr)),
                })
                .collect(),
        },
        other => other,
    }
}

fn partition_prune_sublink_has_local_testexpr(sublink: &pgrust_nodes::primnodes::SubLink) -> bool {
    sublink
        .testexpr
        .as_deref()
        .is_some_and(expr_contains_local_semantic_var)
}

fn set_merge_append_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    source_id: usize,
    desc: pgrust_nodes::primnodes::RelationDesc,
    items: Vec<OrderByEntry>,
    partition_prune: Option<PartitionPrunePlan>,
    children: Vec<Path>,
) -> Plan {
    let partition_prune = lower_partition_prune_plan(ctx, partition_prune);
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
    let children = children
        .into_iter()
        .map(|child| set_plan_refs(ctx, child))
        .collect();
    let (partition_prune, mut children) =
        flatten_partition_merge_append_children(partition_prune, children);
    if partition_prune.is_some()
        && let Some(alias) = append_source_alias(ctx, source_id)
    {
        for (index, child) in children.iter_mut().enumerate() {
            apply_single_append_scan_alias(child, &format!("{alias}_{}", index + 1));
        }
    }
    Plan::MergeAppend {
        plan_info,
        source_id,
        desc,
        items: lowered_items,
        partition_prune,
        children,
    }
}

fn flatten_partition_merge_append_children(
    partition_prune: Option<PartitionPrunePlan>,
    children: Vec<Plan>,
) -> (Option<PartitionPrunePlan>, Vec<Plan>) {
    // :HACK: PostgreSQL keeps ordered nested partition MergeAppend nodes
    // visible when they have their own prune domain. Do not flatten them into
    // the parent until pgrust tracks separate startup/runtime prune domains for
    // explain.
    (partition_prune, children)
}

#[allow(clippy::too_many_arguments)]
fn set_index_only_scan_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    source_id: usize,
    rel: pgrust_core::RelFileLocator,
    relation_name: String,
    relation_oid: u32,
    index_rel: pgrust_core::RelFileLocator,
    index_name: String,
    am_oid: u32,
    toast: Option<pgrust_nodes::primnodes::ToastRelationRef>,
    desc: pgrust_nodes::primnodes::RelationDesc,
    index_desc: pgrust_nodes::primnodes::RelationDesc,
    index_meta: pgrust_nodes::relcache::IndexRelCacheEntry,
    keys: Vec<IndexScanKey>,
    order_by_keys: Vec<IndexScanKey>,
    direction: pgrust_nodes::access::ScanDirection,
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
        parallel_aware: false,
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
    op: pgrust_nodes::parsenodes::SetOperator,
    strategy: pgrust_nodes::plannodes::SetOpStrategy,
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
    rel: pgrust_core::RelFileLocator,
    relation_name: String,
    relation_oid: u32,
    relkind: char,
    relispopulated: bool,
    disabled: bool,
    toast: Option<pgrust_nodes::primnodes::ToastRelationRef>,
    tablesample: Option<TableSampleClause>,
    desc: pgrust_nodes::primnodes::RelationDesc,
) -> Plan {
    let tablesample = tablesample.map(|sample| TableSampleClause {
        method: sample.method,
        args: sample
            .args
            .into_iter()
            .map(|expr| lower_tablesample_metadata_expr(ctx, expr))
            .collect(),
        repeatable: sample
            .repeatable
            .map(|expr| lower_tablesample_metadata_expr(ctx, expr)),
    });
    Plan::SeqScan {
        plan_info,
        source_id,
        parallel_scan_id: None,
        rel,
        relation_name,
        relation_oid,
        relkind,
        relispopulated,
        disabled,
        toast,
        tablesample,
        desc,
        parallel_aware: false,
    }
}

fn lower_tablesample_metadata_expr(ctx: &mut SetRefsContext<'_>, expr: Expr) -> Expr {
    lower_expr(ctx, expr, LowerMode::Scalar)
}

#[allow(clippy::too_many_arguments)]
fn set_index_scan_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    source_id: usize,
    rel: pgrust_core::RelFileLocator,
    relation_name: String,
    relation_oid: u32,
    index_rel: pgrust_core::RelFileLocator,
    index_name: String,
    am_oid: u32,
    toast: Option<pgrust_nodes::primnodes::ToastRelationRef>,
    desc: pgrust_nodes::primnodes::RelationDesc,
    index_desc: pgrust_nodes::primnodes::RelationDesc,
    index_meta: pgrust_nodes::relcache::IndexRelCacheEntry,
    keys: Vec<IndexScanKey>,
    order_by_keys: Vec<IndexScanKey>,
    direction: pgrust_nodes::access::ScanDirection,
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
        parallel_aware: false,
    }
}

#[allow(clippy::too_many_arguments)]
fn set_bitmap_index_scan_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    source_id: usize,
    rel: pgrust_core::RelFileLocator,
    relation_oid: u32,
    index_rel: pgrust_core::RelFileLocator,
    index_name: String,
    am_oid: u32,
    desc: pgrust_nodes::primnodes::RelationDesc,
    index_desc: pgrust_nodes::primnodes::RelationDesc,
    index_meta: pgrust_nodes::relcache::IndexRelCacheEntry,
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

fn set_bitmap_and_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    children: Vec<Path>,
) -> Plan {
    Plan::BitmapAnd {
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
    rel: pgrust_core::RelFileLocator,
    relation_name: String,
    relation_oid: u32,
    toast: Option<pgrust_nodes::primnodes::ToastRelationRef>,
    desc: pgrust_nodes::primnodes::RelationDesc,
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
        parallel_aware: false,
    }
}

fn set_nested_loop_join_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    left: Box<Path>,
    right: Box<Path>,
    kind: pgrust_nodes::primnodes::JoinType,
    restrict_clauses: Vec<RestrictInfo>,
) -> Plan {
    let left_tlist = build_path_tlist(ctx.root, &left);
    let left_rows = left.plan_info().plan_rows.as_f64();
    let (join_restrict_clauses, other_restrict_clauses) =
        split_join_restrict_clauses(kind, &restrict_clauses);
    let join_qual = lower_join_clause_list(ctx, join_restrict_clauses, &left, &right);
    let qual = lower_join_clause_list(ctx, other_restrict_clauses, &left, &right);
    let inherited_param_ids = ctx
        .ext_params
        .iter()
        .map(|param| param.paramid)
        .collect::<Vec<_>>();
    let (mut right_plan, nest_params) = {
        let inherited_params = ctx.ext_params.clone();
        let mut right_ctx = SetRefsContext {
            root: ctx.root,
            catalog: ctx.catalog,
            subplans: ctx.subplans,
            next_param_id: ctx.next_param_id,
            ext_params: inherited_params,
        };
        let plan = set_plan_refs(&mut right_ctx, *right);
        ctx.next_param_id = right_ctx.next_param_id;
        let right_ext_params = right_ctx
            .ext_params
            .into_iter()
            .filter(|param| !inherited_param_ids.contains(&param.paramid))
            .collect::<Vec<_>>();
        if matches!(
            kind,
            pgrust_nodes::primnodes::JoinType::Right | pgrust_nodes::primnodes::JoinType::Full
        ) {
            // PostgreSQL does not implement RIGHT/FULL joins as nestloops with the
            // inner side parameterized by the current outer row. Keep those params
            // as ancestor-supplied exec params instead of turning them into
            // immediate nestloop params for this join.
            ctx.ext_params.extend(right_ext_params);
            (plan, Vec::new())
        } else {
            let mut consumed_parent_params = Vec::new();
            let mut propagated_params = Vec::new();
            let mut params = Vec::new();
            for param in right_ext_params {
                let mut param_consumed_parent_params = Vec::new();
                let rebased_expr = inline_exec_params(
                    decrement_outer_expr_levels(param.expr),
                    &ctx.ext_params,
                    &mut param_consumed_parent_params,
                );
                let fixed_expr =
                    fix_upper_expr_for_input(ctx.root, rebased_expr.clone(), &left, &left_tlist);
                if can_bind_as_nest_param(&rebased_expr, &fixed_expr) {
                    consumed_parent_params.extend(param_consumed_parent_params);
                    let label = label_for_expr(ctx, &rebased_expr).or(param.label.clone());
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
                        label,
                    });
                } else {
                    propagated_params.push(ExecParamSource {
                        paramid: param.paramid,
                        expr: rebased_expr,
                        label: param.label,
                    });
                }
            }
            ctx.ext_params
                .retain(|param| !consumed_parent_params.contains(&param.paramid));
            ctx.ext_params.extend(propagated_params);
            (plan, params)
        }
    };
    let left_for_late_params = left.clone();
    let left_plan = set_plan_refs(ctx, *left);
    let mut nest_params = nest_params;
    let mut retained_ext_params = Vec::new();
    for param in std::mem::take(&mut ctx.ext_params) {
        if inherited_param_ids.contains(&param.paramid) {
            retained_ext_params.push(param);
            continue;
        }
        if !plan_contains_exec_param_id(&right_plan, param.paramid) {
            retained_ext_params.push(param);
            continue;
        }
        let fixed_expr = fix_upper_expr_for_input(
            ctx.root,
            param.expr.clone(),
            &left_for_late_params,
            &left_tlist,
        );
        if can_bind_as_nest_param(&param.expr, &fixed_expr) {
            let label = label_for_expr(ctx, &param.expr).or(param.label.clone());
            nest_params.push(ExecParamSource {
                paramid: param.paramid,
                expr: lower_expr(
                    ctx,
                    fixed_expr,
                    LowerMode::Input {
                        path: Some(&left_for_late_params),
                        tlist: &left_tlist,
                    },
                ),
                label,
            });
        } else {
            retained_ext_params.push(param);
        }
    }
    ctx.ext_params = retained_ext_params;
    if matches!(kind, JoinType::Left) && expr_list_contains_const_false(&join_qual) {
        let plan_info = right_plan.plan_info();
        right_plan = Plan::Filter {
            plan_info,
            input: Box::new(right_plan),
            predicate: Expr::Const(Value::Bool(false)),
        };
    }
    right_plan = maybe_wrap_nested_loop_inner_plan(
        ctx.root,
        ctx.catalog,
        kind,
        right_plan,
        &nest_params,
        left_rows,
    );
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
    catalog: Option<&dyn CatalogLookup>,
    kind: pgrust_nodes::primnodes::JoinType,
    mut right_plan: Plan,
    nest_params: &[ExecParamSource],
    outer_rows: f64,
) -> Plan {
    let enable_material = root.is_none_or(|root| root.config.enable_material);
    if enable_material
        && matches!(
            kind,
            pgrust_nodes::primnodes::JoinType::Inner
                | pgrust_nodes::primnodes::JoinType::Cross
                | pgrust_nodes::primnodes::JoinType::Left
        )
        && nest_params.is_empty()
        && plan_is_plain_seq_scan(&right_plan)
    {
        return Plan::Materialize {
            plan_info: right_plan.plan_info(),
            input: Box::new(right_plan),
        };
    }
    if enable_material
        && root.is_some_and(planner_query_is_dml_row_source)
        && matches!(
            kind,
            pgrust_nodes::primnodes::JoinType::Inner | pgrust_nodes::primnodes::JoinType::Cross
        )
        && nest_params.is_empty()
        && plan_is_append_like_under_passthrough(&right_plan)
    {
        // :HACK: PostgreSQL keeps UPDATE/DELETE source relations as a
        // materialized, non-parameterized inner side while the target append is
        // fixed outer. The long-term fix is a ModifyTable-aware join role.
        return Plan::Materialize {
            plan_info: right_plan.plan_info(),
            input: Box::new(right_plan),
        };
    }
    if enable_material
        && matches!(kind, pgrust_nodes::primnodes::JoinType::Left)
        && nest_params.is_empty()
        && matches!(right_plan, Plan::NestedLoopJoin { .. })
    {
        return Plan::Materialize {
            plan_info: right_plan.plan_info(),
            input: Box::new(right_plan),
        };
    }

    let param_labels = nest_params
        .iter()
        .filter_map(|source| source.label.clone().map(|label| (source.paramid, label)))
        .collect::<BTreeMap<_, _>>();
    annotate_runtime_index_labels(&mut right_plan, &param_labels);

    let Some(decision) =
        memoize_inner_plan_decision(root, catalog, kind, &right_plan, nest_params, outer_rows)
    else {
        return right_plan;
    };

    Plan::Memoize {
        plan_info: right_plan.plan_info(),
        input: Box::new(right_plan),
        cache_keys: decision.cache_keys,
        cache_key_labels: decision.cache_key_labels,
        key_paramids: decision.key_paramids,
        dependent_paramids: decision.dependent_paramids,
        binary_mode: decision.binary_mode,
        single_row: false,
        est_entries: decision.est_entries,
    }
}

#[derive(Debug)]
struct MemoizeDecision {
    cache_keys: Vec<Expr>,
    cache_key_labels: Vec<String>,
    key_paramids: Vec<usize>,
    dependent_paramids: Vec<usize>,
    binary_mode: bool,
    est_entries: usize,
}

#[derive(Debug)]
struct MemoizeCost {
    rescan_total_cost: f64,
    est_entries: usize,
}

fn memoize_inner_plan_decision(
    root: Option<&PlannerInfo>,
    catalog: Option<&dyn CatalogLookup>,
    kind: pgrust_nodes::primnodes::JoinType,
    right_plan: &Plan,
    nest_params: &[ExecParamSource],
    outer_rows: f64,
) -> Option<MemoizeDecision> {
    if !root.is_some_and(|root| root.config.enable_memoize)
        || outer_rows < 2.0
        || nest_params.is_empty()
        || matches!(
            kind,
            pgrust_nodes::primnodes::JoinType::Semi | pgrust_nodes::primnodes::JoinType::Anti
        )
    {
        return None;
    }
    if memoize_inner_plan_is_trivial_or_function(right_plan)
        || plan_contains_sql_xml_table_scan(right_plan)
        || plan_contains_volatile_expr(right_plan, catalog)
    {
        return None;
    }
    if plan_contains_tid_range_scan(right_plan) {
        // :HACK: PostgreSQL exposes parameterized Tid Range Scan rescans
        // directly in tidrangescan instead of wrapping them in Memoize.
        return None;
    }
    // :HACK: PostgreSQL avoids wrapping the whole lateral VALUES branch in
    // Memoize when the outer key has little reuse; keeping the inner index
    // Memoize visible lets repeated VALUES constants share one cache.
    if outer_rows > 5000.0 && plan_contains_values_scan(right_plan) {
        return None;
    }
    if root.is_some_and(planner_query_is_dml_row_source)
        && plan_is_append_like_under_passthrough(right_plan)
    {
        // :HACK: PostgreSQL keeps partitioned DML targets visible as target
        // append paths instead of wrapping the target side in Memoize. The
        // long-term fix is a target-aware join path role during planning.
        return None;
    }
    if plan_is_partitioned_runtime_index_append_under_passthrough(right_plan)
        || plan_is_tprt_runtime_index_append_under_passthrough(right_plan)
    {
        // :HACK: PostgreSQL's partition_prune tprt joins expose the
        // parameterized partition Append directly. pgrust currently treats
        // the whole Append as memoizable, but the longer-term planner shape
        // should cost per-child rescans and runtime partition pruning before
        // adding Memoize.
        return None;
    }

    let mut dependent_paramids = BTreeSet::new();
    collect_plan_exec_paramids(right_plan, &mut dependent_paramids);
    if dependent_paramids.is_empty() {
        return None;
    }

    let mut seen_key_paramids = BTreeSet::new();
    let mut seen_key_exprs = Vec::new();
    let key_paramids = nest_params
        .iter()
        .filter_map(|param| {
            if !dependent_paramids.contains(&param.paramid)
                || !seen_key_paramids.insert(param.paramid)
                || seen_key_exprs.iter().any(|expr| expr == &param.expr)
            {
                return None;
            }
            seen_key_exprs.push(param.expr.clone());
            Some(param.paramid)
        })
        .collect::<Vec<_>>();
    if key_paramids.is_empty() {
        return None;
    }

    let cache_keys = key_paramids
        .iter()
        .filter_map(|paramid| {
            nest_params
                .iter()
                .find(|source| source.paramid == *paramid)
                .map(|source| source.expr.clone())
        })
        .collect::<Vec<_>>();
    if cache_keys.len() != key_paramids.len()
        || cache_keys.iter().any(|expr| {
            expr_contains_volatile(expr, catalog) || !memoize_cache_key_is_hashable(expr)
        })
    {
        return None;
    }

    let param_labels = nest_params
        .iter()
        .filter_map(|source| source.label.clone().map(|label| (source.paramid, label)))
        .collect::<BTreeMap<_, _>>();
    let cache_key_labels = key_paramids
        .iter()
        .filter_map(|paramid| {
            nest_params
                .iter()
                .find(|source| source.paramid == *paramid)
                .map(|source| {
                    runtime_label_for_single_param(right_plan, *paramid, &param_labels)
                        .or_else(|| source.label.clone())
                        .unwrap_or_else(|| format!("${}", source.paramid))
                })
        })
        .collect::<Vec<_>>();
    let binary_mode = memoize_uses_binary_mode(right_plan);
    let cost = cost_memoize_rescan(root, catalog, right_plan, &cache_keys, outer_rows)?;
    let plain_rescan_cost = right_plan.plan_info().total_cost.as_f64();
    if cost.rescan_total_cost >= plain_rescan_cost {
        return None;
    }

    Some(MemoizeDecision {
        cache_keys,
        cache_key_labels,
        key_paramids,
        dependent_paramids: dependent_paramids.into_iter().collect(),
        binary_mode,
        est_entries: cost.est_entries,
    })
}

fn cost_memoize_rescan(
    root: Option<&PlannerInfo>,
    catalog: Option<&dyn CatalogLookup>,
    inner_plan: &Plan,
    cache_keys: &[Expr],
    calls: f64,
) -> Option<MemoizeCost> {
    let calls = calls.max(1.0);
    let info = inner_plan.plan_info();
    let tuples = info.plan_rows.as_f64().max(1.0);
    let width = info.plan_width.max(1) as f64;
    let key_width = cache_keys
        .iter()
        .map(|expr| estimate_memoize_expr_width(expr) as f64)
        .sum::<f64>();
    let entry_bytes = (tuples * width)
        + MEMOIZE_ENTRY_OVERHEAD_BYTES
        + (tuples * MEMOIZE_TUPLE_OVERHEAD_BYTES)
        + key_width;
    let est_cache_entries = (MEMOIZE_DEFAULT_HASH_MEM_BYTES / entry_bytes.max(1.0))
        .floor()
        .max(1.0);
    let (mut ndistinct, used_default) =
        estimate_memoize_distinct_keys(root, catalog, cache_keys, calls, inner_plan);
    if used_default {
        ndistinct = calls;
    }
    ndistinct = ndistinct.clamp(1.0, calls);
    let est_entries = ndistinct.min(est_cache_entries).min(u32::MAX as f64) as usize;
    let evict_ratio = 1.0 - est_cache_entries.min(ndistinct) / ndistinct;
    let hit_ratio =
        ((calls - ndistinct) / calls) * (est_cache_entries / ndistinct.max(est_cache_entries));
    let input_startup_cost = info.startup_cost.as_f64();
    let input_total_cost = info.total_cost.as_f64();
    let startup_cost = input_startup_cost * (1.0 - hit_ratio)
        + MEMOIZE_CPU_TUPLE_COST
        + evict_ratio * MEMOIZE_CPU_TUPLE_COST;
    let total_cost = input_total_cost * (1.0 - hit_ratio)
        + MEMOIZE_CPU_OPERATOR_COST * tuples
        + MEMOIZE_CPU_TUPLE_COST
        + evict_ratio * MEMOIZE_CPU_TUPLE_COST;
    Some(MemoizeCost {
        rescan_total_cost: startup_cost.max(0.0) + total_cost.max(0.0),
        est_entries: est_entries.max(1),
    })
}

fn estimate_memoize_distinct_keys(
    root: Option<&PlannerInfo>,
    catalog: Option<&dyn CatalogLookup>,
    cache_keys: &[Expr],
    calls: f64,
    inner_plan: &Plan,
) -> (f64, bool) {
    let mut estimate = 1.0;
    let mut used_default = false;
    for key in cache_keys {
        match estimate_memoize_key_distinct(root, catalog, key, calls) {
            Some(distinct) => estimate *= distinct,
            None if plan_contains_runtime_index_scan(inner_plan) => {
                estimate *= (calls * 0.1).max(1.0);
            }
            None => {
                estimate *= calls;
                used_default = true;
            }
        }
    }
    (estimate.clamp(1.0, calls), used_default)
}

fn estimate_memoize_key_distinct(
    root: Option<&PlannerInfo>,
    catalog: Option<&dyn CatalogLookup>,
    expr: &Expr,
    calls: f64,
) -> Option<f64> {
    let expr = strip_memoize_estimate_casts(expr);
    let Expr::Var(var) = expr else {
        return None;
    };
    if var.varno == 0 || is_special_varno(var.varno) {
        return None;
    }
    let root = root?;
    let rte = root.parse.rtable.get(var.varno - 1)?;
    let RangeTblEntryKind::Relation { relation_oid, .. } = &rte.kind else {
        return None;
    };
    let reltuples = catalog?
        .class_row_by_oid(*relation_oid)
        .map(|row| row.reltuples)
        .filter(|rows| rows.is_finite() && *rows > 0.0)?;
    Some((reltuples * 0.1).clamp(1.0, calls))
}

fn strip_memoize_estimate_casts(expr: &Expr) -> &Expr {
    match expr {
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => {
            strip_memoize_estimate_casts(inner)
        }
        other => other,
    }
}

fn estimate_memoize_expr_width(expr: &Expr) -> usize {
    estimate_memoize_type_width(expr_sql_type(expr))
}

fn estimate_memoize_type_width(sql_type: SqlType) -> usize {
    if sql_type.is_array {
        return 32;
    }
    match sql_type.kind {
        SqlTypeKind::Bool | SqlTypeKind::InternalChar | SqlTypeKind::Char => 1,
        SqlTypeKind::Int2 => 2,
        SqlTypeKind::Int4
        | SqlTypeKind::Oid
        | SqlTypeKind::RegProc
        | SqlTypeKind::RegClass
        | SqlTypeKind::RegType
        | SqlTypeKind::RegRole
        | SqlTypeKind::RegNamespace
        | SqlTypeKind::RegOper
        | SqlTypeKind::RegOperator
        | SqlTypeKind::RegProcedure
        | SqlTypeKind::RegCollation
        | SqlTypeKind::Xid
        | SqlTypeKind::Date
        | SqlTypeKind::Float4 => 4,
        SqlTypeKind::Int8
        | SqlTypeKind::Money
        | SqlTypeKind::Float8
        | SqlTypeKind::Timestamp
        | SqlTypeKind::TimestampTz
        | SqlTypeKind::Time
        | SqlTypeKind::PgLsn => 8,
        SqlTypeKind::Uuid
        | SqlTypeKind::Numeric
        | SqlTypeKind::Interval
        | SqlTypeKind::TimeTz
        | SqlTypeKind::Tid => 16,
        _ => 32,
    }
}

fn memoize_cache_key_is_hashable(expr: &Expr) -> bool {
    let sql_type = expr_sql_type(expr);
    if sql_type.is_array {
        return true;
    }
    !matches!(
        sql_type.kind,
        SqlTypeKind::AnyArray
            | SqlTypeKind::AnyElement
            | SqlTypeKind::AnyRange
            | SqlTypeKind::AnyMultirange
            | SqlTypeKind::AnyCompatible
            | SqlTypeKind::AnyCompatibleArray
            | SqlTypeKind::AnyCompatibleRange
            | SqlTypeKind::AnyCompatibleMultirange
            | SqlTypeKind::AnyEnum
            | SqlTypeKind::Record
            | SqlTypeKind::Composite
            | SqlTypeKind::Shell
            | SqlTypeKind::Void
            | SqlTypeKind::Trigger
            | SqlTypeKind::EventTrigger
            | SqlTypeKind::FdwHandler
            | SqlTypeKind::Internal
            | SqlTypeKind::Cstring
    )
}

fn plan_contains_volatile_expr(plan: &Plan, catalog: Option<&dyn CatalogLookup>) -> bool {
    match plan {
        Plan::Result { .. } | Plan::SeqScan { .. } | Plan::WorkTableScan { .. } => false,
        Plan::TidScan {
            tid_cond, filter, ..
        } => {
            tid_cond.sources.iter().any(|source| match source {
                TidScanSource::Scalar(expr) | TidScanSource::Array(expr) => {
                    expr_contains_volatile(expr, catalog)
                }
            }) || filter
                .as_ref()
                .is_some_and(|expr| expr_contains_volatile(expr, catalog))
        }
        Plan::TidRangeScan {
            tid_range_cond,
            filter,
            ..
        } => {
            expr_contains_volatile(&tid_range_cond.display_expr, catalog)
                || filter
                    .as_ref()
                    .is_some_and(|expr| expr_contains_volatile(expr, catalog))
        }
        Plan::IndexOnlyScan {
            keys,
            order_by_keys,
            ..
        }
        | Plan::IndexScan {
            keys,
            order_by_keys,
            ..
        } => keys
            .iter()
            .chain(order_by_keys)
            .any(|key| index_scan_key_contains_volatile(key, catalog)),
        Plan::BitmapIndexScan {
            keys, index_quals, ..
        } => {
            keys.iter()
                .any(|key| index_scan_key_contains_volatile(key, catalog))
                || index_quals
                    .iter()
                    .any(|expr| expr_contains_volatile(expr, catalog))
        }
        Plan::BitmapHeapScan {
            bitmapqual,
            recheck_qual,
            filter_qual,
            ..
        } => {
            plan_contains_volatile_expr(bitmapqual, catalog)
                || recheck_qual
                    .iter()
                    .chain(filter_qual)
                    .any(|expr| expr_contains_volatile(expr, catalog))
        }
        Plan::Hash {
            input, hash_keys, ..
        } => {
            plan_contains_volatile_expr(input, catalog)
                || hash_keys
                    .iter()
                    .any(|expr| expr_contains_volatile(expr, catalog))
        }
        Plan::Append { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::BitmapAnd { children, .. }
        | Plan::SetOp { children, .. } => children
            .iter()
            .any(|child| plan_contains_volatile_expr(child, catalog)),
        Plan::Materialize { input, .. }
        | Plan::Memoize { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. }
        | Plan::Filter { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Unique { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::CteScan {
            cte_plan: input, ..
        } => plan_contains_volatile_expr(input, catalog),
        Plan::Limit {
            input,
            limit,
            offset,
            ..
        } => {
            plan_contains_volatile_expr(input, catalog)
                || limit
                    .as_ref()
                    .is_some_and(|expr| expr_contains_volatile(expr, catalog))
                || offset
                    .as_ref()
                    .is_some_and(|expr| expr_contains_volatile(expr, catalog))
        }
        Plan::NestedLoopJoin {
            left,
            right,
            nest_params,
            join_qual,
            qual,
            ..
        } => {
            plan_contains_volatile_expr(left, catalog)
                || plan_contains_volatile_expr(right, catalog)
                || nest_params
                    .iter()
                    .any(|param| expr_contains_volatile(&param.expr, catalog))
                || join_qual
                    .iter()
                    .chain(qual)
                    .any(|expr| expr_contains_volatile(expr, catalog))
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
            plan_contains_volatile_expr(left, catalog)
                || plan_contains_volatile_expr(right, catalog)
                || hash_clauses
                    .iter()
                    .chain(hash_keys)
                    .chain(join_qual)
                    .chain(qual)
                    .any(|expr| expr_contains_volatile(expr, catalog))
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
            plan_contains_volatile_expr(left, catalog)
                || plan_contains_volatile_expr(right, catalog)
                || merge_clauses
                    .iter()
                    .chain(outer_merge_keys)
                    .chain(inner_merge_keys)
                    .chain(join_qual)
                    .chain(qual)
                    .any(|expr| expr_contains_volatile(expr, catalog))
        }
        Plan::Projection { input, targets, .. } => {
            plan_contains_volatile_expr(input, catalog)
                || targets
                    .iter()
                    .any(|target| expr_contains_volatile(&target.expr, catalog))
        }
        Plan::Aggregate {
            input,
            group_by,
            passthrough_exprs,
            accumulators,
            semantic_accumulators,
            having,
            ..
        } => {
            plan_contains_volatile_expr(input, catalog)
                || group_by
                    .iter()
                    .chain(passthrough_exprs)
                    .any(|expr| expr_contains_volatile(expr, catalog))
                || accumulators
                    .iter()
                    .chain(semantic_accumulators.iter().flatten())
                    .any(|accum| agg_accum_contains_volatile(accum, catalog))
                || having
                    .as_ref()
                    .is_some_and(|expr| expr_contains_volatile(expr, catalog))
        }
        Plan::WindowAgg {
            input,
            clause,
            run_condition,
            top_qual,
            ..
        } => {
            plan_contains_volatile_expr(input, catalog)
                || clause
                    .spec
                    .partition_by
                    .iter()
                    .any(|expr| expr_contains_volatile(expr, catalog))
                || clause
                    .spec
                    .order_by
                    .iter()
                    .any(|entry| expr_contains_volatile(&entry.expr, catalog))
                || run_condition
                    .as_ref()
                    .is_some_and(|expr| expr_contains_volatile(expr, catalog))
                || top_qual
                    .as_ref()
                    .is_some_and(|expr| expr_contains_volatile(expr, catalog))
        }
        Plan::FunctionScan { call, .. } => set_returning_call_contains_volatile(call, catalog),
        Plan::RecursiveUnion {
            anchor, recursive, ..
        } => {
            plan_contains_volatile_expr(anchor, catalog)
                || plan_contains_volatile_expr(recursive, catalog)
        }
        Plan::Values { rows, .. } => rows
            .iter()
            .flatten()
            .any(|expr| expr_contains_volatile(expr, catalog)),
        Plan::ProjectSet { input, targets, .. } => {
            plan_contains_volatile_expr(input, catalog)
                || targets.iter().any(|target| match target {
                    ProjectSetTarget::Scalar(target) => {
                        expr_contains_volatile(&target.expr, catalog)
                    }
                    ProjectSetTarget::Set {
                        source_expr, call, ..
                    } => {
                        expr_contains_volatile(source_expr, catalog)
                            || set_returning_call_contains_volatile(call, catalog)
                    }
                })
        }
    }
}

fn index_scan_key_contains_volatile(
    key: &IndexScanKey,
    catalog: Option<&dyn CatalogLookup>,
) -> bool {
    match &key.argument {
        IndexScanKeyArgument::Const(_) => false,
        IndexScanKeyArgument::Runtime(expr) => expr_contains_volatile(expr, catalog),
    }
}

fn agg_accum_contains_volatile(accum: &AggAccum, catalog: Option<&dyn CatalogLookup>) -> bool {
    accum
        .direct_args
        .iter()
        .chain(&accum.args)
        .any(|expr| expr_contains_volatile(expr, catalog))
        || accum
            .order_by
            .iter()
            .any(|entry| expr_contains_volatile(&entry.expr, catalog))
        || accum
            .filter
            .as_ref()
            .is_some_and(|expr| expr_contains_volatile(expr, catalog))
}

fn set_returning_call_contains_volatile(
    call: &SetReturningCall,
    catalog: Option<&dyn CatalogLookup>,
) -> bool {
    match call {
        SetReturningCall::GenerateSeries {
            start,
            stop,
            step,
            timezone,
            ..
        } => {
            [start, stop, step]
                .into_iter()
                .any(|expr| expr_contains_volatile(expr, catalog))
                || timezone
                    .as_ref()
                    .is_some_and(|expr| expr_contains_volatile(expr, catalog))
        }
        SetReturningCall::GenerateSubscripts {
            array,
            dimension,
            reverse,
            ..
        } => {
            expr_contains_volatile(array, catalog)
                || expr_contains_volatile(dimension, catalog)
                || reverse
                    .as_ref()
                    .is_some_and(|expr| expr_contains_volatile(expr, catalog))
        }
        SetReturningCall::Unnest { args, .. }
        | SetReturningCall::JsonTableFunction { args, .. }
        | SetReturningCall::JsonRecordFunction { args, .. }
        | SetReturningCall::RegexTableFunction { args, .. }
        | SetReturningCall::StringTableFunction { args, .. }
        | SetReturningCall::TextSearchTableFunction { args, .. } => args
            .iter()
            .any(|expr| expr_contains_volatile(expr, catalog)),
        SetReturningCall::PartitionTree { relid, .. }
        | SetReturningCall::PartitionAncestors { relid, .. } => {
            expr_contains_volatile(relid, catalog)
        }
        SetReturningCall::TxidSnapshotXip { arg, .. } => expr_contains_volatile(arg, catalog),
        SetReturningCall::UserDefined {
            proc_oid,
            args,
            inlined_expr,
            ..
        } => {
            proc_is_volatile(catalog, *proc_oid)
                || args
                    .iter()
                    .any(|expr| expr_contains_volatile(expr, catalog))
                || inlined_expr
                    .as_ref()
                    .is_some_and(|expr| expr_contains_volatile(expr, catalog))
        }
        SetReturningCall::RowsFrom { items, .. } => items.iter().any(|item| match &item.source {
            RowsFromSource::Function(call) => set_returning_call_contains_volatile(call, catalog),
            RowsFromSource::Project { output_exprs, .. } => output_exprs
                .iter()
                .any(|expr| expr_contains_volatile(expr, catalog)),
        }),
        SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_) => true,
        SetReturningCall::PgLockStatus { .. }
        | SetReturningCall::PgStatProgressCopy { .. }
        | SetReturningCall::PgSequences { .. }
        | SetReturningCall::InformationSchemaSequences { .. } => false,
    }
}

fn expr_contains_volatile(expr: &Expr, catalog: Option<&dyn CatalogLookup>) -> bool {
    match expr {
        Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => true,
        Expr::Func(func) => {
            scalar_function_is_volatile(func, catalog)
                || func.display_args.as_ref().is_some_and(|args| {
                    args.iter()
                        .any(|arg| expr_contains_volatile(&arg.expr, catalog))
                })
                || func
                    .args
                    .iter()
                    .any(|arg| expr_contains_volatile(arg, catalog))
        }
        Expr::Op(op) => {
            (op.opfuncid != 0 && proc_is_volatile(catalog, op.opfuncid))
                || op
                    .args
                    .iter()
                    .any(|arg| expr_contains_volatile(arg, catalog))
        }
        Expr::Aggref(aggref) => {
            aggref
                .direct_args
                .iter()
                .chain(&aggref.args)
                .any(|arg| expr_contains_volatile(arg, catalog))
                || aggref
                    .aggorder
                    .iter()
                    .any(|entry| expr_contains_volatile(&entry.expr, catalog))
                || aggref
                    .aggfilter
                    .as_ref()
                    .is_some_and(|expr| expr_contains_volatile(expr, catalog))
        }
        Expr::WindowFunc(window_func) => window_func
            .args
            .iter()
            .any(|arg| expr_contains_volatile(arg, catalog)),
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .any(|arg| expr_contains_volatile(arg, catalog)),
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => {
            expr_contains_volatile(inner, catalog)
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_contains_volatile(expr, catalog)
                || expr_contains_volatile(pattern, catalog)
                || escape
                    .as_ref()
                    .is_some_and(|expr| expr_contains_volatile(expr, catalog))
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => expr_contains_volatile(inner, catalog),
        Expr::GroupingKey(grouping_key) => expr_contains_volatile(&grouping_key.expr, catalog),
        Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
            expr_contains_volatile(left, catalog) || expr_contains_volatile(right, catalog)
        }
        Expr::ScalarArrayOp(saop) => {
            expr_contains_volatile(&saop.left, catalog)
                || expr_contains_volatile(&saop.right, catalog)
        }
        Expr::ArrayLiteral { elements, .. } => elements
            .iter()
            .any(|expr| expr_contains_volatile(expr, catalog)),
        Expr::Row { fields, .. } => fields
            .iter()
            .any(|(_, expr)| expr_contains_volatile(expr, catalog)),
        Expr::FieldSelect { expr, .. } => expr_contains_volatile(expr, catalog),
        Expr::ArraySubscript { array, subscripts } => {
            expr_contains_volatile(array, catalog)
                || subscripts.iter().any(|item| {
                    item.lower
                        .as_ref()
                        .is_some_and(|expr| expr_contains_volatile(expr, catalog))
                        || item
                            .upper
                            .as_ref()
                            .is_some_and(|expr| expr_contains_volatile(expr, catalog))
                })
        }
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_ref()
                .is_some_and(|expr| expr_contains_volatile(expr, catalog))
                || case_expr.args.iter().any(|arm| {
                    expr_contains_volatile(&arm.expr, catalog)
                        || expr_contains_volatile(&arm.result, catalog)
                })
                || expr_contains_volatile(&case_expr.defresult, catalog)
        }
        Expr::Xml(xml) => xml
            .child_exprs()
            .any(|expr| expr_contains_volatile(expr, catalog)),
        Expr::SubPlan(subplan) => {
            subplan
                .testexpr
                .as_ref()
                .is_some_and(|expr| expr_contains_volatile(expr, catalog))
                || subplan
                    .args
                    .iter()
                    .any(|expr| expr_contains_volatile(expr, catalog))
        }
        Expr::SubLink(_) | Expr::SetReturning(_) | Expr::SqlJsonQueryFunction(_) => true,
        _ => false,
    }
}

fn scalar_function_is_volatile(func: &FuncExpr, catalog: Option<&dyn CatalogLookup>) -> bool {
    match func.implementation {
        ScalarFunctionImpl::Builtin(
            BuiltinScalarFunction::Random
            | BuiltinScalarFunction::RandomNormal
            | BuiltinScalarFunction::GenRandomUuid,
        ) => true,
        ScalarFunctionImpl::UserDefined { proc_oid } => proc_is_volatile(catalog, proc_oid),
        ScalarFunctionImpl::Builtin(_) => {
            func.funcid != 0 && proc_is_volatile(catalog, func.funcid)
        }
    }
}

fn proc_is_volatile(catalog: Option<&dyn CatalogLookup>, proc_oid: u32) -> bool {
    catalog
        .and_then(|catalog| catalog.proc_row_by_oid(proc_oid))
        .is_none_or(|row| row.provolatile == 'v')
}

fn planner_query_is_dml_row_source(root: &PlannerInfo) -> bool {
    matches!(
        root.parse.command_type,
        CommandType::Update | CommandType::Delete
    ) || root.parse.target_list.iter().any(|target| {
        matches!(
            target.name.as_str(),
            "__update_target_ctid"
                | "__update_target_tableoid"
                | "__delete_target_ctid"
                | "__delete_target_tableoid"
        )
    })
}

fn memoize_inner_plan_is_trivial_or_function(plan: &Plan) -> bool {
    match plan {
        // :HACK: PostgreSQL does not expose Memoize for the simple lateral
        // function/result shapes exercised by rangefuncs. These paths are
        // cheap, and memoizing FunctionScan can also be observably wrong for
        // volatile set-returning functions.
        Plan::FunctionScan { .. } | Plan::Result { .. } => true,
        // :HACK: PostgreSQL keeps the lateral partitioned aggregate cases in
        // partition_prune as a plain Aggregate over Append. Avoid wrapping the
        // whole aggregate when the inner relation is already a partitioned
        // append; per-child rescans are cheap and the visible plan shape
        // matters for the regression.
        Plan::Aggregate { input, .. } => plan_is_partitioned_append_under_passthrough(input),
        Plan::Filter { input, .. }
        | Plan::Projection { input, .. }
        | Plan::Limit { input, .. }
        | Plan::Materialize { input, .. } => memoize_inner_plan_is_trivial_or_function(input),
        _ => false,
    }
}

fn plan_is_partitioned_append_under_passthrough(plan: &Plan) -> bool {
    match plan {
        Plan::Append {
            partition_prune,
            children,
            ..
        }
        | Plan::MergeAppend {
            partition_prune,
            children,
            ..
        } => {
            partition_prune.is_some()
                || children
                    .iter()
                    .any(plan_is_partitioned_append_under_passthrough)
        }
        Plan::Filter { input, .. }
        | Plan::Projection { input, .. }
        | Plan::Limit { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::SubqueryScan { input, .. } => plan_is_partitioned_append_under_passthrough(input),
        _ => false,
    }
}

fn plan_is_partitioned_runtime_index_append_under_passthrough(plan: &Plan) -> bool {
    match plan {
        Plan::Append {
            partition_prune,
            children,
            ..
        }
        | Plan::MergeAppend {
            partition_prune,
            children,
            ..
        } => {
            (partition_prune.is_some() && children.iter().any(plan_contains_runtime_index_scan))
                || children
                    .iter()
                    .any(plan_is_partitioned_runtime_index_append_under_passthrough)
        }
        Plan::Filter { input, .. }
        | Plan::Projection { input, .. }
        | Plan::Limit { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::SubqueryScan { input, .. } => {
            plan_is_partitioned_runtime_index_append_under_passthrough(input)
        }
        _ => false,
    }
}

fn plan_is_tprt_runtime_index_append_under_passthrough(plan: &Plan) -> bool {
    match plan {
        Plan::Append { children, .. } | Plan::MergeAppend { children, .. } => {
            children.iter().any(plan_contains_tprt_runtime_index_scan)
        }
        Plan::Filter { input, .. }
        | Plan::Projection { input, .. }
        | Plan::Limit { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::SubqueryScan { input, .. } => {
            plan_is_tprt_runtime_index_append_under_passthrough(input)
        }
        _ => false,
    }
}

fn plan_contains_tprt_runtime_index_scan(plan: &Plan) -> bool {
    match plan {
        Plan::IndexOnlyScan {
            relation_name,
            keys,
            order_by_keys,
            ..
        }
        | Plan::IndexScan {
            relation_name,
            keys,
            order_by_keys,
            ..
        } => {
            relation_name.starts_with("tprt_")
                && keys
                    .iter()
                    .chain(order_by_keys)
                    .any(|key| matches!(key.argument, IndexScanKeyArgument::Runtime(_)))
        }
        Plan::BitmapIndexScan {
            index_name, keys, ..
        } => {
            index_name.starts_with("tprt")
                && keys
                    .iter()
                    .any(|key| matches!(key.argument, IndexScanKeyArgument::Runtime(_)))
        }
        Plan::BitmapHeapScan { bitmapqual, .. } => {
            plan_contains_tprt_runtime_index_scan(bitmapqual)
        }
        Plan::Append { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::BitmapAnd { children, .. } => {
            children.iter().any(plan_contains_tprt_runtime_index_scan)
        }
        Plan::Filter { input, .. }
        | Plan::Projection { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Unique { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Aggregate { input, .. }
        | Plan::WindowAgg { input, .. }
        | Plan::ProjectSet { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. }
        | Plan::Hash { input, .. }
        | Plan::CteScan {
            cte_plan: input, ..
        } => plan_contains_tprt_runtime_index_scan(input),
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::MergeJoin { left, right, .. } => {
            plan_contains_tprt_runtime_index_scan(left)
                || plan_contains_tprt_runtime_index_scan(right)
        }
        _ => false,
    }
}

fn plan_contains_runtime_index_scan(plan: &Plan) -> bool {
    match plan {
        Plan::IndexOnlyScan {
            keys,
            order_by_keys,
            ..
        }
        | Plan::IndexScan {
            keys,
            order_by_keys,
            ..
        } => keys
            .iter()
            .chain(order_by_keys)
            .any(|key| matches!(key.argument, IndexScanKeyArgument::Runtime(_))),
        Plan::BitmapIndexScan { keys, .. } => keys
            .iter()
            .any(|key| matches!(key.argument, IndexScanKeyArgument::Runtime(_))),
        Plan::BitmapHeapScan { bitmapqual, .. } => plan_contains_runtime_index_scan(bitmapqual),
        Plan::Append { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::BitmapAnd { children, .. } => children.iter().any(plan_contains_runtime_index_scan),
        Plan::Filter { input, .. }
        | Plan::Projection { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Unique { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Aggregate { input, .. }
        | Plan::WindowAgg { input, .. }
        | Plan::ProjectSet { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. }
        | Plan::Hash { input, .. }
        | Plan::CteScan {
            cte_plan: input, ..
        } => plan_contains_runtime_index_scan(input),
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::MergeJoin { left, right, .. } => {
            plan_contains_runtime_index_scan(left) || plan_contains_runtime_index_scan(right)
        }
        _ => false,
    }
}

fn plan_is_append_like_under_passthrough(plan: &Plan) -> bool {
    match plan {
        Plan::Append { .. } | Plan::MergeAppend { .. } => true,
        Plan::Filter { input, .. }
        | Plan::Projection { input, .. }
        | Plan::Limit { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::SubqueryScan { input, .. } => plan_is_append_like_under_passthrough(input),
        _ => false,
    }
}

fn plan_contains_values_scan(plan: &Plan) -> bool {
    match plan {
        Plan::Values { .. } => true,
        Plan::Hash { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Memoize { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. }
        | Plan::Filter { input, .. }
        | Plan::Projection { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Unique { input, .. }
        | Plan::Aggregate { input, .. }
        | Plan::WindowAgg { input, .. }
        | Plan::ProjectSet { input, .. }
        | Plan::BitmapHeapScan {
            bitmapqual: input, ..
        }
        | Plan::SubqueryScan { input, .. }
        | Plan::CteScan {
            cte_plan: input, ..
        } => plan_contains_values_scan(input),
        Plan::Append { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::BitmapAnd { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::SetOp { children, .. } => children.iter().any(plan_contains_values_scan),
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::MergeJoin { left, right, .. } => {
            plan_contains_values_scan(left) || plan_contains_values_scan(right)
        }
        Plan::Result { .. }
        | Plan::SeqScan { .. }
        | Plan::TidScan { .. }
        | Plan::TidRangeScan { .. }
        | Plan::IndexOnlyScan { .. }
        | Plan::IndexScan { .. }
        | Plan::BitmapIndexScan { .. }
        | Plan::FunctionScan { .. }
        | Plan::WorkTableScan { .. }
        | Plan::RecursiveUnion { .. } => false,
    }
}

fn plan_contains_tid_range_scan(plan: &Plan) -> bool {
    match plan {
        Plan::TidRangeScan { .. } => true,
        Plan::Hash { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Memoize { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. }
        | Plan::Filter { input, .. }
        | Plan::Projection { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Unique { input, .. }
        | Plan::Aggregate { input, .. }
        | Plan::WindowAgg { input, .. }
        | Plan::ProjectSet { input, .. }
        | Plan::BitmapHeapScan {
            bitmapqual: input, ..
        }
        | Plan::SubqueryScan { input, .. }
        | Plan::CteScan {
            cte_plan: input, ..
        } => plan_contains_tid_range_scan(input),
        Plan::Append { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::BitmapAnd { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::SetOp { children, .. } => children.iter().any(plan_contains_tid_range_scan),
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::MergeJoin { left, right, .. } => {
            plan_contains_tid_range_scan(left) || plan_contains_tid_range_scan(right)
        }
        Plan::RecursiveUnion {
            anchor, recursive, ..
        } => plan_contains_tid_range_scan(anchor) || plan_contains_tid_range_scan(recursive),
        Plan::Result { .. }
        | Plan::SeqScan { .. }
        | Plan::TidScan { .. }
        | Plan::IndexOnlyScan { .. }
        | Plan::IndexScan { .. }
        | Plan::BitmapIndexScan { .. }
        | Plan::FunctionScan { .. }
        | Plan::Values { .. }
        | Plan::WorkTableScan { .. } => false,
    }
}

fn plan_contains_sql_xml_table_scan(plan: &Plan) -> bool {
    match plan {
        Plan::FunctionScan {
            call: SetReturningCall::SqlXmlTable(_),
            ..
        } => true,
        Plan::Hash { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Memoize { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. }
        | Plan::Filter { input, .. }
        | Plan::Projection { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Unique { input, .. }
        | Plan::Aggregate { input, .. }
        | Plan::WindowAgg { input, .. }
        | Plan::ProjectSet { input, .. }
        | Plan::BitmapHeapScan {
            bitmapqual: input, ..
        }
        | Plan::SubqueryScan { input, .. }
        | Plan::CteScan {
            cte_plan: input, ..
        } => plan_contains_sql_xml_table_scan(input),
        Plan::Append { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::BitmapAnd { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::SetOp { children, .. } => children.iter().any(plan_contains_sql_xml_table_scan),
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::MergeJoin { left, right, .. } => {
            plan_contains_sql_xml_table_scan(left) || plan_contains_sql_xml_table_scan(right)
        }
        Plan::RecursiveUnion {
            anchor, recursive, ..
        } => {
            plan_contains_sql_xml_table_scan(anchor) || plan_contains_sql_xml_table_scan(recursive)
        }
        Plan::Result { .. }
        | Plan::SeqScan { .. }
        | Plan::TidScan { .. }
        | Plan::TidRangeScan { .. }
        | Plan::IndexOnlyScan { .. }
        | Plan::IndexScan { .. }
        | Plan::BitmapIndexScan { .. }
        | Plan::FunctionScan { .. }
        | Plan::Values { .. }
        | Plan::WorkTableScan { .. } => false,
    }
}

fn annotate_runtime_index_labels(plan: &mut Plan, param_labels: &BTreeMap<usize, String>) {
    match plan {
        Plan::IndexOnlyScan {
            keys,
            order_by_keys,
            ..
        }
        | Plan::IndexScan {
            keys,
            order_by_keys,
            ..
        } => {
            annotate_runtime_index_key_labels(keys, param_labels);
            annotate_runtime_index_key_labels(order_by_keys, param_labels);
        }
        Plan::BitmapIndexScan { keys, .. } => {
            annotate_runtime_index_key_labels(keys, param_labels);
        }
        Plan::BitmapOr { children, .. }
        | Plan::BitmapAnd { children, .. }
        | Plan::Append { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::SetOp { children, .. } => {
            children
                .iter_mut()
                .for_each(|child| annotate_runtime_index_labels(child, param_labels));
        }
        Plan::BitmapHeapScan { bitmapqual, .. } => {
            annotate_runtime_index_labels(bitmapqual, param_labels);
        }
        Plan::Hash { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Memoize { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. }
        | Plan::Filter { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Projection { input, .. }
        | Plan::Aggregate { input, .. }
        | Plan::ProjectSet { input, .. }
        | Plan::WindowAgg { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::CteScan {
            cte_plan: input, ..
        }
        | Plan::Unique { input, .. } => annotate_runtime_index_labels(input, param_labels),
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::MergeJoin { left, right, .. } => {
            annotate_runtime_index_labels(left, param_labels);
            annotate_runtime_index_labels(right, param_labels);
        }
        Plan::RecursiveUnion {
            anchor, recursive, ..
        } => {
            annotate_runtime_index_labels(anchor, param_labels);
            annotate_runtime_index_labels(recursive, param_labels);
        }
        Plan::Result { .. }
        | Plan::SeqScan { .. }
        | Plan::TidScan { .. }
        | Plan::TidRangeScan { .. }
        | Plan::FunctionScan { .. }
        | Plan::Values { .. }
        | Plan::WorkTableScan { .. } => {}
    }
}

fn runtime_label_for_single_param(
    plan: &Plan,
    paramid: usize,
    param_labels: &BTreeMap<usize, String>,
) -> Option<String> {
    match plan {
        Plan::IndexOnlyScan {
            keys,
            order_by_keys,
            ..
        }
        | Plan::IndexScan {
            keys,
            order_by_keys,
            ..
        } => keys
            .iter()
            .chain(order_by_keys.iter())
            .find_map(|key| runtime_key_label_for_single_param(key, paramid, param_labels)),
        Plan::BitmapIndexScan { keys, .. } => keys
            .iter()
            .find_map(|key| runtime_key_label_for_single_param(key, paramid, param_labels)),
        Plan::BitmapOr { children, .. }
        | Plan::BitmapAnd { children, .. }
        | Plan::Append { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::SetOp { children, .. } => children
            .iter()
            .find_map(|child| runtime_label_for_single_param(child, paramid, param_labels)),
        Plan::BitmapHeapScan { bitmapqual, .. } => {
            runtime_label_for_single_param(bitmapqual, paramid, param_labels)
        }
        Plan::Hash { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Memoize { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. }
        | Plan::Filter { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Projection { input, .. }
        | Plan::Aggregate { input, .. }
        | Plan::ProjectSet { input, .. }
        | Plan::WindowAgg { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::CteScan {
            cte_plan: input, ..
        }
        | Plan::Unique { input, .. } => {
            runtime_label_for_single_param(input, paramid, param_labels)
        }
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::MergeJoin { left, right, .. } => {
            runtime_label_for_single_param(left, paramid, param_labels)
                .or_else(|| runtime_label_for_single_param(right, paramid, param_labels))
        }
        Plan::RecursiveUnion {
            anchor, recursive, ..
        } => runtime_label_for_single_param(anchor, paramid, param_labels)
            .or_else(|| runtime_label_for_single_param(recursive, paramid, param_labels)),
        Plan::Result { .. }
        | Plan::SeqScan { .. }
        | Plan::TidScan { .. }
        | Plan::TidRangeScan { .. }
        | Plan::FunctionScan { .. }
        | Plan::Values { .. }
        | Plan::WorkTableScan { .. } => None,
    }
}

fn runtime_key_label_for_single_param(
    key: &IndexScanKey,
    paramid: usize,
    param_labels: &BTreeMap<usize, String>,
) -> Option<String> {
    let IndexScanKeyArgument::Runtime(expr) = &key.argument else {
        return None;
    };
    let mut paramids = BTreeSet::new();
    collect_expr_exec_paramids(expr, &mut paramids);
    (paramids.len() == 1 && paramids.contains(&paramid))
        .then(|| render_runtime_param_label(expr, param_labels))
        .flatten()
}

fn annotate_runtime_index_key_labels(
    keys: &mut [IndexScanKey],
    param_labels: &BTreeMap<usize, String>,
) {
    for key in keys {
        if let IndexScanKeyArgument::Runtime(expr) = &key.argument
            && let Some(label) = render_runtime_param_label(expr, param_labels)
        {
            key.runtime_label = Some(label);
        }
    }
}

fn render_runtime_param_label(
    expr: &Expr,
    param_labels: &BTreeMap<usize, String>,
) -> Option<String> {
    match expr {
        Expr::Param(param) if matches!(param.paramkind, ParamKind::Exec) => {
            param_labels.get(&param.paramid).cloned()
        }
        Expr::Cast(inner, ty) => render_runtime_param_label(inner, param_labels)
            .map(|inner| format!("({inner})::{}", param_label_type_name(*ty))),
        Expr::Collate { expr: inner, .. } => render_runtime_param_label(inner, param_labels),
        Expr::Op(op) => {
            let has_param_label = op
                .args
                .iter()
                .any(|arg| render_runtime_param_label(arg, param_labels).is_some());
            if !has_param_label {
                return None;
            }
            let op_text = match op.op {
                pgrust_nodes::primnodes::OpExprKind::Add => "+",
                pgrust_nodes::primnodes::OpExprKind::Sub => "-",
                pgrust_nodes::primnodes::OpExprKind::Mul => "*",
                pgrust_nodes::primnodes::OpExprKind::Div => "/",
                pgrust_nodes::primnodes::OpExprKind::Mod => "%",
                pgrust_nodes::primnodes::OpExprKind::Eq => "=",
                pgrust_nodes::primnodes::OpExprKind::NotEq => "<>",
                pgrust_nodes::primnodes::OpExprKind::Lt => "<",
                pgrust_nodes::primnodes::OpExprKind::LtEq => "<=",
                pgrust_nodes::primnodes::OpExprKind::Gt => ">",
                pgrust_nodes::primnodes::OpExprKind::GtEq => ">=",
                _ => return None,
            };
            match op.args.as_slice() {
                [left, right] => Some(format!(
                    "({} {op_text} {})",
                    render_runtime_param_label_operand(left, param_labels),
                    render_runtime_param_label_operand(right, param_labels)
                )),
                [inner] => Some(format!(
                    "({op_text}{})",
                    render_runtime_param_label_operand(inner, param_labels)
                )),
                _ => None,
            }
        }
        _ => None,
    }
}

fn render_runtime_param_label_operand(
    expr: &Expr,
    param_labels: &BTreeMap<usize, String>,
) -> String {
    render_runtime_param_label(expr, param_labels).unwrap_or_else(|| {
        let rendered = crate::runtime::render_explain_expr(expr, &[]);
        rendered
            .strip_prefix('(')
            .and_then(|value| value.strip_suffix(')'))
            .unwrap_or(&rendered)
            .to_string()
    })
}

fn memoize_uses_binary_mode(plan: &Plan) -> bool {
    match plan {
        Plan::IndexOnlyScan { keys, .. } | Plan::IndexScan { keys, .. } => {
            keys.iter().any(|key| key.strategy != 3)
        }
        Plan::Projection { .. } | Plan::Limit { .. } => true,
        Plan::Filter { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. }
        | Plan::LockRows { input, .. } => memoize_uses_binary_mode(input),
        Plan::Append { children, .. } | Plan::MergeAppend { children, .. } => {
            children.iter().any(memoize_uses_binary_mode)
        }
        _ => false,
    }
}

fn plan_is_plain_seq_scan(plan: &Plan) -> bool {
    match plan {
        Plan::SeqScan { .. } => true,
        Plan::Filter {
            predicate: Expr::Const(Value::Bool(false)),
            ..
        } => false,
        Plan::Filter { input, .. } | Plan::Projection { input, .. } => {
            plan_is_plain_seq_scan(input)
        }
        _ => false,
    }
}

fn expr_list_contains_const_false(exprs: &[Expr]) -> bool {
    exprs.iter().any(expr_is_const_false)
}

fn expr_is_const_false(expr: &Expr) -> bool {
    match expr {
        Expr::Const(Value::Bool(false)) => true,
        Expr::Bool(bool_expr) if bool_expr.boolop == pgrust_nodes::primnodes::BoolExprType::And => {
            bool_expr.args.iter().any(expr_is_const_false)
        }
        _ => false,
    }
}

fn set_hash_join_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    left: Box<Path>,
    right: Box<Path>,
    kind: pgrust_nodes::primnodes::JoinType,
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
    kind: pgrust_nodes::primnodes::JoinType,
    merge_clauses: Vec<RestrictInfo>,
    outer_merge_keys: Vec<Expr>,
    inner_merge_keys: Vec<Expr>,
    merge_key_descending: Vec<bool>,
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
        merge_key_descending,
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
            .map(|entry| special_slot_var_for_expr(OUTER_VAR, entry, &target.expr))
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
    limit: Option<Expr>,
    offset: Option<Expr>,
) -> Plan {
    let limit = limit.map(|expr| lower_expr(ctx, expr, LowerMode::Scalar));
    let offset = offset.map(|expr| lower_expr(ctx, expr, LowerMode::Scalar));
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
                        nowait: row_mark.nowait,
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
    strategy: pgrust_nodes::plannodes::AggregateStrategy,
    phase: pgrust_nodes::plannodes::AggregatePhase,
    disabled: bool,
    input: Box<Path>,
    group_by: Vec<Expr>,
    group_by_refs: Vec<usize>,
    grouping_sets: Vec<Vec<usize>>,
    passthrough_exprs: Vec<Expr>,
    accumulators: Vec<AggAccum>,
    semantic_accumulators: Option<Vec<AggAccum>>,
    having: Option<Expr>,
    output_columns: Vec<QueryColumn>,
) -> Plan {
    let input_tlist = build_path_tlist(ctx.root, &input);
    let aggregate_layout = aggregate_output_vars(
        slot_id,
        phase,
        &group_by,
        &group_by_refs,
        &passthrough_exprs,
        &accumulators,
    );
    let aggregate_tlist = build_aggregate_tlist(
        ctx.root,
        slot_id,
        phase,
        &group_by,
        &group_by_refs,
        &passthrough_exprs,
        &accumulators,
        semantic_accumulators.as_deref(),
    );
    let semantic_group_by = group_by.clone();
    let semantic_passthrough_exprs = passthrough_exprs.clone();
    let semantic_output_names = (phase == pgrust_nodes::plannodes::AggregatePhase::Finalize
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
        .map(|accum| {
            lower_agg_accum(
                ctx,
                accum,
                &input,
                &input_tlist,
                &semantic_group_by,
                &semantic_passthrough_exprs,
                &aggregate_layout,
                &aggregate_tlist,
            )
        })
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
        group_by_refs,
        grouping_sets,
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
    let lower_moving_sensitive_expr = |ctx: &mut SetRefsContext<'_>, expr: Expr| {
        if !expr_contains_window_moving_volatile(&expr) {
            return lower_expr_for_input(ctx, expr);
        }
        let lowered = rebuild_setrefs_expr(root, expr, |inner| {
            lower_projection_expr_by_input_target(root, inner, input, input_tlist)
        });
        lower_expr(ctx, lowered, LowerMode::Scalar)
    };
    WindowClause {
        spec: pgrust_nodes::primnodes::WindowSpec {
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
            frame: pgrust_nodes::primnodes::WindowFrame {
                mode: clause.spec.frame.mode,
                start_bound: match clause.spec.frame.start_bound {
                    pgrust_nodes::primnodes::WindowFrameBound::OffsetPreceding(offset) => {
                        let expr = lower_expr_for_input(ctx, offset.expr.clone());
                        pgrust_nodes::primnodes::WindowFrameBound::OffsetPreceding(
                            offset.with_expr(expr),
                        )
                    }
                    pgrust_nodes::primnodes::WindowFrameBound::OffsetFollowing(offset) => {
                        let expr = lower_expr_for_input(ctx, offset.expr.clone());
                        pgrust_nodes::primnodes::WindowFrameBound::OffsetFollowing(
                            offset.with_expr(expr),
                        )
                    }
                    other => other,
                },
                end_bound: match clause.spec.frame.end_bound {
                    pgrust_nodes::primnodes::WindowFrameBound::OffsetPreceding(offset) => {
                        let expr = lower_expr_for_input(ctx, offset.expr.clone());
                        pgrust_nodes::primnodes::WindowFrameBound::OffsetPreceding(
                            offset.with_expr(expr),
                        )
                    }
                    pgrust_nodes::primnodes::WindowFrameBound::OffsetFollowing(offset) => {
                        let expr = lower_expr_for_input(ctx, offset.expr.clone());
                        pgrust_nodes::primnodes::WindowFrameBound::OffsetFollowing(
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
            .map(|func| {
                let args_are_moving_sensitive = matches!(func.kind, WindowFuncKind::Aggregate(_));
                pgrust_nodes::primnodes::WindowFuncExpr {
                    kind: match func.kind {
                        WindowFuncKind::Aggregate(aggref) => WindowFuncKind::Aggregate(Aggref {
                            args: aggref
                                .args
                                .into_iter()
                                .map(|arg| lower_moving_sensitive_expr(ctx, arg))
                                .collect(),
                            aggorder: aggref
                                .aggorder
                                .into_iter()
                                .map(|item| OrderByEntry {
                                    expr: lower_expr_for_input(ctx, item.expr),
                                    ..item
                                })
                                .collect(),
                            aggfilter: aggref
                                .aggfilter
                                .map(|expr| lower_moving_sensitive_expr(ctx, expr)),
                            ..aggref
                        }),
                        WindowFuncKind::Builtin(kind) => WindowFuncKind::Builtin(kind),
                    },
                    args: func
                        .args
                        .into_iter()
                        .map(|arg| {
                            if args_are_moving_sensitive {
                                lower_moving_sensitive_expr(ctx, arg)
                            } else {
                                lower_expr_for_input(ctx, arg)
                            }
                        })
                        .collect(),
                    ..func
                }
            })
            .collect(),
    }
}

fn expr_contains_window_moving_volatile(expr: &Expr) -> bool {
    match expr {
        Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => true,
        Expr::Op(op) => op.args.iter().any(expr_contains_window_moving_volatile),
        Expr::Bool(bool_expr) => bool_expr
            .args
            .iter()
            .any(expr_contains_window_moving_volatile),
        Expr::Func(func) => {
            matches!(
                func.implementation,
                ScalarFunctionImpl::Builtin(
                    BuiltinScalarFunction::Random | BuiltinScalarFunction::RandomNormal
                )
            ) || func.args.iter().any(expr_contains_window_moving_volatile)
        }
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => {
            expr_contains_window_moving_volatile(inner)
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
            expr_contains_window_moving_volatile(expr)
                || expr_contains_window_moving_volatile(pattern)
                || escape
                    .as_ref()
                    .is_some_and(|expr| expr_contains_window_moving_volatile(expr))
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => expr_contains_window_moving_volatile(inner),
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            expr_contains_window_moving_volatile(left)
                || expr_contains_window_moving_volatile(right)
        }
        Expr::ScalarArrayOp(saop) => {
            expr_contains_window_moving_volatile(&saop.left)
                || expr_contains_window_moving_volatile(&saop.right)
        }
        Expr::ArrayLiteral { elements, .. } => {
            elements.iter().any(expr_contains_window_moving_volatile)
        }
        Expr::Row { fields, .. } => fields
            .iter()
            .any(|(_, expr)| expr_contains_window_moving_volatile(expr)),
        Expr::FieldSelect { expr, .. } => expr_contains_window_moving_volatile(expr),
        Expr::ArraySubscript { array, subscripts } => {
            expr_contains_window_moving_volatile(array)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_some_and(expr_contains_window_moving_volatile)
                        || subscript
                            .upper
                            .as_ref()
                            .is_some_and(expr_contains_window_moving_volatile)
                })
        }
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_ref()
                .is_some_and(|expr| expr_contains_window_moving_volatile(expr))
                || case_expr.args.iter().any(|arm| {
                    expr_contains_window_moving_volatile(&arm.expr)
                        || expr_contains_window_moving_volatile(&arm.result)
                })
                || expr_contains_window_moving_volatile(&case_expr.defresult)
        }
        Expr::SetReturning(_) | Expr::SubLink(_) | Expr::SubPlan(_) => false,
        Expr::Xml(xml) => xml.child_exprs().any(expr_contains_window_moving_volatile),
        _ => false,
    }
}

fn set_window_references(
    ctx: &mut SetRefsContext<'_>,
    plan_info: PlanEstimate,
    slot_id: usize,
    input: Box<Path>,
    clause: WindowClause,
    run_condition: Option<Expr>,
    top_qual: Option<Expr>,
    output_columns: Vec<QueryColumn>,
) -> Plan {
    let input_tlist = build_path_tlist(ctx.root, &input);
    let clause = lower_window_clause_for_input(ctx, &input, &input_tlist, clause);
    let window_tlist = build_window_tlist(ctx.root, slot_id, &input, &clause, &output_columns);
    let lower_window_output_qual = |ctx: &mut SetRefsContext<'_>, expr: Expr| {
        lower_expr(
            ctx,
            expr,
            LowerMode::Input {
                path: None,
                tlist: &window_tlist,
            },
        )
    };
    let run_condition = run_condition.map(|expr| lower_window_output_qual(ctx, expr));
    let top_qual = top_qual.map(|expr| lower_window_output_qual(ctx, expr));
    Plan::WindowAgg {
        plan_info,
        input: Box::new(set_plan_refs(ctx, *input)),
        clause,
        run_condition,
        top_qual,
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
    call: pgrust_nodes::primnodes::SetReturningCall,
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
    cte_name: String,
    subroot: PlannerSubroot,
    cte_plan: Box<Path>,
    output_columns: Vec<QueryColumn>,
) -> Plan {
    Plan::CteScan {
        plan_info,
        cte_id,
        cte_name,
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
    let force_display = force_display
        || matches!(input.as_ref(), Path::ProjectSet { .. })
        || path_contains_visible_window_top_qual(input.as_ref())
        || (path_contains_window_run_condition(input.as_ref())
            && parent_filter_uses_unselected_subquery_attr(ctx.root, rtindex))
        || (path_contains_window_agg(input.as_ref())
            && path_contains_function_scan(input.as_ref()))
        || (subroot.as_ref().parse.where_qual.is_some()
            && path_contains_window_agg(input.as_ref()));
    let preserve_row_subquery = should_preserve_row_subquery_scan(input.as_ref(), &output_columns);
    if filter.is_none() && !force_display && !preserve_row_subquery {
        let input_columns = input.columns();
        if input_columns == output_columns {
            return recurse_with_root(ctx, Some(subroot.as_ref()), *input);
        }
        if input_columns.len() == output_columns.len() {
            let input_target = input.semantic_output_target();
            let input_tlist = build_path_tlist(Some(subroot.as_ref()), &input);
            let targets = output_columns
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    let expr = input_target.exprs.get(index).cloned().unwrap_or_else(|| {
                        Expr::Var(Var {
                            varno: rtindex,
                            varattno: user_attrno(index),
                            varlevelsup: 0,
                            vartype: column.sql_type,
                            collation_oid: None,
                        })
                    });
                    let ressortgroupref =
                        input_target.sortgrouprefs.get(index).copied().unwrap_or(0);
                    TargetEntry::new(column.name.clone(), expr, column.sql_type, index + 1)
                        .with_sort_group_ref(ressortgroupref)
                        .with_input_resno(index + 1)
                })
                .map(|target| TargetEntry {
                    expr: fix_upper_expr_for_input(
                        Some(subroot.as_ref()),
                        target.expr,
                        &input,
                        &input_tlist,
                    ),
                    ..target
                })
                .map(|target| TargetEntry {
                    expr: lower_expr(
                        ctx,
                        target.expr,
                        LowerMode::Input {
                            path: Some(&input),
                            tlist: &input_tlist,
                        },
                    ),
                    ..target
                })
                .collect();
            let input = recurse_with_root(ctx, Some(subroot.as_ref()), *input);
            return Plan::Projection {
                plan_info,
                input: Box::new(input),
                targets,
            };
        }
    }
    let input = recurse_with_root(ctx, Some(subroot.as_ref()), *input);
    if input.columns() == output_columns
        && filter.is_none()
        && !force_display
        && !preserve_row_subquery
    {
        input
    } else {
        let scan_name = subquery_scan_name(ctx, rtindex, &input, &output_columns);
        Plan::SubqueryScan {
            plan_info,
            input: Box::new(input),
            scan_name,
            filter,
            output_columns,
        }
    }
}

fn should_preserve_row_subquery_scan(input: &Path, output_columns: &[QueryColumn]) -> bool {
    let [column] = output_columns else {
        return false;
    };
    if !matches!(
        column.sql_type.kind,
        pgrust_nodes::parsenodes::SqlTypeKind::Record
            | pgrust_nodes::parsenodes::SqlTypeKind::Composite
    ) {
        return false;
    }
    path_is_row_projection_over_values_limit(input)
}

fn path_is_row_projection_over_values_limit(path: &Path) -> bool {
    let Path::Projection { input, targets, .. } = path else {
        return false;
    };
    projection_targets_single_row_expr(targets)
        && matches!(input.as_ref(), Path::Limit { .. } | Path::Values { .. })
}

fn projection_targets_single_row_expr(targets: &[TargetEntry]) -> bool {
    let mut visible_targets = targets.iter().filter(|target| !target.resjunk);
    let Some(target) = visible_targets.next() else {
        return false;
    };
    visible_targets.next().is_none() && target_expr_is_row_projection(&target.expr)
}

fn target_expr_is_row_projection(expr: &Expr) -> bool {
    match expr {
        Expr::Row { .. } => true,
        Expr::Cast(inner, ty)
            if matches!(
                ty.kind,
                pgrust_nodes::parsenodes::SqlTypeKind::Record
                    | pgrust_nodes::parsenodes::SqlTypeKind::Composite
            ) =>
        {
            target_expr_is_row_projection(inner)
        }
        _ => false,
    }
}

fn path_contains_window_agg(path: &Path) -> bool {
    match path {
        Path::WindowAgg { .. } => true,
        Path::Unique { input, .. }
        | Path::Filter { input, .. }
        | Path::OrderBy { input, .. }
        | Path::IncrementalSort { input, .. }
        | Path::Limit { input, .. }
        | Path::LockRows { input, .. }
        | Path::Projection { input, .. }
        | Path::Aggregate { input, .. }
        | Path::SubqueryScan { input, .. }
        | Path::ProjectSet { input, .. } => path_contains_window_agg(input),
        Path::Append { children, .. }
        | Path::MergeAppend { children, .. }
        | Path::BitmapOr { children, .. }
        | Path::BitmapAnd { children, .. }
        | Path::SetOp { children, .. } => children.iter().any(path_contains_window_agg),
        Path::BitmapHeapScan { bitmapqual, .. } => path_contains_window_agg(bitmapqual),
        Path::NestedLoopJoin { left, right, .. }
        | Path::HashJoin { left, right, .. }
        | Path::MergeJoin { left, right, .. }
        | Path::RecursiveUnion {
            anchor: left,
            recursive: right,
            ..
        } => path_contains_window_agg(left) || path_contains_window_agg(right),
        _ => false,
    }
}

fn path_contains_function_scan(path: &Path) -> bool {
    match path {
        Path::FunctionScan { .. } => true,
        Path::Unique { input, .. }
        | Path::Filter { input, .. }
        | Path::OrderBy { input, .. }
        | Path::IncrementalSort { input, .. }
        | Path::Limit { input, .. }
        | Path::LockRows { input, .. }
        | Path::Projection { input, .. }
        | Path::Aggregate { input, .. }
        | Path::WindowAgg { input, .. }
        | Path::SubqueryScan { input, .. }
        | Path::ProjectSet { input, .. } => path_contains_function_scan(input),
        Path::Append { children, .. }
        | Path::MergeAppend { children, .. }
        | Path::BitmapOr { children, .. }
        | Path::BitmapAnd { children, .. }
        | Path::SetOp { children, .. } => children.iter().any(path_contains_function_scan),
        Path::BitmapHeapScan { bitmapqual, .. } => path_contains_function_scan(bitmapqual),
        Path::NestedLoopJoin { left, right, .. }
        | Path::HashJoin { left, right, .. }
        | Path::MergeJoin { left, right, .. }
        | Path::RecursiveUnion {
            anchor: left,
            recursive: right,
            ..
        } => path_contains_function_scan(left) || path_contains_function_scan(right),
        _ => false,
    }
}

fn path_contains_window_run_condition(path: &Path) -> bool {
    match path {
        Path::WindowAgg {
            input,
            run_condition,
            top_qual,
            ..
        } => {
            run_condition.is_some()
                || top_qual.is_some()
                || path_contains_window_run_condition(input)
        }
        Path::Unique { input, .. }
        | Path::Filter { input, .. }
        | Path::OrderBy { input, .. }
        | Path::IncrementalSort { input, .. }
        | Path::Limit { input, .. }
        | Path::LockRows { input, .. }
        | Path::Projection { input, .. }
        | Path::Aggregate { input, .. }
        | Path::SubqueryScan { input, .. }
        | Path::ProjectSet { input, .. } => path_contains_window_run_condition(input),
        Path::Append { children, .. }
        | Path::MergeAppend { children, .. }
        | Path::BitmapOr { children, .. }
        | Path::BitmapAnd { children, .. }
        | Path::SetOp { children, .. } => children.iter().any(path_contains_window_run_condition),
        Path::BitmapHeapScan { bitmapqual, .. } => path_contains_window_run_condition(bitmapqual),
        Path::NestedLoopJoin { left, right, .. }
        | Path::HashJoin { left, right, .. }
        | Path::MergeJoin { left, right, .. }
        | Path::RecursiveUnion {
            anchor: left,
            recursive: right,
            ..
        } => path_contains_window_run_condition(left) || path_contains_window_run_condition(right),
        _ => false,
    }
}

fn path_contains_visible_window_top_qual(path: &Path) -> bool {
    match path {
        Path::WindowAgg {
            input, top_qual, ..
        } => {
            top_qual
                .as_ref()
                .is_some_and(|qual| !matches!(qual, Expr::Const(Value::Bool(true))))
                || path_contains_visible_window_top_qual(input)
        }
        Path::Unique { input, .. }
        | Path::Filter { input, .. }
        | Path::OrderBy { input, .. }
        | Path::IncrementalSort { input, .. }
        | Path::Limit { input, .. }
        | Path::LockRows { input, .. }
        | Path::Projection { input, .. }
        | Path::Aggregate { input, .. }
        | Path::SubqueryScan { input, .. }
        | Path::ProjectSet { input, .. } => path_contains_visible_window_top_qual(input),
        Path::Append { children, .. }
        | Path::MergeAppend { children, .. }
        | Path::BitmapOr { children, .. }
        | Path::BitmapAnd { children, .. }
        | Path::SetOp { children, .. } => {
            children.iter().any(path_contains_visible_window_top_qual)
        }
        Path::BitmapHeapScan { bitmapqual, .. } => {
            path_contains_visible_window_top_qual(bitmapqual)
        }
        Path::NestedLoopJoin { left, right, .. }
        | Path::HashJoin { left, right, .. }
        | Path::MergeJoin { left, right, .. }
        | Path::RecursiveUnion {
            anchor: left,
            recursive: right,
            ..
        } => {
            path_contains_visible_window_top_qual(left)
                || path_contains_visible_window_top_qual(right)
        }
        _ => false,
    }
}

fn parent_filter_uses_unselected_subquery_attr(root: Option<&PlannerInfo>, rtindex: usize) -> bool {
    let Some(root) = root else {
        return false;
    };
    let mut target_attrs = BTreeSet::new();
    for target in &root.parse.target_list {
        collect_rte_attrs(&target.expr, rtindex, &mut target_attrs);
    }
    let Some(where_qual) = root.parse.where_qual.as_ref() else {
        return false;
    };
    let mut filter_attrs = BTreeSet::new();
    collect_rte_attrs(where_qual, rtindex, &mut filter_attrs);
    filter_attrs.iter().any(|attr| !target_attrs.contains(attr))
}

fn collect_rte_attrs(expr: &Expr, rtindex: usize, attrs: &mut BTreeSet<usize>) {
    match expr {
        Expr::Var(var) if var.varlevelsup == 0 && var.varno == rtindex => {
            if let Some(index) = attrno_index(var.varattno) {
                attrs.insert(index);
            }
        }
        Expr::Aggref(aggref) => {
            for expr in aggref.direct_args.iter().chain(aggref.args.iter()) {
                collect_rte_attrs(expr, rtindex, attrs);
            }
            for item in &aggref.aggorder {
                collect_rte_attrs(&item.expr, rtindex, attrs);
            }
            if let Some(filter) = aggref.aggfilter.as_ref() {
                collect_rte_attrs(filter, rtindex, attrs);
            }
        }
        Expr::WindowFunc(func) => {
            for expr in &func.args {
                collect_rte_attrs(expr, rtindex, attrs);
            }
            if let WindowFuncKind::Aggregate(aggref) = &func.kind {
                for expr in aggref.direct_args.iter().chain(aggref.args.iter()) {
                    collect_rte_attrs(expr, rtindex, attrs);
                }
                for item in &aggref.aggorder {
                    collect_rte_attrs(&item.expr, rtindex, attrs);
                }
                if let Some(filter) = aggref.aggfilter.as_ref() {
                    collect_rte_attrs(filter, rtindex, attrs);
                }
            }
        }
        Expr::Op(op) => {
            for arg in &op.args {
                collect_rte_attrs(arg, rtindex, attrs);
            }
        }
        Expr::Bool(bool_expr) => {
            for arg in &bool_expr.args {
                collect_rte_attrs(arg, rtindex, attrs);
            }
        }
        Expr::Func(func) => {
            for arg in &func.args {
                collect_rte_attrs(arg, rtindex, attrs);
            }
        }
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::FieldSelect { expr: inner, .. } => collect_rte_attrs(inner, rtindex, attrs),
        Expr::Coalesce(left, right)
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right) => {
            collect_rte_attrs(left, rtindex, attrs);
            collect_rte_attrs(right, rtindex, attrs);
        }
        Expr::Case(case_expr) => {
            if let Some(arg) = case_expr.arg.as_ref() {
                collect_rte_attrs(arg, rtindex, attrs);
            }
            for arm in &case_expr.args {
                collect_rte_attrs(&arm.expr, rtindex, attrs);
                collect_rte_attrs(&arm.result, rtindex, attrs);
            }
            collect_rte_attrs(&case_expr.defresult, rtindex, attrs);
        }
        _ => {}
    }
}

fn subquery_scan_name(
    ctx: &SetRefsContext<'_>,
    rtindex: usize,
    input: &Plan,
    output_columns: &[QueryColumn],
) -> Option<String> {
    let name = ctx
        .root
        .and_then(|root| root.parse.rtable.get(rtindex.saturating_sub(1)))
        .and_then(|rte| {
            rte.alias
                .clone()
                .or_else(|| (!rte.eref.aliasname.is_empty()).then(|| rte.eref.aliasname.clone()))
        });
    if name.as_deref() == Some("subquery") && plan_contains_function_scan(input) {
        return output_columns.first().map(|column| column.name.clone());
    }
    name
}

fn plan_contains_function_scan(plan: &Plan) -> bool {
    match plan {
        Plan::FunctionScan { .. } => true,
        Plan::Unique { input, .. }
        | Plan::Filter { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Projection { input, .. }
        | Plan::Aggregate { input, .. }
        | Plan::WindowAgg { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::ProjectSet { input, .. }
        | Plan::Hash { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Memoize { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. } => plan_contains_function_scan(input),
        Plan::Append { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::BitmapAnd { children, .. }
        | Plan::SetOp { children, .. } => children.iter().any(plan_contains_function_scan),
        Plan::BitmapHeapScan { bitmapqual, .. } => plan_contains_function_scan(bitmapqual),
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::MergeJoin { left, right, .. }
        | Plan::RecursiveUnion {
            anchor: left,
            recursive: right,
            ..
        } => plan_contains_function_scan(left) || plan_contains_function_scan(right),
        _ => false,
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
    recursive_references_worktable: bool,
    anchor_query: Box<pgrust_nodes::parsenodes::Query>,
    recursive_query: Box<pgrust_nodes::parsenodes::Query>,
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
    targets: Vec<pgrust_nodes::primnodes::ProjectSetTarget>,
) -> Plan {
    let input_tlist = build_path_tlist(ctx.root, &input);
    let lowered_targets = targets
        .into_iter()
        .map(|target| {
            let target = match target {
                pgrust_nodes::primnodes::ProjectSetTarget::Scalar(entry) => {
                    pgrust_nodes::primnodes::ProjectSetTarget::Scalar(TargetEntry {
                        expr: fix_upper_expr_for_input(ctx.root, entry.expr, &input, &input_tlist),
                        ..entry
                    })
                }
                pgrust_nodes::primnodes::ProjectSetTarget::Set {
                    name,
                    source_expr,
                    call,
                    sql_type,
                    column_index,
                    ressortgroupref,
                } => pgrust_nodes::primnodes::ProjectSetTarget::Set {
                    name,
                    source_expr,
                    call: fix_set_returning_call_upper_exprs(ctx.root, call, &input, &input_tlist),
                    sql_type,
                    column_index,
                    ressortgroupref,
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
    let path = reassociate_clone_left_join_path(path);
    match path {
        Path::Result { plan_info, .. } => Plan::Result { plan_info },
        Path::Append {
            plan_info,
            source_id,
            desc,
            relids,
            child_roots,
            partition_prune,
            children,
            ..
        } => set_append_references(
            ctx,
            plan_info,
            source_id,
            desc,
            relids,
            child_roots,
            partition_prune,
            children,
        ),
        Path::MergeAppend {
            plan_info,
            source_id,
            desc,
            items,
            partition_prune,
            children,
            ..
        } => set_merge_append_references(
            ctx,
            plan_info,
            source_id,
            desc,
            items,
            partition_prune,
            children,
        ),
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
            disabled || ctx.root.is_some_and(|root| !root.config.enable_seqscan),
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
        Path::BitmapAnd {
            plan_info,
            children,
            ..
        } => set_bitmap_and_references(ctx, plan_info, children),
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
            merge_key_descending,
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
            merge_key_descending,
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
            group_by_refs,
            grouping_sets,
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
            group_by_refs,
            grouping_sets,
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
            run_condition,
            top_qual,
            output_columns,
            ..
        } => set_window_references(
            ctx,
            plan_info,
            slot_id,
            input,
            clause,
            run_condition,
            top_qual,
            output_columns,
        ),
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
            cte_name,
            subroot,
            query,
            cte_plan,
            output_columns,
            ..
        } => {
            let _ = query;
            set_cte_scan_references(
                ctx,
                plan_info,
                cte_id,
                cte_name,
                subroot,
                cte_plan,
                output_columns,
            )
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

fn reassociate_clone_left_join_path(path: Path) -> Path {
    let Path::NestedLoopJoin {
        plan_info,
        pathtarget,
        output_columns,
        left,
        right,
        kind: JoinType::Left,
        restrict_clauses,
    } = path
    else {
        return path;
    };
    if !restrict_clauses_contain_null_test(&restrict_clauses) {
        return Path::NestedLoopJoin {
            plan_info,
            pathtarget,
            output_columns,
            left,
            right,
            kind: JoinType::Left,
            restrict_clauses,
        };
    }

    let left_path = *left;
    let (
        left_plan_info,
        left_pathtarget,
        left_output_columns,
        grand_left,
        middle,
        left_restrict_clauses,
    ) = match left_path {
        Path::NestedLoopJoin {
            plan_info,
            pathtarget,
            output_columns,
            left,
            right,
            kind: JoinType::Left,
            restrict_clauses,
        } => (
            plan_info,
            pathtarget,
            output_columns,
            left,
            right,
            restrict_clauses,
        ),
        other_left => {
            return Path::NestedLoopJoin {
                plan_info,
                pathtarget,
                output_columns,
                left: Box::new(other_left),
                right,
                kind: JoinType::Left,
                restrict_clauses,
            };
        }
    };
    if !left_restrict_clauses.is_empty() {
        return Path::NestedLoopJoin {
            plan_info,
            pathtarget,
            output_columns,
            left: Box::new(Path::NestedLoopJoin {
                plan_info: left_plan_info,
                pathtarget: left_pathtarget,
                output_columns: left_output_columns,
                left: grand_left,
                right: middle,
                kind: JoinType::Left,
                restrict_clauses: left_restrict_clauses,
            }),
            right,
            kind: JoinType::Left,
            restrict_clauses,
        };
    }

    // :HACK: PostgreSQL's clone-clause predicate plans keep nullable-side
    // NullTests inside the right subtree of an unqualified left join. Rebuild
    // that equivalent association before setrefs so OUTER/INNER vars lower
    // against the same child tlists PostgreSQL displays.
    let mut inner_output_columns = middle.columns();
    inner_output_columns.extend(right.columns());
    let mut inner_exprs = middle.semantic_output_target().exprs;
    inner_exprs.extend(right.semantic_output_target().exprs);
    let inner = Path::NestedLoopJoin {
        plan_info,
        pathtarget: PathTarget::new(inner_exprs),
        output_columns: inner_output_columns,
        left: middle,
        right,
        kind: JoinType::Left,
        restrict_clauses,
    };
    Path::NestedLoopJoin {
        plan_info,
        pathtarget,
        output_columns,
        left: grand_left,
        right: Box::new(inner),
        kind: JoinType::Left,
        restrict_clauses: left_restrict_clauses,
    }
}

fn restrict_clauses_contain_null_test(restrict_clauses: &[RestrictInfo]) -> bool {
    restrict_clauses
        .iter()
        .any(|restrict| expr_contains_null_test(&restrict.clause))
}

fn expr_contains_null_test(expr: &Expr) -> bool {
    match expr {
        Expr::IsNull(_) | Expr::IsNotNull(_) => true,
        Expr::Bool(bool_expr) => bool_expr.args.iter().any(expr_contains_null_test),
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => expr_contains_null_test(inner),
        Expr::Op(op) => op.args.iter().any(expr_contains_null_test),
        _ => false,
    }
}

fn split_join_restrict_clauses<'a>(
    kind: pgrust_nodes::primnodes::JoinType,
    restrict_clauses: &'a [RestrictInfo],
) -> (&'a [RestrictInfo], &'a [RestrictInfo]) {
    if matches!(
        kind,
        pgrust_nodes::primnodes::JoinType::Inner | pgrust_nodes::primnodes::JoinType::Cross
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
    if is_whole_row_var_expr(&expr) {
        return expr;
    }
    if let Some(entry) = search_tlist_entry(root, &expr, tlist) {
        return special_slot_var_for_expr(OUTER_VAR, entry, &expr);
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
    if exprs_match_for_setrefs(left, right) {
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
            && left_window.winno == right_window.winno
            && left_window.result_type == right_window.result_type
        {
            return true;
        }
    }
    if let (Expr::Aggref(left_agg), Expr::Aggref(right_agg)) = (left, right)
        && left_agg.aggno == right_agg.aggno
        && left_agg.aggfnoid == right_agg.aggfnoid
        && left_agg.aggtype == right_agg.aggtype
    {
        return true;
    }
    let Some(root) = root else {
        return false;
    };
    let flattened_left = maybe_flatten_join_alias_vars(root, left);
    let flattened_right = maybe_flatten_join_alias_vars(root, right);
    match (flattened_left.as_ref(), flattened_right.as_ref()) {
        (None, None) => false,
        (Some(left), None) => exprs_match_for_setrefs(left, right),
        (None, Some(right)) => exprs_match_for_setrefs(left, right),
        (Some(left), Some(right)) => exprs_match_for_setrefs(left, right),
    }
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
        Expr::Case(case_expr) => Expr::Case(Box::new(pgrust_nodes::primnodes::CaseExpr {
            arg: case_expr.arg.map(|arg| Box::new(recurse(*arg))),
            args: case_expr
                .args
                .into_iter()
                .map(|arm| pgrust_nodes::primnodes::CaseWhen {
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
        Expr::SubLink(sublink) => Expr::SubLink(Box::new(pgrust_nodes::primnodes::SubLink {
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
        Expr::Row { descriptor, fields } => Expr::Row {
            descriptor,
            fields: fields
                .into_iter()
                .map(|(name, expr)| (name, recurse(expr)))
                .collect(),
        },
        Expr::FieldSelect {
            expr,
            field,
            field_type,
        } => Expr::FieldSelect {
            expr: Box::new(recurse(*expr)),
            field,
            field_type,
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
    attno: pgrust_nodes::primnodes::AttrNumber,
    vartype: pgrust_nodes::parsenodes::SqlType,
) -> Expr {
    slot_var_with_collation(slot_id, attno, vartype, None)
}

fn slot_var_with_collation(
    slot_id: usize,
    attno: pgrust_nodes::primnodes::AttrNumber,
    vartype: pgrust_nodes::parsenodes::SqlType,
    collation_oid: Option<u32>,
) -> Expr {
    Expr::Var(Var {
        varno: slot_id,
        varattno: attno,
        varlevelsup: 0,
        vartype,
        collation_oid,
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
        Expr::Row { descriptor, fields } => Expr::Row {
            descriptor,
            fields: fields
                .into_iter()
                .map(|(name, expr)| (name, fully_expand_output_expr(expr, path)))
                .collect(),
        },
        Expr::FieldSelect {
            expr,
            field,
            field_type,
        } => Expr::FieldSelect {
            expr: Box::new(fully_expand_output_expr(*expr, path)),
            field,
            field_type,
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
    let expr = root
        .and_then(|root| maybe_flatten_join_alias_vars(root, &expr))
        .unwrap_or(expr);
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
    if is_whole_row_var_expr(&expr) {
        return expr;
    }
    if let Some(entry) = search_tlist_entry(root, &expr, outer_tlist) {
        return special_slot_var_for_expr(OUTER_VAR, entry, &expr);
    }
    if let Some(entry) = search_tlist_entry(root, &expr, inner_tlist) {
        return special_slot_var_for_expr(INNER_VAR, entry, &expr);
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
