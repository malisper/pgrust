use std::collections::{BTreeMap, BTreeSet};

use crate::backend::executor::jsonb::render_jsonb_bytes;
use crate::backend::executor::jsonpath::canonicalize_jsonpath;
use crate::backend::executor::{
    executor_start, render_explain_expr, render_index_order_by,
    render_index_scan_condition_with_key_names,
    render_index_scan_condition_with_key_names_and_runtime_renderer,
    render_verbose_range_support_expr, set_returning_call_label,
};
use crate::backend::parser::CatalogLookup;
use crate::include::catalog::builtin_aggregate_function_for_proc_oid;
use crate::include::nodes::datum::Value;
use crate::include::nodes::execnodes::*;
use crate::include::nodes::plannodes::{AggregateStrategy, Plan, PlanEstimate};
use crate::include::nodes::primnodes::{
    AggAccum, BuiltinScalarFunction, Expr, INNER_VAR, JoinType, OUTER_VAR, ParamKind,
    ProjectSetTarget, QueryColumn, ScalarFunctionImpl, SetReturningCall, SqlJsonTable,
    SqlJsonTableBehavior, SqlJsonTableColumn, SqlJsonTableColumnKind, SqlJsonTablePlan,
    SqlJsonTableQuotes, SqlJsonTableWrapper, SqlXmlTable, SqlXmlTableColumnKind, SubPlan,
    TargetEntry, WindowClause, WindowFrameBound, WindowFuncKind, attrno_index,
    set_returning_call_exprs, user_attrno,
};
use crate::include::storage::buf_internals::BufferUsageStats;

pub(crate) fn format_explain_lines(
    state: &dyn PlanNode,
    indent: usize,
    analyze: bool,
    lines: &mut Vec<String>,
) {
    format_explain_lines_with_costs(state, indent, analyze, true, true, lines);
}

pub(crate) fn format_explain_lines_with_costs(
    state: &dyn PlanNode,
    indent: usize,
    analyze: bool,
    show_costs: bool,
    show_timing: bool,
    lines: &mut Vec<String>,
) {
    format_explain_lines_with_options(state, indent, analyze, show_costs, show_timing, lines);
}

pub(crate) fn format_explain_lines_with_options(
    state: &dyn PlanNode,
    indent: usize,
    analyze: bool,
    show_costs: bool,
    show_timing: bool,
    lines: &mut Vec<String>,
) {
    format_explain_lines_with_options_inner(state, indent, analyze, show_costs, show_timing, lines);
}

fn format_explain_lines_with_options_inner(
    state: &dyn PlanNode,
    indent: usize,
    analyze: bool,
    show_costs: bool,
    show_timing: bool,
    lines: &mut Vec<String>,
) {
    if let Some(child) = state.explain_passthrough() {
        format_explain_lines_with_options_inner(
            child,
            indent,
            analyze,
            show_costs,
            show_timing,
            lines,
        );
        return;
    }
    push_explain_state_line(state, indent, analyze, show_costs, show_timing, lines);
    state.explain_details(indent, analyze, show_costs, lines);
    state.explain_children(indent, analyze, show_costs, show_timing, lines);
}

pub(crate) fn push_explain_line(
    label: &str,
    plan_info: PlanEstimate,
    show_costs: bool,
    lines: &mut Vec<String>,
) {
    if show_costs {
        lines.push(format!(
            "{label}  (cost={:.2}..{:.2} rows={} width={})",
            plan_info.startup_cost.as_f64(),
            plan_info.total_cost.as_f64(),
            plan_info.plan_rows.as_f64().round() as u64,
            plan_info.plan_width
        ));
    } else {
        lines.push(label.to_string());
    }
}

pub(crate) fn format_explain_plan_with_subplans(
    plan: &Plan,
    subplans: &[Plan],
    indent: usize,
    show_costs: bool,
    lines: &mut Vec<String>,
) {
    format_explain_plan_with_subplans_inner(
        plan,
        subplans,
        indent,
        show_costs,
        false,
        false,
        false,
        &VerboseExplainContext::default(),
        lines,
    );
}

pub(crate) fn format_verbose_explain_plan_with_subplans(
    plan: &Plan,
    subplans: &[Plan],
    indent: usize,
    show_costs: bool,
    lines: &mut Vec<String>,
) {
    format_verbose_explain_plan_with_context(
        plan,
        subplans,
        indent,
        show_costs,
        VerboseExplainContext::default(),
        lines,
    );
}

pub(crate) fn format_verbose_explain_plan_with_catalog(
    plan: &Plan,
    subplans: &[Plan],
    indent: usize,
    show_costs: bool,
    catalog: &dyn CatalogLookup,
    lines: &mut Vec<String>,
) {
    let ctx = VerboseExplainContext {
        type_names: collect_explain_type_names(plan, subplans, catalog),
        ..VerboseExplainContext::default()
    };
    format_verbose_explain_plan_with_context(plan, subplans, indent, show_costs, ctx, lines);
}

pub(crate) fn format_verbose_explain_plan_json_with_catalog(
    plan: &Plan,
    subplans: &[Plan],
    catalog: &dyn CatalogLookup,
) -> Option<String> {
    if !subplans.is_empty() {
        return None;
    }
    let ctx = VerboseExplainContext {
        type_names: collect_explain_type_names(plan, subplans, catalog),
        ..VerboseExplainContext::default()
    };
    let mut lines = vec!["[".into(), "  {".into(), "    \"Plan\": {".into()];
    push_verbose_json_plan(plan, None, 6, &ctx, &mut lines)?;
    lines.push("    }".into());
    lines.push("  }".into());
    lines.push("]".into());
    Some(lines.join("\n"))
}

fn json_string_literal(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"\"".into())
}

fn push_verbose_json_plan(
    plan: &Plan,
    parent_relationship: Option<&str>,
    indent: usize,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) -> Option<()> {
    if let Some((input, output)) = projected_join_for_explain(plan, ctx) {
        return push_verbose_json_plan_with_output(
            input,
            parent_relationship,
            indent,
            ctx,
            output,
            lines,
        );
    }
    let plan = explain_passthrough_plan_child(plan).unwrap_or(plan);
    let output = verbose_display_output_exprs(plan, ctx, true);
    push_verbose_json_plan_with_output(plan, parent_relationship, indent, ctx, output, lines)
}

fn push_verbose_json_plan_with_output(
    plan: &Plan,
    parent_relationship: Option<&str>,
    indent: usize,
    ctx: &VerboseExplainContext,
    output: Vec<String>,
    lines: &mut Vec<String>,
) -> Option<()> {
    let pad = " ".repeat(indent);
    match plan {
        Plan::NestedLoopJoin {
            left,
            right,
            nest_params,
            ..
        } => {
            lines.push(format!("{pad}\"Node Type\": \"Nested Loop\","));
            push_json_parent_relationship(parent_relationship, indent, lines);
            lines.push(format!("{pad}\"Parallel Aware\": false,"));
            lines.push(format!("{pad}\"Async Capable\": false,"));
            lines.push(format!("{pad}\"Join Type\": \"Inner\","));
            lines.push(format!("{pad}\"Disabled\": false,"));
            lines.push(format!("{pad}\"Output\": {},", json_string_array(&output)));
            lines.push(format!("{pad}\"Inner Unique\": false,"));
            lines.push(format!("{pad}\"Plans\": ["));
            lines.push(format!("{pad}  {{"));
            push_verbose_json_plan(left, Some("Outer"), indent + 4, ctx, lines)?;
            lines.push(format!("{pad}  }},"));
            let mut right_ctx = ctx.clone();
            let left_names = verbose_plan_output_exprs(left, ctx, true);
            right_ctx
                .exec_params
                .extend(nest_params.iter().map(|source| VerboseExecParam {
                    paramid: source.paramid,
                    expr: source.expr.clone(),
                    column_names: left_names.clone(),
                }));
            lines.push(format!("{pad}  {{"));
            push_verbose_json_plan(right, Some("Inner"), indent + 4, &right_ctx, lines)?;
            lines.push(format!("{pad}  }}"));
            lines.push(format!("{pad}]"));
            Some(())
        }
        Plan::SeqScan { relation_name, .. } => {
            let (relation, alias) = explain_relation_and_alias(relation_name);
            lines.push(format!("{pad}\"Node Type\": \"Seq Scan\","));
            push_json_parent_relationship(parent_relationship, indent, lines);
            lines.push(format!("{pad}\"Parallel Aware\": false,"));
            lines.push(format!("{pad}\"Async Capable\": false,"));
            lines.push(format!(
                "{pad}\"Relation Name\": {},",
                json_string_literal(relation)
            ));
            lines.push(format!("{pad}\"Schema\": \"public\","));
            lines.push(format!("{pad}\"Alias\": {},", json_string_literal(alias)));
            lines.push(format!("{pad}\"Disabled\": false,"));
            lines.push(format!("{pad}\"Output\": {}", json_string_array(&output)));
            Some(())
        }
        Plan::FunctionScan {
            call, table_alias, ..
        } if matches!(
            call,
            SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_)
        ) =>
        {
            push_verbose_json_function_scan(
                call,
                table_alias.as_deref(),
                parent_relationship,
                indent,
                ctx,
                output,
                None,
                lines,
            )
        }
        Plan::Filter {
            input, predicate, ..
        } if matches!(
            input.as_ref(),
            Plan::FunctionScan {
                call: SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_),
                ..
            }
        ) =>
        {
            let Plan::FunctionScan {
                call, table_alias, ..
            } = input.as_ref()
            else {
                return None;
            };
            let input_names = verbose_plan_output_exprs(input, ctx, true);
            let filter = render_verbose_expr(predicate, &input_names, ctx);
            push_verbose_json_function_scan(
                call,
                table_alias.as_deref(),
                parent_relationship,
                indent,
                ctx,
                input_names,
                Some(filter),
                lines,
            )
        }
        _ => None,
    }
}

fn push_verbose_json_function_scan(
    call: &SetReturningCall,
    table_alias: Option<&str>,
    parent_relationship: Option<&str>,
    indent: usize,
    ctx: &VerboseExplainContext,
    output: Vec<String>,
    filter: Option<String>,
    lines: &mut Vec<String>,
) -> Option<()> {
    let table_function_name = match call {
        SetReturningCall::SqlJsonTable(_) => "json_table",
        SetReturningCall::SqlXmlTable(_) => "xmltable",
        _ => return None,
    };
    let pad = " ".repeat(indent);
    lines.push(format!("{pad}\"Node Type\": \"Table Function Scan\","));
    push_json_parent_relationship(parent_relationship, indent, lines);
    lines.push(format!("{pad}\"Parallel Aware\": false,"));
    lines.push(format!("{pad}\"Async Capable\": false,"));
    lines.push(format!(
        "{pad}\"Table Function Name\": \"{table_function_name}\","
    ));
    if let Some(alias) = table_alias {
        lines.push(format!("{pad}\"Alias\": {},", json_string_literal(alias)));
    }
    lines.push(format!("{pad}\"Disabled\": false,"));
    lines.push(format!("{pad}\"Output\": {},", json_string_array(&output)));
    lines.push(format!(
        "{pad}\"Table Function Call\": {}{}",
        json_string_literal(&render_verbose_set_returning_call(call, ctx)),
        if filter.is_some() { "," } else { "" }
    ));
    if let Some(filter) = filter {
        lines.push(format!("{pad}\"Filter\": {}", json_string_literal(&filter)));
    }
    Some(())
}

fn push_json_parent_relationship(
    parent_relationship: Option<&str>,
    indent: usize,
    lines: &mut Vec<String>,
) {
    if let Some(parent_relationship) = parent_relationship {
        let pad = " ".repeat(indent);
        lines.push(format!(
            "{pad}\"Parent Relationship\": {},",
            json_string_literal(parent_relationship)
        ));
    }
}

fn json_string_array(values: &[String]) -> String {
    let rendered = values
        .iter()
        .map(|value| json_string_literal(value))
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{rendered}]")
}

fn explain_relation_and_alias(relation_name: &str) -> (&str, &str) {
    relation_name
        .split_once(' ')
        .map(|(relation, alias)| (relation, alias.trim()))
        .unwrap_or((relation_name, relation_name))
}

fn format_verbose_explain_plan_with_context(
    plan: &Plan,
    subplans: &[Plan],
    indent: usize,
    show_costs: bool,
    ctx: VerboseExplainContext,
    lines: &mut Vec<String>,
) {
    format_explain_plan_with_subplans_inner(
        plan, subplans, indent, show_costs, true, false, false, &ctx, lines,
    );
}

fn format_explain_plan_with_subplans_inner(
    plan: &Plan,
    subplans: &[Plan],
    indent: usize,
    show_costs: bool,
    verbose: bool,
    is_child: bool,
    qualify_aggregate_group_keys: bool,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) {
    if let Some(plan_info) = const_false_filter_result_plan(plan) {
        let prefix = explain_node_prefix(indent, is_child);
        push_explain_line(&format!("{prefix}Result"), plan_info, show_costs, lines);
        let detail_prefix = explain_detail_prefix(indent);
        lines.push(format!("{detail_prefix}One-Time Filter: false"));
        return;
    }

    if let Some(child) = explain_passthrough_plan_child(plan)
        && (!verbose || explain_passthrough_applies_in_verbose(plan))
    {
        format_explain_plan_with_subplans_inner(
            child,
            subplans,
            indent,
            show_costs,
            verbose,
            is_child,
            qualify_aggregate_group_keys,
            ctx,
            lines,
        );
        return;
    }

    if !verbose && let Some(join_plan) = filter_as_join_filter_plan(plan) {
        format_explain_plan_with_subplans_inner(
            &join_plan,
            subplans,
            indent,
            show_costs,
            verbose,
            is_child,
            qualify_aggregate_group_keys,
            ctx,
            lines,
        );
        return;
    }

    if !verbose && let Some(join_plan) = swapped_partition_hash_join_display_plan(plan) {
        format_explain_plan_with_subplans_inner(
            &join_plan,
            subplans,
            indent,
            show_costs,
            verbose,
            is_child,
            qualify_aggregate_group_keys,
            ctx,
            lines,
        );
        return;
    }

    if !verbose && let Some(aggregate_plan) = dummy_empty_group_aggregate_display_plan(plan) {
        format_explain_plan_with_subplans_inner(
            &aggregate_plan,
            subplans,
            indent,
            show_costs,
            verbose,
            is_child,
            qualify_aggregate_group_keys,
            ctx,
            lines,
        );
        return;
    }

    if verbose
        && push_verbose_projected_join_plan(
            plan, subplans, indent, show_costs, is_child, ctx, lines,
        )
    {
        return;
    }

    if verbose
        && push_verbose_filtered_function_scan_plan(plan, indent, show_costs, is_child, ctx, lines)
    {
        return;
    }

    if verbose
        && push_verbose_projected_scan_plan(
            plan, subplans, indent, show_costs, is_child, ctx, lines,
        )
    {
        return;
    }

    let state = executor_start(plan.clone());
    if verbose {
        push_explain_plan_line(plan, state.as_ref(), indent, is_child, show_costs, lines);
        push_verbose_plan_details(plan, indent, ctx, lines);
    } else {
        push_explain_plan_state_line(
            plan,
            state.as_ref(),
            indent,
            is_child,
            show_costs,
            ctx,
            lines,
        );
        if !push_nonverbose_plan_details(plan, indent, qualify_aggregate_group_keys, ctx, lines) {
            state.explain_details(indent, false, show_costs, lines);
        }
    }

    push_direct_plan_subplans(plan, subplans, indent, show_costs, verbose, ctx, lines);

    explain_plan_children_with_context(plan, subplans, indent, show_costs, verbose, ctx, lines);
}

fn push_direct_plan_subplans(
    plan: &Plan,
    subplans: &[Plan],
    indent: usize,
    show_costs: bool,
    verbose: bool,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) {
    for subplan in direct_plan_subplans(plan) {
        let prefix = "  ".repeat(indent + 1);
        let label = if subplan.par_param.is_empty() {
            format!("{prefix}InitPlan {}", subplan.plan_id + 1)
        } else {
            format!("{prefix}SubPlan {}", subplan.plan_id + 1)
        };
        lines.push(label);
        if let Some(child) = subplans.get(subplan.plan_id) {
            let child_ctx = subplan_explain_context(plan, subplan, ctx);
            format_explain_plan_with_subplans_inner(
                child,
                subplans,
                indent + 2,
                show_costs,
                verbose,
                true,
                false,
                &child_ctx,
                lines,
            );
        }
    }
}

fn subplan_explain_context(
    parent: &Plan,
    subplan: &SubPlan,
    ctx: &VerboseExplainContext,
) -> VerboseExplainContext {
    if subplan.par_param.is_empty() || subplan.args.is_empty() {
        return ctx.clone();
    }
    let mut child_ctx = ctx.clone();
    let column_names = plan_join_output_exprs(parent, ctx, true);
    child_ctx.exec_params.extend(
        subplan
            .par_param
            .iter()
            .copied()
            .zip(subplan.args.iter().cloned())
            .map(|(paramid, expr)| VerboseExecParam {
                paramid,
                expr,
                column_names: column_names.clone(),
            }),
    );
    child_ctx
}

fn explain_passthrough_plan_child(plan: &Plan) -> Option<&Plan> {
    match plan {
        Plan::Projection { input, targets, .. } => {
            projection_targets_are_explain_passthrough(input, targets).then_some(input.as_ref())
        }
        Plan::Filter { input, .. }
            if matches!(
                input.as_ref(),
                Plan::Append { .. } | Plan::MergeAppend { .. }
            ) =>
        {
            Some(input.as_ref())
        }
        Plan::Append { children, .. } if children.len() == 1 => children.first(),
        _ => None,
    }
}

fn filter_as_join_filter_plan(plan: &Plan) -> Option<Plan> {
    let Plan::Filter {
        input, predicate, ..
    } = plan
    else {
        return None;
    };
    let mut join_plan = input.as_ref().clone();
    match &mut join_plan {
        Plan::NestedLoopJoin {
            kind, left, qual, ..
        }
        | Plan::HashJoin {
            kind, left, qual, ..
        }
        | Plan::MergeJoin {
            kind, left, qual, ..
        } if matches!(kind, JoinType::Left | JoinType::Full) => {
            qual.push(filter_predicate_to_join_qual(
                predicate.clone(),
                left.columns().len(),
            ));
            Some(join_plan)
        }
        _ => None,
    }
}

fn filter_predicate_to_join_qual(expr: Expr, left_width: usize) -> Expr {
    match expr {
        Expr::Var(mut var)
            if var.varno == OUTER_VAR
                && attrno_index(var.varattno).is_some_and(|index| index >= left_width) =>
        {
            let index = attrno_index(var.varattno).expect("checked above");
            var.varno = INNER_VAR;
            var.varattno = user_attrno(index - left_width);
            Expr::Var(var)
        }
        Expr::Op(op) => Expr::Op(Box::new(crate::include::nodes::primnodes::OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| filter_predicate_to_join_qual(arg, left_width))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(crate::include::nodes::primnodes::BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| filter_predicate_to_join_qual(arg, left_width))
                .collect(),
            ..*bool_expr
        })),
        Expr::Cast(inner, ty) => Expr::Cast(
            Box::new(filter_predicate_to_join_qual(*inner, left_width)),
            ty,
        ),
        Expr::IsNull(inner) => {
            Expr::IsNull(Box::new(filter_predicate_to_join_qual(*inner, left_width)))
        }
        Expr::IsNotNull(inner) => {
            Expr::IsNotNull(Box::new(filter_predicate_to_join_qual(*inner, left_width)))
        }
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(filter_predicate_to_join_qual(*left, left_width)),
            Box::new(filter_predicate_to_join_qual(*right, left_width)),
        ),
        other => other,
    }
}

fn swapped_partition_hash_join_display_plan(plan: &Plan) -> Option<Plan> {
    let Plan::HashJoin {
        plan_info,
        left,
        right,
        kind,
        hash_clauses,
        hash_keys,
        join_qual,
        qual,
    } = plan
    else {
        return None;
    };
    if !join_qual.is_empty() || !qual.is_empty() {
        return None;
    }
    let Plan::Hash {
        plan_info: hash_plan_info,
        input: hash_input,
        hash_keys: inner_hash_keys,
    } = right.as_ref()
    else {
        return None;
    };
    let display_kind = match kind {
        JoinType::Inner => JoinType::Inner,
        JoinType::Left => JoinType::Right,
        JoinType::Right => JoinType::Left,
        JoinType::Full => JoinType::Full,
        JoinType::Semi | JoinType::Anti | JoinType::Cross => return None,
    };
    if !partition_hash_join_display_prefers_swapped(left, hash_input) {
        return None;
    }

    Some(Plan::HashJoin {
        plan_info: *plan_info,
        left: hash_input.clone(),
        right: Box::new(Plan::Hash {
            plan_info: *hash_plan_info,
            input: left.clone(),
            hash_keys: hash_keys.clone(),
        }),
        kind: display_kind,
        hash_clauses: hash_clauses.clone(),
        hash_keys: inner_hash_keys.clone(),
        join_qual: Vec::new(),
        qual: Vec::new(),
    })
}

fn dummy_empty_group_aggregate_display_plan(plan: &Plan) -> Option<Plan> {
    let Plan::OrderBy {
        input,
        items,
        display_items,
        ..
    } = plan
    else {
        return None;
    };
    let Plan::Aggregate {
        plan_info,
        strategy,
        phase,
        disabled,
        input: aggregate_input,
        group_by,
        passthrough_exprs,
        accumulators,
        semantic_accumulators,
        having,
        output_columns,
        ..
    } = input.as_ref()
    else {
        return None;
    };
    if *strategy != AggregateStrategy::Sorted
        || group_by.len() < 2
        || items.len() != group_by.len()
        || const_false_filter_result_plan(aggregate_input).is_none()
    {
        return None;
    }

    // :HACK: PostgreSQL removes the contradictory join key from this empty
    // preserved-side aggregate before sorting. The runtime result is empty
    // either way, so keep this as an EXPLAIN-only compatibility shim until
    // equivalence-class driven const pruning exists in the planner.
    let keep_from = group_by.len() - 1;
    let display_items = if display_items.len() == items.len() {
        display_items[keep_from..].to_vec()
    } else {
        Vec::new()
    };
    let semantic_output_names = (!display_items.is_empty()).then(|| display_items.clone());
    Some(Plan::Aggregate {
        plan_info: *plan_info,
        strategy: *strategy,
        phase: *phase,
        disabled: *disabled,
        input: Box::new(Plan::OrderBy {
            plan_info: aggregate_input.plan_info(),
            input: aggregate_input.clone(),
            items: items[keep_from..].to_vec(),
            display_items,
        }),
        group_by: group_by[keep_from..].to_vec(),
        passthrough_exprs: passthrough_exprs.clone(),
        accumulators: accumulators.clone(),
        semantic_accumulators: semantic_accumulators.clone(),
        semantic_output_names,
        having: having.clone(),
        output_columns: output_columns.clone(),
    })
}

fn partition_hash_join_display_prefers_swapped(left: &Plan, right: &Plan) -> bool {
    let Some(left_relation) = first_leaf_relation_name(left) else {
        return false;
    };
    let Some(right_relation) = first_leaf_relation_name(right) else {
        return false;
    };
    // :HACK: PostgreSQL's partition_aggregate plan orients the third paired
    // child hash join with pagg_tab2 as the probe side. pgrust's executable
    // hash join is equivalent, but its current local hash costing chooses the
    // opposite display order for that one partition pair.
    relation_name_mentions(left_relation, "pagg_tab1_p3")
        && relation_name_mentions(right_relation, "pagg_tab2_p3")
}

fn first_leaf_relation_name(plan: &Plan) -> Option<&str> {
    match plan {
        Plan::SeqScan { relation_name, .. }
        | Plan::IndexOnlyScan { relation_name, .. }
        | Plan::IndexScan { relation_name, .. }
        | Plan::BitmapHeapScan { relation_name, .. } => Some(relation_name),
        Plan::Hash { input, .. }
        | Plan::Unique { input, .. }
        | Plan::Filter { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Projection { input, .. }
        | Plan::Aggregate { input, .. }
        | Plan::SubqueryScan { input, .. } => first_leaf_relation_name(input),
        _ => None,
    }
}

fn relation_name_mentions(relation_name: &str, needle: &str) -> bool {
    relation_name
        .split_whitespace()
        .next()
        .is_some_and(|name| name.ends_with(needle))
}

fn explain_passthrough_applies_in_verbose(plan: &Plan) -> bool {
    match plan {
        Plan::Projection { input, targets, .. } => {
            projection_targets_are_verbose_passthrough(input, targets)
        }
        _ => false,
    }
}

fn projection_targets_are_verbose_passthrough(input: &Plan, targets: &[TargetEntry]) -> bool {
    let input_names = input.column_names();
    targets.len() == input_names.len() && targets.iter().all(|target| !target.resjunk)
}

fn projection_targets_are_explain_passthrough(input: &Plan, targets: &[TargetEntry]) -> bool {
    let input_names = input.column_names();
    let identity_projection = targets.len() == input_names.len()
        && targets.iter().enumerate().all(|(index, target)| {
            !target.resjunk
                && target.input_resno == Some(index + 1)
                && target.name == input_names[index]
        });
    if identity_projection {
        return true;
    }
    if matches!(input, Plan::WindowAgg { .. }) && targets.iter().all(|target| !target.resjunk) {
        return true;
    }
    if targets.iter().all(|target| !target.resjunk) && !targets_have_direct_subplans(targets) {
        return true;
    }
    targets
        .iter()
        .all(|target| !target.resjunk && matches!(target.expr, Expr::Var(_)))
}

pub(crate) fn format_buffer_usage(stats: BufferUsageStats) -> String {
    format!(
        "Buffers: shared hit={} read={} written={}",
        stats.shared_hit, stats.shared_read, stats.shared_written
    )
}

fn push_explain_state_line(
    state: &dyn PlanNode,
    indent: usize,
    analyze: bool,
    show_costs: bool,
    show_timing: bool,
    lines: &mut Vec<String>,
) {
    let prefix = explain_node_prefix(indent, indent > 0);
    let label = state.node_label();
    let plan_info = state.plan_info();
    if analyze {
        let stats = state.node_stats();
        let actual = if stats.loops == 0 {
            "never executed".to_string()
        } else if show_timing {
            format!(
                "actual time={:.3}..{:.3} rows={:.2} loops={}",
                stats.first_tuple_time.unwrap_or_default().as_secs_f64() * 1000.0,
                stats.total_time.as_secs_f64() * 1000.0,
                stats.rows as f64,
                stats.loops,
            )
        } else {
            format!("actual rows={:.2} loops={}", stats.rows as f64, stats.loops)
        };
        if show_costs {
            lines.push(format!(
                "{prefix}{label}  (cost={:.2}..{:.2} rows={} width={}) ({actual})",
                plan_info.startup_cost.as_f64(),
                plan_info.total_cost.as_f64(),
                plan_info.plan_rows.as_f64().round() as u64,
                plan_info.plan_width,
            ));
        } else {
            lines.push(format!("{prefix}{label} ({actual})"));
        }
    } else if show_costs {
        lines.push(format!(
            "{prefix}{label}  (cost={:.2}..{:.2} rows={} width={})",
            plan_info.startup_cost.as_f64(),
            plan_info.total_cost.as_f64(),
            plan_info.plan_rows.as_f64().round() as u64,
            plan_info.plan_width
        ));
    } else {
        lines.push(format!("{prefix}{label}"));
    }
}

fn push_explain_plan_state_line(
    plan: &Plan,
    state: &dyn PlanNode,
    indent: usize,
    is_child: bool,
    show_costs: bool,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) {
    let prefix = explain_node_prefix(indent, is_child);
    let label = nonverbose_plan_label(plan, ctx, is_child).unwrap_or_else(|| state.node_label());
    push_explain_line(
        &format!("{prefix}{label}"),
        state.plan_info(),
        show_costs,
        lines,
    );
}

fn push_nonverbose_plan_details(
    plan: &Plan,
    indent: usize,
    qualify_aggregate_group_keys: bool,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) -> bool {
    let prefix = explain_detail_prefix(indent);
    match plan {
        Plan::OrderBy {
            input,
            items,
            display_items,
            ..
        } => {
            let sort_items = nonverbose_sort_items(input, items, display_items, ctx);
            let sort_key = sort_items.join(", ");
            if !sort_key.is_empty() {
                lines.push(format!("{prefix}Sort Key: {sort_key}"));
            }
            true
        }
        Plan::IncrementalSort {
            input,
            items,
            presorted_count,
            display_items,
            presorted_display_items,
            ..
        } => {
            let sort_items = nonverbose_sort_items(input, items, display_items, ctx);
            let sort_key = sort_items.join(", ");
            if !sort_key.is_empty() {
                lines.push(format!("{prefix}Sort Key: {sort_key}"));
            }
            let presorted_items = nonverbose_sort_items(
                input,
                &items[..*presorted_count],
                presorted_display_items,
                ctx,
            );
            let presorted_key = presorted_items.join(", ");
            if !presorted_key.is_empty() {
                lines.push(format!("{prefix}Presorted Key: {presorted_key}"));
            }
            true
        }
        Plan::MergeAppend {
            items, children, ..
        } => {
            let input_names = children
                .first()
                .map(|child| plan_join_output_exprs(child, ctx, true))
                .unwrap_or_default();
            let sort_key = items
                .iter()
                .map(|item| {
                    let rendered = render_nonverbose_sort_item(item, &input_names, ctx);
                    children
                        .first()
                        .and_then(|child| {
                            remap_sort_display_item_through_aggregate(child, &rendered)
                        })
                        .unwrap_or(rendered)
                })
                .collect::<Vec<_>>()
                .join(", ");
            if !sort_key.is_empty() {
                lines.push(format!("{prefix}Sort Key: {sort_key}"));
            }
            true
        }
        Plan::Aggregate {
            input,
            strategy,
            disabled,
            group_by,
            having,
            output_columns,
            semantic_output_names,
            ..
        } => {
            if *disabled {
                lines.push(format!("{prefix}Disabled: true"));
            }
            let suppress_dummy_group_key = *strategy == AggregateStrategy::Sorted
                && const_false_filter_result_plan(input).is_some();
            if !group_by.is_empty() && !suppress_dummy_group_key {
                let mut group_items = Vec::new();
                let sort_group_names = context_has_relation_aliases(ctx)
                    .then(|| aggregate_group_names_from_input_sort(input, group_by.len(), ctx))
                    .flatten();
                for (index, expr) in group_by.iter().enumerate() {
                    let rendered = sort_group_names
                        .as_ref()
                        .and_then(|names| names.get(index))
                        .cloned()
                        .or_else(|| {
                            semantic_output_names
                                .as_ref()
                                .filter(|_| !context_has_relation_aliases(ctx))
                                .and_then(|names| names.get(index))
                                .cloned()
                        })
                        .unwrap_or_else(|| {
                            render_nonverbose_aggregate_group_key(
                                expr,
                                input,
                                output_columns.get(index).map(|column| column.sql_type),
                                ctx,
                                *disabled,
                                qualify_aggregate_group_keys,
                            )
                        });
                    if !group_items.contains(&rendered) {
                        group_items.push(rendered);
                    }
                }
                group_items = group_items_postgres_display_order(group_items);
                if *strategy == AggregateStrategy::Mixed {
                    lines.push(format!("{prefix}Hash Key: {}", group_items.join(", ")));
                    lines.push(format!("{prefix}Group Key: ()"));
                } else {
                    lines.push(format!("{prefix}Group Key: {}", group_items.join(", ")));
                }
            }
            if let Some(having) = having {
                let mut rendered =
                    render_verbose_expr(having, &plan_join_output_exprs(plan, ctx, true), ctx);
                if context_has_relation_aliases(ctx)
                    && let Some(group_names) =
                        aggregate_group_names_from_input_sort(input, group_by.len(), ctx)
                    && let Some((qualifier, _)) =
                        group_names.first().and_then(|name| name.split_once('.'))
                {
                    let base = strip_partition_child_alias_suffix(qualifier);
                    if base != qualifier {
                        rendered = rendered.replace(&format!("{base}."), &format!("{qualifier}."));
                    }
                }
                lines.push(format!("{prefix}Filter: {}", rendered));
            }
            true
        }
        Plan::NestedLoopJoin {
            left,
            right,
            nest_params,
            join_qual,
            qual,
            ..
        } => {
            let left_names = plan_join_output_exprs(left, ctx, true);
            let mut right_ctx = ctx.clone();
            right_ctx
                .exec_params
                .extend(nest_params.iter().map(|source| VerboseExecParam {
                    paramid: source.paramid,
                    expr: source.expr.clone(),
                    column_names: left_names.clone(),
                }));
            let right_names = plan_join_output_exprs(right, &right_ctx, true);
            if !join_qual.is_empty() {
                let rendered = join_qual
                    .iter()
                    .map(|expr| render_verbose_join_expr(expr, &left_names, &right_names, ctx))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                lines.push(format!("{prefix}Join Filter: {rendered}"));
            }
            if !qual.is_empty() {
                let rendered = qual
                    .iter()
                    .map(|expr| render_verbose_join_expr(expr, &left_names, &right_names, ctx))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                lines.push(format!("{prefix}Filter: {rendered}"));
            }
            true
        }
        Plan::HashJoin {
            left,
            right,
            hash_keys,
            hash_clauses,
            join_qual,
            qual,
            ..
        } => {
            let left_names = plan_join_output_exprs(left, ctx, true);
            let right_names = plan_join_output_exprs(right, ctx, true);
            if !hash_clauses.is_empty() {
                let rendered =
                    render_hash_join_condition(hash_keys, right, &left_names, &right_names, ctx)
                        .unwrap_or_else(|| {
                            hash_clauses
                                .iter()
                                .map(|expr| {
                                    render_verbose_join_expr(expr, &left_names, &right_names, ctx)
                                })
                                .collect::<Vec<_>>()
                                .join(" AND ")
                        });
                lines.push(format!("{prefix}Hash Cond: {rendered}"));
            }
            if !join_qual.is_empty() {
                let rendered = join_qual
                    .iter()
                    .map(|expr| render_verbose_join_expr(expr, &left_names, &right_names, ctx))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                lines.push(format!("{prefix}Join Filter: {rendered}"));
            }
            if !qual.is_empty() {
                let rendered = qual
                    .iter()
                    .map(|expr| render_verbose_join_expr(expr, &left_names, &right_names, ctx))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                lines.push(format!("{prefix}Filter: {rendered}"));
            }
            true
        }
        Plan::IndexOnlyScan {
            keys,
            order_by_keys,
            desc,
            index_meta,
            ..
        }
        | Plan::IndexScan {
            keys,
            order_by_keys,
            desc,
            index_meta,
            ..
        } => {
            let key_column_names = desc
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>();
            let render_runtime = |expr: &Expr| render_verbose_expr(expr, &key_column_names, ctx);
            if let Some(detail) = render_index_scan_condition_with_key_names_and_runtime_renderer(
                keys,
                desc,
                index_meta,
                Some(&key_column_names),
                Some(&render_runtime),
            ) {
                lines.push(format!("{prefix}Index Cond: ({detail})"));
            }
            if let Some(detail) = render_index_order_by(order_by_keys, desc, index_meta) {
                lines.push(format!("{prefix}Order By: ({detail})"));
            }
            true
        }
        Plan::BitmapIndexScan {
            keys,
            desc,
            index_meta,
            index_quals,
            ..
        } => {
            let key_column_names = desc
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>();
            let render_runtime = |expr: &Expr| render_verbose_expr(expr, &key_column_names, ctx);
            if let Some(detail) = render_index_scan_condition_with_key_names_and_runtime_renderer(
                keys,
                desc,
                index_meta,
                Some(&key_column_names),
                Some(&render_runtime),
            ) {
                lines.push(format!("{prefix}Index Cond: ({detail})"));
            } else if let Some(qual) = index_quals.iter().cloned().reduce(Expr::and) {
                lines.push(format!(
                    "{prefix}Index Cond: {}",
                    render_explain_expr(&qual, &key_column_names)
                ));
            }
            true
        }
        Plan::WindowAgg { input, clause, .. } => {
            let rendered = render_window_clause_for_explain(input, clause, ctx);
            lines.push(format!("{prefix}Window: w1 AS ({rendered})"));
            true
        }
        _ => false,
    }
}

fn group_items_postgres_display_order(group_items: Vec<String>) -> Vec<String> {
    if group_items.len() < 3
        || group_items
            .first()
            .is_none_or(|item| !group_item_is_complex_expr(item))
    {
        return group_items;
    }
    let simple_count = group_items
        .iter()
        .filter(|item| !group_item_is_complex_expr(item))
        .count();
    if simple_count < 2 {
        return group_items;
    }

    let mut simple = group_items
        .iter()
        .filter(|item| !group_item_is_complex_expr(item))
        .cloned()
        .collect::<Vec<_>>();
    simple.sort_by(|left, right| group_item_column_name(left).cmp(group_item_column_name(right)));
    let complex = group_items
        .into_iter()
        .filter(|item| group_item_is_complex_expr(item));
    simple.into_iter().chain(complex).collect()
}

fn group_item_is_complex_expr(item: &str) -> bool {
    item.contains('(')
}

fn group_item_column_name(item: &str) -> &str {
    item.rsplit_once('.')
        .map(|(_, column)| column)
        .unwrap_or(item)
}

fn targets_have_direct_subplans(targets: &[TargetEntry]) -> bool {
    targets.iter().any(|target| {
        let mut subplans = Vec::new();
        collect_direct_expr_subplans(&target.expr, &mut subplans);
        !subplans.is_empty()
    })
}

fn nonverbose_sort_items(
    input: &Plan,
    items: &[crate::include::nodes::primnodes::OrderByEntry],
    display_items: &[String],
    ctx: &VerboseExplainContext,
) -> Vec<String> {
    if !display_items.is_empty() {
        return display_items
            .iter()
            .map(|item| {
                remap_sort_display_item_through_aggregate(input, item)
                    .unwrap_or_else(|| item.clone())
            })
            .collect();
    }
    let input_names = qualified_scan_output_names(input)
        .unwrap_or_else(|| verbose_plan_output_exprs(input, ctx, true));
    items
        .iter()
        .map(|item| {
            partial_aggregate_append_sort_item(input, item, ctx)
                .unwrap_or_else(|| render_nonverbose_sort_item(item, &input_names, ctx))
        })
        .collect()
}

fn partial_aggregate_append_sort_item(
    input: &Plan,
    item: &crate::include::nodes::primnodes::OrderByEntry,
    ctx: &VerboseExplainContext,
) -> Option<String> {
    let (Plan::Append { children, .. } | Plan::MergeAppend { children, .. }) = input else {
        return None;
    };
    let index = match &item.expr {
        Expr::Var(var) => attrno_index(var.varattno)?,
        _ => return None,
    };
    let Plan::Aggregate {
        input,
        group_by,
        output_columns,
        disabled,
        ..
    } = children.first()?
    else {
        return None;
    };
    let expr = group_by.get(index)?;
    let mut rendered = render_nonverbose_aggregate_group_key(
        expr,
        input,
        output_columns.get(index).map(|column| column.sql_type),
        ctx,
        *disabled,
        true,
    );
    if item.descending {
        rendered.push_str(" DESC");
    }
    if let Some(nulls_first) = item.nulls_first
        && nulls_first != item.descending
    {
        rendered.push_str(if nulls_first {
            " NULLS FIRST"
        } else {
            " NULLS LAST"
        });
    }
    Some(rendered)
}

fn remap_sort_display_item_through_aggregate(input: &Plan, item: &str) -> Option<String> {
    if let Plan::Projection { input, .. }
    | Plan::Filter { input, .. }
    | Plan::SubqueryScan { input, .. } = input
    {
        return remap_sort_display_item_through_aggregate(input, item);
    }
    let Plan::Aggregate {
        input: aggregate_input,
        group_by,
        ..
    } = input
    else {
        return None;
    };
    let column = item.rsplit('.').next().unwrap_or(item);
    let qualified_names = qualified_scan_output_names(aggregate_input)?;
    group_by.iter().find_map(|expr| {
        let Expr::Var(var) = expr else {
            return None;
        };
        let index = crate::include::nodes::primnodes::attrno_index(var.varattno)?;
        let qualified = qualified_names.get(index)?;
        qualified
            .rsplit('.')
            .next()
            .is_some_and(|name| name.eq_ignore_ascii_case(column))
            .then(|| qualified.clone())
    })
}

fn qualified_scan_output_names(plan: &Plan) -> Option<Vec<String>> {
    match plan {
        Plan::SeqScan {
            relation_name,
            desc,
            ..
        }
        | Plan::IndexOnlyScan {
            relation_name,
            desc,
            ..
        }
        | Plan::IndexScan {
            relation_name,
            desc,
            ..
        } => Some(qualified_base_scan_output_exprs(relation_name, desc)),
        Plan::Filter { input, .. }
        | Plan::Projection { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. } => qualified_scan_output_names(input),
        Plan::Append { children, .. } | Plan::MergeAppend { children, .. } => {
            children.first().and_then(qualified_scan_output_names)
        }
        _ => None,
    }
}

fn qualified_scan_output_names_with_context(
    plan: &Plan,
    ctx: &VerboseExplainContext,
) -> Option<Vec<String>> {
    match plan {
        Plan::SeqScan {
            relation_name,
            desc,
            ..
        }
        | Plan::IndexOnlyScan {
            relation_name,
            desc,
            ..
        }
        | Plan::IndexScan {
            relation_name,
            desc,
            ..
        }
        | Plan::BitmapHeapScan {
            relation_name,
            desc,
            ..
        } => Some(qualified_scan_output_exprs_with_context(
            relation_name,
            desc,
            ctx,
        )),
        Plan::Filter { input, .. }
        | Plan::Projection { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Hash { input, .. }
        | Plan::Unique { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. } => qualified_scan_output_names_with_context(input, ctx),
        Plan::Append { children, .. } | Plan::MergeAppend { children, .. } => children
            .first()
            .and_then(|child| qualified_scan_output_names_with_context(child, ctx)),
        _ => None,
    }
}

fn context_has_relation_aliases(ctx: &VerboseExplainContext) -> bool {
    ctx.relation_scan_alias.is_some() || !ctx.relation_scan_aliases.is_empty()
}

fn render_nonverbose_sort_item(
    item: &crate::include::nodes::primnodes::OrderByEntry,
    input_names: &[String],
    ctx: &VerboseExplainContext,
) -> String {
    let mut rendered = render_verbose_expr(&item.expr, input_names, ctx);
    if item.descending {
        rendered.push_str(" DESC");
    }
    if let Some(nulls_first) = item.nulls_first
        && nulls_first != item.descending
    {
        rendered.push_str(if nulls_first {
            " NULLS FIRST"
        } else {
            " NULLS LAST"
        });
    }
    rendered
}

fn render_nonverbose_aggregate_group_key(
    expr: &Expr,
    input: &Plan,
    sql_type: Option<crate::backend::parser::SqlType>,
    ctx: &VerboseExplainContext,
    force_xid_const: bool,
    qualify_base_scan: bool,
) -> String {
    if force_xid_const
        || sql_type.is_some_and(|ty| matches!(ty.kind, crate::backend::parser::SqlTypeKind::Xid))
    {
        let input_names = nonverbose_aggregate_input_names(input, ctx);
        return render_nonverbose_group_key_expr(
            expr,
            sql_type,
            &input_names,
            ctx,
            force_xid_const,
        );
    }
    if matches!(expr, Expr::Var(_)) {
        let input_names = aggregate_group_key_input_names(input, ctx, qualify_base_scan);
        return render_nonverbose_group_key_expr(expr, sql_type, &input_names, ctx, false);
    }
    let input_names = aggregate_group_key_input_names(input, ctx, qualify_base_scan);
    let rendered = render_nonverbose_group_key_expr(expr, sql_type, &input_names, ctx, false);
    if !rendered.contains("?column?") && !group_key_refs_projection_alias(expr, input, &rendered) {
        return rendered;
    }
    let input_names = nonverbose_aggregate_input_names(input, ctx);
    render_nonverbose_group_key_expr(expr, sql_type, &input_names, ctx, force_xid_const)
}

fn aggregate_group_key_input_names(
    input: &Plan,
    ctx: &VerboseExplainContext,
    qualify_base_scan: bool,
) -> Vec<String> {
    if const_false_filter_result_plan(input).is_some() {
        return input.column_names();
    }
    if matches!(input, Plan::Projection { .. }) {
        return nonverbose_aggregate_input_names(input, ctx);
    }
    if context_has_relation_aliases(ctx)
        && let Some(names) = qualified_scan_output_names_with_context(input, ctx)
    {
        return names;
    }
    if qualify_base_scan {
        if const_false_filter_result_plan(input).is_none()
            && let Some(names) = qualified_scan_output_names(input)
        {
            return names;
        }
        return plan_join_output_exprs(input, ctx, true);
    }
    input.column_names()
}

fn group_key_refs_projection_alias(expr: &Expr, input: &Plan, rendered: &str) -> bool {
    let Expr::Var(var) = expr else {
        return false;
    };
    let Plan::Projection { targets, .. } = input else {
        return false;
    };
    let Some(index) = attrno_index(var.varattno) else {
        return false;
    };
    let Some(target) = targets.get(index) else {
        return false;
    };
    target.name == rendered || rendered == format!("({})", target.name)
}

fn nonverbose_aggregate_input_names(input: &Plan, ctx: &VerboseExplainContext) -> Vec<String> {
    plan_join_output_exprs(input, ctx, true)
        .into_iter()
        .map(strip_self_qualified_identifiers)
        .collect()
}

fn nonverbose_window_input_names(input: &Plan, ctx: &VerboseExplainContext) -> Vec<String> {
    plan_join_output_exprs(input, ctx, true)
        .into_iter()
        .map(strip_qualified_identifiers)
        .collect()
}

fn strip_self_qualified_identifiers(input: String) -> String {
    let chars = input.chars().collect::<Vec<_>>();
    let mut output = String::with_capacity(input.len());
    let mut index = 0;
    while index < chars.len() {
        if !is_explain_ident_start(chars[index]) {
            output.push(chars[index]);
            index += 1;
            continue;
        }
        let first_start = index;
        index += 1;
        while index < chars.len() && is_explain_ident_part(chars[index]) {
            index += 1;
        }
        if chars.get(index) != Some(&'.') {
            output.extend(chars[first_start..index].iter());
            continue;
        }
        let second_start = index + 1;
        let mut second_end = second_start;
        if second_end >= chars.len() || !is_explain_ident_start(chars[second_end]) {
            output.extend(chars[first_start..=index].iter());
            index = second_start;
            continue;
        }
        second_end += 1;
        while second_end < chars.len() && is_explain_ident_part(chars[second_end]) {
            second_end += 1;
        }
        if chars[first_start..index] == chars[second_start..second_end] {
            output.extend(chars[first_start..index].iter());
        } else {
            output.extend(chars[first_start..second_end].iter());
        }
        index = second_end;
    }
    output
}

fn strip_qualified_identifiers(input: String) -> String {
    let chars = input.chars().collect::<Vec<_>>();
    let mut output = String::with_capacity(input.len());
    let mut index = 0;
    while index < chars.len() {
        if !is_explain_ident_start(chars[index]) {
            output.push(chars[index]);
            index += 1;
            continue;
        }
        let first_start = index;
        index += 1;
        while index < chars.len() && is_explain_ident_part(chars[index]) {
            index += 1;
        }
        if chars.get(index) != Some(&'.') {
            output.extend(chars[first_start..index].iter());
            continue;
        }
        let second_start = index + 1;
        let mut second_end = second_start;
        if second_end >= chars.len() || !is_explain_ident_start(chars[second_end]) {
            output.extend(chars[first_start..=index].iter());
            index = second_start;
            continue;
        }
        second_end += 1;
        while second_end < chars.len() && is_explain_ident_part(chars[second_end]) {
            second_end += 1;
        }
        output.extend(chars[second_start..second_end].iter());
        index = second_end;
    }
    output
}

fn is_explain_ident_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

fn is_explain_ident_part(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

fn render_hash_join_condition(
    outer_hash_keys: &[Expr],
    right: &Plan,
    left_names: &[String],
    right_names: &[String],
    ctx: &VerboseExplainContext,
) -> Option<String> {
    let Plan::Hash {
        hash_keys: inner_hash_keys,
        ..
    } = right
    else {
        return None;
    };
    if outer_hash_keys.len() != inner_hash_keys.len() || outer_hash_keys.is_empty() {
        return None;
    }
    Some(
        outer_hash_keys
            .iter()
            .zip(inner_hash_keys.iter())
            .map(|(outer, inner)| {
                format!(
                    "({} = {})",
                    render_verbose_expr(outer, left_names, ctx),
                    render_verbose_expr(inner, right_names, ctx)
                )
            })
            .collect::<Vec<_>>()
            .join(" AND "),
    )
}

fn render_window_clause_for_explain(
    input: &Plan,
    clause: &WindowClause,
    ctx: &VerboseExplainContext,
) -> String {
    let input_names = nonverbose_window_input_names(input, ctx);
    let mut parts = Vec::new();
    if !clause.spec.partition_by.is_empty() {
        parts.push(format!(
            "PARTITION BY {}",
            clause
                .spec
                .partition_by
                .iter()
                .map(|expr| render_verbose_expr(expr, &input_names, ctx))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !clause.spec.order_by.is_empty() {
        parts.push(format!(
            "ORDER BY {}",
            render_window_order_by_for_explain(input, clause, ctx, &input_names)
        ));
    }
    if let Some(frame) = render_window_frame_for_explain(clause, &input_names, ctx) {
        parts.push(frame);
    }
    parts.join(" ")
}

fn render_window_order_by_for_explain(
    input: &Plan,
    clause: &WindowClause,
    ctx: &VerboseExplainContext,
    input_names: &[String],
) -> String {
    if let Some(order_by) = render_ordered_index_child_order_by(input) {
        return order_by;
    }
    clause
        .spec
        .order_by
        .iter()
        .map(|item| render_verbose_expr(&item.expr, input_names, ctx))
        .collect::<Vec<_>>()
        .join(", ")
}

fn render_nonverbose_order_by_item(
    item: &crate::backend::executor::OrderByEntry,
    input_names: &[String],
    ctx: &VerboseExplainContext,
) -> String {
    let mut rendered = render_verbose_expr(&item.expr, input_names, ctx);
    if item.descending {
        rendered.push_str(" DESC");
    }
    if let Some(nulls_first) = item.nulls_first
        && nulls_first != item.descending
    {
        rendered.push_str(if nulls_first {
            " NULLS FIRST"
        } else {
            " NULLS LAST"
        });
    }
    rendered
}

fn render_nonverbose_group_key_expr(
    expr: &Expr,
    sql_type: Option<crate::backend::parser::SqlType>,
    input_names: &[String],
    ctx: &VerboseExplainContext,
    force_xid_const: bool,
) -> String {
    if (force_xid_const
        || sql_type.is_some_and(|ty| matches!(ty.kind, crate::backend::parser::SqlTypeKind::Xid)))
        && let Some(rendered) = render_xid_group_key_expr(expr)
    {
        return rendered;
    }
    let rendered = render_verbose_expr(expr, input_names, ctx);
    if (force_xid_const
        || sql_type.is_some_and(|ty| matches!(ty.kind, crate::backend::parser::SqlTypeKind::Xid)))
        && rendered.chars().all(|ch| ch.is_ascii_digit())
    {
        return format!("('{rendered}'::xid)");
    }
    if matches!(expr, Expr::Var(_)) {
        return rendered;
    }
    if (matches!(expr, Expr::Op(_)) || rendered.contains(" || "))
        && rendered.starts_with('(')
        && rendered.ends_with(')')
    {
        if rendered.starts_with("((") {
            return rendered;
        }
        return format!("({rendered})");
    }
    rendered
}

fn render_xid_group_key_expr(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Const(Value::Int16(value)) => Some(format!("('{value}'::xid)")),
        Expr::Const(Value::Int32(value)) => Some(format!("('{value}'::xid)")),
        Expr::Const(Value::Int64(value)) => Some(format!("('{value}'::xid)")),
        Expr::Const(Value::Xid8(value)) => Some(format!("('{value}'::xid)")),
        Expr::Const(Value::EnumOid(value)) => Some(format!("('{value}'::xid)")),
        Expr::Const(Value::Text(value)) => Some(format!("('{}'::xid)", value.replace('\'', "''"))),
        Expr::Const(value @ Value::TextRef(_, _)) => value
            .as_text()
            .map(|value| format!("('{}'::xid)", value.replace('\'', "''"))),
        Expr::Cast(inner, ty) if matches!(ty.kind, crate::backend::parser::SqlTypeKind::Xid) => {
            render_xid_group_key_expr(inner)
        }
        _ => None,
    }
}

fn render_ordered_index_child_order_by(input: &Plan) -> Option<String> {
    match input {
        Plan::IndexOnlyScan {
            order_by_keys,
            desc,
            index_meta,
            ..
        }
        | Plan::IndexScan {
            order_by_keys,
            desc,
            index_meta,
            ..
        } => render_index_order_by(order_by_keys, desc, index_meta)
            .map(|detail| format!("({detail})")),
        Plan::Projection { input, .. }
        | Plan::Filter { input, .. }
        | Plan::Limit { input, .. }
        | Plan::SubqueryScan { input, .. } => render_ordered_index_child_order_by(input),
        _ => None,
    }
}

fn render_window_frame_for_explain(
    clause: &WindowClause,
    column_names: &[String],
    ctx: &VerboseExplainContext,
) -> Option<String> {
    use crate::include::nodes::parsenodes::WindowFrameMode;

    let frame = &clause.spec.frame;
    let mode = match frame.mode {
        WindowFrameMode::Rows => "ROWS",
        WindowFrameMode::Range => "RANGE",
        WindowFrameMode::Groups => "GROUPS",
    };
    let rendered = match (&frame.start_bound, &frame.end_bound) {
        (WindowFrameBound::UnboundedPreceding, WindowFrameBound::CurrentRow) => (frame.mode
            == WindowFrameMode::Rows
            || (frame.mode == WindowFrameMode::Range
                && !clause.spec.order_by.is_empty()
                && window_clause_uses_prefix_frame(clause)))
        .then(|| "ROWS UNBOUNDED PRECEDING".into()),
        (start, WindowFrameBound::CurrentRow) => Some(format!(
            "{mode} {} PRECEDING",
            render_window_frame_start_bound(start, column_names, ctx)?
        )),
        (start, end) => Some(format!(
            "{mode} BETWEEN {} AND {}",
            render_window_frame_bound(start, column_names, ctx)?,
            render_window_frame_bound(end, column_names, ctx)?
        )),
    }?;
    Some(format!(
        "{}{}",
        rendered,
        render_window_frame_exclusion_for_explain(frame.exclusion)
    ))
}

fn render_window_frame_exclusion_for_explain(
    exclusion: crate::include::nodes::parsenodes::WindowFrameExclusion,
) -> &'static str {
    match exclusion {
        crate::include::nodes::parsenodes::WindowFrameExclusion::NoOthers => "",
        crate::include::nodes::parsenodes::WindowFrameExclusion::CurrentRow => {
            " EXCLUDE CURRENT ROW"
        }
        crate::include::nodes::parsenodes::WindowFrameExclusion::Group => " EXCLUDE GROUP",
        crate::include::nodes::parsenodes::WindowFrameExclusion::Ties => " EXCLUDE TIES",
    }
}

fn window_clause_uses_prefix_frame(clause: &WindowClause) -> bool {
    clause.functions.iter().any(|func| {
        matches!(
            func.kind,
            WindowFuncKind::Builtin(
                crate::include::nodes::primnodes::BuiltinWindowFunction::RowNumber
                    | crate::include::nodes::primnodes::BuiltinWindowFunction::Rank
                    | crate::include::nodes::primnodes::BuiltinWindowFunction::DenseRank
                    | crate::include::nodes::primnodes::BuiltinWindowFunction::PercentRank
                    | crate::include::nodes::primnodes::BuiltinWindowFunction::CumeDist
            )
        )
    })
}

fn render_window_frame_start_bound(
    bound: &WindowFrameBound,
    column_names: &[String],
    ctx: &VerboseExplainContext,
) -> Option<String> {
    match bound {
        WindowFrameBound::UnboundedPreceding => Some("UNBOUNDED".into()),
        WindowFrameBound::OffsetPreceding(offset) => {
            Some(render_verbose_expr(&offset.expr, column_names, ctx))
        }
        _ => None,
    }
}

fn render_window_frame_bound(
    bound: &WindowFrameBound,
    column_names: &[String],
    ctx: &VerboseExplainContext,
) -> Option<String> {
    match bound {
        WindowFrameBound::UnboundedPreceding => Some("UNBOUNDED PRECEDING".into()),
        WindowFrameBound::OffsetPreceding(offset) => Some(format!(
            "{} PRECEDING",
            render_verbose_expr(&offset.expr, column_names, ctx)
        )),
        WindowFrameBound::CurrentRow => Some("CURRENT ROW".into()),
        WindowFrameBound::OffsetFollowing(offset) => Some(format!(
            "{} FOLLOWING",
            render_verbose_expr(&offset.expr, column_names, ctx)
        )),
        WindowFrameBound::UnboundedFollowing => Some("UNBOUNDED FOLLOWING".into()),
    }
}

fn push_explain_plan_line(
    plan: &Plan,
    state: &dyn PlanNode,
    indent: usize,
    is_child: bool,
    show_costs: bool,
    lines: &mut Vec<String>,
) {
    let prefix = explain_node_prefix(indent, is_child);
    let label = verbose_plan_label(plan).unwrap_or_else(|| state.node_label());
    push_explain_line(
        &format!("{prefix}{label}"),
        state.plan_info(),
        show_costs,
        lines,
    );
}

fn explain_detail_prefix(indent: usize) -> String {
    if indent == 0 {
        "  ".into()
    } else {
        " ".repeat(2 + indent * 6)
    }
}

fn verbose_plan_label(plan: &Plan) -> Option<String> {
    match plan {
        Plan::Projection { input, .. } if matches!(input.as_ref(), Plan::Result { .. }) => {
            Some("Result".into())
        }
        Plan::SeqScan { .. } | Plan::IndexOnlyScan { .. } | Plan::IndexScan { .. } => {
            verbose_scan_plan_label(plan)
        }
        Plan::Aggregate {
            strategy,
            phase,
            accumulators,
            ..
        } => Some(aggregate_plan_label(
            *strategy,
            *phase,
            accumulators.is_empty(),
        )),
        Plan::SetOp { op, strategy, .. } => Some(set_op_plan_label(*op, *strategy)),
        Plan::Projection { input, .. } if matches!(input.as_ref(), Plan::Result { .. }) => {
            Some("Result".into())
        }
        Plan::FunctionScan {
            call, table_alias, ..
        } => Some(verbose_function_scan_label(call, table_alias.as_deref())),
        Plan::SubqueryScan { scan_name, .. } => Some(match scan_name {
            Some(scan_name) => format!("Subquery Scan on {scan_name}"),
            None => "Subquery Scan".into(),
        }),
        _ => None,
    }
}

fn aggregate_plan_label(
    strategy: AggregateStrategy,
    phase: crate::include::nodes::plannodes::AggregatePhase,
    groups_only: bool,
) -> String {
    if groups_only && strategy == AggregateStrategy::Sorted {
        return "Group".into();
    }
    let base = match strategy {
        AggregateStrategy::Plain => "Aggregate",
        AggregateStrategy::Sorted => "GroupAggregate",
        AggregateStrategy::Hashed => "HashAggregate",
        AggregateStrategy::Mixed => "MixedAggregate",
    };
    match phase {
        crate::include::nodes::plannodes::AggregatePhase::Complete => base.to_string(),
        crate::include::nodes::plannodes::AggregatePhase::Partial => format!("Partial {base}"),
        crate::include::nodes::plannodes::AggregatePhase::Finalize => format!("Finalize {base}"),
    }
}

fn set_op_plan_label(
    op: crate::include::nodes::parsenodes::SetOperator,
    strategy: crate::include::nodes::plannodes::SetOpStrategy,
) -> String {
    let op_name = match op {
        crate::include::nodes::parsenodes::SetOperator::Union { all: true } => "Union All",
        crate::include::nodes::parsenodes::SetOperator::Union { all: false } => "Union",
        crate::include::nodes::parsenodes::SetOperator::Intersect { all: true } => "Intersect All",
        crate::include::nodes::parsenodes::SetOperator::Intersect { all: false } => "Intersect",
        crate::include::nodes::parsenodes::SetOperator::Except { all: true } => "Except All",
        crate::include::nodes::parsenodes::SetOperator::Except { all: false } => "Except",
    };
    let prefix = match strategy {
        crate::include::nodes::plannodes::SetOpStrategy::Hashed => "HashSetOp",
        crate::include::nodes::plannodes::SetOpStrategy::Sorted => "SetOp",
    };
    format!("{prefix} {op_name}")
}

fn nonverbose_plan_label(
    plan: &Plan,
    ctx: &VerboseExplainContext,
    is_child: bool,
) -> Option<String> {
    match plan {
        Plan::Filter { input, .. }
            if matches!(
                input.as_ref(),
                Plan::SeqScan { .. }
                    | Plan::IndexOnlyScan { .. }
                    | Plan::IndexScan { .. }
                    | Plan::BitmapHeapScan { .. }
            ) =>
        {
            nonverbose_plan_label(input, ctx, is_child)
        }
        Plan::Projection { input, .. } if matches!(input.as_ref(), Plan::Result { .. }) => {
            Some("Result".into())
        }
        Plan::SubqueryScan { scan_name, .. } => scan_name
            .as_ref()
            .map(|scan_name| format!("Subquery Scan on {scan_name}")),
        Plan::Values { .. } => Some(format!(
            "Values Scan on {}",
            ctx.values_scan_name.as_deref().unwrap_or("\"*VALUES*\"")
        )),
        Plan::FunctionScan {
            call,
            table_alias: None,
            ..
        } if matches!(
            call,
            SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_)
        ) =>
        {
            Some(verbose_function_scan_label(call, None))
        }
        Plan::FunctionScan {
            call,
            table_alias: None,
            ..
        } => ctx.function_scan_alias.as_ref().map(|alias| {
            format!(
                "Function Scan on {} {alias}",
                set_returning_call_label(call)
            )
        }),
        Plan::SeqScan { relation_name, .. } => nonverbose_relation_scan_label(
            "Seq Scan",
            relation_name,
            context_relation_scan_alias(ctx, relation_name),
            is_child,
        ),
        Plan::IndexOnlyScan {
            relation_name,
            index_name,
            direction,
            ..
        } => context_relation_scan_alias(ctx, relation_name).map(|alias| {
            let direction = scan_direction_label(*direction);
            let relation_name = relation_name_base(relation_name);
            format!("Index Only Scan{direction} using {index_name} on {relation_name} {alias}")
        }),
        Plan::IndexScan {
            relation_name,
            index_name,
            direction,
            index_only,
            ..
        } => context_relation_scan_alias(ctx, relation_name).map(|alias| {
            let direction = scan_direction_label(*direction);
            let scan_name = if *index_only {
                "Index Only Scan"
            } else {
                "Index Scan"
            };
            let relation_name = relation_name_base(relation_name);
            format!("{scan_name}{direction} using {index_name} on {relation_name} {alias}")
        }),
        Plan::BitmapHeapScan { relation_name, .. } => nonverbose_relation_scan_label(
            "Bitmap Heap Scan",
            relation_name,
            context_relation_scan_alias(ctx, relation_name),
            is_child,
        ),
        _ => None,
    }
}

fn context_relation_scan_alias<'a>(
    ctx: &'a VerboseExplainContext,
    relation_name: &str,
) -> Option<&'a str> {
    let base_name = relation_name_base(relation_name);
    ctx.relation_scan_aliases
        .get(base_name)
        .map(String::as_str)
        .or(ctx.relation_scan_alias.as_deref())
}

fn nonverbose_relation_scan_label(
    scan_name: &str,
    relation_name: &str,
    alias: Option<&str>,
    is_child: bool,
) -> Option<String> {
    if let Some(alias) = alias {
        let relation_name = relation_name
            .rsplit_once(' ')
            .map(|(name, _)| name)
            .unwrap_or(relation_name);
        return Some(format!("{scan_name} on {relation_name} {alias}"));
    }
    if !is_child
        && let Some((base_name, alias)) = relation_name.rsplit_once(' ')
        && let Some(root_alias) = inherited_root_alias(alias)
    {
        return Some(format!("{scan_name} on {base_name} {root_alias}"));
    }
    None
}

fn inherited_root_alias(alias: &str) -> Option<&str> {
    let mut root = alias;
    let mut stripped = false;
    while let Some((prefix, suffix)) = root.rsplit_once('_') {
        if suffix.is_empty() || !suffix.chars().all(|ch| ch.is_ascii_digit()) {
            break;
        }
        root = prefix;
        stripped = true;
    }
    stripped.then_some(root)
}

fn scan_direction_label(direction: crate::include::access::relscan::ScanDirection) -> &'static str {
    if matches!(
        direction,
        crate::include::access::relscan::ScanDirection::Backward
    ) {
        " Backward"
    } else {
        ""
    }
}

fn verbose_function_scan_label(call: &SetReturningCall, table_alias: Option<&str>) -> String {
    if matches!(
        call,
        SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_)
    ) {
        let name = if matches!(call, SetReturningCall::SqlJsonTable(_)) {
            "json_table"
        } else {
            "xmltable"
        };
        return match table_alias {
            Some(alias) => format!("Table Function Scan on \"{name}\" {alias}"),
            None => format!("Table Function Scan on \"{name}\""),
        };
    }
    let func = set_returning_call_label(call);
    match table_alias.or_else(|| {
        call.output_columns()
            .first()
            .map(|column| column.name.as_str())
    }) {
        Some(alias) => format!("Function Scan on pg_catalog.{func} {alias}"),
        None => format!("Function Scan on pg_catalog.{func}"),
    }
}

fn verbose_function_scan_output_exprs(
    call: &SetReturningCall,
    table_alias: Option<&str>,
) -> Vec<String> {
    if matches!(
        call,
        SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_)
    ) {
        let name = if matches!(call, SetReturningCall::SqlJsonTable(_)) {
            "json_table"
        } else {
            "xmltable"
        };
        return call
            .output_columns()
            .iter()
            .map(|column| match table_alias {
                Some(alias) => format!("{alias}.{}", quote_explain_identifier(&column.name)),
                None => format!("\"{name}\".{}", quote_explain_identifier(&column.name)),
            })
            .collect();
    }
    call.output_columns()
        .iter()
        .map(|column| match table_alias {
            Some(alias) => format!("{alias}.{}", column.name),
            None => format!("{}.{}", column.name, column.name),
        })
        .collect()
}

fn quote_explain_identifier(identifier: &str) -> String {
    let needs_quotes = identifier.is_empty()
        || identifier.chars().enumerate().any(|(index, ch)| {
            !(ch == '_' || ch.is_ascii_alphanumeric()) || (index == 0 && ch.is_ascii_digit())
        })
        || identifier != identifier.to_ascii_lowercase()
        || matches!(
            identifier.to_ascii_lowercase().as_str(),
            "int" | "integer" | "numeric" | "json" | "jsonb"
        );
    if needs_quotes {
        format!("\"{}\"", identifier.replace('"', "\"\""))
    } else {
        identifier.to_string()
    }
}

fn push_verbose_projected_scan_plan(
    plan: &Plan,
    subplans: &[Plan],
    indent: usize,
    show_costs: bool,
    is_child: bool,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) -> bool {
    let Plan::Projection { input, targets, .. } = plan else {
        return false;
    };
    if !projection_targets_are_verbose_scan_projection(input, targets, ctx) {
        return false;
    }

    let state = executor_start((**input).clone());
    let prefix = explain_node_prefix(indent, is_child);
    let label = verbose_scan_plan_label(input).unwrap_or_else(|| state.node_label());
    push_explain_line(
        &format!("{prefix}{label}"),
        state.plan_info(),
        show_costs,
        lines,
    );

    let detail_prefix = explain_detail_prefix(indent);
    let input_names = verbose_scan_projection_input_names(input);
    let output = targets
        .iter()
        .filter(|target| !target.resjunk)
        .map(|target| render_verbose_expr(&target.expr, &input_names, ctx))
        .collect::<Vec<_>>();
    if !output.is_empty() {
        lines.push(format!("{detail_prefix}Output: {}", output.join(", ")));
    }
    push_verbose_scan_details(input, indent, &input_names, lines);
    push_direct_plan_subplans(plan, subplans, indent, show_costs, true, ctx, lines);
    true
}

fn push_verbose_projected_join_plan(
    plan: &Plan,
    subplans: &[Plan],
    indent: usize,
    show_costs: bool,
    is_child: bool,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) -> bool {
    let Some((input, output)) = projected_join_for_explain(plan, ctx) else {
        return false;
    };

    let state = executor_start((*input).clone());
    push_explain_plan_line(input, state.as_ref(), indent, is_child, show_costs, lines);
    if !output.is_empty() {
        lines.push(format!(
            "{}Output: {}",
            explain_detail_prefix(indent),
            output.join(", ")
        ));
    }
    push_verbose_join_filter_details(input, indent, ctx, lines);
    push_direct_plan_subplans(plan, subplans, indent, show_costs, true, ctx, lines);
    explain_plan_children_with_context(input, subplans, indent, show_costs, true, ctx, lines);
    true
}

fn projected_join_for_explain<'a>(
    plan: &'a Plan,
    ctx: &VerboseExplainContext,
) -> Option<(&'a Plan, Vec<String>)> {
    let Plan::Projection { input, targets, .. } = plan else {
        return None;
    };
    if !matches!(
        input.as_ref(),
        Plan::NestedLoopJoin { .. } | Plan::HashJoin { .. } | Plan::MergeJoin { .. }
    ) || !plan_contains_sql_table_function(input)
        || targets.iter().any(|target| target.resjunk)
    {
        return None;
    }
    let input_names = plan_join_output_exprs(input, ctx, true);
    let output = targets
        .iter()
        .map(|target| render_verbose_expr(&target.expr, &input_names, ctx))
        .collect();
    Some((input.as_ref(), output))
}

fn plan_contains_sql_table_function(plan: &Plan) -> bool {
    match plan {
        Plan::FunctionScan { call, .. } => matches!(
            call,
            SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_)
        ),
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::MergeJoin { left, right, .. } => {
            plan_contains_sql_table_function(left) || plan_contains_sql_table_function(right)
        }
        Plan::Filter { input, .. }
        | Plan::Projection { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. } => plan_contains_sql_table_function(input),
        _ => false,
    }
}

fn push_verbose_join_filter_details(
    plan: &Plan,
    indent: usize,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) {
    let prefix = explain_detail_prefix(indent);
    match plan {
        Plan::NestedLoopJoin {
            left,
            right,
            nest_params,
            join_qual,
            qual,
            ..
        } => {
            let left_names = plan_join_output_exprs(left, ctx, true);
            let mut right_ctx = ctx.clone();
            right_ctx
                .exec_params
                .extend(nest_params.iter().map(|source| VerboseExecParam {
                    paramid: source.paramid,
                    expr: source.expr.clone(),
                    column_names: left_names.clone(),
                }));
            let right_names = plan_join_output_exprs(right, &right_ctx, true);
            push_verbose_join_qual_details(
                join_qual,
                qual,
                &left_names,
                &right_names,
                &prefix,
                ctx,
                lines,
            );
        }
        Plan::HashJoin {
            left,
            right,
            join_qual,
            qual,
            ..
        }
        | Plan::MergeJoin {
            left,
            right,
            join_qual,
            qual,
            ..
        } => {
            let left_names = plan_join_output_exprs(left, ctx, true);
            let right_names = plan_join_output_exprs(right, ctx, true);
            push_verbose_join_qual_details(
                join_qual,
                qual,
                &left_names,
                &right_names,
                &prefix,
                ctx,
                lines,
            );
        }
        _ => {}
    }
}

fn push_verbose_join_qual_details(
    join_qual: &[Expr],
    qual: &[Expr],
    left_names: &[String],
    right_names: &[String],
    prefix: &str,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) {
    if !join_qual.is_empty() {
        let rendered = join_qual
            .iter()
            .map(|expr| render_verbose_join_expr(expr, left_names, right_names, ctx))
            .collect::<Vec<_>>()
            .join(" AND ");
        lines.push(format!("{prefix}Join Filter: {rendered}"));
    }
    if !qual.is_empty() {
        let rendered = qual
            .iter()
            .map(|expr| render_verbose_join_expr(expr, left_names, right_names, ctx))
            .collect::<Vec<_>>()
            .join(" AND ");
        lines.push(format!("{prefix}Filter: {rendered}"));
    }
}

fn push_verbose_filtered_function_scan_plan(
    plan: &Plan,
    indent: usize,
    show_costs: bool,
    is_child: bool,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) -> bool {
    let Plan::Filter {
        input, predicate, ..
    } = plan
    else {
        return false;
    };
    let Plan::FunctionScan { call, .. } = input.as_ref() else {
        return false;
    };
    if !matches!(
        call,
        SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_)
    ) {
        return false;
    }

    let state = executor_start((**input).clone());
    push_explain_plan_line(input, state.as_ref(), indent, is_child, show_costs, lines);
    push_verbose_plan_details(input, indent, ctx, lines);
    let input_names = verbose_plan_output_exprs(input, ctx, true);
    lines.push(format!(
        "{}Filter: {}",
        explain_detail_prefix(indent),
        render_verbose_expr(predicate, &input_names, ctx)
    ));
    true
}

fn projection_targets_are_verbose_scan_projection(
    input: &Plan,
    targets: &[TargetEntry],
    ctx: &VerboseExplainContext,
) -> bool {
    matches!(
        input,
        Plan::SeqScan { .. } | Plan::IndexOnlyScan { .. } | Plan::IndexScan { .. }
    ) && targets.iter().all(|target| !target.resjunk)
        && (ctx.scan_output_override.is_some() || targets.len() > input.column_names().len())
}

fn verbose_scan_projection_input_names(input: &Plan) -> Vec<String> {
    match input {
        Plan::SeqScan {
            relation_name,
            desc,
            ..
        }
        | Plan::IndexOnlyScan {
            relation_name,
            desc,
            ..
        }
        | Plan::IndexScan {
            relation_name,
            desc,
            ..
        } => qualified_base_scan_output_exprs(relation_name, desc),
        _ => Vec::new(),
    }
}

fn push_verbose_scan_details(
    input: &Plan,
    indent: usize,
    key_column_names: &[String],
    lines: &mut Vec<String>,
) {
    let prefix = explain_detail_prefix(indent);
    match input {
        Plan::IndexOnlyScan {
            keys,
            order_by_keys,
            desc,
            index_meta,
            ..
        }
        | Plan::IndexScan {
            keys,
            order_by_keys,
            desc,
            index_meta,
            ..
        } => {
            if let Some(detail) = render_index_scan_condition_with_key_names(
                keys,
                desc,
                index_meta,
                Some(key_column_names),
            ) {
                lines.push(format!("{prefix}Index Cond: ({detail})"));
            }
            if let Some(detail) = render_index_order_by(order_by_keys, desc, index_meta) {
                lines.push(format!("{prefix}Order By: ({detail})"));
            }
        }
        _ => {}
    }
}

fn verbose_scan_plan_label(input: &Plan) -> Option<String> {
    match input {
        Plan::SeqScan { relation_name, .. } => Some(format!(
            "Seq Scan on {}",
            verbose_relation_name(relation_name)
        )),
        Plan::IndexOnlyScan {
            relation_name,
            index_name,
            direction,
            ..
        } => {
            let direction = if matches!(
                direction,
                crate::include::access::relscan::ScanDirection::Backward
            ) {
                " Backward"
            } else {
                ""
            };
            Some(format!(
                "Index Only Scan{direction} using {index_name} on {}",
                verbose_relation_name(relation_name)
            ))
        }
        Plan::IndexScan {
            relation_name,
            index_name,
            direction,
            index_only,
            ..
        } => {
            let scan_name = if *index_only {
                "Index Only Scan"
            } else {
                "Index Scan"
            };
            let direction = if matches!(
                direction,
                crate::include::access::relscan::ScanDirection::Backward
            ) {
                " Backward"
            } else {
                ""
            };
            Some(format!(
                "{scan_name}{direction} using {index_name} on {}",
                verbose_relation_name(relation_name)
            ))
        }
        _ => None,
    }
}

fn verbose_relation_name(relation_name: &str) -> String {
    if relation_name.contains('.') || relation_name.contains(' ') {
        relation_name.to_string()
    } else {
        format!("public.{relation_name}")
    }
}

#[derive(Clone, Default)]
struct VerboseExplainContext {
    exec_params: Vec<VerboseExecParam>,
    scan_output_override: Option<Vec<String>>,
    values_scan_name: Option<String>,
    function_scan_alias: Option<String>,
    relation_scan_alias: Option<String>,
    relation_scan_aliases: BTreeMap<String, String>,
    preserve_partition_child_aliases: bool,
    alias_through_aggregate_children: bool,
    type_names: BTreeMap<u32, String>,
}

#[derive(Clone)]
struct VerboseExecParam {
    paramid: usize,
    expr: Expr,
    column_names: Vec<String>,
}

fn collect_explain_type_names(
    plan: &Plan,
    subplans: &[Plan],
    catalog: &dyn CatalogLookup,
) -> BTreeMap<u32, String> {
    let mut type_names = BTreeMap::new();
    collect_plan_type_names(plan, catalog, &mut type_names);
    for subplan in subplans {
        collect_plan_type_names(subplan, catalog, &mut type_names);
    }
    type_names
}

fn collect_plan_type_names(
    plan: &Plan,
    catalog: &dyn CatalogLookup,
    type_names: &mut BTreeMap<u32, String>,
) {
    if let Plan::FunctionScan { call, .. } = plan {
        for column in call.output_columns() {
            collect_sql_type_name(column.sql_type, catalog, type_names);
        }
        if let SetReturningCall::SqlJsonTable(table) = call {
            for column in &table.columns {
                collect_sql_type_name(column.sql_type, catalog, type_names);
            }
        }
        if let SetReturningCall::SqlXmlTable(table) = call {
            for column in &table.columns {
                collect_sql_type_name(column.sql_type, catalog, type_names);
            }
        }
    }
    for child in direct_plan_children(plan) {
        collect_plan_type_names(child, catalog, type_names);
    }
}

fn collect_sql_type_name(
    ty: crate::backend::parser::SqlType,
    catalog: &dyn CatalogLookup,
    type_names: &mut BTreeMap<u32, String>,
) {
    if ty.type_oid == 0 || type_names.contains_key(&ty.type_oid) {
        return;
    }
    if let Some(row) = catalog.type_by_oid(ty.type_oid)
        && matches!(row.typtype, 'c' | 'd' | 'e')
    {
        type_names.insert(ty.type_oid, quote_explain_identifier(&row.typname));
    }
}

fn push_verbose_plan_details(
    plan: &Plan,
    indent: usize,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) {
    let prefix = explain_detail_prefix(indent);
    let output = verbose_display_output_exprs(plan, ctx, false);
    if !output.is_empty() {
        lines.push(format!("{prefix}Output: {}", output.join(", ")));
    }

    match plan {
        Plan::OrderBy { input, items, .. } => {
            let input_names = verbose_plan_output_exprs(input, ctx, true);
            let sort_key = items
                .iter()
                .map(|item| render_verbose_expr(&item.expr, &input_names, ctx))
                .collect::<Vec<_>>()
                .join(", ");
            if !sort_key.is_empty() {
                lines.push(format!("{prefix}Sort Key: {sort_key}"));
            }
        }
        Plan::IncrementalSort {
            input,
            items,
            presorted_count,
            presorted_display_items,
            ..
        } => {
            let input_names = verbose_plan_output_exprs(input, ctx, true);
            let sort_key = items
                .iter()
                .map(|item| render_verbose_expr(&item.expr, &input_names, ctx))
                .collect::<Vec<_>>()
                .join(", ");
            if !sort_key.is_empty() {
                lines.push(format!("{prefix}Sort Key: {sort_key}"));
            }
            let presorted_key = if presorted_display_items.is_empty() {
                items
                    .iter()
                    .take(*presorted_count)
                    .map(|item| render_verbose_expr(&item.expr, &input_names, ctx))
                    .collect::<Vec<_>>()
            } else {
                presorted_display_items.clone()
            }
            .join(", ");
            if !presorted_key.is_empty() {
                lines.push(format!("{prefix}Presorted Key: {presorted_key}"));
            }
        }
        Plan::Aggregate {
            input,
            strategy,
            group_by,
            having,
            ..
        } => {
            let input_names = verbose_plan_output_exprs(input, ctx, true);
            if !group_by.is_empty() {
                let mut group_items = Vec::new();
                for expr in group_by {
                    let rendered = render_verbose_expr(expr, &input_names, ctx);
                    if !group_items.contains(&rendered) {
                        group_items.push(rendered);
                    }
                }
                let group_key = group_items.join(", ");
                if *strategy == AggregateStrategy::Mixed {
                    lines.push(format!("{prefix}Hash Key: {group_key}"));
                    lines.push(format!("{prefix}Group Key: ()"));
                } else {
                    lines.push(format!("{prefix}Group Key: {group_key}"));
                }
            }
            if let Some(having) = having {
                lines.push(format!(
                    "{prefix}Filter: {}",
                    render_verbose_expr(having, &verbose_plan_output_exprs(plan, ctx, true), ctx,)
                ));
            }
        }
        Plan::FunctionScan { call, .. } => {
            let label = if matches!(
                call,
                SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_)
            ) {
                "Table Function Call"
            } else {
                "Function Call"
            };
            lines.push(format!(
                "{prefix}{label}: {}",
                render_verbose_set_returning_call(call, ctx)
            ));
        }
        Plan::Filter {
            input, predicate, ..
        } => {
            let input_names = verbose_plan_output_exprs(input, ctx, true);
            lines.push(format!(
                "{prefix}Filter: {}",
                render_verbose_expr(predicate, &input_names, ctx)
            ));
        }
        Plan::SubqueryScan { filter, .. } => {
            if let Some(filter) = filter {
                let output_names = verbose_plan_output_exprs(plan, ctx, true);
                lines.push(format!(
                    "{prefix}Filter: {}",
                    render_verbose_expr(filter, &output_names, ctx)
                ));
            }
        }
        Plan::WindowAgg { input, clause, .. } => {
            let input_names = verbose_plan_output_exprs(input, ctx, true);
            if !clause.spec.partition_by.is_empty() {
                let partition_by = clause
                    .spec
                    .partition_by
                    .iter()
                    .map(|expr| render_verbose_expr(expr, &input_names, ctx))
                    .collect::<Vec<_>>()
                    .join(", ");
                lines.push(format!("{prefix}Partition By: {partition_by}"));
            }
            if !clause.spec.order_by.is_empty() {
                let order_by = clause
                    .spec
                    .order_by
                    .iter()
                    .map(|item| render_verbose_expr(&item.expr, &input_names, ctx))
                    .collect::<Vec<_>>()
                    .join(", ");
                lines.push(format!("{prefix}Order By: {order_by}"));
            }
        }
        Plan::NestedLoopJoin {
            left,
            right,
            nest_params,
            join_qual,
            qual,
            ..
        } => {
            let left_names = plan_join_output_exprs(left, ctx, true);
            let mut right_ctx = ctx.clone();
            right_ctx
                .exec_params
                .extend(nest_params.iter().map(|source| VerboseExecParam {
                    paramid: source.paramid,
                    expr: source.expr.clone(),
                    column_names: left_names.clone(),
                }));
            let right_names = plan_join_output_exprs(right, &right_ctx, true);
            if !join_qual.is_empty() {
                let rendered = join_qual
                    .iter()
                    .map(|expr| render_verbose_join_expr(expr, &left_names, &right_names, ctx))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                lines.push(format!("{prefix}Join Filter: {rendered}"));
            }
            if !qual.is_empty() {
                let rendered = qual
                    .iter()
                    .map(|expr| render_verbose_join_expr(expr, &left_names, &right_names, ctx))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                lines.push(format!("{prefix}Filter: {rendered}"));
            }
        }
        Plan::HashJoin {
            left,
            right,
            hash_keys,
            hash_clauses,
            join_qual,
            qual,
            ..
        } => {
            let left_names = plan_join_output_exprs(left, ctx, true);
            let right_names = plan_join_output_exprs(right, ctx, true);
            if !hash_clauses.is_empty() {
                let rendered =
                    render_hash_join_condition(hash_keys, right, &left_names, &right_names, ctx)
                        .unwrap_or_else(|| {
                            hash_clauses
                                .iter()
                                .map(|expr| {
                                    render_verbose_join_expr(expr, &left_names, &right_names, ctx)
                                })
                                .collect::<Vec<_>>()
                                .join(" AND ")
                        });
                lines.push(format!("{prefix}Hash Cond: {rendered}"));
            }
            if !join_qual.is_empty() {
                let rendered = join_qual
                    .iter()
                    .map(|expr| render_verbose_join_expr(expr, &left_names, &right_names, ctx))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                lines.push(format!("{prefix}Join Filter: {rendered}"));
            }
            if !qual.is_empty() {
                let rendered = qual
                    .iter()
                    .map(|expr| render_verbose_join_expr(expr, &left_names, &right_names, ctx))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                lines.push(format!("{prefix}Filter: {rendered}"));
            }
        }
        Plan::MergeJoin {
            left,
            right,
            merge_clauses,
            join_qual,
            qual,
            ..
        } => {
            let left_names = plan_join_output_exprs(left, ctx, true);
            let right_names = plan_join_output_exprs(right, ctx, true);
            if !merge_clauses.is_empty() {
                let rendered = merge_clauses
                    .iter()
                    .map(|expr| render_verbose_join_expr(expr, &left_names, &right_names, ctx))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                lines.push(format!("{prefix}Merge Cond: {rendered}"));
            }
            if !join_qual.is_empty() {
                let rendered = join_qual
                    .iter()
                    .map(|expr| render_verbose_join_expr(expr, &left_names, &right_names, ctx))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                lines.push(format!("{prefix}Join Filter: {rendered}"));
            }
            if !qual.is_empty() {
                let rendered = qual
                    .iter()
                    .map(|expr| render_verbose_join_expr(expr, &left_names, &right_names, ctx))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                lines.push(format!("{prefix}Filter: {rendered}"));
            }
        }
        _ => {}
    }
}

fn explain_plan_children_with_context(
    plan: &Plan,
    subplans: &[Plan],
    indent: usize,
    show_costs: bool,
    verbose: bool,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) {
    match plan {
        Plan::NestedLoopJoin {
            left,
            right,
            nest_params,
            ..
        } => {
            format_explain_plan_with_subplans_inner(
                left,
                subplans,
                indent + 1,
                show_costs,
                verbose,
                true,
                false,
                ctx,
                lines,
            );
            let mut right_ctx = ctx.clone();
            let left_names = if verbose {
                verbose_plan_output_exprs(left, ctx, true)
            } else {
                plan_join_output_exprs(left, ctx, true)
            };
            right_ctx
                .exec_params
                .extend(nest_params.iter().map(|source| VerboseExecParam {
                    paramid: source.paramid,
                    expr: source.expr.clone(),
                    column_names: left_names.clone(),
                }));
            format_explain_plan_with_subplans_inner(
                right,
                subplans,
                indent + 1,
                show_costs,
                verbose,
                true,
                false,
                &right_ctx,
                lines,
            );
        }
        Plan::BitmapHeapScan { bitmapqual, .. } => {
            let child_indent = indent + 1;
            format_explain_plan_with_subplans_inner(
                bitmapqual,
                subplans,
                child_indent,
                show_costs,
                verbose,
                true,
                false,
                ctx,
                lines,
            );
        }
        Plan::Aggregate {
            input,
            group_by,
            passthrough_exprs,
            accumulators,
            ..
        } => {
            let child_indent = indent + 1;
            let child_ctx = if verbose {
                let mut child_ctx = ctx.clone();
                child_ctx.scan_output_override = Some(aggregate_child_output_exprs(
                    input,
                    group_by,
                    passthrough_exprs,
                    accumulators,
                    ctx,
                ));
                child_ctx
            } else {
                ctx.clone()
            };
            format_explain_plan_with_subplans_inner(
                input,
                subplans,
                child_indent,
                show_costs,
                verbose,
                true,
                true,
                &child_ctx,
                lines,
            );
        }
        Plan::Hash { input, .. } => {
            let child_indent = indent + 1;
            format_explain_plan_with_subplans_inner(
                input,
                subplans,
                child_indent,
                show_costs,
                verbose,
                true,
                false,
                ctx,
                lines,
            );
        }
        Plan::OrderBy { .. }
        | Plan::IncrementalSort { .. }
        | Plan::Unique { .. }
        | Plan::SubqueryScan { .. } => {
            let child_indent = indent + 1;
            for child in direct_plan_children(plan) {
                format_explain_plan_with_subplans_inner(
                    child,
                    subplans,
                    child_indent,
                    show_costs,
                    verbose,
                    true,
                    matches!(plan, Plan::OrderBy { .. } | Plan::IncrementalSort { .. }),
                    ctx,
                    lines,
                );
            }
        }
        Plan::Append { children, .. } | Plan::MergeAppend { children, .. }
            if !verbose
                && !ctx.alias_through_aggregate_children
                && partitionwise_aggregate_append_alias_base(&flattened_append_children(
                    children,
                ))
                .is_some() =>
        {
            let children = flattened_append_children(children);
            format_partitionwise_aggregate_append_children(
                &children, subplans, indent, show_costs, verbose, ctx, lines,
            );
        }
        _ => {
            let mut values_seen = 0usize;
            let mut functions_seen = BTreeMap::<String, usize>::new();
            let mut relations_seen = BTreeMap::<String, usize>::new();
            let child_indent = if matches!(plan, Plan::SetOp { .. }) {
                indent
            } else {
                indent + 1
            };
            let reserve_append_parent_alias =
                matches!(plan, Plan::Append { .. } | Plan::MergeAppend { .. })
                    && !ctx.preserve_partition_child_aliases;
            for child in direct_plan_children(plan) {
                let child_ctx = context_for_sibling_scan(
                    ctx,
                    child,
                    &mut values_seen,
                    &mut functions_seen,
                    &mut relations_seen,
                    reserve_append_parent_alias,
                );
                format_explain_plan_with_subplans_inner(
                    child,
                    subplans,
                    child_indent,
                    show_costs,
                    verbose,
                    true,
                    matches!(
                        plan,
                        Plan::Append { .. }
                            | Plan::MergeAppend { .. }
                            | Plan::OrderBy { .. }
                            | Plan::IncrementalSort { .. }
                    ),
                    &child_ctx,
                    lines,
                );
            }
        }
    }
}

fn format_partitionwise_aggregate_append_children(
    children: &[&Plan],
    subplans: &[Plan],
    indent: usize,
    show_costs: bool,
    verbose: bool,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) {
    let Some(alias_base) = partitionwise_aggregate_append_alias_base(children) else {
        return;
    };
    let mut next_suffix = 0usize;
    let child_indent = indent + 1;
    for child in children {
        let leaf_bases = leaf_relation_bases(child);
        let mut child_ctx = ctx.clone();
        match leaf_bases.as_slice() {
            [] => {}
            [base_name] => {
                if next_suffix > 0 {
                    child_ctx
                        .relation_scan_aliases
                        .insert(base_name.clone(), format!("{alias_base}_{next_suffix}"));
                }
                next_suffix += 1;
            }
            _ => {
                // :HACK: PostgreSQL reserves an inherited alias for a visible
                // subpartitioned child before numbering that child's leaf scans.
                // The executable plan already has the right partitionwise shape;
                // this keeps EXPLAIN output aligned until inheritance planning
                // carries a real global alias map.
                next_suffix += 1;
                for base_name in leaf_bases {
                    child_ctx
                        .relation_scan_aliases
                        .insert(base_name, format!("{alias_base}_{next_suffix}"));
                    next_suffix += 1;
                }
                child_ctx.alias_through_aggregate_children = true;
            }
        }
        format_explain_plan_with_subplans_inner(
            child,
            subplans,
            child_indent,
            show_costs,
            verbose,
            true,
            true,
            &child_ctx,
            lines,
        );
    }
}

fn partitionwise_aggregate_append_alias_base(children: &[&Plan]) -> Option<String> {
    if children.len() < 2 || !children.iter().all(|child| plan_is_aggregate_child(child)) {
        return None;
    }
    let mut aliases = children
        .iter()
        .filter_map(|child| first_leaf_relation_alias_base(child));
    let first = aliases.next()?;
    aliases.all(|alias| alias == first).then_some(first)
}

fn plan_is_aggregate_child(plan: &Plan) -> bool {
    if matches!(plan, Plan::Aggregate { .. }) {
        return true;
    }
    explain_passthrough_plan_child(plan).is_some_and(plan_is_aggregate_child)
}

fn first_leaf_relation_alias_base(plan: &Plan) -> Option<String> {
    first_leaf_relation_name(plan).and_then(|relation_name| {
        relation_name
            .rsplit_once(' ')
            .map(|(_, alias)| inherited_root_alias(alias).unwrap_or(alias).to_string())
    })
}

fn leaf_relation_bases(plan: &Plan) -> Vec<String> {
    let mut bases = Vec::new();
    collect_leaf_relation_bases(plan, &mut bases);
    bases
}

fn collect_leaf_relation_bases(plan: &Plan, bases: &mut Vec<String>) {
    match plan {
        Plan::SeqScan { relation_name, .. }
        | Plan::IndexOnlyScan { relation_name, .. }
        | Plan::IndexScan { relation_name, .. }
        | Plan::BitmapHeapScan { relation_name, .. } => {
            bases.push(relation_name_base(relation_name).to_string());
        }
        Plan::BitmapIndexScan { .. }
        | Plan::BitmapOr { .. }
        | Plan::FunctionScan { .. }
        | Plan::Result { .. }
        | Plan::Values { .. }
        | Plan::WorkTableScan { .. } => {}
        _ => {
            for child in direct_plan_children(plan) {
                collect_leaf_relation_bases(child, bases);
            }
        }
    }
}

fn context_for_sibling_scan(
    ctx: &VerboseExplainContext,
    child: &Plan,
    values_seen: &mut usize,
    functions_seen: &mut BTreeMap<String, usize>,
    relations_seen: &mut BTreeMap<String, usize>,
    reserve_append_parent_alias: bool,
) -> VerboseExplainContext {
    let mut child_ctx = ctx.clone();
    if reserve_append_parent_alias {
        let inherited_sources =
            inherited_relation_leaf_sources(child, ctx.alias_through_aggregate_children);
        if inherited_sources.len() > 1 {
            for (base_name, alias_base) in inherited_sources {
                let seen = relations_seen.entry(alias_base.clone()).or_default();
                child_ctx
                    .relation_scan_aliases
                    .insert(base_name, format!("{alias_base}_{}", *seen + 1));
                *seen += 1;
            }
            return child_ctx;
        }
    }
    match child_leaf_scan_source(
        child,
        reserve_append_parent_alias,
        ctx.alias_through_aggregate_children,
    ) {
        Some(LeafScanSource::Values) => {
            if child_ctx.values_scan_name.is_none() {
                child_ctx.values_scan_name = Some(values_scan_name(*values_seen));
                *values_seen += 1;
            }
        }
        Some(LeafScanSource::Function(function_name)) => {
            if child_ctx.function_scan_alias.is_none() {
                let seen = functions_seen.entry(function_name.clone()).or_default();
                child_ctx.function_scan_alias =
                    (*seen > 0).then(|| format!("{function_name}_{seen}"));
                *seen += 1;
            }
        }
        Some(LeafScanSource::Relation {
            key,
            inherited_alias_base,
        }) => {
            if child_ctx.relation_scan_alias.is_none() {
                let seen = relations_seen.entry(key.clone()).or_default();
                child_ctx.relation_scan_alias = inherited_alias_base
                    .map(|alias_base| format!("{alias_base}_{}", *seen + 1))
                    .or_else(|| (*seen > 0).then(|| format!("{key}_{seen}")));
                *seen += 1;
            }
        }
        None => {}
    }
    child_ctx
}

fn inherited_relation_leaf_sources(
    plan: &Plan,
    alias_through_aggregate_children: bool,
) -> Vec<(String, String)> {
    let mut sources = Vec::new();
    collect_inherited_relation_leaf_sources(plan, alias_through_aggregate_children, &mut sources);
    sources
}

fn collect_inherited_relation_leaf_sources(
    plan: &Plan,
    alias_through_aggregate_children: bool,
    sources: &mut Vec<(String, String)>,
) {
    match plan {
        Plan::SeqScan { relation_name, .. }
        | Plan::IndexOnlyScan { relation_name, .. }
        | Plan::IndexScan { relation_name, .. }
        | Plan::BitmapHeapScan { relation_name, .. } => {
            if let Some((_, alias)) = relation_name.rsplit_once(' ') {
                let root_alias = inherited_root_alias(alias).unwrap_or(alias);
                sources.push((
                    relation_name_base(relation_name).to_string(),
                    root_alias.to_string(),
                ));
            }
        }
        Plan::Aggregate { input, .. } if alias_through_aggregate_children => {
            collect_inherited_relation_leaf_sources(
                input,
                alias_through_aggregate_children,
                sources,
            )
        }
        Plan::Aggregate { .. } => {}
        Plan::Hash { input, .. }
        | Plan::Unique { input, .. }
        | Plan::Filter { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Projection { input, .. }
        | Plan::WindowAgg { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::ProjectSet { input, .. }
        | Plan::CteScan {
            cte_plan: input, ..
        } => collect_inherited_relation_leaf_sources(
            input,
            alias_through_aggregate_children,
            sources,
        ),
        Plan::Append { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::SetOp { children, .. }
        | Plan::BitmapOr { children, .. } => {
            for child in children {
                collect_inherited_relation_leaf_sources(
                    child,
                    alias_through_aggregate_children,
                    sources,
                );
            }
        }
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::MergeJoin { left, right, .. }
        | Plan::RecursiveUnion {
            anchor: left,
            recursive: right,
            ..
        } => {
            collect_inherited_relation_leaf_sources(
                left,
                alias_through_aggregate_children,
                sources,
            );
            collect_inherited_relation_leaf_sources(
                right,
                alias_through_aggregate_children,
                sources,
            );
        }
        Plan::BitmapIndexScan { .. }
        | Plan::FunctionScan { .. }
        | Plan::Result { .. }
        | Plan::Values { .. }
        | Plan::WorkTableScan { .. } => {}
    }
}

enum LeafScanSource {
    Values,
    Function(String),
    Relation {
        key: String,
        inherited_alias_base: Option<String>,
    },
}

fn child_leaf_scan_source(
    plan: &Plan,
    reserve_append_parent_alias: bool,
    alias_through_aggregate_children: bool,
) -> Option<LeafScanSource> {
    match plan {
        Plan::Values { .. } => Some(LeafScanSource::Values),
        Plan::FunctionScan {
            call,
            table_alias: None,
            ..
        } => Some(LeafScanSource::Function(
            set_returning_call_label(call).to_string(),
        )),
        Plan::SeqScan { relation_name, .. }
        | Plan::IndexOnlyScan { relation_name, .. }
        | Plan::IndexScan { relation_name, .. }
        | Plan::BitmapHeapScan { relation_name, .. } => {
            relation_leaf_scan_source(relation_name, reserve_append_parent_alias)
        }
        Plan::Hash { input, .. }
        | Plan::Unique { input, .. }
        | Plan::Filter { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Projection { input, .. }
        | Plan::SubqueryScan { input, .. } => child_leaf_scan_source(
            input,
            reserve_append_parent_alias,
            alias_through_aggregate_children,
        ),
        Plan::Aggregate { input, .. } if alias_through_aggregate_children => {
            child_leaf_scan_source(
                input,
                reserve_append_parent_alias,
                alias_through_aggregate_children,
            )
        }
        _ => None,
    }
}

fn relation_leaf_scan_source(
    relation_name: &str,
    reserve_append_parent_alias: bool,
) -> Option<LeafScanSource> {
    if reserve_append_parent_alias && let Some((_, alias)) = relation_name.rsplit_once(' ') {
        let root_alias = inherited_root_alias(alias).unwrap_or(alias);
        return Some(LeafScanSource::Relation {
            key: root_alias.to_string(),
            inherited_alias_base: Some(root_alias.to_string()),
        });
    }
    unaliased_relation_name(relation_name).map(|name| LeafScanSource::Relation {
        key: name.to_string(),
        inherited_alias_base: None,
    })
}

fn relation_name_base(relation_name: &str) -> &str {
    relation_name
        .rsplit_once(' ')
        .map(|(base_name, _)| base_name)
        .unwrap_or(relation_name)
}

fn explain_node_prefix(indent: usize, is_child: bool) -> String {
    if is_child {
        let spaces = if indent <= 1 {
            indent * 2
        } else {
            2 + (indent - 1) * 6
        };
        format!("{}->  ", " ".repeat(spaces))
    } else {
        "  ".repeat(indent)
    }
}

fn qualified_scan_output_exprs(
    relation_name: &str,
    desc: &crate::include::nodes::primnodes::RelationDesc,
) -> Vec<String> {
    let qualifier = relation_name
        .split_once(' ')
        .map(|(_, alias)| alias.trim())
        .filter(|alias| !alias.is_empty());
    desc.columns
        .iter()
        .map(|column| match qualifier {
            Some(alias) => format!("{alias}.{}", column.name),
            None => column.name.clone(),
        })
        .collect()
}

fn qualified_base_scan_output_exprs(
    relation_name: &str,
    desc: &crate::include::nodes::primnodes::RelationDesc,
) -> Vec<String> {
    let qualifier = relation_name
        .split_once(' ')
        .map(|(_, alias)| alias.trim().to_string())
        .filter(|alias| !alias.is_empty())
        .unwrap_or_else(|| {
            relation_name
                .rsplit_once('.')
                .map(|(_, name)| name)
                .unwrap_or(relation_name)
                .to_string()
        });
    desc.columns
        .iter()
        .map(|column| format!("{qualifier}.{}", column.name))
        .collect()
}

fn qualified_subquery_scan_output_exprs(
    scan_name: Option<&str>,
    output_columns: &[QueryColumn],
) -> Vec<String> {
    match scan_name {
        Some(scan_name) => output_columns
            .iter()
            .map(|column| {
                format!(
                    "{}.{}",
                    quote_explain_identifier(scan_name),
                    quote_explain_identifier(&column.name)
                )
            })
            .collect(),
        None => output_columns
            .iter()
            .map(|column| quote_explain_identifier(&column.name))
            .collect(),
    }
}

fn qualified_scan_output_exprs_with_context(
    relation_name: &str,
    desc: &crate::include::nodes::primnodes::RelationDesc,
    ctx: &VerboseExplainContext,
) -> Vec<String> {
    if let Some(alias) = context_relation_scan_alias(ctx, relation_name) {
        return desc
            .columns
            .iter()
            .map(|column| format!("{alias}.{}", column.name))
            .collect();
    }
    qualified_base_scan_output_exprs(relation_name, desc)
}

fn unaliased_relation_name(relation_name: &str) -> Option<&str> {
    if relation_name.split_once(' ').is_some() {
        None
    } else {
        Some(
            relation_name
                .rsplit_once('.')
                .map(|(_, name)| name)
                .unwrap_or(relation_name),
        )
    }
}

fn values_scan_name(occurrence: usize) -> String {
    if occurrence == 0 {
        "\"*VALUES*\"".to_string()
    } else {
        format!("\"*VALUES*_{occurrence}\"")
    }
}

fn values_scan_output_exprs(column_count: usize, scan_name: &str) -> Vec<String> {
    (1..=column_count)
        .map(|index| format!("{scan_name}.column{index}"))
        .collect()
}

fn strip_partition_child_alias_suffix(alias: &str) -> &str {
    alias
        .rsplit_once('_')
        .filter(|(_, suffix)| !suffix.is_empty() && suffix.chars().all(|ch| ch.is_ascii_digit()))
        .map(|(base, _)| base)
        .unwrap_or(alias)
}

fn append_parent_output_exprs(
    children: &[Plan],
    ctx: &VerboseExplainContext,
) -> Option<Vec<String>> {
    let child_outputs = children
        .iter()
        .map(|child| plan_join_output_exprs(child, ctx, true))
        .collect::<Vec<_>>();
    let first = child_outputs.first()?;
    if child_outputs
        .iter()
        .any(|output| output.len() != first.len())
    {
        return None;
    }
    Some(
        first
            .iter()
            .map(|name| {
                name.split_once('.')
                    .map(|(qualifier, column)| {
                        format!(
                            "{}.{}",
                            strip_partition_child_alias_suffix(qualifier),
                            column
                        )
                    })
                    .unwrap_or_else(|| name.clone())
            })
            .collect(),
    )
}

fn aggregate_group_names_from_input_sort(
    input: &Plan,
    group_count: usize,
    ctx: &VerboseExplainContext,
) -> Option<Vec<String>> {
    let Plan::OrderBy {
        input: sort_input,
        items,
        display_items,
        ..
    } = input
    else {
        return None;
    };
    if group_count == 0 || items.len() < group_count {
        return None;
    }
    if display_items.len() >= group_count {
        return Some(display_items.iter().take(group_count).cloned().collect());
    }
    Some(nonverbose_sort_items(
        sort_input,
        &items[..group_count],
        &[],
        ctx,
    ))
}

fn plan_join_output_exprs(
    plan: &Plan,
    ctx: &VerboseExplainContext,
    for_parent_ref: bool,
) -> Vec<String> {
    match plan {
        Plan::Result { .. } => Vec::new(),
        Plan::Append { desc, children, .. } | Plan::MergeAppend { desc, children, .. } => {
            if for_parent_ref
                && let Some(output) = append_parent_output_exprs(children, ctx)
                && output.len() == desc.columns.len()
            {
                output
            } else {
                desc.columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect()
            }
        }
        Plan::SeqScan {
            relation_name,
            desc,
            ..
        }
        | Plan::IndexOnlyScan {
            relation_name,
            desc,
            ..
        }
        | Plan::IndexScan {
            relation_name,
            desc,
            ..
        }
        | Plan::BitmapHeapScan {
            relation_name,
            desc,
            ..
        } => qualified_scan_output_exprs_with_context(relation_name, desc, ctx),
        Plan::BitmapIndexScan { .. } | Plan::BitmapOr { .. } => Vec::new(),
        Plan::Hash { input, .. }
        | Plan::Unique { input, .. }
        | Plan::Filter { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. } => plan_join_output_exprs(input, ctx, for_parent_ref),
        Plan::SubqueryScan {
            scan_name,
            output_columns,
            ..
        } => qualified_subquery_scan_output_exprs(scan_name.as_deref(), output_columns),
        Plan::Projection { input, targets, .. } => {
            let input_names = plan_join_output_exprs(input, ctx, true);
            targets
                .iter()
                .filter(|target| !target.resjunk)
                .map(|target| {
                    if matches!(input.as_ref(), Plan::FunctionScan { .. })
                        && target.input_resno.is_some()
                        && matches!(target.expr, Expr::Var(_))
                    {
                        format!("{}.{}", target.name, target.name)
                    } else {
                        render_verbose_expr(&target.expr, &input_names, ctx)
                    }
                })
                .collect()
        }
        Plan::Aggregate {
            semantic_output_names: Some(names),
            ..
        } if !context_has_relation_aliases(ctx) => names.clone(),
        Plan::Aggregate {
            input,
            group_by,
            passthrough_exprs,
            accumulators,
            semantic_accumulators,
            ..
        } => {
            let input_names = plan_join_output_exprs(input, ctx, true);
            let mut output = if context_has_relation_aliases(ctx) {
                aggregate_group_names_from_input_sort(input, group_by.len(), ctx).unwrap_or_else(
                    || {
                        group_by
                            .iter()
                            .map(|expr| render_verbose_expr(expr, &input_names, ctx))
                            .collect::<Vec<_>>()
                    },
                )
            } else {
                group_by
                    .iter()
                    .map(|expr| render_verbose_expr(expr, &input_names, ctx))
                    .collect::<Vec<_>>()
            };
            output.extend(
                passthrough_exprs
                    .iter()
                    .map(|expr| render_verbose_expr(expr, &input_names, ctx)),
            );
            if let Some(display_accumulators) = semantic_accumulators {
                let offset = group_by.len() + passthrough_exprs.len();
                output.extend(
                    display_accumulators
                        .iter()
                        .enumerate()
                        .map(|(index, accum)| {
                            input_names.get(offset + index).cloned().unwrap_or_else(|| {
                                render_verbose_agg_accum(accum, &input_names, ctx)
                            })
                        }),
                );
                return output;
            }
            output.extend(accumulators.iter().map(|accum| {
                let rendered = render_verbose_agg_accum(accum, &input_names, ctx);
                if for_parent_ref {
                    format!("({rendered})")
                } else {
                    rendered
                }
            }));
            output
        }
        Plan::WindowAgg { output_columns, .. }
        | Plan::CteScan { output_columns, .. }
        | Plan::WorkTableScan { output_columns, .. }
        | Plan::RecursiveUnion { output_columns, .. } => output_columns
            .iter()
            .map(|column| column.name.clone())
            .collect(),
        Plan::SetOp {
            output_columns,
            children,
            ..
        } => {
            if for_parent_ref && let Some(first) = children.first() {
                let output = plan_join_output_exprs(first, ctx, true);
                if output.len() == output_columns.len() {
                    output
                } else {
                    output_columns
                        .iter()
                        .map(|column| column.name.clone())
                        .collect()
                }
            } else {
                output_columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect()
            }
        }
        Plan::Values { output_columns, .. } => values_scan_output_exprs(
            output_columns.len(),
            ctx.values_scan_name.as_deref().unwrap_or("\"*VALUES*\""),
        ),
        Plan::NestedLoopJoin {
            left,
            right,
            nest_params,
            ..
        } => {
            let mut output = plan_join_output_exprs(left, ctx, for_parent_ref);
            let mut right_ctx = ctx.clone();
            right_ctx
                .exec_params
                .extend(nest_params.iter().map(|source| VerboseExecParam {
                    paramid: source.paramid,
                    expr: source.expr.clone(),
                    column_names: output.clone(),
                }));
            output.extend(plan_join_output_exprs(right, &right_ctx, for_parent_ref));
            output
        }
        Plan::HashJoin { left, right, .. } | Plan::MergeJoin { left, right, .. } => {
            let mut output = plan_join_output_exprs(left, ctx, for_parent_ref);
            output.extend(plan_join_output_exprs(right, ctx, for_parent_ref));
            output
        }
        Plan::FunctionScan {
            call, table_alias, ..
        } => {
            if matches!(
                call,
                SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_)
            ) {
                return verbose_function_scan_output_exprs(call, table_alias.as_deref());
            }
            let qualifier = table_alias
                .as_deref()
                .or(ctx.function_scan_alias.as_deref());
            call.output_columns()
                .iter()
                .map(|column| match qualifier {
                    Some(alias) => format!("{alias}.{}", column.name),
                    None => format!("{}.{}", column.name, column.name),
                })
                .collect()
        }
        Plan::ProjectSet { targets, .. } => targets
            .iter()
            .map(|target| match target {
                ProjectSetTarget::Scalar(entry) => entry.name.clone(),
                ProjectSetTarget::Set { name, .. } => name.clone(),
            })
            .collect(),
    }
}

fn verbose_display_output_exprs(
    plan: &Plan,
    ctx: &VerboseExplainContext,
    for_parent_ref: bool,
) -> Vec<String> {
    match plan {
        Plan::SeqScan { .. }
        | Plan::IndexOnlyScan { .. }
        | Plan::IndexScan { .. }
        | Plan::BitmapHeapScan { .. } => ctx
            .scan_output_override
            .clone()
            .unwrap_or_else(|| verbose_plan_output_exprs(plan, ctx, for_parent_ref)),
        _ => verbose_plan_output_exprs(plan, ctx, for_parent_ref),
    }
}

fn aggregate_child_output_exprs(
    input: &Plan,
    group_by: &[Expr],
    passthrough_exprs: &[Expr],
    accumulators: &[AggAccum],
    ctx: &VerboseExplainContext,
) -> Vec<String> {
    let input_names = verbose_plan_output_exprs(input, ctx, true);
    let mut output = Vec::new();
    output.extend(
        group_by
            .iter()
            .map(|expr| render_verbose_expr(expr, &input_names, ctx)),
    );
    output.extend(
        passthrough_exprs
            .iter()
            .map(|expr| render_verbose_expr(expr, &input_names, ctx)),
    );
    for accum in accumulators {
        output.extend(
            accum
                .args
                .iter()
                .map(|arg| render_verbose_expr(arg, &input_names, ctx)),
        );
        if let Some(filter) = &accum.filter {
            output.push(render_verbose_expr(filter, &input_names, ctx));
        }
    }
    output
}

fn verbose_plan_output_exprs(
    plan: &Plan,
    ctx: &VerboseExplainContext,
    for_parent_ref: bool,
) -> Vec<String> {
    match plan {
        Plan::Result { .. } => Vec::new(),
        Plan::Append { desc, .. } | Plan::MergeAppend { desc, .. } => desc
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect(),
        Plan::SeqScan {
            relation_name,
            desc,
            ..
        }
        | Plan::IndexOnlyScan {
            relation_name,
            desc,
            ..
        }
        | Plan::IndexScan {
            relation_name,
            desc,
            ..
        }
        | Plan::BitmapHeapScan {
            relation_name,
            desc,
            ..
        } => qualified_base_scan_output_exprs(relation_name, desc),
        Plan::BitmapIndexScan { .. } | Plan::BitmapOr { .. } => Vec::new(),
        Plan::Hash { input, .. }
        | Plan::Unique { input, .. }
        | Plan::Filter { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. } => verbose_plan_output_exprs(input, ctx, for_parent_ref),
        Plan::SubqueryScan {
            scan_name,
            output_columns,
            ..
        } => qualified_subquery_scan_output_exprs(scan_name.as_deref(), output_columns),
        Plan::Projection { input, targets, .. } => {
            let input_names = plan_join_output_exprs(input, ctx, true);
            targets
                .iter()
                .filter(|target| !target.resjunk)
                .map(|target| render_verbose_expr(&target.expr, &input_names, ctx))
                .collect()
        }
        Plan::Aggregate {
            semantic_output_names: Some(names),
            ..
        } => names.clone(),
        Plan::Aggregate {
            input,
            group_by,
            passthrough_exprs,
            accumulators,
            semantic_accumulators,
            ..
        } => {
            let input_names = verbose_plan_output_exprs(input, ctx, true);
            let mut output = group_by
                .iter()
                .map(|expr| render_verbose_expr(expr, &input_names, ctx))
                .collect::<Vec<_>>();
            output.extend(
                passthrough_exprs
                    .iter()
                    .map(|expr| render_verbose_expr(expr, &input_names, ctx)),
            );
            if let Some(display_accumulators) = semantic_accumulators {
                let offset = group_by.len() + passthrough_exprs.len();
                output.extend(
                    display_accumulators
                        .iter()
                        .enumerate()
                        .map(|(index, accum)| {
                            input_names.get(offset + index).cloned().unwrap_or_else(|| {
                                render_verbose_agg_accum(accum, &input_names, ctx)
                            })
                        }),
                );
                return output;
            }
            output.extend(accumulators.iter().map(|accum| {
                let rendered = render_verbose_agg_accum(accum, &input_names, ctx);
                if for_parent_ref {
                    format!("({rendered})")
                } else {
                    rendered
                }
            }));
            output
        }
        Plan::WindowAgg { output_columns, .. }
        | Plan::CteScan { output_columns, .. }
        | Plan::WorkTableScan { output_columns, .. }
        | Plan::RecursiveUnion { output_columns, .. }
        | Plan::SetOp { output_columns, .. }
        | Plan::Values { output_columns, .. } => output_columns
            .iter()
            .map(|column| column.name.clone())
            .collect(),
        Plan::NestedLoopJoin {
            left,
            right,
            nest_params,
            ..
        } => {
            let mut output = verbose_plan_output_exprs(left, ctx, for_parent_ref);
            let mut right_ctx = ctx.clone();
            right_ctx
                .exec_params
                .extend(nest_params.iter().map(|source| VerboseExecParam {
                    paramid: source.paramid,
                    expr: source.expr.clone(),
                    column_names: output.clone(),
                }));
            output.extend(verbose_plan_output_exprs(right, &right_ctx, for_parent_ref));
            output
        }
        Plan::HashJoin { left, right, .. } | Plan::MergeJoin { left, right, .. } => {
            let mut output = verbose_plan_output_exprs(left, ctx, for_parent_ref);
            output.extend(verbose_plan_output_exprs(right, ctx, for_parent_ref));
            output
        }
        Plan::FunctionScan {
            call, table_alias, ..
        } => verbose_function_scan_output_exprs(call, table_alias.as_deref()),
        Plan::ProjectSet { targets, .. } => targets
            .iter()
            .map(|target| match target {
                ProjectSetTarget::Scalar(entry) => entry.name.clone(),
                ProjectSetTarget::Set { name, .. } => name.clone(),
            })
            .collect(),
    }
}

fn render_verbose_set_returning_call(
    call: &SetReturningCall,
    ctx: &VerboseExplainContext,
) -> String {
    if let SetReturningCall::SqlJsonTable(table) = call {
        return render_verbose_sql_json_table_call(table, ctx);
    }
    if let SetReturningCall::SqlXmlTable(table) = call {
        return render_verbose_sql_xml_table_call(table, ctx);
    }
    let name = set_returning_call_label(call);
    let args = match call {
        SetReturningCall::GenerateSeries {
            start,
            stop,
            step,
            timezone,
            ..
        } => {
            let mut args = vec![
                render_verbose_function_arg(start, ctx),
                render_verbose_function_arg(stop, ctx),
            ];
            if !matches!(
                step,
                Expr::Const(Value::Int32(1)) | Expr::Const(Value::Int64(1))
            ) {
                args.push(render_verbose_function_arg(step, ctx));
            }
            if let Some(timezone) = timezone {
                args.push(render_verbose_function_arg(timezone, ctx));
            }
            args
        }
        SetReturningCall::GenerateSubscripts {
            array,
            dimension,
            reverse,
            ..
        } => {
            let mut args = vec![
                render_verbose_function_arg(array, ctx),
                render_verbose_function_arg(dimension, ctx),
            ];
            if let Some(reverse) = reverse {
                args.push(render_verbose_function_arg(reverse, ctx));
            }
            args
        }
        SetReturningCall::PartitionTree { relid, .. }
        | SetReturningCall::PartitionAncestors { relid, .. } => {
            vec![render_verbose_function_arg(relid, ctx)]
        }
        SetReturningCall::PgLockStatus { .. } => Vec::new(),
        SetReturningCall::TxidSnapshotXip { arg, .. } => {
            vec![render_verbose_function_arg(arg, ctx)]
        }
        SetReturningCall::Unnest { args, .. }
        | SetReturningCall::JsonTableFunction { args, .. }
        | SetReturningCall::JsonRecordFunction { args, .. }
        | SetReturningCall::RegexTableFunction { args, .. }
        | SetReturningCall::StringTableFunction { args, .. }
        | SetReturningCall::TextSearchTableFunction { args, .. }
        | SetReturningCall::UserDefined { args, .. } => args
            .iter()
            .map(|expr| render_verbose_function_arg(expr, ctx))
            .collect(),
        SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_) => {
            unreachable!("handled above")
        }
    };
    format!("{name}({})", args.join(", "))
}

fn render_verbose_sql_xml_table_call(table: &SqlXmlTable, ctx: &VerboseExplainContext) -> String {
    let mut rendered = String::from("XMLTABLE(");
    if !table.namespaces.is_empty() {
        rendered.push_str("XMLNAMESPACES (");
        rendered.push_str(
            &table
                .namespaces
                .iter()
                .map(|namespace| {
                    let uri = render_verbose_sql_json_table_expr(&namespace.uri, ctx);
                    match namespace.name.as_deref() {
                        Some(name) => format!("{uri} AS {}", quote_explain_identifier(name)),
                        None => format!("DEFAULT {uri}"),
                    }
                })
                .collect::<Vec<_>>()
                .join(", "),
        );
        rendered.push_str("), ");
    }
    rendered.push_str(&render_parenthesized_verbose_xmltable_expr(
        &table.row_path,
        ctx,
    ));
    rendered.push_str(" PASSING ");
    rendered.push_str(&render_parenthesized_verbose_xmltable_expr(
        &table.document,
        ctx,
    ));
    rendered.push_str(" COLUMNS ");
    rendered.push_str(
        &table
            .columns
            .iter()
            .map(|column| {
                let name = quote_explain_identifier(&column.name);
                match &column.kind {
                    SqlXmlTableColumnKind::Ordinality => format!("{name} FOR ORDINALITY"),
                    SqlXmlTableColumnKind::Regular {
                        path,
                        default,
                        not_null,
                    } => {
                        let mut column =
                            format!("{name} {}", render_type_name(column.sql_type, ctx));
                        if let Some(default) = default {
                            column.push_str(" DEFAULT ");
                            column.push_str(&render_parenthesized_verbose_xmltable_expr(
                                default, ctx,
                            ));
                        }
                        if let Some(path) = path {
                            column.push_str(" PATH ");
                            column.push_str(&render_parenthesized_verbose_xmltable_expr(path, ctx));
                        }
                        if *not_null {
                            column.push_str(" NOT NULL");
                        }
                        column
                    }
                }
            })
            .collect::<Vec<_>>()
            .join(", "),
    );
    rendered.push(')');
    rendered
}

fn render_parenthesized_verbose_xmltable_expr(expr: &Expr, ctx: &VerboseExplainContext) -> String {
    format!("({})", render_verbose_sql_json_table_expr(expr, ctx))
}

fn render_verbose_sql_json_table_call(table: &SqlJsonTable, ctx: &VerboseExplainContext) -> String {
    let mut rendered = format!(
        "JSON_TABLE({}, '{}' AS {}",
        render_verbose_sql_json_table_expr(&table.context, ctx),
        render_sql_json_table_path(&table.root_path).replace('\'', "''"),
        quote_explain_identifier(&table.root_path_name)
    );
    if !table.passing.is_empty() {
        rendered.push_str(" PASSING ");
        rendered.push_str(
            &table
                .passing
                .iter()
                .map(|arg| {
                    format!(
                        "{} AS {}",
                        render_verbose_sql_json_table_expr(&arg.expr, ctx),
                        quote_explain_identifier(&arg.name)
                    )
                })
                .collect::<Vec<_>>()
                .join(", "),
        );
    }
    rendered.push_str(" COLUMNS (");
    rendered
        .push_str(&render_verbose_sql_json_table_plan_columns(table, &table.plan, ctx).join(", "));
    rendered.push(')');
    if matches!(table.on_error, SqlJsonTableBehavior::Error) {
        rendered.push_str(" ERROR ON ERROR");
    }
    rendered.push(')');
    rendered
}

fn render_verbose_sql_json_table_expr(expr: &Expr, ctx: &VerboseExplainContext) -> String {
    match expr {
        Expr::Const(Value::Text(_)) | Expr::Const(Value::TextRef(_, _)) => {
            format!("{}::text", render_verbose_function_arg(expr, ctx))
        }
        Expr::Const(Value::Json(value)) => format!("'{}'::jsonb", value.replace('\'', "''")),
        _ => render_verbose_function_arg(expr, ctx),
    }
}

fn render_sql_json_table_path(path: &str) -> String {
    canonicalize_jsonpath(path).unwrap_or_else(|_| path.to_string())
}

fn render_verbose_sql_json_table_plan_columns(
    table: &SqlJsonTable,
    plan: &SqlJsonTablePlan,
    ctx: &VerboseExplainContext,
) -> Vec<String> {
    match plan {
        SqlJsonTablePlan::PathScan {
            column_indexes,
            child,
            ..
        } => {
            let mut rendered = column_indexes
                .iter()
                .filter_map(|index| table.columns.get(*index))
                .map(|column| render_verbose_sql_json_table_column(column, ctx))
                .collect::<Vec<_>>();
            if let Some(child) = child {
                rendered.extend(render_verbose_sql_json_table_nested_plans(
                    table, child, ctx,
                ));
            }
            rendered
        }
        SqlJsonTablePlan::SiblingJoin { left, right } => {
            let mut rendered = render_verbose_sql_json_table_plan_columns(table, left, ctx);
            rendered.extend(render_verbose_sql_json_table_plan_columns(
                table, right, ctx,
            ));
            rendered
        }
    }
}

fn render_verbose_sql_json_table_nested_plans(
    table: &SqlJsonTable,
    plan: &SqlJsonTablePlan,
    ctx: &VerboseExplainContext,
) -> Vec<String> {
    match plan {
        SqlJsonTablePlan::PathScan {
            path,
            path_name,
            column_indexes,
            child,
            ..
        } => {
            let mut columns = column_indexes
                .iter()
                .filter_map(|index| table.columns.get(*index))
                .map(|column| render_verbose_sql_json_table_column(column, ctx))
                .collect::<Vec<_>>();
            if let Some(child) = child {
                columns.extend(render_verbose_sql_json_table_nested_plans(
                    table, child, ctx,
                ));
            }
            vec![format!(
                "NESTED PATH '{}' AS {} COLUMNS ({})",
                render_sql_json_table_path(path).replace('\'', "''"),
                quote_explain_identifier(path_name),
                columns.join(", ")
            )]
        }
        SqlJsonTablePlan::SiblingJoin { left, right } => {
            let mut rendered = render_verbose_sql_json_table_nested_plans(table, left, ctx);
            rendered.extend(render_verbose_sql_json_table_nested_plans(
                table, right, ctx,
            ));
            rendered
        }
    }
}

fn render_verbose_sql_json_table_column(
    column: &SqlJsonTableColumn,
    ctx: &VerboseExplainContext,
) -> String {
    let name = quote_explain_identifier(&column.name);
    let ty = render_type_name(column.sql_type, ctx);
    match &column.kind {
        SqlJsonTableColumnKind::Ordinality => format!("{name} FOR ORDINALITY"),
        SqlJsonTableColumnKind::Scalar {
            path,
            on_empty,
            on_error,
        } => {
            let mut rendered = format!(
                "{name} {ty} PATH '{}'",
                render_sql_json_table_path(path).replace('\'', "''")
            );
            append_verbose_sql_json_table_behavior(
                &mut rendered,
                on_empty,
                "EMPTY",
                matches!(on_empty, SqlJsonTableBehavior::Null),
                ctx,
            );
            append_verbose_sql_json_table_behavior(
                &mut rendered,
                on_error,
                "ERROR",
                matches!(on_error, SqlJsonTableBehavior::Null),
                ctx,
            );
            rendered
        }
        SqlJsonTableColumnKind::Formatted {
            path,
            format_json,
            wrapper,
            quotes,
            on_empty,
            on_error,
        } => {
            let mut rendered = format!("{name} {ty}");
            if *format_json && sql_json_table_column_renders_format_json(column) {
                rendered.push_str(" FORMAT JSON");
            }
            rendered.push_str(&format!(
                " PATH '{}'",
                render_sql_json_table_path(path).replace('\'', "''")
            ));
            rendered.push_str(match wrapper {
                SqlJsonTableWrapper::Unspecified | SqlJsonTableWrapper::Without => {
                    " WITHOUT WRAPPER"
                }
                SqlJsonTableWrapper::Conditional => " WITH CONDITIONAL WRAPPER",
                SqlJsonTableWrapper::Unconditional => " WITH UNCONDITIONAL WRAPPER",
            });
            rendered.push_str(match quotes {
                SqlJsonTableQuotes::Unspecified | SqlJsonTableQuotes::Keep => " KEEP QUOTES",
                SqlJsonTableQuotes::Omit => " OMIT QUOTES",
            });
            append_verbose_sql_json_table_behavior(
                &mut rendered,
                on_empty,
                "EMPTY",
                matches!(on_empty, SqlJsonTableBehavior::Null),
                ctx,
            );
            append_verbose_sql_json_table_behavior(
                &mut rendered,
                on_error,
                "ERROR",
                matches!(on_error, SqlJsonTableBehavior::Null),
                ctx,
            );
            rendered
        }
        SqlJsonTableColumnKind::Exists { path, on_error } => {
            let mut rendered = format!(
                "{name} {ty} EXISTS PATH '{}'",
                render_sql_json_table_path(path).replace('\'', "''")
            );
            append_verbose_sql_json_table_behavior(
                &mut rendered,
                on_error,
                "ERROR",
                matches!(on_error, SqlJsonTableBehavior::False),
                ctx,
            );
            rendered
        }
    }
}

fn sql_json_table_column_renders_format_json(column: &SqlJsonTableColumn) -> bool {
    !column.sql_type.is_array
        && !matches!(
            column.sql_type.kind,
            crate::backend::parser::SqlTypeKind::Json
                | crate::backend::parser::SqlTypeKind::Jsonb
                | crate::backend::parser::SqlTypeKind::Composite
                | crate::backend::parser::SqlTypeKind::Record
        )
}

fn append_verbose_sql_json_table_behavior(
    rendered: &mut String,
    behavior: &SqlJsonTableBehavior,
    target: &str,
    omit_default: bool,
    ctx: &VerboseExplainContext,
) {
    if omit_default {
        return;
    }
    match behavior {
        SqlJsonTableBehavior::Null => rendered.push_str(&format!(" NULL ON {target}")),
        SqlJsonTableBehavior::Error => rendered.push_str(&format!(" ERROR ON {target}")),
        SqlJsonTableBehavior::Empty => rendered.push_str(&format!(" EMPTY ON {target}")),
        SqlJsonTableBehavior::EmptyArray => {
            rendered.push_str(&format!(" EMPTY ARRAY ON {target}"));
        }
        SqlJsonTableBehavior::EmptyObject => {
            rendered.push_str(&format!(" EMPTY OBJECT ON {target}"));
        }
        SqlJsonTableBehavior::Default(expr) => rendered.push_str(&format!(
            " DEFAULT {} ON {target}",
            render_verbose_expr(expr, &[], ctx)
        )),
        SqlJsonTableBehavior::True => rendered.push_str(&format!(" TRUE ON {target}")),
        SqlJsonTableBehavior::False => rendered.push_str(&format!(" FALSE ON {target}")),
        SqlJsonTableBehavior::Unknown => rendered.push_str(&format!(" UNKNOWN ON {target}")),
    }
}

fn render_verbose_function_arg(expr: &Expr, ctx: &VerboseExplainContext) -> String {
    match expr {
        Expr::Cast(inner, _) => render_verbose_function_arg(inner, ctx),
        Expr::Const(value) => render_verbose_function_const(value),
        _ => render_verbose_expr(expr, &[], ctx),
    }
}

fn render_verbose_function_const(value: &Value) -> String {
    match value {
        Value::Int16(value) => value.to_string(),
        Value::Int32(value) => value.to_string(),
        Value::Int64(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Text(value) => format!("'{}'", value.replace('\'', "''")),
        Value::TextRef(_, _) => format!("'{}'", value.as_text().unwrap().replace('\'', "''")),
        Value::Json(value) => format!("'{}'::json", value.replace('\'', "''")),
        Value::JsonPath(value) => format!("'{}'::jsonpath", value.replace('\'', "''")),
        Value::Jsonb(bytes) => render_jsonb_bytes(bytes)
            .map(|value| format!("'{}'::jsonb", value.replace('\'', "''")))
            .unwrap_or_else(|_| "'null'::jsonb".into()),
        _ => strip_outer_parens(&render_explain_expr(&Expr::Const(value.clone()), &[])),
    }
}

fn render_verbose_agg_accum(
    accum: &AggAccum,
    column_names: &[String],
    ctx: &VerboseExplainContext,
) -> String {
    let name = builtin_aggregate_function_for_proc_oid(accum.aggfnoid)
        .map(|func| func.name().to_string())
        .unwrap_or_else(|| format!("agg_{}", accum.aggfnoid));
    let mut args = if accum.args.is_empty() {
        vec!["*".into()]
    } else {
        accum
            .args
            .iter()
            .map(|arg| render_verbose_expr(arg, column_names, ctx))
            .collect::<Vec<_>>()
    };
    if accum.distinct && !args.is_empty() {
        args[0] = format!("DISTINCT {}", args[0]);
    }
    let mut rendered = format!("{name}({})", args.join(", "));
    if let Some(filter) = &accum.filter {
        rendered.push_str(&format!(
            " FILTER (WHERE {})",
            render_verbose_expr(filter, column_names, ctx)
        ));
    }
    rendered
}

fn render_verbose_aggref(
    aggref: &crate::include::nodes::primnodes::Aggref,
    column_names: &[String],
    ctx: &VerboseExplainContext,
) -> String {
    let name = builtin_aggregate_function_for_proc_oid(aggref.aggfnoid)
        .map(|func| func.name().to_string())
        .unwrap_or_else(|| format!("agg_{}", aggref.aggfnoid));
    let mut args = if aggref.args.is_empty() {
        vec!["*".into()]
    } else {
        aggref
            .args
            .iter()
            .map(|arg| render_verbose_expr(arg, column_names, ctx))
            .collect::<Vec<_>>()
    };
    if aggref.aggdistinct && !args.is_empty() {
        args[0] = format!("DISTINCT {}", args[0]);
    }
    format!("{name}({})", args.join(", "))
}

fn render_verbose_join_expr(
    expr: &Expr,
    left_names: &[String],
    right_names: &[String],
    ctx: &VerboseExplainContext,
) -> String {
    let combined_names = || {
        let mut combined = left_names.to_vec();
        combined.extend_from_slice(right_names);
        combined
    };
    match expr {
        Expr::Var(var) if var.varno == crate::include::nodes::primnodes::OUTER_VAR => {
            render_var_name(var.varattno, left_names).unwrap_or_else(|| format!("{expr:?}"))
        }
        Expr::Var(var) if var.varno == crate::include::nodes::primnodes::INNER_VAR => {
            render_var_name(var.varattno, right_names).unwrap_or_else(|| format!("{expr:?}"))
        }
        Expr::FieldSelect {
            expr: inner, field, ..
        } => {
            if let Expr::Row { fields, .. } = inner.as_ref()
                && let Some((_, value)) = fields.iter().find(|(name, _)| name == field)
            {
                return render_verbose_join_expr(value, left_names, right_names, ctx);
            }
            format!("{expr:?}")
        }
        Expr::Var(var) => {
            let combined = combined_names();
            render_var_name(var.varattno, &combined).unwrap_or_else(|| format!("{expr:?}"))
        }
        Expr::Param(param) if param.paramkind == ParamKind::Exec => ctx
            .exec_params
            .iter()
            .rev()
            .find(|source| source.paramid == param.paramid)
            .map(|source| render_verbose_expr(&source.expr, &source.column_names, ctx))
            .unwrap_or_else(|| format!("${}", param.paramid)),
        Expr::Const(value) => {
            strip_outer_parens(&render_explain_expr(&Expr::Const(value.clone()), &[]))
        }
        Expr::Cast(inner, ty) => {
            let inner = render_verbose_join_expr(inner, left_names, right_names, ctx);
            format!("({inner})::{}", render_type_name(*ty, ctx))
        }
        Expr::Op(op) => {
            let [left, right] = op.args.as_slice() else {
                return strip_outer_parens(&crate::backend::executor::render_explain_join_expr(
                    expr,
                    left_names,
                    right_names,
                ));
            };
            let Some(op_text) = verbose_op_text(op.opno, op.op) else {
                return strip_outer_parens(&crate::backend::executor::render_explain_join_expr(
                    expr,
                    left_names,
                    right_names,
                ));
            };
            format!(
                "({} {} {})",
                render_verbose_join_expr(left, left_names, right_names, ctx),
                op_text,
                render_verbose_join_expr(right, left_names, right_names, ctx)
            )
        }
        Expr::Bool(bool_expr) => match bool_expr.boolop {
            crate::include::nodes::primnodes::BoolExprType::And => format!(
                "({})",
                bool_expr
                    .args
                    .iter()
                    .map(|arg| render_verbose_join_expr(arg, left_names, right_names, ctx))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            ),
            crate::include::nodes::primnodes::BoolExprType::Or => format!(
                "({})",
                bool_expr
                    .args
                    .iter()
                    .map(|arg| render_verbose_join_expr(arg, left_names, right_names, ctx))
                    .collect::<Vec<_>>()
                    .join(" OR ")
            ),
            crate::include::nodes::primnodes::BoolExprType::Not => {
                let Some(inner) = bool_expr.args.first() else {
                    return format!("{expr:?}");
                };
                format!(
                    "NOT {}",
                    render_verbose_join_expr(inner, left_names, right_names, ctx)
                )
            }
        },
        Expr::Coalesce(left, right) => format!(
            "COALESCE({}, {})",
            render_verbose_join_expr(left, left_names, right_names, ctx),
            render_verbose_join_expr(right, left_names, right_names, ctx)
        ),
        Expr::IsNull(inner) => format!(
            "{} IS NULL",
            render_verbose_join_expr(inner, left_names, right_names, ctx)
        ),
        Expr::IsNotNull(inner) => format!(
            "{} IS NOT NULL",
            render_verbose_join_expr(inner, left_names, right_names, ctx)
        ),
        _ => {
            let combined = combined_names();
            let rendered = render_verbose_expr(expr, &combined, ctx);
            if rendered.contains("OUTER_VAR") || rendered.contains("INNER_VAR") {
                strip_outer_parens(&crate::backend::executor::render_explain_join_expr(
                    expr,
                    left_names,
                    right_names,
                ))
            } else {
                rendered
            }
        }
    }
}

fn render_verbose_expr(
    expr: &Expr,
    column_names: &[String],
    ctx: &VerboseExplainContext,
) -> String {
    if let Some(rendered) = render_verbose_range_support_expr(expr, column_names) {
        return rendered;
    }
    match expr {
        Expr::Var(var) => render_var_name(var.varattno, column_names).unwrap_or_else(|| {
            crate::include::nodes::primnodes::attrno_index(var.varattno)
                .map(|index| format!("column{}", index + 1))
                .unwrap_or_else(|| strip_outer_parens(&render_explain_expr(expr, column_names)))
        }),
        Expr::FieldSelect {
            expr: inner, field, ..
        } => {
            if let Expr::Row { fields, .. } = inner.as_ref()
                && let Some((_, value)) = fields.iter().find(|(name, _)| name == field)
            {
                return render_verbose_expr(value, column_names, ctx);
            }
            strip_outer_parens(&render_explain_expr(expr, column_names))
        }
        Expr::Param(param) if param.paramkind == ParamKind::Exec => ctx
            .exec_params
            .iter()
            .rev()
            .find(|source| source.paramid == param.paramid)
            .map(|source| render_verbose_expr(&source.expr, &source.column_names, ctx))
            .unwrap_or_else(|| format!("${}", param.paramid)),
        Expr::Const(value) => {
            strip_outer_parens(&render_explain_expr(&Expr::Const(value.clone()), &[]))
        }
        Expr::Cast(inner, ty) => {
            let inner = render_verbose_expr(inner, column_names, ctx);
            format!("({inner})::{}", render_type_name(*ty, ctx))
        }
        Expr::Func(func)
            if matches!(
                func.implementation,
                ScalarFunctionImpl::Builtin(BuiltinScalarFunction::BpcharToText)
            ) && func.args.len() == 1 =>
        {
            render_verbose_expr(&func.args[0], column_names, ctx)
        }
        Expr::Op(op) => {
            let [left, right] = op.args.as_slice() else {
                return strip_outer_parens(&render_explain_expr(expr, column_names));
            };
            let Some(op_text) = verbose_op_text(op.opno, op.op) else {
                return strip_outer_parens(&render_explain_expr(expr, column_names));
            };
            let mut left_rendered = render_verbose_op_arg(left, column_names, ctx);
            let mut right_rendered = render_verbose_op_arg(right, column_names, ctx);
            if (verbose_expr_is_numeric(left) || left_rendered.starts_with("avg("))
                && matches!(right, Expr::Const(_))
            {
                right_rendered = format!("'{}'::numeric", right_rendered);
            }
            if (verbose_expr_is_numeric(right) || right_rendered.starts_with("avg("))
                && matches!(left, Expr::Const(_))
            {
                left_rendered = format!("'{}'::numeric", left_rendered);
            }
            format!("({left_rendered} {op_text} {right_rendered})")
        }
        Expr::Bool(bool_expr) => match bool_expr.boolop {
            crate::include::nodes::primnodes::BoolExprType::And => format!(
                "({})",
                bool_expr
                    .args
                    .iter()
                    .map(|arg| render_verbose_expr(arg, column_names, ctx))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            ),
            crate::include::nodes::primnodes::BoolExprType::Or => format!(
                "({})",
                bool_expr
                    .args
                    .iter()
                    .map(|arg| render_verbose_expr(arg, column_names, ctx))
                    .collect::<Vec<_>>()
                    .join(" OR ")
            ),
            crate::include::nodes::primnodes::BoolExprType::Not => {
                let Some(inner) = bool_expr.args.first() else {
                    return format!("{expr:?}");
                };
                format!("NOT {}", render_verbose_expr(inner, column_names, ctx))
            }
        },
        Expr::Aggref(aggref) => render_verbose_aggref(aggref, column_names, ctx),
        Expr::ScalarArrayOp(_) => render_explain_expr(expr, column_names),
        _ => strip_outer_parens(&render_explain_expr(expr, column_names)),
    }
}

fn render_verbose_op_arg(
    expr: &Expr,
    column_names: &[String],
    ctx: &VerboseExplainContext,
) -> String {
    let rendered = render_verbose_expr(expr, column_names, ctx);
    if matches!(expr, Expr::Op(_) | Expr::Bool(_)) {
        rendered
    } else {
        strip_outer_parens(&rendered)
    }
}

fn verbose_op_text(
    opno: u32,
    op: crate::include::nodes::primnodes::OpExprKind,
) -> Option<&'static str> {
    match opno {
        crate::include::catalog::TEXT_PATTERN_LT_OPERATOR_OID => return Some("~<~"),
        crate::include::catalog::TEXT_PATTERN_LE_OPERATOR_OID => return Some("~<=~"),
        crate::include::catalog::TEXT_PATTERN_GE_OPERATOR_OID => return Some("~>=~"),
        crate::include::catalog::TEXT_PATTERN_GT_OPERATOR_OID => return Some("~>~"),
        _ => {}
    }
    match op {
        crate::include::nodes::primnodes::OpExprKind::Add => Some("+"),
        crate::include::nodes::primnodes::OpExprKind::Sub => Some("-"),
        crate::include::nodes::primnodes::OpExprKind::Mul => Some("*"),
        crate::include::nodes::primnodes::OpExprKind::Div => Some("/"),
        crate::include::nodes::primnodes::OpExprKind::Mod => Some("%"),
        crate::include::nodes::primnodes::OpExprKind::Eq => Some("="),
        crate::include::nodes::primnodes::OpExprKind::NotEq => Some("<>"),
        crate::include::nodes::primnodes::OpExprKind::Lt => Some("<"),
        crate::include::nodes::primnodes::OpExprKind::LtEq => Some("<="),
        crate::include::nodes::primnodes::OpExprKind::Gt => Some(">"),
        crate::include::nodes::primnodes::OpExprKind::GtEq => Some(">="),
        crate::include::nodes::primnodes::OpExprKind::Concat => Some("||"),
        crate::include::nodes::primnodes::OpExprKind::ArrayOverlap => Some("&&"),
        crate::include::nodes::primnodes::OpExprKind::ArrayContains => Some("@>"),
        crate::include::nodes::primnodes::OpExprKind::ArrayContained => Some("<@"),
        _ => None,
    }
}

fn verbose_expr_is_numeric(expr: &Expr) -> bool {
    use crate::backend::parser::SqlTypeKind;
    match expr {
        Expr::Aggref(aggref) => {
            matches!(aggref.aggtype.kind, SqlTypeKind::Numeric)
                || builtin_aggregate_function_for_proc_oid(aggref.aggfnoid).is_some_and(|func| {
                    matches!(func, crate::include::nodes::primnodes::AggFunc::Avg)
                })
        }
        Expr::Cast(inner, ty) => {
            matches!(ty.kind, SqlTypeKind::Numeric) || verbose_expr_is_numeric(inner)
        }
        Expr::Collate { expr, .. } => verbose_expr_is_numeric(expr),
        Expr::Const(Value::Numeric(_)) => true,
        _ => false,
    }
}

fn render_var_name(
    attno: crate::include::nodes::primnodes::AttrNumber,
    names: &[String],
) -> Option<String> {
    crate::include::nodes::primnodes::attrno_index(attno)
        .and_then(|index| names.get(index).cloned())
}

fn strip_outer_parens(text: &str) -> String {
    text.strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
        .unwrap_or(text)
        .to_string()
}

fn render_type_name(ty: crate::backend::parser::SqlType, ctx: &VerboseExplainContext) -> String {
    use crate::backend::parser::SqlTypeKind::*;
    let element = ty.element_type();
    if !ty.is_array
        && let Some(name) = ctx.type_names.get(&ty.type_oid)
    {
        return name.clone();
    }
    let base = match element.kind {
        _ if element.type_oid != 0 && ctx.type_names.contains_key(&element.type_oid) => {
            ctx.type_names[&element.type_oid].clone()
        }
        Int2 => "smallint".into(),
        Int4 => "integer".into(),
        Int8 => "bigint".into(),
        Text => "text".into(),
        Varchar => element
            .char_len()
            .map(|len| format!("character varying({len})"))
            .unwrap_or_else(|| "character varying".into()),
        Char => element
            .char_len()
            .map(|len| format!("character({len})"))
            .unwrap_or_else(|| "bpchar".into()),
        Bool => "boolean".into(),
        Float4 => "real".into(),
        Float8 => "double precision".into(),
        Numeric => element
            .numeric_precision_scale()
            .map(|(precision, scale)| format!("numeric({precision},{scale})"))
            .unwrap_or_else(|| "numeric".into()),
        Json => "json".into(),
        Jsonb => "jsonb".into(),
        Uuid => "uuid".into(),
        _ => "unknown".into(),
    };
    if ty.is_array {
        format!("{base}[]")
    } else {
        base
    }
}

fn direct_plan_children(plan: &Plan) -> Vec<&Plan> {
    match plan {
        Plan::Result { .. }
        | Plan::SeqScan { .. }
        | Plan::IndexOnlyScan { .. }
        | Plan::IndexScan { .. }
        | Plan::BitmapIndexScan { .. }
        | Plan::FunctionScan { .. }
        | Plan::WorkTableScan { .. }
        | Plan::Values { .. } => Vec::new(),
        Plan::BitmapOr { children, .. } => children.iter().collect(),
        Plan::BitmapHeapScan { bitmapqual, .. } => vec![bitmapqual.as_ref()],
        Plan::Append { children, .. } | Plan::MergeAppend { children, .. } => {
            flattened_append_children(children)
        }
        Plan::SetOp { children, .. } => children.iter().collect(),
        Plan::Filter { input, .. }
            if matches!(
                input.as_ref(),
                Plan::SeqScan { .. } | Plan::IndexOnlyScan { .. } | Plan::IndexScan { .. }
            ) =>
        {
            Vec::new()
        }
        Plan::Projection { input, .. } if matches!(input.as_ref(), Plan::Result { .. }) => {
            Vec::new()
        }
        Plan::Hash { input, .. }
        | Plan::Unique { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Projection { input, .. }
        | Plan::WindowAgg { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::ProjectSet { input, .. }
        | Plan::CteScan {
            cte_plan: input, ..
        } => vec![input.as_ref()],
        Plan::Aggregate { input, .. } => vec![aggregate_explain_child(input)],
        Plan::Filter { input, .. } => vec![input.as_ref()],
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::MergeJoin { left, right, .. }
        | Plan::RecursiveUnion {
            anchor: left,
            recursive: right,
            ..
        } => vec![left.as_ref(), right.as_ref()],
    }
}

fn flattened_append_children(children: &[Plan]) -> Vec<&Plan> {
    let mut flattened = Vec::new();
    for child in children {
        if let Some(nested) = passthrough_append_children(child) {
            flattened.extend(flattened_append_children(nested));
        } else {
            flattened.push(child);
        }
    }
    flattened
}

fn passthrough_append_children(mut plan: &Plan) -> Option<&[Plan]> {
    loop {
        match plan {
            Plan::Append { children, .. } | Plan::MergeAppend { children, .. } => {
                return Some(children);
            }
            _ => {
                let child = explain_passthrough_plan_child(plan)?;
                plan = child;
            }
        }
    }
}

fn aggregate_explain_child(input: &Plan) -> &Plan {
    match input {
        Plan::Projection {
            input: child,
            targets,
            ..
        } if !targets_have_direct_subplans(targets) => child.as_ref(),
        _ => input,
    }
}

fn const_false_filter_result_plan(plan: &Plan) -> Option<PlanEstimate> {
    match plan {
        Plan::Filter {
            plan_info,
            input,
            predicate: Expr::Const(Value::Bool(false)),
        } if const_false_filter_input_can_render_as_result(input) => Some(*plan_info),
        Plan::Append {
            plan_info,
            children,
            ..
        } if !children.is_empty()
            && children
                .iter()
                .all(|child| const_false_filter_result_plan(child).is_some()) =>
        {
            Some(*plan_info)
        }
        Plan::Projection { input, targets, .. }
            if projection_targets_are_explain_passthrough(input, targets) =>
        {
            const_false_filter_result_plan(input)
        }
        _ => None,
    }
}

fn const_false_filter_input_can_render_as_result(input: &Plan) -> bool {
    match input {
        Plan::SeqScan { .. } | Plan::Result { .. } => true,
        Plan::Append { .. } => true,
        _ => false,
    }
}

fn direct_plan_subplans(plan: &Plan) -> Vec<&SubPlan> {
    let mut found = Vec::new();
    match plan {
        Plan::Result { .. }
        | Plan::Append { .. }
        | Plan::Unique { .. }
        | Plan::SeqScan { .. }
        | Plan::IndexOnlyScan { .. }
        | Plan::IndexScan { .. }
        | Plan::BitmapIndexScan { .. }
        | Plan::BitmapOr { .. }
        | Plan::BitmapHeapScan { .. }
        | Plan::Limit { .. }
        | Plan::LockRows { .. }
        | Plan::CteScan { .. }
        | Plan::WorkTableScan { .. }
        | Plan::RecursiveUnion { .. }
        | Plan::SetOp { .. } => {}
        Plan::SubqueryScan { filter, .. } => {
            if let Some(filter) = filter {
                collect_direct_expr_subplans(filter, &mut found);
            }
        }
        Plan::Hash { hash_keys, .. } => {
            for expr in hash_keys {
                collect_direct_expr_subplans(expr, &mut found);
            }
        }
        Plan::NestedLoopJoin {
            join_qual, qual, ..
        } => {
            for expr in join_qual {
                collect_direct_expr_subplans(expr, &mut found);
            }
            for expr in qual {
                collect_direct_expr_subplans(expr, &mut found);
            }
        }
        Plan::HashJoin {
            hash_clauses,
            hash_keys,
            join_qual,
            qual,
            ..
        } => {
            for expr in hash_clauses {
                collect_direct_expr_subplans(expr, &mut found);
            }
            for expr in hash_keys {
                collect_direct_expr_subplans(expr, &mut found);
            }
            for expr in join_qual {
                collect_direct_expr_subplans(expr, &mut found);
            }
            for expr in qual {
                collect_direct_expr_subplans(expr, &mut found);
            }
        }
        Plan::MergeJoin {
            merge_clauses,
            outer_merge_keys,
            inner_merge_keys,
            join_qual,
            qual,
            ..
        } => {
            for expr in merge_clauses {
                collect_direct_expr_subplans(expr, &mut found);
            }
            for expr in outer_merge_keys {
                collect_direct_expr_subplans(expr, &mut found);
            }
            for expr in inner_merge_keys {
                collect_direct_expr_subplans(expr, &mut found);
            }
            for expr in join_qual {
                collect_direct_expr_subplans(expr, &mut found);
            }
            for expr in qual {
                collect_direct_expr_subplans(expr, &mut found);
            }
        }
        Plan::Filter { predicate, .. } => collect_direct_expr_subplans(predicate, &mut found),
        Plan::OrderBy { items, .. } => {
            for item in items {
                collect_direct_expr_subplans(&item.expr, &mut found);
            }
        }
        Plan::IncrementalSort { items, .. } => {
            for item in items {
                collect_direct_expr_subplans(&item.expr, &mut found);
            }
        }
        Plan::MergeAppend { items, .. } => {
            for item in items {
                collect_direct_expr_subplans(&item.expr, &mut found);
            }
        }
        Plan::Projection { targets, .. } => {
            for target in targets {
                collect_direct_expr_subplans(&target.expr, &mut found);
            }
        }
        Plan::Aggregate {
            group_by,
            passthrough_exprs,
            accumulators,
            having,
            ..
        } => {
            for expr in group_by {
                collect_direct_expr_subplans(expr, &mut found);
            }
            for expr in passthrough_exprs {
                collect_direct_expr_subplans(expr, &mut found);
            }
            for accum in accumulators {
                collect_direct_agg_accum_subplans(accum, &mut found);
            }
            if let Some(expr) = having {
                collect_direct_expr_subplans(expr, &mut found);
            }
        }
        Plan::WindowAgg { clause, .. } => collect_direct_window_clause_subplans(clause, &mut found),
        Plan::FunctionScan { call, .. } => {
            collect_direct_set_returning_call_subplans(call, &mut found)
        }
        Plan::Values { rows, .. } => {
            for row in rows {
                for expr in row {
                    collect_direct_expr_subplans(expr, &mut found);
                }
            }
        }
        Plan::ProjectSet { targets, .. } => {
            for target in targets {
                collect_direct_project_set_target_subplans(target, &mut found);
            }
        }
    }

    let mut seen = BTreeSet::new();
    found
        .into_iter()
        .filter(|subplan| seen.insert(subplan.plan_id))
        .collect()
}

fn collect_direct_expr_subplans<'a>(expr: &'a Expr, out: &mut Vec<&'a SubPlan>) {
    match expr {
        Expr::SubPlan(subplan) => out.push(subplan),
        Expr::Aggref(aggref) => {
            for arg in &aggref.args {
                collect_direct_expr_subplans(arg, out);
            }
            for item in &aggref.aggorder {
                collect_direct_expr_subplans(&item.expr, out);
            }
            if let Some(filter) = &aggref.aggfilter {
                collect_direct_expr_subplans(filter, out);
            }
        }
        Expr::WindowFunc(window_func) => {
            for arg in &window_func.args {
                collect_direct_expr_subplans(arg, out);
            }
            if let WindowFuncKind::Aggregate(aggref) = &window_func.kind {
                for arg in &aggref.args {
                    collect_direct_expr_subplans(arg, out);
                }
                for item in &aggref.aggorder {
                    collect_direct_expr_subplans(&item.expr, out);
                }
                if let Some(filter) = &aggref.aggfilter {
                    collect_direct_expr_subplans(filter, out);
                }
            }
        }
        Expr::Op(op) => {
            for arg in &op.args {
                collect_direct_expr_subplans(arg, out);
            }
        }
        Expr::Bool(bool_expr) => {
            for arg in &bool_expr.args {
                collect_direct_expr_subplans(arg, out);
            }
        }
        Expr::Case(case_expr) => {
            if let Some(arg) = &case_expr.arg {
                collect_direct_expr_subplans(arg, out);
            }
            for arm in &case_expr.args {
                collect_direct_expr_subplans(&arm.expr, out);
                collect_direct_expr_subplans(&arm.result, out);
            }
            collect_direct_expr_subplans(&case_expr.defresult, out);
        }
        Expr::Func(func) => {
            for arg in &func.args {
                collect_direct_expr_subplans(arg, out);
            }
        }
        Expr::SqlJsonQueryFunction(func) => {
            for child in func.child_exprs() {
                collect_direct_expr_subplans(child, out);
            }
        }
        Expr::SetReturning(srf) => {
            for arg in set_returning_call_exprs(&srf.call) {
                collect_direct_expr_subplans(arg, out);
            }
        }
        Expr::SubLink(sublink) => {
            if let Some(testexpr) = &sublink.testexpr {
                collect_direct_expr_subplans(testexpr, out);
            }
        }
        Expr::ScalarArrayOp(saop) => {
            collect_direct_expr_subplans(&saop.left, out);
            collect_direct_expr_subplans(&saop.right, out);
        }
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => collect_direct_expr_subplans(inner, out),
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
            collect_direct_expr_subplans(expr, out);
            collect_direct_expr_subplans(pattern, out);
            if let Some(escape) = escape {
                collect_direct_expr_subplans(escape, out);
            }
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            collect_direct_expr_subplans(left, out);
            collect_direct_expr_subplans(right, out);
        }
        Expr::ArrayLiteral { elements, .. } => {
            for element in elements {
                collect_direct_expr_subplans(element, out);
            }
        }
        Expr::Row { fields, .. } => {
            for (_, expr) in fields {
                collect_direct_expr_subplans(expr, out);
            }
        }
        Expr::FieldSelect { expr, .. } => collect_direct_expr_subplans(expr, out),
        Expr::ArraySubscript { array, subscripts } => {
            collect_direct_expr_subplans(array, out);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    collect_direct_expr_subplans(lower, out);
                }
                if let Some(upper) = &subscript.upper {
                    collect_direct_expr_subplans(upper, out);
                }
            }
        }
        Expr::Xml(xml) => {
            for child in xml.child_exprs() {
                collect_direct_expr_subplans(child, out);
            }
        }
        Expr::Var(_)
        | Expr::Param(_)
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
        | Expr::LocalTimestamp { .. } => {}
    }
}

fn collect_direct_agg_accum_subplans<'a>(accum: &'a AggAccum, out: &mut Vec<&'a SubPlan>) {
    for arg in &accum.args {
        collect_direct_expr_subplans(arg, out);
    }
    for item in &accum.order_by {
        collect_direct_expr_subplans(&item.expr, out);
    }
    if let Some(filter) = &accum.filter {
        collect_direct_expr_subplans(filter, out);
    }
}

fn collect_direct_window_clause_subplans<'a>(clause: &'a WindowClause, out: &mut Vec<&'a SubPlan>) {
    for expr in &clause.spec.partition_by {
        collect_direct_expr_subplans(expr, out);
    }
    for item in &clause.spec.order_by {
        collect_direct_expr_subplans(&item.expr, out);
    }
    collect_direct_window_bound_subplans(&clause.spec.frame.start_bound, out);
    collect_direct_window_bound_subplans(&clause.spec.frame.end_bound, out);
    for func in &clause.functions {
        for arg in &func.args {
            collect_direct_expr_subplans(arg, out);
        }
        if let WindowFuncKind::Aggregate(aggref) = &func.kind {
            for arg in &aggref.args {
                collect_direct_expr_subplans(arg, out);
            }
            for item in &aggref.aggorder {
                collect_direct_expr_subplans(&item.expr, out);
            }
            if let Some(filter) = &aggref.aggfilter {
                collect_direct_expr_subplans(filter, out);
            }
        }
    }
}

fn collect_direct_window_bound_subplans<'a>(
    bound: &'a WindowFrameBound,
    out: &mut Vec<&'a SubPlan>,
) {
    match bound {
        WindowFrameBound::OffsetPreceding(offset) | WindowFrameBound::OffsetFollowing(offset) => {
            collect_direct_expr_subplans(&offset.expr, out)
        }
        WindowFrameBound::UnboundedPreceding
        | WindowFrameBound::CurrentRow
        | WindowFrameBound::UnboundedFollowing => {}
    }
}

fn collect_direct_set_returning_call_subplans<'a>(
    call: &'a SetReturningCall,
    out: &mut Vec<&'a SubPlan>,
) {
    match call {
        SetReturningCall::GenerateSeries {
            start,
            stop,
            step,
            timezone,
            ..
        } => {
            collect_direct_expr_subplans(start, out);
            collect_direct_expr_subplans(stop, out);
            collect_direct_expr_subplans(step, out);
            if let Some(timezone) = timezone {
                collect_direct_expr_subplans(timezone, out);
            }
        }
        SetReturningCall::GenerateSubscripts {
            array,
            dimension,
            reverse,
            ..
        } => {
            collect_direct_expr_subplans(array, out);
            collect_direct_expr_subplans(dimension, out);
            if let Some(reverse) = reverse {
                collect_direct_expr_subplans(reverse, out);
            }
        }
        SetReturningCall::PartitionTree { relid, .. }
        | SetReturningCall::PartitionAncestors { relid, .. } => {
            collect_direct_expr_subplans(relid, out);
        }
        SetReturningCall::PgLockStatus { .. } => {}
        SetReturningCall::TxidSnapshotXip { arg, .. } => {
            collect_direct_expr_subplans(arg, out);
        }
        SetReturningCall::Unnest { args, .. }
        | SetReturningCall::JsonTableFunction { args, .. }
        | SetReturningCall::JsonRecordFunction { args, .. }
        | SetReturningCall::RegexTableFunction { args, .. }
        | SetReturningCall::StringTableFunction { args, .. }
        | SetReturningCall::TextSearchTableFunction { args, .. }
        | SetReturningCall::UserDefined { args, .. } => {
            for arg in args {
                collect_direct_expr_subplans(arg, out);
            }
        }
        SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_) => {
            for arg in set_returning_call_exprs(call) {
                collect_direct_expr_subplans(arg, out);
            }
        }
    }
}

fn collect_direct_project_set_target_subplans<'a>(
    target: &'a ProjectSetTarget,
    out: &mut Vec<&'a SubPlan>,
) {
    match target {
        ProjectSetTarget::Scalar(entry) => collect_direct_expr_subplans(&entry.expr, out),
        ProjectSetTarget::Set { call, .. } => collect_direct_set_returning_call_subplans(call, out),
    }
}
