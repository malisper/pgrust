use std::collections::BTreeSet;

use crate::backend::executor::executor_start;
use crate::backend::executor::{
    render_explain_expr, render_explain_join_expr_inner,
    render_explain_projection_expr_inner_with_qualifier,
};
use crate::include::catalog::builtin_aggregate_function_for_proc_oid;
use crate::include::nodes::datum::Value;
use crate::include::nodes::execnodes::*;
use crate::include::nodes::plannodes::{Plan, PlanEstimate};
use crate::include::nodes::primnodes::{
    AggAccum, Expr, INDEX_VAR, INNER_VAR, OUTER_VAR, OrderByEntry, ProjectSetTarget,
    SetReturningCall, SubPlan, TargetEntry, WindowClause, WindowFrameBound, WindowFuncKind,
    attrno_index,
};
use crate::include::storage::buf_internals::BufferUsageStats;

pub(crate) fn format_explain_lines(
    state: &dyn PlanNode,
    indent: usize,
    analyze: bool,
    lines: &mut Vec<String>,
) {
    format_explain_lines_with_costs(state, indent, analyze, true, lines);
}

pub(crate) fn format_explain_lines_with_costs(
    state: &dyn PlanNode,
    indent: usize,
    analyze: bool,
    show_costs: bool,
    lines: &mut Vec<String>,
) {
    if let Some(child) = state.explain_passthrough() {
        format_explain_lines_with_costs(child, indent, analyze, show_costs, lines);
        return;
    }
    push_explain_state_line(state, indent, analyze, show_costs, lines);
    state.explain_details(indent, analyze, show_costs, lines);
    state.explain_children(indent, analyze, show_costs, lines);
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
    verbose: bool,
    lines: &mut Vec<String>,
) {
    format_explain_plan_with_subplans_ctx(
        plan, subplans, indent, show_costs, verbose, None, None, lines,
    );
}

fn format_explain_plan_with_subplans_ctx(
    plan: &Plan,
    subplans: &[Plan],
    indent: usize,
    show_costs: bool,
    verbose: bool,
    outer_names: Option<&[String]>,
    rename_output: Option<&[String]>,
    lines: &mut Vec<String>,
) {
    if let Some(plan_info) = const_false_filter_result_plan(plan) {
        let prefix = "  ".repeat(indent);
        push_explain_line(&format!("{prefix}Result"), plan_info, show_costs, lines);
        lines.push(format!("{prefix}  One-Time Filter: false"));
        return;
    }

    if let Some(child) = explain_passthrough_plan_child(plan) {
        let output_names = verbose.then(|| plan.column_names());
        format_explain_plan_with_subplans_ctx(
            child,
            subplans,
            indent,
            show_costs,
            verbose,
            outer_names,
            output_names.as_deref().or(rename_output),
            lines,
        );
        return;
    }

    let state = executor_start(plan.clone());
    if verbose {
        push_explain_plan_line(
            &plan_node_label(plan, state.as_ref(), true, rename_output),
            plan.plan_info(),
            indent,
            show_costs,
            lines,
        );
        format_verbose_plan_details(plan, indent, outer_names, rename_output, lines);
    } else {
        push_explain_state_line(state.as_ref(), indent, false, show_costs, lines);
        state.explain_details(indent, false, show_costs, lines);
    }

    for subplan in direct_plan_subplans(plan) {
        let prefix = "  ".repeat(indent + 1);
        let label = if subplan.par_param.is_empty() {
            format!("{prefix}InitPlan {}", subplan.plan_id + 1)
        } else {
            format!("{prefix}SubPlan {}", subplan.plan_id + 1)
        };
        lines.push(label);
        if let Some(child) = subplans.get(subplan.plan_id) {
            format_explain_plan_with_subplans_ctx(
                child,
                subplans,
                indent + 2,
                show_costs,
                verbose,
                outer_names,
                None,
                lines,
            );
        }
    }

    let child_indent = if matches!(plan, Plan::SetOp { .. }) {
        indent
    } else {
        indent + 1
    };
    match plan {
        Plan::NestedLoopJoin { left, right, .. } if verbose => {
            format_explain_plan_with_subplans_ctx(
                left,
                subplans,
                child_indent,
                show_costs,
                verbose,
                outer_names,
                None,
                lines,
            );
            let left_outputs = verbose_plan_output_names_with_alias(left, outer_names, None);
            format_explain_plan_with_subplans_ctx(
                right,
                subplans,
                child_indent,
                show_costs,
                verbose,
                Some(&left_outputs),
                None,
                lines,
            );
        }
        _ => {
            for child in direct_plan_children(plan) {
                format_explain_plan_with_subplans_ctx(
                    child,
                    subplans,
                    child_indent,
                    show_costs,
                    verbose,
                    outer_names,
                    None,
                    lines,
                );
            }
        }
    }
}

fn push_explain_plan_line(
    label: &str,
    plan_info: PlanEstimate,
    indent: usize,
    show_costs: bool,
    lines: &mut Vec<String>,
) {
    let prefix = if indent == 0 {
        String::new()
    } else {
        format!("{}->  ", " ".repeat(2 + 6 * (indent - 1)))
    };
    push_explain_line(&format!("{prefix}{label}"), plan_info, show_costs, lines);
}

fn explain_detail_prefix(indent: usize) -> String {
    " ".repeat(2 + 6 * indent)
}

fn plan_node_label(
    plan: &Plan,
    state: &dyn PlanNode,
    verbose: bool,
    rename_output: Option<&[String]>,
) -> String {
    match plan {
        Plan::Aggregate { group_by, .. } if verbose && !group_by.is_empty() => {
            "HashAggregate".into()
        }
        Plan::FunctionScan { call, .. } if verbose => {
            verbose_function_scan_label(call, rename_output)
        }
        _ => state.node_label(),
    }
}

fn format_verbose_plan_details(
    plan: &Plan,
    indent: usize,
    outer_names: Option<&[String]>,
    rename_output: Option<&[String]>,
    lines: &mut Vec<String>,
) {
    let prefix = explain_detail_prefix(indent);
    match plan {
        Plan::OrderBy { input, items, .. } => {
            lines.push(format!(
                "{prefix}Output: {}",
                verbose_plan_output_names_with_alias(input, outer_names, None).join(", ")
            ));
            lines.push(format!(
                "{prefix}Sort Key: {}",
                render_verbose_order_by(
                    items,
                    outer_names,
                    &verbose_plan_output_names_with_alias(input, outer_names, None)
                )
            ));
        }
        Plan::NestedLoopJoin { left, right, .. } => {
            let left_outputs = verbose_plan_output_names_with_alias(left, outer_names, None);
            let right_outputs =
                verbose_plan_output_names_with_alias(right, Some(&left_outputs), None);
            let mut combined = left_outputs;
            combined.extend(right_outputs);
            lines.push(format!("{prefix}Output: {}", combined.join(", ")));
        }
        Plan::FunctionScan { call, .. } => {
            lines.push(format!(
                "{prefix}Output: {}",
                verbose_function_scan_outputs(call, rename_output).join(", ")
            ));
            lines.push(format!(
                "{prefix}Function Call: {}",
                render_set_returning_call(call)
            ));
        }
        Plan::Aggregate {
            group_by,
            accumulators,
            input,
            ..
        } => {
            let input_names = verbose_plan_output_names_with_alias(input, outer_names, None);
            let output =
                verbose_aggregate_outputs(group_by, accumulators, outer_names, &input_names);
            lines.push(format!("{prefix}Output: {}", output.join(", ")));
            if !group_by.is_empty() {
                lines.push(format!(
                    "{prefix}Group Key: {}",
                    render_group_keys(group_by, outer_names, &input_names)
                ));
            }
        }
        _ => {}
    }
}

fn verbose_plan_output_names(plan: &Plan, outer_names: Option<&[String]>) -> Vec<String> {
    verbose_plan_output_names_with_alias(plan, outer_names, None)
}

fn verbose_plan_output_names_with_alias(
    plan: &Plan,
    outer_names: Option<&[String]>,
    rename_output: Option<&[String]>,
) -> Vec<String> {
    match plan {
        Plan::FunctionScan { call, .. } => verbose_function_scan_outputs(call, rename_output),
        Plan::Aggregate {
            group_by,
            accumulators,
            input,
            ..
        } => {
            let input_names =
                verbose_plan_output_names_with_alias(input, outer_names, rename_output);
            verbose_aggregate_outputs(group_by, accumulators, outer_names, &input_names)
        }
        Plan::NestedLoopJoin { left, right, .. } => {
            let left_outputs =
                verbose_plan_output_names_with_alias(left, outer_names, rename_output);
            let right_outputs =
                verbose_plan_output_names_with_alias(right, Some(&left_outputs), rename_output);
            let mut combined = left_outputs;
            combined.extend(right_outputs);
            combined
        }
        Plan::OrderBy { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::Filter { input, .. }
        | Plan::Limit { input, .. }
        | Plan::Hash { input, .. } => {
            verbose_plan_output_names_with_alias(input, outer_names, rename_output)
        }
        Plan::Projection { input, targets, .. }
            if projection_targets_are_explain_passthrough(input, targets) =>
        {
            let output_names = plan.column_names();
            verbose_plan_output_names_with_alias(input, outer_names, Some(&output_names))
        }
        Plan::Projection { .. } => plan.column_names(),
        _ => plan.column_names(),
    }
}

fn verbose_function_scan_label(
    call: &SetReturningCall,
    rename_output: Option<&[String]>,
) -> String {
    let name = set_returning_call_name(call);
    let alias = rename_output
        .and_then(|names| names.first())
        .cloned()
        .or_else(|| {
            call.output_columns()
                .first()
                .map(|column| column.name.clone())
        });
    match alias {
        Some(column) => format!("Function Scan on pg_catalog.{name} {column}"),
        None => format!("Function Scan on pg_catalog.{name}"),
    }
}

fn verbose_function_scan_outputs(
    call: &SetReturningCall,
    rename_output: Option<&[String]>,
) -> Vec<String> {
    let output_names = rename_output.unwrap_or_else(|| {
        // Projection passthroughs carry alias-renamed output names for SRFs.
        // Fall back to the call's native output names when there is no alias layer.
        &[]
    });
    let effective_names = if output_names.is_empty() {
        call.output_columns()
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>()
    } else {
        output_names.to_vec()
    };
    match effective_names.first() {
        Some(column) => call
            .output_columns()
            .iter()
            .enumerate()
            .map(|(index, output)| {
                let name = effective_names.get(index).unwrap_or(&output.name);
                format!("{}.{}", column, name)
            })
            .collect(),
        None => effective_names,
    }
}

fn render_set_returning_call(call: &SetReturningCall) -> String {
    match call {
        SetReturningCall::GenerateSeries {
            start, stop, step, ..
        } => {
            let mut args = vec![
                render_set_returning_arg(start),
                render_set_returning_arg(stop),
            ];
            if !matches!(step, Expr::Const(Value::Int32(1))) {
                args.push(render_set_returning_arg(step));
            }
            format!("{}({})", set_returning_call_name(call), args.join(", "))
        }
        _ => set_returning_call_name(call).to_string(),
    }
}

fn render_set_returning_arg(expr: &Expr) -> String {
    match expr {
        Expr::Const(Value::Int32(v)) => v.to_string(),
        Expr::Const(Value::Int64(v)) => v.to_string(),
        _ => render_explain_expr(expr, &[]),
    }
}

fn set_returning_call_name(call: &SetReturningCall) -> &'static str {
    match call {
        SetReturningCall::GenerateSeries { .. } => "generate_series",
        SetReturningCall::Unnest { .. } => "unnest",
        SetReturningCall::JsonTableFunction { kind, .. } => match kind {
            crate::include::nodes::primnodes::JsonTableFunction::ObjectKeys => "json_object_keys",
            crate::include::nodes::primnodes::JsonTableFunction::Each => "json_each",
            crate::include::nodes::primnodes::JsonTableFunction::EachText => "json_each_text",
            crate::include::nodes::primnodes::JsonTableFunction::ArrayElements => {
                "json_array_elements"
            }
            crate::include::nodes::primnodes::JsonTableFunction::ArrayElementsText => {
                "json_array_elements_text"
            }
            crate::include::nodes::primnodes::JsonTableFunction::JsonbPathQuery => {
                "jsonb_path_query"
            }
            crate::include::nodes::primnodes::JsonTableFunction::JsonbObjectKeys => {
                "jsonb_object_keys"
            }
            crate::include::nodes::primnodes::JsonTableFunction::JsonbEach => "jsonb_each",
            crate::include::nodes::primnodes::JsonTableFunction::JsonbEachText => "jsonb_each_text",
            crate::include::nodes::primnodes::JsonTableFunction::JsonbArrayElements => {
                "jsonb_array_elements"
            }
            crate::include::nodes::primnodes::JsonTableFunction::JsonbArrayElementsText => {
                "jsonb_array_elements_text"
            }
        },
        SetReturningCall::JsonRecordFunction { kind, .. } => kind.name(),
        SetReturningCall::RegexTableFunction { kind, .. } => match kind {
            crate::include::nodes::primnodes::RegexTableFunction::Matches => "regexp_matches",
            crate::include::nodes::primnodes::RegexTableFunction::SplitToTable => {
                "regexp_split_to_table"
            }
        },
        SetReturningCall::StringTableFunction { kind, .. } => match kind {
            crate::include::nodes::primnodes::StringTableFunction::StringToTable => {
                "string_to_table"
            }
        },
        SetReturningCall::PartitionTree { .. } => "pg_partition_tree",
        SetReturningCall::PartitionAncestors { .. } => "pg_partition_ancestors",
        SetReturningCall::TextSearchTableFunction { kind, .. } => match kind {
            crate::include::nodes::primnodes::TextSearchTableFunction::TokenType => "ts_token_type",
            crate::include::nodes::primnodes::TextSearchTableFunction::Parse => "ts_parse",
            crate::include::nodes::primnodes::TextSearchTableFunction::Debug => "ts_debug",
        },
        SetReturningCall::UserDefined { .. } => "function",
    }
}

fn render_verbose_order_by(
    items: &[OrderByEntry],
    outer_names: Option<&[String]>,
    input_names: &[String],
) -> String {
    items
        .iter()
        .map(|item| render_verbose_expr(&item.expr, outer_names, input_names, false))
        .collect::<Vec<_>>()
        .join(", ")
}

fn verbose_aggregate_outputs(
    group_by: &[Expr],
    accumulators: &[AggAccum],
    outer_names: Option<&[String]>,
    input_names: &[String],
) -> Vec<String> {
    let mut outputs = group_by
        .iter()
        .map(|expr| render_verbose_group_expr(expr, outer_names, input_names))
        .collect::<Vec<_>>();
    outputs.extend(
        accumulators
            .iter()
            .map(|accum| render_aggregate_call(accum, outer_names, input_names)),
    );
    outputs
}

fn render_group_keys(
    group_by: &[Expr],
    outer_names: Option<&[String]>,
    input_names: &[String],
) -> String {
    group_by
        .iter()
        .map(|expr| render_verbose_group_expr(expr, outer_names, input_names))
        .collect::<Vec<_>>()
        .join(", ")
}

fn render_verbose_group_expr(
    expr: &Expr,
    outer_names: Option<&[String]>,
    input_names: &[String],
) -> String {
    render_aggregate_expr(expr, outer_names, input_names, false)
}

fn render_aggregate_call(
    accum: &AggAccum,
    outer_names: Option<&[String]>,
    input_names: &[String],
) -> String {
    let name = builtin_aggregate_function_for_proc_oid(accum.aggfnoid)
        .map(|func| func.name().to_string())
        .unwrap_or_else(|| "aggregate".into());
    if accum.args.is_empty() {
        return format!("{name}(*)");
    }
    let args = accum
        .args
        .iter()
        .map(|expr| render_aggregate_expr(expr, outer_names, input_names, true))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{name}({args})")
}

fn render_aggregate_expr(
    expr: &Expr,
    outer_names: Option<&[String]>,
    input_names: &[String],
    wrap: bool,
) -> String {
    let bare = match expr {
        Expr::Param(param) => outer_names
            .and_then(|names| names.get(param.paramid))
            .cloned()
            .unwrap_or_else(|| format!("{expr:?}")),
        Expr::Var(var) => attrno_index(var.varattno)
            .and_then(|index| input_names.get(index))
            .cloned()
            .unwrap_or_else(|| format!("{expr:?}")),
        Expr::Op(op)
            if matches!(
                op.op,
                crate::include::nodes::primnodes::OpExprKind::Add
                    | crate::include::nodes::primnodes::OpExprKind::Sub
                    | crate::include::nodes::primnodes::OpExprKind::Mul
                    | crate::include::nodes::primnodes::OpExprKind::Div
                    | crate::include::nodes::primnodes::OpExprKind::Mod
            ) =>
        {
            let [left, right] = op.args.as_slice() else {
                return format!("{expr:?}");
            };
            let op_text = match op.op {
                crate::include::nodes::primnodes::OpExprKind::Add => "+",
                crate::include::nodes::primnodes::OpExprKind::Sub => "-",
                crate::include::nodes::primnodes::OpExprKind::Mul => "*",
                crate::include::nodes::primnodes::OpExprKind::Div => "/",
                crate::include::nodes::primnodes::OpExprKind::Mod => "%",
                _ => unreachable!(),
            };
            format!(
                "{} {} {}",
                render_aggregate_expr(left, outer_names, input_names, false),
                op_text,
                render_aggregate_expr(right, outer_names, input_names, false)
            )
        }
        _ => render_verbose_expr(expr, outer_names, input_names, false),
    };
    if wrap { format!("({bare})") } else { bare }
}

fn render_verbose_expr(
    expr: &Expr,
    outer_names: Option<&[String]>,
    input_names: &[String],
    wrap: bool,
) -> String {
    let bare = match expr {
        Expr::Param(param) => outer_names
            .and_then(|names| names.get(param.paramid))
            .cloned()
            .unwrap_or_else(|| format!("{expr:?}")),
        Expr::Var(var) if var.varno == OUTER_VAR => outer_names
            .and_then(|names| attrno_index(var.varattno).and_then(|index| names.get(index)))
            .or_else(|| attrno_index(var.varattno).and_then(|index| input_names.get(index)))
            .cloned()
            .unwrap_or_else(|| format!("{expr:?}")),
        Expr::Var(var) if matches!(var.varno, INNER_VAR | INDEX_VAR) => attrno_index(var.varattno)
            .and_then(|index| input_names.get(index))
            .cloned()
            .unwrap_or_else(|| format!("{expr:?}")),
        Expr::Var(var) => attrno_index(var.varattno)
            .and_then(|index| input_names.get(index))
            .cloned()
            .unwrap_or_else(|| format!("{expr:?}")),
        Expr::Op(op)
            if matches!(
                op.op,
                crate::include::nodes::primnodes::OpExprKind::Add
                    | crate::include::nodes::primnodes::OpExprKind::Sub
                    | crate::include::nodes::primnodes::OpExprKind::Mul
                    | crate::include::nodes::primnodes::OpExprKind::Div
                    | crate::include::nodes::primnodes::OpExprKind::Mod
            ) =>
        {
            let [left, right] = op.args.as_slice() else {
                return format!("{expr:?}");
            };
            let op_text = match op.op {
                crate::include::nodes::primnodes::OpExprKind::Add => "+",
                crate::include::nodes::primnodes::OpExprKind::Sub => "-",
                crate::include::nodes::primnodes::OpExprKind::Mul => "*",
                crate::include::nodes::primnodes::OpExprKind::Div => "/",
                crate::include::nodes::primnodes::OpExprKind::Mod => "%",
                _ => unreachable!(),
            };
            format!(
                "{} {} {}",
                render_verbose_expr(left, outer_names, input_names, false),
                op_text,
                render_verbose_expr(right, outer_names, input_names, false)
            )
        }
        _ => match outer_names {
            Some(outer) => render_explain_join_expr_inner(expr, outer, input_names),
            None => render_explain_projection_expr_inner_with_qualifier(expr, None, input_names),
        },
    };
    if wrap { format!("({bare})") } else { bare }
}

fn explain_passthrough_plan_child(plan: &Plan) -> Option<&Plan> {
    match plan {
        Plan::Projection { input, targets, .. } => {
            projection_targets_are_explain_passthrough(input, targets).then_some(input.as_ref())
        }
        _ => None,
    }
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
    let full_width_projection =
        targets.len() == input_names.len() && targets.iter().all(|target| !target.resjunk);
    if matches!(input, Plan::WindowAgg { .. }) && full_width_projection {
        return true;
    }
    full_width_projection
        && targets
            .iter()
            .all(|target| matches!(target.expr, Expr::Var(_)))
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
    lines: &mut Vec<String>,
) {
    let prefix = "  ".repeat(indent);
    let label = state.node_label();
    let plan_info = state.plan_info();
    if analyze && show_costs {
        let stats = state.node_stats();
        lines.push(format!(
            "{prefix}{label}  (cost={:.2}..{:.2} rows={} width={}) (actual time={:.3}..{:.3} rows={:.2} loops={})",
            plan_info.startup_cost.as_f64(),
            plan_info.total_cost.as_f64(),
            plan_info.plan_rows.as_f64().round() as u64,
            plan_info.plan_width,
            stats
                .first_tuple_time
                .unwrap_or_default()
                .as_secs_f64()
                * 1000.0,
            stats.total_time.as_secs_f64() * 1000.0,
            stats.rows as f64,
            stats.loops,
        ));
    } else if analyze {
        let stats = state.node_stats();
        lines.push(format!(
            "{prefix}{label}  (actual time={:.3}..{:.3} rows={:.2} loops={})",
            stats.first_tuple_time.unwrap_or_default().as_secs_f64() * 1000.0,
            stats.total_time.as_secs_f64() * 1000.0,
            stats.rows as f64,
            stats.loops,
        ));
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

fn direct_plan_children(plan: &Plan) -> Vec<&Plan> {
    match plan {
        Plan::Result { .. }
        | Plan::SeqScan { .. }
        | Plan::IndexScan { .. }
        | Plan::BitmapIndexScan { .. }
        | Plan::FunctionScan { .. }
        | Plan::WorkTableScan { .. }
        | Plan::Values { .. } => Vec::new(),
        Plan::BitmapHeapScan { bitmapqual, .. } => vec![bitmapqual.as_ref()],
        Plan::Append { children, .. } | Plan::SetOp { children, .. } => children.iter().collect(),
        Plan::Filter { input, .. } if matches!(input.as_ref(), Plan::SeqScan { .. }) => Vec::new(),
        Plan::Hash { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::Limit { input, .. }
        | Plan::Projection { input, .. }
        | Plan::Aggregate { input, .. }
        | Plan::WindowAgg { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::ProjectSet { input, .. }
        | Plan::CteScan {
            cte_plan: input, ..
        } => vec![input.as_ref()],
        Plan::Filter { input, .. } => vec![input.as_ref()],
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::RecursiveUnion {
            anchor: left,
            recursive: right,
            ..
        } => vec![left.as_ref(), right.as_ref()],
    }
}

fn const_false_filter_result_plan(plan: &Plan) -> Option<PlanEstimate> {
    match plan {
        Plan::Filter {
            plan_info,
            input,
            predicate: Expr::Const(Value::Bool(false)),
        } if matches!(input.as_ref(), Plan::SeqScan { .. }) => Some(*plan_info),
        _ => None,
    }
}

fn direct_plan_subplans(plan: &Plan) -> Vec<&SubPlan> {
    let mut found = Vec::new();
    match plan {
        Plan::Result { .. }
        | Plan::Append { .. }
        | Plan::SeqScan { .. }
        | Plan::IndexScan { .. }
        | Plan::BitmapIndexScan { .. }
        | Plan::BitmapHeapScan { .. }
        | Plan::Limit { .. }
        | Plan::SubqueryScan { .. }
        | Plan::CteScan { .. }
        | Plan::WorkTableScan { .. }
        | Plan::RecursiveUnion { .. }
        | Plan::SetOp { .. } => {}
        Plan::Hash { hash_keys, .. } => {
            for expr in hash_keys {
                collect_direct_expr_subplans(expr, &mut found);
            }
        }
        Plan::NestedLoopJoin {
            join_qual, qual, ..
        }
        | Plan::HashJoin {
            hash_clauses: join_qual,
            qual,
            ..
        } => {
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
        Plan::Projection { targets, .. } => {
            for target in targets {
                collect_direct_expr_subplans(&target.expr, &mut found);
            }
        }
        Plan::Aggregate {
            group_by,
            accumulators,
            having,
            ..
        } => {
            for expr in group_by {
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
        WindowFrameBound::OffsetPreceding(expr) | WindowFrameBound::OffsetFollowing(expr) => {
            collect_direct_expr_subplans(expr, out)
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
            start, stop, step, ..
        } => {
            collect_direct_expr_subplans(start, out);
            collect_direct_expr_subplans(stop, out);
            collect_direct_expr_subplans(step, out);
        }
        SetReturningCall::PartitionTree { relid, .. }
        | SetReturningCall::PartitionAncestors { relid, .. } => {
            collect_direct_expr_subplans(relid, out);
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
