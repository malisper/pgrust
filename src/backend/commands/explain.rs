use std::collections::BTreeSet;

use crate::backend::executor::executor_start;
use crate::include::nodes::datum::Value;
use crate::include::nodes::execnodes::*;
use crate::include::nodes::plannodes::{Plan, PlanEstimate};
use crate::include::nodes::primnodes::{
    AggAccum, Expr, ProjectSetTarget, SetReturningCall, SubPlan, TargetEntry, WindowClause,
    WindowFrameBound, WindowFuncKind,
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
    lines: &mut Vec<String>,
) {
    if let Some(plan_info) = const_false_filter_result_plan(plan) {
        let prefix = "  ".repeat(indent);
        push_explain_line(&format!("{prefix}Result"), plan_info, show_costs, lines);
        lines.push(format!("{prefix}  One-Time Filter: false"));
        return;
    }

    if let Some(child) = explain_passthrough_plan_child(plan) {
        format_explain_plan_with_subplans(child, subplans, indent, show_costs, lines);
        return;
    }

    let state = executor_start(plan.clone());
    push_explain_state_line(state.as_ref(), indent, false, show_costs, lines);
    state.explain_details(indent, false, show_costs, lines);

    for subplan in direct_plan_subplans(plan) {
        let prefix = "  ".repeat(indent + 1);
        let label = if subplan.par_param.is_empty() {
            format!("{prefix}InitPlan {}", subplan.plan_id + 1)
        } else {
            format!("{prefix}SubPlan {}", subplan.plan_id + 1)
        };
        lines.push(label);
        if let Some(child) = subplans.get(subplan.plan_id) {
            format_explain_plan_with_subplans(child, subplans, indent + 2, show_costs, lines);
        }
    }

    let child_indent = if matches!(plan, Plan::SetOp { .. }) {
        indent
    } else {
        indent + 1
    };
    for child in direct_plan_children(plan) {
        format_explain_plan_with_subplans(child, subplans, child_indent, show_costs, lines);
    }
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
        | Plan::FunctionScan { .. }
        | Plan::WorkTableScan { .. }
        | Plan::Values { .. } => Vec::new(),
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
