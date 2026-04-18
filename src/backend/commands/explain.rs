use crate::include::nodes::execnodes::*;
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
            stats.total_time.as_secs_f64() * 1000.0
            ,
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

    state.explain_details(indent, analyze, show_costs, lines);
    state.explain_children(indent, analyze, show_costs, lines);
}

pub(crate) fn format_buffer_usage(stats: BufferUsageStats) -> String {
    format!(
        "Buffers: shared hit={} read={} written={}",
        stats.shared_hit, stats.shared_read, stats.shared_written
    )
}
