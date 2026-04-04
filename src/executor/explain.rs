use super::nodes::*;
use crate::BufferUsageStats;

pub(crate) fn format_explain_lines(
    state: &PlanState,
    indent: usize,
    analyze: bool,
    lines: &mut Vec<String>,
) {
    let prefix = "  ".repeat(indent);
    let label = node_label(state);
    if analyze {
        let stats = node_stats(state);
        lines.push(format!(
            "{prefix}{label} (actual rows={} loops={} time={:.3} ms)",
            stats.rows,
            stats.loops,
            stats.total_time.as_secs_f64() * 1000.0
        ));
    } else {
        lines.push(format!("{prefix}{label}"));
    }

    match state {
        PlanState::Result(_) => {}
        PlanState::SeqScan(_) => {}
        PlanState::NestedLoopJoin(join) => {
            format_explain_lines(&join.left, indent + 1, analyze, lines);
            format_explain_lines(&join.right, indent + 1, analyze, lines);
        }
        PlanState::Filter(filter) => format_explain_lines(&filter.input, indent + 1, analyze, lines),
        PlanState::OrderBy(order_by) => format_explain_lines(&order_by.input, indent + 1, analyze, lines),
        PlanState::Limit(limit) => format_explain_lines(&limit.input, indent + 1, analyze, lines),
        PlanState::Projection(projection) => {
            format_explain_lines(&projection.input, indent + 1, analyze, lines)
        }
        PlanState::Aggregate(aggregate) => {
            format_explain_lines(&aggregate.input, indent + 1, analyze, lines)
        }
    }
}

fn node_label(state: &PlanState) -> String {
    match state {
        PlanState::Result(_) => "Result".into(),
        PlanState::SeqScan(scan) => format!("Seq Scan on rel {}", scan.rel.rel_number),
        PlanState::NestedLoopJoin(_) => "Nested Loop".into(),
        PlanState::Filter(_) => "Filter".into(),
        PlanState::OrderBy(_) => "Sort".into(),
        PlanState::Limit(_) => "Limit".into(),
        PlanState::Projection(_) => "Projection".into(),
        PlanState::Aggregate(_) => "Aggregate".into(),
    }
}

fn node_stats(state: &PlanState) -> &NodeExecStats {
    match state {
        PlanState::Result(result) => &result.stats,
        PlanState::SeqScan(scan) => &scan.stats,
        PlanState::NestedLoopJoin(join) => &join.stats,
        PlanState::Filter(filter) => &filter.stats,
        PlanState::OrderBy(order_by) => &order_by.stats,
        PlanState::Limit(limit) => &limit.stats,
        PlanState::Projection(projection) => &projection.stats,
        PlanState::Aggregate(aggregate) => &aggregate.stats,
    }
}

pub(crate) fn format_buffer_usage(stats: BufferUsageStats) -> String {
    format!(
        "Buffers: shared hit={} read={} written={}",
        stats.shared_hit, stats.shared_read, stats.shared_written
    )
}
