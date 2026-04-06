use super::nodes::{*, PlanStateKind};
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

    match &state.kind {
        PlanStateKind::Result(_) => {}
        PlanStateKind::SeqScan(_) => {}
        PlanStateKind::NestedLoopJoin(join) => {
            format_explain_lines(&join.left, indent + 1, analyze, lines);
            format_explain_lines(&join.right, indent + 1, analyze, lines);
        }
        PlanStateKind::Filter(filter) => format_explain_lines(&filter.input, indent + 1, analyze, lines),
        PlanStateKind::OrderBy(order_by) => format_explain_lines(&order_by.input, indent + 1, analyze, lines),
        PlanStateKind::Limit(limit) => format_explain_lines(&limit.input, indent + 1, analyze, lines),
        PlanStateKind::Projection(projection) => {
            format_explain_lines(&projection.input, indent + 1, analyze, lines)
        }
        PlanStateKind::Aggregate(aggregate) => {
            format_explain_lines(&aggregate.input, indent + 1, analyze, lines)
        }
    }
}

fn node_label(state: &PlanState) -> String {
    match &state.kind {
        PlanStateKind::Result(_) => "Result".into(),
        PlanStateKind::SeqScan(scan) => format!("Seq Scan on rel {}", scan.rel.rel_number),
        PlanStateKind::NestedLoopJoin(_) => "Nested Loop".into(),
        PlanStateKind::Filter(_) => "Filter".into(),
        PlanStateKind::OrderBy(_) => "Sort".into(),
        PlanStateKind::Limit(_) => "Limit".into(),
        PlanStateKind::Projection(_) => "Projection".into(),
        PlanStateKind::Aggregate(_) => "Aggregate".into(),
    }
}

fn node_stats(state: &PlanState) -> &NodeExecStats {
    match &state.kind {
        PlanStateKind::Result(result) => &result.stats,
        PlanStateKind::SeqScan(scan) => &scan.stats,
        PlanStateKind::NestedLoopJoin(join) => &join.stats,
        PlanStateKind::Filter(filter) => &filter.stats,
        PlanStateKind::OrderBy(order_by) => &order_by.stats,
        PlanStateKind::Limit(limit) => &limit.stats,
        PlanStateKind::Projection(projection) => &projection.stats,
        PlanStateKind::Aggregate(aggregate) => &aggregate.stats,
    }
}

pub(crate) fn format_buffer_usage(stats: BufferUsageStats) -> String {
    format!(
        "Buffers: shared hit={} read={} written={}",
        stats.shared_hit, stats.shared_read, stats.shared_written
    )
}
