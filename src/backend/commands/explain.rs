use crate::include::nodes::execnodes::*;
use crate::include::storage::buf_internals::BufferUsageStats;

pub(crate) fn format_explain_lines(
    state: &dyn PlanNode,
    indent: usize,
    analyze: bool,
    lines: &mut Vec<String>,
) {
    let prefix = "  ".repeat(indent);
    let label = state.node_label();
    if analyze {
        let stats = state.node_stats();
        lines.push(format!(
            "{prefix}{label} (actual rows={} loops={} time={:.3} ms)",
            stats.rows,
            stats.loops,
            stats.total_time.as_secs_f64() * 1000.0
        ));
    } else {
        lines.push(format!("{prefix}{label}"));
    }

    state.explain_children(indent, analyze, lines);
}

pub(crate) fn format_buffer_usage(stats: BufferUsageStats) -> String {
    format!(
        "Buffers: shared hit={} read={} written={}",
        stats.shared_hit, stats.shared_read, stats.shared_written
    )
}
