use std::collections::HashMap;

use pgrust_nodes::pathnodes::PlannerConfig;
use pgrust_nodes::plannodes::EstimateValue;

pub fn planner_config_from_executor_gucs(gucs: &HashMap<String, String>) -> PlannerConfig {
    PlannerConfig {
        enable_partitionwise_join: bool_executor_guc(gucs, "enable_partitionwise_join", false),
        enable_partitionwise_aggregate: bool_executor_guc(
            gucs,
            "enable_partitionwise_aggregate",
            false,
        ),
        enable_seqscan: bool_executor_guc(gucs, "enable_seqscan", true),
        enable_indexscan: bool_executor_guc(gucs, "enable_indexscan", true),
        enable_indexonlyscan: bool_executor_guc(gucs, "enable_indexonlyscan", true),
        enable_bitmapscan: bool_executor_guc(gucs, "enable_bitmapscan", true),
        enable_nestloop: bool_executor_guc(gucs, "enable_nestloop", true),
        enable_hashjoin: bool_executor_guc(gucs, "enable_hashjoin", true),
        enable_mergejoin: bool_executor_guc(gucs, "enable_mergejoin", true),
        enable_memoize: bool_executor_guc(gucs, "enable_memoize", true),
        enable_material: bool_executor_guc(gucs, "enable_material", true),
        enable_partition_pruning: bool_executor_guc(gucs, "enable_partition_pruning", true),
        constraint_exclusion_on: gucs
            .get("constraint_exclusion")
            .is_some_and(|value| value.eq_ignore_ascii_case("on")),
        constraint_exclusion_partition: gucs
            .get("constraint_exclusion")
            .map(|value| {
                value.eq_ignore_ascii_case("partition") || value.eq_ignore_ascii_case("on")
            })
            .unwrap_or(true),
        retain_partial_index_filters: false,
        enable_hashagg: bool_executor_guc(gucs, "enable_hashagg", true),
        enable_presorted_aggregate: bool_executor_guc(gucs, "enable_presorted_aggregate", true),
        enable_sort: bool_executor_guc(gucs, "enable_sort", true),
        enable_parallel_append: bool_executor_guc(gucs, "enable_parallel_append", true),
        enable_parallel_hash: bool_executor_guc(gucs, "enable_parallel_hash", true),
        force_parallel_gather: bool_executor_guc(gucs, "debug_parallel_query", false),
        max_parallel_workers: usize_executor_guc(gucs, "max_parallel_workers", 8),
        max_parallel_workers_per_gather: usize_executor_guc(
            gucs,
            "max_parallel_workers_per_gather",
            2,
        ),
        parallel_leader_participation: bool_executor_guc(
            gucs,
            "parallel_leader_participation",
            true,
        ),
        min_parallel_table_scan_size: size_executor_guc_bytes(
            gucs,
            "min_parallel_table_scan_size",
            8 * 1024 * 1024,
        ),
        min_parallel_index_scan_size: size_executor_guc_bytes(
            gucs,
            "min_parallel_index_scan_size",
            512 * 1024,
        ),
        parallel_setup_cost: EstimateValue(f64_executor_guc(gucs, "parallel_setup_cost", 1000.0)),
        parallel_tuple_cost: EstimateValue(f64_executor_guc(gucs, "parallel_tuple_cost", 0.1)),
        fold_constants: true,
    }
}

fn bool_executor_guc(gucs: &HashMap<String, String>, name: &str, default: bool) -> bool {
    gucs.get(name)
        .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
            "on" | "true" | "yes" | "1" | "t" => Some(true),
            "off" | "false" | "no" | "0" | "f" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

fn usize_executor_guc(gucs: &HashMap<String, String>, name: &str, default: usize) -> usize {
    gucs.get(name)
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(default)
}

fn f64_executor_guc(gucs: &HashMap<String, String>, name: &str, default: f64) -> f64 {
    gucs.get(name)
        .and_then(|value| value.trim().parse::<f64>().ok())
        .filter(|value| value.is_finite())
        .unwrap_or(default)
}

fn size_executor_guc_bytes(gucs: &HashMap<String, String>, name: &str, default: usize) -> usize {
    gucs.get(name)
        .and_then(|value| parse_executor_size_bytes(value))
        .unwrap_or(default)
}

fn parse_executor_size_bytes(raw: &str) -> Option<usize> {
    let trimmed = raw.trim().trim_matches('\'').trim();
    let mut digits = String::new();
    let mut unit = String::new();
    for ch in trimmed.chars() {
        if ch.is_ascii_digit() || ch == '.' {
            digits.push(ch);
        } else if !ch.is_whitespace() {
            unit.push(ch);
        }
    }
    let value = digits.parse::<f64>().ok()?;
    let multiplier = match unit.to_ascii_lowercase().as_str() {
        "" | "b" => 1.0,
        "kb" | "k" => 1024.0,
        "mb" | "m" => 1024.0 * 1024.0,
        "gb" | "g" => 1024.0 * 1024.0 * 1024.0,
        _ => return None,
    };
    value
        .is_finite()
        .then(|| (value * multiplier).ceil() as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planner_config_parses_boolean_numeric_and_size_gucs() {
        let gucs = HashMap::from([
            ("enable_seqscan".into(), "off".into()),
            ("max_parallel_workers".into(), "12".into()),
            ("parallel_setup_cost".into(), "2.5".into()),
            ("min_parallel_table_scan_size".into(), "1.5MB".into()),
            ("constraint_exclusion".into(), "partition".into()),
        ]);

        let config = planner_config_from_executor_gucs(&gucs);

        assert!(!config.enable_seqscan);
        assert_eq!(config.max_parallel_workers, 12);
        assert_eq!(config.parallel_setup_cost, EstimateValue(2.5));
        assert_eq!(config.min_parallel_table_scan_size, 1_572_864);
        assert!(!config.constraint_exclusion_on);
        assert!(config.constraint_exclusion_partition);
    }
}
