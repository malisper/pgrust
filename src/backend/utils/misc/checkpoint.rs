use crate::backend::utils::time::datetime::current_postgres_timestamp_usecs;
use crate::include::nodes::datum::Value;
use crate::include::nodes::datetime::TimestampTzADT;
use crate::include::nodes::primnodes::BuiltinScalarFunction;
use std::time::Duration;

const KB_PER_MB: u64 = 1024;
const KB_PER_GB: u64 = 1024 * 1024;

#[derive(Debug, Clone, PartialEq)]
pub struct CheckpointConfig {
    pub checkpoint_timeout: Duration,
    pub checkpoint_completion_target: f64,
    pub checkpoint_warning: Duration,
    pub max_wal_size_kb: u64,
    pub min_wal_size_kb: u64,
    pub fsync: bool,
    pub full_page_writes: bool,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            checkpoint_timeout: Duration::from_secs(300),
            checkpoint_completion_target: 0.9,
            checkpoint_warning: Duration::from_secs(30),
            max_wal_size_kb: KB_PER_GB,
            min_wal_size_kb: 80 * KB_PER_MB,
            fsync: true,
            full_page_writes: true,
        }
    }
}

impl CheckpointConfig {
    pub fn value_for_show(&self, name: &str) -> Option<String> {
        match name {
            "checkpoint_timeout" => Some(format_duration(self.checkpoint_timeout)),
            "checkpoint_completion_target" => {
                Some(self.checkpoint_completion_target.to_string())
            }
            "checkpoint_warning" => Some(format_duration(self.checkpoint_warning)),
            "max_wal_size" => Some(format_wal_size_kb(self.max_wal_size_kb)),
            "min_wal_size" => Some(format_wal_size_kb(self.min_wal_size_kb)),
            "fsync" => Some(format_bool(self.fsync)),
            "full_page_writes" => Some(format_bool(self.full_page_writes)),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CheckpointStatsSnapshot {
    pub num_timed: u64,
    pub num_requested: u64,
    pub num_done: u64,
    pub write_time_ms: f64,
    pub sync_time_ms: f64,
    pub buffers_written: u64,
    pub slru_written: u64,
    pub stats_reset: TimestampTzADT,
}

impl Default for CheckpointStatsSnapshot {
    fn default() -> Self {
        Self {
            num_timed: 0,
            num_requested: 0,
            num_done: 0,
            write_time_ms: 0.0,
            sync_time_ms: 0.0,
            buffers_written: 0,
            slru_written: 0,
            stats_reset: TimestampTzADT(current_postgres_timestamp_usecs()),
        }
    }
}

impl CheckpointStatsSnapshot {
    pub fn record_manual_checkpoint(&mut self) {
        self.num_requested = self.num_requested.saturating_add(1);
        self.num_done = self.num_done.saturating_add(1);
    }
}

pub fn is_checkpoint_guc(name: &str) -> bool {
    matches!(
        name,
        "checkpoint_timeout"
            | "checkpoint_completion_target"
            | "checkpoint_warning"
            | "max_wal_size"
            | "min_wal_size"
            | "fsync"
            | "full_page_writes"
    )
}

pub fn checkpoint_stats_value(
    func: BuiltinScalarFunction,
    stats: &CheckpointStatsSnapshot,
) -> Option<Value> {
    match func {
        BuiltinScalarFunction::PgStatGetCheckpointerNumTimed => {
            Some(Value::Int64(stats.num_timed as i64))
        }
        BuiltinScalarFunction::PgStatGetCheckpointerNumRequested => {
            Some(Value::Int64(stats.num_requested as i64))
        }
        BuiltinScalarFunction::PgStatGetCheckpointerNumPerformed => {
            Some(Value::Int64(stats.num_done as i64))
        }
        BuiltinScalarFunction::PgStatGetCheckpointerBuffersWritten => {
            Some(Value::Int64(stats.buffers_written as i64))
        }
        BuiltinScalarFunction::PgStatGetCheckpointerSlruWritten => {
            Some(Value::Int64(stats.slru_written as i64))
        }
        BuiltinScalarFunction::PgStatGetCheckpointerWriteTime => {
            Some(Value::Float64(stats.write_time_ms))
        }
        BuiltinScalarFunction::PgStatGetCheckpointerSyncTime => {
            Some(Value::Float64(stats.sync_time_ms))
        }
        BuiltinScalarFunction::PgStatGetCheckpointerStatResetTime => {
            Some(Value::TimestampTz(stats.stats_reset))
        }
        _ => None,
    }
}

pub fn default_checkpoint_stats_value(func: BuiltinScalarFunction) -> Option<Value> {
    checkpoint_stats_value(func, &CheckpointStatsSnapshot::default())
}

fn format_bool(value: bool) -> String {
    if value { "on" } else { "off" }.to_string()
}

fn format_duration(value: Duration) -> String {
    let seconds = value.as_secs();
    if seconds % 60 == 0 && seconds >= 60 {
        format!("{}min", seconds / 60)
    } else {
        format!("{seconds}s")
    }
}

fn format_wal_size_kb(value: u64) -> String {
    if value % KB_PER_GB == 0 && value >= KB_PER_GB {
        format!("{}GB", value / KB_PER_GB)
    } else if value % KB_PER_MB == 0 && value >= KB_PER_MB {
        format!("{}MB", value / KB_PER_MB)
    } else {
        format!("{value}kB")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_show_defaults_match_postgres_shape() {
        let config = CheckpointConfig::default();
        assert_eq!(config.value_for_show("checkpoint_timeout").as_deref(), Some("5min"));
        assert_eq!(
            config.value_for_show("checkpoint_completion_target").as_deref(),
            Some("0.9")
        );
        assert_eq!(config.value_for_show("checkpoint_warning").as_deref(), Some("30s"));
        assert_eq!(config.value_for_show("max_wal_size").as_deref(), Some("1GB"));
        assert_eq!(config.value_for_show("min_wal_size").as_deref(), Some("80MB"));
        assert_eq!(config.value_for_show("fsync").as_deref(), Some("on"));
        assert_eq!(
            config.value_for_show("full_page_writes").as_deref(),
            Some("on")
        );
    }

    #[test]
    fn manual_checkpoint_updates_requested_counters() {
        let mut stats = CheckpointStatsSnapshot::default();
        stats.record_manual_checkpoint();
        assert_eq!(stats.num_requested, 1);
        assert_eq!(stats.num_done, 1);
    }
}
