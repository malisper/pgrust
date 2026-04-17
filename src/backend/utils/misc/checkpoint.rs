use crate::backend::utils::time::datetime::current_postgres_timestamp_usecs;
use crate::include::nodes::datetime::TimestampTzADT;
use crate::include::nodes::datum::Value;
use crate::include::nodes::primnodes::BuiltinScalarFunction;
use std::fs;
use std::path::Path;
use std::time::Duration;

use crate::backend::utils::misc::guc::normalize_guc_name;

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
    pub fn load_from_data_dir(base_dir: &Path) -> Result<Self, String> {
        let mut config = Self::default();
        apply_checkpoint_config_file(&mut config, &base_dir.join("postgresql.conf"))?;
        apply_checkpoint_config_file(&mut config, &base_dir.join("postgresql.auto.conf"))?;
        Ok(config)
    }

    pub fn value_for_show(&self, name: &str) -> Option<String> {
        match name {
            "checkpoint_timeout" => Some(format_duration(self.checkpoint_timeout)),
            "checkpoint_completion_target" => Some(self.checkpoint_completion_target.to_string()),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointCompletionKind {
    Timed,
    Requested,
    EndOfRecovery,
    Shutdown,
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
    pub fn record_completed_checkpoint(
        &mut self,
        kind: CheckpointCompletionKind,
        write_time: Duration,
        sync_time: Duration,
        buffers_written: u64,
        slru_written: u64,
    ) {
        match kind {
            CheckpointCompletionKind::Timed => {
                self.num_timed = self.num_timed.saturating_add(1);
            }
            CheckpointCompletionKind::Requested => {
                self.num_requested = self.num_requested.saturating_add(1);
            }
            CheckpointCompletionKind::EndOfRecovery | CheckpointCompletionKind::Shutdown => {}
        }
        self.num_done = self.num_done.saturating_add(1);
        self.write_time_ms += write_time.as_secs_f64() * 1000.0;
        self.sync_time_ms += sync_time.as_secs_f64() * 1000.0;
        self.buffers_written = self.buffers_written.saturating_add(buffers_written);
        self.slru_written = self.slru_written.saturating_add(slru_written);
    }

    pub fn record_manual_checkpoint(&mut self) {
        self.record_completed_checkpoint(
            CheckpointCompletionKind::Requested,
            Duration::ZERO,
            Duration::ZERO,
            0,
            0,
        );
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

fn apply_checkpoint_config_file(config: &mut CheckpointConfig, path: &Path) -> Result<(), String> {
    if !path.exists() {
        return Ok(());
    }
    let text = fs::read_to_string(path)
        .map_err(|err| format!("failed to read {}: {err}", path.display()))?;
    for (index, raw_line) in text.lines().enumerate() {
        let line = strip_config_comment(raw_line).trim();
        if line.is_empty() {
            continue;
        }
        let Some((name, value)) = line.split_once('=') else {
            continue;
        };
        let name = normalize_guc_name(name);
        if !is_checkpoint_guc(&name) {
            continue;
        }
        let value = unquote_config_value(value.trim());
        apply_checkpoint_setting(config, &name, value)
            .map_err(|message| format!("{}:{}: {message}", path.display(), index + 1))?;
    }
    Ok(())
}

fn strip_config_comment(line: &str) -> &str {
    let mut in_single = false;
    let mut in_double = false;
    for (idx, ch) in line.char_indices() {
        match ch {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            '#' if !in_single && !in_double => return &line[..idx],
            _ => {}
        }
    }
    line
}

fn unquote_config_value(value: &str) -> &str {
    let bytes = value.as_bytes();
    if bytes.len() >= 2
        && ((bytes[0] == b'\'' && bytes[bytes.len() - 1] == b'\'')
            || (bytes[0] == b'"' && bytes[bytes.len() - 1] == b'"'))
    {
        &value[1..value.len() - 1]
    } else {
        value
    }
}

fn apply_checkpoint_setting(
    config: &mut CheckpointConfig,
    name: &str,
    value: &str,
) -> Result<(), String> {
    match name {
        "checkpoint_timeout" => {
            config.checkpoint_timeout = parse_duration_setting(value)?;
        }
        "checkpoint_completion_target" => {
            config.checkpoint_completion_target = value
                .trim()
                .parse::<f64>()
                .map_err(|_| format!("invalid value for {name}: {value}"))?;
        }
        "checkpoint_warning" => {
            config.checkpoint_warning = parse_duration_setting(value)?;
        }
        "max_wal_size" => {
            config.max_wal_size_kb = parse_size_kb(value)?;
        }
        "min_wal_size" => {
            config.min_wal_size_kb = parse_size_kb(value)?;
        }
        "fsync" => {
            config.fsync = parse_bool_setting(value)?;
        }
        "full_page_writes" => {
            config.full_page_writes = parse_bool_setting(value)?;
        }
        _ => {}
    }
    Ok(())
}

fn parse_bool_setting(value: &str) -> Result<bool, String> {
    match normalize_guc_name(value).as_str() {
        "on" | "true" | "yes" | "1" => Ok(true),
        "off" | "false" | "no" | "0" => Ok(false),
        _ => Err(format!("invalid boolean value: {value}")),
    }
}

fn parse_duration_setting(value: &str) -> Result<Duration, String> {
    let normalized = normalize_guc_name(value);
    let (amount, unit) = split_numeric_suffix(&normalized)?;
    let amount = amount
        .parse::<u64>()
        .map_err(|_| format!("invalid duration value: {value}"))?;
    let seconds = match unit {
        "" | "s" => amount,
        "ms" => 0,
        "min" => amount.saturating_mul(60),
        "h" => amount.saturating_mul(60 * 60),
        "d" => amount.saturating_mul(60 * 60 * 24),
        _ => return Err(format!("invalid duration unit in {value}")),
    };
    if unit == "ms" {
        Ok(Duration::from_millis(amount))
    } else {
        Ok(Duration::from_secs(seconds))
    }
}

fn parse_size_kb(value: &str) -> Result<u64, String> {
    let normalized = normalize_guc_name(value);
    let (amount, unit) = split_numeric_suffix(&normalized)?;
    let amount = amount
        .parse::<u64>()
        .map_err(|_| format!("invalid size value: {value}"))?;
    match unit {
        "" | "kb" => Ok(amount),
        "mb" => Ok(amount.saturating_mul(KB_PER_MB)),
        "gb" => Ok(amount.saturating_mul(KB_PER_GB)),
        _ => Err(format!("invalid size unit in {value}")),
    }
}

fn split_numeric_suffix(value: &str) -> Result<(&str, &str), String> {
    let idx = value
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(value.len());
    if idx == 0 {
        return Err(format!("missing numeric value: {value}"));
    }
    Ok((&value[..idx], value[idx..].trim()))
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
        assert_eq!(
            config.value_for_show("checkpoint_timeout").as_deref(),
            Some("5min")
        );
        assert_eq!(
            config
                .value_for_show("checkpoint_completion_target")
                .as_deref(),
            Some("0.9")
        );
        assert_eq!(
            config.value_for_show("checkpoint_warning").as_deref(),
            Some("30s")
        );
        assert_eq!(
            config.value_for_show("max_wal_size").as_deref(),
            Some("1GB")
        );
        assert_eq!(
            config.value_for_show("min_wal_size").as_deref(),
            Some("80MB")
        );
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

    #[test]
    fn end_of_recovery_checkpoint_only_counts_as_done() {
        let mut stats = CheckpointStatsSnapshot::default();
        stats.record_completed_checkpoint(
            CheckpointCompletionKind::EndOfRecovery,
            Duration::ZERO,
            Duration::ZERO,
            0,
            0,
        );
        assert_eq!(stats.num_requested, 0);
        assert_eq!(stats.num_timed, 0);
        assert_eq!(stats.num_done, 1);
    }
}
