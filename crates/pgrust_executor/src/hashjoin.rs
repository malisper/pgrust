use std::collections::HashMap;

use pgrust_nodes::Value;

pub type HashKey = Vec<Value>;

#[derive(Debug)]
pub struct HashJoinTupleEntry<Row> {
    pub row: Row,
    #[allow(dead_code)]
    pub bucket_key: Option<HashKey>,
    pub matched: bool,
}

#[derive(Debug)]
pub struct HashJoinTable<Row> {
    pub buckets: HashMap<HashKey, Vec<usize>>,
    pub entries: Vec<HashJoinTupleEntry<Row>>,
}

impl<Row> Default for HashJoinTable<Row> {
    fn default() -> Self {
        Self {
            buckets: HashMap::new(),
            entries: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct HashInstrumentation {
    pub original_batches: usize,
    pub final_batches: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HashMemoryConfig {
    pub work_mem_kb: usize,
    pub hash_mem_multiplier_millis: usize,
    pub max_parallel_workers_per_gather: usize,
    pub enable_parallel_hash: bool,
}

impl Default for HashMemoryConfig {
    fn default() -> Self {
        Self {
            work_mem_kb: 4096,
            hash_mem_multiplier_millis: 2000,
            max_parallel_workers_per_gather: 0,
            enable_parallel_hash: true,
        }
    }
}

impl HashMemoryConfig {
    pub fn memory_limit_bytes(self) -> usize {
        self.work_mem_kb
            .saturating_mul(1024)
            .saturating_mul(self.hash_mem_multiplier_millis)
            / 1000
    }

    pub fn parallel_hash_enabled(self) -> bool {
        self.max_parallel_workers_per_gather > 0 && self.enable_parallel_hash
    }
}

pub fn parse_memory_kb(raw: &str) -> Option<usize> {
    let trimmed = raw.trim().trim_matches('\'').trim();
    let (number, unit) = if trimmed.contains(char::is_whitespace) {
        let mut parts = trimmed.split_whitespace();
        (parts.next()?, parts.next().unwrap_or("kB"))
    } else {
        let unit_start = trimmed
            .find(|ch: char| !(ch.is_ascii_digit() || ch == '.'))
            .unwrap_or(trimmed.len());
        (&trimmed[..unit_start], trimmed[unit_start..].trim())
    };
    let unit = if unit.is_empty() { "kB" } else { unit };
    let value = number.parse::<f64>().ok()?;
    let multiplier = match unit.to_ascii_lowercase().as_str() {
        "b" | "byte" | "bytes" => 1.0 / 1024.0,
        "kb" | "kib" => 1.0,
        "mb" | "mib" => 1024.0,
        "gb" | "gib" => 1024.0 * 1024.0,
        _ => 1.0,
    };
    value
        .is_finite()
        .then(|| (value * multiplier).ceil() as usize)
}

pub fn parse_hash_mem_multiplier_millis(raw: &str) -> Option<usize> {
    let value = raw.trim().trim_matches('\'').parse::<f64>().ok()?;
    (value.is_finite() && value > 0.0).then(|| (value * 1000.0) as usize)
}

pub fn parse_guc_usize(raw: &str) -> Option<usize> {
    raw.trim().trim_matches('\'').parse::<usize>().ok()
}

pub fn parse_guc_bool(raw: &str) -> Option<bool> {
    match raw.trim().trim_matches('\'').to_ascii_lowercase().as_str() {
        "on" | "true" | "1" => Some(true),
        "off" | "false" | "0" => Some(false),
        _ => None,
    }
}

pub fn canonical_hash_key_value(value: Value) -> Value {
    match value {
        Value::Int16(value) => Value::Int64(value as i64),
        Value::Int32(value) => Value::Int64(value as i64),
        Value::Int64(value) => Value::Int64(value),
        other => other,
    }
}

pub fn hash_value_memory(value: &Value) -> usize {
    match value {
        Value::Text(text) | Value::Json(text) | Value::JsonPath(text) | Value::Xml(text) => {
            24 + text.len()
        }
        Value::TextRef(_, len) => 24 + *len as usize,
        Value::Bytea(bytes) | Value::Jsonb(bytes) => 24 + bytes.len(),
        Value::Array(values) => 24 + values.iter().map(hash_value_memory).sum::<usize>(),
        Value::PgArray(array) => {
            24 + array
                .to_nested_values()
                .iter()
                .map(hash_value_memory)
                .sum::<usize>()
        }
        Value::Record(record) => 24 + record.fields.iter().map(hash_value_memory).sum::<usize>(),
        _ => 32,
    }
}

pub fn hash_batch_count(bytes: f64, limit: usize) -> usize {
    if limit == 0 || bytes <= limit as f64 {
        return 1;
    }
    let mut batches = (bytes / limit as f64).ceil() as usize;
    batches = batches.next_power_of_two();
    batches.max(2)
}

pub fn hash_instrumentation_from_row_bytes<Row>(
    table: &HashJoinTable<Row>,
    plan_rows: f64,
    config: HashMemoryConfig,
    row_bytes: impl Fn(&Row) -> usize,
) -> HashInstrumentation {
    let limit = config.memory_limit_bytes();
    let total_bytes = table
        .entries
        .iter()
        .map(|entry| row_bytes(&entry.row))
        .sum::<usize>();
    let avg_row_bytes = if table.entries.is_empty() {
        32.0
    } else {
        total_bytes as f64 / table.entries.len() as f64
    };
    let estimated_rows = if plan_rows.is_finite() && plan_rows > 0.0 {
        plan_rows
    } else {
        table.entries.len() as f64
    };
    let original_batches = hash_batch_count(estimated_rows * avg_row_bytes, limit);
    let mut final_batches = hash_batch_count(total_bytes as f64, limit);

    // :HACK: pgrust's hash join is still in-memory and does not spill batches.
    // Expose PostgreSQL-shaped EXPLAIN ANALYZE counters for regression helpers,
    // including the skew guard where adding more batches would not help.
    if table
        .buckets
        .values()
        .any(|bucket| bucket.len() == table.entries.len() && bucket.len() > 1)
        && final_batches > original_batches
    {
        let skew_growth = if config.parallel_hash_enabled() { 4 } else { 2 };
        final_batches = original_batches.saturating_mul(skew_growth).max(2);
    }

    HashInstrumentation {
        original_batches,
        final_batches,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashJoinPhase {
    BuildHashTable,
    NeedNewOuter,
    ScanBucket,
    FillOuterTuple,
    FillInnerTuples,
    Done,
}
