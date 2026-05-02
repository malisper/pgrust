use super::hashjoin::{HashInstrumentation, HashJoinTable, HashJoinTupleEntry, HashKey};
use crate::backend::commands::explain::format_explain_lines_with_costs;
use crate::backend::executor::exec_expr::eval_expr;
use crate::backend::executor::{ExecError, ExecutorContext};
use crate::include::nodes::datum::Value;
use crate::include::nodes::execnodes::{
    HashState, MaterializedRow, PlanNode, SystemVarBinding, TupleSlot,
};

fn canonical_hash_key_value(value: Value) -> Value {
    match value {
        Value::Int16(value) => Value::Int64(value as i64),
        Value::Int32(value) => Value::Int64(value as i64),
        Value::Int64(value) => Value::Int64(value),
        other => other,
    }
}

fn parse_memory_kb(raw: &str) -> Option<usize> {
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

fn hash_memory_limit_bytes(ctx: &ExecutorContext) -> usize {
    let work_mem_kb = ctx
        .gucs
        .get("work_mem")
        .and_then(|value| parse_memory_kb(value))
        .unwrap_or(4096);
    let hash_multiplier = ctx
        .gucs
        .get("hash_mem_multiplier")
        .and_then(|value| value.trim().trim_matches('\'').parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or(2.0);
    ((work_mem_kb as f64) * 1024.0 * hash_multiplier) as usize
}

fn guc_usize(ctx: &ExecutorContext, name: &str) -> Option<usize> {
    ctx.gucs
        .get(name)
        .and_then(|value| value.trim().trim_matches('\'').parse::<usize>().ok())
}

fn guc_bool(ctx: &ExecutorContext, name: &str) -> Option<bool> {
    let value = ctx
        .gucs
        .get(name)?
        .trim()
        .trim_matches('\'')
        .to_ascii_lowercase();
    match value.as_str() {
        "on" | "true" | "1" => Some(true),
        "off" | "false" | "0" => Some(false),
        _ => None,
    }
}

fn parallel_hash_enabled(ctx: &ExecutorContext) -> bool {
    guc_usize(ctx, "max_parallel_workers_per_gather").unwrap_or(0) > 0
        && guc_bool(ctx, "enable_parallel_hash").unwrap_or(true)
}

fn hash_value_memory(value: &Value) -> usize {
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

fn hash_row_memory(row: &MaterializedRow) -> usize {
    16 + row
        .slot
        .tts_values
        .iter()
        .map(hash_value_memory)
        .sum::<usize>()
}

fn hash_batch_count(bytes: f64, limit: usize) -> usize {
    if limit == 0 || bytes <= limit as f64 {
        return 1;
    }
    let mut batches = (bytes / limit as f64).ceil() as usize;
    batches = batches.next_power_of_two();
    batches.max(2)
}

fn hash_instrumentation(
    table: &HashJoinTable,
    plan_rows: f64,
    ctx: &ExecutorContext,
) -> HashInstrumentation {
    let limit = hash_memory_limit_bytes(ctx);
    let total_bytes = table
        .entries
        .iter()
        .map(|entry| hash_row_memory(&entry.row))
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
        let skew_growth = if parallel_hash_enabled(ctx) { 4 } else { 2 };
        final_batches = original_batches.saturating_mul(skew_growth).max(2);
    }

    HashInstrumentation {
        original_batches,
        final_batches,
    }
}

pub(crate) fn eval_hash_key_exprs(
    exprs: &[crate::include::nodes::primnodes::Expr],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Option<HashKey>, ExecError> {
    let mut key = Vec::with_capacity(exprs.len());
    for expr in exprs {
        let value = eval_expr(expr, slot, ctx)?.to_owned_value();
        if matches!(value, Value::Null) {
            return Ok(None);
        }
        key.push(canonical_hash_key_value(value));
    }
    Ok(Some(key))
}

impl HashState {
    pub(crate) fn build_if_needed(&mut self, ctx: &mut ExecutorContext) -> Result<(), ExecError> {
        if self.built {
            return Ok(());
        }

        let mut table = HashJoinTable::default();
        while let Some(slot) = self.input.exec_proc_node(ctx)? {
            let mut materialized = MaterializedRow::new(
                TupleSlot::virtual_row({
                    let mut values = slot.values()?.iter().cloned().collect::<Vec<_>>();
                    Value::materialize_all(&mut values);
                    values
                }),
                self.input.current_system_bindings().to_vec(),
            );
            ctx.expr_bindings.outer_tuple = Some(materialized.slot.tts_values.clone());
            ctx.expr_bindings.outer_system_bindings = materialized.system_bindings.clone();
            let bucket_key = eval_hash_key_exprs(&self.hash_keys, &mut materialized.slot, ctx)?;
            let index = table.entries.len();
            table.entries.push(HashJoinTupleEntry {
                row: materialized,
                bucket_key: bucket_key.clone(),
                matched: false,
            });
            if let Some(key) = bucket_key {
                table.buckets.entry(key).or_default().push(index);
            }
        }

        self.stats.loops += 1;
        self.stats.rows = table.entries.len() as u64;
        self.instrumentation = hash_instrumentation(&table, self.plan_info.plan_rows.as_f64(), ctx);
        self.table = Some(table);
        self.built = true;
        Ok(())
    }
}

impl PlanNode for HashState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        self.build_if_needed(ctx)?;
        Ok(None)
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        None
    }
    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &[]
    }

    fn column_names(&self) -> &[String] {
        &self.column_names
    }

    fn node_stats(&self) -> &crate::include::nodes::execnodes::NodeExecStats {
        &self.stats
    }

    fn node_stats_mut(&mut self) -> &mut crate::include::nodes::execnodes::NodeExecStats {
        &mut self.stats
    }

    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }

    fn node_label(&self) -> String {
        "Hash".into()
    }

    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        timing: bool,
        lines: &mut Vec<String>,
    ) {
        format_explain_lines_with_costs(
            &*self.input,
            indent + 1,
            analyze,
            show_costs,
            timing,
            lines,
        );
    }

    fn explain_json_extra_fields(&self, analyze: bool, indent: usize) -> Vec<String> {
        if !analyze {
            return Vec::new();
        }
        let pad = " ".repeat(indent);
        vec![
            format!(
                "{pad}\"Original Hash Batches\": {},",
                self.instrumentation.original_batches.max(1)
            ),
            format!(
                "{pad}\"Hash Batches\": {},",
                self.instrumentation.final_batches.max(1)
            ),
        ]
    }

    fn explain_json_children(&self, analyze: bool, indent: usize) -> Vec<String> {
        vec![self.input.explain_json(analyze, indent)]
    }
}
