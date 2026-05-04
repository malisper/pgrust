use super::hashjoin::{HashInstrumentation, HashJoinTable, HashJoinTupleEntry, HashKey};
use crate::backend::commands::explain::format_explain_lines_with_costs;
use crate::backend::executor::exec_expr::eval_expr;
use crate::backend::executor::{ExecError, ExecutorContext};
use crate::include::nodes::datum::Value;
use crate::include::nodes::execnodes::{
    HashState, MaterializedRow, PlanNode, SystemVarBinding, TupleSlot,
};
use pgrust_executor::{
    HashMemoryConfig, canonical_hash_key_value, hash_instrumentation_from_row_bytes,
    hash_value_memory, parse_guc_bool, parse_guc_usize, parse_hash_mem_multiplier_millis,
    parse_memory_kb,
};

fn hash_memory_config(ctx: &ExecutorContext) -> HashMemoryConfig {
    HashMemoryConfig {
        work_mem_kb: ctx
            .gucs
            .get("work_mem")
            .and_then(|value| parse_memory_kb(value))
            .unwrap_or(4096),
        hash_mem_multiplier_millis: ctx
            .gucs
            .get("hash_mem_multiplier")
            .and_then(|value| parse_hash_mem_multiplier_millis(value))
            .unwrap_or(2000),
        max_parallel_workers_per_gather: ctx
            .gucs
            .get("max_parallel_workers_per_gather")
            .and_then(|value| parse_guc_usize(value))
            .unwrap_or(0),
        enable_parallel_hash: ctx
            .gucs
            .get("enable_parallel_hash")
            .and_then(|value| parse_guc_bool(value))
            .unwrap_or(true),
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

fn hash_instrumentation(
    table: &HashJoinTable,
    plan_rows: f64,
    ctx: &ExecutorContext,
) -> HashInstrumentation {
    hash_instrumentation_from_row_bytes(table, plan_rows, hash_memory_config(ctx), hash_row_memory)
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
