use super::hashjoin::{HashJoinTable, HashJoinTupleEntry, HashKey};
use crate::backend::commands::explain::format_explain_lines;
use crate::backend::executor::exec_expr::eval_expr;
use crate::backend::executor::{ExecError, ExecutorContext};
use crate::include::nodes::datum::Value;
use crate::include::nodes::execnodes::{HashState, PlanNode, TupleSlot};

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
        key.push(value);
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
            let mut values = slot.values()?.iter().cloned().collect::<Vec<_>>();
            Value::materialize_all(&mut values);
            let mut materialized = TupleSlot::virtual_row(values);
            let bucket_key = eval_hash_key_exprs(&self.hash_keys, &mut materialized, ctx)?;
            let index = table.entries.len();
            table.entries.push(HashJoinTupleEntry {
                slot: materialized,
                bucket_key: bucket_key.clone(),
                matched: false,
            });
            if let Some(key) = bucket_key {
                table.buckets.entry(key).or_default().push(index);
            }
        }

        self.stats.loops += 1;
        self.stats.rows = table.entries.len() as u64;
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

    fn explain_children(&self, indent: usize, analyze: bool, lines: &mut Vec<String>) {
        format_explain_lines(&*self.input, indent + 1, analyze, lines);
    }
}
