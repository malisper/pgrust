use super::hashjoin::HashJoinPhase;
use super::node_hash::eval_hash_key_exprs;
use crate::backend::commands::explain::format_explain_lines;
use crate::backend::executor::exec_expr::eval_expr;
use crate::backend::executor::{ExecError, ExecutorContext};
use crate::include::nodes::datum::Value;
use crate::include::nodes::execnodes::{HashJoinState, PlanNode, SlotKind, TupleSlot};
use crate::include::nodes::primnodes::{Expr, JoinType};

fn eval_bool_expr(
    expr: &Expr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    match eval_expr(expr, slot, ctx)? {
        Value::Bool(true) => Ok(true),
        Value::Bool(false) | Value::Null => Ok(false),
        other => Err(ExecError::NonBoolQual(other)),
    }
}

fn eval_hash_clauses(
    clauses: &[Expr],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    for clause in clauses {
        if !eval_bool_expr(clause, slot, ctx)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn store_virtual_row(slot: &mut TupleSlot, values: Vec<Value>) {
    let nvalid = values.len();
    slot.tts_values = values;
    slot.tts_nvalid = nvalid;
    slot.kind = SlotKind::Virtual;
    slot.decode_offset = 0;
}

fn combine_slots(left: &TupleSlot, right: &[Value]) -> Vec<Value> {
    let mut combined = left.tts_values.clone();
    combined.extend(right.iter().cloned());
    combined
}

impl PlanNode for HashJoinState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        loop {
            match self.phase {
                HashJoinPhase::BuildHashTable => {
                    self.right.build_if_needed(ctx)?;
                    self.phase = HashJoinPhase::NeedNewOuter;
                }
                HashJoinPhase::NeedNewOuter => match self.left.exec_proc_node(ctx)? {
                    Some(slot) => {
                        let mut values = slot.values()?.iter().cloned().collect::<Vec<_>>();
                        Value::materialize_all(&mut values);
                        self.current_outer = Some(TupleSlot::virtual_row(values));
                        self.current_bucket_entries.clear();
                        self.current_bucket_index = 0;
                        self.matched_outer = false;

                        let current_outer = self
                            .current_outer
                            .as_mut()
                            .expect("current outer tuple must be materialized");
                        if let Some(key) = eval_hash_key_exprs(&self.hash_keys, current_outer, ctx)?
                        {
                            self.current_bucket_entries = self
                                .right
                                .table
                                .as_ref()
                                .expect("hash table must be built before probing")
                                .buckets
                                .get(&key)
                                .cloned()
                                .unwrap_or_default();
                            self.phase = HashJoinPhase::ScanBucket;
                        } else {
                            self.phase = HashJoinPhase::FillOuterTuple;
                        }
                    }
                    None => {
                        self.phase = if matches!(self.kind, JoinType::Right | JoinType::Full) {
                            HashJoinPhase::FillInnerTuples
                        } else {
                            HashJoinPhase::Done
                        };
                    }
                },
                HashJoinPhase::ScanBucket => {
                    if self.current_bucket_index >= self.current_bucket_entries.len() {
                        self.phase = HashJoinPhase::FillOuterTuple;
                        continue;
                    }

                    let entry_index = self.current_bucket_entries[self.current_bucket_index];
                    self.current_bucket_index += 1;

                    let right_values = self
                        .right
                        .table
                        .as_ref()
                        .expect("hash table must be built before probing")
                        .entries[entry_index]
                        .slot
                        .tts_values
                        .clone();
                    let outer = self
                        .current_outer
                        .as_ref()
                        .expect("current outer tuple must exist while scanning a bucket");
                    store_virtual_row(&mut self.slot, combine_slots(outer, &right_values));

                    if !eval_hash_clauses(&self.hash_clauses, &mut self.slot, ctx)? {
                        continue;
                    }
                    if let Some(expr) = &self.join_qual {
                        if !eval_bool_expr(expr, &mut self.slot, ctx)? {
                            continue;
                        }
                    }

                    self.matched_outer = true;
                    self.right
                        .table
                        .as_mut()
                        .expect("hash table must be built before probing")
                        .entries[entry_index]
                        .matched = true;
                    self.stats.rows += 1;
                    return Ok(Some(&mut self.slot));
                }
                HashJoinPhase::FillOuterTuple => {
                    self.phase = HashJoinPhase::NeedNewOuter;
                    if !self.matched_outer && matches!(self.kind, JoinType::Left | JoinType::Full) {
                        let outer = self
                            .current_outer
                            .take()
                            .expect("current outer tuple must exist for outer fill");
                        let mut values = outer.tts_values;
                        values.extend(std::iter::repeat_n(Value::Null, self.right_width));
                        store_virtual_row(&mut self.slot, values);
                        self.stats.rows += 1;
                        return Ok(Some(&mut self.slot));
                    }
                    self.current_outer = None;
                }
                HashJoinPhase::FillInnerTuples => {
                    let table = self
                        .right
                        .table
                        .as_mut()
                        .expect("hash table must be built before unmatched scan");
                    while self.unmatched_inner_index < table.entries.len() {
                        let entry_index = self.unmatched_inner_index;
                        self.unmatched_inner_index += 1;
                        if table.entries[entry_index].matched {
                            continue;
                        }

                        let mut values = vec![Value::Null; self.left_width];
                        values.extend(table.entries[entry_index].slot.tts_values.iter().cloned());
                        store_virtual_row(&mut self.slot, values);
                        self.stats.rows += 1;
                        return Ok(Some(&mut self.slot));
                    }
                    self.phase = HashJoinPhase::Done;
                }
                HashJoinPhase::Done => {
                    self.stats.loops += 1;
                    return Ok(None);
                }
            }
        }
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }

    fn column_names(&self) -> &[String] {
        &self.combined_names
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
        match self.kind {
            JoinType::Inner => "Hash Join".into(),
            JoinType::Left => "Hash Left Join".into(),
            JoinType::Right => "Hash Right Join".into(),
            JoinType::Full => "Hash Full Join".into(),
            JoinType::Cross => "Hash Join".into(),
        }
    }

    fn explain_children(&self, indent: usize, analyze: bool, lines: &mut Vec<String>) {
        format_explain_lines(&*self.left, indent, analyze, lines);
        format_explain_lines(self.right.as_ref(), indent, analyze, lines);
    }
}
