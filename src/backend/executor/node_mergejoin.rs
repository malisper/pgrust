use std::cmp::Ordering;

use super::mergejoin::{MergeJoinBufferedRow, MergeKey};
use crate::backend::commands::explain::format_explain_lines_with_costs;
use crate::backend::executor::exec_expr::eval_expr;
use crate::backend::executor::expr_ops::compare_order_values;
use crate::backend::executor::nodes::{render_explain_expr, render_explain_join_expr};
use crate::backend::executor::{ExecError, ExecutorContext};
use crate::include::nodes::datum::Value;
use crate::include::nodes::execnodes::{
    MaterializedRow, MergeJoinState, NodeExecStats, PlanNode, SystemVarBinding, TupleSlot,
};
use crate::include::nodes::plannodes::PlanEstimate;
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

fn eval_qual_list(
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

fn format_qual_list(quals: &[Expr]) -> Expr {
    let mut quals = quals.to_vec();
    let first = quals.remove(0);
    quals
        .into_iter()
        .fold(first, |acc, qual| Expr::and(acc, qual))
}

fn set_active_system_bindings(ctx: &mut ExecutorContext, bindings: &[SystemVarBinding]) {
    ctx.system_bindings.clear();
    ctx.system_bindings.extend_from_slice(bindings);
}

fn set_outer_expr_bindings(
    ctx: &mut ExecutorContext,
    values: Vec<Value>,
    bindings: &[SystemVarBinding],
) {
    ctx.expr_bindings.outer_tuple = Some(values);
    ctx.expr_bindings.outer_system_bindings = bindings.to_vec();
}

fn set_inner_expr_bindings(
    ctx: &mut ExecutorContext,
    values: Vec<Value>,
    bindings: &[SystemVarBinding],
) {
    ctx.expr_bindings.inner_tuple = Some(values);
    ctx.expr_bindings.inner_system_bindings = bindings.to_vec();
}

fn merge_system_bindings(
    left: &[SystemVarBinding],
    right: &[SystemVarBinding],
) -> Vec<SystemVarBinding> {
    let mut merged = left.to_vec();
    for binding in right {
        if !merged
            .iter()
            .any(|existing| existing.varno == binding.varno)
        {
            merged.push(*binding);
        }
    }
    merged
}

fn eval_merge_key_exprs(
    exprs: &[Expr],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<(MergeKey, bool), ExecError> {
    let mut key = Vec::with_capacity(exprs.len());
    let mut matchable = true;
    for expr in exprs {
        let value = eval_expr(expr, slot, ctx)?.to_owned_value();
        if matches!(value, Value::Null) {
            matchable = false;
        }
        key.push(value);
    }
    Ok((key, matchable))
}

fn merge_clause_collation(clause: &Expr) -> Option<u32> {
    match clause {
        Expr::Op(op) => op.collation_oid,
        _ => None,
    }
}

fn compare_merge_keys(
    clauses: &[Expr],
    descending: &[bool],
    left: &[Value],
    right: &[Value],
) -> Result<Ordering, ExecError> {
    for (index, ((left_value, right_value), clause)) in left
        .iter()
        .zip(right.iter())
        .zip(clauses.iter())
        .enumerate()
    {
        let descending = descending.get(index).copied().unwrap_or(false);
        let mut ordering = compare_order_values(
            left_value,
            right_value,
            merge_clause_collation(clause),
            Some(false),
            false,
        )?;
        if descending {
            ordering = ordering.reverse();
        }
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(Ordering::Equal)
}

fn materialize_keyed_rows(
    input: &mut crate::include::nodes::execnodes::PlanState,
    key_exprs: &[Expr],
    ctx: &mut ExecutorContext,
) -> Result<Vec<MergeJoinBufferedRow>, ExecError> {
    let mut rows = Vec::new();
    while input.exec_proc_node(ctx)?.is_some() {
        ctx.check_for_interrupts()?;
        let mut row = input.materialize_current_row()?;
        set_active_system_bindings(ctx, &row.system_bindings);
        set_outer_expr_bindings(ctx, row.slot.tts_values.clone(), &row.system_bindings);
        let (key, matchable) = eval_merge_key_exprs(key_exprs, &mut row.slot, ctx)?;
        rows.push(MergeJoinBufferedRow {
            row,
            key,
            matchable,
            matched: false,
        });
    }
    Ok(rows)
}

fn same_merge_key(clauses: &[Expr], left: &MergeKey, right: &MergeKey) -> Result<bool, ExecError> {
    Ok(compare_merge_keys(clauses, &[], left, right)? == Ordering::Equal)
}

fn group_end(
    rows: &[MergeJoinBufferedRow],
    start: usize,
    clauses: &[Expr],
) -> Result<usize, ExecError> {
    let first_key = &rows[start].key;
    let mut end = start + 1;
    while end < rows.len() {
        let next_key = &rows[end].key;
        if !same_merge_key(clauses, first_key, next_key)? {
            break;
        }
        end += 1;
    }
    Ok(end)
}

fn combined_values(left: &MaterializedRow, right: &MaterializedRow) -> Vec<Value> {
    let mut values = left.slot.tts_values.clone();
    values.extend(right.slot.tts_values.iter().cloned());
    values
}

fn output_left_only(left: &MaterializedRow) -> MaterializedRow {
    MaterializedRow::new(
        TupleSlot::virtual_row(left.slot.tts_values.clone()),
        left.system_bindings.clone(),
    )
}

fn output_null_extended_left(left: &MaterializedRow, right_width: usize) -> MaterializedRow {
    let mut values = left.slot.tts_values.clone();
    values.extend(std::iter::repeat_n(Value::Null, right_width));
    MaterializedRow::new(TupleSlot::virtual_row(values), left.system_bindings.clone())
}

fn output_null_extended_right(right: &MaterializedRow, left_width: usize) -> MaterializedRow {
    let mut values = vec![Value::Null; left_width];
    values.extend(right.slot.tts_values.iter().cloned());
    MaterializedRow::new(
        TupleSlot::virtual_row(values),
        right.system_bindings.clone(),
    )
}

fn emit_unmatched_left(
    kind: JoinType,
    left: &MaterializedRow,
    right_width: usize,
    outputs: &mut Vec<MaterializedRow>,
) {
    match kind {
        JoinType::Left | JoinType::Full => {
            outputs.push(output_null_extended_left(left, right_width))
        }
        JoinType::Anti => outputs.push(output_left_only(left)),
        JoinType::Inner | JoinType::Right | JoinType::Semi | JoinType::Cross => {}
    }
}

fn emit_unmatched_right(
    kind: JoinType,
    right: &MaterializedRow,
    left_width: usize,
    outputs: &mut Vec<MaterializedRow>,
) {
    if matches!(kind, JoinType::Right | JoinType::Full) {
        outputs.push(output_null_extended_right(right, left_width));
    }
}

fn process_matching_groups(
    state: &mut MergeJoinState,
    left_start: usize,
    left_end: usize,
    right_start: usize,
    right_end: usize,
    ctx: &mut ExecutorContext,
    outputs: &mut Vec<MaterializedRow>,
) -> Result<(), ExecError> {
    let left_rows = state.left_rows.as_mut().expect("left rows built");
    let right_rows = state.right_rows.as_mut().expect("right rows built");

    for left_index in left_start..left_end {
        let mut left_matched = false;
        let mut semi_emitted = false;

        for right_index in right_start..right_end {
            ctx.check_for_interrupts()?;
            let values = combined_values(&left_rows[left_index].row, &right_rows[right_index].row);
            let bindings = merge_system_bindings(
                &left_rows[left_index].row.system_bindings,
                &right_rows[right_index].row.system_bindings,
            );
            let mut slot = TupleSlot::virtual_row(values);
            set_active_system_bindings(ctx, &bindings);
            set_outer_expr_bindings(
                ctx,
                left_rows[left_index].row.slot.tts_values.clone(),
                &left_rows[left_index].row.system_bindings,
            );
            set_inner_expr_bindings(
                ctx,
                right_rows[right_index].row.slot.tts_values.clone(),
                &right_rows[right_index].row.system_bindings,
            );

            if !eval_qual_list(&state.merge_clauses, &mut slot, ctx)? {
                continue;
            }
            if !eval_qual_list(&state.join_qual, &mut slot, ctx)? {
                continue;
            }

            left_matched = true;
            left_rows[left_index].matched = true;
            right_rows[right_index].matched = true;

            if matches!(state.kind, JoinType::Anti) {
                break;
            }

            if !eval_qual_list(&state.qual, &mut slot, ctx)? {
                continue;
            }

            if matches!(state.kind, JoinType::Semi) {
                outputs.push(output_left_only(&left_rows[left_index].row));
                semi_emitted = true;
                break;
            }

            outputs.push(MaterializedRow::new(slot, bindings));
        }

        if !left_matched && !semi_emitted {
            emit_unmatched_left(
                state.kind,
                &left_rows[left_index].row,
                state.right_width,
                outputs,
            );
        }
    }

    Ok(())
}

fn build_outputs(
    state: &mut MergeJoinState,
    ctx: &mut ExecutorContext,
) -> Result<Vec<MaterializedRow>, ExecError> {
    state.left_rows = Some(materialize_keyed_rows(
        &mut state.left,
        &state.outer_merge_keys,
        ctx,
    )?);
    state.right_rows = Some(materialize_keyed_rows(
        &mut state.right,
        &state.inner_merge_keys,
        ctx,
    )?);

    let mut outputs = Vec::new();
    let mut left_index = 0;
    let mut right_index = 0;

    while left_index < state.left_rows.as_ref().unwrap().len()
        || right_index < state.right_rows.as_ref().unwrap().len()
    {
        ctx.check_for_interrupts()?;

        if left_index >= state.left_rows.as_ref().unwrap().len() {
            let right_rows = state.right_rows.as_mut().unwrap();
            let right_row = &right_rows[right_index].row;
            emit_unmatched_right(state.kind, right_row, state.left_width, &mut outputs);
            right_rows[right_index].matched = true;
            right_index += 1;
            continue;
        }
        if right_index >= state.right_rows.as_ref().unwrap().len() {
            let left_row = &state.left_rows.as_ref().unwrap()[left_index].row;
            emit_unmatched_left(state.kind, left_row, state.right_width, &mut outputs);
            left_index += 1;
            continue;
        }

        let left_key = &state.left_rows.as_ref().unwrap()[left_index].key;
        let right_key = &state.right_rows.as_ref().unwrap()[right_index].key;
        let ordering = compare_merge_keys(
            &state.merge_clauses,
            &state.merge_key_descending,
            left_key,
            right_key,
        )?;
        if !state.left_rows.as_ref().unwrap()[left_index].matchable
            || !state.right_rows.as_ref().unwrap()[right_index].matchable
        {
            match ordering {
                Ordering::Less | Ordering::Equal => {
                    let left_end = group_end(
                        state.left_rows.as_ref().unwrap(),
                        left_index,
                        &state.merge_clauses,
                    )?;
                    for index in left_index..left_end {
                        let left_row = &state.left_rows.as_ref().unwrap()[index].row;
                        emit_unmatched_left(state.kind, left_row, state.right_width, &mut outputs);
                    }
                    left_index = left_end;
                }
                Ordering::Greater => {
                    let right_end = group_end(
                        state.right_rows.as_ref().unwrap(),
                        right_index,
                        &state.merge_clauses,
                    )?;
                    for index in right_index..right_end {
                        let right_rows = state.right_rows.as_mut().unwrap();
                        let right_row = &right_rows[index].row;
                        emit_unmatched_right(state.kind, right_row, state.left_width, &mut outputs);
                        right_rows[index].matched = true;
                    }
                    right_index = right_end;
                }
            }
            continue;
        }

        match ordering {
            Ordering::Less => {
                let left_end = group_end(
                    state.left_rows.as_ref().unwrap(),
                    left_index,
                    &state.merge_clauses,
                )?;
                for index in left_index..left_end {
                    let left_row = &state.left_rows.as_ref().unwrap()[index].row;
                    emit_unmatched_left(state.kind, left_row, state.right_width, &mut outputs);
                }
                left_index = left_end;
            }
            Ordering::Greater => {
                let right_end = group_end(
                    state.right_rows.as_ref().unwrap(),
                    right_index,
                    &state.merge_clauses,
                )?;
                for index in right_index..right_end {
                    let right_rows = state.right_rows.as_mut().unwrap();
                    let right_row = &right_rows[index].row;
                    emit_unmatched_right(state.kind, right_row, state.left_width, &mut outputs);
                    right_rows[index].matched = true;
                }
                right_index = right_end;
            }
            Ordering::Equal => {
                let left_end = group_end(
                    state.left_rows.as_ref().unwrap(),
                    left_index,
                    &state.merge_clauses,
                )?;
                let right_end = group_end(
                    state.right_rows.as_ref().unwrap(),
                    right_index,
                    &state.merge_clauses,
                )?;
                process_matching_groups(
                    state,
                    left_index,
                    left_end,
                    right_index,
                    right_end,
                    ctx,
                    &mut outputs,
                )?;
                if matches!(state.kind, JoinType::Right | JoinType::Full) {
                    let right_rows = state.right_rows.as_mut().unwrap();
                    for index in right_index..right_end {
                        if !right_rows[index].matched {
                            emit_unmatched_right(
                                state.kind,
                                &right_rows[index].row,
                                state.left_width,
                                &mut outputs,
                            );
                            right_rows[index].matched = true;
                        }
                    }
                }
                left_index = left_end;
                right_index = right_end;
            }
        }
    }

    if matches!(state.kind, JoinType::Right | JoinType::Full) {
        for right in state.right_rows.as_ref().unwrap() {
            if !right.matched {
                emit_unmatched_right(state.kind, &right.row, state.left_width, &mut outputs);
            }
        }
    }

    Ok(outputs)
}

impl PlanNode for MergeJoinState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        if self.output_rows.is_none() {
            let outputs = build_outputs(self, ctx)?;
            self.output_rows = Some(outputs);
        }

        let rows = self.output_rows.as_mut().expect("merge join outputs built");
        if self.next_output_index >= rows.len() {
            self.stats.loops += 1;
            return Ok(None);
        }

        let index = self.next_output_index;
        self.next_output_index += 1;
        self.current_bindings = rows[index].system_bindings.clone();
        set_active_system_bindings(ctx, &self.current_bindings);
        self.stats.rows += 1;
        Ok(Some(&mut rows[index].slot))
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        let rows = self.output_rows.as_mut()?;
        let index = self.next_output_index.checked_sub(1)?;
        rows.get_mut(index).map(|row| &mut row.slot)
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }

    fn column_names(&self) -> &[String] {
        &self.output_names
    }

    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }

    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }

    fn plan_info(&self) -> PlanEstimate {
        self.plan_info
    }

    fn node_label(&self) -> String {
        match self.kind {
            JoinType::Inner => "Merge Join".into(),
            JoinType::Left => "Merge Left Join".into(),
            JoinType::Right => "Merge Right Join".into(),
            JoinType::Full => "Merge Full Join".into(),
            JoinType::Semi => "Merge Semi Join".into(),
            JoinType::Anti => "Merge Anti Join".into(),
            JoinType::Cross => "Merge Join".into(),
        }
    }

    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        timing: bool,
        lines: &mut Vec<String>,
    ) {
        let prefix = "  ".repeat(indent + 1);
        let (left_names, right_names) = self.combined_names.split_at(self.left_width);
        if !self.merge_clauses.is_empty() {
            lines.push(format!(
                "{prefix}Merge Cond: {}",
                render_explain_join_expr(
                    &format_qual_list(&self.merge_clauses),
                    left_names,
                    right_names,
                )
            ));
        } else if !self.outer_merge_keys.is_empty() {
            let rendered = self
                .outer_merge_keys
                .iter()
                .zip(self.inner_merge_keys.iter())
                .map(|(outer_key, inner_key)| {
                    format!(
                        "{} = {}",
                        render_explain_expr(outer_key, left_names),
                        render_explain_expr(inner_key, right_names)
                    )
                })
                .collect::<Vec<_>>()
                .join(" AND ");
            lines.push(format!("{prefix}Merge Cond: ({rendered})"));
        }
        if !self.join_qual.is_empty() {
            lines.push(format!(
                "{prefix}Join Filter: {}",
                render_explain_join_expr(
                    &format_qual_list(&self.join_qual),
                    left_names,
                    right_names,
                )
            ));
        }
        if !self.qual.is_empty() {
            let (left_names, right_names) = self.combined_names.split_at(self.left_width);
            lines.push(format!(
                "{prefix}Filter: {}",
                render_explain_join_expr(&format_qual_list(&self.qual), left_names, right_names)
            ));
        }
        format_explain_lines_with_costs(
            &*self.left,
            indent + 1,
            analyze,
            show_costs,
            timing,
            lines,
        );
        format_explain_lines_with_costs(
            &*self.right,
            indent + 1,
            analyze,
            show_costs,
            timing,
            lines,
        );
    }
}
