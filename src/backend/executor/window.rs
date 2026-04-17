use super::agg::AccumState;
use super::exec_expr::eval_expr;
use super::expr_ops::{compare_order_values, values_are_distinct};
use super::{ExecError, ExecutorContext};
use crate::include::catalog::builtin_aggregate_function_for_proc_oid;
use crate::include::nodes::datum::Value;
use crate::include::nodes::execnodes::{MaterializedRow, SystemVarBinding, TupleSlot};
use crate::include::nodes::primnodes::{BuiltinWindowFunction, WindowClause, WindowFuncKind};
use std::cmp::Ordering;

#[derive(Debug)]
struct PreparedWindowRow {
    row: MaterializedRow,
    partition_keys: Vec<Value>,
    order_keys: Vec<Value>,
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
    ctx.expr_bindings.inner_tuple = None;
    ctx.expr_bindings.inner_system_bindings.clear();
}

fn prepare_input_rows(
    ctx: &mut ExecutorContext,
    clause: &WindowClause,
    rows: Vec<MaterializedRow>,
) -> Result<Vec<PreparedWindowRow>, ExecError> {
    rows.into_iter()
        .map(|mut row| {
            set_active_system_bindings(ctx, &row.system_bindings);
            set_outer_expr_bindings(ctx, row.slot.tts_values.clone(), &row.system_bindings);
            let partition_keys = clause
                .spec
                .partition_by
                .iter()
                .map(|expr| eval_expr(expr, &mut row.slot, ctx).map(|value| value.to_owned_value()))
                .collect::<Result<Vec<_>, _>>()?;
            let order_keys = clause
                .spec
                .order_by
                .iter()
                .map(|item| {
                    eval_expr(&item.expr, &mut row.slot, ctx).map(|value| value.to_owned_value())
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(PreparedWindowRow {
                row,
                partition_keys,
                order_keys,
            })
        })
        .collect()
}

fn same_partition(left: &PreparedWindowRow, right: &PreparedWindowRow) -> bool {
    left.partition_keys.len() == right.partition_keys.len()
        && left
            .partition_keys
            .iter()
            .zip(right.partition_keys.iter())
            .all(|(left, right)| !values_are_distinct(left, right))
}

fn same_peer(left: &PreparedWindowRow, right: &PreparedWindowRow) -> bool {
    left.order_keys.len() == right.order_keys.len()
        && left
            .order_keys
            .iter()
            .zip(right.order_keys.iter())
            .all(|(left, right)| compare_order_values(left, right, None, false) == Ordering::Equal)
}

fn advance_window_aggregate(
    ctx: &mut ExecutorContext,
    state: &mut AccumState,
    row: &mut MaterializedRow,
    aggref: &crate::include::nodes::primnodes::Aggref,
) -> Result<(), ExecError> {
    set_active_system_bindings(ctx, &row.system_bindings);
    set_outer_expr_bindings(ctx, row.slot.tts_values.clone(), &row.system_bindings);
    if let Some(filter) = aggref.aggfilter.as_ref() {
        match eval_expr(filter, &mut row.slot, ctx)? {
            Value::Bool(true) => {}
            Value::Bool(false) | Value::Null => return Ok(()),
            other => return Err(ExecError::NonBoolQual(other)),
        }
    }
    let values = aggref
        .args
        .iter()
        .map(|arg| eval_expr(arg, &mut row.slot, ctx).map(|value| value.to_owned_value()))
        .collect::<Result<Vec<_>, _>>()?;
    let func = builtin_aggregate_function_for_proc_oid(aggref.aggfnoid).unwrap_or_else(|| {
        panic!(
            "window aggregate {:?} lacks builtin implementation mapping",
            aggref.aggfnoid
        )
    });
    let transition = AccumState::transition_fn(func, aggref.args.len(), aggref.aggdistinct);
    transition(state, &values);
    Ok(())
}

fn evaluate_builtin_window(
    func: BuiltinWindowFunction,
    partition_rows: &[PreparedWindowRow],
) -> Vec<Value> {
    let mut values = Vec::with_capacity(partition_rows.len());
    let mut peer_start = 0usize;
    let mut dense_rank = 1i64;
    for index in 0..partition_rows.len() {
        if index > 0 && !same_peer(&partition_rows[index - 1], &partition_rows[index]) {
            peer_start = index;
            dense_rank += 1;
        }
        values.push(match func {
            BuiltinWindowFunction::RowNumber => Value::Int64(index as i64 + 1),
            BuiltinWindowFunction::Rank => Value::Int64(peer_start as i64 + 1),
            BuiltinWindowFunction::DenseRank => Value::Int64(dense_rank),
        });
    }
    values
}

fn evaluate_aggregate_window(
    ctx: &mut ExecutorContext,
    aggref: &crate::include::nodes::primnodes::Aggref,
    partition_rows: &mut [PreparedWindowRow],
    has_order_by: bool,
) -> Result<Vec<Value>, ExecError> {
    let func = builtin_aggregate_function_for_proc_oid(aggref.aggfnoid).unwrap_or_else(|| {
        panic!(
            "window aggregate {:?} lacks builtin implementation mapping",
            aggref.aggfnoid
        )
    });
    let mut state = AccumState::new(func, aggref.aggdistinct, aggref.aggtype);
    if !has_order_by {
        for row in partition_rows.iter_mut() {
            advance_window_aggregate(ctx, &mut state, &mut row.row, aggref)?;
        }
        return Ok(vec![state.finalize(); partition_rows.len()]);
    }

    let mut values = vec![Value::Null; partition_rows.len()];
    let mut peer_start = 0usize;
    while peer_start < partition_rows.len() {
        let mut peer_end = peer_start + 1;
        while peer_end < partition_rows.len()
            && same_peer(&partition_rows[peer_end - 1], &partition_rows[peer_end])
        {
            peer_end += 1;
        }
        for row in partition_rows[peer_start..peer_end].iter_mut() {
            advance_window_aggregate(ctx, &mut state, &mut row.row, aggref)?;
        }
        let value = state.finalize();
        for result in &mut values[peer_start..peer_end] {
            *result = value.clone();
        }
        peer_start = peer_end;
    }
    Ok(values)
}

pub(crate) fn execute_window_clause(
    ctx: &mut ExecutorContext,
    clause: &WindowClause,
    rows: Vec<MaterializedRow>,
) -> Result<Vec<MaterializedRow>, ExecError> {
    let mut prepared = prepare_input_rows(ctx, clause, rows)?;
    let mut output_rows = Vec::with_capacity(prepared.len());
    let mut partition_start = 0usize;
    while partition_start < prepared.len() {
        let mut partition_end = partition_start + 1;
        while partition_end < prepared.len()
            && same_partition(&prepared[partition_end - 1], &prepared[partition_end])
        {
            partition_end += 1;
        }

        let partition = &mut prepared[partition_start..partition_end];
        let mut function_values = Vec::with_capacity(clause.functions.len());
        for func in &clause.functions {
            function_values.push(match &func.kind {
                WindowFuncKind::Builtin(kind) => evaluate_builtin_window(*kind, partition),
                WindowFuncKind::Aggregate(aggref) => evaluate_aggregate_window(
                    ctx,
                    aggref,
                    partition,
                    !clause.spec.order_by.is_empty(),
                )?,
            });
        }

        for (row_index, prepared_row) in partition.iter().enumerate() {
            let mut values = prepared_row.row.slot.tts_values.clone();
            for results in &function_values {
                values.push(results[row_index].clone());
            }
            output_rows.push(MaterializedRow::new(
                TupleSlot::virtual_row(values),
                prepared_row.row.system_bindings.clone(),
            ));
        }

        partition_start = partition_end;
    }
    Ok(output_rows)
}
