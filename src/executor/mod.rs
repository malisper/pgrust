pub mod nodes;
pub mod expr;
pub(crate) mod tuple_decoder;
mod explain;
pub mod commands;

pub use nodes::*;
pub use expr::eval_expr;

use crate::access::heap::am::{
    HeapError, heap_scan_begin_visible, heap_scan_end, heap_scan_next_visible_raw,
    heap_scan_page_next_tuple, heap_scan_prepare_next_page,
};
use crate::access::heap::mvcc::{CommandId, MvccError, Snapshot, TransactionId, TransactionManager};
use crate::access::heap::tuple::TupleError;
use crate::catalog::Catalog;
use crate::parser::{
    ParseError, Statement, bind_delete, bind_insert, bind_update, build_plan, parse_statement,
};
use crate::{BufferPool, ClientId, SmgrStorageBackend};

use std::cmp::Ordering;
use std::rc::Rc;
use std::time::{Duration, Instant};

use expr::{compare_order_by_keys, compare_order_values};
use commands::*;

pub struct ExecutorContext {
    pub pool: std::sync::Arc<BufferPool<SmgrStorageBackend>>,
    pub txns: std::sync::Arc<parking_lot::RwLock<TransactionManager>>,
    pub snapshot: Snapshot,
    pub client_id: ClientId,
    pub next_command_id: CommandId,
}

#[derive(Debug)]
pub enum ExecError {
    Heap(HeapError),
    Tuple(TupleError),
    Parse(ParseError),
    InvalidColumn(usize),
    TypeMismatch {
        op: &'static str,
        left: Value,
        right: Value,
    },
    NonBoolQual(Value),
    UnsupportedStorageType {
        column: String,
        ty: ScalarType,
        attlen: i16,
    },
    InvalidStorageValue {
        column: String,
        details: String,
    },
    MissingRequiredColumn(String),
}

impl From<HeapError> for ExecError {
    fn from(value: HeapError) -> Self {
        Self::Heap(value)
    }
}

impl From<TupleError> for ExecError {
    fn from(value: TupleError) -> Self {
        Self::Tuple(value)
    }
}

impl From<ParseError> for ExecError {
    fn from(value: ParseError) -> Self {
        Self::Parse(value)
    }
}

impl From<MvccError> for ExecError {
    fn from(value: MvccError) -> Self {
        Self::Heap(HeapError::Mvcc(value))
    }
}

#[derive(Debug, Clone)]
pub(crate) enum AccumState {
    Count { count: i64 },
    Sum { sum: Option<i64> },
    Avg { sum: Option<i64>, count: i64 },
    Min { min: Option<Value> },
    Max { max: Option<Value> },
}

impl AccumState {
    pub(crate) fn new(func: AggFunc) -> Self {
        match func {
            AggFunc::Count => AccumState::Count { count: 0 },
            AggFunc::Sum => AccumState::Sum { sum: None },
            AggFunc::Avg => AccumState::Avg { sum: None, count: 0 },
            AggFunc::Min => AccumState::Min { min: None },
            AggFunc::Max => AccumState::Max { max: None },
        }
    }

    pub(crate) fn accumulate(&mut self, value: &Value, is_count_star: bool) {
        match self {
            AccumState::Count { count } => {
                if is_count_star || !matches!(value, Value::Null) {
                    *count += 1;
                }
            }
            AccumState::Sum { sum } => {
                if let Value::Int32(v) = value {
                    *sum = Some(sum.unwrap_or(0) + *v as i64);
                }
            }
            AccumState::Avg { sum, count } => {
                if let Value::Int32(v) = value {
                    *sum = Some(sum.unwrap_or(0) + *v as i64);
                    *count += 1;
                }
            }
            AccumState::Min { min } => {
                if !matches!(value, Value::Null) {
                    *min = Some(match min.take() {
                        None => value.clone(),
                        Some(current) => {
                            if compare_order_values(value, &current, None, false) == Ordering::Less {
                                value.clone()
                            } else {
                                current
                            }
                        }
                    });
                }
            }
            AccumState::Max { max } => {
                if !matches!(value, Value::Null) {
                    *max = Some(match max.take() {
                        None => value.clone(),
                        Some(current) => {
                            if compare_order_values(value, &current, None, false) == Ordering::Greater {
                                value.clone()
                            } else {
                                current
                            }
                        }
                    });
                }
            }
        }
    }

    pub(crate) fn finalize(&self) -> Value {
        match self {
            AccumState::Count { count } => Value::Int32(*count as i32),
            AccumState::Sum { sum } => match sum {
                Some(v) => Value::Int32(*v as i32),
                None => Value::Null,
            },
            AccumState::Avg { sum, count } => {
                if *count == 0 {
                    Value::Null
                } else {
                    match sum {
                        Some(v) => Value::Int32((*v / *count) as i32),
                        None => Value::Null,
                    }
                }
            }
            AccumState::Min { min } => min.clone().unwrap_or(Value::Null),
            AccumState::Max { max } => max.clone().unwrap_or(Value::Null),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AggGroup {
    pub(crate) key_values: Vec<Value>,
    pub(crate) accum_states: Vec<AccumState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatementResult {
    Query {
        column_names: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    AffectedRows(usize),
}

fn make_plan_state(kind: PlanStateKind) -> PlanState {
    let exec_proc_node = match &kind {
        PlanStateKind::Result(_) => exec_result_node as fn(&mut PlanState, &mut ExecutorContext) -> _,
        PlanStateKind::SeqScan(_) => exec_seq_scan_node,
        PlanStateKind::NestedLoopJoin(_) => exec_nested_loop_join_node,
        PlanStateKind::Filter(_) => exec_filter_node,
        PlanStateKind::OrderBy(_) => exec_order_by_node,
        PlanStateKind::Limit(_) => exec_limit_node,
        PlanStateKind::Projection(_) => exec_projection_node,
        PlanStateKind::Aggregate(_) => exec_aggregate_node,
    };
    PlanState { kind, exec_proc_node }
}

pub fn executor_start(plan: Plan) -> PlanState {
    match plan {
        Plan::Result => make_plan_state(PlanStateKind::Result(ResultState {
            emitted: false,
            stats: NodeExecStats::default(),
        })),
        Plan::SeqScan { rel, desc } => {
            let column_names: Rc<[String]> = desc.columns.iter().map(|c| c.name.clone()).collect();
            let attr_descs = desc.attribute_descs();
            let decoder = Rc::new(tuple_decoder::CompiledTupleDecoder::compile(&desc, &attr_descs));
            let ncols = desc.columns.len();
            make_plan_state(PlanStateKind::SeqScan(SeqScanState {
                rel,
                column_names,
                scan: None,
                decoder,
                values_buf: Vec::with_capacity(ncols),
                stats: NodeExecStats::default(),
            }))
        }
        Plan::NestedLoopJoin { left, right, on } => make_plan_state(PlanStateKind::NestedLoopJoin(NestedLoopJoinState {
            left: Box::new(executor_start(*left)),
            right: Box::new(executor_start(*right)),
            on,
            right_rows: None,
            current_left: None,
            right_index: 0,
            stats: NodeExecStats::default(),
        })),
        Plan::Filter { input, predicate } => make_plan_state(PlanStateKind::Filter(FilterState {
            input: Box::new(executor_start(*input)),
            predicate,
            stats: NodeExecStats::default(),
        })),
        Plan::OrderBy { input, items } => make_plan_state(PlanStateKind::OrderBy(OrderByState {
            input: Box::new(executor_start(*input)),
            items,
            rows: None,
            next_index: 0,
            stats: NodeExecStats::default(),
        })),
        Plan::Limit {
            input,
            limit,
            offset,
        } => make_plan_state(PlanStateKind::Limit(LimitState {
            input: Box::new(executor_start(*input)),
            limit,
            offset,
            skipped: 0,
            returned: 0,
            stats: NodeExecStats::default(),
        })),
        Plan::Projection { input, targets } => make_plan_state(PlanStateKind::Projection(ProjectionState {
            input: Box::new(executor_start(*input)),
            targets,
            stats: NodeExecStats::default(),
        })),
        Plan::Aggregate {
            input,
            group_by,
            accumulators,
            having,
            output_columns,
        } => make_plan_state(PlanStateKind::Aggregate(AggregateState {
            input: Box::new(executor_start(*input)),
            group_by,
            accumulators,
            having,
            output_columns,
            result_rows: None,
            next_index: 0,
            stats: NodeExecStats::default(),
        })),
    }
}

pub fn execute_plan(
    plan: Plan,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    Ok(execute_plan_internal(plan, ctx, false)?.0)
}

pub(crate) fn execute_plan_internal(
    plan: Plan,
    ctx: &mut ExecutorContext,
    timed: bool,
) -> Result<(StatementResult, PlanState, Duration), ExecError> {
    let column_names = plan.column_names();
    let mut state = executor_start(plan);
    let mut rows = Vec::new();
    let started_at = Instant::now();
    while let Some(slot) = exec_next_inner(&mut state, ctx, timed)? {
        rows.push(slot.into_values()?);
    }
    Ok((
        StatementResult::Query {
            column_names,
            rows,
        },
        state,
        started_at.elapsed(),
    ))
}

pub fn execute_sql(
    sql: &str,
    catalog: &mut Catalog,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
) -> Result<StatementResult, ExecError> {
    let stmt = parse_statement(sql)?;
    execute_statement(stmt, catalog, ctx, xid)
}

pub fn execute_statement(
    stmt: Statement,
    catalog: &mut Catalog,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
) -> Result<StatementResult, ExecError> {
    let cid = ctx.next_command_id;
    ctx.snapshot = ctx.txns.read().snapshot_for_command(xid, cid)?;
    let result = match stmt {
        Statement::Explain(stmt) => execute_explain(stmt, catalog, ctx),
        Statement::Select(stmt) => execute_plan(build_plan(&stmt, catalog)?, ctx),
        Statement::ShowTables => execute_show_tables(catalog),
        Statement::CreateTable(stmt) => execute_create_table(stmt, catalog),
        Statement::DropTable(stmt) => execute_drop_table(stmt, catalog, ctx),
        Statement::TruncateTable(stmt) => execute_truncate_table(stmt, catalog, ctx),
        Statement::Vacuum(_) => Ok(StatementResult::AffectedRows(0)),
        Statement::Insert(stmt) => execute_insert(bind_insert(&stmt, catalog)?, ctx, xid, cid),
        Statement::Update(stmt) => execute_update(bind_update(&stmt, catalog)?, ctx, xid, cid),
        Statement::Delete(stmt) => execute_delete(bind_delete(&stmt, catalog)?, ctx, xid),
        Statement::Begin | Statement::Commit | Statement::Rollback => {
            Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "non-transaction-control statement",
                actual: "BEGIN/COMMIT/ROLLBACK".into(),
            }))
        }
    };
    ctx.next_command_id = ctx.next_command_id.saturating_add(1);
    result
}

/// Execute a read-only statement (SELECT, EXPLAIN, SHOW) with only a shared catalog reference.
pub fn execute_readonly_statement(
    stmt: Statement,
    catalog: &Catalog,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    match stmt {
        Statement::Explain(stmt) => execute_explain(stmt, catalog, ctx),
        Statement::Select(stmt) => execute_plan(build_plan(&stmt, catalog)?, ctx),
        Statement::ShowTables => execute_show_tables(catalog),
        other => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: format!("{other:?}"),
        })),
    }
}

pub fn exec_next(
    state: &mut PlanState,
    ctx: &mut ExecutorContext,
) -> Result<Option<TupleSlot>, ExecError> {
    exec_next_inner(state, ctx, false)
}

pub(crate) fn exec_next_inner(
    state: &mut PlanState,
    ctx: &mut ExecutorContext,
    timed: bool,
) -> Result<Option<TupleSlot>, ExecError> {
    if !timed {
        return (state.exec_proc_node)(state, ctx);
    }
    // EXPLAIN ANALYZE path: record per-node timing like PG's InstrStartNode/InstrStopNode.
    let started_at = Instant::now();
    let result = (state.exec_proc_node)(state, ctx);
    if let Ok(slot) = &result {
        let stats = node_stats_mut(state);
        stats.loops += 1;
        stats.total_time += started_at.elapsed();
        if slot.is_some() {
            stats.rows += 1;
        }
    }
    result
}

// Thin wrappers that extract the inner state from PlanState and call the
// real exec function. These are stored as function pointers in PlanState.
fn exec_result_node(state: &mut PlanState, _ctx: &mut ExecutorContext) -> Result<Option<TupleSlot>, ExecError> {
    let PlanStateKind::Result(inner) = &mut state.kind else { unreachable!() };
    exec_result(inner)
}
fn exec_seq_scan_node(state: &mut PlanState, ctx: &mut ExecutorContext) -> Result<Option<TupleSlot>, ExecError> {
    let PlanStateKind::SeqScan(inner) = &mut state.kind else { unreachable!() };
    exec_seq_scan(inner, ctx)
}
fn exec_nested_loop_join_node(state: &mut PlanState, ctx: &mut ExecutorContext) -> Result<Option<TupleSlot>, ExecError> {
    let PlanStateKind::NestedLoopJoin(inner) = &mut state.kind else { unreachable!() };
    exec_nested_loop_join(inner, ctx)
}
fn exec_filter_node(state: &mut PlanState, ctx: &mut ExecutorContext) -> Result<Option<TupleSlot>, ExecError> {
    let PlanStateKind::Filter(inner) = &mut state.kind else { unreachable!() };
    exec_filter(inner, ctx)
}
fn exec_order_by_node(state: &mut PlanState, ctx: &mut ExecutorContext) -> Result<Option<TupleSlot>, ExecError> {
    let PlanStateKind::OrderBy(inner) = &mut state.kind else { unreachable!() };
    exec_order_by(inner, ctx)
}
fn exec_limit_node(state: &mut PlanState, ctx: &mut ExecutorContext) -> Result<Option<TupleSlot>, ExecError> {
    let PlanStateKind::Limit(inner) = &mut state.kind else { unreachable!() };
    exec_limit(inner, ctx)
}
fn exec_projection_node(state: &mut PlanState, ctx: &mut ExecutorContext) -> Result<Option<TupleSlot>, ExecError> {
    let PlanStateKind::Projection(inner) = &mut state.kind else { unreachable!() };
    exec_projection(inner, ctx)
}
fn exec_aggregate_node(state: &mut PlanState, ctx: &mut ExecutorContext) -> Result<Option<TupleSlot>, ExecError> {
    let PlanStateKind::Aggregate(inner) = &mut state.kind else { unreachable!() };
    exec_aggregate(inner, ctx)
}

fn exec_result(state: &mut ResultState) -> Result<Option<TupleSlot>, ExecError> {
    if state.emitted {
        Ok(None)
    } else {
        state.emitted = true;
        Ok(Some(TupleSlot::virtual_row(Rc::from(Vec::<String>::new()), Vec::new())))
    }
}

fn exec_seq_scan(
    state: &mut SeqScanState,
    ctx: &mut ExecutorContext,
) -> Result<Option<TupleSlot>, ExecError> {
    if state.scan.is_none() {
        state.scan = Some(heap_scan_begin_visible(
            &ctx.pool,
            ctx.client_id,
            state.rel,
            ctx.snapshot.clone(),
        )?);
    }

    let scan = state.scan.as_mut().unwrap();
    let column_names = Rc::clone(&state.column_names);

    loop {
        // Try to get the next tuple from the current page's visibility list.
        if scan.has_page_tuples() {
            let buffer_id = scan.pinned_buffer_id().expect("buffer must be pinned");
            let guard = ctx.pool.lock_buffer_shared(buffer_id)
                .map_err(|e| ExecError::Heap(HeapError::Buffer(e)))?;
            let page = &*guard;

            if let Some((_tid, tuple_bytes)) = heap_scan_page_next_tuple(page, scan) {
                // Capture raw pointer — safe because page is pinned and user
                // data is immutable.
                let raw_ptr = tuple_bytes.as_ptr();
                let raw_len = tuple_bytes.len();
                drop(guard);

                // Get shared pin (Rc clone, keeps page alive for TextRef pointers)
                let pin = scan.pinned_buffer_rc()
                    .expect("buffer must be pinned");

                // Decode into reusable buffer. TextRef values point at the
                // still-pinned page. The pin Rc in the slot keeps it alive.
                state.values_buf.clear();
                let raw_bytes = unsafe { std::slice::from_raw_parts(raw_ptr, raw_len) };
                state.decoder.decode_into(raw_bytes, &mut state.values_buf)?;

                return Ok(Some(TupleSlot {
                    column_names,
                    source: SlotSource::ScanBuf {
                        values_ptr: state.values_buf.as_ptr(),
                        values_len: state.values_buf.len(),
                        pin,
                    },
                }));
            }
            drop(guard);
        }

        // Current page exhausted — prepare the next page (collects visible offsets).
        let next: Result<Option<usize>, ExecError> =
            heap_scan_prepare_next_page(&*ctx.pool, ctx.client_id, &ctx.txns, scan);
        if next?.is_none() {
            heap_scan_end::<ExecError>(&*ctx.pool, ctx.client_id, scan)?;
            return Ok(None);
        }
        // Loop back to read tuples from the newly prepared page.
    }
}

fn exec_filter(
    state: &mut FilterState,
    ctx: &mut ExecutorContext,
) -> Result<Option<TupleSlot>, ExecError> {
    loop {
        let Some(mut slot) = exec_next(&mut state.input, ctx)? else {
            return Ok(None);
        };

        match eval_expr(&state.predicate, &mut slot)? {
            Value::Bool(true) => return Ok(Some(slot)),
            Value::Bool(false) | Value::Null => continue,
            other => return Err(ExecError::NonBoolQual(other)),
        }
    }
}

fn exec_nested_loop_join(
    state: &mut NestedLoopJoinState,
    ctx: &mut ExecutorContext,
) -> Result<Option<TupleSlot>, ExecError> {
    if state.right_rows.is_none() {
        let mut rows = Vec::new();
        while let Some(slot) = exec_next(&mut state.right, ctx)? {
            rows.push(slot.materialize()?);
        }
        state.right_rows = Some(rows);
    }

    let right_rows = state.right_rows.as_ref().unwrap();
    loop {
        if state.current_left.is_none() {
            state.current_left = exec_next(&mut state.left, ctx)?
                .map(|s| s.materialize()).transpose()?;
            state.right_index = 0;
        }

        let Some(left_slot) = state.current_left.as_ref() else {
            return Ok(None);
        };

        while state.right_index < right_rows.len() {
            let joined = combine_slots(left_slot.clone(), right_rows[state.right_index].clone())?;
            state.right_index += 1;
            let mut eval_slot = joined.clone();
            match eval_expr(&state.on, &mut eval_slot)? {
                Value::Bool(true) => return Ok(Some(joined)),
                Value::Bool(false) | Value::Null => {}
                other => return Err(ExecError::NonBoolQual(other)),
            }
        }

        state.current_left = None;
    }
}

fn exec_projection(
    state: &mut ProjectionState,
    ctx: &mut ExecutorContext,
) -> Result<Option<TupleSlot>, ExecError> {
    let Some(mut input) = exec_next(&mut state.input, ctx)? else {
        return Ok(None);
    };

    let mut values = Vec::with_capacity(state.targets.len());
    let mut names = Vec::with_capacity(state.targets.len());
    for target in &state.targets {
        values.push(eval_expr(&target.expr, &mut input)?);
        names.push(target.name.clone());
    }

    Ok(Some(TupleSlot::virtual_row(names.into(), values)))
}

fn exec_order_by(
    state: &mut OrderByState,
    ctx: &mut ExecutorContext,
) -> Result<Option<TupleSlot>, ExecError> {
    if state.rows.is_none() {
        let mut rows = Vec::new();
        while let Some(slot) = exec_next(&mut state.input, ctx)? {
            rows.push(slot.materialize()?);
        }

        let mut keyed_rows = Vec::with_capacity(rows.len());
        for mut row in rows {
            let mut keys = Vec::with_capacity(state.items.len());
            for item in &state.items {
                keys.push(eval_expr(&item.expr, &mut row)?);
            }
            keyed_rows.push((keys, row));
        }

        keyed_rows.sort_by(|(left_keys, _), (right_keys, _)| {
            compare_order_by_keys(&state.items, left_keys, right_keys)
        });
        state.rows = Some(keyed_rows.into_iter().map(|(_, row)| row).collect());
    }

    let rows = state.rows.as_ref().unwrap();
    if state.next_index >= rows.len() {
        return Ok(None);
    }

    let slot = rows[state.next_index].clone();
    state.next_index += 1;
    Ok(Some(slot))
}

fn exec_limit(
    state: &mut LimitState,
    ctx: &mut ExecutorContext,
) -> Result<Option<TupleSlot>, ExecError> {
    if let Some(limit) = state.limit {
        if state.returned >= limit {
            return Ok(None);
        }
    }

    while state.skipped < state.offset {
        if exec_next(&mut state.input, ctx)?.is_none() {
            return Ok(None);
        }
        state.skipped += 1;
    }

    let next = exec_next(&mut state.input, ctx)?;
    if next.is_some() {
        state.returned += 1;
    }
    Ok(next)
}

fn exec_aggregate(
    state: &mut AggregateState,
    ctx: &mut ExecutorContext,
) -> Result<Option<TupleSlot>, ExecError> {
    if state.result_rows.is_none() {
        let mut groups: Vec<AggGroup> = Vec::new();

        while let Some(mut slot) = exec_next(&mut state.input, ctx)? {
            let key_values: Vec<Value> = state
                .group_by
                .iter()
                .map(|expr| eval_expr(expr, &mut slot))
                .collect::<Result<_, _>>()?;

            let group_idx = groups
                .iter()
                .position(|g| g.key_values == key_values)
                .unwrap_or_else(|| {
                    let accum_states = state
                        .accumulators
                        .iter()
                        .map(|a| AccumState::new(a.func))
                        .collect();
                    groups.push(AggGroup {
                        key_values: key_values.clone(),
                        accum_states,
                    });
                    groups.len() - 1
                });

            let group = &mut groups[group_idx];
            for (i, accum) in state.accumulators.iter().enumerate() {
                let is_count_star = accum.func == AggFunc::Count && accum.arg.is_none();
                let value = if let Some(arg) = &accum.arg {
                    eval_expr(arg, &mut slot)?
                } else {
                    Value::Null
                };
                group.accum_states[i].accumulate(&value, is_count_star);
            }
        }

        if groups.is_empty() && state.group_by.is_empty() {
            let accum_states = state
                .accumulators
                .iter()
                .map(|a| AccumState::new(a.func))
                .collect();
            groups.push(AggGroup {
                key_values: Vec::new(),
                accum_states,
            });
        }

        let mut result_rows = Vec::new();
        for group in &groups {
            let mut row_values = group.key_values.clone();
            for accum_state in &group.accum_states {
                row_values.push(accum_state.finalize());
            }

            if let Some(having) = &state.having {
                let mut having_slot =
                    TupleSlot::virtual_row(state.output_columns.clone().into(), row_values.clone());
                match eval_expr(having, &mut having_slot)? {
                    Value::Bool(true) => {}
                    Value::Bool(false) | Value::Null => continue,
                    other => return Err(ExecError::NonBoolQual(other)),
                }
            }

            result_rows.push(TupleSlot::virtual_row(
                state.output_columns.clone().into(),
                row_values,
            ));
        }

        state.result_rows = Some(result_rows);
    }

    let rows = state.result_rows.as_ref().unwrap();
    if state.next_index >= rows.len() {
        return Ok(None);
    }

    let slot = rows[state.next_index].clone();
    state.next_index += 1;
    Ok(Some(slot))
}

fn combine_slots(left: TupleSlot, right: TupleSlot) -> Result<TupleSlot, ExecError> {
    let mut names: Vec<String> = left.column_names().to_vec();
    names.extend_from_slice(right.column_names());
    let mut values = left.into_values()?;
    values.extend(right.into_values()?);
    Ok(TupleSlot::virtual_row(names.into(), values))
}

fn node_stats_mut(state: &mut PlanState) -> &mut NodeExecStats {
    match &mut state.kind {
        PlanStateKind::Result(result) => &mut result.stats,
        PlanStateKind::SeqScan(scan) => &mut scan.stats,
        PlanStateKind::NestedLoopJoin(join) => &mut join.stats,
        PlanStateKind::Filter(filter) => &mut filter.stats,
        PlanStateKind::OrderBy(order_by) => &mut order_by.stats,
        PlanStateKind::Limit(limit) => &mut limit.stats,
        PlanStateKind::Projection(projection) => &mut projection.stats,
        PlanStateKind::Aggregate(aggregate) => &mut aggregate.stats,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::heap::am::{heap_flush, heap_insert_mvcc, heap_update};
    use crate::access::heap::mvcc::INVALID_TRANSACTION_ID;
    use crate::access::heap::tuple::{AttributeAlign, TupleValue};
    use crate::parser::{Catalog, CatalogEntry};
    use crate::storage::smgr::{ForkNumber, MdStorageManager, StorageManager};
    use crate::access::heap::tuple::{AttributeDesc, HeapTuple};
    use crate::RelFileLocator;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

    fn temp_dir(label: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "pgrust_executor_{}_{}_{}",
            label,
            std::process::id(),
            NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn rel() -> RelFileLocator {
        RelFileLocator {
            spc_oid: 0,
            db_oid: 1,
            rel_number: 14000,
        }
    }

    fn pets_rel() -> RelFileLocator {
        RelFileLocator {
            spc_oid: 0,
            db_oid: 1,
            rel_number: 14001,
        }
    }

    fn relation_desc() -> RelationDesc {
        RelationDesc {
            columns: vec![
                ColumnDesc {
                    name: "id".into(),
                    storage: AttributeDesc {
                        name: "id".into(),
                        attlen: 4,
                        attalign: AttributeAlign::Int,
                        nullable: false,
                    },
                    ty: ScalarType::Int32,
                },
                ColumnDesc {
                    name: "name".into(),
                    storage: AttributeDesc {
                        name: "name".into(),
                        attlen: -1,
                        attalign: AttributeAlign::Int,
                        nullable: false,
                    },
                    ty: ScalarType::Text,
                },
                ColumnDesc {
                    name: "note".into(),
                    storage: AttributeDesc {
                        name: "note".into(),
                        attlen: -1,
                        attalign: AttributeAlign::Int,
                        nullable: true,
                    },
                    ty: ScalarType::Text,
                },
            ],
        }
    }

    fn catalog() -> Catalog {
        let mut catalog = Catalog::default();
        catalog.insert(
            "people",
            CatalogEntry {
                rel: rel(),
                desc: relation_desc(),
            },
        );
        catalog
    }

    fn pets_relation_desc() -> RelationDesc {
        RelationDesc {
            columns: vec![
                ColumnDesc {
                    name: "id".into(),
                    storage: AttributeDesc {
                        name: "id".into(),
                        attlen: 4,
                        attalign: AttributeAlign::Int,
                        nullable: false,
                    },
                    ty: ScalarType::Int32,
                },
                ColumnDesc {
                    name: "name".into(),
                    storage: AttributeDesc {
                        name: "name".into(),
                        attlen: -1,
                        attalign: AttributeAlign::Int,
                        nullable: false,
                    },
                    ty: ScalarType::Text,
                },
                ColumnDesc {
                    name: "owner_id".into(),
                    storage: AttributeDesc {
                        name: "owner_id".into(),
                        attlen: 4,
                        attalign: AttributeAlign::Int,
                        nullable: false,
                    },
                    ty: ScalarType::Int32,
                },
            ],
        }
    }

    fn catalog_with_pets() -> Catalog {
        let mut catalog = catalog();
        catalog.insert(
            "pets",
            CatalogEntry {
                rel: pets_rel(),
                desc: pets_relation_desc(),
            },
        );
        catalog
    }

    fn tuple(id: i32, name: &str, note: Option<&str>) -> HeapTuple {
        let desc = relation_desc().attribute_descs();
        HeapTuple::from_values(
            &desc,
            &[
                TupleValue::Bytes(id.to_le_bytes().to_vec()),
                TupleValue::Bytes(name.as_bytes().to_vec()),
                match note {
                    Some(note) => TupleValue::Bytes(note.as_bytes().to_vec()),
                    None => TupleValue::Null,
                },
            ],
        )
        .unwrap()
    }

    fn pet_tuple(id: i32, name: &str, owner_id: i32) -> HeapTuple {
        let desc = pets_relation_desc().attribute_descs();
        HeapTuple::from_values(
            &desc,
            &[
                TupleValue::Bytes(id.to_le_bytes().to_vec()),
                TupleValue::Bytes(name.as_bytes().to_vec()),
                TupleValue::Bytes(owner_id.to_le_bytes().to_vec()),
            ],
        )
        .unwrap()
    }

    /// Test-only: create the storage fork for a relation.
    fn create_fork(pool: &BufferPool<SmgrStorageBackend>, rel: RelFileLocator) {
        pool.with_storage_mut(|s| {
            s.smgr.open(rel).unwrap();
            match s.smgr.create(rel, ForkNumber::Main, false) {
                Ok(()) => {}
                Err(crate::storage::smgr::SmgrError::AlreadyExists { .. }) => {}
                Err(e) => panic!("create_fork failed: {e:?}"),
            }
        });
    }

    /// Test-only: create a buffer pool with the "people" table fork ready.
    fn test_pool(base: &PathBuf) -> std::sync::Arc<BufferPool<SmgrStorageBackend>> {
        let smgr = MdStorageManager::new(base);
        let pool = std::sync::Arc::new(BufferPool::new(SmgrStorageBackend::new(smgr), 8));
        create_fork(&*pool, rel());
        pool
    }

    /// Test-only: create a buffer pool with both "people" and "pets" forks ready.
    fn test_pool_with_pets(base: &PathBuf) -> std::sync::Arc<BufferPool<SmgrStorageBackend>> {
        let pool = test_pool(base);
        create_fork(&*pool, pets_rel());
        pool
    }

    fn run_plan(
        base: &PathBuf,
        txns: &TransactionManager,
        plan: Plan,
    ) -> Result<Vec<(Vec<String>, Vec<Value>)>, ExecError> {
        let pool = test_pool(base);
        let txns_arc = std::sync::Arc::new(parking_lot::RwLock::new(txns.clone()));
        let mut state = executor_start(plan);
        let mut ctx = ExecutorContext {
            pool,
            txns: txns_arc,
            snapshot: txns.snapshot(INVALID_TRANSACTION_ID).unwrap(),
            client_id: 42,
            next_command_id: 0,
        };

        let mut rows = Vec::new();
        while let Some(slot) = exec_next(&mut state, &mut ctx)? {
            rows.push((slot.column_names().to_vec(), slot.into_values()?));
        }
        Ok(rows)
    }

    fn run_sql(
        base: &PathBuf,
        txns: &TransactionManager,
        xid: TransactionId,
        sql: &str,
    ) -> Result<StatementResult, ExecError> {
        run_sql_with_catalog(base, txns, xid, sql, catalog())
    }

    fn run_sql_with_catalog(
        base: &PathBuf,
        txns: &TransactionManager,
        xid: TransactionId,
        sql: &str,
        mut catalog: Catalog,
    ) -> Result<StatementResult, ExecError> {
        let smgr = MdStorageManager::new(base);
        let pool = std::sync::Arc::new(BufferPool::new(SmgrStorageBackend::new(smgr), 8));
        for name in catalog.table_names().collect::<Vec<_>>() {
            if let Some(entry) = catalog.get(&name) {
                create_fork(&*pool, entry.rel);
            }
        }
        let txns_arc = std::sync::Arc::new(parking_lot::RwLock::new(txns.clone()));
        let mut ctx = ExecutorContext {
            pool,
            txns: txns_arc,
            snapshot: txns.snapshot(xid).unwrap(),
            client_id: 77,
            next_command_id: 0,
        };
        execute_sql(sql, &mut catalog, &mut ctx, xid)
    }

    #[test]
    fn expr_eval_obeys_null_semantics() {
        let desc = relation_desc();
        let col_names: Rc<[String]> = desc.columns.iter().map(|c| c.name.clone()).collect();
        let mut slot = TupleSlot::virtual_row(
            col_names,
            vec![Value::Int32(7), Value::Text("alice".into()), Value::Null],
        );
        assert_eq!(eval_expr(&Expr::Eq(Box::new(Expr::Column(0)), Box::new(Expr::Const(Value::Int32(7)))), &mut slot).unwrap(), Value::Bool(true));
        assert_eq!(eval_expr(&Expr::Eq(Box::new(Expr::Column(2)), Box::new(Expr::Const(Value::Text("x".into())))), &mut slot).unwrap(), Value::Null);
        assert_eq!(eval_expr(&Expr::And(Box::new(Expr::Const(Value::Bool(true))), Box::new(Expr::Const(Value::Null))), &mut slot).unwrap(), Value::Null);
        assert_eq!(eval_expr(&Expr::IsNull(Box::new(Expr::Column(2))), &mut slot).unwrap(), Value::Bool(true));
        assert_eq!(eval_expr(&Expr::IsNotNull(Box::new(Expr::Column(2))), &mut slot).unwrap(), Value::Bool(false));
        assert_eq!(eval_expr(&Expr::IsDistinctFrom(Box::new(Expr::Column(2)), Box::new(Expr::Const(Value::Null))), &mut slot).unwrap(), Value::Bool(false));
        assert_eq!(eval_expr(&Expr::IsDistinctFrom(Box::new(Expr::Column(1)), Box::new(Expr::Const(Value::Null))), &mut slot).unwrap(), Value::Bool(true));
    }

    #[test]
    fn physical_slot_lazily_deforms_heap_tuple() {
        use crate::access::heap::tuple::ItemPointerData;
        let desc = Rc::new(relation_desc());
        let attr_descs: Rc<[AttributeDesc]> = desc.attribute_descs().into();
        let col_names: Rc<[String]> = desc.columns.iter().map(|c| c.name.clone()).collect();
        let mut slot = TupleSlot::from_heap_tuple(
            desc,
            attr_descs,
            col_names,
            ItemPointerData { block_number: 0, offset_number: 1 },
            tuple(1, "alice", None),
        );
        assert_eq!(slot.values().unwrap(), &[Value::Int32(1), Value::Text("alice".into()), Value::Null]);
        assert_eq!(slot.tid(), Some(ItemPointerData { block_number: 0, offset_number: 1 }));
    }

    #[test]
    fn seqscan_filter_projection_returns_expected_rows() {
        let base = temp_dir("scan_filter_project");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let pool = test_pool(&base);
        let xid = txns.begin();
        let rows = [tuple(1, "alice", Some("alpha")), tuple(2, "bob", None), tuple(3, "carol", Some("gamma"))];
        let mut blocks = Vec::new();
        for row in rows { let tid = heap_insert_mvcc(&*pool, 1, rel(), xid, &row).unwrap(); blocks.push(tid.block_number); }
        txns.commit(xid).unwrap();
        blocks.sort(); blocks.dedup();
        for block in blocks { heap_flush(&*pool, 1, rel(), block).unwrap(); }
        drop(pool);
        let plan = Plan::Projection {
            input: Box::new(Plan::Filter {
                input: Box::new(Plan::SeqScan { rel: rel(), desc: relation_desc() }),
                predicate: Expr::Gt(Box::new(Expr::Column(0)), Box::new(Expr::Const(Value::Int32(1)))),
            }),
            targets: vec![
                TargetEntry { name: "name".into(), expr: Expr::Column(1) },
                TargetEntry { name: "note_is_null".into(), expr: Expr::IsNull(Box::new(Expr::Column(2))) },
            ],
        };
        let rows = run_plan(&base, &txns, plan).unwrap();
        assert_eq!(rows, vec![
            (vec!["name".into(), "note_is_null".into()], vec![Value::Text("bob".into()), Value::Bool(true)]),
            (vec!["name".into(), "note_is_null".into()], vec![Value::Text("carol".into()), Value::Bool(false)]),
        ]);
    }

    #[test]
    fn seqscan_skips_superseded_versions() {
        let base = temp_dir("visible_versions");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let pool = test_pool(&base);
        let insert_xid = txns.begin();
        let old_tid = heap_insert_mvcc(&*pool, 1, rel(), insert_xid, &tuple(1, "alice", Some("old"))).unwrap();
        txns.commit(insert_xid).unwrap();
        let update_xid = txns.begin();
        let new_tid = heap_update(&*pool, 1, rel(), &txns, update_xid, old_tid, &tuple(1, "alice", Some("new"))).unwrap();
        txns.commit(update_xid).unwrap();
        heap_flush(&*pool, 1, rel(), old_tid.block_number).unwrap();
        if new_tid.block_number != old_tid.block_number { heap_flush(&*pool, 1, rel(), new_tid.block_number).unwrap(); }
        drop(pool);
        let plan = Plan::SeqScan { rel: rel(), desc: relation_desc() };
        let rows = run_plan(&base, &txns, plan).unwrap();
        assert_eq!(rows, vec![(vec!["id".into(), "name".into(), "note".into()], vec![Value::Int32(1), Value::Text("alice".into()), Value::Text("new".into())])]);
    }

    #[test] fn insert_sql_inserts_row() { let base = temp_dir("insert_sql"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); assert_eq!(run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'alpha')").unwrap(), StatementResult::AffectedRows(1)); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select name, note from people").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Text("alice".into()), Value::Text("alpha".into())]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn insert_sql_inserts_multiple_rows() { let base = temp_dir("insert_multi_sql"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); assert_eq!(run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'alpha'), (2, 'bob', null)").unwrap(), StatementResult::AffectedRows(2)); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select id, name, note from people").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Int32(1), Value::Text("alice".into()), Value::Text("alpha".into())], vec![Value::Int32(2), Value::Text("bob".into()), Value::Null]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn update_sql_updates_matching_rows() { let base = temp_dir("update_sql"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let insert_xid = txns.begin(); run_sql(&base, &txns, insert_xid, "insert into people (id, name, note) values (1, 'alice', 'old')").unwrap(); txns.commit(insert_xid).unwrap(); let update_xid = txns.begin(); assert_eq!(run_sql(&base, &txns, update_xid, "update people set note = 'new' where id = 1").unwrap(), StatementResult::AffectedRows(1)); txns.commit(update_xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select note from people where id = 1").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Text("new".into())]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn delete_sql_deletes_matching_rows() { let base = temp_dir("delete_sql"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let insert_xid = txns.begin(); run_sql(&base, &txns, insert_xid, "insert into people (id, name, note) values (1, 'alice', null)").unwrap(); run_sql(&base, &txns, insert_xid, "insert into people (id, name, note) values (2, 'bob', 'keep')").unwrap(); txns.commit(insert_xid).unwrap(); let delete_xid = txns.begin(); assert_eq!(run_sql(&base, &txns, delete_xid, "delete from people where note is null").unwrap(), StatementResult::AffectedRows(1)); txns.commit(delete_xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select name from people").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Text("bob".into())]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn order_by_limit_offset_returns_expected_rows() { let base = temp_dir("order_by_limit_offset"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let insert_xid = txns.begin(); run_sql(&base, &txns, insert_xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (3, 'carol', 'c'), (2, 'bob', null)").unwrap(); txns.commit(insert_xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select id, name from people order by id desc limit 2 offset 1").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Int32(2), Value::Text("bob".into())], vec![Value::Int32(1), Value::Text("alice".into())]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn explain_mentions_sort_and_limit_nodes() { let base = temp_dir("explain_sort_limit"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "explain select name from people order by id desc limit 1 offset 2").unwrap() { StatementResult::Query { rows, .. } => { let rendered = rows.into_iter().map(|row| match &row[0] { Value::Text(text) => text.clone(), other => panic!("expected explain text row, got {:?}", other), }).collect::<Vec<_>>(); assert!(rendered.iter().any(|line| line.contains("Projection"))); assert!(rendered.iter().any(|line| line.contains("Limit"))); assert!(rendered.iter().any(|line| line.contains("Sort"))); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn order_by_nulls_first_and_last_work() { let base = temp_dir("order_by_nulls"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let insert_xid = txns.begin(); run_sql(&base, &txns, insert_xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', null), (3, 'carol', 'c')").unwrap(); txns.commit(insert_xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select id from people order by note asc nulls first").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Int32(2)], vec![Value::Int32(1)], vec![Value::Int32(3)]]); } other => panic!("expected query result, got {:?}", other), } match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select id from people order by note desc nulls last").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Int32(3)], vec![Value::Int32(1)], vec![Value::Int32(2)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn null_predicates_work_in_where_clause() { let base = temp_dir("null_predicates"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let insert_xid = txns.begin(); run_sql(&base, &txns, insert_xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', null), (3, 'carol', 'c')").unwrap(); txns.commit(insert_xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select id from people where note is null").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Int32(2)]]); } other => panic!("expected query result, got {:?}", other), } match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select id from people where note is not null order by id").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Int32(1)], vec![Value::Int32(3)]]); } other => panic!("expected query result, got {:?}", other), } match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select id from people where note is distinct from null order by id").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Int32(1)], vec![Value::Int32(3)]]); } other => panic!("expected query result, got {:?}", other), } match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select id from people where note is not distinct from null").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Int32(2)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn show_tables_lists_catalog_tables() { let base = temp_dir("show_tables"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "show tables").unwrap() { StatementResult::Query { column_names, rows } => { assert_eq!(column_names, vec!["table_name".to_string()]); assert_eq!(rows, vec![vec![Value::Text("people".into())]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn explain_returns_plan_lines() { let base = temp_dir("explain_sql"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "explain select name from people").unwrap() { StatementResult::Query { column_names, rows } => { assert_eq!(column_names, vec!["QUERY PLAN".to_string()]); let rendered = rows.into_iter().map(|row| match &row[0] { Value::Text(text) => text.clone(), other => panic!("expected text explain line, got {:?}", other), }).collect::<Vec<_>>(); assert!(rendered.iter().any(|line| line.contains("Projection"))); assert!(rendered.iter().any(|line| line.contains("Seq Scan"))); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn select_without_from_returns_constant_row() { let base = temp_dir("select_without_from"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 1").unwrap() { StatementResult::Query { column_names, rows } => { assert_eq!(column_names, vec!["expr1".to_string()]); assert_eq!(rows, vec![vec![Value::Int32(1)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn select_from_people_returns_zero_column_rows() { let base = temp_dir("select_from_people"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'alpha'), (2, 'bob', null)").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select from people").unwrap() { StatementResult::Query { column_names, rows } => { assert!(column_names.is_empty()); assert_eq!(rows, vec![vec![], vec![]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn explain_analyze_buffers_reports_runtime_and_buffers() { let base = temp_dir("explain_analyze_sql"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'alpha')").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "explain (analyze, buffers) select name from people").unwrap() { StatementResult::Query { rows, .. } => { let rendered = rows.into_iter().map(|row| match &row[0] { Value::Text(text) => text.clone(), other => panic!("expected text explain line, got {:?}", other), }).collect::<Vec<_>>(); assert!(rendered.iter().any(|line| line.contains("actual rows="))); assert!(rendered.iter().any(|line| line.contains("Execution Time:"))); assert!(rendered.iter().any(|line| line.contains("Buffers: shared"))); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn inner_join_returns_matching_rows() { let base = temp_dir("join_sql"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let pool = test_pool_with_pets(&base); let xid = txns.begin(); for row in [tuple(1, "alice", Some("alpha")), tuple(2, "bob", None), tuple(3, "carol", Some("storage"))] { let tid = heap_insert_mvcc(&*pool, 1, rel(), xid, &row).unwrap(); heap_flush(&*pool, 1, rel(), tid.block_number).unwrap(); } for row in [pet_tuple(10, "Kitchen", 2), pet_tuple(11, "Mocha", 3)] { let tid = heap_insert_mvcc(&*pool, 1, pets_rel(), xid, &row).unwrap(); heap_flush(&*pool, 1, pets_rel(), tid.block_number).unwrap(); } txns.commit(xid).unwrap(); match run_sql_with_catalog(&base, &txns, INVALID_TRANSACTION_ID, "select people.name, pets.name from people join pets on people.id = pets.owner_id", catalog_with_pets()).unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Text("bob".into()), Value::Text("Kitchen".into())], vec![Value::Text("carol".into()), Value::Text("Mocha".into())]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn cross_join_returns_cartesian_product() { let base = temp_dir("cross_join_sql"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let pool = test_pool_with_pets(&base); let xid = txns.begin(); for row in [tuple(1, "alice", Some("alpha")), tuple(2, "bob", None)] { let tid = heap_insert_mvcc(&*pool, 1, rel(), xid, &row).unwrap(); heap_flush(&*pool, 1, rel(), tid.block_number).unwrap(); } for row in [pet_tuple(10, "Kitchen", 2), pet_tuple(11, "Mocha", 3)] { let tid = heap_insert_mvcc(&*pool, 1, pets_rel(), xid, &row).unwrap(); heap_flush(&*pool, 1, pets_rel(), tid.block_number).unwrap(); } txns.commit(xid).unwrap(); match run_sql_with_catalog(&base, &txns, INVALID_TRANSACTION_ID, "select people.name, pets.name from people, pets", catalog_with_pets()).unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Text("alice".into()), Value::Text("Kitchen".into())], vec![Value::Text("alice".into()), Value::Text("Mocha".into())], vec![Value::Text("bob".into()), Value::Text("Kitchen".into())], vec![Value::Text("bob".into()), Value::Text("Mocha".into())]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn cross_join_where_clause_can_use_addition() { let base = temp_dir("cross_join_addition_sql"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let pool = test_pool_with_pets(&base); let xid = txns.begin(); for row in [tuple(1, "alice", Some("alpha")), tuple(2, "bob", None)] { let tid = heap_insert_mvcc(&*pool, 1, rel(), xid, &row).unwrap(); heap_flush(&*pool, 1, rel(), tid.block_number).unwrap(); } for row in [pet_tuple(10, "Kitchen", 1), pet_tuple(11, "Mocha", 2)] { let tid = heap_insert_mvcc(&*pool, 1, pets_rel(), xid, &row).unwrap(); heap_flush(&*pool, 1, pets_rel(), tid.block_number).unwrap(); } txns.commit(xid).unwrap(); match run_sql_with_catalog(&base, &txns, INVALID_TRANSACTION_ID, "select people.name, pets.name from people, pets where pets.owner_id + 1 = people.id", catalog_with_pets()).unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Text("bob".into()), Value::Text("Kitchen".into())]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn count_star_without_group_by() { let base = temp_dir("count_star"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', null), (3, 'carol', 'c')").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select count(*) from people").unwrap() { StatementResult::Query { column_names, rows } => { assert_eq!(column_names, vec!["count"]); assert_eq!(rows, vec![vec![Value::Int32(3)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn count_star_on_empty_table() { let base = temp_dir("count_star_empty"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select count(*) from people").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Int32(0)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn group_by_with_count() { let base = temp_dir("group_by_count"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', 'a'), (3, 'carol', 'b')").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select note, count(*) from people group by note order by note").unwrap() { StatementResult::Query { column_names, rows } => { assert_eq!(column_names, vec!["note", "count"]); assert_eq!(rows, vec![vec![Value::Text("a".into()), Value::Int32(2)], vec![Value::Text("b".into()), Value::Int32(1)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn sum_avg_min_max_aggregates() { let base = temp_dir("sum_avg_min_max"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (10, 'alice', 'a'), (20, 'bob', 'b'), (30, 'carol', 'c')").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select sum(id), avg(id), min(id), max(id) from people").unwrap() { StatementResult::Query { column_names, rows } => { assert_eq!(column_names, vec!["sum", "avg", "min", "max"]); assert_eq!(rows, vec![vec![Value::Int32(60), Value::Int32(20), Value::Int32(10), Value::Int32(30)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn having_filters_groups() { let base = temp_dir("having_filter"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', 'a'), (3, 'carol', 'b')").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select note, count(*) from people group by note having count(*) > 1").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Text("a".into()), Value::Int32(2)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn count_expr_skips_nulls() { let base = temp_dir("count_expr_nulls"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', null), (3, 'carol', 'c')").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select count(note) from people").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Int32(2)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn sum_of_all_nulls_returns_null() { let base = temp_dir("sum_all_nulls"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', null), (2, 'bob', null)").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select min(note), max(note) from people").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Null, Value::Null]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn null_group_by_keys_are_grouped_together() { let base = temp_dir("null_group_keys"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', null), (2, 'bob', 'a'), (3, 'carol', null), (4, 'dave', 'a')").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select note, count(*) from people group by note order by note").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows.len(), 2); assert_eq!(rows[0], vec![Value::Text("a".into()), Value::Int32(2)]); assert_eq!(rows[1], vec![Value::Null, Value::Int32(2)]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn sum_and_avg_skip_nulls() { let base = temp_dir("sum_avg_skip_nulls"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (10, 'alice', 'a'), (20, 'bob', null), (30, 'carol', 'c')").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select count(*), count(note), sum(id), avg(id), min(id), max(id) from people").unwrap() { StatementResult::Query { column_names, rows } => { assert_eq!(column_names, vec!["count", "count", "sum", "avg", "min", "max"]); assert_eq!(rows, vec![vec![Value::Int32(3), Value::Int32(2), Value::Int32(60), Value::Int32(20), Value::Int32(10), Value::Int32(30)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn ungrouped_column_is_rejected() { let base = temp_dir("ungrouped_column"); let txns = TransactionManager::new_durable(&base).unwrap(); let result = run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select name, count(*) from people"); assert!(result.is_err()); }
    #[test] fn aggregate_in_where_is_rejected() { let base = temp_dir("agg_in_where"); let txns = TransactionManager::new_durable(&base).unwrap(); let result = run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select name from people where count(*) > 1"); assert!(result.is_err()); }
    #[test] fn explain_shows_aggregate_node() { let base = temp_dir("explain_agg"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "explain select note, count(*) from people group by note").unwrap() { StatementResult::Query { rows, .. } => { let rendered = rows.into_iter().map(|row| match &row[0] { Value::Text(text) => text.clone(), other => panic!("expected text, got {:?}", other), }).collect::<Vec<_>>(); assert!(rendered.iter().any(|line| line.contains("Aggregate"))); assert!(rendered.iter().any(|line| line.contains("Seq Scan"))); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn group_by_with_order_by_and_limit() { let base = temp_dir("group_by_order_limit"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'x'), (2, 'bob', 'y'), (3, 'carol', 'x'), (4, 'dave', 'y'), (5, 'eve', 'z')").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select note, count(*) from people group by note order by count(*) desc limit 2").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Text("x".into()), Value::Int32(2)], vec![Value::Text("y".into()), Value::Int32(2)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn random_returns_float_in_range() { let base = temp_dir("random_func"); let txns = TransactionManager::new_durable(&base).unwrap(); for _ in 0..10 { match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select random()").unwrap() { StatementResult::Query { column_names, rows } => { assert_eq!(column_names, vec!["random".to_string()]); assert_eq!(rows.len(), 1); match &rows[0][0] { Value::Float64(v) => assert!(*v >= 0.0 && *v < 1.0, "random() must be in [0,1), got {v}"), other => panic!("expected Float64, got {:?}", other), } } other => panic!("expected query result, got {:?}", other), } } }
}
