use std::rc::Rc;

use parking_lot::RwLock;

use crate::access::heap::am::{
    HeapError, heap_delete_with_waiter, heap_fetch, heap_insert_mvcc_with_cid,
    heap_scan_begin_visible, heap_scan_end,
    heap_scan_page_next_tuple, heap_scan_prepare_next_page,
    heap_update_with_waiter,
};
use crate::access::heap::mvcc::{TransactionId, TransactionManager};
use crate::access::heap::mvcc::CommandId;
use crate::catalog::Catalog;
use crate::database::TransactionWaiter;
use crate::parser::{
    BoundDeleteStatement, BoundInsertStatement, BoundUpdateStatement, DropTableStatement,
    ExplainStatement, ParseError, Statement, TruncateTableStatement, bind_create_table, build_plan,
};
use crate::storage::smgr::ForkNumber;
use crate::storage::smgr::StorageManager;

use super::nodes::*;
use super::expr::{eval_expr, compile_predicate_with_decoder, tuple_from_values};
use super::explain::{format_buffer_usage, format_explain_lines};
use super::tuple_decoder::CompiledTupleDecoder;
use super::{ExecError, ExecutorContext, StatementResult, executor_start};

pub(crate) fn execute_explain(
    stmt: ExplainStatement,
    catalog: &Catalog,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    let Statement::Select(select) = *stmt.statement else {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "SELECT statement after EXPLAIN",
            actual: "non-select statement".into(),
        }));
    };

    let plan_start = std::time::Instant::now();
    let plan = build_plan(&select, catalog)?;
    let mut lines = Vec::new();
    if stmt.analyze {
        ctx.pool.reset_usage_stats();
        ctx.timed = stmt.timing;
        let mut state = executor_start(plan);
        let plan_elapsed = plan_start.elapsed();
        let mut row_count: u64 = 0;
        let started_at = std::time::Instant::now();
        while let Some(_slot) = state.exec_proc_node(ctx)? {
            row_count += 1;
        }
        ctx.timed = false;
        let elapsed = started_at.elapsed();
        format_explain_lines(state.as_ref(), 0, true, &mut lines);
        lines.push(format!("Planning Time: {:.3} ms", plan_elapsed.as_secs_f64() * 1000.0));
        lines.push(format!("Execution Time: {:.3} ms", elapsed.as_secs_f64() * 1000.0));
        if stmt.buffers {
            let stats = ctx.pool.usage_stats();
            lines.push(format_buffer_usage(stats));
        }
        lines.push(format!("Result Rows: {}", row_count));
    } else {
        let state = executor_start(plan);
        format_explain_lines(state.as_ref(), 0, false, &mut lines);
    }

    Ok(StatementResult::Query {
        columns: vec![QueryColumn::text("QUERY PLAN")],
        column_names: vec!["QUERY PLAN".into()],
        rows: lines.into_iter().map(|line| vec![Value::Text(line.into())]).collect(),
    })
}

pub fn execute_show_tables(catalog: &Catalog) -> Result<StatementResult, ExecError> {
    Ok(StatementResult::Query {
        columns: vec![QueryColumn::text("table_name")],
        column_names: vec!["table_name".into()],
        rows: catalog
            .table_names()
            .map(|name| vec![Value::Text(name.to_string().into())])
            .collect(),
    })
}

pub fn execute_create_table(
    stmt: crate::parser::CreateTableStatement,
    catalog: &mut Catalog,
) -> Result<StatementResult, ExecError> {
    let _entry = bind_create_table(&stmt, catalog)?;
    Ok(StatementResult::AffectedRows(0))
}

pub fn execute_drop_table(
    stmt: DropTableStatement,
    catalog: &mut Catalog,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    let mut dropped = 0;
    for table_name in stmt.table_names {
        match catalog.drop_table(&table_name) {
            Ok(entry) => {
                let _ = ctx.pool.invalidate_relation(entry.rel);
                ctx.pool.with_storage_mut(|s| s.smgr.unlink(entry.rel, None, false));
                dropped += 1;
            }
            Err(crate::catalog::CatalogError::UnknownTable(name)) if stmt.if_exists => {
                let _ = name;
            }
            Err(crate::catalog::CatalogError::UnknownTable(name)) => {
                return Err(ExecError::Parse(ParseError::TableDoesNotExist(name)));
            }
            Err(other) => {
                return Err(ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "droppable table",
                    actual: format!("{other:?}"),
                }));
            }
        }
    }
    Ok(StatementResult::AffectedRows(dropped))
}

pub fn execute_truncate_table(
    stmt: TruncateTableStatement,
    catalog: &Catalog,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    for table_name in stmt.table_names {
        let entry = catalog
            .get(&table_name)
            .ok_or_else(|| ExecError::Parse(ParseError::UnknownTable(table_name.clone())))?;
        let _ = ctx.pool.invalidate_relation(entry.rel);
        ctx.pool
            .with_storage_mut(|s| s.smgr.truncate(entry.rel, ForkNumber::Main, 0))
            .map_err(HeapError::Storage)?;
    }
    Ok(StatementResult::AffectedRows(0))
}

pub fn execute_insert(
    stmt: BoundInsertStatement,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<StatementResult, ExecError> {
    let values = stmt
        .values
        .iter()
        .map(|row| {
            let mut slot = TupleSlot::virtual_row(vec![Value::Null; stmt.desc.columns.len()]);
            let mut values = vec![Value::Null; stmt.desc.columns.len()];
            for (column_index, expr) in stmt.target_columns.iter().zip(row.iter()) {
                values[*column_index] = eval_expr(expr, &mut slot, ctx)?;
            }
            Ok(values)
        })
        .collect::<Result<Vec<_>, ExecError>>()?;

    let inserted = execute_insert_values(stmt.rel, &stmt.desc, &values, ctx, xid, cid)?;
    Ok(StatementResult::AffectedRows(inserted))
}

pub fn execute_insert_values(
    rel: crate::storage::smgr::RelFileLocator,
    desc: &RelationDesc,
    rows: &[Vec<Value>],
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<usize, ExecError> {
    for values in rows {
        let tuple = tuple_from_values(desc, values)?;
        heap_insert_mvcc_with_cid(&*ctx.pool, ctx.client_id, rel, xid, cid, &tuple)?;
    }

    Ok(rows.len())
}

/// Execute a single-row insert from a prepared insert plan and parameter values.
/// This skips parsing, binding, and expression evaluation entirely.
pub fn execute_prepared_insert_row(
    prepared: &crate::parser::PreparedInsert,
    params: &[Value],
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<(), ExecError> {
    let mut values = vec![Value::Null; prepared.desc.columns.len()];
    for (column_index, param) in prepared.target_columns.iter().zip(params.iter()) {
        values[*column_index] = param.clone();
    }
    let tuple = tuple_from_values(&prepared.desc, &values)?;
    heap_insert_mvcc_with_cid(&*ctx.pool, ctx.client_id, prepared.rel, xid, cid, &tuple)?;
    Ok(())
}

pub(crate) fn execute_update(
    stmt: BoundUpdateStatement,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<StatementResult, ExecError> {
    execute_update_with_waiter(stmt, ctx, xid, cid, None)
}

pub fn execute_update_with_waiter(
    stmt: BoundUpdateStatement,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
    waiter: Option<(&RwLock<TransactionManager>, &TransactionWaiter)>,
) -> Result<StatementResult, ExecError> {
    let mut scan = heap_scan_begin_visible(&ctx.pool, ctx.client_id, stmt.rel, ctx.snapshot.clone())?;
    let mut affected_rows = 0;

    // Hoist descriptor allocation, decoder compilation, and predicate
    // compilation out of the per-tuple loop.
    let desc = Rc::new(stmt.desc.clone());
    let attr_descs: Rc<[_]> = desc.attribute_descs().into();
    let decoder = Rc::new(CompiledTupleDecoder::compile(&desc, &attr_descs));
    // Compiled predicate: uses the fixed-offset fast path for BufferHeapTuple
    // and falls back for HeapTuple.
    let qual = stmt.predicate.as_ref().map(|p| compile_predicate_with_decoder(p, &decoder));

    // Reusable slot — allocated once, reset per tuple (like PG's ss_ScanTupleSlot).
    let mut slot = TupleSlot::empty(decoder.ncols());
    slot.decoder = Some(Rc::clone(&decoder));

    // Page-mode scan: batch visibility checks per page under a single lock,
    // matching the SELECT path (heap_scan_prepare_next_page). This replaces
    // the old per-tuple txns.read() which caused 19% lock contention.
    loop {
        let next: Result<Option<usize>, ExecError> =
            heap_scan_prepare_next_page(&*ctx.pool, ctx.client_id, &ctx.txns, &mut scan);
        let Some(buffer_id) = next? else { break; };

        // SAFETY: buffer is pinned, visibility offsets were collected under
        // lock in prepare_next_page, and tuple user data is immutable.
        let page = unsafe { ctx.pool.page_unlocked(buffer_id) }
            .expect("pinned buffer must be valid");

        let pin = scan.pinned_buffer_rc().expect("buffer must be pinned after prepare_next_page");

        let mut page_rows = Vec::new();
        while let Some((tid, tuple_bytes)) = heap_scan_page_next_tuple(page, &mut scan) {
            // Materialize page rows before expression evaluation so the page
            // borrow can end; correlated subqueries need mutable access to ctx.
            slot.kind = SlotKind::BufferHeapTuple {
                tuple_ptr: tuple_bytes.as_ptr(),
                tuple_len: tuple_bytes.len(),
                pin: Rc::clone(&pin),
            };
            slot.tts_nvalid = 0;
            slot.tts_values.clear();
            slot.decode_offset = 0;
            slot.values()?;
            Value::materialize_all(&mut slot.tts_values);
            page_rows.push((tid, slot.tts_values.clone()));
        }
        drop(pin);

        for (tid, original_values) in page_rows {
            let mut slot = TupleSlot::virtual_row(original_values.clone());
            if let Some(q) = &qual {
                if !q(&mut slot, ctx)? { continue; }
            }
            let mut eval_slot = TupleSlot::virtual_row(original_values.clone());
            let mut values = original_values;
            for assignment in &stmt.assignments {
                values[assignment.column_index] = eval_expr(&assignment.expr, &mut eval_slot, ctx)?;
            }

            let replacement = tuple_from_values(&stmt.desc, &values)?;
            let mut current_tid = tid;
            let mut current_replacement = replacement;
            loop {
                match heap_update_with_waiter(
                    &*ctx.pool,
                    ctx.client_id,
                    stmt.rel,
                    &ctx.txns,
                    xid,
                    cid,
                    current_tid,
                    &current_replacement,
                    waiter,
                ) {
                    Ok(new_tid) => {
                        let _ = new_tid;
                        affected_rows += 1;
                        break;
                    }
                    Err(HeapError::TupleUpdated(_old_tid, new_ctid)) => {
                        let new_tuple = heap_fetch(&*ctx.pool, ctx.client_id, stmt.rel, new_ctid)?;
                        let mut new_slot = TupleSlot::from_heap_tuple(
                            Rc::clone(&desc), Rc::clone(&attr_descs), new_ctid, new_tuple,
                        );
                        let passes = match &qual { Some(q) => q(&mut new_slot, ctx)?, None => true };
                        if !passes {
                            break;
                        }
                        let new_values_base = new_slot.into_values()?;
                        let mut eval_slot = TupleSlot::virtual_row(new_values_base.clone());
                        let mut new_values = new_values_base;
                        for assignment in &stmt.assignments {
                            new_values[assignment.column_index] =
                                eval_expr(&assignment.expr, &mut eval_slot, ctx)?;
                        }
                        current_replacement = tuple_from_values(&stmt.desc, &new_values)?;
                        current_tid = new_ctid;
                    }
                    Err(HeapError::TupleAlreadyModified(_)) => {
                        break;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        }
    }

    heap_scan_end::<ExecError>(&*ctx.pool, ctx.client_id, &mut scan)?;
    Ok(StatementResult::AffectedRows(affected_rows))
}

pub(crate) fn execute_delete(
    stmt: BoundDeleteStatement,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
) -> Result<StatementResult, ExecError> {
    execute_delete_with_waiter(stmt, ctx, xid, None)
}

pub fn execute_delete_with_waiter(
    stmt: BoundDeleteStatement,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    waiter: Option<(&RwLock<TransactionManager>, &TransactionWaiter)>,
) -> Result<StatementResult, ExecError> {
    let mut scan = heap_scan_begin_visible(&ctx.pool, ctx.client_id, stmt.rel, ctx.snapshot.clone())?;
    let mut targets = Vec::new();

    // Hoist descriptor allocation, decoder compilation, and predicate
    // compilation out of the per-tuple loop.
    let desc = Rc::new(stmt.desc.clone());
    let attr_descs: Rc<[_]> = desc.attribute_descs().into();
    let decoder = Rc::new(CompiledTupleDecoder::compile(&desc, &attr_descs));
    let qual = stmt.predicate.as_ref().map(|p| compile_predicate_with_decoder(p, &decoder));

    // Reusable slot — allocated once, reset per tuple.
    let mut slot = TupleSlot::empty(decoder.ncols());
    slot.decoder = Some(Rc::clone(&decoder));

    // Page-mode scan: batch visibility checks per page under a single lock.
    loop {
        let next: Result<Option<usize>, ExecError> =
            heap_scan_prepare_next_page(&*ctx.pool, ctx.client_id, &ctx.txns, &mut scan);
        let Some(buffer_id) = next? else { break; };

        let page = unsafe { ctx.pool.page_unlocked(buffer_id) }
            .expect("pinned buffer must be valid");

        let pin = scan.pinned_buffer_rc().expect("buffer must be pinned after prepare_next_page");

        let mut page_rows = Vec::new();
        while let Some((tid, tuple_bytes)) = heap_scan_page_next_tuple(page, &mut scan) {
            slot.kind = SlotKind::BufferHeapTuple {
                tuple_ptr: tuple_bytes.as_ptr(),
                tuple_len: tuple_bytes.len(),
                pin: Rc::clone(&pin),
            };
            slot.tts_nvalid = 0;
            slot.tts_values.clear();
            slot.decode_offset = 0;
            slot.values()?;
            Value::materialize_all(&mut slot.tts_values);
            page_rows.push((tid, slot.tts_values.clone()));
        }
        drop(pin);

        for (tid, values) in page_rows {
            let mut slot = TupleSlot::virtual_row(values);
            if let Some(q) = &qual {
                if !q(&mut slot, ctx)? { continue; }
            }
            targets.push(tid);
        }
    }

    // Use the scan snapshot for visibility checks in the delete phase so that
    // rows committed by other transactions after the scan still appear visible
    // (enabling the correct TupleAlreadyModified path rather than TupleNotVisible).
    let snapshot = scan.snapshot.clone();
    heap_scan_end::<ExecError>(&*ctx.pool, ctx.client_id, &mut scan)?;

    let mut affected_rows = 0;
    for tid in &targets {
        let mut current_tid = *tid;
        loop {
            match heap_delete_with_waiter(&*ctx.pool, ctx.client_id, stmt.rel, &ctx.txns, xid, current_tid, &snapshot, waiter) {
                Ok(()) => {
                    affected_rows += 1;
                    break;
                }
                // Row was concurrently deleted — skip it.
                Err(HeapError::TupleAlreadyModified(_)) => { break; }
                // Row was concurrently updated — follow ctid chain, recheck
                // predicate, and retry. Matches PostgreSQL's ExecDelete.
                Err(HeapError::TupleUpdated(_old_tid, new_ctid)) => {
                    let new_tuple = heap_fetch(&*ctx.pool, ctx.client_id, stmt.rel, new_ctid)?;
                    let mut new_slot = TupleSlot::from_heap_tuple(
                        Rc::clone(&desc), Rc::clone(&attr_descs), new_ctid, new_tuple,
                    );
                    let passes = match &qual { Some(q) => q(&mut new_slot, ctx)?, None => true };
                    if !passes {
                        // Concurrent update changed the row so it no longer
                        // matches our WHERE — skip it.
                        break;
                    }
                    current_tid = new_ctid;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    Ok(StatementResult::AffectedRows(affected_rows))
}
