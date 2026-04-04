use std::rc::Rc;

use parking_lot::RwLock;

use crate::access::heap::am::{
    HeapError, heap_delete_with_waiter, heap_fetch, heap_flush, heap_insert_mvcc_with_cid,
    heap_scan_begin_visible, heap_scan_next_visible, heap_update_with_waiter,
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
use super::expr::{eval_expr, predicate_matches, tuple_from_values};
use super::explain::{format_buffer_usage, format_explain_lines};
use super::{ExecError, ExecutorContext, QueryResult, StatementResult, execute_plan_internal, executor_start};

pub(crate) fn execute_explain(
    stmt: ExplainStatement,
    catalog: &Catalog,
    ctx: &mut ExecutorContext<'_>,
) -> Result<StatementResult, ExecError> {
    let Statement::Select(select) = *stmt.statement else {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "SELECT statement after EXPLAIN",
            actual: "non-select statement".into(),
        }));
    };

    let plan = build_plan(&select, catalog)?;
    let mut lines = Vec::new();
    if stmt.analyze {
        ctx.pool.reset_usage_stats();
        let (result, state, elapsed) = execute_plan_internal(plan, ctx, true)?;
        format_explain_lines(&state, 0, true, &mut lines);
        lines.push(format!("Execution Time: {:.3} ms", elapsed.as_secs_f64() * 1000.0));
        if stmt.buffers {
            let stats = ctx.pool.usage_stats();
            lines.push(format_buffer_usage(stats));
        }
        if let StatementResult::Query(qr) = &result {
            lines.push(format!("Result Rows: {}", qr.row_count()));
        }
    } else {
        let state = executor_start(plan);
        format_explain_lines(&state, 0, false, &mut lines);
    }

    let nrows = lines.len();
    let values: Vec<Value> = lines.into_iter().map(|line| Value::Text(line.into())).collect();
    Ok(StatementResult::Query(QueryResult::new(
        vec!["QUERY PLAN".into()],
        1,
        nrows,
        values,
    )))
}

pub fn execute_show_tables(catalog: &Catalog) -> Result<StatementResult, ExecError> {
    let values: Vec<Value> = catalog
        .table_names()
        .map(|name| Value::Text(name.to_string().into()))
        .collect();
    let nrows = values.len();
    Ok(StatementResult::Query(QueryResult::new(
        vec!["table_name".into()],
        1,
        nrows,
        values,
    )))
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
    ctx: &mut ExecutorContext<'_>,
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
    ctx: &mut ExecutorContext<'_>,
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
    ctx: &mut ExecutorContext<'_>,
    xid: TransactionId,
    cid: CommandId,
) -> Result<StatementResult, ExecError> {
    let values = stmt
        .values
        .iter()
        .map(|row| {
            let column_names: Vec<String> =
                stmt.desc.columns.iter().map(|c| c.name.clone()).collect();
            let mut slot =
                TupleSlot::virtual_row(column_names.into(), vec![Value::Null; stmt.desc.columns.len()]);
            let mut values = vec![Value::Null; stmt.desc.columns.len()];
            for (column_index, expr) in stmt.target_indexes.iter().zip(row.iter()) {
                values[*column_index] = eval_expr(expr, &mut slot)?;
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
    ctx: &mut ExecutorContext<'_>,
    xid: TransactionId,
    cid: CommandId,
) -> Result<usize, ExecError> {
    let mut touched_blocks = std::collections::BTreeSet::new();

    for values in rows {
        let tuple = tuple_from_values(desc, values)?;
        let tid = heap_insert_mvcc_with_cid(ctx.pool, ctx.client_id, rel, xid, cid, &tuple)?;
        touched_blocks.insert(tid.block_number);
    }

    for block_number in touched_blocks {
        heap_flush(ctx.pool, ctx.client_id, rel, block_number)?;
    }

    Ok(rows.len())
}

pub(crate) fn execute_update(
    stmt: BoundUpdateStatement,
    ctx: &mut ExecutorContext<'_>,
    xid: TransactionId,
    cid: CommandId,
) -> Result<StatementResult, ExecError> {
    execute_update_with_waiter(stmt, ctx, xid, cid, None)
}

pub fn execute_update_with_waiter(
    stmt: BoundUpdateStatement,
    ctx: &mut ExecutorContext<'_>,
    xid: TransactionId,
    cid: CommandId,
    waiter: Option<(&RwLock<TransactionManager>, &TransactionWaiter)>,
) -> Result<StatementResult, ExecError> {
    let mut scan = heap_scan_begin_visible(ctx.pool, stmt.rel, ctx.snapshot.clone())?;
    let mut affected_rows = 0;

    while let Some((tid, tuple)) = {
        let txns_guard = ctx.txns.read();
        heap_scan_next_visible(ctx.pool, ctx.client_id, &txns_guard, &mut scan)?
    } {
        let desc = Rc::new(stmt.desc.clone());
        let attr_descs: Rc<[_]> = desc.attribute_descs().into();
        let col_names: Rc<[String]> = desc.columns.iter().map(|c| c.name.clone()).collect();
        let mut slot = TupleSlot::from_heap_tuple(desc, attr_descs, Rc::clone(&col_names), tid, tuple);
        if !predicate_matches(stmt.predicate.as_ref(), &mut slot)? {
            continue;
        }
        let original_values = slot.into_values()?;
        let mut eval_slot = TupleSlot::virtual_row(
            col_names,
            original_values.clone(),
        );
        let mut values = original_values;
        for assignment in &stmt.assignments {
            values[assignment.column_index] = eval_expr(&assignment.expr, &mut eval_slot)?;
        }

        let replacement = tuple_from_values(&stmt.desc, &values)?;
        let mut current_tid = tid;
        let mut current_replacement = replacement;
        loop {
            match heap_update_with_waiter(
                ctx.pool,
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
                    heap_flush(ctx.pool, ctx.client_id, stmt.rel, current_tid.block_number)?;
                    if new_tid.block_number != current_tid.block_number {
                        heap_flush(ctx.pool, ctx.client_id, stmt.rel, new_tid.block_number)?;
                    }
                    affected_rows += 1;
                    break;
                }
                Err(HeapError::TupleUpdated(_old_tid, new_ctid)) => {
                    let new_tuple = heap_fetch(ctx.pool, ctx.client_id, stmt.rel, new_ctid)?;
                    let desc = Rc::new(stmt.desc.clone());
                    let attr_descs: Rc<[_]> = desc.attribute_descs().into();
                    let col_names: Rc<[String]> = desc.columns.iter().map(|c| c.name.clone()).collect();
                    let mut new_slot = TupleSlot::from_heap_tuple(
                        desc,
                        attr_descs,
                        Rc::clone(&col_names),
                        new_ctid,
                        new_tuple,
                    );
                    if !predicate_matches(stmt.predicate.as_ref(), &mut new_slot)? {
                        break;
                    }
                    let new_values_base = new_slot.into_values()?;
                    let mut eval_slot = TupleSlot::virtual_row(
                        col_names,
                        new_values_base.clone(),
                    );
                    let mut new_values = new_values_base;
                    for assignment in &stmt.assignments {
                        new_values[assignment.column_index] =
                            eval_expr(&assignment.expr, &mut eval_slot)?;
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

    Ok(StatementResult::AffectedRows(affected_rows))
}

pub(crate) fn execute_delete(
    stmt: BoundDeleteStatement,
    ctx: &mut ExecutorContext<'_>,
    xid: TransactionId,
) -> Result<StatementResult, ExecError> {
    execute_delete_with_waiter(stmt, ctx, xid, None)
}

pub fn execute_delete_with_waiter(
    stmt: BoundDeleteStatement,
    ctx: &mut ExecutorContext<'_>,
    xid: TransactionId,
    waiter: Option<(&RwLock<TransactionManager>, &TransactionWaiter)>,
) -> Result<StatementResult, ExecError> {
    let mut scan = heap_scan_begin_visible(ctx.pool, stmt.rel, ctx.snapshot.clone())?;
    let mut targets = Vec::new();

    while let Some((tid, tuple)) = {
        let txns_guard = ctx.txns.read();
        heap_scan_next_visible(ctx.pool, ctx.client_id, &txns_guard, &mut scan)?
    } {
        let desc = Rc::new(stmt.desc.clone());
        let attr_descs: Rc<[_]> = desc.attribute_descs().into();
        let col_names: Rc<[String]> = desc.columns.iter().map(|c| c.name.clone()).collect();
        let mut slot = TupleSlot::from_heap_tuple(desc, attr_descs, col_names, tid, tuple);
        if !predicate_matches(stmt.predicate.as_ref(), &mut slot)? {
            continue;
        }
        targets.push(tid);
    }

    for tid in &targets {
        heap_delete_with_waiter(ctx.pool, ctx.client_id, stmt.rel, &ctx.txns, xid, *tid, waiter)?;
        heap_flush(ctx.pool, ctx.client_id, stmt.rel, tid.block_number)?;
    }

    Ok(StatementResult::AffectedRows(targets.len()))
}
