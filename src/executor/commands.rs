use crate::access::heap::am::{
    heap_delete, heap_flush, heap_insert_mvcc_with_cid, heap_scan_begin_visible,
    heap_scan_next_visible, heap_update_with_cid,
};
use crate::access::heap::mvcc::TransactionId;
use crate::access::heap::mvcc::CommandId;
use crate::catalog::Catalog;
use crate::parser::{
    BoundDeleteStatement, BoundInsertStatement, BoundUpdateStatement, DropTableStatement,
    ExplainStatement, ParseError, Statement, bind_create_table, build_plan,
};
use crate::storage::smgr::StorageManager;

use super::nodes::*;
use super::expr::{eval_expr, predicate_matches, tuple_from_values};
use super::explain::{format_buffer_usage, format_explain_lines};
use super::{ExecError, ExecutorContext, StatementResult, execute_plan_internal, executor_start};

pub(crate) fn execute_explain(
    stmt: ExplainStatement,
    catalog: &mut Catalog,
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
        let (result, state, elapsed) = execute_plan_internal(plan, ctx)?;
        format_explain_lines(&state, 0, true, &mut lines);
        lines.push(format!("Execution Time: {:.3} ms", elapsed.as_secs_f64() * 1000.0));
        if stmt.buffers {
            let stats = ctx.pool.usage_stats();
            lines.push(format_buffer_usage(stats));
        }
        if let StatementResult::Query { rows, .. } = result {
            lines.push(format!("Result Rows: {}", rows.len()));
        }
    } else {
        let state = executor_start(plan);
        format_explain_lines(&state, 0, false, &mut lines);
    }

    Ok(StatementResult::Query {
        column_names: vec!["QUERY PLAN".into()],
        rows: lines.into_iter().map(|line| vec![Value::Text(line)]).collect(),
    })
}

pub(crate) fn execute_show_tables(catalog: &Catalog) -> Result<StatementResult, ExecError> {
    Ok(StatementResult::Query {
        column_names: vec!["table_name".into()],
        rows: catalog
            .table_names()
            .map(|name| vec![Value::Text(name.to_string())])
            .collect(),
    })
}

pub(crate) fn execute_create_table(
    stmt: crate::parser::CreateTableStatement,
    catalog: &mut Catalog,
) -> Result<StatementResult, ExecError> {
    let _entry = bind_create_table(&stmt, catalog)?;
    Ok(StatementResult::AffectedRows(0))
}

pub(crate) fn execute_drop_table(
    stmt: DropTableStatement,
    catalog: &mut Catalog,
    ctx: &mut ExecutorContext<'_>,
) -> Result<StatementResult, ExecError> {
    let entry = catalog
        .drop_table(&stmt.table_name)
        .map_err(|err| match err {
            crate::catalog::CatalogError::UnknownTable(name) => ExecError::Parse(ParseError::TableDoesNotExist(name)),
            other => ExecError::Parse(ParseError::UnexpectedToken {
                expected: "droppable table",
                actual: format!("{other:?}"),
            }),
        })?;

    let _ = ctx.pool.invalidate_relation(entry.rel);
    ctx.pool.with_storage_mut(|s| s.smgr.unlink(entry.rel, None, false));
    Ok(StatementResult::AffectedRows(0))
}

pub(crate) fn execute_insert(
    stmt: BoundInsertStatement,
    ctx: &mut ExecutorContext<'_>,
    xid: TransactionId,
    cid: CommandId,
) -> Result<StatementResult, ExecError> {
    let column_names: Vec<String> = stmt.desc.columns.iter().map(|c| c.name.clone()).collect();
    let mut touched_blocks = std::collections::BTreeSet::new();

    for row in &stmt.values {
        let mut slot =
            TupleSlot::virtual_row(column_names.clone(), vec![Value::Null; stmt.desc.columns.len()]);
        let mut values = vec![Value::Null; stmt.desc.columns.len()];
        for (column_index, expr) in stmt.target_indexes.iter().zip(row.iter()) {
            values[*column_index] = eval_expr(expr, &mut slot)?;
        }

        let tuple = tuple_from_values(&stmt.desc, &values)?;
        let tid = heap_insert_mvcc_with_cid(ctx.pool, ctx.client_id, stmt.rel, xid, cid, &tuple)?;
        touched_blocks.insert(tid.block_number);
    }

    for block_number in touched_blocks {
        heap_flush(ctx.pool, ctx.client_id, stmt.rel, block_number)?;
    }

    Ok(StatementResult::AffectedRows(stmt.values.len()))
}

pub(crate) fn execute_update(
    stmt: BoundUpdateStatement,
    ctx: &mut ExecutorContext<'_>,
    xid: TransactionId,
    cid: CommandId,
) -> Result<StatementResult, ExecError> {
    let mut scan = heap_scan_begin_visible(ctx.pool, stmt.rel, ctx.snapshot.clone())?;
    let mut affected_rows = 0;

    while let Some((tid, tuple)) =
        heap_scan_next_visible(ctx.pool, ctx.client_id, ctx.txns, &mut scan)?
    {
        let mut slot = TupleSlot::from_heap_tuple(stmt.desc.clone(), tid, tuple);
        if !predicate_matches(stmt.predicate.as_ref(), &mut slot)? {
            continue;
        }
        let original_values = slot.into_values()?;
        let mut eval_slot = TupleSlot::virtual_row(
            stmt.desc.columns.iter().map(|c| c.name.clone()).collect(),
            original_values.clone(),
        );
        let mut values = original_values;
        for assignment in &stmt.assignments {
            values[assignment.column_index] = eval_expr(&assignment.expr, &mut eval_slot)?;
        }

        let replacement = tuple_from_values(&stmt.desc, &values)?;
        let new_tid = heap_update_with_cid(
            ctx.pool,
            ctx.client_id,
            stmt.rel,
            ctx.txns,
            xid,
            cid,
            tid,
            &replacement,
        )?;
        heap_flush(ctx.pool, ctx.client_id, stmt.rel, tid.block_number)?;
        if new_tid.block_number != tid.block_number {
            heap_flush(ctx.pool, ctx.client_id, stmt.rel, new_tid.block_number)?;
        }
        affected_rows += 1;
    }

    Ok(StatementResult::AffectedRows(affected_rows))
}

pub(crate) fn execute_delete(
    stmt: BoundDeleteStatement,
    ctx: &mut ExecutorContext<'_>,
    xid: TransactionId,
) -> Result<StatementResult, ExecError> {
    let mut scan = heap_scan_begin_visible(ctx.pool, stmt.rel, ctx.snapshot.clone())?;
    let mut targets = Vec::new();

    while let Some((tid, tuple)) =
        heap_scan_next_visible(ctx.pool, ctx.client_id, ctx.txns, &mut scan)?
    {
        let mut slot = TupleSlot::from_heap_tuple(stmt.desc.clone(), tid, tuple);
        if !predicate_matches(stmt.predicate.as_ref(), &mut slot)? {
            continue;
        }
        targets.push(tid);
    }

    for tid in &targets {
        heap_delete(ctx.pool, ctx.client_id, stmt.rel, ctx.txns, xid, *tid)?;
        heap_flush(ctx.pool, ctx.client_id, stmt.rel, tid.block_number)?;
    }

    Ok(StatementResult::AffectedRows(targets.len()))
}
