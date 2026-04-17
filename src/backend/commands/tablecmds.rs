use std::collections::HashSet;
use std::rc::Rc;

use parking_lot::RwLock;

use crate::backend::access::heap::heapam::{
    HeapError, heap_delete_with_waiter, heap_fetch, heap_fetch_visible_with_txns,
    heap_insert_mvcc_with_cid, heap_scan_begin_visible, heap_scan_end, heap_scan_page_next_tuple,
    heap_scan_prepare_next_page, heap_update_with_waiter,
};
use crate::backend::access::heap::heaptoast::{
    StoredToastValue, cleanup_new_toast_value, delete_external_from_tuple, encoded_pointer_bytes,
    store_external_value,
};
use crate::backend::access::index::indexam;
use crate::backend::access::transam::xact::CommandId;
use crate::backend::access::transam::xact::{TransactionId, TransactionManager};
use crate::backend::optimizer::{finalize_expr_subqueries, planner};
use crate::backend::parser::{
    AnalyzeStatement, BoundArraySubscript, BoundAssignment, BoundAssignmentTarget,
    BoundDeleteStatement, BoundIndexRelation, BoundInsertSource, BoundInsertStatement,
    BoundModifyRowSource, BoundUpdateStatement, Catalog, CatalogLookup, DropTableStatement,
    ExplainStatement, MaintenanceTarget, ParseError, SqlType, SqlTypeKind, Statement,
    TruncateTableStatement, VacuumStatement, bind_create_table,
};
use crate::backend::rewrite::pg_rewrite_query;
use crate::backend::storage::smgr::ForkNumber;
use crate::backend::storage::smgr::StorageManager;
use crate::backend::utils::time::instant::Instant;
use crate::pgrust::database::TransactionWaiter;

use super::explain::{format_buffer_usage, format_explain_lines};
use crate::backend::executor::exec_expr::{compile_predicate_with_decoder, eval_expr};
use crate::backend::executor::exec_tuples::CompiledTupleDecoder;
use crate::backend::executor::value_io::{coerce_assignment_value, encode_tuple_values};
use crate::backend::executor::{
    ExecError, ExecutorContext, Expr, StatementResult, ToastRelationRef, create_query_desc,
    executor_start,
};
use crate::backend::storage::page::bufpage::MAX_HEAP_TUPLE_SIZE;
use crate::include::access::detoast::is_ondisk_toast_pointer;
use crate::include::access::htup::{HeapTuple, TupleValue};
use crate::include::access::itemptr::ItemPointerData;
use crate::include::nodes::datum::{ArrayDimension, ArrayValue, array_value_from_value};
use crate::include::nodes::execnodes::*;

fn finalize_bound_insert(
    mut stmt: BoundInsertStatement,
    catalog: &dyn CatalogLookup,
) -> BoundInsertStatement {
    let mut subplans = Vec::new();
    stmt.column_defaults = stmt
        .column_defaults
        .into_iter()
        .map(|expr| finalize_expr_subqueries(expr, catalog, &mut subplans))
        .collect();
    stmt.source = match stmt.source {
        BoundInsertSource::Values(rows) => BoundInsertSource::Values(
            rows.into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|expr| finalize_expr_subqueries(expr, catalog, &mut subplans))
                        .collect()
                })
                .collect(),
        ),
        BoundInsertSource::DefaultValues(defaults) => BoundInsertSource::DefaultValues(
            defaults
                .into_iter()
                .map(|expr| finalize_expr_subqueries(expr, catalog, &mut subplans))
                .collect(),
        ),
        BoundInsertSource::Select(query) => BoundInsertSource::Select(query),
    };
    stmt.subplans = subplans;
    stmt
}

fn finalize_bound_update(
    mut stmt: BoundUpdateStatement,
    catalog: &dyn CatalogLookup,
) -> BoundUpdateStatement {
    let mut subplans = Vec::new();
    stmt.targets = stmt
        .targets
        .into_iter()
        .map(|target| crate::backend::parser::BoundUpdateTarget {
            assignments: target
                .assignments
                .into_iter()
                .map(|assignment| BoundAssignment {
                    column_index: assignment.column_index,
                    expr: finalize_expr_subqueries(assignment.expr, catalog, &mut subplans),
                    subscripts: assignment
                        .subscripts
                        .into_iter()
                        .map(|subscript| BoundArraySubscript {
                            is_slice: subscript.is_slice,
                            lower: subscript
                                .lower
                                .map(|expr| finalize_expr_subqueries(expr, catalog, &mut subplans)),
                            upper: subscript
                                .upper
                                .map(|expr| finalize_expr_subqueries(expr, catalog, &mut subplans)),
                        })
                        .collect(),
                })
                .collect(),
            predicate: target
                .predicate
                .map(|expr| finalize_expr_subqueries(expr, catalog, &mut subplans)),
            ..target
        })
        .collect();
    stmt.subplans = subplans;
    stmt
}

fn finalize_bound_delete(
    mut stmt: BoundDeleteStatement,
    catalog: &dyn CatalogLookup,
) -> BoundDeleteStatement {
    let mut subplans = Vec::new();
    stmt.targets = stmt
        .targets
        .into_iter()
        .map(|target| crate::backend::parser::BoundDeleteTarget {
            predicate: target
                .predicate
                .map(|expr| finalize_expr_subqueries(expr, catalog, &mut subplans)),
            ..target
        })
        .collect();
    stmt.subplans = subplans;
    stmt
}

pub(crate) fn execute_explain(
    stmt: ExplainStatement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    let Statement::Select(select) = *stmt.statement else {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "SELECT statement after EXPLAIN",
            actual: "non-select statement".into(),
        }));
    };

    ctx.pool.reset_usage_stats();
    let plan_start = Instant::now();
    let query_desc = create_query_desc(
        crate::backend::parser::pg_plan_query(&select, catalog)?,
        None,
    );
    let planning_elapsed = plan_start.elapsed();
    let planning_buffer_stats = ctx.pool.usage_stats();
    let mut lines = Vec::new();
    if stmt.analyze {
        ctx.pool.reset_usage_stats();
        ctx.timed = stmt.timing;
        let saved_subplans =
            std::mem::replace(&mut ctx.subplans, query_desc.planned_stmt.subplans.clone());
        let exec_result: Result<(_, _, _), ExecError> = (|| {
            let mut state = executor_start(query_desc.planned_stmt.plan_tree.clone());
            let mut row_count: u64 = 0;
            let started_at = Instant::now();
            while let Some(_slot) = state.exec_proc_node(ctx)? {
                row_count += 1;
            }
            Ok((state, row_count, started_at.elapsed()))
        })();
        ctx.subplans = saved_subplans;
        ctx.timed = false;
        let execution_buffer_stats = ctx.pool.usage_stats();
        let (state, row_count, elapsed) = exec_result?;
        format_explain_lines(state.as_ref(), 0, true, &mut lines);
        if stmt.buffers {
            lines.push("Planning:".into());
            lines.push(format!("  {}", format_buffer_usage(planning_buffer_stats)));
        }
        lines.push(format!(
            "Planning Time: {:.3} ms",
            planning_elapsed.as_secs_f64() * 1000.0
        ));
        lines.push(format!(
            "Execution Time: {:.3} ms",
            elapsed.as_secs_f64() * 1000.0
        ));
        if stmt.buffers {
            lines.push(format_buffer_usage(execution_buffer_stats));
        }
        lines.push(format!("Result Rows: {}", row_count));
    } else {
        let state = executor_start(query_desc.planned_stmt.plan_tree);
        format_explain_lines(state.as_ref(), 0, false, &mut lines);
    }

    Ok(StatementResult::Query {
        columns: vec![QueryColumn::text("QUERY PLAN")],
        column_names: vec!["QUERY PLAN".into()],
        rows: lines
            .into_iter()
            .map(|line| vec![Value::Text(line.into())])
            .collect(),
    })
}

fn validate_maintenance_targets(
    targets: &[MaintenanceTarget],
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    for target in targets {
        let entry = match catalog.lookup_any_relation(&target.table_name) {
            Some(entry) if entry.relkind == 'r' => entry,
            Some(_) => {
                return Err(ExecError::Parse(ParseError::WrongObjectType {
                    name: target.table_name.clone(),
                    expected: "table",
                }));
            }
            None => {
                return Err(ExecError::Parse(ParseError::UnknownTable(
                    target.table_name.clone(),
                )));
            }
        };
        for column in &target.columns {
            if !entry
                .desc
                .columns
                .iter()
                .any(|desc| desc.name.eq_ignore_ascii_case(column))
            {
                return Err(ExecError::Parse(ParseError::UnknownColumn(column.clone())));
            }
        }
    }
    Ok(())
}

pub(crate) fn maintain_indexes_for_row(
    heap_rel: crate::backend::storage::smgr::RelFileLocator,
    heap_desc: &RelationDesc,
    indexes: &[BoundIndexRelation],
    values: &[Value],
    heap_tid: ItemPointerData,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    for index in indexes
        .iter()
        .filter(|index| index.index_meta.indisvalid && index.index_meta.indisready)
    {
        crate::backend::access::index::indexam::index_insert_stub(
            &crate::include::access::amapi::IndexInsertContext {
                pool: ctx.pool.clone(),
                txns: ctx.txns.clone(),
                txn_waiter: ctx.txn_waiter.clone(),
                client_id: ctx.client_id,
                interrupts: ctx.interrupts.clone(),
                snapshot: ctx.snapshot.clone(),
                heap_relation: heap_rel,
                heap_desc: heap_desc.clone(),
                index_relation: index.rel,
                index_name: index.name.clone(),
                index_desc: index.desc.clone(),
                index_meta: index.index_meta.clone(),
                values: values.to_vec(),
                heap_tid,
                unique_check: if index.index_meta.indisunique {
                    crate::include::access::amapi::IndexUniqueCheck::Yes
                } else {
                    crate::include::access::amapi::IndexUniqueCheck::No
                },
            },
            index.index_meta.am_oid,
        )
        .map_err(|err| match err {
            crate::backend::catalog::CatalogError::UniqueViolation(constraint) => {
                ExecError::UniqueViolation { constraint }
            }
            crate::backend::catalog::CatalogError::Interrupted(reason) => {
                ExecError::Interrupted(reason)
            }
            other => ExecError::Parse(ParseError::UnexpectedToken {
                expected: "index insertion",
                actual: format!("{other:?}"),
            }),
        })?;
    }
    Ok(())
}

fn slot_toast_context(
    toast: Option<ToastRelationRef>,
    ctx: &ExecutorContext,
) -> Option<crate::include::nodes::execnodes::ToastFetchContext> {
    toast.map(
        |relation| crate::include::nodes::execnodes::ToastFetchContext {
            relation,
            pool: ctx.pool.clone(),
            txns: ctx.txns.clone(),
            snapshot: ctx.snapshot.clone(),
            client_id: ctx.client_id,
        },
    )
}

fn toast_tuple_for_write(
    desc: &RelationDesc,
    values: &[Value],
    toast: Option<ToastRelationRef>,
    toast_index: Option<&BoundIndexRelation>,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<(HeapTuple, Vec<StoredToastValue>), ExecError> {
    let mut tuple_values = encode_tuple_values(desc, values)?;
    let attr_descs = desc.attribute_descs();
    let mut tuple = HeapTuple::from_values(&attr_descs, &tuple_values)?;
    let Some(toast) = toast else {
        return Ok((tuple, Vec::new()));
    };

    let mut stored = Vec::new();
    while tuple.serialized_len() > MAX_HEAP_TUPLE_SIZE {
        let Some((attno, data)) = desc
            .columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| match &tuple_values[index] {
                TupleValue::Bytes(bytes)
                    if column.storage.attlen == -1
                        && column.storage.attstorage
                            != crate::include::access::htup::AttributeStorage::Plain
                        && !is_ondisk_toast_pointer(bytes) =>
                {
                    Some((index, bytes.clone()))
                }
                _ => None,
            })
            .max_by_key(|(_, bytes)| bytes.len())
        else {
            break;
        };

        let toasted = store_external_value(ctx, toast, toast_index, &data, xid, cid)?;
        tuple_values[attno] = TupleValue::Bytes(encoded_pointer_bytes(toasted.pointer));
        tuple = HeapTuple::from_values(&attr_descs, &tuple_values)?;
        stored.push(toasted);
    }
    Ok((tuple, stored))
}

fn cleanup_toast_attempt(
    toast: Option<ToastRelationRef>,
    toasted: &[StoredToastValue],
    ctx: &ExecutorContext,
    xid: TransactionId,
) -> Result<(), ExecError> {
    let Some(toast) = toast else {
        return Ok(());
    };
    for value in toasted {
        cleanup_new_toast_value(ctx, toast, &value.chunk_tids, xid)?;
    }
    Ok(())
}

fn reinitialize_index_relation(
    index: &BoundIndexRelation,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
) -> Result<(), ExecError> {
    let _ = ctx.pool.invalidate_relation(index.rel);
    ctx.pool
        .with_storage_mut(|s| s.smgr.truncate(index.rel, ForkNumber::Main, 0))
        .map_err(HeapError::Storage)?;
    crate::backend::access::index::indexam::index_build_empty_stub(
        &crate::include::access::amapi::IndexBuildEmptyContext {
            pool: ctx.pool.clone(),
            client_id: ctx.client_id,
            xid,
            index_relation: index.rel,
        },
        index.index_meta.am_oid,
    )
    .map_err(|err| {
        ExecError::Parse(ParseError::UnexpectedToken {
            expected: "index reinitialization",
            actual: format!("{err:?}"),
        })
    })?;
    Ok(())
}

pub(crate) fn collect_matching_rows_heap(
    rel: crate::backend::storage::smgr::RelFileLocator,
    desc: &RelationDesc,
    toast: Option<ToastRelationRef>,
    predicate: Option<&Expr>,
    ctx: &mut ExecutorContext,
) -> Result<Vec<(ItemPointerData, Vec<Value>)>, ExecError> {
    let mut scan = heap_scan_begin_visible(&ctx.pool, ctx.client_id, rel, ctx.snapshot.clone())?;

    let desc = Rc::new(desc.clone());
    let attr_descs: Rc<[_]> = desc.attribute_descs().into();
    let decoder = Rc::new(CompiledTupleDecoder::compile(&desc, &attr_descs));
    let qual = predicate.map(|p| compile_predicate_with_decoder(p, &decoder));

    let mut slot = TupleSlot::empty(decoder.ncols());
    slot.decoder = Some(Rc::clone(&decoder));
    slot.toast = slot_toast_context(toast, ctx);
    let mut rows = Vec::new();

    loop {
        ctx.check_for_interrupts()?;
        let next: Result<Option<usize>, ExecError> =
            heap_scan_prepare_next_page(&*ctx.pool, ctx.client_id, &ctx.txns, &mut scan);
        let Some(buffer_id) = next? else {
            break;
        };

        let page =
            unsafe { ctx.pool.page_unlocked(buffer_id) }.expect("pinned buffer must be valid");

        let pin = scan
            .pinned_buffer_rc()
            .expect("buffer must be pinned after prepare_next_page");

        let mut page_rows = Vec::new();
        while let Some((tid, tuple_bytes)) = heap_scan_page_next_tuple(page, &mut scan) {
            ctx.check_for_interrupts()?;
            slot.kind = SlotKind::BufferHeapTuple {
                desc: Rc::clone(&desc),
                attr_descs: Rc::clone(&attr_descs),
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
            ctx.check_for_interrupts()?;
            let mut slot = TupleSlot::virtual_row(values.clone());
            if let Some(q) = &qual {
                if !q(&mut slot, ctx)? {
                    continue;
                }
            }
            rows.push((tid, values));
        }
    }

    heap_scan_end::<ExecError>(&*ctx.pool, ctx.client_id, &mut scan)?;
    Ok(rows)
}

fn collect_matching_rows_index(
    rel: crate::backend::storage::smgr::RelFileLocator,
    desc: &RelationDesc,
    toast: Option<ToastRelationRef>,
    index: &BoundIndexRelation,
    keys: &[crate::include::access::scankey::ScanKeyData],
    predicate: Option<&Expr>,
    ctx: &mut ExecutorContext,
) -> Result<Vec<(ItemPointerData, Vec<Value>)>, ExecError> {
    let desc = Rc::new(desc.clone());
    let attr_descs: Rc<[_]> = desc.attribute_descs().into();
    let decoder = Rc::new(CompiledTupleDecoder::compile(&desc, &attr_descs));
    let qual = predicate.map(|p| compile_predicate_with_decoder(p, &decoder));

    let begin = crate::include::access::amapi::IndexBeginScanContext {
        pool: ctx.pool.clone(),
        client_id: ctx.client_id,
        snapshot: ctx.snapshot.clone(),
        heap_relation: rel,
        index_relation: index.rel,
        index_desc: index.desc.clone(),
        index_meta: index.index_meta.clone(),
        key_data: keys.to_vec(),
        direction: crate::include::access::relscan::ScanDirection::Forward,
    };
    let mut scan = indexam::index_beginscan(&begin, index.index_meta.am_oid).map_err(|err| {
        ExecError::Parse(ParseError::UnexpectedToken {
            expected: "index access method begin scan",
            actual: format!("{err:?}"),
        })
    })?;
    let mut seen = HashSet::new();
    let mut rows = Vec::new();

    loop {
        ctx.check_for_interrupts()?;
        let has_tuple =
            indexam::index_getnext(&mut scan, index.index_meta.am_oid).map_err(|err| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "index access method tuple",
                    actual: format!("{err:?}"),
                })
            })?;
        if !has_tuple {
            break;
        }
        let tid = scan.xs_heaptid.expect("index scan tuple must set heap tid");
        if !seen.insert(tid) {
            continue;
        }
        let Some(tuple) = heap_fetch_visible_with_txns(
            &ctx.pool,
            ctx.client_id,
            rel,
            tid,
            &ctx.txns,
            &ctx.snapshot,
        )?
        else {
            continue;
        };
        let mut slot =
            TupleSlot::from_heap_tuple(Rc::clone(&desc), Rc::clone(&attr_descs), tid, tuple);
        slot.toast = slot_toast_context(toast, ctx);
        if let Some(q) = &qual {
            if !q(&mut slot, ctx)? {
                continue;
            }
        }
        rows.push((tid, slot.into_values()?));
    }

    indexam::index_endscan(scan, index.index_meta.am_oid).map_err(|err| {
        ExecError::Parse(ParseError::UnexpectedToken {
            expected: "index access method end scan",
            actual: format!("{err:?}"),
        })
    })?;
    Ok(rows)
}

pub fn execute_analyze(
    stmt: AnalyzeStatement,
    catalog: &dyn CatalogLookup,
) -> Result<StatementResult, ExecError> {
    validate_maintenance_targets(&stmt.targets, catalog)?;
    Ok(StatementResult::AffectedRows(0))
}

pub fn execute_vacuum(
    stmt: VacuumStatement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    validate_maintenance_targets(&stmt.targets, catalog)?;
    let mut processed = 0u64;
    for target in stmt.targets {
        let Some(entry) = catalog.lookup_relation(&target.table_name) else {
            continue;
        };
        let indexes = catalog.index_relations_for_heap(entry.relation_oid);
        for index in indexes {
            let vacuum_ctx = crate::include::access::amapi::IndexVacuumContext {
                pool: ctx.pool.clone(),
                txns: ctx.txns.clone(),
                client_id: ctx.client_id,
                interrupts: ctx.interrupts.clone(),
                heap_relation: entry.rel,
                heap_desc: entry.desc.clone(),
                index_relation: index.rel,
                index_name: index.name.clone(),
                index_desc: index.desc.clone(),
                index_meta: index.index_meta.clone(),
            };
            let stats = indexam::index_bulk_delete(&vacuum_ctx, index.index_meta.am_oid, None)
                .map_err(|err| {
                    ExecError::Parse(ParseError::UnexpectedToken {
                        expected: "VACUUM bulk delete",
                        actual: format!("{err:?}"),
                    })
                })?;
            let _ =
                indexam::index_vacuum_cleanup(&vacuum_ctx, index.index_meta.am_oid, Some(stats))
                    .map_err(|err| {
                        ExecError::Parse(ParseError::UnexpectedToken {
                            expected: "VACUUM cleanup",
                            actual: format!("{err:?}"),
                        })
                    })?;
        }
        processed += 1;
    }
    let _ = processed;
    Ok(StatementResult::AffectedRows(0))
}

pub fn execute_create_table(
    stmt: crate::backend::parser::CreateTableStatement,
    catalog: &mut Catalog,
) -> Result<StatementResult, ExecError> {
    let _entry = bind_create_table(&stmt, catalog)?;
    Ok(StatementResult::AffectedRows(0))
}

pub fn execute_create_index(
    stmt: crate::backend::parser::CreateIndexStatement,
    catalog: &mut Catalog,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    if stmt
        .using_method
        .as_deref()
        .is_some_and(|method| !method.eq_ignore_ascii_case("btree"))
    {
        return Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "unsupported index access method".into(),
        )));
    }
    if !stmt.include_columns.is_empty() || stmt.predicate.is_some() || !stmt.options.is_empty() {
        return Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "CREATE INDEX options".into(),
        )));
    }
    let entry = match catalog.create_index(
        stmt.index_name,
        &stmt.table_name,
        stmt.unique,
        &stmt.columns,
    ) {
        Ok(entry) => entry,
        Err(crate::backend::catalog::catalog::CatalogError::TableAlreadyExists(_))
            if stmt.if_not_exists =>
        {
            return Ok(StatementResult::AffectedRows(0));
        }
        Err(crate::backend::catalog::catalog::CatalogError::TableAlreadyExists(name)) => {
            return Err(ExecError::Parse(ParseError::TableAlreadyExists(name)));
        }
        Err(crate::backend::catalog::catalog::CatalogError::UnknownTable(name)) => {
            return Err(ExecError::Parse(ParseError::TableDoesNotExist(name)));
        }
        Err(crate::backend::catalog::catalog::CatalogError::UnknownColumn(name)) => {
            return Err(ExecError::Parse(ParseError::UnknownColumn(name)));
        }
        Err(other) => {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "catalog index creation",
                actual: format!("{other:?}"),
            }));
        }
    };
    let _ = ctx;
    let _ = entry;
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
                ctx.pool
                    .with_storage_mut(|s| s.smgr.unlink(entry.rel, None, false));
                dropped += 1;
            }
            Err(crate::backend::catalog::catalog::CatalogError::UnknownTable(name))
                if stmt.if_exists =>
            {
                let _ = name;
            }
            Err(crate::backend::catalog::catalog::CatalogError::UnknownTable(name)) => {
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
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
) -> Result<StatementResult, ExecError> {
    for table_name in stmt.table_names {
        let entry = match catalog.lookup_any_relation(&table_name) {
            Some(entry) if entry.relkind == 'r' => entry,
            Some(_) => {
                return Err(ExecError::Parse(ParseError::WrongObjectType {
                    name: table_name.clone(),
                    expected: "table",
                }));
            }
            None => {
                return Err(ExecError::Parse(ParseError::UnknownTable(
                    table_name.clone(),
                )));
            }
        };
        if catalog.has_subclass(entry.relation_oid) {
            return Err(ExecError::Parse(ParseError::FeatureNotSupported(
                "TRUNCATE on inherited parents is not supported yet".into(),
            )));
        }
        let indexes = catalog.index_relations_for_heap(entry.relation_oid);
        let _ = ctx.pool.invalidate_relation(entry.rel);
        ctx.pool
            .with_storage_mut(|s| s.smgr.truncate(entry.rel, ForkNumber::Main, 0))
            .map_err(HeapError::Storage)?;
        for index in indexes
            .iter()
            .filter(|index| index.index_meta.indisvalid && index.index_meta.indisready)
        {
            reinitialize_index_relation(index, ctx, xid)?;
        }
    }
    Ok(StatementResult::AffectedRows(0))
}

pub fn execute_insert(
    stmt: BoundInsertStatement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<StatementResult, ExecError> {
    let stmt = finalize_bound_insert(stmt, catalog);
    let saved_subplans = std::mem::replace(&mut ctx.subplans, stmt.subplans.clone());
    let result = (|| {
        fn eval_implicit_insert_defaults(
            defaults: &[crate::backend::executor::Expr],
            targets: &[BoundAssignmentTarget],
            width: usize,
            ctx: &mut ExecutorContext,
        ) -> Result<(TupleSlot, Vec<Value>), ExecError> {
            let mut slot = TupleSlot::virtual_row(vec![Value::Null; width]);
            let mut targeted = vec![false; width];
            for target in targets {
                if let Some(mark) = targeted.get_mut(target.column_index) {
                    *mark = true;
                }
            }
            let mut values = vec![Value::Null; width];
            for (column_index, expr) in defaults.iter().enumerate() {
                if targeted.get(column_index).copied().unwrap_or(false) {
                    continue;
                }
                values[column_index] = eval_expr(expr, &mut slot, ctx)?;
            }
            Ok((slot, values))
        }

        let values = match &stmt.source {
            BoundInsertSource::Values(rows) => rows
                .iter()
                .map(|row| {
                    let (mut slot, mut values) = eval_implicit_insert_defaults(
                        &stmt.column_defaults,
                        &stmt.target_columns,
                        stmt.desc.columns.len(),
                        ctx,
                    )?;
                    for (target, expr) in stmt.target_columns.iter().zip(row.iter()) {
                        let value = eval_expr(expr, &mut slot, ctx)?;
                        apply_assignment_target(
                            &stmt.desc,
                            &mut values,
                            target,
                            value,
                            &mut slot,
                            ctx,
                        )?;
                    }
                    Ok(values)
                })
                .collect::<Result<Vec<_>, ExecError>>()?,
            BoundInsertSource::DefaultValues(defaults) => {
                let mut slot = TupleSlot::virtual_row(vec![Value::Null; stmt.desc.columns.len()]);
                let mut values = vec![Value::Null; stmt.desc.columns.len()];
                for (target, expr) in stmt.target_columns.iter().zip(defaults.iter()) {
                    let value = eval_expr(expr, &mut slot, ctx)?;
                    apply_assignment_target(
                        &stmt.desc,
                        &mut values,
                        target,
                        value,
                        &mut slot,
                        ctx,
                    )?;
                }
                vec![values]
            }
            BoundInsertSource::Select(query) => {
                let [query] = pg_rewrite_query((**query).clone(), catalog)
                    .map_err(ExecError::Parse)?
                    .try_into()
                    .expect("insert-select rewrite should return a single query");
                let planned = planner(query, catalog);
                let result: Result<Vec<Vec<Value>>, ExecError> = (|| {
                    let saved_subplans =
                        std::mem::replace(&mut ctx.subplans, planned.subplans.clone());
                    let mut state = executor_start(planned.plan_tree.clone());
                    let mut rows = Vec::new();
                    while let Some(slot) = state.exec_proc_node(ctx)? {
                        ctx.check_for_interrupts()?;
                        let row_values = slot.values()?.iter().cloned().collect::<Vec<_>>();
                        let (_, mut values) = eval_implicit_insert_defaults(
                            &stmt.column_defaults,
                            &stmt.target_columns,
                            stmt.desc.columns.len(),
                            ctx,
                        )?;
                        for (target, value) in
                            stmt.target_columns.iter().zip(row_values.into_iter())
                        {
                            apply_assignment_target(
                                &stmt.desc,
                                &mut values,
                                target,
                                value,
                                slot,
                                ctx,
                            )?;
                        }
                        rows.push(values);
                    }
                    ctx.subplans = saved_subplans;
                    Ok(rows)
                })();
                result?
            }
        };

        let inserted = execute_insert_values(
            &stmt.relation_name,
            stmt.rel,
            stmt.toast,
            stmt.toast_index.as_ref(),
            &stmt.desc,
            &stmt.relation_constraints,
            &stmt.indexes,
            &values,
            ctx,
            xid,
            cid,
        )?;
        Ok(StatementResult::AffectedRows(inserted))
    })();
    ctx.subplans = saved_subplans;
    result
}

fn apply_assignment_target(
    desc: &RelationDesc,
    values: &mut [Value],
    target: &BoundAssignmentTarget,
    value: Value,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let assignment_type = assignment_target_sql_type(desc, target);
    let value = coerce_assignment_value(&value, assignment_type)
        .map_err(|err| rewrite_subscripted_assignment_error(desc, target, &value, err))?;
    if target.subscripts.is_empty() {
        values[target.column_index] = value;
        return Ok(());
    }
    let resolved = target
        .subscripts
        .iter()
        .map(|subscript| {
            Ok(ResolvedAssignmentSubscript {
                is_slice: subscript.is_slice,
                lower: subscript
                    .lower
                    .as_ref()
                    .map(|expr| eval_expr(expr, slot, ctx))
                    .transpose()?,
                upper: subscript
                    .upper
                    .as_ref()
                    .map(|expr| eval_expr(expr, slot, ctx))
                    .transpose()?,
            })
        })
        .collect::<Result<Vec<_>, ExecError>>()?;
    let current = values[target.column_index].clone();
    values[target.column_index] = assign_array_value(current, &resolved, value)?;
    Ok(())
}

fn rewrite_subscripted_assignment_error(
    desc: &RelationDesc,
    target: &BoundAssignmentTarget,
    value: &Value,
    err: ExecError,
) -> ExecError {
    if target.subscripts.is_empty() || !matches!(err, ExecError::TypeMismatch { .. }) {
        return err;
    }

    let Some(actual_type) = value.sql_type_hint() else {
        return err;
    };

    ExecError::DetailedError {
        message: format!(
            "subscripted assignment to \"{}\" requires type {} but expression is of type {}",
            desc.columns[target.column_index].name,
            sql_type_display_name(assignment_target_sql_type(desc, target)),
            sql_type_display_name(actual_type),
        ),
        detail: None,
        hint: Some("You will need to rewrite or cast the expression.".into()),
        sqlstate: "42804",
    }
}

fn sql_type_display_name(ty: SqlType) -> String {
    let base = match ty.kind {
        SqlTypeKind::AnyArray => "anyarray",
        SqlTypeKind::Record | SqlTypeKind::Composite => "record",
        SqlTypeKind::Int2 => "smallint",
        SqlTypeKind::Int2Vector => "int2vector",
        SqlTypeKind::Int4 => "integer",
        SqlTypeKind::Int8 => "bigint",
        SqlTypeKind::Name => "name",
        SqlTypeKind::Oid => "oid",
        SqlTypeKind::Tid => "tid",
        SqlTypeKind::Xid => "xid",
        SqlTypeKind::OidVector => "oidvector",
        SqlTypeKind::Bit => "bit",
        SqlTypeKind::VarBit => "bit varying",
        SqlTypeKind::Bytea => "bytea",
        SqlTypeKind::Float4 => "real",
        SqlTypeKind::Float8 => "double precision",
        SqlTypeKind::Money => "money",
        SqlTypeKind::Numeric => "numeric",
        SqlTypeKind::Int4Range => "int4range",
        SqlTypeKind::Int8Range => "int8range",
        SqlTypeKind::NumericRange => "numrange",
        SqlTypeKind::Json => "json",
        SqlTypeKind::Jsonb => "jsonb",
        SqlTypeKind::JsonPath => "jsonpath",
        SqlTypeKind::Date => "date",
        SqlTypeKind::DateRange => "daterange",
        SqlTypeKind::Time => "time without time zone",
        SqlTypeKind::TimeTz => "time with time zone",
        SqlTypeKind::Interval => "interval",
        SqlTypeKind::TsVector => "tsvector",
        SqlTypeKind::TsQuery => "tsquery",
        SqlTypeKind::RegConfig => "regconfig",
        SqlTypeKind::RegDictionary => "regdictionary",
        SqlTypeKind::Text => "text",
        SqlTypeKind::Bool => "boolean",
        SqlTypeKind::Point => "point",
        SqlTypeKind::Lseg => "lseg",
        SqlTypeKind::Path => "path",
        SqlTypeKind::Box => "box",
        SqlTypeKind::Polygon => "polygon",
        SqlTypeKind::Line => "line",
        SqlTypeKind::Circle => "circle",
        SqlTypeKind::Timestamp => "timestamp without time zone",
        SqlTypeKind::TimestampRange => "tsrange",
        SqlTypeKind::TimestampTz => "timestamp with time zone",
        SqlTypeKind::TimestampTzRange => "tstzrange",
        SqlTypeKind::PgNodeTree => "pg_node_tree",
        SqlTypeKind::InternalChar => "\"char\"",
        SqlTypeKind::Char => "character",
        SqlTypeKind::Varchar => "character varying",
    };

    if ty.is_array {
        format!("{base}[]")
    } else {
        base.to_string()
    }
}

fn assignment_target_sql_type(desc: &RelationDesc, target: &BoundAssignmentTarget) -> SqlType {
    let column_type = desc.columns[target.column_index].sql_type;
    if target.subscripts.is_empty() {
        return column_type;
    }
    if target.subscripts.iter().any(|subscript| subscript.is_slice) {
        return SqlType::array_of(column_type.element_type());
    }
    column_type.element_type()
}

#[derive(Clone)]
struct ResolvedAssignmentSubscript {
    is_slice: bool,
    lower: Option<Value>,
    upper: Option<Value>,
}

fn assign_array_value(
    current: Value,
    subscripts: &[ResolvedAssignmentSubscript],
    replacement: Value,
) -> Result<Value, ExecError> {
    if subscripts.is_empty() {
        return Ok(replacement);
    }
    if subscripts.iter().any(|subscript| subscript.is_slice) {
        return assign_array_slice_value(current, subscripts, replacement);
    }
    let subscript = &subscripts[0];
    let (mut lower_bound, mut items) = assignment_top_level(current)?;
    if subscript.is_slice {
        let Some(start) = assignment_subscript_index(subscript.lower.as_ref())? else {
            return Err(ExecError::InvalidStorageValue {
                column: "<array>".into(),
                details: "array subscript in assignment must not be null".into(),
            });
        };
        let Some(end) = assignment_subscript_index(subscript.upper.as_ref())? else {
            return Err(ExecError::InvalidStorageValue {
                column: "<array>".into(),
                details: "array subscript in assignment must not be null".into(),
            });
        };
        let replacement_items = assignment_replacement_items(replacement.clone())?;
        if items.is_empty() {
            lower_bound = start;
        }
        extend_assignment_items(&mut lower_bound, &mut items, start, end);
        let start_idx = (start - lower_bound) as usize;
        let end_idx = (end - lower_bound) as usize;
        let span = end_idx - start_idx + 1;
        if replacement_items.len() != span {
            return Err(ExecError::TypeMismatch {
                op: "array slice assignment",
                left: build_assignment_array_value(lower_bound, items.clone())?,
                right: replacement,
            });
        }
        for (idx, item) in replacement_items.into_iter().enumerate() {
            items[start_idx + idx] = if subscripts.len() == 1 {
                item
            } else {
                assign_array_value(items[start_idx + idx].clone(), &subscripts[1..], item)?
            };
        }
        build_assignment_array_value(lower_bound, items)
    } else {
        let Some(index) = assignment_subscript_index(subscript.lower.as_ref())? else {
            return Err(ExecError::InvalidStorageValue {
                column: "<array>".into(),
                details: "array subscript in assignment must not be null".into(),
            });
        };
        if items.is_empty() {
            lower_bound = index;
        }
        extend_assignment_items(&mut lower_bound, &mut items, index, index);
        let index = (index - lower_bound) as usize;
        items[index] = if subscripts.len() == 1 {
            replacement
        } else {
            assign_array_value(items[index].clone(), &subscripts[1..], replacement)?
        };
        build_assignment_array_value(lower_bound, items)
    }
}

fn assign_array_slice_value(
    current: Value,
    subscripts: &[ResolvedAssignmentSubscript],
    replacement: Value,
) -> Result<Value, ExecError> {
    if matches!(replacement, Value::Null) {
        return Ok(current);
    }

    let current_array = assignment_current_array(current)?;
    let source_array = assignment_source_array(replacement)?;

    if subscripts.len() > 6 {
        return Err(array_assignment_error("wrong number of array subscripts"));
    }

    if current_array.ndim() == 0 {
        return assign_array_slice_into_empty(subscripts, source_array);
    }

    let ndim = current_array.ndim();
    if ndim < subscripts.len() || ndim > 6 {
        return Err(array_assignment_error("wrong number of array subscripts"));
    }

    let mut dimensions = current_array.dimensions.clone();
    let mut lower_bounds = Vec::with_capacity(ndim);
    let mut upper_bounds = Vec::with_capacity(ndim);

    for (dim_idx, subscript) in subscripts.iter().enumerate() {
        let dim = &dimensions[dim_idx];
        let lower = resolve_assignment_slice_bound(
            subscript.lower.as_ref(),
            dim.lower_bound,
            subscript.is_slice,
        )?;
        let upper = resolve_assignment_slice_bound(
            if subscript.is_slice {
                subscript.upper.as_ref()
            } else {
                subscript.lower.as_ref()
            },
            dim.lower_bound + dim.length as i32 - 1,
            subscript.is_slice,
        )?;
        if lower > upper {
            return Err(array_assignment_error(
                "upper bound cannot be less than lower bound",
            ));
        }

        if ndim == 1 {
            if lower < dimensions[0].lower_bound {
                let extension = (dimensions[0].lower_bound - lower) as usize;
                dimensions[0].lower_bound = lower;
                dimensions[0].length += extension;
            }
            let current_upper = dimensions[0].lower_bound + dimensions[0].length as i32 - 1;
            if upper > current_upper {
                dimensions[0].length += (upper - current_upper) as usize;
            }
        } else if lower < dim.lower_bound || upper >= dim.lower_bound + dim.length as i32 {
            return Err(array_assignment_error("array subscript out of range"));
        }

        lower_bounds.push(lower);
        upper_bounds.push(upper);
    }

    for dim in dimensions.iter().skip(subscripts.len()) {
        lower_bounds.push(dim.lower_bound);
        upper_bounds.push(dim.lower_bound + dim.length as i32 - 1);
    }

    let span_lengths = lower_bounds
        .iter()
        .zip(upper_bounds.iter())
        .map(|(lower, upper)| (*upper - *lower + 1) as usize)
        .collect::<Vec<_>>();
    let target_items = span_lengths
        .iter()
        .try_fold(1usize, |count, span| count.checked_mul(*span))
        .ok_or_else(|| array_assignment_limit_error())?;
    if source_array.elements.len() < target_items {
        return Err(array_assignment_error("source array too small"));
    }

    let element_type_oid = current_array.element_type_oid.or(source_array.element_type_oid);
    if ndim == 1 {
        let mut elements = vec![Value::Null; dimensions[0].length];
        let original_lower = current_array.lower_bound(0).unwrap_or(1);
        for (idx, value) in current_array.elements.iter().enumerate() {
            let target_idx = (original_lower + idx as i32 - dimensions[0].lower_bound) as usize;
            elements[target_idx] = value.clone();
        }
        let start_idx = (lower_bounds[0] - dimensions[0].lower_bound) as usize;
        for (offset, value) in source_array.elements.into_iter().take(target_items).enumerate() {
            elements[start_idx + offset] = value;
        }
        return Ok(Value::PgArray(array_with_element_type(
            ArrayValue::from_dimensions(dimensions, elements),
            element_type_oid,
        )));
    }

    let mut elements = current_array.elements.clone();
    for (offset, value) in source_array.elements.into_iter().take(target_items).enumerate() {
        let coords = linear_index_to_assignment_coords(offset, &lower_bounds, &span_lengths);
        let target_idx = assignment_coords_to_linear_index(&coords, &dimensions);
        elements[target_idx] = value;
    }
    Ok(Value::PgArray(array_with_element_type(
        ArrayValue::from_dimensions(dimensions, elements),
        element_type_oid,
    )))
}

fn assign_array_slice_into_empty(
    subscripts: &[ResolvedAssignmentSubscript],
    source_array: ArrayValue,
) -> Result<Value, ExecError> {
    let mut dimensions = Vec::with_capacity(subscripts.len());
    for subscript in subscripts {
        let Some(lower_value) = subscript.lower.as_ref() else {
            return Err(ExecError::DetailedError {
                message: "array slice subscript must provide both boundaries".into(),
                detail: Some(
                    "When assigning to a slice of an empty array value, slice boundaries must be fully specified."
                        .into(),
                ),
                hint: None,
                sqlstate: "2202E",
            });
        };
        let Some(upper_value) = (if subscript.is_slice {
            subscript.upper.as_ref()
        } else {
            subscript.lower.as_ref()
        }) else {
            return Err(ExecError::DetailedError {
                message: "array slice subscript must provide both boundaries".into(),
                detail: Some(
                    "When assigning to a slice of an empty array value, slice boundaries must be fully specified."
                        .into(),
                ),
                hint: None,
                sqlstate: "2202E",
            });
        };
        let lower = assignment_subscript_index(Some(lower_value))?
            .ok_or_else(|| assignment_null_subscript_error())?;
        let upper = assignment_subscript_index(Some(upper_value))?
            .ok_or_else(|| assignment_null_subscript_error())?;
        if lower > upper {
            return Err(array_assignment_error(
                "upper bound cannot be less than lower bound",
            ));
        }
        dimensions.push(ArrayDimension {
            lower_bound: lower,
            length: (upper - lower + 1) as usize,
        });
    }

    let target_items = dimensions
        .iter()
        .try_fold(1usize, |count, dim| count.checked_mul(dim.length))
        .ok_or_else(|| array_assignment_limit_error())?;
    if source_array.elements.len() < target_items {
        return Err(array_assignment_error("source array too small"));
    }

    Ok(Value::PgArray(array_with_element_type(
        ArrayValue::from_dimensions(
            dimensions,
            source_array.elements.into_iter().take(target_items).collect(),
        ),
        source_array.element_type_oid,
    )))
}

fn assignment_current_array(current: Value) -> Result<ArrayValue, ExecError> {
    match current {
        Value::Null => Ok(ArrayValue::empty()),
        other => array_value_from_value(&other).ok_or(ExecError::TypeMismatch {
            op: "array assignment",
            left: other,
            right: Value::Null,
        }),
    }
}

fn assignment_source_array(replacement: Value) -> Result<ArrayValue, ExecError> {
    array_value_from_value(&replacement).ok_or(ExecError::TypeMismatch {
        op: "array slice assignment",
        left: Value::Null,
        right: replacement,
    })
}

fn resolve_assignment_slice_bound(
    value: Option<&Value>,
    default: i32,
    is_slice: bool,
) -> Result<i32, ExecError> {
    match value {
        None if is_slice => Ok(default),
        None => assignment_subscript_index(None)?.ok_or_else(assignment_null_subscript_error),
        Some(_) => assignment_subscript_index(value)?.ok_or_else(assignment_null_subscript_error),
    }
}

fn assignment_null_subscript_error() -> ExecError {
    ExecError::InvalidStorageValue {
        column: "<array>".into(),
        details: "array subscript in assignment must not be null".into(),
    }
}

fn array_assignment_error(message: &str) -> ExecError {
    ExecError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate: "2202E",
    }
}

fn array_assignment_limit_error() -> ExecError {
    ExecError::DetailedError {
        message: "array size exceeds the maximum allowed".into(),
        detail: None,
        hint: None,
        sqlstate: "54000",
    }
}

fn array_with_element_type(mut array: ArrayValue, element_type_oid: Option<u32>) -> ArrayValue {
    array.element_type_oid = element_type_oid;
    array
}

fn linear_index_to_assignment_coords(
    mut offset: usize,
    lower_bounds: &[i32],
    lengths: &[usize],
) -> Vec<i32> {
    let mut coords = vec![0; lengths.len()];
    for dim_idx in 0..lengths.len() {
        let stride = lengths[dim_idx + 1..]
            .iter()
            .fold(1usize, |product, length| product.saturating_mul(*length));
        let axis_offset = if stride == 0 { 0 } else { offset / stride };
        if stride != 0 {
            offset %= stride;
        }
        coords[dim_idx] = lower_bounds[dim_idx] + axis_offset as i32;
    }
    coords
}

fn assignment_coords_to_linear_index(coords: &[i32], dimensions: &[ArrayDimension]) -> usize {
    let mut offset = 0usize;
    for (dim_idx, coord) in coords.iter().enumerate() {
        let stride = dimensions[dim_idx + 1..]
            .iter()
            .fold(1usize, |product, dim| product.saturating_mul(dim.length));
        offset += (*coord - dimensions[dim_idx].lower_bound) as usize * stride;
    }
    offset
}

fn assignment_top_level(current: Value) -> Result<(i32, Vec<Value>), ExecError> {
    match current {
        Value::Null => Ok((1, Vec::new())),
        Value::Array(items) => Ok((1, items)),
        Value::PgArray(array) => Ok((
            array.lower_bound(0).unwrap_or(1),
            assignment_top_level_items(&array),
        )),
        other => Err(ExecError::TypeMismatch {
            op: "array assignment",
            left: other,
            right: Value::Null,
        }),
    }
}

fn assignment_top_level_items(array: &ArrayValue) -> Vec<Value> {
    if array.dimensions.len() <= 1 {
        return array.elements.clone();
    }
    let child_dims = array.dimensions[1..].to_vec();
    let child_width = child_dims
        .iter()
        .fold(1usize, |acc, dim| acc.saturating_mul(dim.length));
    let mut out = Vec::with_capacity(array.dimensions[0].length);
    for idx in 0..array.dimensions[0].length {
        let start = idx * child_width;
        out.push(Value::PgArray(ArrayValue::from_dimensions(
            child_dims.clone(),
            array.elements[start..start + child_width].to_vec(),
        )));
    }
    out
}

fn assignment_replacement_items(replacement: Value) -> Result<Vec<Value>, ExecError> {
    match replacement {
        Value::Array(items) => Ok(items),
        Value::PgArray(array) => Ok(assignment_top_level_items(&array)),
        other => Err(ExecError::TypeMismatch {
            op: "array slice assignment",
            left: Value::Null,
            right: other,
        }),
    }
}

fn extend_assignment_items(lower_bound: &mut i32, items: &mut Vec<Value>, start: i32, end: i32) {
    if items.is_empty() {
        *lower_bound = start;
    }
    if start < *lower_bound {
        let prepend = (*lower_bound - start) as usize;
        items.splice(0..0, std::iter::repeat_n(Value::Null, prepend));
        *lower_bound = start;
    }
    let upper_bound = *lower_bound + items.len() as i32 - 1;
    if end > upper_bound {
        items.resize(items.len() + (end - upper_bound) as usize, Value::Null);
    }
}

fn build_assignment_array_value(lower_bound: i32, items: Vec<Value>) -> Result<Value, ExecError> {
    if items.is_empty() {
        return Ok(Value::PgArray(ArrayValue::empty()));
    }
    let child_arrays = items
        .iter()
        .filter_map(|item| match item {
            Value::PgArray(array) => Some(Some(array.clone())),
            Value::Array(values) => {
                Some(ArrayValue::from_nested_values(values.clone(), vec![1]).ok())
            }
            Value::Null => Some(None),
            _ => None,
        })
        .collect::<Vec<_>>();
    if child_arrays.len() != items.len() {
        return Ok(Value::PgArray(ArrayValue::from_dimensions(
            vec![ArrayDimension {
                lower_bound,
                length: items.len(),
            }],
            items,
        )));
    }
    let Some(template) = child_arrays.iter().find_map(|entry| entry.clone()) else {
        return Ok(Value::PgArray(ArrayValue::from_dimensions(
            vec![ArrayDimension {
                lower_bound,
                length: items.len(),
            }],
            items,
        )));
    };
    let child_width = template.elements.len();
    let mut elements = Vec::with_capacity(items.len() * child_width);
    for entry in child_arrays {
        match entry {
            Some(array) => elements.extend(array.elements),
            None => elements.extend(std::iter::repeat_n(Value::Null, child_width)),
        }
    }
    let mut dimensions = vec![ArrayDimension {
        lower_bound,
        length: items.len(),
    }];
    dimensions.extend(template.dimensions);
    Ok(Value::PgArray(ArrayValue::from_dimensions(
        dimensions, elements,
    )))
}

fn assignment_subscript_index(value: Option<&Value>) -> Result<Option<i32>, ExecError> {
    match value {
        None => Ok(Some(1)),
        Some(Value::Null) => Ok(None),
        Some(Value::Int16(v)) => Ok(Some(*v as i32)),
        Some(Value::Int32(v)) => Ok(Some(*v)),
        Some(Value::Int64(v)) => i32::try_from(*v)
            .map(Some)
            .map_err(|_| ExecError::Int4OutOfRange),
        Some(other) => Err(ExecError::TypeMismatch {
            op: "array assignment",
            left: other.clone(),
            right: Value::Null,
        }),
    }
}

pub fn execute_insert_values(
    relation_name: &str,
    rel: crate::backend::storage::smgr::RelFileLocator,
    toast: Option<ToastRelationRef>,
    toast_index: Option<&BoundIndexRelation>,
    desc: &RelationDesc,
    relation_constraints: &crate::backend::parser::BoundRelationConstraints,
    indexes: &[BoundIndexRelation],
    rows: &[Vec<Value>],
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<usize, ExecError> {
    for values in rows {
        crate::backend::executor::enforce_relation_constraints(
            relation_name,
            desc,
            relation_constraints,
            values,
            ctx,
        )?;
        crate::backend::executor::enforce_outbound_foreign_keys(
            relation_name,
            &relation_constraints.foreign_keys,
            None,
            values,
            ctx,
        )?;
        let (tuple, _toasted) =
            toast_tuple_for_write(desc, values, toast, toast_index, ctx, xid, cid)?;
        let heap_tid = heap_insert_mvcc_with_cid(&*ctx.pool, ctx.client_id, rel, xid, cid, &tuple)?;
        maintain_indexes_for_row(rel, desc, indexes, values, heap_tid, ctx)?;
    }

    Ok(rows.len())
}

/// Execute a single-row insert from a prepared insert plan and parameter values.
/// This skips parsing, binding, and expression evaluation entirely.
pub fn execute_prepared_insert_row(
    prepared: &crate::backend::parser::PreparedInsert,
    params: &[Value],
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<(), ExecError> {
    let mut slot = TupleSlot::virtual_row(vec![Value::Null; prepared.desc.columns.len()]);
    let mut values = prepared
        .column_defaults
        .iter()
        .map(|expr| eval_expr(expr, &mut slot, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    for (column_index, param) in prepared.target_columns.iter().zip(params.iter()) {
        values[*column_index] = param.clone();
    }
    crate::backend::executor::enforce_relation_constraints(
        &prepared.relation_name,
        &prepared.desc,
        &prepared.relation_constraints,
        &values,
        ctx,
    )?;
    crate::backend::executor::enforce_outbound_foreign_keys(
        &prepared.relation_name,
        &prepared.relation_constraints.foreign_keys,
        None,
        &values,
        ctx,
    )?;
    let (tuple, _toasted) = toast_tuple_for_write(
        &prepared.desc,
        &values,
        prepared.toast,
        prepared.toast_index.as_ref(),
        ctx,
        xid,
        cid,
    )?;
    let heap_tid =
        heap_insert_mvcc_with_cid(&*ctx.pool, ctx.client_id, prepared.rel, xid, cid, &tuple)?;
    maintain_indexes_for_row(
        prepared.rel,
        &prepared.desc,
        &prepared.indexes,
        &values,
        heap_tid,
        ctx,
    )?;
    Ok(())
}

pub(crate) fn execute_update(
    stmt: BoundUpdateStatement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<StatementResult, ExecError> {
    execute_update_with_waiter(stmt, catalog, ctx, xid, cid, None)
}

pub fn execute_update_with_waiter(
    stmt: BoundUpdateStatement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
    waiter: Option<(
        &RwLock<TransactionManager>,
        &TransactionWaiter,
        &crate::backend::utils::misc::interrupts::InterruptState,
    )>,
) -> Result<StatementResult, ExecError> {
    let stmt = finalize_bound_update(stmt, catalog);
    let saved_subplans = std::mem::replace(&mut ctx.subplans, stmt.subplans.clone());
    let result = (|| {
        let mut affected_rows = 0;

        for target in &stmt.targets {
            let desc = Rc::new(target.desc.clone());
            let attr_descs: Rc<[_]> = desc.attribute_descs().into();
            let decoder = Rc::new(CompiledTupleDecoder::compile(&desc, &attr_descs));
            let qual = target
                .predicate
                .as_ref()
                .map(|p| compile_predicate_with_decoder(p, &decoder));
            let target_rows = match &target.row_source {
                BoundModifyRowSource::Heap => collect_matching_rows_heap(
                    target.rel,
                    &target.desc,
                    target.toast,
                    target.predicate.as_ref(),
                    ctx,
                )?,
                BoundModifyRowSource::Index { index, keys } => collect_matching_rows_index(
                    target.rel,
                    &target.desc,
                    target.toast,
                    index,
                    keys,
                    target.predicate.as_ref(),
                    ctx,
                )?,
            };

            for (tid, original_values) in target_rows {
                ctx.check_for_interrupts()?;
                let mut current_old_values = original_values.clone();
                let mut eval_slot = TupleSlot::virtual_row(original_values.clone());
                let mut values = original_values;
                for assignment in &target.assignments {
                    let value = eval_expr(&assignment.expr, &mut eval_slot, ctx)?;
                    apply_assignment_target(
                        &target.desc,
                        &mut values,
                        &BoundAssignmentTarget {
                            column_index: assignment.column_index,
                            subscripts: assignment.subscripts.clone(),
                        },
                        value,
                        &mut eval_slot,
                        ctx,
                    )?;
                }

                let mut current_tid = tid;
                let mut current_values = values;
                loop {
                    ctx.check_for_interrupts()?;
                    let old_tuple = heap_fetch(&*ctx.pool, ctx.client_id, target.rel, current_tid)?;
                    crate::backend::executor::enforce_relation_constraints(
                        &target.relation_name,
                        &target.desc,
                        &target.relation_constraints,
                        &current_values,
                        ctx,
                    )?;
                    crate::backend::executor::enforce_outbound_foreign_keys(
                        &target.relation_name,
                        &target.relation_constraints.foreign_keys,
                        Some(&current_old_values),
                        &current_values,
                        ctx,
                    )?;
                    crate::backend::executor::enforce_inbound_foreign_keys_on_update(
                        &target.relation_name,
                        &target.referenced_by_foreign_keys,
                        &current_old_values,
                        &current_values,
                        ctx,
                    )?;
                    let (current_replacement, toasted) = toast_tuple_for_write(
                        &target.desc,
                        &current_values,
                        target.toast,
                        target.toast_index.as_ref(),
                        ctx,
                        xid,
                        cid,
                    )?;
                    match heap_update_with_waiter(
                        &*ctx.pool,
                        ctx.client_id,
                        target.rel,
                        &ctx.txns,
                        xid,
                        cid,
                        current_tid,
                        &current_replacement,
                        waiter,
                    ) {
                        Ok(new_tid) => {
                            if let Some(toast) = target.toast {
                                delete_external_from_tuple(
                                    ctx,
                                    toast,
                                    &target.desc,
                                    &old_tuple,
                                    xid,
                                )?;
                            }
                            maintain_indexes_for_row(
                                target.rel,
                                &target.desc,
                                &target.indexes,
                                &current_values,
                                new_tid,
                                ctx,
                            )?;
                            affected_rows += 1;
                            break;
                        }
                        Err(HeapError::TupleUpdated(_old_tid, new_ctid)) => {
                            cleanup_toast_attempt(target.toast, &toasted, ctx, xid)?;
                            let new_tuple =
                                heap_fetch(&*ctx.pool, ctx.client_id, target.rel, new_ctid)?;
                            let mut new_slot = TupleSlot::from_heap_tuple(
                                Rc::clone(&desc),
                                Rc::clone(&attr_descs),
                                new_ctid,
                                new_tuple,
                            );
                            new_slot.toast = slot_toast_context(target.toast, ctx);
                            let passes = match &qual {
                                Some(q) => q(&mut new_slot, ctx)?,
                                None => true,
                            };
                            if !passes {
                                break;
                            }
                            let new_values_base = new_slot.into_values()?;
                            let mut eval_slot = TupleSlot::virtual_row(new_values_base.clone());
                            let mut new_values = new_values_base.clone();
                            for assignment in &target.assignments {
                                let value = eval_expr(&assignment.expr, &mut eval_slot, ctx)?;
                                apply_assignment_target(
                                    &target.desc,
                                    &mut new_values,
                                    &BoundAssignmentTarget {
                                        column_index: assignment.column_index,
                                        subscripts: assignment.subscripts.clone(),
                                    },
                                    value,
                                    &mut eval_slot,
                                    ctx,
                                )?;
                            }
                            current_old_values = new_values_base;
                            current_values = new_values.clone();
                            current_tid = new_ctid;
                        }
                        Err(HeapError::TupleAlreadyModified(_)) => {
                            cleanup_toast_attempt(target.toast, &toasted, ctx, xid)?;
                            break;
                        }
                        Err(e) => {
                            cleanup_toast_attempt(target.toast, &toasted, ctx, xid)?;
                            return Err(e.into());
                        }
                    }
                }
            }
        }

        Ok(StatementResult::AffectedRows(affected_rows))
    })();
    ctx.subplans = saved_subplans;
    result
}

pub(crate) fn execute_delete(
    stmt: BoundDeleteStatement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
) -> Result<StatementResult, ExecError> {
    execute_delete_with_waiter(stmt, catalog, ctx, xid, None)
}

pub fn execute_delete_with_waiter(
    stmt: BoundDeleteStatement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    waiter: Option<(
        &RwLock<TransactionManager>,
        &TransactionWaiter,
        &crate::backend::utils::misc::interrupts::InterruptState,
    )>,
) -> Result<StatementResult, ExecError> {
    let stmt = finalize_bound_delete(stmt, catalog);
    let saved_subplans = std::mem::replace(&mut ctx.subplans, stmt.subplans.clone());
    let result = (|| {
        let mut affected_rows = 0;
        for target in &stmt.targets {
            let desc = Rc::new(target.desc.clone());
            let attr_descs: Rc<[_]> = desc.attribute_descs().into();
            let decoder = Rc::new(CompiledTupleDecoder::compile(&desc, &attr_descs));
            let qual = target
                .predicate
                .as_ref()
                .map(|p| compile_predicate_with_decoder(p, &decoder));
            let targets = match &target.row_source {
                BoundModifyRowSource::Heap => collect_matching_rows_heap(
                    target.rel,
                    &target.desc,
                    target.toast,
                    target.predicate.as_ref(),
                    ctx,
                )?,
                BoundModifyRowSource::Index { index, keys } => collect_matching_rows_index(
                    target.rel,
                    &target.desc,
                    target.toast,
                    index,
                    keys,
                    target.predicate.as_ref(),
                    ctx,
                )?,
            };
            let snapshot = ctx.snapshot.clone();

            for (tid, values) in &targets {
                ctx.check_for_interrupts()?;
                let mut current_tid = *tid;
                let mut current_values = values.clone();
                loop {
                    ctx.check_for_interrupts()?;
                    crate::backend::executor::enforce_inbound_foreign_keys_on_delete(
                        &target.relation_name,
                        &target.referenced_by_foreign_keys,
                        &current_values,
                        ctx,
                    )?;
                    let old_tuple = if target.toast.is_some() {
                        Some(heap_fetch(
                            &*ctx.pool,
                            ctx.client_id,
                            target.rel,
                            current_tid,
                        )?)
                    } else {
                        None
                    };
                    match heap_delete_with_waiter(
                        &*ctx.pool,
                        ctx.client_id,
                        target.rel,
                        &ctx.txns,
                        xid,
                        current_tid,
                        &snapshot,
                        waiter,
                    ) {
                        Ok(()) => {
                            if let (Some(toast), Some(old_tuple)) =
                                (target.toast, old_tuple.as_ref())
                            {
                                delete_external_from_tuple(
                                    ctx,
                                    toast,
                                    &target.desc,
                                    old_tuple,
                                    xid,
                                )?;
                            }
                            affected_rows += 1;
                            break;
                        }
                        Err(HeapError::TupleAlreadyModified(_)) => {
                            break;
                        }
                        Err(HeapError::TupleUpdated(_old_tid, new_ctid)) => {
                            let new_tuple =
                                heap_fetch(&*ctx.pool, ctx.client_id, target.rel, new_ctid)?;
                            let mut new_slot = TupleSlot::from_heap_tuple(
                                Rc::clone(&desc),
                                Rc::clone(&attr_descs),
                                new_ctid,
                                new_tuple,
                            );
                            new_slot.toast = slot_toast_context(target.toast, ctx);
                            let passes = match &qual {
                                Some(q) => q(&mut new_slot, ctx)?,
                                None => true,
                            };
                            if !passes {
                                break;
                            }
                            current_values = new_slot.into_values()?;
                            current_tid = new_ctid;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
            }
        }

        Ok(StatementResult::AffectedRows(affected_rows))
    })();
    ctx.subplans = saved_subplans;
    result
}
