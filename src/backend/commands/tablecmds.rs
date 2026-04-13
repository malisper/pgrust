use std::rc::Rc;
use std::collections::HashSet;

use parking_lot::RwLock;

use crate::backend::access::heap::heapam::{
    HeapError, heap_delete_with_waiter, heap_fetch, heap_fetch_visible, heap_insert_mvcc_with_cid,
    heap_scan_begin_visible, heap_scan_end, heap_scan_page_next_tuple, heap_scan_prepare_next_page,
    heap_update_with_waiter,
};
use crate::backend::access::index::indexam;
use crate::backend::access::transam::xact::CommandId;
use crate::backend::access::transam::xact::{TransactionId, TransactionManager};
use crate::backend::parser::{
    AnalyzeStatement, BoundDeleteStatement, BoundInsertSource, BoundInsertStatement,
    BoundIndexRelation, BoundModifyRowSource, BoundUpdateStatement, BoundAssignmentTarget,
    Catalog, CatalogLookup, DropTableStatement, ExplainStatement,
    MaintenanceTarget, ParseError, Statement, TruncateTableStatement, VacuumStatement,
    SqlType, bind_create_table, build_plan,
};
use crate::backend::storage::smgr::ForkNumber;
use crate::backend::storage::smgr::StorageManager;
use crate::pgrust::database::TransactionWaiter;

use super::explain::{format_buffer_usage, format_explain_lines};
use crate::backend::executor::exec_expr::{
    compile_predicate_with_decoder, eval_expr, tuple_from_values,
};
use crate::backend::executor::exec_tuples::CompiledTupleDecoder;
use crate::backend::executor::value_io::coerce_assignment_value;
use crate::backend::executor::{ExecError, ExecutorContext, StatementResult, executor_start, Expr};
use crate::include::access::itemptr::ItemPointerData;
use crate::include::nodes::datum::{ArrayDimension, ArrayValue};
use crate::include::nodes::execnodes::*;

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
        lines.push(format!(
            "Planning Time: {:.3} ms",
            plan_elapsed.as_secs_f64() * 1000.0
        ));
        lines.push(format!(
            "Execution Time: {:.3} ms",
            elapsed.as_secs_f64() * 1000.0
        ));
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
        let entry = catalog
            .lookup_relation(&target.table_name)
            .ok_or_else(|| ExecError::Parse(ParseError::UnknownTable(target.table_name.clone())))?;
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

fn maintain_indexes_for_row(
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
            other => ExecError::Parse(ParseError::UnexpectedToken {
                expected: "index insertion",
                actual: format!("{other:?}"),
            }),
        })?;
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
    .map_err(|err| ExecError::Parse(ParseError::UnexpectedToken {
        expected: "index reinitialization",
        actual: format!("{err:?}"),
    }))?;
    Ok(())
}

fn collect_matching_rows_heap(
    rel: crate::backend::storage::smgr::RelFileLocator,
    desc: &RelationDesc,
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
    let mut rows = Vec::new();

    loop {
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
        let has_tuple = indexam::index_getnext(&mut scan, index.index_meta.am_oid).map_err(|err| {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "index access method tuple",
                actual: format!("{err:?}"),
            })
        })?;
        if !has_tuple {
            break;
        }
        let tid = scan
            .xs_heaptid
            .expect("index scan tuple must set heap tid");
        if !seen.insert(tid) {
            continue;
        }
        let visible = {
            let txns = ctx.txns.read();
            heap_fetch_visible(&ctx.pool, ctx.client_id, rel, tid, &txns, &ctx.snapshot)?
        };
        let Some(tuple) = visible else {
            continue;
        };
        let mut slot = TupleSlot::from_heap_tuple(
            Rc::clone(&desc),
            Rc::clone(&attr_descs),
            tid,
            tuple,
        );
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
) -> Result<StatementResult, ExecError> {
    validate_maintenance_targets(&stmt.targets, catalog)?;
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
    let entry = catalog
        .create_index(
            stmt.index_name,
            &stmt.table_name,
            stmt.unique,
            &stmt.columns,
        )
        .map_err(|err| match err {
            crate::backend::catalog::catalog::CatalogError::TableAlreadyExists(name) => {
                ExecError::Parse(ParseError::TableAlreadyExists(name))
            }
            crate::backend::catalog::catalog::CatalogError::UnknownTable(name) => {
                ExecError::Parse(ParseError::TableDoesNotExist(name))
            }
            crate::backend::catalog::catalog::CatalogError::UnknownColumn(name) => {
                ExecError::Parse(ParseError::UnknownColumn(name))
            }
            other => ExecError::Parse(ParseError::UnexpectedToken {
                expected: "catalog index creation",
                actual: format!("{other:?}"),
            }),
        })?;
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
        let entry = catalog
            .lookup_relation(&table_name)
            .ok_or_else(|| ExecError::Parse(ParseError::UnknownTable(table_name.clone())))?;
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
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<StatementResult, ExecError> {
    fn eval_insert_defaults(
        defaults: &[crate::backend::executor::Expr],
        width: usize,
        ctx: &mut ExecutorContext,
    ) -> Result<Vec<Value>, ExecError> {
        let mut slot = TupleSlot::virtual_row(vec![Value::Null; width]);
        defaults
            .iter()
            .map(|expr| eval_expr(expr, &mut slot, ctx))
            .collect()
    }

    let values = match &stmt.source {
        BoundInsertSource::Values(rows) => rows
            .iter()
            .map(|row| {
                let mut slot = TupleSlot::virtual_row(vec![Value::Null; stmt.desc.columns.len()]);
                let mut values =
                    eval_insert_defaults(&stmt.column_defaults, stmt.desc.columns.len(), ctx)?;
                for (target, expr) in stmt.target_columns.iter().zip(row.iter()) {
                    let value = eval_expr(expr, &mut slot, ctx)?;
                    apply_assignment_target(&stmt.desc, &mut values, target, value, &mut slot, ctx)?;
                }
                Ok(values)
            })
            .collect::<Result<Vec<_>, ExecError>>()?,
        BoundInsertSource::DefaultValues(defaults) => {
            let mut slot = TupleSlot::virtual_row(vec![Value::Null; stmt.desc.columns.len()]);
            let mut values =
                eval_insert_defaults(&stmt.column_defaults, stmt.desc.columns.len(), ctx)?;
            for (target, expr) in stmt.target_columns.iter().zip(defaults.iter()) {
                let value = eval_expr(expr, &mut slot, ctx)?;
                apply_assignment_target(&stmt.desc, &mut values, target, value, &mut slot, ctx)?;
            }
            vec![values]
        }
        BoundInsertSource::Select(plan) => {
            let mut state = executor_start((**plan).clone());
            let mut rows = Vec::new();
            while let Some(slot) = state.exec_proc_node(ctx)? {
                let row_values = slot.values()?.iter().cloned().collect::<Vec<_>>();
                let mut values =
                    eval_insert_defaults(&stmt.column_defaults, stmt.desc.columns.len(), ctx)?;
                for (target, value) in stmt.target_columns.iter().zip(row_values.into_iter()) {
                    apply_assignment_target(&stmt.desc, &mut values, target, value, slot, ctx)?;
                }
                rows.push(values);
            }
            rows
        }
    };

    let inserted = execute_insert_values(
        stmt.rel,
        &stmt.desc,
        &stmt.indexes,
        &values,
        ctx,
        xid,
        cid,
    )?;
    Ok(StatementResult::AffectedRows(inserted))
}

fn apply_assignment_target(
    desc: &RelationDesc,
    values: &mut [Value],
    target: &BoundAssignmentTarget,
    value: Value,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let value = coerce_assignment_value(&value, assignment_target_sql_type(desc, target))?;
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

fn extend_assignment_items(
    lower_bound: &mut i32,
    items: &mut Vec<Value>,
    start: i32,
    end: i32,
) {
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
            Value::Array(values) => Some(ArrayValue::from_nested_values(values.clone(), vec![1]).ok()),
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
        dimensions,
        elements,
    )))
}

fn assignment_subscript_index(value: Option<&Value>) -> Result<Option<i32>, ExecError> {
    match value {
        None => Ok(Some(1)),
        Some(Value::Null) => Ok(None),
        Some(Value::Int16(v)) => Ok(Some(*v as i32)),
        Some(Value::Int32(v)) => Ok(Some(*v)),
        Some(Value::Int64(v)) => i32::try_from(*v).map(Some).map_err(|_| ExecError::Int4OutOfRange),
        Some(other) => Err(ExecError::TypeMismatch {
            op: "array assignment",
            left: other.clone(),
            right: Value::Null,
        }),
    }
}

pub fn execute_insert_values(
    rel: crate::backend::storage::smgr::RelFileLocator,
    desc: &RelationDesc,
    indexes: &[BoundIndexRelation],
    rows: &[Vec<Value>],
    ctx: &mut ExecutorContext,
    xid: TransactionId,
    cid: CommandId,
) -> Result<usize, ExecError> {
    for values in rows {
        let tuple = tuple_from_values(desc, values)?;
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
    let tuple = tuple_from_values(&prepared.desc, &values)?;
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
    let mut affected_rows = 0;

    let desc = Rc::new(stmt.desc.clone());
    let attr_descs: Rc<[_]> = desc.attribute_descs().into();
    let decoder = Rc::new(CompiledTupleDecoder::compile(&desc, &attr_descs));
    let qual = stmt
        .predicate
        .as_ref()
        .map(|p| compile_predicate_with_decoder(p, &decoder));
    let target_rows = match &stmt.row_source {
        BoundModifyRowSource::Heap => {
            collect_matching_rows_heap(stmt.rel, &stmt.desc, stmt.predicate.as_ref(), ctx)?
        }
        BoundModifyRowSource::Index { index, keys } => collect_matching_rows_index(
            stmt.rel,
            &stmt.desc,
            index,
            keys,
            stmt.predicate.as_ref(),
            ctx,
        )?,
    };

    for (tid, original_values) in target_rows {
        let mut eval_slot = TupleSlot::virtual_row(original_values.clone());
        let mut values = original_values;
        for assignment in &stmt.assignments {
            let value = eval_expr(&assignment.expr, &mut eval_slot, ctx)?;
            apply_assignment_target(
                &stmt.desc,
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

        let replacement = tuple_from_values(&stmt.desc, &values)?;
        let mut current_tid = tid;
        let mut current_replacement = replacement;
        let mut current_values = values;
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
                    maintain_indexes_for_row(
                        stmt.rel,
                        &stmt.desc,
                        &stmt.indexes,
                        &current_values,
                        new_tid,
                        ctx,
                    )?;
                    affected_rows += 1;
                    break;
                }
                Err(HeapError::TupleUpdated(_old_tid, new_ctid)) => {
                    let new_tuple = heap_fetch(&*ctx.pool, ctx.client_id, stmt.rel, new_ctid)?;
                    let mut new_slot = TupleSlot::from_heap_tuple(
                        Rc::clone(&desc),
                        Rc::clone(&attr_descs),
                        new_ctid,
                        new_tuple,
                    );
                    let passes = match &qual {
                        Some(q) => q(&mut new_slot, ctx)?,
                        None => true,
                    };
                    if !passes {
                        break;
                    }
                    let new_values_base = new_slot.into_values()?;
                    let mut eval_slot = TupleSlot::virtual_row(new_values_base.clone());
                    let mut new_values = new_values_base;
                    for assignment in &stmt.assignments {
                        let value = eval_expr(&assignment.expr, &mut eval_slot, ctx)?;
                        apply_assignment_target(
                            &stmt.desc,
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
                    current_values = new_values.clone();
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
    let desc = Rc::new(stmt.desc.clone());
    let attr_descs: Rc<[_]> = desc.attribute_descs().into();
    let decoder = Rc::new(CompiledTupleDecoder::compile(&desc, &attr_descs));
    let qual = stmt
        .predicate
        .as_ref()
        .map(|p| compile_predicate_with_decoder(p, &decoder));
    let targets = match &stmt.row_source {
        BoundModifyRowSource::Heap => {
            collect_matching_rows_heap(stmt.rel, &stmt.desc, stmt.predicate.as_ref(), ctx)?
                .into_iter()
                .map(|(tid, _)| tid)
                .collect::<Vec<_>>()
        }
        BoundModifyRowSource::Index { index, keys } => collect_matching_rows_index(
            stmt.rel,
            &stmt.desc,
            index,
            keys,
            stmt.predicate.as_ref(),
            ctx,
        )?
        .into_iter()
        .map(|(tid, _)| tid)
        .collect::<Vec<_>>(),
    };
    let snapshot = ctx.snapshot.clone();

    let mut affected_rows = 0;
    for tid in &targets {
        let mut current_tid = *tid;
        loop {
            match heap_delete_with_waiter(
                &*ctx.pool,
                ctx.client_id,
                stmt.rel,
                &ctx.txns,
                xid,
                current_tid,
                &snapshot,
                waiter,
            ) {
                Ok(()) => {
                    affected_rows += 1;
                    break;
                }
                // Row was concurrently deleted — skip it.
                Err(HeapError::TupleAlreadyModified(_)) => {
                    break;
                }
                // Row was concurrently updated — follow ctid chain, recheck
                // predicate, and retry. Matches PostgreSQL's ExecDelete.
                Err(HeapError::TupleUpdated(_old_tid, new_ctid)) => {
                    let new_tuple = heap_fetch(&*ctx.pool, ctx.client_id, stmt.rel, new_ctid)?;
                    let mut new_slot = TupleSlot::from_heap_tuple(
                        Rc::clone(&desc),
                        Rc::clone(&attr_descs),
                        new_ctid,
                        new_tuple,
                    );
                    let passes = match &qual {
                        Some(q) => q(&mut new_slot, ctx)?,
                        None => true,
                    };
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
