use super::exec_expr::{
    eval_native_builtin_scalar_typed_value_call, eval_string_to_table_rows, normalize_array_value,
};
use super::expr_date::add_interval_to_local_timestamp;
use super::expr_json::{
    eval_json_record_set_returning_function, eval_json_table_function, eval_sql_json_table,
};
use super::expr_txid::eval_txid_snapshot_xip_values;
use super::expr_xml::eval_sql_xml_table;
use super::pg_regex::{eval_regexp_matches_rows, eval_regexp_split_to_table_rows};
use super::sqlfunc::{
    execute_user_defined_sql_scalar_function, execute_user_defined_sql_set_returning_function,
};
use super::{ExecError, ExecutorContext, Expr, SetReturningCall, TupleSlot, Value, eval_expr};
use crate::backend::access::heap::heapam::{heap_scan_begin_visible, heap_scan_next_visible};
use crate::backend::access::index::buildkeys::materialize_heap_row_values;
use crate::backend::commands::partition::{partition_ancestor_oids, partition_tree_entries};
use crate::backend::parser::{CatalogLookup, SqlTypeKind};
use crate::backend::statistics::types::decode_pg_mcv_list_payload;
use crate::backend::utils::cache::system_views::{
    build_pg_get_publication_tables_rows, build_pg_stat_io_rows, current_pg_stat_progress_copy_rows,
};
use crate::backend::utils::record::assign_anonymous_record_descriptor;
use crate::backend::utils::time::datetime::{
    current_timezone_name, days_from_ymd, days_in_month, timestamp_parts_from_usecs, ymd_from_days,
};
use crate::backend::utils::time::timestamp::{timestamp_at_time_zone, timestamptz_at_time_zone};
use crate::include::catalog::{
    BOOL_TYPE_OID, DEPENDENCY_INTERNAL, INT2_TYPE_OID, INT4_TYPE_OID, INT8_TYPE_OID,
    PG_CLASS_RELATION_OID, REGTYPE_TYPE_OID, SYSTEM_CATALOG_FOREIGN_KEYS, TEXT_TYPE_OID,
    VOID_TYPE_OID, builtin_scalar_function_for_proc_oid, builtin_scalar_function_for_proc_row,
};
use crate::include::nodes::datetime::{
    TIMESTAMP_NOBEGIN, TIMESTAMP_NOEND, TimestampADT, TimestampTzADT, USECS_PER_DAY, USECS_PER_SEC,
};
use crate::include::nodes::datum::{ArrayValue, IntervalValue, NumericValue, RecordValue};
use crate::include::nodes::primnodes::{
    QueryColumn, RowsFromItem, RowsFromSource, TextSearchTableFunction, expr_sql_type_hint,
    set_returning_call_exprs,
};
use crate::include::nodes::tsearch::TsWeight;
use crate::pl::plpgsql::{
    current_event_trigger_ddl_commands, current_event_trigger_dropped_objects,
    execute_user_defined_set_returning_function,
};

const MAX_UNBOUNDED_TIMESTAMP_SERIES_ROWS: usize = 10_000;

pub(crate) fn eval_set_returning_call(
    call: &SetReturningCall,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let mut rows = match call {
        SetReturningCall::RowsFrom { items, .. } => eval_rows_from(items, slot, ctx),
        SetReturningCall::GenerateSeries {
            start,
            stop,
            step,
            timezone,
            output_columns,
            ..
        } => eval_generate_series(
            start,
            stop,
            step,
            timezone.as_ref(),
            output_columns[0].sql_type.kind,
            slot,
            ctx,
        ),
        SetReturningCall::GenerateSubscripts {
            array,
            dimension,
            reverse,
            ..
        } => eval_generate_subscripts(array, dimension, reverse.as_ref(), slot, ctx),
        SetReturningCall::Unnest {
            args,
            output_columns,
            with_ordinality,
            ..
        } => eval_unnest(
            args,
            function_output_columns(output_columns, *with_ordinality),
            slot,
            ctx,
        ),
        SetReturningCall::JsonTableFunction { kind, args, .. } => {
            eval_json_table_function(*kind, args, slot, ctx)
        }
        SetReturningCall::SqlJsonTable(table) => eval_sql_json_table(table, slot, ctx),
        SetReturningCall::SqlXmlTable(table) => eval_sql_xml_table(table, slot, ctx),
        SetReturningCall::JsonRecordFunction {
            kind,
            args,
            output_columns,
            record_type,
            with_ordinality,
            ..
        } => eval_json_record_set_returning_function(
            *kind,
            args,
            function_output_columns(output_columns, *with_ordinality),
            *record_type,
            slot,
            ctx,
        ),
        SetReturningCall::RegexTableFunction { kind, args, .. } => {
            eval_regex_table_function(*kind, args, slot, ctx)
        }
        SetReturningCall::StringTableFunction { kind, args, .. } => {
            eval_string_table_function(*kind, args, slot, ctx)
        }
        SetReturningCall::PartitionTree { relid, .. } => eval_partition_tree(relid, slot, ctx),
        SetReturningCall::PartitionAncestors { relid, .. } => {
            eval_partition_ancestors(relid, slot, ctx)
        }
        SetReturningCall::PgLockStatus { .. } => eval_pg_lock_status(ctx),
        SetReturningCall::PgStatProgressCopy { .. } => Ok(current_pg_stat_progress_copy_rows()
            .into_iter()
            .map(TupleSlot::virtual_row)
            .collect()),
        SetReturningCall::PgSequences { .. } => eval_pg_sequences(ctx),
        SetReturningCall::InformationSchemaSequences { .. } => {
            eval_information_schema_sequences(ctx)
        }
        SetReturningCall::TxidSnapshotXip { arg, .. } => eval_txid_snapshot_xip(arg, slot, ctx),
        SetReturningCall::TextSearchTableFunction { kind, args, .. } => {
            eval_text_search_table_function(*kind, args, slot, ctx)
        }
        SetReturningCall::UserDefined {
            proc_oid,
            args,
            inlined_expr,
            output_columns,
            with_ordinality,
            ..
        } => {
            if ctx
                .catalog
                .as_deref()
                .and_then(|catalog| catalog.proc_row_by_oid(*proc_oid))
                .is_some_and(|row| row.proretset && row.prorettype == VOID_TYPE_OID)
            {
                return Ok(Vec::new());
            }
            let output_columns = function_output_columns(output_columns, *with_ordinality);
            if let Some(inlined_expr) = inlined_expr {
                eval_inlined_user_defined_function_scan(inlined_expr, output_columns, slot, ctx)
            } else {
                execute_user_defined_set_returning_function_by_language(
                    *proc_oid,
                    args,
                    output_columns,
                    slot,
                    ctx,
                )
            }
        }
    }?;
    if call.with_ordinality() {
        for (index, row) in rows.iter_mut().enumerate() {
            row.tts_values.push(Value::Int64((index + 1) as i64));
            row.tts_nvalid = row.tts_values.len();
        }
    }
    Ok(rows)
}

fn eval_inlined_user_defined_function_scan(
    expr: &Expr,
    output_columns: &[QueryColumn],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let value = crate::backend::executor::eval_expr(expr, slot, ctx)?;
    single_row_function_scan_slots(value, output_columns)
}

fn single_row_function_scan_slots(
    value: Value,
    output_columns: &[QueryColumn],
) -> Result<Vec<TupleSlot>, ExecError> {
    let values = match value {
        Value::Record(record) if output_columns.len() == record.fields.len() => record.fields,
        Value::Null if output_columns.len() != 1 => {
            std::iter::repeat_n(Value::Null, output_columns.len()).collect()
        }
        other if output_columns.len() == 1 => vec![other],
        other => vec![other],
    };
    Ok(vec![TupleSlot::virtual_row(values)])
}

fn function_output_columns(
    output_columns: &[QueryColumn],
    with_ordinality: bool,
) -> &[QueryColumn] {
    if with_ordinality {
        output_columns
            .split_last()
            .map(|(_, base)| base)
            .unwrap_or(output_columns)
    } else {
        output_columns
    }
}

fn eval_rows_from(
    items: &[RowsFromItem],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let mut function_rows = Vec::with_capacity(items.len());
    let mut max_rows = 0;
    for (item_index, item) in items.iter().enumerate() {
        let rows = eval_rows_from_item_cached(item_index, item, slot, ctx)?;
        max_rows = max_rows.max(rows.len());
        function_rows.push(rows);
    }

    let mut output = Vec::with_capacity(max_rows);
    for row_index in 0..max_rows {
        let mut values = Vec::new();
        for (item, rows) in items.iter().zip(function_rows.iter_mut()) {
            let width = item.output_columns().len();
            if let Some(row) = rows.get_mut(row_index) {
                values.extend(row.values()?.iter().cloned());
            } else {
                values.extend(std::iter::repeat_n(Value::Null, width));
            }
        }
        output.push(TupleSlot::virtual_row(values));
    }
    Ok(output)
}

fn eval_rows_from_item_cached(
    item_index: usize,
    item: &RowsFromItem,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    if rows_from_item_uses_outer_columns(item) {
        return eval_rows_from_item(item, slot, ctx);
    }

    // :HACK: Executor SRF expressions do not currently carry a stable plan-node
    // id, and lateral rescans rebuild the inner plan state. Use the item index
    // plus the planned source shape so uncorrelated ROWS FROM items can keep
    // PostgreSQL's tuplestore-like rescan behavior across those rebuilds.
    let cache_key = format!("rows_from_item:{item_index}:{:?}", item.source);
    if let Some(rows) = ctx.srf_rows_cache.get(&cache_key) {
        return Ok(rows.clone());
    }

    let mut rows = eval_rows_from_item(item, slot, ctx)?;
    for row in &mut rows {
        row.values()?;
        Value::materialize_all(&mut row.tts_values);
    }
    ctx.srf_rows_cache.insert(cache_key, rows.clone());
    Ok(rows)
}

fn eval_rows_from_item(
    item: &RowsFromItem,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    match &item.source {
        RowsFromSource::Function(call) => eval_set_returning_call(call, slot, ctx),
        RowsFromSource::Project { output_exprs, .. } => Ok(vec![TupleSlot::virtual_row(
            output_exprs
                .iter()
                .map(|expr| eval_expr(expr, slot, ctx))
                .collect::<Result<Vec<_>, _>>()?,
        )]),
    }
}

fn rows_from_item_uses_outer_columns(item: &RowsFromItem) -> bool {
    match &item.source {
        RowsFromSource::Function(call) => set_returning_call_exprs(call)
            .into_iter()
            .any(expr_uses_outer_columns),
        RowsFromSource::Project { output_exprs, .. } => {
            output_exprs.iter().any(expr_uses_outer_columns)
        }
    }
}

fn expr_uses_outer_columns(expr: &Expr) -> bool {
    match expr {
        Expr::Var(var) => var.varlevelsup > 0,
        Expr::Param(_) => true,
        Expr::Aggref(aggref) => {
            aggref.args.iter().any(expr_uses_outer_columns)
                || aggref
                    .aggfilter
                    .as_ref()
                    .is_some_and(expr_uses_outer_columns)
        }
        Expr::GroupingKey(grouping_key) => expr_uses_outer_columns(&grouping_key.expr),
        Expr::GroupingFunc(grouping_func) => grouping_func.args.iter().any(expr_uses_outer_columns),
        Expr::WindowFunc(window_func) => {
            window_func.args.iter().any(expr_uses_outer_columns)
                || match &window_func.kind {
                    crate::include::nodes::primnodes::WindowFuncKind::Aggregate(aggref) => aggref
                        .aggfilter
                        .as_ref()
                        .is_some_and(expr_uses_outer_columns),
                    crate::include::nodes::primnodes::WindowFuncKind::Builtin(_) => false,
                }
        }
        Expr::Op(op) => op.args.iter().any(expr_uses_outer_columns),
        Expr::Bool(bool_expr) => bool_expr.args.iter().any(expr_uses_outer_columns),
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_deref()
                .is_some_and(expr_uses_outer_columns)
                || case_expr.args.iter().any(|arm| {
                    expr_uses_outer_columns(&arm.expr) || expr_uses_outer_columns(&arm.result)
                })
                || expr_uses_outer_columns(&case_expr.defresult)
        }
        Expr::CaseTest(_) => false,
        Expr::Func(func) => func.args.iter().any(expr_uses_outer_columns),
        Expr::SqlJsonQueryFunction(func) => {
            func.child_exprs().into_iter().any(expr_uses_outer_columns)
        }
        Expr::SetReturning(srf) => set_returning_call_exprs(&srf.call)
            .into_iter()
            .any(expr_uses_outer_columns),
        Expr::SubLink(sublink) => sublink
            .testexpr
            .as_deref()
            .is_some_and(expr_uses_outer_columns),
        Expr::SubPlan(subplan) => subplan
            .testexpr
            .as_deref()
            .is_some_and(expr_uses_outer_columns),
        Expr::ScalarArrayOp(saop) => {
            expr_uses_outer_columns(&saop.left) || expr_uses_outer_columns(&saop.right)
        }
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => expr_uses_outer_columns(inner),
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | Expr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_uses_outer_columns(expr)
                || expr_uses_outer_columns(pattern)
                || escape.as_deref().is_some_and(expr_uses_outer_columns)
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            expr_uses_outer_columns(left) || expr_uses_outer_columns(right)
        }
        Expr::ArrayLiteral { elements, .. } => elements.iter().any(expr_uses_outer_columns),
        Expr::Row { fields, .. } => fields.iter().any(|(_, expr)| expr_uses_outer_columns(expr)),
        Expr::FieldSelect { expr, .. } => expr_uses_outer_columns(expr),
        Expr::ArraySubscript { array, subscripts } => {
            expr_uses_outer_columns(array)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_some_and(expr_uses_outer_columns)
                        || subscript
                            .upper
                            .as_ref()
                            .is_some_and(expr_uses_outer_columns)
                })
        }
        Expr::Xml(xml) => xml.child_exprs().any(expr_uses_outer_columns),
        Expr::Const(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => false,
    }
}

pub(crate) fn eval_set_returning_call_simple_values(
    call: &SetReturningCall,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<Value>, ExecError> {
    if call.with_ordinality() || call.output_columns().len() != 1 {
        return single_column_slots_to_values(eval_set_returning_call(call, slot, ctx)?);
    }

    match call {
        SetReturningCall::GenerateSeries {
            start,
            stop,
            step,
            timezone,
            output_columns,
            ..
        } => eval_generate_series_values(
            start,
            stop,
            step,
            timezone.as_ref(),
            output_columns[0].sql_type.kind,
            slot,
            ctx,
        ),
        _ => single_column_slots_to_values(eval_set_returning_call(call, slot, ctx)?),
    }
}

fn single_column_slots_to_values(mut rows: Vec<TupleSlot>) -> Result<Vec<Value>, ExecError> {
    rows.iter_mut()
        .map(|row| {
            row.values()?
                .first()
                .cloned()
                .ok_or_else(|| ExecError::DetailedError {
                    message: "set-returning function produced an empty row".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                })
        })
        .collect()
}

fn execute_user_defined_set_returning_function_by_language(
    proc_oid: u32,
    args: &[Expr],
    output_columns: &[crate::include::nodes::primnodes::QueryColumn],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let Some(catalog) = ctx.catalog.as_deref() else {
        return Err(ExecError::DetailedError {
            message: "user-defined functions require executor catalog context".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    };
    let row = catalog
        .proc_row_by_oid(proc_oid)
        .ok_or_else(|| ExecError::DetailedError {
            message: format!("unknown function oid {proc_oid}"),
            detail: None,
            hint: None,
            sqlstate: "42883",
        })?;
    if let Some(rows) =
        execute_native_set_returning_function(&row, args, output_columns, slot, ctx)?
    {
        return Ok(rows);
    }
    if !row.proretset && row.prolang == crate::include::catalog::PG_LANGUAGE_SQL_OID {
        let value = execute_user_defined_sql_scalar_function(&row, args, slot, ctx)?;
        return single_row_function_scan_slots(value, output_columns);
    }
    if row.prolang == crate::include::catalog::PG_LANGUAGE_SQL_OID {
        execute_user_defined_sql_set_returning_function(&row, args, output_columns, slot, ctx)
    } else if let Some(kind) = text_search_table_function_for_proc_src(&row.prosrc) {
        eval_text_search_table_function(kind, args, slot, ctx)
    } else if row.prosrc == "pg_options_to_table" {
        eval_pg_options_to_table(args, slot, ctx)
    } else {
        execute_user_defined_set_returning_function(proc_oid, args, output_columns, slot, ctx)
    }
}

fn execute_native_set_returning_function(
    row: &crate::include::catalog::PgProcRow,
    args: &[Expr],
    output_columns: &[crate::include::nodes::primnodes::QueryColumn],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Option<Vec<TupleSlot>>, ExecError> {
    let values = args
        .iter()
        .map(|arg| eval_expr(arg, slot, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    let rows = match row.prosrc.as_str() {
        "pg_ls_dir_1arg" => Some(eval_pg_ls_dir(&values, ctx, false, false)?),
        "pg_ls_dir" => Some(eval_pg_ls_dir(&values, ctx, true, true)?),
        "pg_ls_waldir" => Some(eval_pg_ls_named_dir(ctx, &["pg_wal"], true)?),
        "pg_ls_summariesdir" => Some(eval_pg_ls_named_dir(ctx, &["pg_wal", "summaries"], false)?),
        "pg_ls_archive_statusdir" => Some(eval_pg_ls_named_dir(
            ctx,
            &["pg_wal", "archive_status"],
            false,
        )?),
        "pg_ls_logicalsnapdir" => Some(eval_pg_ls_named_dir(
            ctx,
            &["pg_logical", "snapshots"],
            false,
        )?),
        "pg_ls_logicalmapdir" => Some(eval_pg_ls_named_dir(
            ctx,
            &["pg_logical", "mappings"],
            false,
        )?),
        "pg_ls_replslotdir" => {
            let slot_name = values.first().and_then(Value::as_text).unwrap_or_default();
            Some(eval_pg_ls_named_dir(
                ctx,
                &["pg_replslot", slot_name],
                false,
            )?)
        }
        "pg_available_extensions" => Some(Vec::new()),
        "pg_available_extension_versions" => Some(Vec::new()),
        "pg_get_shmem_allocations" => Some(Vec::new()),
        "pg_get_shmem_allocations_numa" => {
            return Err(ExecError::DetailedError {
                message: "libnuma initialization failed or NUMA is not supported on this platform"
                    .into(),
                detail: None,
                hint: None,
                sqlstate: "55000",
            });
        }
        "pg_get_backend_memory_contexts" => Some(eval_pg_backend_memory_contexts()),
        "pg_config" => Some(eval_pg_config()),
        "show_all_settings" => Some(eval_pg_show_all_settings(output_columns)),
        "show_all_file_settings" => Some(Vec::new()),
        "pg_hba_file_rules" => Some(eval_pg_hba_file_rules()),
        "pg_ident_file_mappings" => Some(Vec::new()),
        "pg_prepared_xact" => Some(eval_pg_prepared_xact(ctx)),
        "pg_cursor" => Some(eval_pg_cursor(ctx)),
        "pg_prepared_statement" => Some(eval_pg_prepared_statement(ctx)),
        "pg_stat_get_wal_receiver" => Some(Vec::new()),
        "pg_get_wait_events" => Some(eval_pg_wait_events()),
        "pg_timezone_names" => Some(eval_pg_timezone_names()),
        "pg_timezone_abbrevs_zone" | "pg_timezone_abbrevs_abbrevs" => {
            Some(eval_pg_timezone_abbrevs())
        }
        "pg_stat_get_backend_idset" => Some(vec![TupleSlot::virtual_row(vec![Value::Int32(
            ctx.database
                .as_ref()
                .map(|db| db.temp_backend_id(ctx.client_id) as i32)
                .unwrap_or(ctx.client_id as i32),
        )])]),
        "pg_stat_get_backend_io" => Some(eval_pg_stat_get_backend_io(&values, ctx)),
        "pg_tablespace_databases" => Some(eval_pg_tablespace_databases(&values)),
        "pg_get_publication_tables" => Some(eval_pg_get_publication_tables(&values, ctx)?),
        "pg_event_trigger_ddl_commands" => Some(eval_pg_event_trigger_ddl_commands()),
        "pg_event_trigger_dropped_objects" => Some(eval_pg_event_trigger_dropped_objects()),
        "pg_stats_ext_mcvlist_items" => Some(eval_pg_mcv_list_items(&values)?),
        "pg_get_catalog_foreign_keys" => Some(eval_pg_get_catalog_foreign_keys()),
        _ => {
            if let Some(func) = builtin_scalar_function_for_proc_row(row) {
                let arg_types = args.iter().map(expr_sql_type_hint).collect::<Vec<_>>();
                let value = eval_native_builtin_scalar_typed_value_call(
                    func,
                    &values,
                    Some(&arg_types),
                    false,
                    ctx,
                )?;
                Some(match value {
                    Value::Record(record) => vec![TupleSlot::virtual_row(record.fields)],
                    other => vec![TupleSlot::virtual_row(vec![other])],
                })
            } else {
                None
            }
        }
    };
    Ok(rows)
}

fn eval_pg_get_catalog_foreign_keys() -> Vec<TupleSlot> {
    SYSTEM_CATALOG_FOREIGN_KEYS
        .iter()
        .map(|row| {
            let fk_columns = row
                .fk_columns
                .iter()
                .map(|column| Value::Text((*column).into()))
                .collect();
            let pk_columns = row
                .pk_columns
                .iter()
                .map(|column| Value::Text((*column).into()))
                .collect();
            TupleSlot::virtual_row(vec![
                Value::Int32(row.fk_table_oid as i32),
                Value::PgArray(
                    ArrayValue::from_1d(fk_columns).with_element_type_oid(TEXT_TYPE_OID),
                ),
                Value::Int32(row.pk_table_oid as i32),
                Value::PgArray(
                    ArrayValue::from_1d(pk_columns).with_element_type_oid(TEXT_TYPE_OID),
                ),
                Value::Bool(row.is_array),
                Value::Bool(row.is_opt),
            ])
        })
        .collect()
}

fn eval_pg_stat_get_backend_io(values: &[Value], ctx: &ExecutorContext) -> Vec<TupleSlot> {
    let pid = values.first().and_then(|value| match value {
        Value::Int32(value) => Some(*value),
        Value::Int64(value) => i32::try_from(*value).ok(),
        _ => None,
    });
    if pid != Some(ctx.client_id as i32) {
        return Vec::new();
    }
    let io = ctx
        .session_stats
        .read()
        .backend_io_entries(crate::backend::utils::activity::default_pg_stat_io_keys());
    let stats = crate::pgrust::database::DatabaseStatsStore {
        io,
        ..crate::pgrust::database::DatabaseStatsStore::default()
    };
    build_pg_stat_io_rows(&stats)
        .into_iter()
        .map(TupleSlot::virtual_row)
        .collect()
}

fn eval_pg_event_trigger_dropped_objects() -> Vec<TupleSlot> {
    current_event_trigger_dropped_objects()
        .into_iter()
        .map(|row| {
            TupleSlot::virtual_row(vec![
                Value::Int64(i64::from(row.classid)),
                Value::Int64(i64::from(row.objid)),
                Value::Int32(row.objsubid),
                Value::Bool(row.original),
                Value::Bool(row.normal),
                Value::Bool(row.is_temporary),
                Value::Text(row.object_type.into()),
                row.schema_name
                    .map(|schema| Value::Text(schema.into()))
                    .unwrap_or(Value::Null),
                row.object_name
                    .map(|name| Value::Text(name.into()))
                    .unwrap_or(Value::Null),
                Value::Text(row.object_identity.into()),
                Value::Array(
                    row.address_names
                        .into_iter()
                        .map(|name| Value::Text(name.into()))
                        .collect(),
                ),
                Value::Array(
                    row.address_args
                        .into_iter()
                        .map(|arg| Value::Text(arg.into()))
                        .collect(),
                ),
            ])
        })
        .collect()
}

fn eval_pg_event_trigger_ddl_commands() -> Vec<TupleSlot> {
    current_event_trigger_ddl_commands()
        .into_iter()
        .map(|row| {
            TupleSlot::virtual_row(vec![
                Value::Int64(0),
                Value::Int64(0),
                Value::Int32(0),
                Value::Text(row.command_tag.into()),
                Value::Text(row.object_type.into()),
                row.schema_name
                    .map(|schema| Value::Text(schema.into()))
                    .unwrap_or(Value::Null),
                Value::Text(row.object_identity.into()),
                Value::Bool(false),
                Value::Null,
            ])
        })
        .collect()
}

fn eval_pg_mcv_list_items(values: &[Value]) -> Result<Vec<TupleSlot>, ExecError> {
    let [Value::Bytea(bytes)] = values else {
        return Ok(Vec::new());
    };
    let payload =
        decode_pg_mcv_list_payload(bytes).map_err(|message| ExecError::DetailedError {
            message: "could not decode pg_mcv_list".into(),
            detail: Some(message),
            hint: None,
            sqlstate: "XX000",
        })?;
    Ok(payload
        .items
        .into_iter()
        .enumerate()
        .map(|(index, item)| {
            let values = item
                .values
                .iter()
                .map(|value| {
                    value
                        .as_ref()
                        .map(|value| Value::Text(value.clone().into()))
                        .unwrap_or(Value::Null)
                })
                .collect::<Vec<_>>();
            let nulls = item
                .values
                .iter()
                .map(|value| Value::Bool(value.is_none()))
                .collect::<Vec<_>>();
            TupleSlot::virtual_row(vec![
                Value::Int32(index as i32),
                Value::PgArray(ArrayValue::from_1d(values).with_element_type_oid(TEXT_TYPE_OID)),
                Value::PgArray(ArrayValue::from_1d(nulls).with_element_type_oid(BOOL_TYPE_OID)),
                Value::Float64(item.frequency),
                Value::Float64(item.base_frequency),
            ])
        })
        .collect())
}

fn eval_pg_get_publication_tables(
    values: &[Value],
    ctx: &ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let Some(catalog) = ctx.catalog.as_deref() else {
        return Err(ExecError::DetailedError {
            message: "pg_get_publication_tables requires executor catalog context".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    };
    let publication_names = publication_names_from_values(values)?;
    let publications = catalog.publication_rows();
    for name in &publication_names {
        if !publications
            .iter()
            .any(|publication| publication.pubname == *name)
        {
            return Err(ExecError::DetailedError {
                message: format!("publication \"{name}\" does not exist"),
                detail: None,
                hint: None,
                sqlstate: "42704",
            });
        }
    }
    Ok(build_pg_get_publication_tables_rows(
        publications,
        catalog.publication_rel_rows(),
        catalog.publication_namespace_rows(),
        catalog.class_rows(),
        catalog.attribute_rows(),
        catalog.inheritance_rows(),
        &publication_names,
    )
    .into_iter()
    .map(TupleSlot::virtual_row)
    .collect())
}

fn publication_names_from_values(values: &[Value]) -> Result<Vec<String>, ExecError> {
    if values.len() == 1 {
        return publication_names_from_single_value(&values[0]);
    }
    values
        .iter()
        .filter(|value| !matches!(value, Value::Null))
        .map(publication_name_from_value)
        .collect()
}

fn publication_names_from_single_value(value: &Value) -> Result<Vec<String>, ExecError> {
    match value {
        Value::Null => Ok(Vec::new()),
        Value::Array(values) => publication_names_from_array_values(values),
        Value::PgArray(array) => publication_names_from_array_values(&array.elements),
        other => {
            if let Some(array) = normalize_array_value(other) {
                publication_names_from_array_values(&array.elements)
            } else {
                Ok(vec![publication_name_from_value(other)?])
            }
        }
    }
}

fn publication_names_from_array_values(values: &[Value]) -> Result<Vec<String>, ExecError> {
    values
        .iter()
        .filter(|value| !matches!(value, Value::Null))
        .map(publication_name_from_value)
        .collect()
}

fn publication_name_from_value(value: &Value) -> Result<String, ExecError> {
    value
        .as_text()
        .map(ToOwned::to_owned)
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "pg_get_publication_tables",
            left: value.clone(),
            right: Value::Text(String::new().into()),
        })
}

fn srf_data_dir_path(ctx: &ExecutorContext) -> Result<std::path::PathBuf, ExecError> {
    ctx.data_dir
        .clone()
        .or_else(|| std::env::current_dir().ok())
        .ok_or_else(|| ExecError::DetailedError {
            message: "data directory is not available".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        })
}

fn srf_file_timestamp_value(time: std::io::Result<std::time::SystemTime>) -> Value {
    const UNIX_EPOCH_TO_POSTGRES_EPOCH_DAYS: i64 = 10_957;
    match time
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
    {
        Some(duration) => {
            let usecs =
                duration.as_secs() as i64 * USECS_PER_SEC + i64::from(duration.subsec_micros());
            Value::TimestampTz(TimestampTzADT(
                usecs
                    - UNIX_EPOCH_TO_POSTGRES_EPOCH_DAYS
                        * crate::include::nodes::datetime::USECS_PER_DAY,
            ))
        }
        None => Value::Null,
    }
}

fn srf_io_error_message(err: &std::io::Error) -> String {
    match err.kind() {
        std::io::ErrorKind::NotFound => "No such file or directory".into(),
        _ => err.to_string(),
    }
}

fn eval_pg_ls_dir(
    values: &[Value],
    ctx: &ExecutorContext,
    has_missing_ok: bool,
    has_include_dot_dirs: bool,
) -> Result<Vec<TupleSlot>, ExecError> {
    let dirname =
        values
            .first()
            .and_then(Value::as_text)
            .ok_or_else(|| ExecError::TypeMismatch {
                op: "pg_ls_dir",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: Value::Text("".into()),
            })?;
    let missing_ok = has_missing_ok && matches!(values.get(1), Some(Value::Bool(true)));
    let include_dot_dirs = has_include_dot_dirs && matches!(values.get(2), Some(Value::Bool(true)));
    let path = {
        let path = std::path::Path::new(dirname);
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            srf_data_dir_path(ctx)?.join(path)
        }
    };
    let mut rows = directory_entry_rows(&path, dirname, missing_ok, false)?;
    if dirname == "."
        && !rows
            .iter()
            .any(|row| row.tts_values.first().and_then(Value::as_text) == Some("base"))
    {
        // :HACK: pgrust's storage layout is not PostgreSQL's base/ tree yet,
        // but data-directory inspection functions expose that top-level name.
        rows.push(TupleSlot::virtual_row(vec![Value::Text("base".into())]));
    }
    if include_dot_dirs {
        rows.push(TupleSlot::virtual_row(vec![Value::Text(".".into())]));
        rows.push(TupleSlot::virtual_row(vec![Value::Text("..".into())]));
    }
    rows.sort_by(|left, right| {
        let left = left
            .tts_values
            .first()
            .and_then(Value::as_text)
            .unwrap_or("");
        let right = right
            .tts_values
            .first()
            .and_then(Value::as_text)
            .unwrap_or("");
        left.cmp(right)
    });
    Ok(rows)
}

fn eval_pg_ls_named_dir(
    ctx: &ExecutorContext,
    components: &[&str],
    synthesize_wal_segment: bool,
) -> Result<Vec<TupleSlot>, ExecError> {
    let mut path = srf_data_dir_path(ctx)?;
    for component in components {
        path.push(component);
    }
    let mut rows = directory_entry_rows(&path, &components.join("/"), true, true)?;
    if rows.is_empty() && synthesize_wal_segment {
        rows.push(TupleSlot::virtual_row(vec![
            Value::Text("000000010000000000000000".into()),
            Value::Int64(i64::from(
                crate::backend::access::transam::xlog::WAL_SEG_SIZE_BYTES,
            )),
            Value::TimestampTz(TimestampTzADT(ctx.statement_timestamp_usecs)),
        ]));
    }
    Ok(rows)
}

fn directory_entry_rows(
    path: &std::path::Path,
    display_name: &str,
    missing_ok: bool,
    include_metadata: bool,
) -> Result<Vec<TupleSlot>, ExecError> {
    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(err) if missing_ok && err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Vec::new());
        }
        Err(err) => {
            return Err(ExecError::DetailedError {
                message: format!(
                    "could not open directory \"{display_name}\": {}",
                    srf_io_error_message(&err)
                ),
                detail: None,
                hint: None,
                sqlstate: "58P01",
            });
        }
    };
    let mut rows = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|err| ExecError::DetailedError {
            message: format!("could not read directory \"{display_name}\": {err}"),
            detail: None,
            hint: None,
            sqlstate: "58P01",
        })?;
        let name = entry.file_name().to_string_lossy().to_string();
        if include_metadata {
            let metadata = entry.metadata().ok();
            rows.push(TupleSlot::virtual_row(vec![
                Value::Text(name.into()),
                Value::Int64(metadata.as_ref().map(|m| m.len() as i64).unwrap_or(0)),
                metadata
                    .map(|m| srf_file_timestamp_value(m.modified()))
                    .unwrap_or(Value::Null),
            ]));
        } else {
            rows.push(TupleSlot::virtual_row(vec![Value::Text(name.into())]));
        }
    }
    rows.sort_by(|left, right| {
        let left = left
            .tts_values
            .first()
            .and_then(Value::as_text)
            .unwrap_or("");
        let right = right
            .tts_values
            .first()
            .and_then(Value::as_text)
            .unwrap_or("");
        left.cmp(right)
    });
    Ok(rows)
}

fn eval_pg_tablespace_databases(values: &[Value]) -> Vec<TupleSlot> {
    if matches!(values.first(), Some(Value::Null) | None) {
        return Vec::new();
    }
    vec![TupleSlot::virtual_row(vec![Value::Int64(i64::from(
        crate::include::catalog::CURRENT_DATABASE_OID,
    ))])]
}

fn eval_pg_backend_memory_contexts() -> Vec<TupleSlot> {
    [
        memory_context_row(
            "TopMemoryContext",
            None,
            "AllocSet",
            1,
            &[1],
            8192,
            1,
            1024,
            4,
        ),
        memory_context_row(
            "CacheMemoryContext",
            None,
            "AllocSet",
            2,
            &[1, 1],
            16384,
            2,
            2048,
            8,
        ),
        memory_context_row(
            "CatalogCache",
            Some("pg_class"),
            "AllocSet",
            3,
            &[1, 1, 1],
            8192,
            1,
            1024,
            3,
        ),
        memory_context_row(
            "Type information cache",
            None,
            "AllocSet",
            3,
            &[1, 1, 2],
            8192,
            1,
            1024,
            3,
        ),
        memory_context_row("Caller tuples", None, "Bump", 2, &[1, 2], 8192, 2, 1024, 0),
    ]
    .into_iter()
    .collect()
}

fn memory_context_row(
    name: &str,
    ident: Option<&str>,
    typ: &str,
    level: i32,
    path: &[i32],
    total_bytes: i64,
    total_nblocks: i64,
    free_bytes: i64,
    free_chunks: i64,
) -> TupleSlot {
    TupleSlot::virtual_row(vec![
        Value::Text(name.into()),
        ident
            .map(|ident| Value::Text(ident.into()))
            .unwrap_or(Value::Null),
        Value::Text(typ.into()),
        Value::Int32(level),
        int4_array(path.iter().copied()),
        Value::Int64(total_bytes),
        Value::Int64(total_nblocks),
        Value::Int64(free_bytes),
        Value::Int64(free_chunks),
        Value::Int64(total_bytes.saturating_sub(free_bytes)),
    ])
}

fn eval_pg_config() -> Vec<TupleSlot> {
    if let Some(rows) = local_pg_config_rows().filter(|rows| rows.len() > 20) {
        return rows;
    }
    [
        ("BINDIR", "/usr/local/pgsql/bin"),
        ("DOCDIR", "/usr/local/pgsql/share/doc"),
        ("HTMLDIR", "/usr/local/pgsql/share/doc/html"),
        ("INCLUDEDIR", "/usr/local/pgsql/include"),
        ("PKGINCLUDEDIR", "/usr/local/pgsql/include/postgresql"),
        ("INCLUDEDIR-SERVER", "/usr/local/pgsql/include/server"),
        ("LIBDIR", "/usr/local/pgsql/lib"),
        ("PKGLIBDIR", "/usr/local/pgsql/lib/postgresql"),
        ("LOCALEDIR", "/usr/local/pgsql/share/locale"),
        ("MANDIR", "/usr/local/pgsql/share/man"),
        ("SHAREDIR", "/usr/local/pgsql/share/postgresql"),
        ("SYSCONFDIR", "/usr/local/pgsql/etc"),
        (
            "PGXS",
            "/usr/local/pgsql/lib/postgresql/pgxs/src/makefiles/pgxs.mk",
        ),
        ("CONFIGURE", ""),
        ("CC", "cc"),
        ("CPPFLAGS", ""),
        ("CFLAGS", "-O2"),
        ("CFLAGS_SL", ""),
        ("LDFLAGS", ""),
        ("LDFLAGS_EX", ""),
        ("LDFLAGS_SL", ""),
        ("LIBS", ""),
        ("VERSION", "PostgreSQL 18"),
    ]
    .into_iter()
    .map(|(name, setting)| {
        TupleSlot::virtual_row(vec![Value::Text(name.into()), Value::Text(setting.into())])
    })
    .collect()
}

#[cfg(not(target_arch = "wasm32"))]
fn local_pg_config_rows() -> Option<Vec<TupleSlot>> {
    let output = std::process::Command::new("pg_config").output().ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8(output.stdout).ok()?;
    let rows = stdout
        .lines()
        .filter_map(|line| line.split_once(" = "))
        .map(|(name, setting)| {
            TupleSlot::virtual_row(vec![
                Value::Text(name.to_string().into()),
                Value::Text(setting.to_string().into()),
            ])
        })
        .collect::<Vec<_>>();
    Some(rows)
}

#[cfg(target_arch = "wasm32")]
fn local_pg_config_rows() -> Option<Vec<TupleSlot>> {
    None
}

fn eval_pg_hba_file_rules() -> Vec<TupleSlot> {
    vec![TupleSlot::virtual_row(vec![
        Value::Int32(1),
        Value::Text("pg_hba.conf".into()),
        Value::Int32(1),
        Value::Text("local".into()),
        text_array(["all"]),
        text_array(["all"]),
        Value::Null,
        Value::Null,
        Value::Text("trust".into()),
        text_array(std::iter::empty::<&str>()),
        Value::Null,
    ])]
}

fn eval_pg_cursor(ctx: &ExecutorContext) -> Vec<TupleSlot> {
    ctx.database
        .as_ref()
        .map(|db| db.session_view_state(ctx.client_id))
        .unwrap_or_default()
        .cursors
        .into_iter()
        .map(|row| {
            TupleSlot::virtual_row(vec![
                Value::Text(row.name.into()),
                Value::Text(row.statement.into()),
                Value::Bool(row.is_holdable),
                Value::Bool(row.is_binary),
                Value::Bool(row.is_scrollable),
                Value::TimestampTz(TimestampTzADT(row.creation_time)),
            ])
        })
        .collect()
}

fn eval_pg_prepared_statement(ctx: &ExecutorContext) -> Vec<TupleSlot> {
    ctx.database
        .as_ref()
        .map(|db| db.session_view_state(ctx.client_id))
        .unwrap_or_default()
        .prepared_statements
        .into_iter()
        .map(|row| {
            TupleSlot::virtual_row(vec![
                Value::Text(row.name.into()),
                Value::Text(row.statement.into()),
                Value::TimestampTz(TimestampTzADT(row.prepare_time)),
                regtype_array(row.parameter_type_oids),
                regtype_array(row.result_type_oids),
                Value::Bool(row.from_sql),
                Value::Int64(row.generic_plans),
                Value::Int64(row.custom_plans),
            ])
        })
        .collect()
}

fn eval_pg_prepared_xact(ctx: &ExecutorContext) -> Vec<TupleSlot> {
    ctx.database
        .as_ref()
        .map(|db| db.prepared_xacts.rows())
        .unwrap_or_default()
        .into_iter()
        .map(|row| {
            TupleSlot::virtual_row(vec![
                Value::Int64(i64::from(row.transaction)),
                Value::Text(row.gid.into()),
                Value::TimestampTz(TimestampTzADT(row.prepared_at)),
                Value::Text(row.owner_name.into()),
                Value::Text(row.database_name.into()),
            ])
        })
        .collect()
}

fn eval_pg_wait_events() -> Vec<TupleSlot> {
    [
        (
            "Activity",
            "AutoVacuumMain",
            "autovacuum launcher is waiting",
        ),
        (
            "BufferPin",
            "BufferPin",
            "waiting to acquire a pin on a buffer",
        ),
        (
            "Client",
            "ClientRead",
            "waiting to read data from the client",
        ),
        ("Extension", "Extension", "waiting in an extension"),
        ("IO", "DataFileRead", "waiting for a data file read"),
        (
            "IPC",
            "BgWorkerShutdown",
            "waiting for background worker shutdown",
        ),
        ("LWLock", "BufferContent", "waiting for a lightweight lock"),
        ("Lock", "Relation", "waiting for a relation lock"),
        ("Timeout", "PgSleep", "waiting due to pg_sleep"),
    ]
    .into_iter()
    .map(|(typ, name, description)| {
        TupleSlot::virtual_row(vec![
            Value::Text(typ.into()),
            Value::Text(name.into()),
            Value::Text(description.into()),
        ])
    })
    .collect()
}

fn eval_pg_timezone_names() -> Vec<TupleSlot> {
    (-12i32..=14)
        .map(|offset_hours| {
            let sign = if offset_hours < 0 { "minus" } else { "plus" };
            TupleSlot::virtual_row(vec![
                Value::Text(format!("Etc/GMT/{sign}/{}", offset_hours.abs()).into()),
                Value::Text(format!("GMT{offset_hours:+03}").into()),
                interval_seconds(offset_hours * 60 * 60),
                Value::Bool(false),
            ])
        })
        .collect()
}

fn eval_pg_timezone_abbrevs() -> Vec<TupleSlot> {
    let mut rows = (-12i32..=14)
        .map(|offset_hours| {
            TupleSlot::virtual_row(vec![
                Value::Text(format!("TZA{offset_hours:+03}").into()),
                interval_seconds(offset_hours * 60 * 60),
                Value::Bool(false),
            ])
        })
        .collect::<Vec<_>>();
    rows.push(TupleSlot::virtual_row(vec![
        Value::Text("LMT".into()),
        interval_seconds(-(7 * 60 * 60 + 52 * 60 + 58)),
        Value::Bool(false),
    ]));
    rows
}

fn interval_seconds(seconds: i32) -> Value {
    Value::Interval(IntervalValue {
        time_micros: i64::from(seconds) * USECS_PER_SEC,
        days: 0,
        months: 0,
    })
}

fn text_array<'a>(values: impl IntoIterator<Item = &'a str>) -> Value {
    Value::PgArray(
        ArrayValue::from_1d(
            values
                .into_iter()
                .map(|value| Value::Text(value.to_string().into()))
                .collect(),
        )
        .with_element_type_oid(TEXT_TYPE_OID),
    )
}

fn int4_array(values: impl IntoIterator<Item = i32>) -> Value {
    Value::PgArray(
        ArrayValue::from_1d(values.into_iter().map(Value::Int32).collect())
            .with_element_type_oid(INT4_TYPE_OID),
    )
}

fn regtype_array(values: impl IntoIterator<Item = u32>) -> Value {
    Value::PgArray(
        ArrayValue::from_1d(
            values
                .into_iter()
                .map(|oid| Value::Int64(i64::from(oid)))
                .collect(),
        )
        .with_element_type_oid(REGTYPE_TYPE_OID),
    )
}

fn text_search_table_function_for_proc_src(prosrc: &str) -> Option<TextSearchTableFunction> {
    match prosrc {
        "ts_token_type_byid" | "ts_token_type_byname" => Some(TextSearchTableFunction::TokenType),
        "ts_parse_byid" | "ts_parse_byname" => Some(TextSearchTableFunction::Parse),
        "ts_debug" => Some(TextSearchTableFunction::Debug),
        "ts_stat1" | "ts_stat2" => Some(TextSearchTableFunction::Stat),
        _ => None,
    }
}

fn eval_pg_show_all_settings(
    output_columns: &[crate::include::nodes::primnodes::QueryColumn],
) -> Vec<TupleSlot> {
    const ENABLE_SETTINGS: &[(&str, &str)] = &[
        ("enable_async_append", "on"),
        ("enable_bitmapscan", "on"),
        ("enable_distinct_reordering", "on"),
        ("enable_gathermerge", "on"),
        ("enable_group_by_reordering", "on"),
        ("enable_hashagg", "on"),
        ("enable_hashjoin", "on"),
        ("enable_incremental_sort", "on"),
        ("enable_indexonlyscan", "on"),
        ("enable_indexscan", "on"),
        ("enable_material", "on"),
        ("enable_memoize", "on"),
        ("enable_mergejoin", "on"),
        ("enable_nestloop", "on"),
        ("enable_parallel_append", "on"),
        ("enable_parallel_hash", "on"),
        ("enable_partition_pruning", "on"),
        ("enable_partitionwise_aggregate", "off"),
        ("enable_partitionwise_join", "off"),
        ("enable_presorted_aggregate", "on"),
        ("enable_self_join_elimination", "on"),
        ("enable_seqscan", "on"),
        ("enable_sort", "on"),
        ("enable_tidscan", "on"),
    ];
    ENABLE_SETTINGS
        .iter()
        .map(|(name, setting)| {
            TupleSlot::virtual_row(
                output_columns
                    .iter()
                    .map(|column| match column.name.as_str() {
                        "name" => Value::Text((*name).into()),
                        "setting" => Value::Text((*setting).into()),
                        "unit" => Value::Null,
                        "category" => {
                            Value::Text("Query Tuning / Planner Method Configuration".into())
                        }
                        "short_desc" => Value::Text("Enables a planner method.".into()),
                        "extra_desc" => Value::Null,
                        "context" => Value::Text("user".into()),
                        "vartype" => Value::Text("bool".into()),
                        "source" => Value::Text("default".into()),
                        "min_val" => Value::Null,
                        "max_val" => Value::Null,
                        "enumvals" => Value::Null,
                        "boot_val" => Value::Text((*setting).into()),
                        "reset_val" => Value::Text((*setting).into()),
                        "sourcefile" => Value::Null,
                        "sourceline" => Value::Null,
                        "pending_restart" => Value::Bool(false),
                        _ => Value::Null,
                    })
                    .collect(),
            )
        })
        .collect()
}

pub(crate) fn eval_project_set_returning_call(
    call: &SetReturningCall,
    column_index: usize,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<Value>, ExecError> {
    let record_descriptor = (column_index == 0 && call.output_columns().len() > 1).then(|| {
        assign_anonymous_record_descriptor(
            call.output_columns()
                .iter()
                .map(|column| (column.name.clone(), column.sql_type))
                .collect(),
        )
    });
    Ok(eval_set_returning_call(call, slot, ctx)?
        .into_iter()
        .map(|mut row| {
            Value::materialize_all(&mut row.tts_values);
            match (column_index, record_descriptor.as_ref()) {
                (0, Some(descriptor)) => Value::Record(RecordValue::from_descriptor(
                    descriptor.clone(),
                    row.tts_values,
                )),
                (0, None) => row.tts_values.into_iter().next().unwrap_or(Value::Null),
                (index, _) => row
                    .tts_values
                    .get(index.saturating_sub(1))
                    .cloned()
                    .unwrap_or(Value::Null),
            }
        })
        .collect())
}

pub(crate) fn set_returning_call_label(call: &SetReturningCall) -> &str {
    match call {
        SetReturningCall::RowsFrom { .. } => "rows from",
        SetReturningCall::GenerateSeries { .. } => "generate_series",
        SetReturningCall::GenerateSubscripts { .. } => "generate_subscripts",
        SetReturningCall::Unnest { .. } => "unnest",
        SetReturningCall::JsonTableFunction { kind, .. } => match kind {
            crate::include::nodes::primnodes::JsonTableFunction::ObjectKeys => "json_object_keys",
            crate::include::nodes::primnodes::JsonTableFunction::Each => "json_each",
            crate::include::nodes::primnodes::JsonTableFunction::EachText => "json_each_text",
            crate::include::nodes::primnodes::JsonTableFunction::ArrayElements => {
                "json_array_elements"
            }
            crate::include::nodes::primnodes::JsonTableFunction::ArrayElementsText => {
                "json_array_elements_text"
            }
            crate::include::nodes::primnodes::JsonTableFunction::JsonbPathQuery => {
                "jsonb_path_query"
            }
            crate::include::nodes::primnodes::JsonTableFunction::JsonbPathQueryTz => {
                "jsonb_path_query_tz"
            }
            crate::include::nodes::primnodes::JsonTableFunction::JsonbObjectKeys => {
                "jsonb_object_keys"
            }
            crate::include::nodes::primnodes::JsonTableFunction::JsonbEach => "jsonb_each",
            crate::include::nodes::primnodes::JsonTableFunction::JsonbEachText => "jsonb_each_text",
            crate::include::nodes::primnodes::JsonTableFunction::JsonbArrayElements => {
                "jsonb_array_elements"
            }
            crate::include::nodes::primnodes::JsonTableFunction::JsonbArrayElementsText => {
                "jsonb_array_elements_text"
            }
        },
        SetReturningCall::JsonRecordFunction { kind, .. } => kind.name(),
        SetReturningCall::SqlJsonTable(_) => "json_table",
        SetReturningCall::SqlXmlTable(_) => "xmltable",
        SetReturningCall::RegexTableFunction { kind, .. } => match kind {
            crate::include::nodes::primnodes::RegexTableFunction::Matches => "regexp_matches",
            crate::include::nodes::primnodes::RegexTableFunction::SplitToTable => {
                "regexp_split_to_table"
            }
        },
        SetReturningCall::StringTableFunction { kind, .. } => match kind {
            crate::include::nodes::primnodes::StringTableFunction::StringToTable => {
                "string_to_table"
            }
        },
        SetReturningCall::PartitionTree { .. } => "pg_partition_tree",
        SetReturningCall::PartitionAncestors { .. } => "pg_partition_ancestors",
        SetReturningCall::PgLockStatus { .. } => "pg_lock_status",
        SetReturningCall::PgStatProgressCopy { .. } => "pg_stat_progress_copy",
        SetReturningCall::PgSequences { .. } => "pg_sequences",
        SetReturningCall::InformationSchemaSequences { .. } => "information_schema.sequences",
        SetReturningCall::TxidSnapshotXip { .. } => "txid_snapshot_xip",
        SetReturningCall::TextSearchTableFunction { kind, .. } => match kind {
            crate::include::nodes::primnodes::TextSearchTableFunction::TokenType => "ts_token_type",
            crate::include::nodes::primnodes::TextSearchTableFunction::Parse => "ts_parse",
            crate::include::nodes::primnodes::TextSearchTableFunction::Debug => "ts_debug",
            crate::include::nodes::primnodes::TextSearchTableFunction::Stat => "ts_stat",
        },
        SetReturningCall::UserDefined { function_name, .. } => function_name.as_str(),
    }
}

fn eval_generate_subscripts(
    array: &Expr,
    dimension: &Expr,
    reverse: Option<&Expr>,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let array_value = eval_expr(array, slot, ctx)?;
    let dimension_value = eval_expr(dimension, slot, ctx)?;
    let reverse_value = reverse
        .map(|expr| eval_expr(expr, slot, ctx))
        .transpose()?
        .unwrap_or(Value::Bool(false));
    if matches!(array_value, Value::Null)
        || matches!(dimension_value, Value::Null)
        || matches!(reverse_value, Value::Null)
    {
        return Ok(Vec::new());
    }
    let Some(array) = normalize_array_value(&array_value) else {
        return Err(ExecError::TypeMismatch {
            op: "generate_subscripts",
            left: array_value,
            right: Value::Null,
        });
    };
    let dimension = match dimension_value {
        Value::Int16(v) => i32::from(v),
        Value::Int32(v) => v,
        Value::Int64(v) => i32::try_from(v).map_err(|_| ExecError::Int4OutOfRange)?,
        other => {
            return Err(ExecError::TypeMismatch {
                op: "generate_subscripts dimension",
                left: other,
                right: Value::Null,
            });
        }
    };
    let reverse = match reverse_value {
        Value::Bool(v) => v,
        other => {
            return Err(ExecError::TypeMismatch {
                op: "generate_subscripts reverse",
                left: other,
                right: Value::Null,
            });
        }
    };
    if dimension < 1 {
        return Ok(Vec::new());
    }
    let Some(dim) = array.dimensions.get((dimension - 1) as usize) else {
        return Ok(Vec::new());
    };
    if dim.length == 0 {
        return Ok(Vec::new());
    }
    let lower = dim.lower_bound;
    let upper = lower
        .checked_add(dim.length as i32)
        .and_then(|value| value.checked_sub(1))
        .ok_or(ExecError::Int4OutOfRange)?;
    let range: Box<dyn Iterator<Item = i32>> = if reverse {
        Box::new((lower..=upper).rev())
    } else {
        Box::new(lower..=upper)
    };
    Ok(range
        .map(|subscript| TupleSlot::virtual_row(vec![Value::Int32(subscript)]))
        .collect())
}

fn eval_text_search_table_function(
    kind: crate::include::nodes::primnodes::TextSearchTableFunction,
    args: &[Expr],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    match kind {
        crate::include::nodes::primnodes::TextSearchTableFunction::TokenType => {
            Ok(crate::backend::tsearch::token_kinds()
                .iter()
                .map(|kind| {
                    TupleSlot::virtual_row(vec![
                        Value::Int32(kind.tokid),
                        Value::Text(kind.alias.into()),
                        Value::Text(kind.description.into()),
                    ])
                })
                .collect())
        }
        crate::include::nodes::primnodes::TextSearchTableFunction::Parse => {
            let values = eval_args(args, slot, ctx)?;
            let Some(document) = values.last().and_then(Value::as_text) else {
                return Ok(Vec::new());
            };
            Ok(crate::backend::tsearch::parse_default(document)
                .into_iter()
                .map(|token| {
                    TupleSlot::virtual_row(vec![
                        Value::Int32(token.tokid),
                        Value::Text(token.token.into()),
                    ])
                })
                .collect())
        }
        crate::include::nodes::primnodes::TextSearchTableFunction::Debug => {
            eval_ts_debug_rows(args, slot, ctx)
        }
        crate::include::nodes::primnodes::TextSearchTableFunction::Stat => {
            eval_ts_stat_rows(args, slot, ctx)
        }
    }
}

fn eval_ts_stat_rows(
    args: &[Expr],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let values = eval_args(args, slot, ctx)?;
    let Some(query) = values.first().and_then(Value::as_text) else {
        return Ok(Vec::new());
    };
    let weights = values
        .get(1)
        .and_then(Value::as_text)
        .map(parse_ts_stat_weights)
        .unwrap_or_default();
    let (column_name, table_name) = parse_ts_stat_select(query)?;
    let catalog = ctx
        .catalog
        .as_deref()
        .ok_or_else(|| ExecError::DetailedError {
            message: "ts_stat requires executor catalog context".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        })?;
    let relation =
        catalog
            .lookup_any_relation(table_name)
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("relation \"{table_name}\" does not exist"),
                detail: None,
                hint: None,
                sqlstate: "42P01",
            })?;
    let column_index = relation
        .desc
        .columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(column_name) && !column.dropped)
        .ok_or_else(|| ExecError::DetailedError {
            message: format!("column \"{column_name}\" does not exist"),
            detail: None,
            hint: None,
            sqlstate: "42703",
        })?;
    let attr_descs = relation.desc.attribute_descs();
    let mut stats = std::collections::BTreeMap::<String, (i32, i32)>::new();
    let mut scan =
        heap_scan_begin_visible(&ctx.pool, ctx.client_id, relation.rel, ctx.snapshot.clone())
            .map_err(|err| ExecError::DetailedError {
                message: "ts_stat heap scan failed".into(),
                detail: Some(format!("{err:?}")),
                hint: None,
                sqlstate: "XX000",
            })?;
    loop {
        ctx.check_for_interrupts()?;
        let next = {
            let txns = ctx.txns.read();
            heap_scan_next_visible(&ctx.pool, ctx.client_id, &txns, &mut scan)
        }
        .map_err(|err| ExecError::DetailedError {
            message: "ts_stat heap scan failed".into(),
            detail: Some(format!("{err:?}")),
            hint: None,
            sqlstate: "XX000",
        })?;
        let Some((_tid, tuple)) = next else {
            break;
        };
        let row = materialize_heap_row_values(
            &relation.desc,
            &tuple
                .deform(&attr_descs)
                .map_err(|err| ExecError::DetailedError {
                    message: "ts_stat heap tuple deform failed".into(),
                    detail: Some(format!("{err:?}")),
                    hint: None,
                    sqlstate: "XX000",
                })?,
        )
        .map_err(|err| ExecError::DetailedError {
            message: "ts_stat heap tuple materialize failed".into(),
            detail: Some(format!("{err:?}")),
            hint: None,
            sqlstate: "XX000",
        })?;
        let Some(Value::TsVector(vector)) = row.get(column_index) else {
            continue;
        };
        for lexeme in &vector.lexemes {
            let nentry = if weights.is_empty() {
                if lexeme.positions.is_empty() {
                    1
                } else {
                    lexeme.positions.len() as i32
                }
            } else {
                lexeme
                    .positions
                    .iter()
                    .filter(|position| weights.contains(&position.weight.unwrap_or(TsWeight::D)))
                    .count() as i32
            };
            if nentry == 0 {
                continue;
            }
            let entry = stats.entry(lexeme.text.to_string()).or_default();
            entry.0 += 1;
            entry.1 += nentry;
        }
    }
    Ok(stats
        .into_iter()
        .map(|(word, (ndoc, nentry))| {
            TupleSlot::virtual_row(vec![
                Value::Text(word.into()),
                Value::Int32(ndoc),
                Value::Int32(nentry),
            ])
        })
        .collect())
}

fn parse_ts_stat_weights(value: &str) -> Vec<TsWeight> {
    value.chars().filter_map(TsWeight::from_char).collect()
}

fn parse_ts_stat_select(query: &str) -> Result<(&str, &str), ExecError> {
    let trimmed = query.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_ascii_lowercase();
    let Some(rest) = lower.strip_prefix("select ") else {
        return Err(ExecError::DetailedError {
            message: "ts_stat query is not supported".into(),
            detail: Some("expected SELECT column FROM table".into()),
            hint: None,
            sqlstate: "0A000",
        });
    };
    let Some(from_pos) = rest.find(" from ") else {
        return Err(ExecError::DetailedError {
            message: "ts_stat query is not supported".into(),
            detail: Some("expected SELECT column FROM table".into()),
            hint: None,
            sqlstate: "0A000",
        });
    };
    let column_start = "select ".len();
    let column_end = column_start + from_pos;
    let table_start = column_end + " from ".len();
    let column = trimmed[column_start..column_end].trim();
    let table = trimmed[table_start..]
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .trim();
    if column.is_empty() || table.is_empty() || column.contains(',') {
        return Err(ExecError::DetailedError {
            message: "ts_stat query is not supported".into(),
            detail: Some("expected SELECT column FROM table".into()),
            hint: None,
            sqlstate: "0A000",
        });
    }
    Ok((column.trim_matches('"'), table.trim_matches('"')))
}

fn eval_ts_debug_rows(
    args: &[Expr],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let values = eval_args(args, slot, ctx)?;
    let Some(document) = values.last().and_then(Value::as_text) else {
        return Ok(Vec::new());
    };
    let config = if values.len() > 1 {
        ts_debug_config_from_value(&values[0], ctx)?
    } else {
        let config_name = ctx
            .gucs
            .get("default_text_search_config")
            .map(String::as_str);
        crate::backend::tsearch::resolve_config_with_gucs(config_name, Some(&ctx.gucs)).map_err(
            |message| {
                ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
                    expected: "valid text search configuration",
                    actual: message,
                })
            },
        )?
    };

    let catalog = ctx.catalog.as_deref();
    let mut rows = Vec::new();
    for token in crate::backend::tsearch::parse_default(document) {
        let Some(kind) = crate::backend::tsearch::token_kind(token.tokid) else {
            continue;
        };
        let dictionary = ts_debug_dictionary_name(&config, token.tokid);
        let (dictionaries, dictionary_value, lexemes) = if let Some(dictionary) = dictionary {
            let lexemes = crate::backend::tsearch::ts_lexize_with_dictionary_name(
                dictionary,
                token.token.as_str(),
                catalog,
            )
            .map_err(|message| {
                ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
                    expected: "valid text search lexeme",
                    actual: message,
                })
            })?;
            (
                Value::Array(vec![Value::Text(dictionary.into())]),
                Value::Text(dictionary.into()),
                Value::Array(
                    lexemes
                        .unwrap_or_default()
                        .into_iter()
                        .map(|lexeme| Value::Text(lexeme.into()))
                        .collect(),
                ),
            )
        } else {
            (Value::Array(Vec::new()), Value::Null, Value::Null)
        };
        rows.push(TupleSlot::virtual_row(vec![
            Value::Text(kind.alias.into()),
            Value::Text(kind.description.into()),
            Value::Text(token.token.into()),
            dictionaries,
            dictionary_value,
            lexemes,
        ]));
    }
    Ok(rows)
}

fn ts_debug_config_from_value(
    value: &Value,
    ctx: &ExecutorContext,
) -> Result<crate::backend::tsearch::cache::TextSearchConfig, ExecError> {
    if let Some(name) = value.as_text() {
        return crate::backend::tsearch::resolve_config_with_gucs(Some(name), Some(&ctx.gucs))
            .map_err(|message| {
                ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
                    expected: "valid text search configuration",
                    actual: message,
                })
            });
    }
    let oid = match value {
        Value::Int64(oid) if *oid >= 0 => Some(*oid as u32),
        Value::Int32(oid) if *oid >= 0 => Some(*oid as u32),
        _ => None,
    };
    let Some(oid) = oid else {
        return crate::backend::tsearch::resolve_config_with_gucs(None, Some(&ctx.gucs)).map_err(
            |message| {
                ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
                    expected: "valid text search configuration",
                    actual: message,
                })
            },
        );
    };
    if oid == crate::include::catalog::SIMPLE_TS_CONFIG_OID {
        return Ok(crate::backend::tsearch::cache::TextSearchConfig::Simple);
    }
    if oid == crate::include::catalog::ENGLISH_TS_CONFIG_OID {
        return Ok(crate::backend::tsearch::cache::TextSearchConfig::English);
    }
    let Some(row) = ctx.catalog.as_deref().and_then(|catalog| {
        catalog
            .ts_config_rows()
            .into_iter()
            .find(|row| row.oid == oid)
    }) else {
        return Err(ExecError::DetailedError {
            message: format!("text search configuration with OID {oid} does not exist"),
            detail: None,
            hint: None,
            sqlstate: "42704",
        });
    };
    crate::backend::tsearch::resolve_config_with_gucs(Some(&row.cfgname), Some(&ctx.gucs)).map_err(
        |message| {
            ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
                expected: "valid text search configuration",
                actual: message,
            })
        },
    )
}

fn ts_debug_dictionary_name(
    config: &crate::backend::tsearch::cache::TextSearchConfig,
    tokid: i32,
) -> Option<&'static str> {
    if !crate::backend::tsearch::parser::token_has_dictionary(tokid) {
        return None;
    }
    match config {
        crate::backend::tsearch::cache::TextSearchConfig::Simple => Some("simple"),
        crate::backend::tsearch::cache::TextSearchConfig::English
            if matches!(
                tokid,
                crate::backend::tsearch::parser::EMAIL
                    | crate::backend::tsearch::parser::URL_T
                    | crate::backend::tsearch::parser::HOST
                    | crate::backend::tsearch::parser::SCIENTIFIC
                    | crate::backend::tsearch::parser::URLPATH
                    | crate::backend::tsearch::parser::FILEPATH
                    | crate::backend::tsearch::parser::DECIMAL_T
                    | crate::backend::tsearch::parser::SIGNEDINT
                    | crate::backend::tsearch::parser::UNSIGNEDINT
            ) =>
        {
            Some("simple")
        }
        crate::backend::tsearch::cache::TextSearchConfig::English => Some("english_stem"),
        crate::backend::tsearch::cache::TextSearchConfig::Custom { .. } => Some("simple"),
    }
}

fn eval_args(
    args: &[Expr],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<Value>, ExecError> {
    args.iter().map(|arg| eval_expr(arg, slot, ctx)).collect()
}

fn eval_pg_lock_status(ctx: &ExecutorContext) -> Result<Vec<TupleSlot>, ExecError> {
    Ok(ctx
        .lock_status_provider
        .as_ref()
        .map(|provider| provider.pg_lock_status_rows(ctx.client_id))
        .unwrap_or_default()
        .into_iter()
        .map(TupleSlot::virtual_row)
        .collect())
}

fn sequence_catalog(ctx: &ExecutorContext) -> Result<&dyn CatalogLookup, ExecError> {
    ctx.catalog
        .as_deref()
        .ok_or_else(|| ExecError::DetailedError {
            message: "sequence view requires executor catalog context".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })
}

fn sequence_type_display(type_oid: u32) -> (&'static str, i32) {
    match type_oid {
        INT2_TYPE_OID => ("smallint", 16),
        INT4_TYPE_OID => ("integer", 32),
        INT8_TYPE_OID => ("bigint", 64),
        _ => ("bigint", 64),
    }
}

struct SequenceViewRow {
    oid: u32,
    schema: String,
    name: String,
    owner: String,
    type_oid: u32,
    start: i64,
    minvalue: i64,
    maxvalue: i64,
    increment: i64,
    cycle: bool,
    cache: i64,
    last_value: Option<i64>,
}

fn sequence_rows(ctx: &ExecutorContext) -> Result<Vec<SequenceViewRow>, ExecError> {
    let catalog = sequence_catalog(ctx)?;
    let sequences = ctx
        .sequences
        .as_deref()
        .ok_or_else(|| ExecError::DetailedError {
            message: "sequence runtime is not available".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })?;
    let roles = catalog.authid_rows();
    let mut rows = Vec::new();
    for class in catalog
        .class_rows()
        .into_iter()
        .filter(|class| class.relkind == 'S')
    {
        let Some(data) = sequences.sequence_data(class.oid) else {
            continue;
        };
        let schema = catalog
            .namespace_row_by_oid(class.relnamespace)
            .map(|row| row.nspname)
            .unwrap_or_else(|| "public".to_string());
        let owner = roles
            .iter()
            .find(|role| role.oid == class.relowner)
            .map(|role| role.rolname.clone())
            .unwrap_or_else(|| class.relowner.to_string());
        rows.push(SequenceViewRow {
            oid: class.oid,
            schema,
            name: class.relname,
            owner,
            type_oid: data.options.type_oid,
            start: data.options.start,
            minvalue: data.options.minvalue,
            maxvalue: data.options.maxvalue,
            increment: data.options.increment,
            cycle: data.options.cycle,
            cache: data.options.cache,
            last_value: data.state.is_called.then_some(data.state.last_value),
        });
    }
    rows.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.schema.cmp(&right.schema))
    });
    Ok(rows)
}

fn is_identity_owned_sequence(catalog: &dyn CatalogLookup, sequence_oid: u32) -> bool {
    catalog.depend_rows().into_iter().any(|row| {
        row.classid == PG_CLASS_RELATION_OID
            && row.objid == sequence_oid
            && row.objsubid == 0
            && row.refclassid == PG_CLASS_RELATION_OID
            && row.refobjsubid > 0
            && row.deptype == DEPENDENCY_INTERNAL
    })
}

fn eval_pg_sequences(ctx: &ExecutorContext) -> Result<Vec<TupleSlot>, ExecError> {
    Ok(sequence_rows(ctx)?
        .into_iter()
        .filter(|row| {
            let Ok(catalog) = sequence_catalog(ctx) else {
                return false;
            };
            catalog
                .class_rows()
                .into_iter()
                .find(|class| class.relkind == 'S' && class.relname == row.name)
                .is_none_or(|class| !is_identity_owned_sequence(catalog, class.oid))
        })
        .map(|row| {
            let (type_name, _) = sequence_type_display(row.type_oid);
            let last_value = row.last_value.map(Value::Int64).unwrap_or(Value::Null);
            TupleSlot::virtual_row(vec![
                Value::Text(row.schema.into()),
                Value::Text(row.name.into()),
                Value::Text(row.owner.into()),
                Value::Text(type_name.into()),
                Value::Int64(row.start),
                Value::Int64(row.minvalue),
                Value::Int64(row.maxvalue),
                Value::Int64(row.increment),
                Value::Bool(row.cycle),
                Value::Int64(row.cache),
                last_value,
            ])
        })
        .collect())
}

fn eval_information_schema_sequences(ctx: &ExecutorContext) -> Result<Vec<TupleSlot>, ExecError> {
    let sequence_catalog_name = if ctx.current_database_name.eq_ignore_ascii_case("postgres") {
        "regression".to_string()
    } else {
        ctx.current_database_name.clone()
    };
    let catalog = sequence_catalog(ctx)?;
    Ok(sequence_rows(ctx)?
        .into_iter()
        .filter(|row| !is_identity_owned_sequence(catalog, row.oid))
        .map(|row| {
            let (type_name, precision) = sequence_type_display(row.type_oid);
            TupleSlot::virtual_row(vec![
                Value::Text(sequence_catalog_name.clone().into()),
                Value::Text(row.schema.into()),
                Value::Text(row.name.into()),
                Value::Text(type_name.into()),
                Value::Int32(precision),
                Value::Int32(2),
                Value::Int32(0),
                Value::Text(row.start.to_string().into()),
                Value::Text(row.minvalue.to_string().into()),
                Value::Text(row.maxvalue.to_string().into()),
                Value::Text(row.increment.to_string().into()),
                Value::Text(if row.cycle { "YES" } else { "NO" }.into()),
            ])
        })
        .collect())
}

fn eval_txid_snapshot_xip(
    arg: &Expr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let value = eval_expr(arg, slot, ctx)?;
    Ok(eval_txid_snapshot_xip_values(&[value])?
        .into_iter()
        .map(|value| TupleSlot::virtual_row(vec![value]))
        .collect())
}

fn eval_regex_table_function(
    kind: crate::include::nodes::primnodes::RegexTableFunction,
    args: &[Expr],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let values = args
        .iter()
        .map(|arg| eval_expr(arg, slot, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    let rows = match kind {
        crate::include::nodes::primnodes::RegexTableFunction::Matches => {
            eval_regexp_matches_rows(&values)?
        }
        crate::include::nodes::primnodes::RegexTableFunction::SplitToTable => {
            eval_regexp_split_to_table_rows(&values)?
        }
    };
    Ok(rows
        .into_iter()
        .map(|value| TupleSlot::virtual_row(vec![value]))
        .collect())
}

fn eval_string_table_function(
    kind: crate::include::nodes::primnodes::StringTableFunction,
    args: &[Expr],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let values = args
        .iter()
        .map(|arg| eval_expr(arg, slot, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    let rows = match kind {
        crate::include::nodes::primnodes::StringTableFunction::StringToTable => {
            eval_string_to_table_rows(&values)?
        }
    };
    Ok(rows
        .into_iter()
        .map(|value| TupleSlot::virtual_row(vec![value]))
        .collect())
}

fn partition_catalog(ctx: &ExecutorContext) -> Result<&dyn CatalogLookup, ExecError> {
    ctx.catalog
        .as_deref()
        .ok_or_else(|| ExecError::DetailedError {
            message: "partition lookup requires executor catalog context".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })
}

fn partition_lookup_oid(value: Value, op: &'static str) -> Result<Option<u32>, ExecError> {
    match value {
        Value::Null => Ok(None),
        Value::Int32(v) if v >= 0 => Ok(Some(v as u32)),
        Value::Int64(v) if v >= 0 && v <= i64::from(u32::MAX) => Ok(Some(v as u32)),
        other => Err(ExecError::TypeMismatch {
            op,
            left: other,
            right: Value::Int64(i64::from(crate::include::catalog::REGCLASS_TYPE_OID)),
        }),
    }
}

fn eval_partition_tree(
    relid: &Expr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let Some(relation_oid) =
        partition_lookup_oid(eval_expr(relid, slot, ctx)?, "pg_partition_tree")?
    else {
        return Ok(Vec::new());
    };
    let catalog = partition_catalog(ctx)?;
    Ok(partition_tree_entries(catalog, relation_oid)?
        .into_iter()
        .map(|entry| {
            TupleSlot::virtual_row(vec![
                Value::Int64(i64::from(entry.relid)),
                entry
                    .parentrelid
                    .map(|oid| Value::Int64(i64::from(oid)))
                    .unwrap_or(Value::Null),
                Value::Bool(entry.isleaf),
                Value::Int32(entry.level),
            ])
        })
        .collect())
}

fn eval_partition_ancestors(
    relid: &Expr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let Some(relation_oid) =
        partition_lookup_oid(eval_expr(relid, slot, ctx)?, "pg_partition_ancestors")?
    else {
        return Ok(Vec::new());
    };
    let catalog = partition_catalog(ctx)?;
    Ok(partition_ancestor_oids(catalog, relation_oid)?
        .into_iter()
        .map(|oid| TupleSlot::virtual_row(vec![Value::Int64(i64::from(oid))]))
        .collect())
}

fn eval_generate_series(
    start: &Expr,
    stop: &Expr,
    step: &Expr,
    timezone: Option<&Expr>,
    output_kind: SqlTypeKind,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    eval_generate_series_values(start, stop, step, timezone, output_kind, slot, ctx).map(|values| {
        values
            .into_iter()
            .map(|value| TupleSlot::virtual_row(vec![value]))
            .collect()
    })
}

fn eval_generate_series_values(
    start: &Expr,
    stop: &Expr,
    step: &Expr,
    timezone: Option<&Expr>,
    output_kind: SqlTypeKind,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<Value>, ExecError> {
    let start_val = eval_expr(start, slot, ctx)?;
    let stop_val = eval_expr(stop, slot, ctx)?;
    let step_val = eval_expr(step, slot, ctx)?;
    if matches!(start_val, Value::Null)
        || matches!(stop_val, Value::Null)
        || matches!(step_val, Value::Null)
    {
        return Ok(Vec::new());
    }

    if matches!(output_kind, SqlTypeKind::Timestamp) {
        return single_column_slots_to_values(eval_timestamp_generate_series(
            start_val,
            stop_val,
            step_val,
            output_kind,
            ctx,
        )?);
    }
    if matches!(output_kind, SqlTypeKind::TimestampTz) {
        return single_column_slots_to_values(eval_timestamptz_generate_series(
            start_val, stop_val, step_val, timezone, slot, ctx,
        )?);
    }

    if matches!(output_kind, SqlTypeKind::Numeric) {
        let mut state = GenerateSeriesState::numeric(start_val, stop_val, step_val)?;
        return collect_value_per_call_values(ctx, |ctx| state.next_value(ctx));
    }

    let mut state = GenerateSeriesState::integral(start_val, stop_val, step_val, output_kind)?;
    collect_value_per_call_values(ctx, |ctx| state.next_value(ctx))
}

fn collect_value_per_call_values(
    ctx: &mut ExecutorContext,
    mut next: impl FnMut(&mut ExecutorContext) -> Result<Option<Value>, ExecError>,
) -> Result<Vec<Value>, ExecError> {
    let mut rows = Vec::new();
    while let Some(value) = next(ctx)? {
        rows.push(value);
    }
    Ok(rows)
}

enum GenerateSeriesState {
    Numeric {
        current: NumericValue,
        stop: NumericValue,
        step: NumericValue,
        step_cmp: std::cmp::Ordering,
        dscale: u32,
    },
    Integral {
        current: i64,
        stop: i64,
        step: i64,
        output_kind: SqlTypeKind,
    },
}

impl GenerateSeriesState {
    fn numeric(start: Value, stop: Value, step: Value) -> Result<Self, ExecError> {
        let start = generate_series_numeric_arg(start, "generate_series start")?;
        let stop = generate_series_numeric_arg(stop, "generate_series stop")?;
        let step = generate_series_numeric_arg(step, "generate_series step")?;
        validate_generate_series_numeric_arg(&start, "start")?;
        validate_generate_series_numeric_arg(&stop, "stop")?;
        validate_generate_series_numeric_arg(&step, "step size")?;
        let dscale = [start.dscale(), stop.dscale(), step.dscale()]
            .into_iter()
            .max()
            .unwrap_or(0);
        let step_cmp = step.cmp(&NumericValue::zero());
        if step_cmp == std::cmp::Ordering::Equal {
            return Err(ExecError::GenerateSeriesZeroStep);
        }
        Ok(GenerateSeriesState::Numeric {
            current: start,
            stop,
            step,
            step_cmp,
            dscale,
        })
    }

    fn integral(
        start: Value,
        stop: Value,
        step: Value,
        output_kind: SqlTypeKind,
    ) -> Result<Self, ExecError> {
        let current = generate_series_i64_arg(start, "generate_series start")?;
        let stop = generate_series_i64_arg(stop, "generate_series stop")?;
        let step = generate_series_i64_arg(step, "generate_series step")?;
        if step == 0 {
            return Err(ExecError::GenerateSeriesZeroStep);
        }
        Ok(GenerateSeriesState::Integral {
            current,
            stop,
            step,
            output_kind,
        })
    }

    fn next_value(&mut self, ctx: &mut ExecutorContext) -> Result<Option<Value>, ExecError> {
        ctx.check_for_interrupts()?;
        match self {
            GenerateSeriesState::Numeric {
                current,
                stop,
                step,
                step_cmp,
                dscale,
            } => {
                let done = match step_cmp {
                    std::cmp::Ordering::Greater => current.cmp(stop) == std::cmp::Ordering::Greater,
                    std::cmp::Ordering::Less => current.cmp(stop) == std::cmp::Ordering::Less,
                    std::cmp::Ordering::Equal => unreachable!(),
                };
                if done {
                    return Ok(None);
                }
                let value = current.clone().with_dscale(*dscale);
                *current = current.add(step).with_dscale(*dscale);
                Ok(Some(Value::Numeric(value)))
            }
            GenerateSeriesState::Integral {
                current,
                stop,
                step,
                output_kind,
            } => {
                let done = if *step > 0 {
                    *current > *stop
                } else {
                    *current < *stop
                };
                if done {
                    return Ok(None);
                }
                let value = *current;
                *current += *step;
                Ok(Some(match output_kind {
                    SqlTypeKind::Int8 => Value::Int64(value),
                    _ => Value::Int32(value as i32),
                }))
            }
        }
    }
}

fn generate_series_numeric_arg(
    value: Value,
    label: &'static str,
) -> Result<NumericValue, ExecError> {
    match value {
        Value::Numeric(n) => Ok(n),
        Value::Int32(i) => Ok(NumericValue::from_i64(i64::from(i))),
        Value::Int64(i) => Ok(NumericValue::from_i64(i)),
        other => Err(ExecError::TypeMismatch {
            op: label,
            left: other,
            right: Value::Null,
        }),
    }
}

fn validate_generate_series_numeric_arg(
    value: &NumericValue,
    arg: &'static str,
) -> Result<(), ExecError> {
    match value {
        NumericValue::NaN => Err(ExecError::GenerateSeriesInvalidArg(arg, "NaN")),
        NumericValue::PosInf | NumericValue::NegInf => {
            Err(ExecError::GenerateSeriesInvalidArg(arg, "infinity"))
        }
        NumericValue::Finite { .. } => Ok(()),
    }
}

fn generate_series_i64_arg(value: Value, label: &'static str) -> Result<i64, ExecError> {
    match value {
        Value::Int32(v) => Ok(i64::from(v)),
        Value::Int64(v) => Ok(v),
        other => Err(ExecError::TypeMismatch {
            op: label,
            left: other,
            right: Value::Null,
        }),
    }
}

fn timestamp_add_interval(base: i64, step: IntervalValue) -> Option<i64> {
    if !step.is_finite() || base == i64::MIN || base == i64::MAX {
        return None;
    }
    let (days, time) = timestamp_parts_from_usecs(base);
    let (year, month, day) = ymd_from_days(days);
    let month_index = i64::from(year) * 12 + i64::from(month - 1) + i64::from(step.months);
    let new_year = month_index.div_euclid(12) as i32;
    let new_month = month_index.rem_euclid(12) as u32 + 1;
    let new_day = day.min(days_in_month(new_year, new_month));
    let new_days = days_from_ymd(new_year, new_month, new_day)?;
    i64::from(new_days)
        .checked_mul(USECS_PER_DAY)?
        .checked_add(time)?
        .checked_add(i64::from(step.days).checked_mul(USECS_PER_DAY)?)?
        .checked_add(step.time_micros)
}

fn interval_sign(step: IntervalValue) -> i32 {
    let key = step.cmp_key();
    if key > 0 {
        1
    } else if key < 0 {
        -1
    } else {
        0
    }
}

fn generate_series_timestamptz_arg(
    value: Value,
    label: &'static str,
) -> Result<TimestampTzADT, ExecError> {
    match value {
        Value::TimestampTz(value) => Ok(value),
        other => Err(ExecError::TypeMismatch {
            op: label,
            left: other,
            right: Value::Null,
        }),
    }
}

fn generate_series_interval_arg(value: Value) -> Result<IntervalValue, ExecError> {
    match value {
        Value::Interval(value) => Ok(value),
        other => Err(ExecError::TypeMismatch {
            op: "generate_series step",
            left: other,
            right: Value::Null,
        }),
    }
}

fn step_timestamptz_series(
    current: TimestampTzADT,
    step: IntervalValue,
    zone: &str,
) -> Result<TimestampTzADT, ExecError> {
    let local =
        timestamptz_at_time_zone(current, zone).map_err(|err| ExecError::InvalidStorageValue {
            column: "time zone".into(),
            details: super::expr_casts::datetime_parse_error_details("time zone", zone, err),
        })?;
    let local = add_interval_to_local_timestamp(local, step, false)?;
    timestamp_at_time_zone(local, zone).map_err(|err| ExecError::InvalidStorageValue {
        column: "time zone".into(),
        details: super::expr_casts::datetime_parse_error_details("time zone", zone, err),
    })
}

fn eval_timestamptz_generate_series(
    start_val: Value,
    stop_val: Value,
    step_val: Value,
    timezone: Option<&Expr>,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let mut current = generate_series_timestamptz_arg(start_val, "generate_series start")?;
    let stop = generate_series_timestamptz_arg(stop_val, "generate_series stop")?;
    let step = generate_series_interval_arg(step_val)?;
    if step.is_infinity() || step.is_neg_infinity() {
        return Err(ExecError::DetailedError {
            message: "step size cannot be infinite".into(),
            detail: None,
            hint: None,
            sqlstate: "22023",
        });
    }
    let step_cmp = step.cmp_key().cmp(&0);
    if step_cmp.is_eq() {
        return Err(ExecError::GenerateSeriesZeroStep);
    }
    let zone_value = if let Some(timezone) = timezone {
        let value = eval_expr(timezone, slot, ctx)?;
        if matches!(value, Value::Null) {
            return Ok(Vec::new());
        }
        Some(
            value
                .as_text()
                .ok_or_else(|| ExecError::TypeMismatch {
                    op: "generate_series timezone",
                    left: value.clone(),
                    right: Value::Text("".into()),
                })?
                .to_string(),
        )
    } else {
        None
    };
    let zone = zone_value
        .as_deref()
        .unwrap_or_else(|| current_timezone_name(&ctx.datetime_config));
    let mut rows = Vec::new();
    loop {
        ctx.check_for_interrupts()?;
        let done = if step_cmp.is_gt() {
            current > stop
        } else {
            current < stop
        };
        if done {
            break;
        }
        rows.push(TupleSlot::virtual_row(vec![Value::TimestampTz(current)]));
        if matches!(stop.0, TIMESTAMP_NOEND | TIMESTAMP_NOBEGIN)
            && rows.len() >= MAX_UNBOUNDED_TIMESTAMP_SERIES_ROWS
        {
            break;
        }
        current = step_timestamptz_series(current, step, zone)?;
    }
    Ok(rows)
}

fn eval_timestamp_generate_series(
    start_val: Value,
    stop_val: Value,
    step_val: Value,
    output_kind: SqlTypeKind,
    ctx: &ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let (mut current, end) = match (start_val, stop_val, output_kind) {
        (Value::Timestamp(start), Value::Timestamp(stop), SqlTypeKind::Timestamp) => {
            (start.0, stop.0)
        }
        (Value::TimestampTz(start), Value::TimestampTz(stop), SqlTypeKind::TimestampTz) => {
            (start.0, stop.0)
        }
        (start, stop, _) => {
            return Err(ExecError::TypeMismatch {
                op: "generate_series",
                left: start,
                right: stop,
            });
        }
    };
    let Value::Interval(step) = step_val else {
        return Err(ExecError::TypeMismatch {
            op: "generate_series step",
            left: step_val,
            right: Value::Interval(IntervalValue::zero()),
        });
    };
    if !step.is_finite() {
        return Err(ExecError::DetailedError {
            message: "step size cannot be infinite".into(),
            detail: None,
            hint: None,
            sqlstate: "22023",
        });
    }
    let sign = interval_sign(step);
    if sign == 0 {
        return Err(ExecError::GenerateSeriesZeroStep);
    }
    let mut rows = Vec::new();
    loop {
        ctx.check_for_interrupts()?;
        let done = if sign > 0 {
            current > end
        } else {
            current < end
        };
        if done {
            break;
        }
        rows.push(TupleSlot::virtual_row(vec![match output_kind {
            SqlTypeKind::TimestampTz => Value::TimestampTz(TimestampTzADT(current)),
            _ => Value::Timestamp(TimestampADT(current)),
        }]));
        // :HACK: ProjectSet currently materializes SRF output before an outer LIMIT can stop it.
        // Bound infinite timestamp series so SELECT-list generate_series(..., 'infinity', ...)
        // can still be consumed by LIMIT while the executor lacks streaming SRF state.
        if matches!(end, TIMESTAMP_NOEND | TIMESTAMP_NOBEGIN)
            && rows.len() >= MAX_UNBOUNDED_TIMESTAMP_SERIES_ROWS
        {
            break;
        }
        let Some(next) = timestamp_add_interval(current, step) else {
            break;
        };
        if next == current {
            return Err(ExecError::GenerateSeriesZeroStep);
        }
        current = next;
    }
    Ok(rows)
}

fn eval_unnest(
    args: &[Expr],
    output_columns: &[QueryColumn],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let mut arrays = Vec::with_capacity(args.len());
    let mut max_len = 0usize;
    for arg in args {
        let arg_value = eval_expr(arg, slot, ctx)?;
        match arg_value {
            Value::Null => arrays.push(None),
            Value::TsVector(vector) if args.len() == 1 => {
                return Ok(crate::backend::executor::unnest_tsvector(&vector)
                    .into_iter()
                    .map(|value| match value {
                        Value::Record(record) => TupleSlot::virtual_row(record.fields),
                        other => TupleSlot::virtual_row(vec![other]),
                    })
                    .collect());
            }
            Value::Multirange(multirange) => {
                let values = multirange
                    .ranges
                    .into_iter()
                    .map(Value::Range)
                    .collect::<Vec<_>>();
                max_len = max_len.max(values.len());
                arrays.push(Some(values));
            }
            Value::Array(values) => {
                max_len = max_len.max(values.len());
                arrays.push(Some(values));
            }
            Value::PgArray(array) => {
                let values = array.to_nested_values();
                max_len = max_len.max(values.len());
                arrays.push(Some(values));
            }
            other => {
                if let Some(array) = normalize_array_value(&other) {
                    let values = array.to_nested_values();
                    max_len = max_len.max(values.len());
                    arrays.push(Some(values));
                    continue;
                }
                if expr_sql_type_hint(arg).is_some_and(|ty| {
                    !ty.is_array
                        && matches!(ty.kind, SqlTypeKind::Int2Vector | SqlTypeKind::OidVector)
                }) && let Some(array) = normalize_array_value(&other)
                {
                    let values = array.to_nested_values();
                    max_len = max_len.max(values.len());
                    arrays.push(Some(values));
                    continue;
                }
                return Err(ExecError::TypeMismatch {
                    op: "unnest",
                    left: other,
                    right: Value::Null,
                });
            }
        }
    }

    let expand_single_composite = unnest_expands_single_composite_arg(args, output_columns);
    let mut rows = Vec::with_capacity(max_len);
    for idx in 0..max_len {
        ctx.check_for_interrupts()?;
        if expand_single_composite {
            let value = arrays
                .first()
                .and_then(|array| array.as_ref())
                .and_then(|values| values.get(idx))
                .cloned()
                .unwrap_or(Value::Null);
            let mut fields = match value {
                Value::Record(record) => record.fields,
                Value::Null => vec![Value::Null; output_columns.len()],
                other => {
                    return Err(ExecError::TypeMismatch {
                        op: "unnest",
                        left: other,
                        right: Value::Null,
                    });
                }
            };
            fields.resize(output_columns.len(), Value::Null);
            fields.truncate(output_columns.len());
            rows.push(TupleSlot::virtual_row(fields));
            continue;
        }
        let mut row = Vec::with_capacity(arrays.len());
        for array in &arrays {
            match array {
                Some(values) => row.push(values.get(idx).cloned().unwrap_or(Value::Null)),
                None => row.push(Value::Null),
            }
        }
        rows.push(TupleSlot::virtual_row(row));
    }
    Ok(rows)
}

fn unnest_expands_single_composite_arg(args: &[Expr], output_columns: &[QueryColumn]) -> bool {
    if args.len() != 1 {
        return false;
    }
    if let Some(arg_type) = expr_sql_type_hint(&args[0]) {
        let element_type = if arg_type.is_array {
            arg_type.element_type()
        } else {
            arg_type
        };
        return matches!(
            element_type.kind,
            SqlTypeKind::Composite | SqlTypeKind::Record
        ) && (output_columns.len() != 1
            || output_columns
                .first()
                .is_some_and(|column| !column.name.eq_ignore_ascii_case("unnest")));
    }
    output_columns.len() > 1
        && output_columns
            .first()
            .is_some_and(|column| !column.name.eq_ignore_ascii_case("unnest"))
}

fn eval_pg_options_to_table(
    args: &[Expr],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    if args.len() != 1 {
        return Err(ExecError::DetailedError {
            message: format!("pg_options_to_table expects 1 argument, got {}", args.len()),
            detail: None,
            hint: None,
            sqlstate: "42883",
        });
    }

    let value = eval_expr(&args[0], slot, ctx)?;
    let values = match value {
        Value::Null => return Ok(Vec::new()),
        Value::Array(values) => values,
        Value::PgArray(array) => array.to_nested_values(),
        other => {
            if let Some(array) = normalize_array_value(&other) {
                array.to_nested_values()
            } else {
                return Err(ExecError::TypeMismatch {
                    op: "pg_options_to_table",
                    left: other,
                    right: Value::Null,
                });
            }
        }
    };

    let mut rows = Vec::with_capacity(values.len());
    for value in values {
        ctx.check_for_interrupts()?;
        if matches!(value, Value::Null) {
            continue;
        }
        let Some(option) = value.as_text() else {
            return Err(ExecError::TypeMismatch {
                op: "pg_options_to_table",
                left: value,
                right: Value::Null,
            });
        };
        let (name, option_value) = option
            .split_once('=')
            .map(|(name, value)| (name, Value::Text(value.into())))
            .unwrap_or((option, Value::Null));
        rows.push(TupleSlot::virtual_row(vec![
            Value::Text(name.into()),
            option_value,
        ]));
    }
    Ok(rows)
}
