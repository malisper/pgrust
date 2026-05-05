use super::exec_expr::{
    eval_native_builtin_scalar_typed_value_call, eval_string_to_table_rows, normalize_array_value,
};
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
use crate::backend::utils::cache::system_views::{
    build_pg_get_publication_tables_rows, build_pg_stat_io_rows, current_pg_stat_progress_copy_rows,
};
use crate::backend::utils::record::assign_anonymous_record_descriptor;
use crate::backend::utils::time::datetime::current_timezone_name;
use crate::backend::utils::time::timestamp::{timestamp_at_time_zone, timestamptz_at_time_zone};
use crate::include::catalog::{
    DEPENDENCY_INTERNAL, INT4_TYPE_OID, PG_CLASS_RELATION_OID, VOID_TYPE_OID,
    builtin_scalar_function_for_proc_oid, builtin_scalar_function_for_proc_row,
};
use crate::include::nodes::datetime::{TIMESTAMP_NOBEGIN, TIMESTAMP_NOEND, TimestampTzADT};
use crate::include::nodes::datum::{IntervalValue, NumericValue, RecordValue};
use crate::include::nodes::primnodes::{
    QueryColumn, RowsFromItem, RowsFromSource, expr_sql_type_hint,
};
use crate::include::nodes::tsearch::TsWeight;
use crate::pl::plpgsql::{
    current_event_trigger_ddl_commands, current_event_trigger_dropped_objects,
    execute_user_defined_set_returning_function,
};
use pgrust_executor::{GenerateSeriesState, SrfValueError, UnnestRows};
use pgrust_expr::expr_date::add_interval_to_local_timestamp;

impl From<SrfValueError> for ExecError {
    fn from(err: SrfValueError) -> Self {
        match err {
            SrfValueError::TypeMismatch { op, left, right } => {
                ExecError::TypeMismatch { op, left, right }
            }
            SrfValueError::UnsupportedTsStatQuery => ExecError::DetailedError {
                message: "ts_stat query is not supported".into(),
                detail: Some("expected SELECT column FROM table".into()),
                hint: None,
                sqlstate: "0A000",
            },
            SrfValueError::DirectoryOpen {
                display_name,
                message,
            } => ExecError::DetailedError {
                message: format!("could not open directory \"{display_name}\": {message}"),
                detail: None,
                hint: None,
                sqlstate: "58P01",
            },
            SrfValueError::DirectoryRead {
                display_name,
                message,
            } => ExecError::DetailedError {
                message: format!("could not read directory \"{display_name}\": {message}"),
                detail: None,
                hint: None,
                sqlstate: "58P01",
            },
        }
    }
}

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
    Ok(
        pgrust_executor::single_row_function_scan_values(value, output_columns.len())
            .into_iter()
            .map(TupleSlot::virtual_row)
            .collect(),
    )
}

fn function_output_columns(
    output_columns: &[QueryColumn],
    with_ordinality: bool,
) -> &[QueryColumn] {
    pgrust_executor::function_output_columns(output_columns, with_ordinality)
}

fn eval_rows_from(
    items: &[RowsFromItem],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let mut function_rows = Vec::with_capacity(items.len());
    for (item_index, item) in items.iter().enumerate() {
        let rows = eval_rows_from_item_cached(item_index, item, slot, ctx)?;
        let mut row_values = Vec::with_capacity(rows.len());
        for mut row in rows {
            row_values.push(row.values()?.to_vec());
        }
        function_rows.push(row_values);
    }
    let item_widths = items
        .iter()
        .map(|item| item.output_columns().len())
        .collect::<Vec<_>>();
    Ok(
        pgrust_executor::combine_rows_from_item_values(&item_widths, function_rows)
            .into_iter()
            .map(TupleSlot::virtual_row)
            .collect(),
    )
}

fn eval_rows_from_item_cached(
    item_index: usize,
    item: &RowsFromItem,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let Some(cache_key) = pgrust_executor::rows_from_cache_key(item_index, item) else {
        return eval_rows_from_item(item, slot, ctx);
    };

    // :HACK: Executor SRF expressions do not currently carry a stable plan-node
    // id, and lateral rescans rebuild the inner plan state. Use the item index
    // plus the planned source shape so uncorrelated ROWS FROM items can keep
    // PostgreSQL's tuplestore-like rescan behavior across those rebuilds.
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
    } else if let Some(kind) = pgrust_executor::text_search_table_function_for_proc_src(&row.prosrc)
    {
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
    pgrust_executor::pg_get_catalog_foreign_key_rows()
        .into_iter()
        .map(TupleSlot::virtual_row)
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
    pgrust_executor::event_trigger_dropped_object_rows(current_event_trigger_dropped_objects())
        .into_iter()
        .map(TupleSlot::virtual_row)
        .collect()
}

fn eval_pg_event_trigger_ddl_commands() -> Vec<TupleSlot> {
    pgrust_executor::event_trigger_ddl_command_rows(current_event_trigger_ddl_commands())
        .into_iter()
        .map(TupleSlot::virtual_row)
        .collect()
}

fn eval_pg_mcv_list_items(values: &[Value]) -> Result<Vec<TupleSlot>, ExecError> {
    pgrust_executor::pg_mcv_list_item_rows(values)
        .map_err(|message| ExecError::DetailedError {
            message: "could not decode pg_mcv_list".into(),
            detail: Some(message),
            hint: None,
            sqlstate: "XX000",
        })
        .map(|rows| rows.into_iter().map(TupleSlot::virtual_row).collect())
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
    let publication_names = pgrust_executor::publication_names_from_values(values)?;
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
    let rows = pgrust_executor::pg_ls_dir_rows(&path, dirname, missing_ok, include_dot_dirs)?
        .into_iter()
        .map(TupleSlot::virtual_row)
        .collect::<Vec<_>>();
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
    let rows = pgrust_executor::pg_ls_named_dir_rows(
        &path,
        &components.join("/"),
        synthesize_wal_segment,
        i64::from(crate::backend::access::transam::xlog::WAL_SEG_SIZE_BYTES),
        ctx.statement_timestamp_usecs,
    )?
    .into_iter()
    .map(TupleSlot::virtual_row)
    .collect::<Vec<_>>();
    Ok(rows)
}

fn eval_pg_tablespace_databases(values: &[Value]) -> Vec<TupleSlot> {
    pgrust_executor::pg_tablespace_databases_rows(values)
        .into_iter()
        .map(TupleSlot::virtual_row)
        .collect()
}

fn eval_pg_backend_memory_contexts() -> Vec<TupleSlot> {
    pgrust_executor::pg_backend_memory_context_rows()
        .into_iter()
        .map(TupleSlot::virtual_row)
        .collect()
}

fn eval_pg_config() -> Vec<TupleSlot> {
    if let Some(rows) = pgrust_executor::local_pg_config_rows().filter(|rows| rows.len() > 20) {
        return rows.into_iter().map(TupleSlot::virtual_row).collect();
    }
    pgrust_executor::pg_config_fallback_rows()
        .into_iter()
        .map(TupleSlot::virtual_row)
        .collect()
}

fn eval_pg_hba_file_rules() -> Vec<TupleSlot> {
    pgrust_executor::pg_hba_file_rule_rows()
        .into_iter()
        .map(TupleSlot::virtual_row)
        .collect()
}

fn eval_pg_cursor(ctx: &ExecutorContext) -> Vec<TupleSlot> {
    let rows = ctx
        .database
        .as_ref()
        .map(|db| db.session_view_state(ctx.client_id))
        .unwrap_or_default()
        .cursors
        .into_iter()
        .map(|row| pgrust_executor::CursorViewRow {
            name: row.name,
            statement: row.statement,
            is_holdable: row.is_holdable,
            is_binary: row.is_binary,
            is_scrollable: row.is_scrollable,
            creation_time: row.creation_time,
        })
        .collect::<Vec<_>>();
    pgrust_executor::pg_cursor_rows(rows)
        .into_iter()
        .map(TupleSlot::virtual_row)
        .collect()
}

fn eval_pg_prepared_statement(ctx: &ExecutorContext) -> Vec<TupleSlot> {
    let rows = ctx
        .database
        .as_ref()
        .map(|db| db.session_view_state(ctx.client_id))
        .unwrap_or_default()
        .prepared_statements
        .into_iter()
        .map(|row| pgrust_executor::PreparedStatementViewRow {
            name: row.name,
            statement: row.statement,
            prepare_time: row.prepare_time,
            parameter_type_oids: row.parameter_type_oids,
            result_type_oids: row.result_type_oids,
            from_sql: row.from_sql,
            generic_plans: row.generic_plans,
            custom_plans: row.custom_plans,
        })
        .collect::<Vec<_>>();
    pgrust_executor::pg_prepared_statement_rows(rows)
        .into_iter()
        .map(TupleSlot::virtual_row)
        .collect()
}

fn eval_pg_prepared_xact(ctx: &ExecutorContext) -> Vec<TupleSlot> {
    let rows = ctx
        .database
        .as_ref()
        .map(|db| db.prepared_xacts.rows())
        .unwrap_or_default()
        .into_iter()
        .map(|row| pgrust_executor::PreparedXactViewRow {
            transaction: row.transaction,
            gid: row.gid,
            prepared_at: row.prepared_at,
            owner_name: row.owner_name,
            database_name: row.database_name,
        })
        .collect::<Vec<_>>();
    pgrust_executor::pg_prepared_xact_rows(rows)
        .into_iter()
        .map(TupleSlot::virtual_row)
        .collect()
}

fn eval_pg_wait_events() -> Vec<TupleSlot> {
    pgrust_executor::pg_wait_event_rows()
        .into_iter()
        .map(TupleSlot::virtual_row)
        .collect()
}

fn eval_pg_timezone_names() -> Vec<TupleSlot> {
    pgrust_executor::pg_timezone_name_rows()
        .into_iter()
        .map(TupleSlot::virtual_row)
        .collect()
}

fn eval_pg_timezone_abbrevs() -> Vec<TupleSlot> {
    pgrust_executor::pg_timezone_abbrev_rows()
        .into_iter()
        .map(TupleSlot::virtual_row)
        .collect()
}

fn eval_pg_show_all_settings(
    output_columns: &[crate::include::nodes::primnodes::QueryColumn],
) -> Vec<TupleSlot> {
    let wal_segment_size = crate::backend::access::transam::xlog::WAL_SEG_SIZE_BYTES.to_string();
    pgrust_executor::pg_show_all_settings_rows(&wal_segment_size, output_columns)
        .into_iter()
        .map(TupleSlot::virtual_row)
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
    pgrust_executor::set_returning_call_label(call)
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
    Ok(
        pgrust_executor::generate_subscripts_values(&array, dimension, reverse)?
            .into_iter()
            .map(|value| TupleSlot::virtual_row(vec![value]))
            .collect(),
    )
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
        .map(pgrust_executor::parse_ts_stat_weights)
        .unwrap_or_default();
    let (column_name, table_name) = pgrust_executor::parse_ts_stat_select(query)?;
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

fn sequence_rows(
    ctx: &ExecutorContext,
) -> Result<Vec<pgrust_executor::SequenceViewRow>, ExecError> {
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
        let Some(data) = sequences.sequence_data(class.oid, class.relpersistence != 't')? else {
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
        rows.push(pgrust_executor::SequenceViewRow {
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
    let rows = sequence_rows(ctx)?
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
        .collect::<Vec<_>>();
    Ok(pgrust_executor::pg_sequences_rows(rows)
        .into_iter()
        .map(TupleSlot::virtual_row)
        .collect())
}

fn eval_information_schema_sequences(ctx: &ExecutorContext) -> Result<Vec<TupleSlot>, ExecError> {
    let sequence_catalog_name = if ctx.current_database_name.eq_ignore_ascii_case("postgres") {
        "regression".to_string()
    } else {
        ctx.current_database_name.clone()
    };
    let catalog = sequence_catalog(ctx)?;
    let rows = sequence_rows(ctx)?
        .into_iter()
        .filter(|row| !is_identity_owned_sequence(catalog, row.oid))
        .collect::<Vec<_>>();
    Ok(
        pgrust_executor::information_schema_sequence_rows(&sequence_catalog_name, rows)
            .into_iter()
            .map(TupleSlot::virtual_row)
            .collect(),
    )
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

fn eval_partition_tree(
    relid: &Expr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let Some(relation_oid) =
        pgrust_executor::partition_lookup_oid(eval_expr(relid, slot, ctx)?, "pg_partition_tree")?
    else {
        return Ok(Vec::new());
    };
    let catalog = partition_catalog(ctx)?;
    let rows = partition_tree_entries(catalog, relation_oid)?
        .into_iter()
        .map(|entry| pgrust_executor::PartitionTreeViewRow {
            relid: entry.relid,
            parentrelid: entry.parentrelid,
            isleaf: entry.isleaf,
            level: entry.level,
        })
        .collect::<Vec<_>>();
    Ok(pgrust_executor::pg_partition_tree_rows(rows)
        .into_iter()
        .map(TupleSlot::virtual_row)
        .collect())
}

fn eval_partition_ancestors(
    relid: &Expr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let Some(relation_oid) = pgrust_executor::partition_lookup_oid(
        eval_expr(relid, slot, ctx)?,
        "pg_partition_ancestors",
    )?
    else {
        return Ok(Vec::new());
    };
    let catalog = partition_catalog(ctx)?;
    Ok(
        pgrust_executor::pg_partition_ancestor_rows(partition_ancestor_oids(
            catalog,
            relation_oid,
        )?)
        .into_iter()
        .map(TupleSlot::virtual_row)
        .collect(),
    )
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
        let mut state =
            GenerateSeriesState::numeric(start_val, stop_val, step_val).map_err(ExecError::from)?;
        return collect_value_per_call_values(ctx, || Ok(state.next_value()));
    }

    let mut state = GenerateSeriesState::integral(start_val, stop_val, step_val, output_kind)
        .map_err(ExecError::from)?;
    collect_value_per_call_values(ctx, || Ok(state.next_value()))
}

fn collect_value_per_call_values(
    ctx: &mut ExecutorContext,
    mut next: impl FnMut() -> Result<Option<Value>, ExecError>,
) -> Result<Vec<Value>, ExecError> {
    let mut rows = Vec::new();
    loop {
        ctx.check_for_interrupts()?;
        let Some(value) = next()? else {
            break;
        };
        rows.push(value);
    }
    Ok(rows)
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
    let mut state = pgrust_executor::TimestampGenerateSeriesState::new(
        start_val,
        stop_val,
        step_val,
        output_kind,
    )?;
    let mut rows = Vec::new();
    while let Some(value) = {
        ctx.check_for_interrupts()?;
        state.next_value()?
    } {
        rows.push(TupleSlot::virtual_row(vec![value]));
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
                arrays.push(Some(values));
            }
            Value::Array(values) => {
                arrays.push(Some(values));
            }
            Value::PgArray(array) => {
                let values = pgrust_executor::unnest_array_values(array);
                arrays.push(Some(values));
            }
            other => {
                if let Some(array) = normalize_array_value(&other) {
                    let values = pgrust_executor::unnest_array_values(array);
                    arrays.push(Some(values));
                    continue;
                }
                if expr_sql_type_hint(arg).is_some_and(|ty| {
                    !ty.is_array
                        && matches!(ty.kind, SqlTypeKind::Int2Vector | SqlTypeKind::OidVector)
                }) && let Some(array) = normalize_array_value(&other)
                {
                    let values = pgrust_executor::unnest_array_values(array);
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

    let mut unnest_rows = UnnestRows::with_args_and_columns(arrays, args, output_columns);
    let mut rows = Vec::new();
    while let Some(row) = {
        ctx.check_for_interrupts()?;
        unnest_rows.next_row().map_err(ExecError::from)?
    } {
        rows.push(TupleSlot::virtual_row(row));
    }
    Ok(rows)
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

    let mut option_rows = pgrust_executor::PgOptionsToTableRows::new(values);
    let mut rows = Vec::new();
    while let Some(row) = {
        ctx.check_for_interrupts()?;
        option_rows.next_row().map_err(ExecError::from)?
    } {
        rows.push(TupleSlot::virtual_row(row));
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::parser::{SqlType, SqlTypeKind};

    #[test]
    fn pg_show_all_settings_includes_wal_segment_size() {
        let output_columns = vec![
            QueryColumn {
                name: "name".into(),
                sql_type: SqlType::new(SqlTypeKind::Text),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "setting".into(),
                sql_type: SqlType::new(SqlTypeKind::Text),
                wire_type_oid: None,
            },
        ];

        let rows = eval_pg_show_all_settings(&output_columns);
        let expected_setting = Value::Text(
            crate::backend::access::transam::xlog::WAL_SEG_SIZE_BYTES
                .to_string()
                .into(),
        );

        assert!(
            rows.into_iter().any(|row| {
                row.tts_values
                    == vec![
                        Value::Text("wal_segment_size".into()),
                        expected_setting.clone(),
                    ]
            }),
            "pg_show_all_settings should expose wal_segment_size for regression \\gset"
        );
    }
}
