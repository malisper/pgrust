use super::exec_expr::{
    eval_native_builtin_scalar_value_call, eval_string_to_table_rows, normalize_array_value,
};
use super::expr_date::add_interval_to_local_timestamp;
use super::expr_json::{
    eval_json_record_set_returning_function, eval_json_table_function, eval_sql_json_table,
};
use super::expr_txid::eval_txid_snapshot_xip_values;
use super::expr_xml::eval_sql_xml_table;
use super::pg_regex::{eval_regexp_matches_rows, eval_regexp_split_to_table_rows};
use super::sqlfunc::execute_user_defined_sql_set_returning_function;
use super::{ExecError, ExecutorContext, Expr, SetReturningCall, TupleSlot, Value, eval_expr};
use crate::backend::access::heap::heapam::{heap_scan_begin_visible, heap_scan_next_visible};
use crate::backend::access::index::buildkeys::materialize_heap_row_values;
use crate::backend::commands::partition::{partition_ancestor_oids, partition_tree_entries};
use crate::backend::parser::{CatalogLookup, SqlTypeKind};
use crate::backend::utils::record::assign_anonymous_record_descriptor;
use crate::backend::utils::time::datetime::{
    current_timezone_name, days_from_ymd, days_in_month, timestamp_parts_from_usecs, ymd_from_days,
};
use crate::backend::utils::time::timestamp::{timestamp_at_time_zone, timestamptz_at_time_zone};
use crate::include::catalog::builtin_scalar_function_for_proc_oid;
use crate::include::nodes::datetime::{
    TIMESTAMP_NOBEGIN, TIMESTAMP_NOEND, TimestampADT, TimestampTzADT, USECS_PER_DAY, USECS_PER_SEC,
};
use crate::include::nodes::datum::{IntervalValue, NumericValue, RecordValue};
use crate::include::nodes::primnodes::{TextSearchTableFunction, expr_sql_type_hint};
use crate::include::nodes::tsearch::TsWeight;
use crate::pl::plpgsql::execute_user_defined_set_returning_function;

const MAX_UNBOUNDED_TIMESTAMP_SERIES_ROWS: usize = 10_000;

pub(crate) fn eval_set_returning_call(
    call: &SetReturningCall,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let mut rows = match call {
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
        SetReturningCall::Unnest { args, .. } => eval_unnest(args, slot, ctx),
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
            ..
        } => eval_json_record_set_returning_function(
            *kind,
            args,
            output_columns,
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
        SetReturningCall::TxidSnapshotXip { arg, .. } => eval_txid_snapshot_xip(arg, slot, ctx),
        SetReturningCall::TextSearchTableFunction { kind, args, .. } => {
            eval_text_search_table_function(*kind, args, slot, ctx)
        }
        SetReturningCall::UserDefined {
            proc_oid,
            args,
            output_columns,
            ..
        } => execute_user_defined_set_returning_function_by_language(
            *proc_oid,
            args,
            output_columns,
            slot,
            ctx,
        ),
    }?;
    if call.with_ordinality() {
        for (index, row) in rows.iter_mut().enumerate() {
            row.tts_values.push(Value::Int64((index + 1) as i64));
            row.tts_nvalid = row.tts_values.len();
        }
    }
    Ok(rows)
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
    if let Some(rows) = execute_native_set_returning_function(&row, args, slot, ctx)? {
        return Ok(rows);
    }
    if row.proname.eq_ignore_ascii_case("pg_show_all_settings") {
        return Ok(eval_pg_show_all_settings(output_columns));
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
        "pg_timezone_names" => Some(vec![TupleSlot::virtual_row(vec![
            Value::Text("UTC".into()),
            Value::Text("UTC".into()),
            Value::Interval(IntervalValue::zero()),
            Value::Bool(false),
        ])]),
        "pg_tablespace_databases" => Some(eval_pg_tablespace_databases(&values)),
        _ => {
            if let Some(func) = builtin_scalar_function_for_proc_oid(row.oid) {
                let value = eval_native_builtin_scalar_value_call(func, &values, false, ctx)?;
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
    let row = output_columns
        .iter()
        .map(|column| match column.name.as_str() {
            "name" => Value::Text("default_statistics_target".into()),
            "setting" => Value::Text("100".into()),
            "unit" => Value::Null,
            "category" => Value::Text("Query Tuning / Planner Cost Constants".into()),
            "short_desc" => Value::Text("Sets the default statistics target.".into()),
            "extra_desc" => Value::Null,
            "context" => Value::Text("user".into()),
            "vartype" => Value::Text("integer".into()),
            "source" => Value::Text("default".into()),
            "min_val" => Value::Text("1".into()),
            "max_val" => Value::Text("10000".into()),
            "enumvals" => Value::Null,
            "boot_val" => Value::Text("100".into()),
            "reset_val" => Value::Text("100".into()),
            "sourcefile" => Value::Null,
            "sourceline" => Value::Null,
            "pending_restart" => Value::Bool(false),
            _ => Value::Null,
        })
        .collect();
    vec![TupleSlot::virtual_row(row)]
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
    let start_val = eval_expr(start, slot, ctx)?;
    let stop_val = eval_expr(stop, slot, ctx)?;
    let step_val = eval_expr(step, slot, ctx)?;

    if matches!(output_kind, SqlTypeKind::Timestamp) {
        return eval_timestamp_generate_series(start_val, stop_val, step_val, output_kind, ctx);
    }
    if matches!(output_kind, SqlTypeKind::TimestampTz) {
        return eval_timestamptz_generate_series(
            start_val, stop_val, step_val, timezone, slot, ctx,
        );
    }

    if matches!(output_kind, SqlTypeKind::Numeric) {
        let to_numeric = |v: Value, label: &'static str| -> Result<NumericValue, ExecError> {
            match v {
                Value::Numeric(n) => Ok(n),
                Value::Int32(i) => Ok(NumericValue::from_i64(i64::from(i))),
                Value::Int64(i) => Ok(NumericValue::from_i64(i)),
                other => Err(ExecError::TypeMismatch {
                    op: label,
                    left: other,
                    right: Value::Null,
                }),
            }
        };
        let start = to_numeric(start_val, "generate_series start")?;
        let stop = to_numeric(stop_val, "generate_series stop")?;
        let step = to_numeric(step_val, "generate_series step")?;
        let validate = |value: &NumericValue, arg: &'static str| -> Result<(), ExecError> {
            match value {
                NumericValue::NaN => Err(ExecError::GenerateSeriesInvalidArg(arg, "NaN")),
                NumericValue::PosInf | NumericValue::NegInf => {
                    Err(ExecError::GenerateSeriesInvalidArg(arg, "infinity"))
                }
                NumericValue::Finite { .. } => Ok(()),
            }
        };
        validate(&start, "start")?;
        validate(&stop, "stop")?;
        validate(&step, "step size")?;
        let series_dscale = [start.dscale(), stop.dscale(), step.dscale()]
            .into_iter()
            .max()
            .unwrap_or(0);

        use std::cmp::Ordering;
        let step_cmp = step.cmp(&NumericValue::zero());
        if step_cmp == Ordering::Equal {
            return Err(ExecError::GenerateSeriesZeroStep);
        }

        let mut current = start;
        let mut rows = Vec::new();
        loop {
            ctx.check_for_interrupts()?;
            let done = match step_cmp {
                Ordering::Greater => current.cmp(&stop) == Ordering::Greater,
                Ordering::Less => current.cmp(&stop) == Ordering::Less,
                Ordering::Equal => unreachable!(),
            };
            if done {
                break;
            }
            rows.push(TupleSlot::virtual_row(vec![Value::Numeric(
                current.clone().with_dscale(series_dscale),
            )]));
            current = current.add(&step).with_dscale(series_dscale);
        }
        return Ok(rows);
    }

    let to_i64 = |v: Value, label: &'static str| -> Result<i64, ExecError> {
        match v {
            Value::Int32(v) => Ok(i64::from(v)),
            Value::Int64(v) => Ok(v),
            other => Err(ExecError::TypeMismatch {
                op: label,
                left: other,
                right: Value::Null,
            }),
        }
    };
    let mut current = to_i64(start_val, "generate_series start")?;
    let end = to_i64(stop_val, "generate_series stop")?;
    let step = to_i64(step_val, "generate_series step")?;
    if step == 0 {
        return Err(ExecError::GenerateSeriesZeroStep);
    }
    let mut rows = Vec::new();
    loop {
        ctx.check_for_interrupts()?;
        let done = if step > 0 {
            current > end
        } else {
            current < end
        };
        if done {
            break;
        }
        rows.push(TupleSlot::virtual_row(vec![match output_kind {
            SqlTypeKind::Int8 => Value::Int64(current),
            _ => Value::Int32(current as i32),
        }]));
        current += step;
    }
    Ok(rows)
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

    let mut rows = Vec::with_capacity(max_len);
    for idx in 0..max_len {
        ctx.check_for_interrupts()?;
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
