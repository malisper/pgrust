use super::exec_expr::{eval_string_to_table_rows, normalize_array_value};
use super::expr_date::add_interval_to_local_timestamp;
use super::expr_json::{eval_json_record_set_returning_function, eval_json_table_function};
use super::expr_txid::eval_txid_snapshot_xip_values;
use super::pg_regex::{eval_regexp_matches_rows, eval_regexp_split_to_table_rows};
use super::sqlfunc::execute_user_defined_sql_set_returning_function;
use super::{ExecError, ExecutorContext, Expr, SetReturningCall, TupleSlot, Value, eval_expr};
use crate::backend::commands::partition::{partition_ancestor_oids, partition_tree_entries};
use crate::backend::parser::CatalogLookup;
use crate::backend::parser::SqlTypeKind;
use crate::backend::utils::record::assign_anonymous_record_descriptor;
use crate::backend::utils::time::datetime::{
    current_timezone_name, days_from_ymd, days_in_month, timestamp_parts_from_usecs, ymd_from_days,
};
use crate::backend::utils::time::timestamp::{timestamp_at_time_zone, timestamptz_at_time_zone};
use crate::include::nodes::datetime::{
    TIMESTAMP_NOBEGIN, TIMESTAMP_NOEND, TimestampADT, TimestampTzADT, USECS_PER_DAY,
};
use crate::include::nodes::datum::{IntervalValue, NumericValue, RecordValue};
use crate::include::nodes::primnodes::expr_sql_type_hint;
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
        SetReturningCall::Unnest { args, .. } => eval_unnest(args, slot, ctx),
        SetReturningCall::JsonTableFunction { kind, args, .. } => {
            eval_json_table_function(*kind, args, slot, ctx)
        }
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
        SetReturningCall::TextSearchTableFunction { .. } => Err(ExecError::Parse(
            crate::backend::parser::ParseError::UnexpectedToken {
                expected: "implemented text search table function",
                actual: "text search table function".into(),
            },
        )),
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
    let Some(catalog) = ctx.catalog.as_ref() else {
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
    if row.prolang == crate::include::catalog::PG_LANGUAGE_SQL_OID {
        execute_user_defined_sql_set_returning_function(&row, args, output_columns, slot, ctx)
    } else {
        execute_user_defined_set_returning_function(proc_oid, args, output_columns, slot, ctx)
    }
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

pub(crate) fn set_returning_call_label(call: &SetReturningCall) -> &'static str {
    match call {
        SetReturningCall::GenerateSeries { .. } => "generate_series",
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
        },
        SetReturningCall::UserDefined { .. } => "user_defined_srf",
    }
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

fn partition_catalog(
    ctx: &ExecutorContext,
) -> Result<&crate::backend::utils::cache::visible_catalog::VisibleCatalog, ExecError> {
    ctx.catalog
        .as_ref()
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
        return Err(ExecError::GenerateSeriesInvalidArg("step size", "infinity"));
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
