use super::expr_json::eval_json_table_function;
use super::pg_regex::{eval_regexp_matches_rows, eval_regexp_split_to_table_rows};
use super::{ExecError, ExecutorContext, Expr, SetReturningCall, TupleSlot, Value, eval_expr};
use crate::backend::parser::SqlTypeKind;
use crate::include::nodes::datum::NumericValue;

pub(crate) fn eval_set_returning_call(
    call: &SetReturningCall,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    match call {
        SetReturningCall::GenerateSeries {
            start,
            stop,
            step,
            output,
            ..
        } => eval_generate_series(start, stop, step, output.sql_type.kind, slot, ctx),
        SetReturningCall::Unnest { args, .. } => eval_unnest(args, slot, ctx),
        SetReturningCall::JsonTableFunction { kind, args, .. } => {
            eval_json_table_function(*kind, args, slot, ctx)
        }
        SetReturningCall::RegexTableFunction { kind, args, .. } => {
            eval_regex_table_function(*kind, args, slot, ctx)
        }
        SetReturningCall::TextSearchTableFunction { .. } => Err(ExecError::Parse(
            crate::backend::parser::ParseError::UnexpectedToken {
                expected: "implemented text search table function",
                actual: "text search table function".into(),
            },
        )),
    }
}

pub(crate) fn eval_scalar_set_returning_call(
    call: &SetReturningCall,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<Value>, ExecError> {
    if call.output_columns().len() != 1 {
        return Err(ExecError::RaiseException(
            "set-returning function returning record called in context that cannot accept type record"
                .into(),
        ));
    }
    Ok(eval_set_returning_call(call, slot, ctx)?
        .into_iter()
        .map(|mut row| {
            Value::materialize_all(&mut row.tts_values);
            row.tts_values.into_iter().next().unwrap_or(Value::Null)
        })
        .collect())
}

pub(crate) fn set_returning_call_label(call: &SetReturningCall) -> &'static str {
    match call {
        SetReturningCall::GenerateSeries { .. } => "generate_series",
        SetReturningCall::Unnest { .. } => "unnest",
        SetReturningCall::JsonTableFunction { kind, .. } => match kind {
            crate::include::nodes::plannodes::JsonTableFunction::ObjectKeys => "json_object_keys",
            crate::include::nodes::plannodes::JsonTableFunction::Each => "json_each",
            crate::include::nodes::plannodes::JsonTableFunction::EachText => "json_each_text",
            crate::include::nodes::plannodes::JsonTableFunction::ArrayElements => {
                "json_array_elements"
            }
            crate::include::nodes::plannodes::JsonTableFunction::ArrayElementsText => {
                "json_array_elements_text"
            }
            crate::include::nodes::plannodes::JsonTableFunction::JsonbPathQuery => {
                "jsonb_path_query"
            }
            crate::include::nodes::plannodes::JsonTableFunction::JsonbObjectKeys => {
                "jsonb_object_keys"
            }
            crate::include::nodes::plannodes::JsonTableFunction::JsonbEach => "jsonb_each",
            crate::include::nodes::plannodes::JsonTableFunction::JsonbEachText => "jsonb_each_text",
            crate::include::nodes::plannodes::JsonTableFunction::JsonbArrayElements => {
                "jsonb_array_elements"
            }
            crate::include::nodes::plannodes::JsonTableFunction::JsonbArrayElementsText => {
                "jsonb_array_elements_text"
            }
        },
        SetReturningCall::RegexTableFunction { kind, .. } => match kind {
            crate::include::nodes::plannodes::RegexTableFunction::Matches => "regexp_matches",
            crate::include::nodes::plannodes::RegexTableFunction::SplitToTable => {
                "regexp_split_to_table"
            }
        },
        SetReturningCall::TextSearchTableFunction { kind, .. } => match kind {
            crate::include::nodes::plannodes::TextSearchTableFunction::TokenType => "ts_token_type",
            crate::include::nodes::plannodes::TextSearchTableFunction::Parse => "ts_parse",
            crate::include::nodes::plannodes::TextSearchTableFunction::Debug => "ts_debug",
        },
    }
}

fn eval_regex_table_function(
    kind: crate::include::nodes::plannodes::RegexTableFunction,
    args: &[Expr],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let values = args
        .iter()
        .map(|arg| eval_expr(arg, slot, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    let rows = match kind {
        crate::include::nodes::plannodes::RegexTableFunction::Matches => {
            eval_regexp_matches_rows(&values)?
        }
        crate::include::nodes::plannodes::RegexTableFunction::SplitToTable => {
            eval_regexp_split_to_table_rows(&values)?
        }
    };
    Ok(rows
        .into_iter()
        .map(|value| TupleSlot::virtual_row(vec![value]))
        .collect())
}

fn eval_generate_series(
    start: &Expr,
    stop: &Expr,
    step: &Expr,
    output_kind: SqlTypeKind,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let start_val = eval_expr(start, slot, ctx)?;
    let stop_val = eval_expr(stop, slot, ctx)?;
    let step_val = eval_expr(step, slot, ctx)?;

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

        use std::cmp::Ordering;
        let step_cmp = step.cmp(&NumericValue::zero());
        if step_cmp == Ordering::Equal {
            return Err(ExecError::GenerateSeriesZeroStep);
        }

        let mut current = start;
        let mut rows = Vec::new();
        loop {
            let done = match step_cmp {
                Ordering::Greater => current.cmp(&stop) == Ordering::Greater,
                Ordering::Less => current.cmp(&stop) == Ordering::Less,
                Ordering::Equal => unreachable!(),
            };
            if done {
                break;
            }
            rows.push(TupleSlot::virtual_row(vec![Value::Numeric(
                current.clone(),
            )]));
            current = current.add(&step);
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

fn eval_unnest(
    args: &[Expr],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<TupleSlot>, ExecError> {
    let mut arrays = Vec::with_capacity(args.len());
    let mut max_len = 0usize;
    for arg in args {
        match eval_expr(arg, slot, ctx)? {
            Value::Null => arrays.push(None),
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
