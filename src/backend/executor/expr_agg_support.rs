use super::agg::{AccumState, AggregateRuntime, CustomAggregateRuntime};
use super::expr_casts::cast_value;
use super::expr_ops::{add_values, div_values, sub_values};
use super::sqlfunc::execute_user_defined_sql_scalar_function_values;
use super::{ExecError, ExecutorContext};
use crate::backend::parser::CatalogLookup;
use crate::include::catalog::{
    INT8_TYPE_OID, PG_LANGUAGE_SQL_OID, builtin_aggregate_function_for_proc_oid,
    builtin_hypothetical_aggregate_function_for_proc_oid, builtin_scalar_function_for_proc_oid,
};
use crate::include::nodes::datum::{ArrayValue, NumericValue, Value};
use crate::include::nodes::primnodes::{AggAccum, BuiltinScalarFunction};
use crate::pl::plpgsql::execute_user_defined_scalar_function_values;

pub(crate) fn build_aggregate_runtime(
    accum: &AggAccum,
    ctx: &ExecutorContext,
) -> Result<AggregateRuntime, ExecError> {
    if let Some(func) = builtin_aggregate_function_for_proc_oid(accum.aggfnoid) {
        return Ok(AggregateRuntime::Builtin {
            func,
            transition: AccumState::transition_fn(func, accum.args.len(), accum.distinct),
        });
    }
    if let Some(func) = builtin_hypothetical_aggregate_function_for_proc_oid(accum.aggfnoid) {
        return Ok(AggregateRuntime::Hypothetical { func });
    }

    let catalog = ctx
        .catalog
        .as_deref()
        .ok_or_else(|| ExecError::DetailedError {
            message: "aggregate execution requires executor catalog context".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        })?;
    let aggregate = load_visible_aggregate_row(accum, catalog)?;
    if aggregate.aggkind != 'n' {
        return Err(ExecError::DetailedError {
            message: "only plain aggregates are supported".into(),
            detail: Some(format!("aggkind = {}", aggregate.aggkind)),
            hint: None,
            sqlstate: "0A000",
        });
    }
    let transfn = catalog
        .proc_row_by_oid(aggregate.aggtransfn)
        .ok_or_else(|| ExecError::DetailedError {
            message: format!(
                "unknown aggregate transition function oid {}",
                aggregate.aggtransfn
            ),
            detail: None,
            hint: None,
            sqlstate: "42883",
        })?;
    let finalfn = if aggregate.aggfinalfn == 0 {
        None
    } else {
        Some(
            catalog
                .proc_row_by_oid(aggregate.aggfinalfn)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!(
                        "unknown aggregate final function oid {}",
                        aggregate.aggfinalfn
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "42883",
                })?,
        )
    };
    let transtype = catalog
        .type_by_oid(aggregate.aggtranstype)
        .ok_or_else(|| ExecError::DetailedError {
            message: format!(
                "unknown aggregate transition type oid {}",
                aggregate.aggtranstype
            ),
            detail: None,
            hint: None,
            sqlstate: "42883",
        })?
        .sql_type;
    let init_value = aggregate
        .agginitval
        .as_ref()
        .map(|text| cast_value(Value::Text(text.clone().into()), transtype))
        .transpose()?;

    Ok(AggregateRuntime::Custom(CustomAggregateRuntime {
        transfn_oid: aggregate.aggtransfn,
        transfn_strict: transfn.proisstrict,
        finalfn_oid: finalfn.as_ref().map(|row| row.oid),
        finalfn_strict: finalfn.as_ref().is_some_and(|row| row.proisstrict),
        transtype,
        init_value,
    }))
}

fn load_visible_aggregate_row(
    accum: &AggAccum,
    catalog: &dyn CatalogLookup,
) -> Result<crate::include::catalog::PgAggregateRow, ExecError> {
    catalog
        .aggregate_by_fnoid(accum.aggfnoid)
        .ok_or_else(|| ExecError::DetailedError {
            message: format!("unknown aggregate oid {}", accum.aggfnoid),
            detail: None,
            hint: None,
            sqlstate: "42883",
        })
}

pub(crate) fn execute_scalar_function_value_call(
    proc_oid: u32,
    arg_values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    if let Some(func) = builtin_scalar_function_for_proc_oid(proc_oid) {
        return execute_builtin_scalar_function_value_call(func, arg_values);
    }

    let catalog = ctx
        .catalog
        .as_ref()
        .ok_or_else(|| ExecError::DetailedError {
            message: "scalar function execution requires executor catalog context".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        })?;
    let row = catalog
        .proc_row_by_oid(proc_oid)
        .ok_or_else(|| ExecError::DetailedError {
            message: format!("unknown function oid {proc_oid}"),
            detail: None,
            hint: None,
            sqlstate: "42883",
        })?;
    if row.prokind != 'f' {
        return Err(ExecError::DetailedError {
            message: "only scalar functions are executable in aggregate support".into(),
            detail: Some(format!("prokind = {}", row.prokind)),
            hint: None,
            sqlstate: "0A000",
        });
    }
    match row.prolang {
        PG_LANGUAGE_SQL_OID => {
            execute_user_defined_sql_scalar_function_values(&row, arg_values, ctx)
        }
        _ => execute_user_defined_scalar_function_values(proc_oid, arg_values, ctx),
    }
}

pub(crate) fn execute_builtin_scalar_function_value_call(
    func: BuiltinScalarFunction,
    arg_values: &[Value],
) -> Result<Value, ExecError> {
    match func {
        BuiltinScalarFunction::Int4Pl => match arg_values {
            [left, right] => add_values(left.clone(), right.clone()),
            _ => malformed_aggregate_support_call("int4pl"),
        },
        BuiltinScalarFunction::Int4Mi => match arg_values {
            [left, right] => sub_values(left.clone(), right.clone()),
            _ => malformed_aggregate_support_call("int4mi"),
        },
        BuiltinScalarFunction::Int8Inc => match arg_values {
            [state] => add_values(state.clone(), Value::Int64(1)),
            _ => malformed_aggregate_support_call("int8inc"),
        },
        BuiltinScalarFunction::Int8IncAny => match arg_values {
            [state, _] => add_values(state.clone(), Value::Int64(1)),
            _ => malformed_aggregate_support_call("int8inc_any"),
        },
        BuiltinScalarFunction::Int4AvgAccum => match arg_values {
            [state, Value::Int32(new_value)] => {
                let (count, sum) = aggregate_int8_pair(state, "int4_avg_accum")?;
                Ok(Value::PgArray(
                    ArrayValue::from_1d(vec![
                        Value::Int64(count + 1),
                        Value::Int64(sum + i64::from(*new_value)),
                    ])
                    .with_element_type_oid(INT8_TYPE_OID),
                ))
            }
            _ => malformed_aggregate_support_call("int4_avg_accum"),
        },
        BuiltinScalarFunction::Int8Avg => match arg_values {
            [state] => {
                let (count, sum) = aggregate_int8_pair(state, "int8_avg")?;
                if count == 0 {
                    return Ok(Value::Null);
                }
                div_values(
                    Value::Numeric(NumericValue::from_i64(sum)),
                    Value::Numeric(NumericValue::from_i64(count)),
                )
            }
            _ => malformed_aggregate_support_call("int8_avg"),
        },
        BuiltinScalarFunction::BoolAndStateFunc => match arg_values {
            [Value::Bool(left), Value::Bool(right)] => Ok(Value::Bool(*left && *right)),
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            _ => malformed_aggregate_support_call("booland_statefunc"),
        },
        BuiltinScalarFunction::BoolOrStateFunc => match arg_values {
            [Value::Bool(left), Value::Bool(right)] => Ok(Value::Bool(*left || *right)),
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            _ => malformed_aggregate_support_call("boolor_statefunc"),
        },
        other => Err(ExecError::DetailedError {
            message: format!(
                "builtin function {:?} is not supported by aggregate value execution",
                other
            ),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        }),
    }
}

fn aggregate_int8_pair(value: &Value, func: &'static str) -> Result<(i64, i64), ExecError> {
    let elements = match value {
        Value::PgArray(array) => &array.elements,
        Value::Array(elements) => elements,
        other => {
            return Err(ExecError::TypeMismatch {
                op: func,
                left: other.clone(),
                right: Value::PgArray(ArrayValue::from_1d(vec![Value::Int64(0), Value::Int64(0)])),
            });
        }
    };
    match elements.as_slice() {
        [Value::Int64(count), Value::Int64(sum)] => Ok((*count, *sum)),
        [Value::Int32(count), Value::Int32(sum)] => Ok((i64::from(*count), i64::from(*sum))),
        _ => Err(ExecError::DetailedError {
            message: format!("{func} expected a 2-element int8 array state"),
            detail: Some(format!("state = {value:?}")),
            hint: None,
            sqlstate: "22023",
        }),
    }
}

fn malformed_aggregate_support_call(name: &'static str) -> Result<Value, ExecError> {
    Err(ExecError::DetailedError {
        message: format!("malformed aggregate support call to {name}"),
        detail: None,
        hint: None,
        sqlstate: "XX000",
    })
}
