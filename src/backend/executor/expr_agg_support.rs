use super::agg::{AccumState, AggregateRuntime, CustomAggregateRuntime};
use super::exec_expr::append_array_value;
use super::exec_expr::{
    ensure_builtin_side_effects_allowed, eval_pg_column_is_updatable, eval_pg_describe_object,
    eval_pg_get_object_address, eval_pg_identify_object, eval_pg_identify_object_as_address,
    eval_pg_relation_is_updatable, eval_pg_settings_get_flags,
};
use super::expr_casts::cast_value_with_source_type_catalog_and_config;
use super::expr_ops::{add_values, compare_order_values, div_values, sub_values};
use super::expr_string::{
    eval_parse_ident_function, eval_pg_rust_test_enc_conversion, eval_quote_nullable_function,
};
use super::sqlfunc::{
    execute_user_defined_sql_scalar_function_values,
    execute_user_defined_sql_scalar_function_values_with_arg_type_oids,
};
use super::{ExecError, ExecutorContext, TypedFunctionArg};
use crate::backend::parser::{CatalogLookup, SqlType, SqlTypeKind};
use crate::include::catalog::{
    BPCHAR_HASH_OPCLASS_OID, INT8_TYPE_OID, PG_LANGUAGE_SQL_OID,
    builtin_scalar_function_for_proc_oid,
};
use crate::include::nodes::datum::{ArrayValue, NumericValue, Value};
use crate::include::nodes::parsenodes::ParseError;
use crate::include::nodes::primnodes::{
    AggAccum, BuiltinScalarFunction, HashFunctionKind, HypotheticalAggFunc, OrderedSetAggFunc,
    expr_sql_type_hint,
};
use crate::pl::plpgsql::{
    execute_user_defined_scalar_function_values,
    execute_user_defined_scalar_function_values_with_arg_types,
};
use pgrust_executor::{
    AggregateRuntimeSelection, AggregateSupportError, aggregate_runtime_selection,
    concrete_custom_aggregate_transtype, normalize_variadic_scalar_function_args,
};
use std::cmp::Ordering;

pub(crate) fn build_aggregate_runtime(
    accum: &AggAccum,
    ctx: &mut ExecutorContext,
) -> Result<AggregateRuntime, ExecError> {
    super::exec_expr::ensure_proc_execute_allowed(accum.aggfnoid, ctx)?;
    if let AggregateRuntimeSelection::Builtin(func) =
        aggregate_runtime_selection(accum.aggfnoid, None, None)
    {
        return Ok(AggregateRuntime::Builtin {
            func,
            transition: AccumState::transition_fn(func, accum.args.len(), accum.distinct),
        });
    }
    if let AggregateRuntimeSelection::Hypothetical(func) =
        aggregate_runtime_selection(accum.aggfnoid, None, None)
    {
        return Ok(AggregateRuntime::Hypothetical { func });
    }
    if let AggregateRuntimeSelection::OrderedSet(func) =
        aggregate_runtime_selection(accum.aggfnoid, None, None)
    {
        return Ok(AggregateRuntime::OrderedSet { func });
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
    match aggregate_runtime_selection(
        accum.aggfnoid,
        Some(aggregate.aggkind),
        Some(aggregate.aggfinalfn),
    ) {
        AggregateRuntimeSelection::Builtin(func) => {
            return Ok(AggregateRuntime::Builtin {
                func,
                transition: AccumState::transition_fn(func, accum.args.len(), accum.distinct),
            });
        }
        AggregateRuntimeSelection::Hypothetical(func) => {
            return Ok(AggregateRuntime::Hypothetical { func });
        }
        AggregateRuntimeSelection::OrderedSet(func) => {
            return Ok(AggregateRuntime::OrderedSet { func });
        }
        AggregateRuntimeSelection::PlainCustom => {}
        AggregateRuntimeSelection::UnsupportedKind(kind) => {
            return Err(ExecError::DetailedError {
                message: "only plain aggregates are supported".into(),
                detail: Some(format!("aggkind = {kind}")),
                hint: None,
                sqlstate: "0A000",
            });
        }
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
    let combinefn = if aggregate.aggcombinefn == 0 {
        None
    } else {
        Some(
            catalog
                .proc_row_by_oid(aggregate.aggcombinefn)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!(
                        "unknown aggregate combine function oid {}",
                        aggregate.aggcombinefn
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "42883",
                })?,
        )
    };
    let mtransfn = if aggregate.aggmtransfn == 0 {
        None
    } else {
        Some(
            catalog
                .proc_row_by_oid(aggregate.aggmtransfn)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!(
                        "unknown aggregate moving transition function oid {}",
                        aggregate.aggmtransfn
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "42883",
                })?,
        )
    };
    let minvtransfn = if aggregate.aggminvtransfn == 0 {
        None
    } else {
        Some(
            catalog
                .proc_row_by_oid(aggregate.aggminvtransfn)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!(
                        "unknown aggregate moving inverse transition function oid {}",
                        aggregate.aggminvtransfn
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "42883",
                })?,
        )
    };
    let mfinalfn = if aggregate.aggmfinalfn == 0 {
        None
    } else {
        Some(
            catalog
                .proc_row_by_oid(aggregate.aggmfinalfn)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!(
                        "unknown aggregate moving final function oid {}",
                        aggregate.aggmfinalfn
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "42883",
                })?,
        )
    };
    let declared_transtype = catalog
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
    let input_arg_types = accum
        .args
        .iter()
        .filter_map(expr_sql_type_hint)
        .collect::<Vec<_>>();
    let transtype = concrete_custom_aggregate_transtype(declared_transtype, &input_arg_types);
    let declared_mtranstype = if aggregate.aggmtranstype == 0 {
        declared_transtype
    } else {
        catalog
            .type_by_oid(aggregate.aggmtranstype)
            .ok_or_else(|| ExecError::DetailedError {
                message: format!(
                    "unknown aggregate moving transition type oid {}",
                    aggregate.aggmtranstype
                ),
                detail: None,
                hint: None,
                sqlstate: "42883",
            })?
            .sql_type
    };
    let mtranstype = concrete_custom_aggregate_transtype(declared_mtranstype, &input_arg_types);
    let mut transfn_arg_types = Vec::with_capacity(input_arg_types.len() + 1);
    transfn_arg_types.push(transtype);
    transfn_arg_types.extend(input_arg_types.iter().copied());
    let combinefn_arg_types = vec![transtype, transtype];
    let mut finalfn_arg_types = vec![transtype];
    if aggregate.aggfinalextra {
        finalfn_arg_types.extend(input_arg_types.iter().copied());
    }
    let mut mtransfn_arg_types = Vec::with_capacity(input_arg_types.len() + 1);
    mtransfn_arg_types.push(mtranstype);
    mtransfn_arg_types.extend(input_arg_types.iter().copied());
    let minvtransfn_arg_types = mtransfn_arg_types.clone();
    let mut mfinalfn_arg_types = vec![mtranstype];
    if aggregate.aggmfinalextra {
        mfinalfn_arg_types.extend(input_arg_types.iter().copied());
    }
    let init_value = aggregate
        .agginitval
        .as_ref()
        .map(|text| {
            cast_value_with_source_type_catalog_and_config(
                Value::Text(text.clone().into()),
                None,
                transtype,
                ctx.catalog.as_deref(),
                &ctx.datetime_config,
            )
        })
        .transpose()?;
    let minit_value = aggregate
        .aggminitval
        .as_ref()
        .map(|text| {
            cast_value_with_source_type_catalog_and_config(
                Value::Text(text.clone().into()),
                None,
                mtranstype,
                ctx.catalog.as_deref(),
                &ctx.datetime_config,
            )
        })
        .transpose()?;

    Ok(AggregateRuntime::Custom(CustomAggregateRuntime {
        transfn_oid: aggregate.aggtransfn,
        transfn_strict: transfn.proisstrict,
        combinefn_oid: combinefn.as_ref().map(|row| row.oid),
        combinefn_strict: combinefn.as_ref().is_some_and(|row| row.proisstrict),
        finalfn_oid: finalfn.as_ref().map(|row| row.oid),
        finalfn_strict: finalfn.as_ref().is_some_and(|row| row.proisstrict),
        mtransfn_oid: mtransfn.as_ref().map(|row| row.oid),
        mtransfn_strict: mtransfn.as_ref().is_some_and(|row| row.proisstrict),
        minvtransfn_oid: minvtransfn.as_ref().map(|row| row.oid),
        minvtransfn_strict: minvtransfn.as_ref().is_some_and(|row| row.proisstrict),
        mfinalfn_oid: mfinalfn.as_ref().map(|row| row.oid),
        mfinalfn_strict: mfinalfn.as_ref().is_some_and(|row| row.proisstrict),
        transtype,
        mtranstype,
        transfn_arg_types,
        combinefn_arg_types,
        finalfn_arg_types,
        mtransfn_arg_types,
        minvtransfn_arg_types,
        mfinalfn_arg_types,
        init_value,
        minit_value,
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
    execute_scalar_function_value_call_with_arg_types(proc_oid, arg_values, None, ctx)
}

pub(crate) fn execute_scalar_function_value_call_with_arg_types(
    proc_oid: u32,
    arg_values: &[Value],
    arg_types: Option<&[crate::backend::parser::SqlType]>,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    if let Some(func) = builtin_scalar_function_for_proc_oid(proc_oid) {
        match func {
            BuiltinScalarFunction::PgDescribeObject => {
                return eval_pg_describe_object(arg_values, ctx);
            }
            BuiltinScalarFunction::PgIdentifyObject => {
                return eval_pg_identify_object(arg_values, ctx);
            }
            BuiltinScalarFunction::PgIdentifyObjectAsAddress => {
                return eval_pg_identify_object_as_address(arg_values, ctx);
            }
            BuiltinScalarFunction::PgGetObjectAddress => {
                return eval_pg_get_object_address(arg_values, ctx);
            }
            BuiltinScalarFunction::PgRelationIsUpdatable => {
                return eval_pg_relation_is_updatable(arg_values, ctx);
            }
            BuiltinScalarFunction::PgColumnIsUpdatable => {
                return eval_pg_column_is_updatable(arg_values, ctx);
            }
            _ => {}
        }
        if matches!(
            func,
            BuiltinScalarFunction::PgRestoreRelationStats
                | BuiltinScalarFunction::PgClearRelationStats
                | BuiltinScalarFunction::PgRestoreAttributeStats
                | BuiltinScalarFunction::PgClearAttributeStats
        ) {
            return execute_stats_import_value_call(func, arg_values, arg_types, ctx);
        }
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
    let normalized_variadic =
        normalize_variadic_scalar_function_args(&row, arg_values, arg_types, |ty| {
            catalog.type_oid_for_sql_type(ty)
        });
    let call_arg_values = normalized_variadic.values.as_deref().unwrap_or(arg_values);
    let call_arg_types = normalized_variadic.arg_types.as_deref().or(arg_types);
    match row.prolang {
        PG_LANGUAGE_SQL_OID => match call_arg_types {
            Some(arg_types) => {
                let arg_type_oids = arg_types
                    .iter()
                    .map(|ty| catalog.type_oid_for_sql_type(*ty).unwrap_or(0))
                    .collect::<Vec<_>>();
                execute_user_defined_sql_scalar_function_values_with_arg_type_oids(
                    &row,
                    call_arg_values,
                    Some(&arg_type_oids),
                    ctx,
                )
            }
            None => execute_user_defined_sql_scalar_function_values(&row, call_arg_values, ctx),
        },
        _ => match call_arg_types {
            Some(arg_types) => execute_user_defined_scalar_function_values_with_arg_types(
                proc_oid,
                call_arg_values,
                arg_types,
                ctx,
            ),
            None => execute_user_defined_scalar_function_values(proc_oid, call_arg_values, ctx),
        },
    }
}

fn execute_stats_import_value_call(
    func: BuiltinScalarFunction,
    arg_values: &[Value],
    arg_types: Option<&[SqlType]>,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    ensure_builtin_side_effects_allowed(func, ctx)?;
    let runtime = ctx
        .stats_import_runtime
        .clone()
        .ok_or_else(|| ExecError::DetailedError {
            message: format!("{func:?} requires database executor context"),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        })?;
    let typed_args = arg_values
        .iter()
        .enumerate()
        .map(|(idx, value)| TypedFunctionArg {
            value: value.clone(),
            sql_type: arg_types.and_then(|types| types.get(idx)).copied(),
        })
        .collect::<Vec<_>>();
    match func {
        BuiltinScalarFunction::PgRestoreRelationStats => {
            runtime.pg_restore_relation_stats(ctx, typed_args)
        }
        BuiltinScalarFunction::PgClearRelationStats => {
            let [schemaname, relname] = arg_values else {
                return malformed_aggregate_support_call("pg_clear_relation_stats");
            };
            runtime.pg_clear_relation_stats(ctx, schemaname.clone(), relname.clone())
        }
        BuiltinScalarFunction::PgRestoreAttributeStats => {
            runtime.pg_restore_attribute_stats(ctx, typed_args)
        }
        BuiltinScalarFunction::PgClearAttributeStats => {
            let [schemaname, relname, attname, inherited] = arg_values else {
                return malformed_aggregate_support_call("pg_clear_attribute_stats");
            };
            runtime.pg_clear_attribute_stats(
                ctx,
                schemaname.clone(),
                relname.clone(),
                attname.clone(),
                inherited.clone(),
            )
        }
        _ => unreachable!(),
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
        BuiltinScalarFunction::Int4Smaller => match arg_values {
            [Value::Int32(left), Value::Int32(right)] => Ok(Value::Int32((*left).min(*right))),
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            _ => malformed_aggregate_support_call("int4smaller"),
        },
        BuiltinScalarFunction::Int4Sum => match arg_values {
            [Value::Int64(state), Value::Int32(value)] => state
                .checked_add(i64::from(*value))
                .map(Value::Int64)
                .ok_or(ExecError::IntegerOutOfRange {
                    ty: "bigint",
                    value: state.to_string(),
                }),
            [Value::Null, Value::Int32(value)] => Ok(Value::Int64(i64::from(*value))),
            [state, Value::Null] => Ok(state.clone()),
            _ => malformed_aggregate_support_call("int4_sum"),
        },
        BuiltinScalarFunction::Int8Inc => match arg_values {
            [state] => add_values(state.clone(), Value::Int64(1)),
            _ => malformed_aggregate_support_call("int8inc"),
        },
        BuiltinScalarFunction::Int8IncAny => match arg_values {
            [state, _] => add_values(state.clone(), Value::Int64(1)),
            _ => malformed_aggregate_support_call("int8inc_any"),
        },
        BuiltinScalarFunction::ArrayAppend => match arg_values {
            [array, element] => append_array_value(array, element, false),
            _ => malformed_aggregate_support_call("array_append"),
        },
        BuiltinScalarFunction::Abs => {
            pgrust_expr::expr_math::eval_abs_function(arg_values).map_err(Into::into)
        }
        BuiltinScalarFunction::ArrayLarger => match arg_values {
            [left, right] => {
                if matches!(left, Value::Null) || matches!(right, Value::Null) {
                    Ok(Value::Null)
                } else if compare_order_values(left, right, None, None, false)? == Ordering::Less {
                    Ok(right.clone())
                } else {
                    Ok(left.clone())
                }
            }
            _ => malformed_aggregate_support_call("array_larger"),
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
        BuiltinScalarFunction::QuoteNullable => eval_quote_nullable_function(arg_values),
        BuiltinScalarFunction::HashValue(kind) => {
            execute_builtin_hash_value_call(kind, false, arg_values)
        }
        BuiltinScalarFunction::HashValueExtended(kind) => {
            execute_builtin_hash_value_call(kind, true, arg_values)
        }
        BuiltinScalarFunction::ParseIdent => eval_parse_ident_function(arg_values),
        BuiltinScalarFunction::PgSettingsGetFlags => eval_pg_settings_get_flags(arg_values),
        BuiltinScalarFunction::PgRustTestEncConversion => {
            eval_pg_rust_test_enc_conversion(arg_values)
        }
        BuiltinScalarFunction::TsVectorIn => match arg_values {
            [Value::Null] | [Value::Null, _, _] => Ok(Value::Null),
            [_] | [_, _, _] => {
                let Some(text) = arg_values[0].as_text() else {
                    return Err(ExecError::TypeMismatch {
                        op: "tsvectorin",
                        left: arg_values[0].clone(),
                        right: Value::Text("".into()),
                    });
                };
                crate::backend::executor::parse_tsvector_text(text).map(Value::TsVector)
            }
            _ => malformed_aggregate_support_call("tsvectorin"),
        },
        BuiltinScalarFunction::TsVectorOut => match arg_values {
            [Value::Null] => Ok(Value::Null),
            [Value::TsVector(vector)] => Ok(Value::Text(
                crate::backend::executor::render_tsvector_text(vector).into(),
            )),
            [other] => Err(ExecError::TypeMismatch {
                op: "tsvectorout",
                left: other.clone(),
                right: Value::Null,
            }),
            _ => malformed_aggregate_support_call("tsvectorout"),
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

fn execute_builtin_hash_value_call(
    kind: HashFunctionKind,
    extended: bool,
    arg_values: &[Value],
) -> Result<Value, ExecError> {
    let opclass = (kind == HashFunctionKind::BpChar).then_some(BPCHAR_HASH_OPCLASS_OID);
    if extended {
        let [value, seed] = arg_values else {
            return malformed_aggregate_support_call("hash_extended");
        };
        if matches!(value, Value::Null) || matches!(seed, Value::Null) {
            return Ok(Value::Null);
        }
        let seed = match seed {
            Value::Int16(seed) => i64::from(*seed),
            Value::Int32(seed) => i64::from(*seed),
            Value::Int64(seed) => *seed,
            _ => {
                return Err(ExecError::TypeMismatch {
                    op: "hash_extended",
                    left: value.clone(),
                    right: seed.clone(),
                });
            }
        };
        let hash = crate::backend::access::hash::hash_value_extended(value, opclass, seed as u64)
            .map_err(|message| hash_function_error(message, true))?
            .unwrap_or(0);
        return Ok(Value::Int64(hash as i64));
    }

    let [value] = arg_values else {
        return malformed_aggregate_support_call("hash");
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let hash = crate::backend::access::hash::hash_value_extended(value, opclass, 0)
        .map_err(|message| hash_function_error(message, false))?
        .unwrap_or(0);
    Ok(Value::Int32(hash as u32 as i32))
}

fn hash_function_error(message: String, extended: bool) -> ExecError {
    let message = if extended {
        message.replacen(
            "could not identify a hash function",
            "could not identify an extended hash function",
            1,
        )
    } else {
        message
    };
    ExecError::DetailedError {
        message,
        detail: None,
        hint: None,
        sqlstate: "42883",
    }
}

pub(crate) fn aggregate_support_error(error: AggregateSupportError) -> ExecError {
    match error {
        AggregateSupportError::TypeMismatch { op, left, right } => {
            ExecError::TypeMismatch { op, left, right }
        }
        AggregateSupportError::InvalidInt8PairState { func, state } => ExecError::DetailedError {
            message: format!("{func} expected a 2-element int8 array state"),
            detail: Some(format!("state = {state:?}")),
            hint: None,
            sqlstate: "22023",
        },
        AggregateSupportError::CannotAccumulateNullArrays => ExecError::DetailedError {
            message: "cannot accumulate null arrays".into(),
            detail: None,
            hint: None,
            sqlstate: "22004",
        },
        AggregateSupportError::CannotAccumulateEmptyArrays => ExecError::DetailedError {
            message: "cannot accumulate empty arrays".into(),
            detail: None,
            hint: None,
            sqlstate: "2202E",
        },
        AggregateSupportError::ArrayDimensionalityMismatch => ExecError::DetailedError {
            message: "cannot accumulate arrays of different dimensionality".into(),
            detail: None,
            hint: None,
            sqlstate: "2202E",
        },
        AggregateSupportError::InvalidFloat8TransitionCall {
            expected,
            actual_args,
        } => ExecError::Parse(ParseError::UnexpectedToken {
            expected,
            actual: format!("{actual_args} args"),
        }),
        AggregateSupportError::InvalidFloat8TransitionState { op, expected_len } => {
            ExecError::DetailedError {
                message: format!(
                    "{op} requires a float8[] transition state of length {expected_len}"
                ),
                detail: None,
                hint: None,
                sqlstate: "22023",
            }
        }
        AggregateSupportError::Float8Overflow => ExecError::DetailedError {
            message: "value out of range: overflow".into(),
            detail: None,
            hint: None,
            sqlstate: "22003",
        },
    }
}

fn aggregate_int8_pair(value: &Value, func: &'static str) -> Result<(i64, i64), ExecError> {
    pgrust_executor::aggregate_int8_pair(value, func).map_err(aggregate_support_error)
}

fn malformed_aggregate_support_call(name: &'static str) -> Result<Value, ExecError> {
    Err(ExecError::DetailedError {
        message: format!("malformed aggregate support call to {name}"),
        detail: None,
        hint: None,
        sqlstate: "XX000",
    })
}
