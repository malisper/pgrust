use std::time::{SystemTime, UNIX_EPOCH};

use num_bigint::{BigInt, Sign};
use rand::{Rng, RngCore};

use super::expr_bit::{
    bit_count as eval_bit_count, bit_length as eval_bit_length, get_bit as eval_get_bit,
    overlay as eval_bit_overlay, position as eval_bit_position, set_bit as eval_set_bit,
    substring as eval_bit_substring,
};
use super::expr_bool::{eval_booleq, eval_boolne};
use super::expr_casts::{cast_value, cast_value_with_config, soft_input_error_info_with_config};
pub(crate) use super::expr_compile::{
    CompiledPredicate, compile_predicate, compile_predicate_with_decoder,
};
use super::expr_date::{
    eval_date_part_function, eval_date_trunc_function, eval_isfinite_function,
    eval_make_date_function,
};
use super::expr_datetime::{
    current_date_value, current_date_value_with_config, current_time_value,
    current_time_value_with_config, current_timestamp_value, current_timestamp_value_with_config,
    render_datetime_value_text_with_config,
};
use super::expr_geometry::eval_geometry_function;
use super::expr_json::{
    eval_json_builtin_function, eval_json_get, eval_json_path, eval_json_record_builtin_function,
    eval_jsonpath_operator,
};
use super::expr_math::{
    cosd, cotd, eval_abs_function, eval_acosd, eval_acosh, eval_asind, eval_atanh,
    eval_binary_float_function, eval_bitcast_bigint_to_float8, eval_bitcast_integer_to_float4,
    eval_erf, eval_erfc, eval_float_send_function, eval_gamma, eval_gcd_function,
    eval_lcm_function, eval_lgamma, eval_unary_float_function, sind, snap_degree, tand,
};
use super::expr_money::{cash_words_text, money_larger, money_smaller};
use super::expr_numeric::{
    eval_ceil_function, eval_div_function, eval_exp_function, eval_factorial_function,
    eval_floor_function, eval_ln_function, eval_log_function, eval_log10_function,
    eval_min_scale_function, eval_numeric_inc_function, eval_pg_lsn_function, eval_power_function,
    eval_round_function, eval_scale_function, eval_sign_function, eval_sqrt_function,
    eval_trim_scale_function, eval_trunc_function, eval_width_bucket_function,
};
use super::expr_ops::compare_order_values;
use super::expr_ops::{
    add_values, bitwise_and_values, bitwise_not_value, bitwise_or_values, bitwise_xor_values,
    compare_values, concat_values, div_values, eval_and, eval_or, mod_values, mul_values,
    negate_value, not_equal_values, order_values, shift_left_values, shift_right_values,
    sub_values, values_are_distinct,
};
pub(crate) use super::expr_ops::{compare_order_by_keys, parse_numeric_text};
use super::expr_range::eval_range_function;
use super::expr_string::{
    eval_ascii_function, eval_bit_count_bytes, eval_bpchar_to_text_function, eval_bytea_overlay,
    eval_bytea_position_function, eval_bytea_substring, eval_chr_function, eval_concat_function,
    eval_concat_ws_function, eval_convert_from_function, eval_crc32_function, eval_crc32c_function,
    eval_decode_function, eval_encode_function, eval_format_function, eval_get_bit_bytes,
    eval_get_byte, eval_initcap_function, eval_left_function, eval_length_function, eval_like,
    eval_lower_function, eval_lpad_function, eval_md5_function, eval_pg_rust_test_enc_conversion,
    eval_pg_rust_test_enc_setup, eval_position_function, eval_quote_literal_function,
    eval_repeat_function, eval_replace_function, eval_reverse_function, eval_right_function,
    eval_rpad_function, eval_set_bit_bytes, eval_set_byte, eval_sha224_function,
    eval_sha256_function, eval_sha384_function, eval_sha512_function, eval_split_part_function,
    eval_strpos_function, eval_text_overlay, eval_text_substring, eval_to_bin_function,
    eval_to_char_function, eval_to_hex_function, eval_to_number_function, eval_to_oct_function,
    eval_translate_function, eval_trim_function, eval_unistr_function,
};
use super::node_types::*;
use super::pg_regex::{
    eval_regex_match_operator, eval_regexp_count, eval_regexp_instr, eval_regexp_like,
    eval_regexp_match, eval_regexp_replace, eval_regexp_split_to_array, eval_regexp_substr,
    eval_similar, eval_similar_substring, eval_sql_regex_substring,
};
pub(crate) use super::value_io::{format_array_text, format_array_value_text};
use super::{ExecError, ExecutorContext, exec_next, executor_start};
use crate::backend::executor::jsonb::{
    JsonbValue, jsonb_contains, jsonb_exists, jsonb_exists_all, jsonb_exists_any, jsonb_from_value,
};
use crate::backend::executor::sqlfunc::execute_user_defined_sql_scalar_function;
use crate::backend::parser::analyze::is_binary_coercible_type;
use crate::backend::parser::{
    CatalogLookup, ParseError, SqlType, SqlTypeKind, SubqueryComparisonOp,
};
use crate::backend::utils::misc::checkpoint::checkpoint_stats_value;
use crate::include::catalog::builtin_scalar_function_for_proc_oid;
use crate::include::nodes::datum::{ArrayDimension, ArrayValue, NumericValue};
use crate::include::nodes::primnodes::{
    BoolExpr, BoolExprType, FuncExpr, INDEX_VAR, INNER_VAR, OUTER_VAR, OpExpr, OpExprKind,
    SELF_ITEM_POINTER_ATTR_NO, ScalarArrayOpExpr, ScalarFunctionImpl, SubLinkType,
    TABLE_OID_ATTR_NO, attrno_index,
};
use crate::pgrust::compact_string::CompactString;
use crate::pl::plpgsql::execute_user_defined_scalar_function;

mod arrays;
mod subquery;

use arrays::{
    eval_array_dims_function, eval_array_fill_function, eval_array_length_function,
    eval_array_lower_function, eval_array_ndims_function, eval_array_overlap,
    eval_array_position_function, eval_array_positions_function, eval_array_remove_function,
    eval_array_replace_function, eval_array_sort_function, eval_array_subscript,
    eval_array_subscript_plpgsql, eval_array_to_string_function, eval_cardinality_function,
    eval_quantified_array, eval_string_to_array_function, eval_width_bucket_thresholds,
};
use subquery::{
    eval_array_subquery, eval_exists_subquery, eval_quantified_subquery, eval_scalar_subquery,
};

extern crate rand;

const INVALID_PARAMETER_VALUE_SQLSTATE: &str = "22023";

fn malformed_expr_error(kind: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("malformed {kind} expression").into(),
        detail: None,
        hint: None,
        sqlstate: "XX000",
    }
}

fn sql_type_from_builtin_oid(oid: u32) -> Option<SqlType> {
    crate::include::catalog::builtin_type_rows()
        .iter()
        .find(|row| row.oid == oid && row.typrelid == 0)
        .map(|row| row.sql_type)
}

fn stats_oid_arg(values: &[Value], op: &'static str) -> Result<u32, ExecError> {
    match values.first() {
        Some(Value::Int32(v)) if *v >= 0 => Ok(*v as u32),
        Some(Value::Int64(v)) if *v >= 0 && *v <= i64::from(u32::MAX) => Ok(*v as u32),
        Some(other) => Err(ExecError::TypeMismatch {
            op,
            left: other.clone(),
            right: Value::Int64(i64::from(crate::include::catalog::OID_TYPE_OID)),
        }),
        None => Err(malformed_expr_error(op)),
    }
}

fn relation_stats_value(
    func: BuiltinScalarFunction,
    oid: u32,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let entry = ctx
        .session_stats
        .write()
        .visible_relation_entry(&ctx.stats, oid)
        .unwrap_or_default();
    Ok(match func {
        BuiltinScalarFunction::PgStatGetNumscans => Value::Int64(entry.numscans),
        BuiltinScalarFunction::PgStatGetLastscan => entry
            .lastscan
            .map(Value::TimestampTz)
            .unwrap_or(Value::Null),
        BuiltinScalarFunction::PgStatGetTuplesReturned => Value::Int64(entry.tuples_returned),
        BuiltinScalarFunction::PgStatGetTuplesFetched => Value::Int64(entry.tuples_fetched),
        BuiltinScalarFunction::PgStatGetTuplesInserted => Value::Int64(entry.tuples_inserted),
        BuiltinScalarFunction::PgStatGetTuplesUpdated => Value::Int64(entry.tuples_updated),
        BuiltinScalarFunction::PgStatGetTuplesDeleted => Value::Int64(entry.tuples_deleted),
        BuiltinScalarFunction::PgStatGetLiveTuples => Value::Int64(entry.live_tuples),
        BuiltinScalarFunction::PgStatGetDeadTuples => Value::Int64(entry.dead_tuples),
        BuiltinScalarFunction::PgStatGetBlocksFetched => Value::Int64(entry.blocks_fetched),
        BuiltinScalarFunction::PgStatGetBlocksHit => Value::Int64(entry.blocks_hit),
        _ => unreachable!("non-relation stats builtin in relation_stats_value"),
    })
}

fn relation_xact_stats_value(
    func: BuiltinScalarFunction,
    oid: u32,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let session = ctx.session_stats.read();
    if session.dropped_relations_in_xact.contains(&oid) {
        return Ok(Value::Int64(0));
    }
    let current = session
        .relation_xact
        .get(&oid)
        .map(|entry| &entry.current)
        .cloned()
        .unwrap_or_default();
    Ok(match func {
        BuiltinScalarFunction::PgStatGetXactNumscans => Value::Int64(current.numscans),
        BuiltinScalarFunction::PgStatGetXactTuplesReturned => Value::Int64(current.tuples_returned),
        BuiltinScalarFunction::PgStatGetXactTuplesFetched => Value::Int64(current.tuples_fetched),
        BuiltinScalarFunction::PgStatGetXactTuplesInserted => Value::Int64(current.tuples_inserted),
        BuiltinScalarFunction::PgStatGetXactTuplesUpdated => Value::Int64(current.tuples_updated),
        BuiltinScalarFunction::PgStatGetXactTuplesDeleted => Value::Int64(current.tuples_deleted),
        _ => unreachable!("non-xact relation stats builtin in relation_xact_stats_value"),
    })
}

fn function_stats_value(
    func: BuiltinScalarFunction,
    oid: u32,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let Some(entry) = ctx
        .session_stats
        .write()
        .visible_function_entry(&ctx.stats, oid)
    else {
        return Ok(Value::Null);
    };
    Ok(match func {
        BuiltinScalarFunction::PgStatGetFunctionCalls => Value::Int64(entry.calls),
        BuiltinScalarFunction::PgStatGetFunctionTotalTime => {
            Value::Float64(entry.total_time_micros as f64 / 1000.0)
        }
        BuiltinScalarFunction::PgStatGetFunctionSelfTime => {
            Value::Float64(entry.self_time_micros as f64 / 1000.0)
        }
        _ => unreachable!("non-function stats builtin in function_stats_value"),
    })
}

fn function_xact_stats_value(
    func: BuiltinScalarFunction,
    oid: u32,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let session = ctx.session_stats.read();
    if session.dropped_functions_in_xact.contains(&oid) {
        return Ok(Value::Null);
    }
    let Some(entry) = session.function_xact.get(&oid) else {
        return Ok(Value::Null);
    };
    Ok(match func {
        BuiltinScalarFunction::PgStatGetXactFunctionCalls => Value::Int64(entry.calls),
        BuiltinScalarFunction::PgStatGetXactFunctionTotalTime => {
            Value::Float64(entry.total_time_micros as f64 / 1000.0)
        }
        BuiltinScalarFunction::PgStatGetXactFunctionSelfTime => {
            Value::Float64(entry.self_time_micros as f64 / 1000.0)
        }
        _ => unreachable!("non-xact function stats builtin in function_xact_stats_value"),
    })
}

fn oid_arg_to_u32(value: &Value, op: &'static str) -> Result<u32, ExecError> {
    match value {
        Value::Int32(oid) => u32::try_from(*oid).map_err(|_| ExecError::OidOutOfRange),
        Value::Int64(oid) => u32::try_from(*oid).map_err(|_| ExecError::OidOutOfRange),
        _ => Err(ExecError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::Int64(i64::from(crate::include::catalog::OID_TYPE_OID)),
        }),
    }
}

fn eval_pg_rust_internal_binary_coercible(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [left, right] => {
            let left_oid = oid_arg_to_u32(left, "pg_rust_internal_binary_coercible")?;
            let right_oid = oid_arg_to_u32(right, "pg_rust_internal_binary_coercible")?;
            let left_type =
                sql_type_from_builtin_oid(left_oid).ok_or_else(|| ExecError::DetailedError {
                    message: format!("type with OID {left_oid} does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                })?;
            let right_type =
                sql_type_from_builtin_oid(right_oid).ok_or_else(|| ExecError::DetailedError {
                    message: format!("type with OID {right_oid} does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                })?;
            Ok(Value::Bool(is_binary_coercible_type(left_type, right_type)))
        }
        _ => Err(ExecError::TypeMismatch {
            op: "pg_rust_internal_binary_coercible",
            left: values.first().cloned().unwrap_or(Value::Null),
            right: values.get(1).cloned().unwrap_or(Value::Null),
        }),
    }
}

fn lookup_system_binding(
    bindings: &[crate::include::nodes::execnodes::SystemVarBinding],
    varno: usize,
) -> Result<Value, ExecError> {
    bindings
        .iter()
        .find(|binding| binding.varno == varno)
        .map(|binding| Value::Int64(i64::from(binding.table_oid)))
        .ok_or(ExecError::DetailedError {
            message: "tableoid is not available for this row".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })
}

fn lookup_ctid(slot: &TupleSlot) -> Result<Value, ExecError> {
    slot.tid()
        .map(|tid| {
            Value::Text(CompactString::from_owned(format!(
                "({},{})",
                tid.block_number, tid.offset_number
            )))
        })
        .ok_or(ExecError::DetailedError {
            message: "ctid is not available for this row".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })
}

fn builtin_function_for_expr(funcid: u32) -> Result<BuiltinScalarFunction, ExecError> {
    builtin_scalar_function_for_proc_oid(funcid).ok_or_else(|| ExecError::DetailedError {
        message: format!("no builtin implementation for function oid {funcid}").into(),
        detail: None,
        hint: None,
        sqlstate: "XX000",
    })
}

fn ensure_builtin_side_effects_allowed(
    func: BuiltinScalarFunction,
    ctx: &ExecutorContext,
) -> Result<(), ExecError> {
    if matches!(
        func,
        BuiltinScalarFunction::NextVal
            | BuiltinScalarFunction::SetVal
            | BuiltinScalarFunction::LoCreate
            | BuiltinScalarFunction::LoUnlink
    ) && !ctx.allow_side_effects
    {
        return Err(ExecError::DetailedError {
            message: format!(
                "{} is not allowed in a read-only execution context",
                match func {
                    BuiltinScalarFunction::NextVal => "nextval",
                    BuiltinScalarFunction::SetVal => "setval",
                    BuiltinScalarFunction::LoCreate => "lo_create",
                    BuiltinScalarFunction::LoUnlink => "lo_unlink",
                    _ => unreachable!(),
                }
            ),
            detail: None,
            hint: None,
            sqlstate: "25006",
        });
    }
    Ok(())
}

fn sequence_catalog(
    ctx: &ExecutorContext,
) -> Result<&crate::backend::utils::cache::visible_catalog::VisibleCatalog, ExecError> {
    ctx.catalog
        .as_ref()
        .ok_or_else(|| ExecError::DetailedError {
            message: "sequence lookup requires a visible catalog".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })
}

fn sequence_runtime(
    ctx: &ExecutorContext,
) -> Result<&crate::pgrust::database::SequenceRuntime, ExecError> {
    ctx.sequences
        .as_deref()
        .ok_or_else(|| ExecError::DetailedError {
            message: "sequence runtime is not available".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })
}

fn sequence_name_for_oid(
    catalog: &crate::backend::utils::cache::visible_catalog::VisibleCatalog,
    relation_oid: u32,
) -> Option<String> {
    catalog
        .relcache()
        .entries()
        .find(|(_, entry)| entry.relation_oid == relation_oid)
        .map(|(name, _)| name.to_string())
}

fn large_object_runtime(
    ctx: &ExecutorContext,
) -> Result<&crate::pgrust::database::LargeObjectRuntime, ExecError> {
    ctx.large_objects
        .as_deref()
        .ok_or_else(|| ExecError::DetailedError {
            message: "large object runtime is not available".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })
}

fn resolve_sequence_call_target(
    ctx: &ExecutorContext,
    value: &Value,
) -> Result<(u32, bool), ExecError> {
    let catalog = sequence_catalog(ctx)?;
    let relation = match value {
        Value::Int32(oid) => {
            let oid = u32::try_from(*oid).map_err(|_| ExecError::OidOutOfRange)?;
            catalog
                .relation_by_oid(oid)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("sequence with OID {oid} does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "42P01",
                })?
        }
        Value::Int64(oid) => {
            let oid = u32::try_from(*oid).map_err(|_| ExecError::OidOutOfRange)?;
            catalog
                .relation_by_oid(oid)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("sequence with OID {oid} does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "42P01",
                })?
        }
        Value::Text(_) | Value::TextRef(_, _) => {
            let name = value.as_text().expect("text value");
            catalog
                .lookup_any_relation(name)
                .ok_or_else(|| ExecError::Parse(ParseError::TableDoesNotExist(name.to_string())))?
        }
        other => {
            return Err(ExecError::TypeMismatch {
                op: "sequence function",
                left: other.clone(),
                right: Value::Text("sequence".into()),
            });
        }
    };
    if relation.relkind != 'S' {
        return Err(ExecError::Parse(ParseError::WrongObjectType {
            name: sequence_name_for_oid(catalog, relation.relation_oid)
                .unwrap_or_else(|| relation.relation_oid.to_string()),
            expected: "sequence",
        }));
    }
    Ok((relation.relation_oid, relation.relpersistence != 't'))
}

fn eval_sequence_builtin_function(
    func: BuiltinScalarFunction,
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    match (func, values) {
        (_, [Value::Null, ..]) | (_, [_, Value::Null, ..]) | (_, [_, _, Value::Null]) => {
            Ok(Value::Null)
        }
        (BuiltinScalarFunction::NextVal, [target]) => {
            let (relation_oid, persistent) = resolve_sequence_call_target(ctx, target)?;
            let value =
                sequence_runtime(ctx)?.next_value(ctx.client_id, relation_oid, persistent)?;
            Ok(Value::Int64(value))
        }
        (BuiltinScalarFunction::CurrVal, [target]) => {
            let (relation_oid, _) = resolve_sequence_call_target(ctx, target)?;
            let value = sequence_runtime(ctx)?.curr_value(ctx.client_id, relation_oid)?;
            Ok(Value::Int64(value))
        }
        (BuiltinScalarFunction::SetVal, [target, Value::Int64(value)]) => {
            let (relation_oid, persistent) = resolve_sequence_call_target(ctx, target)?;
            let value = sequence_runtime(ctx)?.set_value(
                ctx.client_id,
                relation_oid,
                *value,
                true,
                persistent,
            )?;
            Ok(Value::Int64(value))
        }
        (BuiltinScalarFunction::SetVal, [target, Value::Int32(value)]) => {
            let (relation_oid, persistent) = resolve_sequence_call_target(ctx, target)?;
            let value = sequence_runtime(ctx)?.set_value(
                ctx.client_id,
                relation_oid,
                i64::from(*value),
                true,
                persistent,
            )?;
            Ok(Value::Int64(value))
        }
        (BuiltinScalarFunction::SetVal, [target, Value::Int64(value), Value::Bool(is_called)]) => {
            let (relation_oid, persistent) = resolve_sequence_call_target(ctx, target)?;
            let value = sequence_runtime(ctx)?.set_value(
                ctx.client_id,
                relation_oid,
                *value,
                *is_called,
                persistent,
            )?;
            Ok(Value::Int64(value))
        }
        (BuiltinScalarFunction::SetVal, [target, Value::Int32(value), Value::Bool(is_called)]) => {
            let (relation_oid, persistent) = resolve_sequence_call_target(ctx, target)?;
            let value = sequence_runtime(ctx)?.set_value(
                ctx.client_id,
                relation_oid,
                i64::from(*value),
                *is_called,
                persistent,
            )?;
            Ok(Value::Int64(value))
        }
        (BuiltinScalarFunction::PgGetSerialSequence, [table, column]) => {
            let table_name = table.as_text().ok_or_else(|| ExecError::TypeMismatch {
                op: "pg_get_serial_sequence",
                left: table.clone(),
                right: Value::Text("".into()),
            })?;
            let column_name = column.as_text().ok_or_else(|| ExecError::TypeMismatch {
                op: "pg_get_serial_sequence",
                left: column.clone(),
                right: Value::Text("".into()),
            })?;
            let catalog = sequence_catalog(ctx)?;
            let relation = catalog.lookup_relation(table_name).ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(table_name.to_string()))
            })?;
            let Some(column) = relation.desc.columns.iter().find(|candidate| {
                !candidate.dropped && candidate.name.eq_ignore_ascii_case(column_name)
            }) else {
                return Err(ExecError::Parse(ParseError::UnknownColumn(
                    column_name.to_string(),
                )));
            };
            let Some(sequence_oid) = column.default_sequence_oid else {
                return Ok(Value::Null);
            };
            Ok(sequence_name_for_oid(catalog, sequence_oid)
                .map(Into::into)
                .map(Value::Text)
                .unwrap_or(Value::Null))
        }
        (BuiltinScalarFunction::SetVal, [target, other]) => Err(ExecError::TypeMismatch {
            op: "setval",
            left: target.clone(),
            right: other.clone(),
        }),
        (BuiltinScalarFunction::SetVal, [target, other, _]) => Err(ExecError::TypeMismatch {
            op: "setval",
            left: target.clone(),
            right: other.clone(),
        }),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "valid sequence builtin call",
            actual: format!("{func:?}"),
        })),
    }
}

fn eval_large_object_builtin_function(
    func: BuiltinScalarFunction,
    values: &[Value],
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    match (func, values) {
        (_, [Value::Null]) => Ok(Value::Null),
        (BuiltinScalarFunction::LoCreate, [value]) => {
            let oid = oid_arg_to_u32(value, "lo_create")?;
            Ok(Value::Int64(i64::from(
                large_object_runtime(ctx)?.create(oid, ctx.current_user_oid)?,
            )))
        }
        (BuiltinScalarFunction::LoUnlink, [value]) => {
            let oid = oid_arg_to_u32(value, "lo_unlink")?;
            Ok(Value::Int32(large_object_runtime(ctx)?.unlink(oid)?))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "valid large object builtin call",
            actual: format!("{func:?}"),
        })),
    }
}

fn eval_op_expr(
    op: &OpExpr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    match (op.op, op.args.as_slice()) {
        (OpExprKind::UnaryPlus, [inner]) => eval_expr(inner, slot, ctx),
        (OpExprKind::Negate, [inner]) => negate_value(eval_expr(inner, slot, ctx)?),
        (OpExprKind::BitNot, [inner]) => bitwise_not_value(eval_expr(inner, slot, ctx)?),
        (OpExprKind::Add, [left, right]) => {
            add_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::Sub, [left, right]) => {
            sub_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::BitAnd, [left, right]) => {
            bitwise_and_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::BitOr, [left, right]) => {
            bitwise_or_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::BitXor, [left, right]) => {
            bitwise_xor_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::Shl, [left, right]) => {
            shift_left_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::Shr, [left, right]) => {
            shift_right_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::Mul, [left, right]) => {
            mul_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::Div, [left, right]) => {
            div_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::Mod, [left, right]) => {
            mod_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::Concat, [left, right]) => {
            concat_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::Eq, [left, right]) => compare_values(
            "=",
            eval_expr(left, slot, ctx)?,
            eval_expr(right, slot, ctx)?,
        ),
        (OpExprKind::NotEq, [left, right]) => {
            not_equal_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::Lt, [left, right]) => order_values(
            "<",
            eval_expr(left, slot, ctx)?,
            eval_expr(right, slot, ctx)?,
        ),
        (OpExprKind::LtEq, [left, right]) => order_values(
            "<=",
            eval_expr(left, slot, ctx)?,
            eval_expr(right, slot, ctx)?,
        ),
        (OpExprKind::Gt, [left, right]) => order_values(
            ">",
            eval_expr(left, slot, ctx)?,
            eval_expr(right, slot, ctx)?,
        ),
        (OpExprKind::GtEq, [left, right]) => order_values(
            ">=",
            eval_expr(left, slot, ctx)?,
            eval_expr(right, slot, ctx)?,
        ),
        (OpExprKind::RegexMatch, [left, right]) => {
            let text = eval_expr(left, slot, ctx)?;
            let pattern = eval_expr(right, slot, ctx)?;
            eval_regex_match_operator(&text, &pattern)
        }
        (OpExprKind::ArrayOverlap, [left, right]) => {
            eval_array_overlap(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::JsonbContains, [left, right]) => {
            eval_jsonb_contains(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::JsonbContained, [left, right]) => {
            eval_jsonb_contained(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::JsonbExists, [left, right]) => {
            eval_jsonb_exists(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::JsonbExistsAny, [left, right]) => {
            eval_jsonb_exists_any(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::JsonbExistsAll, [left, right]) => {
            eval_jsonb_exists_all(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        (OpExprKind::JsonbPathExists, [left, right]) => {
            eval_jsonpath_operator(left, right, false, slot, ctx)
        }
        (OpExprKind::JsonbPathMatch, [left, right]) => {
            eval_jsonpath_operator(left, right, true, slot, ctx)
        }
        (OpExprKind::JsonGet, [left, right]) => eval_json_get(left, right, false, slot, ctx),
        (OpExprKind::JsonGetText, [left, right]) => eval_json_get(left, right, true, slot, ctx),
        (OpExprKind::JsonPath, [left, right]) => eval_json_path(left, right, false, slot, ctx),
        (OpExprKind::JsonPathText, [left, right]) => eval_json_path(left, right, true, slot, ctx),
        _ => Err(malformed_expr_error("operator")),
    }
}

fn eval_bool_expr(
    bool_expr: &BoolExpr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    match bool_expr.boolop {
        BoolExprType::And => {
            let mut result = Value::Bool(true);
            for arg in &bool_expr.args {
                result = eval_and(result, eval_expr(arg, slot, ctx)?)?;
            }
            Ok(result)
        }
        BoolExprType::Or => {
            let mut result = Value::Bool(false);
            for arg in &bool_expr.args {
                result = eval_or(result, eval_expr(arg, slot, ctx)?)?;
            }
            Ok(result)
        }
        BoolExprType::Not => match bool_expr.args.as_slice() {
            [inner] => match eval_expr(inner, slot, ctx)? {
                Value::Bool(value) => Ok(Value::Bool(!value)),
                Value::Null => Ok(Value::Null),
                other => Err(ExecError::NonBoolQual(other)),
            },
            _ => Err(malformed_expr_error("boolean")),
        },
    }
}

fn eval_case_expr(
    case_expr: &crate::include::nodes::primnodes::CaseExpr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let arg_value = match &case_expr.arg {
        Some(arg) => Some(eval_expr(arg, slot, ctx)?),
        None => None,
    };
    let eval_active =
        |slot: &mut TupleSlot, ctx: &mut ExecutorContext| -> Result<Value, ExecError> {
            for when in &case_expr.args {
                match eval_expr(&when.expr, slot, ctx)? {
                    Value::Bool(true) => return eval_expr(&when.result, slot, ctx),
                    Value::Bool(false) | Value::Null => {}
                    other => return Err(ExecError::NonBoolQual(other)),
                }
            }
            eval_expr(&case_expr.defresult, slot, ctx)
        };
    if let Some(arg_value) = arg_value {
        ctx.case_test_values.push(arg_value);
        let result = eval_active(slot, ctx);
        ctx.case_test_values.pop();
        result
    } else {
        eval_active(slot, ctx)
    }
}

fn eval_func_expr(
    func: &FuncExpr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    match func.implementation {
        ScalarFunctionImpl::Builtin(builtin) => eval_builtin_function(
            builtin,
            func.funcresulttype,
            &func.args,
            func.funcvariadic,
            slot,
            ctx,
        ),
        ScalarFunctionImpl::UserDefined { proc_oid } => {
            let catalog = ctx
                .catalog
                .as_ref()
                .ok_or_else(|| ExecError::DetailedError {
                    message: "user-defined functions require executor catalog context".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "0A000",
                })?;
            let row =
                catalog
                    .proc_row_by_oid(proc_oid)
                    .ok_or_else(|| ExecError::DetailedError {
                        message: format!("unknown function oid {proc_oid}"),
                        detail: None,
                        hint: None,
                        sqlstate: "42883",
                    })?;
            match row.prolang {
                crate::include::catalog::PG_LANGUAGE_SQL_OID => {
                    execute_user_defined_sql_scalar_function(&row, &func.args, slot, ctx)
                }
                _ => execute_user_defined_scalar_function(proc_oid, &func.args, slot, ctx),
            }
        }
    }
}

fn eval_scalar_array_op_expr(
    saop: &ScalarArrayOpExpr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let left_value = eval_expr(&saop.left, slot, ctx)?;
    let right_value = eval_expr(&saop.right, slot, ctx)?;
    eval_quantified_array(&left_value, saop.op, !saop.use_or, &right_value)
}

fn eval_bound_tuple_var(
    tuple: Option<&Vec<Value>>,
    var: &crate::include::nodes::primnodes::Var,
) -> Result<Value, ExecError> {
    let index = attrno_index(var.varattno).ok_or_else(|| ExecError::DetailedError {
        message: "special executor Var referenced an unsupported system attribute".into(),
        detail: Some(format!(
            "varno={}, varattno={}, varlevelsup={}",
            var.varno, var.varattno, var.varlevelsup
        )),
        hint: None,
        sqlstate: "XX000",
    })?;
    let row = tuple.ok_or_else(|| ExecError::DetailedError {
        message: "special executor Var referenced without a bound tuple".into(),
        detail: Some(format!(
            "varno={}, varattno={}, index={}",
            var.varno, var.varattno, index
        )),
        hint: None,
        sqlstate: "XX000",
    })?;
    row.get(index)
        .cloned()
        .ok_or_else(|| ExecError::DetailedError {
            message: "special executor Var referenced beyond the bound tuple width".into(),
            detail: Some(format!(
                "varno={}, varattno={}, index={}, tuple_width={}",
                var.varno,
                var.varattno,
                index,
                row.len()
            )),
            hint: None,
            sqlstate: "XX000",
        })
}

pub fn eval_expr(
    expr: &Expr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    match expr {
        Expr::Op(op) => eval_op_expr(op, slot, ctx),
        Expr::Bool(bool_expr) => eval_bool_expr(bool_expr, slot, ctx),
        Expr::Case(case_expr) => eval_case_expr(case_expr, slot, ctx),
        Expr::CaseTest(_) => ctx
            .case_test_values
            .last()
            .cloned()
            .ok_or_else(|| malformed_expr_error("CASE test")),
        Expr::Func(func) => eval_func_expr(func, slot, ctx),
        Expr::Aggref(_) => Err(ExecError::DetailedError {
            message: "aggregate reference reached executor outside aggregate lowering".into(),
            detail: Some("the planner should have lowered Aggref nodes to aggregate output references before execution".into()),
            hint: None,
            sqlstate: "XX000",
        }),
        Expr::WindowFunc(_) => Err(ExecError::DetailedError {
            message: "window function reached executor outside window lowering".into(),
            detail: Some(
                "the planner should have lowered WindowFunc nodes to window output references before execution"
                    .into(),
            ),
            hint: None,
            sqlstate: "XX000",
        }),
        Expr::ScalarArrayOp(saop) => eval_scalar_array_op_expr(saop, slot, ctx),
        Expr::SubLink(_) => Err(ExecError::DetailedError {
            message: "unplanned subquery reached executor".into(),
            detail: Some("the planner should have lowered SubLink nodes before execution".into()),
            hint: None,
            sqlstate: "XX000",
        }),
        Expr::Param(param) => ctx
            .expr_bindings
            .exec_params
            .get(&param.paramid)
            .cloned()
            .ok_or(ExecError::DetailedError {
                message: "executor param reached expression evaluation without a binding".into(),
                detail: Some(format!(
                    "paramkind={:?}, paramid={}, paramtype={:?}",
                    param.paramkind, param.paramid, param.paramtype
                )),
                hint: None,
                sqlstate: "XX000",
            }),
        Expr::Var(var) => {
            if var.varno == OUTER_VAR {
                eval_bound_tuple_var(ctx.expr_bindings.outer_tuple.as_ref(), var)
            } else if var.varno == INNER_VAR {
                eval_bound_tuple_var(ctx.expr_bindings.inner_tuple.as_ref(), var)
            } else if var.varno == INDEX_VAR {
                eval_bound_tuple_var(ctx.expr_bindings.index_tuple.as_ref(), var)
            } else if var.varlevelsup > 0 {
                Err(ExecError::DetailedError {
                    message: "unlowered outer Var reached executor".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                })
            } else if var.varattno == TABLE_OID_ATTR_NO {
                lookup_system_binding(&ctx.system_bindings, var.varno)
            } else if var.varattno == SELF_ITEM_POINTER_ATTR_NO {
                lookup_ctid(slot)
            } else {
                let index = attrno_index(var.varattno).ok_or_else(|| {
                    malformed_expr_error("system attribute outside executor support")
                })?;
                let val = slot.get_attr(index)?;
                Ok(val.clone())
            }
        }
        Expr::Const(value) => Ok(value.clone()),
        Expr::Row { descriptor, fields } => Ok(Value::Record(
            crate::include::nodes::datum::RecordValue::from_descriptor(
                descriptor.clone(),
                fields
                    .iter()
                    .map(|(_, expr)| eval_expr(expr, slot, ctx))
                    .collect::<Result<Vec<_>, ExecError>>()?,
            ),
        )),
        Expr::FieldSelect { expr, field, .. } => {
            let value = eval_expr(expr, slot, ctx)?;
            eval_record_field(value, field)
        }
        Expr::Cast(inner, ty) => cast_value_with_config(eval_expr(inner, slot, ctx)?, *ty, &ctx.datetime_config),
        Expr::Coalesce(left, right) => {
            let left = eval_expr(left, slot, ctx)?;
            if !matches!(left, Value::Null) {
                Ok(left)
            } else {
                eval_expr(right, slot, ctx)
            }
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => {
            let left = eval_expr(expr, slot, ctx)?;
            let pattern = eval_expr(pattern, slot, ctx)?;
            let escape = match escape {
                Some(value) => Some(eval_expr(value, slot, ctx)?),
                None => None,
            };
            eval_like(
                &left,
                &pattern,
                escape.as_ref(),
                *case_insensitive,
                *negated,
            )
        }
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => {
            let left = eval_expr(expr, slot, ctx)?;
            let pattern = eval_expr(pattern, slot, ctx)?;
            let escape = match escape {
                Some(value) => Some(eval_expr(value, slot, ctx)?),
                None => None,
            };
            eval_similar(&left, &pattern, escape.as_ref(), *negated)
        }
        Expr::IsNull(inner) => Ok(Value::Bool(matches!(
            eval_expr(inner, slot, ctx)?,
            Value::Null
        ))),
        Expr::IsNotNull(inner) => Ok(Value::Bool(!matches!(
            eval_expr(inner, slot, ctx)?,
            Value::Null
        ))),
        Expr::IsDistinctFrom(left, right) => Ok(Value::Bool(values_are_distinct(
            &eval_expr(left, slot, ctx)?,
            &eval_expr(right, slot, ctx)?,
        ))),
        Expr::IsNotDistinctFrom(left, right) => Ok(Value::Bool(!values_are_distinct(
            &eval_expr(left, slot, ctx)?,
            &eval_expr(right, slot, ctx)?,
        ))),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => {
            let element_type = array_type.element_type();
            let mut values = Vec::with_capacity(elements.len());
            let mut has_nested_arrays = false;
            for expr in elements {
                let value = eval_expr(expr, slot, ctx)?;
                if matches!(value, Value::Array(_) | Value::PgArray(_)) {
                    has_nested_arrays = true;
                    values.push(cast_value_with_config(
                        value,
                        *array_type,
                        &ctx.datetime_config,
                    )?);
                } else {
                    values.push(cast_value_with_config(
                        value,
                        element_type,
                        &ctx.datetime_config,
                    )?);
                }
            }
            if has_nested_arrays {
                let array = ArrayValue::from_nested_values(values, vec![1]).map_err(|details| {
                    ExecError::DetailedError {
                        message: "malformed array literal".into(),
                        detail: Some(details),
                        hint: None,
                        sqlstate: "22P02",
                    }
                })?;
                Ok(Value::PgArray(array))
            } else {
                Ok(Value::PgArray(ArrayValue::from_1d(values)))
            }
        }
        Expr::SubPlan(subplan) => match subplan.sublink_type {
            SubLinkType::ExprSubLink => eval_scalar_subquery(subplan, slot, ctx),
            SubLinkType::ArraySubLink => eval_array_subquery(subplan, slot, ctx),
            SubLinkType::ExistsSubLink => eval_exists_subquery(subplan, slot, ctx),
            SubLinkType::AnySubLink(op) => {
                let left = subplan.testexpr.as_ref().ok_or(ExecError::DetailedError {
                    message: "malformed ANY subplan".into(),
                    detail: Some("ANY subplans must carry a test expression".into()),
                    hint: None,
                    sqlstate: "XX000",
                })?;
                let left_value = eval_expr(left, slot, ctx)?;
                eval_quantified_subquery(&left_value, op, false, subplan, slot, ctx)
            }
            SubLinkType::AllSubLink(op) => {
                let left = subplan.testexpr.as_ref().ok_or(ExecError::DetailedError {
                    message: "malformed ALL subplan".into(),
                    detail: Some("ALL subplans must carry a test expression".into()),
                    hint: None,
                    sqlstate: "XX000",
                })?;
                let left_value = eval_expr(left, slot, ctx)?;
                eval_quantified_subquery(&left_value, op, true, subplan, slot, ctx)
            }
        },
        Expr::ArraySubscript { array, subscripts } => {
            let value = eval_expr(array, slot, ctx)?;
            eval_array_subscript(value, subscripts, slot, ctx)
        }
        Expr::Random => Ok(Value::Float64(rand::random::<f64>())),
        Expr::CurrentDate => Ok(current_date_value_with_config(&ctx.datetime_config)),
        Expr::CurrentTime { precision } => Ok(current_time_value_with_config(
            &ctx.datetime_config,
            *precision,
            true,
        )),
        Expr::CurrentTimestamp { precision } => Ok(current_timestamp_value_with_config(
            &ctx.datetime_config,
            *precision,
            true,
        )),
        Expr::LocalTime { precision } => Ok(current_time_value_with_config(
            &ctx.datetime_config,
            *precision,
            false,
        )),
        Expr::LocalTimestamp { precision } => Ok(current_timestamp_value_with_config(
            &ctx.datetime_config,
            *precision,
            false,
        )),
    }
}

pub fn eval_plpgsql_expr(expr: &Expr, slot: &mut TupleSlot) -> Result<Value, ExecError> {
    match expr {
        Expr::Op(op) => match (op.op, op.args.as_slice()) {
            (OpExprKind::UnaryPlus, [inner]) => eval_plpgsql_expr(inner, slot),
            (OpExprKind::Negate, [inner]) => negate_value(eval_plpgsql_expr(inner, slot)?),
            (OpExprKind::BitNot, [inner]) => bitwise_not_value(eval_plpgsql_expr(inner, slot)?),
            (OpExprKind::Add, [left, right]) => add_values(
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
            ),
            (OpExprKind::Sub, [left, right]) => sub_values(
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
            ),
            (OpExprKind::BitAnd, [left, right]) => bitwise_and_values(
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
            ),
            (OpExprKind::BitOr, [left, right]) => bitwise_or_values(
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
            ),
            (OpExprKind::BitXor, [left, right]) => bitwise_xor_values(
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
            ),
            (OpExprKind::Shl, [left, right]) => shift_left_values(
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
            ),
            (OpExprKind::Shr, [left, right]) => shift_right_values(
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
            ),
            (OpExprKind::Mul, [left, right]) => mul_values(
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
            ),
            (OpExprKind::Div, [left, right]) => div_values(
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
            ),
            (OpExprKind::Mod, [left, right]) => mod_values(
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
            ),
            (OpExprKind::Concat, [left, right]) => concat_values(
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
            ),
            (OpExprKind::Eq, [left, right]) => compare_values(
                "=",
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
            ),
            (OpExprKind::NotEq, [left, right]) => not_equal_values(
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
            ),
            (OpExprKind::Lt, [left, right]) => order_values(
                "<",
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
            ),
            (OpExprKind::LtEq, [left, right]) => order_values(
                "<=",
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
            ),
            (OpExprKind::Gt, [left, right]) => order_values(
                ">",
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
            ),
            (OpExprKind::GtEq, [left, right]) => order_values(
                ">=",
                eval_plpgsql_expr(left, slot)?,
                eval_plpgsql_expr(right, slot)?,
            ),
            (OpExprKind::RegexMatch, [left, right]) => {
                let text = eval_plpgsql_expr(left, slot)?;
                let pattern = eval_plpgsql_expr(right, slot)?;
                eval_regex_match_operator(&text, &pattern)
            }
            _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "plpgsql expression without subqueries or SQL statements",
                actual: format!("{expr:?}"),
            })),
        },
        Expr::Bool(bool_expr) => match bool_expr.boolop {
            BoolExprType::And => {
                let mut result = Value::Bool(true);
                for arg in &bool_expr.args {
                    result = eval_and(result, eval_plpgsql_expr(arg, slot)?)?;
                }
                Ok(result)
            }
            BoolExprType::Or => {
                let mut result = Value::Bool(false);
                for arg in &bool_expr.args {
                    result = eval_or(result, eval_plpgsql_expr(arg, slot)?)?;
                }
                Ok(result)
            }
            BoolExprType::Not => match bool_expr.args.as_slice() {
                [inner] => match eval_plpgsql_expr(inner, slot)? {
                    Value::Bool(value) => Ok(Value::Bool(!value)),
                    Value::Null => Ok(Value::Null),
                    other => Err(ExecError::NonBoolQual(other)),
                },
                _ => Err(malformed_expr_error("boolean")),
            },
        },
        Expr::Func(func) => {
            let builtin = builtin_function_for_expr(func.funcid)?;
            eval_plpgsql_builtin_function(
                builtin,
                func.funcresulttype,
                &func.args,
                func.funcvariadic,
                slot,
            )
        }
        Expr::ScalarArrayOp(saop) => {
            let left_value = eval_plpgsql_expr(&saop.left, slot)?;
            let right_value = eval_plpgsql_expr(&saop.right, slot)?;
            eval_quantified_array(&left_value, saop.op, !saop.use_or, &right_value)
        }
        Expr::SubLink(_) | Expr::SubPlan(_) => Err(ExecError::DetailedError {
            message: "subqueries are not supported in PL/pgSQL expression evaluation".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        }),
        Expr::Param(_) => Err(ExecError::DetailedError {
            message: "executor params are not supported in PL/pgSQL expression evaluation".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        }),
        Expr::Var(var) => {
            if var.varlevelsup == 0 && var.varattno == TABLE_OID_ATTR_NO {
                slot.table_oid
                    .map(|table_oid| Value::Int64(i64::from(table_oid)))
                    .ok_or_else(|| malformed_expr_error("tableoid in PL/pgSQL"))
            } else if var.varlevelsup == 0 {
                let index = attrno_index(var.varattno).ok_or_else(|| {
                    malformed_expr_error("system attribute outside PL/pgSQL support")
                })?;
                Ok(slot.get_attr(index)?.clone())
            } else {
                Err(ExecError::UnboundOuterColumn {
                    depth: var.varlevelsup - 1,
                    index: attrno_index(var.varattno).unwrap_or(0),
                })
            }
        }
        Expr::Const(value) => Ok(value.clone()),
        Expr::Row { descriptor, fields } => Ok(Value::Record(
            crate::include::nodes::datum::RecordValue::from_descriptor(
                descriptor.clone(),
                fields
                    .iter()
                    .map(|(_, expr)| eval_plpgsql_expr(expr, slot))
                    .collect::<Result<Vec<_>, ExecError>>()?,
            ),
        )),
        Expr::FieldSelect { expr, field, .. } => {
            let value = eval_plpgsql_expr(expr, slot)?;
            eval_record_field(value, field)
        }
        Expr::Case(case_expr) => {
            if case_expr.arg.is_some() {
                return Err(malformed_expr_error("CASE in PL/pgSQL"));
            }
            for when in &case_expr.args {
                match eval_plpgsql_expr(&when.expr, slot)? {
                    Value::Bool(true) => return eval_plpgsql_expr(&when.result, slot),
                    Value::Bool(false) | Value::Null => {}
                    other => return Err(ExecError::NonBoolQual(other)),
                }
            }
            eval_plpgsql_expr(&case_expr.defresult, slot)
        }
        Expr::CaseTest(_) => Err(malformed_expr_error("CASE test in PL/pgSQL")),
        Expr::Cast(inner, ty) => cast_value(eval_plpgsql_expr(inner, slot)?, *ty),
        Expr::Coalesce(left, right) => {
            let left = eval_plpgsql_expr(left, slot)?;
            if !matches!(left, Value::Null) {
                Ok(left)
            } else {
                eval_plpgsql_expr(right, slot)
            }
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => {
            let left = eval_plpgsql_expr(expr, slot)?;
            let pattern = eval_plpgsql_expr(pattern, slot)?;
            let escape = match escape {
                Some(value) => Some(eval_plpgsql_expr(value, slot)?),
                None => None,
            };
            eval_like(
                &left,
                &pattern,
                escape.as_ref(),
                *case_insensitive,
                *negated,
            )
        }
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => {
            let left = eval_plpgsql_expr(expr, slot)?;
            let pattern = eval_plpgsql_expr(pattern, slot)?;
            let escape = match escape {
                Some(value) => Some(eval_plpgsql_expr(value, slot)?),
                None => None,
            };
            eval_similar(&left, &pattern, escape.as_ref(), *negated)
        }
        Expr::IsNull(inner) => Ok(Value::Bool(matches!(
            eval_plpgsql_expr(inner, slot)?,
            Value::Null
        ))),
        Expr::IsNotNull(inner) => Ok(Value::Bool(!matches!(
            eval_plpgsql_expr(inner, slot)?,
            Value::Null
        ))),
        Expr::IsDistinctFrom(left, right) => Ok(Value::Bool(values_are_distinct(
            &eval_plpgsql_expr(left, slot)?,
            &eval_plpgsql_expr(right, slot)?,
        ))),
        Expr::IsNotDistinctFrom(left, right) => Ok(Value::Bool(!values_are_distinct(
            &eval_plpgsql_expr(left, slot)?,
            &eval_plpgsql_expr(right, slot)?,
        ))),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => {
            let element_type = array_type.element_type();
            let mut values = Vec::with_capacity(elements.len());
            let mut has_nested_arrays = false;
            for expr in elements {
                let value = eval_plpgsql_expr(expr, slot)?;
                if matches!(value, Value::Array(_) | Value::PgArray(_)) {
                    has_nested_arrays = true;
                    values.push(cast_value(value, *array_type)?);
                } else {
                    values.push(cast_value(value, element_type)?);
                }
            }
            if has_nested_arrays {
                let array = ArrayValue::from_nested_values(values, vec![1]).map_err(|details| {
                    ExecError::DetailedError {
                        message: "malformed array literal".into(),
                        detail: Some(details),
                        hint: None,
                        sqlstate: "22P02",
                    }
                })?;
                Ok(Value::PgArray(array))
            } else {
                Ok(Value::PgArray(ArrayValue::from_1d(values)))
            }
        }
        Expr::ArraySubscript { array, subscripts } => {
            let value = eval_plpgsql_expr(array, slot)?;
            eval_array_subscript_plpgsql(value, subscripts, slot)
        }
        Expr::CurrentDate => Ok(current_date_value()),
        Expr::CurrentTime { precision } => Ok(current_time_value(*precision, true)),
        Expr::CurrentTimestamp { precision } => Ok(current_timestamp_value(*precision, true)),
        Expr::LocalTime { precision } => Ok(current_time_value(*precision, false)),
        Expr::LocalTimestamp { precision } => Ok(current_timestamp_value(*precision, false)),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "plpgsql expression without subqueries or SQL statements",
            actual: format!("{expr:?}"),
        })),
    }
}

fn eval_record_field(value: Value, field: &str) -> Result<Value, ExecError> {
    let Value::Record(record) = value else {
        return Err(ExecError::DetailedError {
            message: format!("cannot select field \"{field}\" from non-record value"),
            detail: None,
            hint: None,
            sqlstate: "42809",
        });
    };
    record
        .iter()
        .find(|(desc, _)| desc.name.eq_ignore_ascii_case(field))
        .map(|(_, value)| value.clone())
        .ok_or_else(|| ExecError::DetailedError {
            message: format!("record has no field \"{field}\""),
            detail: None,
            hint: None,
            sqlstate: "42703",
        })
}

fn eval_plpgsql_builtin_function(
    func: BuiltinScalarFunction,
    result_type: Option<SqlType>,
    args: &[Expr],
    func_variadic: bool,
    slot: &mut TupleSlot,
) -> Result<Value, ExecError> {
    let values = args
        .iter()
        .map(|arg| eval_plpgsql_expr(arg, slot))
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(result) = eval_geometry_function(func, &values) {
        return result;
    }
    if let Some(result) = eval_range_function(func, &values, result_type) {
        return result;
    }
    match func {
        BuiltinScalarFunction::ToTsVector
        | BuiltinScalarFunction::ToTsQuery
        | BuiltinScalarFunction::PlainToTsQuery
        | BuiltinScalarFunction::PhraseToTsQuery
        | BuiltinScalarFunction::WebSearchToTsQuery
        | BuiltinScalarFunction::TsLexize => eval_text_search_builtin_function(func, &values),
        BuiltinScalarFunction::Length => match values.first() {
            Some(Value::Bit(bits)) => Ok(Value::Int32(eval_bit_length(bits))),
            _ => eval_length_function(&values),
        },
        BuiltinScalarFunction::CashLarger => match values.as_slice() {
            [Value::Money(left), Value::Money(right)] => {
                Ok(Value::Money(money_larger(*left, *right)))
            }
            _ => Err(ExecError::TypeMismatch {
                op: "cashlarger",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            }),
        },
        BuiltinScalarFunction::CashSmaller => match values.as_slice() {
            [Value::Money(left), Value::Money(right)] => {
                Ok(Value::Money(money_smaller(*left, *right)))
            }
            _ => Err(ExecError::TypeMismatch {
                op: "cashsmaller",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            }),
        },
        BuiltinScalarFunction::CashWords => match values.as_slice() {
            [Value::Money(value)] => Ok(Value::Text(cash_words_text(*value).into())),
            _ => Err(ExecError::TypeMismatch {
                op: "cash_words",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: Value::Null,
            }),
        },
        BuiltinScalarFunction::Lower => eval_lower_function(&values),
        BuiltinScalarFunction::Unistr => eval_unistr_function(&values),
        BuiltinScalarFunction::Initcap => eval_initcap_function(&values),
        BuiltinScalarFunction::BTrim => eval_trim_function("btrim", &values),
        BuiltinScalarFunction::LTrim => eval_trim_function("ltrim", &values),
        BuiltinScalarFunction::RTrim => eval_trim_function("rtrim", &values),
        BuiltinScalarFunction::Concat => eval_concat_function(&values),
        BuiltinScalarFunction::ConcatWs => eval_concat_ws_function(&values),
        BuiltinScalarFunction::Format => eval_format_function(&values),
        BuiltinScalarFunction::Left => eval_left_function(&values),
        BuiltinScalarFunction::Right => eval_right_function(&values),
        BuiltinScalarFunction::LPad => eval_lpad_function(&values),
        BuiltinScalarFunction::RPad => eval_rpad_function(&values),
        BuiltinScalarFunction::Repeat => eval_repeat_function(&values),
        BuiltinScalarFunction::Replace => eval_replace_function(&values),
        BuiltinScalarFunction::SplitPart => eval_split_part_function(&values),
        BuiltinScalarFunction::Translate => eval_translate_function(&values),
        BuiltinScalarFunction::Ascii => eval_ascii_function(&values),
        BuiltinScalarFunction::Chr => eval_chr_function(&values),
        BuiltinScalarFunction::QuoteLiteral => eval_quote_literal_function(&values),
        BuiltinScalarFunction::BpcharToText => eval_bpchar_to_text_function(&values),
        BuiltinScalarFunction::Strpos => eval_strpos_function(&values),
        BuiltinScalarFunction::Position => match values.as_slice() {
            [Value::Bit(needle), Value::Bit(haystack)] => {
                Ok(Value::Int32(eval_bit_position(needle, haystack)))
            }
            [Value::Bytea(_), Value::Bytea(_)] => eval_bytea_position_function(&values),
            _ => eval_position_function(&values),
        },
        BuiltinScalarFunction::Substring => match values.as_slice() {
            [Value::Bit(bits), Value::Int32(start)] => {
                Ok(Value::Bit(eval_bit_substring(bits, *start, None)?))
            }
            [Value::Bit(bits), Value::Int32(start), Value::Int32(len)] => {
                Ok(Value::Bit(eval_bit_substring(bits, *start, Some(*len))?))
            }
            [Value::Bytea(_), Value::Int32(_)]
            | [Value::Bytea(_), Value::Int32(_), Value::Int32(_)] => eval_bytea_substring(&values),
            [Value::Text(_), Value::Text(_)] => eval_sql_regex_substring(&values),
            _ => eval_text_substring(&values),
        },
        BuiltinScalarFunction::SimilarSubstring => eval_similar_substring(&values),
        BuiltinScalarFunction::Overlay => match values.as_slice() {
            [Value::Bit(bits), Value::Bit(place), Value::Int32(start)] => {
                Ok(Value::Bit(eval_bit_overlay(bits, place, *start, None)?))
            }
            [
                Value::Bit(bits),
                Value::Bit(place),
                Value::Int32(start),
                Value::Int32(len),
            ] => Ok(Value::Bit(eval_bit_overlay(
                bits,
                place,
                *start,
                Some(*len),
            )?)),
            [Value::Bytea(_), Value::Bytea(_), Value::Int32(_)]
            | [
                Value::Bytea(_),
                Value::Bytea(_),
                Value::Int32(_),
                Value::Int32(_),
            ] => eval_bytea_overlay(&values),
            [Value::Text(_), Value::Text(_), Value::Int32(_)]
            | [
                Value::Text(_),
                Value::Text(_),
                Value::Int32(_),
                Value::Int32(_),
            ] => eval_text_overlay(&values),
            _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "plpgsql builtin function supported by the standalone evaluator",
                actual: format!("{func:?}"),
            })),
        },
        BuiltinScalarFunction::GetBit => match values.as_slice() {
            [Value::Bit(bits), Value::Int32(index)] => {
                Ok(Value::Int32(eval_get_bit(bits, *index)?))
            }
            [Value::Bytea(_), Value::Int32(_)] => eval_get_bit_bytes(&values),
            _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "plpgsql builtin function supported by the standalone evaluator",
                actual: format!("{func:?}"),
            })),
        },
        BuiltinScalarFunction::SetBit => match values.as_slice() {
            [
                Value::Bit(bits),
                Value::Int32(index),
                Value::Int32(new_value),
            ] => Ok(Value::Bit(eval_set_bit(bits, *index, *new_value)?)),
            [Value::Bytea(_), Value::Int32(_), Value::Int32(_)] => eval_set_bit_bytes(&values),
            _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "plpgsql builtin function supported by the standalone evaluator",
                actual: format!("{func:?}"),
            })),
        },
        BuiltinScalarFunction::BitCount => match values.as_slice() {
            [Value::Bit(bits)] => Ok(Value::Int64(eval_bit_count(bits))),
            [Value::Bytea(_)] => eval_bit_count_bytes(&values),
            _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "plpgsql builtin function supported by the standalone evaluator",
                actual: format!("{func:?}"),
            })),
        },
        BuiltinScalarFunction::GetByte => eval_get_byte(&values),
        BuiltinScalarFunction::SetByte => eval_set_byte(&values),
        BuiltinScalarFunction::ConvertFrom => eval_convert_from_function(&values),
        BuiltinScalarFunction::Md5 => eval_md5_function(&values),
        BuiltinScalarFunction::Reverse => eval_reverse_function(&values),
        BuiltinScalarFunction::Encode => eval_encode_function(&values),
        BuiltinScalarFunction::Decode => eval_decode_function(&values),
        BuiltinScalarFunction::Sha224 => eval_sha224_function(&values),
        BuiltinScalarFunction::Sha256 => eval_sha256_function(&values),
        BuiltinScalarFunction::Sha384 => eval_sha384_function(&values),
        BuiltinScalarFunction::Sha512 => eval_sha512_function(&values),
        BuiltinScalarFunction::Crc32 => eval_crc32_function(&values),
        BuiltinScalarFunction::Crc32c => eval_crc32c_function(&values),
        BuiltinScalarFunction::ToBin => eval_to_bin_function(&values),
        BuiltinScalarFunction::ToOct => eval_to_oct_function(&values),
        BuiltinScalarFunction::ToHex => eval_to_hex_function(&values),
        BuiltinScalarFunction::RegexpMatch => eval_regexp_match(&values),
        BuiltinScalarFunction::RegexpLike => eval_regexp_like(&values),
        BuiltinScalarFunction::RegexpReplace => eval_regexp_replace(&values),
        BuiltinScalarFunction::RegexpCount => eval_regexp_count(&values),
        BuiltinScalarFunction::RegexpInstr => eval_regexp_instr(&values),
        BuiltinScalarFunction::RegexpSubstr => eval_regexp_substr(&values),
        BuiltinScalarFunction::RegexpSplitToArray => eval_regexp_split_to_array(&values),
        BuiltinScalarFunction::ToChar => eval_to_char_function(&values),
        BuiltinScalarFunction::ToNumber => eval_to_number_function(&values),
        BuiltinScalarFunction::Abs => eval_abs_function(&values),
        BuiltinScalarFunction::Gcd => eval_gcd_function(&values),
        BuiltinScalarFunction::Lcm => eval_lcm_function(&values),
        BuiltinScalarFunction::BoolEq => eval_booleq(&values),
        BuiltinScalarFunction::BoolNe => eval_boolne(&values),
        BuiltinScalarFunction::TsMatch => match values.as_slice() {
            [Value::TsVector(vector), Value::TsQuery(query)] => Ok(Value::Bool(
                crate::backend::executor::eval_tsvector_matches_tsquery(vector, query),
            )),
            [Value::TsQuery(query), Value::TsVector(vector)] => Ok(Value::Bool(
                crate::backend::executor::eval_tsquery_matches_tsvector(query, vector),
            )),
            _ => Err(ExecError::TypeMismatch {
                op: "@@",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            }),
        },
        BuiltinScalarFunction::TsQueryAnd => match values.as_slice() {
            [Value::TsQuery(left), Value::TsQuery(right)] => Ok(Value::TsQuery(
                crate::backend::executor::tsquery_and(left.clone(), right.clone()),
            )),
            _ => Err(ExecError::TypeMismatch {
                op: "&&",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            }),
        },
        BuiltinScalarFunction::TsQueryOr => match values.as_slice() {
            [Value::TsQuery(left), Value::TsQuery(right)] => Ok(Value::TsQuery(
                crate::backend::executor::tsquery_or(left.clone(), right.clone()),
            )),
            _ => Err(ExecError::TypeMismatch {
                op: "||",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            }),
        },
        BuiltinScalarFunction::TsQueryNot => match values.as_slice() {
            [Value::TsQuery(query)] => Ok(Value::TsQuery(crate::backend::executor::tsquery_not(
                query.clone(),
            ))),
            _ => Err(ExecError::TypeMismatch {
                op: "!!",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: Value::Null,
            }),
        },
        BuiltinScalarFunction::TsVectorConcat => match values.as_slice() {
            [Value::TsVector(left), Value::TsVector(right)] => Ok(Value::TsVector(
                crate::backend::executor::concat_tsvector(left, right),
            )),
            _ => Err(ExecError::TypeMismatch {
                op: "||",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            }),
        },
        BuiltinScalarFunction::BitcastIntegerToFloat4 => eval_bitcast_integer_to_float4(&values),
        BuiltinScalarFunction::BitcastBigintToFloat8 => eval_bitcast_bigint_to_float8(&values),
        BuiltinScalarFunction::Random
        | BuiltinScalarFunction::GetDatabaseEncoding
        | BuiltinScalarFunction::ToJson
        | BuiltinScalarFunction::ToJsonb
        | BuiltinScalarFunction::ArrayToJson
        | BuiltinScalarFunction::RowToJson
        | BuiltinScalarFunction::JsonBuildArray
        | BuiltinScalarFunction::JsonBuildObject
        | BuiltinScalarFunction::JsonObject
        | BuiltinScalarFunction::JsonTypeof
        | BuiltinScalarFunction::JsonArrayLength
        | BuiltinScalarFunction::JsonExtractPath
        | BuiltinScalarFunction::JsonExtractPathText
        | BuiltinScalarFunction::JsonbTypeof
        | BuiltinScalarFunction::JsonbArrayLength
        | BuiltinScalarFunction::JsonbExtractPath
        | BuiltinScalarFunction::JsonbExtractPathText
        | BuiltinScalarFunction::JsonbBuildArray
        | BuiltinScalarFunction::JsonbBuildObject
        | BuiltinScalarFunction::JsonbPathExists
        | BuiltinScalarFunction::JsonbPathMatch
        | BuiltinScalarFunction::Trunc
        | BuiltinScalarFunction::Round
        | BuiltinScalarFunction::Ceil
        | BuiltinScalarFunction::Ceiling
        | BuiltinScalarFunction::Floor
        | BuiltinScalarFunction::Sign
        | BuiltinScalarFunction::Sqrt
        | BuiltinScalarFunction::Cbrt
        | BuiltinScalarFunction::Power
        | BuiltinScalarFunction::Exp
        | BuiltinScalarFunction::Ln
        | BuiltinScalarFunction::Sinh
        | BuiltinScalarFunction::Cosh
        | BuiltinScalarFunction::Tanh
        | BuiltinScalarFunction::Asinh
        | BuiltinScalarFunction::Acosh
        | BuiltinScalarFunction::Atanh
        | BuiltinScalarFunction::Sind
        | BuiltinScalarFunction::Cosd
        | BuiltinScalarFunction::Tand
        | BuiltinScalarFunction::Cotd
        | BuiltinScalarFunction::Asind
        | BuiltinScalarFunction::Acosd
        | BuiltinScalarFunction::Atand
        | BuiltinScalarFunction::Atan2d
        | BuiltinScalarFunction::Erf
        | BuiltinScalarFunction::Erfc
        | BuiltinScalarFunction::Gamma
        | BuiltinScalarFunction::Lgamma
        | BuiltinScalarFunction::Float4Send
        | BuiltinScalarFunction::Float8Send => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "plpgsql builtin function supported by the standalone evaluator",
            actual: format!("{func:?}"),
        })),
        _ => {
            let _ = func_variadic;
            Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "plpgsql builtin function supported by the standalone evaluator",
                actual: format!("{func:?}"),
            }))
        }
    }
}

fn eval_text_search_builtin_function(
    func: BuiltinScalarFunction,
    values: &[Value],
) -> Result<Value, ExecError> {
    fn arg_text(
        values: &[Value],
        index: usize,
        op: &'static str,
    ) -> Result<Option<String>, ExecError> {
        let Some(value) = values.get(index) else {
            return Ok(None);
        };
        if matches!(value, Value::Null) {
            return Ok(None);
        }
        value
            .as_text()
            .map(|text| Some(text.to_string()))
            .ok_or_else(|| ExecError::TypeMismatch {
                op,
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            })
    }

    let parse_error = |op: &'static str, message: String| {
        ExecError::Parse(ParseError::UnexpectedToken {
            expected: "valid text search input",
            actual: format!("{op}: {message}"),
        })
    };

    match func {
        BuiltinScalarFunction::ToTsVector => {
            let result = match values {
                [Value::Null] | [_, Value::Null] | [Value::Null, _] => return Ok(Value::Null),
                [_] => crate::backend::tsearch::to_tsvector_with_config_name(
                    None,
                    arg_text(values, 0, "to_tsvector")?
                        .as_deref()
                        .unwrap_or_default(),
                ),
                [_, _] => crate::backend::tsearch::to_tsvector_with_config_name(
                    arg_text(values, 0, "to_tsvector")?.as_deref(),
                    arg_text(values, 1, "to_tsvector")?
                        .as_deref()
                        .unwrap_or_default(),
                ),
                _ => unreachable!(),
            };
            result
                .map(Value::TsVector)
                .map_err(|e| parse_error("to_tsvector", e))
        }
        BuiltinScalarFunction::ToTsQuery => {
            let result = match values {
                [Value::Null] | [_, Value::Null] | [Value::Null, _] => return Ok(Value::Null),
                [_] => crate::backend::tsearch::to_tsquery_with_config_name(
                    None,
                    arg_text(values, 0, "to_tsquery")?
                        .as_deref()
                        .unwrap_or_default(),
                ),
                [_, _] => crate::backend::tsearch::to_tsquery_with_config_name(
                    arg_text(values, 0, "to_tsquery")?.as_deref(),
                    arg_text(values, 1, "to_tsquery")?
                        .as_deref()
                        .unwrap_or_default(),
                ),
                _ => unreachable!(),
            };
            result
                .map(Value::TsQuery)
                .map_err(|e| parse_error("to_tsquery", e))
        }
        BuiltinScalarFunction::PlainToTsQuery => {
            let result = match values {
                [Value::Null] | [_, Value::Null] | [Value::Null, _] => return Ok(Value::Null),
                [_] => crate::backend::tsearch::plainto_tsquery_with_config_name(
                    None,
                    arg_text(values, 0, "plainto_tsquery")?
                        .as_deref()
                        .unwrap_or_default(),
                ),
                [_, _] => crate::backend::tsearch::plainto_tsquery_with_config_name(
                    arg_text(values, 0, "plainto_tsquery")?.as_deref(),
                    arg_text(values, 1, "plainto_tsquery")?
                        .as_deref()
                        .unwrap_or_default(),
                ),
                _ => unreachable!(),
            };
            result
                .map(Value::TsQuery)
                .map_err(|e| parse_error("plainto_tsquery", e))
        }
        BuiltinScalarFunction::PhraseToTsQuery => {
            let result = match values {
                [Value::Null] | [_, Value::Null] | [Value::Null, _] => return Ok(Value::Null),
                [_] => crate::backend::tsearch::phraseto_tsquery_with_config_name(
                    None,
                    arg_text(values, 0, "phraseto_tsquery")?
                        .as_deref()
                        .unwrap_or_default(),
                ),
                [_, _] => crate::backend::tsearch::phraseto_tsquery_with_config_name(
                    arg_text(values, 0, "phraseto_tsquery")?.as_deref(),
                    arg_text(values, 1, "phraseto_tsquery")?
                        .as_deref()
                        .unwrap_or_default(),
                ),
                _ => unreachable!(),
            };
            result
                .map(Value::TsQuery)
                .map_err(|e| parse_error("phraseto_tsquery", e))
        }
        BuiltinScalarFunction::WebSearchToTsQuery => {
            let result = match values {
                [Value::Null] | [_, Value::Null] | [Value::Null, _] => return Ok(Value::Null),
                [_] => crate::backend::tsearch::websearch_to_tsquery_with_config_name(
                    None,
                    arg_text(values, 0, "websearch_to_tsquery")?
                        .as_deref()
                        .unwrap_or_default(),
                ),
                [_, _] => crate::backend::tsearch::websearch_to_tsquery_with_config_name(
                    arg_text(values, 0, "websearch_to_tsquery")?.as_deref(),
                    arg_text(values, 1, "websearch_to_tsquery")?
                        .as_deref()
                        .unwrap_or_default(),
                ),
                _ => unreachable!(),
            };
            result
                .map(Value::TsQuery)
                .map_err(|e| parse_error("websearch_to_tsquery", e))
        }
        BuiltinScalarFunction::TsLexize => match values {
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            [_, _] => crate::backend::tsearch::ts_lexize_with_dictionary_name(
                arg_text(values, 0, "ts_lexize")?
                    .as_deref()
                    .unwrap_or_default(),
                arg_text(values, 1, "ts_lexize")?
                    .as_deref()
                    .unwrap_or_default(),
            )
            .map(|lexemes| {
                Value::Array(
                    lexemes
                        .into_iter()
                        .map(|lexeme| Value::Text(lexeme.into()))
                        .collect(),
                )
            })
            .map_err(|e| parse_error("ts_lexize", e)),
            _ => unreachable!(),
        },
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "text search builtin function",
            actual: format!("{func:?}"),
        })),
    }
}

fn eval_builtin_function(
    func: BuiltinScalarFunction,
    result_type: Option<SqlType>,
    args: &[Expr],
    func_variadic: bool,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    ensure_builtin_side_effects_allowed(func, ctx)?;
    if let Some(result) = eval_json_record_builtin_function(func, result_type, args, slot, ctx) {
        return result;
    }
    let values = args
        .iter()
        .map(|arg| eval_expr(arg, slot, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(result) = eval_geometry_function(func, &values) {
        return result;
    }
    if let Some(result) = eval_range_function(func, &values, result_type) {
        return result;
    }
    if let Some(result) = eval_json_builtin_function(func, &values, func_variadic) {
        return result;
    }
    if matches!(
        func,
        BuiltinScalarFunction::NextVal
            | BuiltinScalarFunction::CurrVal
            | BuiltinScalarFunction::SetVal
            | BuiltinScalarFunction::PgGetSerialSequence
    ) {
        return eval_sequence_builtin_function(func, &values, ctx);
    }
    if matches!(
        func,
        BuiltinScalarFunction::LoCreate | BuiltinScalarFunction::LoUnlink
    ) {
        return eval_large_object_builtin_function(func, &values, ctx);
    }
    match func {
        BuiltinScalarFunction::ToTsVector
        | BuiltinScalarFunction::ToTsQuery
        | BuiltinScalarFunction::PlainToTsQuery
        | BuiltinScalarFunction::PhraseToTsQuery
        | BuiltinScalarFunction::WebSearchToTsQuery
        | BuiltinScalarFunction::TsLexize => eval_text_search_builtin_function(func, &values),
        BuiltinScalarFunction::Random => eval_random_function(&values),
        BuiltinScalarFunction::RandomNormal => eval_random_normal_function(&values),
        BuiltinScalarFunction::CashLarger => match values.as_slice() {
            [Value::Money(left), Value::Money(right)] => {
                Ok(Value::Money(money_larger(*left, *right)))
            }
            _ => Err(ExecError::TypeMismatch {
                op: "cashlarger",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            }),
        },
        BuiltinScalarFunction::CashSmaller => match values.as_slice() {
            [Value::Money(left), Value::Money(right)] => {
                Ok(Value::Money(money_smaller(*left, *right)))
            }
            _ => Err(ExecError::TypeMismatch {
                op: "cashsmaller",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            }),
        },
        BuiltinScalarFunction::CashWords => match values.as_slice() {
            [Value::Money(value)] => Ok(Value::Text(cash_words_text(*value).into())),
            _ => Err(ExecError::TypeMismatch {
                op: "cash_words",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: Value::Null,
            }),
        },
        BuiltinScalarFunction::Now
        | BuiltinScalarFunction::TransactionTimestamp
        | BuiltinScalarFunction::StatementTimestamp
        | BuiltinScalarFunction::ClockTimestamp => Ok(current_timestamp_value_with_config(
            &ctx.datetime_config,
            None,
            true,
        )),
        BuiltinScalarFunction::TimeOfDay => {
            let value = current_timestamp_value_with_config(&ctx.datetime_config, None, true);
            Ok(Value::Text(
                render_datetime_value_text_with_config(&value, &ctx.datetime_config)
                    .unwrap_or_else(render_current_timestamp)
                    .into(),
            ))
        }
        BuiltinScalarFunction::NextVal
        | BuiltinScalarFunction::CurrVal
        | BuiltinScalarFunction::SetVal
        | BuiltinScalarFunction::PgGetSerialSequence => {
            unreachable!("sequence builtins handled earlier");
        }
        BuiltinScalarFunction::DatePart => eval_date_part_function(&values),
        BuiltinScalarFunction::DateTrunc => eval_date_trunc_function(&values, &ctx.datetime_config),
        BuiltinScalarFunction::IsFinite => eval_isfinite_function(&values),
        BuiltinScalarFunction::MakeDate => eval_make_date_function(&values),
        BuiltinScalarFunction::GetDatabaseEncoding => Ok(Value::Text("UTF8".into())),
        BuiltinScalarFunction::PgRustInternalBinaryCoercible => {
            eval_pg_rust_internal_binary_coercible(&values)
        }
        BuiltinScalarFunction::PgRustTestEncSetup => eval_pg_rust_test_enc_setup(&values),
        BuiltinScalarFunction::PgRustTestEncConversion => eval_pg_rust_test_enc_conversion(&values),
        BuiltinScalarFunction::PgStatGetCheckpointerNumTimed
        | BuiltinScalarFunction::PgStatGetCheckpointerNumRequested
        | BuiltinScalarFunction::PgStatGetCheckpointerNumPerformed
        | BuiltinScalarFunction::PgStatGetCheckpointerBuffersWritten
        | BuiltinScalarFunction::PgStatGetCheckpointerSlruWritten
        | BuiltinScalarFunction::PgStatGetCheckpointerWriteTime
        | BuiltinScalarFunction::PgStatGetCheckpointerSyncTime
        | BuiltinScalarFunction::PgStatGetCheckpointerStatResetTime => {
            Ok(checkpoint_stats_value(func, &ctx.checkpoint_stats)
                .expect("checkpoint stats builtin must map to a value"))
        }
        BuiltinScalarFunction::PgStatForceNextFlush => {
            ctx.session_stats.write().flush_pending(&ctx.stats);
            Ok(Value::Null)
        }
        BuiltinScalarFunction::PgStatGetSnapshotTimestamp => Ok(ctx
            .session_stats
            .read()
            .snapshot_timestamp()
            .map(Value::TimestampTz)
            .unwrap_or(Value::Null)),
        BuiltinScalarFunction::PgStatClearSnapshot => {
            ctx.session_stats.write().clear_snapshot();
            Ok(Value::Null)
        }
        BuiltinScalarFunction::PgStatHaveStats => {
            let kind =
                values
                    .first()
                    .and_then(Value::as_text)
                    .ok_or_else(|| ExecError::TypeMismatch {
                        op: "pg_stat_have_stats",
                        left: values.first().cloned().unwrap_or(Value::Null),
                        right: Value::Text("".into()),
                    })?;
            let objid = stats_oid_arg(&values[1..], "pg_stat_have_stats")?;
            let objsubid = values
                .get(2)
                .map(|value| match value {
                    Value::Int64(v) => Ok(*v),
                    Value::Int32(v) => Ok(i64::from(*v)),
                    other => Err(ExecError::TypeMismatch {
                        op: "pg_stat_have_stats",
                        left: other.clone(),
                        right: Value::Int64(0),
                    }),
                })
                .transpose()?
                .unwrap_or_default();
            let has_stats = match kind.to_ascii_lowercase().as_str() {
                "bgwriter" | "checkpointer" | "wal" => objid == 0 && objsubid == 0,
                "database" => objid != 0 && (objsubid == 0 || objsubid == 1),
                "relation" => ctx
                    .session_stats
                    .write()
                    .has_visible_relation_stats(&ctx.stats, objid),
                "function" => ctx
                    .session_stats
                    .write()
                    .has_visible_function_stats(&ctx.stats, objid),
                other => {
                    return Err(ExecError::DetailedError {
                        message: format!("unrecognized statistics kind \"{other}\""),
                        detail: None,
                        hint: None,
                        sqlstate: "22023",
                    });
                }
            };
            Ok(Value::Bool(has_stats))
        }
        BuiltinScalarFunction::PgStatGetNumscans
        | BuiltinScalarFunction::PgStatGetLastscan
        | BuiltinScalarFunction::PgStatGetTuplesReturned
        | BuiltinScalarFunction::PgStatGetTuplesFetched
        | BuiltinScalarFunction::PgStatGetTuplesInserted
        | BuiltinScalarFunction::PgStatGetTuplesUpdated
        | BuiltinScalarFunction::PgStatGetTuplesDeleted
        | BuiltinScalarFunction::PgStatGetLiveTuples
        | BuiltinScalarFunction::PgStatGetDeadTuples
        | BuiltinScalarFunction::PgStatGetBlocksFetched
        | BuiltinScalarFunction::PgStatGetBlocksHit => {
            relation_stats_value(func, stats_oid_arg(&values, "pg_stat_get_*")?, ctx)
        }
        BuiltinScalarFunction::PgStatGetXactNumscans
        | BuiltinScalarFunction::PgStatGetXactTuplesReturned
        | BuiltinScalarFunction::PgStatGetXactTuplesFetched
        | BuiltinScalarFunction::PgStatGetXactTuplesInserted
        | BuiltinScalarFunction::PgStatGetXactTuplesUpdated
        | BuiltinScalarFunction::PgStatGetXactTuplesDeleted => {
            relation_xact_stats_value(func, stats_oid_arg(&values, "pg_stat_get_xact_*")?, ctx)
        }
        BuiltinScalarFunction::PgStatGetFunctionCalls
        | BuiltinScalarFunction::PgStatGetFunctionTotalTime
        | BuiltinScalarFunction::PgStatGetFunctionSelfTime => {
            function_stats_value(func, stats_oid_arg(&values, "pg_stat_get_function_*")?, ctx)
        }
        BuiltinScalarFunction::PgStatGetXactFunctionCalls
        | BuiltinScalarFunction::PgStatGetXactFunctionTotalTime
        | BuiltinScalarFunction::PgStatGetXactFunctionSelfTime => function_xact_stats_value(
            func,
            stats_oid_arg(&values, "pg_stat_get_xact_function_*")?,
            ctx,
        ),
        BuiltinScalarFunction::PgInputIsValid => {
            let input = values[0].as_text().ok_or_else(|| ExecError::TypeMismatch {
                op: "pg_input_is_valid",
                left: values[0].clone(),
                right: Value::Text("".into()),
            })?;
            let ty = values[1].as_text().ok_or_else(|| ExecError::TypeMismatch {
                op: "pg_input_is_valid",
                left: values[1].clone(),
                right: Value::Text("".into()),
            })?;
            Ok(Value::Bool(
                soft_input_error_info_with_config(input, ty, &ctx.datetime_config)?.is_none(),
            ))
        }
        BuiltinScalarFunction::PgInputErrorMessage
        | BuiltinScalarFunction::PgInputErrorDetail
        | BuiltinScalarFunction::PgInputErrorHint
        | BuiltinScalarFunction::PgInputErrorSqlState => {
            let input = values[0].as_text().ok_or_else(|| ExecError::TypeMismatch {
                op: "pg_input_error_info",
                left: values[0].clone(),
                right: Value::Text("".into()),
            })?;
            let ty = values[1].as_text().ok_or_else(|| ExecError::TypeMismatch {
                op: "pg_input_error_info",
                left: values[1].clone(),
                right: Value::Text("".into()),
            })?;
            let info = soft_input_error_info_with_config(input, ty, &ctx.datetime_config)?;
            Ok(match (func, info) {
                (_, None) => Value::Null,
                (BuiltinScalarFunction::PgInputErrorMessage, Some(info)) => {
                    Value::Text(info.message.into())
                }
                (BuiltinScalarFunction::PgInputErrorDetail, Some(info)) => info
                    .detail
                    .map(Into::into)
                    .map(Value::Text)
                    .unwrap_or(Value::Null),
                (BuiltinScalarFunction::PgInputErrorHint, Some(info)) => info
                    .hint
                    .map(Into::into)
                    .map(Value::Text)
                    .unwrap_or(Value::Null),
                (BuiltinScalarFunction::PgInputErrorSqlState, Some(info)) => {
                    Value::Text(info.sqlstate.into())
                }
                _ => Value::Null,
            })
        }
        BuiltinScalarFunction::Abs => eval_abs_function(&values),
        BuiltinScalarFunction::Log => eval_log_function(&values),
        BuiltinScalarFunction::Log10 => eval_log10_function(&values),
        BuiltinScalarFunction::Div => eval_div_function(&values),
        BuiltinScalarFunction::Mod => mod_values(values[0].clone(), values[1].clone()),
        BuiltinScalarFunction::Scale => eval_scale_function(&values),
        BuiltinScalarFunction::MinScale => eval_min_scale_function(&values),
        BuiltinScalarFunction::TrimScale => eval_trim_scale_function(&values),
        BuiltinScalarFunction::NumericInc => eval_numeric_inc_function(&values),
        BuiltinScalarFunction::Factorial => eval_factorial_function(&values),
        BuiltinScalarFunction::ArrayNdims => eval_array_ndims_function(&values),
        BuiltinScalarFunction::ArrayDims => eval_array_dims_function(&values),
        BuiltinScalarFunction::ArrayLower => eval_array_lower_function(&values),
        BuiltinScalarFunction::ArrayFill => eval_array_fill_function(&values),
        BuiltinScalarFunction::StringToArray => eval_string_to_array_function(&values),
        BuiltinScalarFunction::ArrayToString => eval_array_to_string_function(&values),
        BuiltinScalarFunction::ArrayLength => eval_array_length_function(&values),
        BuiltinScalarFunction::Cardinality => eval_cardinality_function(&values),
        BuiltinScalarFunction::ArrayPosition => eval_array_position_function(&values),
        BuiltinScalarFunction::ArrayPositions => eval_array_positions_function(&values),
        BuiltinScalarFunction::ArrayRemove => eval_array_remove_function(&values),
        BuiltinScalarFunction::ArrayReplace => eval_array_replace_function(&values),
        BuiltinScalarFunction::ArraySort => eval_array_sort_function(&values),
        BuiltinScalarFunction::PgLsn => eval_pg_lsn_function(&values),
        BuiltinScalarFunction::Trunc => eval_trunc_function(&values),
        BuiltinScalarFunction::Round => eval_round_function(&values),
        BuiltinScalarFunction::WidthBucket => {
            if values.len() == 2 {
                eval_width_bucket_thresholds(&values)
            } else {
                eval_width_bucket_function(&values)
            }
        }
        BuiltinScalarFunction::Ceil | BuiltinScalarFunction::Ceiling => eval_ceil_function(&values),
        BuiltinScalarFunction::Floor => eval_floor_function(&values),
        BuiltinScalarFunction::Sign => eval_sign_function(&values),
        BuiltinScalarFunction::Sqrt => eval_sqrt_function(&values),
        BuiltinScalarFunction::Cbrt => eval_unary_float_function("cbrt", &values, |v| Ok(v.cbrt())),
        BuiltinScalarFunction::Power => eval_power_function(&values),
        BuiltinScalarFunction::Exp => eval_exp_function(&values),
        BuiltinScalarFunction::Ln => eval_ln_function(&values),
        BuiltinScalarFunction::Sinh => eval_unary_float_function("sinh", &values, |v| Ok(v.sinh())),
        BuiltinScalarFunction::Cosh => eval_unary_float_function("cosh", &values, |v| Ok(v.cosh())),
        BuiltinScalarFunction::Tanh => eval_unary_float_function("tanh", &values, |v| Ok(v.tanh())),
        BuiltinScalarFunction::Asinh => {
            eval_unary_float_function("asinh", &values, |v| Ok(v.asinh()))
        }
        BuiltinScalarFunction::Acosh => eval_unary_float_function("acosh", &values, eval_acosh),
        BuiltinScalarFunction::Atanh => eval_unary_float_function("atanh", &values, eval_atanh),
        BuiltinScalarFunction::Sind => eval_unary_float_function("sind", &values, |v| Ok(sind(v))),
        BuiltinScalarFunction::Cosd => eval_unary_float_function("cosd", &values, |v| Ok(cosd(v))),
        BuiltinScalarFunction::Tand => eval_unary_float_function("tand", &values, |v| Ok(tand(v))),
        BuiltinScalarFunction::Cotd => eval_unary_float_function("cotd", &values, |v| Ok(cotd(v))),
        BuiltinScalarFunction::Asind => eval_unary_float_function("asind", &values, eval_asind),
        BuiltinScalarFunction::Acosd => eval_unary_float_function("acosd", &values, eval_acosd),
        BuiltinScalarFunction::Atand => {
            eval_unary_float_function("atand", &values, |v| Ok(snap_degree(v.atan().to_degrees())))
        }
        BuiltinScalarFunction::Atan2d => eval_binary_float_function("atan2d", &values, |y, x| {
            Ok(snap_degree(y.atan2(x).to_degrees()))
        }),
        BuiltinScalarFunction::Float4Send => eval_float_send_function("float4send", &values, true),
        BuiltinScalarFunction::Float8Send => eval_float_send_function("float8send", &values, false),
        BuiltinScalarFunction::Erf => eval_unary_float_function("erf", &values, eval_erf),
        BuiltinScalarFunction::Erfc => eval_unary_float_function("erfc", &values, eval_erfc),
        BuiltinScalarFunction::Gamma => eval_unary_float_function("gamma", &values, eval_gamma),
        BuiltinScalarFunction::Lgamma => eval_unary_float_function("lgamma", &values, eval_lgamma),
        BuiltinScalarFunction::BoolEq => eval_booleq(&values),
        BuiltinScalarFunction::BoolNe => eval_boolne(&values),
        BuiltinScalarFunction::TsMatch => match values.as_slice() {
            [Value::TsVector(vector), Value::TsQuery(query)] => Ok(Value::Bool(
                crate::backend::executor::eval_tsvector_matches_tsquery(vector, query),
            )),
            [Value::TsQuery(query), Value::TsVector(vector)] => Ok(Value::Bool(
                crate::backend::executor::eval_tsquery_matches_tsvector(query, vector),
            )),
            _ => Err(ExecError::TypeMismatch {
                op: "@@",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            }),
        },
        BuiltinScalarFunction::TsQueryAnd => match values.as_slice() {
            [Value::TsQuery(left), Value::TsQuery(right)] => Ok(Value::TsQuery(
                crate::backend::executor::tsquery_and(left.clone(), right.clone()),
            )),
            _ => Err(ExecError::TypeMismatch {
                op: "&&",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            }),
        },
        BuiltinScalarFunction::TsQueryOr => match values.as_slice() {
            [Value::TsQuery(left), Value::TsQuery(right)] => Ok(Value::TsQuery(
                crate::backend::executor::tsquery_or(left.clone(), right.clone()),
            )),
            _ => Err(ExecError::TypeMismatch {
                op: "||",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            }),
        },
        BuiltinScalarFunction::TsQueryNot => match values.as_slice() {
            [Value::TsQuery(query)] => Ok(Value::TsQuery(crate::backend::executor::tsquery_not(
                query.clone(),
            ))),
            _ => Err(ExecError::TypeMismatch {
                op: "!!",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: Value::Null,
            }),
        },
        BuiltinScalarFunction::TsVectorConcat => match values.as_slice() {
            [Value::TsVector(left), Value::TsVector(right)] => Ok(Value::TsVector(
                crate::backend::executor::concat_tsvector(left, right),
            )),
            _ => Err(ExecError::TypeMismatch {
                op: "||",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            }),
        },
        BuiltinScalarFunction::BitcastIntegerToFloat4 => eval_bitcast_integer_to_float4(&values),
        BuiltinScalarFunction::BitcastBigintToFloat8 => eval_bitcast_bigint_to_float8(&values),
        BuiltinScalarFunction::Gcd => eval_gcd_function(&values),
        BuiltinScalarFunction::Lcm => eval_lcm_function(&values),
        BuiltinScalarFunction::Length => match values.first() {
            Some(Value::Bit(bits)) => Ok(Value::Int32(eval_bit_length(bits))),
            _ => eval_length_function(&values),
        },
        BuiltinScalarFunction::Concat => eval_concat_function(&values),
        BuiltinScalarFunction::ConcatWs => eval_concat_ws_function(&values),
        BuiltinScalarFunction::Format => eval_format_function(&values),
        BuiltinScalarFunction::Left => eval_left_function(&values),
        BuiltinScalarFunction::Right => eval_right_function(&values),
        BuiltinScalarFunction::LPad => eval_lpad_function(&values),
        BuiltinScalarFunction::RPad => eval_rpad_function(&values),
        BuiltinScalarFunction::Repeat => eval_repeat_function(&values),
        BuiltinScalarFunction::Lower => eval_lower_function(&values),
        BuiltinScalarFunction::Unistr => eval_unistr_function(&values),
        BuiltinScalarFunction::Initcap => eval_initcap_function(&values),
        BuiltinScalarFunction::BTrim => eval_trim_function("btrim", &values),
        BuiltinScalarFunction::LTrim => eval_trim_function("ltrim", &values),
        BuiltinScalarFunction::RTrim => eval_trim_function("rtrim", &values),
        BuiltinScalarFunction::Md5 => eval_md5_function(&values),
        BuiltinScalarFunction::Reverse => eval_reverse_function(&values),
        BuiltinScalarFunction::BpcharToText => eval_bpchar_to_text_function(&values),
        BuiltinScalarFunction::QuoteLiteral => eval_quote_literal_function(&values),
        BuiltinScalarFunction::Replace => eval_replace_function(&values),
        BuiltinScalarFunction::SplitPart => eval_split_part_function(&values),
        BuiltinScalarFunction::Translate => eval_translate_function(&values),
        BuiltinScalarFunction::Ascii => eval_ascii_function(&values),
        BuiltinScalarFunction::Chr => eval_chr_function(&values),
        BuiltinScalarFunction::Strpos => eval_strpos_function(&values),
        BuiltinScalarFunction::Position => match values.as_slice() {
            [Value::Bit(needle), Value::Bit(haystack)] => {
                Ok(Value::Int32(eval_bit_position(needle, haystack)))
            }
            [Value::Bytea(_), Value::Bytea(_)] => eval_bytea_position_function(&values),
            _ => eval_position_function(&values),
        },
        BuiltinScalarFunction::Substring => match values.as_slice() {
            [Value::Bit(bits), Value::Int32(start)] => {
                Ok(Value::Bit(eval_bit_substring(bits, *start, None)?))
            }
            [Value::Bit(bits), Value::Int32(start), Value::Int32(len)] => {
                Ok(Value::Bit(eval_bit_substring(bits, *start, Some(*len))?))
            }
            [Value::Bytea(_), Value::Int32(_)]
            | [Value::Bytea(_), Value::Int32(_), Value::Int32(_)] => eval_bytea_substring(&values),
            [Value::Text(_), Value::Text(_)] => eval_sql_regex_substring(&values),
            _ => eval_text_substring(&values),
        },
        BuiltinScalarFunction::SimilarSubstring => eval_similar_substring(&values),
        BuiltinScalarFunction::Overlay => match values.as_slice() {
            [Value::Bit(bits), Value::Bit(place), Value::Int32(start)] => {
                Ok(Value::Bit(eval_bit_overlay(bits, place, *start, None)?))
            }
            [
                Value::Bit(bits),
                Value::Bit(place),
                Value::Int32(start),
                Value::Int32(len),
            ] => Ok(Value::Bit(eval_bit_overlay(
                bits,
                place,
                *start,
                Some(*len),
            )?)),
            [Value::Bytea(_), Value::Bytea(_), Value::Int32(_)]
            | [
                Value::Bytea(_),
                Value::Bytea(_),
                Value::Int32(_),
                Value::Int32(_),
            ] => eval_bytea_overlay(&values),
            [Value::Text(_), Value::Text(_), Value::Int32(_)]
            | [
                Value::Text(_),
                Value::Text(_),
                Value::Int32(_),
                Value::Int32(_),
            ] => eval_text_overlay(&values),
            _ => unreachable!("validated overlay arguments"),
        },
        BuiltinScalarFunction::GetBit => match values.as_slice() {
            [Value::Bit(bits), Value::Int32(index)] => {
                Ok(Value::Int32(eval_get_bit(bits, *index)?))
            }
            [Value::Bytea(_), Value::Int32(_)] => eval_get_bit_bytes(&values),
            _ => unreachable!("validated get_bit arguments"),
        },
        BuiltinScalarFunction::SetBit => match values.as_slice() {
            [
                Value::Bit(bits),
                Value::Int32(index),
                Value::Int32(new_value),
            ] => Ok(Value::Bit(eval_set_bit(bits, *index, *new_value)?)),
            [Value::Bytea(_), Value::Int32(_), Value::Int32(_)] => eval_set_bit_bytes(&values),
            _ => unreachable!("validated set_bit arguments"),
        },
        BuiltinScalarFunction::BitCount => match values.as_slice() {
            [Value::Bit(bits)] => Ok(Value::Int64(eval_bit_count(bits))),
            [Value::Bytea(_)] => eval_bit_count_bytes(&values),
            _ => unreachable!("validated bit_count arguments"),
        },
        BuiltinScalarFunction::GetByte => eval_get_byte(&values),
        BuiltinScalarFunction::SetByte => eval_set_byte(&values),
        BuiltinScalarFunction::ConvertFrom => eval_convert_from_function(&values),
        BuiltinScalarFunction::Encode => eval_encode_function(&values),
        BuiltinScalarFunction::Decode => eval_decode_function(&values),
        BuiltinScalarFunction::Sha224 => eval_sha224_function(&values),
        BuiltinScalarFunction::Sha256 => eval_sha256_function(&values),
        BuiltinScalarFunction::Sha384 => eval_sha384_function(&values),
        BuiltinScalarFunction::Sha512 => eval_sha512_function(&values),
        BuiltinScalarFunction::Crc32 => eval_crc32_function(&values),
        BuiltinScalarFunction::Crc32c => eval_crc32c_function(&values),
        BuiltinScalarFunction::ToBin => eval_to_bin_function(&values),
        BuiltinScalarFunction::ToOct => eval_to_oct_function(&values),
        BuiltinScalarFunction::ToHex => eval_to_hex_function(&values),
        BuiltinScalarFunction::RegexpMatch => eval_regexp_match(&values),
        BuiltinScalarFunction::RegexpLike => eval_regexp_like(&values),
        BuiltinScalarFunction::RegexpReplace => eval_regexp_replace(&values),
        BuiltinScalarFunction::RegexpCount => eval_regexp_count(&values),
        BuiltinScalarFunction::RegexpInstr => eval_regexp_instr(&values),
        BuiltinScalarFunction::RegexpSubstr => eval_regexp_substr(&values),
        BuiltinScalarFunction::RegexpSplitToArray => eval_regexp_split_to_array(&values),
        BuiltinScalarFunction::ToChar => eval_to_char_function(&values),
        BuiltinScalarFunction::ToNumber => eval_to_number_function(&values),
        _ => unreachable!("json builtins handled by expr_json"),
    }
}

fn eval_jsonb_contains(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    let left_jsonb = jsonb_from_value(&left)?;
    let right_jsonb = jsonb_from_value(&right)?;
    Ok(Value::Bool(jsonb_contains(&left_jsonb, &right_jsonb)))
}

fn eval_jsonb_contained(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    let left_jsonb = jsonb_from_value(&left)?;
    let right_jsonb = jsonb_from_value(&right)?;
    Ok(Value::Bool(jsonb_contains(&right_jsonb, &left_jsonb)))
}

fn eval_jsonb_exists(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    let key = right.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "?",
        left: left.clone(),
        right: right.clone(),
    })?;
    let jsonb = jsonb_from_value(&left)?;
    Ok(Value::Bool(jsonb_exists(&jsonb, key)))
}

fn eval_jsonb_exists_any(left: Value, right: Value) -> Result<Value, ExecError> {
    eval_jsonb_exists_list(left, right, "?|", jsonb_exists_any)
}

fn eval_jsonb_exists_all(left: Value, right: Value) -> Result<Value, ExecError> {
    eval_jsonb_exists_list(left, right, "?&", jsonb_exists_all)
}

fn eval_jsonb_exists_list(
    left: Value,
    right: Value,
    op: &'static str,
    pred: fn(&JsonbValue, &[String]) -> bool,
) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    let keys = match right {
        Value::Array(items) => items
            .iter()
            .map(|item| {
                item.as_text()
                    .map(|text| text.to_string())
                    .ok_or_else(|| ExecError::TypeMismatch {
                        op,
                        left: left.clone(),
                        right: item.clone(),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?,
        Value::PgArray(array) => array
            .elements
            .iter()
            .map(|item| {
                item.as_text()
                    .map(|text| text.to_string())
                    .ok_or_else(|| ExecError::TypeMismatch {
                        op,
                        left: left.clone(),
                        right: item.clone(),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?,
        other => {
            return Err(ExecError::TypeMismatch {
                op,
                left,
                right: other,
            });
        }
    };
    let jsonb = jsonb_from_value(&left)?;
    Ok(Value::Bool(pred(&jsonb, &keys)))
}

fn render_current_timestamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(dur) => format!("{}.{:06}+00", dur.as_secs(), dur.subsec_micros()),
        Err(_) => "0.000000+00".to_string(),
    }
}

fn eval_random_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [] => Ok(Value::Float64(rand::random::<f64>())),
        [Value::Int32(min), Value::Int32(max)] => {
            if min > max {
                return Err(invalid_random_bound_error(
                    "lower bound must be less than or equal to upper bound",
                ));
            }
            Ok(Value::Int32(rand::thread_rng().gen_range(*min..=*max)))
        }
        [Value::Int64(min), Value::Int64(max)] => {
            if min > max {
                return Err(invalid_random_bound_error(
                    "lower bound must be less than or equal to upper bound",
                ));
            }
            Ok(Value::Int64(rand::thread_rng().gen_range(*min..=*max)))
        }
        [Value::Numeric(min), Value::Numeric(max)] => eval_random_numeric_range(min, max),
        [left, right] => Err(ExecError::TypeMismatch {
            op: "random",
            left: left.clone(),
            right: right.clone(),
        }),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "valid builtin function arity",
            actual: format!("Random({} args)", values.len()),
        })),
    }
}

fn eval_random_normal_function(values: &[Value]) -> Result<Value, ExecError> {
    let (mean, stddev) = match values {
        [] => (0.0, 1.0),
        [Value::Float64(mean), Value::Float64(stddev)] => (*mean, *stddev),
        [left, right] => {
            return Err(ExecError::TypeMismatch {
                op: "random_normal",
                left: left.clone(),
                right: right.clone(),
            });
        }
        _ => {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "valid builtin function arity",
                actual: format!("RandomNormal({} args)", values.len()),
            }));
        }
    };

    if stddev == 0.0 {
        return Ok(Value::Float64(mean));
    }

    Ok(Value::Float64((sample_standard_normal() * stddev) + mean))
}

fn eval_random_numeric_range(min: &NumericValue, max: &NumericValue) -> Result<Value, ExecError> {
    match min {
        NumericValue::NaN => return Err(invalid_random_bound_error("lower bound cannot be NaN")),
        NumericValue::PosInf | NumericValue::NegInf => {
            return Err(invalid_random_bound_error("lower bound cannot be infinity"));
        }
        NumericValue::Finite { .. } => {}
    }
    match max {
        NumericValue::NaN => return Err(invalid_random_bound_error("upper bound cannot be NaN")),
        NumericValue::PosInf | NumericValue::NegInf => {
            return Err(invalid_random_bound_error("upper bound cannot be infinity"));
        }
        NumericValue::Finite { .. } => {}
    }
    if min.cmp(max).is_gt() {
        return Err(invalid_random_bound_error(
            "lower bound must be less than or equal to upper bound",
        ));
    }

    let (
        NumericValue::Finite {
            coeff: min_coeff,
            scale: min_scale,
            ..
        },
        NumericValue::Finite {
            coeff: max_coeff,
            scale: max_scale,
            ..
        },
    ) = (min, max)
    else {
        unreachable!();
    };

    let scale = (*min_scale).max(*max_scale);
    let min_aligned = align_numeric_coeff(min_coeff.clone(), *min_scale, scale);
    let max_aligned = align_numeric_coeff(max_coeff.clone(), *max_scale, scale);

    if min_aligned == max_aligned {
        return Ok(Value::Numeric(min.clone()));
    }

    let span = (&max_aligned - &min_aligned) + BigInt::from(1u8);
    let offset = random_bigint_below(&span, &mut rand::thread_rng());
    Ok(Value::Numeric(
        NumericValue::finite(min_aligned + offset, scale)
            .with_dscale(scale)
            .normalize(),
    ))
}

fn invalid_random_bound_error(message: &str) -> ExecError {
    ExecError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate: INVALID_PARAMETER_VALUE_SQLSTATE,
    }
}

fn sample_standard_normal() -> f64 {
    let mut rng = rand::thread_rng();
    loop {
        let u1 = rng.r#gen::<f64>();
        if u1 == 0.0 {
            continue;
        }
        let u2 = rng.r#gen::<f64>();
        let radius = (-2.0 * u1.ln()).sqrt();
        let theta = 2.0 * std::f64::consts::PI * u2;
        return radius * theta.cos();
    }
}

fn align_numeric_coeff(coeff: BigInt, from_scale: u32, to_scale: u32) -> BigInt {
    coeff * pow10_bigint(to_scale.saturating_sub(from_scale))
}

fn pow10_bigint(exp: u32) -> BigInt {
    let mut value = BigInt::from(1u8);
    for _ in 0..exp {
        value *= 10u8;
    }
    value
}

fn random_bigint_below(upper_exclusive: &BigInt, rng: &mut impl RngCore) -> BigInt {
    debug_assert!(*upper_exclusive > BigInt::from(0u8));
    let (_, upper_bytes) = upper_exclusive.to_bytes_be();
    let mut candidate_bytes = vec![0u8; upper_bytes.len().max(1)];
    let high_mask = if upper_bytes.is_empty() {
        0xff
    } else {
        0xff_u8 >> upper_bytes[0].leading_zeros()
    };

    loop {
        rng.fill_bytes(&mut candidate_bytes);
        candidate_bytes[0] &= high_mask;
        let candidate = BigInt::from_bytes_be(Sign::Plus, &candidate_bytes);
        if candidate < *upper_exclusive {
            return candidate;
        }
    }
}
