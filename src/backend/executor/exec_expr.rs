use std::time::{SystemTime, UNIX_EPOCH};

use super::expr_bit::{
    bit_count as eval_bit_count, bit_length as eval_bit_length, get_bit as eval_get_bit,
    overlay as eval_bit_overlay, position as eval_bit_position, set_bit as eval_set_bit,
    substring as eval_bit_substring,
};
use super::expr_bool::{eval_booleq, eval_boolne};
use super::expr_casts::{cast_value, soft_input_error_info};
pub(crate) use super::expr_compile::{
    CompiledPredicate, compile_predicate, compile_predicate_with_decoder,
};
use super::expr_geometry::eval_geometry_function;
use super::expr_json::{
    eval_json_builtin_function, eval_json_get, eval_json_path, eval_jsonpath_operator,
};
use super::expr_math::{
    cosd, cotd, eval_abs_function, eval_acosd, eval_acosh, eval_asind, eval_atanh,
    eval_binary_float_function, eval_bitcast_bigint_to_float8, eval_bitcast_integer_to_float4,
    eval_erf, eval_erfc, eval_exp, eval_float_send_function, eval_gamma, eval_gcd_function,
    eval_lcm_function, eval_lgamma, eval_ln, eval_power, eval_sqrt, eval_unary_float_function,
    sind, snap_degree, tand,
};
use super::expr_numeric::{
    eval_ceil_function, eval_div_function, eval_factorial_function, eval_floor_function,
    eval_log_function, eval_log10_function, eval_min_scale_function, eval_numeric_inc_function,
    eval_pg_lsn_function, eval_round_function, eval_scale_function, eval_sign_function,
    eval_trim_scale_function, eval_trunc_function, eval_width_bucket_function,
};
use super::expr_ops::{
    add_values, bitwise_and_values, bitwise_not_value, bitwise_or_values, bitwise_xor_values,
    compare_values, concat_values, div_values, eval_and, eval_or, mod_values, mul_values,
    negate_value, not_equal_values, order_values, shift_left_values, shift_right_values,
    sub_values, values_are_distinct,
};
pub(crate) use super::expr_ops::{compare_order_by_keys, parse_numeric_text};
use super::expr_string::{
    eval_ascii_function, eval_bit_count_bytes, eval_bpchar_to_text_function, eval_bytea_overlay,
    eval_bytea_position_function, eval_bytea_substring, eval_chr_function, eval_concat_function,
    eval_concat_ws_function, eval_convert_from_function, eval_crc32_function, eval_crc32c_function,
    eval_decode_function, eval_encode_function, eval_format_function, eval_get_bit_bytes,
    eval_get_byte, eval_initcap_function, eval_left_function, eval_length_function, eval_like,
    eval_lower_function, eval_lpad_function, eval_md5_function, eval_position_function,
    eval_quote_literal_function, eval_repeat_function, eval_replace_function,
    eval_reverse_function, eval_right_function, eval_rpad_function, eval_set_bit_bytes,
    eval_set_byte, eval_sha224_function, eval_sha256_function, eval_sha384_function,
    eval_sha512_function, eval_split_part_function, eval_strpos_function, eval_text_substring,
    eval_to_char_function, eval_to_number_function, eval_translate_function, eval_trim_function,
    eval_unistr_function,
};
use super::expr_ops::compare_order_values;
use super::node_types::*;
use super::pg_regex::{
    eval_regex_match_operator, eval_regexp_count, eval_regexp_instr, eval_regexp_like,
    eval_regexp_match, eval_regexp_replace, eval_regexp_split_to_array, eval_regexp_substr,
    eval_similar, eval_similar_substring, eval_sql_regex_substring,
};
pub(crate) use super::value_io::{
    format_array_text, format_array_value_text,
};
use super::{ExecError, ExecutorContext, exec_next, executor_start};
use crate::backend::executor::jsonb::{
    JsonbValue, jsonb_contains, jsonb_exists, jsonb_exists_all, jsonb_exists_any, jsonb_from_value,
};
use crate::backend::parser::{ParseError, SqlType, SqlTypeKind, SubqueryComparisonOp};
use crate::include::nodes::datum::{ArrayDimension, ArrayValue};
use crate::pgrust::compact_string::CompactString;

extern crate rand;

pub fn eval_expr(
    expr: &Expr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    match expr {
        Expr::Column(index) => {
            let val = slot.get_attr(*index)?;
            Ok(val.clone())
        }
        Expr::OuterColumn { depth, index } => ctx
            .outer_rows
            .get(*depth)
            .and_then(|row| row.get(*index))
            .cloned()
            .ok_or(ExecError::UnboundOuterColumn {
                depth: *depth,
                index: *index,
            }),
        Expr::Const(value) => Ok(value.clone()),
        Expr::Add(left, right) => {
            add_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::Sub(left, right) => {
            sub_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::BitAnd(left, right) => {
            bitwise_and_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::BitOr(left, right) => {
            bitwise_or_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::BitXor(left, right) => {
            bitwise_xor_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::Shl(left, right) => {
            shift_left_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::Shr(left, right) => {
            shift_right_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::Mul(left, right) => {
            mul_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::Div(left, right) => {
            div_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::Mod(left, right) => {
            mod_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::Concat(left, right) => {
            concat_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::UnaryPlus(inner) => eval_expr(inner, slot, ctx),
        Expr::Negate(inner) => negate_value(eval_expr(inner, slot, ctx)?),
        Expr::BitNot(inner) => bitwise_not_value(eval_expr(inner, slot, ctx)?),
        Expr::Cast(inner, ty) => cast_value(eval_expr(inner, slot, ctx)?, *ty),
        Expr::Coalesce(left, right) => {
            let left = eval_expr(left, slot, ctx)?;
            if !matches!(left, Value::Null) {
                Ok(left)
            } else {
                eval_expr(right, slot, ctx)
            }
        }
        Expr::Eq(left, right) => compare_values(
            "=",
            eval_expr(left, slot, ctx)?,
            eval_expr(right, slot, ctx)?,
        ),
        Expr::NotEq(left, right) => {
            not_equal_values(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::Lt(left, right) => order_values(
            "<",
            eval_expr(left, slot, ctx)?,
            eval_expr(right, slot, ctx)?,
        ),
        Expr::LtEq(left, right) => order_values(
            "<=",
            eval_expr(left, slot, ctx)?,
            eval_expr(right, slot, ctx)?,
        ),
        Expr::Gt(left, right) => order_values(
            ">",
            eval_expr(left, slot, ctx)?,
            eval_expr(right, slot, ctx)?,
        ),
        Expr::GtEq(left, right) => order_values(
            ">=",
            eval_expr(left, slot, ctx)?,
            eval_expr(right, slot, ctx)?,
        ),
        Expr::RegexMatch(left, right) => {
            let text = eval_expr(left, slot, ctx)?;
            let pattern = eval_expr(right, slot, ctx)?;
            eval_regex_match_operator(&text, &pattern)
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
        Expr::And(left, right) => {
            eval_and(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::Or(left, right) => eval_or(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?),
        Expr::Not(inner) => match eval_expr(inner, slot, ctx)? {
            Value::Bool(value) => Ok(Value::Bool(!value)),
            Value::Null => Ok(Value::Null),
            other => Err(ExecError::NonBoolQual(other)),
        },
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
            for expr in elements {
                values.push(cast_value(eval_expr(expr, slot, ctx)?, element_type)?);
            }
            Ok(Value::PgArray(ArrayValue::from_1d(values)))
        }
        Expr::ArrayOverlap(left, right) => {
            eval_array_overlap(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::JsonbContains(left, right) => {
            eval_jsonb_contains(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::JsonbContained(left, right) => {
            eval_jsonb_contained(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::JsonbExists(left, right) => {
            eval_jsonb_exists(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::JsonbExistsAny(left, right) => {
            eval_jsonb_exists_any(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::JsonbExistsAll(left, right) => {
            eval_jsonb_exists_all(eval_expr(left, slot, ctx)?, eval_expr(right, slot, ctx)?)
        }
        Expr::JsonbPathExists(left, right) => eval_jsonpath_operator(left, right, false, slot, ctx),
        Expr::JsonbPathMatch(left, right) => eval_jsonpath_operator(left, right, true, slot, ctx),
        Expr::ScalarSubquery(plan) => eval_scalar_subquery(plan, slot, ctx),
        Expr::ExistsSubquery(plan) => eval_exists_subquery(plan, slot, ctx),
        Expr::AnySubquery { left, op, subquery } => {
            let left_value = eval_expr(left, slot, ctx)?;
            eval_quantified_subquery(&left_value, *op, false, subquery, slot, ctx)
        }
        Expr::AllSubquery { left, op, subquery } => {
            let left_value = eval_expr(left, slot, ctx)?;
            eval_quantified_subquery(&left_value, *op, true, subquery, slot, ctx)
        }
        Expr::AnyArray { left, op, right } => {
            let left_value = eval_expr(left, slot, ctx)?;
            let right_value = eval_expr(right, slot, ctx)?;
            eval_quantified_array(&left_value, *op, false, &right_value)
        }
        Expr::AllArray { left, op, right } => {
            let left_value = eval_expr(left, slot, ctx)?;
            let right_value = eval_expr(right, slot, ctx)?;
            eval_quantified_array(&left_value, *op, true, &right_value)
        }
        Expr::ArraySubscript { array, subscripts } => {
            let value = eval_expr(array, slot, ctx)?;
            eval_array_subscript(value, subscripts, slot, ctx)
        }
        Expr::Random => Ok(Value::Float64(rand::random::<f64>())),
        Expr::JsonGet(left, right) => eval_json_get(left, right, false, slot, ctx),
        Expr::JsonGetText(left, right) => eval_json_get(left, right, true, slot, ctx),
        Expr::JsonPath(left, right) => eval_json_path(left, right, false, slot, ctx),
        Expr::JsonPathText(left, right) => eval_json_path(left, right, true, slot, ctx),
        Expr::FuncCall {
            func,
            args,
            func_variadic,
            ..
        } => eval_builtin_function(*func, args, *func_variadic, slot, ctx),
        Expr::CurrentTimestamp => Ok(Value::Text(CompactString::from_owned(
            render_current_timestamp(),
        ))),
    }
}

pub fn eval_plpgsql_expr(expr: &Expr, slot: &mut TupleSlot) -> Result<Value, ExecError> {
    match expr {
        Expr::Column(index) => Ok(slot.get_attr(*index)?.clone()),
        Expr::Const(value) => Ok(value.clone()),
        Expr::Add(left, right) => add_values(
            eval_plpgsql_expr(left, slot)?,
            eval_plpgsql_expr(right, slot)?,
        ),
        Expr::Sub(left, right) => sub_values(
            eval_plpgsql_expr(left, slot)?,
            eval_plpgsql_expr(right, slot)?,
        ),
        Expr::BitAnd(left, right) => bitwise_and_values(
            eval_plpgsql_expr(left, slot)?,
            eval_plpgsql_expr(right, slot)?,
        ),
        Expr::BitOr(left, right) => bitwise_or_values(
            eval_plpgsql_expr(left, slot)?,
            eval_plpgsql_expr(right, slot)?,
        ),
        Expr::BitXor(left, right) => bitwise_xor_values(
            eval_plpgsql_expr(left, slot)?,
            eval_plpgsql_expr(right, slot)?,
        ),
        Expr::Shl(left, right) => shift_left_values(
            eval_plpgsql_expr(left, slot)?,
            eval_plpgsql_expr(right, slot)?,
        ),
        Expr::Shr(left, right) => shift_right_values(
            eval_plpgsql_expr(left, slot)?,
            eval_plpgsql_expr(right, slot)?,
        ),
        Expr::Mul(left, right) => mul_values(
            eval_plpgsql_expr(left, slot)?,
            eval_plpgsql_expr(right, slot)?,
        ),
        Expr::Div(left, right) => div_values(
            eval_plpgsql_expr(left, slot)?,
            eval_plpgsql_expr(right, slot)?,
        ),
        Expr::Mod(left, right) => mod_values(
            eval_plpgsql_expr(left, slot)?,
            eval_plpgsql_expr(right, slot)?,
        ),
        Expr::Concat(left, right) => concat_values(
            eval_plpgsql_expr(left, slot)?,
            eval_plpgsql_expr(right, slot)?,
        ),
        Expr::UnaryPlus(inner) => eval_plpgsql_expr(inner, slot),
        Expr::Negate(inner) => negate_value(eval_plpgsql_expr(inner, slot)?),
        Expr::BitNot(inner) => bitwise_not_value(eval_plpgsql_expr(inner, slot)?),
        Expr::Cast(inner, ty) => cast_value(eval_plpgsql_expr(inner, slot)?, *ty),
        Expr::Coalesce(left, right) => {
            let left = eval_plpgsql_expr(left, slot)?;
            if !matches!(left, Value::Null) {
                Ok(left)
            } else {
                eval_plpgsql_expr(right, slot)
            }
        }
        Expr::Eq(left, right) => compare_values(
            "=",
            eval_plpgsql_expr(left, slot)?,
            eval_plpgsql_expr(right, slot)?,
        ),
        Expr::NotEq(left, right) => not_equal_values(
            eval_plpgsql_expr(left, slot)?,
            eval_plpgsql_expr(right, slot)?,
        ),
        Expr::Lt(left, right) => order_values(
            "<",
            eval_plpgsql_expr(left, slot)?,
            eval_plpgsql_expr(right, slot)?,
        ),
        Expr::LtEq(left, right) => order_values(
            "<=",
            eval_plpgsql_expr(left, slot)?,
            eval_plpgsql_expr(right, slot)?,
        ),
        Expr::Gt(left, right) => order_values(
            ">",
            eval_plpgsql_expr(left, slot)?,
            eval_plpgsql_expr(right, slot)?,
        ),
        Expr::GtEq(left, right) => order_values(
            ">=",
            eval_plpgsql_expr(left, slot)?,
            eval_plpgsql_expr(right, slot)?,
        ),
        Expr::RegexMatch(left, right) => {
            let text = eval_plpgsql_expr(left, slot)?;
            let pattern = eval_plpgsql_expr(right, slot)?;
            eval_regex_match_operator(&text, &pattern)
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
        Expr::And(left, right) => eval_and(
            eval_plpgsql_expr(left, slot)?,
            eval_plpgsql_expr(right, slot)?,
        ),
        Expr::Or(left, right) => eval_or(
            eval_plpgsql_expr(left, slot)?,
            eval_plpgsql_expr(right, slot)?,
        ),
        Expr::Not(inner) => match eval_plpgsql_expr(inner, slot)? {
            Value::Bool(value) => Ok(Value::Bool(!value)),
            Value::Null => Ok(Value::Null),
            other => Err(ExecError::NonBoolQual(other)),
        },
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
            for expr in elements {
                values.push(cast_value(eval_plpgsql_expr(expr, slot)?, element_type)?);
            }
            Ok(Value::PgArray(ArrayValue::from_1d(values)))
        }
        Expr::FuncCall {
            func,
            args,
            func_variadic,
            ..
        } => eval_plpgsql_builtin_function(*func, args, *func_variadic, slot),
        Expr::ArraySubscript { array, subscripts } => {
            let value = eval_plpgsql_expr(array, slot)?;
            eval_array_subscript_plpgsql(value, subscripts, slot)
        }
        Expr::CurrentTimestamp => Ok(Value::Text(CompactString::from_owned(
            render_current_timestamp(),
        ))),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "plpgsql expression without subqueries or SQL statements",
            actual: format!("{expr:?}"),
        })),
    }
}

fn eval_plpgsql_builtin_function(
    func: BuiltinScalarFunction,
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
    match func {
        BuiltinScalarFunction::Length => match values.first() {
            Some(Value::Bit(bits)) => Ok(Value::Int32(eval_bit_length(bits))),
            _ => eval_length_function(&values),
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
        BuiltinScalarFunction::BitcastIntegerToFloat4 => eval_bitcast_integer_to_float4(&values),
        BuiltinScalarFunction::BitcastBigintToFloat8 => eval_bitcast_bigint_to_float8(&values),
        BuiltinScalarFunction::Random
        | BuiltinScalarFunction::GetDatabaseEncoding
        | BuiltinScalarFunction::ToJson
        | BuiltinScalarFunction::ToJsonb
        | BuiltinScalarFunction::ArrayToJson
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

fn eval_builtin_function(
    func: BuiltinScalarFunction,
    args: &[Expr],
    func_variadic: bool,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let values = args
        .iter()
        .map(|arg| eval_expr(arg, slot, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(result) = eval_geometry_function(func, &values) {
        return result;
    }
    if let Some(result) = eval_json_builtin_function(func, &values, func_variadic) {
        return result;
    }
    match func {
        BuiltinScalarFunction::Random => Ok(Value::Float64(rand::random::<f64>())),
        BuiltinScalarFunction::GetDatabaseEncoding => Ok(Value::Text("UTF8".into())),
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
            Ok(Value::Bool(soft_input_error_info(input, ty)?.is_none()))
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
            let info = soft_input_error_info(input, ty)?;
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
        BuiltinScalarFunction::Sqrt => eval_unary_float_function("sqrt", &values, eval_sqrt),
        BuiltinScalarFunction::Cbrt => eval_unary_float_function("cbrt", &values, |v| Ok(v.cbrt())),
        BuiltinScalarFunction::Power => eval_binary_float_function("power", &values, eval_power),
        BuiltinScalarFunction::Exp => eval_unary_float_function("exp", &values, eval_exp),
        BuiltinScalarFunction::Ln => eval_unary_float_function("ln", &values, eval_ln),
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
            _ => unreachable!("validated bit overlay arguments"),
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

fn eval_quantified_array(
    left_value: &Value,
    op: SubqueryComparisonOp,
    is_all: bool,
    array_value: &Value,
) -> Result<Value, ExecError> {
    if matches!(array_value, Value::Null) {
        return Ok(Value::Null);
    }
    if let Some(array) = normalize_array_value(array_value) {
        let items = &array.elements;
        {
            let mut saw_null = false;
            for item in items {
                match compare_subquery_values(left_value, item, op)? {
                    Value::Bool(result) => {
                        if !is_all && result {
                            return Ok(Value::Bool(true));
                        }
                        if is_all && !result {
                            return Ok(Value::Bool(false));
                        }
                    }
                    Value::Null => saw_null = true,
                    other => return Err(ExecError::NonBoolQual(other)),
                }
            }
            if items.is_empty() {
                Ok(Value::Bool(is_all))
            } else if saw_null {
                Ok(Value::Null)
            } else {
                Ok(Value::Bool(is_all))
            }
        }
    } else {
        Err(ExecError::TypeMismatch {
            op: if is_all { "ALL" } else { "ANY" },
            left: array_value.clone(),
            right: Value::Null,
        })
    }
}

fn eval_array_subscript(
    value: Value,
    subscripts: &[crate::include::nodes::plannodes::ExprArraySubscript],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let resolved = subscripts
        .iter()
        .map(|subscript| {
            Ok(ResolvedArraySubscript {
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
    apply_array_subscripts(value, &resolved)
}

fn eval_array_subscript_plpgsql(
    value: Value,
    subscripts: &[crate::include::nodes::plannodes::ExprArraySubscript],
    slot: &mut TupleSlot,
) -> Result<Value, ExecError> {
    let resolved = subscripts
        .iter()
        .map(|subscript| {
            Ok(ResolvedArraySubscript {
                is_slice: subscript.is_slice,
                lower: subscript
                    .lower
                    .as_ref()
                    .map(|expr| eval_plpgsql_expr(expr, slot))
                    .transpose()?,
                upper: subscript
                    .upper
                    .as_ref()
                    .map(|expr| eval_plpgsql_expr(expr, slot))
                    .transpose()?,
            })
        })
        .collect::<Result<Vec<_>, ExecError>>()?;
    apply_array_subscripts(value, &resolved)
}

#[derive(Clone)]
struct ResolvedArraySubscript {
    is_slice: bool,
    lower: Option<Value>,
    upper: Option<Value>,
}

fn apply_array_subscripts(
    value: Value,
    subscripts: &[ResolvedArraySubscript],
) -> Result<Value, ExecError> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let array = normalize_array_value(&value).ok_or_else(|| ExecError::TypeMismatch {
        op: "array subscript",
        left: value.clone(),
        right: Value::Null,
    })?;
    let any_slice = subscripts.iter().any(|subscript| subscript.is_slice);
    if array.dimensions.is_empty() {
        return if any_slice {
            Ok(Value::PgArray(ArrayValue::empty()))
        } else {
            Ok(Value::Null)
        };
    }
    if subscripts.len() > array.ndim() {
        return Ok(Value::Null);
    }
    apply_array_subscripts_to_value(&array, subscripts, any_slice)
}

fn apply_array_subscripts_to_value(
    array: &ArrayValue,
    subscripts: &[ResolvedArraySubscript],
    any_slice: bool,
) -> Result<Value, ExecError> {
    let mut selectors = Vec::with_capacity(array.ndim());
    let mut result_dimensions = Vec::new();
    for (dim_idx, dim) in array.dimensions.iter().enumerate() {
        if let Some(subscript) = subscripts.get(dim_idx) {
            if any_slice {
                let (lower, upper) = if subscript.is_slice {
                    (
                        array_slice_bound_index(subscript.lower.as_ref())?.unwrap_or(dim.lower_bound),
                        array_slice_bound_index(subscript.upper.as_ref())?
                            .unwrap_or(dim.lower_bound + dim.length as i32 - 1),
                    )
                } else {
                    let Some(index) = array_subscript_index(subscript.lower.as_ref())? else {
                        return Ok(Value::Null);
                    };
                    (1, index)
                };
                let clamped_lower = lower.max(dim.lower_bound);
                let clamped_upper = upper.min(dim.lower_bound + dim.length as i32 - 1);
                let length = if clamped_upper < clamped_lower {
                    0
                } else {
                    (clamped_upper - clamped_lower + 1) as usize
                };
                selectors.push(ArraySelector::Slice {
                    lower: clamped_lower,
                    upper: clamped_upper,
                });
                result_dimensions.push(ArrayDimension {
                    lower_bound: clamped_lower,
                    length,
                });
            } else {
                let Some(index) = array_subscript_index(subscript.lower.as_ref())? else {
                    return Ok(Value::Null);
                };
                selectors.push(ArraySelector::Index(index));
            }
        } else {
            selectors.push(ArraySelector::Slice {
                lower: dim.lower_bound,
                upper: dim.lower_bound + dim.length as i32 - 1,
            });
            result_dimensions.push(dim.clone());
        }
    }

    let mut matched = Vec::new();
    for (offset, item) in array.elements.iter().enumerate() {
        let coords = linear_index_to_coords(offset, &array.dimensions);
        if coords_match_selectors(&coords, &selectors) {
            matched.push(item.clone());
        }
    }
    if result_dimensions.is_empty() {
        return Ok(matched.into_iter().next().unwrap_or(Value::Null));
    }
    Ok(Value::PgArray(ArrayValue::from_dimensions(
        result_dimensions,
        matched,
    )))
}

#[derive(Clone)]
enum ArraySelector {
    Index(i32),
    Slice { lower: i32, upper: i32 },
}

fn coords_match_selectors(coords: &[i32], selectors: &[ArraySelector]) -> bool {
    coords.iter().zip(selectors.iter()).all(|(coord, selector)| match selector {
        ArraySelector::Index(index) => coord == index,
        ArraySelector::Slice { lower, upper } => coord >= lower && coord <= upper,
    })
}

fn linear_index_to_coords(offset: usize, dimensions: &[ArrayDimension]) -> Vec<i32> {
    if dimensions.is_empty() {
        return Vec::new();
    }
    let mut coords = vec![0; dimensions.len()];
    let mut remaining = offset;
    for dim_idx in 0..dimensions.len() {
        let stride = dimensions[dim_idx + 1..]
            .iter()
            .fold(1usize, |acc, dim| acc.saturating_mul(dim.length));
        let axis_offset = if stride == 0 { 0 } else { remaining / stride };
        coords[dim_idx] = dimensions[dim_idx].lower_bound + axis_offset as i32;
        remaining %= stride.max(1);
    }
    coords
}

fn normalize_array_value(value: &Value) -> Option<ArrayValue> {
    match value {
        Value::PgArray(array) => Some(array.clone()),
        Value::Array(items) => Some(ArrayValue::from_1d(items.clone())),
        _ => None,
    }
}

fn eval_array_ndims_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [value] => Ok(normalize_array_value(value)
            .and_then(|array| (!array.dimensions.is_empty()).then_some(Value::Int32(array.ndim() as i32)))
            .unwrap_or(Value::Null)),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "array_ndims(array)",
            actual: format!("ArrayNdims({} args)", values.len()),
        })),
    }
}

fn eval_array_dims_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [value] => {
            let Some(array) = normalize_array_value(value) else {
                return Ok(Value::Null);
            };
            if array.dimensions.is_empty() {
                return Ok(Value::Null);
            }
            let mut out = String::new();
            for dim in &array.dimensions {
                let upper = dim.lower_bound + dim.length as i32 - 1;
                out.push('[');
                out.push_str(&dim.lower_bound.to_string());
                out.push(':');
                out.push_str(&upper.to_string());
                out.push(']');
            }
            Ok(Value::Text(out.into()))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "array_dims(array)",
            actual: format!("ArrayDims({} args)", values.len()),
        })),
    }
}

fn eval_array_fill_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [fill, dims] => build_filled_array(fill, dims, None),
        [fill, dims, lbs] => build_filled_array(fill, dims, Some(lbs)),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "array_fill(value, dimensions [, lower_bounds])",
            actual: format!("ArrayFill({} args)", values.len()),
        })),
    }
}

fn build_filled_array(
    fill: &Value,
    dims: &Value,
    lower_bounds: Option<&Value>,
) -> Result<Value, ExecError> {
    if matches!(dims, Value::Null) || lower_bounds.is_some_and(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let dims = parse_int_array_argument("array_fill", dims)?;
    let lower_bounds = lower_bounds
        .map(|value| parse_int_array_argument("array_fill", value))
        .transpose()?;
    if let Some(lbs) = &lower_bounds {
        if lbs.len() != dims.len() {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "matching dimension and lower-bound array lengths",
                actual: "array_fill".into(),
            }));
        }
    }
    if dims.iter().any(|dim| dim.is_none()) {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "dimension values cannot be null",
            actual: "array_fill".into(),
        }));
    }
    let dims = dims.into_iter().map(|dim| dim.unwrap()).collect::<Vec<_>>();
    if lower_bounds
        .as_ref()
        .is_some_and(|lbs| lbs.iter().any(|lb| lb.is_none()))
    {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "low bound values cannot be null",
            actual: "array_fill".into(),
        }));
    }
    if dims.is_empty() || dims.iter().any(|dim| *dim == 0) {
        return Ok(Value::PgArray(ArrayValue::empty()));
    }
    let dimensions = dims
        .iter()
        .enumerate()
        .map(|(idx, dim)| ArrayDimension {
            lower_bound: lower_bounds
                .as_ref()
                .and_then(|lbs| lbs.get(idx).and_then(|lb| *lb))
                .unwrap_or(1),
            length: *dim as usize,
        })
        .collect::<Vec<_>>();
    let total = dimensions
        .iter()
        .fold(1usize, |acc, dim| acc.saturating_mul(dim.length));
    Ok(Value::PgArray(ArrayValue::from_dimensions(
        dimensions,
        std::iter::repeat_with(|| fill.to_owned_value())
            .take(total)
            .collect(),
    )))
}

fn eval_string_to_array_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] | [Value::Null, _, _] | [_, Value::Null, _] => {
            Ok(Value::Null)
        }
        [input, delimiter] => string_to_array_values(input, delimiter, None),
        [input, delimiter, null_text] => {
            if matches!(input, Value::Null) || matches!(delimiter, Value::Null) {
                return Ok(Value::Null);
            }
            string_to_array_values(input, delimiter, Some(null_text))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "string_to_array(text, delimiter [, null_string])",
            actual: format!("StringToArray({} args)", values.len()),
        })),
    }
}

fn string_to_array_values(
    input: &Value,
    delimiter: &Value,
    null_text: Option<&Value>,
) -> Result<Value, ExecError> {
    let input = input.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "string_to_array",
        left: input.clone(),
        right: delimiter.clone(),
    })?;
    let delimiter = delimiter.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "string_to_array",
        left: delimiter.clone(),
        right: Value::Text(input.into()),
    })?;
    let null_text = null_text.and_then(Value::as_text);
    let parts: Vec<String> = if delimiter.is_empty() {
        input.chars().map(|ch| ch.to_string()).collect()
    } else if input.is_empty() {
        Vec::new()
    } else {
        input.split(delimiter).map(|part| part.to_string()).collect()
    };
    Ok(Value::PgArray(ArrayValue::from_dimensions(
        vec![ArrayDimension {
            lower_bound: 1,
            length: parts.len(),
        }],
        parts
            .into_iter()
            .map(|part| match null_text {
                Some(null_marker) if part == null_marker => Value::Null,
                _ => Value::Text(part.into()),
            })
            .collect(),
    )))
}

fn eval_array_to_string_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [Value::Null, _, _] => Ok(Value::Null),
        [_, Value::Null] | [_, Value::Null, _] => Ok(Value::Null),
        [array, delimiter] => array_to_string_value(array, delimiter, None),
        [array, delimiter, null_text] => array_to_string_value(array, delimiter, Some(null_text)),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "array_to_string(array, delimiter [, null_string])",
            actual: format!("ArrayToString({} args)", values.len()),
        })),
    }
}

fn array_to_string_value(
    array: &Value,
    delimiter: &Value,
    null_text: Option<&Value>,
) -> Result<Value, ExecError> {
    let array = normalize_array_value(array).ok_or_else(|| ExecError::TypeMismatch {
        op: "array_to_string",
        left: array.clone(),
        right: delimiter.clone(),
    })?;
    let delimiter = delimiter.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "array_to_string",
        left: delimiter.clone(),
        right: Value::Null,
    })?;
    let null_text = null_text.and_then(Value::as_text);
    let mut out = String::new();
    let mut first = true;
    for item in &array.elements {
        if matches!(item, Value::Null) && null_text.is_none() {
            continue;
        }
        if !first {
            out.push_str(delimiter);
        }
        first = false;
        if matches!(item, Value::Null) {
            out.push_str(null_text.unwrap_or_default());
        } else {
            out.push_str(&render_scalar_text(item)?);
        }
    }
    Ok(Value::Text(out.into()))
}

fn eval_array_length_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [array, dim] => {
            let Some(array) = normalize_array_value(array) else {
                return Ok(Value::Null);
            };
            let dim = array_subscript_index(Some(dim))?.unwrap_or(0);
            if dim < 1 {
                return Ok(Value::Null);
            }
            Ok(array
                .axis_len((dim - 1) as usize)
                .map(|len| Value::Int32(len as i32))
                .unwrap_or(Value::Null))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "array_length(array, dimension)",
            actual: format!("ArrayLength({} args)", values.len()),
        })),
    }
}

fn eval_cardinality_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [array] => Ok(normalize_array_value(array)
            .map(|array| Value::Int32(array.elements.len() as i32))
            .unwrap_or(Value::Null)),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "cardinality(array)",
            actual: format!("Cardinality({} args)", values.len()),
        })),
    }
}

fn eval_array_position_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [Value::Null, _, _] => Ok(Value::Null),
        [array, needle] => array_position_value(array, needle, None, false),
        [array, needle, start] => {
            let start = array_subscript_index(Some(start))?;
            array_position_value(array, needle, start, false)
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "array_position(array, value [, start])",
            actual: format!("ArrayPosition({} args)", values.len()),
        })),
    }
}

fn eval_array_positions_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] => Ok(Value::Null),
        [array, needle] => array_position_value(array, needle, None, true),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "array_positions(array, value)",
            actual: format!("ArrayPositions({} args)", values.len()),
        })),
    }
}

fn array_position_value(
    array: &Value,
    needle: &Value,
    start: Option<i32>,
    all: bool,
) -> Result<Value, ExecError> {
    let Some(array) = normalize_array_value(array) else {
        return Ok(Value::Null);
    };
    if array.ndim() > 1 {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "one-dimensional array",
            actual: if all { "array_positions" } else { "array_position" }.into(),
        }));
    }
    let lower_bound = array.lower_bound(0).unwrap_or(1);
    let start = start.unwrap_or(lower_bound);
    let mut matches = Vec::new();
    for (idx, item) in array.elements.iter().enumerate() {
        let position = lower_bound + idx as i32;
        if position < start {
            continue;
        }
        let is_match = if matches!(needle, Value::Null) {
            matches!(item, Value::Null)
        } else if matches!(item, Value::Null) {
            false
        } else {
            matches!(
                compare_values("=", item.clone(), needle.clone())?,
                Value::Bool(true)
            )
        };
        if is_match {
            if !all {
                return Ok(Value::Int32(position));
            }
            matches.push(Value::Int32(position));
        }
    }
    if all {
        Ok(Value::PgArray(ArrayValue::from_dimensions(
            vec![ArrayDimension {
                lower_bound: 1,
                length: matches.len(),
            }],
            matches,
        )))
    } else {
        Ok(Value::Null)
    }
}

fn eval_array_remove_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] => Ok(Value::Null),
        [array, target] => array_replace_like(array, target, None, true),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "array_remove(array, value)",
            actual: format!("ArrayRemove({} args)", values.len()),
        })),
    }
}

fn eval_array_replace_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _, _] => Ok(Value::Null),
        [array, search, replace] => array_replace_like(array, search, Some(replace), false),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "array_replace(array, search, replace)",
            actual: format!("ArrayReplace({} args)", values.len()),
        })),
    }
}

fn array_replace_like(
    array: &Value,
    search: &Value,
    replace: Option<&Value>,
    remove: bool,
) -> Result<Value, ExecError> {
    let Some(array) = normalize_array_value(array) else {
        return Ok(Value::Null);
    };
    if array.ndim() > 1 {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "one-dimensional array",
            actual: if remove { "array_remove" } else { "array_replace" }.into(),
        }));
    }
    let mut items = Vec::new();
    for item in &array.elements {
        let matched = if matches!(search, Value::Null) {
            matches!(item, Value::Null)
        } else if matches!(item, Value::Null) {
            false
        } else {
            matches!(
                compare_values("=", item.clone(), search.clone())?,
                Value::Bool(true)
            )
        };
        if matched {
            if remove {
                continue;
            }
            items.push(replace.unwrap_or(&Value::Null).to_owned_value());
        } else {
            items.push(item.to_owned_value());
        }
    }
    Ok(Value::PgArray(ArrayValue::from_dimensions(
        vec![ArrayDimension {
            lower_bound: array.lower_bound(0).unwrap_or(1),
            length: items.len(),
        }],
        items,
    )))
}

fn eval_array_sort_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null] | [Value::Null, ..] => Ok(Value::Null),
        [array] => array_sort_value(array, false, false),
        [array, Value::Bool(desc)] => array_sort_value(array, *desc, false),
        [array, Value::Bool(desc), Value::Bool(nulls_first)] => {
            array_sort_value(array, *desc, *nulls_first)
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "array_sort(array [, descending [, nulls_first]])",
            actual: format!("ArraySort({} args)", values.len()),
        })),
    }
}

fn array_sort_value(array: &Value, descending: bool, nulls_first: bool) -> Result<Value, ExecError> {
    let Some(array) = normalize_array_value(array) else {
        return Ok(Value::Null);
    };
    if array.dimensions.is_empty() {
        return Ok(Value::PgArray(array));
    }
    if array.ndim() == 1 {
        let mut items = array.elements.clone();
        items.sort_by(|left, right| compare_order_values(left, right, Some(nulls_first), descending));
        return Ok(Value::PgArray(ArrayValue::from_dimensions(array.dimensions, items)));
    }
    let slice_dims = array.dimensions[1..].to_vec();
    let slice_len = slice_dims.iter().fold(1usize, |acc, dim| acc.saturating_mul(dim.length));
    let mut slices = array
        .elements
        .chunks(slice_len)
        .map(|chunk| {
            Value::PgArray(ArrayValue::from_dimensions(
                slice_dims.clone(),
                chunk.to_vec(),
            ))
        })
        .collect::<Vec<_>>();
    slices.sort_by(|left, right| compare_order_values(left, right, Some(nulls_first), descending));
    let mut elements = Vec::with_capacity(array.elements.len());
    for slice in slices {
        if let Value::PgArray(slice_array) = slice {
            elements.extend(slice_array.elements);
        }
    }
    Ok(Value::PgArray(ArrayValue::from_dimensions(
        array.dimensions,
        elements,
    )))
}

fn parse_int_array_argument(op: &'static str, value: &Value) -> Result<Vec<Option<i32>>, ExecError> {
    let Some(array) = normalize_array_value(value) else {
        return Err(ExecError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::Null,
        });
    };
    if array.ndim() > 1 {
        return Err(ExecError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::Null,
        });
    }
    array.elements
        .iter()
        .map(|item| array_subscript_index(Some(item)))
        .collect()
}

fn render_scalar_text(value: &Value) -> Result<String, ExecError> {
    match value {
        Value::PgArray(array) => Ok(format_array_value_text(array)),
        Value::Array(items) => Ok(format_array_text(items)),
        _ => cast_value(value.to_owned_value(), SqlType::new(SqlTypeKind::Text))?
            .as_text()
            .map(|text| text.to_string())
            .ok_or_else(|| ExecError::TypeMismatch {
                op: "::text",
                left: value.clone(),
                right: Value::Text("".into()),
            }),
    }
}

fn eval_width_bucket_thresholds(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [operand, thresholds] => {
            let Some(thresholds) = normalize_array_value(thresholds) else {
                return Err(ExecError::TypeMismatch {
                    op: "width_bucket",
                    left: operand.clone(),
                    right: thresholds.clone(),
                });
            };
            if thresholds.ndim() != 1 {
                return Err(ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "one-dimensional thresholds array",
                    actual: "width_bucket".into(),
                }));
            }
            if thresholds.elements.iter().any(|value| matches!(value, Value::Null)) {
                return Err(ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "thresholds array without NULLs",
                    actual: "width_bucket".into(),
                }));
            }
            if thresholds.elements.is_empty() {
                return Err(ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "non-empty thresholds array",
                    actual: "width_bucket".into(),
                }));
            }
            let mut bucket = 0i32;
            for threshold in &thresholds.elements {
                if matches!(order_values("<", operand.clone(), threshold.clone())?, Value::Bool(true)) {
                    break;
                }
                bucket = bucket.checked_add(1).ok_or(ExecError::Int4OutOfRange)?;
            }
            Ok(Value::Int32(bucket))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "width_bucket(operand, thresholds)",
            actual: format!("WidthBucket({} args)", values.len()),
        })),
    }
}

fn eval_array_lower_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [value, Value::Int16(dim)] => eval_array_lower_value(value, *dim as i32),
        [value, Value::Int32(dim)] => eval_array_lower_value(value, *dim),
        [value, Value::Int64(dim)] => {
            let dim = i32::try_from(*dim).map_err(|_| ExecError::Int4OutOfRange)?;
            eval_array_lower_value(value, dim)
        }
        [value, other] => Err(ExecError::TypeMismatch {
            op: "array_lower",
            left: value.clone(),
            right: other.clone(),
        }),
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "array_lower(array, dimension)",
            actual: format!("ArrayLower({} args)", values.len()),
        })),
    }
}

fn eval_array_lower_value(value: &Value, dimension: i32) -> Result<Value, ExecError> {
    let Some(array) = normalize_array_value(value) else {
        return Ok(Value::Null);
    };
    if dimension < 1 {
        return Ok(Value::Null);
    }
    Ok(array
        .lower_bound((dimension - 1) as usize)
        .map(Value::Int32)
        .unwrap_or(Value::Null))
}

fn array_subscript_index(value: Option<&Value>) -> Result<Option<i32>, ExecError> {
    match value {
        None => Ok(Some(1)),
        Some(Value::Null) => Ok(None),
        Some(Value::Int16(v)) => Ok(Some(*v as i32)),
        Some(Value::Int32(v)) => Ok(Some(*v)),
        Some(Value::Int64(v)) => i32::try_from(*v).map(Some).map_err(|_| ExecError::Int4OutOfRange),
        Some(other) => Err(ExecError::TypeMismatch {
            op: "array subscript",
            left: other.clone(),
            right: Value::Null,
        }),
    }
}

fn array_slice_bound_index(value: Option<&Value>) -> Result<Option<i32>, ExecError> {
    match value {
        None => Ok(None),
        Some(Value::Null) => Ok(None),
        Some(Value::Int16(v)) => Ok(Some(*v as i32)),
        Some(Value::Int32(v)) => Ok(Some(*v)),
        Some(Value::Int64(v)) => i32::try_from(*v).map(Some).map_err(|_| ExecError::Int4OutOfRange),
        Some(other) => Err(ExecError::TypeMismatch {
            op: "array subscript",
            left: other.clone(),
            right: Value::Null,
        }),
    }
}

fn eval_array_overlap(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    let Some(left_array) = normalize_array_value(&left) else {
        return Err(ExecError::TypeMismatch {
            op: "&&",
            left,
            right: right.clone(),
        });
    };
    let Some(right_array) = normalize_array_value(&right) else {
        return Err(ExecError::TypeMismatch {
            op: "&&",
            left,
            right,
        });
    };
    for left_item in &left_array.elements {
        if matches!(left_item, Value::Null) {
            continue;
        }
        for right_item in &right_array.elements {
            if matches!(right_item, Value::Null) {
                continue;
            }
            if matches!(
                compare_values("=", left_item.clone(), right_item.clone())?,
                Value::Bool(true)
            ) {
                return Ok(Value::Bool(true));
            }
        }
    }
    Ok(Value::Bool(false))
}

fn eval_scalar_subquery(
    plan: &Plan,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let outer_row = slot.values()?.iter().cloned().collect::<Vec<_>>();
    ctx.outer_rows.insert(0, outer_row);
    let result = (|| {
        let mut state = executor_start(plan.clone());
        let mut first_value = None;
        while let Some(inner_slot) = exec_next(&mut state, ctx)? {
            let values = inner_slot.values()?.iter().cloned().collect::<Vec<_>>();
            if values.len() != 1 {
                return Err(ExecError::CardinalityViolation(
                    "subquery must return only one column".into(),
                ));
            }
            if first_value.is_some() {
                return Err(ExecError::CardinalityViolation(
                    "more than one row returned by a subquery used as an expression".into(),
                ));
            }
            first_value = Some(values[0].clone());
        }
        Ok(first_value.unwrap_or(Value::Null))
    })();
    ctx.outer_rows.remove(0);
    result
}

fn eval_exists_subquery(
    plan: &Plan,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let outer_row = slot.values()?.iter().cloned().collect::<Vec<_>>();
    ctx.outer_rows.insert(0, outer_row);
    let result = (|| {
        let mut state = executor_start(plan.clone());
        Ok(Value::Bool(exec_next(&mut state, ctx)?.is_some()))
    })();
    ctx.outer_rows.remove(0);
    result
}

fn eval_quantified_subquery(
    left_value: &Value,
    op: SubqueryComparisonOp,
    is_all: bool,
    plan: &Plan,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let outer_row = slot.values()?.iter().cloned().collect::<Vec<_>>();
    ctx.outer_rows.insert(0, outer_row);
    let result = (|| {
        let mut state = executor_start(plan.clone());
        let mut saw_row = false;
        let mut saw_null = false;
        while let Some(inner_slot) = exec_next(&mut state, ctx)? {
            saw_row = true;
            let values = inner_slot.values()?.iter().cloned().collect::<Vec<_>>();
            if values.len() != 1 {
                return Err(ExecError::CardinalityViolation(
                    "subquery must return only one column".into(),
                ));
            }
            match compare_subquery_values(left_value, &values[0], op)? {
                Value::Bool(result) => {
                    if !is_all && result {
                        return Ok(Value::Bool(true));
                    }
                    if is_all && !result {
                        return Ok(Value::Bool(false));
                    }
                }
                Value::Null => saw_null = true,
                other => return Err(ExecError::NonBoolQual(other)),
            }
        }
        if !saw_row {
            Ok(Value::Bool(is_all))
        } else if saw_null {
            Ok(Value::Null)
        } else {
            Ok(Value::Bool(is_all))
        }
    })();
    ctx.outer_rows.remove(0);
    result
}

fn compare_subquery_values(
    left: &Value,
    right: &Value,
    op: SubqueryComparisonOp,
) -> Result<Value, ExecError> {
    let (left, right) = coerce_quantified_compare_values(left, right)?;
    match op {
        SubqueryComparisonOp::Eq => compare_values("=", left, right),
        SubqueryComparisonOp::NotEq => not_equal_values(left, right),
        SubqueryComparisonOp::Lt => order_values("<", left, right),
        SubqueryComparisonOp::LtEq => order_values("<=", left, right),
        SubqueryComparisonOp::Gt => order_values(">", left, right),
        SubqueryComparisonOp::GtEq => order_values(">=", left, right),
    }
}

fn coerce_quantified_compare_values(
    left: &Value,
    right: &Value,
) -> Result<(Value, Value), ExecError> {
    use Value::*;
    let needs_float = matches!(
        (left, right),
        (Float64(_), Int16(_) | Int32(_) | Int64(_))
            | (Int16(_) | Int32(_) | Int64(_), Float64(_))
            | (Float64(_), Numeric(_))
            | (Numeric(_), Float64(_))
    );
    if needs_float {
        return Ok((
            cast_value(left.clone(), SqlType::new(SqlTypeKind::Float8))?,
            cast_value(right.clone(), SqlType::new(SqlTypeKind::Float8))?,
        ));
    }
    Ok((left.clone(), right.clone()))
}

fn render_current_timestamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(dur) => format!("{}.{:06}+00", dur.as_secs(), dur.subsec_micros()),
        Err(_) => "0.000000+00".to_string(),
    }
}
