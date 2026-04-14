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
use super::expr_datetime::{current_date_value, current_time_value, current_timestamp_value};
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
use super::expr_ops::compare_order_values;
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
use crate::backend::parser::{ParseError, SqlType, SqlTypeKind, SubqueryComparisonOp};
use crate::include::nodes::datum::{ArrayDimension, ArrayValue};

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
use subquery::{eval_exists_subquery, eval_quantified_subquery, eval_scalar_subquery};

extern crate rand;

pub fn eval_expr(
    expr: &Expr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    match expr {
        Expr::Var(var) => {
            if var.varlevelsup > 0 {
                let depth = var.varlevelsup - 1;
                let index = var.varattno.saturating_sub(1);
                ctx.outer_rows
                    .get(depth)
                    .and_then(|row| row.get(index))
                    .cloned()
                    .ok_or(ExecError::UnboundOuterColumn { depth, index })
            } else {
                let index = var.varattno.saturating_sub(1);
                let val = slot.get_attr(index)?;
                Ok(val.clone())
            }
        }
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
        Expr::CurrentDate => Ok(current_date_value()),
        Expr::CurrentTime { precision } => Ok(current_time_value(*precision, true)),
        Expr::CurrentTimestamp { precision } => Ok(current_timestamp_value(*precision, true)),
        Expr::LocalTime { precision } => Ok(current_time_value(*precision, false)),
        Expr::LocalTimestamp { precision } => Ok(current_timestamp_value(*precision, false)),
    }
}

pub fn eval_plpgsql_expr(expr: &Expr, slot: &mut TupleSlot) -> Result<Value, ExecError> {
    match expr {
        Expr::Var(var) => {
            if var.varlevelsup == 0 {
                Ok(slot.get_attr(var.varattno.saturating_sub(1))?.clone())
            } else {
                Err(ExecError::UnboundOuterColumn {
                    depth: var.varlevelsup - 1,
                    index: var.varattno.saturating_sub(1),
                })
            }
        }
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
        BuiltinScalarFunction::ToTsVector
        | BuiltinScalarFunction::ToTsQuery
        | BuiltinScalarFunction::PlainToTsQuery
        | BuiltinScalarFunction::PhraseToTsQuery
        | BuiltinScalarFunction::WebSearchToTsQuery
        | BuiltinScalarFunction::TsLexize => eval_text_search_builtin_function(func, &values),
        BuiltinScalarFunction::Random => Ok(Value::Float64(rand::random::<f64>())),
        BuiltinScalarFunction::Now
        | BuiltinScalarFunction::TransactionTimestamp
        | BuiltinScalarFunction::StatementTimestamp
        | BuiltinScalarFunction::ClockTimestamp => Ok(current_timestamp_value(None, true)),
        BuiltinScalarFunction::TimeOfDay => {
            let value = current_timestamp_value(None, true);
            Ok(Value::Text(
                super::render_datetime_value_text(&value)
                    .unwrap_or_else(render_current_timestamp)
                    .into(),
            ))
        }
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

fn render_current_timestamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(dur) => format!("{}.{:06}+00", dur.as_secs(), dur.subsec_micros()),
        Err(_) => "0.000000+00".to_string(),
    }
}
