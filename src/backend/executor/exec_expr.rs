use std::time::{SystemTime, UNIX_EPOCH};

use super::node_types::*;
use super::expr_casts::{cast_value, soft_input_error_info};
pub(crate) use super::expr_compile::{
    CompiledPredicate, compile_predicate, compile_predicate_with_decoder,
};
use super::expr_json::{
    eval_json_builtin_function, eval_json_get, eval_json_path, eval_jsonpath_operator,
};
pub(crate) use super::expr_ops::{compare_order_by_keys, parse_numeric_text};
use super::expr_ops::{
    add_values, compare_values, concat_values, div_values, eval_and, eval_or, mod_values,
    mul_values, negate_value, not_equal_values, order_values, shift_left_values,
    shift_right_values, sub_values, values_are_distinct,
};
use super::{ExecError, ExecutorContext, exec_next, executor_start};
pub(crate) use super::expr_json::eval_json_table_function;
pub(crate) use super::value_io::{decode_value, format_array_text, tuple_from_values};
use crate::backend::executor::jsonb::{
    JsonbValue, jsonb_contains, jsonb_exists, jsonb_exists_all, jsonb_exists_any,
    jsonb_from_value,
};
use crate::backend::parser::SubqueryComparisonOp;
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
        Expr::Cast(inner, ty) => cast_value(eval_expr(inner, slot, ctx)?, *ty),
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
            if matches!(text, Value::Null) || matches!(pattern, Value::Null) {
                return Ok(Value::Null);
            }
            let text_str = text.as_text().ok_or_else(|| ExecError::TypeMismatch {
                op: "~",
                left: text.clone(),
                right: pattern.clone(),
            })?;
            let pat_str = pattern.as_text().ok_or_else(|| ExecError::TypeMismatch {
                op: "~",
                left: text.clone(),
                right: pattern.clone(),
            })?;
            let re =
                regex::Regex::new(pat_str).map_err(|e| ExecError::InvalidRegex(e.to_string()))?;
            Ok(Value::Bool(re.is_match(text_str)))
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
            Ok(Value::Array(values))
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
        Expr::Random => Ok(Value::Float64(rand::random::<f64>())),
        Expr::JsonGet(left, right) => eval_json_get(left, right, false, slot, ctx),
        Expr::JsonGetText(left, right) => eval_json_get(left, right, true, slot, ctx),
        Expr::JsonPath(left, right) => eval_json_path(left, right, false, slot, ctx),
        Expr::JsonPathText(left, right) => eval_json_path(left, right, true, slot, ctx),
        Expr::FuncCall { func, args } => eval_builtin_function(*func, args, slot, ctx),
        Expr::CurrentTimestamp => Ok(Value::Text(CompactString::from_owned(
            render_current_timestamp(),
        ))),
    }
}

fn eval_builtin_function(
    func: BuiltinScalarFunction,
    args: &[Expr],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let values = args
        .iter()
        .map(|arg| eval_expr(arg, slot, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(result) = eval_json_builtin_function(func, &values) {
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
                (BuiltinScalarFunction::PgInputErrorMessage, Some(info)) => Value::Text(info.message.into()),
                (BuiltinScalarFunction::PgInputErrorDetail, Some(info)) => info.detail.map(Into::into).map(Value::Text).unwrap_or(Value::Null),
                (BuiltinScalarFunction::PgInputErrorHint, Some(info)) => info.hint.map(Into::into).map(Value::Text).unwrap_or(Value::Null),
                (BuiltinScalarFunction::PgInputErrorSqlState, Some(info)) => Value::Text(info.sqlstate.into()),
                _ => Value::Null,
            })
        }
        BuiltinScalarFunction::Abs => eval_abs_function(&values),
        BuiltinScalarFunction::Gcd => eval_gcd_function(&values),
        BuiltinScalarFunction::Lcm => eval_lcm_function(&values),
        BuiltinScalarFunction::Left => eval_left_function(&values),
        BuiltinScalarFunction::Repeat => eval_repeat_function(&values),
        _ => unreachable!("json builtins handled by expr_json"),
    }
}

fn eval_left_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(text_value) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(count_value) = values.get(1) else {
        return Ok(Value::Null);
    };
    if matches!(text_value, Value::Null) || matches!(count_value, Value::Null) {
        return Ok(Value::Null);
    }
    let text = text_value.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "left",
        left: text_value.clone(),
        right: count_value.clone(),
    })?;
    let count = match count_value {
        Value::Int32(v) => *v,
        other => {
            return Err(ExecError::TypeMismatch {
                op: "left",
                left: text_value.clone(),
                right: other.clone(),
            });
        }
    };
    let chars: Vec<char> = text.chars().collect();
    let take = if count >= 0 {
        usize::min(count as usize, chars.len())
    } else {
        chars.len().saturating_sub(count.unsigned_abs() as usize)
    };
    Ok(Value::Text(CompactString::from_owned(
        chars[..take].iter().collect(),
    )))
}

fn eval_repeat_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(text_value) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(count_value) = values.get(1) else {
        return Ok(Value::Null);
    };
    if matches!(text_value, Value::Null) || matches!(count_value, Value::Null) {
        return Ok(Value::Null);
    }
    let text = text_value.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "repeat",
        left: text_value.clone(),
        right: count_value.clone(),
    })?;
    let count = match count_value {
        Value::Int32(v) => *v,
        other => {
            return Err(ExecError::TypeMismatch {
                op: "repeat",
                left: text_value.clone(),
                right: other.clone(),
            });
        }
    };
    if count <= 0 {
        return Ok(Value::Text(CompactString::new("")));
    }
    let count = count as usize;
    let len = text
        .len()
        .checked_mul(count)
        .filter(|len| *len <= i32::MAX as usize)
        .ok_or(ExecError::RequestedLengthTooLarge)?;
    let mut out = String::with_capacity(len);
    for _ in 0..count {
        out.push_str(text);
    }
    Ok(Value::Text(CompactString::from_owned(out)))
}

fn eval_abs_function(values: &[Value]) -> Result<Value, ExecError> {
    match &values[0] {
        Value::Null => Ok(Value::Null),
        Value::Int16(v) => Ok(Value::Int16(v.checked_abs().ok_or(ExecError::Int2OutOfRange)?)),
        Value::Int32(v) => Ok(Value::Int32(v.checked_abs().ok_or(ExecError::Int4OutOfRange)?)),
        Value::Int64(v) => Ok(Value::Int64(v.checked_abs().ok_or(ExecError::Int8OutOfRange)?)),
        Value::Float64(v) => Ok(Value::Float64(v.abs())),
        Value::Numeric(v) => Ok(Value::Numeric(v.abs())),
        other => Err(ExecError::TypeMismatch {
            op: "abs",
            left: other.clone(),
            right: Value::Null,
        }),
    }
}

fn gcd_i128(mut left: i128, mut right: i128) -> u128 {
    left = left.abs();
    right = right.abs();
    let mut left = left as u128;
    let mut right = right as u128;
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

fn eval_gcd_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [Value::Int16(left), Value::Int16(right)] => {
            let gcd = gcd_i128(i128::from(*left), i128::from(*right));
            i16::try_from(gcd)
                .map(Value::Int16)
                .map_err(|_| ExecError::Int2OutOfRange)
        }
        [Value::Int32(left), Value::Int32(right)] => {
            let gcd = gcd_i128(i128::from(*left), i128::from(*right));
            i32::try_from(gcd)
                .map(Value::Int32)
                .map_err(|_| ExecError::Int4OutOfRange)
        }
        [Value::Int64(left), Value::Int64(right)] => {
            let gcd = gcd_i128(i128::from(*left), i128::from(*right));
            i64::try_from(gcd)
                .map(Value::Int64)
                .map_err(|_| ExecError::Int8OutOfRange)
        }
        [left, right] => Err(ExecError::TypeMismatch {
            op: "gcd",
            left: left.clone(),
            right: right.clone(),
        }),
        _ => Ok(Value::Null),
    }
}

fn eval_lcm_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [Value::Int16(left), Value::Int16(right)] => {
            let gcd = gcd_i128(i128::from(*left), i128::from(*right));
            let lcm = if *left == 0 || *right == 0 {
                0
            } else {
                (i128::from(*left) / gcd as i128)
                    .checked_mul(i128::from(*right))
                    .and_then(|value| value.checked_abs())
                    .ok_or(ExecError::Int2OutOfRange)?
            };
            i16::try_from(lcm)
                .map(Value::Int16)
                .map_err(|_| ExecError::Int2OutOfRange)
        }
        [Value::Int32(left), Value::Int32(right)] => {
            let gcd = gcd_i128(i128::from(*left), i128::from(*right));
            let lcm = if *left == 0 || *right == 0 {
                0
            } else {
                (i128::from(*left) / gcd as i128)
                    .checked_mul(i128::from(*right))
                    .and_then(|value| value.checked_abs())
                    .ok_or(ExecError::Int4OutOfRange)?
            };
            i32::try_from(lcm)
                .map(Value::Int32)
                .map_err(|_| ExecError::Int4OutOfRange)
        }
        [Value::Int64(left), Value::Int64(right)] => {
            let gcd = gcd_i128(i128::from(*left), i128::from(*right));
            let lcm = if *left == 0 || *right == 0 {
                0
            } else {
                (i128::from(*left) / gcd as i128)
                    .checked_mul(i128::from(*right))
                    .and_then(|value| value.checked_abs())
                    .ok_or(ExecError::Int8OutOfRange)?
            };
            i64::try_from(lcm)
                .map(Value::Int64)
                .map_err(|_| ExecError::Int8OutOfRange)
        }
        [left, right] => Err(ExecError::TypeMismatch {
            op: "lcm",
            left: left.clone(),
            right: right.clone(),
        }),
        _ => Ok(Value::Null),
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
    match array_value {
        Value::Null => Ok(Value::Null),
        Value::Array(items) => {
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
        other => Err(ExecError::TypeMismatch {
            op: if is_all { "ALL" } else { "ANY" },
            left: other.clone(),
            right: Value::Null,
        }),
    }
}

fn eval_array_overlap(left: Value, right: Value) -> Result<Value, ExecError> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Array(left_items), Value::Array(right_items)) => {
            for left_item in &left_items {
                if matches!(left_item, Value::Null) {
                    continue;
                }
                for right_item in &right_items {
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
        (left, right) => Err(ExecError::TypeMismatch {
            op: "&&",
            left,
            right,
        }),
    }
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
    match op {
        SubqueryComparisonOp::Eq => compare_values("=", left.clone(), right.clone()),
        SubqueryComparisonOp::NotEq => not_equal_values(left.clone(), right.clone()),
        SubqueryComparisonOp::Lt => order_values("<", left.clone(), right.clone()),
        SubqueryComparisonOp::LtEq => order_values("<=", left.clone(), right.clone()),
        SubqueryComparisonOp::Gt => order_values(">", left.clone(), right.clone()),
        SubqueryComparisonOp::GtEq => order_values(">=", left.clone(), right.clone()),
    }
}

fn render_current_timestamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(dur) => format!("{}.{:06}+00", dur.as_secs(), dur.subsec_micros()),
        Err(_) => "0.000000+00".to_string(),
    }
}
