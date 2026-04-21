use std::cmp::Ordering;

use num_bigint::BigInt;
use num_integer::Integer;
use num_traits::{Signed, ToPrimitive, Zero};

use super::ExecError;
use super::expr_bit::{
    bitwise_binary as bitwise_binary_bits, bitwise_not as bitwise_not_bits, compare_bit_strings,
    concat_bit_strings, shift_left as shift_left_bits, shift_right as shift_right_bits,
};
use super::expr_bool::order_bool_values;
use super::expr_casts::cast_value;
use super::expr_money::{
    money_add, money_cash_div, money_cmp, money_div_float, money_div_int, money_mul_float,
    money_mul_int, money_sub,
};
use super::{compare_multirange_values, expr_range::compare_range_values};
use super::node_types::*;
use crate::backend::executor::jsonb::{
    JsonbValue, compare_jsonb, decode_jsonb, encode_jsonb, jsonb_concat,
};
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::pgrust::compact_string::CompactString;

pub(crate) fn compare_order_by_keys(
    items: &[OrderByEntry],
    left_keys: &[Value],
    right_keys: &[Value],
) -> Ordering {
    for (item, (left_value, right_value)) in
        items.iter().zip(left_keys.iter().zip(right_keys.iter()))
    {
        let ordering =
            compare_order_values(left_value, right_value, item.nulls_first, item.descending);
        if ordering != Ordering::Equal {
            return if item.descending
                && !matches!(
                    (left_value, right_value),
                    (Value::Null, _) | (_, Value::Null)
                ) {
                ordering.reverse()
            } else {
                ordering
            };
        }
    }
    Ordering::Equal
}

pub(crate) fn compare_order_values(
    left: &Value,
    right: &Value,
    nulls_first: Option<bool>,
    descending: bool,
) -> Ordering {
    let nulls_first = nulls_first.unwrap_or(descending);
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (_, Value::Null) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (Value::Int32(a), Value::Int32(b)) => a.cmp(b),
        (Value::Date(a), Value::Date(b)) => a.cmp(b),
        (Value::Time(a), Value::Time(b)) => a.cmp(b),
        (Value::TimeTz(a), Value::TimeTz(b)) => a
            .time
            .cmp(&b.time)
            .then_with(|| a.offset_seconds.cmp(&b.offset_seconds)),
        (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
        (Value::TimestampTz(a), Value::TimestampTz(b)) => a.cmp(b),
        (Value::Bit(a), Value::Bit(b)) => compare_bit_strings(a, b),
        (Value::Bytea(a), Value::Bytea(b)) => a.cmp(b),
        (Value::Float64(a), Value::Float64(b)) => pg_float_cmp(*a, *b),
        (Value::Money(a), Value::Money(b)) => money_cmp(*a, *b),
        (a, b) if parsed_numeric_value(a).is_some() && parsed_numeric_value(b).is_some() => {
            parsed_numeric_value(a)
                .and_then(|left| parsed_numeric_value(b).map(|right| left.cmp(&right)))
                .unwrap_or(Ordering::Equal)
        }
        (Value::Jsonb(a), Value::Jsonb(b)) => compare_jsonb(
            &decode_jsonb(a).unwrap_or(JsonbValue::Null),
            &decode_jsonb(b).unwrap_or(JsonbValue::Null),
        ),
        (Value::Range(a), Value::Range(b)) => compare_range_values(a, b),
        (Value::Multirange(a), Value::Multirange(b)) => compare_multirange_values(a, b),
        (Value::TsVector(a), Value::TsVector(b)) => {
            crate::backend::executor::compare_tsvector(a, b)
        }
        (Value::TsQuery(a), Value::TsQuery(b)) => crate::backend::executor::compare_tsquery(a, b),
        (Value::Record(a), Value::Record(b)) => compare_record_values(a, b),
        (a, b) if a.as_text().is_some() && b.as_text().is_some() => {
            a.as_text().unwrap().cmp(b.as_text().unwrap())
        }
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (a, b) if normalize_array_value(a).is_some() && normalize_array_value(b).is_some() => {
            compare_array_values(
                &normalize_array_value(a).unwrap(),
                &normalize_array_value(b).unwrap(),
            )
        }
        _ => Ordering::Equal,
    }
}

pub(crate) fn eval_and(left: Value, right: Value) -> Result<Value, ExecError> {
    match (left, right) {
        (Value::Bool(false), _) | (_, Value::Bool(false)) => Ok(Value::Bool(false)),
        (Value::Bool(true), Value::Bool(true)) => Ok(Value::Bool(true)),
        (Value::Bool(true), Value::Null)
        | (Value::Null, Value::Bool(true))
        | (Value::Null, Value::Null) => Ok(Value::Null),
        (left, right) => Err(ExecError::TypeMismatch {
            op: "AND",
            left,
            right,
        }),
    }
}

pub(crate) fn eval_or(left: Value, right: Value) -> Result<Value, ExecError> {
    match (left, right) {
        (Value::Bool(true), _) | (_, Value::Bool(true)) => Ok(Value::Bool(true)),
        (Value::Bool(false), Value::Bool(false)) => Ok(Value::Bool(false)),
        (Value::Bool(false), Value::Null)
        | (Value::Null, Value::Bool(false))
        | (Value::Null, Value::Null) => Ok(Value::Null),
        (left, right) => Err(ExecError::TypeMismatch {
            op: "OR",
            left,
            right,
        }),
    }
}

pub(crate) fn compare_values(
    op: &'static str,
    left: Value,
    right: Value,
) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match (&left, &right) {
        (Value::Int16(l), Value::Int16(r)) => Ok(Value::Bool(l == r)),
        (Value::Int16(l), Value::Int32(r)) => Ok(Value::Bool((*l as i32) == *r)),
        (Value::Int16(l), Value::Int64(r)) => Ok(Value::Bool((*l as i64) == *r)),
        (Value::Int32(l), Value::Int16(r)) => Ok(Value::Bool(*l == (*r as i32))),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Bool(l == r)),
        (Value::Int32(l), Value::Int64(r)) => Ok(Value::Bool((*l as i64) == *r)),
        (Value::Int64(l), Value::Int16(r)) => Ok(Value::Bool(*l == (*r as i64))),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Bool(*l == (*r as i64))),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Bool(l == r)),
        (Value::Money(l), Value::Money(r)) => Ok(Value::Bool(l == r)),
        (Value::Date(l), Value::Date(r)) => Ok(Value::Bool(l == r)),
        (Value::Time(l), Value::Time(r)) => Ok(Value::Bool(l == r)),
        (Value::TimeTz(l), Value::TimeTz(r)) => Ok(Value::Bool(l == r)),
        (Value::Timestamp(l), Value::Timestamp(r)) => Ok(Value::Bool(l == r)),
        (Value::TimestampTz(l), Value::TimestampTz(r)) => Ok(Value::Bool(l == r)),
        (Value::Bytea(l), Value::Bytea(r)) => Ok(Value::Bool(l == r)),
        (Value::Bit(l), Value::Bit(r)) => Ok(Value::Bool(l == r)),
        (Value::Float64(l), Value::Float64(r)) => Ok(Value::Bool(pg_float_eq(*l, *r))),
        (l, r) if parsed_numeric_value(l).is_some() && parsed_numeric_value(r).is_some() => {
            Ok(Value::Bool(
                parsed_numeric_value(l)
                    .unwrap()
                    .cmp(&parsed_numeric_value(r).unwrap())
                    == Ordering::Equal,
            ))
        }
        (Value::Jsonb(l), Value::Jsonb(r)) => Ok(Value::Bool(
            compare_jsonb(&decode_jsonb(l)?, &decode_jsonb(r)?) == Ordering::Equal,
        )),
        (Value::Range(l), Value::Range(r)) => {
            Ok(Value::Bool(compare_range_values(l, r) == Ordering::Equal))
        }
        (Value::Multirange(l), Value::Multirange(r)) => {
            Ok(Value::Bool(compare_multirange_values(l, r) == Ordering::Equal))
        }
        (Value::TsVector(l), Value::TsVector(r)) => Ok(Value::Bool(l == r)),
        (Value::TsQuery(l), Value::TsQuery(r)) => Ok(Value::Bool(l == r)),
        (Value::Record(l), Value::Record(r)) => {
            Ok(Value::Bool(compare_record_values(l, r) == Ordering::Equal))
        }
        (l, r) if l.as_text().is_some() && r.as_text().is_some() => {
            Ok(Value::Bool(l.as_text() == r.as_text()))
        }
        (Value::Bool(l), Value::Bool(r)) => Ok(Value::Bool(l == r)),
        (l, r) if normalize_array_value(l).is_some() && normalize_array_value(r).is_some() => {
            Ok(Value::Bool(
                compare_array_values(
                    &normalize_array_value(l).unwrap(),
                    &normalize_array_value(r).unwrap(),
                ) == Ordering::Equal,
            ))
        }
        _ => Err(ExecError::TypeMismatch { op, left, right }),
    }
}

pub(crate) fn not_equal_values(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match compare_values("=", left.clone(), right.clone())? {
        Value::Bool(value) => Ok(Value::Bool(!value)),
        other => Err(ExecError::NonBoolQual(other)),
    }
}

pub(crate) fn values_are_distinct(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Null, Value::Null) => false,
        (Value::Null, _) | (_, Value::Null) => true,
        (Value::Int16(l), Value::Int16(r)) => l != r,
        (Value::Int16(l), Value::Int32(r)) => (*l as i32) != *r,
        (Value::Int16(l), Value::Int64(r)) => (*l as i64) != *r,
        (Value::Int32(l), Value::Int32(r)) => l != r,
        (Value::Int32(l), Value::Int16(r)) => *l != (*r as i32),
        (Value::Int32(l), Value::Int64(r)) => (*l as i64) != *r,
        (Value::Int64(l), Value::Int16(r)) => *l != (*r as i64),
        (Value::Int64(l), Value::Int32(r)) => *l != (*r as i64),
        (Value::Int64(l), Value::Int64(r)) => l != r,
        (Value::Money(l), Value::Money(r)) => l != r,
        (Value::Date(l), Value::Date(r)) => l != r,
        (Value::Time(l), Value::Time(r)) => l != r,
        (Value::TimeTz(l), Value::TimeTz(r)) => l != r,
        (Value::Timestamp(l), Value::Timestamp(r)) => l != r,
        (Value::TimestampTz(l), Value::TimestampTz(r)) => l != r,
        (Value::Bytea(l), Value::Bytea(r)) => l != r,
        (Value::Bit(l), Value::Bit(r)) => l != r,
        (Value::Float64(l), Value::Float64(r)) => !pg_float_eq(*l, *r),
        (l, r) if parsed_numeric_value(l).is_some() && parsed_numeric_value(r).is_some() => {
            parsed_numeric_value(l)
                .unwrap()
                .cmp(&parsed_numeric_value(r).unwrap())
                != Ordering::Equal
        }
        (Value::Jsonb(l), Value::Jsonb(r)) => decode_jsonb(l)
            .ok()
            .zip(decode_jsonb(r).ok())
            .map(|(l, r)| compare_jsonb(&l, &r) != Ordering::Equal)
            .unwrap_or(true),
        (Value::Range(l), Value::Range(r)) => compare_range_values(l, r) != Ordering::Equal,
        (Value::Multirange(l), Value::Multirange(r)) => {
            compare_multirange_values(l, r) != Ordering::Equal
        }
        (Value::TsVector(l), Value::TsVector(r)) => l != r,
        (Value::TsQuery(l), Value::TsQuery(r)) => l != r,
        (Value::Record(l), Value::Record(r)) => compare_record_values(l, r) != Ordering::Equal,
        (l, r) if l.as_text().is_some() && r.as_text().is_some() => l.as_text() != r.as_text(),
        (Value::Bool(l), Value::Bool(r)) => l != r,
        (l, r) if normalize_array_value(l).is_some() && normalize_array_value(r).is_some() => {
            compare_array_values(
                &normalize_array_value(l).unwrap(),
                &normalize_array_value(r).unwrap(),
            ) != Ordering::Equal
        }
        _ => true,
    }
}

pub(crate) fn add_values(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match (&left, &right) {
        (Value::Int16(l), Value::Int16(r)) => Ok(Value::Int16(checked_add_i16(*l, *r)?)),
        (Value::Int16(l), Value::Int32(r)) => Ok(Value::Int32(checked_add_i32(*l as i32, *r)?)),
        (Value::Int16(l), Value::Int64(r)) => Ok(Value::Int64(checked_add_i64(*l as i64, *r)?)),
        (Value::Int32(l), Value::Int16(r)) => Ok(Value::Int32(checked_add_i32(*l, *r as i32)?)),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(checked_add_i32(*l, *r)?)),
        (Value::Int32(l), Value::Int64(r)) => Ok(Value::Int64(checked_add_i64(*l as i64, *r)?)),
        (Value::Int64(l), Value::Int16(r)) => Ok(Value::Int64(checked_add_i64(*l, *r as i64)?)),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Int64(checked_add_i64(*l, *r as i64)?)),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(checked_add_i64(*l, *r)?)),
        (Value::Money(l), Value::Money(r)) => Ok(Value::Money(money_add(*l, *r)?)),
        (Value::Float64(l), Value::Float64(r)) => Ok(Value::Float64(l + r)),
        (l, r) if parsed_numeric_value(l).is_some() && parsed_numeric_value(r).is_some() => {
            exact_numeric_binary(l, r, |lv, rv| Some(lv.add(rv)), "+")
        }
        _ => Err(ExecError::TypeMismatch {
            op: "+",
            left,
            right,
        }),
    }
}

pub(crate) fn sub_values(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match (&left, &right) {
        (Value::Int16(l), Value::Int16(r)) => Ok(Value::Int16(checked_sub_i16(*l, *r)?)),
        (Value::Int16(l), Value::Int32(r)) => Ok(Value::Int32(checked_sub_i32(*l as i32, *r)?)),
        (Value::Int16(l), Value::Int64(r)) => Ok(Value::Int64(checked_sub_i64(*l as i64, *r)?)),
        (Value::Int32(l), Value::Int16(r)) => Ok(Value::Int32(checked_sub_i32(*l, *r as i32)?)),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(checked_sub_i32(*l, *r)?)),
        (Value::Int32(l), Value::Int64(r)) => Ok(Value::Int64(checked_sub_i64(*l as i64, *r)?)),
        (Value::Int64(l), Value::Int16(r)) => Ok(Value::Int64(checked_sub_i64(*l, *r as i64)?)),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Int64(checked_sub_i64(*l, *r as i64)?)),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(checked_sub_i64(*l, *r)?)),
        (Value::Date(l), Value::Date(r)) => Ok(Value::Int32(l.0 - r.0)),
        (Value::Money(l), Value::Money(r)) => Ok(Value::Money(money_sub(*l, *r)?)),
        (Value::Float64(l), Value::Float64(r)) => Ok(Value::Float64(l - r)),
        (l, r) if parsed_numeric_value(l).is_some() && parsed_numeric_value(r).is_some() => {
            exact_numeric_binary(l, r, |lv, rv| Some(lv.sub(rv)), "-")
        }
        _ => Err(ExecError::TypeMismatch {
            op: "-",
            left,
            right,
        }),
    }
}

pub(crate) fn mul_values(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match (&left, &right) {
        (Value::Int16(l), Value::Int16(r)) => Ok(Value::Int16(checked_mul_i16(*l, *r)?)),
        (Value::Int16(l), Value::Int32(r)) => Ok(Value::Int32(checked_mul_i32(*l as i32, *r)?)),
        (Value::Int16(l), Value::Int64(r)) => Ok(Value::Int64(checked_mul_i64(*l as i64, *r)?)),
        (Value::Int32(l), Value::Int16(r)) => Ok(Value::Int32(checked_mul_i32(*l, *r as i32)?)),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(checked_mul_i32(*l, *r)?)),
        (Value::Int32(l), Value::Int64(r)) => Ok(Value::Int64(checked_mul_i64(*l as i64, *r)?)),
        (Value::Int64(l), Value::Int16(r)) => Ok(Value::Int64(checked_mul_i64(*l, *r as i64)?)),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Int64(checked_mul_i64(*l, *r as i64)?)),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(checked_mul_i64(*l, *r)?)),
        (Value::Money(l), Value::Int16(r)) => Ok(Value::Money(money_mul_int(*l, i64::from(*r))?)),
        (Value::Money(l), Value::Int32(r)) => Ok(Value::Money(money_mul_int(*l, i64::from(*r))?)),
        (Value::Money(l), Value::Int64(r)) => Ok(Value::Money(money_mul_int(*l, *r)?)),
        (Value::Int16(l), Value::Money(r)) => Ok(Value::Money(money_mul_int(*r, i64::from(*l))?)),
        (Value::Int32(l), Value::Money(r)) => Ok(Value::Money(money_mul_int(*r, i64::from(*l))?)),
        (Value::Int64(l), Value::Money(r)) => Ok(Value::Money(money_mul_int(*r, *l)?)),
        (Value::Money(l), Value::Float64(r)) => Ok(Value::Money(money_mul_float(*l, *r)?)),
        (Value::Float64(l), Value::Money(r)) => Ok(Value::Money(money_mul_float(*r, *l)?)),
        (Value::Float64(l), Value::Float64(r)) => {
            let product = l * r;
            if l.is_finite() && r.is_finite() && *l != 0.0 && *r != 0.0 && product.is_infinite() {
                Err(ExecError::FloatOverflow)
            } else {
                Ok(Value::Float64(product))
            }
        }
        (l, r) if parsed_numeric_value(l).is_some() && parsed_numeric_value(r).is_some() => {
            exact_numeric_binary(l, r, |lv, rv| Some(lv.mul(rv)), "*")
        }
        _ => Err(ExecError::TypeMismatch {
            op: "*",
            left,
            right,
        }),
    }
}

pub(crate) fn shift_left_values(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match (&left, &right) {
        (Value::Bit(l), Value::Int32(r)) => Ok(Value::Bit(shift_left_bits(l, *r))),
        (Value::Int16(l), Value::Int32(r)) => Ok(Value::Int16(l.wrapping_shl(*r as u32))),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(l.wrapping_shl(*r as u32))),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Int64(l.wrapping_shl(*r as u32))),
        _ => Err(ExecError::TypeMismatch {
            op: "<<",
            left,
            right,
        }),
    }
}

pub(crate) fn shift_right_values(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match (&left, &right) {
        (Value::Bit(l), Value::Int32(r)) => Ok(Value::Bit(shift_right_bits(l, *r))),
        (Value::Int16(l), Value::Int32(r)) => Ok(Value::Int16(l.wrapping_shr(*r as u32))),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(l.wrapping_shr(*r as u32))),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Int64(l.wrapping_shr(*r as u32))),
        _ => Err(ExecError::TypeMismatch {
            op: ">>",
            left,
            right,
        }),
    }
}

pub(crate) fn bitwise_and_values(left: Value, right: Value) -> Result<Value, ExecError> {
    match (left, right) {
        (Value::Bit(l), Value::Bit(r)) => Ok(Value::Bit(bitwise_binary_bits("&", &l, &r)?)),
        (Value::Int16(l), Value::Int16(r)) => Ok(Value::Int16(l & r)),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(l & r)),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l & r)),
        (l, r) => Err(ExecError::TypeMismatch {
            op: "&",
            left: l,
            right: r,
        }),
    }
}

pub(crate) fn bitwise_or_values(left: Value, right: Value) -> Result<Value, ExecError> {
    match (left, right) {
        (Value::Bit(l), Value::Bit(r)) => Ok(Value::Bit(bitwise_binary_bits("|", &l, &r)?)),
        (Value::Int16(l), Value::Int16(r)) => Ok(Value::Int16(l | r)),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(l | r)),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l | r)),
        (l, r) => Err(ExecError::TypeMismatch {
            op: "|",
            left: l,
            right: r,
        }),
    }
}

pub(crate) fn bitwise_xor_values(left: Value, right: Value) -> Result<Value, ExecError> {
    match (left, right) {
        (Value::Bit(l), Value::Bit(r)) => Ok(Value::Bit(bitwise_binary_bits("#", &l, &r)?)),
        (Value::Int16(l), Value::Int16(r)) => Ok(Value::Int16(l ^ r)),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(l ^ r)),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l ^ r)),
        (l, r) => Err(ExecError::TypeMismatch {
            op: "#",
            left: l,
            right: r,
        }),
    }
}

pub(crate) fn bitwise_not_value(value: Value) -> Result<Value, ExecError> {
    match value {
        Value::Bit(bits) => Ok(Value::Bit(bitwise_not_bits(&bits))),
        Value::Int16(v) => Ok(Value::Int16(!v)),
        Value::Int32(v) => Ok(Value::Int32(!v)),
        Value::Int64(v) => Ok(Value::Int64(!v)),
        other => Err(ExecError::TypeMismatch {
            op: "~",
            left: other,
            right: Value::Null,
        }),
    }
}

pub(crate) fn div_values(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    if let (Value::Float64(l), Value::Float64(r)) = (&left, &right) {
        if *r == 0.0 && l.is_nan() {
            return Ok(Value::Float64(f64::NAN));
        }
    }
    if matches!(
        (&left, &right),
        (Value::Numeric(_) | Value::Text(_) | Value::TextRef(_, _), _)
            | (_, Value::Numeric(_) | Value::Text(_) | Value::TextRef(_, _))
    ) && let (Some(left_num), Some(right_num)) =
        (parsed_numeric_value(&left), parsed_numeric_value(&right))
    {
        if right_num == NumericValue::zero() {
            return if matches!(left_num, NumericValue::NaN) {
                Ok(Value::Numeric(NumericValue::NaN))
            } else {
                Err(ExecError::DivisionByZero("/"))
            };
        }
        let out_scale = select_div_scale_numeric(&left_num, &right_num);
        return exact_numeric_binary(&left, &right, |lv, rv| lv.div(rv, out_scale), "/");
    }
    let zero = match &right {
        Value::Int16(v) => *v == 0,
        Value::Int32(v) => *v == 0,
        Value::Int64(v) => *v == 0,
        Value::Float64(v) => *v == 0.0,
        Value::Money(v) => *v == 0,
        _ => false,
    };
    if zero {
        return Err(ExecError::DivisionByZero("/"));
    }
    match (&left, &right) {
        (Value::Int16(l), Value::Int16(r)) => Ok(Value::Int16(checked_div_i16(*l, *r)?)),
        (Value::Int16(l), Value::Int32(r)) => Ok(Value::Int32(checked_div_i32(*l as i32, *r)?)),
        (Value::Int16(l), Value::Int64(r)) => Ok(Value::Int64(checked_div_i64(*l as i64, *r)?)),
        (Value::Int32(l), Value::Int16(r)) => Ok(Value::Int32(checked_div_i32(*l, *r as i32)?)),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(checked_div_i32(*l, *r)?)),
        (Value::Int32(l), Value::Int64(r)) => Ok(Value::Int64(checked_div_i64(*l as i64, *r)?)),
        (Value::Int64(l), Value::Int16(r)) => Ok(Value::Int64(checked_div_i64(*l, *r as i64)?)),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Int64(checked_div_i64(*l, *r as i64)?)),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(checked_div_i64(*l, *r)?)),
        (Value::Money(l), Value::Money(r)) => Ok(Value::Float64(money_cash_div(*l, *r)?)),
        (Value::Money(l), Value::Int16(r)) => Ok(Value::Money(money_div_int(*l, i64::from(*r))?)),
        (Value::Money(l), Value::Int32(r)) => Ok(Value::Money(money_div_int(*l, i64::from(*r))?)),
        (Value::Money(l), Value::Int64(r)) => Ok(Value::Money(money_div_int(*l, *r)?)),
        (Value::Money(l), Value::Float64(r)) => Ok(Value::Money(money_div_float(*l, *r)?)),
        (Value::Float64(l), Value::Float64(r)) => Ok(Value::Float64(l / r)),
        _ => Err(ExecError::TypeMismatch {
            op: "/",
            left,
            right,
        }),
    }
}

pub(crate) fn mod_values(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    if matches!(
        (&left, &right),
        (Value::Numeric(_) | Value::Text(_) | Value::TextRef(_, _), _)
            | (_, Value::Numeric(_) | Value::Text(_) | Value::TextRef(_, _))
    ) && let (Some(left_num), Some(right_num)) =
        (parsed_numeric_value(&left), parsed_numeric_value(&right))
    {
        if right_num == NumericValue::zero() {
            return if matches!(left_num, NumericValue::NaN) {
                Ok(Value::Numeric(NumericValue::NaN))
            } else {
                Err(ExecError::DivisionByZero("%"))
            };
        }
        return exact_numeric_binary(&left, &right, |lv, rv| lv.rem(rv), "%");
    }
    let zero = match &right {
        Value::Int16(v) => *v == 0,
        Value::Int32(v) => *v == 0,
        Value::Int64(v) => *v == 0,
        _ => false,
    };
    if zero {
        return Err(ExecError::DivisionByZero("%"));
    }
    match (&left, &right) {
        (Value::Int16(l), Value::Int16(r)) => Ok(Value::Int16(checked_rem_i16(*l, *r)?)),
        (Value::Int16(l), Value::Int32(r)) => Ok(Value::Int32(checked_rem_i32(*l as i32, *r)?)),
        (Value::Int16(l), Value::Int64(r)) => Ok(Value::Int64(checked_rem_i64(*l as i64, *r)?)),
        (Value::Int32(l), Value::Int16(r)) => Ok(Value::Int32(checked_rem_i32(*l, *r as i32)?)),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Int32(checked_rem_i32(*l, *r)?)),
        (Value::Int32(l), Value::Int64(r)) => Ok(Value::Int64(checked_rem_i64(*l as i64, *r)?)),
        (Value::Int64(l), Value::Int16(r)) => Ok(Value::Int64(checked_rem_i64(*l, *r as i64)?)),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Int64(checked_rem_i64(*l, *r as i64)?)),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(checked_rem_i64(*l, *r)?)),
        _ => Err(ExecError::TypeMismatch {
            op: "%",
            left,
            right,
        }),
    }
}

pub(crate) fn concat_values(left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    if let (Some(mut left_array), Some(right_array)) =
        (normalize_array_value(&left), normalize_array_value(&right))
    {
        left_array.elements.extend(right_array.elements);
        left_array.dimensions = vec![ArrayDimension {
            lower_bound: 1,
            length: left_array.elements.len(),
        }];
        return Ok(Value::PgArray(left_array));
    }
    if let Some(mut left_array) = normalize_array_value(&left) {
        left_array.elements.push(right);
        left_array.dimensions = vec![ArrayDimension {
            lower_bound: 1,
            length: left_array.elements.len(),
        }];
        return Ok(Value::PgArray(left_array));
    }
    if let Some(right_array) = normalize_array_value(&right) {
        let mut elements = Vec::with_capacity(right_array.elements.len() + 1);
        elements.push(left);
        elements.extend(right_array.elements);
        return Ok(Value::PgArray(ArrayValue::from_1d(elements)));
    }
    match (&left, &right) {
        (Value::Bit(l), Value::Bit(r)) => Ok(Value::Bit(concat_bit_strings(l, r))),
        (Value::Jsonb(l), Value::Jsonb(r)) => Ok(Value::Jsonb(encode_jsonb(&jsonb_concat(
            &decode_jsonb(l)?,
            &decode_jsonb(r)?,
        )))),
        (Value::TsVector(l), Value::TsVector(r)) => Ok(Value::TsVector(
            crate::backend::executor::concat_tsvector(l, r),
        )),
        (Value::TsQuery(l), Value::TsQuery(r)) => Ok(Value::TsQuery(
            crate::backend::executor::tsquery_or(l.clone(), r.clone()),
        )),
        (Value::Array(l), Value::Array(r)) => {
            let mut items = l.clone();
            items.extend(r.iter().cloned());
            Ok(Value::Array(items))
        }
        (Value::Array(l), _) => {
            let mut items = l.clone();
            items.push(right);
            Ok(Value::Array(items))
        }
        (_, Value::Array(r)) => {
            let mut items = Vec::with_capacity(r.len() + 1);
            items.push(left);
            items.extend(r.iter().cloned());
            Ok(Value::Array(items))
        }
        _ => {
            let text_type = SqlType::new(SqlTypeKind::Text);
            let left_text = cast_value(left, text_type)?;
            let right_text = cast_value(right, text_type)?;
            let mut out = String::new();
            out.push_str(left_text.as_text().ok_or_else(|| ExecError::TypeMismatch {
                op: "||",
                left: left_text.clone(),
                right: right_text.clone(),
            })?);
            out.push_str(
                right_text
                    .as_text()
                    .ok_or_else(|| ExecError::TypeMismatch {
                        op: "||",
                        left: left_text.clone(),
                        right: right_text.clone(),
                    })?,
            );
            Ok(Value::Text(CompactString::from_owned(out)))
        }
    }
}

pub(crate) fn negate_value(value: Value) -> Result<Value, ExecError> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::Int16(v) => Ok(Value::Int16(checked_neg_i16(v)?)),
        Value::Int32(v) => Ok(Value::Int32(checked_neg_i32(v)?)),
        Value::Int64(v) => Ok(Value::Int64(checked_neg_i64(v)?)),
        Value::Money(v) => Ok(Value::Money(checked_neg_i64(v)?)),
        Value::Float64(v) => Ok(Value::Float64(-v)),
        Value::Numeric(v) => Ok(Value::Numeric(v.negate())),
        other => Err(ExecError::TypeMismatch {
            op: "unary -",
            left: other,
            right: Value::Null,
        }),
    }
}

pub(crate) fn order_values(
    op: &'static str,
    left: Value,
    right: Value,
) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match (&left, &right) {
        (Value::Int16(l), Value::Int16(r)) => Ok(Value::Bool(compare_ord(*l, *r, op))),
        (Value::Int16(l), Value::Int32(r)) => Ok(Value::Bool(compare_ord(*l as i32, *r, op))),
        (Value::Int16(l), Value::Int64(r)) => Ok(Value::Bool(compare_ord(*l as i64, *r, op))),
        (Value::Int32(l), Value::Int16(r)) => Ok(Value::Bool(compare_ord(*l, *r as i32, op))),
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Bool(compare_ord(*l, *r, op))),
        (Value::Int32(l), Value::Int64(r)) => Ok(Value::Bool(compare_ord(*l as i64, *r, op))),
        (Value::Int64(l), Value::Int16(r)) => Ok(Value::Bool(compare_ord(*l, *r as i64, op))),
        (Value::Int64(l), Value::Int32(r)) => Ok(Value::Bool(compare_ord(*l, *r as i64, op))),
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Bool(compare_ord(*l, *r, op))),
        (Value::Money(l), Value::Money(r)) => Ok(Value::Bool(compare_ord(*l, *r, op))),
        (Value::Bit(l), Value::Bit(r)) => {
            let ordering = compare_bit_strings(l, r);
            Ok(Value::Bool(match op {
                "<" => ordering == Ordering::Less,
                "<=" => ordering != Ordering::Greater,
                ">" => ordering == Ordering::Greater,
                ">=" => ordering != Ordering::Less,
                _ => unreachable!(),
            }))
        }
        (Value::Bytea(l), Value::Bytea(r)) => Ok(Value::Bool(compare_ord(l, r, op))),
        (Value::Date(l), Value::Date(r)) => Ok(Value::Bool(compare_ord(*l, *r, op))),
        (Value::Float64(l), Value::Float64(r)) => Ok(Value::Bool(match op {
            "<" => pg_float_cmp(*l, *r) == Ordering::Less,
            "<=" => pg_float_cmp(*l, *r) != Ordering::Greater,
            ">" => pg_float_cmp(*l, *r) == Ordering::Greater,
            ">=" => pg_float_cmp(*l, *r) != Ordering::Less,
            _ => unreachable!(),
        })),
        (Value::Bool(_), Value::Bool(_)) => order_bool_values(op, &left, &right),
        (l, r) if parsed_numeric_value(l).is_some() && parsed_numeric_value(r).is_some() => {
            let ordering = parsed_numeric_value(l)
                .zip(parsed_numeric_value(r))
                .map(|(lv, rv)| lv.cmp(&rv))
                .ok_or_else(|| ExecError::TypeMismatch {
                    op,
                    left: left.clone(),
                    right: right.clone(),
                })?;
            Ok(Value::Bool(match op {
                "<" => ordering == Ordering::Less,
                "<=" => ordering != Ordering::Greater,
                ">" => ordering == Ordering::Greater,
                ">=" => ordering != Ordering::Less,
                _ => unreachable!(),
            }))
        }
        (Value::Jsonb(l), Value::Jsonb(r)) => {
            let ordering = compare_jsonb(&decode_jsonb(l)?, &decode_jsonb(r)?);
            Ok(Value::Bool(match op {
                "<" => ordering == Ordering::Less,
                "<=" => ordering != Ordering::Greater,
                ">" => ordering == Ordering::Greater,
                ">=" => ordering != Ordering::Less,
                _ => unreachable!(),
            }))
        }
        (Value::Range(l), Value::Range(r)) => {
            let ordering = compare_range_values(l, r);
            Ok(Value::Bool(match op {
                "<" => ordering == Ordering::Less,
                "<=" => ordering != Ordering::Greater,
                ">" => ordering == Ordering::Greater,
                ">=" => ordering != Ordering::Less,
                _ => unreachable!(),
            }))
        }
        (Value::Multirange(l), Value::Multirange(r)) => {
            let ordering = compare_multirange_values(l, r);
            Ok(Value::Bool(match op {
                "<" => ordering == Ordering::Less,
                "<=" => ordering != Ordering::Greater,
                ">" => ordering == Ordering::Greater,
                ">=" => ordering != Ordering::Less,
                _ => unreachable!(),
            }))
        }
        (l, r) if l.as_text().is_some() && r.as_text().is_some() => Ok(Value::Bool(match op {
            "<" => l.as_text().unwrap() < r.as_text().unwrap(),
            "<=" => l.as_text().unwrap() <= r.as_text().unwrap(),
            ">" => l.as_text().unwrap() > r.as_text().unwrap(),
            ">=" => l.as_text().unwrap() >= r.as_text().unwrap(),
            _ => unreachable!(),
        })),
        (l, r) if normalize_array_value(l).is_some() && normalize_array_value(r).is_some() => {
            Ok(Value::Bool(compare_ord(
                compare_array_values(
                    &normalize_array_value(l).unwrap(),
                    &normalize_array_value(r).unwrap(),
                ),
                Ordering::Equal,
                op,
            )))
        }
        _ => Err(ExecError::TypeMismatch { op, left, right }),
    }
}

fn compare_ord<T: Ord>(left: T, right: T, op: &'static str) -> bool {
    match op {
        "<" => left < right,
        "<=" => left <= right,
        ">" => left > right,
        ">=" => left >= right,
        _ => unreachable!(),
    }
}

fn compare_record_values(
    left: &crate::include::nodes::datum::RecordValue,
    right: &crate::include::nodes::datum::RecordValue,
) -> Ordering {
    for (left_value, right_value) in left.fields.iter().zip(&right.fields) {
        let value_ordering = compare_order_values(left_value, right_value, None, false);
        if value_ordering != Ordering::Equal {
            return value_ordering;
        }
    }
    left.fields.len().cmp(&right.fields.len())
}

fn normalize_array_value(value: &Value) -> Option<ArrayValue> {
    match value {
        Value::PgArray(array) => Some(array.clone()),
        Value::Array(items) => Some(ArrayValue::from_1d(items.clone())),
        _ => None,
    }
}

fn compare_array_values(left: &ArrayValue, right: &ArrayValue) -> Ordering {
    for (left_item, right_item) in left.elements.iter().zip(right.elements.iter()) {
        match (left_item, right_item) {
            (Value::Null, Value::Null) => {}
            (Value::Null, _) => return Ordering::Greater,
            (_, Value::Null) => return Ordering::Less,
            _ => {
                if matches!(
                    compare_values("=", left_item.clone(), right_item.clone()),
                    Ok(Value::Bool(true))
                ) {
                    continue;
                }
                if matches!(
                    order_values("<", left_item.clone(), right_item.clone()),
                    Ok(Value::Bool(true))
                ) {
                    return Ordering::Less;
                }
                return Ordering::Greater;
            }
        }
    }
    left.elements
        .len()
        .cmp(&right.elements.len())
        .then_with(|| left.dimensions.len().cmp(&right.dimensions.len()))
        .then_with(|| {
            left.dimensions
                .iter()
                .map(|dim| dim.length)
                .cmp(right.dimensions.iter().map(|dim| dim.length))
        })
        .then_with(|| {
            left.dimensions
                .iter()
                .map(|dim| dim.lower_bound)
                .cmp(right.dimensions.iter().map(|dim| dim.lower_bound))
        })
}

fn pg_float_eq(left: f64, right: f64) -> bool {
    if left.is_nan() && right.is_nan() {
        true
    } else {
        left == right
    }
}

fn pg_float_cmp(left: f64, right: f64) -> Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => left.partial_cmp(&right).unwrap_or(Ordering::Equal),
    }
}

impl NumericValue {
    pub(crate) fn round_to_scale(&self, target_scale: u32) -> Option<Self> {
        match self {
            Self::PosInf => Some(Self::PosInf),
            Self::NegInf => Some(Self::NegInf),
            Self::NaN => Some(Self::NaN),
            Self::Finite { coeff, scale, .. } => {
                if *scale <= target_scale {
                    let factor = pow10_bigint(target_scale - *scale);
                    return Some(
                        Self::finite(coeff * factor, target_scale).with_dscale(target_scale),
                    );
                }
                let diff = *scale - target_scale;
                let factor = pow10_bigint(diff);
                let (quotient, remainder) = coeff.div_rem(&factor);
                let twice = remainder.abs() * 2u8;
                let rounded = if twice >= factor.abs() {
                    quotient + coeff.signum()
                } else {
                    quotient
                };
                Some(Self::finite(rounded, target_scale).with_dscale(target_scale))
            }
        }
    }

    pub(crate) fn add(&self, other: &Self) -> Self {
        match (self, other) {
            (Self::NaN, _) | (_, Self::NaN) => Self::NaN,
            (Self::PosInf, Self::NegInf) | (Self::NegInf, Self::PosInf) => Self::NaN,
            (Self::PosInf, _) | (_, Self::PosInf) => Self::PosInf,
            (Self::NegInf, _) | (_, Self::NegInf) => Self::NegInf,
            (
                Self::Finite {
                    coeff: lcoeff,
                    scale: lscale,
                    ..
                },
                Self::Finite {
                    coeff: rcoeff,
                    scale: rscale,
                    ..
                },
            ) => {
                let scale = (*lscale).max(*rscale);
                let left = align_coeff(lcoeff.clone(), *lscale, scale);
                let right = align_coeff(rcoeff.clone(), *rscale, scale);
                Self::finite(left + right, scale)
                    .with_dscale(scale)
                    .normalize()
            }
        }
    }

    pub(crate) fn sub(&self, other: &Self) -> Self {
        self.add(&other.negate())
    }

    pub(crate) fn mul(&self, other: &Self) -> Self {
        match (self, other) {
            (Self::NaN, _) | (_, Self::NaN) => Self::NaN,
            (Self::PosInf | Self::NegInf, Self::Finite { coeff, .. })
            | (Self::Finite { coeff, .. }, Self::PosInf | Self::NegInf)
                if coeff.is_zero() =>
            {
                Self::NaN
            }
            (Self::PosInf, Self::PosInf) | (Self::NegInf, Self::NegInf) => Self::PosInf,
            (Self::PosInf, Self::NegInf) | (Self::NegInf, Self::PosInf) => Self::NegInf,
            (Self::PosInf, Self::Finite { coeff, .. })
            | (Self::Finite { coeff, .. }, Self::PosInf) => {
                if coeff.is_negative() {
                    Self::NegInf
                } else {
                    Self::PosInf
                }
            }
            (Self::NegInf, Self::Finite { coeff, .. })
            | (Self::Finite { coeff, .. }, Self::NegInf) => {
                if coeff.is_negative() {
                    Self::PosInf
                } else {
                    Self::NegInf
                }
            }
            (
                Self::Finite {
                    coeff: lcoeff,
                    scale: lscale,
                    ..
                },
                Self::Finite {
                    coeff: rcoeff,
                    scale: rscale,
                    ..
                },
            ) => Self::finite(lcoeff * rcoeff, lscale.saturating_add(*rscale)).normalize(),
        }
    }

    fn rem(&self, other: &Self) -> Option<Self> {
        match (self, other) {
            (Self::NaN, _) | (_, Self::NaN) => Some(Self::NaN),
            (Self::PosInf | Self::NegInf, _) => Some(Self::NaN),
            (_, Self::PosInf | Self::NegInf) => Some(self.clone()),
            (_, Self::Finite { coeff, .. }) if coeff.is_zero() => None,
            (
                Self::Finite {
                    coeff: lcoeff,
                    scale: lscale,
                    ..
                },
                Self::Finite {
                    coeff: rcoeff,
                    scale: rscale,
                    ..
                },
            ) => {
                let scale = (*lscale).max(*rscale);
                let left = align_coeff(lcoeff.clone(), *lscale, scale);
                let right = align_coeff(rcoeff.clone(), *rscale, scale);
                Some(
                    Self::finite(left % right, scale)
                        .with_dscale(scale)
                        .normalize(),
                )
            }
        }
    }

    pub(crate) fn div(&self, other: &Self, out_scale: u32) -> Option<Self> {
        match (self, other) {
            (Self::NaN, _) | (_, Self::NaN) => Some(Self::NaN),
            (_, Self::Finite { coeff, .. }) if coeff.is_zero() => None,
            (Self::PosInf | Self::NegInf, Self::PosInf | Self::NegInf) => Some(Self::NaN),
            (Self::PosInf, Self::Finite { coeff, .. }) => Some(if coeff.is_negative() {
                Self::NegInf
            } else {
                Self::PosInf
            }),
            (Self::NegInf, Self::Finite { coeff, .. }) => Some(if coeff.is_negative() {
                Self::PosInf
            } else {
                Self::NegInf
            }),
            (Self::Finite { .. }, Self::PosInf | Self::NegInf) => Some(Self::zero()),
            (
                Self::Finite {
                    coeff: lcoeff,
                    scale: lscale,
                    ..
                },
                Self::Finite {
                    coeff: rcoeff,
                    scale: rscale,
                    ..
                },
            ) => {
                let exp = (out_scale as i64) + (*rscale as i64) - (*lscale as i64);
                let num = if exp >= 0 {
                    lcoeff * pow10_bigint(exp as u32)
                } else {
                    lcoeff / pow10_bigint((-exp) as u32)
                };
                let (quotient, remainder) = num.div_rem(rcoeff);
                let twice = remainder.abs() * 2u8;
                let rounded = if twice >= rcoeff.abs() {
                    quotient + (num.signum() * rcoeff.signum())
                } else {
                    quotient
                };
                Some(
                    Self::finite(rounded, out_scale)
                        .with_dscale(out_scale)
                        .normalize(),
                )
            }
        }
    }

    pub(crate) fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::NaN, Self::NaN) => Ordering::Equal,
            (Self::NaN, _) => Ordering::Greater,
            (_, Self::NaN) => Ordering::Less,
            (Self::PosInf, Self::PosInf) | (Self::NegInf, Self::NegInf) => Ordering::Equal,
            (Self::PosInf, _) => Ordering::Greater,
            (_, Self::PosInf) => Ordering::Less,
            (Self::NegInf, _) => Ordering::Less,
            (_, Self::NegInf) => Ordering::Greater,
            (
                Self::Finite {
                    coeff: lcoeff,
                    scale: lscale,
                    ..
                },
                Self::Finite {
                    coeff: rcoeff,
                    scale: rscale,
                    ..
                },
            ) => {
                let scale = (*lscale).max(*rscale);
                let left = align_coeff(lcoeff.clone(), *lscale, scale);
                let right = align_coeff(rcoeff.clone(), *rscale, scale);
                left.cmp(&right)
            }
        }
    }
}

fn numeric_pg_weight_and_first_digit(value: &NumericValue) -> Option<(i32, i32)> {
    let NumericValue::Finite { coeff, scale, .. } = value else {
        return None;
    };
    if coeff.is_zero() {
        return Some((0, 0));
    }

    let digits = coeff.abs().to_str_radix(10);
    let decimal_pos = digits.len() as i32 - *scale as i32;
    let weight = (decimal_pos - 1).div_euclid(4);
    let first_group_exp = weight * 4;
    let shift = *scale as i32 + first_group_exp;
    let first_digit = if shift >= 0 {
        (coeff.abs() / pow10_bigint(shift as u32)).to_i32()?
    } else {
        (coeff.abs() * pow10_bigint((-shift) as u32)).to_i32()?
    };
    Some((weight, first_digit))
}

fn select_div_scale_numeric(left: &NumericValue, right: &NumericValue) -> u32 {
    let (weight1, first1) = numeric_pg_weight_and_first_digit(left).unwrap_or((0, 0));
    let (weight2, first2) = numeric_pg_weight_and_first_digit(right).unwrap_or((0, 0));
    let mut qweight = weight1 - weight2;
    if first1 <= first2 {
        qweight -= 1;
    }

    let mut rscale = 16 - qweight * 4;
    if let NumericValue::Finite { dscale, .. } = left {
        rscale = rscale.max(*dscale as i32);
    }
    if let NumericValue::Finite { dscale, .. } = right {
        rscale = rscale.max(*dscale as i32);
    }
    rscale.clamp(0, 1000) as u32
}

fn checked_div_i16(left: i16, right: i16) -> Result<i16, ExecError> {
    left.checked_div(right).ok_or(ExecError::Int2OutOfRange)
}

fn checked_div_i32(left: i32, right: i32) -> Result<i32, ExecError> {
    left.checked_div(right).ok_or(ExecError::Int4OutOfRange)
}

fn checked_div_i64(left: i64, right: i64) -> Result<i64, ExecError> {
    left.checked_div(right).ok_or(ExecError::Int8OutOfRange)
}

fn checked_add_i16(left: i16, right: i16) -> Result<i16, ExecError> {
    left.checked_add(right).ok_or(ExecError::Int2OutOfRange)
}

fn checked_add_i32(left: i32, right: i32) -> Result<i32, ExecError> {
    left.checked_add(right).ok_or(ExecError::Int4OutOfRange)
}

fn checked_add_i64(left: i64, right: i64) -> Result<i64, ExecError> {
    left.checked_add(right).ok_or(ExecError::Int8OutOfRange)
}

fn checked_sub_i16(left: i16, right: i16) -> Result<i16, ExecError> {
    left.checked_sub(right).ok_or(ExecError::Int2OutOfRange)
}

fn checked_sub_i32(left: i32, right: i32) -> Result<i32, ExecError> {
    left.checked_sub(right).ok_or(ExecError::Int4OutOfRange)
}

fn checked_sub_i64(left: i64, right: i64) -> Result<i64, ExecError> {
    left.checked_sub(right).ok_or(ExecError::Int8OutOfRange)
}

fn checked_mul_i16(left: i16, right: i16) -> Result<i16, ExecError> {
    left.checked_mul(right).ok_or(ExecError::Int2OutOfRange)
}

fn checked_mul_i32(left: i32, right: i32) -> Result<i32, ExecError> {
    left.checked_mul(right).ok_or(ExecError::Int4OutOfRange)
}

fn checked_mul_i64(left: i64, right: i64) -> Result<i64, ExecError> {
    left.checked_mul(right).ok_or(ExecError::Int8OutOfRange)
}

fn checked_neg_i16(value: i16) -> Result<i16, ExecError> {
    value.checked_neg().ok_or(ExecError::Int2OutOfRange)
}

fn checked_neg_i32(value: i32) -> Result<i32, ExecError> {
    value.checked_neg().ok_or(ExecError::Int4OutOfRange)
}

fn checked_neg_i64(value: i64) -> Result<i64, ExecError> {
    value.checked_neg().ok_or(ExecError::Int8OutOfRange)
}

fn checked_rem_i16(left: i16, right: i16) -> Result<i16, ExecError> {
    if right == -1 {
        return Ok(0);
    }
    left.checked_rem(right).ok_or(ExecError::Int2OutOfRange)
}

fn checked_rem_i32(left: i32, right: i32) -> Result<i32, ExecError> {
    if right == -1 {
        return Ok(0);
    }
    left.checked_rem(right).ok_or(ExecError::Int4OutOfRange)
}

fn checked_rem_i64(left: i64, right: i64) -> Result<i64, ExecError> {
    if right == -1 {
        return Ok(0);
    }
    left.checked_rem(right).ok_or(ExecError::Int8OutOfRange)
}

fn align_coeff(coeff: BigInt, from_scale: u32, to_scale: u32) -> BigInt {
    coeff * pow10_bigint(to_scale - from_scale)
}

fn pow10_bigint(exp: u32) -> BigInt {
    let mut value = BigInt::from(1u8);
    for _ in 0..exp {
        value *= 10u8;
    }
    value
}

pub(crate) fn parse_numeric_text(text: &str) -> Option<NumericValue> {
    let trimmed = text.trim();
    if trimmed.eq_ignore_ascii_case("nan") {
        return Some(NumericValue::NaN);
    }
    if trimmed.is_empty() {
        return None;
    }
    match trimmed.to_ascii_lowercase().as_str() {
        "inf" | "+inf" | "infinity" | "+infinity" => return Some(NumericValue::PosInf),
        "-inf" | "-infinity" => return Some(NumericValue::NegInf),
        _ => {}
    }
    if trimmed.chars().any(|ch| ch.is_ascii_whitespace()) {
        return None;
    }

    let (negative, unsigned) = if let Some(rest) = trimmed.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = trimmed.strip_prefix('+') {
        (false, rest)
    } else {
        (false, trimmed)
    };

    if let Some(rest) = unsigned
        .strip_prefix("0x")
        .or_else(|| unsigned.strip_prefix("0X"))
    {
        let rest = rest.strip_prefix('_').unwrap_or(rest);
        let digits = normalize_numeric_digits(rest, |ch| ch.is_ascii_hexdigit())?;
        let mut coeff = BigInt::parse_bytes(digits.as_bytes(), 16)?;
        if negative {
            coeff = -coeff;
        }
        return Some(NumericValue::finite(coeff, 0).normalize());
    }
    if let Some(rest) = unsigned
        .strip_prefix("0o")
        .or_else(|| unsigned.strip_prefix("0O"))
    {
        let rest = rest.strip_prefix('_').unwrap_or(rest);
        let digits = normalize_numeric_digits(rest, |ch| matches!(ch, '0'..='7'))?;
        let mut coeff = BigInt::parse_bytes(digits.as_bytes(), 8)?;
        if negative {
            coeff = -coeff;
        }
        return Some(NumericValue::finite(coeff, 0).normalize());
    }
    if let Some(rest) = unsigned
        .strip_prefix("0b")
        .or_else(|| unsigned.strip_prefix("0B"))
    {
        let rest = rest.strip_prefix('_').unwrap_or(rest);
        let digits = normalize_numeric_digits(rest, |ch| matches!(ch, '0' | '1'))?;
        let mut coeff = BigInt::parse_bytes(digits.as_bytes(), 2)?;
        if negative {
            coeff = -coeff;
        }
        return Some(NumericValue::finite(coeff, 0).normalize());
    }

    let (mantissa, exponent) = match trimmed.find(['e', 'E']) {
        Some(index) => {
            let exponent = parse_numeric_exponent(&trimmed[index + 1..])?;
            (&trimmed[..index], exponent)
        }
        None => (trimmed, 0),
    };
    let unsigned = mantissa.strip_prefix(['+', '-']).unwrap_or(mantissa);
    let parts: Vec<&str> = unsigned.split('.').collect();
    if parts.len() > 2 {
        return None;
    }
    let whole = parts[0];
    let frac = parts.get(1).copied().unwrap_or("");
    if whole.is_empty() && frac.is_empty() {
        return None;
    }
    let whole = normalize_numeric_decimal_component(whole, true)?;
    let frac = normalize_numeric_decimal_component(frac, true)?;
    let mut digits = format!("{whole}{frac}");
    if digits.is_empty() {
        digits.push('0');
    }
    let mut scale = frac.len() as i32 - exponent;
    if scale < 0 {
        digits.extend(std::iter::repeat_n('0', (-scale) as usize));
        scale = 0;
    }
    let mut coeff = digits.parse::<BigInt>().ok()?;
    if negative {
        coeff = -coeff;
    }
    Some(NumericValue::finite(coeff, scale as u32).normalize())
}

fn normalize_numeric_decimal_component(component: &str, allow_empty: bool) -> Option<String> {
    if component.is_empty() {
        return allow_empty.then(String::new);
    }
    normalize_numeric_digits(component, |ch| ch.is_ascii_digit())
}

fn normalize_numeric_digits(digits: &str, valid_digit: impl Fn(char) -> bool) -> Option<String> {
    if digits.is_empty()
        || digits.starts_with('_')
        || digits.ends_with('_')
        || digits.contains("__")
    {
        return None;
    }
    let normalized: String = digits.chars().filter(|&ch| ch != '_').collect();
    if normalized.is_empty() || !normalized.chars().all(valid_digit) {
        return None;
    }
    Some(normalized)
}

fn parse_numeric_exponent(text: &str) -> Option<i32> {
    let (negative, digits) = if let Some(rest) = text.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = text.strip_prefix('+') {
        (false, rest)
    } else {
        (false, text)
    };
    let digits = normalize_numeric_digits(digits, |ch| ch.is_ascii_digit())?;
    let value = digits.parse::<i32>().ok()?;
    Some(if negative { -value } else { value })
}

fn parsed_numeric_value(value: &Value) -> Option<NumericValue> {
    match value {
        Value::Int16(v) => Some(NumericValue::from_i64(*v as i64)),
        Value::Int32(v) => Some(NumericValue::from_i64(*v as i64)),
        Value::Int64(v) => Some(NumericValue::from_i64(*v)),
        Value::Numeric(v) => Some(v.clone()),
        Value::Float64(_) => None,
        _ => None,
    }
}

fn exact_numeric_binary(
    left: &Value,
    right: &Value,
    op: impl Fn(&NumericValue, &NumericValue) -> Option<NumericValue>,
    opname: &'static str,
) -> Result<Value, ExecError> {
    let left_num = parsed_numeric_value(left).ok_or_else(|| ExecError::TypeMismatch {
        op: opname,
        left: left.clone(),
        right: right.clone(),
    })?;
    let right_num = parsed_numeric_value(right).ok_or_else(|| ExecError::TypeMismatch {
        op: opname,
        left: left.clone(),
        right: right.clone(),
    })?;
    let result = op(&left_num, &right_num).ok_or_else(|| ExecError::TypeMismatch {
        op: opname,
        left: left.clone(),
        right: right.clone(),
    })?;
    Ok(Value::Numeric(result))
}
