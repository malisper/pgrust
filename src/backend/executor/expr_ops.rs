use std::cmp::Ordering;

use num_bigint::BigInt;
use num_integer::Integer;
use num_traits::{Signed, ToPrimitive, Zero};

use super::ExecError;
use super::exec_expr::{append_array_value, concatenate_arrays};
use super::expr_bit::{
    bitwise_binary as bitwise_binary_bits, bitwise_not as bitwise_not_bits, compare_bit_strings,
    concat_bit_strings, shift_left as shift_left_bits, shift_right as shift_right_bits,
};
use super::expr_bool::order_bool_values;
use super::expr_casts::{cast_value, pg_lsn_out_of_range};
use super::expr_money::{
    money_add, money_cash_div, money_cmp, money_div_float, money_div_int, money_mul_float,
    money_mul_int, money_sub,
};
use super::node_types::*;
use super::{compare_multirange_values, expr_range::compare_range_values};
use crate::backend::executor::jsonb::{
    JsonbValue, compare_jsonb, decode_jsonb, encode_jsonb, jsonb_concat,
};
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{C_COLLATION_OID, DEFAULT_COLLATION_OID, POSIX_COLLATION_OID};
use crate::include::nodes::datetime::{
    TIMESTAMP_NOBEGIN, TIMESTAMP_NOEND, TimeTzADT, USECS_PER_SEC,
};
use crate::pgrust::compact_string::CompactString;

pub(crate) fn compare_order_by_keys(
    items: &[OrderByEntry],
    left_keys: &[Value],
    right_keys: &[Value],
) -> Result<Ordering, ExecError> {
    for (item, (left_value, right_value)) in
        items.iter().zip(left_keys.iter().zip(right_keys.iter()))
    {
        let ordering = compare_order_values(
            left_value,
            right_value,
            item.collation_oid,
            item.nulls_first,
            item.descending,
        )?;
        if ordering != Ordering::Equal {
            return Ok(
                if item.descending
                    && !matches!(
                        (left_value, right_value),
                        (Value::Null, _) | (_, Value::Null)
                    )
                {
                    ordering.reverse()
                } else {
                    ordering
                },
            );
        }
    }
    Ok(Ordering::Equal)
}

pub(crate) fn compare_order_values(
    left: &Value,
    right: &Value,
    collation_oid: Option<u32>,
    nulls_first: Option<bool>,
    descending: bool,
) -> Result<Ordering, ExecError> {
    let nulls_first = nulls_first.unwrap_or(descending);
    match (left, right) {
        (Value::Null, Value::Null) => Ok(Ordering::Equal),
        (Value::Null, _) => {
            if nulls_first {
                Ok(Ordering::Less)
            } else {
                Ok(Ordering::Greater)
            }
        }
        (_, Value::Null) => {
            if nulls_first {
                Ok(Ordering::Greater)
            } else {
                Ok(Ordering::Less)
            }
        }
        (Value::Int32(a), Value::Int32(b)) => Ok(a.cmp(b)),
        (Value::Int64(a), Value::Int64(b)) => Ok(a.cmp(b)),
        (Value::PgLsn(a), Value::PgLsn(b)) => Ok(a.cmp(b)),
        (Value::Int16(a), Value::Float64(b)) => Ok(pg_float_cmp(f64::from(*a), *b)),
        (Value::Int32(a), Value::Float64(b)) => Ok(pg_float_cmp(f64::from(*a), *b)),
        (Value::Int64(a), Value::Float64(b)) => Ok(pg_float_cmp(*a as f64, *b)),
        (Value::Float64(a), Value::Int16(b)) => Ok(pg_float_cmp(*a, f64::from(*b))),
        (Value::Float64(a), Value::Int32(b)) => Ok(pg_float_cmp(*a, f64::from(*b))),
        (Value::Float64(a), Value::Int64(b)) => Ok(pg_float_cmp(*a, *b as f64)),
        (Value::Date(a), Value::Date(b)) => Ok(a.cmp(b)),
        (Value::Time(a), Value::Time(b)) => Ok(a.cmp(b)),
        (Value::TimeTz(a), Value::TimeTz(b)) => Ok(timetz_order_key(*a).cmp(&timetz_order_key(*b))),
        (Value::Timestamp(a), Value::Timestamp(b)) => Ok(a.cmp(b)),
        (Value::TimestampTz(a), Value::TimestampTz(b)) => Ok(a.cmp(b)),
        (Value::Interval(a), Value::Interval(b)) => Ok(a.cmp_key().cmp(&b.cmp_key())),
        (Value::Bit(a), Value::Bit(b)) => Ok(compare_bit_strings(a, b)),
        (Value::Bytea(a), Value::Bytea(b)) => Ok(a.cmp(b)),
        (Value::Uuid(a), Value::Uuid(b)) => Ok(a.cmp(b)),
        (Value::Inet(a), Value::Inet(b)) => {
            Ok(crate::backend::executor::compare_network_values(a, b))
        }
        (Value::Cidr(a), Value::Cidr(b)) => {
            Ok(crate::backend::executor::compare_network_values(a, b))
        }
        (Value::Float64(a), Value::Float64(b)) => Ok(pg_float_cmp(*a, *b)),
        (Value::Money(a), Value::Money(b)) => Ok(money_cmp(*a, *b)),
        (a, b) if parsed_numeric_value(a).is_some() && parsed_numeric_value(b).is_some() => {
            Ok(parsed_numeric_value(a)
                .and_then(|left| parsed_numeric_value(b).map(|right| left.cmp(&right)))
                .unwrap_or(Ordering::Equal))
        }
        (Value::Jsonb(a), Value::Jsonb(b)) => Ok(compare_jsonb(
            &decode_jsonb(a).unwrap_or(JsonbValue::Null),
            &decode_jsonb(b).unwrap_or(JsonbValue::Null),
        )),
        (Value::Range(a), Value::Range(b)) => Ok(compare_range_values(a, b)),
        (Value::Multirange(a), Value::Multirange(b)) => Ok(compare_multirange_values(a, b)),
        (Value::TsVector(a), Value::TsVector(b)) => {
            Ok(crate::backend::executor::compare_tsvector(a, b))
        }
        (Value::TsQuery(a), Value::TsQuery(b)) => {
            Ok(crate::backend::executor::compare_tsquery(a, b))
        }
        (Value::Record(a), Value::Record(b)) => Ok(compare_record_values(a, b)),
        (a, b) if a.as_text().is_some() && b.as_text().is_some() => {
            compare_text_values(a.as_text().unwrap(), b.as_text().unwrap(), collation_oid)
        }
        (Value::Bool(a), Value::Bool(b)) => Ok(a.cmp(b)),
        (a, b) if normalize_array_value(a).is_some() && normalize_array_value(b).is_some() => {
            Ok(compare_array_values(
                &normalize_array_value(a).unwrap(),
                &normalize_array_value(b).unwrap(),
            ))
        }
        _ => Ok(Ordering::Equal),
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
    collation_oid: Option<u32>,
) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    if let Some((left, right)) = coerce_temporal_text_pair(&left, &right) {
        return compare_values(op, left, right, collation_oid);
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
        (Value::PgLsn(l), Value::PgLsn(r)) => Ok(Value::Bool(l == r)),
        (Value::Money(l), Value::Money(r)) => Ok(Value::Bool(l == r)),
        (Value::Date(l), Value::Date(r)) => Ok(Value::Bool(l == r)),
        (Value::Time(l), Value::Time(r)) => Ok(Value::Bool(l == r)),
        (Value::TimeTz(l), Value::TimeTz(r)) => Ok(Value::Bool(l == r)),
        (Value::Timestamp(l), Value::Timestamp(r)) => Ok(Value::Bool(l == r)),
        (Value::TimestampTz(l), Value::TimestampTz(r)) => Ok(Value::Bool(l == r)),
        (Value::Interval(l), Value::Interval(r)) => Ok(Value::Bool(l == r)),
        (Value::Bytea(l), Value::Bytea(r)) => Ok(Value::Bool(l == r)),
        (Value::Uuid(l), Value::Uuid(r)) => Ok(Value::Bool(l == r)),
        (Value::Inet(l), Value::Inet(r)) => Ok(Value::Bool(l == r)),
        (Value::Cidr(l), Value::Cidr(r)) => Ok(Value::Bool(l == r)),
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
        (Value::Multirange(l), Value::Multirange(r)) => Ok(Value::Bool(
            compare_multirange_values(l, r) == Ordering::Equal,
        )),
        (Value::TsVector(l), Value::TsVector(r)) => Ok(Value::Bool(l == r)),
        (Value::TsQuery(l), Value::TsQuery(r)) => Ok(Value::Bool(l == r)),
        (Value::Record(l), Value::Record(r)) => {
            Ok(Value::Bool(compare_record_values(l, r) == Ordering::Equal))
        }
        (l, r) if l.as_text().is_some() && r.as_text().is_some() => {
            ensure_builtin_collation_supported(collation_oid)?;
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

pub(crate) fn compare_values_with_type(
    op: &'static str,
    left: Value,
    left_type: Option<SqlType>,
    right: Value,
    right_type: Option<SqlType>,
    collation_oid: Option<u32>,
) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    if let (Some(left_text), Some(right_text)) = (left.as_text(), right.as_text())
        && (is_bpchar_type(left_type) || is_bpchar_type(right_type))
    {
        ensure_builtin_collation_supported(collation_oid)?;
        return Ok(Value::Bool(
            bpchar_comparison_text(left_text, left_type)
                == bpchar_comparison_text(right_text, right_type),
        ));
    }
    compare_values(op, left, right, collation_oid)
}

pub(crate) fn not_equal_values(
    left: Value,
    right: Value,
    collation_oid: Option<u32>,
) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match compare_values("=", left.clone(), right.clone(), collation_oid)? {
        Value::Bool(value) => Ok(Value::Bool(!value)),
        other => Err(ExecError::NonBoolQual(other)),
    }
}

pub(crate) fn not_equal_values_with_type(
    left: Value,
    left_type: Option<SqlType>,
    right: Value,
    right_type: Option<SqlType>,
    collation_oid: Option<u32>,
) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match compare_values_with_type("=", left, left_type, right, right_type, collation_oid)? {
        Value::Bool(value) => Ok(Value::Bool(!value)),
        other => Err(ExecError::NonBoolQual(other)),
    }
}

fn is_bpchar_type(ty: Option<SqlType>) -> bool {
    ty.is_some_and(|ty| !ty.is_array && matches!(ty.kind, SqlTypeKind::Char))
}

fn bpchar_comparison_text(text: &str, ty: Option<SqlType>) -> &str {
    if is_bpchar_type(ty) {
        text.trim_end_matches(' ')
    } else {
        text
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
        (Value::PgLsn(l), Value::PgLsn(r)) => l != r,
        (Value::Money(l), Value::Money(r)) => l != r,
        (Value::Date(l), Value::Date(r)) => l != r,
        (Value::Time(l), Value::Time(r)) => l != r,
        (Value::TimeTz(l), Value::TimeTz(r)) => l != r,
        (Value::Timestamp(l), Value::Timestamp(r)) => l != r,
        (Value::TimestampTz(l), Value::TimestampTz(r)) => l != r,
        (Value::Interval(l), Value::Interval(r)) => l != r,
        (Value::Bytea(l), Value::Bytea(r)) => l != r,
        (Value::Uuid(l), Value::Uuid(r)) => l != r,
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
        (Value::PgLsn(l), r) if parsed_numeric_value(r).is_some() => {
            Ok(Value::PgLsn(add_pg_lsn_offset(*l, r)?))
        }
        (l, Value::PgLsn(r)) if parsed_numeric_value(l).is_some() => {
            Ok(Value::PgLsn(add_pg_lsn_offset(*r, l)?))
        }
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
        (Value::PgLsn(l), Value::PgLsn(r)) => Ok(Value::Numeric(NumericValue::finite(
            BigInt::from(*l) - BigInt::from(*r),
            0,
        ))),
        (Value::PgLsn(l), r) if parsed_numeric_value(r).is_some() => {
            Ok(Value::PgLsn(sub_pg_lsn_offset(*l, r)?))
        }
        (Value::Date(l), Value::Date(r)) => Ok(Value::Int32(l.0 - r.0)),
        (Value::Timestamp(l), Value::Timestamp(r)) => timestamp_difference_interval(l.0, r.0),
        (Value::TimestampTz(l), Value::TimestampTz(r)) => timestamp_difference_interval(l.0, r.0),
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

fn interval_out_of_range() -> ExecError {
    ExecError::DetailedError {
        message: "interval out of range".into(),
        detail: None,
        hint: None,
        sqlstate: "22008",
    }
}

fn timestamp_difference_interval(left: i64, right: i64) -> Result<Value, ExecError> {
    match (left, right) {
        (TIMESTAMP_NOEND, TIMESTAMP_NOEND) | (TIMESTAMP_NOBEGIN, TIMESTAMP_NOBEGIN) => {
            Err(interval_out_of_range())
        }
        (TIMESTAMP_NOEND, TIMESTAMP_NOBEGIN) => Ok(Value::Interval(IntervalValue::infinity())),
        (TIMESTAMP_NOBEGIN, TIMESTAMP_NOEND) => Ok(Value::Interval(IntervalValue::neg_infinity())),
        (TIMESTAMP_NOEND, _) | (_, TIMESTAMP_NOBEGIN) => {
            Ok(Value::Interval(IntervalValue::infinity()))
        }
        (TIMESTAMP_NOBEGIN, _) | (_, TIMESTAMP_NOEND) => {
            Ok(Value::Interval(IntervalValue::neg_infinity()))
        }
        _ => {
            let diff = left.checked_sub(right).ok_or_else(interval_out_of_range)?;
            let days = diff / crate::include::nodes::datetime::USECS_PER_DAY;
            let time_micros = diff % crate::include::nodes::datetime::USECS_PER_DAY;
            Ok(Value::Interval(IntervalValue {
                time_micros,
                days: i32::try_from(days).map_err(|_| interval_out_of_range())?,
                months: 0,
            }))
        }
    }
}

fn multiply_interval_by_i64(value: IntervalValue, factor: i64) -> Result<Value, ExecError> {
    if !value.is_finite() {
        return Ok(Value::Interval(if factor < 0 {
            value.negate()
        } else {
            value
        }));
    }
    Ok(Value::Interval(IntervalValue {
        time_micros: value
            .time_micros
            .checked_mul(factor)
            .ok_or_else(interval_out_of_range)?,
        days: i32::try_from(
            i64::from(value.days)
                .checked_mul(factor)
                .ok_or_else(interval_out_of_range)?,
        )
        .map_err(|_| interval_out_of_range())?,
        months: i32::try_from(
            i64::from(value.months)
                .checked_mul(factor)
                .ok_or_else(interval_out_of_range)?,
        )
        .map_err(|_| interval_out_of_range())?,
    }))
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
        (Value::Interval(l), Value::Int16(r)) => multiply_interval_by_i64(*l, i64::from(*r)),
        (Value::Interval(l), Value::Int32(r)) => multiply_interval_by_i64(*l, i64::from(*r)),
        (Value::Interval(l), Value::Int64(r)) => multiply_interval_by_i64(*l, *r),
        (Value::Int16(l), Value::Interval(r)) => multiply_interval_by_i64(*r, i64::from(*l)),
        (Value::Int32(l), Value::Interval(r)) => multiply_interval_by_i64(*r, i64::from(*l)),
        (Value::Int64(l), Value::Interval(r)) => multiply_interval_by_i64(*r, *l),
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
    if let (Some(left_array), Some(right_array)) =
        (normalize_array_value(&left), normalize_array_value(&right))
    {
        return Ok(Value::PgArray(concatenate_arrays(left_array, right_array)?));
    }
    if normalize_array_value(&left).is_some() {
        return append_array_value(&left, &right, false);
    }
    if normalize_array_value(&right).is_some() {
        return append_array_value(&right, &left, true);
    }
    match (&left, &right) {
        (Value::Bit(l), Value::Bit(r)) => Ok(Value::Bit(concat_bit_strings(l, r))),
        (Value::Bytea(l), Value::Bytea(r)) => {
            let mut bytes = Vec::with_capacity(l.len() + r.len());
            bytes.extend_from_slice(l);
            bytes.extend_from_slice(r);
            Ok(Value::Bytea(bytes))
        }
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
    collation_oid: Option<u32>,
) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    if let Some((left, right)) = coerce_temporal_text_pair(&left, &right) {
        return order_values(op, left, right, collation_oid);
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
        (Value::PgLsn(l), Value::PgLsn(r)) => Ok(Value::Bool(compare_ord(*l, *r, op))),
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
        (Value::Uuid(l), Value::Uuid(r)) => Ok(Value::Bool(compare_ord(l, r, op))),
        (Value::Inet(l), Value::Inet(r)) | (Value::Cidr(l), Value::Cidr(r)) => {
            let ordering = crate::backend::executor::compare_network_values(l, r);
            Ok(Value::Bool(match op {
                "<" => ordering == Ordering::Less,
                "<=" => ordering != Ordering::Greater,
                ">" => ordering == Ordering::Greater,
                ">=" => ordering != Ordering::Less,
                _ => unreachable!(),
            }))
        }
        (Value::Date(l), Value::Date(r)) => Ok(Value::Bool(compare_ord(*l, *r, op))),
        (Value::Time(l), Value::Time(r)) => Ok(Value::Bool(compare_ord(*l, *r, op))),
        (Value::TimeTz(l), Value::TimeTz(r)) => Ok(Value::Bool(compare_ord(
            timetz_order_key(*l),
            timetz_order_key(*r),
            op,
        ))),
        (Value::Timestamp(l), Value::Timestamp(r)) => Ok(Value::Bool(compare_ord(*l, *r, op))),
        (Value::TimestampTz(l), Value::TimestampTz(r)) => Ok(Value::Bool(compare_ord(*l, *r, op))),
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
        (l, r) if l.as_text().is_some() && r.as_text().is_some() => {
            let ordering =
                compare_text_values(l.as_text().unwrap(), r.as_text().unwrap(), collation_oid)?;
            Ok(Value::Bool(match op {
                "<" => ordering == Ordering::Less,
                "<=" => ordering != Ordering::Greater,
                ">" => ordering == Ordering::Greater,
                ">=" => ordering != Ordering::Less,
                _ => unreachable!(),
            }))
        }
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

pub(crate) fn ensure_builtin_collation_supported(
    collation_oid: Option<u32>,
) -> Result<(), ExecError> {
    match collation_oid {
        None | Some(DEFAULT_COLLATION_OID | C_COLLATION_OID | POSIX_COLLATION_OID) => Ok(()),
        Some(oid) => Err(ExecError::DetailedError {
            message: format!("collation with OID {oid} is not supported"),
            detail: Some(
                "Only the built-in collations \"default\", \"C\", and \"POSIX\" are supported"
                    .into(),
            ),
            hint: None,
            sqlstate: "0A000",
        }),
    }
}

fn compare_text_values(
    left: &str,
    right: &str,
    collation_oid: Option<u32>,
) -> Result<Ordering, ExecError> {
    ensure_builtin_collation_supported(collation_oid)?;
    Ok(left.cmp(right))
}

fn coerce_temporal_text_pair(left: &Value, right: &Value) -> Option<(Value, Value)> {
    let left_target = match left {
        Value::Date(_) => Some(SqlType::new(SqlTypeKind::Date)),
        Value::Time(_) => Some(SqlType::new(SqlTypeKind::Time)),
        Value::TimeTz(_) => Some(SqlType::new(SqlTypeKind::TimeTz)),
        Value::Timestamp(_) => Some(SqlType::new(SqlTypeKind::Timestamp)),
        Value::TimestampTz(_) => Some(SqlType::new(SqlTypeKind::TimestampTz)),
        _ => None,
    };
    if let (Some(target), true) = (left_target, right.as_text().is_some()) {
        return cast_value(right.clone(), target)
            .ok()
            .map(|right| (left.clone(), right));
    }

    let right_target = match right {
        Value::Date(_) => Some(SqlType::new(SqlTypeKind::Date)),
        Value::Time(_) => Some(SqlType::new(SqlTypeKind::Time)),
        Value::TimeTz(_) => Some(SqlType::new(SqlTypeKind::TimeTz)),
        Value::Timestamp(_) => Some(SqlType::new(SqlTypeKind::Timestamp)),
        Value::TimestampTz(_) => Some(SqlType::new(SqlTypeKind::TimestampTz)),
        _ => None,
    };
    if let (Some(target), true) = (right_target, left.as_text().is_some()) {
        return cast_value(left.clone(), target)
            .ok()
            .map(|left| (left, right.clone()));
    }

    None
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

fn timetz_order_key(value: TimeTzADT) -> i64 {
    value.time.0 - i64::from(value.offset_seconds) * USECS_PER_SEC
}

fn compare_record_values(
    left: &crate::include::nodes::datum::RecordValue,
    right: &crate::include::nodes::datum::RecordValue,
) -> Ordering {
    for (left_value, right_value) in left.fields.iter().zip(&right.fields) {
        let value_ordering = compare_order_values(left_value, right_value, None, None, false)
            .expect("record field comparisons use implicit default collation");
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
                    compare_values("=", left_item.clone(), right_item.clone(), None),
                    Ok(Value::Bool(true))
                ) {
                    continue;
                }
                if matches!(
                    order_values("<", left_item.clone(), right_item.clone(), None),
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
                if lscale == rscale {
                    return Self::finite(lcoeff + rcoeff, *lscale)
                        .with_dscale(*lscale)
                        .normalize();
                }
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
                    dscale: ldscale,
                },
                Self::Finite {
                    coeff: rcoeff,
                    scale: rscale,
                    dscale: rdscale,
                },
            ) => {
                let scale = (*lscale).max(*rscale);
                let dscale = (*ldscale).max(*rdscale);
                let left = align_coeff(lcoeff.clone(), *lscale, scale);
                let right = align_coeff(rcoeff.clone(), *rscale, scale);
                Some(
                    Self::finite(left % right, scale)
                        .with_dscale(dscale)
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
                if let Some(divisor_exp) = power_of_ten_exponent(rcoeff) {
                    let divisor_sign = rcoeff.signum();
                    let rounded = rounded_divide_coeff_by_power_of_ten(
                        lcoeff,
                        divisor_exp as i64 - exp,
                        divisor_sign,
                    );
                    return Some(
                        Self::finite(rounded, out_scale)
                            .with_dscale(out_scale)
                            .normalize(),
                    );
                }
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
                if lscale == rscale {
                    return lcoeff.cmp(rcoeff);
                }
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
        let end = digits.len().checked_sub(shift as usize)?;
        digits[..end].parse::<i32>().ok()?
    } else {
        let zeros = (-shift) as usize;
        let mut first = String::with_capacity(digits.len() + zeros);
        first.push_str(&digits);
        first.extend(std::iter::repeat_n('0', zeros));
        first.parse::<i32>().ok()?
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
    let mut digits = String::with_capacity(exp as usize + 1);
    digits.push('1');
    digits.extend(std::iter::repeat_n('0', exp as usize));
    BigInt::parse_bytes(digits.as_bytes(), 10).expect("power of ten digits are decimal")
}

fn power_of_ten_exponent(value: &BigInt) -> Option<u32> {
    let digits = value.abs().to_str_radix(10);
    if !digits.starts_with('1') || !digits.as_bytes()[1..].iter().all(|digit| *digit == b'0') {
        return None;
    }
    Some((digits.len() - 1) as u32)
}

fn rounded_divide_coeff_by_power_of_ten(
    coeff: &BigInt,
    shift: i64,
    divisor_sign: BigInt,
) -> BigInt {
    if coeff.is_zero() {
        return BigInt::zero();
    }

    let negative = coeff.signum() != divisor_sign;
    let digits = coeff.abs().to_str_radix(10);
    let mut rounded = if shift <= 0 {
        let zeros = (-shift) as usize;
        let mut shifted = String::with_capacity(digits.len() + zeros);
        shifted.push_str(&digits);
        shifted.extend(std::iter::repeat_n('0', zeros));
        BigInt::parse_bytes(shifted.as_bytes(), 10).expect("shifted coefficient digits are decimal")
    } else {
        let drop = shift as usize;
        if drop > digits.len() {
            BigInt::zero()
        } else {
            let keep_len = digits.len() - drop;
            let mut quotient = if keep_len == 0 {
                BigInt::zero()
            } else {
                BigInt::parse_bytes(digits[..keep_len].as_bytes(), 10)
                    .expect("coefficient digits are decimal")
            };
            if drop > 0 && digits.as_bytes()[keep_len] >= b'5' {
                quotient += 1u8;
            }
            quotient
        }
    };

    if negative {
        rounded = -rounded;
    }
    rounded
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

fn numeric_value_to_i128(value: &Value, nan_message: &'static str) -> Result<i128, ExecError> {
    match parsed_numeric_value(value).ok_or_else(|| ExecError::TypeMismatch {
        op: "pg_lsn numeric offset",
        left: value.clone(),
        right: Value::Null,
    })? {
        NumericValue::Finite { coeff, scale, .. } if scale == 0 => {
            coeff.to_i128().ok_or_else(pg_lsn_out_of_range)
        }
        NumericValue::NaN => Err(ExecError::DetailedError {
            message: nan_message.into(),
            detail: None,
            hint: None,
            sqlstate: "22003",
        }),
        _ => Err(pg_lsn_out_of_range()),
    }
}

fn add_pg_lsn_offset(lsn: u64, offset: &Value) -> Result<u64, ExecError> {
    let offset = numeric_value_to_i128(offset, "cannot add NaN to pg_lsn")?;
    let result = i128::from(lsn) + offset;
    u64::try_from(result).map_err(|_| pg_lsn_out_of_range())
}

fn sub_pg_lsn_offset(lsn: u64, offset: &Value) -> Result<u64, ExecError> {
    let offset = numeric_value_to_i128(offset, "cannot subtract NaN from pg_lsn")?;
    let result = i128::from(lsn) - offset;
    u64::try_from(result).map_err(|_| pg_lsn_out_of_range())
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

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use crate::include::catalog::{C_COLLATION_OID, DEFAULT_COLLATION_OID, POSIX_COLLATION_OID};
    use crate::include::nodes::datum::{NumericValue, Value};

    #[test]
    fn compare_order_values_orders_int64_values_directly() {
        assert_eq!(
            super::compare_order_values(
                &Value::Int64(1234),
                &Value::Int64(4_294_966_256),
                None,
                None,
                false
            )
            .unwrap(),
            Ordering::Less
        );
    }

    #[test]
    fn compare_order_values_accepts_builtin_text_collations() {
        for oid in [DEFAULT_COLLATION_OID, C_COLLATION_OID, POSIX_COLLATION_OID] {
            assert_eq!(
                super::compare_order_values(
                    &Value::Text("alpha".into()),
                    &Value::Text("beta".into()),
                    Some(oid),
                    None,
                    false,
                )
                .unwrap(),
                Ordering::Less
            );
        }
    }

    #[test]
    fn compare_values_rejects_unsupported_collation_oid() {
        assert!(matches!(
            super::compare_values(
                "=",
                Value::Text("alpha".into()),
                Value::Text("alpha".into()),
                Some(123_456),
            ),
            Err(crate::backend::executor::ExecError::DetailedError { sqlstate, .. })
                if sqlstate == "0A000"
        ));
    }

    #[test]
    fn numeric_divides_by_large_power_of_ten_without_full_bigint_division() {
        let numerator = super::parse_numeric_text("6e131071").unwrap();
        let denominator = super::parse_numeric_text("1e131071").unwrap();

        assert_eq!(
            numerator.div(&denominator, 0),
            Some(NumericValue::from_i64(6))
        );
    }
}
