use num_bigint::BigInt;
use num_integer::Integer;
use num_traits::{Signed, ToPrimitive, Zero};

use super::ExecError;
use super::expr_ops::parse_numeric_text;
use super::node_types::{NumericValue, Value};

fn numeric_domain_error(message: impl Into<String>) -> ExecError {
    ExecError::InvalidStorageValue {
        column: String::new(),
        details: message.into(),
    }
}

fn pow10_bigint(exp: u32) -> BigInt {
    let mut value = BigInt::from(1u8);
    for _ in 0..exp {
        value *= 10u8;
    }
    value
}

fn trailing_decimal_zeros(coeff: &BigInt, max: u32) -> u32 {
    if coeff.is_zero() {
        return max;
    }
    let ten = BigInt::from(10u8);
    let mut zeros = 0u32;
    let mut current = coeff.clone();
    while zeros < max {
        let (quotient, remainder) = current.div_rem(&ten);
        if !remainder.is_zero() {
            break;
        }
        current = quotient;
        zeros += 1;
    }
    zeros
}

fn align_coeff(coeff: &BigInt, from_scale: u32, to_scale: u32) -> BigInt {
    if from_scale == to_scale {
        return coeff.clone();
    }
    coeff * pow10_bigint(to_scale - from_scale)
}

fn value_as_numeric(value: &Value) -> Option<NumericValue> {
    match value {
        Value::Int16(v) => Some(NumericValue::from_i64(i64::from(*v))),
        Value::Int32(v) => Some(NumericValue::from_i64(i64::from(*v))),
        Value::Int64(v) => Some(NumericValue::from_i64(*v)),
        Value::Numeric(v) => Some(v.clone()),
        Value::Text(text) => parse_numeric_text(text),
        Value::TextRef(_, _) => value.as_text().and_then(parse_numeric_text),
        _ => None,
    }
}

fn numeric_to_f64(value: &NumericValue) -> Option<f64> {
    match value {
        NumericValue::PosInf => Some(f64::INFINITY),
        NumericValue::NegInf => Some(f64::NEG_INFINITY),
        NumericValue::NaN => Some(f64::NAN),
        NumericValue::Finite { coeff, scale } => {
            let coeff = coeff.to_f64()?;
            Some(coeff / 10f64.powi(*scale as i32))
        }
    }
}

fn numeric_from_f64(value: f64, scale: usize) -> NumericValue {
    if value.is_nan() {
        return NumericValue::NaN;
    }
    if value == f64::INFINITY {
        return NumericValue::PosInf;
    }
    if value == f64::NEG_INFINITY {
        return NumericValue::NegInf;
    }
    parse_numeric_text(&format!("{value:.scale$}")).unwrap_or_else(|| NumericValue::zero())
}

fn round_numeric_to_scale(value: &NumericValue, target_scale: i32) -> NumericValue {
    let target_scale = target_scale.min(16383);
    match value {
        NumericValue::PosInf => NumericValue::PosInf,
        NumericValue::NegInf => NumericValue::NegInf,
        NumericValue::NaN => NumericValue::NaN,
        NumericValue::Finite { coeff, scale } if target_scale >= 0 => value
            .round_to_scale(target_scale as u32)
            .unwrap_or_else(|| value.clone()),
        NumericValue::Finite { coeff, scale } => {
            let shift = target_scale.unsigned_abs();
            if negative_scale_rounds_to_zero(coeff, *scale, shift) {
                return NumericValue::zero();
            }
            let factor = pow10_bigint(scale.saturating_add(shift));
            let (quotient, remainder) = coeff.div_rem(&factor);
            let twice = remainder.abs() * 2u8;
            let rounded = if twice >= factor.abs() {
                quotient + coeff.signum()
            } else {
                quotient
            };
            NumericValue::Finite {
                coeff: rounded * pow10_bigint(shift),
                scale: 0,
            }
        }
    }
}

fn trunc_numeric_to_scale(value: &NumericValue, target_scale: i32) -> NumericValue {
    let target_scale = target_scale.min(16383);
    match value {
        NumericValue::PosInf => NumericValue::PosInf,
        NumericValue::NegInf => NumericValue::NegInf,
        NumericValue::NaN => NumericValue::NaN,
        NumericValue::Finite { .. } if target_scale >= 0 => match value {
            NumericValue::Finite { coeff, scale } if *scale > target_scale as u32 => {
                let factor = pow10_bigint(*scale - target_scale as u32);
                NumericValue::Finite {
                    coeff: coeff / factor,
                    scale: target_scale as u32,
                }
            }
            NumericValue::Finite { coeff, scale } if (*scale as i32) < target_scale => {
                let factor = pow10_bigint(target_scale as u32 - *scale);
                NumericValue::Finite {
                    coeff: coeff * factor,
                    scale: target_scale as u32,
                }
            }
            _ => value.clone(),
        },
        NumericValue::Finite { coeff, scale } => {
            let shift = target_scale.unsigned_abs();
            if negative_scale_rounds_to_zero(coeff, *scale, shift) {
                return NumericValue::zero();
            }
            let factor = pow10_bigint(scale.saturating_add(shift));
            let quotient = coeff / &factor;
            NumericValue::Finite {
                coeff: quotient * pow10_bigint(shift),
                scale: 0,
            }
        }
    }
}

fn numeric_digits_before_decimal(value: &NumericValue) -> u32 {
    match value {
        NumericValue::Finite { coeff, scale } => {
            let digits = coeff
                .to_str_radix(10)
                .trim_start_matches('-')
                .trim_start_matches('0')
                .len()
                .max(1) as u32;
            digits.saturating_sub(*scale)
        }
        _ => 0,
    }
}

fn negative_scale_rounds_to_zero(coeff: &BigInt, scale: u32, shift: u32) -> bool {
    if coeff.is_zero() {
        return true;
    }
    let digits = coeff
        .to_str_radix(10)
        .trim_start_matches('-')
        .trim_start_matches('0')
        .len()
        .max(1) as u32;
    digits.saturating_sub(scale) < shift
}

fn ensure_numeric_range(value: NumericValue) -> Result<NumericValue, ExecError> {
    if matches!(value, NumericValue::Finite { .. }) && numeric_digits_before_decimal(&value) > 131072 {
        return Err(numeric_domain_error("value overflows numeric format"));
    }
    Ok(value)
}

pub(super) fn eval_round_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [] => Ok(Value::Null),
        [Value::Null] | [_, Value::Null] => Ok(Value::Null),
        [Value::Float64(v)] => Ok(Value::Float64(v.round())),
        [value] => Ok(Value::Numeric(ensure_numeric_range(round_numeric_to_scale(
            &value_as_numeric(value).ok_or_else(|| ExecError::TypeMismatch {
                op: "round",
                left: value.clone(),
                right: Value::Null,
            })?,
            0,
        ))?)),
        [value, Value::Int32(scale)] => Ok(Value::Numeric(ensure_numeric_range(round_numeric_to_scale(
            &value_as_numeric(value).ok_or_else(|| ExecError::TypeMismatch {
                op: "round",
                left: value.clone(),
                right: Value::Int32(*scale),
            })?,
            *scale,
        ))?)),
        [left, right] => Err(ExecError::TypeMismatch {
            op: "round",
            left: left.clone(),
            right: right.clone(),
        }),
        _ => Ok(Value::Null),
    }
}

pub(super) fn eval_trunc_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [] => Ok(Value::Null),
        [Value::Null] | [_, Value::Null] => Ok(Value::Null),
        [Value::Float64(v)] => Ok(Value::Float64(v.trunc())),
        [value] => Ok(Value::Numeric(ensure_numeric_range(trunc_numeric_to_scale(
            &value_as_numeric(value).ok_or_else(|| ExecError::TypeMismatch {
                op: "trunc",
                left: value.clone(),
                right: Value::Null,
            })?,
            0,
        ))?)),
        [value, Value::Int32(scale)] => Ok(Value::Numeric(ensure_numeric_range(trunc_numeric_to_scale(
            &value_as_numeric(value).ok_or_else(|| ExecError::TypeMismatch {
                op: "trunc",
                left: value.clone(),
                right: Value::Int32(*scale),
            })?,
            *scale,
        ))?)),
        [left, right] => Err(ExecError::TypeMismatch {
            op: "trunc",
            left: left.clone(),
            right: right.clone(),
        }),
        _ => Ok(Value::Null),
    }
}

pub(super) fn eval_scale_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [] | [Value::Null] => Ok(Value::Null),
        [value] => match value_as_numeric(value) {
            Some(NumericValue::Finite { scale, .. }) => Ok(Value::Int32(scale as i32)),
            Some(NumericValue::PosInf | NumericValue::NegInf | NumericValue::NaN) => Ok(Value::Null),
            None => Err(ExecError::TypeMismatch {
                op: "scale",
                left: value.clone(),
                right: Value::Null,
            }),
        },
        _ => Ok(Value::Null),
    }
}

pub(super) fn eval_numeric_inc_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [] | [Value::Null] => Ok(Value::Null),
        [value] => {
            let numeric = value_as_numeric(value).ok_or_else(|| ExecError::TypeMismatch {
                op: "numeric_inc",
                left: value.clone(),
                right: Value::Null,
            })?;
            let result = match numeric {
                NumericValue::PosInf => NumericValue::PosInf,
                NumericValue::NegInf => NumericValue::NegInf,
                NumericValue::NaN => NumericValue::NaN,
                NumericValue::Finite { coeff, scale } => NumericValue::Finite {
                    coeff: coeff + pow10_bigint(scale),
                    scale,
                }
                .normalize(),
            };
            Ok(Value::Numeric(result))
        }
        _ => Ok(Value::Null),
    }
}

pub(super) fn eval_min_scale_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [] | [Value::Null] => Ok(Value::Null),
        [value] => match value_as_numeric(value) {
            Some(NumericValue::Finite { coeff, scale }) => {
                if coeff.is_zero() {
                    Ok(Value::Int32(0))
                } else {
                    Ok(Value::Int32(
                        scale.saturating_sub(trailing_decimal_zeros(&coeff, scale)) as i32,
                    ))
                }
            }
            Some(NumericValue::PosInf | NumericValue::NegInf | NumericValue::NaN) => Ok(Value::Null),
            None => Err(ExecError::TypeMismatch {
                op: "min_scale",
                left: value.clone(),
                right: Value::Null,
            }),
        },
        _ => Ok(Value::Null),
    }
}

pub(super) fn eval_trim_scale_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [] | [Value::Null] => Ok(Value::Null),
        [value] => match value_as_numeric(value) {
            Some(NumericValue::Finite { coeff, scale }) => {
                if coeff.is_zero() {
                    Ok(Value::Numeric(NumericValue::zero()))
                } else {
                    let zeros = trailing_decimal_zeros(&coeff, scale);
                    Ok(Value::Numeric(
                        NumericValue::Finite {
                            coeff: coeff / pow10_bigint(zeros),
                            scale: scale - zeros,
                        }
                        .normalize(),
                    ))
                }
            }
            Some(other) => Ok(Value::Numeric(other)),
            None => Err(ExecError::TypeMismatch {
                op: "trim_scale",
                left: value.clone(),
                right: Value::Null,
            }),
        },
        _ => Ok(Value::Null),
    }
}

pub(super) fn eval_div_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [left, right] => {
            let left_num = value_as_numeric(left).ok_or_else(|| ExecError::TypeMismatch {
                op: "div",
                left: left.clone(),
                right: right.clone(),
            })?;
            let right_num = value_as_numeric(right).ok_or_else(|| ExecError::TypeMismatch {
                op: "div",
                left: left.clone(),
                right: right.clone(),
            })?;
            if matches!(left_num, NumericValue::NaN) || matches!(right_num, NumericValue::NaN) {
                return Ok(Value::Numeric(NumericValue::NaN));
            }
            if matches!(right_num, NumericValue::Finite { ref coeff, .. } if coeff.is_zero()) {
                return Err(ExecError::DivisionByZero("/"));
            }
            let result = match (&left_num, &right_num) {
                (NumericValue::PosInf, NumericValue::PosInf | NumericValue::NegInf)
                | (NumericValue::NegInf, NumericValue::PosInf | NumericValue::NegInf) => {
                    NumericValue::NaN
                }
                (NumericValue::PosInf, NumericValue::Finite { coeff, .. }) => {
                    if coeff.is_negative() {
                        NumericValue::NegInf
                    } else {
                        NumericValue::PosInf
                    }
                }
                (NumericValue::NegInf, NumericValue::Finite { coeff, .. }) => {
                    if coeff.is_negative() {
                        NumericValue::PosInf
                    } else {
                        NumericValue::NegInf
                    }
                }
                (NumericValue::Finite { .. }, NumericValue::PosInf | NumericValue::NegInf) => {
                    NumericValue::zero()
                }
                (
                    NumericValue::Finite {
                        coeff: left_coeff,
                        scale: left_scale,
                    },
                    NumericValue::Finite {
                        coeff: right_coeff,
                        scale: right_scale,
                    },
                ) => {
                    let scale = (*left_scale).max(*right_scale);
                    NumericValue::Finite {
                        coeff: align_coeff(left_coeff, *left_scale, scale)
                            / align_coeff(right_coeff, *right_scale, scale),
                        scale: 0,
                    }
                    .normalize()
                }
                _ => NumericValue::NaN,
            };
            Ok(Value::Numeric(result))
        }
        _ => Ok(Value::Null),
    }
}

fn eval_log_numeric_unary(value: &NumericValue) -> Result<NumericValue, ExecError> {
    match value {
        NumericValue::NaN => Ok(NumericValue::NaN),
        NumericValue::PosInf => Ok(NumericValue::PosInf),
        NumericValue::NegInf => Err(numeric_domain_error(
            "cannot take logarithm of a negative number",
        )),
        NumericValue::Finite { coeff, .. } if coeff.is_zero() => {
            Err(numeric_domain_error("cannot take logarithm of zero"))
        }
        NumericValue::Finite { coeff, .. } if coeff.is_negative() => Err(numeric_domain_error(
            "cannot take logarithm of a negative number",
        )),
        finite => Ok(numeric_from_f64(
            numeric_to_f64(finite).unwrap_or(f64::NAN).log10(),
            16,
        )),
    }
}

fn eval_log_numeric_binary(base: &NumericValue, value: &NumericValue) -> Result<NumericValue, ExecError> {
    match (base, value) {
        (NumericValue::NaN, _) | (_, NumericValue::NaN) => Ok(NumericValue::NaN),
        (NumericValue::PosInf | NumericValue::NegInf, NumericValue::PosInf | NumericValue::NegInf) => {
            Ok(NumericValue::NaN)
        }
        (NumericValue::NegInf, _) => Err(numeric_domain_error(
            "cannot take logarithm of a negative number",
        )),
        (_, NumericValue::NegInf) => Err(numeric_domain_error(
            "cannot take logarithm of a negative number",
        )),
        (NumericValue::Finite { coeff, .. }, _) if coeff.is_zero() => {
            Err(numeric_domain_error("cannot take logarithm of zero"))
        }
        (_, NumericValue::Finite { coeff, .. }) if coeff.is_zero() => {
            Err(numeric_domain_error("cannot take logarithm of zero"))
        }
        (NumericValue::Finite { coeff, .. }, _) if coeff.is_negative() => Err(numeric_domain_error(
            "cannot take logarithm of a negative number",
        )),
        (_, NumericValue::Finite { coeff, .. }) if coeff.is_negative() => Err(numeric_domain_error(
            "cannot take logarithm of a negative number",
        )),
        (base, value) => Ok(numeric_from_f64(
            numeric_to_f64(value).unwrap_or(f64::NAN).log(numeric_to_f64(base).unwrap_or(f64::NAN)),
            16,
        )),
    }
}

pub(super) fn eval_log_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [] => Ok(Value::Null),
        [Value::Null] | [_, Value::Null] => Ok(Value::Null),
        [Value::Float64(value)] => {
            if *value == 0.0 {
                return Err(numeric_domain_error("cannot take logarithm of zero"));
            }
            if *value < 0.0 {
                return Err(numeric_domain_error(
                    "cannot take logarithm of a negative number",
                ));
            }
            Ok(Value::Float64(value.log10()))
        }
        [value] => Ok(Value::Numeric(eval_log_numeric_unary(
            &value_as_numeric(value).ok_or_else(|| ExecError::TypeMismatch {
                op: "log",
                left: value.clone(),
                right: Value::Null,
            })?,
        )?)),
        [Value::Float64(base), Value::Float64(value)] => {
            if *base == 0.0 || *value == 0.0 {
                return Err(numeric_domain_error("cannot take logarithm of zero"));
            }
            if *base < 0.0 || *value < 0.0 {
                return Err(numeric_domain_error(
                    "cannot take logarithm of a negative number",
                ));
            }
            Ok(Value::Float64(value.log(*base)))
        }
        [base, value] => Ok(Value::Numeric(eval_log_numeric_binary(
            &value_as_numeric(base).ok_or_else(|| ExecError::TypeMismatch {
                op: "log",
                left: base.clone(),
                right: value.clone(),
            })?,
            &value_as_numeric(value).ok_or_else(|| ExecError::TypeMismatch {
                op: "log",
                left: base.clone(),
                right: value.clone(),
            })?,
        )?)),
        _ => Ok(Value::Null),
    }
}

pub(super) fn eval_log10_function(values: &[Value]) -> Result<Value, ExecError> {
    eval_log_function(values)
}

fn factorial_overflows(n: u64) -> bool {
    if n < 2 {
        return false;
    }
    let n = n as f64;
    let digits = (n * (n / std::f64::consts::E).log10() + (2.0 * std::f64::consts::PI * n).log10() / 2.0).floor() + 1.0;
    digits > 131072.0
}

pub(super) fn eval_factorial_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [] | [Value::Null] => Ok(Value::Null),
        [value] => {
            let numeric = value_as_numeric(value).ok_or_else(|| ExecError::TypeMismatch {
                op: "factorial",
                left: value.clone(),
                right: Value::Null,
            })?;
            let n = match numeric {
                NumericValue::NaN => return Ok(Value::Numeric(NumericValue::NaN)),
                NumericValue::PosInf | NumericValue::NegInf => {
                    return Err(numeric_domain_error("factorial of a negative number is undefined"))
                }
                NumericValue::Finite { coeff, scale } => {
                    if scale != 0 {
                        return Err(numeric_domain_error("factorial of a negative number is undefined"));
                    }
                    coeff.to_i64().ok_or_else(|| numeric_domain_error("value overflows numeric format"))?
                }
            };
            if n < 0 {
                return Err(numeric_domain_error("factorial of a negative number is undefined"));
            }
            let n = n as u64;
            if factorial_overflows(n) {
                return Err(numeric_domain_error("value overflows numeric format"));
            }
            let mut acc = BigInt::from(1u8);
            for i in 2..=n {
                acc *= i;
            }
            Ok(Value::Numeric(NumericValue::Finite { coeff: acc, scale: 0 }.normalize()))
        }
        _ => Ok(Value::Null),
    }
}

pub(super) fn eval_pg_lsn_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [] | [Value::Null] => Ok(Value::Null),
        [value] => {
            let numeric = value_as_numeric(value).ok_or_else(|| ExecError::TypeMismatch {
                op: "pg_lsn",
                left: value.clone(),
                right: Value::Null,
            })?;
            match numeric {
                NumericValue::NaN => Err(numeric_domain_error("cannot convert NaN to pg_lsn")),
                NumericValue::PosInf | NumericValue::NegInf => Err(numeric_domain_error("pg_lsn out of range")),
                NumericValue::Finite { coeff, scale } => {
                    if scale != 0 {
                        return Err(numeric_domain_error("pg_lsn out of range"));
                    }
                    let value = coeff.to_u64().ok_or_else(|| numeric_domain_error("pg_lsn out of range"))?;
                    Ok(Value::Text(format!("{:X}/{:X}", value >> 32, value & 0xFFFF_FFFF).into()))
                }
            }
        }
        _ => Ok(Value::Null),
    }
}

pub(super) fn eval_ceil_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [] | [Value::Null] => Ok(Value::Null),
        [Value::Float64(v)] => Ok(Value::Float64(v.ceil())),
        [value] => {
            let numeric = value_as_numeric(value).ok_or_else(|| ExecError::TypeMismatch {
                op: "ceil",
                left: value.clone(),
                right: Value::Null,
            })?;
            Ok(Value::Numeric(match numeric {
                NumericValue::PosInf => NumericValue::PosInf,
                NumericValue::NegInf => NumericValue::NegInf,
                NumericValue::NaN => NumericValue::NaN,
                NumericValue::Finite { coeff, scale } if scale == 0 => NumericValue::Finite { coeff, scale: 0 },
                NumericValue::Finite { coeff, scale } => {
                    let factor = pow10_bigint(scale);
                    let quotient = &coeff / &factor;
                    let remainder = &coeff % &factor;
                    let adjusted = if coeff.is_positive() && !remainder.is_zero() {
                        quotient + 1
                    } else {
                        quotient
                    };
                    NumericValue::Finite { coeff: adjusted, scale: 0 }.normalize()
                }
            }))
        }
        _ => Ok(Value::Null),
    }
}

pub(super) fn eval_floor_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [] | [Value::Null] => Ok(Value::Null),
        [Value::Float64(v)] => Ok(Value::Float64(v.floor())),
        [value] => {
            let numeric = value_as_numeric(value).ok_or_else(|| ExecError::TypeMismatch {
                op: "floor",
                left: value.clone(),
                right: Value::Null,
            })?;
            Ok(Value::Numeric(match numeric {
                NumericValue::PosInf => NumericValue::PosInf,
                NumericValue::NegInf => NumericValue::NegInf,
                NumericValue::NaN => NumericValue::NaN,
                NumericValue::Finite { coeff, scale } if scale == 0 => NumericValue::Finite { coeff, scale: 0 },
                NumericValue::Finite { coeff, scale } => {
                    let factor = pow10_bigint(scale);
                    let quotient = &coeff / &factor;
                    let remainder = &coeff % &factor;
                    let adjusted = if coeff.is_negative() && !remainder.is_zero() {
                        quotient - 1
                    } else {
                        quotient
                    };
                    NumericValue::Finite { coeff: adjusted, scale: 0 }.normalize()
                }
            }))
        }
        _ => Ok(Value::Null),
    }
}

pub(super) fn eval_sign_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [] | [Value::Null] => Ok(Value::Null),
        [Value::Float64(v)] => Ok(Value::Float64(if *v == 0.0 { 0.0 } else { v.signum() })),
        [value] => {
            let numeric = value_as_numeric(value).ok_or_else(|| ExecError::TypeMismatch {
                op: "sign",
                left: value.clone(),
                right: Value::Null,
            })?;
            Ok(Value::Numeric(match numeric {
                NumericValue::PosInf => NumericValue::from_i64(1),
                NumericValue::NegInf => NumericValue::from_i64(-1),
                NumericValue::NaN => NumericValue::NaN,
                NumericValue::Finite { coeff, .. } if coeff.is_zero() => NumericValue::zero(),
                NumericValue::Finite { coeff, .. } if coeff.is_negative() => NumericValue::from_i64(-1),
                NumericValue::Finite { .. } => NumericValue::from_i64(1),
            }))
        }
        _ => Ok(Value::Null),
    }
}

fn validate_width_bucket_count(count: i32) -> Result<(), ExecError> {
    if count <= 0 {
        Err(numeric_domain_error("count must be greater than zero"))
    } else {
        Ok(())
    }
}

fn width_bucket_outside(count: i32, above: bool) -> Result<Value, ExecError> {
    if above {
        count.checked_add(1)
            .map(Value::Int32)
            .ok_or(ExecError::Int4OutOfRange)
    } else {
        Ok(Value::Int32(0))
    }
}

fn eval_width_bucket_float(operand: f64, low: f64, high: f64, count: i32) -> Result<Value, ExecError> {
    validate_width_bucket_count(count)?;
    if operand.is_nan() || low.is_nan() || high.is_nan() {
        return Err(numeric_domain_error(
            "operand, lower bound, and upper bound cannot be NaN",
        ));
    }
    if !low.is_finite() || !high.is_finite() {
        return Err(numeric_domain_error("lower and upper bounds must be finite"));
    }
    if low == high {
        return Err(numeric_domain_error("lower bound cannot equal upper bound"));
    }
    if low < high {
        if operand < low {
            return width_bucket_outside(count, false);
        }
        if operand >= high {
            return width_bucket_outside(count, true);
        }
        let bucket = (((operand - low) / (high - low)) * f64::from(count)).floor() as i64 + 1;
        return i32::try_from(bucket)
            .map(Value::Int32)
            .map_err(|_| ExecError::Int4OutOfRange);
    }
    if operand > low {
        return width_bucket_outside(count, false);
    }
    if operand <= high {
        return width_bucket_outside(count, true);
    }
    let bucket = (((low - operand) / (low - high)) * f64::from(count)).floor() as i64 + 1;
    i32::try_from(bucket)
        .map(Value::Int32)
        .map_err(|_| ExecError::Int4OutOfRange)
}

fn eval_width_bucket_numeric(
    operand: &NumericValue,
    low: &NumericValue,
    high: &NumericValue,
    count: i32,
) -> Result<Value, ExecError> {
    validate_width_bucket_count(count)?;
    if matches!(operand, NumericValue::NaN)
        || matches!(low, NumericValue::NaN)
        || matches!(high, NumericValue::NaN)
    {
        return Err(numeric_domain_error(
            "operand, lower bound, and upper bound cannot be NaN",
        ));
    }
    if !matches!(low, NumericValue::Finite { .. }) || !matches!(high, NumericValue::Finite { .. }) {
        return Err(numeric_domain_error("lower and upper bounds must be finite"));
    }
    use std::cmp::Ordering;
    let ascending = low.cmp(high);
    if ascending == Ordering::Equal {
        return Err(numeric_domain_error("lower bound cannot equal upper bound"));
    }
    match operand {
        NumericValue::PosInf => return width_bucket_outside(count, ascending == Ordering::Less),
        NumericValue::NegInf => return width_bucket_outside(count, ascending == Ordering::Greater),
        NumericValue::Finite { .. } => {}
        NumericValue::NaN => unreachable!(),
    }
    if ascending == Ordering::Less {
        if operand.cmp(low) == Ordering::Less {
            return width_bucket_outside(count, false);
        }
        if operand.cmp(high) != Ordering::Less {
            return width_bucket_outside(count, true);
        }
    } else {
        if operand.cmp(low) == Ordering::Greater {
            return width_bucket_outside(count, false);
        }
        if operand.cmp(high) != Ordering::Greater {
            return width_bucket_outside(count, true);
        }
    }
    let (
        NumericValue::Finite {
            coeff: operand_coeff,
            scale: operand_scale,
        },
        NumericValue::Finite {
            coeff: low_coeff,
            scale: low_scale,
        },
        NumericValue::Finite {
            coeff: high_coeff,
            scale: high_scale,
        },
    ) = (operand, low, high)
    else {
        unreachable!()
    };
    let scale = (*operand_scale).max(*low_scale).max(*high_scale);
    let operand_coeff = align_coeff(operand_coeff, *operand_scale, scale);
    let low_coeff = align_coeff(low_coeff, *low_scale, scale);
    let high_coeff = align_coeff(high_coeff, *high_scale, scale);
    let count_big = BigInt::from(count);
    let (numerator, denominator) = if ascending == Ordering::Less {
        (
            (operand_coeff - &low_coeff) * &count_big,
            high_coeff - &low_coeff,
        )
    } else {
        (
            (low_coeff.clone() - operand_coeff) * &count_big,
            low_coeff - &high_coeff,
        )
    };
    let bucket = (numerator / denominator) + BigInt::from(1u8);
    bucket
        .to_i32()
        .map(Value::Int32)
        .ok_or(ExecError::Int4OutOfRange)
}

pub(super) fn eval_width_bucket_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, ..] | [_, Value::Null, ..] | [_, _, Value::Null, _] | [_, _, _, Value::Null] => {
            Ok(Value::Null)
        }
        [Value::Float64(operand), Value::Float64(low), Value::Float64(high), Value::Int32(count)] => {
            eval_width_bucket_float(*operand, *low, *high, *count)
        }
        [operand, low, high, Value::Int32(count)] => {
            let operand = value_as_numeric(operand).ok_or_else(|| ExecError::TypeMismatch {
                op: "width_bucket",
                left: operand.clone(),
                right: low.clone(),
            })?;
            let low = value_as_numeric(low).ok_or_else(|| ExecError::TypeMismatch {
                op: "width_bucket",
                left: low.clone(),
                right: high.clone(),
            })?;
            let high = value_as_numeric(high).ok_or_else(|| ExecError::TypeMismatch {
                op: "width_bucket",
                left: high.clone(),
                right: Value::Int32(*count),
            })?;
            eval_width_bucket_numeric(&operand, &low, &high, *count)
        }
        [left, right, ..] => Err(ExecError::TypeMismatch {
            op: "width_bucket",
            left: left.clone(),
            right: right.clone(),
        }),
        _ => Ok(Value::Null),
    }
}
