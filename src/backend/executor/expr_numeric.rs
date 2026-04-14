use num_bigint::BigInt;
use num_integer::Integer;
use num_traits::{Signed, ToPrimitive, Zero};

use super::ExecError;
use super::expr_ops::parse_numeric_text;
use super::node_types::{NumericValue, Value};

const NUMERIC_MIN_SIG_DIGITS: i32 = 16;
const NUMERIC_MIN_DISPLAY_SCALE: i32 = 0;
const NUMERIC_MAX_DISPLAY_SCALE: i32 = 16383;
const NUMERIC_MAX_RESULT_WEIGHT: f64 = 131072.0;

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

fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Int16(v) => Some(f64::from(*v)),
        Value::Int32(v) => Some(f64::from(*v)),
        Value::Int64(v) => Some(*v as f64),
        Value::Float64(v) => Some(*v),
        Value::Numeric(v) => numeric_to_f64(v),
        Value::Text(text) => parse_numeric_text(text).and_then(|numeric| numeric_to_f64(&numeric)),
        Value::TextRef(_, _) => value
            .as_text()
            .and_then(parse_numeric_text)
            .and_then(|numeric| numeric_to_f64(&numeric)),
        _ => None,
    }
}

fn numeric_to_f64(value: &NumericValue) -> Option<f64> {
    match value {
        NumericValue::PosInf => Some(f64::INFINITY),
        NumericValue::NegInf => Some(f64::NEG_INFINITY),
        NumericValue::NaN => Some(f64::NAN),
        NumericValue::Finite { coeff, scale, .. } => {
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

fn finite_integer(value: &NumericValue) -> Option<i64> {
    match value {
        NumericValue::Finite { coeff, scale, .. } if *scale == 0 => coeff.to_i64(),
        _ => None,
    }
}

fn finite_dscale(value: &NumericValue) -> u32 {
    match value {
        NumericValue::Finite { dscale, .. } => *dscale,
        _ => 0,
    }
}

fn approximate_decimal_weight(value: &NumericValue) -> f64 {
    match value {
        NumericValue::Finite { coeff, scale, .. } if !coeff.is_zero() => {
            let digits = coeff.abs().to_str_radix(10);
            let head_len = digits.len().min(16);
            let head = digits[..head_len].parse::<f64>().unwrap_or(1.0);
            head.log10() + (digits.len() - head_len) as f64 - f64::from(*scale)
        }
        _ => 0.0,
    }
}

fn choose_power_result_scale(base: &NumericValue, exp_dscale: u32, approx_weight: f64) -> u32 {
    let desired = (NUMERIC_MIN_SIG_DIGITS - approx_weight as i32)
        .clamp(NUMERIC_MIN_DISPLAY_SCALE, NUMERIC_MAX_DISPLAY_SCALE) as f64;
    let mut rscale = desired as i32;
    rscale = rscale.max(finite_dscale(base) as i32);
    rscale = rscale.max(exp_dscale as i32);
    rscale = rscale.max(NUMERIC_MIN_DISPLAY_SCALE);
    rscale = rscale.min(NUMERIC_MAX_DISPLAY_SCALE);
    rscale as u32
}

fn clamp_numeric_scale(value: NumericValue, max_scale: u32) -> NumericValue {
    match value {
        NumericValue::Finite { scale, .. } if scale > max_scale => value
            .round_to_scale(max_scale)
            .unwrap_or(value)
            .with_dscale(max_scale),
        other => other,
    }
}

fn floor_div_i32(value: i32, divisor: i32) -> i32 {
    if value >= 0 {
        value / divisor
    } else {
        -(((-value) + divisor - 1) / divisor)
    }
}

fn decimal_weight(value: &NumericValue) -> i32 {
    match value {
        NumericValue::Finite { coeff, scale, .. } if !coeff.is_zero() => {
            coeff.abs().to_str_radix(10).len() as i32 - (*scale as i32) - 1
        }
        _ => 0,
    }
}

fn sqrt_result_scale(value: &NumericValue) -> u32 {
    let exponent = floor_div_i32(decimal_weight(value), 2);
    let sweight = exponent + 1;
    let mut rscale = NUMERIC_MIN_SIG_DIGITS - sweight;
    rscale = rscale.max(finite_dscale(value) as i32);
    rscale = rscale.max(NUMERIC_MIN_DISPLAY_SCALE);
    rscale = rscale.min(NUMERIC_MAX_DISPLAY_SCALE);
    rscale as u32
}

fn bigint_sqrt_floor(value: &BigInt) -> BigInt {
    if value <= &BigInt::from(0u8) {
        return BigInt::from(0u8);
    }
    let mut guess = pow10_bigint(value.abs().to_str_radix(10).len().div_ceil(2) as u32);
    loop {
        let next = (&guess + value / &guess) / 2u8;
        if next >= guess {
            break;
        }
        guess = next;
    }
    while (&guess * &guess) > *value {
        guess -= 1u8;
    }
    loop {
        let next = &guess + 1u8;
        if (&next * &next) > *value {
            break;
        }
        guess = next;
    }
    guess
}

fn numeric_pow_integer(
    base: &NumericValue,
    exp: i64,
    exp_dscale: u32,
) -> Result<NumericValue, ExecError> {
    let approx_weight = approximate_decimal_weight(base) * exp as f64;
    let out_scale = choose_power_result_scale(base, exp_dscale, approx_weight);

    if exp == 0 {
        return Ok(NumericValue::from_i64(1).with_dscale(out_scale));
    }

    match base {
        NumericValue::Finite { coeff, .. } if coeff.is_zero() => {
            if exp < 0 {
                return Err(numeric_domain_error("zero raised to a negative power is undefined"));
            }
            return Ok(NumericValue::zero().with_dscale(out_scale));
        }
        NumericValue::Finite { coeff, scale, .. }
            if *scale == 0 && coeff.abs() == BigInt::from(1u8) =>
        {
            let sign = if coeff.is_negative() && exp % 2 != 0 {
                -1
            } else {
                1
            };
            return Ok(NumericValue::from_i64(sign).with_dscale(out_scale));
        }
        _ => {}
    }

    if approx_weight > NUMERIC_MAX_RESULT_WEIGHT {
        return Err(numeric_domain_error("value overflows numeric format"));
    }
    if approx_weight + 1.0 < -(out_scale as f64) {
        return Ok(NumericValue::zero().with_dscale(out_scale));
    }

    let negative = exp < 0;
    let work_scale = out_scale.saturating_add(8).min(NUMERIC_MAX_DISPLAY_SCALE as u32);
    let mut result = NumericValue::from_i64(1);
    let mut power = clamp_numeric_scale(base.clone(), work_scale);
    let mut remaining = exp.unsigned_abs();

    while remaining > 0 {
        if remaining & 1 == 1 {
            result = clamp_numeric_scale(result.mul(&power), work_scale);
            if !negative && numeric_digits_before_decimal(&result) > 131072 {
                return Err(numeric_domain_error("value overflows numeric format"));
            }
        }
        remaining >>= 1;
        if remaining > 0 {
            power = clamp_numeric_scale(power.mul(&power), work_scale);
            if !negative && numeric_digits_before_decimal(&power) > 131072 {
                return Err(numeric_domain_error("value overflows numeric format"));
            }
        }
    }

    if negative {
        return NumericValue::from_i64(1)
            .div(&result, out_scale)
            .map(|value| value.with_dscale(out_scale))
            .ok_or_else(|| numeric_domain_error("zero raised to a negative power is undefined"));
    }

    Ok(result.with_dscale(out_scale))
}

fn eval_sqrt_numeric(value: &NumericValue) -> Result<NumericValue, ExecError> {
    match value {
        NumericValue::NaN => Ok(NumericValue::NaN),
        NumericValue::PosInf => Ok(NumericValue::PosInf),
        NumericValue::NegInf => Err(numeric_domain_error(
            "cannot take square root of a negative number",
        )),
        NumericValue::Finite { coeff, .. } if coeff.is_negative() => Err(numeric_domain_error(
            "cannot take square root of a negative number",
        )),
        NumericValue::Finite { coeff, scale, .. } if coeff.is_zero() => {
            let rscale = sqrt_result_scale(value);
            Ok(NumericValue::zero().with_dscale(rscale))
        }
        NumericValue::Finite { coeff, scale, .. } => {
            let rscale = sqrt_result_scale(value);
            let exp = rscale.saturating_mul(2).saturating_sub(*scale);
            let scaled = coeff * pow10_bigint(exp);
            let floor = bigint_sqrt_floor(&scaled);
            let remainder = scaled - (&floor * &floor);
            let rounded = if remainder > floor {
                floor + 1u8
            } else {
                floor
            };
            Ok(NumericValue::finite(rounded, rscale)
                .with_dscale(rscale)
                .normalize())
        }
    }
}

fn eval_exp_numeric(value: &NumericValue) -> Result<NumericValue, ExecError> {
    match value {
        NumericValue::NaN => Ok(NumericValue::NaN),
        NumericValue::PosInf => Ok(NumericValue::PosInf),
        NumericValue::NegInf => Ok(NumericValue::zero()),
        finite => {
            let as_f64 = numeric_to_f64(finite).unwrap_or(f64::NAN);
            let result = as_f64.exp();
            if result.is_infinite() && as_f64.is_finite() {
                return Err(numeric_domain_error("value overflows numeric format"));
            }
            Ok(numeric_from_f64(result, 16))
        }
    }
}

fn eval_ln_numeric(value: &NumericValue) -> Result<NumericValue, ExecError> {
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
            numeric_to_f64(finite).unwrap_or(f64::NAN).ln(),
            16,
        )),
    }
}

fn eval_power_numeric(base: &NumericValue, exp: &NumericValue) -> Result<NumericValue, ExecError> {
    if matches!(exp, NumericValue::NaN) {
        return if matches!(base, NumericValue::Finite { coeff, .. } if coeff == &BigInt::from(1u8))
        {
            Ok(NumericValue::from_i64(1))
        } else {
            Ok(NumericValue::NaN)
        };
    }
    if let Some(exp_i64) = finite_integer(exp) {
        return numeric_pow_integer(base, exp_i64, finite_dscale(exp));
    }
    match (base, exp) {
        (NumericValue::NaN, _) => Ok(NumericValue::NaN),
        (NumericValue::Finite { coeff, .. }, _) if coeff.is_zero() => {
            let exp_f = numeric_to_f64(exp).unwrap_or(f64::NAN);
            if exp_f < 0.0 {
                Err(numeric_domain_error("zero raised to a negative power is undefined"))
            } else {
                Ok(NumericValue::zero())
            }
        }
        (NumericValue::Finite { coeff, .. }, _) if coeff.is_negative() => Err(
            numeric_domain_error(
                "a negative number raised to a non-integer power yields a complex result",
            ),
        ),
        (base, exp) => Ok(numeric_from_f64(
            numeric_to_f64(base)
                .unwrap_or(f64::NAN)
                .powf(numeric_to_f64(exp).unwrap_or(f64::NAN)),
            16,
        )),
    }
}

fn round_numeric_to_scale(value: &NumericValue, target_scale: i32) -> NumericValue {
    let target_scale = target_scale.min(16383);
    match value {
        NumericValue::PosInf => NumericValue::PosInf,
        NumericValue::NegInf => NumericValue::NegInf,
        NumericValue::NaN => NumericValue::NaN,
        NumericValue::Finite { coeff, scale, .. } if target_scale >= 0 => value
            .round_to_scale(target_scale as u32)
            .unwrap_or_else(|| value.clone()),
        NumericValue::Finite { coeff, scale, .. } => {
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
            NumericValue::finite(rounded * pow10_bigint(shift), 0)
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
            NumericValue::Finite { coeff, scale, .. } if *scale > target_scale as u32 => {
                let factor = pow10_bigint(*scale - target_scale as u32);
                NumericValue::finite(coeff / factor, target_scale as u32)
                    .with_dscale(target_scale as u32)
            }
            NumericValue::Finite { coeff, scale, .. } if (*scale as i32) < target_scale => {
                let factor = pow10_bigint(target_scale as u32 - *scale);
                NumericValue::finite(coeff * factor, target_scale as u32)
                    .with_dscale(target_scale as u32)
            }
            _ => value.clone(),
        },
        NumericValue::Finite { coeff, scale, .. } => {
            let shift = target_scale.unsigned_abs();
            if negative_scale_rounds_to_zero(coeff, *scale, shift) {
                return NumericValue::zero();
            }
            let factor = pow10_bigint(scale.saturating_add(shift));
            let quotient = coeff / &factor;
            NumericValue::finite(quotient * pow10_bigint(shift), 0)
        }
    }
}

fn numeric_digits_before_decimal(value: &NumericValue) -> u32 {
    match value {
        NumericValue::Finite { coeff, scale, .. } => {
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
    if matches!(value, NumericValue::Finite { .. })
        && numeric_digits_before_decimal(&value) > 131072
    {
        return Err(numeric_domain_error("value overflows numeric format"));
    }
    Ok(value)
}

pub(super) fn eval_round_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [] => Ok(Value::Null),
        [Value::Null] | [_, Value::Null] => Ok(Value::Null),
        [Value::Float64(v)] => Ok(Value::Float64(v.round())),
        [value] => Ok(Value::Numeric(ensure_numeric_range(
            round_numeric_to_scale(
                &value_as_numeric(value).ok_or_else(|| ExecError::TypeMismatch {
                    op: "round",
                    left: value.clone(),
                    right: Value::Null,
                })?,
                0,
            ),
        )?)),
        [value, Value::Int32(scale)] => Ok(Value::Numeric(ensure_numeric_range(
            round_numeric_to_scale(
                &value_as_numeric(value).ok_or_else(|| ExecError::TypeMismatch {
                    op: "round",
                    left: value.clone(),
                    right: Value::Int32(*scale),
                })?,
                *scale,
            ),
        )?)),
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
        [value] => Ok(Value::Numeric(ensure_numeric_range(
            trunc_numeric_to_scale(
                &value_as_numeric(value).ok_or_else(|| ExecError::TypeMismatch {
                    op: "trunc",
                    left: value.clone(),
                    right: Value::Null,
                })?,
                0,
            ),
        )?)),
        [value, Value::Int32(scale)] => Ok(Value::Numeric(ensure_numeric_range(
            trunc_numeric_to_scale(
                &value_as_numeric(value).ok_or_else(|| ExecError::TypeMismatch {
                    op: "trunc",
                    left: value.clone(),
                    right: Value::Int32(*scale),
                })?,
                *scale,
            ),
        )?)),
        [left, right] => Err(ExecError::TypeMismatch {
            op: "trunc",
            left: left.clone(),
            right: right.clone(),
        }),
        _ => Ok(Value::Null),
    }
}

pub(super) fn eval_sqrt_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [] | [Value::Null] => Ok(Value::Null),
        [Value::Float64(v)] => Ok(Value::Float64(super::expr_math::eval_sqrt(*v)?)),
        [value] => Ok(Value::Numeric(eval_sqrt_numeric(
            &value_as_numeric(value).ok_or_else(|| ExecError::TypeMismatch {
                op: "sqrt",
                left: value.clone(),
                right: Value::Null,
            })?,
        )?)),
        [left, right, ..] => Err(ExecError::TypeMismatch {
            op: "sqrt",
            left: left.clone(),
            right: right.clone(),
        }),
    }
}

pub(super) fn eval_exp_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [] | [Value::Null] => Ok(Value::Null),
        [Value::Float64(v)] => Ok(Value::Float64(super::expr_math::eval_exp(*v)?)),
        [value] => Ok(Value::Numeric(eval_exp_numeric(
            &value_as_numeric(value).ok_or_else(|| ExecError::TypeMismatch {
                op: "exp",
                left: value.clone(),
                right: Value::Null,
            })?,
        )?)),
        [left, right, ..] => Err(ExecError::TypeMismatch {
            op: "exp",
            left: left.clone(),
            right: right.clone(),
        }),
    }
}

pub(super) fn eval_ln_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [] | [Value::Null] => Ok(Value::Null),
        [Value::Float64(v)] => Ok(Value::Float64(super::expr_math::eval_ln(*v)?)),
        [value] => Ok(Value::Numeric(eval_ln_numeric(
            &value_as_numeric(value).ok_or_else(|| ExecError::TypeMismatch {
                op: "ln",
                left: value.clone(),
                right: Value::Null,
            })?,
        )?)),
        [left, right, ..] => Err(ExecError::TypeMismatch {
            op: "ln",
            left: left.clone(),
            right: right.clone(),
        }),
    }
}

pub(super) fn eval_power_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [Value::Float64(base), Value::Float64(exp)] => {
            Ok(Value::Float64(super::expr_math::eval_power(*base, *exp)?))
        }
        [left, right] => Ok(Value::Numeric(eval_power_numeric(
            &value_as_numeric(left).ok_or_else(|| ExecError::TypeMismatch {
                op: "power",
                left: left.clone(),
                right: right.clone(),
            })?,
            &value_as_numeric(right).ok_or_else(|| ExecError::TypeMismatch {
                op: "power",
                left: left.clone(),
                right: right.clone(),
            })?,
        )?)),
        [left, right, ..] => Err(ExecError::TypeMismatch {
            op: "power",
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
            Some(NumericValue::PosInf | NumericValue::NegInf | NumericValue::NaN) => {
                Ok(Value::Null)
            }
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
                NumericValue::Finite { coeff, scale, dscale } => {
                    NumericValue::finite(coeff + pow10_bigint(scale), scale)
                        .with_dscale(dscale)
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
            Some(NumericValue::Finite { coeff, scale, .. }) => {
                if coeff.is_zero() {
                    Ok(Value::Int32(0))
                } else {
                    Ok(Value::Int32(
                        scale.saturating_sub(trailing_decimal_zeros(&coeff, scale)) as i32,
                    ))
                }
            }
            Some(NumericValue::PosInf | NumericValue::NegInf | NumericValue::NaN) => {
                Ok(Value::Null)
            }
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
            Some(NumericValue::Finite { coeff, scale, .. }) => {
                if coeff.is_zero() {
                    Ok(Value::Numeric(NumericValue::zero()))
                } else {
                    let zeros = trailing_decimal_zeros(&coeff, scale);
                    Ok(Value::Numeric(
                        NumericValue::finite(coeff / pow10_bigint(zeros), scale - zeros)
                            .with_dscale(scale - zeros)
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
            if matches!(right_num, NumericValue::Finite { ref coeff, .. } if coeff.is_zero()) {
                return if matches!(left_num, NumericValue::NaN) {
                    Ok(Value::Numeric(NumericValue::NaN))
                } else {
                    Err(ExecError::DivisionByZero("/"))
                };
            }
            if matches!(left_num, NumericValue::NaN) || matches!(right_num, NumericValue::NaN) {
                return Ok(Value::Numeric(NumericValue::NaN));
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
                        ..
                    },
                    NumericValue::Finite {
                        coeff: right_coeff,
                        scale: right_scale,
                        ..
                    },
                ) => {
                    let scale = (*left_scale).max(*right_scale);
                    NumericValue::finite(
                        align_coeff(left_coeff, *left_scale, scale)
                            / align_coeff(right_coeff, *right_scale, scale),
                        0,
                    )
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

fn eval_log_numeric_binary(
    base: &NumericValue,
    value: &NumericValue,
) -> Result<NumericValue, ExecError> {
    match (base, value) {
        (NumericValue::NaN, _) | (_, NumericValue::NaN) => Ok(NumericValue::NaN),
        (
            NumericValue::PosInf | NumericValue::NegInf,
            NumericValue::PosInf | NumericValue::NegInf,
        ) => Ok(NumericValue::NaN),
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
        (NumericValue::Finite { coeff, .. }, _) if coeff.is_negative() => Err(
            numeric_domain_error("cannot take logarithm of a negative number"),
        ),
        (_, NumericValue::Finite { coeff, .. }) if coeff.is_negative() => Err(
            numeric_domain_error("cannot take logarithm of a negative number"),
        ),
        (base, value) => Ok(numeric_from_f64(
            numeric_to_f64(value)
                .unwrap_or(f64::NAN)
                .log(numeric_to_f64(base).unwrap_or(f64::NAN)),
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
    let digits = (n * (n / std::f64::consts::E).log10()
        + (2.0 * std::f64::consts::PI * n).log10() / 2.0)
        .floor()
        + 1.0;
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
                    return Err(numeric_domain_error(
                        "factorial of a negative number is undefined",
                    ));
                }
                NumericValue::Finite { coeff, scale, .. } => {
                    if scale != 0 {
                        return Err(numeric_domain_error(
                            "factorial of a negative number is undefined",
                        ));
                    }
                    coeff
                        .to_i64()
                        .ok_or_else(|| numeric_domain_error("value overflows numeric format"))?
                }
            };
            if n < 0 {
                return Err(numeric_domain_error(
                    "factorial of a negative number is undefined",
                ));
            }
            let n = n as u64;
            if factorial_overflows(n) {
                return Err(numeric_domain_error("value overflows numeric format"));
            }
            let mut acc = BigInt::from(1u8);
            for i in 2..=n {
                acc *= i;
            }
            Ok(Value::Numeric(NumericValue::finite(acc, 0).normalize()))
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
                NumericValue::PosInf | NumericValue::NegInf => {
                    Err(numeric_domain_error("pg_lsn out of range"))
                }
                NumericValue::Finite { coeff, scale, .. } => {
                    if scale != 0 {
                        return Err(numeric_domain_error("pg_lsn out of range"));
                    }
                    let value = coeff
                        .to_u64()
                        .ok_or_else(|| numeric_domain_error("pg_lsn out of range"))?;
                    Ok(Value::Text(
                        format!("{:X}/{:X}", value >> 32, value & 0xFFFF_FFFF).into(),
                    ))
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
                NumericValue::Finite { coeff, scale, dscale } if scale == 0 => {
                    NumericValue::finite(coeff, 0).with_dscale(dscale)
                }
                NumericValue::Finite { coeff, scale, .. } => {
                    let factor = pow10_bigint(scale);
                    let quotient = &coeff / &factor;
                    let remainder = &coeff % &factor;
                    let adjusted = if coeff.is_positive() && !remainder.is_zero() {
                        quotient + 1
                    } else {
                        quotient
                    };
                    NumericValue::finite(adjusted, 0).normalize()
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
                NumericValue::Finite { coeff, scale, dscale } if scale == 0 => {
                    NumericValue::finite(coeff, 0).with_dscale(dscale)
                }
                NumericValue::Finite { coeff, scale, .. } => {
                    let factor = pow10_bigint(scale);
                    let quotient = &coeff / &factor;
                    let remainder = &coeff % &factor;
                    let adjusted = if coeff.is_negative() && !remainder.is_zero() {
                        quotient - 1
                    } else {
                        quotient
                    };
                    NumericValue::finite(adjusted, 0).normalize()
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
                NumericValue::Finite { coeff, .. } if coeff.is_negative() => {
                    NumericValue::from_i64(-1)
                }
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
        count
            .checked_add(1)
            .map(Value::Int32)
            .ok_or(ExecError::Int4OutOfRange)
    } else {
        Ok(Value::Int32(0))
    }
}

fn eval_width_bucket_float(
    operand: f64,
    low: f64,
    high: f64,
    count: i32,
) -> Result<Value, ExecError> {
    validate_width_bucket_count(count)?;
    if operand.is_nan() || low.is_nan() || high.is_nan() {
        return Err(numeric_domain_error(
            "operand, lower bound, and upper bound cannot be NaN",
        ));
    }
    if !low.is_finite() || !high.is_finite() {
        return Err(numeric_domain_error(
            "lower and upper bounds must be finite",
        ));
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
        return Err(numeric_domain_error(
            "lower and upper bounds must be finite",
        ));
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
            ..
        },
        NumericValue::Finite {
            coeff: low_coeff,
            scale: low_scale,
            ..
        },
        NumericValue::Finite {
            coeff: high_coeff,
            scale: high_scale,
            ..
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
        [Value::Null, ..]
        | [_, Value::Null, ..]
        | [_, _, Value::Null, _]
        | [_, _, _, Value::Null] => Ok(Value::Null),
        [
            Value::Float64(operand),
            Value::Float64(low),
            Value::Float64(high),
            Value::Int32(count),
        ] => eval_width_bucket_float(*operand, *low, *high, *count),
        [operand, low, high, Value::Int32(count)] => {
            if matches!(operand, Value::Float64(_))
                || matches!(low, Value::Float64(_))
                || matches!(high, Value::Float64(_))
            {
                let operand_f = value_as_f64(operand).ok_or_else(|| ExecError::TypeMismatch {
                    op: "width_bucket",
                    left: operand.clone(),
                    right: low.clone(),
                })?;
                let low_f = value_as_f64(low).ok_or_else(|| ExecError::TypeMismatch {
                    op: "width_bucket",
                    left: low.clone(),
                    right: high.clone(),
                })?;
                let high_f = value_as_f64(high).ok_or_else(|| ExecError::TypeMismatch {
                    op: "width_bucket",
                    left: high.clone(),
                    right: Value::Int32(*count),
                })?;
                return eval_width_bucket_float(operand_f, low_f, high_f, *count);
            }
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
