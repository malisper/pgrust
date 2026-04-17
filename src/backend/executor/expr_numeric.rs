use num_bigint::BigInt;
use num_integer::Integer;
use num_traits::{Signed, ToPrimitive, Zero};
use std::cmp::Ordering;

use super::ExecError;
use super::expr_ops::parse_numeric_text;
use super::node_types::{NumericValue, Value};

const NUMERIC_MIN_SIG_DIGITS: i32 = 16;
const NUMERIC_MIN_DISPLAY_SCALE: i32 = 0;
const NUMERIC_MAX_DISPLAY_SCALE: i32 = 16383;
const NUMERIC_MAX_RESULT_WEIGHT: f64 = 131072.0;
const NUMERIC_GUARD_DIGITS: i32 = 20;
const LOG10_E: f64 = 0.434294481903252;
const LOG10_2: f64 = 0.301029995663981;
const LN_10: f64 = 2.302585092994046;
const NUMERIC_EXP_LIMIT: i64 = (NUMERIC_MAX_DISPLAY_SCALE as i64) * 3;

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

fn numeric_const(text: &str) -> NumericValue {
    parse_numeric_text(text).expect("valid numeric constant")
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

fn clamp_display_scale(scale: i32) -> u32 {
    scale.clamp(NUMERIC_MIN_DISPLAY_SCALE, NUMERIC_MAX_DISPLAY_SCALE) as u32
}

fn numeric_sign(value: &NumericValue) -> i32 {
    match value {
        NumericValue::PosInf => 1,
        NumericValue::NegInf => -1,
        NumericValue::NaN => 0,
        NumericValue::Finite { coeff, .. } if coeff.is_zero() => 0,
        NumericValue::Finite { coeff, .. } if coeff.is_negative() => -1,
        NumericValue::Finite { .. } => 1,
    }
}

fn numeric_is_zero(value: &NumericValue) -> bool {
    matches!(value, NumericValue::Finite { coeff, .. } if coeff.is_zero())
}

fn numeric_is_integral(value: &NumericValue) -> bool {
    matches!(
        value.clone().normalize(),
        NumericValue::Finite { scale: 0, .. }
    )
}

fn integral_is_odd(value: &NumericValue) -> bool {
    match value.clone().normalize() {
        NumericValue::Finite {
            coeff, scale: 0, ..
        } => coeff.is_odd(),
        _ => false,
    }
}

fn numeric_to_f64_approx(value: &NumericValue) -> f64 {
    if let Some(exact) = numeric_to_f64(value) {
        return exact;
    }
    match value {
        NumericValue::PosInf => f64::INFINITY,
        NumericValue::NegInf => f64::NEG_INFINITY,
        NumericValue::NaN => f64::NAN,
        NumericValue::Finite { coeff, scale, .. } => {
            if coeff.is_zero() {
                return 0.0;
            }
            let digits = coeff.abs().to_str_radix(10);
            let head_len = digits.len().min(16);
            let head = digits[..head_len].parse::<f64>().unwrap_or(0.0);
            let exp10 = digits.len() as i32 - head_len as i32 - *scale as i32;
            let magnitude = if exp10 > 308 {
                f64::INFINITY
            } else if exp10 < -350 {
                0.0
            } else {
                head * 10f64.powi(exp10)
            };
            if coeff.is_negative() {
                -magnitude
            } else {
                magnitude
            }
        }
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

fn clamp_numeric_to_scale(value: NumericValue, target_scale: i32) -> NumericValue {
    match value {
        NumericValue::Finite { scale, .. } if target_scale >= 0 && scale <= target_scale as u32 => {
            value
        }
        _ => round_numeric_to_scale(&value, target_scale),
    }
}

fn add_numeric_clamped(
    left: &NumericValue,
    right: &NumericValue,
    target_scale: i32,
) -> NumericValue {
    clamp_numeric_to_scale(left.add(right), target_scale)
}

fn mul_numeric_clamped(
    left: &NumericValue,
    right: &NumericValue,
    target_scale: i32,
) -> NumericValue {
    clamp_numeric_to_scale(left.mul(right), target_scale)
}

fn div_numeric_clamped(
    left: &NumericValue,
    right: &NumericValue,
    target_scale: i32,
) -> Result<NumericValue, ExecError> {
    let divided = left
        .div(right, target_scale.max(0) as u32)
        .ok_or(ExecError::DivisionByZero("/"))?;
    Ok(if target_scale >= 0 {
        divided
    } else {
        round_numeric_to_scale(&divided, target_scale)
    })
}

fn div_numeric_by_i64(
    value: &NumericValue,
    divisor: i64,
    target_scale: i32,
) -> Result<NumericValue, ExecError> {
    div_numeric_clamped(value, &NumericValue::from_i64(divisor), target_scale)
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

fn estimate_ln_dweight(value: &NumericValue) -> i32 {
    if numeric_sign(value) <= 0 {
        return 0;
    }

    let zero_nine = numeric_const("0.9");
    let one = NumericValue::from_i64(1);
    let one_point_one = numeric_const("1.1");

    if value.cmp(&zero_nine) != Ordering::Less && value.cmp(&one_point_one) != Ordering::Greater {
        let delta = value.sub(&one).abs();
        if numeric_is_zero(&delta) {
            0
        } else {
            decimal_weight(&delta)
        }
    } else {
        match value {
            NumericValue::Finite { coeff, scale, .. } if !coeff.is_zero() => {
                let digits = coeff.abs().to_str_radix(10);
                let head_len = digits.len().min(16);
                let head = digits[..head_len].parse::<f64>().unwrap_or(1.0);
                let dweight = digits.len() as i32 - head_len as i32 - *scale as i32;
                let ln_approx = head.ln() + f64::from(dweight) * LN_10;
                if ln_approx == 0.0 {
                    0
                } else {
                    ln_approx.abs().log10() as i32
                }
            }
            _ => 0,
        }
    }
}

fn exp_result_scale(value: &NumericValue) -> u32 {
    let mut approx_weight = numeric_to_f64_approx(value) * LOG10_E;
    approx_weight = approx_weight.clamp(
        -(NUMERIC_MAX_DISPLAY_SCALE as f64),
        NUMERIC_MAX_DISPLAY_SCALE as f64,
    );

    let mut rscale = NUMERIC_MIN_SIG_DIGITS - approx_weight as i32;
    rscale = rscale.max(finite_dscale(value) as i32);
    clamp_display_scale(rscale)
}

fn ln_result_scale(value: &NumericValue) -> u32 {
    let mut rscale = NUMERIC_MIN_SIG_DIGITS - estimate_ln_dweight(value);
    rscale = rscale.max(finite_dscale(value) as i32);
    clamp_display_scale(rscale)
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

fn sqrt_numeric_with_scale(
    value: &NumericValue,
    target_scale: i32,
) -> Result<NumericValue, ExecError> {
    match value {
        NumericValue::NaN => Ok(NumericValue::NaN),
        NumericValue::PosInf => Ok(NumericValue::PosInf),
        NumericValue::NegInf => Err(numeric_domain_error(
            "cannot take square root of a negative number",
        )),
        NumericValue::Finite { coeff, .. } if coeff.is_negative() => Err(numeric_domain_error(
            "cannot take square root of a negative number",
        )),
        NumericValue::Finite { coeff, .. } if coeff.is_zero() => {
            let dscale = target_scale.max(0) as u32;
            Ok(NumericValue::zero().with_dscale(dscale))
        }
        NumericValue::Finite { coeff, scale, .. } => {
            let work_scale = (*scale).div_ceil(2).max(target_scale.max(0) as u32);
            let scaled = coeff * pow10_bigint(work_scale.saturating_mul(2) - *scale);
            let floor = bigint_sqrt_floor(&scaled);
            let remainder = &scaled - (&floor * &floor);
            let rounded = if remainder > floor {
                floor + 1u8
            } else {
                floor
            };
            let work = NumericValue::finite(rounded, work_scale).normalize();
            let result = clamp_numeric_to_scale(work, target_scale);
            Ok(if target_scale >= 0 {
                result.with_dscale(target_scale as u32).normalize()
            } else {
                result.normalize()
            })
        }
    }
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
                return Err(numeric_domain_error(
                    "zero raised to a negative power is undefined",
                ));
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

    match exp {
        1 => return Ok(round_numeric_to_scale(base, out_scale as i32).with_dscale(out_scale)),
        -1 => {
            return NumericValue::from_i64(1)
                .div(base, out_scale)
                .map(|value| value.with_dscale(out_scale))
                .ok_or_else(|| {
                    numeric_domain_error("zero raised to a negative power is undefined")
                });
        }
        2 => return Ok(mul_numeric_clamped(base, base, out_scale as i32).with_dscale(out_scale)),
        _ => {}
    }

    let negative = exp < 0;
    let mut sig_digits = 1 + out_scale as i32 + approx_weight as i32;
    sig_digits = sig_digits.max(1);
    sig_digits += (f64::from(exp.unsigned_abs().max(1) as u32).ln() as i32) + NUMERIC_GUARD_DIGITS;

    let mut base_prod = base.clone();
    let mut result = if exp.unsigned_abs() & 1 == 1 {
        base.clone()
    } else {
        NumericValue::from_i64(1)
    };
    let mut mask = exp.unsigned_abs();

    while {
        mask >>= 1;
        mask > 0
    } {
        let mut local_scale = sig_digits - 2 * decimal_weight(&base_prod);
        local_scale = local_scale.min((2 * finite_dscale(&base_prod) as i32).max(0));
        local_scale = local_scale.max(NUMERIC_MIN_DISPLAY_SCALE);
        base_prod = mul_numeric_clamped(&base_prod, &base_prod, local_scale);

        if mask & 1 == 1 {
            let mut local_scale = sig_digits - decimal_weight(&base_prod) - decimal_weight(&result);
            local_scale =
                local_scale.min((finite_dscale(&base_prod) + finite_dscale(&result)) as i32);
            local_scale = local_scale.max(NUMERIC_MIN_DISPLAY_SCALE);
            result = mul_numeric_clamped(&base_prod, &result, local_scale);
        }

        if numeric_digits_before_decimal(&base_prod) > 131072
            || numeric_digits_before_decimal(&result) > 131072
        {
            if negative {
                return Ok(NumericValue::zero());
            }
            return Err(numeric_domain_error("value overflows numeric format"));
        }
    }

    if negative {
        return NumericValue::from_i64(1)
            .div(&result, out_scale)
            .map(|value| value.with_dscale(out_scale))
            .ok_or_else(|| numeric_domain_error("zero raised to a negative power is undefined"));
    }

    Ok(round_numeric_to_scale(&result, out_scale as i32).with_dscale(out_scale))
}

fn eval_sqrt_numeric(value: &NumericValue) -> Result<NumericValue, ExecError> {
    sqrt_numeric_with_scale(value, sqrt_result_scale(value) as i32)
}

fn eval_exp_numeric_with_scale(
    value: &NumericValue,
    rscale: u32,
) -> Result<NumericValue, ExecError> {
    match value {
        NumericValue::NaN => Ok(NumericValue::NaN),
        NumericValue::PosInf => Ok(NumericValue::PosInf),
        NumericValue::NegInf => Ok(NumericValue::zero()),
        finite if finite.cmp(&NumericValue::from_i64(NUMERIC_EXP_LIMIT)) == Ordering::Greater => {
            Err(numeric_domain_error("value overflows numeric format"))
        }
        finite if finite.cmp(&NumericValue::from_i64(-NUMERIC_EXP_LIMIT)) == Ordering::Less => {
            Ok(NumericValue::zero())
        }
        finite => {
            let mut x = finite.clone();
            let mut ndiv2 = 0;
            let reduce_limit = numeric_const("0.01");
            while x.abs().cmp(&reduce_limit) == Ordering::Greater {
                ndiv2 += 1;
                let local_scale = finite_dscale(finite) as i32 + ndiv2 + NUMERIC_GUARD_DIGITS;
                x = div_numeric_by_i64(&x, 2, local_scale)?;
            }

            let dweight = (numeric_to_f64_approx(finite) * LOG10_E) as i32;
            let sig_digits = (1 + dweight + rscale as i32 + (f64::from(ndiv2) * LOG10_2) as i32)
                .max(0)
                + NUMERIC_GUARD_DIGITS;
            let mut local_scale = sig_digits - 1;

            let one = NumericValue::from_i64(1);
            let mut result = add_numeric_clamped(&one, &x, local_scale);
            let xx = mul_numeric_clamped(&x, &x, local_scale);
            let mut elem = div_numeric_by_i64(&xx, 2, local_scale)?;
            let mut ni = 2i64;

            while !numeric_is_zero(&elem) {
                let next = add_numeric_clamped(&result, &elem, local_scale);
                if next == result {
                    break;
                }
                result = next;

                ni += 1;
                let next_elem = mul_numeric_clamped(&elem, &x, local_scale);
                elem = div_numeric_by_i64(&next_elem, ni, local_scale)?;
            }

            while ndiv2 > 0 {
                local_scale =
                    (sig_digits - 2 * decimal_weight(&result)).max(NUMERIC_MIN_DISPLAY_SCALE);
                result = mul_numeric_clamped(&result, &result, local_scale);
                ndiv2 -= 1;
            }

            let result = round_numeric_to_scale(&result, rscale as i32)
                .with_dscale(rscale)
                .normalize();
            if numeric_digits_before_decimal(&result) > 131072 {
                return Err(numeric_domain_error("value overflows numeric format"));
            }
            Ok(result)
        }
    }
}

fn eval_exp_numeric(value: &NumericValue) -> Result<NumericValue, ExecError> {
    eval_exp_numeric_with_scale(value, exp_result_scale(value))
}

fn eval_ln_numeric_with_scale(
    value: &NumericValue,
    rscale: u32,
) -> Result<NumericValue, ExecError> {
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
        finite if finite.cmp(&NumericValue::from_i64(1)) == Ordering::Equal => {
            Ok(NumericValue::zero().with_dscale(rscale))
        }
        finite => {
            let mut x = finite.clone();
            let mut nsqrt = 0usize;
            let zero_nine = numeric_const("0.9");
            let one = NumericValue::from_i64(1);
            let one_point_one = numeric_const("1.1");

            while x.cmp(&zero_nine) != Ordering::Greater {
                let local_scale = (rscale as i32 - decimal_weight(&x) / 2 + NUMERIC_GUARD_DIGITS)
                    .max(-NUMERIC_MAX_DISPLAY_SCALE);
                x = sqrt_numeric_with_scale(&x, local_scale)?;
                nsqrt += 1;
            }
            while x.cmp(&one_point_one) != Ordering::Less {
                let local_scale = (rscale as i32 - decimal_weight(&x) / 2 + NUMERIC_GUARD_DIGITS)
                    .max(-NUMERIC_MAX_DISPLAY_SCALE);
                x = sqrt_numeric_with_scale(&x, local_scale)?;
                nsqrt += 1;
            }

            let local_scale = (rscale as i32
                + (f64::from((nsqrt + 1) as u32) * LOG10_2) as i32
                + NUMERIC_GUARD_DIGITS)
                .max(NUMERIC_MIN_DISPLAY_SCALE);

            let numerator = x.sub(&one);
            let denominator = x.add(&one);
            let mut result = div_numeric_clamped(&numerator, &denominator, local_scale)?;
            let zsq = mul_numeric_clamped(&result, &result, local_scale);
            let mut term = result.clone();
            let mut ni = 1i64;

            loop {
                ni += 2;
                term = mul_numeric_clamped(&term, &zsq, local_scale);
                let elem = div_numeric_by_i64(&term, ni, local_scale)?;
                if numeric_is_zero(&elem) {
                    break;
                }
                let next = add_numeric_clamped(&result, &elem, local_scale);
                if next == result {
                    break;
                }
                result = next;
            }

            let factor = NumericValue::finite(BigInt::from(1u8) << (nsqrt + 1), 0);
            let result = mul_numeric_clamped(&result, &factor, rscale as i32);
            Ok(round_numeric_to_scale(&result, rscale as i32)
                .with_dscale(rscale)
                .normalize())
        }
    }
}

fn eval_ln_numeric(value: &NumericValue) -> Result<NumericValue, ExecError> {
    eval_ln_numeric_with_scale(value, ln_result_scale(value))
}

fn eval_power_numeric(base: &NumericValue, exp: &NumericValue) -> Result<NumericValue, ExecError> {
    if matches!(base, NumericValue::NaN) {
        return if exp.cmp(&NumericValue::zero()) == Ordering::Equal {
            Ok(NumericValue::from_i64(1))
        } else {
            Ok(NumericValue::NaN)
        };
    }
    if matches!(exp, NumericValue::NaN) {
        return if base.cmp(&NumericValue::from_i64(1)) == Ordering::Equal {
            Ok(NumericValue::from_i64(1))
        } else {
            Ok(NumericValue::NaN)
        };
    }

    let sign1 = numeric_sign(base);
    let sign2 = numeric_sign(exp);
    let base_is_inf = matches!(base, NumericValue::PosInf | NumericValue::NegInf);
    let exp_is_inf = matches!(exp, NumericValue::PosInf | NumericValue::NegInf);

    if base_is_inf || exp_is_inf {
        if sign1 == 0 && sign2 < 0 {
            return Err(numeric_domain_error(
                "zero raised to a negative power is undefined",
            ));
        }
        if sign1 < 0 && !exp_is_inf && !numeric_is_integral(exp) {
            return Err(numeric_domain_error(
                "a negative number raised to a non-integer power yields a complex result",
            ));
        }
        if base.cmp(&NumericValue::from_i64(1)) == Ordering::Equal {
            return Ok(NumericValue::from_i64(1));
        }
        if sign2 == 0 {
            return Ok(NumericValue::from_i64(1));
        }
        if sign1 == 0 && sign2 > 0 {
            return Ok(NumericValue::zero());
        }

        if exp_is_inf {
            if base.cmp(&NumericValue::from_i64(-1)) == Ordering::Equal {
                return Ok(NumericValue::from_i64(1));
            }
            let abs_x_gt_one = if base_is_inf {
                true
            } else {
                base.abs().cmp(&NumericValue::from_i64(1)) == Ordering::Greater
            };
            return Ok(if abs_x_gt_one == (sign2 > 0) {
                NumericValue::PosInf
            } else {
                NumericValue::zero()
            });
        }

        if matches!(base, NumericValue::PosInf) {
            return Ok(if sign2 > 0 {
                NumericValue::PosInf
            } else {
                NumericValue::zero()
            });
        }

        if sign2 < 0 {
            return Ok(NumericValue::zero());
        }
        return Ok(if integral_is_odd(exp) {
            NumericValue::NegInf
        } else {
            NumericValue::PosInf
        });
    }

    if sign1 == 0 && sign2 < 0 {
        return Err(numeric_domain_error(
            "zero raised to a negative power is undefined",
        ));
    }
    if let Some(exp_i64) = finite_integer(exp) {
        return numeric_pow_integer(base, exp_i64, finite_dscale(exp));
    }
    match base {
        NumericValue::Finite { coeff, .. } if coeff.is_zero() => Ok(NumericValue::zero()),
        NumericValue::Finite { coeff, .. } if coeff.is_negative() => Err(numeric_domain_error(
            "a negative number raised to a non-integer power yields a complex result",
        )),
        _ if base.cmp(&NumericValue::from_i64(1)) == Ordering::Equal => {
            Ok(NumericValue::from_i64(1))
        }
        _ => {
            let ln_dweight = estimate_ln_dweight(base);
            let mut local_rscale =
                (NUMERIC_GUARD_DIGITS - ln_dweight).max(NUMERIC_MIN_DISPLAY_SCALE);
            let ln_base = eval_ln_numeric_with_scale(base, clamp_display_scale(local_rscale))?;
            let ln_num = mul_numeric_clamped(&ln_base, exp, local_rscale);

            let mut approx_weight = numeric_to_f64_approx(&ln_num);
            if approx_weight.abs() > NUMERIC_MAX_DISPLAY_SCALE as f64 * 3.01 {
                return if approx_weight > 0.0 {
                    Err(numeric_domain_error("value overflows numeric format"))
                } else {
                    Ok(NumericValue::zero())
                };
            }

            approx_weight *= LOG10_E;
            let mut rscale = NUMERIC_MIN_SIG_DIGITS - approx_weight as i32;
            rscale = rscale.max(finite_dscale(base) as i32);
            rscale = rscale.max(finite_dscale(exp) as i32);
            rscale = rscale.max(NUMERIC_MIN_DISPLAY_SCALE);
            rscale = rscale.min(NUMERIC_MAX_DISPLAY_SCALE);

            let sig_digits = (rscale + approx_weight as i32).max(0);
            local_rscale =
                (sig_digits - ln_dweight + NUMERIC_GUARD_DIGITS).max(NUMERIC_MIN_DISPLAY_SCALE);

            let ln_base = eval_ln_numeric_with_scale(base, clamp_display_scale(local_rscale))?;
            let ln_num = mul_numeric_clamped(&ln_base, exp, local_rscale);

            eval_exp_numeric_with_scale(&ln_num, rscale as u32)
        }
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
                NumericValue::Finite {
                    coeff,
                    scale,
                    dscale,
                } => {
                    { NumericValue::finite(coeff + pow10_bigint(scale), scale).with_dscale(dscale) }
                        .normalize()
                }
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
    eval_log_numeric_binary(&NumericValue::from_i64(10), value)
}

fn eval_log_numeric_binary(
    base: &NumericValue,
    value: &NumericValue,
) -> Result<NumericValue, ExecError> {
    if matches!(base, NumericValue::NaN) || matches!(value, NumericValue::NaN) {
        return Ok(NumericValue::NaN);
    }

    let sign1 = numeric_sign(base);
    let sign2 = numeric_sign(value);
    if sign1 < 0 || sign2 < 0 {
        return Err(numeric_domain_error(
            "cannot take logarithm of a negative number",
        ));
    }
    if sign1 == 0 || sign2 == 0 {
        return Err(numeric_domain_error("cannot take logarithm of zero"));
    }

    match (base, value) {
        (NumericValue::PosInf, NumericValue::PosInf) => Ok(NumericValue::NaN),
        (NumericValue::PosInf, _) => Ok(NumericValue::zero()),
        (_, NumericValue::PosInf) => Ok(NumericValue::PosInf),
        _ if base.cmp(&NumericValue::from_i64(1)) == Ordering::Equal => {
            Err(ExecError::DivisionByZero("/"))
        }
        _ => {
            let ln_base_dweight = estimate_ln_dweight(base);
            let ln_num_dweight = estimate_ln_dweight(value);
            let result_dweight = ln_num_dweight - ln_base_dweight;

            let mut rscale = NUMERIC_MIN_SIG_DIGITS - result_dweight;
            rscale = rscale.max(finite_dscale(base) as i32);
            rscale = rscale.max(finite_dscale(value) as i32);
            rscale = rscale.max(NUMERIC_MIN_DISPLAY_SCALE);
            rscale = rscale.min(NUMERIC_MAX_DISPLAY_SCALE);

            let mut ln_base_rscale =
                rscale + result_dweight - ln_base_dweight + NUMERIC_GUARD_DIGITS;
            ln_base_rscale = ln_base_rscale.max(NUMERIC_MIN_DISPLAY_SCALE);

            let mut ln_num_rscale = rscale + result_dweight - ln_num_dweight + NUMERIC_GUARD_DIGITS;
            ln_num_rscale = ln_num_rscale.max(NUMERIC_MIN_DISPLAY_SCALE);

            let ln_base = eval_ln_numeric_with_scale(base, clamp_display_scale(ln_base_rscale))?;
            let ln_num = eval_ln_numeric_with_scale(value, clamp_display_scale(ln_num_rscale))?;

            ln_num
                .div(&ln_base, clamp_display_scale(rscale + NUMERIC_GUARD_DIGITS))
                .ok_or(ExecError::DivisionByZero("/"))
                .map(|result| {
                    round_numeric_to_scale(&result, rscale)
                        .with_dscale(rscale as u32)
                        .normalize()
                })
        }
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
                NumericValue::Finite {
                    coeff,
                    scale,
                    dscale,
                } if scale == 0 => NumericValue::finite(coeff, 0).with_dscale(dscale),
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
                NumericValue::Finite {
                    coeff,
                    scale,
                    dscale,
                } if scale == 0 => NumericValue::finite(coeff, 0).with_dscale(dscale),
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
        let mut bucket = if !(high - low).is_infinite() {
            (f64::from(count) * ((operand - low) / (high - low))) as i64
        } else {
            // Match PostgreSQL's overflow-avoidance path for finite bounds spanning > DBL_MAX.
            (f64::from(count) * ((operand / 2.0 - low / 2.0) / (high / 2.0 - low / 2.0))) as i64
        };
        if bucket >= i64::from(count) {
            bucket = i64::from(count - 1);
        }
        return i32::try_from(bucket + 1)
            .map(Value::Int32)
            .map_err(|_| ExecError::Int4OutOfRange);
    }
    if operand > low {
        return width_bucket_outside(count, false);
    }
    if operand <= high {
        return width_bucket_outside(count, true);
    }
    let mut bucket = if !(low - high).is_infinite() {
        (f64::from(count) * ((low - operand) / (low - high))) as i64
    } else {
        (f64::from(count) * ((low / 2.0 - operand / 2.0) / (low / 2.0 - high / 2.0))) as i64
    };
    if bucket >= i64::from(count) {
        bucket = i64::from(count - 1);
    }
    i32::try_from(bucket + 1)
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
