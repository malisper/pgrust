use std::f64::consts::PI;

use super::ExecError;
use super::expr_casts::cast_value;
use super::node_types::Value;
use crate::backend::parser::{SqlType, SqlTypeKind};

unsafe extern "C" {
    fn erf(x: f64) -> f64;
    fn erfc(x: f64) -> f64;
    fn tgamma(x: f64) -> f64;
    fn lgamma(x: f64) -> f64;
}

pub(super) fn eval_abs_function(values: &[Value]) -> Result<Value, ExecError> {
    match &values[0] {
        Value::Null => Ok(Value::Null),
        Value::Int16(v) => Ok(Value::Int16(
            v.checked_abs().ok_or(ExecError::Int2OutOfRange)?,
        )),
        Value::Int32(v) => Ok(Value::Int32(
            v.checked_abs().ok_or(ExecError::Int4OutOfRange)?,
        )),
        Value::Int64(v) => Ok(Value::Int64(
            v.checked_abs().ok_or(ExecError::Int8OutOfRange)?,
        )),
        Value::Float64(v) => Ok(Value::Float64(v.abs())),
        Value::Numeric(v) => Ok(Value::Numeric(v.abs())),
        other => Err(ExecError::TypeMismatch {
            op: "abs",
            left: other.clone(),
            right: Value::Null,
        }),
    }
}

pub(super) fn eval_unary_float_function(
    op: &'static str,
    values: &[Value],
    func: impl FnOnce(f64) -> Result<f64, ExecError>,
) -> Result<Value, ExecError> {
    let Some(value) = values.first() else {
        return Ok(Value::Null);
    };
    match value {
        Value::Null => Ok(Value::Null),
        value => {
            let coerced = cast_value(value.clone(), SqlType::new(SqlTypeKind::Float8))?;
            match coerced {
                Value::Float64(v) => Ok(Value::Float64(func(v)?)),
                other => Err(ExecError::TypeMismatch {
                    op,
                    left: other,
                    right: Value::Null,
                }),
            }
        }
    }
}

pub(super) fn eval_binary_float_function(
    op: &'static str,
    values: &[Value],
    func: impl FnOnce(f64, f64) -> Result<f64, ExecError>,
) -> Result<Value, ExecError> {
    let Some(left) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(right) = values.get(1) else {
        return Ok(Value::Null);
    };
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (left, right) => {
            let left = cast_value(left.clone(), SqlType::new(SqlTypeKind::Float8))?;
            let right = cast_value(right.clone(), SqlType::new(SqlTypeKind::Float8))?;
            match (left, right) {
                (Value::Float64(left), Value::Float64(right)) => {
                    Ok(Value::Float64(func(left, right)?))
                }
                (left, right) => Err(ExecError::TypeMismatch { op, left, right }),
            }
        }
    }
}

pub(super) fn eval_float_send_function(
    op: &'static str,
    values: &[Value],
    narrow: bool,
) -> Result<Value, ExecError> {
    let Some(value) = values.first() else {
        return Ok(Value::Null);
    };
    match value {
        Value::Null => Ok(Value::Null),
        Value::Float64(v) => {
            let bytes = if narrow {
                (f64::from(*v as f32) as f32)
                    .to_bits()
                    .to_be_bytes()
                    .to_vec()
            } else {
                v.to_bits().to_be_bytes().to_vec()
            };
            let mut out = String::from("\\x");
            for byte in bytes {
                out.push_str(&format!("{byte:02x}"));
            }
            Ok(Value::Text(out.into()))
        }
        other => Err(ExecError::TypeMismatch {
            op,
            left: other.clone(),
            right: Value::Null,
        }),
    }
}

pub(super) fn eval_erf(value: f64) -> Result<f64, ExecError> {
    Ok(unsafe { erf(value) })
}

pub(super) fn eval_erfc(value: f64) -> Result<f64, ExecError> {
    Ok(unsafe { erfc(value) })
}

pub(super) fn eval_gamma(value: f64) -> Result<f64, ExecError> {
    if value.is_nan() {
        return Ok(f64::NAN);
    }
    if value == f64::INFINITY {
        return Ok(f64::INFINITY);
    }
    if value == f64::NEG_INFINITY
        || value == 0.0
        || (value.is_finite() && value < 0.0 && value.fract() == 0.0)
    {
        return Err(ExecError::FloatOverflow);
    }
    let result = unsafe { tgamma(value) };
    if result.is_nan() {
        return Err(float_domain_error("input is out of range"));
    }
    if result.is_infinite() {
        if value.is_sign_negative() {
            return Err(ExecError::FloatUnderflow);
        }
        return Err(ExecError::FloatOverflow);
    }
    if result == 0.0 && value.is_finite() && value < 0.0 {
        return Err(ExecError::FloatUnderflow);
    }
    Ok(result)
}

pub(super) fn eval_lgamma(value: f64) -> Result<f64, ExecError> {
    if value.is_nan() {
        return Ok(f64::NAN);
    }
    if value == f64::INFINITY || value == f64::NEG_INFINITY {
        return Ok(f64::INFINITY);
    }
    if value == 0.0 || (value.is_finite() && value < 0.0 && value.fract() == 0.0) {
        return Err(ExecError::FloatOverflow);
    }
    let result = unsafe { lgamma(value) };
    if result.is_infinite() {
        return Err(ExecError::FloatOverflow);
    }
    if result.is_nan() {
        return Err(float_domain_error("input is out of range"));
    }
    Ok(result)
}

pub(super) fn eval_bitcast_integer_to_float4(values: &[Value]) -> Result<Value, ExecError> {
    match values.first() {
        Some(Value::Null) | None => Ok(Value::Null),
        Some(Value::Int32(bits)) => Ok(Value::Float64(f32::from_bits(*bits as u32) as f64)),
        Some(other) => Err(ExecError::TypeMismatch {
            op: "bitcast_integer_to_float4",
            left: other.clone(),
            right: Value::Int32(0),
        }),
    }
}

pub(super) fn eval_bitcast_bigint_to_float8(values: &[Value]) -> Result<Value, ExecError> {
    match values.first() {
        Some(Value::Null) | None => Ok(Value::Null),
        Some(Value::Int64(bits)) => Ok(Value::Float64(f64::from_bits(*bits as u64))),
        Some(other) => Err(ExecError::TypeMismatch {
            op: "bitcast_bigint_to_float8",
            left: other.clone(),
            right: Value::Int64(0),
        }),
    }
}

fn float_domain_error(message: impl Into<String>) -> ExecError {
    ExecError::InvalidStorageValue {
        column: String::new(),
        details: message.into(),
    }
}

pub(super) fn eval_sqrt(value: f64) -> Result<f64, ExecError> {
    if value < 0.0 {
        return Err(float_domain_error(
            "cannot take square root of a negative number",
        ));
    }
    Ok(value.sqrt())
}

pub(super) fn eval_power(base: f64, exp: f64) -> Result<f64, ExecError> {
    if base == 0.0 && exp.is_infinite() && exp.is_sign_negative() {
        return Err(float_domain_error(
            "zero raised to a negative power is undefined",
        ));
    }
    if exp.is_nan() {
        return if base == 1.0 { Ok(1.0) } else { Ok(f64::NAN) };
    }
    if exp.is_infinite() {
        if base == 1.0 || base == -1.0 {
            return Ok(1.0);
        }
        let abs_base = base.abs();
        if exp.is_sign_positive() {
            return Ok(if abs_base < 1.0 { 0.0 } else { f64::INFINITY });
        }
        return Ok(if abs_base < 1.0 { f64::INFINITY } else { 0.0 });
    }
    if base.is_infinite() {
        if exp == 0.0 {
            return Ok(1.0);
        }
        if base.is_sign_positive() {
            return Ok(if exp.is_sign_positive() {
                f64::INFINITY
            } else {
                0.0
            });
        }
        if exp.fract() != 0.0 {
            return Err(float_domain_error(
                "a negative number raised to a non-integer power yields a complex result",
            ));
        }
        let odd_integer = (exp as i64).abs() % 2 == 1;
        return if exp.is_sign_positive() {
            Ok(if odd_integer {
                f64::NEG_INFINITY
            } else {
                f64::INFINITY
            })
        } else {
            Ok(if odd_integer { -0.0 } else { 0.0 })
        };
    }
    if base == 0.0 && exp < 0.0 {
        return Err(float_domain_error(
            "zero raised to a negative power is undefined",
        ));
    }
    if base < 0.0 && exp.fract() != 0.0 {
        return Err(float_domain_error(
            "a negative number raised to a non-integer power yields a complex result",
        ));
    }
    let result = base.powf(exp);
    if result == 0.0 && base != 0.0 && exp.is_finite() && base.is_finite() {
        let abs_result = result.abs();
        if abs_result == 0.0 && !base.is_nan() {
            return Err(ExecError::FloatUnderflow);
        }
    }
    if result.is_infinite() && base.is_finite() && exp.is_finite() {
        return Err(ExecError::FloatOverflow);
    }
    Ok(result)
}

pub(super) fn eval_exp(value: f64) -> Result<f64, ExecError> {
    let result = value.exp();
    if result.is_infinite() && value.is_finite() {
        return Err(ExecError::FloatOverflow);
    }
    if result == 0.0 && value.is_finite() {
        return Err(ExecError::FloatUnderflow);
    }
    Ok(result)
}

pub(super) fn eval_ln(value: f64) -> Result<f64, ExecError> {
    if value == 0.0 {
        return Err(float_domain_error("cannot take logarithm of zero"));
    }
    if value < 0.0 {
        return Err(float_domain_error(
            "cannot take logarithm of a negative number",
        ));
    }
    Ok(value.ln())
}

pub(super) fn eval_acosh(value: f64) -> Result<f64, ExecError> {
    if value < 1.0 {
        return Err(float_domain_error("input is out of range"));
    }
    Ok(value.acosh())
}

pub(super) fn eval_atanh(value: f64) -> Result<f64, ExecError> {
    if value.is_nan() {
        return Ok(f64::NAN);
    }
    if !(-1.0..=1.0).contains(&value) || value == -1.0 || value == 1.0 {
        return Err(float_domain_error("input is out of range"));
    }
    Ok(value.atanh())
}

pub(super) fn eval_asind(value: f64) -> Result<f64, ExecError> {
    if !(-1.0..=1.0).contains(&value) {
        return Err(float_domain_error("input is out of range"));
    }
    Ok(snap_degree(value.asin().to_degrees()))
}

pub(super) fn eval_acosd(value: f64) -> Result<f64, ExecError> {
    if !(-1.0..=1.0).contains(&value) {
        return Err(float_domain_error("input is out of range"));
    }
    Ok(snap_degree(value.acos().to_degrees()))
}

fn approx_eq(left: f64, right: f64) -> bool {
    (left - right).abs() <= 1e-12
}

fn snap_degree_unit(value: f64) -> f64 {
    for landmark in [-1.0, -0.5, 0.0, 0.5, 1.0] {
        if approx_eq(value, landmark) {
            return landmark;
        }
    }
    value
}

pub(super) fn snap_degree(value: f64) -> f64 {
    for landmark in [
        -180.0, -135.0, -90.0, -60.0, -45.0, -30.0, 0.0, 30.0, 45.0, 60.0, 90.0, 120.0, 135.0,
        180.0,
    ] {
        if approx_eq(value, landmark) {
            return landmark;
        }
    }
    value
}

fn normalize_degrees(value: f64) -> f64 {
    let mut normalized = value % 360.0;
    if normalized <= -180.0 {
        normalized += 360.0;
    } else if normalized > 180.0 {
        normalized -= 360.0;
    }
    if approx_eq(normalized, 0.0) {
        0.0
    } else {
        normalized
    }
}

pub(super) fn sind(value: f64) -> f64 {
    snap_degree_unit((normalize_degrees(value) * PI / 180.0).sin())
}

pub(super) fn cosd(value: f64) -> f64 {
    snap_degree_unit((normalize_degrees(value) * PI / 180.0).cos())
}

pub(super) fn tand(value: f64) -> f64 {
    let normalized = normalize_degrees(value);
    if approx_eq(normalized.abs(), 90.0) {
        if normalized.is_sign_negative() {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        }
    } else {
        snap_degree_unit((normalized * PI / 180.0).tan())
    }
}

pub(super) fn cotd(value: f64) -> f64 {
    let normalized = normalize_degrees(value);
    let tangent = tand(value);
    if tangent == 0.0 {
        if approx_eq(normalized.abs(), 180.0) {
            f64::NEG_INFINITY
        } else if normalized.is_sign_negative() {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        }
    } else if tangent.is_infinite() {
        0.0
    } else {
        snap_degree_unit(1.0 / tangent)
    }
}

fn numeric_gcd(
    left: &crate::include::nodes::datum::NumericValue,
    right: &crate::include::nodes::datum::NumericValue,
) -> crate::include::nodes::datum::NumericValue {
    use crate::include::nodes::datum::NumericValue;
    use num_bigint::BigInt;
    match (left, right) {
        (NumericValue::NaN, _) | (_, NumericValue::NaN) => NumericValue::NaN,
        (
            NumericValue::Finite {
                coeff: lcoeff,
                scale: lscale,
            },
            NumericValue::Finite {
                coeff: rcoeff,
                scale: rscale,
            },
        ) => {
            // Align scales, compute GCD of integer coefficients, then rescale
            let max_scale = (*lscale).max(*rscale);
            let la = lcoeff * pow10_bigint(max_scale - lscale);
            let ra = rcoeff * pow10_bigint(max_scale - rscale);
            let g = bigint_gcd(&la, &ra);
            NumericValue::Finite {
                coeff: g,
                scale: max_scale,
            }
            .normalize()
        }
        _ => NumericValue::NaN,
    }
}

fn numeric_lcm(
    left: &crate::include::nodes::datum::NumericValue,
    right: &crate::include::nodes::datum::NumericValue,
) -> crate::include::nodes::datum::NumericValue {
    use crate::include::nodes::datum::NumericValue;
    use num_bigint::BigInt;
    use num_traits::Zero;
    match (left, right) {
        (NumericValue::NaN, _) | (_, NumericValue::NaN) => NumericValue::NaN,
        (
            NumericValue::Finite {
                coeff: lcoeff,
                scale: lscale,
            },
            NumericValue::Finite {
                coeff: rcoeff,
                scale: rscale,
            },
        ) => {
            let max_scale = (*lscale).max(*rscale);
            let la = lcoeff * pow10_bigint(max_scale - lscale);
            let ra = rcoeff * pow10_bigint(max_scale - rscale);
            if la.is_zero() || ra.is_zero() {
                return NumericValue::Finite {
                    coeff: BigInt::from(0),
                    scale: max_scale,
                }
                .normalize();
            }
            let g = bigint_gcd(&la, &ra);
            let lcm = (&la / &g) * &ra;
            let lcm = if lcm < BigInt::from(0) { -lcm } else { lcm };
            NumericValue::Finite {
                coeff: lcm,
                scale: max_scale,
            }
            .normalize()
        }
        _ => NumericValue::NaN,
    }
}

fn bigint_gcd(a: &num_bigint::BigInt, b: &num_bigint::BigInt) -> num_bigint::BigInt {
    use num_bigint::BigInt;
    use num_traits::Zero;
    let mut a = if a < &BigInt::from(0) {
        -a.clone()
    } else {
        a.clone()
    };
    let mut b = if b < &BigInt::from(0) {
        -b.clone()
    } else {
        b.clone()
    };
    while !b.is_zero() {
        let r = &a % &b;
        a = b;
        b = r;
    }
    a
}

fn pow10_bigint(exp: u32) -> num_bigint::BigInt {
    let mut v = num_bigint::BigInt::from(1);
    for _ in 0..exp {
        v *= 10;
    }
    v
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

pub(super) fn eval_gcd_function(values: &[Value]) -> Result<Value, ExecError> {
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
        [Value::Numeric(left), Value::Numeric(right)] => {
            Ok(Value::Numeric(numeric_gcd(left, right)))
        }
        [left, right] => Err(ExecError::TypeMismatch {
            op: "gcd",
            left: left.clone(),
            right: right.clone(),
        }),
        _ => Ok(Value::Null),
    }
}

pub(super) fn eval_lcm_function(values: &[Value]) -> Result<Value, ExecError> {
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
        [Value::Numeric(left), Value::Numeric(right)] => {
            Ok(Value::Numeric(numeric_lcm(left, right)))
        }
        [left, right] => Err(ExecError::TypeMismatch {
            op: "lcm",
            left: left.clone(),
            right: right.clone(),
        }),
        _ => Ok(Value::Null),
    }
}
