// :HACK: Keep the historical root executor path while scalar math lives in pgrust_expr.
use super::ExecError;
use super::expr_casts::cast_value;
use super::node_types::Value;
use crate::backend::parser::{SqlType, SqlTypeKind};

fn map_expr_error(error: pgrust_expr::ExprError) -> ExecError {
    error.into()
}

pub(super) fn eval_abs_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::backend::executor::expr_math::eval_abs_function(values).map_err(map_expr_error)
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
    pgrust_expr::backend::executor::expr_math::eval_float_send_function(op, values, narrow)
        .map_err(map_expr_error)
}

pub(super) fn eval_erf(value: f64) -> Result<f64, ExecError> {
    pgrust_expr::backend::executor::expr_math::eval_erf(value).map_err(map_expr_error)
}

pub(super) fn eval_erfc(value: f64) -> Result<f64, ExecError> {
    pgrust_expr::backend::executor::expr_math::eval_erfc(value).map_err(map_expr_error)
}

pub(super) fn eval_gamma(value: f64) -> Result<f64, ExecError> {
    pgrust_expr::backend::executor::expr_math::eval_gamma(value).map_err(map_expr_error)
}

pub(super) fn eval_lgamma(value: f64) -> Result<f64, ExecError> {
    pgrust_expr::backend::executor::expr_math::eval_lgamma(value).map_err(map_expr_error)
}

pub(super) fn eval_bitcast_integer_to_float4(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::backend::executor::expr_math::eval_bitcast_integer_to_float4(values)
        .map_err(map_expr_error)
}

pub(super) fn eval_bitcast_bigint_to_float8(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::backend::executor::expr_math::eval_bitcast_bigint_to_float8(values)
        .map_err(map_expr_error)
}

pub(super) fn eval_sqrt(value: f64) -> Result<f64, ExecError> {
    pgrust_expr::backend::executor::expr_math::eval_sqrt(value).map_err(map_expr_error)
}

pub(super) fn eval_power(base: f64, exp: f64) -> Result<f64, ExecError> {
    pgrust_expr::backend::executor::expr_math::eval_power(base, exp).map_err(map_expr_error)
}

pub(super) fn eval_exp(value: f64) -> Result<f64, ExecError> {
    pgrust_expr::backend::executor::expr_math::eval_exp(value).map_err(map_expr_error)
}

pub(super) fn eval_ln(value: f64) -> Result<f64, ExecError> {
    pgrust_expr::backend::executor::expr_math::eval_ln(value).map_err(map_expr_error)
}

pub(super) fn eval_acosh(value: f64) -> Result<f64, ExecError> {
    pgrust_expr::backend::executor::expr_math::eval_acosh(value).map_err(map_expr_error)
}

pub(super) fn eval_atanh(value: f64) -> Result<f64, ExecError> {
    pgrust_expr::backend::executor::expr_math::eval_atanh(value).map_err(map_expr_error)
}

pub(super) fn eval_asind(value: f64) -> Result<f64, ExecError> {
    pgrust_expr::backend::executor::expr_math::eval_asind(value).map_err(map_expr_error)
}

pub(super) fn eval_acosd(value: f64) -> Result<f64, ExecError> {
    pgrust_expr::backend::executor::expr_math::eval_acosd(value).map_err(map_expr_error)
}

pub(super) fn snap_degree(value: f64) -> f64 {
    pgrust_expr::backend::executor::expr_math::snap_degree(value)
}

pub(super) fn sind(value: f64) -> f64 {
    pgrust_expr::backend::executor::expr_math::sind(value)
}

pub(super) fn cosd(value: f64) -> f64 {
    pgrust_expr::backend::executor::expr_math::cosd(value)
}

pub(super) fn tand(value: f64) -> f64 {
    pgrust_expr::backend::executor::expr_math::tand(value)
}

pub(super) fn cotd(value: f64) -> f64 {
    pgrust_expr::backend::executor::expr_math::cotd(value)
}

pub(super) fn eval_gcd_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::backend::executor::expr_math::eval_gcd_function(values).map_err(map_expr_error)
}

pub(super) fn eval_lcm_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::backend::executor::expr_math::eval_lcm_function(values).map_err(map_expr_error)
}
