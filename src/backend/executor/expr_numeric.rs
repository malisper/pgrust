use super::ExecError;
use super::node_types::Value;

// :HACK: Keep the historical root executor module path while numeric scalar
// helpers live in `pgrust_expr`.
pub(super) fn eval_round_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_numeric::eval_round_function(values).map_err(Into::into)
}

pub(super) fn eval_trunc_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_numeric::eval_trunc_function(values).map_err(Into::into)
}

pub(super) fn eval_sqrt_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_numeric::eval_sqrt_function(values).map_err(Into::into)
}

pub(super) fn eval_exp_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_numeric::eval_exp_function(values).map_err(Into::into)
}

pub(super) fn eval_ln_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_numeric::eval_ln_function(values).map_err(Into::into)
}

pub(crate) fn eval_power_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_numeric::eval_power_function(values).map_err(Into::into)
}

pub(super) fn eval_scale_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_numeric::eval_scale_function(values).map_err(Into::into)
}

pub(super) fn eval_numeric_inc_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_numeric::eval_numeric_inc_function(values).map_err(Into::into)
}

pub(super) fn eval_min_scale_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_numeric::eval_min_scale_function(values).map_err(Into::into)
}

pub(super) fn eval_trim_scale_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_numeric::eval_trim_scale_function(values).map_err(Into::into)
}

pub(super) fn eval_div_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_numeric::eval_div_function(values).map_err(Into::into)
}

pub(super) fn eval_log_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_numeric::eval_log_function(values).map_err(Into::into)
}

pub(super) fn eval_log10_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_numeric::eval_log10_function(values).map_err(Into::into)
}

pub(super) fn eval_factorial_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_numeric::eval_factorial_function(values).map_err(Into::into)
}

pub(super) fn eval_pg_lsn_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_numeric::eval_pg_lsn_function(values).map_err(Into::into)
}

pub(super) fn eval_ceil_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_numeric::eval_ceil_function(values).map_err(Into::into)
}

pub(super) fn eval_floor_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_numeric::eval_floor_function(values).map_err(Into::into)
}

pub(super) fn eval_sign_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_numeric::eval_sign_function(values).map_err(Into::into)
}

pub(super) fn eval_width_bucket_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_numeric::eval_width_bucket_function(values).map_err(Into::into)
}
