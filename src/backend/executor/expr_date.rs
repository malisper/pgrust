use super::{ExecError, Value};
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::backend::utils::time::datetime::TimeZoneSpec;
use crate::include::nodes::datetime::TimestampADT;
use crate::include::nodes::datum::IntervalValue;

// :HACK: Keep the historical root executor module path while date/time scalar
// helpers live in `pgrust_expr`.
pub(crate) fn timezone_target_offset_seconds(
    value: &Value,
    config: &DateTimeConfig,
) -> Result<i32, ExecError> {
    pgrust_expr::expr_date::timezone_target_offset_seconds(value, config).map_err(Into::into)
}

pub(crate) fn eval_date_part_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_date::eval_date_part_function(values).map_err(Into::into)
}

pub(crate) fn eval_date_part_function_with_config(
    values: &[Value],
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    pgrust_expr::expr_date::eval_date_part_function_with_config(values, config).map_err(Into::into)
}

pub(crate) fn eval_extract_function_with_config(
    values: &[Value],
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    pgrust_expr::expr_date::eval_extract_function_with_config(values, config).map_err(Into::into)
}

pub(crate) fn eval_extract_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_date::eval_extract_function(values).map_err(Into::into)
}

pub(crate) fn eval_timezone_function(
    values: &[Value],
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    pgrust_expr::expr_date::eval_timezone_function(values, config).map_err(Into::into)
}

pub(crate) fn eval_isfinite_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_date::eval_isfinite_function(values).map_err(Into::into)
}

pub(crate) fn eval_date_bin_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_date::eval_date_bin_function(values).map_err(Into::into)
}

pub(crate) fn timezone_interval_seconds(zone: IntervalValue) -> Result<i64, ExecError> {
    pgrust_expr::expr_date::timezone_interval_seconds(zone).map_err(Into::into)
}

pub(crate) fn eval_justify_days_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_date::eval_justify_days_function(values).map_err(Into::into)
}

pub(crate) fn eval_justify_hours_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_date::eval_justify_hours_function(values).map_err(Into::into)
}

pub(crate) fn eval_justify_interval_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_date::eval_justify_interval_function(values).map_err(Into::into)
}

pub(crate) fn eval_date_trunc_function(
    values: &[Value],
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    pgrust_expr::expr_date::eval_date_trunc_function(values, config).map_err(Into::into)
}

pub(crate) fn eval_datetime_add_function(
    values: &[Value],
    subtract: bool,
) -> Result<Value, ExecError> {
    pgrust_expr::expr_date::eval_datetime_add_function(values, subtract).map_err(Into::into)
}

pub(crate) fn eval_age_function(
    values: &[Value],
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    pgrust_expr::expr_date::eval_age_function(values, config).map_err(Into::into)
}

pub(crate) fn add_interval_to_local_timestamp(
    timestamp: TimestampADT,
    interval: IntervalValue,
    subtract: bool,
) -> Result<TimestampADT, ExecError> {
    pgrust_expr::expr_date::add_interval_to_local_timestamp(timestamp, interval, subtract)
        .map_err(Into::into)
}

pub(crate) fn eval_to_timestamp_function(
    values: &[Value],
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    pgrust_expr::expr_date::eval_to_timestamp_function(values, config).map_err(Into::into)
}

pub(crate) fn eval_make_date_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_date::eval_make_date_function(values).map_err(Into::into)
}

pub(crate) fn eval_make_time_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_date::eval_make_time_function(values).map_err(Into::into)
}

pub(crate) fn eval_make_timestamp_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_date::eval_make_timestamp_function(values).map_err(Into::into)
}

pub(crate) fn eval_make_interval_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_date::eval_make_interval_function(values).map_err(Into::into)
}

pub(crate) fn eval_make_timestamptz_function(
    values: &[Value],
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    pgrust_expr::expr_date::eval_make_timestamptz_function(values, config).map_err(Into::into)
}

pub(crate) fn eval_timestamptz_constructor_function(
    values: &[Value],
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    pgrust_expr::expr_date::eval_timestamptz_constructor_function(values, config)
        .map_err(Into::into)
}

pub(crate) fn eval_to_date_function(values: &[Value]) -> Result<Value, ExecError> {
    pgrust_expr::expr_date::eval_to_date_function(values).map_err(Into::into)
}
