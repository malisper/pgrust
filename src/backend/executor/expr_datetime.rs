use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::include::nodes::datum::Value;

pub fn render_datetime_value_text_with_config(
    value: &Value,
    config: &DateTimeConfig,
) -> Option<String> {
    pgrust_expr::expr_datetime::render_datetime_value_text_with_config(value, config)
}

// :HACK: Keep the historical root executor module path while datetime scalar
// helpers live in `pgrust_expr`.
pub fn render_datetime_value_text(value: &Value) -> Option<String> {
    pgrust_expr::expr_datetime::render_datetime_value_text(value)
}

pub(crate) fn render_json_datetime_value_text_with_config(
    value: &Value,
    config: &DateTimeConfig,
) -> Option<String> {
    pgrust_expr::expr_datetime::render_json_datetime_value_text_with_config(value, config)
}

pub(crate) fn render_json_datetime_value_text(value: &Value) -> Option<String> {
    pgrust_expr::expr_datetime::render_json_datetime_value_text(value)
}

pub(crate) fn apply_time_precision(value: Value, precision: Option<i32>) -> Value {
    pgrust_expr::expr_datetime::apply_time_precision(value, precision)
}

pub(crate) fn current_date_value_with_config(config: &DateTimeConfig) -> Value {
    pgrust_expr::expr_datetime::current_date_value_with_config(config)
}

pub(crate) fn current_date_value() -> Value {
    pgrust_expr::expr_datetime::current_date_value()
}

pub(crate) fn current_date_value_from_timestamp_with_config(
    config: &DateTimeConfig,
    timestamp_usecs: i64,
) -> Value {
    pgrust_expr::expr_datetime::current_date_value_from_timestamp_with_config(
        config,
        timestamp_usecs,
    )
}

pub(crate) fn current_time_value_with_config(
    config: &DateTimeConfig,
    precision: Option<i32>,
    with_time_zone: bool,
) -> Value {
    pgrust_expr::expr_datetime::current_time_value_with_config(config, precision, with_time_zone)
}

pub(crate) fn current_time_value_from_timestamp_with_config(
    config: &DateTimeConfig,
    timestamp_usecs: i64,
    precision: Option<i32>,
    with_time_zone: bool,
) -> Value {
    pgrust_expr::expr_datetime::current_time_value_from_timestamp_with_config(
        config,
        timestamp_usecs,
        precision,
        with_time_zone,
    )
}

pub(crate) fn current_time_value(precision: Option<i32>, with_time_zone: bool) -> Value {
    pgrust_expr::expr_datetime::current_time_value(precision, with_time_zone)
}

pub(crate) fn current_timestamp_value_with_config(
    config: &DateTimeConfig,
    precision: Option<i32>,
    with_time_zone: bool,
) -> Value {
    pgrust_expr::expr_datetime::current_timestamp_value_with_config(
        config,
        precision,
        with_time_zone,
    )
}

pub(crate) fn current_timestamp_value_from_timestamp_with_config(
    config: &DateTimeConfig,
    timestamp_usecs: i64,
    precision: Option<i32>,
    with_time_zone: bool,
) -> Value {
    pgrust_expr::expr_datetime::current_timestamp_value_from_timestamp_with_config(
        config,
        timestamp_usecs,
        precision,
        with_time_zone,
    )
}

pub(crate) fn current_timestamp_value(precision: Option<i32>, with_time_zone: bool) -> Value {
    pgrust_expr::expr_datetime::current_timestamp_value(precision, with_time_zone)
}
