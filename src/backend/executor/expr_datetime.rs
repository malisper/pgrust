use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::backend::utils::time::date::{format_date_text, format_time_text, format_timetz_text};
use crate::backend::utils::time::datetime::{
    current_postgres_timestamp_usecs, timestamp_parts_from_usecs, timezone_offset_seconds,
    today_pg_days,
};
use crate::backend::utils::time::timestamp::{format_timestamp_text, format_timestamptz_text};
use crate::include::nodes::datetime::{
    TimeADT, TimeTzADT, TimestampADT, TimestampTzADT, USECS_PER_DAY,
};
use crate::include::nodes::datum::Value;

pub fn render_datetime_value_text_with_config(
    value: &Value,
    config: &DateTimeConfig,
) -> Option<String> {
    match value {
        Value::Date(v) => Some(format_date_text(*v, config)),
        Value::Time(v) => Some(format_time_text(*v, config)),
        Value::TimeTz(v) => Some(format_timetz_text(*v, config)),
        Value::Timestamp(v) => Some(format_timestamp_text(*v, config)),
        Value::TimestampTz(v) => Some(format_timestamptz_text(*v, config)),
        _ => None,
    }
}

pub fn render_datetime_value_text(value: &Value) -> Option<String> {
    render_datetime_value_text_with_config(value, &DateTimeConfig::default())
}

fn rounded_usecs(value: i64, precision: Option<i32>) -> i64 {
    let precision = precision.unwrap_or(6).clamp(0, 6);
    let factor = 10_i64.pow((6 - precision) as u32);
    if factor <= 1 {
        value
    } else {
        value.div_euclid(factor) * factor
    }
}

pub(crate) fn apply_time_precision(value: Value, precision: Option<i32>) -> Value {
    match value {
        Value::Time(crate::include::nodes::datetime::TimeADT(usecs)) => Value::Time(
            crate::include::nodes::datetime::TimeADT(rounded_usecs(usecs, precision)),
        ),
        Value::TimeTz(mut timetz) => {
            timetz.time =
                crate::include::nodes::datetime::TimeADT(rounded_usecs(timetz.time.0, precision));
            Value::TimeTz(timetz)
        }
        Value::Timestamp(crate::include::nodes::datetime::TimestampADT(usecs)) => Value::Timestamp(
            crate::include::nodes::datetime::TimestampADT(rounded_usecs(usecs, precision)),
        ),
        Value::TimestampTz(crate::include::nodes::datetime::TimestampTzADT(usecs)) => {
            Value::TimestampTz(crate::include::nodes::datetime::TimestampTzADT(
                rounded_usecs(usecs, precision),
            ))
        }
        other => other,
    }
}

pub(crate) fn current_date_value_with_config(config: &DateTimeConfig) -> Value {
    Value::Date(crate::include::nodes::datetime::DateADT(today_pg_days(
        config,
    )))
}

pub(crate) fn current_date_value() -> Value {
    current_date_value_with_config(&DateTimeConfig::default())
}

pub(crate) fn current_time_value_with_config(
    config: &DateTimeConfig,
    precision: Option<i32>,
    with_time_zone: bool,
) -> Value {
    let offset_seconds = timezone_offset_seconds(config);
    let local_timestamp =
        current_postgres_timestamp_usecs() + i64::from(offset_seconds) * 1_000_000;
    let (_, time_usecs) = timestamp_parts_from_usecs(local_timestamp);
    let time = TimeADT(rounded_usecs(
        time_usecs.rem_euclid(USECS_PER_DAY),
        precision,
    ));
    if with_time_zone {
        Value::TimeTz(TimeTzADT {
            time,
            offset_seconds,
        })
    } else {
        Value::Time(time)
    }
}

pub(crate) fn current_time_value(precision: Option<i32>, with_time_zone: bool) -> Value {
    current_time_value_with_config(&DateTimeConfig::default(), precision, with_time_zone)
}

pub(crate) fn current_timestamp_value_with_config(
    config: &DateTimeConfig,
    precision: Option<i32>,
    with_time_zone: bool,
) -> Value {
    let now = current_postgres_timestamp_usecs();
    if with_time_zone {
        Value::TimestampTz(TimestampTzADT(rounded_usecs(now, precision)))
    } else {
        let local = now + i64::from(timezone_offset_seconds(config)) * 1_000_000;
        Value::Timestamp(TimestampADT(rounded_usecs(local, precision)))
    }
}

pub(crate) fn current_timestamp_value(precision: Option<i32>, with_time_zone: bool) -> Value {
    current_timestamp_value_with_config(&DateTimeConfig::default(), precision, with_time_zone)
}
