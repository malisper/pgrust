use crate::backend::utils::misc::guc_datetime::{DateOrder, DateStyleFormat, DateTimeConfig};
use crate::backend::utils::time::date::{format_date_text, format_time_text, format_timetz_text};
use crate::backend::utils::time::datetime::{
    current_postgres_timestamp_usecs, format_time_usecs, timestamp_parts_from_usecs,
    timezone_offset_seconds, timezone_offset_seconds_at_utc, today_pg_days, ymd_from_days,
};
use crate::backend::utils::time::timestamp::{format_timestamp_text, format_timestamptz_text};
use crate::include::nodes::datetime::{
    DATEVAL_NOBEGIN, DATEVAL_NOEND, DateADT, TIMESTAMP_NOBEGIN, TIMESTAMP_NOEND, TimeADT,
    TimeTzADT, TimestampADT, TimestampTzADT, USECS_PER_DAY,
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

fn json_datetime_config(config: &DateTimeConfig) -> DateTimeConfig {
    DateTimeConfig {
        date_style_format: DateStyleFormat::Iso,
        date_order: DateOrder::Ymd,
        time_zone: config.time_zone.clone(),
        transaction_timestamp_usecs: config.transaction_timestamp_usecs,
        statement_timestamp_usecs: config.statement_timestamp_usecs,
        max_stack_depth_kb: config.max_stack_depth_kb,
        xml: config.xml,
    }
}

fn render_json_date_component(pg_days: i32) -> (String, bool) {
    let (mut year, month, day) = ymd_from_days(pg_days);
    let bc = year <= 0;
    if bc {
        year = 1 - year;
    }
    (format!("{year:04}-{month:02}-{day:02}"), bc)
}

fn render_json_offset(offset_seconds: i32) -> String {
    let sign = if offset_seconds < 0 { '-' } else { '+' };
    let mut remaining = offset_seconds.abs();
    let hour = remaining / 3600;
    remaining %= 3600;
    let minute = remaining / 60;
    let second = remaining % 60;
    if second != 0 {
        format!("{sign}{hour:02}:{minute:02}:{second:02}")
    } else {
        format!("{sign}{hour:02}:{minute:02}")
    }
}

fn render_json_timestamp_parts(days: i32, time_usecs: i64, offset_seconds: Option<i32>) -> String {
    let (date, bc) = render_json_date_component(days);
    let mut out = format!("{date}T{}", format_time_usecs(time_usecs));
    if let Some(offset_seconds) = offset_seconds {
        out.push_str(&render_json_offset(offset_seconds));
    }
    if bc {
        out.push_str(" BC");
    }
    out
}

pub(crate) fn render_json_datetime_value_text_with_config(
    value: &Value,
    config: &DateTimeConfig,
) -> Option<String> {
    let json_config = json_datetime_config(config);
    match value {
        Value::Date(v) => {
            if v.0 == DATEVAL_NOEND || v.0 == DATEVAL_NOBEGIN {
                Some(format_date_text(*v, &json_config))
            } else {
                let (date, bc) = render_json_date_component(v.0);
                Some(if bc { format!("{date} BC") } else { date })
            }
        }
        Value::Time(v) => Some(format_time_text(*v, &json_config)),
        Value::TimeTz(v) => Some(format!(
            "{}{}",
            format_time_text(v.time, &json_config),
            render_json_offset(v.offset_seconds)
        )),
        Value::Timestamp(v) => {
            if v.0 == TIMESTAMP_NOEND || v.0 == TIMESTAMP_NOBEGIN {
                Some(format_timestamp_text(*v, &json_config))
            } else {
                let (days, time_usecs) = timestamp_parts_from_usecs(v.0);
                Some(render_json_timestamp_parts(days, time_usecs, None))
            }
        }
        Value::TimestampTz(v) => {
            if v.0 == TIMESTAMP_NOEND || v.0 == TIMESTAMP_NOBEGIN {
                Some(format_timestamptz_text(*v, &json_config))
            } else {
                let offset_seconds = timezone_offset_seconds(config);
                let adjusted = v.0 + i64::from(offset_seconds) * 1_000_000;
                let (days, time_usecs) = timestamp_parts_from_usecs(adjusted);
                Some(render_json_timestamp_parts(
                    days,
                    time_usecs,
                    Some(offset_seconds),
                ))
            }
        }
        _ => None,
    }
}

pub(crate) fn render_json_datetime_value_text(value: &Value) -> Option<String> {
    render_json_datetime_value_text_with_config(value, &DateTimeConfig::default())
}

fn rounded_usecs(value: i64, precision: Option<i32>) -> i64 {
    let precision = precision.unwrap_or(6).clamp(0, 6);
    let factor = 10_i64.pow((6 - precision) as u32);
    if factor <= 1 {
        value
    } else if value >= 0 {
        ((value + factor / 2) / factor) * factor
    } else {
        let quotient = value.div_euclid(factor);
        let remainder = value.rem_euclid(factor);
        (quotient + i64::from(remainder >= factor / 2)) * factor
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
        Value::Timestamp(timestamp) if !timestamp.is_finite() => Value::Timestamp(timestamp),
        Value::Timestamp(crate::include::nodes::datetime::TimestampADT(usecs)) => Value::Timestamp(
            crate::include::nodes::datetime::TimestampADT(rounded_usecs(usecs, precision)),
        ),
        Value::TimestampTz(timestamp) if !timestamp.is_finite() => Value::TimestampTz(timestamp),
        Value::TimestampTz(crate::include::nodes::datetime::TimestampTzADT(usecs)) => {
            Value::TimestampTz(crate::include::nodes::datetime::TimestampTzADT(
                rounded_usecs(usecs, precision),
            ))
        }
        other => other,
    }
}

pub(crate) fn current_date_value_with_config(config: &DateTimeConfig) -> Value {
    current_date_value_from_timestamp_with_config(config, current_postgres_timestamp_usecs())
}

pub(crate) fn current_date_value() -> Value {
    current_date_value_with_config(&DateTimeConfig::default())
}

pub(crate) fn current_date_value_from_timestamp_with_config(
    config: &DateTimeConfig,
    timestamp_usecs: i64,
) -> Value {
    let local = timestamp_usecs + i64::from(timezone_offset_seconds(config)) * 1_000_000;
    let (days, _) = timestamp_parts_from_usecs(local);
    Value::Date(DateADT(days))
}

pub(crate) fn current_time_value_with_config(
    config: &DateTimeConfig,
    precision: Option<i32>,
    with_time_zone: bool,
) -> Value {
    let timestamp_usecs = config
        .transaction_timestamp_usecs
        .unwrap_or_else(current_postgres_timestamp_usecs);
    current_time_value_from_timestamp_with_config(
        config,
        timestamp_usecs,
        precision,
        with_time_zone,
    )
}

pub(crate) fn current_time_value_from_timestamp_with_config(
    config: &DateTimeConfig,
    timestamp_usecs: i64,
    precision: Option<i32>,
    with_time_zone: bool,
) -> Value {
    let offset_seconds = timezone_offset_seconds_at_utc(config, timestamp_usecs);
    let local_timestamp = timestamp_usecs + i64::from(offset_seconds) * 1_000_000;
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
    let timestamp_usecs = config
        .transaction_timestamp_usecs
        .unwrap_or_else(current_postgres_timestamp_usecs);
    current_timestamp_value_from_timestamp_with_config(
        config,
        timestamp_usecs,
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
    if with_time_zone {
        Value::TimestampTz(TimestampTzADT(rounded_usecs(timestamp_usecs, precision)))
    } else {
        let local =
            timestamp_usecs + i64::from(timezone_offset_seconds_at_utc(config, timestamp_usecs)) * 1_000_000;
        Value::Timestamp(TimestampADT(rounded_usecs(local, precision)))
    }
}

pub(crate) fn current_timestamp_value(precision: Option<i32>, with_time_zone: bool) -> Value {
    current_timestamp_value_with_config(&DateTimeConfig::default(), precision, with_time_zone)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_time_precision_rounds_timestamps() {
        assert_eq!(
            apply_time_precision(Value::Timestamp(TimestampADT(1_999_999)), Some(2)),
            Value::Timestamp(TimestampADT(2_000_000))
        );
        assert_eq!(
            apply_time_precision(Value::TimestampTz(TimestampTzADT(1_994_999)), Some(2)),
            Value::TimestampTz(TimestampTzADT(1_990_000))
        );
    }
}
