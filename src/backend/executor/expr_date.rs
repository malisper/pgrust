use super::expr_casts::render_interval_text;
use super::{ExecError, Value};
use crate::backend::executor::expr_datetime::current_date_value_with_config;
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::backend::utils::time::datetime::{DateTimeParseError, TimeZoneSpec};
use crate::backend::utils::time::datetime::{
    current_timezone_name, day_of_week_from_julian_day, day_of_year, days_from_ymd,
    expand_two_digit_year, format_offset, iso_day_of_week_from_julian_day, iso_week_and_year,
    julian_day_from_postgres_date, month_number, parse_offset_seconds, parse_timezone_spec,
    timestamp_parts_from_usecs, timezone_offset_seconds, timezone_offset_seconds_at_utc,
    unix_days_from_postgres_date, ymd_from_days,
};
use crate::backend::utils::time::timestamp::{
    is_valid_finite_timestamp_usecs, make_timestamptz_from_parts, timestamp_at_time_zone,
    timestamptz_at_time_zone,
};
use crate::include::nodes::datetime::{
    DATEVAL_NOBEGIN, DATEVAL_NOEND, DateADT, TIMESTAMP_NOBEGIN, TIMESTAMP_NOEND, TimeADT,
    TimeTzADT, TimestampADT, TimestampTzADT, USECS_PER_DAY, USECS_PER_HOUR, USECS_PER_MINUTE,
    USECS_PER_SEC,
};
use crate::include::nodes::datum::{IntervalValue, NumericValue};
use chrono::Datelike;
use num_traits::ToPrimitive;

const MONTHS_PER_YEAR: i32 = 12;
const DAYS_PER_MONTH: i32 = 30;
const DAYS_PER_WEEK: i32 = 7;
const SECS_PER_DAY: f64 = 86_400.0;
const DAYS_PER_YEAR: f64 = 365.25;

fn extract_year_number(astronomical_year: i32) -> i32 {
    if astronomical_year > 0 {
        astronomical_year
    } else {
        astronomical_year - 1
    }
}

fn extract_century(year: i32) -> i32 {
    if year > 0 {
        (year + 99) / 100
    } else {
        -(((-year - 1) / 100) + 1)
    }
}

fn extract_millennium(year: i32) -> i32 {
    if year > 0 {
        (year + 999) / 1000
    } else {
        -(((-year - 1) / 1000) + 1)
    }
}

fn unsupported_date_part(field: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("unit \"{field}\" not supported for type date"),
        detail: None,
        hint: None,
        sqlstate: "0A000",
    }
}

fn unrecognized_date_part(field: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("unit \"{field}\" not recognized for type date"),
        detail: None,
        hint: None,
        sqlstate: "22023",
    }
}

fn unsupported_time_part(field: &str, with_timezone: bool) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "unit \"{field}\" not supported for type time {} time zone",
            if with_timezone { "with" } else { "without" }
        ),
        detail: None,
        hint: None,
        sqlstate: "0A000",
    }
}

fn unrecognized_time_part(field: &str, with_timezone: bool) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "unit \"{field}\" not recognized for type time {} time zone",
            if with_timezone { "with" } else { "without" }
        ),
        detail: None,
        hint: None,
        sqlstate: "22023",
    }
}

fn normalize_date_part_field(field: &str) -> &str {
    match field {
        "msec" => "milliseconds",
        "usec" => "microseconds",
        "seconds" => "second",
        "timezone_hour" => "timezone_h",
        "timezone_minute" => "timezone_m",
        other => other,
    }
}

fn timestamp_type_name(with_timezone: bool) -> &'static str {
    if with_timezone {
        "timestamp with time zone"
    } else {
        "timestamp without time zone"
    }
}

fn unsupported_timestamp_part(field: &str, with_timezone: bool) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "unit \"{field}\" not supported for type {}",
            timestamp_type_name(with_timezone)
        ),
        detail: None,
        hint: None,
        sqlstate: "0A000",
    }
}

fn unrecognized_timestamp_part(field: &str, with_timezone: bool) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "unit \"{field}\" not recognized for type {}",
            timestamp_type_name(with_timezone)
        ),
        detail: None,
        hint: None,
        sqlstate: "22023",
    }
}

fn unsupported_interval_part(field: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("unit \"{field}\" not supported for type interval"),
        detail: None,
        hint: None,
        sqlstate: "0A000",
    }
}

fn unsupported_interval_trunc_part(field: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("unit \"{field}\" not supported for type interval"),
        detail: (field == "week").then(|| "Months usually have fractional weeks.".into()),
        hint: None,
        sqlstate: "0A000",
    }
}

fn unrecognized_interval_part(field: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("unit \"{field}\" not recognized for type interval"),
        detail: None,
        hint: None,
        sqlstate: "22023",
    }
}

#[derive(Debug, Clone, Copy)]
struct IntervalParts {
    year: i32,
    month: i32,
    day: i32,
    hour: i64,
    minute: i32,
    second: i32,
    usec: i32,
}

fn interval_to_parts(interval: IntervalValue) -> IntervalParts {
    let mut time = interval.time_micros;
    let hour = time / USECS_PER_HOUR;
    time -= hour * USECS_PER_HOUR;
    let minute = (time / USECS_PER_MINUTE) as i32;
    time -= i64::from(minute) * USECS_PER_MINUTE;
    let second = (time / USECS_PER_SEC) as i32;
    time -= i64::from(second) * USECS_PER_SEC;
    IntervalParts {
        year: interval.months / MONTHS_PER_YEAR,
        month: interval.months % MONTHS_PER_YEAR,
        day: interval.days,
        hour,
        minute,
        second,
        usec: time as i32,
    }
}

fn interval_from_parts(parts: IntervalParts) -> Result<IntervalValue, ExecError> {
    let months = i64::from(parts.year)
        .checked_mul(i64::from(MONTHS_PER_YEAR))
        .and_then(|value| value.checked_add(i64::from(parts.month)))
        .and_then(|value| i32::try_from(value).ok())
        .ok_or_else(interval_out_of_range_error)?;
    let time_micros = parts
        .hour
        .checked_mul(USECS_PER_HOUR)
        .and_then(|value| value.checked_add(i64::from(parts.minute) * USECS_PER_MINUTE))
        .and_then(|value| value.checked_add(i64::from(parts.second) * USECS_PER_SEC))
        .and_then(|value| value.checked_add(i64::from(parts.usec)))
        .ok_or_else(interval_out_of_range_error)?;
    let interval = IntervalValue {
        time_micros,
        days: parts.day,
        months,
    };
    interval
        .is_finite()
        .then_some(interval)
        .ok_or_else(interval_out_of_range_error)
}

fn finite_interval_part(field: &str, interval: IntervalValue) -> Result<Value, ExecError> {
    let parts = interval_to_parts(interval);
    let result = match field {
        "microsecond" | "microseconds" => {
            i64::from(parts.second) as f64 * 1_000_000.0 + f64::from(parts.usec)
        }
        "millisecond" | "milliseconds" => {
            i64::from(parts.second) as f64 * 1_000.0 + f64::from(parts.usec) / 1_000.0
        }
        "second" | "seconds" => f64::from(parts.second) + f64::from(parts.usec) / 1_000_000.0,
        "minute" | "minutes" => f64::from(parts.minute),
        "hour" | "hours" => parts.hour as f64,
        "day" | "days" => f64::from(parts.day),
        "week" | "weeks" => f64::from(parts.day / 7),
        "month" | "months" => f64::from(parts.month),
        "quarter" | "quarters" => {
            if interval.months >= 0 {
                f64::from(parts.month / 3 + 1)
            } else {
                f64::from(-(((-interval.months % MONTHS_PER_YEAR) / 3) + 1))
            }
        }
        "year" | "years" => f64::from(parts.year),
        "decade" | "decades" => f64::from(parts.year / 10),
        "century" | "centuries" => f64::from(parts.year / 100),
        "millennium" | "millenniums" | "millennia" => f64::from(parts.year / 1000),
        "epoch" => {
            interval.time_micros as f64 / 1_000_000.0
                + DAYS_PER_YEAR * SECS_PER_DAY * f64::from(interval.months / MONTHS_PER_YEAR)
                + f64::from(DAYS_PER_MONTH)
                    * SECS_PER_DAY
                    * f64::from(interval.months % MONTHS_PER_YEAR)
                + SECS_PER_DAY * f64::from(interval.days)
        }
        "timezone" | "timezone_h" | "timezone_m" => return Err(unsupported_interval_part(field)),
        _ => return Err(unrecognized_interval_part(field)),
    };
    Ok(Value::Float64(result))
}

fn eval_interval_part(field: &str, interval: IntervalValue) -> Result<Value, ExecError> {
    if interval.is_finite() {
        return finite_interval_part(field, interval);
    }
    let sign = if interval.is_neg_infinity() {
        f64::NEG_INFINITY
    } else {
        f64::INFINITY
    };
    match field {
        "microsecond" | "microseconds" | "millisecond" | "milliseconds" | "second" | "seconds"
        | "minute" | "minutes" | "week" | "weeks" | "month" | "months" | "quarter" | "quarters" => {
            Ok(Value::Null)
        }
        "hour" | "hours" | "day" | "days" | "year" | "years" | "decade" | "decades" | "century"
        | "centuries" | "millennium" | "millenniums" | "millennia" | "epoch" => {
            Ok(Value::Float64(sign))
        }
        "timezone" | "timezone_h" | "timezone_m" => Err(unsupported_interval_part(field)),
        _ => Err(unrecognized_interval_part(field)),
    }
}

fn eval_time_part(field: &str, time: TimeADT, with_timezone: bool) -> Result<Value, ExecError> {
    let second = time.0.rem_euclid(USECS_PER_MINUTE) as f64 / USECS_PER_SEC as f64;
    let result = match field {
        "microsecond" | "microseconds" => time.0.rem_euclid(USECS_PER_MINUTE) as f64,
        "millisecond" | "milliseconds" => time.0.rem_euclid(USECS_PER_MINUTE) as f64 / 1_000.0,
        "second" => second,
        "minute" => time.0.div_euclid(USECS_PER_MINUTE).rem_euclid(60) as f64,
        "hour" => time.0.div_euclid(USECS_PER_HOUR) as f64,
        "epoch" => time.0 as f64 / USECS_PER_SEC as f64,
        "timezone" | "timezone_h" | "timezone_hour" | "timezone_m" | "timezone_minute" | "day"
        | "month" | "year" | "quarter" | "decade" | "century" | "millennium" | "isoyear"
        | "week" | "dow" | "isodow" | "doy" | "julian" => {
            return Err(unsupported_time_part(field, with_timezone));
        }
        _ => return Err(unrecognized_time_part(field, with_timezone)),
    };
    Ok(Value::Float64(result))
}

fn eval_timetz_part(field: &str, timetz: TimeTzADT) -> Result<Value, ExecError> {
    let time_result = eval_time_part(field, timetz.time, true);
    match field {
        "epoch" => {
            let utc_usecs = timetz.time.0 - i64::from(timetz.offset_seconds) * USECS_PER_SEC;
            Ok(Value::Float64(
                utc_usecs.rem_euclid(USECS_PER_DAY) as f64 / USECS_PER_SEC as f64,
            ))
        }
        "timezone" => Ok(Value::Float64(timetz.offset_seconds as f64)),
        "timezone_h" | "timezone_hour" => {
            Ok(Value::Float64((timetz.offset_seconds / 3_600) as f64))
        }
        "timezone_m" | "timezone_minute" => {
            Ok(Value::Float64(((timetz.offset_seconds / 60) % 60) as f64))
        }
        _ => time_result,
    }
}

fn timezone_function_error(message: impl Into<String>, sqlstate: &'static str) -> ExecError {
    ExecError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate,
    }
}

pub(crate) fn timezone_target_offset_seconds(
    value: &Value,
    config: &DateTimeConfig,
) -> Result<i32, ExecError> {
    match value {
        Value::Text(name) => {
            if let Some(offset) = parse_posix_timezone_offset(name) {
                return Ok(offset);
            }
            let spec = parse_timezone_spec(name).map_err(|err| match err {
                DateTimeParseError::UnknownTimeZone(zone) => {
                    timezone_function_error(format!("time zone \"{zone}\" not recognized"), "22023")
                }
                DateTimeParseError::Invalid
                | DateTimeParseError::FieldOutOfRange
                | DateTimeParseError::TimeZoneDisplacementOutOfRange
                | DateTimeParseError::TimestampOutOfRange => timezone_function_error(
                    format!("invalid input syntax for type time zone: \"{name}\""),
                    "22007",
                ),
            })?;
            match spec {
                Some(TimeZoneSpec::FixedOffset(offset)) => Ok(offset),
                Some(TimeZoneSpec::Named(_)) => Ok(timezone_offset_seconds(config)),
                None => Err(timezone_function_error(
                    format!("time zone \"{name}\" not recognized"),
                    "22023",
                )),
            }
        }
        Value::Interval(interval) => i32::try_from(timezone_interval_seconds(*interval)?)
            .map_err(|_| timezone_function_error("time zone interval out of range", "22008")),
        other => Err(ExecError::TypeMismatch {
            op: "timezone",
            left: other.clone(),
            right: Value::TimeTz(TimeTzADT {
                time: TimeADT(0),
                offset_seconds: 0,
            }),
        }),
    }
}

fn parse_posix_timezone_offset(name: &str) -> Option<i32> {
    let trimmed = name.trim();
    let sign_idx = trimmed
        .char_indices()
        .skip(1)
        .find_map(|(idx, ch)| matches!(ch, '+' | '-').then_some(idx))?;
    let prefix = &trimmed[..sign_idx];
    if !(prefix.eq_ignore_ascii_case("utc") || prefix.eq_ignore_ascii_case("gmt")) {
        return None;
    }
    let suffix = &trimmed[sign_idx..];
    let spec = parse_timezone_spec(suffix).ok()??;
    match spec {
        TimeZoneSpec::FixedOffset(offset) => Some(-offset),
        TimeZoneSpec::Named(_) => None,
    }
}

fn retime_timetz(timetz: TimeTzADT, target_offset_seconds: i32) -> TimeTzADT {
    let utc_usecs = timetz.time.0 - i64::from(timetz.offset_seconds) * USECS_PER_SEC;
    let target_time =
        (utc_usecs + i64::from(target_offset_seconds) * USECS_PER_SEC).rem_euclid(USECS_PER_DAY);
    TimeTzADT {
        time: TimeADT(target_time),
        offset_seconds: target_offset_seconds,
    }
}

fn invalid_make_date(year: i32, month: i32, day: i32) -> ExecError {
    ExecError::DetailedError {
        message: format!("date field value out of range: {year}-{month:02}-{day:02}"),
        detail: None,
        hint: None,
        sqlstate: "22008",
    }
}

fn truncation_field_start_display_year(display_year: i32, unit_size: i32) -> i32 {
    if display_year > 0 {
        ((display_year - 1) / unit_size) * unit_size + 1
    } else {
        -(((-display_year - 1) / unit_size + 1) * unit_size)
    }
}

fn display_year_to_astronomical(display_year: i32) -> i32 {
    if display_year > 0 {
        display_year
    } else {
        display_year + 1
    }
}

pub(crate) fn eval_date_part_function(values: &[Value]) -> Result<Value, ExecError> {
    eval_date_part_function_with_config(values, &DateTimeConfig::default())
}

pub(crate) fn eval_date_part_function_with_config(
    values: &[Value],
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    let [field_value, date_value] = values else {
        return Err(ExecError::DetailedError {
            message: "malformed date_part call".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        });
    };
    if matches!(field_value, Value::Null) || matches!(date_value, Value::Null) {
        return Ok(Value::Null);
    }
    let field = field_value
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "date_part",
            left: field_value.clone(),
            right: Value::Text("".into()),
        })?;
    let field = field.trim().to_ascii_lowercase();
    let field = normalize_date_part_field(&field);

    match date_value {
        Value::Interval(interval) => return eval_interval_part(field, *interval),
        Value::Time(time) => return eval_time_part(field, *time, false),
        Value::TimeTz(timetz) => return eval_timetz_part(field, *timetz),
        Value::Timestamp(timestamp) => {
            return eval_timestamp_part(field, timestamp.0, None);
        }
        Value::TimestampTz(timestamp) => {
            return eval_timestamptz_part(field, *timestamp, config);
        }
        Value::Date(_) => {}
        other => {
            return Err(ExecError::TypeMismatch {
                op: "date_part",
                left: field_value.clone(),
                right: other.clone(),
            });
        }
    }
    let Value::Date(date) = date_value else {
        unreachable!("checked above")
    };
    let date = *date;

    if matches!(
        field,
        "microseconds"
            | "milliseconds"
            | "second"
            | "minute"
            | "hour"
            | "timezone"
            | "timezone_m"
            | "timezone_minute"
            | "timezone_h"
            | "timezone_hour"
    ) {
        return Err(unsupported_date_part(field));
    }

    if !matches!(
        field,
        "day"
            | "month"
            | "year"
            | "quarter"
            | "decade"
            | "century"
            | "millennium"
            | "isoyear"
            | "week"
            | "dow"
            | "isodow"
            | "doy"
            | "julian"
            | "epoch"
    ) {
        return Err(unrecognized_date_part(field));
    }

    if !date.is_finite() {
        return Ok(match field {
            "day" | "month" | "quarter" | "week" | "dow" | "isodow" | "doy" => Value::Null,
            "year" | "decade" | "century" | "millennium" | "julian" | "isoyear" | "epoch" => {
                Value::Float64(if date.0.is_positive() {
                    f64::INFINITY
                } else {
                    f64::NEG_INFINITY
                })
            }
            _ => Value::Null,
        });
    }

    let (astronomical_year, month, day) = ymd_from_days(date.0);
    let year = extract_year_number(astronomical_year);
    let julian_day = julian_day_from_postgres_date(date.0);
    let (iso_year_astronomical, iso_week) = iso_week_and_year(astronomical_year, month, day);
    let iso_year = extract_year_number(iso_year_astronomical);

    let result = match field {
        "day" => day as f64,
        "month" => month as f64,
        "year" => year as f64,
        "quarter" => ((month - 1) / 3 + 1) as f64,
        "decade" => astronomical_year.div_euclid(10) as f64,
        "century" => extract_century(year) as f64,
        "millennium" => extract_millennium(year) as f64,
        "isoyear" => iso_year as f64,
        "week" => iso_week as f64,
        "dow" => day_of_week_from_julian_day(julian_day) as f64,
        "isodow" => iso_day_of_week_from_julian_day(julian_day) as f64,
        "doy" => day_of_year(astronomical_year, month, day) as f64,
        "julian" => julian_day as f64,
        "epoch" => unix_days_from_postgres_date(date.0) as f64 * 86_400.0,
        _ => return Err(unrecognized_date_part(field)),
    };
    Ok(Value::Float64(result))
}

pub(crate) fn eval_extract_function_with_config(
    values: &[Value],
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    let [field_value, _] = values else {
        return Err(ExecError::DetailedError {
            message: "malformed date_part call".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        });
    };
    if matches!(field_value, Value::Null) {
        return Ok(Value::Null);
    }
    let field = field_value
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "date_part",
            left: field_value.clone(),
            right: Value::Text("".into()),
        })?
        .trim()
        .to_ascii_lowercase();
    let field = normalize_date_part_field(&field);
    let source_value = &values[1];
    match eval_date_part_function_with_config(values, config)? {
        Value::Float64(value) => Ok(Value::Numeric(extract_numeric_value(
            value,
            extract_numeric_scale(field, source_value),
        ))),
        other => Ok(other),
    }
}

pub(crate) fn eval_extract_function(values: &[Value]) -> Result<Value, ExecError> {
    eval_extract_function_with_config(values, &DateTimeConfig::default())
}

fn extract_numeric_scale(field: &str, source_value: &Value) -> u32 {
    match field {
        "millisecond" | "milliseconds" => 3,
        "epoch" if matches!(source_value, Value::Date(_)) => 0,
        "second" | "epoch" => 6,
        _ => 0,
    }
}

fn extract_numeric_value(value: f64, scale: u32) -> NumericValue {
    if value.is_infinite() {
        return if value.is_sign_positive() {
            NumericValue::PosInf
        } else {
            NumericValue::NegInf
        };
    }
    if value.is_nan() {
        return NumericValue::NaN;
    }
    let value = if value == 0.0 { 0.0 } else { value };
    NumericValue::from(format!("{value:.precision$}", precision = scale as usize))
}

fn timestamp_infinity_part(field: &str, positive: bool) -> Value {
    match field {
        "day" | "month" | "quarter" | "week" | "dow" | "isodow" | "doy" | "timezone"
        | "timezone_h" | "timezone_m" | "microsecond" | "microseconds" | "millisecond"
        | "milliseconds" | "second" | "minute" | "hour" => Value::Null,
        "year" | "decade" | "century" | "millennium" | "julian" | "isoyear" | "epoch" => {
            Value::Float64(if positive {
                f64::INFINITY
            } else {
                f64::NEG_INFINITY
            })
        }
        _ => Value::Null,
    }
}

fn eval_timestamp_part(
    field: &str,
    timestamp_usecs: i64,
    offset_seconds: Option<i32>,
) -> Result<Value, ExecError> {
    if timestamp_usecs == TIMESTAMP_NOEND || timestamp_usecs == TIMESTAMP_NOBEGIN {
        return Ok(timestamp_infinity_part(
            field,
            timestamp_usecs == TIMESTAMP_NOEND,
        ));
    }
    let (days, time_usecs) = timestamp_parts_from_usecs(timestamp_usecs);
    let (astronomical_year, month, day) = ymd_from_days(days);
    let year = extract_year_number(astronomical_year);
    let julian_day = julian_day_from_postgres_date(days);
    let (iso_year_astronomical, iso_week) = iso_week_and_year(astronomical_year, month, day);
    let iso_year = extract_year_number(iso_year_astronomical);
    let second = time_usecs.rem_euclid(USECS_PER_MINUTE) as f64 / USECS_PER_SEC as f64;
    let result = match field {
        "microsecond" | "microseconds" => time_usecs.rem_euclid(USECS_PER_MINUTE) as f64,
        "millisecond" | "milliseconds" => time_usecs.rem_euclid(USECS_PER_MINUTE) as f64 / 1_000.0,
        "second" => second,
        "minute" => time_usecs.div_euclid(USECS_PER_MINUTE).rem_euclid(60) as f64,
        "hour" => time_usecs.div_euclid(USECS_PER_HOUR) as f64,
        "day" => day as f64,
        "month" => month as f64,
        "year" => year as f64,
        "quarter" => ((month - 1) / 3 + 1) as f64,
        "decade" => astronomical_year.div_euclid(10) as f64,
        "century" => extract_century(year) as f64,
        "millennium" => extract_millennium(year) as f64,
        "isoyear" => iso_year as f64,
        "week" => iso_week as f64,
        "dow" => day_of_week_from_julian_day(julian_day) as f64,
        "isodow" => iso_day_of_week_from_julian_day(julian_day) as f64,
        "doy" => day_of_year(astronomical_year, month, day) as f64,
        "julian" => julian_day as f64 + time_usecs as f64 / USECS_PER_DAY as f64,
        "epoch" => {
            unix_days_from_postgres_date(days) as f64 * 86_400.0
                + time_usecs as f64 / USECS_PER_SEC as f64
        }
        "timezone" | "timezone_h" | "timezone_m" if offset_seconds.is_none() => {
            return Err(unsupported_timestamp_part(field, false));
        }
        "timezone" => f64::from(offset_seconds.unwrap_or_default()),
        "timezone_h" => f64::from(offset_seconds.unwrap_or_default() / 3_600),
        "timezone_m" => f64::from(offset_seconds.unwrap_or_default() / 60 % 60),
        _ => return Err(unrecognized_timestamp_part(field, offset_seconds.is_some())),
    };
    Ok(Value::Float64(result))
}

pub(crate) fn eval_timezone_function(
    values: &[Value],
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    let target_offset_seconds;
    let timetz = match values {
        [Value::Null] | [_, Value::Null] => return Ok(Value::Null),
        [Value::TimeTz(value)] => {
            target_offset_seconds = timezone_offset_seconds(config);
            *value
        }
        [zone, Value::TimeTz(value)] => {
            target_offset_seconds = timezone_target_offset_seconds(zone, config)?;
            *value
        }
        [other] => {
            return Err(ExecError::TypeMismatch {
                op: "timezone",
                left: other.clone(),
                right: Value::TimeTz(TimeTzADT {
                    time: TimeADT(0),
                    offset_seconds: 0,
                }),
            });
        }
        _ => {
            return Err(ExecError::DetailedError {
                message: "malformed timezone call".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            });
        }
    };

    Ok(Value::TimeTz(retime_timetz(timetz, target_offset_seconds)))
}

fn eval_timestamptz_part(
    field: &str,
    timestamp: TimestampTzADT,
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    if !timestamp.is_finite() {
        return Ok(timestamp_infinity_part(
            field,
            timestamp.0 == TIMESTAMP_NOEND,
        ));
    }
    let offset = timezone_offset_seconds_at_utc(config, timestamp.0);
    let local = timestamp.0 + i64::from(offset) * USECS_PER_SEC;
    if field == "epoch" {
        return Ok(Value::Float64(
            unix_days_from_postgres_date(0) as f64 * 86_400.0
                + timestamp.0 as f64 / USECS_PER_SEC as f64,
        ));
    }
    eval_timestamp_part(field, local, Some(offset))
}

pub(crate) fn eval_isfinite_function(values: &[Value]) -> Result<Value, ExecError> {
    let [value] = values else {
        return Err(ExecError::DetailedError {
            message: "malformed isfinite call".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        });
    };
    match value {
        Value::Null => Ok(Value::Null),
        Value::Date(date) => Ok(Value::Bool(date.is_finite())),
        Value::Interval(interval) => Ok(Value::Bool(interval.is_finite())),
        other => Err(ExecError::TypeMismatch {
            op: "isfinite",
            left: other.clone(),
            right: Value::Date(crate::include::nodes::datetime::DateADT(0)),
        }),
    }
}

fn truncate_timestamp_local(field: &str, days: i32) -> Result<i32, ExecError> {
    let (astronomical_year, month, _) = ymd_from_days(days);
    let display_year = extract_year_number(astronomical_year);
    let truncated_astronomical_year = match field {
        "day" => return Ok(days),
        "week" => {
            let isodow =
                iso_day_of_week_from_julian_day(julian_day_from_postgres_date(days)) as i32;
            return Ok(days - (isodow - 1));
        }
        "month" => {
            return days_from_ymd(astronomical_year, month, 1).ok_or_else(|| {
                ExecError::DetailedError {
                    message: format!("unit \"{field}\" not supported for type timestamp"),
                    detail: None,
                    hint: None,
                    sqlstate: "0A000",
                }
            });
        }
        "quarter" => {
            let quarter_month = ((month - 1) / 3) * 3 + 1;
            return days_from_ymd(astronomical_year, quarter_month, 1).ok_or_else(|| {
                ExecError::DetailedError {
                    message: format!("unit \"{field}\" not supported for type timestamp"),
                    detail: None,
                    hint: None,
                    sqlstate: "0A000",
                }
            });
        }
        "year" => {
            return days_from_ymd(astronomical_year, 1, 1).ok_or_else(|| {
                ExecError::DetailedError {
                    message: format!("unit \"{field}\" not supported for type timestamp"),
                    detail: None,
                    hint: None,
                    sqlstate: "0A000",
                }
            });
        }
        "millennium" => {
            display_year_to_astronomical(truncation_field_start_display_year(display_year, 1000))
        }
        "century" => {
            display_year_to_astronomical(truncation_field_start_display_year(display_year, 100))
        }
        "decade" => astronomical_year.div_euclid(10) * 10,
        _ => {
            return Err(ExecError::DetailedError {
                message: format!("unit \"{field}\" not supported for type timestamp"),
                detail: None,
                hint: None,
                sqlstate: "0A000",
            });
        }
    };
    days_from_ymd(truncated_astronomical_year, 1, 1).ok_or_else(|| ExecError::DetailedError {
        message: format!("unit \"{field}\" not supported for type timestamp"),
        detail: None,
        hint: None,
        sqlstate: "0A000",
    })
}

fn date_trunc_field_supported(field: &str) -> bool {
    matches!(
        field,
        "microsecond"
            | "microseconds"
            | "millisecond"
            | "milliseconds"
            | "second"
            | "minute"
            | "hour"
            | "day"
            | "week"
            | "month"
            | "quarter"
            | "year"
            | "decade"
            | "century"
            | "millennium"
    )
}

fn date_trunc_field_known(field: &str) -> bool {
    date_trunc_field_supported(field)
        || matches!(
            field,
            "timezone"
                | "timezone_h"
                | "timezone_m"
                | "epoch"
                | "julian"
                | "dow"
                | "isodow"
                | "doy"
                | "isoyear"
        )
}

fn validate_date_trunc_field(field: &str, with_timezone: bool) -> Result<(), ExecError> {
    if date_trunc_field_supported(field) {
        Ok(())
    } else if date_trunc_field_known(field) {
        Err(unsupported_timestamp_part(field, with_timezone))
    } else {
        Err(unrecognized_timestamp_part(field, with_timezone))
    }
}

fn local_timestamp_usecs(days: i32, time_usecs: i64) -> Result<i64, ExecError> {
    let usecs = i128::from(days) * i128::from(USECS_PER_DAY) + i128::from(time_usecs);
    i64::try_from(usecs).map_err(|_| timestamp_out_of_range_error())
}

fn truncate_timestamp_usecs_local(
    field: &str,
    timestamp_usecs: i64,
    with_timezone: bool,
) -> Result<i64, ExecError> {
    validate_date_trunc_field(field, with_timezone)?;
    let (days, time_usecs) = timestamp_parts_from_usecs(timestamp_usecs);
    match field {
        "microsecond" | "microseconds" => Ok(timestamp_usecs),
        "millisecond" | "milliseconds" => {
            local_timestamp_usecs(days, time_usecs.div_euclid(1_000) * 1_000)
        }
        "second" => {
            local_timestamp_usecs(days, time_usecs.div_euclid(USECS_PER_SEC) * USECS_PER_SEC)
        }
        "minute" => local_timestamp_usecs(
            days,
            time_usecs.div_euclid(USECS_PER_MINUTE) * USECS_PER_MINUTE,
        ),
        "hour" => {
            local_timestamp_usecs(days, time_usecs.div_euclid(USECS_PER_HOUR) * USECS_PER_HOUR)
        }
        _ => truncate_timestamp_local(field, days).and_then(|days| local_timestamp_usecs(days, 0)),
    }
}

fn truncate_interval(field: &str, interval: IntervalValue) -> Result<IntervalValue, ExecError> {
    if !interval.is_finite() {
        return match field {
            "millennium" | "millenniums" | "millennia" | "century" | "centuries" | "decade"
            | "decades" | "year" | "years" | "quarter" | "quarters" | "month" | "months"
            | "day" | "days" | "hour" | "hours" | "minute" | "minutes" | "second" | "seconds"
            | "millisecond" | "milliseconds" | "microsecond" | "microseconds" => Ok(interval),
            "week" | "weeks" | "timezone" | "timezone_h" | "timezone_m" => {
                Err(unsupported_interval_trunc_part(field))
            }
            _ => Err(unrecognized_interval_part(field)),
        };
    }

    let mut parts = interval_to_parts(interval);
    match field {
        "millennium" | "millenniums" | "millennia" => {
            parts.year = (parts.year / 1000) * 1000;
            parts.month = 0;
            parts.day = 0;
            parts.hour = 0;
            parts.minute = 0;
            parts.second = 0;
            parts.usec = 0;
        }
        "century" | "centuries" => {
            parts.year = (parts.year / 100) * 100;
            parts.month = 0;
            parts.day = 0;
            parts.hour = 0;
            parts.minute = 0;
            parts.second = 0;
            parts.usec = 0;
        }
        "decade" | "decades" => {
            parts.year = (parts.year / 10) * 10;
            parts.month = 0;
            parts.day = 0;
            parts.hour = 0;
            parts.minute = 0;
            parts.second = 0;
            parts.usec = 0;
        }
        "year" | "years" => {
            parts.month = 0;
            parts.day = 0;
            parts.hour = 0;
            parts.minute = 0;
            parts.second = 0;
            parts.usec = 0;
        }
        "quarter" | "quarters" => {
            parts.month = 3 * (parts.month / 3);
            parts.day = 0;
            parts.hour = 0;
            parts.minute = 0;
            parts.second = 0;
            parts.usec = 0;
        }
        "month" | "months" => {
            parts.day = 0;
            parts.hour = 0;
            parts.minute = 0;
            parts.second = 0;
            parts.usec = 0;
        }
        "day" | "days" => {
            parts.hour = 0;
            parts.minute = 0;
            parts.second = 0;
            parts.usec = 0;
        }
        "hour" | "hours" => {
            parts.minute = 0;
            parts.second = 0;
            parts.usec = 0;
        }
        "minute" | "minutes" => {
            parts.second = 0;
            parts.usec = 0;
        }
        "second" | "seconds" => {
            parts.usec = 0;
        }
        "millisecond" | "milliseconds" => {
            parts.usec = (parts.usec / 1_000) * 1_000;
        }
        "microsecond" | "microseconds" => {}
        "week" | "weeks" | "timezone" | "timezone_h" | "timezone_m" => {
            return Err(unsupported_interval_trunc_part(field));
        }
        _ => return Err(unrecognized_interval_part(field)),
    }
    interval_from_parts(parts)
}

fn datetime_value_out_of_range(message: impl Into<String>) -> ExecError {
    ExecError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate: "22008",
    }
}

fn invalid_parameter_value(message: impl Into<String>) -> ExecError {
    ExecError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate: "22023",
    }
}

fn date_bin_stride_micros(stride: IntervalValue) -> Result<i64, ExecError> {
    if !stride.is_finite() {
        return Err(datetime_value_out_of_range(
            "timestamps cannot be binned into infinite intervals",
        ));
    }
    if stride.months != 0 {
        return Err(ExecError::DetailedError {
            message: "timestamps cannot be binned into intervals containing months or years".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    let micros = i64::from(stride.days)
        .checked_mul(USECS_PER_DAY)
        .and_then(|value| value.checked_add(stride.time_micros))
        .ok_or_else(|| datetime_value_out_of_range("interval out of range"))?;
    if micros <= 0 {
        return Err(datetime_value_out_of_range(
            "stride must be greater than zero",
        ));
    }
    Ok(micros)
}

fn bin_timestamp_value(timestamp: i64, origin: i64, stride: i64) -> Result<i64, ExecError> {
    let diff = timestamp
        .checked_sub(origin)
        .ok_or_else(|| datetime_value_out_of_range("interval out of range"))?;
    let mut result = origin
        .checked_add(diff - diff % stride)
        .ok_or_else(|| datetime_value_out_of_range("timestamp out of range"))?;
    if diff % stride < 0 {
        result = result
            .checked_sub(stride)
            .ok_or_else(|| datetime_value_out_of_range("timestamp out of range"))?;
    }
    Ok(result)
}

fn validate_timestamp_usecs_range(value: i64) -> Result<i64, ExecError> {
    if is_valid_finite_timestamp_usecs(value) {
        Ok(value)
    } else {
        Err(timestamp_out_of_range_error())
    }
}

pub(crate) fn eval_date_bin_function(values: &[Value]) -> Result<Value, ExecError> {
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let [Value::Interval(stride), source, origin] = values else {
        return Err(ExecError::DetailedError {
            message: "malformed date_bin call".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        });
    };
    let stride = date_bin_stride_micros(*stride)?;
    match (source, origin) {
        (Value::Timestamp(source), _) if !source.is_finite() => Ok(Value::Timestamp(*source)),
        (Value::TimestampTz(source), _) if !source.is_finite() => Ok(Value::TimestampTz(*source)),
        (_, Value::Timestamp(origin)) if !origin.is_finite() => {
            Err(datetime_value_out_of_range("origin out of range"))
        }
        (_, Value::TimestampTz(origin)) if !origin.is_finite() => {
            Err(datetime_value_out_of_range("origin out of range"))
        }
        (Value::Timestamp(source), Value::Timestamp(origin)) => Ok(Value::Timestamp(TimestampADT(
            validate_timestamp_usecs_range(bin_timestamp_value(source.0, origin.0, stride)?)?,
        ))),
        (Value::TimestampTz(source), Value::TimestampTz(origin)) => {
            Ok(Value::TimestampTz(TimestampTzADT(
                validate_timestamp_usecs_range(bin_timestamp_value(source.0, origin.0, stride)?)?,
            )))
        }
        _ => Err(ExecError::TypeMismatch {
            op: "date_bin",
            left: source.clone(),
            right: origin.clone(),
        }),
    }
}

pub(crate) fn timezone_interval_seconds(zone: IntervalValue) -> Result<i64, ExecError> {
    let rendered = render_interval_text(zone);
    if !zone.is_finite() {
        return Err(invalid_parameter_value(format!(
            "interval time zone \"{rendered}\" must be finite"
        )));
    }
    if zone.months != 0 || zone.days != 0 {
        return Err(invalid_parameter_value(format!(
            "interval time zone \"{rendered}\" must not include months or days"
        )));
    }
    Ok(zone.time_micros / USECS_PER_SEC)
}

fn checked_justify_result(value: IntervalValue) -> Result<IntervalValue, ExecError> {
    value
        .is_finite()
        .then_some(value)
        .ok_or_else(interval_out_of_range_error)
}

fn justify_hours_value(interval: IntervalValue) -> Result<IntervalValue, ExecError> {
    if !interval.is_finite() {
        return Ok(interval);
    }
    let mut time = interval.time_micros;
    let mut days = interval.days;
    let whole_days = time / USECS_PER_DAY;
    time -= whole_days * USECS_PER_DAY;
    days = days
        .checked_add(i32::try_from(whole_days).map_err(|_| interval_out_of_range_error())?)
        .ok_or_else(interval_out_of_range_error)?;

    if days > 0 && time < 0 {
        time += USECS_PER_DAY;
        days -= 1;
    } else if days < 0 && time > 0 {
        time -= USECS_PER_DAY;
        days += 1;
    }

    checked_justify_result(IntervalValue {
        time_micros: time,
        days,
        months: interval.months,
    })
}

fn justify_days_value(interval: IntervalValue) -> Result<IntervalValue, ExecError> {
    if !interval.is_finite() {
        return Ok(interval);
    }
    let mut months = interval.months;
    let mut days = interval.days;
    let whole_months = days / DAYS_PER_MONTH;
    days -= whole_months * DAYS_PER_MONTH;
    months = months
        .checked_add(whole_months)
        .ok_or_else(interval_out_of_range_error)?;

    if months > 0 && days < 0 {
        days += DAYS_PER_MONTH;
        months -= 1;
    } else if months < 0 && days > 0 {
        days -= DAYS_PER_MONTH;
        months += 1;
    }

    checked_justify_result(IntervalValue {
        time_micros: interval.time_micros,
        days,
        months,
    })
}

fn justify_interval_value(interval: IntervalValue) -> Result<IntervalValue, ExecError> {
    if !interval.is_finite() {
        return Ok(interval);
    }
    let mut months = interval.months;
    let mut days = interval.days;
    let mut time = interval.time_micros;

    if (days > 0 && time > 0) || (days < 0 && time < 0) {
        let whole_months = days / DAYS_PER_MONTH;
        days -= whole_months * DAYS_PER_MONTH;
        months = months
            .checked_add(whole_months)
            .ok_or_else(interval_out_of_range_error)?;
    }

    let whole_days = time / USECS_PER_DAY;
    time -= whole_days * USECS_PER_DAY;
    days = days
        .checked_add(i32::try_from(whole_days).map_err(|_| interval_out_of_range_error())?)
        .ok_or_else(interval_out_of_range_error)?;

    let whole_months = days / DAYS_PER_MONTH;
    days -= whole_months * DAYS_PER_MONTH;
    months = months
        .checked_add(whole_months)
        .ok_or_else(interval_out_of_range_error)?;

    if months > 0 && (days < 0 || (days == 0 && time < 0)) {
        days += DAYS_PER_MONTH;
        months -= 1;
    } else if months < 0 && (days > 0 || (days == 0 && time > 0)) {
        days -= DAYS_PER_MONTH;
        months += 1;
    }

    if days > 0 && time < 0 {
        time += USECS_PER_DAY;
        days -= 1;
    } else if days < 0 && time > 0 {
        time -= USECS_PER_DAY;
        days += 1;
    }

    checked_justify_result(IntervalValue {
        time_micros: time,
        days,
        months,
    })
}

pub(crate) fn eval_justify_days_function(values: &[Value]) -> Result<Value, ExecError> {
    let [value] = values else {
        return Err(ExecError::DetailedError {
            message: "malformed justify_days call".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        });
    };
    match value {
        Value::Null => Ok(Value::Null),
        Value::Interval(interval) => justify_days_value(*interval).map(Value::Interval),
        other => Err(ExecError::TypeMismatch {
            op: "justify_days",
            left: other.clone(),
            right: Value::Interval(IntervalValue::zero()),
        }),
    }
}

pub(crate) fn eval_justify_hours_function(values: &[Value]) -> Result<Value, ExecError> {
    let [value] = values else {
        return Err(ExecError::DetailedError {
            message: "malformed justify_hours call".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        });
    };
    match value {
        Value::Null => Ok(Value::Null),
        Value::Interval(interval) => justify_hours_value(*interval).map(Value::Interval),
        other => Err(ExecError::TypeMismatch {
            op: "justify_hours",
            left: other.clone(),
            right: Value::Interval(IntervalValue::zero()),
        }),
    }
}

pub(crate) fn eval_justify_interval_function(values: &[Value]) -> Result<Value, ExecError> {
    let [value] = values else {
        return Err(ExecError::DetailedError {
            message: "malformed justify_interval call".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        });
    };
    match value {
        Value::Null => Ok(Value::Null),
        Value::Interval(interval) => justify_interval_value(*interval).map(Value::Interval),
        other => Err(ExecError::TypeMismatch {
            op: "justify_interval",
            left: other.clone(),
            right: Value::Interval(IntervalValue::zero()),
        }),
    }
}

pub(crate) fn eval_date_trunc_function(
    values: &[Value],
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    let (field_value, date_value, zone_value) = match values {
        [field_value, date_value] => (field_value, date_value, None),
        [field_value, date_value, zone_value] => (field_value, date_value, Some(zone_value)),
        _ => {
            return Err(ExecError::DetailedError {
                message: "malformed date_trunc call".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            });
        }
    };
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let mut zone_config;
    let config = if let Some(zone_value) = zone_value {
        let zone = zone_value
            .as_text()
            .ok_or_else(|| ExecError::TypeMismatch {
                op: "date_trunc",
                left: zone_value.clone(),
                right: Value::Text("".into()),
            })?;
        zone_config = config.clone();
        zone_config.time_zone = zone.to_string();
        &zone_config
    } else {
        config
    };
    if matches!(field_value, Value::Null) || matches!(date_value, Value::Null) {
        return Ok(Value::Null);
    }
    let field = field_value
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "date_trunc",
            left: field_value.clone(),
            right: Value::Text("".into()),
        })?;
    let field = field.trim().to_ascii_lowercase();
    let field = normalize_date_part_field(&field);
    if field == "ago" && !matches!(date_value, Value::Interval(_)) {
        return Err(unrecognized_timestamp_part(
            field,
            matches!(date_value, Value::TimestampTz(_)),
        ));
    }
    if !matches!(date_value, Value::Interval(_)) {
        validate_date_trunc_field(field, matches!(date_value, Value::TimestampTz(_)))?;
    }
    match date_value {
        Value::Interval(interval) => truncate_interval(field, *interval).map(Value::Interval),
        Value::Date(date) => {
            if !date.is_finite() {
                return Ok(Value::Timestamp(TimestampADT(match date.0 {
                    DATEVAL_NOEND => TIMESTAMP_NOEND,
                    DATEVAL_NOBEGIN => TIMESTAMP_NOBEGIN,
                    _ => unreachable!("checked finite date above"),
                })));
            }
            let days =
                truncate_timestamp_local(&field, date.0).map_err(|_| ExecError::DetailedError {
                    message: format!("unit \"{field}\" not supported for type date"),
                    detail: None,
                    hint: None,
                    sqlstate: "0A000",
                })?;
            let local_midnight = TimestampADT(local_timestamp_usecs(days, 0)?);
            timestamp_at_time_zone(local_midnight, current_timezone_name(config))
                .map(Value::TimestampTz)
                .map_err(|err| ExecError::InvalidStorageValue {
                    column: "time zone".into(),
                    details: super::expr_casts::datetime_parse_error_details(
                        "time zone",
                        current_timezone_name(config),
                        err,
                    ),
                })
        }
        Value::Timestamp(timestamp) => {
            if !timestamp.is_finite() {
                return Ok(Value::Timestamp(*timestamp));
            }
            Ok(Value::Timestamp(TimestampADT(
                truncate_timestamp_usecs_local(field, timestamp.0, false)?,
            )))
        }
        Value::TimestampTz(timestamp) => {
            if !timestamp.is_finite() {
                return Ok(Value::TimestampTz(*timestamp));
            }
            let zone = current_timezone_name(config);
            let local = timestamptz_at_time_zone(*timestamp, zone).map_err(|err| {
                ExecError::InvalidStorageValue {
                    column: "time zone".into(),
                    details: super::expr_casts::datetime_parse_error_details(
                        "time zone",
                        zone,
                        err,
                    ),
                }
            })?;
            let truncated = TimestampADT(truncate_timestamp_usecs_local(field, local.0, true)?);
            timestamp_at_time_zone(truncated, zone)
                .map(Value::TimestampTz)
                .map_err(|err| ExecError::InvalidStorageValue {
                    column: "time zone".into(),
                    details: super::expr_casts::datetime_parse_error_details(
                        "time zone",
                        zone,
                        err,
                    ),
                })
        }
        other => Err(ExecError::TypeMismatch {
            op: "date_trunc",
            left: field_value.clone(),
            right: other.clone(),
        }),
    }
}

fn interval_stride_usecs(interval: IntervalValue) -> Result<i64, ExecError> {
    if interval.months != 0 {
        return Err(ExecError::DetailedError {
            message: "timestamps cannot be binned into intervals containing months or years".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    let stride =
        i128::from(interval.days) * i128::from(USECS_PER_DAY) + i128::from(interval.time_micros);
    if stride <= 0 {
        return Err(ExecError::DetailedError {
            message: "stride must be greater than zero".into(),
            detail: None,
            hint: None,
            sqlstate: "22008",
        });
    }
    i64::try_from(stride).map_err(|_| ExecError::DetailedError {
        message: "interval out of range".into(),
        detail: None,
        hint: None,
        sqlstate: "22008",
    })
}

fn timestamp_bin_usecs(stride: i64, source: i64, origin: i64) -> Result<i64, ExecError> {
    let diff = source
        .checked_sub(origin)
        .ok_or_else(|| ExecError::DetailedError {
            message: "interval out of range".into(),
            detail: None,
            hint: None,
            sqlstate: "22008",
        })?;
    let modulo = diff % stride;
    let delta = diff - modulo;
    let mut result = origin
        .checked_add(delta)
        .ok_or_else(timestamp_out_of_range_error)?;
    if modulo < 0 {
        result = result
            .checked_sub(stride)
            .ok_or_else(timestamp_out_of_range_error)?;
        if !is_valid_finite_timestamp_usecs(result) {
            return Err(timestamp_out_of_range_error());
        }
    }
    Ok(result)
}

fn interval_total_usecs(interval: IntervalValue) -> Result<i64, ExecError> {
    if !interval.is_finite() {
        return Err(ExecError::DetailedError {
            message: "interval out of range".into(),
            detail: None,
            hint: None,
            sqlstate: "22008",
        });
    }
    let usecs = i128::from(interval.months) * 30 * i128::from(USECS_PER_DAY)
        + i128::from(interval.days) * i128::from(USECS_PER_DAY)
        + i128::from(interval.time_micros);
    i64::try_from(usecs).map_err(|_| ExecError::DetailedError {
        message: "interval out of range".into(),
        detail: None,
        hint: None,
        sqlstate: "22008",
    })
}

pub(crate) fn eval_datetime_add_function(
    values: &[Value],
    subtract: bool,
) -> Result<Value, ExecError> {
    let (timestamp, interval, zone) = match values {
        [timestamp, interval] => (timestamp, interval, None),
        [timestamp, interval, zone] => (timestamp, interval, Some(zone)),
        _ => {
            return Err(ExecError::DetailedError {
                message: "malformed date_add call".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            });
        }
    };
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let (Value::TimestampTz(timestamp), Value::Interval(interval)) = (timestamp, interval) else {
        return Err(ExecError::TypeMismatch {
            op: if subtract {
                "date_subtract"
            } else {
                "date_add"
            },
            left: timestamp.clone(),
            right: interval.clone(),
        });
    };
    if let Some(zone) = zone {
        let zone = zone.as_text().ok_or_else(|| ExecError::TypeMismatch {
            op: if subtract {
                "date_subtract"
            } else {
                "date_add"
            },
            left: zone.clone(),
            right: Value::Text("".into()),
        })?;
        let local = timestamptz_at_time_zone(*timestamp, zone).map_err(|err| {
            ExecError::InvalidStorageValue {
                column: "time zone".into(),
                details: super::expr_casts::datetime_parse_error_details("time zone", zone, err),
            }
        })?;
        let local = add_interval_to_local_timestamp(local, *interval, subtract)?;
        return timestamp_at_time_zone(local, zone)
            .map(Value::TimestampTz)
            .map_err(|err| ExecError::InvalidStorageValue {
                column: "time zone".into(),
                details: super::expr_casts::datetime_parse_error_details("time zone", zone, err),
            });
    }
    let delta = interval_total_usecs(*interval)?;
    let delta = if subtract { -delta } else { delta };
    timestamp
        .0
        .checked_add(delta)
        .map(|value| Value::TimestampTz(TimestampTzADT(value)))
        .ok_or_else(timestamp_out_of_range_error)
}

fn timestamp_out_of_range_error() -> ExecError {
    ExecError::DetailedError {
        message: "timestamp out of range".into(),
        detail: None,
        hint: None,
        sqlstate: "22008",
    }
}

fn interval_out_of_range_error() -> ExecError {
    ExecError::DetailedError {
        message: "interval out of range".into(),
        detail: None,
        hint: None,
        sqlstate: "22008",
    }
}

fn timestamp_tz_local_timestamp(
    timestamp: TimestampTzADT,
    config: &DateTimeConfig,
) -> Result<TimestampADT, ExecError> {
    let zone = current_timezone_name(config);
    timestamptz_at_time_zone(timestamp, zone).map_err(|err| ExecError::InvalidStorageValue {
        column: "time zone".into(),
        details: super::expr_casts::datetime_parse_error_details("time zone", zone, err),
    })
}

fn current_date_timestamp_tz(config: &DateTimeConfig) -> Result<TimestampTzADT, ExecError> {
    let Value::Date(date) = current_date_value_with_config(config) else {
        unreachable!("current_date returns date")
    };
    let local_midnight = TimestampADT(i64::from(date.0) * USECS_PER_DAY);
    let zone = current_timezone_name(config);
    timestamp_at_time_zone(local_midnight, zone).map_err(|err| ExecError::InvalidStorageValue {
        column: "time zone".into(),
        details: super::expr_casts::datetime_parse_error_details("time zone", zone, err),
    })
}

fn age_infinity_interval(left: i64, right: i64) -> Option<Result<IntervalValue, ExecError>> {
    match (left, right) {
        (TIMESTAMP_NOEND, TIMESTAMP_NOEND) | (TIMESTAMP_NOBEGIN, TIMESTAMP_NOBEGIN) => {
            Some(Err(interval_out_of_range_error()))
        }
        (TIMESTAMP_NOEND, _) | (_, TIMESTAMP_NOBEGIN) => Some(Ok(IntervalValue::infinity())),
        (TIMESTAMP_NOBEGIN, _) | (_, TIMESTAMP_NOEND) => Some(Ok(IntervalValue::neg_infinity())),
        _ => None,
    }
}

fn symbolic_age_interval(
    left_local: i64,
    right_local: i64,
    left_less: bool,
) -> Result<IntervalValue, ExecError> {
    let (left_days, left_time) = timestamp_parts_from_usecs(left_local);
    let (right_days, right_time) = timestamp_parts_from_usecs(right_local);
    let (left_year, left_month, left_day) = ymd_from_days(left_days);
    let (right_year, right_month, right_day) = ymd_from_days(right_days);

    let mut time = left_time - right_time;
    let mut days = i64::from(left_day) - i64::from(right_day);
    let mut months = (i64::from(left_year) - i64::from(right_year)) * 12 + i64::from(left_month)
        - i64::from(right_month);

    if left_less {
        time = -time;
        days = -days;
        months = -months;
    }
    while time < 0 {
        time += USECS_PER_DAY;
        days -= 1;
    }
    while days < 0 {
        let (borrow_year, borrow_month) = if left_less {
            (left_year, left_month)
        } else {
            (right_year, right_month)
        };
        days += i64::from(crate::backend::utils::time::datetime::days_in_month(
            borrow_year,
            borrow_month,
        ));
        months -= 1;
    }
    if left_less {
        time = -time;
        days = -days;
        months = -months;
    }

    Ok(IntervalValue {
        time_micros: time,
        days: i32::try_from(days).map_err(|_| interval_out_of_range_error())?,
        months: i32::try_from(months).map_err(|_| interval_out_of_range_error())?,
    })
}

fn age_timestamp_interval(
    left: TimestampADT,
    right: TimestampADT,
) -> Result<IntervalValue, ExecError> {
    if let Some(result) = age_infinity_interval(left.0, right.0) {
        return result;
    }
    symbolic_age_interval(left.0, right.0, left.0 < right.0)
}

fn age_timestamptz_interval(
    left: TimestampTzADT,
    right: TimestampTzADT,
    config: &DateTimeConfig,
) -> Result<IntervalValue, ExecError> {
    if let Some(result) = age_infinity_interval(left.0, right.0) {
        return result;
    }
    let left_local = timestamp_tz_local_timestamp(left, config)?;
    let right_local = timestamp_tz_local_timestamp(right, config)?;
    symbolic_age_interval(left_local.0, right_local.0, left.0 < right.0)
}

pub(crate) fn eval_age_function(
    values: &[Value],
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let interval = match values {
        [Value::Timestamp(timestamp)] => {
            let Value::Date(today) = current_date_value_with_config(config) else {
                unreachable!("current_date returns date")
            };
            age_timestamp_interval(TimestampADT(i64::from(today.0) * USECS_PER_DAY), *timestamp)?
        }
        [Value::TimestampTz(timestamp)] => {
            if timestamp.0 == TIMESTAMP_NOEND {
                IntervalValue::neg_infinity()
            } else if timestamp.0 == TIMESTAMP_NOBEGIN {
                IntervalValue::infinity()
            } else {
                age_timestamptz_interval(current_date_timestamp_tz(config)?, *timestamp, config)?
            }
        }
        [Value::Timestamp(left), Value::Timestamp(right)] => age_timestamp_interval(*left, *right)?,
        [Value::TimestampTz(left), Value::TimestampTz(right)] => {
            age_timestamptz_interval(*left, *right, config)?
        }
        _ => {
            return Err(ExecError::TypeMismatch {
                op: "age",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            });
        }
    };
    Ok(Value::Interval(interval))
}

pub(crate) fn add_interval_to_local_timestamp(
    timestamp: TimestampADT,
    interval: IntervalValue,
    subtract: bool,
) -> Result<TimestampADT, ExecError> {
    if !interval.is_finite() {
        return Err(ExecError::DetailedError {
            message: "interval out of range".into(),
            detail: None,
            hint: None,
            sqlstate: "22008",
        });
    }
    let sign = if subtract { -1 } else { 1 };
    let (days, time_usecs) = timestamp_parts_from_usecs(timestamp.0);
    let (year, month, day) = ymd_from_days(days);
    let total_month =
        i64::from(year) * 12 + i64::from(month - 1) + i64::from(interval.months) * i64::from(sign);
    let new_year =
        i32::try_from(total_month.div_euclid(12)).map_err(|_| timestamp_out_of_range_error())?;
    let new_month = total_month.rem_euclid(12) as u32 + 1;
    let max_day = crate::backend::utils::time::datetime::days_in_month(new_year, new_month);
    let new_day = day.min(max_day);
    let mut days =
        days_from_ymd(new_year, new_month, new_day).ok_or_else(timestamp_out_of_range_error)?;
    days = days
        .checked_add(interval.days.saturating_mul(sign))
        .ok_or_else(timestamp_out_of_range_error)?;
    let total_time = i128::from(time_usecs) + i128::from(interval.time_micros) * i128::from(sign);
    let day_adjust = total_time.div_euclid(i128::from(USECS_PER_DAY));
    let time = total_time.rem_euclid(i128::from(USECS_PER_DAY));
    days = days
        .checked_add(i32::try_from(day_adjust).map_err(|_| timestamp_out_of_range_error())?)
        .ok_or_else(timestamp_out_of_range_error)?;
    let usecs = i128::from(days) * i128::from(USECS_PER_DAY) + time;
    i64::try_from(usecs)
        .map(TimestampADT)
        .map_err(|_| timestamp_out_of_range_error())
}

fn numeric_value_as_f64(value: &NumericValue) -> Option<f64> {
    match value {
        NumericValue::PosInf => Some(f64::INFINITY),
        NumericValue::NegInf => Some(f64::NEG_INFINITY),
        NumericValue::NaN => Some(f64::NAN),
        NumericValue::Finite { coeff, scale, .. } => {
            Some(coeff.to_f64()? / 10f64.powi(*scale as i32))
        }
    }
}

fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Int16(v) => Some(f64::from(*v)),
        Value::Int32(v) => Some(f64::from(*v)),
        Value::Int64(v) => Some(*v as f64),
        Value::Float64(v) => Some(*v),
        Value::Numeric(v) => numeric_value_as_f64(v),
        _ => None,
    }
}

fn to_timestamp_parse_error(input: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("invalid input syntax for type timestamp with time zone: \"{input}\""),
        detail: None,
        hint: None,
        sqlstate: "22007",
    }
}

fn to_timestamp_field_out_of_range(input: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("date/time field value out of range: \"{input}\""),
        detail: None,
        hint: None,
        sqlstate: "22008",
    }
}

fn to_timestamp_invalid_value_error(value: &str, field: &str, detail: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("invalid value \"{value}\" for \"{field}\""),
        detail: Some(detail.into()),
        hint: None,
        sqlstate: "22007",
    }
}

fn to_timestamp_invalid_integer_error(value: &str, field: &str) -> ExecError {
    to_timestamp_invalid_value_error(value, field, "Value must be an integer.")
}

fn to_timestamp_source_too_short_error(
    field: &str,
    required: usize,
    remaining: usize,
) -> ExecError {
    ExecError::DetailedError {
        message: format!("source string too short for \"{field}\" formatting field"),
        detail: Some(format!(
            "Field requires {required} characters, but only {remaining} remain."
        )),
        hint: Some(
            "If your source string is not fixed-width, try using the \"FM\" modifier.".into(),
        ),
        sqlstate: "22007",
    }
}

fn to_timestamp_fixed_width_parse_error(
    value: &str,
    field: &str,
    required: usize,
    parsed: usize,
) -> ExecError {
    ExecError::DetailedError {
        message: format!("invalid value \"{value}\" for \"{field}\""),
        detail: Some(format!(
            "Field requires {required} characters, but only {parsed} could be parsed."
        )),
        hint: Some(
            "If your source string is not fixed-width, try using the \"FM\" modifier.".into(),
        ),
        sqlstate: "22007",
    }
}

fn to_timestamp_value_out_of_range_error(field: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("value for \"{field}\" in source string is out of range"),
        detail: Some("Value must be in the range -2147483648 to 2147483647.".into()),
        hint: None,
        sqlstate: "22003",
    }
}

fn to_timestamp_conflicting_field_error(field: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("conflicting values for \"{field}\" field in formatting string"),
        detail: Some("This value contradicts a previous setting for the same field type.".into()),
        hint: None,
        sqlstate: "22007",
    }
}

fn to_timestamp_mixed_date_conventions_error() -> ExecError {
    ExecError::DetailedError {
        message: "invalid combination of date conventions".into(),
        detail: None,
        hint: Some(
            "Do not mix Gregorian and ISO week date conventions in a formatting template.".into(),
        ),
        sqlstate: "22007",
    }
}

fn read_digits(input: &str, pos: &mut usize, min: usize, max: usize) -> Option<i32> {
    let bytes = input.as_bytes();
    let start = *pos;
    let mut end = start;
    while end < bytes.len() && bytes[end].is_ascii_digit() && end - start < max {
        end += 1;
    }
    if end - start < min {
        return None;
    }
    let value = input[start..end].parse().ok()?;
    *pos = end;
    Some(value)
}

fn read_signed_digits(input: &str, pos: &mut usize, min: usize, max: usize) -> Option<i32> {
    let sign = match input.as_bytes().get(*pos) {
        Some(b'-') => {
            *pos += 1;
            -1
        }
        Some(b'+') => {
            *pos += 1;
            1
        }
        _ => 1,
    };
    read_digits(input, pos, min, max).map(|value| sign * value)
}

fn read_int_field(
    input: &str,
    pos: &mut usize,
    field: &str,
    min: usize,
    max: usize,
    fixed_width: Option<usize>,
) -> Result<i32, ExecError> {
    if let Some(width) = fixed_width {
        let remaining = input.len().saturating_sub(*pos);
        if remaining < width {
            return Err(to_timestamp_source_too_short_error(field, width, remaining));
        }
        let end = *pos + width;
        let text = &input[*pos..end];
        let parsed = text
            .as_bytes()
            .iter()
            .take_while(|byte| byte.is_ascii_digit())
            .count();
        if parsed < width {
            return Err(to_timestamp_fixed_width_parse_error(
                text, field, width, parsed,
            ));
        }
        *pos = end;
        return text
            .parse::<i32>()
            .map_err(|_| to_timestamp_value_out_of_range_error(field));
    }

    let start = *pos;
    match read_digits(input, pos, min, max) {
        Some(value) => Ok(value),
        None => {
            let end = input
                .get(start..)
                .and_then(|rest| {
                    rest.char_indices()
                        .nth(max.saturating_sub(1))
                        .map(|(idx, ch)| start + idx + ch.len_utf8())
                })
                .unwrap_or(input.len());
            Err(to_timestamp_invalid_integer_error(
                &input[start..end],
                field,
            ))
        }
    }
}

fn read_signed_int_field(
    input: &str,
    pos: &mut usize,
    field: &str,
    min: usize,
    max: usize,
) -> Result<i32, ExecError> {
    let start = *pos;
    if input
        .as_bytes()
        .get(*pos)
        .is_some_and(|byte| matches!(byte, b'+' | b'-'))
    {
        *pos += 1;
    }
    let digit_start = *pos;
    while *pos < input.len() && input.as_bytes()[*pos].is_ascii_digit() {
        *pos += 1;
    }
    let digit_count = *pos - digit_start;
    if digit_count < min {
        *pos = start;
        let end = input
            .get(start..)
            .and_then(|rest| {
                rest.chars()
                    .next()
                    .filter(|ch| ch.is_ascii_alphabetic())
                    .map(|_| {
                        start
                            + rest
                                .chars()
                                .take_while(|ch| ch.is_ascii_alphabetic())
                                .map(char::len_utf8)
                                .sum::<usize>()
                    })
            })
            .unwrap_or_else(|| (start + max).min(input.len()));
        return Err(to_timestamp_invalid_integer_error(
            &input[start..end],
            field,
        ));
    }
    if digit_count > max {
        return Err(to_timestamp_value_out_of_range_error(field));
    }
    input[start..*pos]
        .parse::<i32>()
        .map_err(|_| to_timestamp_value_out_of_range_error(field))
}

fn read_alpha(input: &str, pos: &mut usize) -> Option<String> {
    read_alpha_with_max(input, pos, usize::MAX)
}

fn read_alpha_with_max(input: &str, pos: &mut usize, max: usize) -> Option<String> {
    let bytes = input.as_bytes();
    let start = *pos;
    let mut end = start;
    while end < bytes.len() && bytes[end].is_ascii_alphabetic() && end - start < max {
        end += 1;
    }
    if end == start {
        return None;
    }
    *pos = end;
    Some(input[start..end].to_string())
}

fn plus_is_after_spaced_plus(input: &str, plus_pos: usize) -> bool {
    if input.as_bytes().get(plus_pos) != Some(&b'+') || plus_pos == 0 {
        return false;
    }
    let mut index = plus_pos;
    let mut saw_whitespace = false;
    while index > 0 && input.as_bytes()[index - 1].is_ascii_whitespace() {
        saw_whitespace = true;
        index -= 1;
    }
    saw_whitespace && index > 0 && input.as_bytes()[index - 1] == b'+'
}

fn read_month_field(
    input: &str,
    pos: &mut usize,
    max_alpha: usize,
    allow_spaced_plus: bool,
) -> Option<String> {
    if matches!(input.as_bytes().get(*pos), Some(b'+')) {
        if !allow_spaced_plus && plus_is_after_spaced_plus(input, *pos) {
            return None;
        }
        *pos += 1;
        while matches!(input.as_bytes().get(*pos), Some(b'+')) {
            *pos += 1;
        }
        skip_ascii_whitespace(input, pos);
    }
    read_alpha_with_max(input, pos, max_alpha)
}

fn invalid_month_value(input: &str, pos: usize) -> String {
    let rest = input.get(pos..).unwrap_or_default();
    if rest.starts_with('+') {
        return "+".into();
    }
    rest.split_whitespace().next().unwrap_or(rest).to_string()
}

fn preceding_format_spaces(format: &str, pos: usize) -> usize {
    let mut count = 0usize;
    let mut index = pos;
    while index > 0 && format.as_bytes()[index - 1].is_ascii_whitespace() {
        count += 1;
        index -= 1;
    }
    count
}

fn read_offset_token(input: &str, pos: &mut usize) -> Option<String> {
    let bytes = input.as_bytes();
    let start = *pos;
    if !matches!(bytes.get(start), Some(b'+') | Some(b'-')) {
        return None;
    }
    let mut end = start + 1;
    while end < bytes.len() && (bytes[end].is_ascii_digit() || bytes[end] == b':') {
        end += 1;
    }
    if end == start + 1 {
        return None;
    }
    *pos = end;
    Some(input[start..end].to_string())
}

fn invalid_offset_value_fragment(input: &str, pos: usize) -> String {
    let rest = input.get(pos..).unwrap_or_default();
    let without_sign = rest
        .strip_prefix('+')
        .or_else(|| rest.strip_prefix('-'))
        .unwrap_or(rest);
    let after_colon = without_sign.rsplit(':').next().unwrap_or(without_sign);
    after_colon.chars().take(2).collect()
}

fn read_timezone_token(input: &str, pos: &mut usize) -> Option<String> {
    if let Some(offset) = read_offset_token(input, pos) {
        return Some(offset);
    }
    let rest = &input[*pos..];
    for abbrev in [
        "EST", "EDT", "CST", "CDT", "MST", "MDT", "PST", "PDT", "MSK", "LMT",
    ] {
        if rest.len() >= abbrev.len() && rest[..abbrev.len()].eq_ignore_ascii_case(abbrev) {
            *pos += abbrev.len();
            return Some(abbrev.to_string());
        }
    }
    read_alpha(input, pos)
}

fn read_meridiem_token(input: &str, pos: &mut usize) -> Option<bool> {
    let rest = &input[*pos..];
    for (token, pm) in [
        ("A.M.", false),
        ("P.M.", true),
        ("AM", false),
        ("PM", true),
        ("A", false),
        ("P", true),
    ] {
        if rest.len() >= token.len() && rest[..token.len()].eq_ignore_ascii_case(token) {
            *pos += token.len();
            return Some(pm);
        }
    }
    None
}

fn read_era_token(input: &str, pos: &mut usize) -> Option<bool> {
    let rest = &input[*pos..];
    for (token, bc) in [
        ("A.D.", false),
        ("B.C.", true),
        ("AD", false),
        ("BC", true),
        ("A", false),
        ("B", true),
    ] {
        if rest.len() >= token.len() && rest[..token.len()].eq_ignore_ascii_case(token) {
            *pos += token.len();
            return Some(bc);
        }
    }
    None
}

fn skip_ascii_whitespace(input: &str, pos: &mut usize) {
    let bytes = input.as_bytes();
    while *pos < bytes.len() && bytes[*pos].is_ascii_whitespace() {
        *pos += 1;
    }
}

fn skip_to_timestamp_ordinal_suffix(input: &str, pos: &mut usize) {
    let rest = &input[*pos..];
    for suffix in ["st", "nd", "rd", "th", "ST", "ND", "RD", "TH"] {
        if rest.starts_with(suffix) {
            *pos += suffix.len();
            break;
        }
    }
}

fn escaped_quote_delimiter_len(format: &str, pos: usize) -> Option<usize> {
    let bytes = format.as_bytes();
    let mut index = pos;
    while bytes.get(index) == Some(&b'\\') {
        index += 1;
    }
    (index > pos && bytes.get(index) == Some(&b'"')).then_some(index - pos + 1)
}

fn find_format_quote_end(format: &str, start: usize) -> Option<usize> {
    let bytes = format.as_bytes();
    let mut index = start;
    while index < bytes.len() {
        if bytes[index] == b'"' && (index == 0 || bytes[index - 1] != b'\\') {
            return Some(index);
        }
        index += 1;
    }
    None
}

fn unescape_format_quoted_literal(literal: &str) -> String {
    let mut out = String::with_capacity(literal.len());
    let mut chars = literal.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\'
            && let Some('"') = chars.clone().next()
        {
            chars.next();
            out.push('"');
        } else {
            out.push(ch);
        }
    }
    out
}

fn consume_to_timestamp_literal(input: &str, pos: &mut usize, literal: &str, exact: bool) -> bool {
    if literal.is_empty() {
        return true;
    }
    if input
        .get(*pos..)
        .is_some_and(|rest| rest.starts_with(literal))
    {
        *pos += literal.len();
        return true;
    }
    if exact {
        let Some(ch) = input[*pos..].chars().next() else {
            return false;
        };
        *pos += ch.len_utf8();
        return true;
    }
    for _ in literal.chars() {
        let Some(ch) = input[*pos..].chars().next() else {
            return true;
        };
        *pos += ch.len_utf8();
    }
    true
}

fn skip_template_separator(input: &str, pos: &mut usize, exact: bool, expected: char) -> bool {
    if exact {
        let Some(ch) = input[*pos..].chars().next() else {
            return false;
        };
        *pos += ch.len_utf8();
        return true;
    }
    if expected.is_whitespace() {
        if input
            .as_bytes()
            .get(*pos)
            .is_some_and(|byte| byte.is_ascii_whitespace() || !byte.is_ascii_alphanumeric())
        {
            *pos += 1;
        }
        return true;
    }
    if expected.is_ascii_alphanumeric() {
        let Some(ch) = input[*pos..].chars().next() else {
            return true;
        };
        *pos += ch.len_utf8();
        return true;
    }
    if input
        .as_bytes()
        .get(*pos)
        .is_some_and(|byte| !byte.is_ascii_alphanumeric())
    {
        *pos += 1;
    }
    true
}

fn next_template_part_is_adjacent_numeric(format: &str, pos: usize) -> bool {
    let rest = &format[pos..];
    [
        "YYYY", "YYY", "YY", "Y", "IYYY", "IYY", "IY", "I", "MM", "DD", "DDD", "HH24", "HH12",
        "HH", "MI", "SS", "MS", "FF", "TZH", "TZM", "WW", "IW", "IDDD", "ID", "W", "D", "CC",
    ]
    .iter()
    .any(|token| rest.starts_with(token))
}

fn timestamp_template_action_len(format: &str) -> Option<usize> {
    [
        "SSSSS", "MONTH", "HH24", "HH12", "IDDD", "IYYY", "Y,YYY", "YYYY", "A.M.", "P.M.", "B.C.",
        "A.D.", "SSSS", "YYY", "MON", "DDD", "TZH", "TZM", "DAY", "IYY", "HH", "MI", "SS", "MS",
        "FF", "TZ", "OF", "AM", "PM", "BC", "AD", "RM", "IW", "ID", "IY", "YY", "MM", "DD", "DY",
        "CC", "WW", "Q", "W", "D", "Y", "I", "J",
    ]
    .iter()
    .find_map(|token| format.starts_with(token).then_some(token.len()))
}

fn roman_month_number(value: &str) -> Option<u32> {
    match value.to_ascii_uppercase().as_str() {
        "I" => Some(1),
        "II" => Some(2),
        "III" => Some(3),
        "IV" => Some(4),
        "V" => Some(5),
        "VI" => Some(6),
        "VII" => Some(7),
        "VIII" => Some(8),
        "IX" => Some(9),
        "X" => Some(10),
        "XI" => Some(11),
        "XII" => Some(12),
        _ => None,
    }
}

#[derive(Default)]
struct ToTimestampFields {
    year: Option<i32>,
    year_digits: Option<usize>,
    century: Option<i32>,
    iso_year: Option<i32>,
    month: Option<u32>,
    day: Option<u32>,
    ordinal_day: Option<u32>,
    week: Option<u32>,
    week_of_month: Option<u32>,
    iso_week: Option<u32>,
    day_of_week: Option<u32>,
    iso_day_of_week: Option<u32>,
    julian_day: Option<i32>,
    hour: Option<u32>,
    minute: Option<u32>,
    second: Option<u32>,
    micros: i64,
    seconds_of_day: Option<u32>,
    pm: Option<bool>,
    bc: bool,
    tz_hour: Option<i32>,
    tz_minute: Option<i32>,
    timezone: Option<String>,
    has_gregorian: bool,
    has_iso: bool,
    hour_12: bool,
}

impl ToTimestampFields {
    fn set_month(&mut self, value: u32, field: &str) -> Result<(), ExecError> {
        if self.month.is_some_and(|existing| existing != value) {
            return Err(to_timestamp_conflicting_field_error(field));
        }
        self.month = Some(value);
        Ok(())
    }

    fn mark_gregorian(&mut self) {
        self.has_gregorian = true;
    }

    fn mark_iso(&mut self) {
        self.has_iso = true;
    }
}

fn parse_to_timestamp_text_format(
    input: &str,
    format: &str,
    config: &DateTimeConfig,
) -> Result<TimestampTzADT, ExecError> {
    let upper = format.to_ascii_uppercase();
    let mut fmt_pos = 0usize;
    let mut input_pos = 0usize;
    let mut fields = ToTimestampFields::default();
    let mut fill_mode = false;
    let mut exact = false;

    while fmt_pos < upper.len() {
        let rest = &upper[fmt_pos..];
        if rest.starts_with("FM") {
            fill_mode = true;
            fmt_pos += 2;
            continue;
        }
        if rest.starts_with("FX") {
            exact = true;
            fmt_pos += 2;
            continue;
        }
        if let Some(open_len) = escaped_quote_delimiter_len(format, fmt_pos) {
            let literal_start = fmt_pos + open_len;
            let mut close_start = None;
            let mut close_len = 0usize;
            let mut search = literal_start;
            while search < format.len() {
                if let Some(len) = escaped_quote_delimiter_len(format, search) {
                    close_start = Some(search);
                    close_len = len;
                    break;
                }
                search += 1;
            }
            let Some(close_start) = close_start else {
                return Err(to_timestamp_parse_error(input));
            };
            let literal = &format[literal_start..close_start];
            let mut quoted = String::from("\"");
            quoted.push_str(literal);
            quoted.push('"');
            if input
                .get(input_pos..)
                .is_some_and(|rest| rest.starts_with('"'))
            {
                if !consume_to_timestamp_literal(input, &mut input_pos, &quoted, exact) {
                    return Err(to_timestamp_parse_error(input));
                }
            } else if !consume_to_timestamp_literal(input, &mut input_pos, literal, exact) {
                return Err(to_timestamp_parse_error(input));
            }
            fmt_pos = close_start + close_len;
            continue;
        }
        if format[fmt_pos..].starts_with("\"\"")
            && let Some(close_rel) = format[fmt_pos + 2..].find("\"\"")
        {
            let close_start = fmt_pos + 2 + close_rel;
            let mut literal = String::from("\"");
            literal.push_str(&format[fmt_pos + 2..close_start]);
            literal.push('"');
            if !consume_to_timestamp_literal(input, &mut input_pos, &literal, exact) {
                return Err(to_timestamp_parse_error(input));
            }
            fmt_pos = close_start + 2;
            continue;
        }
        if format.as_bytes().get(fmt_pos) == Some(&b'"') {
            let Some(end) = find_format_quote_end(format, fmt_pos + 1) else {
                return Err(to_timestamp_parse_error(input));
            };
            let literal = unescape_format_quoted_literal(&format[fmt_pos + 1..end]);
            if input
                .get(input_pos..)
                .is_some_and(|rest| rest.starts_with('"'))
                && !(literal.starts_with('"') && literal.ends_with('"'))
            {
                let mut quoted = String::from("\"");
                quoted.push_str(&literal);
                quoted.push('"');
                if !consume_to_timestamp_literal(input, &mut input_pos, &quoted, exact) {
                    return Err(to_timestamp_parse_error(input));
                }
            } else if !consume_to_timestamp_literal(input, &mut input_pos, &literal, exact) {
                return Err(to_timestamp_parse_error(input));
            }
            fmt_pos = end + 1;
            continue;
        }
        if format[fmt_pos..].starts_with("\\\"")
            && let Some(close_rel) = format[fmt_pos + 2..].find("\\\"")
        {
            let close_start = fmt_pos + 2 + close_rel;
            let mut literal = format[fmt_pos + 1..close_start].to_string();
            literal.push('"');
            if !consume_to_timestamp_literal(input, &mut input_pos, &literal, exact) {
                return Err(to_timestamp_parse_error(input));
            }
            fmt_pos = close_start + 2;
            continue;
        }
        if format.as_bytes().get(fmt_pos) == Some(&b'\\') {
            let literal_start = fmt_pos + 1;
            if literal_start >= format.len() {
                return Err(to_timestamp_parse_error(input));
            }
            let literal = &format[literal_start..literal_start + 1];
            if !consume_to_timestamp_literal(input, &mut input_pos, literal, exact) {
                return Err(to_timestamp_parse_error(input));
            }
            fmt_pos += 2;
            continue;
        }

        if !exact && timestamp_template_action_len(rest).is_some() {
            skip_ascii_whitespace(input, &mut input_pos);
        }
        let mut consumed_token = true;
        if rest.starts_with("Y,YYY") {
            if input[input_pos..]
                .chars()
                .take_while(|ch| ch.is_ascii_digit())
                .count()
                > 1
            {
                return Err(ExecError::DetailedError {
                    message: "value for \"Y,YYY\" in source string is out of range".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "22003",
                });
            }
            let hi = read_digits(input, &mut input_pos, 1, 1)
                .ok_or_else(|| to_timestamp_parse_error(input))?;
            consume_to_timestamp_literal(input, &mut input_pos, ",", exact);
            let lo = read_digits(input, &mut input_pos, 3, 3)
                .ok_or_else(|| to_timestamp_parse_error(input))?;
            fields.year = Some(hi * 1000 + lo);
            fields.year_digits = Some(4);
            fields.mark_gregorian();
            fmt_pos += 5;
        } else if rest.starts_with("IYYY") {
            let max = if fill_mode || !next_template_part_is_adjacent_numeric(&upper, fmt_pos + 4) {
                9
            } else {
                4
            };
            fields.iso_year = Some(
                read_signed_digits(input, &mut input_pos, 1, max)
                    .ok_or_else(|| to_timestamp_parse_error(input))?,
            );
            fields.mark_iso();
            fmt_pos += 4;
        } else if rest.starts_with("YYYY") {
            let adjacent = next_template_part_is_adjacent_numeric(&upper, fmt_pos + 4);
            let max = if fill_mode || !adjacent { 10 } else { 4 };
            fields.year = Some(if adjacent && !fill_mode {
                read_signed_digits(input, &mut input_pos, 4, 4)
                    .ok_or_else(|| to_timestamp_parse_error(input))?
            } else {
                read_signed_int_field(input, &mut input_pos, "YYYY", 1, max)?
            });
            fields.year_digits = Some(4);
            fields.mark_gregorian();
            fmt_pos += 4;
        } else if rest.starts_with("YYY") {
            let value = read_int_field(input, &mut input_pos, "YYY", 3, 3, Some(3))?;
            fields.year = Some(if value >= 100 { 1000 + value } else { value });
            fields.year_digits = Some(3);
            fields.mark_gregorian();
            fmt_pos += 3;
        } else if rest.starts_with("IYY") {
            fields.iso_year =
                Some(2000 + read_int_field(input, &mut input_pos, "IYY", 3, 3, Some(3))?);
            fields.mark_iso();
            fmt_pos += 3;
        } else if rest.starts_with("YY") {
            fields.year = Some(read_int_field(input, &mut input_pos, "YY", 2, 2, Some(2))?);
            fields.year_digits = Some(2);
            fields.mark_gregorian();
            fmt_pos += 2;
        } else if rest.starts_with('Y') {
            fields.year = Some(read_int_field(input, &mut input_pos, "Y", 1, 1, Some(1))?);
            fields.year_digits = Some(1);
            fields.mark_gregorian();
            fmt_pos += 1;
        } else if rest.starts_with("IY") {
            fields.iso_year = Some(expand_two_digit_year(read_int_field(
                input,
                &mut input_pos,
                "IY",
                2,
                2,
                Some(2),
            )?));
            fields.mark_iso();
            fmt_pos += 2;
        } else if rest.starts_with("MONTH") {
            let field = &format[fmt_pos..fmt_pos + 5];
            let allow_spaced_plus = preceding_format_spaces(format, fmt_pos) >= 3;
            let word = read_month_field(input, &mut input_pos, usize::MAX, allow_spaced_plus)
                .ok_or_else(|| {
                    to_timestamp_invalid_value_error(
                        &invalid_month_value(input, input_pos),
                        field,
                        "The given value did not match any of the allowed values for this field.",
                    )
                })?;
            let month = month_number(&word).ok_or_else(|| {
                to_timestamp_invalid_value_error(
                    &word,
                    field,
                    "The given value did not match any of the allowed values for this field.",
                )
            })?;
            fields.set_month(month, field)?;
            fields.mark_gregorian();
            fmt_pos += 5;
        } else if rest.starts_with("MON") {
            let field = &format[fmt_pos..fmt_pos + 3];
            let allow_spaced_plus = preceding_format_spaces(format, fmt_pos) >= 3;
            let word =
                read_month_field(input, &mut input_pos, 3, allow_spaced_plus).ok_or_else(|| {
                    to_timestamp_invalid_value_error(
                        &invalid_month_value(input, input_pos),
                        field,
                        "The given value did not match any of the allowed values for this field.",
                    )
                })?;
            let month = month_number(&word).ok_or_else(|| {
                to_timestamp_invalid_value_error(
                    &word,
                    field,
                    "The given value did not match any of the allowed values for this field.",
                )
            })?;
            fields.set_month(month, field)?;
            fields.mark_gregorian();
            fmt_pos += 3;
        } else if rest.starts_with("MM") {
            let fixed = (!fill_mode && next_template_part_is_adjacent_numeric(&upper, fmt_pos + 2))
                .then_some(2);
            let month = read_int_field(input, &mut input_pos, "MM", 1, 2, fixed)? as u32;
            fields.set_month(month, "MM")?;
            fields.mark_gregorian();
            fmt_pos += 2;
        } else if rest.starts_with("DDD") {
            fields.ordinal_day = Some(read_int_field(
                input,
                &mut input_pos,
                "DDD",
                if fill_mode { 1 } else { 3 },
                3,
                (!fill_mode).then_some(3),
            )? as u32);
            fields.mark_gregorian();
            fmt_pos += 3;
        } else if rest.starts_with("DD") {
            fields.day = Some(read_int_field(input, &mut input_pos, "DD", 1, 2, None)? as u32);
            fields.mark_gregorian();
            fmt_pos += 2;
        } else if rest.starts_with("HH24") {
            fields.hour = Some(read_int_field(input, &mut input_pos, "HH24", 1, 2, None)? as u32);
            fmt_pos += 4;
        } else if rest.starts_with("HH12") {
            fields.hour = Some(read_int_field(input, &mut input_pos, "HH12", 1, 2, None)? as u32);
            fields.hour_12 = true;
            fmt_pos += 4;
        } else if rest.starts_with("HH") {
            fields.hour = Some(read_int_field(input, &mut input_pos, "HH", 1, 2, None)? as u32);
            fields.hour_12 = true;
            fmt_pos += 2;
        } else if rest.starts_with("MI") {
            fields.minute = Some(read_int_field(input, &mut input_pos, "MI", 1, 2, None)? as u32);
            fmt_pos += 2;
        } else if rest.starts_with("SSSSS") || rest.starts_with("SSSS") {
            fields.seconds_of_day =
                Some(read_int_field(input, &mut input_pos, "SSSS", 1, 5, None)? as u32);
            fmt_pos += if rest.starts_with("SSSSS") { 5 } else { 4 };
        } else if rest.starts_with("SS") {
            fields.second = Some(read_int_field(input, &mut input_pos, "SS", 1, 2, None)? as u32);
            fmt_pos += 2;
        } else if rest.starts_with("MS") {
            let start = input_pos;
            if input
                .as_bytes()
                .get(input_pos)
                .is_some_and(|byte| *byte == b'-')
            {
                return Err(to_timestamp_field_out_of_range(input));
            }
            let value = read_int_field(input, &mut input_pos, "MS", 1, 3, None)?;
            let digits = input_pos - start;
            fields.micros = i64::from(value) * 10_i64.pow(3 - digits as u32) * 1000;
            fmt_pos += 2;
        } else if rest.starts_with("FF") {
            let precision = rest
                .as_bytes()
                .get(2)
                .and_then(|byte| byte.is_ascii_digit().then_some((byte - b'0') as usize));
            let width = precision.unwrap_or(6).clamp(1, 6);
            let start = input_pos;
            while input_pos < input.len()
                && input.as_bytes()[input_pos].is_ascii_digit()
                && input_pos - start < 9
            {
                input_pos += 1;
            }
            let digits = &input[start..input_pos];
            if digits.len() > 6 {
                return Err(to_timestamp_field_out_of_range(input));
            }
            let padded = format!("{digits:0<6}");
            let mut micros = padded[..6]
                .parse::<i64>()
                .map_err(|_| to_timestamp_invalid_integer_error(digits, "FF"))?;
            let unit = 10_i64.pow((6 - width) as u32);
            if width < 6
                && digits
                    .as_bytes()
                    .get(width)
                    .is_some_and(|byte| *byte >= b'5')
            {
                micros += unit;
            }
            fields.micros = micros / unit * unit;
            fmt_pos += 2 + precision.is_some() as usize;
        } else if rest.starts_with("TZH") {
            fields.tz_hour = Some(read_signed_int_field(input, &mut input_pos, "TZH", 1, 2)?);
            fmt_pos += 3;
        } else if rest.starts_with("TZM") {
            fields.tz_minute = Some(read_int_field(input, &mut input_pos, "TZM", 1, 2, None)?);
            fmt_pos += 3;
        } else if rest.starts_with("TZ") {
            fields.timezone =
                Some(read_timezone_token(input, &mut input_pos).ok_or_else(|| {
                    let value = input[input_pos..].chars().take(2).collect::<String>();
                    to_timestamp_invalid_integer_error(&value, "TZ")
                })?);
            fmt_pos += 2;
        } else if rest.starts_with("OF") {
            let offset_start = input_pos;
            let offset = read_offset_token(input, &mut input_pos).ok_or_else(|| {
                let value = invalid_offset_value_fragment(input, offset_start);
                to_timestamp_invalid_integer_error(&value, "OF")
            })?;
            if offset.ends_with(':') || parse_offset_seconds(&offset).is_none() {
                let value = invalid_offset_value_fragment(input, offset_start);
                return Err(to_timestamp_invalid_integer_error(&value, "OF"));
            }
            fields.timezone = Some(offset);
            fmt_pos += 2;
        } else if rest.starts_with("A.M.") || rest.starts_with("P.M.") {
            fields.pm = Some(
                read_meridiem_token(input, &mut input_pos)
                    .ok_or_else(|| to_timestamp_parse_error(input))?,
            );
            fmt_pos += 4;
        } else if rest.starts_with("AM") || rest.starts_with("PM") {
            fields.pm = Some(
                read_meridiem_token(input, &mut input_pos)
                    .ok_or_else(|| to_timestamp_parse_error(input))?,
            );
            fmt_pos += 2;
        } else if rest.starts_with("B.C.") || rest.starts_with("A.D.") {
            fields.bc = read_era_token(input, &mut input_pos)
                .ok_or_else(|| to_timestamp_parse_error(input))?;
            fmt_pos += 4;
        } else if rest.starts_with("BC") || rest.starts_with("AD") {
            fields.bc = read_era_token(input, &mut input_pos)
                .ok_or_else(|| to_timestamp_parse_error(input))?;
            fmt_pos += 2;
        } else if rest.starts_with("RM") {
            let word = read_month_field(input, &mut input_pos, usize::MAX, false)
                .ok_or_else(|| to_timestamp_parse_error(input))?;
            let month = roman_month_number(&word).ok_or_else(|| to_timestamp_parse_error(input))?;
            fields.set_month(month, "RM")?;
            fields.mark_gregorian();
            fmt_pos += 2;
        } else if rest.starts_with("IW") {
            fields.iso_week = Some(read_int_field(input, &mut input_pos, "IW", 1, 2, None)? as u32);
            fields.mark_iso();
            fmt_pos += 2;
        } else if rest.starts_with("IDDD") {
            fields.ordinal_day =
                Some(read_int_field(input, &mut input_pos, "IDDD", 1, 3, None)? as u32);
            fields.mark_iso();
            fmt_pos += 4;
        } else if rest.starts_with("ID") {
            fields.iso_day_of_week =
                Some(read_int_field(input, &mut input_pos, "ID", 1, 1, Some(1))? as u32);
            fields.mark_iso();
            fmt_pos += 2;
        } else if rest.starts_with("I") {
            fields.iso_year =
                Some(2000 + read_int_field(input, &mut input_pos, "I", 1, 1, Some(1))?);
            fields.mark_iso();
            fmt_pos += 1;
        } else if rest.starts_with("WW") {
            let fixed = (!fill_mode && next_template_part_is_adjacent_numeric(&upper, fmt_pos + 2))
                .then_some(2);
            fields.week = Some(read_int_field(input, &mut input_pos, "WW", 1, 10, fixed)? as u32);
            fields.mark_gregorian();
            fmt_pos += 2;
        } else if rest.starts_with('W') {
            let fixed = (!fill_mode && next_template_part_is_adjacent_numeric(&upper, fmt_pos + 1))
                .then_some(1);
            fields.week_of_month =
                Some(read_int_field(input, &mut input_pos, "W", 1, 10, fixed)? as u32);
            fields.mark_gregorian();
            fmt_pos += 1;
        } else if rest.starts_with("CC") {
            fields.century = Some(read_signed_int_field(input, &mut input_pos, "CC", 1, 10)?);
            fields.mark_gregorian();
            fmt_pos += 2;
        } else if rest.starts_with('J') {
            fields.julian_day = Some(read_signed_int_field(input, &mut input_pos, "J", 1, 10)?);
            fmt_pos += 1;
        } else if rest.starts_with('Q') {
            let _ = read_int_field(input, &mut input_pos, "Q", 1, 1, Some(1))?;
            fields.mark_gregorian();
            fmt_pos += 1;
        } else if rest.starts_with("DAY") || rest.starts_with("DY") {
            let max = if rest.starts_with("DAY") {
                usize::MAX
            } else {
                3
            };
            let _ = read_alpha_with_max(input, &mut input_pos, max)
                .ok_or_else(|| to_timestamp_parse_error(input))?;
            fmt_pos += if rest.starts_with("DAY") { 3 } else { 2 };
        } else if rest.starts_with("TH") {
            skip_to_timestamp_ordinal_suffix(input, &mut input_pos);
            fmt_pos += 2;
        } else if rest.starts_with('D') {
            fields.day_of_week =
                Some(read_int_field(input, &mut input_pos, "D", 1, 1, Some(1))? as u32);
            fields.mark_gregorian();
            fmt_pos += 1;
        } else {
            consumed_token = false;
        }
        if consumed_token {
            fill_mode = false;
            continue;
        }

        let ch = format[fmt_pos..]
            .chars()
            .next()
            .ok_or_else(|| to_timestamp_parse_error(input))?;
        if !skip_template_separator(input, &mut input_pos, exact, ch) {
            return Err(to_timestamp_parse_error(input));
        }
        fmt_pos += ch.len_utf8();
        fill_mode = false;
    }
    if input[input_pos..]
        .chars()
        .any(|ch| !ch.is_ascii_whitespace())
    {
        return Err(to_timestamp_parse_error(input));
    }
    if fields.has_gregorian && fields.has_iso {
        return Err(to_timestamp_mixed_date_conventions_error());
    }

    if let Some(julian_day) = fields.julian_day {
        let days = julian_day
            .checked_sub(crate::include::nodes::datetime::POSTGRES_EPOCH_JDATE)
            .ok_or_else(|| to_timestamp_field_out_of_range(input))?;
        let (resolved_year, resolved_month, resolved_day) = ymd_from_days(days);
        fields.year = Some(resolved_year);
        fields.year_digits = None;
        fields.month = Some(resolved_month);
        fields.day = Some(resolved_day);
    }

    let resolved_field_year = fields.year.map(|year| {
        if fields.century.is_none() {
            match fields.year_digits {
                Some(1) => 2000 + year,
                Some(2) => expand_two_digit_year(year),
                _ => year,
            }
        } else {
            year
        }
    });
    let mut year = match (fields.century, fields.year) {
        (Some(century), Some(year)) if fields.year_digits.is_some_and(|digits| digits <= 2) => {
            if year == 0 {
                century
                    .checked_mul(100)
                    .and_then(|value| value.checked_add(if century >= 0 { 0 } else { 1 }))
                    .ok_or_else(|| to_timestamp_field_out_of_range(input))?
            } else if century >= 0 {
                century
                    .checked_sub(1)
                    .and_then(|value| value.checked_mul(100))
                    .and_then(|value| value.checked_add(year.rem_euclid(100)))
                    .ok_or_else(|| to_timestamp_field_out_of_range(input))?
            } else {
                century
                    .checked_add(1)
                    .and_then(|value| value.checked_mul(100))
                    .and_then(|value| value.checked_sub(year.rem_euclid(100)))
                    .and_then(|value| value.checked_add(1))
                    .ok_or_else(|| to_timestamp_field_out_of_range(input))?
            }
        }
        (_, Some(_)) => resolved_field_year.expect("guarded by match arm"),
        (Some(century), None) => {
            if century >= 0 {
                century
                    .checked_sub(1)
                    .and_then(|value| value.checked_mul(100))
                    .and_then(|value| value.checked_add(1))
                    .ok_or_else(|| to_timestamp_field_out_of_range(input))?
            } else {
                century
                    .checked_mul(100)
                    .and_then(|value| value.checked_add(1))
                    .ok_or_else(|| to_timestamp_field_out_of_range(input))?
            }
        }
        (None, None) => fields.iso_year.unwrap_or(2000),
    };
    if fields.bc {
        year = -year;
    }
    if year == 0 {
        year = -1;
    }
    let mut month = fields.month.unwrap_or(1);
    let mut day = fields.day.unwrap_or(1);
    if let (Some(iso_year), Some(ordinal)) = (fields.iso_year, fields.ordinal_day) {
        let date = chrono::NaiveDate::from_isoywd_opt(iso_year, 1, chrono::Weekday::Mon)
            .and_then(|first| first.checked_add_signed(chrono::Duration::days(ordinal as i64 - 1)))
            .ok_or_else(|| to_timestamp_field_out_of_range(input))?;
        year = date.year();
        month = date.month();
        day = date.day();
    } else if let Some(ordinal) = fields.ordinal_day {
        let year_start =
            days_from_ymd(year, 1, 1).ok_or_else(|| to_timestamp_field_out_of_range(input))?;
        let next_year = year
            .checked_add(1)
            .ok_or_else(|| to_timestamp_field_out_of_range(input))?;
        let next_year_start =
            days_from_ymd(next_year, 1, 1).ok_or_else(|| to_timestamp_field_out_of_range(input))?;
        if ordinal == 0 || ordinal as i32 > next_year_start - year_start {
            return Err(to_timestamp_field_out_of_range(input));
        }
        let days = days_from_ymd(year, 1, 1)
            .and_then(|first| first.checked_add(ordinal as i32 - 1))
            .ok_or_else(|| to_timestamp_field_out_of_range(input))?;
        let (resolved_year, resolved_month, resolved_day) = ymd_from_days(days);
        year = resolved_year;
        month = resolved_month;
        day = resolved_day;
    } else if let (Some(iso_year), Some(iso_week)) = (fields.iso_year, fields.iso_week) {
        let weekday = match fields.iso_day_of_week.unwrap_or(1) {
            1 => chrono::Weekday::Mon,
            2 => chrono::Weekday::Tue,
            3 => chrono::Weekday::Wed,
            4 => chrono::Weekday::Thu,
            5 => chrono::Weekday::Fri,
            6 => chrono::Weekday::Sat,
            7 => chrono::Weekday::Sun,
            _ => return Err(to_timestamp_field_out_of_range(input)),
        };
        let date = chrono::NaiveDate::from_isoywd_opt(iso_year, iso_week, weekday)
            .ok_or_else(|| to_timestamp_field_out_of_range(input))?;
        year = date.year();
        month = date.month();
        day = date.day();
    } else if let Some(week) = fields.week {
        let offset = week
            .checked_sub(1)
            .and_then(|week| week.checked_mul(7))
            .ok_or_else(|| to_timestamp_field_out_of_range(input))?;
        let offset = i32::try_from(offset).map_err(|_| to_timestamp_field_out_of_range(input))?;
        let days = days_from_ymd(year, 1, 1)
            .and_then(|first| first.checked_add(offset))
            .ok_or_else(|| to_timestamp_field_out_of_range(input))?;
        let (resolved_year, resolved_month, resolved_day) = ymd_from_days(days);
        year = resolved_year;
        month = resolved_month;
        day = resolved_day;
    } else if let Some(week) = fields.week_of_month {
        let offset = week
            .checked_sub(1)
            .and_then(|week| week.checked_mul(7))
            .ok_or_else(|| to_timestamp_field_out_of_range(input))?;
        let offset = i32::try_from(offset).map_err(|_| to_timestamp_field_out_of_range(input))?;
        let days = days_from_ymd(year, month, 1)
            .and_then(|first| first.checked_add(offset))
            .ok_or_else(|| to_timestamp_field_out_of_range(input))?;
        let (resolved_year, resolved_month, resolved_day) = ymd_from_days(days);
        year = resolved_year;
        month = resolved_month;
        day = resolved_day;
    }

    let (mut hour, minute, second) = if let Some(seconds) = fields.seconds_of_day {
        if seconds >= 86_400 {
            return Err(to_timestamp_field_out_of_range(input));
        }
        (seconds / 3600, (seconds % 3600) / 60, seconds % 60)
    } else {
        (
            fields.hour.unwrap_or(0),
            fields.minute.unwrap_or(0),
            fields.second.unwrap_or(0),
        )
    };
    if fields.hour_12 && !(1..=12).contains(&hour) {
        return Err(ExecError::DetailedError {
            message: format!("hour \"{hour}\" is invalid for the 12-hour clock"),
            detail: None,
            hint: Some("Use the 24-hour clock, or give an hour between 1 and 12.".into()),
            sqlstate: "22007",
        });
    }
    if let Some(pm) = fields.pm {
        if !(1..=12).contains(&hour) {
            return Err(ExecError::DetailedError {
                message: format!("hour \"{hour}\" is invalid for the 12-hour clock"),
                detail: None,
                hint: Some("Use the 24-hour clock, or give an hour between 1 and 12.".into()),
                sqlstate: "22007",
            });
        }
        if pm && hour != 12 {
            hour += 12;
        } else if !pm && hour == 12 {
            hour = 0;
        }
    }
    if minute >= 60 || second >= 60 || fields.micros >= USECS_PER_SEC {
        return Err(to_timestamp_field_out_of_range(input));
    }

    let zone = fields.timezone.unwrap_or_else(|| {
        if let Some(hour) = fields.tz_hour {
            let minute = fields.tz_minute.unwrap_or(0).abs();
            let sign = if hour < 0 { -1 } else { 1 };
            format_offset(sign * (hour.abs() * 3600 + minute * 60))
        } else if let Some(minute) = fields.tz_minute {
            format_offset(minute * 60)
        } else {
            current_timezone_name(config).to_string()
        }
    });
    make_timestamptz_from_parts(
        year,
        month,
        day,
        hour,
        minute,
        second as f64 + fields.micros as f64 / USECS_PER_SEC as f64,
        &zone,
        config,
    )
    .map_err(|err| match err {
        DateTimeParseError::UnknownTimeZone(zone) => to_timestamp_invalid_value_error(
            &zone,
            "TZ",
            "Time zone abbreviation is not recognized.",
        ),
        _ => to_timestamp_field_out_of_range(input),
    })
}

pub(crate) fn eval_to_timestamp_function(
    values: &[Value],
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    if let [input_value, format_value] = values {
        if matches!(input_value, Value::Null) || matches!(format_value, Value::Null) {
            return Ok(Value::Null);
        }
        let input = input_value
            .as_text()
            .ok_or_else(|| ExecError::TypeMismatch {
                op: "to_timestamp",
                left: input_value.clone(),
                right: Value::Text("".into()),
            })?;
        let format = format_value
            .as_text()
            .ok_or_else(|| ExecError::TypeMismatch {
                op: "to_timestamp",
                left: format_value.clone(),
                right: Value::Text("".into()),
            })?;
        return parse_to_timestamp_text_format(input, format, config).map(Value::TimestampTz);
    }
    let [value] = values else {
        return Err(ExecError::DetailedError {
            message: "malformed to_timestamp call".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        });
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let seconds = value_as_f64(value).ok_or_else(|| ExecError::TypeMismatch {
        op: "to_timestamp",
        left: value.clone(),
        right: Value::Float64(0.0),
    })?;
    if seconds.is_nan() {
        return Err(ExecError::DetailedError {
            message: "timestamp cannot be NaN".into(),
            detail: None,
            hint: None,
            sqlstate: "22008",
        });
    }
    if seconds == f64::INFINITY {
        return Ok(Value::TimestampTz(TimestampTzADT(TIMESTAMP_NOEND)));
    }
    if seconds == f64::NEG_INFINITY {
        return Ok(Value::TimestampTz(TimestampTzADT(TIMESTAMP_NOBEGIN)));
    }
    let unix_epoch_usecs =
        i64::from(days_from_ymd(1970, 1, 1).expect("valid unix epoch")) * USECS_PER_DAY;
    let usecs = (seconds * USECS_PER_SEC as f64).round();
    if !(i64::MIN as f64..=i64::MAX as f64).contains(&usecs) {
        return Err(ExecError::DetailedError {
            message: "timestamp out of range".into(),
            detail: None,
            hint: None,
            sqlstate: "22008",
        });
    }
    unix_epoch_usecs
        .checked_add(usecs as i64)
        .map(|usecs| Value::TimestampTz(TimestampTzADT(usecs)))
        .ok_or_else(|| ExecError::DetailedError {
            message: "timestamp out of range".into(),
            detail: None,
            hint: None,
            sqlstate: "22008",
        })
}

pub(crate) fn eval_make_date_function(values: &[Value]) -> Result<Value, ExecError> {
    let [year_value, month_value, day_value] = values else {
        return Err(ExecError::DetailedError {
            message: "malformed make_date call".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        });
    };
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let (year, month, day) = match (year_value, month_value, day_value) {
        (Value::Int32(year), Value::Int32(month), Value::Int32(day)) => (*year, *month, *day),
        _ => {
            return Err(ExecError::TypeMismatch {
                op: "make_date",
                left: year_value.clone(),
                right: month_value.clone(),
            });
        }
    };
    if year == 0 {
        return Err(invalid_make_date(year, month, day));
    }
    let astronomical_year = if year < 0 { year + 1 } else { year };
    let month_u32 = u32::try_from(month).map_err(|_| invalid_make_date(year, month, day))?;
    let day_u32 = u32::try_from(day).map_err(|_| invalid_make_date(year, month, day))?;
    let days = days_from_ymd(astronomical_year, month_u32, day_u32)
        .ok_or_else(|| invalid_make_date(year, month, day))?;
    Ok(Value::Date(crate::include::nodes::datetime::DateADT(days)))
}

fn invalid_make_time(hour: i32, minute: i32, second: f64) -> ExecError {
    ExecError::DetailedError {
        message: format!("time field value out of range: {hour:02}:{minute:02}:{second}"),
        detail: None,
        hint: None,
        sqlstate: "22008",
    }
}

pub(crate) fn eval_make_time_function(values: &[Value]) -> Result<Value, ExecError> {
    let [hour_value, minute_value, second_value] = values else {
        return Err(ExecError::DetailedError {
            message: "malformed make_time call".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        });
    };
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let (hour, minute, second) = match (hour_value, minute_value, second_value) {
        (Value::Int32(hour), Value::Int32(minute), Value::Float64(second)) => {
            (*hour, *minute, *second)
        }
        _ => {
            return Err(ExecError::TypeMismatch {
                op: "make_time",
                left: hour_value.clone(),
                right: minute_value.clone(),
            });
        }
    };
    if !(0..=23).contains(&hour) || !(0..=59).contains(&minute) || !(0.0..60.0).contains(&second) {
        return Err(invalid_make_time(hour, minute, second));
    }
    let whole_seconds = second.trunc() as i64;
    let micros = ((second.fract() * USECS_PER_SEC as f64).round()) as i64;
    let usecs = i64::from(hour) * USECS_PER_HOUR
        + i64::from(minute) * USECS_PER_MINUTE
        + whole_seconds * USECS_PER_SEC
        + micros;
    Ok(Value::Time(TimeADT(usecs)))
}

fn numeric_second_to_f64(value: &Value, op: &'static str) -> Result<f64, ExecError> {
    match value {
        Value::Int16(v) => Ok(f64::from(*v)),
        Value::Int32(v) => Ok(f64::from(*v)),
        Value::Int64(v) => Ok(*v as f64),
        Value::Float64(v) => Ok(*v),
        Value::Numeric(n) => n
            .render()
            .parse::<f64>()
            .map_err(|_| ExecError::TypeMismatch {
                op,
                left: value.clone(),
                right: Value::Float64(0.0),
            }),
        _ => Err(ExecError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::Float64(0.0),
        }),
    }
}

pub(crate) fn eval_make_timestamp_function(values: &[Value]) -> Result<Value, ExecError> {
    let [
        year_value,
        month_value,
        day_value,
        hour_value,
        minute_value,
        second_value,
    ] = values
    else {
        return Err(ExecError::DetailedError {
            message: "malformed make_timestamp call".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        });
    };
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let (
        Value::Int32(year),
        Value::Int32(month),
        Value::Int32(day),
        Value::Int32(hour),
        Value::Int32(minute),
    ) = (year_value, month_value, day_value, hour_value, minute_value)
    else {
        return Err(ExecError::TypeMismatch {
            op: "make_timestamp",
            left: year_value.clone(),
            right: second_value.clone(),
        });
    };
    if *year == 0 {
        return Err(invalid_make_date(*year, *month, *day));
    }
    let second = numeric_second_to_f64(second_value, "make_timestamp")?;
    if !second.is_finite() || !(0.0..60.0).contains(&second) {
        return Err(invalid_make_date(*year, *month, *day));
    }
    let astronomical_year = if *year < 0 { *year + 1 } else { *year };
    let month_u32 = u32::try_from(*month).map_err(|_| invalid_make_date(*year, *month, *day))?;
    let day_u32 = u32::try_from(*day).map_err(|_| invalid_make_date(*year, *month, *day))?;
    let days = days_from_ymd(astronomical_year, month_u32, day_u32)
        .ok_or_else(|| invalid_make_date(*year, *month, *day))?;
    let whole_second = second.trunc() as u32;
    let micros = ((second - f64::from(whole_second)) * USECS_PER_SEC as f64).round() as i64;
    let time = crate::backend::utils::time::datetime::time_usecs_from_hms(
        u32::try_from(*hour).map_err(|_| invalid_make_date(*year, *month, *day))?,
        u32::try_from(*minute).map_err(|_| invalid_make_date(*year, *month, *day))?,
        whole_second,
        micros,
    )
    .ok_or_else(|| invalid_make_date(*year, *month, *day))?;
    Ok(Value::Timestamp(TimestampADT(
        i64::from(days) * USECS_PER_DAY + time,
    )))
}

pub(crate) fn eval_make_interval_function(values: &[Value]) -> Result<Value, ExecError> {
    let [
        years_value,
        months_value,
        weeks_value,
        days_value,
        hours_value,
        mins_value,
        secs_value,
    ] = values
    else {
        return Err(ExecError::DetailedError {
            message: "malformed make_interval call".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        });
    };
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let (
        Value::Int32(years),
        Value::Int32(months),
        Value::Int32(weeks),
        Value::Int32(days),
        Value::Int32(hours),
        Value::Int32(mins),
        Value::Float64(secs),
    ) = (
        years_value,
        months_value,
        weeks_value,
        days_value,
        hours_value,
        mins_value,
        secs_value,
    )
    else {
        return Err(ExecError::TypeMismatch {
            op: "make_interval",
            left: values.first().cloned().unwrap_or(Value::Null),
            right: Value::Null,
        });
    };
    if !secs.is_finite() {
        return Err(interval_out_of_range_error());
    }

    let months = years
        .checked_mul(MONTHS_PER_YEAR)
        .and_then(|value| value.checked_add(*months))
        .ok_or_else(interval_out_of_range_error)?;
    let days = weeks
        .checked_mul(DAYS_PER_WEEK)
        .and_then(|value| value.checked_add(*days))
        .ok_or_else(interval_out_of_range_error)?;
    let base_micros = i64::from(*hours)
        .checked_mul(USECS_PER_HOUR)
        .and_then(|value| value.checked_add(i64::from(*mins).checked_mul(USECS_PER_MINUTE)?))
        .ok_or_else(interval_out_of_range_error)?;
    let secs_micros_float = secs * USECS_PER_SEC as f64;
    if !secs_micros_float.is_finite() {
        return Err(ExecError::FloatOverflow);
    }
    let secs_micros = secs_micros_float
        .round()
        .to_i64()
        .ok_or_else(interval_out_of_range_error)?;
    let time_micros = base_micros
        .checked_add(secs_micros)
        .ok_or_else(interval_out_of_range_error)?;
    let result = IntervalValue {
        time_micros,
        days,
        months,
    };
    if result.is_finite() {
        Ok(Value::Interval(result))
    } else {
        Err(interval_out_of_range_error())
    }
}

fn make_timestamptz_numeric_timezone_error(zone: &str) -> Option<ExecError> {
    let trimmed = zone.trim();
    if trimmed.chars().all(|ch| ch.is_ascii_digit()) {
        return Some(ExecError::DetailedError {
            message: format!("invalid input syntax for type numeric time zone: \"{trimmed}\""),
            detail: None,
            hint: Some("Numeric time zones must have \"-\" or \"+\" as first character.".into()),
            sqlstate: "22007",
        });
    }
    if matches!(trimmed.as_bytes().first(), Some(b'+') | Some(b'-'))
        && let Some(offset) = parse_offset_seconds(trimmed)
        && offset.abs() > 15 * 3600 + 59 * 60 + 59
    {
        return Some(ExecError::DetailedError {
            message: format!("numeric time zone \"{trimmed}\" out of range"),
            detail: None,
            hint: None,
            sqlstate: "22023",
        });
    }
    None
}

pub(crate) fn eval_make_timestamptz_function(
    values: &[Value],
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let (year, month, day, hour, minute, second, zone) = match values {
        [
            Value::Int32(year),
            Value::Int32(month),
            Value::Int32(day),
            Value::Int32(hour),
            Value::Int32(minute),
            Value::Float64(second),
        ] => (
            *year,
            *month,
            *day,
            *hour,
            *minute,
            *second,
            current_timezone_name(config),
        ),
        [
            Value::Int32(year),
            Value::Int32(month),
            Value::Int32(day),
            Value::Int32(hour),
            Value::Int32(minute),
            Value::Float64(second),
            zone_value,
        ] => {
            let zone = zone_value
                .as_text()
                .ok_or_else(|| ExecError::TypeMismatch {
                    op: "make_timestamptz",
                    left: zone_value.clone(),
                    right: Value::Text("".into()),
                })?;
            (*year, *month, *day, *hour, *minute, *second, zone)
        }
        _ => {
            return Err(ExecError::DetailedError {
                message: "malformed make_timestamptz call".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            });
        }
    };
    let month = u32::try_from(month).map_err(|_| ExecError::DetailedError {
        message: "date field value out of range".into(),
        detail: None,
        hint: None,
        sqlstate: "22008",
    })?;
    let day = u32::try_from(day).map_err(|_| ExecError::DetailedError {
        message: "date field value out of range".into(),
        detail: None,
        hint: None,
        sqlstate: "22008",
    })?;
    let hour = u32::try_from(hour).map_err(|_| ExecError::DetailedError {
        message: "time field value out of range".into(),
        detail: None,
        hint: None,
        sqlstate: "22008",
    })?;
    let minute = u32::try_from(minute).map_err(|_| ExecError::DetailedError {
        message: "time field value out of range".into(),
        detail: None,
        hint: None,
        sqlstate: "22008",
    })?;
    if let Some(err) = make_timestamptz_numeric_timezone_error(zone) {
        return Err(err);
    }
    make_timestamptz_from_parts(year, month, day, hour, minute, second, zone, config)
        .map(Value::TimestampTz)
        .map_err(|err| ExecError::InvalidStorageValue {
            column: "timestamptz".into(),
            details: super::expr_casts::datetime_parse_error_details(
                "timestamp with time zone",
                zone,
                err,
            ),
        })
}

fn timestamp_usecs_from_date(date: DateADT) -> Result<i64, ExecError> {
    match date.0 {
        DATEVAL_NOBEGIN => Ok(TIMESTAMP_NOBEGIN),
        DATEVAL_NOEND => Ok(TIMESTAMP_NOEND),
        days => i64::from(days)
            .checked_mul(USECS_PER_DAY)
            .ok_or_else(timestamp_out_of_range_error),
    }
}

pub(crate) fn eval_timestamptz_constructor_function(
    values: &[Value],
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    match values {
        [Value::Date(date), Value::Time(time)] => {
            let timestamp = timestamp_usecs_from_date(*date)?;
            if timestamp == TIMESTAMP_NOBEGIN || timestamp == TIMESTAMP_NOEND {
                return Ok(Value::TimestampTz(TimestampTzADT(timestamp)));
            }
            let local = timestamp
                .checked_add(time.0)
                .ok_or_else(timestamp_out_of_range_error)
                .and_then(|usecs| {
                    is_valid_finite_timestamp_usecs(usecs)
                        .then_some(TimestampADT(usecs))
                        .ok_or_else(timestamp_out_of_range_error)
                })?;
            timestamp_at_time_zone(local, current_timezone_name(config))
                .map(Value::TimestampTz)
                .map_err(|err| ExecError::InvalidStorageValue {
                    column: "timestamptz".into(),
                    details: super::expr_casts::datetime_parse_error_details(
                        "timestamp with time zone",
                        current_timezone_name(config),
                        err,
                    ),
                })
        }
        [Value::Date(date), Value::TimeTz(timetz)] => {
            let timestamp = timestamp_usecs_from_date(*date)?;
            if timestamp == TIMESTAMP_NOBEGIN || timestamp == TIMESTAMP_NOEND {
                return Ok(Value::TimestampTz(TimestampTzADT(timestamp)));
            }
            timestamp
                .checked_add(timetz.time.0)
                .and_then(|value| {
                    value.checked_sub(i64::from(timetz.offset_seconds) * USECS_PER_SEC)
                })
                .filter(|value| is_valid_finite_timestamp_usecs(*value))
                .map(|value| Value::TimestampTz(TimestampTzADT(value)))
                .ok_or_else(timestamp_out_of_range_error)
        }
        _ => Err(ExecError::DetailedError {
            message: "malformed timestamptz call".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        }),
    }
}

fn to_date_parse_error(input: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("invalid input syntax for type date: \"{input}\""),
        detail: None,
        hint: None,
        sqlstate: "22007",
    }
}

fn read_fixed_digits(input: &str, pos: &mut usize, width: usize) -> Option<i32> {
    let end = pos.checked_add(width)?;
    let bytes = input.as_bytes();
    if end > bytes.len() || !bytes[*pos..end].iter().all(u8::is_ascii_digit) {
        return None;
    }
    let value = input[*pos..end].parse().ok()?;
    *pos = end;
    Some(value)
}

fn parse_to_date_numeric_format(input: &str, format: &str) -> Result<DateADT, ExecError> {
    let normalized = format.to_ascii_uppercase();
    let mut fmt_pos = 0usize;
    let mut input_pos = 0usize;
    let mut year = None;
    let mut month = None;
    let mut day = None;

    while fmt_pos < normalized.len() {
        let remaining = &normalized[fmt_pos..];
        if remaining.starts_with("YYYY") {
            year = Some(
                read_fixed_digits(input, &mut input_pos, 4)
                    .ok_or_else(|| to_date_parse_error(input))?,
            );
            fmt_pos += 4;
        } else if remaining.starts_with("YY") {
            let yy = read_fixed_digits(input, &mut input_pos, 2)
                .ok_or_else(|| to_date_parse_error(input))?;
            year = Some(if yy < 70 { 2000 + yy } else { 1900 + yy });
            fmt_pos += 2;
        } else if remaining.starts_with("MM") {
            month = Some(
                read_fixed_digits(input, &mut input_pos, 2)
                    .ok_or_else(|| to_date_parse_error(input))?,
            );
            fmt_pos += 2;
        } else if remaining.starts_with("DD") {
            day = Some(
                read_fixed_digits(input, &mut input_pos, 2)
                    .ok_or_else(|| to_date_parse_error(input))?,
            );
            fmt_pos += 2;
        } else {
            let fmt_ch = normalized[fmt_pos..]
                .chars()
                .next()
                .expect("format position points at a char");
            let input_ch = input[input_pos..]
                .chars()
                .next()
                .ok_or_else(|| to_date_parse_error(input))?;
            if fmt_ch != input_ch.to_ascii_uppercase() {
                return Err(to_date_parse_error(input));
            }
            fmt_pos += fmt_ch.len_utf8();
            input_pos += input_ch.len_utf8();
        }
    }

    if input_pos != input.len() {
        return Err(to_date_parse_error(input));
    }
    let (Some(year), Some(month), Some(day)) = (year, month, day) else {
        return Err(ExecError::DetailedError {
            message: format!("format pattern not supported by to_date: \"{format}\""),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    };
    if year == 0 {
        return Err(to_date_parse_error(input));
    }
    let astronomical_year = display_year_to_astronomical(year);
    let month = u32::try_from(month).map_err(|_| to_date_parse_error(input))?;
    let day = u32::try_from(day).map_err(|_| to_date_parse_error(input))?;
    let days =
        days_from_ymd(astronomical_year, month, day).ok_or_else(|| to_date_parse_error(input))?;
    Ok(DateADT(days))
}

pub(crate) fn eval_to_date_function(values: &[Value]) -> Result<Value, ExecError> {
    let [input_value, format_value] = values else {
        return Err(ExecError::DetailedError {
            message: "malformed to_date call".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        });
    };
    if matches!(input_value, Value::Null) || matches!(format_value, Value::Null) {
        return Ok(Value::Null);
    }
    let input = input_value
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "to_date",
            left: input_value.clone(),
            right: Value::Text("".into()),
        })?;
    let format = format_value
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "to_date",
            left: format_value.clone(),
            right: Value::Text("".into()),
        })?;
    let config = DateTimeConfig {
        time_zone: "UTC".into(),
        ..DateTimeConfig::default()
    };
    let timestamp = parse_to_timestamp_text_format(input, format, &config)
        .map_err(|err| to_date_template_error(err, input))?;
    let (days, _) = timestamp_parts_from_usecs(timestamp.0);
    Ok(Value::Date(DateADT(days)))
}

fn to_date_template_error(err: ExecError, input: &str) -> ExecError {
    match err {
        ExecError::DetailedError {
            message,
            detail,
            hint,
            sqlstate,
        } if message.starts_with("invalid input syntax for type timestamp with time zone:") => {
            let _ = (detail, hint, sqlstate);
            to_date_parse_error(input)
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::libpq::pqformat::format_exec_error;
    use crate::include::nodes::datetime::{DateADT, TimeADT, TimeTzADT};

    #[test]
    fn to_timestamp_text_format_supports_horology_templates() {
        let config = DateTimeConfig {
            time_zone: "UTC".into(),
            ..DateTimeConfig::default()
        };

        for (input, format, expected) in [
            (
                "2011-12-18 11:38 PM",
                "YYYY-MM-DD HH12:MI PM",
                "2011-12-18 23:38:00+00",
            ),
            (
                "2011-12-18 11:38 +05",
                "YYYY-MM-DD HH12:MI TZH",
                "2011-12-18 06:38:00+00",
            ),
            (
                "1985 January 12",
                "YYYY FMMonth DD",
                "1985-01-12 00:00:00+00",
            ),
            (
                "05121445482000",
                "MMDDHH24MISSYYYY",
                "2000-05-12 14:45:48+00",
            ),
            (
                r#"15 "text between quote marks" 98 54 45"#,
                r#"HH24 \"text between quote marks\" YY MI SS"#,
                "1998-01-01 15:54:45+00",
            ),
            (
                r#"15 "text between quote marks" 98 54 45"#,
                r#"HH24 \\"text between quote marks\\" YY MI SS"#,
                "1998-01-01 15:54:45+00",
            ),
            (
                r#"15 "text between quote marks" 98 54 45"#,
                r#"HH24 "\"text between quote marks\"" YY MI SS"#,
                "1998-01-01 15:54:45+00",
            ),
            (
                r#"15 "text between quote marks" 98 54 45"#,
                r#"HH24 ""text between quote marks"" YY MI SS"#,
                "1998-01-01 15:54:45+00",
            ),
            ("9-1116", "Y-MMDD", "2009-11-16 00:00:00+00"),
            ("2005426", "YYYYWWD", "2005-10-15 00:00:00+00"),
            ("2005527", "IYYYIWID", "2006-01-01 00:00:00+00"),
            ("2005364", "IYYYIDDD", "2006-01-01 00:00:00+00"),
            ("2005 03 02", "YYYYMMDD", "2005-03-02 00:00:00+00"),
            (
                "1997 A.D. 11 16",
                "YYYY B.C. MM DD",
                "1997-11-16 00:00:00+00",
            ),
            (
                "44-02-01 11:12:13 BC",
                "YYYY-MM-DD HH24:MI:SS BC",
                "0044-02-01 11:12:13 BC",
            ),
            ("2000 ++ JUN", "YYYY  MON", "2000-06-01 00:00:00+00"),
            ("2000 + + JUN", "YYYY   MON", "2000-06-01 00:00:00+00"),
        ] {
            let parsed = parse_to_timestamp_text_format(input, format, &config)
                .unwrap_or_else(|err| panic!("{input} / {format}: {}", format_exec_error(&err)));
            let expected =
                crate::backend::utils::time::timestamp::parse_timestamptz_text(expected, &config)
                    .unwrap();
            assert_eq!(parsed, expected, "{input} / {format}");
        }
    }

    #[test]
    fn to_timestamp_fractional_template_edges() {
        let config = DateTimeConfig {
            time_zone: "UTC".into(),
            ..DateTimeConfig::default()
        };
        let parsed = parse_to_timestamp_text_format(
            "2018-11-02 12:34:56.025",
            "YYYY-MM-DD HH24:MI:SS.MS",
            &config,
        )
        .unwrap();
        let expected = crate::backend::utils::time::timestamp::parse_timestamptz_text(
            "2018-11-02 12:34:56.025+00",
            &config,
        )
        .unwrap();
        assert_eq!(parsed, expected);

        assert!(
            parse_to_timestamp_text_format(
                "2018-11-02 12:34:56.123456789",
                "YYYY-MM-DD HH24:MI:SS.FF6",
                &config,
            )
            .is_err()
        );
        assert!(
            parse_to_timestamp_text_format(
                "2018-11-02 12:34:56",
                "YYYY-MM-DD HH24:MI:SS.FF6",
                &config,
            )
            .is_ok()
        );
    }

    #[test]
    fn to_timestamp_template_reports_postgres_field_errors() {
        let config = DateTimeConfig {
            time_zone: "UTC".into(),
            ..DateTimeConfig::default()
        };
        for (input, format, expected) in [
            (
                "2005527",
                "YYYYIWID",
                "invalid combination of date conventions",
            ),
            (
                "19971",
                "YYYYMMDD",
                "source string too short for \"MM\" formatting field",
            ),
            (
                "1997-11-Jan-16",
                "YYYY-MM-Mon-DD",
                "conflicting values for \"Mon\" field in formatting string",
            ),
            (
                "2016-06-13 15:50:55",
                "YYYY-MM-DD HH:MI:SS",
                "hour \"15\" is invalid for the 12-hour clock",
            ),
            (
                "2011-12-18 11:38 +xyz",
                "YYYY-MM-DD HH12:MI OF",
                "invalid value \"xy\" for \"OF\"",
            ),
            (
                "2000 + + JUN",
                "YYYY  MON",
                "invalid value \"+\" for \"MON\"",
            ),
            (
                "Fri 1-January-1999",
                "DY DD MON YYYY",
                "invalid value \"uary\" for \"YYYY\"",
            ),
        ] {
            let err = parse_to_timestamp_text_format(input, format, &config).unwrap_err();
            assert_eq!(format_exec_error(&err), expected, "{input} / {format}");
        }
    }

    #[test]
    fn to_date_uses_postgres_template_parser_cases() {
        assert_eq!(
            eval_to_date_function(&[
                Value::Text("1 4 1902".into()),
                Value::Text("Q MM YYYY".into())
            ])
            .unwrap(),
            Value::Date(DateADT(days_from_ymd(1902, 4, 1).unwrap()))
        );
        assert_eq!(
            eval_to_date_function(&[
                Value::Text("3 4 21 01".into()),
                Value::Text("W MM CC YY".into()),
            ])
            .unwrap(),
            Value::Date(DateADT(days_from_ymd(2001, 4, 15).unwrap()))
        );
        assert_eq!(
            eval_to_date_function(&[Value::Text("2458872".into()), Value::Text("J".into())])
                .unwrap(),
            Value::Date(DateADT(days_from_ymd(2020, 1, 23).unwrap()))
        );
        assert_eq!(
            eval_to_date_function(&[
                Value::Text("44-02-01 BC".into()),
                Value::Text("YYYY-MM-DD BC".into()),
            ])
            .unwrap(),
            Value::Date(DateADT(days_from_ymd(-43, 2, 1).unwrap()))
        );
        assert_eq!(
            eval_to_date_function(&[
                Value::Text("0000-02-01".into()),
                Value::Text("YYYY-MM-DD".into()),
            ])
            .unwrap(),
            Value::Date(DateADT(days_from_ymd(0, 2, 1).unwrap()))
        );
    }

    #[test]
    fn date_part_handles_bc_and_iso_fields() {
        assert_eq!(
            eval_date_part_function(&[
                Value::Text("year".into()),
                Value::Date(DateADT(
                    crate::backend::utils::time::datetime::days_from_ymd(-2019, 8, 11,).unwrap()
                )),
            ])
            .unwrap(),
            Value::Float64(-2020.0)
        );
        assert_eq!(
            eval_date_part_function(&[
                Value::Text("dow".into()),
                Value::Date(DateADT(
                    crate::backend::utils::time::datetime::days_from_ymd(2020, 8, 16,).unwrap()
                )),
            ])
            .unwrap(),
            Value::Float64(0.0)
        );
        assert_eq!(
            eval_date_part_function(&[
                Value::Text("isodow".into()),
                Value::Date(DateADT(
                    crate::backend::utils::time::datetime::days_from_ymd(2020, 8, 16,).unwrap()
                )),
            ])
            .unwrap(),
            Value::Float64(7.0)
        );
    }

    #[test]
    fn date_trunc_handles_bc_boundaries() {
        assert_eq!(
            eval_date_trunc_function(
                &[
                    Value::Text("century".into()),
                    Value::Date(DateADT(days_from_ymd(-54, 8, 10).unwrap())),
                ],
                &DateTimeConfig::default()
            )
            .unwrap(),
            Value::TimestampTz(TimestampTzADT(
                i64::from(days_from_ymd(-99, 1, 1).unwrap()) * USECS_PER_DAY,
            ))
        );
        assert_eq!(
            eval_date_trunc_function(
                &[
                    Value::Text("decade".into()),
                    Value::Date(DateADT(days_from_ymd(4, 12, 25).unwrap())),
                ],
                &DateTimeConfig::default()
            )
            .unwrap(),
            Value::TimestampTz(TimestampTzADT(
                i64::from(days_from_ymd(0, 1, 1).unwrap()) * USECS_PER_DAY,
            ))
        );
    }

    #[test]
    fn date_trunc_date_uses_local_zone_rules() {
        let config = DateTimeConfig {
            time_zone: "America/Los_Angeles".into(),
            ..DateTimeConfig::default()
        };
        assert_eq!(
            eval_date_trunc_function(
                &[
                    Value::Text("century".into()),
                    Value::Date(DateADT(days_from_ymd(2004, 8, 10).unwrap())),
                ],
                &config,
            )
            .unwrap(),
            Value::TimestampTz(TimestampTzADT(
                i64::from(days_from_ymd(2001, 1, 1).unwrap()) * USECS_PER_DAY + 8 * USECS_PER_HOUR,
            ))
        );
    }

    #[test]
    fn date_trunc_supports_timestamp_and_timestamptz() {
        let config = DateTimeConfig {
            time_zone: "America/Los_Angeles".into(),
            ..DateTimeConfig::default()
        };
        assert_eq!(
            eval_date_trunc_function(
                &[
                    Value::Text("century".into()),
                    Value::Timestamp(TimestampADT(
                        i64::from(days_from_ymd(1970, 3, 20).unwrap()) * USECS_PER_DAY
                            + (4 * 3600 + 30 * 60) * USECS_PER_SEC,
                    )),
                ],
                &config,
            )
            .unwrap(),
            Value::Timestamp(TimestampADT(
                i64::from(days_from_ymd(1901, 1, 1).unwrap()) * USECS_PER_DAY,
            ))
        );
        assert_eq!(
            eval_date_trunc_function(
                &[
                    Value::Text("decade".into()),
                    Value::TimestampTz(TimestampTzADT(
                        i64::from(days_from_ymd(1993, 12, 25).unwrap()) * USECS_PER_DAY
                            + 8 * 3600 * USECS_PER_SEC,
                    )),
                ],
                &config,
            )
            .unwrap(),
            Value::TimestampTz(TimestampTzADT(
                i64::from(days_from_ymd(1990, 1, 1).unwrap()) * USECS_PER_DAY
                    + 8 * 3600 * USECS_PER_SEC,
            ))
        );
    }

    #[test]
    fn date_trunc_timestamptz_uses_local_zone_rules() {
        let config = DateTimeConfig {
            time_zone: "America/Los_Angeles".into(),
            ..DateTimeConfig::default()
        };
        let source = TimestampTzADT(
            i64::from(days_from_ymd(2004, 2, 29).unwrap()) * USECS_PER_DAY
                + (15 * 3600 + 44 * 60 + 17) * USECS_PER_SEC
                + 8 * USECS_PER_HOUR,
        );
        assert_eq!(
            eval_date_trunc_function(
                &[Value::Text("week".into()), Value::TimestampTz(source)],
                &config,
            )
            .unwrap(),
            Value::TimestampTz(TimestampTzADT(
                i64::from(days_from_ymd(2004, 2, 23).unwrap()) * USECS_PER_DAY + 8 * USECS_PER_HOUR,
            ))
        );
    }

    #[test]
    fn date_trunc_rejects_unsupported_field_for_infinite_timestamptz() {
        let err = eval_date_trunc_function(
            &[
                Value::Text("timezone".into()),
                Value::TimestampTz(TimestampTzADT(TIMESTAMP_NOEND)),
            ],
            &DateTimeConfig::default(),
        )
        .unwrap_err();
        match err {
            ExecError::DetailedError {
                message, sqlstate, ..
            } => {
                assert_eq!(
                    message,
                    "unit \"timezone\" not supported for type timestamp with time zone"
                );
                assert_eq!(sqlstate, "0A000");
            }
            other => panic!("expected detailed error, got {other:?}"),
        }
    }

    #[test]
    fn make_date_maps_negative_years_to_bc() {
        assert_eq!(
            eval_make_date_function(&[Value::Int32(-44), Value::Int32(3), Value::Int32(15)])
                .unwrap(),
            Value::Date(DateADT(days_from_ymd(-43, 3, 15).unwrap()))
        );
    }

    #[test]
    fn make_date_rejects_minimum_i32_year() {
        match eval_make_date_function(&[Value::Int32(i32::MIN), Value::Int32(1), Value::Int32(1)])
            .unwrap_err()
        {
            ExecError::DetailedError {
                message, sqlstate, ..
            } => {
                assert_eq!(message, "date field value out of range: -2147483648-01-01");
                assert_eq!(sqlstate, "22008");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn date_part_supports_time_fields() {
        let time = TimeADT(((13 * 60 * 60 + 30 * 60 + 25) as i64 * USECS_PER_SEC) + 575_401);
        assert_eq!(
            eval_date_part_function(&[Value::Text("microseconds".into()), Value::Time(time)])
                .unwrap(),
            Value::Float64(25_575_401.0)
        );
        assert_eq!(
            eval_date_part_function(&[Value::Text("milliseconds".into()), Value::Time(time)])
                .unwrap(),
            Value::Float64(25_575.401)
        );
        assert_eq!(
            eval_date_part_function(&[Value::Text("second".into()), Value::Time(time)]).unwrap(),
            Value::Float64(25.575401)
        );
        assert_eq!(
            eval_date_part_function(&[Value::Text("minute".into()), Value::Time(time)]).unwrap(),
            Value::Float64(30.0)
        );
        assert_eq!(
            eval_date_part_function(&[Value::Text("hour".into()), Value::Time(time)]).unwrap(),
            Value::Float64(13.0)
        );
        assert_eq!(
            eval_date_part_function(&[Value::Text("epoch".into()), Value::Time(time)]).unwrap(),
            Value::Float64(48_625.575401)
        );
    }

    #[test]
    fn date_part_rejects_invalid_time_fields_with_postgres_errors() {
        let time = TimeADT((13 * 60 * 60) as i64 * USECS_PER_SEC);
        match eval_date_part_function(&[Value::Text("day".into()), Value::Time(time)]).unwrap_err()
        {
            ExecError::DetailedError {
                message, sqlstate, ..
            } => {
                assert_eq!(
                    message,
                    "unit \"day\" not supported for type time without time zone"
                );
                assert_eq!(sqlstate, "0A000");
            }
            other => panic!("expected detailed error, got {other:?}"),
        }
        match eval_date_part_function(&[Value::Text("fortnight".into()), Value::Time(time)])
            .unwrap_err()
        {
            ExecError::DetailedError {
                message, sqlstate, ..
            } => {
                assert_eq!(
                    message,
                    "unit \"fortnight\" not recognized for type time without time zone"
                );
                assert_eq!(sqlstate, "22023");
            }
            other => panic!("expected detailed error, got {other:?}"),
        }
    }

    #[test]
    fn date_part_supports_timetz_timezone_fields() {
        let timetz = TimeTzADT {
            time: TimeADT((13 * 60 * 60 + 30 * 60) as i64 * USECS_PER_SEC),
            offset_seconds: -7 * 60 * 60,
        };
        assert_eq!(
            eval_date_part_function(&[Value::Text("timezone".into()), Value::TimeTz(timetz)])
                .unwrap(),
            Value::Float64(-25_200.0)
        );
        assert_eq!(
            eval_date_part_function(&[Value::Text("timezone_h".into()), Value::TimeTz(timetz)])
                .unwrap(),
            Value::Float64(-7.0)
        );
        assert_eq!(
            eval_date_part_function(&[Value::Text("timezone_hour".into()), Value::TimeTz(timetz)])
                .unwrap(),
            Value::Float64(-7.0)
        );
        assert_eq!(
            eval_date_part_function(&[Value::Text("timezone_m".into()), Value::TimeTz(timetz)])
                .unwrap(),
            Value::Float64(0.0)
        );
        assert_eq!(
            eval_date_part_function(&[Value::Text("epoch".into()), Value::TimeTz(timetz)]).unwrap(),
            Value::Float64((20 * 60 * 60 + 30 * 60) as f64)
        );
    }

    #[test]
    fn date_part_timestamptz_timezone_fields_use_pg_sign() {
        let config = DateTimeConfig {
            time_zone: "America/Los_Angeles".into(),
            ..DateTimeConfig::default()
        };
        let timestamp = TimestampTzADT(
            i64::from(days_from_ymd(1997, 2, 10).unwrap()) * USECS_PER_DAY + 8 * USECS_PER_HOUR,
        );
        assert_eq!(
            eval_date_part_function_with_config(
                &[
                    Value::Text("timezone".into()),
                    Value::TimestampTz(timestamp),
                ],
                &config,
            )
            .unwrap(),
            Value::Float64(-28_800.0)
        );
        assert_eq!(
            eval_date_part_function_with_config(
                &[
                    Value::Text("timezone_h".into()),
                    Value::TimestampTz(timestamp),
                ],
                &config,
            )
            .unwrap(),
            Value::Float64(-8.0)
        );
    }
}
