use super::{ExecError, Value};
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::backend::utils::time::datetime::{DateTimeParseError, TimeZoneSpec};
use crate::backend::utils::time::datetime::{
    day_of_week_from_julian_day, day_of_year, days_from_ymd, days_in_month,
    iso_day_of_week_from_julian_day, iso_week_and_year, julian_day_from_postgres_date,
    parse_timezone_spec, timestamp_parts_from_usecs, timezone_offset_seconds,
    unix_days_from_postgres_date, ymd_from_days,
};
use crate::include::nodes::datetime::{
    DATEVAL_NOBEGIN, DATEVAL_NOEND, DateADT, TIMESTAMP_NOBEGIN, TIMESTAMP_NOEND, TimeADT,
    TimeTzADT, TimestampADT, TimestampTzADT, USECS_PER_DAY, USECS_PER_HOUR, USECS_PER_MINUTE,
    USECS_PER_SEC,
};
use crate::include::nodes::datum::{IntervalValue, NumericValue};

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

fn timezone_target_offset_seconds(
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
                DateTimeParseError::Invalid | DateTimeParseError::FieldOutOfRange => {
                    timezone_function_error(
                        format!("invalid input syntax for type time zone: \"{name}\""),
                        "22007",
                    )
                }
                DateTimeParseError::TimeZoneDisplacementOutOfRange => {
                    timezone_function_error("time zone displacement out of range", "22008")
                }
                DateTimeParseError::TimestampOutOfRange => timezone_function_error(
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
        Value::Interval(interval) if interval.months == 0 && interval.days == 0 => {
            i32::try_from(interval.time_micros / USECS_PER_SEC)
                .map_err(|_| timezone_function_error("time zone interval out of range", "22008"))
        }
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

fn normalize_datetime_part(field: &str) -> &str {
    match field {
        "msec" => "milliseconds",
        "usec" => "microseconds",
        "secs" => "seconds",
        other => other,
    }
}

fn eval_timestamp_part(
    field: &str,
    timestamp_usecs: i64,
    with_timezone: bool,
) -> Result<Value, ExecError> {
    let field = normalize_datetime_part(field);
    if !matches!(
        field,
        "microsecond"
            | "microseconds"
            | "millisecond"
            | "milliseconds"
            | "second"
            | "seconds"
            | "minute"
            | "hour"
            | "day"
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
        if matches!(field, "timezone" | "timezone_m" | "timezone_h") {
            return Err(unsupported_timestamp_part(field, with_timezone));
        }
        return Err(unrecognized_timestamp_part(field, with_timezone));
    }

    if timestamp_usecs == TIMESTAMP_NOEND || timestamp_usecs == TIMESTAMP_NOBEGIN {
        return Ok(match field {
            "day" | "month" | "quarter" | "week" | "dow" | "isodow" | "doy" | "microsecond"
            | "microseconds" | "millisecond" | "milliseconds" | "second" | "seconds" | "minute"
            | "hour" => Value::Null,
            "year" | "decade" | "century" | "millennium" | "julian" | "isoyear" | "epoch" => {
                Value::Float64(if timestamp_usecs == TIMESTAMP_NOEND {
                    f64::INFINITY
                } else {
                    f64::NEG_INFINITY
                })
            }
            _ => Value::Null,
        });
    }

    let (days, time_usecs) = timestamp_parts_from_usecs(timestamp_usecs);
    let (astronomical_year, month, day) = ymd_from_days(days);
    let year = extract_year_number(astronomical_year);
    let julian_day = julian_day_from_postgres_date(days);
    let (iso_year_astronomical, iso_week) = iso_week_and_year(astronomical_year, month, day);
    let iso_year = extract_year_number(iso_year_astronomical);
    let second_in_minute = time_usecs.rem_euclid(USECS_PER_MINUTE);
    let result = match field {
        "microsecond" | "microseconds" => second_in_minute as f64,
        "millisecond" | "milliseconds" => second_in_minute as f64 / 1_000.0,
        "second" | "seconds" => second_in_minute as f64 / USECS_PER_SEC as f64,
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
        _ => return Err(unrecognized_timestamp_part(field, with_timezone)),
    };
    Ok(Value::Float64(result))
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

    match date_value {
        Value::Time(time) => return eval_time_part(&field, *time, false),
        Value::TimeTz(timetz) => return eval_timetz_part(&field, *timetz),
        Value::Timestamp(timestamp) => return eval_timestamp_part(&field, timestamp.0, false),
        Value::TimestampTz(timestamp) => {
            let local = if timestamp.is_finite() {
                timestamp.0
                    + i64::from(timezone_offset_seconds(&DateTimeConfig::default())) * USECS_PER_SEC
            } else {
                timestamp.0
            };
            return eval_timestamp_part(&field, local, true);
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
        field.as_str(),
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
        return Err(unsupported_date_part(&field));
    }

    if !matches!(
        field.as_str(),
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
        return Err(unrecognized_date_part(&field));
    }

    if !date.is_finite() {
        return Ok(match field.as_str() {
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

    let result = match field.as_str() {
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
        _ => return Err(unrecognized_date_part(&field)),
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

fn extract_numeric_display_scale(field: &str) -> usize {
    match normalize_datetime_part(field) {
        "millisecond" | "milliseconds" => 3,
        "second" | "seconds" | "epoch" | "julian" => 6,
        _ => 0,
    }
}

fn float_part_to_numeric(value: Value, field: &str) -> Value {
    let Value::Float64(value) = value else {
        return value;
    };
    let numeric = if value.is_nan() {
        NumericValue::NaN
    } else if value.is_infinite() {
        if value.is_sign_negative() {
            NumericValue::NegInf
        } else {
            NumericValue::PosInf
        }
    } else {
        let scale = extract_numeric_display_scale(field);
        NumericValue::from(format!("{value:.scale$}"))
    };
    Value::Numeric(numeric)
}

pub(crate) fn eval_extract_function(values: &[Value]) -> Result<Value, ExecError> {
    let [field_value, _] = values else {
        return Err(ExecError::DetailedError {
            message: "malformed extract call".into(),
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
            op: "extract",
            left: field_value.clone(),
            right: Value::Text("".into()),
        })?
        .trim()
        .to_ascii_lowercase();
    eval_date_part_function(values).map(|value| float_part_to_numeric(value, &field))
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
        other => Err(ExecError::TypeMismatch {
            op: "isfinite",
            left: other.clone(),
            right: Value::Date(crate::include::nodes::datetime::DateADT(0)),
        }),
    }
}

fn truncate_timestamp_local(field: &str, days: i32) -> Result<i32, ExecError> {
    let (astronomical_year, month, _day) = ymd_from_days(days);
    let display_year = extract_year_number(astronomical_year);
    let truncated_astronomical_year = match field {
        "millennium" => {
            display_year_to_astronomical(truncation_field_start_display_year(display_year, 1000))
        }
        "century" => {
            display_year_to_astronomical(truncation_field_start_display_year(display_year, 100))
        }
        "decade" => astronomical_year.div_euclid(10) * 10,
        "year" => astronomical_year,
        "quarter" => {
            return days_from_ymd(astronomical_year, ((month - 1) / 3) * 3 + 1, 1)
                .ok_or_else(|| unsupported_timestamp_part(field, false));
        }
        "month" => {
            return days_from_ymd(astronomical_year, month, 1)
                .ok_or_else(|| unsupported_timestamp_part(field, false));
        }
        "week" => {
            let julian = julian_day_from_postgres_date(days);
            let isodow = iso_day_of_week_from_julian_day(julian) as i32;
            return Ok(days - (isodow - 1));
        }
        "day" => return Ok(days),
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

fn timestamp_trunc_usecs(field: &str, timestamp_usecs: i64) -> Result<i64, ExecError> {
    let field = normalize_datetime_part(field);
    if !matches!(
        field,
        "millennium"
            | "century"
            | "decade"
            | "year"
            | "quarter"
            | "month"
            | "week"
            | "day"
            | "hour"
            | "minute"
            | "second"
            | "seconds"
            | "millisecond"
            | "milliseconds"
            | "microsecond"
            | "microseconds"
    ) {
        if matches!(field, "timezone" | "timezone_m" | "timezone_h") {
            return Err(unsupported_timestamp_part(field, false));
        }
        return Err(unrecognized_timestamp_part(field, false));
    }
    if timestamp_usecs == TIMESTAMP_NOEND || timestamp_usecs == TIMESTAMP_NOBEGIN {
        return Ok(timestamp_usecs);
    }
    let (days, time_usecs) = timestamp_parts_from_usecs(timestamp_usecs);
    let truncated = match field {
        "hour" => {
            i64::from(days) * USECS_PER_DAY + time_usecs.div_euclid(USECS_PER_HOUR) * USECS_PER_HOUR
        }
        "minute" => {
            i64::from(days) * USECS_PER_DAY
                + time_usecs.div_euclid(USECS_PER_MINUTE) * USECS_PER_MINUTE
        }
        "second" | "seconds" => {
            i64::from(days) * USECS_PER_DAY + time_usecs.div_euclid(USECS_PER_SEC) * USECS_PER_SEC
        }
        "millisecond" | "milliseconds" => {
            i64::from(days) * USECS_PER_DAY + time_usecs.div_euclid(1_000) * 1_000
        }
        "microsecond" | "microseconds" => timestamp_usecs,
        _ => i64::from(truncate_timestamp_local(field, days)?) * USECS_PER_DAY,
    };
    Ok(truncated)
}

pub(crate) fn eval_date_trunc_function(
    values: &[Value],
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    let [field_value, date_value] = values else {
        return Err(ExecError::DetailedError {
            message: "malformed date_trunc call".into(),
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
            op: "date_trunc",
            left: field_value.clone(),
            right: Value::Text("".into()),
        })?;
    let field = field.trim().to_ascii_lowercase();
    match date_value {
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
            let offset_seconds = i64::from(timezone_offset_seconds(config));
            Ok(Value::TimestampTz(TimestampTzADT(
                i64::from(days) * USECS_PER_DAY - offset_seconds * USECS_PER_SEC,
            )))
        }
        Value::Timestamp(timestamp) => timestamp_trunc_usecs(&field, timestamp.0)
            .map(|value| Value::Timestamp(TimestampADT(value))),
        Value::TimestampTz(timestamp) => {
            if !timestamp.is_finite() {
                timestamp_trunc_usecs(&field, timestamp.0)?;
                return Ok(Value::TimestampTz(*timestamp));
            }
            let offset_seconds = i64::from(timezone_offset_seconds(config));
            let local_usecs = timestamp.0 + offset_seconds * USECS_PER_SEC;
            timestamp_trunc_usecs(&field, local_usecs).map(|value| {
                Value::TimestampTz(TimestampTzADT(value - offset_seconds * USECS_PER_SEC))
            })
        }
        other => Err(ExecError::TypeMismatch {
            op: "date_trunc",
            left: field_value.clone(),
            right: other.clone(),
        }),
    }
}

fn interval_total_usecs(value: IntervalValue) -> Option<i64> {
    if !value.is_finite() || value.months != 0 {
        return None;
    }
    i64::from(value.days)
        .checked_mul(USECS_PER_DAY)
        .and_then(|days| days.checked_add(value.time_micros))
}

fn interval_out_of_range() -> ExecError {
    ExecError::DetailedError {
        message: "interval out of range".into(),
        detail: None,
        hint: None,
        sqlstate: "22008",
    }
}

fn timestamp_out_of_range() -> ExecError {
    ExecError::DetailedError {
        message: "timestamp out of range".into(),
        detail: None,
        hint: None,
        sqlstate: "22008",
    }
}

fn validate_timestamp_usecs_range(value: i64) -> Result<i64, ExecError> {
    let min = i64::from(days_from_ymd(-4713, 11, 24).expect("valid timestamp lower bound"))
        * USECS_PER_DAY;
    let max_exclusive =
        i64::from(days_from_ymd(294277, 1, 1).expect("valid timestamp upper bound"))
            * USECS_PER_DAY;
    if value < min || value >= max_exclusive {
        Err(timestamp_out_of_range())
    } else {
        Ok(value)
    }
}

pub(crate) fn eval_date_bin_function(values: &[Value]) -> Result<Value, ExecError> {
    let [stride_value, source_value, origin_value] = values else {
        return Err(ExecError::DetailedError {
            message: "malformed date_bin call".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        });
    };
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let Value::Interval(stride) = stride_value else {
        return Err(ExecError::TypeMismatch {
            op: "date_bin",
            left: stride_value.clone(),
            right: Value::Interval(IntervalValue::zero()),
        });
    };
    if stride.months != 0 {
        return Err(ExecError::DetailedError {
            message: "timestamps cannot be binned into intervals containing months or years".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    let stride_usecs = interval_total_usecs(*stride).ok_or_else(interval_out_of_range)?;
    if stride_usecs <= 0 {
        return Err(ExecError::DetailedError {
            message: "stride must be greater than zero".into(),
            detail: None,
            hint: None,
            sqlstate: "22008",
        });
    }
    let (source, origin, timestamptz) = match (source_value, origin_value) {
        (Value::Timestamp(source), Value::Timestamp(origin)) => (source.0, origin.0, false),
        (Value::TimestampTz(source), Value::TimestampTz(origin)) => (source.0, origin.0, true),
        _ => {
            return Err(ExecError::TypeMismatch {
                op: "date_bin",
                left: source_value.clone(),
                right: origin_value.clone(),
            });
        }
    };
    let diff = source
        .checked_sub(origin)
        .ok_or_else(interval_out_of_range)?;
    let bins = diff.div_euclid(stride_usecs);
    let offset = bins
        .checked_mul(stride_usecs)
        .ok_or_else(interval_out_of_range)?;
    let result = origin
        .checked_add(offset)
        .ok_or_else(interval_out_of_range)?;
    let result = validate_timestamp_usecs_range(result)?;
    Ok(if timestamptz {
        Value::TimestampTz(TimestampTzADT(result))
    } else {
        Value::Timestamp(TimestampADT(result))
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

pub(crate) fn eval_age_function(values: &[Value]) -> Result<Value, ExecError> {
    let (left, right) = match values {
        [Value::Null] | [_, Value::Null] | [Value::Null, _] => return Ok(Value::Null),
        [Value::Timestamp(ts)] => (
            crate::backend::utils::time::datetime::current_postgres_timestamp_usecs(),
            ts.0,
        ),
        [Value::Timestamp(left), Value::Timestamp(right)] => (left.0, right.0),
        _ => {
            return Err(ExecError::TypeMismatch {
                op: "age",
                left: values.first().cloned().unwrap_or(Value::Null),
                right: values.get(1).cloned().unwrap_or(Value::Null),
            });
        }
    };
    match (left, right) {
        (TIMESTAMP_NOEND, TIMESTAMP_NOEND) | (TIMESTAMP_NOBEGIN, TIMESTAMP_NOBEGIN) => {
            Err(interval_out_of_range())
        }
        (TIMESTAMP_NOEND, _) | (_, TIMESTAMP_NOBEGIN) => {
            Ok(Value::Interval(IntervalValue::infinity()))
        }
        (TIMESTAMP_NOBEGIN, _) | (_, TIMESTAMP_NOEND) => {
            Ok(Value::Interval(IntervalValue::neg_infinity()))
        }
        _ => {
            let diff = left.checked_sub(right).ok_or_else(interval_out_of_range)?;
            Ok(Value::Interval(IntervalValue {
                time_micros: diff % USECS_PER_DAY,
                days: i32::try_from(diff / USECS_PER_DAY).map_err(|_| interval_out_of_range())?,
                months: 0,
            }))
        }
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
    parse_to_date_numeric_format(input, format).map(Value::Date)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::include::nodes::datetime::{DateADT, TimeADT, TimeTzADT};

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
}
