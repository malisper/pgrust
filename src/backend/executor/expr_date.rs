use super::{ExecError, Value};
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::backend::utils::time::datetime::{
    day_of_week_from_julian_day, day_of_year, days_from_ymd, iso_day_of_week_from_julian_day,
    iso_week_and_year, julian_day_from_postgres_date, timestamp_parts_from_usecs,
    timezone_offset_seconds, unix_days_from_postgres_date, ymd_from_days,
};
use crate::include::nodes::datetime::{
    DATEVAL_NOBEGIN, DATEVAL_NOEND, DateADT, TIMESTAMP_NOBEGIN, TIMESTAMP_NOEND, TimeADT,
    TimeTzADT, TimestampADT, TimestampTzADT, USECS_PER_DAY, USECS_PER_HOUR, USECS_PER_MINUTE,
    USECS_PER_SEC,
};

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

fn unsupported_timestamp_part(field: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("unit \"{field}\" not supported for type timestamp"),
        detail: None,
        hint: None,
        sqlstate: "0A000",
    }
}

fn unrecognized_timestamp_part(field: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("unit \"{field}\" not recognized for type timestamp"),
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
        "timezone" | "timezone_h" | "timezone_m" | "day" | "month" | "year" | "quarter"
        | "decade" | "century" | "millennium" | "isoyear" | "week" | "dow" | "isodow" | "doy"
        | "julian" => return Err(unsupported_time_part(field, with_timezone)),
        _ => return Err(unrecognized_time_part(field, with_timezone)),
    };
    Ok(Value::Float64(result))
}

fn eval_timetz_part(field: &str, timetz: TimeTzADT) -> Result<Value, ExecError> {
    let time_result = eval_time_part(field, timetz.time, true);
    match field {
        "timezone" => Ok(Value::Float64((-timetz.offset_seconds) as f64)),
        "timezone_h" => Ok(Value::Float64(((-timetz.offset_seconds) / 3_600) as f64)),
        "timezone_m" => Ok(Value::Float64(((-timetz.offset_seconds) / 60 % 60) as f64)),
        _ => time_result,
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
        Value::Timestamp(timestamp) => return eval_timestamp_part(&field, timestamp.0),
        Value::TimestampTz(timestamp) => return eval_timestamp_part(&field, timestamp.0),
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
            | "timezone_h"
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

fn eval_timestamp_part(field: &str, timestamp_usecs: i64) -> Result<Value, ExecError> {
    let (days, time_usecs) = timestamp_parts_from_usecs(timestamp_usecs);
    let time_result = match field {
        "microsecond" | "microseconds" => Some(time_usecs.rem_euclid(USECS_PER_MINUTE) as f64),
        "millisecond" | "milliseconds" => {
            Some(time_usecs.rem_euclid(USECS_PER_MINUTE) as f64 / 1_000.0)
        }
        "second" => Some(time_usecs.rem_euclid(USECS_PER_MINUTE) as f64 / USECS_PER_SEC as f64),
        "minute" => Some(time_usecs.div_euclid(USECS_PER_MINUTE).rem_euclid(60) as f64),
        "hour" => Some(time_usecs.div_euclid(USECS_PER_HOUR) as f64),
        _ => None,
    };
    if let Some(result) = time_result {
        return Ok(Value::Float64(result));
    }

    let (astronomical_year, month, day) = ymd_from_days(days);
    let year = extract_year_number(astronomical_year);
    let julian_day = julian_day_from_postgres_date(days);
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
        "julian" => julian_day as f64 + time_usecs as f64 / USECS_PER_DAY as f64,
        "epoch" => timestamp_usecs as f64 / USECS_PER_SEC as f64 + 946_684_800.0,
        "timezone" | "timezone_h" | "timezone_m" => {
            return Err(unsupported_timestamp_part(field));
        }
        _ => return Err(unrecognized_timestamp_part(field)),
    };
    Ok(Value::Float64(result))
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
    let (astronomical_year, _, _) = ymd_from_days(days);
    let display_year = extract_year_number(astronomical_year);
    let truncated_astronomical_year = match field {
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
        Value::Timestamp(timestamp) => {
            if !timestamp.is_finite() {
                return Ok(Value::Timestamp(*timestamp));
            }
            let (days, _) =
                crate::backend::utils::time::datetime::timestamp_parts_from_usecs(timestamp.0);
            let truncated_days = truncate_timestamp_local(&field, days)?;
            Ok(Value::Timestamp(TimestampADT(
                i64::from(truncated_days) * USECS_PER_DAY,
            )))
        }
        Value::TimestampTz(timestamp) => {
            if !timestamp.is_finite() {
                return Ok(Value::TimestampTz(*timestamp));
            }
            let offset_seconds = i64::from(timezone_offset_seconds(config));
            let local_usecs = timestamp.0 + offset_seconds * USECS_PER_SEC;
            let (days, _) =
                crate::backend::utils::time::datetime::timestamp_parts_from_usecs(local_usecs);
            let truncated_days = truncate_timestamp_local(&field, days)?;
            Ok(Value::TimestampTz(TimestampTzADT(
                i64::from(truncated_days) * USECS_PER_DAY - offset_seconds * USECS_PER_SEC,
            )))
        }
        other => Err(ExecError::TypeMismatch {
            op: "date_trunc",
            left: field_value.clone(),
            right: other.clone(),
        }),
    }
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
            Value::Float64(25_200.0)
        );
        assert_eq!(
            eval_date_part_function(&[Value::Text("timezone_h".into()), Value::TimeTz(timetz)])
                .unwrap(),
            Value::Float64(7.0)
        );
        assert_eq!(
            eval_date_part_function(&[Value::Text("timezone_m".into()), Value::TimeTz(timetz)])
                .unwrap(),
            Value::Float64(0.0)
        );
    }
}
