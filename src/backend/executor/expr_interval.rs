use super::ExecError;
use super::expr_casts::canonicalize_interval_text;
use super::node_types::*;
use crate::backend::utils::time::datetime::{
    DateTimeParseError, TimeZoneSpec, named_timezone_offset_seconds, parse_timezone_spec,
};
use crate::include::nodes::datetime::{
    TimeADT, TimeTzADT, TimestampADT, TimestampTzADT, USECS_PER_DAY, USECS_PER_HOUR,
    USECS_PER_MINUTE, USECS_PER_SEC,
};
use crate::pgrust::compact_string::CompactString;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IntervalParts {
    months: i32,
    days: i32,
    micros: i64,
}

impl IntervalParts {
    fn total_usecs_30_day_months(self) -> Option<i128> {
        let month_days = i128::from(self.months).checked_mul(30)?;
        let days = month_days.checked_add(i128::from(self.days))?;
        days.checked_mul(i128::from(USECS_PER_DAY))?
            .checked_add(i128::from(self.micros))
    }
}

fn invalid_interval(text: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("invalid input syntax for type interval: \"{text}\""),
        detail: None,
        hint: None,
        sqlstate: "22P02",
    }
}

fn interval_out_of_range() -> ExecError {
    ExecError::DetailedError {
        message: "interval out of range".into(),
        detail: None,
        hint: None,
        sqlstate: "22015",
    }
}

fn unit_suffix(value: i64, singular: &str, plural: &str) -> String {
    let label = if value.abs() == 1 { singular } else { plural };
    format!("{value} {label}")
}

fn render_interval_parts(parts: IntervalParts) -> String {
    let mut months = i64::from(parts.months);
    let mut days = i64::from(parts.days);
    let mut micros = parts.micros;
    let all_negative =
        (months <= 0 && days <= 0 && micros <= 0) && (months != 0 || days != 0 || micros != 0);
    if all_negative {
        months = -months;
        days = -days;
        micros = -micros;
    }

    let mut out = Vec::new();
    let years = months / 12;
    let rem_months = months % 12;
    if years != 0 {
        out.push(unit_suffix(years, "year", "years"));
    }
    if rem_months != 0 {
        out.push(unit_suffix(rem_months, "mon", "mons"));
    }
    if days != 0 {
        out.push(unit_suffix(days, "day", "days"));
    }

    let total_seconds = micros / USECS_PER_SEC;
    let subsec = micros % USECS_PER_SEC;
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;
    if hours != 0 {
        out.push(unit_suffix(hours, "hour", "hours"));
    }
    if minutes != 0 {
        out.push(unit_suffix(minutes, "min", "mins"));
    }
    if seconds != 0 || subsec != 0 || out.is_empty() {
        let seconds_text = if subsec == 0 {
            seconds.to_string()
        } else {
            let mut rendered = format!("{}.{:06}", seconds, subsec.abs());
            while rendered.ends_with('0') {
                rendered.pop();
            }
            rendered
        };
        let label = if seconds_text == "1" { "sec" } else { "secs" };
        out.push(format!("{seconds_text} {label}"));
    }

    let mut rendered = format!("@ {}", out.join(" "));
    if all_negative && rendered != "@ 0 secs" {
        rendered.push_str(" ago");
    }
    rendered
}

fn parse_interval_parts_text(text: &str) -> Result<IntervalParts, ExecError> {
    let canonical = canonicalize_interval_text(text)?;
    let mut rest = canonical.trim();
    let mut negative = false;
    if let Some(stripped) = rest.strip_prefix('@') {
        rest = stripped.trim();
    }
    if let Some(stripped) = rest.strip_suffix("ago") {
        negative = true;
        rest = stripped.trim();
    }
    if rest.is_empty() {
        return Err(invalid_interval(text));
    }

    let tokens = rest.split_whitespace().collect::<Vec<_>>();
    if tokens.len() % 2 != 0 {
        return Err(invalid_interval(text));
    }

    let mut months = 0i32;
    let mut days = 0i32;
    let mut micros = 0i64;
    for pair in tokens.chunks(2) {
        match pair[1] {
            "year" | "years" => {
                let value = pair[0].parse::<i64>().map_err(|_| invalid_interval(text))?;
                months = months
                    .checked_add(
                        i32::try_from(value.checked_mul(12).ok_or_else(interval_out_of_range)?)
                            .map_err(|_| interval_out_of_range())?,
                    )
                    .ok_or_else(interval_out_of_range)?;
            }
            "mon" | "mons" => {
                let value = pair[0].parse::<i32>().map_err(|_| invalid_interval(text))?;
                months = months
                    .checked_add(value)
                    .ok_or_else(interval_out_of_range)?;
            }
            "day" | "days" => {
                let value = pair[0].parse::<i32>().map_err(|_| invalid_interval(text))?;
                days = days.checked_add(value).ok_or_else(interval_out_of_range)?;
            }
            "hour" | "hours" => {
                let value = pair[0].parse::<i64>().map_err(|_| invalid_interval(text))?;
                micros = micros
                    .checked_add(
                        value
                            .checked_mul(USECS_PER_HOUR)
                            .ok_or_else(interval_out_of_range)?,
                    )
                    .ok_or_else(interval_out_of_range)?;
            }
            "min" | "mins" => {
                let value = pair[0].parse::<i64>().map_err(|_| invalid_interval(text))?;
                micros = micros
                    .checked_add(
                        value
                            .checked_mul(USECS_PER_MINUTE)
                            .ok_or_else(interval_out_of_range)?,
                    )
                    .ok_or_else(interval_out_of_range)?;
            }
            "sec" | "secs" => {
                let value = pair[0].parse::<f64>().map_err(|_| invalid_interval(text))?;
                micros = micros
                    .checked_add((value * USECS_PER_SEC as f64).round() as i64)
                    .ok_or_else(interval_out_of_range)?;
            }
            _ => return Err(invalid_interval(text)),
        }
    }

    if negative {
        months = months.checked_neg().ok_or_else(interval_out_of_range)?;
        days = days.checked_neg().ok_or_else(interval_out_of_range)?;
        micros = micros.checked_neg().ok_or_else(interval_out_of_range)?;
    }

    Ok(IntervalParts {
        months,
        days,
        micros,
    })
}

fn parse_interval_value(value: &Value, op: &'static str) -> Result<IntervalParts, ExecError> {
    match value {
        Value::Text(text) => parse_interval_parts_text(text),
        other => Err(ExecError::TypeMismatch {
            op,
            left: other.clone(),
            right: Value::Text(CompactString::new("@ 0 secs")),
        }),
    }
}

fn interval_value(parts: IntervalParts) -> Value {
    Value::Text(CompactString::from_owned(render_interval_parts(parts)))
}

fn int_arg(values: &[Value], idx: usize) -> Result<i32, ExecError> {
    match values.get(idx) {
        Some(Value::Int32(value)) => Ok(*value),
        Some(Value::Null) | None => Ok(0),
        Some(other) => Err(ExecError::TypeMismatch {
            op: "make_interval",
            left: other.clone(),
            right: Value::Int32(0),
        }),
    }
}

fn secs_arg(values: &[Value]) -> Result<f64, ExecError> {
    match values.get(6) {
        Some(Value::Float64(value)) => Ok(*value),
        Some(Value::Null) | None => Ok(0.0),
        Some(other) => Err(ExecError::TypeMismatch {
            op: "make_interval",
            left: other.clone(),
            right: Value::Float64(0.0),
        }),
    }
}

pub(crate) fn eval_make_interval_function(values: &[Value]) -> Result<Value, ExecError> {
    let years = int_arg(values, 0)?;
    let months_arg = int_arg(values, 1)?;
    let weeks = int_arg(values, 2)?;
    let days_arg = int_arg(values, 3)?;
    let hours = int_arg(values, 4)?;
    let mins = int_arg(values, 5)?;
    let secs = secs_arg(values)?;

    let months = years
        .checked_mul(12)
        .and_then(|value| value.checked_add(months_arg))
        .ok_or_else(interval_out_of_range)?;
    let days = weeks
        .checked_mul(7)
        .and_then(|value| value.checked_add(days_arg))
        .ok_or_else(interval_out_of_range)?;
    let micros = i64::from(hours)
        .checked_mul(USECS_PER_HOUR)
        .and_then(|value| value.checked_add(i64::from(mins).checked_mul(USECS_PER_MINUTE)?))
        .and_then(|value| value.checked_add((secs * USECS_PER_SEC as f64).round() as i64))
        .ok_or_else(interval_out_of_range)?;

    Ok(interval_value(IntervalParts {
        months,
        days,
        micros,
    }))
}

pub(crate) fn eval_justify_hours_function(values: &[Value]) -> Result<Value, ExecError> {
    let mut parts = parse_interval_value(values.first().unwrap_or(&Value::Null), "justify_hours")?;
    let days = parts.micros.div_euclid(USECS_PER_DAY);
    parts.micros = parts.micros.rem_euclid(USECS_PER_DAY);
    parts.days = parts
        .days
        .checked_add(i32::try_from(days).map_err(|_| interval_out_of_range())?)
        .ok_or_else(interval_out_of_range)?;
    Ok(interval_value(parts))
}

pub(crate) fn eval_justify_days_function(values: &[Value]) -> Result<Value, ExecError> {
    let mut parts = parse_interval_value(values.first().unwrap_or(&Value::Null), "justify_days")?;
    let months = parts.days.div_euclid(30);
    parts.days = parts.days.rem_euclid(30);
    parts.months = parts
        .months
        .checked_add(months)
        .ok_or_else(interval_out_of_range)?;
    Ok(interval_value(parts))
}

pub(crate) fn eval_justify_interval_function(values: &[Value]) -> Result<Value, ExecError> {
    let hours = eval_justify_hours_function(values)?;
    eval_justify_days_function(&[hours])
}

pub(crate) fn eval_interval_hash_function(values: &[Value]) -> Result<Value, ExecError> {
    let parts = parse_interval_value(values.first().unwrap_or(&Value::Null), "interval_hash")?;
    let total = parts
        .total_usecs_30_day_months()
        .ok_or_else(interval_out_of_range)?;
    Ok(Value::Int32((total ^ (total >> 32)) as i32))
}

fn interval_stride_usecs(parts: IntervalParts) -> Result<i64, ExecError> {
    if parts.months != 0 {
        return Err(ExecError::DetailedError {
            message: "timestamps cannot be binned into intervals containing months or years".into(),
            detail: None,
            hint: None,
            sqlstate: "22008",
        });
    }
    let total = i128::from(parts.days)
        .checked_mul(i128::from(USECS_PER_DAY))
        .and_then(|value| value.checked_add(i128::from(parts.micros)))
        .ok_or_else(interval_out_of_range)?;
    if total <= 0 {
        return Err(ExecError::DetailedError {
            message: "stride must be greater than zero".into(),
            detail: None,
            hint: None,
            sqlstate: "22008",
        });
    }
    i64::try_from(total).map_err(|_| interval_out_of_range())
}

fn bin_timestamp(stride: i64, source: i64, origin: i64) -> Result<i64, ExecError> {
    let delta = source
        .checked_sub(origin)
        .ok_or_else(interval_out_of_range)?;
    let bins = delta.div_euclid(stride);
    origin
        .checked_add(bins.checked_mul(stride).ok_or_else(interval_out_of_range)?)
        .ok_or_else(interval_out_of_range)
}

pub(crate) fn eval_date_bin_function(values: &[Value]) -> Result<Value, ExecError> {
    let stride = interval_stride_usecs(parse_interval_value(
        values.first().unwrap_or(&Value::Null),
        "date_bin",
    )?)?;
    match (values.get(1), values.get(2)) {
        (Some(Value::Timestamp(source)), Some(Value::Timestamp(origin))) => Ok(Value::Timestamp(
            TimestampADT(bin_timestamp(stride, source.0, origin.0)?),
        )),
        (Some(Value::TimestampTz(source)), Some(Value::TimestampTz(origin))) => Ok(
            Value::TimestampTz(TimestampTzADT(bin_timestamp(stride, source.0, origin.0)?)),
        ),
        (Some(left), Some(right)) => Err(ExecError::TypeMismatch {
            op: "date_bin",
            left: left.clone(),
            right: right.clone(),
        }),
        _ => Err(ExecError::DetailedError {
            message: "date_bin requires three arguments".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        }),
    }
}

fn timezone_offset_usecs(parts: IntervalParts) -> Result<i64, ExecError> {
    if parts.months != 0 || parts.days != 0 {
        return Err(ExecError::DetailedError {
            message: "interval time zone must not include months or days".into(),
            detail: None,
            hint: None,
            sqlstate: "22023",
        });
    }
    Ok(parts.micros)
}

fn timezone_text_offset_usecs(text: &str) -> Result<i64, ExecError> {
    match parse_timezone_spec(text) {
        Ok(Some(TimeZoneSpec::FixedOffset(offset))) => Ok(i64::from(offset) * USECS_PER_SEC),
        Ok(Some(TimeZoneSpec::Named(name))) => named_timezone_offset_seconds(&name)
            .map(|offset| i64::from(offset) * USECS_PER_SEC)
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("time zone \"{}\" not recognized", name.to_ascii_lowercase()),
                detail: None,
                hint: None,
                sqlstate: "22023",
            }),
        Ok(None) => Err(ExecError::DetailedError {
            message: format!("time zone \"{}\" not recognized", text.to_ascii_lowercase()),
            detail: None,
            hint: None,
            sqlstate: "22023",
        }),
        Err(DateTimeParseError::UnknownTimeZone(zone)) => Err(ExecError::DetailedError {
            message: format!("time zone \"{zone}\" not recognized"),
            detail: None,
            hint: None,
            sqlstate: "22023",
        }),
        Err(_) => Err(ExecError::DetailedError {
            message: format!("invalid input syntax for type time zone: \"{text}\""),
            detail: None,
            hint: None,
            sqlstate: "22023",
        }),
    }
}

fn timezone_arg_offset_usecs(value: &Value) -> Result<i64, ExecError> {
    match value {
        Value::Text(text) => parse_interval_parts_text(text)
            .and_then(timezone_offset_usecs)
            .or_else(|_| timezone_text_offset_usecs(text)),
        other => timezone_offset_usecs(parse_interval_value(other, "timezone")?),
    }
}

fn normalize_time_usecs(value: i64) -> i64 {
    value.rem_euclid(USECS_PER_DAY)
}

pub(crate) fn eval_timezone_function(values: &[Value]) -> Result<Value, ExecError> {
    let offset = timezone_arg_offset_usecs(values.first().unwrap_or(&Value::Null))?;
    match values.get(1) {
        Some(Value::Timestamp(value)) => Ok(Value::TimestampTz(TimestampTzADT(
            value
                .0
                .checked_sub(offset)
                .ok_or_else(interval_out_of_range)?,
        ))),
        Some(Value::TimestampTz(value)) => Ok(Value::Timestamp(TimestampADT(
            value
                .0
                .checked_add(offset)
                .ok_or_else(interval_out_of_range)?,
        ))),
        Some(Value::Time(value)) => Ok(Value::TimeTz(TimeTzADT {
            time: TimeADT(normalize_time_usecs(value.0)),
            offset_seconds: i32::try_from((-offset).div_euclid(USECS_PER_SEC))
                .map_err(|_| interval_out_of_range())?,
        })),
        Some(Value::TimeTz(value)) => Ok(Value::TimeTz(TimeTzADT {
            time: TimeADT(normalize_time_usecs(value.time.0)),
            offset_seconds: i32::try_from((-offset).div_euclid(USECS_PER_SEC))
                .map_err(|_| interval_out_of_range())?,
        })),
        Some(other) => Err(ExecError::TypeMismatch {
            op: "timezone",
            left: values[0].clone(),
            right: other.clone(),
        }),
        None => Err(ExecError::DetailedError {
            message: "timezone requires two arguments".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn make_interval_matches_basic_postgres_rendering() {
        assert_eq!(
            eval_make_interval_function(&[
                Value::Int32(1),
                Value::Int32(-1),
                Value::Int32(5),
                Value::Int32(-7),
                Value::Int32(25),
                Value::Int32(-180),
                Value::Float64(0.0),
            ])
            .unwrap(),
            Value::Text("@ 11 mons 28 days 22 hours".into())
        );
    }

    #[test]
    fn justify_interval_promotes_hours_and_days() {
        assert_eq!(
            eval_justify_hours_function(&[Value::Text(
                "@ 6 mons 3 days 52 hours 3 mins 2 secs".into()
            )])
            .unwrap(),
            Value::Text("@ 6 mons 5 days 4 hours 3 mins 2 secs".into())
        );
        assert_eq!(
            eval_justify_days_function(&[Value::Text(
                "@ 6 mons 36 days 5 hours 4 mins 3 secs".into()
            )])
            .unwrap(),
            Value::Text("@ 7 mons 6 days 5 hours 4 mins 3 secs".into())
        );
    }

    #[test]
    fn interval_hash_treats_month_as_thirty_days() {
        assert_eq!(
            eval_interval_hash_function(&[Value::Text("@ 30 days".into())]).unwrap(),
            eval_interval_hash_function(&[Value::Text("@ 1 mon".into())]).unwrap(),
        );
    }
}
