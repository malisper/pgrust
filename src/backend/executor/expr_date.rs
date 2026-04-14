use super::{ExecError, Value};
use crate::backend::utils::time::datetime::{
    day_of_week_from_julian_day, day_of_year, iso_day_of_week_from_julian_day, iso_week_and_year,
    days_from_ymd, julian_day_from_postgres_date, unix_days_from_postgres_date, ymd_from_days,
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
    let field = field_value.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "date_part",
        left: field_value.clone(),
        right: Value::Text("".into()),
    })?;
    let date = match date_value {
        Value::Date(date) => *date,
        other => {
            return Err(ExecError::TypeMismatch {
                op: "date_part",
                left: field_value.clone(),
                right: other.clone(),
            });
        }
    };
    let field = field.trim().to_ascii_lowercase();

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
            | "microsec"
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

pub(crate) fn eval_date_trunc_function(values: &[Value]) -> Result<Value, ExecError> {
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
    let field = field_value.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "date_trunc",
        left: field_value.clone(),
        right: Value::Text("".into()),
    })?;
    let date = match date_value {
        Value::Date(date) => *date,
        other => {
            return Err(ExecError::TypeMismatch {
                op: "date_trunc",
                left: field_value.clone(),
                right: other.clone(),
            });
        }
    };
    if !date.is_finite() {
        return Ok(Value::Date(date));
    }
    let field = field.trim().to_ascii_lowercase();
    let (astronomical_year, _, _) = ymd_from_days(date.0);
    let display_year = extract_year_number(astronomical_year);
    let truncated_astronomical_year = match field.as_str() {
        "millennium" => {
            display_year_to_astronomical(truncation_field_start_display_year(display_year, 1000))
        }
        "century" => {
            display_year_to_astronomical(truncation_field_start_display_year(display_year, 100))
        }
        "decade" => astronomical_year.div_euclid(10) * 10,
        _ => {
            return Err(ExecError::DetailedError {
                message: format!("unit \"{field}\" not supported for type date"),
                detail: None,
                hint: None,
                sqlstate: "0A000",
            });
        }
    };
    let days = days_from_ymd(truncated_astronomical_year, 1, 1).ok_or_else(|| {
        ExecError::DetailedError {
            message: format!("unit \"{field}\" not supported for type date"),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        }
    })?;
    Ok(Value::Date(crate::include::nodes::datetime::DateADT(days)))
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
    let days =
        days_from_ymd(astronomical_year, month_u32, day_u32).ok_or_else(|| invalid_make_date(year, month, day))?;
    Ok(Value::Date(crate::include::nodes::datetime::DateADT(days)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::include::nodes::datetime::DateADT;

    #[test]
    fn date_part_handles_bc_and_iso_fields() {
        assert_eq!(
            eval_date_part_function(&[
                Value::Text("year".into()),
                Value::Date(DateADT(crate::backend::utils::time::datetime::days_from_ymd(
                    -2019, 8, 11,
                )
                .unwrap())),
            ])
            .unwrap(),
            Value::Float64(-2020.0)
        );
        assert_eq!(
            eval_date_part_function(&[
                Value::Text("dow".into()),
                Value::Date(DateADT(crate::backend::utils::time::datetime::days_from_ymd(
                    2020, 8, 16,
                )
                .unwrap())),
            ])
            .unwrap(),
            Value::Float64(0.0)
        );
        assert_eq!(
            eval_date_part_function(&[
                Value::Text("isodow".into()),
                Value::Date(DateADT(crate::backend::utils::time::datetime::days_from_ymd(
                    2020, 8, 16,
                )
                .unwrap())),
            ])
            .unwrap(),
            Value::Float64(7.0)
        );
    }

    #[test]
    fn date_trunc_handles_bc_boundaries() {
        assert_eq!(
            eval_date_trunc_function(&[
                Value::Text("century".into()),
                Value::Date(DateADT(days_from_ymd(-54, 8, 10).unwrap())),
            ])
            .unwrap(),
            Value::Date(DateADT(days_from_ymd(-99, 1, 1).unwrap()))
        );
        assert_eq!(
            eval_date_trunc_function(&[
                Value::Text("decade".into()),
                Value::Date(DateADT(days_from_ymd(4, 12, 25).unwrap())),
            ])
            .unwrap(),
            Value::Date(DateADT(days_from_ymd(0, 1, 1).unwrap()))
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
}
