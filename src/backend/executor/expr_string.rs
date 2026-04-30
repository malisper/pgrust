use super::ExecError;
use super::expr_bit::render_bit_text;
use super::expr_casts::{
    cast_value, numeric_input_would_overflow, parse_bytea_text, render_internal_char_text,
    render_interval_text, render_interval_text_with_config,
};
use super::expr_datetime::{render_datetime_value_text, render_datetime_value_text_with_config};
use super::expr_format::{
    format_roman, ordinal_suffix, to_char_float, to_char_float4, to_char_int, to_char_numeric,
    to_number_numeric,
};
use super::expr_ops::ensure_builtin_collation_supported;
use super::expr_range::render_range_text_with_config;
use super::node_types::Value;
use super::render_macaddr_text;
use super::render_macaddr8_text;
use super::value_io::{format_array_text, render_tid_text};
use crate::backend::executor::jsonb::render_jsonb_bytes;
use crate::backend::libpq::pqformat::format_bytea_text;
use crate::backend::parser::{ParseError, SqlType, SqlTypeKind};
use crate::backend::utils::crc32c;
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::backend::utils::record::assign_anonymous_record_descriptor;
use crate::backend::utils::time::datetime::{
    current_timezone_name, day_of_week_from_julian_day, day_of_year, days_from_ymd, format_offset,
    iso_day_of_week_from_julian_day, iso_week_and_year, julian_day_from_postgres_date,
    named_timezone_abbreviation_at_utc, timestamp_parts_from_usecs, timezone_offset_seconds_at_utc,
    ymd_from_days,
};
use crate::include::nodes::datetime::{
    DATEVAL_NOBEGIN, DATEVAL_NOEND, TIMESTAMP_NOBEGIN, TIMESTAMP_NOEND, USECS_PER_DAY,
    USECS_PER_HOUR, USECS_PER_MINUTE, USECS_PER_SEC,
};
use crate::include::nodes::datum::{IntervalValue, NumericValue};
use crate::pgrust::compact_string::CompactString;
use crate::pgrust::session::ByteaOutputFormat;
use base64::Engine as _;
use encoding_rs::{DecoderResult, EncoderResult, Encoding};
use md5::{Digest, Md5};
use num_bigint::BigInt;
use num_traits::Signed;
use sha2::{Sha224, Sha256, Sha384, Sha512};
use unicode_general_category::{GeneralCategory, get_general_category};
use unicode_normalization::{UnicodeNormalization, is_nfc, is_nfd, is_nfkc, is_nfkd};

struct SizePrettyUnit {
    name: &'static str,
    limit: u64,
    round: bool,
    unitbits: u8,
}

const SIZE_PRETTY_UNITS: [SizePrettyUnit; 6] = [
    SizePrettyUnit {
        name: "bytes",
        limit: 10 * 1024,
        round: false,
        unitbits: 0,
    },
    SizePrettyUnit {
        name: "kB",
        limit: 20 * 1024 - 1,
        round: true,
        unitbits: 10,
    },
    SizePrettyUnit {
        name: "MB",
        limit: 20 * 1024 - 1,
        round: true,
        unitbits: 20,
    },
    SizePrettyUnit {
        name: "GB",
        limit: 20 * 1024 - 1,
        round: true,
        unitbits: 30,
    },
    SizePrettyUnit {
        name: "TB",
        limit: 20 * 1024 - 1,
        round: true,
        unitbits: 40,
    },
    SizePrettyUnit {
        name: "PB",
        limit: 20 * 1024 - 1,
        round: true,
        unitbits: 50,
    },
];

const WEEKDAY_NAMES: [&str; 7] = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
];
const MONTH_NAMES: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];
const DATETIME_TO_CHAR_PATTERNS: &[&str] = &[
    "A.D.", "a.d.", "B.C.", "b.c.", "P.M.", "p.m.", "A.M.", "a.m.", "Y,YYY", "HH24", "HH12",
    "IYYY", "IDDD", "YYYY", "MONTH", "Month", "month", "DAY", "Day", "day", "SSSSS", "SSSS", "TZH",
    "tzh", "TZM", "tzm", "TZ", "tz", "FF1", "FF2", "FF3", "FF4", "FF5", "FF6", "ff1", "ff2", "ff3",
    "ff4", "ff5", "ff6", "IYY", "YYY", "MON", "Mon", "mon", "DY", "Dy", "dy", "HH", "MI", "SS",
    "MS", "US", "DDD", "YY", "CC", "MM", "WW", "DD", "IW", "IY", "ID", "RM", "rm", "AD", "ad",
    "BC", "bc", "PM", "pm", "AM", "am", "OF", "of", "Y", "I", "Q", "J", "D",
];

struct TimestampFormatParts {
    year: i32,
    display_year: i32,
    bc: bool,
    month: u32,
    day: u32,
    day_of_year: u32,
    julian_day: i32,
    dow: u32,
    isodow: u32,
    iso_display_year: i32,
    iso_week: u32,
    iso_day_of_year: i32,
    hour: i64,
    minute: i64,
    second: i64,
    seconds_since_midnight: i64,
    micros: i64,
    offset_seconds: Option<i32>,
    timezone_label: Option<String>,
}

fn titlecase_ascii(text: &str) -> String {
    let mut chars = text.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    let mut out = String::new();
    out.push(first.to_ascii_uppercase());
    out.push_str(&chars.as_str().to_ascii_lowercase());
    out
}

fn pad_right_unless_fill(text: String, width: usize, fill_mode: bool) -> String {
    if fill_mode {
        text
    } else {
        format!("{text:<width$}")
    }
}

fn format_signed_width(value: i32, width: usize, fill_mode: bool) -> String {
    if fill_mode {
        return value.to_string();
    }
    if value < 0 {
        format!("-{:0width$}", value.abs(), width = width)
    } else {
        format!("{value:0width$}")
    }
}

fn format_last_digits(value: i32, modulo: i32, width: usize, fill_mode: bool) -> String {
    let value = value.rem_euclid(modulo);
    if fill_mode {
        value.to_string()
    } else {
        format!("{value:0width$}")
    }
}

fn format_grouped_year(display_year: i32) -> String {
    let negative = display_year < 0;
    let digits = format!("{:04}", display_year.abs());
    let split = digits.len().saturating_sub(3);
    let mut out = format!("{},{}", &digits[..split], &digits[split..]);
    if negative {
        out.insert(0, '-');
    }
    out
}

fn timestamp_century(year: i32) -> i32 {
    if year > 0 {
        (year + 99) / 100
    } else {
        -((-year + 99) / 100)
    }
}

fn timestamp_to_char_parts(
    timestamp_usecs: i64,
    offset_seconds: Option<i32>,
    timezone_label: Option<String>,
) -> Option<TimestampFormatParts> {
    if timestamp_usecs == TIMESTAMP_NOBEGIN || timestamp_usecs == TIMESTAMP_NOEND {
        return None;
    }
    let (pg_days, time_usecs) = timestamp_parts_from_usecs(timestamp_usecs);
    let (year, month, day) = ymd_from_days(pg_days);
    let display_year = if year <= 0 { 1 - year } else { year };
    let julian_day = julian_day_from_postgres_date(pg_days);
    let dow = day_of_week_from_julian_day(julian_day);
    let isodow = iso_day_of_week_from_julian_day(julian_day);
    let (iso_year, iso_week) = iso_week_and_year(year, month, day);
    let iso_display_year = if iso_year <= 0 {
        1 - iso_year
    } else {
        iso_year
    };
    let jan4 = days_from_ymd(iso_year, 1, 4)?;
    let jan4_isodow = iso_day_of_week_from_julian_day(julian_day_from_postgres_date(jan4)) as i32;
    let iso_year_start = jan4 - (jan4_isodow - 1);
    let hour = time_usecs / USECS_PER_HOUR;
    let minute = (time_usecs % USECS_PER_HOUR) / USECS_PER_MINUTE;
    let second = (time_usecs % USECS_PER_MINUTE) / USECS_PER_SEC;
    Some(TimestampFormatParts {
        year,
        display_year,
        bc: year <= 0,
        month,
        day,
        day_of_year: day_of_year(year, month, day),
        julian_day,
        dow,
        isodow,
        iso_display_year,
        iso_week,
        iso_day_of_year: pg_days - iso_year_start + 1,
        hour,
        minute,
        second,
        seconds_since_midnight: time_usecs / USECS_PER_SEC,
        micros: time_usecs % USECS_PER_SEC,
        offset_seconds,
        timezone_label,
    })
}

fn match_datetime_to_char_pattern(format: &str, idx: usize) -> Option<&'static str> {
    DATETIME_TO_CHAR_PATTERNS
        .iter()
        .copied()
        .find(|pattern| format[idx..].starts_with(pattern))
}

fn datetime_ordinal_suffix(value: i128, lower: bool) -> String {
    let suffix = ordinal_suffix(value);
    if lower {
        suffix.to_ascii_lowercase()
    } else {
        suffix.to_string()
    }
}

fn render_ad_bc(parts: &TimestampFormatParts, pattern: &str) -> String {
    let dotted = pattern.contains('.');
    let lower = pattern.chars().all(|ch| !ch.is_ascii_uppercase());
    let text = if parts.bc {
        if dotted { "B.C." } else { "BC" }
    } else if dotted {
        "A.D."
    } else {
        "AD"
    };
    if lower {
        text.to_ascii_lowercase()
    } else {
        text.to_string()
    }
}

fn render_am_pm(parts: &TimestampFormatParts, pattern: &str) -> String {
    let dotted = pattern.contains('.');
    let lower = pattern.chars().all(|ch| !ch.is_ascii_uppercase());
    let text = if parts.hour < 12 {
        if dotted { "A.M." } else { "AM" }
    } else if dotted {
        "P.M."
    } else {
        "PM"
    };
    if lower {
        text.to_ascii_lowercase()
    } else {
        text.to_string()
    }
}

fn format_timezone_offset(offset: Option<i32>) -> String {
    let offset = offset.unwrap_or(0);
    let sign = if offset < 0 { '-' } else { '+' };
    let abs = offset.abs();
    let hour = abs / 3600;
    let minute = abs % 3600 / 60;
    let second = abs % 60;
    if second != 0 {
        format!("{sign}{hour:02}:{minute:02}:{second:02}")
    } else if minute != 0 {
        format!("{sign}{hour:02}:{minute:02}")
    } else {
        format!("{sign}{hour:02}")
    }
}

fn format_timezone_hour(offset: Option<i32>) -> String {
    let offset = offset.unwrap_or(0);
    let sign = if offset < 0 { '-' } else { '+' };
    format!("{sign}{:02}", offset.abs() / 3600)
}

fn format_timezone_minute(offset: Option<i32>) -> String {
    format!("{:02}", offset.unwrap_or(0).abs() % 3600 / 60)
}

fn timezone_label_for_to_char(config: &DateTimeConfig, utc_usecs: i64, offset: i32) -> String {
    let zone = current_timezone_name(config);
    named_timezone_abbreviation_at_utc(zone, utc_usecs).unwrap_or_else(|| format_offset(offset))
}

fn render_datetime_to_char_pattern(
    parts: &TimestampFormatParts,
    pattern: &str,
    fill_mode: bool,
) -> (String, Option<i128>) {
    let month_idx = parts.month.saturating_sub(1) as usize;
    let weekday = WEEKDAY_NAMES[parts.dow as usize];
    let month = MONTH_NAMES[month_idx];
    match pattern {
        "DAY" => (
            pad_right_unless_fill(weekday.to_ascii_uppercase(), 9, fill_mode),
            None,
        ),
        "Day" => (
            pad_right_unless_fill(titlecase_ascii(weekday), 9, fill_mode),
            None,
        ),
        "day" => (
            pad_right_unless_fill(weekday.to_ascii_lowercase(), 9, fill_mode),
            None,
        ),
        "DY" => (weekday[..3].to_ascii_uppercase(), None),
        "Dy" => (titlecase_ascii(&weekday[..3]), None),
        "dy" => (weekday[..3].to_ascii_lowercase(), None),
        "MONTH" => (
            pad_right_unless_fill(month.to_ascii_uppercase(), 9, fill_mode),
            None,
        ),
        "Month" => (
            pad_right_unless_fill(titlecase_ascii(month), 9, fill_mode),
            None,
        ),
        "month" => (
            pad_right_unless_fill(month.to_ascii_lowercase(), 9, fill_mode),
            None,
        ),
        "MON" => (month[..3].to_ascii_uppercase(), None),
        "Mon" => (titlecase_ascii(&month[..3]), None),
        "mon" => (month[..3].to_ascii_lowercase(), None),
        "RM" | "rm" => {
            let lower = pattern == "rm";
            let roman = format_roman(i128::from(parts.month), true, lower);
            (pad_right_unless_fill(roman, 4, fill_mode), None)
        }
        "Y,YYY" => (format_grouped_year(parts.display_year), None),
        "YYYY" => {
            let rendered = format_signed_width(parts.display_year, 4, fill_mode);
            (rendered, Some(i128::from(parts.display_year)))
        }
        "YYY" => (
            format_last_digits(parts.display_year, 1000, 3, fill_mode),
            Some(i128::from(parts.display_year.rem_euclid(1000))),
        ),
        "YY" => (
            format_last_digits(parts.display_year, 100, 2, fill_mode),
            Some(i128::from(parts.display_year.rem_euclid(100))),
        ),
        "Y" => (
            format_last_digits(parts.display_year, 10, 1, fill_mode),
            Some(i128::from(parts.display_year.rem_euclid(10))),
        ),
        "CC" => {
            let century = timestamp_century(parts.year);
            (
                format_signed_width(century, 2, fill_mode),
                Some(century.into()),
            )
        }
        "Q" => {
            let quarter = ((parts.month - 1) / 3) + 1;
            (quarter.to_string(), Some(quarter.into()))
        }
        "MM" => (
            format_signed_width(parts.month as i32, 2, fill_mode),
            Some(parts.month.into()),
        ),
        "WW" => {
            let week = ((parts.day_of_year - 1) / 7) + 1;
            (
                format_signed_width(week as i32, 2, fill_mode),
                Some(week.into()),
            )
        }
        "DDD" => (
            format_signed_width(parts.day_of_year as i32, 3, fill_mode),
            Some(parts.day_of_year.into()),
        ),
        "DD" => (
            format_signed_width(parts.day as i32, 2, fill_mode),
            Some(parts.day.into()),
        ),
        "D" => {
            let day = parts.dow + 1;
            (day.to_string(), Some(day.into()))
        }
        "J" => (
            parts.julian_day.to_string(),
            Some(i128::from(parts.julian_day)),
        ),
        "HH" | "HH12" => {
            let mut hour = parts.hour % 12;
            if hour == 0 {
                hour = 12;
            }
            (format!("{hour:02}"), Some(hour.into()))
        }
        "HH24" => (format!("{:02}", parts.hour), Some(parts.hour.into())),
        "MI" => (format!("{:02}", parts.minute), Some(parts.minute.into())),
        "SS" => (format!("{:02}", parts.second), Some(parts.second.into())),
        "SSSS" | "SSSSS" => (
            parts.seconds_since_midnight.to_string(),
            Some(parts.seconds_since_midnight.into()),
        ),
        "TZ" | "tz" => {
            let Some(label) = parts.timezone_label.as_deref() else {
                return (pattern.to_string(), None);
            };
            if pattern == "tz" {
                (label.to_ascii_lowercase(), None)
            } else {
                (label.to_string(), None)
            }
        }
        "TZH" | "tzh" => (format_timezone_hour(parts.offset_seconds), None),
        "TZM" | "tzm" => (format_timezone_minute(parts.offset_seconds), None),
        "OF" | "of" => (format_timezone_offset(parts.offset_seconds), None),
        "MS" => (format!("{:03}", parts.micros / 1_000), None),
        "US" => (format!("{:06}", parts.micros), None),
        "FF1" | "FF2" | "FF3" | "FF4" | "FF5" | "FF6" | "ff1" | "ff2" | "ff3" | "ff4" | "ff5"
        | "ff6" => {
            let digits = format!("{:06}", parts.micros);
            let width = pattern[2..3].parse::<usize>().unwrap_or(6);
            (digits[..width].to_string(), None)
        }
        "A.D." | "a.d." | "B.C." | "b.c." | "AD" | "ad" | "BC" | "bc" => {
            (render_ad_bc(parts, pattern), None)
        }
        "P.M." | "p.m." | "A.M." | "a.m." | "PM" | "pm" | "AM" | "am" => {
            (render_am_pm(parts, pattern), None)
        }
        "IYYY" => {
            let rendered = format_signed_width(parts.iso_display_year, 4, fill_mode);
            (rendered, Some(i128::from(parts.iso_display_year)))
        }
        "IYY" => (
            format_last_digits(parts.iso_display_year, 1000, 3, fill_mode),
            Some(i128::from(parts.iso_display_year.rem_euclid(1000))),
        ),
        "IY" => (
            format_last_digits(parts.iso_display_year, 100, 2, fill_mode),
            Some(i128::from(parts.iso_display_year.rem_euclid(100))),
        ),
        "I" => (
            format_last_digits(parts.iso_display_year, 10, 1, fill_mode),
            Some(i128::from(parts.iso_display_year.rem_euclid(10))),
        ),
        "IW" => (
            format_signed_width(parts.iso_week as i32, 2, fill_mode),
            Some(parts.iso_week.into()),
        ),
        "IDDD" => (
            format_signed_width(parts.iso_day_of_year, 3, fill_mode),
            Some(parts.iso_day_of_year.into()),
        ),
        "ID" => (parts.isodow.to_string(), Some(parts.isodow.into())),
        _ => (pattern.to_string(), None),
    }
}

fn to_char_timestamp_usecs(
    timestamp_usecs: i64,
    format: &str,
    offset_seconds: Option<i32>,
    timezone_label: Option<String>,
) -> String {
    let Some(parts) = timestamp_to_char_parts(timestamp_usecs, offset_seconds, timezone_label)
    else {
        return String::new();
    };
    let mut out = String::new();
    let mut idx = 0usize;
    while idx < format.len() {
        let mut fill_mode = false;
        if format[idx..].starts_with("FM") {
            fill_mode = true;
            idx += 2;
            if idx >= format.len() {
                break;
            }
        }
        if format[idx..].starts_with('"') {
            idx += 1;
            while idx < format.len() {
                let ch = format[idx..]
                    .chars()
                    .next()
                    .expect("format index points at a char");
                idx += ch.len_utf8();
                if ch == '"' {
                    break;
                }
                if ch == '\\' && idx < format.len() {
                    let escaped = format[idx..]
                        .chars()
                        .next()
                        .expect("format index points at a char");
                    out.push(escaped);
                    idx += escaped.len_utf8();
                } else {
                    out.push(ch);
                }
            }
            continue;
        }
        if format[idx..].starts_with("\\\"") {
            out.push('"');
            idx += 2;
            continue;
        }
        if let Some(pattern) = match_datetime_to_char_pattern(format, idx) {
            let (mut rendered, ordinal_value) =
                render_datetime_to_char_pattern(&parts, pattern, fill_mode);
            idx += pattern.len();
            if let Some(value) = ordinal_value {
                if format[idx..].starts_with("TH") || format[idx..].starts_with("th") {
                    rendered.push_str(&datetime_ordinal_suffix(
                        value,
                        &format[idx..idx + 2] == "th",
                    ));
                    idx += 2;
                }
            }
            out.push_str(&rendered);
            continue;
        }
        let ch = format[idx..]
            .chars()
            .next()
            .expect("format index points at a char");
        out.push(ch);
        idx += ch.len_utf8();
    }
    out
}

fn to_char_interval_value(value: IntervalValue, format: &str) -> Option<String> {
    let mut idx = 0usize;
    let mut out = String::new();
    while idx < format.len() {
        let mut fill_mode = false;
        if format[idx..].starts_with("FM") {
            fill_mode = true;
            idx += 2;
            if idx >= format.len() {
                break;
            }
        }
        if format[idx..].starts_with("RM") || format[idx..].starts_with("rm") {
            let lower = format[idx..].starts_with("rm");
            idx += 2;
            if value.months == 0 {
                continue;
            }
            let month = if value.months > 0 {
                (value.months - 1).rem_euclid(12) + 1
            } else {
                (value.months + 12).rem_euclid(12) + 1
            };
            let roman = format_roman(i128::from(month), true, lower);
            out.push_str(&pad_right_unless_fill(roman, 4, fill_mode));
            continue;
        }
        let ch = format[idx..]
            .chars()
            .next()
            .expect("format index points at a char");
        out.push(ch);
        idx += ch.len_utf8();
    }
    Some(out)
}

pub(crate) fn eval_to_char_function(
    values: &[Value],
    datetime_config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    eval_to_char_function_with_float4(values, false, datetime_config)
}

pub(super) fn eval_to_char_float4_function(values: &[Value]) -> Result<Value, ExecError> {
    eval_to_char_function_with_float4(values, true, &DateTimeConfig::default())
}

fn eval_to_char_function_with_float4(
    values: &[Value],
    float4: bool,
    datetime_config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    let Some(value) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(format) = values.get(1) else {
        return Ok(Value::Null);
    };
    let fmt = format.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "to_char",
        left: format.clone(),
        right: Value::Text("".into()),
    })?;
    let rendered = match value {
        Value::Int16(v) => to_char_int(*v as i128, fmt)?,
        Value::Int32(v) => to_char_int(*v as i128, fmt)?,
        Value::Int64(v) => to_char_int(*v as i128, fmt)?,
        Value::Numeric(v) => to_char_numeric(v, fmt)?,
        Value::Float64(v) if float4 => to_char_float4(*v, fmt)?,
        Value::Float64(v) => to_char_float(*v, fmt)?,
        Value::Date(v) if matches!(v.0, DATEVAL_NOBEGIN | DATEVAL_NOEND) => String::new(),
        Value::Date(v) => to_char_timestamp_usecs(i64::from(v.0) * USECS_PER_DAY, fmt, None, None),
        Value::Timestamp(v) => to_char_timestamp_usecs(v.0, fmt, None, None),
        Value::TimestampTz(v) if matches!(v.0, TIMESTAMP_NOBEGIN | TIMESTAMP_NOEND) => {
            String::new()
        }
        Value::TimestampTz(v) => {
            let offset = timezone_offset_seconds_at_utc(datetime_config, v.0);
            let label = timezone_label_for_to_char(datetime_config, v.0, offset);
            to_char_timestamp_usecs(
                v.0 + i64::from(offset) * USECS_PER_SEC,
                fmt,
                Some(offset),
                Some(label),
            )
        }
        Value::Interval(v) if !v.is_finite() => String::new(),
        Value::Interval(v) => {
            to_char_interval_value(*v, fmt).ok_or_else(|| ExecError::TypeMismatch {
                op: "to_char",
                left: value.clone(),
                right: Value::Text("".into()),
            })?
        }
        _ => {
            return Err(ExecError::TypeMismatch {
                op: "to_char",
                left: value.clone(),
                right: Value::Text("".into()),
            });
        }
    };
    Ok(Value::Text(rendered.into()))
}

pub(super) fn eval_to_number_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(text_value) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(format_value) = values.get(1) else {
        return Ok(Value::Null);
    };
    if matches!(text_value, Value::Null) || matches!(format_value, Value::Null) {
        return Ok(Value::Null);
    }
    let text = text_value
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "to_number",
            left: text_value.clone(),
            right: format_value.clone(),
        })?;
    let format = format_value
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "to_number",
            left: text_value.clone(),
            right: format_value.clone(),
        })?;
    Ok(Value::Numeric(to_number_numeric(text, format)?))
}

pub(super) fn eval_pg_size_pretty_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let rendered = match value {
        Value::Int64(size) => format_size_pretty_int64(*size),
        Value::Numeric(size) => format_size_pretty_numeric(size)?,
        other => {
            return Err(ExecError::TypeMismatch {
                op: "pg_size_pretty",
                left: other.clone(),
                right: Value::Null,
            });
        }
    };
    Ok(Value::Text(rendered.into()))
}

pub(super) fn eval_pg_size_bytes_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let input = value.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "pg_size_bytes",
        left: value.clone(),
        right: Value::Text("".into()),
    })?;
    let parsed = parse_size_bytes_numeric(input)?;
    cast_value(Value::Numeric(parsed), SqlType::new(SqlTypeKind::Int8))
}

fn format_size_pretty_int64(mut size: i64) -> String {
    for (idx, unit) in SIZE_PRETTY_UNITS.iter().enumerate() {
        let abs_size = i128::from(size).unsigned_abs();
        let next = SIZE_PRETTY_UNITS.get(idx + 1);
        if next.is_none() || abs_size < u128::from(unit.limit) {
            if unit.round {
                size = half_rounded_i64(size);
            }
            return format!("{size} {}", unit.name);
        }
        let next = next.expect("checked above");
        let shift_by = next.unitbits - unit.unitbits - u8::from(next.round) + u8::from(unit.round);
        size /= 1_i64 << shift_by;
    }
    unreachable!("size units are non-empty")
}

fn format_size_pretty_numeric(size: &NumericValue) -> Result<String, ExecError> {
    match size {
        NumericValue::Finite { .. } => {
            let mut current = size.clone();
            for (idx, unit) in SIZE_PRETTY_UNITS.iter().enumerate() {
                let next = SIZE_PRETTY_UNITS.get(idx + 1);
                if next.is_none() || numeric_abs_less_than_limit(&current, unit.limit) {
                    if unit.round {
                        current = half_rounded_numeric(&current);
                    }
                    return Ok(format!("{} {}", current.render(), unit.name));
                }
                let next = next.expect("checked above");
                let shift_by =
                    next.unitbits - unit.unitbits - u8::from(next.round) + u8::from(unit.round);
                current = trunc_divide_numeric_by_pow2(&current, shift_by);
            }
            unreachable!("size units are non-empty")
        }
        NumericValue::PosInf | NumericValue::NegInf | NumericValue::NaN => {
            Ok(format!("{} bytes", size.render()))
        }
    }
}

fn half_rounded_i64(size: i64) -> i64 {
    if size >= 0 {
        (size + 1) / 2
    } else {
        (size - 1) / 2
    }
}

fn numeric_abs_less_than_limit(value: &NumericValue, limit: u64) -> bool {
    let NumericValue::Finite { coeff, scale, .. } = value else {
        return false;
    };
    coeff.abs() < BigInt::from(limit) * pow10(*scale)
}

fn half_rounded_numeric(value: &NumericValue) -> NumericValue {
    let NumericValue::Finite { coeff, scale, .. } = value else {
        return value.clone();
    };
    if *scale != 0 {
        return value.clone();
    }
    let adjusted = if coeff.sign() == num_bigint::Sign::Minus {
        coeff - 1
    } else {
        coeff + 1
    };
    NumericValue::finite(adjusted / 2, 0).normalize()
}

fn trunc_divide_numeric_by_pow2(value: &NumericValue, bits: u8) -> NumericValue {
    let NumericValue::Finite { coeff, scale, .. } = value else {
        return value.clone();
    };
    let divisor = BigInt::from(1_u64 << bits) * pow10(*scale);
    NumericValue::finite(coeff / divisor, 0).normalize()
}

fn parse_size_bytes_numeric(input: &str) -> Result<NumericValue, ExecError> {
    let (number_text, unit_text) = split_size_bytes_input(input)?;
    if numeric_input_would_overflow(number_text) {
        return Err(size_numeric_overflow_error());
    }
    let mut parsed = match cast_value(
        Value::Text(number_text.into()),
        SqlType::new(SqlTypeKind::Numeric),
    )? {
        Value::Numeric(numeric) => numeric,
        other => {
            return Err(ExecError::TypeMismatch {
                op: "pg_size_bytes",
                left: other,
                right: Value::Null,
            });
        }
    };

    if let Some(unit) = unit_text {
        let multiplier_bits =
            match_size_unit(unit).ok_or_else(|| invalid_size_unit_error(input, unit))?;
        if multiplier_bits > 0 {
            let multiplier = NumericValue::from_i64(1_i64 << multiplier_bits);
            parsed = parsed.mul(&multiplier);
        }
    }

    Ok(parsed)
}

fn split_size_bytes_input(input: &str) -> Result<(&str, Option<&str>), ExecError> {
    let bytes = input.as_bytes();
    let mut idx = 0usize;
    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
        idx += 1;
    }
    let start = idx;

    if idx < bytes.len() && matches!(bytes[idx], b'+' | b'-') {
        idx += 1;
    }

    let mut have_digits = false;
    while idx < bytes.len() && bytes[idx].is_ascii_digit() {
        have_digits = true;
        idx += 1;
    }

    if idx < bytes.len() && bytes[idx] == b'.' {
        idx += 1;
        while idx < bytes.len() && bytes[idx].is_ascii_digit() {
            have_digits = true;
            idx += 1;
        }
    }

    if !have_digits {
        return Err(invalid_size_error(input));
    }

    if idx < bytes.len() && matches!(bytes[idx], b'e' | b'E') {
        let mut exp = idx + 1;
        if exp < bytes.len() && matches!(bytes[exp], b'+' | b'-') {
            exp += 1;
        }
        let exp_start = exp;
        while exp < bytes.len() && bytes[exp].is_ascii_digit() {
            exp += 1;
        }
        if exp > exp_start {
            idx = exp;
        }
    }

    let number = &input[start..idx];
    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
        idx += 1;
    }
    if idx >= bytes.len() {
        return Ok((number, None));
    }
    Ok((
        number,
        Some(input[idx..].trim_end_matches(|ch: char| ch.is_ascii_whitespace())),
    ))
}

fn match_size_unit(unit: &str) -> Option<u8> {
    for candidate in &SIZE_PRETTY_UNITS {
        if candidate.name.eq_ignore_ascii_case(unit) {
            return Some(candidate.unitbits);
        }
    }
    if unit.eq_ignore_ascii_case("B") {
        return Some(0);
    }
    None
}

fn invalid_size_error(input: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("invalid size: \"{input}\""),
        detail: None,
        hint: None,
        sqlstate: "22023",
    }
}

fn invalid_size_unit_error(input: &str, unit: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("invalid size: \"{input}\""),
        detail: Some(format!("Invalid size unit: \"{unit}\".")),
        hint: Some(
            "Valid units are \"bytes\", \"B\", \"kB\", \"MB\", \"GB\", \"TB\", and \"PB\".".into(),
        ),
        sqlstate: "22023",
    }
}

fn size_numeric_overflow_error() -> ExecError {
    ExecError::DetailedError {
        message: "value overflows numeric format".into(),
        detail: None,
        hint: None,
        sqlstate: "22003",
    }
}

fn pow10(exp: u32) -> BigInt {
    let mut out = BigInt::from(1u8);
    for _ in 0..exp {
        out *= 10u8;
    }
    out
}

fn render_integer_base(op: &'static str, value: &Value, base: u32) -> Result<Value, ExecError> {
    let rendered = match value {
        Value::Int32(v) => match base {
            2 => format!("{:b}", *v as u32),
            8 => format!("{:o}", *v as u32),
            16 => format!("{:x}", *v as u32),
            _ => unreachable!("unsupported base"),
        },
        Value::Int64(v) => match base {
            2 => format!("{:b}", *v as u64),
            8 => format!("{:o}", *v as u64),
            16 => format!("{:x}", *v as u64),
            _ => unreachable!("unsupported base"),
        },
        other => {
            return Err(ExecError::TypeMismatch {
                op,
                left: other.clone(),
                right: Value::Null,
            });
        }
    };
    Ok(Value::Text(rendered.into()))
}

pub(super) fn eval_to_bin_function(values: &[Value]) -> Result<Value, ExecError> {
    values.first().map_or(Ok(Value::Null), |value| {
        render_integer_base("to_bin", value, 2)
    })
}

pub(super) fn eval_to_oct_function(values: &[Value]) -> Result<Value, ExecError> {
    values.first().map_or(Ok(Value::Null), |value| {
        render_integer_base("to_oct", value, 8)
    })
}

pub(super) fn eval_to_hex_function(values: &[Value]) -> Result<Value, ExecError> {
    values.first().map_or(Ok(Value::Null), |value| {
        render_integer_base("to_hex", value, 16)
    })
}

fn value_output_text_with_config(
    value: &Value,
    datetime_config: &DateTimeConfig,
) -> Result<String, ExecError> {
    Ok(match value {
        Value::Null => String::new(),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Xid8(v) => v.to_string(),
        Value::PgLsn(v) => crate::backend::executor::render_pg_lsn_text(*v),
        Value::Money(v) => crate::backend::executor::money_format_text(*v),
        Value::Float64(v) => v.to_string(),
        Value::Numeric(v) => v.render(),
        Value::Interval(v) => render_interval_text_with_config(*v, datetime_config),
        Value::Uuid(v) => crate::backend::executor::value_io::render_uuid_text(v),
        Value::Tid(v) => render_tid_text(v),
        Value::Bool(v) => {
            if *v {
                "t".into()
            } else {
                "f".into()
            }
        }
        Value::Text(_) | Value::TextRef(_, _) | Value::JsonPath(_) | Value::Xml(_) => {
            value.as_text().unwrap().into()
        }
        Value::Json(v) => v.as_str().into(),
        Value::Jsonb(bytes) => render_jsonb_bytes(bytes)?,
        Value::Bit(bits) => render_bit_text(bits),
        Value::Bytea(bytes) => format_bytea_text(bytes, ByteaOutputFormat::Hex),
        Value::Inet(v) => v.render_inet(),
        Value::Cidr(v) => v.render_cidr(),
        Value::MacAddr(v) => render_macaddr_text(v),
        Value::MacAddr8(v) => render_macaddr8_text(v),
        Value::Point(_)
        | Value::Lseg(_)
        | Value::Path(_)
        | Value::Line(_)
        | Value::Box(_)
        | Value::Polygon(_)
        | Value::Circle(_) => {
            crate::backend::executor::render_geometry_text(value, Default::default())
                .unwrap_or_default()
        }
        Value::Range(_) => {
            render_range_text_with_config(value, datetime_config).unwrap_or_default()
        }
        Value::Multirange(_) => {
            crate::backend::executor::render_multirange_text_with_config(value, datetime_config)
                .unwrap_or_default()
        }
        Value::InternalChar(byte) => render_internal_char_text(*byte),
        Value::EnumOid(v) => v.to_string(),
        Value::Date(_)
        | Value::Time(_)
        | Value::TimeTz(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_) => render_datetime_value_text_with_config(value, datetime_config)
            .expect("datetime values render"),
        Value::TsVector(vector) => crate::backend::executor::render_tsvector_text(vector),
        Value::TsQuery(query) => crate::backend::executor::render_tsquery_text(query),
        Value::Array(values) => format_array_text(values),
        Value::PgArray(array) => crate::backend::executor::value_io::format_array_value_text(array),
        Value::Record(record) => {
            crate::backend::executor::value_io::format_record_text_with_config(
                record,
                datetime_config,
            )
        }
    })
}

fn value_output_text(value: &Value) -> Result<String, ExecError> {
    value_output_text_with_config(value, &DateTimeConfig::default())
}

fn quote_identifier(identifier: &str) -> String {
    if !identifier.is_empty()
        && !matches!(identifier, "user")
        && identifier.chars().enumerate().all(|(idx, ch)| {
            if idx == 0 {
                ch == '_' || ch.is_ascii_lowercase()
            } else {
                ch == '_' || ch.is_ascii_lowercase() || ch.is_ascii_digit()
            }
        })
    {
        return identifier.into();
    }
    let escaped = identifier.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn quote_literal_text(text: &str) -> String {
    let escaped = text.replace('\'', "''");
    if text.contains('\\') {
        let escaped = escaped.replace('\\', "\\\\");
        format!("E'{escaped}'")
    } else {
        format!("'{escaped}'")
    }
}

fn pad_formatted(mut value: String, width: i32, left_align: bool) -> String {
    let width = width.unsigned_abs() as usize;
    let len = value.chars().count();
    if width <= len {
        return value;
    }
    let padding = " ".repeat(width - len);
    if left_align || width == 0 {
        value.push_str(&padding);
        value
    } else {
        format!("{padding}{value}")
    }
}

fn format_arg_text(
    kind: char,
    value: &Value,
    datetime_config: &DateTimeConfig,
) -> Result<String, ExecError> {
    match kind {
        's' => {
            if matches!(value, Value::Null) {
                Ok(String::new())
            } else {
                value_output_text_with_config(value, datetime_config)
            }
        }
        'I' => {
            if matches!(value, Value::Null) {
                Err(ExecError::RaiseException(
                    "null values cannot be formatted as an SQL identifier".into(),
                ))
            } else {
                Ok(quote_identifier(&value_output_text_with_config(
                    value,
                    datetime_config,
                )?))
            }
        }
        'L' => {
            if matches!(value, Value::Null) {
                Ok("NULL".into())
            } else {
                Ok(quote_literal_text(&value_output_text_with_config(
                    value,
                    datetime_config,
                )?))
            }
        }
        other => Err(ExecError::RaiseException(format!(
            "unrecognized format() type specifier \"{other}\""
        ))),
    }
}

fn format_width_arg(values: &[Value], index: usize) -> Result<i32, ExecError> {
    let Some(value) = values.get(index) else {
        return Err(ExecError::RaiseException(
            "too few arguments for format()".into(),
        ));
    };
    Ok(match value {
        Value::Null => 0,
        Value::Int16(v) => *v as i32,
        Value::Int32(v) => *v,
        Value::Int64(v) => *v as i32,
        other => {
            let casted = cast_value(other.clone(), SqlType::new(SqlTypeKind::Int4))?;
            match casted {
                Value::Int32(v) => v,
                _ => 0,
            }
        }
    })
}

fn variadic_string_args(
    values: &[Value],
    func_variadic: bool,
    fixed_prefix: usize,
    null_as_empty: bool,
    op: &'static str,
) -> Result<Option<Vec<Value>>, ExecError> {
    fn flatten_variadic_items(value: &Value, out: &mut Vec<Value>) {
        match value {
            Value::Array(items) => {
                for item in items {
                    flatten_variadic_items(item, out);
                }
            }
            Value::PgArray(array) => {
                for item in array.to_nested_values() {
                    flatten_variadic_items(&item, out);
                }
            }
            other => out.push(other.clone()),
        }
    }

    if !func_variadic {
        return Ok(Some(values.to_vec()));
    }
    let Some(variadic_value) = values.get(fixed_prefix) else {
        return Ok(Some(values.to_vec()));
    };
    match variadic_value {
        Value::Null if null_as_empty => Ok(Some(values[..fixed_prefix].to_vec())),
        Value::Null => Ok(None),
        Value::Array(items) => {
            let mut out = values[..fixed_prefix].to_vec();
            for item in items {
                flatten_variadic_items(item, &mut out);
            }
            Ok(Some(out))
        }
        Value::PgArray(array) => {
            let mut out = values[..fixed_prefix].to_vec();
            for item in array.to_nested_values() {
                flatten_variadic_items(&item, &mut out);
            }
            Ok(Some(out))
        }
        _ => {
            let _ = op;
            Err(ExecError::RaiseException(
                "VARIADIC argument must be an array".into(),
            ))
        }
    }
}

pub(super) fn eval_concat_function(
    values: &[Value],
    func_variadic: bool,
    datetime_config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    let Some(values) = variadic_string_args(values, func_variadic, 0, false, "concat")? else {
        return Ok(Value::Null);
    };
    let mut out = String::new();
    for value in &values {
        if matches!(value, Value::Null) {
            continue;
        }
        out.push_str(&value_output_text_with_config(value, datetime_config)?);
    }
    Ok(Value::Text(CompactString::from_owned(out)))
}

pub(super) fn eval_concat_ws_function(
    values: &[Value],
    func_variadic: bool,
    datetime_config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    let Some(values) = variadic_string_args(values, func_variadic, 1, false, "concat_ws")? else {
        return Ok(Value::Null);
    };
    let Some(separator_value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(separator_value, Value::Null) {
        return Ok(Value::Null);
    }
    let separator = separator_value
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "concat_ws",
            left: separator_value.clone(),
            right: Value::Text("".into()),
        })?;
    let mut out = String::new();
    let mut first = true;
    for value in values.iter().skip(1) {
        if matches!(value, Value::Null) {
            continue;
        }
        if !first {
            out.push_str(separator);
        }
        first = false;
        out.push_str(&value_output_text_with_config(value, datetime_config)?);
    }
    Ok(Value::Text(CompactString::from_owned(out)))
}

pub(super) fn eval_format_function(
    values: &[Value],
    func_variadic: bool,
    datetime_config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    let Some(values) = variadic_string_args(values, func_variadic, 1, true, "format")? else {
        return Ok(Value::Null);
    };
    let Some(format_value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(format_value, Value::Null) {
        return Ok(Value::Null);
    }
    let format = format_value
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "format",
            left: format_value.clone(),
            right: Value::Text("".into()),
        })?;
    let mut out = String::new();
    let chars: Vec<char> = format.chars().collect();
    let mut idx = 0usize;
    let mut next_arg = 1usize;
    while idx < chars.len() {
        if chars[idx] != '%' {
            out.push(chars[idx]);
            idx += 1;
            continue;
        }
        idx += 1;
        if idx >= chars.len() {
            return Err(ExecError::RaiseException(
                "unterminated format() type specifier".into(),
            ));
        }
        if chars[idx] == '%' {
            out.push('%');
            idx += 1;
            continue;
        }

        let mut explicit_arg = None;
        let mut lookahead = idx;
        let mut digits = String::new();
        while lookahead < chars.len() && chars[lookahead].is_ascii_digit() {
            digits.push(chars[lookahead]);
            lookahead += 1;
        }
        if lookahead < chars.len() && chars[lookahead] == '$' {
            let parsed = digits.parse::<usize>().unwrap_or(0);
            if parsed == 0 {
                return Err(ExecError::RaiseException(
                    "format specifies argument 0, but arguments are numbered from 1".into(),
                ));
            }
            explicit_arg = Some(parsed);
            idx = lookahead + 1;
        }

        let mut left_align = false;
        if idx < chars.len() && chars[idx] == '-' {
            left_align = true;
            idx += 1;
        }

        let mut width = None;
        if idx < chars.len() && chars[idx] == '*' {
            idx += 1;
            let mut width_arg = next_arg;
            let mut width_digits = String::new();
            while idx < chars.len() && chars[idx].is_ascii_digit() {
                width_digits.push(chars[idx]);
                idx += 1;
            }
            if idx < chars.len() && chars[idx] == '$' {
                let parsed = width_digits.parse::<usize>().unwrap_or(0);
                if parsed == 0 {
                    return Err(ExecError::RaiseException(
                        "format specifies argument 0, but arguments are numbered from 1".into(),
                    ));
                }
                width_arg = parsed;
                idx += 1;
                next_arg = next_arg.max(width_arg + 1);
            } else if !width_digits.is_empty() {
                return Err(ExecError::RaiseException(
                    "unterminated format() type specifier".into(),
                ));
            } else {
                next_arg += 1;
            }
            let width_value = format_width_arg(&values[1..], width_arg - 1)?;
            if width_value < 0 {
                left_align = true;
            }
            width = Some(width_value);
        } else {
            let mut width_digits = String::new();
            while idx < chars.len() && chars[idx].is_ascii_digit() {
                width_digits.push(chars[idx]);
                idx += 1;
            }
            if !width_digits.is_empty() {
                width = Some(width_digits.parse::<i32>().unwrap_or(0));
            }
        }

        if idx >= chars.len() {
            return Err(ExecError::RaiseException(
                "unterminated format() type specifier".into(),
            ));
        }
        let kind = chars[idx];
        idx += 1;
        let arg_index = explicit_arg.unwrap_or_else(|| {
            let current = next_arg;
            next_arg += 1;
            current
        });
        if arg_index == 0 {
            return Err(ExecError::RaiseException(
                "format specifies argument 0, but arguments are numbered from 1".into(),
            ));
        }
        let Some(value) = values.get(arg_index) else {
            return Err(ExecError::RaiseException(
                "too few arguments for format()".into(),
            ));
        };
        let rendered = format_arg_text(kind, value, datetime_config)?;
        out.push_str(&pad_formatted(rendered, width.unwrap_or(0), left_align));
    }
    Ok(Value::Text(CompactString::from_owned(out)))
}

pub(super) fn eval_left_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(text_value) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(count_value) = values.get(1) else {
        return Ok(Value::Null);
    };
    if matches!(text_value, Value::Null) || matches!(count_value, Value::Null) {
        return Ok(Value::Null);
    }
    let text = text_value
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "left",
            left: text_value.clone(),
            right: count_value.clone(),
        })?;
    let count = match count_value {
        Value::Int32(v) => *v,
        other => {
            return Err(ExecError::TypeMismatch {
                op: "left",
                left: text_value.clone(),
                right: other.clone(),
            });
        }
    };
    let chars: Vec<char> = text.chars().collect();
    let take = if count >= 0 {
        usize::min(count as usize, chars.len())
    } else {
        chars.len().saturating_sub(count.unsigned_abs() as usize)
    };
    Ok(Value::Text(CompactString::from_owned(
        chars[..take].iter().collect(),
    )))
}

pub(super) fn eval_right_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(text_value) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(count_value) = values.get(1) else {
        return Ok(Value::Null);
    };
    if matches!(text_value, Value::Null) || matches!(count_value, Value::Null) {
        return Ok(Value::Null);
    }
    let text = text_value
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "right",
            left: text_value.clone(),
            right: count_value.clone(),
        })?;
    let count = match count_value {
        Value::Int32(v) => *v,
        other => {
            return Err(ExecError::TypeMismatch {
                op: "right",
                left: text_value.clone(),
                right: other.clone(),
            });
        }
    };
    let chars: Vec<char> = text.chars().collect();
    let start = if count >= 0 {
        chars.len().saturating_sub(count as usize)
    } else {
        usize::min(count.unsigned_abs() as usize, chars.len())
    };
    Ok(Value::Text(CompactString::from_owned(
        chars[start..].iter().collect(),
    )))
}

pub(super) fn eval_length_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    if let Value::TsVector(vector) = value {
        return Ok(Value::Int32(vector.lexemes.len() as i32));
    }
    if let Value::Bytea(bytes) = value {
        return Ok(Value::Int32(bytes.len() as i32));
    }
    let text = value.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "length",
        left: value.clone(),
        right: Value::Null,
    })?;
    Ok(Value::Int32(text.chars().count() as i32))
}

pub(super) fn eval_bit_length_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    if let Value::Bytea(bytes) = value {
        return Ok(Value::Int32(bytes.len().saturating_mul(8) as i32));
    }
    let text = value.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "bit_length",
        left: value.clone(),
        right: Value::Null,
    })?;
    Ok(Value::Int32(text.as_bytes().len().saturating_mul(8) as i32))
}

pub(super) fn eval_octet_length_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    if let Value::Bytea(bytes) = value {
        return Ok(Value::Int32(bytes.len() as i32));
    }
    if let Value::Bit(bits) = value {
        return Ok(Value::Int32(((bits.bit_len.max(0) + 7) / 8) as i32));
    }
    let text = value.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "octet_length",
        left: value.clone(),
        right: Value::Null,
    })?;
    Ok(Value::Int32(text.len() as i32))
}

pub(super) fn eval_repeat_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(text_value) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(count_value) = values.get(1) else {
        return Ok(Value::Null);
    };
    if matches!(text_value, Value::Null) || matches!(count_value, Value::Null) {
        return Ok(Value::Null);
    }
    let text = text_value
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "repeat",
            left: text_value.clone(),
            right: count_value.clone(),
        })?;
    let count = match count_value {
        Value::Int32(v) => *v,
        other => {
            return Err(ExecError::TypeMismatch {
                op: "repeat",
                left: text_value.clone(),
                right: other.clone(),
            });
        }
    };
    if count <= 0 {
        return Ok(Value::Text(CompactString::new("")));
    }
    let count = count as usize;
    let len = text
        .len()
        .checked_mul(count)
        .filter(|len| *len <= i32::MAX as usize)
        .ok_or(ExecError::RequestedLengthTooLarge)?;
    let mut out = String::with_capacity(len);
    for _ in 0..count {
        out.push_str(text);
    }
    Ok(Value::Text(CompactString::from_owned(out)))
}

pub(super) fn eval_lower_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(text_value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(text_value, Value::Null) {
        return Ok(Value::Null);
    }
    let text = text_value
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "lower",
            left: text_value.clone(),
            right: Value::Text("".into()),
        })?;
    Ok(Value::Text(CompactString::from_owned(text.to_lowercase())))
}

pub(super) fn eval_upper_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(text_value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(text_value, Value::Null) {
        return Ok(Value::Null);
    }
    let text = text_value
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "upper",
            left: text_value.clone(),
            right: Value::Text("".into()),
        })?;
    Ok(Value::Text(CompactString::from_owned(text.to_uppercase())))
}

pub(super) fn eval_text_starts_with_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [left, right] => {
            let left = left.as_text().ok_or_else(|| ExecError::TypeMismatch {
                op: "starts_with",
                left: left.clone(),
                right: Value::Text("".into()),
            })?;
            let right = right.as_text().ok_or_else(|| ExecError::TypeMismatch {
                op: "starts_with",
                left: right.clone(),
                right: Value::Text("".into()),
            })?;
            Ok(Value::Bool(left.starts_with(right)))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "starts_with(text, text)",
            actual: format!("starts_with({} args)", values.len()),
        })),
    }
}

pub(super) fn eval_unistr_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(text_value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(text_value, Value::Null) {
        return Ok(Value::Null);
    }
    let text = expect_text_arg("unistr", text_value, &Value::Null)?;
    Ok(Value::Text(CompactString::from_owned(decode_unistr_text(
        text,
    )?)))
}

pub(super) fn eval_unicode_version_function(values: &[Value]) -> Result<Value, ExecError> {
    if !values.is_empty() {
        return Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "unicode_version()",
            actual: format!("unicode_version({} args)", values.len()),
        }));
    }
    let (major, minor, _) = unicode_normalization::UNICODE_VERSION;
    Ok(Value::Text(format!("{major}.{minor}").into()))
}

pub(super) fn eval_unicode_assigned_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [value] => {
            let text = expect_text_arg("unicode_assigned", value, &Value::Text("".into()))?;
            Ok(Value::Bool(text.chars().all(|ch| {
                get_general_category(ch) != GeneralCategory::Unassigned
            })))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "unicode_assigned(text)",
            actual: format!("unicode_assigned({} args)", values.len()),
        })),
    }
}

pub(super) fn eval_unicode_normalize_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [value, form] => {
            let text = expect_text_arg("normalize", value, &Value::Text("".into()))?;
            let form = expect_text_arg("normalize", form, &Value::Text("NFC".into()))?;
            Ok(Value::Text(CompactString::from_owned(
                unicode_normalize_text(text, parse_unicode_normal_form(form)?),
            )))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "normalize(text, text)",
            actual: format!("normalize({} args)", values.len()),
        })),
    }
}

pub(super) fn eval_unicode_is_normalized_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [value, form] => {
            let text = expect_text_arg("is_normalized", value, &Value::Text("".into()))?;
            let form = expect_text_arg("is_normalized", form, &Value::Text("NFC".into()))?;
            Ok(Value::Bool(unicode_text_is_normalized(
                text,
                parse_unicode_normal_form(form)?,
            )))
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "is_normalized(text, text)",
            actual: format!("is_normalized({} args)", values.len()),
        })),
    }
}

#[derive(Clone, Copy)]
enum UnicodeNormalForm {
    Nfc,
    Nfd,
    Nfkc,
    Nfkd,
}

fn parse_unicode_normal_form(form: &str) -> Result<UnicodeNormalForm, ExecError> {
    if form.eq_ignore_ascii_case("NFC") {
        Ok(UnicodeNormalForm::Nfc)
    } else if form.eq_ignore_ascii_case("NFD") {
        Ok(UnicodeNormalForm::Nfd)
    } else if form.eq_ignore_ascii_case("NFKC") {
        Ok(UnicodeNormalForm::Nfkc)
    } else if form.eq_ignore_ascii_case("NFKD") {
        Ok(UnicodeNormalForm::Nfkd)
    } else {
        Err(ExecError::DetailedError {
            message: format!("invalid normalization form: {form}"),
            detail: None,
            hint: None,
            sqlstate: "22023",
        })
    }
}

fn unicode_normalize_text(text: &str, form: UnicodeNormalForm) -> String {
    match form {
        UnicodeNormalForm::Nfc => text.nfc().collect(),
        UnicodeNormalForm::Nfd => text.nfd().collect(),
        UnicodeNormalForm::Nfkc => text.nfkc().collect(),
        UnicodeNormalForm::Nfkd => text.nfkd().collect(),
    }
}

fn unicode_text_is_normalized(text: &str, form: UnicodeNormalForm) -> bool {
    match form {
        UnicodeNormalForm::Nfc => is_nfc(text),
        UnicodeNormalForm::Nfd => is_nfd(text),
        UnicodeNormalForm::Nfkc => is_nfkc(text),
        UnicodeNormalForm::Nfkd => is_nfkd(text),
    }
}

pub(super) fn eval_initcap_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(text_value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(text_value, Value::Null) {
        return Ok(Value::Null);
    }
    let text = expect_text_arg("initcap", text_value, &Value::Null)?;
    let mut out = String::with_capacity(text.len());
    let mut capitalize = true;
    for ch in text.chars() {
        if ch.is_alphanumeric() {
            if capitalize {
                out.extend(ch.to_uppercase());
                capitalize = false;
            } else {
                out.extend(ch.to_lowercase());
            }
        } else {
            capitalize = true;
            out.push(ch);
        }
    }
    Ok(Value::Text(CompactString::from_owned(out)))
}

fn decode_unistr_text(text: &str) -> Result<String, ExecError> {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::new();
    let mut i = 0usize;
    while i < chars.len() {
        if chars[i] != '\\' {
            out.push(chars[i]);
            i += 1;
            continue;
        }
        if i + 1 < chars.len() && chars[i + 1] == '\\' {
            out.push('\\');
            i += 2;
            continue;
        }
        let (code, next) = parse_unistr_escape(chars.as_slice(), i, "invalid Unicode escape")?;
        let (decoded, consumed) = decode_unistr_codepoint(&chars, next, code)?;
        out.push(decoded);
        i = consumed;
    }
    Ok(out)
}

fn decode_unistr_codepoint(
    chars: &[char],
    next: usize,
    code: u32,
) -> Result<(char, usize), ExecError> {
    if let Some(high) = unistr_high_surrogate(code) {
        let Some((low, consumed)) = parse_next_unistr_escape(chars, next)? else {
            return Err(unistr_error("invalid Unicode surrogate pair"));
        };
        let Some(low) = unistr_low_surrogate(low) else {
            return Err(unistr_error("invalid Unicode surrogate pair"));
        };
        let codepoint = 0x10000 + (((high as u32) - 0xD800) << 10) + ((low as u32) - 0xDC00);
        let decoded = char::from_u32(codepoint)
            .ok_or_else(|| unistr_error("invalid Unicode code point: 2FFFFF"))?;
        Ok((decoded, consumed))
    } else if unistr_low_surrogate(code).is_some() {
        Err(unistr_error("invalid Unicode surrogate pair"))
    } else {
        let decoded = char::from_u32(code)
            .ok_or_else(|| unistr_error("invalid Unicode code point: 2FFFFF"))?;
        Ok((decoded, next))
    }
}

fn parse_next_unistr_escape(
    chars: &[char],
    start: usize,
) -> Result<Option<(u32, usize)>, ExecError> {
    if start >= chars.len() {
        return Ok(None);
    }
    if chars[start] != '\\' {
        return Ok(None);
    }
    parse_unistr_escape(chars, start, "invalid Unicode surrogate pair").map(Some)
}

fn unistr_error(message: &'static str) -> ExecError {
    ExecError::Parse(ParseError::UnexpectedToken {
        expected: "valid Unicode escape",
        actual: message.into(),
    })
}

fn unistr_high_surrogate(code: u32) -> Option<u16> {
    (0xD800..=0xDBFF).contains(&code).then_some(code as u16)
}

fn unistr_low_surrogate(code: u32) -> Option<u16> {
    (0xDC00..=0xDFFF).contains(&code).then_some(code as u16)
}

fn parse_unistr_escape(
    chars: &[char],
    start: usize,
    error: &'static str,
) -> Result<(u32, usize), ExecError> {
    if start + 1 >= chars.len() {
        return Err(unistr_error(error));
    }

    let (prefix_len, digit_count) = match chars[start + 1] {
        '+' => (2, 6),
        'u' => (2, 4),
        'U' => (2, 8),
        _ => (1, 4),
    };
    let digits_start = start + prefix_len;
    let digits_end = digits_start + digit_count;
    if digits_end > chars.len() {
        return Err(unistr_error(error));
    }

    let digits = chars[digits_start..digits_end].iter().collect::<String>();
    let code = u32::from_str_radix(&digits, 16).map_err(|_| unistr_error(error))?;
    Ok((code, digits_end))
}

pub(super) fn eval_replace_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(text_value) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(from_value) = values.get(1) else {
        return Ok(Value::Null);
    };
    let Some(to_value) = values.get(2) else {
        return Ok(Value::Null);
    };
    if matches!(text_value, Value::Null)
        || matches!(from_value, Value::Null)
        || matches!(to_value, Value::Null)
    {
        return Ok(Value::Null);
    }
    let text = expect_text_arg("replace", text_value, from_value)?;
    let from = expect_text_arg("replace", from_value, to_value)?;
    let to = expect_text_arg("replace", to_value, text_value)?;
    Ok(Value::Text(CompactString::from_owned(
        text.replace(from, to),
    )))
}

pub(super) fn eval_split_part_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(text_value) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(delim_value) = values.get(1) else {
        return Ok(Value::Null);
    };
    let Some(index_value) = values.get(2) else {
        return Ok(Value::Null);
    };
    if matches!(text_value, Value::Null)
        || matches!(delim_value, Value::Null)
        || matches!(index_value, Value::Null)
    {
        return Ok(Value::Null);
    }
    let text = expect_text_arg("split_part", text_value, delim_value)?;
    let delim = expect_text_arg("split_part", delim_value, index_value)?;
    let field = expect_i32_arg("split_part", index_value, text_value)?;
    if field == 0 {
        return Err(ExecError::RaiseException(
            "field position must not be zero".into(),
        ));
    }
    if delim.is_empty() {
        let result = if field == 1 || field == -1 { text } else { "" };
        return Ok(Value::Text(CompactString::from(result)));
    }
    let parts: Vec<&str> = text.split(delim).collect();
    let result = if field > 0 {
        parts.get((field - 1) as usize).copied().unwrap_or("")
    } else {
        let index = parts.len() as i32 + field;
        if index < 0 {
            ""
        } else {
            parts.get(index as usize).copied().unwrap_or("")
        }
    };
    Ok(Value::Text(CompactString::from(result)))
}

pub(super) fn eval_lpad_function(values: &[Value]) -> Result<Value, ExecError> {
    eval_pad_function("lpad", values, true)
}

pub(super) fn eval_rpad_function(values: &[Value]) -> Result<Value, ExecError> {
    eval_pad_function("rpad", values, false)
}

pub(super) fn eval_translate_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(text_value) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(from_value) = values.get(1) else {
        return Ok(Value::Null);
    };
    let Some(to_value) = values.get(2) else {
        return Ok(Value::Null);
    };
    if matches!(text_value, Value::Null)
        || matches!(from_value, Value::Null)
        || matches!(to_value, Value::Null)
    {
        return Ok(Value::Null);
    }
    let text = expect_text_arg("translate", text_value, from_value)?;
    let from = expect_text_arg("translate", from_value, to_value)?;
    let to = expect_text_arg("translate", to_value, text_value)?;
    let from_chars: Vec<char> = from.chars().collect();
    let to_chars: Vec<char> = to.chars().collect();
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        if let Some(idx) = from_chars.iter().position(|candidate| *candidate == ch) {
            if let Some(replacement) = to_chars.get(idx) {
                out.push(*replacement);
            }
        } else {
            out.push(ch);
        }
    }
    Ok(Value::Text(CompactString::from_owned(out)))
}

pub(super) fn eval_ascii_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(text_value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(text_value, Value::Null) {
        return Ok(Value::Null);
    }
    let text = expect_text_arg("ascii", text_value, &Value::Null)?;
    Ok(Value::Int32(
        text.chars().next().map(|ch| ch as i32).unwrap_or(0),
    ))
}

pub(super) fn eval_chr_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let code = expect_i32_arg("chr", value, &Value::Null)?;
    if code == 0 {
        return Err(ExecError::RaiseException(
            "null character not permitted".into(),
        ));
    }
    let code_u32 = u32::try_from(code)
        .map_err(|_| ExecError::RaiseException("requested character too large".into()))?;
    let ch = char::from_u32(code_u32)
        .ok_or_else(|| ExecError::RaiseException("requested character too large".into()))?;
    Ok(Value::Text(CompactString::from_owned(ch.to_string())))
}

pub(super) fn eval_trim_function(op: &'static str, values: &[Value]) -> Result<Value, ExecError> {
    let Some(source) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(source, Value::Null) {
        return Ok(Value::Null);
    }
    let trim_chars = values.get(1);
    match source {
        Value::Bytea(bytes) => {
            let chars = match trim_chars {
                Some(Value::Null) => return Ok(Value::Null),
                Some(Value::Bytea(chars)) => chars.as_slice(),
                None => b" ",
                Some(other) => {
                    return Err(ExecError::TypeMismatch {
                        op,
                        left: source.clone(),
                        right: other.clone(),
                    });
                }
            };
            Ok(Value::Bytea(match op {
                "btrim" => trim_bytes(bytes, chars, true, true).to_vec(),
                "ltrim" => trim_bytes(bytes, chars, true, false).to_vec(),
                "rtrim" => trim_bytes(bytes, chars, false, true).to_vec(),
                _ => unreachable!(),
            }))
        }
        _ => {
            let text = source.as_text().ok_or_else(|| ExecError::TypeMismatch {
                op,
                left: source.clone(),
                right: trim_chars.cloned().unwrap_or(Value::Null),
            })?;
            let chars = match trim_chars {
                Some(Value::Null) => return Ok(Value::Null),
                Some(value) => value.as_text().ok_or_else(|| ExecError::TypeMismatch {
                    op,
                    left: source.clone(),
                    right: value.clone(),
                })?,
                None => " ",
            };
            Ok(Value::Text(CompactString::from_owned(match op {
                "btrim" => text.trim_matches(|c| chars.contains(c)).to_string(),
                "ltrim" => text.trim_start_matches(|c| chars.contains(c)).to_string(),
                "rtrim" => text.trim_end_matches(|c| chars.contains(c)).to_string(),
                _ => unreachable!(),
            })))
        }
    }
}

pub(super) fn eval_text_substring(values: &[Value]) -> Result<Value, ExecError> {
    let Some(text_value) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(start_value) = values.get(1) else {
        return Ok(Value::Null);
    };
    if matches!(text_value, Value::Null) || matches!(start_value, Value::Null) {
        return Ok(Value::Null);
    }
    let text = text_value
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "substring",
            left: text_value.clone(),
            right: start_value.clone(),
        })?;
    let start = match start_value {
        Value::Int32(v) => *v,
        other => {
            return Err(ExecError::TypeMismatch {
                op: "substring",
                left: text_value.clone(),
                right: other.clone(),
            });
        }
    };
    let len = match values.get(2) {
        Some(Value::Null) => return Ok(Value::Null),
        Some(Value::Int32(v)) => Some(*v),
        Some(other) => {
            return Err(ExecError::TypeMismatch {
                op: "substring",
                left: text_value.clone(),
                right: other.clone(),
            });
        }
        None => None,
    };
    if let Some(len) = len
        && len < 0
    {
        return Err(ExecError::NegativeSubstringLength);
    }
    let chars: Vec<char> = text.chars().collect();
    let start_i64 = i64::from(start);
    let start_idx = if start_i64 <= 1 {
        0usize
    } else {
        usize::min((start_i64 - 1) as usize, chars.len())
    };
    let take_len = match len {
        None => chars.len().saturating_sub(start_idx),
        Some(len) => {
            let skipped_before_start = if start_i64 < 1 { 1 - start_i64 } else { 0 };
            let adjusted = i64::from(len) - skipped_before_start;
            if adjusted <= 0 {
                0
            } else {
                usize::min(adjusted as usize, chars.len().saturating_sub(start_idx))
            }
        }
    };
    Ok(Value::Text(CompactString::from_owned(
        chars[start_idx..start_idx + take_len].iter().collect(),
    )))
}

pub(super) fn eval_bytea_substring(values: &[Value]) -> Result<Value, ExecError> {
    let Some(bytes_value) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(start_value) = values.get(1) else {
        return Ok(Value::Null);
    };
    if matches!(bytes_value, Value::Null) || matches!(start_value, Value::Null) {
        return Ok(Value::Null);
    }
    let bytes = expect_bytea_arg("substring", bytes_value, start_value)?;
    let start = expect_i32_arg("substring", start_value, bytes_value)?;
    let len = match values.get(2) {
        Some(Value::Null) => return Ok(Value::Null),
        Some(value) => Some(expect_i32_arg("substring", value, bytes_value)?),
        None => None,
    };
    if let Some(len) = len
        && len < 0
    {
        return Err(ExecError::NegativeSubstringLength);
    }
    let start_i64 = i64::from(start);
    let start_idx = if start_i64 <= 1 {
        0usize
    } else {
        usize::min((start_i64 - 1) as usize, bytes.len())
    };
    let take_len = match len {
        None => bytes.len().saturating_sub(start_idx),
        Some(len) => {
            let skipped_before_start = if start_i64 < 1 { 1 - start_i64 } else { 0 };
            let adjusted = i64::from(len) - skipped_before_start;
            if adjusted <= 0 {
                0
            } else {
                usize::min(adjusted as usize, bytes.len().saturating_sub(start_idx))
            }
        }
    };
    Ok(Value::Bytea(
        bytes[start_idx..start_idx + take_len].to_vec(),
    ))
}

pub(super) fn eval_like(
    left: &Value,
    pattern: &Value,
    escape: Option<&Value>,
    collation_oid: Option<u32>,
    case_insensitive: bool,
    negated: bool,
) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(pattern, Value::Null) {
        return Ok(Value::Null);
    }
    ensure_builtin_collation_supported(collation_oid)?;
    let matched = match (left, pattern, escape) {
        (Value::Bytea(text), Value::Bytea(pattern), escape) => {
            if case_insensitive {
                return Err(ExecError::TypeMismatch {
                    op: "ilike",
                    left: left.clone(),
                    right: Value::Bytea(pattern.clone()),
                });
            }
            let escape = match escape {
                Some(Value::Null) => return Ok(Value::Null),
                Some(Value::Bytea(bytes)) => Some(bytes.as_slice()),
                None => Some(b"\\".as_slice()),
                Some(other) => {
                    return Err(ExecError::TypeMismatch {
                        op: "like",
                        left: left.clone(),
                        right: other.clone(),
                    });
                }
            };
            like_match_bytes(text, pattern, escape)?
        }
        (_, _, Some(Value::Null)) => return Ok(Value::Null),
        _ => {
            let text = left.as_text().ok_or_else(|| ExecError::TypeMismatch {
                op: if case_insensitive { "ilike" } else { "like" },
                left: left.clone(),
                right: pattern.clone(),
            })?;
            let pattern_text = pattern.as_text().ok_or_else(|| ExecError::TypeMismatch {
                op: if case_insensitive { "ilike" } else { "like" },
                left: left.clone(),
                right: pattern.clone(),
            })?;
            let escape = match escape {
                Some(value) => {
                    let escape_text = value.as_text().ok_or_else(|| ExecError::TypeMismatch {
                        op: if case_insensitive { "ilike" } else { "like" },
                        left: left.clone(),
                        right: value.clone(),
                    })?;
                    let mut chars = escape_text.chars();
                    let Some(ch) = chars.next() else {
                        return Ok(Value::Bool(if negated {
                            !like_match_text(text, pattern_text, None, case_insensitive)
                        } else {
                            like_match_text(text, pattern_text, None, case_insensitive)
                        }));
                    };
                    if chars.next().is_some() {
                        return Err(ExecError::InvalidRegex(
                            "ESCAPE expression must be empty or one character".into(),
                        ));
                    }
                    Some(ch)
                }
                None => None,
            };
            let escape = escape.or(Some('\\'));
            like_match_text(text, pattern_text, escape, case_insensitive)
        }
    };
    Ok(Value::Bool(if negated { !matched } else { matched }))
}
pub(super) fn eval_md5_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let bytes: Vec<u8> = match value {
        Value::Text(text) => text.as_bytes().to_vec(),
        Value::TextRef(_, _) => value.as_text().unwrap().as_bytes().to_vec(),
        Value::Bytea(bytes) => bytes.clone(),
        other => {
            return Err(ExecError::TypeMismatch {
                op: "md5",
                left: other.clone(),
                right: Value::Null,
            });
        }
    };
    let digest = Md5::digest(bytes);
    Ok(Value::Text(CompactString::from_owned(format!(
        "{digest:x}"
    ))))
}

pub(super) fn eval_reverse_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    if let Some(text) = value.as_text() {
        return Ok(Value::Text(CompactString::from_owned(
            text.chars().rev().collect(),
        )));
    }
    let mut bytes = expect_bytea_arg("reverse", value, &Value::Null)?.to_vec();
    bytes.reverse();
    Ok(Value::Bytea(bytes))
}

pub(super) fn eval_quote_literal_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    Ok(Value::Text(CompactString::from_owned(quote_literal_text(
        &value_output_text(value)?,
    ))))
}

pub(super) fn eval_quote_nullable_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(value) = values.first() else {
        return Ok(Value::Text("NULL".into()));
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Text("NULL".into()));
    }
    Ok(Value::Text(CompactString::from_owned(quote_literal_text(
        &value_output_text(value)?,
    ))))
}

pub(super) fn eval_quote_ident_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    Ok(Value::Text(CompactString::from_owned(quote_identifier(
        &value_output_text(value)?,
    ))))
}

pub(crate) fn eval_parse_ident_function(values: &[Value]) -> Result<Value, ExecError> {
    match values {
        [Value::Null] | [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [input] => parse_ident_text(input, true),
        [input, strict] => {
            let Value::Bool(strict) = strict else {
                return Err(ExecError::TypeMismatch {
                    op: "parse_ident",
                    left: strict.clone(),
                    right: Value::Bool(true),
                });
            };
            parse_ident_text(input, *strict)
        }
        _ => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "parse_ident(text [, strict])",
            actual: format!("ParseIdent({} args)", values.len()),
        })),
    }
}

fn parse_ident_text(input: &Value, strict: bool) -> Result<Value, ExecError> {
    let input = input.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "parse_ident",
        left: input.clone(),
        right: Value::Text("".into()),
    })?;
    let parts = parse_ident_parts(input, strict)?;
    Ok(Value::PgArray(
        crate::include::nodes::datum::ArrayValue::from_dimensions(
            vec![crate::include::nodes::datum::ArrayDimension {
                lower_bound: 1,
                length: parts.len(),
            }],
            parts
                .into_iter()
                .map(|part| Value::Text(part.into()))
                .collect(),
        )
        .with_element_type_oid(crate::include::catalog::TEXT_TYPE_OID),
    ))
}

fn parse_ident_parts(input: &str, strict: bool) -> Result<Vec<String>, ExecError> {
    let original = input.to_string();
    let bytes = input.as_bytes();
    let mut index = 0usize;
    let mut after_dot = false;
    let mut parts = Vec::new();

    while index < bytes.len() && bytes[index].is_ascii_whitespace() {
        index += 1;
    }

    loop {
        let mut missing_ident = true;

        if bytes.get(index) == Some(&b'"') {
            index += 1;
            let mut current = String::new();
            loop {
                let Some(ch) = input[index..].chars().next() else {
                    return Err(invalid_identifier_error(
                        &original,
                        Some("String has unclosed double quotes."),
                    ));
                };
                index += ch.len_utf8();
                if ch == '"' {
                    if bytes.get(index) == Some(&b'"') {
                        current.push('"');
                        index += 1;
                        continue;
                    }
                    break;
                }
                current.push(ch);
            }
            if current.is_empty() {
                return Err(invalid_identifier_error(
                    &original,
                    Some("Quoted identifier must not be empty."),
                ));
            }
            parts.push(current);
            missing_ident = false;
        } else if let Some(ch) = input[index..]
            .chars()
            .next()
            .filter(|ch| is_ident_start(*ch))
        {
            let mut current = String::new();
            current.push(ch);
            index += ch.len_utf8();
            while let Some(ch) = input[index..].chars().next() {
                if !is_ident_cont(ch) {
                    break;
                }
                current.push(ch);
                index += ch.len_utf8();
            }
            parts.push(current.to_ascii_lowercase());
            missing_ident = false;
        }

        if missing_ident {
            if bytes.get(index) == Some(&b'.') {
                return Err(invalid_identifier_error(
                    &original,
                    Some("No valid identifier before \".\"."),
                ));
            }
            if after_dot {
                return Err(invalid_identifier_error(
                    &original,
                    Some("No valid identifier after \".\"."),
                ));
            }
            return Err(invalid_identifier_error(&original, None));
        }

        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }

        if bytes.get(index) == Some(&b'.') {
            after_dot = true;
            index += 1;
            while index < bytes.len() && bytes[index].is_ascii_whitespace() {
                index += 1;
            }
            continue;
        }

        if index == bytes.len() {
            break;
        }

        if strict {
            return Err(invalid_identifier_error(&original, None));
        }
        break;
    }

    Ok(parts)
}

fn invalid_identifier_error(input: &str, detail: Option<&str>) -> ExecError {
    ExecError::DetailedError {
        message: format!("string is not a valid identifier: \"{input}\""),
        detail: detail.map(str::to_string),
        hint: None,
        sqlstate: "22023",
    }
}

fn is_ident_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic() || !ch.is_ascii()
}

fn is_ident_cont(ch: char) -> bool {
    is_ident_start(ch) || ch.is_ascii_digit() || ch == '$'
}

pub(super) fn eval_encode_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(bytes_value) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(format_value) = values.get(1) else {
        return Ok(Value::Null);
    };
    if matches!(bytes_value, Value::Null) || matches!(format_value, Value::Null) {
        return Ok(Value::Null);
    }
    let bytes = expect_bytea_arg("encode", bytes_value, format_value)?;
    let format = expect_text_arg("encode", format_value, bytes_value)?.to_ascii_lowercase();
    let rendered = match format.as_str() {
        "hex" => encode_hex_bytes(bytes),
        "escape" => format_bytea_text(bytes, ByteaOutputFormat::Escape),
        "base64" => wrap_base64_text(&base64::engine::general_purpose::STANDARD.encode(bytes)),
        _ => {
            return Err(ExecError::RaiseException(format!(
                "unrecognized encoding: \"{format}\""
            )));
        }
    };
    Ok(Value::Text(CompactString::from_owned(rendered)))
}

fn wrap_base64_text(text: &str) -> String {
    const BASE64_LINE_LEN: usize = 76;
    if text.len() <= BASE64_LINE_LEN {
        return text.to_string();
    }
    let mut out = String::with_capacity(text.len() + text.len() / BASE64_LINE_LEN);
    for (idx, chunk) in text.as_bytes().chunks(BASE64_LINE_LEN).enumerate() {
        if idx > 0 {
            out.push('\n');
        }
        out.push_str(std::str::from_utf8(chunk).expect("base64 output is ASCII"));
    }
    out
}

pub(super) fn eval_decode_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(text_value) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(format_value) = values.get(1) else {
        return Ok(Value::Null);
    };
    if matches!(text_value, Value::Null) || matches!(format_value, Value::Null) {
        return Ok(Value::Null);
    }
    let text = expect_text_arg("decode", text_value, format_value)?;
    let format = expect_text_arg("decode", format_value, text_value)?.to_ascii_lowercase();
    let bytes = match format.as_str() {
        "hex" => decode_hex_blob(text)?,
        "escape" => decode_escape_bytes(text)?,
        "base64" => {
            let compact = text
                .bytes()
                .filter(|byte| !byte.is_ascii_whitespace())
                .collect::<Vec<_>>();
            base64::engine::general_purpose::STANDARD
                .decode(compact)
                .map_err(|_| ExecError::RaiseException("invalid base64 end sequence".into()))?
        }
        _ => {
            return Err(ExecError::RaiseException(format!(
                "unrecognized encoding: \"{format}\""
            )));
        }
    };
    Ok(Value::Bytea(bytes))
}

fn decode_escape_bytes(text: &str) -> Result<Vec<u8>, ExecError> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut idx = 0usize;
    while idx < bytes.len() {
        if bytes[idx] != b'\\' {
            out.push(bytes[idx]);
            idx += 1;
            continue;
        }
        idx += 1;
        if idx >= bytes.len() {
            return Err(ExecError::InvalidByteaInput {
                value: text.to_string(),
            });
        }
        if bytes[idx] == b'\\' {
            out.push(b'\\');
            idx += 1;
        } else if bytes[idx] == b'x' && idx + 2 < bytes.len() {
            let hi = hex_nibble(bytes[idx + 1]);
            let lo = hex_nibble(bytes[idx + 2]);
            if let (Some(hi), Some(lo)) = (hi, lo) {
                out.push((hi << 4) | lo);
                idx += 3;
            } else {
                out.push(bytes[idx]);
                idx += 1;
            }
        } else if idx + 2 < bytes.len()
            && (b'0'..=b'7').contains(&bytes[idx])
            && (b'0'..=b'7').contains(&bytes[idx + 1])
            && (b'0'..=b'7').contains(&bytes[idx + 2])
        {
            let value = u16::from(bytes[idx] - b'0') * 64
                + u16::from(bytes[idx + 1] - b'0') * 8
                + u16::from(bytes[idx + 2] - b'0');
            out.push(value as u8);
            idx += 3;
        } else {
            out.push(bytes[idx]);
            idx += 1;
        }
    }
    Ok(out)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

pub(super) fn eval_sha224_function(values: &[Value]) -> Result<Value, ExecError> {
    eval_sha2_function::<Sha224>("sha224", values)
}

pub(super) fn eval_sha256_function(values: &[Value]) -> Result<Value, ExecError> {
    eval_sha2_function::<Sha256>("sha256", values)
}

pub(super) fn eval_sha384_function(values: &[Value]) -> Result<Value, ExecError> {
    eval_sha2_function::<Sha384>("sha384", values)
}

pub(super) fn eval_sha512_function(values: &[Value]) -> Result<Value, ExecError> {
    eval_sha2_function::<Sha512>("sha512", values)
}

pub(super) fn eval_crc32_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    Ok(Value::Int64(
        crc32fast::hash(&coerce_hash_bytes("crc32", value)?) as i64,
    ))
}

pub(super) fn eval_crc32c_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    Ok(Value::Int64(
        crc32c::crc32c(&coerce_hash_bytes("crc32c", value)?) as i64,
    ))
}

pub(super) fn eval_bpchar_to_text_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(text_value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(text_value, Value::Null) {
        return Ok(Value::Null);
    }
    let text = text_value
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "::text",
            left: text_value.clone(),
            right: Value::Text("".into()),
        })?;
    Ok(Value::Text(CompactString::from_owned(
        text.trim_end_matches(' ').to_string(),
    )))
}

pub(super) fn eval_position_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(needle_value) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(haystack_value) = values.get(1) else {
        return Ok(Value::Null);
    };
    if matches!(needle_value, Value::Null) || matches!(haystack_value, Value::Null) {
        return Ok(Value::Null);
    }
    let needle = needle_value
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "position",
            left: needle_value.clone(),
            right: haystack_value.clone(),
        })?;
    let haystack = haystack_value
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "position",
            left: needle_value.clone(),
            right: haystack_value.clone(),
        })?;
    if needle.is_empty() {
        return Ok(Value::Int32(1));
    }
    let position = haystack
        .find(needle)
        .map(|idx| haystack[..idx].chars().count() as i32 + 1)
        .unwrap_or(0);
    Ok(Value::Int32(position))
}

pub(super) fn eval_strpos_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(text_value) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(substring_value) = values.get(1) else {
        return Ok(Value::Null);
    };
    if matches!(text_value, Value::Null) || matches!(substring_value, Value::Null) {
        return Ok(Value::Null);
    }
    let text = expect_text_arg("strpos", text_value, substring_value)?;
    let substring = expect_text_arg("strpos", substring_value, text_value)?;
    if substring.is_empty() {
        return Ok(Value::Int32(1));
    }
    let position = text
        .find(substring)
        .map(|idx| text[..idx].chars().count() as i32 + 1)
        .unwrap_or(0);
    Ok(Value::Int32(position))
}

pub(super) fn eval_bytea_position_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(needle_value) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(haystack_value) = values.get(1) else {
        return Ok(Value::Null);
    };
    if matches!(needle_value, Value::Null) || matches!(haystack_value, Value::Null) {
        return Ok(Value::Null);
    }
    let needle = expect_bytea_arg("position", needle_value, haystack_value)?;
    let haystack = expect_bytea_arg("position", haystack_value, needle_value)?;
    if needle.is_empty() {
        return Ok(Value::Int32(1));
    }
    let position = haystack
        .windows(needle.len())
        .position(|window| window == needle)
        .map(|idx| idx as i32 + 1)
        .unwrap_or(0);
    Ok(Value::Int32(position))
}

pub(super) fn eval_bytea_overlay(values: &[Value]) -> Result<Value, ExecError> {
    let Some(bytes_value) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(place_value) = values.get(1) else {
        return Ok(Value::Null);
    };
    let Some(start_value) = values.get(2) else {
        return Ok(Value::Null);
    };
    if matches!(bytes_value, Value::Null)
        || matches!(place_value, Value::Null)
        || matches!(start_value, Value::Null)
    {
        return Ok(Value::Null);
    }
    let bytes = expect_bytea_arg("overlay", bytes_value, place_value)?;
    let place = expect_bytea_arg("overlay", place_value, bytes_value)?;
    let start = expect_i32_arg("overlay", start_value, bytes_value)?;
    let len = match values.get(3) {
        Some(Value::Null) => return Ok(Value::Null),
        Some(value) => Some(expect_i32_arg("overlay", value, bytes_value)?),
        None => None,
    };
    let replace_len = len.unwrap_or(place.len() as i32);
    if replace_len < 0 {
        return Err(ExecError::NegativeSubstringLength);
    }
    let prefix_len = (start - 1).max(0).min(bytes.len() as i32) as usize;
    let suffix_start = (start - 1)
        .saturating_add(replace_len)
        .max(0)
        .min(bytes.len() as i32) as usize;
    let mut out =
        Vec::with_capacity(prefix_len + place.len() + bytes.len().saturating_sub(suffix_start));
    out.extend_from_slice(&bytes[..prefix_len]);
    out.extend_from_slice(place);
    out.extend_from_slice(&bytes[suffix_start..]);
    Ok(Value::Bytea(out))
}

pub(super) fn eval_text_overlay(values: &[Value]) -> Result<Value, ExecError> {
    let Some(text_value) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(place_value) = values.get(1) else {
        return Ok(Value::Null);
    };
    let Some(start_value) = values.get(2) else {
        return Ok(Value::Null);
    };
    if matches!(text_value, Value::Null)
        || matches!(place_value, Value::Null)
        || matches!(start_value, Value::Null)
    {
        return Ok(Value::Null);
    }
    let text = text_value
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "overlay",
            left: text_value.clone(),
            right: place_value.clone(),
        })?;
    let place = place_value
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "overlay",
            left: place_value.clone(),
            right: text_value.clone(),
        })?;
    let start = match start_value {
        Value::Int32(v) => *v,
        other => {
            return Err(ExecError::TypeMismatch {
                op: "overlay",
                left: text_value.clone(),
                right: other.clone(),
            });
        }
    };
    let len = match values.get(3) {
        Some(Value::Null) => return Ok(Value::Null),
        Some(Value::Int32(v)) => Some(*v),
        Some(other) => {
            return Err(ExecError::TypeMismatch {
                op: "overlay",
                left: text_value.clone(),
                right: other.clone(),
            });
        }
        None => None,
    };
    if start <= 0 {
        return Err(ExecError::NegativeSubstringLength);
    }
    let replace_len = match len {
        Some(len) => {
            if len < 0 {
                return Err(ExecError::NegativeSubstringLength);
            }
            len
        }
        None => i32::try_from(place.chars().count()).map_err(|_| ExecError::Int4OutOfRange)?,
    };
    let suffix_position = start
        .checked_add(replace_len)
        .ok_or(ExecError::Int4OutOfRange)?;
    let chars: Vec<char> = text.chars().collect();
    let prefix_len = usize::min((start - 1) as usize, chars.len());
    let suffix_start = usize::min((suffix_position - 1) as usize, chars.len());
    let mut out = String::with_capacity(text.len() + place.len());
    out.extend(chars[..prefix_len].iter());
    out.push_str(place);
    out.extend(chars[suffix_start..].iter());
    Ok(Value::Text(CompactString::from_owned(out)))
}

pub(super) fn eval_get_bit_bytes(values: &[Value]) -> Result<Value, ExecError> {
    let Some(bytes_value) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(index_value) = values.get(1) else {
        return Ok(Value::Null);
    };
    if matches!(bytes_value, Value::Null) || matches!(index_value, Value::Null) {
        return Ok(Value::Null);
    }
    let bytes = expect_bytea_arg("get_bit", bytes_value, index_value)?;
    let index = expect_i32_arg("get_bit", index_value, bytes_value)?;
    validate_bytea_bit_index(bytes, index)?;
    let byte = bytes[(index / 8) as usize];
    let shift = (index % 8) as u8;
    Ok(Value::Int32(((byte >> shift) & 1) as i32))
}

pub(super) fn eval_set_bit_bytes(values: &[Value]) -> Result<Value, ExecError> {
    let Some(bytes_value) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(index_value) = values.get(1) else {
        return Ok(Value::Null);
    };
    let Some(new_value) = values.get(2) else {
        return Ok(Value::Null);
    };
    if matches!(bytes_value, Value::Null)
        || matches!(index_value, Value::Null)
        || matches!(new_value, Value::Null)
    {
        return Ok(Value::Null);
    }
    let mut bytes = expect_bytea_arg("set_bit", bytes_value, index_value)?.to_vec();
    let index = expect_i32_arg("set_bit", index_value, bytes_value)?;
    let bit = expect_i32_arg("set_bit", new_value, bytes_value)?;
    validate_bytea_bit_index(&bytes, index)?;
    let mask = 1u8 << (index % 8) as u8;
    if bit == 0 {
        bytes[(index / 8) as usize] &= !mask;
    } else {
        bytes[(index / 8) as usize] |= mask;
    }
    Ok(Value::Bytea(bytes))
}

pub(super) fn eval_bit_count_bytes(values: &[Value]) -> Result<Value, ExecError> {
    let Some(bytes_value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(bytes_value, Value::Null) {
        return Ok(Value::Null);
    }
    let bytes = expect_bytea_arg("bit_count", bytes_value, &Value::Null)?;
    Ok(Value::Int64(
        bytes.iter().map(|byte| byte.count_ones() as i64).sum(),
    ))
}

pub(super) fn eval_get_byte(values: &[Value]) -> Result<Value, ExecError> {
    let Some(bytes_value) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(index_value) = values.get(1) else {
        return Ok(Value::Null);
    };
    if matches!(bytes_value, Value::Null) || matches!(index_value, Value::Null) {
        return Ok(Value::Null);
    }
    let bytes = expect_bytea_arg("get_byte", bytes_value, index_value)?;
    let index = expect_i32_arg("get_byte", index_value, bytes_value)?;
    validate_bytea_index(bytes, index, "get_byte")?;
    Ok(Value::Int32(bytes[index as usize] as i32))
}

pub(super) fn eval_set_byte(values: &[Value]) -> Result<Value, ExecError> {
    let Some(bytes_value) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(index_value) = values.get(1) else {
        return Ok(Value::Null);
    };
    let Some(new_value) = values.get(2) else {
        return Ok(Value::Null);
    };
    if matches!(bytes_value, Value::Null)
        || matches!(index_value, Value::Null)
        || matches!(new_value, Value::Null)
    {
        return Ok(Value::Null);
    }
    let mut bytes = expect_bytea_arg("set_byte", bytes_value, index_value)?.to_vec();
    let index = expect_i32_arg("set_byte", index_value, bytes_value)?;
    let new_byte = expect_i32_arg("set_byte", new_value, bytes_value)?;
    validate_bytea_index(&bytes, index, "set_byte")?;
    if !(0..=255).contains(&new_byte) {
        return Err(ExecError::RaiseException(format!(
            "new byte must be between 0 and 255: {new_byte}"
        )));
    }
    bytes[index as usize] = new_byte as u8;
    Ok(Value::Bytea(bytes))
}

pub(super) fn eval_convert_from_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(bytes_value) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(encoding_value) = values.get(1) else {
        return Ok(Value::Null);
    };
    if matches!(bytes_value, Value::Null) || matches!(encoding_value, Value::Null) {
        return Ok(Value::Null);
    }
    let raw = bytes_value
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "convert_from",
            left: bytes_value.clone(),
            right: encoding_value.clone(),
        })?;
    let encoding_name = encoding_value
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "convert_from",
            left: bytes_value.clone(),
            right: encoding_value.clone(),
        })?;
    let bytes = decode_hex_text_bytes(raw).ok_or_else(|| ExecError::TypeMismatch {
        op: "convert_from",
        left: bytes_value.clone(),
        right: encoding_value.clone(),
    })?;
    let normalized = normalize_encoding_label(encoding_name);
    let encoding =
        Encoding::for_label(normalized.as_bytes()).ok_or_else(|| ExecError::TypeMismatch {
            op: "convert_from",
            left: bytes_value.clone(),
            right: encoding_value.clone(),
        })?;
    let (decoded, _, had_errors) = encoding.decode(&bytes);
    if had_errors {
        return Err(ExecError::TypeMismatch {
            op: "convert_from",
            left: bytes_value.clone(),
            right: encoding_value.clone(),
        });
    }
    Ok(Value::Text(CompactString::from_owned(decoded.into_owned())))
}

pub(super) fn eval_convert_to_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(text_value) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(encoding_value) = values.get(1) else {
        return Ok(Value::Null);
    };
    if matches!(text_value, Value::Null) || matches!(encoding_value, Value::Null) {
        return Ok(Value::Null);
    }
    let text = text_value
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "convert_to",
            left: text_value.clone(),
            right: encoding_value.clone(),
        })?;
    let encoding_name = encoding_value
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "convert_to",
            left: text_value.clone(),
            right: encoding_value.clone(),
        })?;
    let normalized = normalize_encoding_label(encoding_name);
    let encoding =
        Encoding::for_label(normalized.as_bytes()).ok_or_else(|| ExecError::TypeMismatch {
            op: "convert_to",
            left: text_value.clone(),
            right: encoding_value.clone(),
        })?;
    let (encoded, _, had_errors) = encoding.encode(text);
    if had_errors {
        return Err(ExecError::TypeMismatch {
            op: "convert_to",
            left: text_value.clone(),
            right: encoding_value.clone(),
        });
    }
    Ok(Value::Bytea(encoded.into_owned()))
}

fn decode_hex_text_bytes(raw: &str) -> Option<Vec<u8>> {
    let bytes = raw.strip_prefix("\\x")?;
    if bytes.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for chunk in bytes.as_bytes().chunks(2) {
        let hex = std::str::from_utf8(chunk).ok()?;
        out.push(u8::from_str_radix(hex, 16).ok()?);
    }
    Some(out)
}

fn trim_bytes<'a>(input: &'a [u8], chars: &[u8], leading: bool, trailing: bool) -> &'a [u8] {
    let mut start = 0usize;
    let mut end = input.len();
    if leading {
        while start < end && chars.contains(&input[start]) {
            start += 1;
        }
    }
    if trailing {
        while end > start && chars.contains(&input[end - 1]) {
            end -= 1;
        }
    }
    &input[start..end]
}

fn like_match_text(
    text: &str,
    pattern: &str,
    escape: Option<char>,
    case_insensitive: bool,
) -> bool {
    let text: Vec<char> = if case_insensitive {
        text.to_lowercase().chars().collect()
    } else {
        text.chars().collect()
    };
    let pattern: Vec<char> = if case_insensitive {
        pattern.to_lowercase().chars().collect()
    } else {
        pattern.chars().collect()
    };
    like_match_chars(&text, &pattern, escape)
}

fn like_match_chars(text: &[char], pattern: &[char], escape: Option<char>) -> bool {
    let mut ti = 0usize;
    let mut pi = 0usize;
    let mut last_percent = None::<usize>;
    let mut last_match = 0usize;
    while ti < text.len() {
        if pi < pattern.len() {
            let pat = pattern[pi];
            if Some(pat) == escape {
                if pi + 1 >= pattern.len() {
                    return false;
                }
                if text[ti] == pattern[pi + 1] {
                    ti += 1;
                    pi += 2;
                    continue;
                }
            } else if pat == '_' || pat == text[ti] {
                ti += 1;
                pi += 1;
                continue;
            } else if pat == '%' {
                last_percent = Some(pi);
                pi += 1;
                last_match = ti;
                continue;
            }
        }
        if let Some(percent) = last_percent {
            pi = percent + 1;
            last_match += 1;
            ti = last_match;
        } else {
            return false;
        }
    }
    while pi < pattern.len() && pattern[pi] == '%' {
        pi += 1;
    }
    pi == pattern.len()
}

fn like_match_bytes(text: &[u8], pattern: &[u8], escape: Option<&[u8]>) -> Result<bool, ExecError> {
    let escape = match escape {
        Some([]) | None => None,
        Some([byte]) => Some(*byte),
        Some(_) => {
            return Err(ExecError::InvalidRegex(
                "ESCAPE expression must be empty or one character".into(),
            ));
        }
    };
    let mut ti = 0usize;
    let mut pi = 0usize;
    let mut last_percent = None::<usize>;
    let mut last_match = 0usize;
    while ti < text.len() {
        if pi < pattern.len() {
            let pat = pattern[pi];
            if Some(pat) == escape {
                if pi + 1 >= pattern.len() {
                    return Ok(false);
                }
                if text[ti] == pattern[pi + 1] {
                    ti += 1;
                    pi += 2;
                    continue;
                }
            } else if pat == b'_' || pat == text[ti] {
                ti += 1;
                pi += 1;
                continue;
            } else if pat == b'%' {
                last_percent = Some(pi);
                pi += 1;
                last_match = ti;
                continue;
            }
        }
        if let Some(percent) = last_percent {
            pi = percent + 1;
            last_match += 1;
            ti = last_match;
        } else {
            return Ok(false);
        }
    }
    while pi < pattern.len() && pattern[pi] == b'%' {
        pi += 1;
    }
    Ok(pi == pattern.len())
}

fn normalize_encoding_label(name: &str) -> String {
    name.trim().to_ascii_lowercase().replace('_', "-")
}

pub(super) fn eval_pg_rust_test_enc_setup(values: &[Value]) -> Result<Value, ExecError> {
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    if !values.is_empty() {
        return Err(ExecError::TypeMismatch {
            op: "pg_rust_test_enc_setup",
            left: values.first().cloned().unwrap_or(Value::Null),
            right: Value::Null,
        });
    }
    Ok(Value::Null)
}

pub(super) fn eval_pg_rust_test_opclass_options_func(values: &[Value]) -> Result<Value, ExecError> {
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    if values.len() != 1 {
        return Err(ExecError::TypeMismatch {
            op: "pg_rust_test_opclass_options_func",
            left: values.first().cloned().unwrap_or(Value::Null),
            right: Value::Null,
        });
    }
    Ok(Value::Null)
}

pub(super) fn eval_pg_rust_test_fdw_handler(values: &[Value]) -> Result<Value, ExecError> {
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    if !values.is_empty() {
        return Err(ExecError::TypeMismatch {
            op: "pg_rust_test_fdw_handler",
            left: values.first().cloned().unwrap_or(Value::Null),
            right: Value::Null,
        });
    }
    Ok(Value::Null)
}

const PG_PARAMETER_ACL_PARNAME_INDEX_OID: u32 = 6246;
const PG_REPLICATION_ORIGIN_RONAME_INDEX_OID: u32 = 6002;
const PG_SECLABEL_OBJECT_INDEX_OID: u32 = 3597;
const PG_SHSECLABEL_OBJECT_INDEX_OID: u32 = 3593;

pub(super) fn eval_pg_rust_is_catalog_text_unique_index_oid(
    values: &[Value],
) -> Result<Value, ExecError> {
    let [value] = values else {
        return Err(ExecError::TypeMismatch {
            op: "pg_rust_is_catalog_text_unique_index_oid",
            left: values.first().cloned().unwrap_or(Value::Null),
            right: Value::Int64(0),
        });
    };
    let oid = match value {
        Value::Null => return Ok(Value::Null),
        Value::Int32(oid) if *oid >= 0 => *oid as u32,
        Value::Int64(oid) if *oid >= 0 && *oid <= i64::from(u32::MAX) => *oid as u32,
        other => {
            return Err(ExecError::TypeMismatch {
                op: "pg_rust_is_catalog_text_unique_index_oid",
                left: other.clone(),
                right: Value::Int64(0),
            });
        }
    };
    Ok(Value::Bool(matches!(
        oid,
        PG_PARAMETER_ACL_PARNAME_INDEX_OID
            | PG_REPLICATION_ORIGIN_RONAME_INDEX_OID
            | PG_SECLABEL_OBJECT_INDEX_OID
            | PG_SHSECLABEL_OBJECT_INDEX_OID
    )))
}

pub(super) fn eval_pg_rust_test_widget_in(values: &[Value]) -> Result<Value, ExecError> {
    let [input] = values else {
        return Err(ExecError::TypeMismatch {
            op: "pg_rust_test_widget_in",
            left: values.first().cloned().unwrap_or(Value::Null),
            right: Value::Null,
        });
    };
    let Some(input) = input.as_text() else {
        return Ok(Value::Null);
    };
    let (x, y, radius) = parse_test_widget_text(input)?;
    Ok(Value::Text(CompactString::from_owned(format!(
        "({x},{y},{radius})"
    ))))
}

pub(super) fn eval_pg_rust_test_widget_out(values: &[Value]) -> Result<Value, ExecError> {
    let [widget] = values else {
        return Err(ExecError::TypeMismatch {
            op: "pg_rust_test_widget_out",
            left: values.first().cloned().unwrap_or(Value::Null),
            right: Value::Null,
        });
    };
    let Some(widget) = widget.as_text() else {
        return Ok(Value::Null);
    };
    let (x, y, radius) = parse_test_widget_text(widget)?;
    Ok(Value::Text(CompactString::from_owned(format!(
        "({x},{y},{radius})"
    ))))
}

pub(super) fn eval_pg_rust_test_int44in(values: &[Value]) -> Result<Value, ExecError> {
    let [input] = values else {
        return Err(ExecError::TypeMismatch {
            op: "pg_rust_test_int44in",
            left: values.first().cloned().unwrap_or(Value::Null),
            right: Value::Null,
        });
    };
    let Some(input) = input.as_text() else {
        return Ok(Value::Null);
    };
    let mut parts = [0_i32; 4];
    for (idx, item) in input.split(',').take(4).enumerate() {
        parts[idx] = item.trim().parse::<i32>().unwrap_or(0);
    }
    Ok(Value::Text(CompactString::from_owned(format!(
        "{},{},{},{}",
        parts[0], parts[1], parts[2], parts[3]
    ))))
}

pub(super) fn eval_pg_rust_test_int44out(values: &[Value]) -> Result<Value, ExecError> {
    let [input] = values else {
        return Err(ExecError::TypeMismatch {
            op: "pg_rust_test_int44out",
            left: values.first().cloned().unwrap_or(Value::Null),
            right: Value::Null,
        });
    };
    let Some(input) = input.as_text() else {
        return Ok(Value::Null);
    };
    eval_pg_rust_test_int44in(&[Value::Text(input.into())])
}

pub(super) fn eval_pg_rust_test_pt_in_widget(values: &[Value]) -> Result<Value, ExecError> {
    let [point, widget] = values else {
        return Err(ExecError::TypeMismatch {
            op: "pg_rust_test_pt_in_widget",
            left: values.first().cloned().unwrap_or(Value::Null),
            right: values.get(1).cloned().unwrap_or(Value::Null),
        });
    };
    let (Value::Point(point), Some(widget)) = (point, widget.as_text()) else {
        return Ok(Value::Null);
    };
    let (x, y, radius) = parse_test_widget_text(widget)?;
    Ok(Value::Bool((point.x - x).hypot(point.y - y) < radius))
}

fn parse_test_widget_text(input: &str) -> Result<(f64, f64, f64), ExecError> {
    let inner = input
        .trim()
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
        .unwrap_or(input.trim());
    let parts = inner.split(',').map(str::trim).collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(invalid_test_widget_input(input));
    }
    let x = parts[0]
        .parse::<f64>()
        .map_err(|_| invalid_test_widget_input(input))?;
    let y = parts[1]
        .parse::<f64>()
        .map_err(|_| invalid_test_widget_input(input))?;
    let radius = parts[2]
        .parse::<f64>()
        .map_err(|_| invalid_test_widget_input(input))?;
    Ok((x, y, radius))
}

fn invalid_test_widget_input(input: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("invalid input syntax for type widget: \"{input}\""),
        detail: None,
        hint: None,
        sqlstate: "22P02",
    }
}

pub(super) fn eval_pg_rust_test_enc_conversion(values: &[Value]) -> Result<Value, ExecError> {
    let [string, src_encoding, dst_encoding, no_error] = values else {
        return Err(ExecError::TypeMismatch {
            op: "pg_rust_test_enc_conversion",
            left: values.first().cloned().unwrap_or(Value::Null),
            right: values.get(1).cloned().unwrap_or(Value::Null),
        });
    };
    if matches!(string, Value::Null)
        || matches!(src_encoding, Value::Null)
        || matches!(dst_encoding, Value::Null)
        || matches!(no_error, Value::Null)
    {
        return Ok(Value::Null);
    }

    let bytes = match string {
        Value::Bytea(bytes) => bytes.as_slice(),
        _ => {
            return Err(ExecError::TypeMismatch {
                op: "pg_rust_test_enc_conversion",
                left: string.clone(),
                right: src_encoding.clone(),
            });
        }
    };
    let src_name = src_encoding
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "pg_rust_test_enc_conversion",
            left: src_encoding.clone(),
            right: dst_encoding.clone(),
        })?;
    let dst_name = dst_encoding
        .as_text()
        .ok_or_else(|| ExecError::TypeMismatch {
            op: "pg_rust_test_enc_conversion",
            left: dst_encoding.clone(),
            right: no_error.clone(),
        })?;
    let no_error = match no_error {
        Value::Bool(value) => *value,
        _ => {
            return Err(ExecError::TypeMismatch {
                op: "pg_rust_test_enc_conversion",
                left: no_error.clone(),
                right: Value::Bool(false),
            });
        }
    };

    let src =
        lookup_pg_encoding(src_name).ok_or_else(|| invalid_encoding_name("source", src_name))?;
    let dst = lookup_pg_encoding(dst_name)
        .ok_or_else(|| invalid_encoding_name("destination", dst_name))?;

    let prefix = decode_valid_prefix(bytes, src);
    match prefix.status {
        DecodePrefixStatus::InvalidSource => {
            if !no_error {
                return Err(ExecError::DetailedError {
                    message: format!(
                        "invalid byte sequence for encoding \"{}\"",
                        src_name.to_ascii_uppercase()
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "22021",
                });
            }
            return Ok(build_test_enc_conversion_record(
                prefix.valid_bytes,
                prefix.encoded_prefix,
            ));
        }
        DecodePrefixStatus::Valid => {}
    }

    if std::ptr::eq(src, dst) {
        return Ok(build_test_enc_conversion_record(
            bytes.len(),
            bytes.to_vec(),
        ));
    }

    match encode_without_replacement(&prefix.decoded, dst) {
        Ok(encoded) => Ok(build_test_enc_conversion_record(bytes.len(), encoded)),
        Err(EncodeFailure::Unmappable { utf8_bytes_read }) if no_error => {
            let converted_bytes = if src == encoding_rs::UTF_8 {
                utf8_bytes_read
            } else {
                return Err(ExecError::DetailedError {
                    message: format!(
                        "unsupported partial conversion from encoding \"{}\"",
                        src_name.to_ascii_uppercase()
                    ),
                    detail: Some(
                        "pgrust currently only reports partial progress for unmappable output when the source encoding is UTF8"
                            .into(),
                    ),
                    hint: None,
                    sqlstate: "0A000",
                });
            };
            Ok(build_test_enc_conversion_record(
                converted_bytes,
                encode_without_replacement(&prefix.decoded[..utf8_bytes_read], dst)
                    .expect("prefix must remain encodable"),
            ))
        }
        Err(EncodeFailure::Unmappable { .. }) => Err(ExecError::DetailedError {
            message: format!(
                "character is not representable in encoding \"{}\"",
                dst_name.to_ascii_uppercase()
            ),
            detail: None,
            hint: None,
            sqlstate: "22P05",
        }),
    }
}

fn invalid_encoding_name(kind: &str, name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("invalid {kind} encoding name \"{name}\""),
        detail: None,
        hint: None,
        sqlstate: "22023",
    }
}

fn lookup_pg_encoding(name: &str) -> Option<&'static Encoding> {
    let normalized = normalize_encoding_label(name);
    let canonical = match normalized.as_str() {
        "utf8" | "utf-8" => "utf-8",
        "euc-kr" => "euc-kr",
        "big5" => "big5",
        "gb18030" => "gb18030",
        "euc-jp" => "euc-jp",
        "sjis" | "shift-jis" | "shiftjis" | "shiftjis2004" => "shift_jis",
        "latin2" => "iso-8859-2",
        "latin5" => "iso-8859-9",
        "iso8859-5" | "iso-8859-5" | "iso8859_5" => "iso-8859-5",
        "koi8r" | "koi8-r" => "koi8-r",
        _ => normalized.as_str(),
    };
    Encoding::for_label(canonical.as_bytes())
}

enum DecodePrefixStatus {
    Valid,
    InvalidSource,
}

struct DecodePrefix {
    status: DecodePrefixStatus,
    valid_bytes: usize,
    decoded: String,
    encoded_prefix: Vec<u8>,
}

fn decode_valid_prefix(input: &[u8], encoding: &'static Encoding) -> DecodePrefix {
    let nul_index = input.iter().position(|byte| *byte == 0);
    let candidate = nul_index.map_or(input, |index| &input[..index]);
    let mut decoder = encoding.new_decoder_without_bom_handling();
    let mut decoded = String::new();
    let max_len = candidate.len().saturating_mul(4).max(4);
    let mut buffer = vec![0; max_len.max(4)];
    let (result, read, written) =
        decoder.decode_to_utf8_without_replacement(candidate, &mut buffer, true);
    decoded.push_str(std::str::from_utf8(&buffer[..written]).expect("decoder emitted utf8"));
    match result {
        DecoderResult::InputEmpty if nul_index.is_none() => DecodePrefix {
            status: DecodePrefixStatus::Valid,
            valid_bytes: input.len(),
            decoded,
            encoded_prefix: input.to_vec(),
        },
        DecoderResult::InputEmpty => DecodePrefix {
            status: DecodePrefixStatus::InvalidSource,
            valid_bytes: nul_index.expect("nul exists"),
            decoded,
            encoded_prefix: input[..nul_index.expect("nul exists")].to_vec(),
        },
        DecoderResult::Malformed(_, _) => DecodePrefix {
            status: DecodePrefixStatus::InvalidSource,
            valid_bytes: read,
            decoded,
            encoded_prefix: input[..read].to_vec(),
        },
        DecoderResult::OutputFull => unreachable!("buffer sized from max_utf8_buffer_length"),
    }
}

#[derive(Debug)]
enum EncodeFailure {
    Unmappable { utf8_bytes_read: usize },
}

fn encode_without_replacement(
    input: &str,
    encoding: &'static Encoding,
) -> Result<Vec<u8>, EncodeFailure> {
    let mut encoder = encoding.new_encoder();
    let max_len = input.len().saturating_mul(4).max(4);
    let mut buffer = vec![0; max_len.max(4)];
    let (result, read, written) =
        encoder.encode_from_utf8_without_replacement(input, &mut buffer, true);
    match result {
        EncoderResult::InputEmpty => {
            buffer.truncate(written);
            Ok(buffer)
        }
        EncoderResult::Unmappable(_) => Err(EncodeFailure::Unmappable {
            utf8_bytes_read: read,
        }),
        EncoderResult::OutputFull => unreachable!("buffer sized from max_buffer_length"),
    }
}

fn build_test_enc_conversion_record(validlen: usize, result: Vec<u8>) -> Value {
    let descriptor = assign_anonymous_record_descriptor(vec![
        ("validlen".into(), SqlType::new(SqlTypeKind::Int4)),
        ("result".into(), SqlType::new(SqlTypeKind::Bytea)),
    ]);
    Value::Record(crate::include::nodes::datum::RecordValue::from_descriptor(
        descriptor,
        vec![Value::Int32(validlen as i32), Value::Bytea(result)],
    ))
}
fn eval_pad_function(op: &'static str, values: &[Value], left: bool) -> Result<Value, ExecError> {
    let Some(text_value) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(len_value) = values.get(1) else {
        return Ok(Value::Null);
    };
    if matches!(text_value, Value::Null) || matches!(len_value, Value::Null) {
        return Ok(Value::Null);
    }
    let text = expect_text_arg(op, text_value, len_value)?;
    let target_len = expect_i32_arg(op, len_value, text_value)?;
    let fill = match values.get(2) {
        Some(Value::Null) => return Ok(Value::Null),
        Some(value) => expect_text_arg(op, value, text_value)?,
        None => " ",
    };
    if target_len <= 0 {
        return Ok(Value::Text(CompactString::new("")));
    }
    let mut chars: Vec<char> = text.chars().collect();
    let target_len = target_len as usize;
    if chars.len() >= target_len {
        if left {
            chars.truncate(target_len);
        } else {
            chars = chars[..target_len].to_vec();
        }
        return Ok(Value::Text(CompactString::from_owned(
            chars.into_iter().collect(),
        )));
    }
    if fill.is_empty() {
        return Ok(Value::Text(CompactString::from_owned(
            chars.into_iter().collect(),
        )));
    }
    let fill_chars: Vec<char> = fill.chars().collect();
    let mut needed = target_len - chars.len();
    let mut pad = Vec::with_capacity(needed);
    while needed > 0 {
        for ch in &fill_chars {
            if needed == 0 {
                break;
            }
            pad.push(*ch);
            needed -= 1;
        }
    }
    let out: String = if left {
        pad.into_iter().chain(chars).collect()
    } else {
        chars.into_iter().chain(pad).collect()
    };
    Ok(Value::Text(CompactString::from_owned(out)))
}

fn eval_sha2_function<D>(op: &'static str, values: &[Value]) -> Result<Value, ExecError>
where
    D: sha2::Digest,
{
    let Some(value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    Ok(Value::Bytea(
        D::digest(coerce_hash_bytes(op, value)?).to_vec(),
    ))
}

fn coerce_hash_bytes(op: &'static str, value: &Value) -> Result<Vec<u8>, ExecError> {
    match value {
        Value::Text(text) => Ok(text.as_bytes().to_vec()),
        Value::TextRef(_, _) => Ok(value.as_text().unwrap().as_bytes().to_vec()),
        Value::Bytea(bytes) => Ok(bytes.clone()),
        other => Err(ExecError::TypeMismatch {
            op,
            left: other.clone(),
            right: Value::Null,
        }),
    }
}

fn encode_hex_bytes(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn decode_hex_blob(text: &str) -> Result<Vec<u8>, ExecError> {
    if text.len() % 2 != 0 {
        return Err(ExecError::RaiseException(
            "invalid hexadecimal data: odd number of digits".into(),
        ));
    }
    text.as_bytes()
        .chunks_exact(2)
        .map(|chunk| {
            let pair = std::str::from_utf8(chunk)
                .map_err(|_| ExecError::RaiseException("invalid hexadecimal digit".into()))?;
            u8::from_str_radix(pair, 16)
                .map_err(|_| ExecError::RaiseException("invalid hexadecimal digit".into()))
        })
        .collect()
}

fn expect_text_arg<'a>(
    op: &'static str,
    value: &'a Value,
    right: &Value,
) -> Result<&'a str, ExecError> {
    value.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op,
        left: value.clone(),
        right: right.clone(),
    })
}

fn expect_bytea_arg<'a>(
    op: &'static str,
    value: &'a Value,
    right: &Value,
) -> Result<&'a [u8], ExecError> {
    match value {
        Value::Bytea(bytes) => Ok(bytes.as_slice()),
        other => Err(ExecError::TypeMismatch {
            op,
            left: other.clone(),
            right: right.clone(),
        }),
    }
}

fn expect_i32_arg(op: &'static str, value: &Value, left: &Value) -> Result<i32, ExecError> {
    match value {
        Value::Int32(v) => Ok(*v),
        other => Err(ExecError::TypeMismatch {
            op,
            left: left.clone(),
            right: other.clone(),
        }),
    }
}

fn validate_bytea_index(bytes: &[u8], index: i32, _op: &'static str) -> Result<(), ExecError> {
    if !(0..bytes.len() as i32).contains(&index) {
        return Err(ExecError::RaiseException(format!(
            "index {index} out of valid range, 0..{}",
            bytes.len().saturating_sub(1)
        )));
    }
    Ok(())
}

fn validate_bytea_bit_index(bytes: &[u8], index: i32) -> Result<(), ExecError> {
    let max_index = (bytes.len() as i32).saturating_mul(8).saturating_sub(1);
    if index < 0 || index > max_index {
        return Err(ExecError::RaiseException(format!(
            "index {index} out of valid range, 0..{max_index}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        eval_like, eval_pg_size_bytes_function, eval_pg_size_pretty_function, eval_to_char_function,
    };
    use crate::backend::executor::ExecError;
    use crate::backend::libpq::pqformat::format_exec_error;
    use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
    use crate::backend::utils::time::timestamp::parse_timestamptz_text;
    use crate::include::catalog::{C_COLLATION_OID, DEFAULT_COLLATION_OID, POSIX_COLLATION_OID};
    use crate::include::nodes::datum::{NumericValue, Value};

    #[test]
    fn eval_like_accepts_builtin_collations() {
        for oid in [DEFAULT_COLLATION_OID, C_COLLATION_OID, POSIX_COLLATION_OID] {
            assert_eq!(
                eval_like(
                    &Value::Text("alpha".into()),
                    &Value::Text("a%".into()),
                    None,
                    Some(oid),
                    false,
                    false,
                )
                .unwrap(),
                Value::Bool(true)
            );
        }
    }

    #[test]
    fn eval_like_rejects_unsupported_collation_oid() {
        assert!(matches!(
            eval_like(
                &Value::Text("alpha".into()),
                &Value::Text("a%".into()),
                None,
                Some(123_456),
                false,
                false,
            ),
            Err(ExecError::DetailedError { sqlstate, .. }) if sqlstate == "0A000"
        ));
    }

    #[test]
    fn pg_size_pretty_formats_bigint_and_numeric_inputs() {
        assert_eq!(
            eval_pg_size_pretty_function(&[Value::Int64(1_000_000)]).unwrap(),
            Value::Text("977 kB".into())
        );
        assert_eq!(
            eval_pg_size_pretty_function(&[Value::Numeric(NumericValue::from("1000.5"))]).unwrap(),
            Value::Text("1000.5 bytes".into())
        );
        assert_eq!(
            eval_pg_size_pretty_function(&[Value::Numeric(NumericValue::from("1000000.5"))])
                .unwrap(),
            Value::Text("977 kB".into())
        );
    }

    #[test]
    fn pg_size_bytes_parses_units_and_reports_invalid_inputs() {
        assert_eq!(
            eval_pg_size_bytes_function(&[Value::Text("1.5 GB ".into())]).unwrap(),
            Value::Int64(1_610_612_736)
        );
        assert_eq!(
            eval_pg_size_bytes_function(&[Value::Text("-.1 kb".into())]).unwrap(),
            Value::Int64(-102)
        );

        let err = eval_pg_size_bytes_function(&[Value::Text("1 AB".into())]).unwrap_err();
        assert_eq!(format_exec_error(&err), "invalid size: \"1 AB\"");
        assert!(matches!(
            err,
            ExecError::DetailedError {
                detail: Some(detail),
                hint: Some(_),
                sqlstate,
                ..
            } if detail == "Invalid size unit: \"AB\"." && sqlstate == "22023"
        ));
    }

    #[test]
    fn to_char_formats_timestamptz_timezone_tokens() {
        let la = DateTimeConfig {
            time_zone: "America/Los_Angeles".into(),
            ..DateTimeConfig::default()
        };
        let timestamp = parse_timestamptz_text("2012-12-12 12:00", &la).unwrap();
        assert_eq!(
            eval_to_char_function(
                &[
                    Value::TimestampTz(timestamp),
                    Value::Text("YYYY-MM-DD HH:MI:SS TZ".into()),
                ],
                &la,
            )
            .unwrap(),
            Value::Text("2012-12-12 12:00:00 PST".into())
        );
        assert_eq!(
            eval_to_char_function(
                &[
                    Value::TimestampTz(timestamp),
                    Value::Text("YYYY-MM-DD HH:MI:SS tz".into()),
                ],
                &la,
            )
            .unwrap(),
            Value::Text("2012-12-12 12:00:00 pst".into())
        );

        let fixed = DateTimeConfig {
            time_zone: "+02".into(),
            ..DateTimeConfig::default()
        };
        let timestamp = parse_timestamptz_text("2012-12-12 12:00", &fixed).unwrap();
        assert_eq!(
            eval_to_char_function(
                &[
                    Value::TimestampTz(timestamp),
                    Value::Text("YYYY-MM-DD HH:MI:SS TZ".into()),
                ],
                &fixed,
            )
            .unwrap(),
            Value::Text("2012-12-12 12:00:00 +02".into())
        );
        assert_eq!(
            eval_to_char_function(
                &[
                    Value::TimestampTz(timestamp),
                    Value::Text("YYYY-MM-DD SSSSS".into()),
                ],
                &fixed,
            )
            .unwrap(),
            Value::Text("2012-12-12 43200".into())
        );
    }
}
