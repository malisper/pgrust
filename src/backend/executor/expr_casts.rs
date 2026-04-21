use super::ExecError;
use super::exec_expr::parse_numeric_text;
use super::expr_bit::{coerce_bit_string, parse_bit_text, render_bit_text};
use super::expr_bool::cast_integer_to_bool;
use super::expr_bool::parse_pg_bool_text;
use super::expr_datetime::{apply_time_precision, render_datetime_value_text_with_config};
use super::expr_geometry::{
    cast_geometry_value, geometry_input_error_message, parse_geometry_text,
};
use super::expr_json::{canonicalize_jsonpath_text, validate_json_text};
use super::expr_money::{
    money_format_text, money_from_float, money_numeric_text, money_parse_text,
};
use super::expr_range::{parse_range_text, render_range_text};
use super::node_types::*;
use crate::backend::executor::jsonb::{
    parse_json_text_input, parse_jsonb_text, parse_jsonb_text_with_limit, render_jsonb_bytes,
};
use crate::backend::parser::{SqlType, SqlTypeKind, parse_type_name};
use crate::backend::utils::misc::guc_datetime::DateTimeConfig;
use crate::backend::utils::time::date::{
    DateParseError, parse_date_text, parse_time_text, parse_timetz_text,
};
use crate::backend::utils::time::datetime::DateTimeParseError;
use crate::backend::utils::time::timestamp::{parse_timestamp_text, parse_timestamptz_text};
use crate::include::catalog::{
    TEXT_TYPE_OID, bootstrap_pg_cast_rows, builtin_type_rows, range_type_ref_for_sql_type,
};
use crate::pgrust::compact_string::CompactString;
use num_integer::Integer;
use num_traits::{Signed, Zero};
use std::collections::BTreeSet;
use std::sync::OnceLock;

pub(crate) struct InputErrorInfo {
    pub(crate) message: String,
    pub(crate) detail: Option<String>,
    pub(crate) hint: Option<String>,
    pub(crate) sqlstate: &'static str,
}

fn unsupported_anyarray_input() -> ExecError {
    ExecError::DetailedError {
        message: "cannot accept a value of type anyarray".into(),
        detail: None,
        hint: None,
        sqlstate: "0A000",
    }
}

fn unsupported_record_input() -> ExecError {
    ExecError::DetailedError {
        message: "cannot accept a value of type record".into(),
        detail: None,
        hint: None,
        sqlstate: "0A000",
    }
}

fn unsupported_trigger_input() -> ExecError {
    ExecError::DetailedError {
        message: "cannot accept a value of type trigger".into(),
        detail: None,
        hint: None,
        sqlstate: "0A000",
    }
}

fn parse_pg_integer_text(text: &str, ty: &'static str) -> Result<i128, ExecError> {
    let trimmed = text.trim_matches(|ch: char| ch.is_ascii_whitespace());
    if trimmed.is_empty() {
        return Err(ExecError::InvalidIntegerInput {
            ty,
            value: text.to_string(),
        });
    }

    let (negative, rest) = if let Some(rest) = trimmed.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = trimmed.strip_prefix('+') {
        (false, rest)
    } else {
        (false, trimmed)
    };

    let (base, digits, allow_prefix_underscore) =
        if let Some(rest) = rest.strip_prefix("0b").or_else(|| rest.strip_prefix("0B")) {
            (2, rest, true)
        } else if let Some(rest) = rest.strip_prefix("0o").or_else(|| rest.strip_prefix("0O")) {
            (8, rest, true)
        } else if let Some(rest) = rest.strip_prefix("0x").or_else(|| rest.strip_prefix("0X")) {
            (16, rest, true)
        } else {
            (10, rest, false)
        };

    let digits = if allow_prefix_underscore {
        digits.strip_prefix('_').unwrap_or(digits)
    } else {
        digits
    };
    if digits.is_empty()
        || digits.starts_with('_')
        || digits.ends_with('_')
        || digits.contains("__")
    {
        return Err(ExecError::InvalidIntegerInput {
            ty,
            value: text.to_string(),
        });
    }

    let normalized: String = digits.chars().filter(|&ch| ch != '_').collect();
    if normalized.is_empty() || !normalized.chars().all(|ch| ch.is_digit(base)) {
        return Err(ExecError::InvalidIntegerInput {
            ty,
            value: text.to_string(),
        });
    }

    let magnitude =
        i128::from_str_radix(&normalized, base).map_err(|_| ExecError::InvalidIntegerInput {
            ty,
            value: text.to_string(),
        })?;
    Ok(if negative { -magnitude } else { magnitude })
}

fn cast_text_to_int2(text: &str) -> Result<Value, ExecError> {
    let value = parse_pg_integer_text(text, "smallint")?;
    i16::try_from(value)
        .map(Value::Int16)
        .map_err(|_| ExecError::IntegerOutOfRange {
            ty: "smallint",
            value: text.to_string(),
        })
}

fn cast_text_to_int4(text: &str) -> Result<Value, ExecError> {
    let value = parse_pg_integer_text(text, "integer")?;
    i32::try_from(value)
        .map(Value::Int32)
        .map_err(|_| ExecError::IntegerOutOfRange {
            ty: "integer",
            value: text.to_string(),
        })
}

fn cast_text_to_int8(text: &str) -> Result<Value, ExecError> {
    let value = parse_pg_integer_text(text, "bigint")?;
    i64::try_from(value)
        .map(Value::Int64)
        .map_err(|_| ExecError::IntegerOutOfRange {
            ty: "bigint",
            value: text.to_string(),
        })
}

fn cast_text_to_oid(text: &str) -> Result<Value, ExecError> {
    let value = parse_pg_integer_text(text, "oid")?;
    let oid = if (0..=u32::MAX as i128).contains(&value) {
        value as u32
    } else if (i32::MIN as i128..=-1).contains(&value) {
        (value as i32) as u32
    } else {
        return Err(ExecError::IntegerOutOfRange {
            ty: "oid",
            value: text.to_string(),
        });
    };
    Ok(Value::Int64(oid as i64))
}

fn cast_text_to_xid(text: &str) -> Result<Value, ExecError> {
    let value = parse_pg_integer_text(text, "xid")?;
    let xid = u32::try_from(value).map_err(|_| ExecError::IntegerOutOfRange {
        ty: "xid",
        value: text.to_string(),
    })?;
    Ok(Value::Int64(xid as i64))
}

const NUMERIC_MAX_INPUT_DIGITS_BEFORE_DECIMAL: i32 = 131072;

fn numeric_typmod_overflow_error(precision: i32, scale: i32) -> ExecError {
    let limit = precision - scale;
    let detail = if limit == 0 {
        format!(
            "A field with precision {precision}, scale {scale} must round to an absolute value less than 1."
        )
    } else {
        format!(
            "A field with precision {precision}, scale {scale} must round to an absolute value less than 10^{limit}."
        )
    };
    ExecError::DetailedError {
        message: "numeric field overflow".into(),
        detail: Some(detail),
        hint: None,
        sqlstate: "22003",
    }
}

fn numeric_typmod_infinity_error(precision: i32, scale: i32) -> ExecError {
    ExecError::DetailedError {
        message: "numeric field overflow".into(),
        detail: Some(format!(
            "A field with precision {precision}, scale {scale} cannot hold an infinite value."
        )),
        hint: None,
        sqlstate: "22003",
    }
}

fn parse_numeric_input_exponent(text: &str) -> Option<i32> {
    let (negative, digits) = if let Some(rest) = text.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = text.strip_prefix('+') {
        (false, rest)
    } else {
        (false, text)
    };
    if digits.is_empty()
        || digits.starts_with('_')
        || digits.ends_with('_')
        || digits.contains("__")
    {
        return None;
    }
    let normalized: String = digits.chars().filter(|&ch| ch != '_').collect();
    if normalized.is_empty() || !normalized.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    let value = normalized.parse::<i32>().ok()?;
    Some(if negative { -value } else { value })
}

fn normalize_numeric_input_digits(
    digits: &str,
    valid_digit: impl Fn(char) -> bool,
) -> Option<String> {
    if digits.is_empty()
        || digits.starts_with('_')
        || digits.ends_with('_')
        || digits.contains("__")
    {
        return None;
    }
    let normalized: String = digits.chars().filter(|&ch| ch != '_').collect();
    if normalized.is_empty() || !normalized.chars().all(valid_digit) {
        return None;
    }
    Some(normalized)
}

fn numeric_input_would_overflow(text: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("nan")
        || matches!(
            trimmed.to_ascii_lowercase().as_str(),
            "inf" | "+inf" | "infinity" | "+infinity" | "-inf" | "-infinity"
        )
        || trimmed.chars().any(|ch| ch.is_ascii_whitespace())
    {
        return false;
    }

    let unsigned = trimmed.strip_prefix(['+', '-']).unwrap_or(trimmed);

    if let Some(rest) = unsigned
        .strip_prefix("0x")
        .or_else(|| unsigned.strip_prefix("0X"))
    {
        let rest = rest.strip_prefix('_').unwrap_or(rest);
        let Some(digits) = normalize_numeric_input_digits(rest, |ch| ch.is_ascii_hexdigit()) else {
            return false;
        };
        return digits.trim_start_matches('0').len() as i32
            > NUMERIC_MAX_INPUT_DIGITS_BEFORE_DECIMAL;
    }
    if let Some(rest) = unsigned
        .strip_prefix("0o")
        .or_else(|| unsigned.strip_prefix("0O"))
    {
        let rest = rest.strip_prefix('_').unwrap_or(rest);
        let Some(digits) = normalize_numeric_input_digits(rest, |ch| matches!(ch, '0'..='7'))
        else {
            return false;
        };
        return digits.trim_start_matches('0').len() as i32
            > NUMERIC_MAX_INPUT_DIGITS_BEFORE_DECIMAL;
    }
    if let Some(rest) = unsigned
        .strip_prefix("0b")
        .or_else(|| unsigned.strip_prefix("0B"))
    {
        let rest = rest.strip_prefix('_').unwrap_or(rest);
        let Some(digits) = normalize_numeric_input_digits(rest, |ch| matches!(ch, '0' | '1'))
        else {
            return false;
        };
        return digits.trim_start_matches('0').len() as i32
            > NUMERIC_MAX_INPUT_DIGITS_BEFORE_DECIMAL;
    }

    let (mantissa, exponent) = match trimmed.find(['e', 'E']) {
        Some(index) => {
            let Some(exponent) = parse_numeric_input_exponent(&trimmed[index + 1..]) else {
                return false;
            };
            (&trimmed[..index], exponent)
        }
        None => (trimmed, 0),
    };
    let unsigned_mantissa = mantissa.strip_prefix(['+', '-']).unwrap_or(mantissa);
    let parts: Vec<&str> = unsigned_mantissa.split('.').collect();
    if parts.len() > 2 {
        return false;
    }
    let whole = parts[0];
    let frac = parts.get(1).copied().unwrap_or("");
    if whole.is_empty() && frac.is_empty() {
        return false;
    }
    let Some(whole) = normalize_numeric_input_digits(whole, |ch| ch.is_ascii_digit())
        .or_else(|| whole.is_empty().then(String::new))
    else {
        return false;
    };
    let Some(frac) = normalize_numeric_input_digits(frac, |ch| ch.is_ascii_digit())
        .or_else(|| frac.is_empty().then(String::new))
    else {
        return false;
    };
    let digits = format!("{whole}{frac}");
    let significant = digits.trim_start_matches('0');
    if significant.is_empty() {
        return false;
    }
    let leading_zero_count = (digits.len() - significant.len()) as i32;
    let digits_before_decimal = whole.len() as i32 + exponent - leading_zero_count;
    digits_before_decimal > NUMERIC_MAX_INPUT_DIGITS_BEFORE_DECIMAL
}

fn parse_numeric_input_value(
    text: &str,
) -> Result<crate::include::nodes::datum::NumericValue, ExecError> {
    if numeric_input_would_overflow(text) {
        return Err(ExecError::NumericFieldOverflow);
    }
    parse_numeric_text(text).ok_or_else(|| ExecError::InvalidNumericInput(text.to_string()))
}

fn canonicalize_tid_text(text: &str) -> Result<String, ExecError> {
    let trimmed = text.trim();
    let inner = trimmed
        .strip_prefix('(')
        .and_then(|rest| rest.strip_suffix(')'))
        .ok_or_else(|| ExecError::DetailedError {
            message: format!("invalid input syntax for type tid: \"{text}\""),
            detail: None,
            hint: None,
            sqlstate: "22P02",
        })?;
    let (block, offset) = inner
        .split_once(',')
        .ok_or_else(|| ExecError::DetailedError {
            message: format!("invalid input syntax for type tid: \"{text}\""),
            detail: None,
            hint: None,
            sqlstate: "22P02",
        })?;
    let block_number = block
        .trim()
        .parse::<u32>()
        .map_err(|_| ExecError::DetailedError {
            message: format!("invalid input syntax for type tid: \"{text}\""),
            detail: None,
            hint: None,
            sqlstate: "22P02",
        })?;
    let offset_number = offset
        .trim()
        .parse::<u16>()
        .map_err(|_| ExecError::DetailedError {
            message: format!("invalid input syntax for type tid: \"{text}\""),
            detail: None,
            hint: None,
            sqlstate: "22P02",
        })?;
    Ok(format!("({block_number},{offset_number})"))
}

fn canonicalize_interval_text(text: &str) -> Result<String, ExecError> {
    fn invalid(text: &str) -> ExecError {
        ExecError::DetailedError {
            message: format!("invalid input syntax for type interval: \"{text}\""),
            detail: None,
            hint: None,
            sqlstate: "22P02",
        }
    }

    fn unit_suffix(value: i64, singular: &str, plural: &str) -> String {
        if value == 1 {
            format!("{value} {singular}")
        } else {
            format!("{value} {plural}")
        }
    }

    fn render_interval(months: i32, days: i32, micros: i64, negative: bool) -> String {
        let mut parts = Vec::new();
        let years = months / 12;
        let rem_months = months % 12;
        if years != 0 {
            parts.push(unit_suffix(i64::from(years), "year", "years"));
        }
        if rem_months != 0 {
            parts.push(unit_suffix(i64::from(rem_months), "mon", "mons"));
        }
        if days != 0 {
            parts.push(unit_suffix(i64::from(days), "day", "days"));
        }

        let total_seconds = micros / 1_000_000;
        let subsec = micros % 1_000_000;
        let hours = total_seconds / 3600;
        let minutes = (total_seconds % 3600) / 60;
        let seconds = total_seconds % 60;
        if hours != 0 {
            parts.push(unit_suffix(hours, "hour", "hours"));
        }
        if minutes != 0 {
            parts.push(unit_suffix(minutes, "min", "mins"));
        }
        if seconds != 0 || subsec != 0 || parts.is_empty() {
            let seconds_text = if subsec == 0 {
                seconds.to_string()
            } else {
                let mut rendered = format!("{seconds}.{subsec:06}");
                while rendered.ends_with('0') {
                    rendered.pop();
                }
                rendered
            };
            let label = if seconds_text == "1" { "sec" } else { "secs" };
            parts.push(format!("{seconds_text} {label}"));
        }

        let mut out = format!("@ {}", parts.join(" "));
        if negative && out != "@ 0 secs" {
            out.push_str(" ago");
        }
        out
    }

    let mut rest = text.trim();
    if rest.is_empty() {
        return Err(invalid(text));
    }

    let mut negative = false;
    if let Some(stripped) = rest.strip_prefix('-') {
        negative = true;
        rest = stripped.trim();
    } else if let Some(stripped) = rest.strip_prefix('+') {
        rest = stripped.trim();
    }
    if let Some(stripped) = rest.strip_prefix('@') {
        rest = stripped.trim();
    }
    if let Some(stripped) = rest.strip_suffix("ago") {
        negative = true;
        rest = stripped.trim();
    }
    if rest.is_empty() {
        return Err(invalid(text));
    }

    if rest.contains(':') {
        let parsed = parse_time_text(rest).ok_or_else(|| invalid(text))?;
        return Ok(render_interval(0, 0, parsed.0, negative));
    }

    let tokens = rest.split_whitespace().collect::<Vec<_>>();
    if tokens.len() % 2 != 0 {
        return Err(invalid(text));
    }

    let mut months = 0i32;
    let mut days = 0i32;
    let mut micros = 0i64;
    for pair in tokens.chunks(2) {
        match pair[1].to_ascii_lowercase().as_str() {
            "year" | "years" => {
                let value = pair[0].parse::<i64>().map_err(|_| invalid(text))?;
                months += i32::try_from(value * 12).map_err(|_| invalid(text))?
            }
            "mon" | "mons" | "month" | "months" => {
                let value = pair[0].parse::<i64>().map_err(|_| invalid(text))?;
                months += i32::try_from(value).map_err(|_| invalid(text))?
            }
            "day" | "days" => {
                let value = pair[0].parse::<i64>().map_err(|_| invalid(text))?;
                days += i32::try_from(value).map_err(|_| invalid(text))?
            }
            "hour" | "hours" => {
                let value = pair[0].parse::<i64>().map_err(|_| invalid(text))?;
                micros += value.saturating_mul(3_600_000_000)
            }
            "min" | "mins" | "minute" | "minutes" => {
                let value = pair[0].parse::<i64>().map_err(|_| invalid(text))?;
                micros += value.saturating_mul(60_000_000)
            }
            "sec" | "secs" | "second" | "seconds" => {
                let value = pair[0].parse::<f64>().map_err(|_| invalid(text))?;
                micros += (value * 1_000_000.0).round() as i64
            }
            _ => return Err(invalid(text)),
        }
    }

    Ok(render_interval(months, days, micros, negative))
}

pub(crate) fn parse_bytea_text(text: &str) -> Result<Vec<u8>, ExecError> {
    if let Some(rest) = text.strip_prefix("\\x") {
        let mut out = Vec::with_capacity(rest.len() / 2);
        let mut high_nibble = None;
        for ch in rest.chars() {
            if ch.is_ascii_whitespace() {
                continue;
            }
            if !ch.is_ascii_hexdigit() {
                return Err(ExecError::InvalidByteaHexDigit {
                    value: text.to_string(),
                    digit: ch.to_string(),
                });
            }
            let nibble = ch.to_digit(16).expect("ASCII hexadecimal digit must parse") as u8;
            if let Some(high) = high_nibble.take() {
                out.push((high << 4) | nibble);
            } else {
                high_nibble = Some(nibble);
            }
        }
        if high_nibble.is_some() {
            return Err(ExecError::InvalidByteaHexOddDigits {
                value: text.to_string(),
            });
        }
        return Ok(out);
    }

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
            continue;
        }
        if idx + 2 >= bytes.len()
            || !(b'0'..=b'7').contains(&bytes[idx])
            || !(b'0'..=b'7').contains(&bytes[idx + 1])
            || !(b'0'..=b'7').contains(&bytes[idx + 2])
        {
            return Err(ExecError::InvalidByteaInput {
                value: text.to_string(),
            });
        }
        let value =
            (bytes[idx] - b'0') * 64 + (bytes[idx + 1] - b'0') * 8 + (bytes[idx + 2] - b'0');
        out.push(value);
        idx += 3;
    }
    Ok(out)
}

fn parse_oid_token_prefix(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    if bytes.is_empty() {
        return None;
    }
    let mut idx = 0;
    if matches!(bytes[0], b'+' | b'-') {
        idx += 1;
    }
    let start_digits = idx;
    while idx < bytes.len() && bytes[idx].is_ascii_digit() {
        idx += 1;
    }
    (idx > start_digits).then_some(idx)
}

fn soft_parse_oidvector_input(text: &str) -> Result<Option<InputErrorInfo>, ExecError> {
    let mut remaining = text;
    loop {
        remaining = remaining.trim_start_matches(|ch: char| ch.is_ascii_whitespace());
        if remaining.is_empty() {
            return Ok(None);
        }
        let Some(prefix_len) = parse_oid_token_prefix(remaining) else {
            let err = ExecError::InvalidIntegerInput {
                ty: "oid",
                value: remaining.to_string(),
            };
            return Ok(Some(InputErrorInfo {
                message: input_error_message(&err, remaining),
                detail: None,
                hint: None,
                sqlstate: input_error_sqlstate(&err),
            }));
        };
        let token = &remaining[..prefix_len];
        if let Err(err) = cast_text_to_oid(token) {
            return Ok(Some(InputErrorInfo {
                message: input_error_message(&err, token),
                detail: None,
                hint: None,
                sqlstate: input_error_sqlstate(&err),
            }));
        }
        remaining = &remaining[prefix_len..];
    }
}

fn parse_internal_char_text(text: &str) -> u8 {
    let bytes = text.as_bytes();
    if bytes.is_empty() {
        return 0;
    }
    if bytes.len() == 4 && bytes[0] == b'\\' && bytes[1..].iter().all(|b| (b'0'..=b'7').contains(b))
    {
        return (bytes[1] - b'0') * 64 + (bytes[2] - b'0') * 8 + (bytes[3] - b'0');
    }
    bytes[0]
}

pub fn render_internal_char_text(byte: u8) -> String {
    match byte {
        0 => String::new(),
        1..=127 => char::from(byte).to_string(),
        _ => format!("\\{:03o}", byte),
    }
}

pub(crate) fn parse_text_array_literal(
    raw: &str,
    element_type: SqlType,
) -> Result<Value, ExecError> {
    parse_text_array_literal_with_options(raw, element_type, "::array", true)
}

pub(crate) fn parse_text_array_literal_with_op(
    raw: &str,
    element_type: SqlType,
    op: &'static str,
) -> Result<Value, ExecError> {
    parse_text_array_literal_with_options(raw, element_type, op, true)
}

pub(crate) fn parse_text_array_literal_with_options(
    raw: &str,
    element_type: SqlType,
    op: &'static str,
    explicit: bool,
) -> Result<Value, ExecError> {
    let (bounds, input) = parse_array_bounds_prefix(raw)?;
    if input == "{}" {
        return Ok(Value::PgArray(ArrayValue::empty()));
    }
    if !input.starts_with('{') || !input.ends_with('}') {
        return Err(invalid_array_literal(
            raw,
            Some("Array value must start with \"{\" or dimension information.".into()),
        ));
    }
    let mut parser = ArrayTextParser::new(input, element_type, explicit);
    let value = parser.parse_array()?;
    parser.skip_ws();
    if !parser.is_eof() {
        return Err(invalid_array_literal(
            raw,
            Some("Junk after closing right brace.".into()),
        ));
    }
    let nested = match value {
        Value::Array(values) => values,
        other => {
            return Err(ExecError::TypeMismatch {
                op,
                left: other,
                right: Value::Null,
            });
        }
    };
    let array =
        ArrayValue::from_nested_values(nested, bounds.lower_bounds.clone()).map_err(|_| {
            invalid_array_literal(
                raw,
                Some(
                    "Multidimensional arrays must have sub-arrays with matching dimensions.".into(),
                ),
            )
        })?;
    if let Some(expected_lengths) = &bounds.lengths
        && (expected_lengths.len() != array.dimensions.len()
            || expected_lengths
                .iter()
                .zip(array.dimensions.iter())
                .any(|(expected, actual)| *expected != actual.length))
    {
        return Err(invalid_array_literal(
            raw,
            Some("Specified array dimensions do not match array contents.".into()),
        ));
    }
    Ok(Value::PgArray(array))
}

#[derive(Default)]
struct ParsedArrayBounds {
    lower_bounds: Vec<i32>,
    lengths: Option<Vec<usize>>,
}

fn parse_array_bounds_prefix(raw: &str) -> Result<(ParsedArrayBounds, &str), ExecError> {
    if !raw.starts_with('[') {
        return Ok((ParsedArrayBounds::default(), raw));
    }
    let Some(equals) = raw.find('=') else {
        return Ok((ParsedArrayBounds::default(), raw));
    };
    let bounds = &raw[..equals];
    let mut lower_bounds = Vec::new();
    let mut lengths = Vec::new();
    let mut remaining = bounds;
    while let Some(rest) = remaining.strip_prefix('[') {
        let Some(end) = rest.find(']') else {
            return Err(invalid_array_literal(raw, None));
        };
        let part = &rest[..end];
        let Some((lower, upper)) = part.split_once(':') else {
            return Err(invalid_array_literal(
                raw,
                Some("Specified array dimensions do not match array contents.".into()),
            ));
        };
        if lower.trim().is_empty() {
            return Err(invalid_array_literal(
                raw,
                Some("\"[\" must introduce explicitly-specified array dimensions.".into()),
            ));
        };
        if upper.trim().is_empty() {
            return Err(invalid_array_literal(
                raw,
                Some("Missing array dimension value.".into()),
            ));
        }
        let lower = parse_array_bound(lower.trim(), raw)?;
        let upper = parse_array_bound(upper.trim(), raw)?;
        if upper < lower {
            return Err(ExecError::ArrayInput {
                message: "upper bound cannot be less than lower bound".into(),
                value: raw.into(),
                detail: None,
                sqlstate: "2202E",
            });
        }
        if upper >= i32::MAX as i64 {
            return Err(ExecError::ArrayInput {
                message: format!("array upper bound is too large: {upper}"),
                value: raw.into(),
                detail: None,
                sqlstate: "54000",
            });
        }
        lower_bounds.push(lower as i32);
        lengths.push((upper - lower + 1) as usize);
        remaining = &rest[end + 1..];
    }
    Ok((
        ParsedArrayBounds {
            lower_bounds,
            lengths: Some(lengths),
        },
        &raw[equals + 1..],
    ))
}

fn parse_array_bound(text: &str, raw: &str) -> Result<i64, ExecError> {
    text.parse::<i64>().map_err(|_| ExecError::ArrayInput {
        message: "array bound is out of integer range".into(),
        value: raw.into(),
        detail: None,
        sqlstate: "22003",
    })
}

fn invalid_array_literal(raw: &str, detail: Option<String>) -> ExecError {
    ExecError::ArrayInput {
        message: format!("malformed array literal: \"{raw}\""),
        value: raw.into(),
        detail,
        sqlstate: "22P02",
    }
}

struct ArrayTextParser<'a> {
    input: &'a str,
    offset: usize,
    element_type: SqlType,
    explicit: bool,
}

impl<'a> ArrayTextParser<'a> {
    fn new(input: &'a str, element_type: SqlType, explicit: bool) -> Self {
        Self {
            input,
            offset: 0,
            element_type,
            explicit,
        }
    }

    fn parse_array(&mut self) -> Result<Value, ExecError> {
        self.skip_ws();
        self.expect('{')?;
        let mut items = Vec::new();
        loop {
            self.skip_ws();
            if self.peek_char() == Some('}') {
                self.bump_char();
                break;
            }
            items.push(self.parse_item()?);
            self.skip_ws();
            match self.peek_char() {
                Some(',') => {
                    self.bump_char();
                    self.skip_ws();
                    if self.peek_char() == Some('}') {
                        return Err(invalid_array_literal(
                            self.input,
                            Some("Unexpected \"}\" character.".into()),
                        ));
                    }
                }
                Some('}') => {
                    self.bump_char();
                    break;
                }
                _ => return self.type_mismatch(),
            }
        }
        Ok(Value::Array(items))
    }

    fn parse_item(&mut self) -> Result<Value, ExecError> {
        self.skip_ws();
        match self.peek_char() {
            Some('{') => self.parse_array(),
            Some('"') => {
                let text = self.parse_quoted_string()?;
                self.skip_ws();
                if matches!(self.peek_char(), Some(ch) if !matches!(ch, ',' | '}')) {
                    return Err(invalid_array_literal(
                        self.input,
                        Some("Incorrectly quoted array element.".into()),
                    ));
                }
                cast_text_value(&text, self.element_type, self.explicit)
            }
            Some(_) => {
                let text = self.parse_unquoted_token();
                if text.is_empty() {
                    let detail = match self.peek_char() {
                        Some(',') => "Unexpected \",\" character.",
                        Some('}') => "Unexpected \"}\" character.",
                        _ => "Unexpected array element.",
                    };
                    return Err(invalid_array_literal(self.input, Some(detail.into())));
                }
                if text.contains('{') {
                    return Err(invalid_array_literal(
                        self.input,
                        Some("Unexpected \"{\" character.".into()),
                    ));
                }
                if text.eq_ignore_ascii_case("NULL") {
                    Ok(Value::Null)
                } else {
                    cast_text_value(text.trim_end(), self.element_type, self.explicit)
                }
            }
            None => self.type_mismatch(),
        }
    }

    fn parse_quoted_string(&mut self) -> Result<String, ExecError> {
        self.expect('"')?;
        let mut text = String::new();
        while let Some(ch) = self.bump_char() {
            match ch {
                '"' => return Ok(text),
                '\\' => {
                    let escaped = self
                        .bump_char()
                        .ok_or_else(|| invalid_array_literal(self.input, None))?;
                    text.push(escaped);
                }
                other => text.push(other),
            }
        }
        self.type_mismatch()
    }

    fn parse_unquoted_token(&mut self) -> &'a str {
        let start = self.offset;
        while let Some(ch) = self.peek_char() {
            if matches!(ch, ',' | '}') {
                break;
            }
            self.bump_char();
        }
        &self.input[start..self.offset]
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek_char(), Some(ch) if ch.is_ascii_whitespace()) {
            self.bump_char();
        }
    }

    fn expect(&mut self, expected: char) -> Result<(), ExecError> {
        if self.bump_char() == Some(expected) {
            Ok(())
        } else {
            self.type_mismatch()
        }
    }

    fn peek_char(&self) -> Option<char> {
        self.input[self.offset..].chars().next()
    }

    fn bump_char(&mut self) -> Option<char> {
        let ch = self.peek_char()?;
        self.offset += ch.len_utf8();
        Some(ch)
    }

    fn is_eof(&self) -> bool {
        self.offset >= self.input.len()
    }

    fn type_mismatch<T>(&self) -> Result<T, ExecError> {
        Err(invalid_array_literal(
            self.input,
            Some("Unexpected array element.".into()),
        ))
    }
}

fn parse_input_type_name(type_name: &str) -> Result<Option<SqlType>, ExecError> {
    let parsed = match parse_type_name(type_name.trim()) {
        Ok(ty) => ty,
        Err(_) => return Ok(None),
    };
    let Some(parsed) = parsed.as_builtin() else {
        return Ok(None);
    };
    Ok(input_type_name_supported(parsed).then_some(parsed))
}

fn input_type_name_supported(parsed: SqlType) -> bool {
    if !parsed.is_array && matches!(parsed.kind, SqlTypeKind::Text) {
        return true;
    }
    let Some(type_oid) = builtin_type_oid(parsed) else {
        return false;
    };
    explicit_text_input_target_oids().contains(&type_oid)
}

fn builtin_type_oid(sql_type: SqlType) -> Option<u32> {
    if let Some(range_type) = range_type_ref_for_sql_type(sql_type) {
        return Some(range_type.type_oid());
    }
    builtin_type_rows().into_iter().find_map(|row| {
        (row.sql_type.is_array == sql_type.is_array && row.sql_type.kind == sql_type.kind)
            .then_some(row.oid)
    })
}

fn explicit_text_input_target_oids() -> &'static BTreeSet<u32> {
    static OIDS: OnceLock<BTreeSet<u32>> = OnceLock::new();
    OIDS.get_or_init(|| {
        bootstrap_pg_cast_rows()
            .into_iter()
            .filter(|row| row.castsource == TEXT_TYPE_OID && row.castmethod == 'i')
            .map(|row| row.casttarget)
            .collect()
    })
}

fn input_error_message(err: &ExecError, text: &str) -> String {
    match err {
        ExecError::JsonInput { message, .. } => message.clone(),
        ExecError::XmlInput { message, .. } => message.clone(),
        ExecError::DetailedError { message, .. } => message.clone(),
        ExecError::InvalidIntegerInput { ty, .. } => {
            let value = match err {
                ExecError::InvalidIntegerInput { value, .. } => value.as_str(),
                _ => text,
            };
            format!("invalid input syntax for type {ty}: \"{value}\"")
        }
        ExecError::ArrayInput { message, .. } => message.clone(),
        ExecError::IntegerOutOfRange { ty, value } => {
            format!("value \"{value}\" is out of range for type {ty}")
        }
        ExecError::Int2OutOfRange => {
            format!("value \"{text}\" is out of range for type smallint")
        }
        ExecError::Int4OutOfRange => {
            format!("value \"{text}\" is out of range for type integer")
        }
        ExecError::Int8OutOfRange => {
            format!("value \"{text}\" is out of range for type bigint")
        }
        ExecError::OidOutOfRange => format!("value \"{text}\" is out of range for type oid"),
        ExecError::InvalidNumericInput(_) => {
            let value = match err {
                ExecError::InvalidNumericInput(value) => value.as_str(),
                _ => text,
            };
            format!("invalid input syntax for type numeric: \"{value}\"")
        }
        ExecError::InvalidByteaInput { .. } => "invalid input syntax for type bytea".to_string(),
        ExecError::InvalidByteaHexDigit { digit, .. } => {
            format!("invalid hexadecimal digit: \"{digit}\"")
        }
        ExecError::InvalidByteaHexOddDigits { .. } => {
            "invalid hexadecimal data: odd number of digits".to_string()
        }
        ExecError::InvalidGeometryInput { ty, value } => geometry_input_error_message(ty, value)
            .unwrap_or_else(|| format!("invalid input syntax for type {ty}: \"{value}\"")),
        ExecError::InvalidBitInput { digit, is_hex } => {
            if *is_hex {
                format!("\"{digit}\" is not a valid hexadecimal digit")
            } else {
                format!("\"{digit}\" is not a valid binary digit")
            }
        }
        ExecError::BitStringLengthMismatch { actual, expected } => {
            format!("bit string length {actual} does not match type bit({expected})")
        }
        ExecError::BitStringTooLong { limit, .. } => {
            format!("bit string too long for type bit varying({limit})")
        }
        ExecError::InvalidBooleanInput { value } => {
            format!("invalid input syntax for type boolean: \"{value}\"")
        }
        ExecError::InvalidFloatInput { ty, value } => {
            format!("invalid input syntax for type {ty}: \"{value}\"")
        }
        ExecError::FloatOutOfRange { ty, value } => {
            format!("\"{value}\" is out of range for type {ty}")
        }
        ExecError::FloatOverflow => "value out of range: overflow".to_string(),
        ExecError::FloatUnderflow => "value out of range: underflow".to_string(),
        ExecError::NumericFieldOverflow => "value overflows numeric format".to_string(),
        ExecError::StringDataRightTruncation { ty } => {
            format!("value too long for type {ty}")
        }
        ExecError::InvalidStorageValue { column, details }
            if matches!(
                column.as_str(),
                "date" | "time" | "timetz" | "timestamp" | "timestamptz"
            ) =>
        {
            details.clone()
        }
        other => format!("{other:?}"),
    }
}

fn input_error_sqlstate(err: &ExecError) -> &'static str {
    match err {
        ExecError::JsonInput { sqlstate, .. } => sqlstate,
        ExecError::XmlInput { sqlstate, .. } => sqlstate,
        ExecError::InvalidIntegerInput { .. }
        | ExecError::ArrayInput { .. }
        | ExecError::InvalidNumericInput(_)
        | ExecError::InvalidByteaInput { .. }
        | ExecError::InvalidGeometryInput { .. }
        | ExecError::InvalidBitInput { .. }
        | ExecError::InvalidBooleanInput { .. }
        | ExecError::DetailedError {
            sqlstate: "22P02", ..
        } => "22P02",
        ExecError::InvalidByteaHexDigit { .. } | ExecError::InvalidByteaHexOddDigits { .. } => {
            "22023"
        }
        ExecError::InvalidStorageValue { column, details }
            if matches!(
                column.as_str(),
                "date" | "time" | "timetz" | "timestamp" | "timestamptz"
            ) =>
        {
            if details.starts_with("time zone \"") {
                "22023"
            } else {
                "22007"
            }
        }
        ExecError::BitStringLengthMismatch { .. } => "22026",
        ExecError::BitStringTooLong { .. } => "22001",
        ExecError::IntegerOutOfRange { .. }
        | ExecError::Int2OutOfRange
        | ExecError::Int4OutOfRange
        | ExecError::Int8OutOfRange
        | ExecError::OidOutOfRange
        | ExecError::FloatOutOfRange { .. }
        | ExecError::FloatOverflow
        | ExecError::FloatUnderflow
        | ExecError::NumericFieldOverflow
        | ExecError::DetailedError {
            sqlstate: "22003", ..
        } => "22003",
        ExecError::StringDataRightTruncation { .. } => "22001",
        ExecError::InvalidFloatInput { .. } => "22P02",
        ExecError::DetailedError { sqlstate, .. } => sqlstate,
        _ => "XX000",
    }
}

fn datetime_parse_error_details(ty: &'static str, text: &str, err: DateTimeParseError) -> String {
    match err {
        DateTimeParseError::Invalid => format!("invalid input syntax for type {ty}: \"{text}\""),
        DateTimeParseError::FieldOutOfRange => {
            format!("date/time field value out of range: \"{text}\"")
        }
        DateTimeParseError::UnknownTimeZone(zone) => {
            format!("time zone \"{zone}\" not recognized")
        }
    }
}

fn date_parse_error(text: &str, err: DateParseError) -> ExecError {
    match err {
        DateParseError::Invalid => ExecError::DetailedError {
            message: format!("invalid input syntax for type date: \"{text}\""),
            detail: None,
            hint: None,
            sqlstate: "22007",
        },
        DateParseError::FieldOutOfRange { datestyle_hint } => ExecError::DetailedError {
            message: format!("date/time field value out of range: \"{text}\""),
            detail: None,
            hint: datestyle_hint
                .then_some("Perhaps you need a different \"DateStyle\" setting.".into()),
            sqlstate: "22008",
        },
        DateParseError::OutOfRange => ExecError::DetailedError {
            message: format!("date out of range: \"{text}\""),
            detail: None,
            hint: None,
            sqlstate: "22008",
        },
    }
}

fn input_error_info(err: ExecError, text: &str) -> InputErrorInfo {
    match err {
        ExecError::JsonInput {
            message,
            detail,
            sqlstate,
            ..
        } => InputErrorInfo {
            message,
            detail,
            hint: None,
            sqlstate,
        },
        ExecError::XmlInput {
            message,
            detail,
            sqlstate,
            ..
        } => InputErrorInfo {
            message,
            detail,
            hint: None,
            sqlstate,
        },
        ExecError::DetailedError {
            message,
            detail,
            hint,
            sqlstate,
        } => InputErrorInfo {
            message,
            detail,
            hint,
            sqlstate,
        },
        ExecError::ArrayInput {
            message,
            detail,
            sqlstate,
            ..
        } => InputErrorInfo {
            message,
            detail,
            hint: None,
            sqlstate,
        },
        other => InputErrorInfo {
            message: input_error_message(&other, text),
            detail: None,
            hint: None,
            sqlstate: input_error_sqlstate(&other),
        },
    }
}

pub(crate) fn soft_input_error_info(
    text: &str,
    type_name: &str,
) -> Result<Option<InputErrorInfo>, ExecError> {
    soft_input_error_info_with_config(text, type_name, &DateTimeConfig::default())
}

pub(crate) fn soft_input_error_info_with_config(
    text: &str,
    type_name: &str,
    config: &DateTimeConfig,
) -> Result<Option<InputErrorInfo>, ExecError> {
    if type_name.trim().eq_ignore_ascii_case("int2vector") {
        for item in text.split_ascii_whitespace() {
            match cast_text_to_int2(item) {
                Ok(_) => {}
                Err(err) => {
                    return Ok(Some(input_error_info(err, item)));
                }
            }
        }
        return Ok(None);
    }
    if type_name.trim().eq_ignore_ascii_case("oidvector") {
        return soft_parse_oidvector_input(text);
    }

    let ty = parse_input_type_name(type_name)?.ok_or_else(|| ExecError::InvalidStorageValue {
        column: type_name.to_string(),
        details: format!("unsupported type: {type_name}"),
    })?;
    if !ty.is_array && matches!(ty.kind, SqlTypeKind::Json | SqlTypeKind::Jsonb) {
        match parse_json_text_input(text) {
            Ok(_) => {
                if matches!(ty.kind, SqlTypeKind::Jsonb) {
                    match parse_jsonb_text_with_limit(text, config.max_stack_depth_kb) {
                        Ok(_) => return Ok(None),
                        Err(err) => return Ok(Some(input_error_info(err, text))),
                    }
                }
                return Ok(None);
            }
            Err(err) => return Ok(Some(input_error_info(err, text))),
        }
    }
    let parsed = match ty.kind {
        // PostgreSQL's pg_input_* helpers use the type input function semantics,
        // not explicit-cast padding/truncation semantics for bit and typmod-
        // constrained text inputs.
        SqlTypeKind::Bit
        | SqlTypeKind::VarBit
        | SqlTypeKind::Name
        | SqlTypeKind::Char
        | SqlTypeKind::Varchar => cast_text_value_with_config(text, ty, false, config),
        _ => cast_value_with_config(Value::Text(text.into()), ty, config),
    };
    match parsed {
        Ok(_) => Ok(None),
        Err(err) => Ok(Some(input_error_info(err, text))),
    }
}

pub(crate) fn cast_value(value: Value, ty: SqlType) -> Result<Value, ExecError> {
    cast_value_with_config(value, ty, &DateTimeConfig::default())
}

pub(crate) fn cast_value_with_config(
    value: Value,
    ty: SqlType,
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    if ty.is_array {
        return match value {
            Value::Null => Ok(Value::Null),
            Value::Array(items) => {
                let element_type = ty.element_type();
                if items
                    .iter()
                    .any(|item| matches!(item, Value::Array(_) | Value::PgArray(_)))
                {
                    let array =
                        ArrayValue::from_nested_values(items, vec![1]).map_err(|details| {
                            ExecError::DetailedError {
                                message: "malformed array literal".into(),
                                detail: Some(details),
                                hint: None,
                                sqlstate: "22P02",
                            }
                        })?;
                    let mut casted = Vec::with_capacity(array.elements.len());
                    for item in array.elements {
                        casted.push(cast_value_with_config(item, element_type, config)?);
                    }
                    Ok(Value::PgArray(ArrayValue::from_dimensions(
                        array.dimensions,
                        casted,
                    )))
                } else {
                    let mut casted = Vec::with_capacity(items.len());
                    for item in items {
                        casted.push(cast_value_with_config(item, element_type, config)?);
                    }
                    Ok(Value::Array(casted))
                }
            }
            Value::PgArray(array) => {
                let element_type = ty.element_type();
                let mut casted = Vec::with_capacity(array.elements.len());
                for item in array.elements {
                    casted.push(cast_value_with_config(item, element_type, config)?);
                }
                Ok(Value::PgArray(ArrayValue::from_dimensions(
                    array.dimensions,
                    casted,
                )))
            }
            other => match other.as_text() {
                Some(text) => parse_text_array_literal(text, ty.element_type()),
                None => Err(ExecError::TypeMismatch {
                    op: "::array",
                    left: other,
                    right: Value::Null,
                }),
            },
        };
    }

    if let Some(result) = cast_geometry_value(value.clone(), ty) {
        return result;
    }

    match value {
        Value::Null => Ok(Value::Null),
        Value::Int16(v) => match ty {
            ty if ty.is_range() => cast_text_value(&v.to_string(), ty, true),
            SqlType {
                kind: SqlTypeKind::Int2,
                ..
            } => Ok(Value::Int16(v)),
            SqlType {
                kind: SqlTypeKind::Int4,
                ..
            } => Ok(Value::Int32(v as i32)),
            SqlType {
                kind: SqlTypeKind::Int8,
                ..
            } => Ok(Value::Int64(v as i64)),
            SqlType {
                kind:
                    SqlTypeKind::Oid
                    | SqlTypeKind::RegRole
                    | SqlTypeKind::RegProcedure
                    | SqlTypeKind::Xid,
                ..
            } => {
                if v < 0 {
                    Err(ExecError::OidOutOfRange)
                } else {
                    Ok(Value::Int64(v as u32 as i64))
                }
            }
            SqlType {
                kind: SqlTypeKind::Float4 | SqlTypeKind::Float8,
                ..
            } => Ok(Value::Float64(v as f64)),
            SqlType {
                kind: SqlTypeKind::Numeric,
                ..
            } => Ok(Value::Numeric(NumericValue::from_i64(v as i64))),
            SqlType {
                kind: SqlTypeKind::Money,
                ..
            } => Ok(Value::Money(i64::from(v) * 100)),
            SqlType {
                kind:
                    SqlTypeKind::Text
                    | SqlTypeKind::Name
                    | SqlTypeKind::Int2Vector
                    | SqlTypeKind::OidVector
                    | SqlTypeKind::Date
                    | SqlTypeKind::Time
                    | SqlTypeKind::TimeTz
                    | SqlTypeKind::Point
                    | SqlTypeKind::Lseg
                    | SqlTypeKind::Path
                    | SqlTypeKind::Box
                    | SqlTypeKind::Polygon
                    | SqlTypeKind::Line
                    | SqlTypeKind::Circle
                    | SqlTypeKind::Timestamp
                    | SqlTypeKind::TimestampTz
                    | SqlTypeKind::PgNodeTree
                    | SqlTypeKind::InternalChar
                    | SqlTypeKind::Bit
                    | SqlTypeKind::VarBit
                    | SqlTypeKind::Char
                    | SqlTypeKind::Varchar
                    | SqlTypeKind::Json
                    | SqlTypeKind::Jsonb
                    | SqlTypeKind::JsonPath
                    | SqlTypeKind::Xml
                    | SqlTypeKind::TsVector
                    | SqlTypeKind::TsQuery
                    | SqlTypeKind::Void
                    | SqlTypeKind::FdwHandler
                    | SqlTypeKind::Tid
                    | SqlTypeKind::Interval
                    | SqlTypeKind::RegConfig
                    | SqlTypeKind::RegDictionary,
                ..
            } => cast_text_value(&v.to_string(), ty, true),
            SqlType {
                kind: SqlTypeKind::Bool,
                ..
            } => Ok(cast_integer_to_bool(v as i64)),
            SqlType {
                kind: SqlTypeKind::Bytea,
                ..
            } => Err(ExecError::TypeMismatch {
                op: "::bytea",
                left: Value::Int16(v),
                right: Value::Bytea(Vec::new()),
            }),
            SqlType {
                kind: SqlTypeKind::AnyArray,
                ..
            } => Err(unsupported_anyarray_input()),
            SqlType {
                kind: SqlTypeKind::Record | SqlTypeKind::Composite,
                ..
            } => Err(unsupported_record_input()),
            SqlType {
                kind: SqlTypeKind::Trigger,
                ..
            } => Err(unsupported_trigger_input()),
            SqlType {
                kind:
                    SqlTypeKind::Range
                    | SqlTypeKind::Int4Range
                    | SqlTypeKind::Int8Range
                    | SqlTypeKind::NumericRange
                    | SqlTypeKind::DateRange
                    | SqlTypeKind::TimestampRange
                    | SqlTypeKind::TimestampTzRange,
                ..
            } => unreachable!("range handled above"),
        },
        Value::Int32(v) => match ty {
            ty if ty.is_range() => cast_text_value(&v.to_string(), ty, true),
            SqlType {
                kind: SqlTypeKind::Int2,
                ..
            } => i16::try_from(v)
                .map(Value::Int16)
                .map_err(|_| ExecError::Int2OutOfRange),
            SqlType {
                kind: SqlTypeKind::Int4,
                ..
            } => Ok(Value::Int32(v)),
            SqlType {
                kind: SqlTypeKind::Int8,
                ..
            } => Ok(Value::Int64(v as i64)),
            SqlType {
                kind:
                    SqlTypeKind::Oid
                    | SqlTypeKind::RegRole
                    | SqlTypeKind::RegProcedure
                    | SqlTypeKind::Xid,
                ..
            } => {
                if v < 0 {
                    Err(ExecError::OidOutOfRange)
                } else {
                    Ok(Value::Int64(v as u32 as i64))
                }
            }
            SqlType {
                kind: SqlTypeKind::Float4 | SqlTypeKind::Float8,
                ..
            } => Ok(Value::Float64(v as f64)),
            SqlType {
                kind: SqlTypeKind::Numeric,
                ..
            } => Ok(Value::Numeric(NumericValue::from_i64(v as i64))),
            SqlType {
                kind: SqlTypeKind::Money,
                ..
            } => Ok(Value::Money(i64::from(v) * 100)),
            SqlType {
                kind:
                    SqlTypeKind::Text
                    | SqlTypeKind::Name
                    | SqlTypeKind::Int2Vector
                    | SqlTypeKind::OidVector
                    | SqlTypeKind::Date
                    | SqlTypeKind::Time
                    | SqlTypeKind::TimeTz
                    | SqlTypeKind::Point
                    | SqlTypeKind::Lseg
                    | SqlTypeKind::Path
                    | SqlTypeKind::Box
                    | SqlTypeKind::Polygon
                    | SqlTypeKind::Line
                    | SqlTypeKind::Circle
                    | SqlTypeKind::Timestamp
                    | SqlTypeKind::TimestampTz
                    | SqlTypeKind::PgNodeTree
                    | SqlTypeKind::InternalChar
                    | SqlTypeKind::Bit
                    | SqlTypeKind::VarBit
                    | SqlTypeKind::Char
                    | SqlTypeKind::Varchar
                    | SqlTypeKind::Json
                    | SqlTypeKind::Jsonb
                    | SqlTypeKind::JsonPath
                    | SqlTypeKind::Xml
                    | SqlTypeKind::TsVector
                    | SqlTypeKind::TsQuery
                    | SqlTypeKind::Void
                    | SqlTypeKind::FdwHandler
                    | SqlTypeKind::Tid
                    | SqlTypeKind::Interval
                    | SqlTypeKind::RegConfig
                    | SqlTypeKind::RegDictionary,
                ..
            } => cast_text_value(&v.to_string(), ty, true),
            SqlType {
                kind: SqlTypeKind::Bool,
                ..
            } => Ok(cast_integer_to_bool(v as i64)),
            SqlType {
                kind: SqlTypeKind::Bytea,
                ..
            } => Err(ExecError::TypeMismatch {
                op: "::bytea",
                left: Value::Int32(v),
                right: Value::Bytea(Vec::new()),
            }),
            SqlType {
                kind: SqlTypeKind::AnyArray,
                ..
            } => Err(unsupported_anyarray_input()),
            SqlType {
                kind: SqlTypeKind::Record | SqlTypeKind::Composite,
                ..
            } => Err(unsupported_record_input()),
            SqlType {
                kind: SqlTypeKind::Trigger,
                ..
            } => Err(unsupported_trigger_input()),
            SqlType {
                kind:
                    SqlTypeKind::Range
                    | SqlTypeKind::Int4Range
                    | SqlTypeKind::Int8Range
                    | SqlTypeKind::NumericRange
                    | SqlTypeKind::DateRange
                    | SqlTypeKind::TimestampRange
                    | SqlTypeKind::TimestampTzRange,
                ..
            } => unreachable!("range handled above"),
        },
        Value::Bool(v) => match ty {
            ty if ty.is_range() => cast_text_value(if v { "true" } else { "false" }, ty, true),
            SqlType {
                kind: SqlTypeKind::Bool,
                ..
            } => Ok(Value::Bool(v)),
            SqlType {
                kind:
                    SqlTypeKind::Text
                    | SqlTypeKind::Name
                    | SqlTypeKind::Int2Vector
                    | SqlTypeKind::OidVector
                    | SqlTypeKind::Date
                    | SqlTypeKind::Time
                    | SqlTypeKind::TimeTz
                    | SqlTypeKind::Point
                    | SqlTypeKind::Lseg
                    | SqlTypeKind::Path
                    | SqlTypeKind::Box
                    | SqlTypeKind::Polygon
                    | SqlTypeKind::Line
                    | SqlTypeKind::Circle
                    | SqlTypeKind::Timestamp
                    | SqlTypeKind::TimestampTz
                    | SqlTypeKind::PgNodeTree
                    | SqlTypeKind::InternalChar
                    | SqlTypeKind::Bit
                    | SqlTypeKind::VarBit
                    | SqlTypeKind::Char
                    | SqlTypeKind::Varchar
                    | SqlTypeKind::Json
                    | SqlTypeKind::Jsonb
                    | SqlTypeKind::JsonPath
                    | SqlTypeKind::Xml
                    | SqlTypeKind::TsVector
                    | SqlTypeKind::TsQuery
                    | SqlTypeKind::Void
                    | SqlTypeKind::FdwHandler
                    | SqlTypeKind::Tid
                    | SqlTypeKind::Interval
                    | SqlTypeKind::RegConfig
                    | SqlTypeKind::RegDictionary,
                ..
            } => cast_text_value(if v { "true" } else { "false" }, ty, true),
            SqlType {
                kind:
                    SqlTypeKind::Int2
                    | SqlTypeKind::Int4
                    | SqlTypeKind::Int8
                    | SqlTypeKind::Oid
                    | SqlTypeKind::RegRole
                    | SqlTypeKind::RegProcedure
                    | SqlTypeKind::Xid
                    | SqlTypeKind::Bytea
                    | SqlTypeKind::Float4
                    | SqlTypeKind::Float8
                    | SqlTypeKind::Money
                    | SqlTypeKind::Numeric,
                ..
            } => Err(ExecError::TypeMismatch {
                op: "::int4",
                left: Value::Bool(v),
                right: Value::Int32(0),
            }),
            SqlType {
                kind: SqlTypeKind::AnyArray,
                ..
            } => Err(unsupported_anyarray_input()),
            SqlType {
                kind: SqlTypeKind::Record | SqlTypeKind::Composite,
                ..
            } => Err(unsupported_record_input()),
            SqlType {
                kind: SqlTypeKind::Trigger,
                ..
            } => Err(unsupported_trigger_input()),
            SqlType {
                kind:
                    SqlTypeKind::Range
                    | SqlTypeKind::Int4Range
                    | SqlTypeKind::Int8Range
                    | SqlTypeKind::NumericRange
                    | SqlTypeKind::DateRange
                    | SqlTypeKind::TimestampRange
                    | SqlTypeKind::TimestampTzRange,
                ..
            } => unreachable!("range handled above"),
        },
        Value::Date(v) => match ty.kind {
            SqlTypeKind::Date => Ok(Value::Date(v)),
            SqlTypeKind::Text
            | SqlTypeKind::Name
            | SqlTypeKind::Char
            | SqlTypeKind::Varchar
            | SqlTypeKind::Json
            | SqlTypeKind::Jsonb
            | SqlTypeKind::JsonPath
            | SqlTypeKind::Time
            | SqlTypeKind::TimeTz
            | SqlTypeKind::Timestamp
            | SqlTypeKind::TimestampTz => cast_text_value_with_config(
                &render_datetime_value_text_with_config(&Value::Date(v), config)
                    .expect("datetime values render"),
                ty,
                true,
                config,
            ),
            _ => Err(ExecError::TypeMismatch {
                op: "::date",
                left: Value::Date(v),
                right: Value::Null,
            }),
        },
        Value::Time(v) => match ty.kind {
            SqlTypeKind::Time => Ok(Value::Time(v)),
            SqlTypeKind::Text
            | SqlTypeKind::Name
            | SqlTypeKind::Char
            | SqlTypeKind::Varchar
            | SqlTypeKind::Json
            | SqlTypeKind::Jsonb
            | SqlTypeKind::JsonPath
            | SqlTypeKind::TimeTz
            | SqlTypeKind::Timestamp
            | SqlTypeKind::TimestampTz => cast_text_value_with_config(
                &render_datetime_value_text_with_config(&Value::Time(v), config)
                    .expect("datetime values render"),
                ty,
                true,
                config,
            ),
            _ => Err(ExecError::TypeMismatch {
                op: "::time",
                left: Value::Time(v),
                right: Value::Null,
            }),
        },
        Value::TimeTz(v) => match ty.kind {
            SqlTypeKind::TimeTz => Ok(Value::TimeTz(v)),
            SqlTypeKind::Text
            | SqlTypeKind::Name
            | SqlTypeKind::Char
            | SqlTypeKind::Varchar
            | SqlTypeKind::Json
            | SqlTypeKind::Jsonb
            | SqlTypeKind::JsonPath
            | SqlTypeKind::Time => cast_text_value_with_config(
                &render_datetime_value_text_with_config(&Value::TimeTz(v), config)
                    .expect("datetime values render"),
                ty,
                true,
                config,
            ),
            _ => Err(ExecError::TypeMismatch {
                op: "::timetz",
                left: Value::TimeTz(v),
                right: Value::Null,
            }),
        },
        Value::Timestamp(v) => match ty.kind {
            SqlTypeKind::Timestamp => Ok(Value::Timestamp(v)),
            SqlTypeKind::Text
            | SqlTypeKind::Name
            | SqlTypeKind::Char
            | SqlTypeKind::Varchar
            | SqlTypeKind::Json
            | SqlTypeKind::Jsonb
            | SqlTypeKind::JsonPath
            | SqlTypeKind::TimestampTz
            | SqlTypeKind::Date
            | SqlTypeKind::Time
            | SqlTypeKind::TimeTz => cast_text_value_with_config(
                &render_datetime_value_text_with_config(&Value::Timestamp(v), config)
                    .expect("datetime values render"),
                ty,
                true,
                config,
            ),
            _ => Err(ExecError::TypeMismatch {
                op: "::timestamp",
                left: Value::Timestamp(v),
                right: Value::Null,
            }),
        },
        Value::TimestampTz(v) => match ty.kind {
            SqlTypeKind::TimestampTz => Ok(Value::TimestampTz(v)),
            SqlTypeKind::Text
            | SqlTypeKind::Name
            | SqlTypeKind::Char
            | SqlTypeKind::Varchar
            | SqlTypeKind::Json
            | SqlTypeKind::Jsonb
            | SqlTypeKind::JsonPath
            | SqlTypeKind::Timestamp
            | SqlTypeKind::Date
            | SqlTypeKind::Time
            | SqlTypeKind::TimeTz => cast_text_value_with_config(
                &render_datetime_value_text_with_config(&Value::TimestampTz(v), config)
                    .expect("datetime values render"),
                ty,
                true,
                config,
            ),
            _ => Err(ExecError::TypeMismatch {
                op: "::timestamptz",
                left: Value::TimestampTz(v),
                right: Value::Null,
            }),
        },
        Value::Text(text) => cast_text_value_with_config(text.as_str(), ty, true, config),
        Value::TextRef(ptr, len) => {
            let text = unsafe {
                std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len as usize))
            };
            cast_text_value_with_config(text, ty, true, config)
        }
        Value::Range(range) => {
            if ty.is_range() {
                if range_type_ref_for_sql_type(ty).is_some_and(|target_type| {
                    target_type.type_oid() == range.range_type.type_oid()
                }) {
                    Ok(Value::Range(range))
                } else {
                    cast_text_value_with_config(
                        &render_range_text(&Value::Range(range.clone())).unwrap_or_default(),
                        ty,
                        true,
                        config,
                    )
                }
            } else {
                match ty.kind {
                    SqlTypeKind::Text
                    | SqlTypeKind::Name
                    | SqlTypeKind::Char
                    | SqlTypeKind::Varchar
                    | SqlTypeKind::Json
                    | SqlTypeKind::Jsonb
                    | SqlTypeKind::JsonPath => cast_text_value_with_config(
                        &render_range_text(&Value::Range(range.clone())).unwrap_or_default(),
                        ty,
                        true,
                        config,
                    ),
                    _ => Err(ExecError::TypeMismatch {
                        op: "::range",
                        left: Value::Range(range),
                        right: Value::Null,
                    }),
                }
            }
        }
        Value::InternalChar(byte) => match ty.kind {
            SqlTypeKind::InternalChar => Ok(Value::InternalChar(byte)),
            SqlTypeKind::Text
            | SqlTypeKind::Date
            | SqlTypeKind::Time
            | SqlTypeKind::TimeTz
            | SqlTypeKind::Timestamp
            | SqlTypeKind::TimestampTz
            | SqlTypeKind::Bit
            | SqlTypeKind::VarBit => Ok(Value::Text(CompactString::from_owned(
                render_internal_char_text(byte),
            ))),
            SqlTypeKind::Json => {
                let rendered = render_internal_char_text(byte);
                validate_json_text(&rendered)?;
                Ok(Value::Json(CompactString::from_owned(rendered)))
            }
            SqlTypeKind::Jsonb => {
                let rendered = render_internal_char_text(byte);
                Ok(Value::Jsonb(parse_jsonb_text_with_limit(
                    &rendered,
                    config.max_stack_depth_kb,
                )?))
            }
            SqlTypeKind::JsonPath => {
                let rendered = render_internal_char_text(byte);
                Ok(Value::JsonPath(canonicalize_jsonpath_text(&rendered)?))
            }
            SqlTypeKind::Char | SqlTypeKind::Varchar => {
                cast_text_value_with_config(&render_internal_char_text(byte), ty, true, config)
            }
            _ => Err(ExecError::TypeMismatch {
                op: "::char",
                left: Value::InternalChar(byte),
                right: Value::Null,
            }),
        },
        Value::JsonPath(text) => cast_text_value_with_config(text.as_str(), ty, true, config),
        Value::Json(text) => cast_text_value_with_config(text.as_str(), ty, true, config),
        Value::Xml(text) => cast_text_value_with_config(text.as_str(), ty, true, config),
        Value::TsVector(vector) => match ty.kind {
            SqlTypeKind::TsVector => Ok(Value::TsVector(vector)),
            SqlTypeKind::Text
            | SqlTypeKind::Name
            | SqlTypeKind::Timestamp
            | SqlTypeKind::Char
            | SqlTypeKind::Varchar => Ok(Value::Text(CompactString::from_owned(
                crate::backend::executor::render_tsvector_text(&vector),
            ))),
            SqlTypeKind::Json => {
                let rendered = crate::backend::executor::render_tsvector_text(&vector);
                validate_json_text(&rendered)?;
                Ok(Value::Json(CompactString::from_owned(rendered)))
            }
            SqlTypeKind::Jsonb => {
                let rendered = crate::backend::executor::render_tsvector_text(&vector);
                Ok(Value::Jsonb(parse_jsonb_text_with_limit(
                    &rendered,
                    config.max_stack_depth_kb,
                )?))
            }
            _ => Err(ExecError::TypeMismatch {
                op: "::tsvector",
                left: Value::TsVector(vector),
                right: Value::Null,
            }),
        },
        Value::TsQuery(query) => match ty.kind {
            SqlTypeKind::TsQuery => Ok(Value::TsQuery(query)),
            SqlTypeKind::Text
            | SqlTypeKind::Name
            | SqlTypeKind::Timestamp
            | SqlTypeKind::Char
            | SqlTypeKind::Varchar => Ok(Value::Text(CompactString::from_owned(
                crate::backend::executor::render_tsquery_text(&query),
            ))),
            SqlTypeKind::Json => {
                let rendered = crate::backend::executor::render_tsquery_text(&query);
                validate_json_text(&rendered)?;
                Ok(Value::Json(CompactString::from_owned(rendered)))
            }
            SqlTypeKind::Jsonb => {
                let rendered = crate::backend::executor::render_tsquery_text(&query);
                Ok(Value::Jsonb(parse_jsonb_text_with_limit(
                    &rendered,
                    config.max_stack_depth_kb,
                )?))
            }
            _ => Err(ExecError::TypeMismatch {
                op: "::tsquery",
                left: Value::TsQuery(query),
                right: Value::Null,
            }),
        },
        Value::Bytea(bytes) => match ty.kind {
            SqlTypeKind::Bytea => Ok(Value::Bytea(bytes)),
            _ => Err(ExecError::TypeMismatch {
                op: "::bytea",
                left: Value::Bytea(bytes),
                right: Value::Null,
            }),
        },
        Value::Jsonb(bytes) => match ty.kind {
            SqlTypeKind::Jsonb => Ok(Value::Jsonb(bytes)),
            SqlTypeKind::Json => Ok(Value::Json(CompactString::from_owned(render_jsonb_bytes(
                &bytes,
            )?))),
            SqlTypeKind::JsonPath => {
                let rendered = render_jsonb_bytes(&bytes)?;
                Ok(Value::JsonPath(canonicalize_jsonpath_text(&rendered)?))
            }
            SqlTypeKind::Text
            | SqlTypeKind::Date
            | SqlTypeKind::Time
            | SqlTypeKind::TimeTz
            | SqlTypeKind::Timestamp
            | SqlTypeKind::TimestampTz
            | SqlTypeKind::Char
            | SqlTypeKind::Varchar => {
                cast_text_value_with_config(&render_jsonb_bytes(&bytes)?, ty, true, config)
            }
            _ => Err(ExecError::TypeMismatch {
                op: "::jsonb",
                left: Value::Jsonb(bytes),
                right: Value::Null,
            }),
        },
        Value::Int64(v) => match ty {
            ty if ty.is_range() => cast_text_value(&v.to_string(), ty, true),
            SqlType {
                kind: SqlTypeKind::Int2,
                ..
            } => i16::try_from(v)
                .map(Value::Int16)
                .map_err(|_| ExecError::Int2OutOfRange),
            SqlType {
                kind: SqlTypeKind::Int4,
                ..
            } => i32::try_from(v)
                .map(Value::Int32)
                .map_err(|_| ExecError::Int4OutOfRange),
            SqlType {
                kind: SqlTypeKind::Int8,
                ..
            } => Ok(Value::Int64(v)),
            SqlType {
                kind:
                    SqlTypeKind::Oid
                    | SqlTypeKind::RegRole
                    | SqlTypeKind::RegProcedure
                    | SqlTypeKind::Xid,
                ..
            } => {
                if !(0..=i32::MAX as i64).contains(&v) {
                    Err(ExecError::OidOutOfRange)
                } else {
                    Ok(Value::Int64(v as u32 as i64))
                }
            }
            SqlType {
                kind: SqlTypeKind::Float4 | SqlTypeKind::Float8,
                ..
            } => Ok(Value::Float64(v as f64)),
            SqlType {
                kind: SqlTypeKind::Numeric,
                ..
            } => Ok(Value::Numeric(NumericValue::from_i64(v))),
            SqlType {
                kind: SqlTypeKind::Money,
                ..
            } => v
                .checked_mul(100)
                .map(Value::Money)
                .ok_or_else(|| ExecError::DetailedError {
                    message: "money out of range".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "22003",
                }),
            SqlType {
                kind:
                    SqlTypeKind::Text
                    | SqlTypeKind::Name
                    | SqlTypeKind::Int2Vector
                    | SqlTypeKind::OidVector
                    | SqlTypeKind::Date
                    | SqlTypeKind::Time
                    | SqlTypeKind::TimeTz
                    | SqlTypeKind::Point
                    | SqlTypeKind::Lseg
                    | SqlTypeKind::Path
                    | SqlTypeKind::Box
                    | SqlTypeKind::Polygon
                    | SqlTypeKind::Line
                    | SqlTypeKind::Circle
                    | SqlTypeKind::Timestamp
                    | SqlTypeKind::TimestampTz
                    | SqlTypeKind::PgNodeTree
                    | SqlTypeKind::InternalChar
                    | SqlTypeKind::Bit
                    | SqlTypeKind::VarBit
                    | SqlTypeKind::Char
                    | SqlTypeKind::Varchar
                    | SqlTypeKind::Json
                    | SqlTypeKind::Jsonb
                    | SqlTypeKind::JsonPath
                    | SqlTypeKind::Xml
                    | SqlTypeKind::TsVector
                    | SqlTypeKind::TsQuery
                    | SqlTypeKind::Void
                    | SqlTypeKind::FdwHandler
                    | SqlTypeKind::Tid
                    | SqlTypeKind::Interval
                    | SqlTypeKind::RegConfig
                    | SqlTypeKind::RegDictionary,
                ..
            } => cast_text_value(&v.to_string(), ty, true),
            SqlType {
                kind: SqlTypeKind::Bool,
                ..
            } => Ok(cast_integer_to_bool(v)),
            SqlType {
                kind: SqlTypeKind::Bytea,
                ..
            } => Err(ExecError::TypeMismatch {
                op: "::bytea",
                left: Value::Int64(v),
                right: Value::Bytea(Vec::new()),
            }),
            SqlType {
                kind: SqlTypeKind::AnyArray,
                ..
            } => Err(unsupported_anyarray_input()),
            SqlType {
                kind: SqlTypeKind::Record | SqlTypeKind::Composite,
                ..
            } => Err(unsupported_record_input()),
            SqlType {
                kind: SqlTypeKind::Trigger,
                ..
            } => Err(unsupported_trigger_input()),
            SqlType {
                kind:
                    SqlTypeKind::Range
                    | SqlTypeKind::Int4Range
                    | SqlTypeKind::Int8Range
                    | SqlTypeKind::NumericRange
                    | SqlTypeKind::DateRange
                    | SqlTypeKind::TimestampRange
                    | SqlTypeKind::TimestampTzRange,
                ..
            } => unreachable!("range handled above"),
        },
        Value::Float64(v) => match ty {
            ty if ty.is_range() => cast_text_value(&v.to_string(), ty, true),
            SqlType {
                kind: SqlTypeKind::Float4 | SqlTypeKind::Float8,
                ..
            } => Ok(Value::Float64(if matches!(ty.kind, SqlTypeKind::Float4) {
                narrow_float4_runtime(v)?
            } else {
                v
            })),
            SqlType {
                kind: SqlTypeKind::Numeric,
                ..
            } => Ok(Value::Numeric(coerce_numeric_value(
                parse_numeric_text(&v.to_string())
                    .ok_or_else(|| ExecError::InvalidNumericInput(v.to_string()))?,
                ty,
            )?)),
            SqlType {
                kind: SqlTypeKind::Money,
                ..
            } => Ok(Value::Money(money_from_float(v)?)),
            SqlType {
                kind:
                    SqlTypeKind::Text
                    | SqlTypeKind::Name
                    | SqlTypeKind::Int2Vector
                    | SqlTypeKind::OidVector
                    | SqlTypeKind::Date
                    | SqlTypeKind::Time
                    | SqlTypeKind::TimeTz
                    | SqlTypeKind::Point
                    | SqlTypeKind::Lseg
                    | SqlTypeKind::Path
                    | SqlTypeKind::Box
                    | SqlTypeKind::Polygon
                    | SqlTypeKind::Line
                    | SqlTypeKind::Circle
                    | SqlTypeKind::Timestamp
                    | SqlTypeKind::TimestampTz
                    | SqlTypeKind::PgNodeTree
                    | SqlTypeKind::InternalChar
                    | SqlTypeKind::Bit
                    | SqlTypeKind::VarBit
                    | SqlTypeKind::Char
                    | SqlTypeKind::Varchar
                    | SqlTypeKind::Json
                    | SqlTypeKind::Jsonb
                    | SqlTypeKind::JsonPath
                    | SqlTypeKind::Xml
                    | SqlTypeKind::TsVector
                    | SqlTypeKind::TsQuery
                    | SqlTypeKind::Void
                    | SqlTypeKind::FdwHandler
                    | SqlTypeKind::Tid
                    | SqlTypeKind::Interval
                    | SqlTypeKind::RegConfig
                    | SqlTypeKind::RegDictionary,
                ..
            } => cast_text_value(&v.to_string(), ty, true),
            SqlType {
                kind: SqlTypeKind::Int2,
                ..
            } => cast_float_to_int(v, ty),
            SqlType {
                kind: SqlTypeKind::Int4,
                ..
            } => cast_float_to_int(v, ty),
            SqlType {
                kind: SqlTypeKind::Int8,
                ..
            } => cast_float_to_int(v, ty),
            SqlType {
                kind:
                    SqlTypeKind::Oid
                    | SqlTypeKind::RegRole
                    | SqlTypeKind::RegProcedure
                    | SqlTypeKind::Xid,
                ..
            } => cast_float_to_int(v, ty),
            SqlType {
                kind: SqlTypeKind::Bool,
                ..
            } => Err(ExecError::TypeMismatch {
                op: "::",
                left: Value::Float64(v),
                right: Value::Bool(false),
            }),
            SqlType {
                kind: SqlTypeKind::Bytea,
                ..
            } => Err(ExecError::TypeMismatch {
                op: "::bytea",
                left: Value::Float64(v),
                right: Value::Bytea(Vec::new()),
            }),
            SqlType {
                kind: SqlTypeKind::AnyArray,
                ..
            } => Err(unsupported_anyarray_input()),
            SqlType {
                kind: SqlTypeKind::Record | SqlTypeKind::Composite,
                ..
            } => Err(unsupported_record_input()),
            SqlType {
                kind: SqlTypeKind::Trigger,
                ..
            } => Err(unsupported_trigger_input()),
            SqlType {
                kind:
                    SqlTypeKind::Range
                    | SqlTypeKind::Int4Range
                    | SqlTypeKind::Int8Range
                    | SqlTypeKind::NumericRange
                    | SqlTypeKind::DateRange
                    | SqlTypeKind::TimestampRange
                    | SqlTypeKind::TimestampTzRange,
                ..
            } => unreachable!("range handled above"),
        },
        Value::Numeric(numeric) => cast_numeric_value(numeric, ty, true),
        Value::Money(v) => match ty.kind {
            SqlTypeKind::Money => Ok(Value::Money(v)),
            SqlTypeKind::Numeric => Ok(Value::Numeric(NumericValue::from(money_numeric_text(v)))),
            SqlTypeKind::Int8 => Ok(Value::Int64(v / 100)),
            SqlTypeKind::Int4 => i32::try_from(v / 100)
                .map(Value::Int32)
                .map_err(|_| ExecError::Int4OutOfRange),
            SqlTypeKind::Int2 => i16::try_from(v / 100)
                .map(Value::Int16)
                .map_err(|_| ExecError::Int2OutOfRange),
            SqlTypeKind::Float4 | SqlTypeKind::Float8 => Ok(Value::Float64(v as f64 / 100.0)),
            _ => cast_text_value(&money_format_text(v), ty, true),
        },
        Value::Bit(bits) => match ty.kind {
            SqlTypeKind::Bit | SqlTypeKind::VarBit => {
                Ok(Value::Bit(coerce_bit_string(bits, ty, true)?))
            }
            SqlTypeKind::Text
            | SqlTypeKind::Name
            | SqlTypeKind::Date
            | SqlTypeKind::Time
            | SqlTypeKind::TimeTz
            | SqlTypeKind::Timestamp
            | SqlTypeKind::TimestampTz => Ok(Value::Text(CompactString::from_owned(
                render_bit_text(&bits),
            ))),
            _ => Err(ExecError::TypeMismatch {
                op: "::bit",
                left: Value::Bit(bits),
                right: Value::Null,
            }),
        },
        Value::Point(_)
        | Value::Lseg(_)
        | Value::Path(_)
        | Value::Line(_)
        | Value::Box(_)
        | Value::Polygon(_)
        | Value::Circle(_) => unreachable!("geometry casts handled before scalar match"),
        Value::Array(items) => Ok(Value::Array(items)),
        Value::PgArray(array) => Ok(Value::PgArray(array)),
        Value::Record(record) => Ok(Value::Record(record)),
    }
}

pub(super) fn cast_text_value(text: &str, ty: SqlType, explicit: bool) -> Result<Value, ExecError> {
    cast_text_value_with_config(text, ty, explicit, &DateTimeConfig::default())
}

pub(super) fn cast_text_value_with_config(
    text: &str,
    ty: SqlType,
    explicit: bool,
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    if ty.is_range() {
        return parse_range_text(text, ty);
    }
    match ty.kind {
        SqlTypeKind::AnyArray => Err(unsupported_anyarray_input()),
        SqlTypeKind::Record | SqlTypeKind::Composite => Err(unsupported_record_input()),
        SqlTypeKind::Trigger => Err(unsupported_trigger_input()),
        SqlTypeKind::FdwHandler => Err(ExecError::TypeMismatch {
            op: "::fdw_handler",
            left: Value::Text(CompactString::new(text)),
            right: Value::Null,
        }),
        SqlTypeKind::Text
        | SqlTypeKind::Int2Vector
        | SqlTypeKind::OidVector
        | SqlTypeKind::PgNodeTree => Ok(Value::Text(CompactString::new(text))),
        SqlTypeKind::Xml => {
            crate::backend::executor::validate_xml_input(text, config.xml.option)?;
            Ok(Value::Xml(CompactString::new(text)))
        }
        SqlTypeKind::Date => parse_date_text(text, config)
            .map(Value::Date)
            .map_err(|err| date_parse_error(text, err)),
        SqlTypeKind::Time => parse_time_text(text)
            .map(Value::Time)
            .map(|value| apply_time_precision(value, ty.time_precision()))
            .ok_or_else(|| ExecError::InvalidStorageValue {
                column: "time".into(),
                details: format!("invalid input syntax for type time: \"{text}\""),
            }),
        SqlTypeKind::TimeTz => parse_timetz_text(text, config)
            .map(Value::TimeTz)
            .map(|value| apply_time_precision(value, ty.time_precision()))
            .ok_or_else(|| ExecError::InvalidStorageValue {
                column: "timetz".into(),
                details: format!("invalid input syntax for type time with time zone: \"{text}\""),
            }),
        SqlTypeKind::Interval => Ok(Value::Text(CompactString::from_owned(
            canonicalize_interval_text(text)?,
        ))),
        SqlTypeKind::Timestamp => parse_timestamp_text(text, config)
            .map(Value::Timestamp)
            .map(|value| apply_time_precision(value, ty.time_precision()))
            .map_err(|err| ExecError::InvalidStorageValue {
                column: "timestamp".into(),
                details: datetime_parse_error_details("timestamp", text, err),
            }),
        SqlTypeKind::TimestampTz => parse_timestamptz_text(text, config)
            .map(Value::TimestampTz)
            .map(|value| apply_time_precision(value, ty.time_precision()))
            .map_err(|err| ExecError::InvalidStorageValue {
                column: "timestamptz".into(),
                details: datetime_parse_error_details("timestamp with time zone", text, err),
            }),
        SqlTypeKind::InternalChar => Ok(Value::InternalChar(parse_internal_char_text(text))),
        SqlTypeKind::Bit | SqlTypeKind::VarBit => Ok(Value::Bit(coerce_bit_string(
            parse_bit_text(text)?,
            ty,
            explicit,
        )?)),
        SqlTypeKind::Bytea => Ok(Value::Bytea(parse_bytea_text(text)?)),
        SqlTypeKind::Json => {
            validate_json_text(text)?;
            Ok(Value::Json(CompactString::new(text)))
        }
        SqlTypeKind::Jsonb => Ok(Value::Jsonb(parse_jsonb_text_with_limit(
            text,
            config.max_stack_depth_kb,
        )?)),
        SqlTypeKind::JsonPath => Ok(Value::JsonPath(canonicalize_jsonpath_text(text)?)),
        SqlTypeKind::TsVector => {
            crate::backend::executor::parse_tsvector_text(text).map(Value::TsVector)
        }
        SqlTypeKind::TsQuery => {
            crate::backend::executor::parse_tsquery_text(text).map(Value::TsQuery)
        }
        SqlTypeKind::Void => Err(ExecError::TypeMismatch {
            op: "::void",
            left: Value::Text(CompactString::new(text)),
            right: Value::Null,
        }),
        SqlTypeKind::RegRole
        | SqlTypeKind::RegProcedure
        | SqlTypeKind::RegConfig
        | SqlTypeKind::RegDictionary => cast_text_to_oid(text),
        SqlTypeKind::Tid => Ok(Value::Text(CompactString::from_owned(
            canonicalize_tid_text(text)?,
        ))),
        SqlTypeKind::Xid => cast_text_to_xid(text),
        SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar => Ok(Value::Text(
            CompactString::from_owned(coerce_character_string(text, ty, explicit)?),
        )),
        SqlTypeKind::Int2 => cast_text_to_int2(text),
        SqlTypeKind::Int4 => cast_text_to_int4(text),
        SqlTypeKind::Int8 => cast_text_to_int8(text),
        SqlTypeKind::Money => money_parse_text(text).map(Value::Money),
        SqlTypeKind::Oid => cast_text_to_oid(text),
        SqlTypeKind::Float4 | SqlTypeKind::Float8 => parse_pg_float(text, ty.kind).map(|v| {
            Value::Float64(if matches!(ty.kind, SqlTypeKind::Float4) {
                (v as f32) as f64
            } else {
                v
            })
        }),
        SqlTypeKind::Numeric => Ok(Value::Numeric(coerce_numeric_value(
            parse_numeric_input_value(text)?,
            ty,
        )?)),
        SqlTypeKind::Bool => parse_pg_bool_text(text).map(Value::Bool),
        SqlTypeKind::Point
        | SqlTypeKind::Lseg
        | SqlTypeKind::Path
        | SqlTypeKind::Box
        | SqlTypeKind::Polygon
        | SqlTypeKind::Line
        | SqlTypeKind::Circle => parse_geometry_text(text, ty.kind),
        SqlTypeKind::Range
        | SqlTypeKind::Int4Range
        | SqlTypeKind::Int8Range
        | SqlTypeKind::NumericRange
        | SqlTypeKind::DateRange
        | SqlTypeKind::TimestampRange
        | SqlTypeKind::TimestampTzRange => unreachable!("range handled above"),
    }
}

pub(super) fn cast_numeric_value(
    value: NumericValue,
    ty: SqlType,
    explicit: bool,
) -> Result<Value, ExecError> {
    if ty.is_range() {
        return cast_text_value(&value.render(), ty, explicit);
    }
    match ty.kind {
        SqlTypeKind::AnyArray => Err(unsupported_anyarray_input()),
        SqlTypeKind::Record | SqlTypeKind::Composite => Err(unsupported_record_input()),
        SqlTypeKind::Trigger => Err(unsupported_trigger_input()),
        SqlTypeKind::FdwHandler => Err(ExecError::TypeMismatch {
            op: "::fdw_handler",
            left: Value::Numeric(value),
            right: Value::Null,
        }),
        SqlTypeKind::Void => Err(ExecError::TypeMismatch {
            op: "::void",
            left: Value::Numeric(value),
            right: Value::Null,
        }),
        SqlTypeKind::Numeric => Ok(Value::Numeric(coerce_numeric_value(value, ty)?)),
        SqlTypeKind::Money => money_parse_text(&value.render()).map(Value::Money),
        SqlTypeKind::Text
        | SqlTypeKind::Int2Vector
        | SqlTypeKind::OidVector
        | SqlTypeKind::Point
        | SqlTypeKind::Lseg
        | SqlTypeKind::Path
        | SqlTypeKind::Box
        | SqlTypeKind::Polygon
        | SqlTypeKind::Line
        | SqlTypeKind::Circle
        | SqlTypeKind::Tid
        | SqlTypeKind::Interval
        | SqlTypeKind::PgNodeTree => Ok(Value::Text(CompactString::from_owned(value.render()))),
        SqlTypeKind::Xml => Ok(Value::Xml(CompactString::from_owned(value.render()))),
        SqlTypeKind::Date
        | SqlTypeKind::Time
        | SqlTypeKind::TimeTz
        | SqlTypeKind::Timestamp
        | SqlTypeKind::TimestampTz => cast_text_value(&value.render(), ty, explicit),
        SqlTypeKind::Json => {
            let rendered = value.render();
            validate_json_text(&rendered)?;
            Ok(Value::Json(CompactString::from_owned(rendered)))
        }
        SqlTypeKind::Jsonb => {
            let rendered = value.render();
            Ok(Value::Jsonb(parse_jsonb_text(&rendered)?))
        }
        SqlTypeKind::JsonPath => {
            let rendered = value.render();
            Ok(Value::JsonPath(canonicalize_jsonpath_text(&rendered)?))
        }
        SqlTypeKind::InternalChar => cast_text_value(&value.render(), ty, explicit),
        SqlTypeKind::Bit | SqlTypeKind::VarBit => cast_text_value(&value.render(), ty, explicit),
        SqlTypeKind::TsVector
        | SqlTypeKind::TsQuery
        | SqlTypeKind::RegConfig
        | SqlTypeKind::RegDictionary => cast_text_value(&value.render(), ty, explicit),
        SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar => {
            cast_text_value(&value.render(), ty, explicit)
        }
        SqlTypeKind::Float4 => {
            let rendered = value.render();
            let v = parse_pg_float(&rendered, SqlTypeKind::Float4)?;
            Ok(Value::Float64(v as f32 as f64))
        }
        SqlTypeKind::Float8 => {
            let rendered = value.render();
            let v = parse_pg_float(&rendered, SqlTypeKind::Float8)?;
            Ok(Value::Float64(v))
        }
        SqlTypeKind::Int2 => match value {
            NumericValue::NaN => Err(ExecError::NumericNaNToInt { ty: "smallint" }),
            NumericValue::PosInf | NumericValue::NegInf => {
                Err(ExecError::NumericInfinityToInt { ty: "smallint" })
            }
            NumericValue::Finite { .. } => value
                .round_to_scale(0)
                .and_then(|rounded| rounded.render().parse::<i16>().ok())
                .map(Value::Int16)
                .ok_or(ExecError::Int2OutOfRange),
        },
        SqlTypeKind::Int4 => match value {
            NumericValue::NaN => Err(ExecError::NumericNaNToInt { ty: "integer" }),
            NumericValue::PosInf | NumericValue::NegInf => {
                Err(ExecError::NumericInfinityToInt { ty: "integer" })
            }
            NumericValue::Finite { .. } => value
                .round_to_scale(0)
                .and_then(|rounded| rounded.render().parse::<i32>().ok())
                .map(Value::Int32)
                .ok_or(ExecError::Int4OutOfRange),
        },
        SqlTypeKind::Int8 => match value {
            NumericValue::NaN => Err(ExecError::NumericNaNToInt { ty: "bigint" }),
            NumericValue::PosInf | NumericValue::NegInf => {
                Err(ExecError::NumericInfinityToInt { ty: "bigint" })
            }
            NumericValue::Finite { .. } => value
                .round_to_scale(0)
                .and_then(|rounded| rounded.render().parse::<i64>().ok())
                .map(Value::Int64)
                .ok_or(ExecError::Int8OutOfRange),
        },
        SqlTypeKind::Oid | SqlTypeKind::RegRole | SqlTypeKind::RegProcedure | SqlTypeKind::Xid => {
            value
                .round_to_scale(0)
                .and_then(|rounded| rounded.render().parse::<u32>().ok())
                .and_then(|rounded| Some(Value::Int64(rounded as i64)))
                .ok_or(ExecError::OidOutOfRange)
        }
        SqlTypeKind::Bool => Err(ExecError::TypeMismatch {
            op: "::bool",
            left: Value::Numeric(value),
            right: Value::Bool(false),
        }),
        SqlTypeKind::Bytea => Err(ExecError::TypeMismatch {
            op: "::bytea",
            left: Value::Numeric(value),
            right: Value::Bytea(Vec::new()),
        }),
        SqlTypeKind::Range
        | SqlTypeKind::Int4Range
        | SqlTypeKind::Int8Range
        | SqlTypeKind::NumericRange
        | SqlTypeKind::DateRange
        | SqlTypeKind::TimestampRange
        | SqlTypeKind::TimestampTzRange => unreachable!("range handled above"),
    }
}

fn coerce_character_string(text: &str, ty: SqlType, explicit: bool) -> Result<String, ExecError> {
    let max_chars = match ty.kind {
        SqlTypeKind::Name => return Ok(text.to_string()),
        SqlTypeKind::Char => ty.char_len().unwrap_or(1),
        SqlTypeKind::Varchar => match ty.char_len() {
            Some(max_chars) => max_chars,
            None => return Ok(text.to_string()),
        },
        _ => return Ok(text.to_string()),
    };

    let char_count = text.chars().count() as i32;
    if char_count <= max_chars {
        return Ok(match ty.kind {
            SqlTypeKind::Char => pad_char_string(text, max_chars as usize),
            SqlTypeKind::Varchar => text.to_string(),
            _ => text.to_string(),
        });
    }

    let clip_idx = text
        .char_indices()
        .nth(max_chars as usize)
        .map(|(idx, _)| idx)
        .unwrap_or(text.len());
    let truncated = &text[..clip_idx];
    let remainder = &text[clip_idx..];
    if explicit || remainder.chars().all(|ch| ch == ' ') {
        Ok(match ty.kind {
            SqlTypeKind::Char => pad_char_string(truncated, max_chars as usize),
            SqlTypeKind::Varchar => truncated.to_string(),
            _ => truncated.to_string(),
        })
    } else {
        Err(ExecError::StringDataRightTruncation {
            ty: match ty.kind {
                SqlTypeKind::Char => format!("character({max_chars})"),
                SqlTypeKind::Varchar => format!("character varying({max_chars})"),
                _ => format!("character varying({max_chars})"),
            },
        })
    }
}

fn pad_char_string(text: &str, max_chars: usize) -> String {
    let mut out = String::with_capacity(max_chars.max(text.len()));
    out.push_str(text);
    let pad_chars = max_chars.saturating_sub(text.chars().count());
    out.extend(std::iter::repeat_n(' ', pad_chars));
    out
}

fn cast_float_to_int(value: f64, ty: SqlType) -> Result<Value, ExecError> {
    if !value.is_finite() {
        return Err(ExecError::InvalidFloatInput {
            ty: "double precision",
            value: value.to_string(),
        });
    }
    let rounded = value.round_ties_even();
    match ty.kind {
        SqlTypeKind::Int2 => {
            if rounded < i16::MIN as f64 || rounded > i16::MAX as f64 {
                Err(ExecError::Int2OutOfRange)
            } else {
                Ok(Value::Int16(rounded as i16))
            }
        }
        SqlTypeKind::Int4 => {
            if rounded < i32::MIN as f64 || rounded > i32::MAX as f64 {
                Err(ExecError::Int4OutOfRange)
            } else {
                Ok(Value::Int32(rounded as i32))
            }
        }
        SqlTypeKind::Int8 => {
            const INT8_UPPER_EXCLUSIVE: f64 = 9_223_372_036_854_775_808.0;
            if rounded < i64::MIN as f64 || rounded >= INT8_UPPER_EXCLUSIVE {
                Err(ExecError::Int8OutOfRange)
            } else {
                Ok(Value::Int64(rounded as i64))
            }
        }
        SqlTypeKind::Oid => {
            if rounded < 0.0 || rounded > u32::MAX as f64 {
                Err(ExecError::OidOutOfRange)
            } else {
                Ok(Value::Int64(rounded as u32 as i64))
            }
        }
        _ => unreachable!(),
    }
}

fn coerce_numeric_value(parsed: NumericValue, ty: SqlType) -> Result<NumericValue, ExecError> {
    let Some((precision, scale)) = ty.numeric_precision_scale() else {
        return Ok(parsed);
    };

    let rounded = if scale >= 0 {
        parsed
            .round_to_scale(scale as u32)
            .ok_or_else(|| numeric_typmod_overflow_error(precision, scale))?
    } else {
        coerce_numeric_negative_scale(parsed, scale)?
    };

    match rounded {
        NumericValue::NaN => Ok(NumericValue::NaN),
        NumericValue::PosInf | NumericValue::NegInf => {
            Err(numeric_typmod_infinity_error(precision, scale))
        }
        NumericValue::Finite { .. } if numeric_fits_precision_scale(&rounded, precision, scale) => {
            Ok(rounded)
        }
        NumericValue::Finite { .. } => Err(numeric_typmod_overflow_error(precision, scale)),
    }
}

fn coerce_numeric_negative_scale(
    parsed: NumericValue,
    scale: i32,
) -> Result<NumericValue, ExecError> {
    let shift = scale.unsigned_abs();
    match parsed {
        NumericValue::Finite {
            coeff,
            scale: current_scale,
            ..
        } => {
            let integer = coeff;
            let factor = pow10_bigint(current_scale.saturating_add(shift));
            let (quotient, remainder) = integer.div_rem(&factor);
            let twice = remainder.abs() * 2u8;
            let rounded = if twice >= factor.abs() {
                quotient + integer.signum()
            } else {
                quotient
            };
            Ok(NumericValue::finite(rounded * pow10_bigint(shift), 0).normalize())
        }
        other => Ok(other),
    }
}

fn numeric_fits_precision_scale(value: &NumericValue, precision: i32, target_scale: i32) -> bool {
    match value {
        NumericValue::Finite { coeff, scale, .. } => {
            if coeff.is_zero() {
                return true;
            }
            let limit_exp = precision - target_scale + (*scale as i32);
            if limit_exp <= 0 {
                return false;
            }
            coeff.abs() < pow10_bigint(limit_exp as u32)
        }
        _ => true,
    }
}

fn pow10_bigint(exp: u32) -> num_bigint::BigInt {
    let mut value = num_bigint::BigInt::from(1u8);
    for _ in 0..exp {
        value *= 10u8;
    }
    value
}

pub(crate) fn parse_pg_float(text: &str, kind: SqlTypeKind) -> Result<f64, ExecError> {
    let trimmed = text.trim_matches(|ch: char| ch.is_ascii_whitespace());
    let ty = float_sql_type_name(kind);
    if trimmed.is_empty() {
        return Err(ExecError::InvalidFloatInput {
            ty,
            value: text.to_string(),
        });
    }

    let normalized = trimmed.to_ascii_lowercase();
    match normalized.as_str() {
        "nan" | "+nan" | "-nan" => {
            return Ok(if matches!(kind, SqlTypeKind::Float4) {
                f32::NAN as f64
            } else {
                f64::NAN
            });
        }
        "inf" | "+inf" | "infinity" | "+infinity" => {
            return Ok(if matches!(kind, SqlTypeKind::Float4) {
                f32::INFINITY as f64
            } else {
                f64::INFINITY
            });
        }
        "-inf" | "-infinity" => {
            return Ok(if matches!(kind, SqlTypeKind::Float4) {
                f32::NEG_INFINITY as f64
            } else {
                f64::NEG_INFINITY
            });
        }
        _ => {}
    }

    match kind {
        SqlTypeKind::Float4 => parse_pg_float4(trimmed, text),
        SqlTypeKind::Float8 => parse_pg_float8(trimmed, text),
        _ => unreachable!(),
    }
}

fn parse_pg_float4(trimmed: &str, raw: &str) -> Result<f64, ExecError> {
    let parsed = match trimmed.parse::<f32>() {
        Ok(parsed) => parsed,
        Err(_) => {
            let parsed64 = trimmed
                .parse::<f64>()
                .map_err(|_| ExecError::InvalidFloatInput {
                    ty: "real",
                    value: raw.to_string(),
                })?;
            if parsed64.is_infinite() {
                return Err(ExecError::FloatOutOfRange {
                    ty: "real",
                    value: raw.to_string(),
                });
            }
            return Err(ExecError::InvalidFloatInput {
                ty: "real",
                value: raw.to_string(),
            });
        }
    };

    if parsed.is_infinite() {
        return Err(ExecError::FloatOutOfRange {
            ty: "real",
            value: raw.to_string(),
        });
    }
    if parsed == 0.0 && has_nonzero_digit(trimmed) {
        return Err(ExecError::FloatOutOfRange {
            ty: "real",
            value: raw.to_string(),
        });
    }

    Ok(parsed as f64)
}

fn parse_pg_float8(trimmed: &str, raw: &str) -> Result<f64, ExecError> {
    let parsed = trimmed
        .parse::<f64>()
        .map_err(|_| ExecError::InvalidFloatInput {
            ty: "double precision",
            value: raw.to_string(),
        })?;

    if parsed.is_infinite() {
        return Err(ExecError::FloatOutOfRange {
            ty: "double precision",
            value: raw.to_string(),
        });
    }
    if parsed == 0.0 && has_nonzero_digit(trimmed) {
        return Err(ExecError::FloatOutOfRange {
            ty: "double precision",
            value: raw.to_string(),
        });
    }

    Ok(parsed)
}

fn narrow_float4_runtime(value: f64) -> Result<f64, ExecError> {
    if !value.is_finite() {
        return Ok((value as f32) as f64);
    }
    let narrowed = value as f32;
    if narrowed.is_infinite() {
        return Err(ExecError::FloatOverflow);
    }
    if narrowed == 0.0 && value != 0.0 {
        return Err(ExecError::FloatUnderflow);
    }
    Ok(narrowed as f64)
}

fn float_sql_type_name(kind: SqlTypeKind) -> &'static str {
    match kind {
        SqlTypeKind::Float4 => "real",
        SqlTypeKind::Float8 => "double precision",
        _ => unreachable!(),
    }
}

fn has_nonzero_digit(text: &str) -> bool {
    text.bytes().any(|b| b.is_ascii_digit() && b != b'0')
}

#[cfg(test)]
mod tests {
    use super::{
        cast_float_to_int, cast_value, parse_input_type_name, parse_pg_float,
        parse_text_array_literal, soft_input_error_info,
    };
    use crate::backend::executor::{ExecError, Value};
    use crate::backend::parser::{SqlType, SqlTypeKind};

    #[test]
    fn float4_text_input_rounds_at_float4_width() {
        let cases = [
            ("1.1754944e-38", 0x0080_0000_u32),
            ("7038531e-32", 0x15ae_43fd_u32),
            ("82381273e-35", 0x1282_89d1_u32),
        ];

        for (text, expected_bits) in cases {
            let parsed = parse_pg_float(text, SqlTypeKind::Float4).unwrap();
            assert_eq!((parsed as f32).to_bits(), expected_bits, "{text}");
        }
    }

    #[test]
    fn float_to_int8_rejects_rounded_upper_boundary() {
        let int8 = SqlType::new(SqlTypeKind::Int8);

        assert!(matches!(
            cast_float_to_int(-9_223_372_036_854_775_808.0, int8),
            Ok(Value::Int64(v)) if v == i64::MIN
        ));
        assert!(matches!(
            cast_float_to_int(9_223_372_036_854_775_807.0, int8),
            Err(ExecError::Int8OutOfRange)
        ));
        assert!(matches!(
            cast_float_to_int(9_223_372_036_854_775_808.0, int8),
            Err(ExecError::Int8OutOfRange)
        ));
    }

    #[test]
    fn parse_input_type_name_uses_text_input_cast_surface() {
        assert_eq!(
            parse_input_type_name("jsonb").unwrap(),
            Some(SqlType::new(SqlTypeKind::Jsonb))
        );
        assert_eq!(
            parse_input_type_name("jsonpath").unwrap(),
            Some(SqlType::new(SqlTypeKind::JsonPath))
        );
        assert_eq!(
            parse_input_type_name("timestamp").unwrap(),
            Some(SqlType::new(SqlTypeKind::Timestamp))
        );
        assert_eq!(
            parse_input_type_name("varchar(4)").unwrap(),
            Some(SqlType::with_char_len(SqlTypeKind::Varchar, 4))
        );
        assert_eq!(
            parse_input_type_name("int4[]").unwrap(),
            Some(SqlType::array_of(SqlType::new(SqlTypeKind::Int4)))
        );
        assert_eq!(
            parse_input_type_name("varchar(4)[]").unwrap(),
            Some(SqlType::array_of(SqlType::with_char_len(
                SqlTypeKind::Varchar,
                4
            )))
        );
        assert_eq!(
            parse_input_type_name("int4[][]").unwrap(),
            Some(SqlType::array_of(SqlType::new(SqlTypeKind::Int4)))
        );
    }

    #[test]
    fn parse_text_array_literal_uses_scalar_input_parsers() {
        assert_eq!(
            parse_text_array_literal("{1,2}", SqlType::new(SqlTypeKind::Int4)).unwrap(),
            Value::Array(vec![Value::Int32(1), Value::Int32(2)])
        );
        assert_eq!(
            parse_text_array_literal("{\"NULL\",NULL}", SqlType::new(SqlTypeKind::Text)).unwrap(),
            Value::Array(vec![Value::Text("NULL".into()), Value::Null])
        );
        assert_eq!(
            parse_text_array_literal("{true,false}", SqlType::new(SqlTypeKind::Bool)).unwrap(),
            Value::Array(vec![Value::Bool(true), Value::Bool(false)])
        );
        assert_eq!(
            parse_text_array_literal("{{1,4},{2,5},{3,6}}", SqlType::new(SqlTypeKind::Int4))
                .unwrap(),
            Value::Array(vec![
                Value::Array(vec![Value::Int32(1), Value::Int32(4)]),
                Value::Array(vec![Value::Int32(2), Value::Int32(5)]),
                Value::Array(vec![Value::Int32(3), Value::Int32(6)]),
            ])
        );
    }

    #[test]
    fn cast_value_supports_text_input_array_targets() {
        assert_eq!(
            cast_value(
                Value::Text("{1,2,3}".into()),
                SqlType::array_of(SqlType::new(SqlTypeKind::Int4))
            )
            .unwrap(),
            Value::Array(vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)])
        );
        assert_eq!(
            cast_value(
                Value::Text("{\"a\",\"b\"}".into()),
                SqlType::array_of(SqlType::new(SqlTypeKind::Varchar))
            )
            .unwrap(),
            Value::Array(vec![Value::Text("a".into()), Value::Text("b".into())])
        );
    }

    #[test]
    fn soft_input_error_info_supports_catalog_backed_input_types() {
        assert!(
            soft_input_error_info("{\"a\":1}", "jsonb")
                .unwrap()
                .is_none()
        );
        assert!(soft_input_error_info("{\"a\":", "jsonb").unwrap().is_some());
        assert!(soft_input_error_info("$.a", "jsonpath").unwrap().is_none());
        assert!(
            soft_input_error_info("{1,2,3}", "int4[]")
                .unwrap()
                .is_none()
        );
        assert!(
            soft_input_error_info("{1,nope}", "int4[]")
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn soft_input_error_info_reports_structured_xml_errors() {
        let info = soft_input_error_info("<value>one</", "xml")
            .unwrap()
            .expect("invalid xml should return structured info");
        assert_eq!(info.message, "invalid XML content");
        assert_eq!(info.sqlstate, "2200N");
        assert!(info.detail.is_some());

        let info = soft_input_error_info("<?xml version=\"1.0\" standalone=\"y\"?><foo/>", "xml")
            .unwrap()
            .expect("invalid xml declaration should return structured info");
        assert_eq!(info.message, "invalid XML content");
        assert_eq!(info.sqlstate, "2200N");
        assert!(info.detail.is_some());
    }
}
