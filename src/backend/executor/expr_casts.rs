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
use super::expr_mac::{
    macaddr_to_macaddr8, macaddr8_to_macaddr, parse_macaddr_text, parse_macaddr8_text,
    render_macaddr_text, render_macaddr8_text,
};
use super::expr_money::{
    money_format_text, money_from_float, money_numeric_text, money_parse_text,
};
use super::expr_multirange::{
    multirange_from_range, parse_multirange_text, render_multirange_text_with_config,
};
use super::expr_network::{parse_cidr_text, parse_inet_text};
use super::expr_range::{parse_range_text, render_range_text_with_config};
use super::expr_reg;
use super::expr_string::{
    eval_pg_rust_test_int44in, eval_pg_rust_test_int44out, eval_pg_rust_test_widget_in,
    eval_pg_rust_test_widget_out,
};
use super::expr_txid::{cast_text_to_txid_snapshot, is_txid_snapshot_type_oid};
use super::node_types::*;
use crate::backend::executor::jsonb::{
    JsonbValue, decode_jsonb, jsonb_to_value, parse_json_text_input, parse_jsonb_text,
    parse_jsonb_text_with_limit, render_jsonb_bytes,
};
use crate::backend::libpq::pqformat::{FloatFormatOptions, format_float4_text, format_float8_text};
use crate::backend::parser::{
    CatalogLookup, ParseError, RawTypeName, SqlType, SqlTypeKind, parse_type_name,
    resolve_raw_type_name,
};
use crate::backend::utils::misc::guc_datetime::{DateTimeConfig, IntervalStyle};
use crate::backend::utils::time::date::{
    DateParseError, parse_date_text, parse_time_text, parse_timetz_text,
};
use crate::backend::utils::time::datetime::{
    DateTimeParseError, timestamp_parts_from_usecs, timezone_offset_seconds,
    timezone_offset_seconds_at_utc,
};
use crate::backend::utils::time::timestamp::{parse_timestamp_text, parse_timestamptz_text};
use crate::include::catalog::{
    INT2_TYPE_OID, OID_TYPE_OID, TEXT_TYPE_OID, XID8_TYPE_OID, bootstrap_pg_cast_rows,
    builtin_type_rows, multirange_type_ref_for_sql_type, range_type_ref_for_sql_type,
};
use crate::include::nodes::datetime::{
    DATEVAL_NOBEGIN, DATEVAL_NOEND, DateADT, TimeADT, TimeTzADT, TimestampADT, TimestampTzADT,
    USECS_PER_DAY, USECS_PER_HOUR, USECS_PER_MINUTE, USECS_PER_SEC,
};
use crate::include::nodes::datum::{ArrayDimension, BitString};
use crate::pgrust::compact_string::CompactString;
use num_bigint::BigInt;
use num_integer::Integer;
use num_traits::{Signed, ToPrimitive, Zero};
use std::collections::BTreeSet;
use std::sync::OnceLock;

pub(crate) struct InputErrorInfo {
    pub(crate) message: String,
    pub(crate) detail: Option<String>,
    pub(crate) hint: Option<String>,
    pub(crate) sqlstate: &'static str,
}

fn cast_integer_to_bit_string(
    value: u64,
    source_width: i32,
    negative: bool,
    target_type: SqlType,
) -> BitString {
    let target_len = match target_type.kind {
        SqlTypeKind::Bit => target_type.bit_len().unwrap_or(1),
        SqlTypeKind::VarBit => target_type.bit_len().unwrap_or(source_width),
        _ => unreachable!("integer bit cast target checked by caller"),
    }
    .max(0);
    let mut bytes = vec![0u8; BitString::byte_len(target_len)];
    for bit_idx in 0..target_len as usize {
        let right_index = target_len as usize - 1 - bit_idx;
        let bit_set = if right_index < source_width as usize {
            ((value >> right_index) & 1) != 0
        } else {
            negative
        };
        if bit_set {
            bytes[bit_idx / 8] |= 1 << (7 - (bit_idx % 8));
        }
    }
    BitString::new(target_len, bytes)
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

fn timestamp_date_part(value: TimestampADT) -> DateADT {
    if !value.is_finite() {
        return DateADT(match value.0 {
            crate::include::nodes::datetime::TIMESTAMP_NOEND => DATEVAL_NOEND,
            crate::include::nodes::datetime::TIMESTAMP_NOBEGIN => DATEVAL_NOBEGIN,
            _ => unreachable!("checked finite timestamp above"),
        });
    }
    let (days, _) = timestamp_parts_from_usecs(value.0);
    DateADT(days)
}

fn timestamp_time_part(value: TimestampADT) -> Option<TimeADT> {
    value.is_finite().then(|| {
        let (_, time_usecs) = timestamp_parts_from_usecs(value.0);
        TimeADT(time_usecs.rem_euclid(USECS_PER_DAY))
    })
}

fn timestamp_to_timestamptz(value: TimestampADT, config: &DateTimeConfig) -> TimestampTzADT {
    if !value.is_finite() {
        return TimestampTzADT(value.0);
    }
    TimestampTzADT(value.0 - i64::from(timezone_offset_seconds(config)) * USECS_PER_SEC)
}

fn timestamptz_local_timestamp(value: TimestampTzADT, config: &DateTimeConfig) -> TimestampADT {
    if !value.is_finite() {
        return TimestampADT(value.0);
    }
    TimestampADT(
        value.0 + i64::from(timezone_offset_seconds_at_utc(config, value.0)) * USECS_PER_SEC,
    )
}

fn timestamp_time_tz_part(value: TimestampADT, config: &DateTimeConfig) -> Option<TimeTzADT> {
    timestamp_time_part(value).map(|time| TimeTzADT {
        time,
        offset_seconds: timezone_offset_seconds(config),
    })
}

fn timestamptz_time_tz_part(value: TimestampTzADT, config: &DateTimeConfig) -> Option<TimeTzADT> {
    if !value.is_finite() {
        return None;
    }
    let offset_seconds = timezone_offset_seconds_at_utc(config, value.0);
    let local = TimestampADT(value.0 + i64::from(offset_seconds) * USECS_PER_SEC);
    timestamp_time_part(local).map(|time| TimeTzADT {
        time,
        offset_seconds,
    })
}

fn jsonb_type_name(value: &JsonbValue) -> &'static str {
    match value {
        JsonbValue::Null => "null",
        JsonbValue::String(_) => "string",
        JsonbValue::Numeric(_) => "numeric",
        JsonbValue::Bool(_) => "boolean",
        JsonbValue::Date(_) => "date",
        JsonbValue::Time(_) => "time without time zone",
        JsonbValue::TimeTz(_) => "time with time zone",
        JsonbValue::Timestamp(_) => "timestamp without time zone",
        JsonbValue::TimestampTz(_) => "timestamp with time zone",
        JsonbValue::Array(_) => "array",
        JsonbValue::Object(_) => "object",
    }
}

fn jsonb_cast_target_name(kind: SqlTypeKind) -> Option<&'static str> {
    match kind {
        SqlTypeKind::Bool => Some("boolean"),
        SqlTypeKind::Float4 => Some("real"),
        SqlTypeKind::Float8 => Some("double precision"),
        SqlTypeKind::Int2 => Some("smallint"),
        SqlTypeKind::Int4 => Some("integer"),
        SqlTypeKind::Int8 => Some("bigint"),
        SqlTypeKind::Numeric => Some("numeric"),
        _ => None,
    }
}

fn invalid_jsonb_scalar_cast(value: &JsonbValue, target: SqlTypeKind) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "cannot cast jsonb {} to type {}",
            jsonb_type_name(value),
            jsonb_cast_target_name(target).unwrap_or("unknown")
        ),
        detail: None,
        hint: None,
        sqlstate: "22023",
    }
}

fn cast_jsonb_scalar_value(
    parsed: &JsonbValue,
    ty: SqlType,
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    match ty.kind {
        SqlTypeKind::Bool => match parsed {
            JsonbValue::Null => Ok(Value::Null),
            JsonbValue::Bool(value) => Ok(Value::Bool(*value)),
            other => Err(invalid_jsonb_scalar_cast(other, ty.kind)),
        },
        SqlTypeKind::Float4
        | SqlTypeKind::Float8
        | SqlTypeKind::Int2
        | SqlTypeKind::Int4
        | SqlTypeKind::Int8
        | SqlTypeKind::Numeric => match parsed {
            JsonbValue::Null => Ok(Value::Null),
            JsonbValue::Numeric(value) => {
                cast_text_value_with_config(&value.render(), ty, true, config)
            }
            other => Err(invalid_jsonb_scalar_cast(other, ty.kind)),
        },
        _ => Err(ExecError::TypeMismatch {
            op: "::jsonb",
            left: jsonb_to_value(parsed),
            right: Value::Null,
        }),
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

fn parse_xid_integer_text(text: &str, ty: &'static str) -> Result<i128, ExecError> {
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
    if rest.len() > 1
        && rest.starts_with('0')
        && !rest.starts_with("0x")
        && !rest.starts_with("0X")
        && !rest.starts_with("0o")
        && !rest.starts_with("0O")
        && !rest.starts_with("0b")
        && !rest.starts_with("0B")
    {
        let magnitude =
            i128::from_str_radix(rest, 8).map_err(|_| ExecError::InvalidIntegerInput {
                ty,
                value: text.to_string(),
            })?;
        Ok(if negative { -magnitude } else { magnitude })
    } else {
        parse_pg_integer_text(text, ty)
    }
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
    let value = parse_xid_integer_text(text, "xid")?;
    let xid = if (0..=u32::MAX as i128).contains(&value) {
        value as u32
    } else if (i32::MIN as i128..=-1).contains(&value) {
        (value as i32) as u32
    } else {
        return Err(ExecError::IntegerOutOfRange {
            ty: "xid",
            value: text.to_string(),
        });
    };
    Ok(Value::Int64(xid as i64))
}

fn cast_text_to_xid8(text: &str) -> Result<Value, ExecError> {
    let value = parse_xid_integer_text(text, "xid8")?;
    let xid = if (0..=u64::MAX as i128).contains(&value) {
        value as u64
    } else if (i64::MIN as i128..=-1).contains(&value) {
        (value as i64) as u64
    } else {
        return Err(ExecError::IntegerOutOfRange {
            ty: "xid8",
            value: text.to_string(),
        });
    };
    Ok(Value::Xid8(xid))
}

fn cast_text_to_regclass(
    text: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<Value, ExecError> {
    if let Ok(Value::Int64(oid)) = cast_text_to_oid(text) {
        return Ok(Value::Int64(oid));
    }

    let catalog = catalog.ok_or_else(|| ExecError::Parse(ParseError::UnknownTable(text.into())))?;
    let relation_oid = catalog
        .lookup_any_relation(text)
        .map(|entry| entry.relation_oid)
        .ok_or_else(|| ExecError::Parse(ParseError::UnknownTable(text.into())))?;
    Ok(Value::Int64(relation_oid as i64))
}

fn cast_text_to_regnamespace(
    text: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<Value, ExecError> {
    if let Ok(Value::Int64(oid)) = cast_text_to_oid(text) {
        return Ok(Value::Int64(oid));
    }

    let catalog = catalog.ok_or_else(|| ExecError::DetailedError {
        message: format!("schema \"{text}\" does not exist"),
        detail: None,
        hint: None,
        sqlstate: "3F000",
    })?;
    let namespace_oid = catalog
        .namespace_rows()
        .into_iter()
        .find(|row| row.nspname == text)
        .map(|row| row.oid)
        .ok_or_else(|| ExecError::DetailedError {
            message: format!("schema \"{text}\" does not exist"),
            detail: None,
            hint: None,
            sqlstate: "3F000",
        })?;
    Ok(Value::Int64(namespace_oid as i64))
}

fn regclass_text_input(value: &Value, source_type: Option<SqlType>) -> Option<&str> {
    let source_is_text_like = source_type.is_some_and(|ty| {
        matches!(
            ty.element_type().kind,
            SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar
        )
    });
    let source_is_internal_char =
        source_type.is_some_and(|ty| matches!(ty.element_type().kind, SqlTypeKind::InternalChar));
    if source_is_text_like || source_is_internal_char {
        value.as_text()
    } else {
        None
    }
}

fn cast_regnamespace_to_text(
    value: &Value,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<Value, ExecError> {
    let oid = match value {
        Value::Int32(oid) if *oid >= 0 => *oid as u32,
        Value::Int64(oid) if *oid >= 0 && *oid <= i64::from(u32::MAX) => *oid as u32,
        other => {
            return Err(ExecError::TypeMismatch {
                op: "::text",
                left: other.clone(),
                right: Value::Text("".into()),
            });
        }
    };
    if oid == 0 {
        return Ok(Value::Text("-".into()));
    }
    Ok(catalog
        .and_then(|catalog| catalog.namespace_row_by_oid(oid))
        .map(|row| Value::Text(row.nspname.into()))
        .unwrap_or_else(|| Value::Text(oid.to_string().into())))
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
    let (negative, normalized) = parse_numeric_input_exponent_digits(text)?;
    let value = normalized.parse::<i32>().ok()?;
    Some(if negative { -value } else { value })
}

fn numeric_input_exponent_would_overflow(text: &str) -> bool {
    let Some((negative, normalized)) = parse_numeric_input_exponent_digits(text) else {
        return false;
    };
    let limit = if negative {
        i32::MIN.unsigned_abs().to_string()
    } else {
        i32::MAX.to_string()
    };
    if normalized.len() != limit.len() {
        return normalized.len() > limit.len();
    }
    normalized > limit
}

fn parse_numeric_input_exponent_digits(text: &str) -> Option<(bool, String)> {
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
    Some((negative, normalized))
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

pub(crate) fn numeric_input_would_overflow(text: &str) -> bool {
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
                if numeric_input_exponent_would_overflow(&trimmed[index + 1..]) {
                    return true;
                }
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

pub(crate) fn invalid_interval_text_error(text: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("invalid input syntax for type interval: \"{text}\""),
        detail: None,
        hint: None,
        sqlstate: "22007",
    }
}

fn interval_field_value_out_of_range_error(text: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("interval field value out of range: \"{text}\""),
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

fn cannot_convert_infinite_interval_to_time_error() -> ExecError {
    ExecError::DetailedError {
        message: "cannot convert infinite interval to time".into(),
        detail: None,
        hint: None,
        sqlstate: "22008",
    }
}

fn apply_interval_precision(
    value: IntervalValue,
    precision: Option<i32>,
) -> Result<IntervalValue, ExecError> {
    let Some(precision) = precision else {
        return Ok(value);
    };
    if !value.is_finite() || precision >= 6 {
        return Ok(value);
    }
    if precision < 0 {
        return Err(interval_field_value_out_of_range_error(
            &precision.to_string(),
        ));
    }
    let factor = 10_i128.pow((6 - precision) as u32);
    let half = factor / 2;
    let micros = i128::from(value.time_micros);
    let rounded = if micros >= 0 {
        ((micros + half) / factor) * factor
    } else {
        ((micros - half) / factor) * factor
    };
    let time_micros = i64::try_from(rounded).map_err(|_| interval_out_of_range_error())?;
    let rounded = IntervalValue {
        time_micros,
        days: value.days,
        months: value.months,
    };
    if rounded.is_finite() {
        Ok(rounded)
    } else {
        Err(interval_out_of_range_error())
    }
}

fn apply_interval_typmod(value: IntervalValue, ty: SqlType) -> Result<IntervalValue, ExecError> {
    if ty.typmod < 0 || !value.is_finite() {
        return Ok(value);
    }

    let mut adjusted = value;
    if let Some(range) = ty.interval_range() {
        adjusted = apply_interval_range(adjusted, range)?;
    }
    apply_interval_precision(adjusted, ty.interval_precision())
}

fn apply_interval_range(value: IntervalValue, range: i32) -> Result<IntervalValue, ExecError> {
    let mut adjusted = value;
    if range == SqlType::INTERVAL_MASK_YEAR {
        adjusted.months = (adjusted.months / 12) * 12;
        adjusted.days = 0;
        adjusted.time_micros = 0;
    } else if range == SqlType::INTERVAL_MASK_MONTH
        || range == (SqlType::INTERVAL_MASK_YEAR | SqlType::INTERVAL_MASK_MONTH)
    {
        adjusted.days = 0;
        adjusted.time_micros = 0;
    } else if range == SqlType::INTERVAL_MASK_DAY {
        adjusted.time_micros = 0;
    } else if range == SqlType::INTERVAL_MASK_HOUR
        || range == (SqlType::INTERVAL_MASK_DAY | SqlType::INTERVAL_MASK_HOUR)
    {
        adjusted.time_micros = (adjusted.time_micros / USECS_PER_HOUR) * USECS_PER_HOUR;
    } else if range == SqlType::INTERVAL_MASK_MINUTE
        || range
            == (SqlType::INTERVAL_MASK_DAY
                | SqlType::INTERVAL_MASK_HOUR
                | SqlType::INTERVAL_MASK_MINUTE)
        || range == (SqlType::INTERVAL_MASK_HOUR | SqlType::INTERVAL_MASK_MINUTE)
    {
        adjusted.time_micros = (adjusted.time_micros / USECS_PER_MINUTE) * USECS_PER_MINUTE;
    } else if range == SqlType::INTERVAL_MASK_SECOND
        || range
            == (SqlType::INTERVAL_MASK_DAY
                | SqlType::INTERVAL_MASK_HOUR
                | SqlType::INTERVAL_MASK_MINUTE
                | SqlType::INTERVAL_MASK_SECOND)
        || range
            == (SqlType::INTERVAL_MASK_HOUR
                | SqlType::INTERVAL_MASK_MINUTE
                | SqlType::INTERVAL_MASK_SECOND)
        || range == (SqlType::INTERVAL_MASK_MINUTE | SqlType::INTERVAL_MASK_SECOND)
    {
        // Fractional-second precision, if any, is applied by apply_interval_precision.
    } else {
        return Err(interval_out_of_range_error());
    }
    Ok(adjusted)
}

pub(crate) fn render_interval_text(value: IntervalValue) -> String {
    render_interval_text_with_style(value, IntervalStyle::Postgres)
}

pub(crate) fn render_interval_text_with_config(
    value: IntervalValue,
    config: &DateTimeConfig,
) -> String {
    render_interval_text_with_style(value, config.interval_style)
}

fn render_interval_text_with_style(value: IntervalValue, style: IntervalStyle) -> String {
    if value.is_infinity() {
        return "infinity".into();
    }
    if value.is_neg_infinity() {
        return "-infinity".into();
    }

    match style {
        IntervalStyle::Postgres => render_interval_postgres(value),
        IntervalStyle::PostgresVerbose => render_interval_postgres_verbose(value),
        IntervalStyle::SqlStandard => render_interval_sql_standard(value),
        IntervalStyle::Iso8601 => render_interval_iso8601(value),
    }
}

fn render_interval_postgres(value: IntervalValue) -> String {
    fn unit_suffix(value: i64, singular: &str, plural: &str) -> String {
        if value == 1 {
            format!("{value} {singular}")
        } else {
            format!("{value} {plural}")
        }
    }

    fn push_unit_part(
        parts: &mut Vec<String>,
        last_sign: &mut i8,
        value: i64,
        singular: &str,
        plural: &str,
    ) {
        if value == 0 {
            return;
        }
        let mut rendered = unit_suffix(value, singular, plural);
        if value > 0 && *last_sign < 0 {
            rendered.insert(0, '+');
        }
        *last_sign = if value < 0 { -1 } else { 1 };
        parts.push(rendered);
    }

    let mut parts = Vec::new();
    let mut last_sign = 0i8;
    let years = value.months / 12;
    let rem_months = value.months % 12;
    push_unit_part(
        &mut parts,
        &mut last_sign,
        i64::from(years),
        "year",
        "years",
    );
    push_unit_part(
        &mut parts,
        &mut last_sign,
        i64::from(rem_months),
        "mon",
        "mons",
    );
    push_unit_part(
        &mut parts,
        &mut last_sign,
        i64::from(value.days),
        "day",
        "days",
    );

    let abs_time = value.time_micros.unsigned_abs();
    let total_seconds = abs_time / 1_000_000;
    let subsec = abs_time % 1_000_000;
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;
    if value.time_micros != 0 || parts.is_empty() {
        let sign = if value.time_micros < 0 {
            "-"
        } else if last_sign < 0 {
            "+"
        } else {
            ""
        };
        let time_text = if subsec == 0 {
            format!("{sign}{hours:02}:{minutes:02}:{seconds:02}")
        } else {
            let mut rendered = format!("{sign}{hours:02}:{minutes:02}:{seconds:02}.{subsec:06}");
            while rendered.ends_with('0') {
                rendered.pop();
            }
            rendered
        };
        parts.push(time_text);
    }

    parts.join(" ")
}

fn render_interval_postgres_verbose(value: IntervalValue) -> String {
    let negative = value.is_negative();
    let sign = if negative { -1i128 } else { 1i128 };
    let mut parts = Vec::new();
    let months = i128::from(value.months) * sign;
    let years = months / 12;
    let rem_months = months % 12;
    push_verbose_unit(&mut parts, years, "year", "years");
    push_verbose_unit(&mut parts, rem_months, "mon", "mons");
    push_verbose_unit(&mut parts, i128::from(value.days) * sign, "day", "days");
    push_verbose_time_parts(&mut parts, value.time_micros, sign);

    if parts.is_empty() {
        parts.push("0".into());
    }
    let mut out = format!("@ {}", parts.join(" "));
    if negative {
        out.push_str(" ago");
    }
    out
}

fn push_verbose_unit(parts: &mut Vec<String>, value: i128, singular: &str, plural: &str) {
    if value == 0 {
        return;
    }
    let unit = if value == 1 { singular } else { plural };
    parts.push(format!("{value} {unit}"));
}

fn push_verbose_time_parts(parts: &mut Vec<String>, time_micros: i64, outer_sign: i128) {
    let signed_time = i128::from(time_micros) * outer_sign;
    if signed_time == 0 {
        return;
    }
    let sign = if signed_time < 0 { -1i128 } else { 1i128 };
    let abs_time = signed_time.unsigned_abs();
    let total_seconds = abs_time / 1_000_000;
    let subsec = abs_time % 1_000_000;
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;
    push_verbose_unit(parts, sign * hours as i128, "hour", "hours");
    push_verbose_unit(parts, sign * minutes as i128, "min", "mins");
    if seconds != 0 || subsec != 0 {
        let mut rendered = if subsec == 0 {
            seconds.to_string()
        } else {
            trim_fractional_seconds(format!("{seconds}.{subsec:06}"))
        };
        if sign < 0 {
            rendered.insert(0, '-');
        }
        let unit = if rendered == "1" { "sec" } else { "secs" };
        parts.push(format!("{rendered} {unit}"));
    }
}

fn render_interval_sql_standard(value: IntervalValue) -> String {
    if value.months == 0 && value.days == 0 && value.time_micros == 0 {
        return "0".into();
    }

    let has_year_month = value.months != 0;
    let has_day_time = value.days != 0 || value.time_micros != 0;
    let all_nonnegative = value.months >= 0 && value.days >= 0 && value.time_micros >= 0;
    let all_nonpositive = value.months <= 0 && value.days <= 0 && value.time_micros <= 0;

    if has_year_month && !has_day_time {
        return format_sql_year_month(value.months, false);
    }
    if !has_year_month && has_day_time && (all_nonnegative || all_nonpositive) {
        return format_sql_day_time(value.days, value.time_micros, false);
    }

    format!(
        "{} {} {}",
        format_sql_year_month(value.months, true),
        format_sql_signed_day(value.days),
        format_sql_signed_time(value.time_micros)
    )
}

fn format_sql_year_month(months: i32, force_sign: bool) -> String {
    let sign = if months < 0 {
        "-"
    } else if force_sign {
        "+"
    } else {
        ""
    };
    let abs_months = i64::from(months).abs();
    format!("{sign}{}-{}", abs_months / 12, abs_months % 12)
}

fn format_sql_signed_day(days: i32) -> String {
    if days < 0 {
        days.to_string()
    } else {
        format!("+{days}")
    }
}

fn format_sql_day_time(days: i32, time_micros: i64, force_sign: bool) -> String {
    if days == 0 {
        return format_sql_signed_time_with_options(time_micros, force_sign);
    }
    let sign = if days < 0 {
        "-"
    } else if force_sign {
        "+"
    } else {
        ""
    };
    let day_abs = i64::from(days).abs();
    let time = format_sql_time_abs(time_micros.unsigned_abs());
    format!("{sign}{day_abs} {time}")
}

fn format_sql_signed_time(time_micros: i64) -> String {
    format_sql_signed_time_with_options(time_micros, true)
}

fn format_sql_signed_time_with_options(time_micros: i64, force_sign: bool) -> String {
    let sign = if time_micros < 0 {
        "-"
    } else if force_sign {
        "+"
    } else {
        ""
    };
    format!("{sign}{}", format_sql_time_abs(time_micros.unsigned_abs()))
}

fn format_sql_time_abs(abs_time: u64) -> String {
    let total_seconds = abs_time / 1_000_000;
    let subsec = abs_time % 1_000_000;
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;
    if subsec == 0 {
        format!("{hours}:{minutes:02}:{seconds:02}")
    } else {
        trim_fractional_seconds(format!("{hours}:{minutes:02}:{seconds:02}.{subsec:06}"))
    }
}

fn render_interval_iso8601(value: IntervalValue) -> String {
    if value.months == 0 && value.days == 0 && value.time_micros == 0 {
        return "PT0S".into();
    }

    let mut out = String::from("P");
    let years = value.months / 12;
    let months = value.months % 12;
    if years != 0 {
        out.push_str(&format!("{years}Y"));
    }
    if months != 0 {
        out.push_str(&format!("{months}M"));
    }
    if value.days != 0 {
        out.push_str(&format!("{}D", value.days));
    }
    if value.time_micros != 0 {
        out.push('T');
        let sign = if value.time_micros < 0 { -1i128 } else { 1i128 };
        let abs_time = value.time_micros.unsigned_abs();
        let total_seconds = abs_time / 1_000_000;
        let subsec = abs_time % 1_000_000;
        let hours = total_seconds / 3600;
        let minutes = (total_seconds % 3600) / 60;
        let seconds = total_seconds % 60;
        if hours != 0 {
            out.push_str(&format!("{}H", sign * hours as i128));
        }
        if minutes != 0 {
            out.push_str(&format!("{}M", sign * minutes as i128));
        }
        if seconds != 0 || subsec != 0 || (hours == 0 && minutes == 0) {
            let mut second_text = if subsec == 0 {
                seconds.to_string()
            } else {
                trim_fractional_seconds(format!("{seconds}.{subsec:06}"))
            };
            if sign < 0 {
                second_text.insert(0, '-');
            }
            out.push_str(&format!("{second_text}S"));
        }
    }
    out
}

fn trim_fractional_seconds(mut text: String) -> String {
    while text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    text
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IntervalParseErrorKind {
    Invalid,
    FieldOutOfRange,
    IntervalOutOfRange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecimalIntervalError {
    Invalid,
    OutOfRange,
}

pub(crate) fn parse_interval_text_value(text: &str) -> Result<IntervalValue, ExecError> {
    parse_interval_text_value_with_style(text, IntervalStyle::Postgres)
}

pub(crate) fn parse_interval_text_value_with_style(
    text: &str,
    style: IntervalStyle,
) -> Result<IntervalValue, ExecError> {
    fn invalid(text: &str) -> ExecError {
        invalid_interval_text_error(text)
    }

    fn field_out_of_range(text: &str) -> ExecError {
        interval_field_value_out_of_range_error(text)
    }

    fn interval_out_of_range(_text: &str) -> ExecError {
        interval_out_of_range_error()
    }

    fn decimal_error(err: DecimalIntervalError, text: &str, field_error: bool) -> ExecError {
        match err {
            DecimalIntervalError::Invalid => invalid(text),
            DecimalIntervalError::OutOfRange if field_error => field_out_of_range(text),
            DecimalIntervalError::OutOfRange => interval_out_of_range(text),
        }
    }

    fn parse_error(kind: IntervalParseErrorKind, text: &str) -> ExecError {
        match kind {
            IntervalParseErrorKind::Invalid => invalid(text),
            IntervalParseErrorKind::FieldOutOfRange => field_out_of_range(text),
            IntervalParseErrorKind::IntervalOutOfRange => interval_out_of_range(text),
        }
    }

    fn apply_trailing_ago(
        value: IntervalValue,
        negative: bool,
        text: &str,
    ) -> Result<IntervalValue, ExecError> {
        if negative {
            if !value.is_finite() {
                return Err(field_out_of_range(text));
            }
            value
                .checked_negate()
                .ok_or_else(|| field_out_of_range(text))
        } else {
            Ok(value)
        }
    }

    let mut rest = text.trim();
    if rest.is_empty() {
        return Err(invalid(text));
    }

    if rest.eq_ignore_ascii_case("-infinity") || rest.eq_ignore_ascii_case("-inf") {
        return Ok(IntervalValue::neg_infinity());
    }
    if rest.eq_ignore_ascii_case("+infinity") || rest.eq_ignore_ascii_case("+inf") {
        return Ok(IntervalValue::infinity());
    }

    let mut negative = false;
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

    if rest.eq_ignore_ascii_case("infinity") || rest.eq_ignore_ascii_case("inf") {
        if negative {
            return Err(invalid(text));
        }
        return Ok(IntervalValue::infinity());
    }

    if rest.starts_with(['P', 'p']) {
        let value = parse_iso8601_interval(rest).map_err(|err| parse_error(err, text))?;
        return apply_trailing_ago(value, negative, text);
    }

    let tokens = expand_interval_tokens(rest);
    let force_negative = interval_sql_standard_force_negative(&tokens, style);
    if let Some(value) = parse_year_month_interval(&tokens) {
        return apply_trailing_ago(value.map_err(|err| parse_error(err, text))?, negative, text);
    }
    if tokens.len() == 1 && !tokens[0].contains(':') {
        let value = IntervalValue {
            time_micros: decimal_mul_round_to_i64(&tokens[0], 1_000_000)
                .map_err(|err| decimal_error(err, text, false))?,
            days: 0,
            months: 0,
        };
        return apply_trailing_ago(value, negative, text);
    }
    let mut accum = IntervalAccum::default();
    let mut seen_units: BTreeSet<&'static str> = BTreeSet::new();
    let mut saw_time_token = false;
    let mut saw_second_fraction = false;
    let mut saw_subsecond_unit = false;

    let mut idx = 0;
    while idx < tokens.len() {
        let token = tokens[idx].as_str();
        if token.contains(':') {
            if saw_time_token
                || seen_units
                    .iter()
                    .any(|unit| interval_unit_is_time_field(unit))
            {
                return Err(invalid(text));
            }
            saw_time_token = true;
            let time_token = interval_forced_negative_token(token, force_negative);
            accum
                .add_micros(
                    parse_interval_time_token(&time_token).map_err(|err| parse_error(err, text))?,
                )
                .map_err(|err| parse_error(err, text))?;
            idx += 1;
            continue;
        }

        let value_token = interval_forced_negative_token(token, force_negative);
        let token = value_token.as_str();
        let value = token.parse::<f64>().map_err(|_| invalid(text))?;
        if !value.is_finite() {
            return Err(invalid(text));
        }

        let Some(unit) = tokens.get(idx + 1) else {
            return Err(invalid(text));
        };
        if unit.contains(':') {
            if !seen_units.insert("day") || saw_time_token {
                return Err(invalid(text));
            }
            saw_time_token = true;
            accum
                .add_days(
                    decimal_trunc_to_i64(token).map_err(|err| decimal_error(err, text, true))?,
                )
                .map_err(|err| parse_error(err, text))?;
            add_interval_fractional_microseconds(value.fract(), USECS_PER_DAY, &mut accum)
                .map_err(|err| parse_error(err, text))?;
            let time_token = interval_forced_negative_token(unit, force_negative);
            accum
                .add_micros(
                    parse_interval_time_token(&time_token).map_err(|err| parse_error(err, text))?,
                )
                .map_err(|err| parse_error(err, text))?;
            idx += 2;
            continue;
        }
        if unit.eq_ignore_ascii_case("to") {
            return Err(invalid(text));
        }

        let Some(normalized_unit) = normalize_interval_unit(unit) else {
            return Err(invalid(text));
        };
        let unit_key = interval_unit_duplicate_key(&normalized_unit);
        if !seen_units.insert(unit_key) {
            return Err(invalid(text));
        }
        if saw_time_token && interval_unit_is_time_field(unit_key) {
            return Err(invalid(text));
        }
        if matches!(unit_key, "millisecond" | "microsecond") {
            if saw_second_fraction {
                return Err(invalid(text));
            }
            saw_subsecond_unit = true;
        } else if unit_key == "second" {
            let has_fraction = interval_decimal_has_fraction(token)
                .map_err(|err| decimal_error(err, text, true))?;
            if has_fraction && saw_subsecond_unit {
                return Err(invalid(text));
            }
            saw_second_fraction = has_fraction;
        }

        match normalized_unit.as_str() {
            "millennium" => {
                add_interval_year_unit(token, value, 1000, &mut accum)
                    .map_err(|err| parse_error(err, text))?;
            }
            "century" => {
                add_interval_year_unit(token, value, 100, &mut accum)
                    .map_err(|err| parse_error(err, text))?;
            }
            "decade" => {
                add_interval_year_unit(token, value, 10, &mut accum)
                    .map_err(|err| parse_error(err, text))?;
            }
            "year" => {
                let years =
                    decimal_trunc_to_i64(token).map_err(|err| decimal_error(err, text, true))?;
                accum
                    .add_years(years)
                    .map_err(|err| parse_error(err, text))?;
                add_interval_fractional_years(value, 1, &mut accum)
                    .map_err(|err| parse_error(err, text))?;
            }
            "month" => {
                accum
                    .add_months(
                        decimal_trunc_to_i64(token)
                            .map_err(|err| decimal_error(err, text, true))?,
                    )
                    .map_err(|err| parse_error(err, text))?;
                add_interval_fractional_days(value.fract(), 30, &mut accum)
                    .map_err(|err| parse_error(err, text))?;
            }
            "week" => {
                let total_days = decimal_trunc_to_i64(token)
                    .map_err(|err| decimal_error(err, text, true))?
                    .checked_mul(7)
                    .ok_or_else(|| field_out_of_range(text))?;
                accum
                    .add_days(total_days)
                    .map_err(|err| parse_error(err, text))?;
                add_interval_fractional_days(value.fract(), 7, &mut accum)
                    .map_err(|err| parse_error(err, text))?;
            }
            "day" => {
                accum
                    .add_days(
                        decimal_trunc_to_i64(token)
                            .map_err(|err| decimal_error(err, text, true))?,
                    )
                    .map_err(|err| parse_error(err, text))?;
                add_interval_fractional_microseconds(value.fract(), USECS_PER_DAY, &mut accum)
                    .map_err(|err| parse_error(err, text))?;
            }
            "hour" => {
                accum
                    .add_micros(
                        decimal_mul_round_to_i64(token, 3_600_000_000)
                            .map_err(|err| decimal_error(err, text, true))?,
                    )
                    .map_err(|err| parse_error(err, text))?;
            }
            "minute" => {
                accum
                    .add_micros(
                        decimal_mul_round_to_i64(token, 60_000_000)
                            .map_err(|err| decimal_error(err, text, true))?,
                    )
                    .map_err(|err| parse_error(err, text))?;
            }
            "second" => {
                accum
                    .add_micros(
                        decimal_mul_round_to_i64(token, 1_000_000)
                            .map_err(|err| decimal_error(err, text, true))?,
                    )
                    .map_err(|err| parse_error(err, text))?;
            }
            "millisecond" => {
                accum
                    .add_micros(
                        decimal_mul_round_to_i64(token, 1_000)
                            .map_err(|err| decimal_error(err, text, true))?,
                    )
                    .map_err(|err| parse_error(err, text))?;
            }
            "microsecond" => {
                accum
                    .add_micros(
                        decimal_mul_round_to_i64(token, 1)
                            .map_err(|err| decimal_error(err, text, true))?,
                    )
                    .map_err(|err| parse_error(err, text))?;
            }
            _ => return Err(invalid(text)),
        }
        idx += 2;
    }

    let value = accum.finish().map_err(|err| parse_error(err, text))?;
    apply_trailing_ago(value, negative, text)
}

fn expand_interval_tokens(text: &str) -> Vec<String> {
    text.split_whitespace()
        .flat_map(|token| {
            if token.contains(':') {
                return vec![token.to_string()];
            }
            split_compact_interval_token(token)
                .map(|(value, unit)| vec![value, unit])
                .unwrap_or_else(|| vec![token.to_string()])
        })
        .collect()
}

fn interval_sql_standard_force_negative(tokens: &[String], style: IntervalStyle) -> bool {
    matches!(style, IntervalStyle::SqlStandard)
        && tokens.first().is_some_and(|token| token.starts_with('-'))
        && !tokens
            .iter()
            .skip(1)
            .any(|token| interval_token_has_explicit_sign(token))
}

fn interval_token_has_explicit_sign(token: &str) -> bool {
    token.starts_with(['+', '-'])
        || token
            .split(':')
            .skip(1)
            .any(|part| part.starts_with(['+', '-']))
}

fn interval_forced_negative_token(token: &str, force_negative: bool) -> String {
    if force_negative && !interval_token_has_explicit_sign(token) {
        format!("-{token}")
    } else {
        token.to_string()
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct IntervalAccum {
    years: i32,
    months: i32,
    days: i32,
    micros: i64,
}

impl IntervalAccum {
    fn add_years(&mut self, delta: i64) -> Result<(), IntervalParseErrorKind> {
        self.years =
            add_i32_checked(self.years, delta).ok_or(IntervalParseErrorKind::FieldOutOfRange)?;
        Ok(())
    }

    fn add_months(&mut self, delta: i64) -> Result<(), IntervalParseErrorKind> {
        self.months =
            add_i32_checked(self.months, delta).ok_or(IntervalParseErrorKind::FieldOutOfRange)?;
        Ok(())
    }

    fn add_days(&mut self, delta: i64) -> Result<(), IntervalParseErrorKind> {
        self.days =
            add_i32_checked(self.days, delta).ok_or(IntervalParseErrorKind::FieldOutOfRange)?;
        Ok(())
    }

    fn add_micros(&mut self, delta: i64) -> Result<(), IntervalParseErrorKind> {
        self.micros = self
            .micros
            .checked_add(delta)
            .ok_or(IntervalParseErrorKind::FieldOutOfRange)?;
        Ok(())
    }

    fn finish(self) -> Result<IntervalValue, IntervalParseErrorKind> {
        let months = i64::from(self.years)
            .checked_mul(12)
            .and_then(|months| months.checked_add(i64::from(self.months)))
            .and_then(|months| i32::try_from(months).ok())
            .ok_or(IntervalParseErrorKind::IntervalOutOfRange)?;
        Ok(IntervalValue {
            time_micros: self.micros,
            days: self.days,
            months,
        })
    }
}

fn add_interval_fractional_years(
    value: f64,
    multiplier: i32,
    accum: &mut IntervalAccum,
) -> Result<(), IntervalParseErrorKind> {
    let rem = value.fract();
    if rem == 0.0 {
        return Ok(());
    }
    let months = (rem * f64::from(multiplier) * 12.0).round_ties_even();
    if !months.is_finite() || months < i64::MIN as f64 || months > i64::MAX as f64 {
        return Err(IntervalParseErrorKind::FieldOutOfRange);
    }
    accum.add_months(months as i64)
}

fn add_interval_fractional_days(
    fraction: f64,
    scale: i32,
    accum: &mut IntervalAccum,
) -> Result<(), IntervalParseErrorKind> {
    if fraction == 0.0 {
        return Ok(());
    }
    let total = fraction * f64::from(scale);
    if !total.is_finite() || total < i32::MIN as f64 || total > i32::MAX as f64 {
        return Err(IntervalParseErrorKind::FieldOutOfRange);
    }
    let extra_days = total.trunc() as i64;
    accum.add_days(extra_days)?;
    add_interval_fractional_microseconds(total - extra_days as f64, USECS_PER_DAY, accum)
}

fn add_interval_fractional_microseconds(
    fraction: f64,
    scale: i64,
    accum: &mut IntervalAccum,
) -> Result<(), IntervalParseErrorKind> {
    if fraction == 0.0 {
        return Ok(());
    }
    let usecs = pg_round_fractional_microseconds(fraction, scale)?;
    accum.add_micros(usecs)
}

fn pg_round_fractional_microseconds(
    fraction: f64,
    scale: i64,
) -> Result<i64, IntervalParseErrorKind> {
    let scaled = fraction * scale as f64;
    if !scaled.is_finite() || scaled < i64::MIN as f64 || scaled > i64::MAX as f64 {
        return Err(IntervalParseErrorKind::FieldOutOfRange);
    }
    let mut usecs = scaled.trunc() as i64;
    let rem = scaled - usecs as f64;
    if rem > 0.5 {
        usecs = usecs
            .checked_add(1)
            .ok_or(IntervalParseErrorKind::FieldOutOfRange)?;
    } else if rem < -0.5 {
        usecs = usecs
            .checked_sub(1)
            .ok_or(IntervalParseErrorKind::FieldOutOfRange)?;
    }
    Ok(usecs)
}

fn parse_iso8601_interval(text: &str) -> Result<IntervalValue, IntervalParseErrorKind> {
    let body = text
        .strip_prefix(['P', 'p'])
        .ok_or(IntervalParseErrorKind::Invalid)?;
    if body.is_empty() {
        return Err(IntervalParseErrorKind::Invalid);
    }
    if let Some(value) = parse_iso8601_alternative(body)? {
        return Ok(value);
    }

    let mut accum = IntervalAccum::default();
    let saw = parse_iso8601_designators(body, false, &mut accum)?;
    if !saw {
        return Err(IntervalParseErrorKind::Invalid);
    }
    accum.finish()
}

fn parse_iso8601_alternative(body: &str) -> Result<Option<IntervalValue>, IntervalParseErrorKind> {
    let (date_part, time_part) = body.split_once(['T', 't']).unwrap_or((body, ""));
    if date_part.chars().any(|ch| ch.is_ascii_alphabetic()) {
        return Ok(None);
    }
    if date_part.is_empty() && time_part.chars().any(|ch| ch.is_ascii_alphabetic()) {
        return Ok(None);
    }
    if date_part.is_empty() && time_part.is_empty() {
        return Ok(None);
    }

    let mut accum = IntervalAccum::default();
    if !date_part.is_empty() {
        parse_iso8601_alternative_date(date_part, &mut accum)?;
    }
    if !time_part.is_empty() {
        parse_iso8601_alternative_time(time_part, &mut accum)?;
    }
    accum.finish().map(Some)
}

fn parse_iso8601_alternative_date(
    text: &str,
    accum: &mut IntervalAccum,
) -> Result<(), IntervalParseErrorKind> {
    if iso8601_alternative_date_has_separators(text) {
        let parts = split_iso8601_alternative_date(text)?;
        if parts.is_empty() || parts.len() > 3 || parts.iter().any(|part| part.is_empty()) {
            return Err(IntervalParseErrorKind::Invalid);
        }
        add_iso8601_unit(parts[0], 'Y', false, accum)?;
        if let Some(month) = parts.get(1) {
            add_iso8601_unit(month, 'M', false, accum)?;
        }
        if let Some(day) = parts.get(2) {
            add_iso8601_unit(day, 'D', false, accum)?;
        }
        return Ok(());
    }
    let unsigned = text.strip_prefix(['+', '-']).unwrap_or(text);
    if !unsigned.chars().all(|ch| ch.is_ascii_digit() || ch == '.') {
        return Err(IntervalParseErrorKind::Invalid);
    }
    if text.starts_with(['+', '-']) || !matches!(text.len(), 4 | 6 | 8) {
        add_iso8601_unit(text, 'Y', false, accum)?;
        return Ok(());
    }
    add_iso8601_unit(&text[0..4], 'Y', false, accum)?;
    if text.len() >= 6 {
        add_iso8601_unit(&text[4..6], 'M', false, accum)?;
    }
    if text.len() == 8 {
        add_iso8601_unit(&text[6..8], 'D', false, accum)?;
    }
    Ok(())
}

fn parse_iso8601_alternative_time(
    text: &str,
    accum: &mut IntervalAccum,
) -> Result<(), IntervalParseErrorKind> {
    if text.chars().any(|ch| ch.is_ascii_alphabetic()) {
        parse_iso8601_designators(text, true, accum)?;
        return Ok(());
    }
    if text.contains(':') {
        parse_iso8601_alternative_colon_time(text, accum)?;
        return Ok(());
    }
    if text.chars().all(|ch| ch.is_ascii_digit()) && matches!(text.len(), 4 | 6) {
        add_iso8601_unit(&text[0..2], 'H', true, accum)?;
        add_iso8601_unit(&text[2..4], 'M', true, accum)?;
        if text.len() == 6 {
            add_iso8601_unit(&text[4..6], 'S', true, accum)?;
        }
        return Ok(());
    }
    add_iso8601_unit(text, 'H', true, accum)
}

fn parse_iso8601_designators(
    text: &str,
    mut in_time: bool,
    accum: &mut IntervalAccum,
) -> Result<bool, IntervalParseErrorKind> {
    let mut pos = 0usize;
    let mut saw = false;
    while pos < text.len() {
        let ch = text[pos..]
            .chars()
            .next()
            .ok_or(IntervalParseErrorKind::Invalid)?;
        if ch == 'T' || ch == 't' {
            if in_time {
                return Err(IntervalParseErrorKind::Invalid);
            }
            in_time = true;
            pos += ch.len_utf8();
            continue;
        }
        let start = pos;
        pos = consume_iso8601_number(text, pos)?;
        if pos >= text.len() {
            return Err(IntervalParseErrorKind::Invalid);
        }
        let unit = text[pos..]
            .chars()
            .next()
            .ok_or(IntervalParseErrorKind::Invalid)?;
        if !unit.is_ascii_alphabetic() || unit.eq_ignore_ascii_case(&'T') {
            return Err(IntervalParseErrorKind::Invalid);
        }
        pos += unit.len_utf8();
        add_iso8601_unit(&text[start..pos - unit.len_utf8()], unit, in_time, accum)?;
        saw = true;
    }
    Ok(saw)
}

fn consume_iso8601_number(text: &str, mut pos: usize) -> Result<usize, IntervalParseErrorKind> {
    if let Some(ch) = text[pos..].chars().next()
        && matches!(ch, '+' | '-')
    {
        pos += ch.len_utf8();
    }
    let mut saw_digit = false;
    let mut saw_dot = false;
    while pos < text.len() {
        let ch = text[pos..]
            .chars()
            .next()
            .ok_or(IntervalParseErrorKind::Invalid)?;
        if ch.is_ascii_digit() {
            saw_digit = true;
            pos += ch.len_utf8();
        } else if ch == '.' && !saw_dot {
            saw_dot = true;
            pos += ch.len_utf8();
        } else {
            break;
        }
    }
    if let Some(ch) = text[pos..].chars().next()
        && matches!(ch, 'e' | 'E')
    {
        let exp_start = pos;
        pos += ch.len_utf8();
        if let Some(sign) = text[pos..].chars().next()
            && matches!(sign, '+' | '-')
        {
            pos += sign.len_utf8();
        }
        let digit_start = pos;
        while pos < text.len() {
            let ch = text[pos..]
                .chars()
                .next()
                .ok_or(IntervalParseErrorKind::Invalid)?;
            if !ch.is_ascii_digit() {
                break;
            }
            pos += ch.len_utf8();
        }
        if digit_start == pos {
            pos = exp_start;
        }
    }
    if !saw_digit {
        return Err(IntervalParseErrorKind::Invalid);
    }
    Ok(pos)
}

fn iso8601_alternative_date_has_separators(text: &str) -> bool {
    text.char_indices().any(|(idx, ch)| ch == '-' && idx != 0)
}

fn split_iso8601_alternative_date(text: &str) -> Result<Vec<&str>, IntervalParseErrorKind> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    for (idx, ch) in text.char_indices() {
        if ch == '-' && idx != 0 {
            parts.push(&text[start..idx]);
            start = idx + ch.len_utf8();
        }
    }
    parts.push(&text[start..]);
    Ok(parts)
}

fn parse_iso8601_number_parts(token: &str) -> Result<(i64, f64), IntervalParseErrorKind> {
    let first = token
        .chars()
        .next()
        .ok_or(IntervalParseErrorKind::Invalid)?;
    if !(first.is_ascii_digit() || matches!(first, '-' | '+' | '.')) {
        return Err(IntervalParseErrorKind::Invalid);
    }
    let value = token
        .parse::<f64>()
        .map_err(|_| IntervalParseErrorKind::Invalid)?;
    if value.is_nan() || !(-1.0e15..=1.0e15).contains(&value) {
        return Err(IntervalParseErrorKind::FieldOutOfRange);
    }
    let ipart = if value >= 0.0 {
        value.floor()
    } else {
        -(-value).floor()
    };
    Ok((ipart as i64, value - ipart))
}

fn add_iso8601_microseconds(
    token: &str,
    scale: i64,
    accum: &mut IntervalAccum,
) -> Result<(), IntervalParseErrorKind> {
    let (value, fraction) = parse_iso8601_number_parts(token)?;
    let whole = value
        .checked_mul(scale)
        .ok_or(IntervalParseErrorKind::FieldOutOfRange)?;
    accum.add_micros(whole)?;
    add_interval_fractional_microseconds(fraction, scale, accum)
}

fn parse_iso8601_alternative_colon_time(
    text: &str,
    accum: &mut IntervalAccum,
) -> Result<(), IntervalParseErrorKind> {
    let parts = text.split(':').collect::<Vec<_>>();
    if !(2..=3).contains(&parts.len()) || parts.iter().any(|part| part.is_empty()) {
        return Err(IntervalParseErrorKind::Invalid);
    }
    add_iso8601_unit(parts[0], 'H', true, accum)?;
    add_iso8601_unit(parts[1], 'M', true, accum)?;
    if let Some(seconds) = parts.get(2) {
        add_iso8601_unit(seconds, 'S', true, accum)?;
    }
    Ok(())
}

fn add_iso8601_unit(
    token: &str,
    unit: char,
    in_time: bool,
    accum: &mut IntervalAccum,
) -> Result<(), IntervalParseErrorKind> {
    let (value, fraction) = parse_iso8601_number_parts(token)?;
    match unit.to_ascii_uppercase() {
        'Y' if !in_time => {
            accum.add_years(value)?;
            add_interval_fractional_years(fraction, 1, accum)
        }
        'M' if !in_time => {
            accum.add_months(value)?;
            add_interval_fractional_days(fraction, 30, accum)
        }
        'W' if !in_time => {
            let days = value
                .checked_mul(7)
                .ok_or(IntervalParseErrorKind::FieldOutOfRange)?;
            accum.add_days(days)?;
            add_interval_fractional_days(fraction, 7, accum)
        }
        'D' if !in_time => {
            accum.add_days(value)?;
            add_interval_fractional_microseconds(fraction, USECS_PER_DAY, accum)
        }
        'H' if in_time => add_iso8601_microseconds(token, 3_600_000_000, accum),
        'M' if in_time => add_iso8601_microseconds(token, 60_000_000, accum),
        'S' if in_time => add_iso8601_microseconds(token, 1_000_000, accum),
        _ => Err(IntervalParseErrorKind::Invalid),
    }
}

fn decimal_interval_parse_error(err: DecimalIntervalError) -> IntervalParseErrorKind {
    match err {
        DecimalIntervalError::Invalid => IntervalParseErrorKind::Invalid,
        DecimalIntervalError::OutOfRange => IntervalParseErrorKind::FieldOutOfRange,
    }
}

fn parse_year_month_interval(
    tokens: &[String],
) -> Option<Result<IntervalValue, IntervalParseErrorKind>> {
    let (years, months) = parse_year_month_token(tokens.first()?.as_str())?;
    let mut accum = IntervalAccum::default();
    if tokens.len() == 1
        || (tokens.len() == 4
            && tokens[1].eq_ignore_ascii_case("year")
            && tokens[2].eq_ignore_ascii_case("to")
            && tokens[3].eq_ignore_ascii_case("month"))
    {
        return Some(
            accum
                .add_years(years)
                .and_then(|()| accum.add_months(months))
                .and_then(|()| accum.finish()),
        );
    }
    if tokens.len() == 3 && tokens[2].contains(':') {
        return Some(
            accum
                .add_years(years)
                .and_then(|()| accum.add_months(months))
                .and_then(|()| {
                    let days =
                        decimal_trunc_to_i64(&tokens[1]).map_err(decimal_interval_parse_error)?;
                    accum.add_days(days)
                })
                .and_then(|()| {
                    let micros = parse_interval_time_token(&tokens[2])?;
                    accum.add_micros(micros)
                })
                .and_then(|()| accum.finish()),
        );
    }
    None
}

fn parse_year_month_token(value: &str) -> Option<(i64, i64)> {
    let unsigned = value.strip_prefix(['+', '-']).unwrap_or(value);
    let split = unsigned.find('-')?;
    let split = value.len() - unsigned.len() + split;
    let years = value[..split].parse::<i64>().ok()?;
    let month_text = &value[split + 1..];
    if month_text.starts_with(['+', '-']) {
        return None;
    }
    let months = month_text.parse::<i64>().ok()?;
    if !(0..12).contains(&months) {
        return None;
    }
    let month_sign = if value.starts_with('-') { -1 } else { 1 };
    Some((years, months * month_sign))
}

fn split_compact_interval_token(token: &str) -> Option<(String, String)> {
    let split = token
        .char_indices()
        .find(|(_, ch)| ch.is_ascii_alphabetic())
        .map(|(idx, _)| idx)?;
    if split == 0 {
        return None;
    }
    let (value, unit) = token.split_at(split);
    value.parse::<f64>().ok()?;
    Some((value.to_string(), unit.to_string()))
}

fn normalize_interval_unit(unit: &str) -> Option<String> {
    let normalized = unit.to_ascii_lowercase();
    match normalized.as_str() {
        "ms" | "msec" | "msecs" => return Some("millisecond".to_string()),
        "us" | "usec" | "usecs" => return Some("microsecond".to_string()),
        "s" => return Some("second".to_string()),
        "centuries" => return Some("century".to_string()),
        _ => {}
    }
    let singular = normalized.trim_end_matches('s');
    Some(
        match singular {
            "y" | "yr" | "year" => "year",
            "millennium" | "millennia" => "millennium",
            "century" => "century",
            "decade" => "decade",
            "mon" | "month" => "month",
            "w" | "week" => "week",
            "d" | "day" => "day",
            "h" | "hour" | "hr" => "hour",
            "m" | "min" | "minute" => "minute",
            "s" | "sec" | "second" => "second",
            "ms" | "msec" | "millisecond" => "millisecond",
            "us" | "usec" | "microsecond" => "microsecond",
            _ => return None,
        }
        .to_string(),
    )
}

fn interval_unit_duplicate_key(unit: &str) -> &'static str {
    match unit {
        "millennium" => "millennium",
        "century" => "century",
        "decade" => "decade",
        "year" => "year",
        "month" => "month",
        "week" => "week",
        "day" => "day",
        "hour" => "hour",
        "minute" => "minute",
        "second" => "second",
        "millisecond" => "millisecond",
        "microsecond" => "microsecond",
        _ => "unknown",
    }
}

fn interval_unit_is_time_field(unit: &str) -> bool {
    matches!(
        unit,
        "hour" | "minute" | "second" | "millisecond" | "microsecond"
    )
}

fn interval_decimal_has_fraction(text: &str) -> Result<bool, DecimalIntervalError> {
    let (coeff, scale) = parse_interval_decimal(text)?;
    if scale == 0 {
        return Ok(false);
    }
    let denom = pow10_bigint_checked(scale)?;
    Ok(!coeff.mod_floor(&denom).is_zero())
}

fn add_interval_year_unit(
    token: &str,
    value: f64,
    multiplier: i64,
    accum: &mut IntervalAccum,
) -> Result<(), IntervalParseErrorKind> {
    let years = decimal_trunc_to_i64(token)
        .map_err(decimal_interval_parse_error)?
        .checked_mul(multiplier)
        .ok_or(IntervalParseErrorKind::FieldOutOfRange)?;
    let multiplier = i32::try_from(multiplier).map_err(|_| IntervalParseErrorKind::Invalid)?;
    accum.add_years(years)?;
    add_interval_fractional_years(value, multiplier, accum)?;
    Ok(())
}

fn parse_interval_time_token(token: &str) -> Result<i64, IntervalParseErrorKind> {
    let parts = token.split(':').collect::<Vec<_>>();
    if !(2..=3).contains(&parts.len()) {
        return Err(IntervalParseErrorKind::Invalid);
    }
    let mut inherited_sign = 1i64;
    let (hours, _, sign) = parse_time_micros_part(parts[0], inherited_sign, USECS_PER_HOUR, true)?;
    inherited_sign = sign;
    let (minutes, minute_value, sign) =
        parse_time_micros_part(parts[1], inherited_sign, USECS_PER_MINUTE, false)?;
    inherited_sign = sign;
    let (seconds_micros, second_value, _) = parse_time_micros_part(
        parts.get(2).copied().unwrap_or("0"),
        inherited_sign,
        1_000_000,
        true,
    )?;
    if minute_value >= 60.0 || second_value >= 60.0 {
        return Err(IntervalParseErrorKind::Invalid);
    }
    let micros = hours
        .checked_add(minutes)
        .and_then(|value| value.checked_add(seconds_micros))
        .ok_or(IntervalParseErrorKind::FieldOutOfRange)?;
    Ok(micros)
}

fn parse_time_micros_part(
    token: &str,
    inherited_sign: i64,
    scale: i64,
    allow_fraction: bool,
) -> Result<(i64, f64, i64), IntervalParseErrorKind> {
    let (sign, magnitude) = signed_time_part(token, inherited_sign)?;
    if !allow_fraction && magnitude.contains('.') {
        return Err(IntervalParseErrorKind::Invalid);
    }
    let abs_value = magnitude
        .parse::<f64>()
        .map_err(|_| IntervalParseErrorKind::Invalid)?
        .abs();
    if !abs_value.is_finite() {
        return Err(IntervalParseErrorKind::Invalid);
    }
    let signed = if sign < 0 {
        format!("-{magnitude}")
    } else {
        magnitude.to_string()
    };
    let micros = decimal_mul_round_to_i64(&signed, scale).map_err(|err| {
        if matches!(err, DecimalIntervalError::Invalid) {
            IntervalParseErrorKind::Invalid
        } else {
            IntervalParseErrorKind::FieldOutOfRange
        }
    })?;
    Ok((micros, abs_value, sign))
}

fn signed_time_part(
    token: &str,
    inherited_sign: i64,
) -> Result<(i64, &str), IntervalParseErrorKind> {
    if token.is_empty() {
        return Err(IntervalParseErrorKind::Invalid);
    }
    if let Some(rest) = token.strip_prefix('-') {
        if rest.is_empty() {
            return Err(IntervalParseErrorKind::Invalid);
        }
        Ok((-1, rest))
    } else if let Some(rest) = token.strip_prefix('+') {
        if rest.is_empty() {
            return Err(IntervalParseErrorKind::Invalid);
        }
        Ok((1, rest))
    } else {
        Ok((inherited_sign, token))
    }
}

fn add_i32_checked(value: i32, delta: i64) -> Option<i32> {
    value.checked_add(i32::try_from(delta).ok()?)
}

fn checked_round_f64_to_i64(value: f64) -> Option<i64> {
    if !value.is_finite() {
        return None;
    }
    let rounded = value.round();
    if rounded < i64::MIN as f64 || rounded >= 9_223_372_036_854_775_808.0 {
        return None;
    }
    Some(rounded as i64)
}

fn decimal_trunc_to_i64(text: &str) -> Result<i64, DecimalIntervalError> {
    let (coeff, scale) = parse_interval_decimal(text)?;
    let denom = pow10_bigint_checked(scale)?;
    (coeff / denom)
        .to_i64()
        .ok_or(DecimalIntervalError::OutOfRange)
}

fn decimal_mul_trunc_to_i64(text: &str, multiplier: i64) -> Result<i64, DecimalIntervalError> {
    let (coeff, scale) = parse_interval_decimal(text)?;
    let denom = pow10_bigint_checked(scale)?;
    ((coeff * multiplier) / denom)
        .to_i64()
        .ok_or(DecimalIntervalError::OutOfRange)
}

fn decimal_mul_round_to_i64(text: &str, multiplier: i64) -> Result<i64, DecimalIntervalError> {
    let (coeff, scale) = parse_interval_decimal(text)?;
    let denom = pow10_bigint_checked(scale)?;
    round_bigint_ratio_to_i64(coeff * multiplier, denom)
}

fn round_bigint_ratio_to_i64(numer: BigInt, denom: BigInt) -> Result<i64, DecimalIntervalError> {
    let q = &numer / &denom;
    let r = &numer % &denom;
    let mut rounded = q;
    if (r.abs() * 2u8) >= denom {
        if numer.is_negative() {
            rounded -= 1u8;
        } else {
            rounded += 1u8;
        }
    }
    rounded.to_i64().ok_or(DecimalIntervalError::OutOfRange)
}

fn parse_interval_decimal(text: &str) -> Result<(BigInt, u32), DecimalIntervalError> {
    let trimmed = text.trim();
    if trimmed.is_empty() || trimmed.chars().any(|ch| ch.is_ascii_whitespace()) {
        return Err(DecimalIntervalError::Invalid);
    }
    let (mantissa, exponent) = match trimmed.find(['e', 'E']) {
        Some(index) => (
            &trimmed[..index],
            parse_interval_decimal_exponent(&trimmed[index + 1..])?,
        ),
        None => (trimmed, 0),
    };
    let (negative, unsigned) = if let Some(rest) = mantissa.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = mantissa.strip_prefix('+') {
        (false, rest)
    } else {
        (false, mantissa)
    };
    let Some((whole, frac)) = split_decimal_mantissa(unsigned) else {
        return Err(DecimalIntervalError::Invalid);
    };
    if whole.is_empty() && frac.is_empty() {
        return Err(DecimalIntervalError::Invalid);
    }
    if !whole.chars().all(|ch| ch.is_ascii_digit()) || !frac.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(DecimalIntervalError::Invalid);
    }
    let mut digits = format!("{whole}{frac}");
    if digits.is_empty() {
        digits.push('0');
    }
    let mut scale = frac.len() as i64 - i64::from(exponent);
    if scale < 0 {
        let extra = usize::try_from(-scale).map_err(|_| DecimalIntervalError::OutOfRange)?;
        digits.extend(std::iter::repeat_n('0', extra));
        scale = 0;
    }
    let mut coeff =
        BigInt::parse_bytes(digits.as_bytes(), 10).ok_or(DecimalIntervalError::Invalid)?;
    if negative {
        coeff = -coeff;
    }
    let scale = u32::try_from(scale).map_err(|_| DecimalIntervalError::OutOfRange)?;
    Ok((coeff, scale))
}

fn split_decimal_mantissa(text: &str) -> Option<(&str, &str)> {
    let mut parts = text.split('.');
    let whole = parts.next()?;
    let frac = parts.next().unwrap_or("");
    if parts.next().is_some() {
        return None;
    }
    Some((whole, frac))
}

fn parse_interval_decimal_exponent(text: &str) -> Result<i32, DecimalIntervalError> {
    if text.is_empty() {
        return Err(DecimalIntervalError::Invalid);
    }
    text.parse::<i32>()
        .map_err(|_| DecimalIntervalError::Invalid)
}

fn pow10_bigint_checked(exp: u32) -> Result<BigInt, DecimalIntervalError> {
    if exp > 10_000 {
        return Err(DecimalIntervalError::OutOfRange);
    }
    let mut value = BigInt::from(1u8);
    for _ in 0..exp {
        value *= 10u8;
    }
    Ok(value)
}

pub(crate) fn canonicalize_interval_text(text: &str) -> Result<String, ExecError> {
    parse_interval_text_value(text).map(render_interval_text)
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

pub(crate) fn parse_text_array_literal_with_catalog_and_op(
    raw: &str,
    element_type: SqlType,
    op: &'static str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<Value, ExecError> {
    parse_text_array_literal_with_options_and_catalog(raw, element_type, op, true, catalog)
}

pub(crate) fn parse_text_array_literal_with_options(
    raw: &str,
    element_type: SqlType,
    op: &'static str,
    explicit: bool,
) -> Result<Value, ExecError> {
    parse_text_array_literal_with_options_and_catalog(raw, element_type, op, explicit, None)
}

fn parse_text_array_literal_with_options_and_catalog(
    raw: &str,
    element_type: SqlType,
    op: &'static str,
    explicit: bool,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<Value, ExecError> {
    let (bounds, input) = parse_array_bounds_prefix(raw)?;
    let element_type_oid = array_element_type_oid(element_type);
    if input == "{}" {
        let mut array = ArrayValue::empty();
        if let Some(element_type_oid) = element_type_oid {
            array = array.with_element_type_oid(element_type_oid);
        }
        return Ok(Value::PgArray(array));
    }
    if !input.starts_with('{') || !input.ends_with('}') {
        return Err(invalid_array_literal(
            raw,
            Some("Array value must start with \"{\" or dimension information.".into()),
        ));
    }
    let mut parser = ArrayTextParser::new(input, element_type, explicit, catalog);
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
    let mut array =
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
    if let Some(element_type_oid) = element_type_oid {
        array = array.with_element_type_oid(element_type_oid);
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
    catalog: Option<&'a dyn CatalogLookup>,
}

impl<'a> ArrayTextParser<'a> {
    fn new(
        input: &'a str,
        element_type: SqlType,
        explicit: bool,
        catalog: Option<&'a dyn CatalogLookup>,
    ) -> Self {
        Self {
            input,
            offset: 0,
            element_type,
            explicit,
            catalog,
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
                self.cast_item_text(&text)
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
                    self.cast_item_text(text.trim_end())
                }
            }
            None => self.type_mismatch(),
        }
    }

    fn cast_item_text(&self, text: &str) -> Result<Value, ExecError> {
        if matches!(self.element_type.kind, SqlTypeKind::Enum) {
            return cast_text_to_enum(text, self.element_type, self.catalog);
        }
        if let Some(result) = eval_user_defined_type_input(
            text,
            self.element_type,
            self.catalog,
            &DateTimeConfig::default(),
        )? {
            return Ok(result);
        }
        cast_text_value(text, self.element_type, self.explicit)
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

fn parse_input_type_name(
    type_name: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<Option<SqlType>, ExecError> {
    let parsed = match parse_type_name(type_name.trim()) {
        Ok(ty) => ty,
        Err(_) => return Ok(None),
    };
    if let Some(catalog) = catalog
        && let Ok(ty) = resolve_raw_type_name(&parsed, catalog)
    {
        return Ok(Some(ty).filter(|ty| input_type_name_supported(*ty, Some(catalog))));
    }
    let resolved = match parsed {
        RawTypeName::Builtin(ty) => Some(ty),
        RawTypeName::Named { name, array_bounds } => {
            let base = catalog
                .and_then(|catalog| catalog.type_by_name(&name))
                .or_else(|| {
                    builtin_type_rows()
                        .into_iter()
                        .find(|row| row.typrelid == 0 && row.typname.eq_ignore_ascii_case(&name))
                })
                .map(|row| {
                    let typrelid = if row.sql_type.typrelid != 0 {
                        row.sql_type.typrelid
                    } else {
                        row.typrelid
                    };
                    row.sql_type.with_identity(row.oid, typrelid)
                });
            let base = base.or_else(|| {
                builtin_type_rows()
                    .into_iter()
                    .find(|row| row.typrelid == 0 && row.typname.eq_ignore_ascii_case(&name))
                    .map(|row| row.sql_type.with_identity(row.oid, row.typrelid))
            });
            base.map(|ty| {
                if array_bounds == 0 {
                    ty
                } else {
                    SqlType::array_of(ty)
                }
            })
        }
        RawTypeName::Serial(_) | RawTypeName::Record => None,
    };
    Ok(resolved.filter(|ty| input_type_name_supported(*ty, catalog)))
}

fn input_type_name_supported(parsed: SqlType, catalog: Option<&dyn CatalogLookup>) -> bool {
    if uses_user_defined_input_function(parsed, catalog) {
        return true;
    }
    if !parsed.is_array && matches!(parsed.kind, SqlTypeKind::Composite) && parsed.typrelid != 0 {
        return true;
    }
    if !parsed.is_array
        && matches!(
            parsed.kind,
            SqlTypeKind::Text | SqlTypeKind::Char | SqlTypeKind::Varchar
        )
    {
        return true;
    }
    if !parsed.is_array && (parsed.is_range() || parsed.is_multirange()) {
        return true;
    }
    if !parsed.is_array && matches!(parsed.kind, SqlTypeKind::Enum) && parsed.type_oid != 0 {
        return true;
    }
    let Some(type_oid) = builtin_type_oid(parsed) else {
        return false;
    };
    explicit_text_input_target_oids().contains(&type_oid)
}

fn uses_user_defined_input_function(ty: SqlType, catalog: Option<&dyn CatalogLookup>) -> bool {
    if ty.is_array {
        return uses_user_defined_input_function(ty.element_type(), catalog);
    }
    user_defined_base_type_row(ty, catalog).is_some()
}

fn user_defined_base_type_row(
    ty: SqlType,
    catalog: Option<&dyn CatalogLookup>,
) -> Option<crate::include::catalog::PgTypeRow> {
    let catalog = catalog?;
    if ty.type_oid == 0 || matches!(ty.kind, SqlTypeKind::Shell) {
        return None;
    }
    let row = catalog.type_by_oid(ty.type_oid)?;
    if row.typinput == 0 || row.typrelid != 0 || row.sql_type.is_array {
        return None;
    }
    if builtin_type_rows()
        .into_iter()
        .any(|builtin| builtin.oid == row.oid)
    {
        return None;
    }
    Some(row)
}

fn eval_user_defined_type_input(
    text: &str,
    ty: SqlType,
    catalog: Option<&dyn CatalogLookup>,
    _config: &DateTimeConfig,
) -> Result<Option<Value>, ExecError> {
    let Some(row) = user_defined_base_type_row(ty, catalog) else {
        return Ok(None);
    };
    let Some(proc_row) = catalog.and_then(|catalog| catalog.proc_row_by_oid(row.typinput)) else {
        return Ok(Some(Value::Text(CompactString::new(text))));
    };
    let names = [proc_row.prosrc.as_str(), proc_row.proname.as_str()];
    if proc_name_matches(&names, &["widget_in", "pg_rust_test_widget_in"]) {
        return eval_pg_rust_test_widget_in(&[Value::Text(text.into())]).map(Some);
    }
    if proc_name_matches(&names, &["widget_out", "pg_rust_test_widget_out"]) {
        return eval_pg_rust_test_widget_out(&[Value::Text(text.into())]).map(Some);
    }
    if proc_name_matches(&names, &["int44in", "pg_rust_test_int44in"]) {
        return eval_pg_rust_test_int44in(&[Value::Text(text.into())]).map(Some);
    }
    if proc_name_matches(&names, &["int44out", "pg_rust_test_int44out"]) {
        return eval_pg_rust_test_int44out(&[Value::Text(text.into())]).map(Some);
    }
    if proc_name_matches(&names, &["int4in"]) {
        let value = cast_text_to_int4(text)?;
        let Value::Int32(value) = value else {
            unreachable!("int4 input must produce int4 datum");
        };
        return Ok(Some(Value::Text(CompactString::from_owned(
            value.to_string(),
        ))));
    }
    if proc_name_matches(&names, &["textin", "textout"]) {
        return Ok(Some(Value::Text(CompactString::new(text))));
    }
    if proc_name_matches(&names, &["varcharin", "varcharout"]) {
        return Ok(Some(Value::Text(CompactString::from_owned(
            coerce_character_string(
                text,
                SqlType::new(SqlTypeKind::Varchar).with_typmod(ty.typmod),
                false,
            )?,
        ))));
    }
    if proc_name_matches(&names, &["boolin"]) {
        let value = parse_pg_bool_text(text)?;
        return Ok(Some(Value::Text(CompactString::new(if value {
            "t"
        } else {
            "f"
        }))));
    }
    Ok(Some(Value::Text(CompactString::new(text))))
}

fn proc_name_matches(names: &[&str], candidates: &[&str]) -> bool {
    names.iter().any(|name| {
        candidates
            .iter()
            .any(|candidate| name.eq_ignore_ascii_case(candidate))
    })
}

fn validate_composite_text_input(
    text: &str,
    ty: SqlType,
    catalog: Option<&dyn CatalogLookup>,
    config: &DateTimeConfig,
) -> Result<(), ExecError> {
    let Some(catalog) = catalog else {
        return Err(unsupported_record_input());
    };
    let relation = catalog
        .relation_by_oid(ty.typrelid)
        .or_else(|| catalog.lookup_relation_by_oid(ty.typrelid))
        .ok_or_else(unsupported_record_input)?;
    let fields = parse_composite_literal_fields(text)?;
    let columns = relation
        .desc
        .columns
        .iter()
        .filter(|column| !column.dropped)
        .collect::<Vec<_>>();
    if fields.len() != columns.len() {
        return Err(ExecError::DetailedError {
            message: "malformed record literal".into(),
            detail: Some(format!(
                "record literal has {} fields but type expects {}",
                fields.len(),
                columns.len()
            )),
            hint: None,
            sqlstate: "22P02",
        });
    }
    for (column, raw) in columns.into_iter().zip(fields) {
        if let Some(raw) = raw {
            cast_value_with_source_type_catalog_and_config(
                Value::Text(raw.into()),
                Some(SqlType::new(SqlTypeKind::Text)),
                column.sql_type,
                Some(catalog),
                config,
            )?;
        }
    }
    Ok(())
}

fn parse_composite_literal_fields(text: &str) -> Result<Vec<Option<String>>, ExecError> {
    let body = text
        .strip_prefix('(')
        .and_then(|rest| rest.strip_suffix(')'))
        .ok_or_else(|| ExecError::DetailedError {
            message: "malformed record literal".into(),
            detail: Some(format!("missing left parenthesis in \"{text}\"")),
            hint: None,
            sqlstate: "22P02",
        })?;
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut escaped = false;
    for ch in body.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }
        match ch {
            '\\' if quoted => escaped = true,
            '"' => quoted = !quoted,
            ',' if !quoted => {
                fields.push((!current.is_empty()).then(|| current.clone()));
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    if quoted {
        return Err(ExecError::DetailedError {
            message: "malformed record literal".into(),
            detail: Some(format!("unterminated quoted string in \"{text}\"")),
            hint: None,
            sqlstate: "22P02",
        });
    }
    fields.push((!current.is_empty()).then_some(current));
    Ok(fields)
}

pub(crate) fn render_pg_lsn_text(value: u64) -> String {
    format!("{:X}/{:X}", value >> 32, value & 0xFFFF_FFFF)
}

pub(crate) fn pg_lsn_out_of_range() -> ExecError {
    ExecError::DetailedError {
        message: "pg_lsn out of range".into(),
        detail: None,
        hint: None,
        sqlstate: "22003",
    }
}

fn invalid_pg_lsn_input(text: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("invalid input syntax for type pg_lsn: \"{text}\""),
        detail: None,
        hint: None,
        sqlstate: "22P02",
    }
}

pub(crate) fn parse_pg_lsn_text(text: &str) -> Result<u64, ExecError> {
    let Some((hi, lo)) = text.split_once('/') else {
        return Err(invalid_pg_lsn_input(text));
    };
    if hi.is_empty()
        || lo.is_empty()
        || hi.len() > 8
        || lo.len() > 8
        || !hi.bytes().all(|b| b.is_ascii_hexdigit())
        || !lo.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(invalid_pg_lsn_input(text));
    }
    let hi = u64::from_str_radix(hi, 16).map_err(|_| invalid_pg_lsn_input(text))?;
    let lo = u64::from_str_radix(lo, 16).map_err(|_| invalid_pg_lsn_input(text))?;
    Ok((hi << 32) | lo)
}

fn builtin_type_oid(sql_type: SqlType) -> Option<u32> {
    if let Some(row) = builtin_type_rows()
        .into_iter()
        .find(|row| row.sql_type == sql_type)
    {
        return Some(row.oid);
    }
    if let Some(range_type) = range_type_ref_for_sql_type(sql_type) {
        if sql_type.is_array {
            return builtin_type_rows()
                .into_iter()
                .find(|row| row.typelem == range_type.type_oid())
                .map(|row| row.oid);
        }
        return Some(range_type.type_oid());
    }
    if let Some(multirange_type) = multirange_type_ref_for_sql_type(sql_type) {
        if sql_type.is_array {
            return builtin_type_rows()
                .into_iter()
                .find(|row| row.typelem == multirange_type.type_oid())
                .map(|row| row.oid);
        }
        return Some(multirange_type.type_oid());
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
        ExecError::InvalidUuidInput { value } => {
            format!("invalid input syntax for type uuid: \"{value}\"")
        }
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
        ExecError::InvalidStorageValue { column, details }
            if matches!(column.as_str(), "inet" | "cidr") =>
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
        | ExecError::InvalidUuidInput { .. }
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
            } else if details.starts_with("date/time field value out of range:")
                || details.starts_with("date out of range:")
            {
                "22008"
            } else {
                "22007"
            }
        }
        ExecError::InvalidStorageValue { column, .. }
            if matches!(column.as_str(), "inet" | "cidr") =>
        {
            "22P02"
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

pub(crate) fn datetime_parse_error_details(
    ty: &'static str,
    text: &str,
    err: DateTimeParseError,
) -> String {
    match err {
        DateTimeParseError::Invalid => format!("invalid input syntax for type {ty}: \"{text}\""),
        DateTimeParseError::FieldOutOfRange => {
            format!("date/time field value out of range: \"{text}\"")
        }
        DateTimeParseError::TimeZoneDisplacementOutOfRange => {
            format!("time zone displacement out of range: \"{text}\"")
        }
        DateTimeParseError::TimestampOutOfRange => format!("timestamp out of range: \"{text}\""),
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
    soft_input_error_info_with_catalog_and_config(text, type_name, None, config)
}

pub(crate) fn soft_input_error_info_with_catalog_and_config(
    text: &str,
    type_name: &str,
    catalog: Option<&dyn CatalogLookup>,
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

    let ty = parse_input_type_name(type_name, catalog)?.ok_or_else(|| {
        ExecError::InvalidStorageValue {
            column: type_name.to_string(),
            details: format!("unsupported type: {type_name}"),
        }
    })?;
    if uses_user_defined_input_function(ty, catalog) {
        cast_value_with_source_type_catalog_and_config(
            Value::Text(text.into()),
            Some(SqlType::new(SqlTypeKind::Text)),
            ty,
            catalog,
            config,
        )?;
        return Ok(None);
    }
    if !ty.is_array && matches!(ty.kind, SqlTypeKind::Composite) && ty.typrelid != 0 {
        validate_composite_text_input(text, ty, catalog, config)?;
        return Ok(None);
    }
    if !ty.is_array
        && matches!(
            ty.kind,
            SqlTypeKind::RegProc
                | SqlTypeKind::RegProcedure
                | SqlTypeKind::RegOper
                | SqlTypeKind::RegOperator
                | SqlTypeKind::RegClass
                | SqlTypeKind::RegType
                | SqlTypeKind::RegRole
                | SqlTypeKind::RegNamespace
                | SqlTypeKind::RegCollation
                | SqlTypeKind::RegConfig
                | SqlTypeKind::RegDictionary
        )
    {
        return match expr_reg::resolve_reg_object_oid(text, ty.kind, catalog) {
            Ok(_) => Ok(None),
            Err(err)
                if ty.kind == SqlTypeKind::RegType
                    && expr_reg::is_hard_regtype_input_error(&err) =>
            {
                Err(err)
            }
            Err(err) => Ok(Some(input_error_info(err, text))),
        };
    }
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
        _ => cast_value_with_source_type_catalog_and_config(
            Value::Text(text.into()),
            Some(SqlType::new(SqlTypeKind::Text)),
            ty,
            catalog,
            config,
        ),
    };
    match parsed {
        Ok(_) => Ok(None),
        Err(err) => Ok(Some(input_error_info(err, text))),
    }
}

pub(crate) fn cast_value(value: Value, ty: SqlType) -> Result<Value, ExecError> {
    cast_value_with_source_type_catalog_and_config(
        value,
        None,
        ty,
        None,
        &DateTimeConfig::default(),
    )
}

pub(crate) fn cast_value_with_config(
    value: Value,
    ty: SqlType,
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    cast_value_with_source_type_catalog_and_config(value, None, ty, None, config)
}

pub(crate) fn cast_value_with_source_type_and_config(
    value: Value,
    source_type: Option<SqlType>,
    ty: SqlType,
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    cast_value_with_source_type_catalog_and_config(value, source_type, ty, None, config)
}

fn enforce_domain_check(
    value: Value,
    ty: SqlType,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<Value, ExecError> {
    let Some(catalog) = catalog else {
        return Ok(value);
    };
    let Some(check) = catalog.domain_check_by_type_oid(ty.type_oid) else {
        return Ok(value);
    };
    let Some(limit) = parse_upper_less_than_domain_check(&check) else {
        return Ok(value);
    };
    if domain_upper_less_than_limit(&value, limit) {
        return Ok(value);
    }
    let domain_name = catalog
        .type_by_oid(ty.type_oid)
        .map(|row| row.typname)
        .unwrap_or_else(|| ty.type_oid.to_string());
    Err(ExecError::DetailedError {
        message: format!(
            "value for domain {domain_name} violates check constraint \"{domain_name}_check\""
        ),
        detail: None,
        hint: None,
        sqlstate: "23514",
    })
}

fn parse_upper_less_than_domain_check(check: &str) -> Option<i64> {
    let normalized = check
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>()
        .to_ascii_lowercase();
    let rest = normalized
        .strip_prefix("check(")?
        .strip_prefix("upper(value)<")?;
    let number = rest.strip_suffix(')')?;
    number.parse().ok()
}

fn domain_upper_less_than_limit(value: &Value, limit: i64) -> bool {
    let Value::Range(range) = value else {
        return true;
    };
    if range.empty {
        return true;
    }
    let Some(upper) = &range.upper else {
        return true;
    };
    match upper.value.as_ref() {
        Value::Int16(v) => i64::from(*v) < limit,
        Value::Int32(v) => i64::from(*v) < limit,
        Value::Int64(v) => *v < limit,
        _ => true,
    }
}

pub(crate) fn cast_value_with_source_type_catalog_and_config(
    value: Value,
    source_type: Option<SqlType>,
    ty: SqlType,
    catalog: Option<&dyn CatalogLookup>,
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
                        casted.push(cast_value_with_source_type_catalog_and_config(
                            item,
                            None,
                            element_type,
                            catalog,
                            config,
                        )?);
                    }
                    Ok(Value::PgArray(ArrayValue::from_dimensions(
                        array.dimensions,
                        casted,
                    )))
                } else {
                    let mut casted = Vec::with_capacity(items.len());
                    for item in items {
                        casted.push(cast_value_with_source_type_catalog_and_config(
                            item,
                            None,
                            element_type,
                            catalog,
                            config,
                        )?);
                    }
                    Ok(Value::Array(casted))
                }
            }
            Value::PgArray(array) => {
                let element_type = ty.element_type();
                let mut casted = Vec::with_capacity(array.elements.len());
                for item in array.elements {
                    casted.push(cast_value_with_source_type_catalog_and_config(
                        item,
                        None,
                        element_type,
                        catalog,
                        config,
                    )?);
                }
                Ok(Value::PgArray(ArrayValue::from_dimensions(
                    array.dimensions,
                    casted,
                )))
            }
            other => match other.as_text() {
                Some(text) => {
                    let trimmed = text.trim_start();
                    if !trimmed.starts_with('{') && !trimmed.starts_with('[') {
                        match ty.element_type().kind {
                            SqlTypeKind::Int2 => parse_int2vector_array_text(text),
                            kind if is_oid_vector_array_element(kind) => {
                                parse_oidvector_array_text(
                                    text,
                                    array_element_type_oid(ty.element_type())
                                        .unwrap_or(OID_TYPE_OID),
                                )
                            }
                            _ => parse_text_array_literal_with_options_and_catalog(
                                text,
                                ty.element_type(),
                                "::array",
                                true,
                                catalog,
                            ),
                        }
                    } else {
                        parse_text_array_literal_with_options_and_catalog(
                            text,
                            ty.element_type(),
                            "::array",
                            true,
                            catalog,
                        )
                    }
                }
                None => Err(ExecError::TypeMismatch {
                    op: "::array",
                    left: other,
                    right: Value::Null,
                }),
            },
        };
    }

    if matches!(
        ty.kind,
        SqlTypeKind::RegProc
            | SqlTypeKind::RegProcedure
            | SqlTypeKind::RegOper
            | SqlTypeKind::RegOperator
            | SqlTypeKind::RegClass
            | SqlTypeKind::RegType
            | SqlTypeKind::RegRole
            | SqlTypeKind::RegNamespace
            | SqlTypeKind::RegCollation
            | SqlTypeKind::RegConfig
            | SqlTypeKind::RegDictionary
    ) && !ty.is_array
    {
        if let Some(text) = regclass_text_input(&value, source_type) {
            return expr_reg::cast_text_to_reg_object(text, ty.kind, catalog);
        }
    }
    if matches!(ty.kind, SqlTypeKind::Enum) && !ty.is_array {
        if let Some(text) = value.as_text() {
            return cast_text_to_enum(text, ty, catalog);
        }
    }
    if let Some(text) = value.as_text()
        && let Some(result) = eval_user_defined_type_input(text, ty, catalog, config)?
    {
        return Ok(result);
    }
    if matches!(ty.kind, SqlTypeKind::Text)
        && !ty.is_array
        && source_type.is_some_and(|source| {
            matches!(source.element_type().kind, SqlTypeKind::RegNamespace) && !source.is_array
        })
    {
        return cast_regnamespace_to_text(&value, catalog);
    }

    if let Some(result) = cast_geometry_value(value.clone(), ty) {
        return result;
    }

    let result = match value {
        Value::Null => Ok(Value::Null),
        Value::EnumOid(v) => match ty.kind {
            SqlTypeKind::Enum => {
                if let Some(catalog) = catalog {
                    let enum_type_oid = enum_catalog_type_oid(ty);
                    ensure_enum_label_safe(catalog, enum_type_oid, v)?;
                    enforce_enum_domain_constraints(Value::EnumOid(v), ty, catalog)?;
                }
                Ok(Value::EnumOid(v))
            }
            SqlTypeKind::AnyEnum => Ok(Value::EnumOid(v)),
            SqlTypeKind::Text => {
                if let Some(label) = source_type
                    .filter(|source| matches!(source.kind, SqlTypeKind::Enum))
                    .and_then(|source| {
                        catalog.and_then(|catalog| {
                            catalog.enum_label(enum_catalog_type_oid(source), v)
                        })
                    })
                    .or_else(|| catalog.and_then(|catalog| catalog.enum_label_by_oid(v)))
                {
                    Ok(Value::Text(CompactString::from_owned(label)))
                } else {
                    Ok(Value::Text(CompactString::from_owned(v.to_string())))
                }
            }
            _ => cast_text_value(&v.to_string(), ty, true),
        },
        Value::Int16(v) => match ty {
            ty if ty.is_range() => cast_text_value(&v.to_string(), ty, true),
            ty if ty.is_multirange() => cast_text_value(&v.to_string(), ty, true),
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
                    | SqlTypeKind::RegProc
                    | SqlTypeKind::RegClass
                    | SqlTypeKind::RegType
                    | SqlTypeKind::RegRole
                    | SqlTypeKind::RegNamespace
                    | SqlTypeKind::RegOper
                    | SqlTypeKind::RegOperator
                    | SqlTypeKind::RegProcedure
                    | SqlTypeKind::RegCollation
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
                kind: SqlTypeKind::Bit | SqlTypeKind::VarBit,
                ..
            } => Ok(Value::Bit(cast_integer_to_bit_string(
                v as u16 as u64,
                16,
                v < 0,
                ty,
            ))),
            SqlType {
                kind:
                    SqlTypeKind::Text
                    | SqlTypeKind::Cstring
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
                    | SqlTypeKind::Internal
                    | SqlTypeKind::InternalChar
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
                    | SqlTypeKind::RegDictionary
                    | SqlTypeKind::Inet
                    | SqlTypeKind::Cidr
                    | SqlTypeKind::Uuid
                    | SqlTypeKind::PgLsn
                    | SqlTypeKind::Enum
                    | SqlTypeKind::MacAddr
                    | SqlTypeKind::MacAddr8,
                ..
            } => cast_text_value(&v.to_string(), ty, true),
            SqlType {
                kind: SqlTypeKind::Bool,
                ..
            } => Ok(cast_integer_to_bool(v as i64)),
            SqlType {
                kind: SqlTypeKind::Bytea,
                ..
            } => Ok(Value::Bytea(v.to_be_bytes().to_vec())),
            SqlType {
                kind:
                    SqlTypeKind::AnyElement
                    | SqlTypeKind::AnyRange
                    | SqlTypeKind::AnyMultirange
                    | SqlTypeKind::AnyCompatible
                    | SqlTypeKind::AnyCompatibleArray
                    | SqlTypeKind::AnyCompatibleRange
                    | SqlTypeKind::AnyCompatibleMultirange
                    | SqlTypeKind::AnyEnum,
                ..
            } => Ok(Value::Text(CompactString::from_owned(v.to_string()))),
            SqlType {
                kind: SqlTypeKind::AnyArray,
                ..
            } => Err(unsupported_anyarray_input()),
            SqlType {
                kind: SqlTypeKind::Record | SqlTypeKind::Composite | SqlTypeKind::Shell,
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
            SqlType {
                kind: SqlTypeKind::Multirange,
                ..
            } => unreachable!("multirange handled above"),
        },
        Value::Int32(v) => match ty {
            ty if ty.is_range() => cast_text_value(&v.to_string(), ty, true),
            ty if ty.is_multirange() => cast_text_value(&v.to_string(), ty, true),
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
                    | SqlTypeKind::RegProc
                    | SqlTypeKind::RegClass
                    | SqlTypeKind::RegType
                    | SqlTypeKind::RegRole
                    | SqlTypeKind::RegNamespace
                    | SqlTypeKind::RegOper
                    | SqlTypeKind::RegOperator
                    | SqlTypeKind::RegProcedure
                    | SqlTypeKind::RegCollation
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
                kind: SqlTypeKind::Bit | SqlTypeKind::VarBit,
                ..
            } => Ok(Value::Bit(cast_integer_to_bit_string(
                v as u32 as u64,
                32,
                v < 0,
                ty,
            ))),
            SqlType {
                kind:
                    SqlTypeKind::Text
                    | SqlTypeKind::Cstring
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
                    | SqlTypeKind::Internal
                    | SqlTypeKind::InternalChar
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
                    | SqlTypeKind::RegDictionary
                    | SqlTypeKind::Inet
                    | SqlTypeKind::Cidr
                    | SqlTypeKind::Uuid
                    | SqlTypeKind::PgLsn
                    | SqlTypeKind::Enum
                    | SqlTypeKind::MacAddr
                    | SqlTypeKind::MacAddr8,
                ..
            } => cast_text_value(&v.to_string(), ty, true),
            SqlType {
                kind: SqlTypeKind::Bool,
                ..
            } => Ok(cast_integer_to_bool(v as i64)),
            SqlType {
                kind: SqlTypeKind::Bytea,
                ..
            } => Ok(Value::Bytea(v.to_be_bytes().to_vec())),
            SqlType {
                kind:
                    SqlTypeKind::AnyElement
                    | SqlTypeKind::AnyRange
                    | SqlTypeKind::AnyMultirange
                    | SqlTypeKind::AnyCompatible
                    | SqlTypeKind::AnyCompatibleArray
                    | SqlTypeKind::AnyCompatibleRange
                    | SqlTypeKind::AnyCompatibleMultirange
                    | SqlTypeKind::AnyEnum,
                ..
            } => Ok(Value::Text(CompactString::from_owned(v.to_string()))),
            SqlType {
                kind: SqlTypeKind::AnyArray,
                ..
            } => Err(unsupported_anyarray_input()),
            SqlType {
                kind: SqlTypeKind::Record | SqlTypeKind::Composite | SqlTypeKind::Shell,
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
            SqlType {
                kind: SqlTypeKind::Multirange,
                ..
            } => unreachable!("multirange handled above"),
        },
        Value::Bool(v) => match ty {
            ty if ty.is_range() => cast_text_value(if v { "true" } else { "false" }, ty, true),
            ty if ty.is_multirange() => cast_text_value(if v { "true" } else { "false" }, ty, true),
            SqlType {
                kind: SqlTypeKind::Bool,
                ..
            } => Ok(Value::Bool(v)),
            SqlType {
                kind:
                    SqlTypeKind::Text
                    | SqlTypeKind::Cstring
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
                    | SqlTypeKind::Internal
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
                    | SqlTypeKind::RegDictionary
                    | SqlTypeKind::Inet
                    | SqlTypeKind::Cidr
                    | SqlTypeKind::PgLsn
                    | SqlTypeKind::Enum
                    | SqlTypeKind::MacAddr
                    | SqlTypeKind::MacAddr8,
                ..
            } => cast_text_value(if v { "true" } else { "false" }, ty, true),
            SqlType {
                kind:
                    SqlTypeKind::Int2
                    | SqlTypeKind::Int4
                    | SqlTypeKind::Int8
                    | SqlTypeKind::Oid
                    | SqlTypeKind::RegProc
                    | SqlTypeKind::RegClass
                    | SqlTypeKind::RegType
                    | SqlTypeKind::RegRole
                    | SqlTypeKind::RegNamespace
                    | SqlTypeKind::RegOper
                    | SqlTypeKind::RegOperator
                    | SqlTypeKind::RegProcedure
                    | SqlTypeKind::RegCollation
                    | SqlTypeKind::Xid
                    | SqlTypeKind::Bytea
                    | SqlTypeKind::Float4
                    | SqlTypeKind::Float8
                    | SqlTypeKind::Money
                    | SqlTypeKind::Numeric
                    | SqlTypeKind::Uuid,
                ..
            } => Err(ExecError::TypeMismatch {
                op: "::int4",
                left: Value::Bool(v),
                right: Value::Int32(0),
            }),
            SqlType {
                kind:
                    SqlTypeKind::AnyElement
                    | SqlTypeKind::AnyRange
                    | SqlTypeKind::AnyMultirange
                    | SqlTypeKind::AnyCompatible
                    | SqlTypeKind::AnyCompatibleArray
                    | SqlTypeKind::AnyCompatibleRange
                    | SqlTypeKind::AnyCompatibleMultirange
                    | SqlTypeKind::AnyEnum,
                ..
            } => Ok(Value::Text(CompactString::new(if v {
                "true"
            } else {
                "false"
            }))),
            SqlType {
                kind: SqlTypeKind::AnyArray,
                ..
            } => Err(unsupported_anyarray_input()),
            SqlType {
                kind: SqlTypeKind::Record | SqlTypeKind::Composite | SqlTypeKind::Shell,
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
            SqlType {
                kind: SqlTypeKind::Multirange,
                ..
            } => unreachable!("multirange handled above"),
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
            SqlTypeKind::Time => Ok(apply_time_precision(Value::Time(v), ty.time_precision())),
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
            SqlTypeKind::TimeTz => Ok(apply_time_precision(Value::TimeTz(v), ty.time_precision())),
            SqlTypeKind::Time => Ok(Value::Time(v.time)),
            SqlTypeKind::Text
            | SqlTypeKind::Name
            | SqlTypeKind::Char
            | SqlTypeKind::Varchar
            | SqlTypeKind::Json
            | SqlTypeKind::Jsonb
            | SqlTypeKind::JsonPath => cast_text_value_with_config(
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
            SqlTypeKind::Timestamp => Ok(apply_time_precision(
                Value::Timestamp(v),
                ty.time_precision(),
            )),
            SqlTypeKind::TimestampTz => Ok(Value::TimestampTz(timestamp_to_timestamptz(v, config))),
            SqlTypeKind::Date => Ok(Value::Date(timestamp_date_part(v))),
            SqlTypeKind::Time => timestamp_time_part(v).map(Value::Time).ok_or_else(|| {
                ExecError::InvalidStorageValue {
                    column: "time".into(),
                    details: "invalid input syntax for type time".into(),
                }
            }),
            SqlTypeKind::TimeTz => timestamp_time_tz_part(v, config)
                .map(Value::TimeTz)
                .ok_or_else(|| ExecError::InvalidStorageValue {
                    column: "timetz".into(),
                    details: "invalid input syntax for type time with time zone".into(),
                }),
            SqlTypeKind::Text
            | SqlTypeKind::Name
            | SqlTypeKind::Char
            | SqlTypeKind::Varchar
            | SqlTypeKind::Json
            | SqlTypeKind::Jsonb
            | SqlTypeKind::JsonPath => cast_text_value_with_config(
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
            SqlTypeKind::TimestampTz => Ok(apply_time_precision(
                Value::TimestampTz(v),
                ty.time_precision(),
            )),
            SqlTypeKind::Timestamp => Ok(Value::Timestamp(timestamptz_local_timestamp(v, config))),
            SqlTypeKind::Date => Ok(Value::Date(timestamp_date_part(
                timestamptz_local_timestamp(v, config),
            ))),
            SqlTypeKind::Time => timestamp_time_part(timestamptz_local_timestamp(v, config))
                .map(Value::Time)
                .ok_or_else(|| ExecError::InvalidStorageValue {
                    column: "time".into(),
                    details: "invalid input syntax for type time".into(),
                }),
            SqlTypeKind::TimeTz => timestamptz_time_tz_part(v, config)
                .map(Value::TimeTz)
                .ok_or_else(|| ExecError::InvalidStorageValue {
                    column: "timetz".into(),
                    details: "invalid input syntax for type time with time zone".into(),
                }),
            SqlTypeKind::Text
            | SqlTypeKind::Name
            | SqlTypeKind::Char
            | SqlTypeKind::Varchar
            | SqlTypeKind::Json
            | SqlTypeKind::Jsonb
            | SqlTypeKind::JsonPath => cast_text_value_with_config(
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
        Value::Interval(v) => match ty.kind {
            SqlTypeKind::Interval => Ok(Value::Interval(apply_interval_typmod(v, ty)?)),
            SqlTypeKind::Time => {
                if !v.is_finite() {
                    return Err(cannot_convert_infinite_interval_to_time_error());
                }
                Ok(Value::Time(TimeADT(
                    i128::from(v.time_micros).rem_euclid(i128::from(USECS_PER_DAY)) as i64,
                )))
            }
            SqlTypeKind::Text
            | SqlTypeKind::Name
            | SqlTypeKind::Char
            | SqlTypeKind::Varchar
            | SqlTypeKind::Json
            | SqlTypeKind::Jsonb
            | SqlTypeKind::JsonPath => cast_text_value_with_config(
                &render_interval_text_with_config(v, config),
                ty,
                true,
                config,
            ),
            _ => Err(ExecError::TypeMismatch {
                op: "::interval",
                left: Value::Interval(v),
                right: Value::Null,
            }),
        },
        Value::Text(text) => {
            if matches!(ty.kind, SqlTypeKind::Enum) {
                cast_text_to_enum(text.as_str(), ty, catalog)
            } else {
                cast_text_value_with_config(text.as_str(), ty, true, config)
            }
        }
        Value::TextRef(ptr, len) => {
            let text = unsafe {
                std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len as usize))
            };
            if matches!(ty.kind, SqlTypeKind::Enum) {
                cast_text_to_enum(text, ty, catalog)
            } else {
                cast_text_value_with_config(text, ty, true, config)
            }
        }
        Value::Range(range) => {
            let casted = if ty.is_range() {
                if range_type_ref_for_sql_type(ty).is_some_and(|target_type| {
                    target_type.type_oid() == range.range_type.type_oid()
                }) {
                    Ok(Value::Range(range))
                } else {
                    cast_text_value_with_config(
                        &render_range_text_with_config(&Value::Range(range.clone()), config)
                            .unwrap_or_default(),
                        ty,
                        true,
                        config,
                    )
                }
            } else if ty.is_multirange() {
                let singleton = multirange_from_range(&range)?;
                if multirange_type_ref_for_sql_type(ty).is_some_and(|target_type| {
                    target_type.type_oid() == singleton.multirange_type.type_oid()
                }) {
                    Ok(Value::Multirange(singleton))
                } else {
                    cast_text_value_with_config(
                        &render_multirange_text_with_config(&Value::Multirange(singleton), config)
                            .unwrap_or_default(),
                        ty,
                        true,
                        config,
                    )
                }
            } else {
                match ty.kind {
                    SqlTypeKind::Text
                    | SqlTypeKind::Cstring
                    | SqlTypeKind::Name
                    | SqlTypeKind::Char
                    | SqlTypeKind::Varchar
                    | SqlTypeKind::Json
                    | SqlTypeKind::Jsonb
                    | SqlTypeKind::JsonPath => cast_text_value_with_config(
                        &render_range_text_with_config(&Value::Range(range.clone()), config)
                            .unwrap_or_default(),
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
            }?;
            enforce_domain_check(casted, ty, catalog)
        }
        Value::Multirange(multirange) => {
            if ty.is_multirange() {
                Ok(Value::Multirange(multirange))
            } else {
                match ty.kind {
                    SqlTypeKind::Text
                    | SqlTypeKind::Cstring
                    | SqlTypeKind::Name
                    | SqlTypeKind::Char
                    | SqlTypeKind::Varchar
                    | SqlTypeKind::Json
                    | SqlTypeKind::Jsonb
                    | SqlTypeKind::JsonPath => cast_text_value_with_config(
                        &render_multirange_text_with_config(
                            &Value::Multirange(multirange.clone()),
                            config,
                        )
                        .unwrap_or_default(),
                        ty,
                        true,
                        config,
                    ),
                    _ => Err(ExecError::TypeMismatch {
                        op: "::multirange",
                        left: Value::Multirange(multirange),
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
        Value::PgLsn(value) => match ty.kind {
            SqlTypeKind::PgLsn => Ok(Value::PgLsn(value)),
            SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar => Ok(
                Value::Text(CompactString::from_owned(render_pg_lsn_text(value))),
            ),
            _ => Err(ExecError::TypeMismatch {
                op: "::pg_lsn",
                left: Value::PgLsn(value),
                right: Value::Null,
            }),
        },
        Value::Xid8(value) => match ty {
            ty if ty.is_range() => cast_text_value(&value.to_string(), ty, true),
            ty if ty.is_multirange() => cast_text_value(&value.to_string(), ty, true),
            SqlType {
                kind: SqlTypeKind::Int8,
                type_oid: XID8_TYPE_OID,
                ..
            } => Ok(Value::Xid8(value)),
            SqlType {
                kind: SqlTypeKind::Xid,
                ..
            } => Ok(Value::Int64((value as u32) as i64)),
            SqlType {
                kind: SqlTypeKind::Int8,
                ..
            } => i64::try_from(value)
                .map(Value::Int64)
                .map_err(|_| ExecError::Int8OutOfRange),
            SqlType {
                kind: SqlTypeKind::Numeric,
                ..
            } => Ok(Value::Numeric(NumericValue::finite(BigInt::from(value), 0))),
            SqlType {
                kind:
                    SqlTypeKind::Text
                    | SqlTypeKind::Cstring
                    | SqlTypeKind::Name
                    | SqlTypeKind::Char
                    | SqlTypeKind::Varchar,
                ..
            } => Ok(Value::Text(CompactString::from_owned(value.to_string()))),
            _ => cast_text_value(&value.to_string(), ty, true),
        },
        Value::Bytea(bytes) => {
            match ty.kind {
                SqlTypeKind::Bytea => Ok(Value::Bytea(bytes)),
                SqlTypeKind::Int2 => bytea_to_signed_int(&bytes, 2, "smallint")
                    .map(|value| Value::Int16(value as i16)),
                SqlTypeKind::Int4 => bytea_to_signed_int(&bytes, 4, "integer")
                    .map(|value| Value::Int32(value as i32)),
                SqlTypeKind::Int8 => bytea_to_signed_int(&bytes, 8, "bigint").map(Value::Int64),
                _ => Err(ExecError::TypeMismatch {
                    op: "::bytea",
                    left: Value::Bytea(bytes),
                    right: Value::Null,
                }),
            }
        }
        Value::Uuid(value) => match ty.kind {
            SqlTypeKind::Uuid => Ok(Value::Uuid(value)),
            SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar => {
                Ok(Value::Text(CompactString::from_owned(
                    crate::backend::executor::value_io::render_uuid_text(&value),
                )))
            }
            _ => Err(ExecError::TypeMismatch {
                op: "::uuid",
                left: Value::Uuid(value),
                right: Value::Null,
            }),
        },
        Value::Inet(value) => match ty.kind {
            SqlTypeKind::Inet => Ok(Value::Inet(value)),
            SqlTypeKind::Cidr => parse_cidr_text(&value.render_cidr()).map(Value::Cidr),
            SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar => {
                Ok(Value::Text(CompactString::from_owned(value.render_cidr())))
            }
            _ => Err(ExecError::TypeMismatch {
                op: "::inet",
                left: Value::Inet(value),
                right: Value::Null,
            }),
        },
        Value::Cidr(value) => match ty.kind {
            SqlTypeKind::Cidr => Ok(Value::Cidr(value)),
            SqlTypeKind::Inet => Ok(Value::Inet(value)),
            SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar => {
                Ok(Value::Text(CompactString::from_owned(value.render_cidr())))
            }
            _ => Err(ExecError::TypeMismatch {
                op: "::cidr",
                left: Value::Cidr(value),
                right: Value::Null,
            }),
        },
        Value::MacAddr(value) => match ty.kind {
            SqlTypeKind::MacAddr => Ok(Value::MacAddr(value)),
            SqlTypeKind::MacAddr8 => Ok(Value::MacAddr8(macaddr_to_macaddr8(value))),
            SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar => Ok(
                Value::Text(CompactString::from_owned(render_macaddr_text(&value))),
            ),
            _ => Err(ExecError::TypeMismatch {
                op: "::macaddr",
                left: Value::MacAddr(value),
                right: Value::Null,
            }),
        },
        Value::MacAddr8(value) => match ty.kind {
            SqlTypeKind::MacAddr8 => Ok(Value::MacAddr8(value)),
            SqlTypeKind::MacAddr => macaddr8_to_macaddr(value).map(Value::MacAddr),
            SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar => Ok(
                Value::Text(CompactString::from_owned(render_macaddr8_text(&value))),
            ),
            _ => Err(ExecError::TypeMismatch {
                op: "::macaddr8",
                left: Value::MacAddr8(value),
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
            _ => cast_jsonb_scalar_value(&decode_jsonb(&bytes)?, ty, config),
        },
        Value::Int64(v) => match ty {
            ty if ty.is_range() => cast_text_value(&v.to_string(), ty, true),
            ty if ty.is_multirange() => cast_text_value(&v.to_string(), ty, true),
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
                type_oid: XID8_TYPE_OID,
                ..
            } if v >= 0 => Ok(Value::Xid8(v as u64)),
            SqlType {
                kind: SqlTypeKind::Int8,
                ..
            } => Ok(Value::Int64(v)),
            SqlType {
                kind:
                    SqlTypeKind::Oid
                    | SqlTypeKind::RegProc
                    | SqlTypeKind::RegClass
                    | SqlTypeKind::RegType
                    | SqlTypeKind::RegRole
                    | SqlTypeKind::RegNamespace
                    | SqlTypeKind::RegOper
                    | SqlTypeKind::RegOperator
                    | SqlTypeKind::RegProcedure
                    | SqlTypeKind::RegCollation
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
                kind: SqlTypeKind::Bit | SqlTypeKind::VarBit,
                ..
            } => Ok(Value::Bit(cast_integer_to_bit_string(
                v as u64,
                64,
                v < 0,
                ty,
            ))),
            SqlType {
                kind:
                    SqlTypeKind::Text
                    | SqlTypeKind::Cstring
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
                    | SqlTypeKind::Internal
                    | SqlTypeKind::InternalChar
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
                    | SqlTypeKind::RegDictionary
                    | SqlTypeKind::Inet
                    | SqlTypeKind::Cidr
                    | SqlTypeKind::Uuid
                    | SqlTypeKind::PgLsn
                    | SqlTypeKind::Enum
                    | SqlTypeKind::MacAddr
                    | SqlTypeKind::MacAddr8,
                ..
            } => cast_text_value(&v.to_string(), ty, true),
            SqlType {
                kind: SqlTypeKind::Bool,
                ..
            } => Ok(cast_integer_to_bool(v)),
            SqlType {
                kind: SqlTypeKind::Bytea,
                ..
            } => Ok(Value::Bytea(v.to_be_bytes().to_vec())),
            SqlType {
                kind:
                    SqlTypeKind::AnyElement
                    | SqlTypeKind::AnyRange
                    | SqlTypeKind::AnyMultirange
                    | SqlTypeKind::AnyCompatible
                    | SqlTypeKind::AnyCompatibleArray
                    | SqlTypeKind::AnyCompatibleRange
                    | SqlTypeKind::AnyCompatibleMultirange
                    | SqlTypeKind::AnyEnum,
                ..
            } => Ok(Value::Text(CompactString::from_owned(v.to_string()))),
            SqlType {
                kind: SqlTypeKind::AnyArray,
                ..
            } => Err(unsupported_anyarray_input()),
            SqlType {
                kind: SqlTypeKind::Record | SqlTypeKind::Composite | SqlTypeKind::Shell,
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
            SqlType {
                kind: SqlTypeKind::Multirange,
                ..
            } => unreachable!("multirange handled above"),
        },
        Value::Float64(v) => match ty {
            ty if ty.is_range() => cast_text_value(&v.to_string(), ty, true),
            ty if ty.is_multirange() => cast_text_value(&v.to_string(), ty, true),
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
                parse_numeric_text(&float_to_numeric_text(v, source_type))
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
                    | SqlTypeKind::Cstring
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
                    | SqlTypeKind::Internal
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
                    | SqlTypeKind::RegDictionary
                    | SqlTypeKind::Inet
                    | SqlTypeKind::Cidr
                    | SqlTypeKind::Uuid
                    | SqlTypeKind::PgLsn
                    | SqlTypeKind::Enum
                    | SqlTypeKind::MacAddr
                    | SqlTypeKind::MacAddr8,
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
                    | SqlTypeKind::RegProc
                    | SqlTypeKind::RegClass
                    | SqlTypeKind::RegType
                    | SqlTypeKind::RegRole
                    | SqlTypeKind::RegNamespace
                    | SqlTypeKind::RegOper
                    | SqlTypeKind::RegOperator
                    | SqlTypeKind::RegProcedure
                    | SqlTypeKind::RegCollation
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
                kind:
                    SqlTypeKind::AnyElement
                    | SqlTypeKind::AnyRange
                    | SqlTypeKind::AnyMultirange
                    | SqlTypeKind::AnyCompatible
                    | SqlTypeKind::AnyCompatibleArray
                    | SqlTypeKind::AnyCompatibleRange
                    | SqlTypeKind::AnyCompatibleMultirange
                    | SqlTypeKind::AnyEnum,
                ..
            } => Ok(Value::Text(CompactString::from_owned(v.to_string()))),
            SqlType {
                kind: SqlTypeKind::AnyArray,
                ..
            } => Err(unsupported_anyarray_input()),
            SqlType {
                kind: SqlTypeKind::Record | SqlTypeKind::Composite | SqlTypeKind::Shell,
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
            SqlType {
                kind: SqlTypeKind::Multirange,
                ..
            } => unreachable!("multirange handled above"),
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
        Value::Array(items) => match ty.kind {
            SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar => {
                Ok(Value::Text(CompactString::from_owned(
                    crate::backend::executor::value_io::format_array_text(&items),
                )))
            }
            _ => Ok(Value::Array(items)),
        },
        Value::PgArray(array) => match ty.kind {
            SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar => {
                Ok(Value::Text(CompactString::from_owned(
                    crate::backend::executor::value_io::format_array_value_text(&array),
                )))
            }
            _ => Ok(Value::PgArray(array)),
        },
        Value::Record(record) => match ty.kind {
            SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar => {
                Ok(Value::Text(CompactString::from_owned(
                    crate::backend::executor::value_io::format_record_text(&record),
                )))
            }
            _ => Ok(Value::Record(record)),
        },
    }?;
    Ok(apply_time_precision(result, ty.time_precision()))
}

fn bytea_to_signed_int(bytes: &[u8], width: usize, ty: &'static str) -> Result<i64, ExecError> {
    if bytes.len() > width {
        return Err(match ty {
            "smallint" => ExecError::Int2OutOfRange,
            "integer" => ExecError::Int4OutOfRange,
            "bigint" => ExecError::Int8OutOfRange,
            _ => unreachable!("validated integer byte width"),
        });
    }

    let sign = bytes
        .first()
        .is_some_and(|byte| byte & 0x80 != 0)
        .then_some(0xff)
        .unwrap_or(0x00);
    let mut buf = [sign; 8];
    let offset = 8 - bytes.len();
    buf[offset..].copy_from_slice(bytes);
    let value = i64::from_be_bytes(buf);
    match ty {
        "smallint" => Ok(i16::try_from(value).map_err(|_| ExecError::Int2OutOfRange)? as i64),
        "integer" => Ok(i32::try_from(value).map_err(|_| ExecError::Int4OutOfRange)? as i64),
        "bigint" => Ok(value),
        _ => unreachable!("validated integer byte width"),
    }
}

fn parse_int2vector_array_text(text: &str) -> Result<Value, ExecError> {
    let mut items = Vec::new();
    for item in text.split_ascii_whitespace() {
        items.push(cast_text_to_int2(item)?);
    }
    Ok(Value::PgArray(
        ArrayValue::from_dimensions(vector_array_dimensions(items.len()), items)
            .with_element_type_oid(INT2_TYPE_OID),
    ))
}

fn parse_oidvector_array_text(text: &str, element_type_oid: u32) -> Result<Value, ExecError> {
    let mut items = Vec::new();
    for item in text.split_ascii_whitespace() {
        items.push(cast_text_to_oid(item)?);
    }
    Ok(Value::PgArray(
        ArrayValue::from_dimensions(vector_array_dimensions(items.len()), items)
            .with_element_type_oid(element_type_oid),
    ))
}

fn is_oid_vector_array_element(kind: SqlTypeKind) -> bool {
    matches!(
        kind,
        SqlTypeKind::Oid
            | SqlTypeKind::RegProc
            | SqlTypeKind::RegClass
            | SqlTypeKind::RegType
            | SqlTypeKind::RegRole
            | SqlTypeKind::RegNamespace
            | SqlTypeKind::RegOper
            | SqlTypeKind::RegOperator
            | SqlTypeKind::RegProcedure
            | SqlTypeKind::RegCollation
            | SqlTypeKind::RegConfig
            | SqlTypeKind::RegDictionary
    )
}

fn vector_array_dimensions(length: usize) -> Vec<ArrayDimension> {
    if length == 0 {
        Vec::new()
    } else {
        vec![ArrayDimension {
            lower_bound: 0,
            length,
        }]
    }
}

fn array_element_type_oid(element_type: SqlType) -> Option<u32> {
    if let Some(multirange_type) = multirange_type_ref_for_sql_type(element_type) {
        return Some(multirange_type.type_oid());
    }
    if let Some(range_type) = range_type_ref_for_sql_type(element_type) {
        return Some(range_type.type_oid());
    }
    if element_type.type_oid != 0 {
        return Some(element_type.type_oid);
    }
    builtin_type_rows().into_iter().find_map(|row| {
        (!row.sql_type.is_array
            && row.sql_type.kind == element_type.kind
            && !matches!(row.sql_type.kind, SqlTypeKind::AnyArray))
        .then_some(row.oid)
    })
}

pub(super) fn cast_text_value(text: &str, ty: SqlType, explicit: bool) -> Result<Value, ExecError> {
    cast_text_value_with_config(text, ty, explicit, &DateTimeConfig::default())
}

fn cast_text_to_enum(
    text: &str,
    ty: SqlType,
    catalog: Option<&dyn CatalogLookup>,
) -> Result<Value, ExecError> {
    let enum_type_oid = enum_catalog_type_oid(ty);
    if let Some(label_oid) = catalog.and_then(|catalog| catalog.enum_label_oid(enum_type_oid, text))
    {
        if let Some(catalog) = catalog {
            ensure_enum_label_safe(catalog, enum_type_oid, label_oid)?;
            enforce_enum_domain_constraints(Value::EnumOid(label_oid), ty, catalog)?;
        }
        return Ok(Value::EnumOid(label_oid));
    }
    let type_name = catalog
        .and_then(|catalog| catalog.type_by_oid(enum_type_oid))
        .map(|row| row.typname)
        .unwrap_or_else(|| enum_type_oid.to_string());
    Err(ExecError::DetailedError {
        message: format!("invalid input value for enum {type_name}: \"{text}\""),
        detail: None,
        hint: None,
        sqlstate: "22P02",
    })
}

fn enum_catalog_type_oid(ty: SqlType) -> u32 {
    if matches!(ty.kind, SqlTypeKind::Enum) && ty.typrelid != 0 {
        ty.typrelid
    } else {
        ty.type_oid
    }
}

fn ensure_enum_label_safe(
    catalog: &dyn CatalogLookup,
    enum_type_oid: u32,
    label_oid: u32,
) -> Result<(), ExecError> {
    if catalog.enum_label_is_committed(enum_type_oid, label_oid) {
        return Ok(());
    }
    let label = catalog
        .enum_label(enum_type_oid, label_oid)
        .or_else(|| catalog.enum_label_by_oid(label_oid))
        .unwrap_or_else(|| label_oid.to_string());
    let type_name = catalog
        .type_by_oid(enum_type_oid)
        .map(|row| row.typname)
        .unwrap_or_else(|| enum_type_oid.to_string());
    Err(ExecError::DetailedError {
        message: format!("unsafe use of new value \"{label}\" of enum type {type_name}"),
        detail: None,
        hint: Some("New enum values must be committed before they can be used.".into()),
        sqlstate: "55P04",
    })
}

fn enforce_enum_domain_constraints(
    value: Value,
    ty: SqlType,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    if !matches!(ty.kind, SqlTypeKind::Enum) || ty.typrelid == 0 {
        return Ok(());
    }
    let Value::EnumOid(label_oid) = value else {
        return Ok(());
    };
    let Some(allowed) = catalog.domain_allowed_enum_label_oids(ty.type_oid) else {
        return Ok(());
    };
    if allowed.contains(&label_oid) {
        return Ok(());
    }
    let domain_name = catalog
        .type_by_oid(ty.type_oid)
        .map(|row| row.typname)
        .unwrap_or_else(|| ty.type_oid.to_string());
    let check_name = catalog
        .domain_check_name(ty.type_oid)
        .unwrap_or_else(|| format!("{domain_name}_check"));
    Err(ExecError::DetailedError {
        message: format!(
            "value for domain {domain_name} violates check constraint \"{check_name}\""
        ),
        detail: None,
        hint: None,
        sqlstate: "23514",
    })
}

pub(crate) fn cast_text_value_with_config(
    text: &str,
    ty: SqlType,
    explicit: bool,
    config: &DateTimeConfig,
) -> Result<Value, ExecError> {
    if !ty.is_array && is_txid_snapshot_type_oid(ty.type_oid) {
        return cast_text_to_txid_snapshot(text);
    }
    if !ty.is_array && ty.type_oid == XID8_TYPE_OID {
        return cast_text_to_xid8(text);
    }
    if ty.is_range() {
        return parse_range_text(text, ty);
    }
    if ty.is_multirange() {
        return parse_multirange_text(text, ty);
    }
    match ty.kind {
        SqlTypeKind::AnyArray | SqlTypeKind::AnyCompatibleArray => {
            Err(unsupported_anyarray_input())
        }
        SqlTypeKind::AnyElement
        | SqlTypeKind::AnyRange
        | SqlTypeKind::AnyMultirange
        | SqlTypeKind::AnyCompatible
        | SqlTypeKind::AnyCompatibleRange
        | SqlTypeKind::AnyCompatibleMultirange
        | SqlTypeKind::AnyEnum => Ok(Value::Text(CompactString::new(text))),
        SqlTypeKind::Record | SqlTypeKind::Composite => Err(unsupported_record_input()),
        SqlTypeKind::Shell => Err(ExecError::TypeMismatch {
            op: "::shell",
            left: Value::Text(CompactString::new(text)),
            right: Value::Null,
        }),
        SqlTypeKind::Trigger => Err(unsupported_trigger_input()),
        SqlTypeKind::Internal => Err(ExecError::TypeMismatch {
            op: "::internal",
            left: Value::Text(CompactString::new(text)),
            right: Value::Null,
        }),
        SqlTypeKind::FdwHandler => Err(ExecError::TypeMismatch {
            op: "::fdw_handler",
            left: Value::Text(CompactString::new(text)),
            right: Value::Null,
        }),
        SqlTypeKind::Text
        | SqlTypeKind::Cstring
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
        SqlTypeKind::Time => parse_time_text(text, config)
            .map(Value::Time)
            .map(|value| apply_time_precision(value, ty.time_precision()))
            .map_err(|err| ExecError::InvalidStorageValue {
                column: "time".into(),
                details: datetime_parse_error_details("time", text, err),
            }),
        SqlTypeKind::TimeTz => parse_timetz_text(text, config)
            .map(Value::TimeTz)
            .map(|value| apply_time_precision(value, ty.time_precision()))
            .map_err(|err| ExecError::InvalidStorageValue {
                column: "timetz".into(),
                details: datetime_parse_error_details("time with time zone", text, err),
            }),
        SqlTypeKind::Interval => Ok(Value::Interval(apply_interval_typmod(
            parse_interval_text_value_with_style(text, config.interval_style)?,
            ty,
        )?)),
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
        SqlTypeKind::Uuid => Ok(Value::Uuid(parse_uuid_text(text)?)),
        SqlTypeKind::Inet => parse_inet_text(text).map(Value::Inet),
        SqlTypeKind::Cidr => parse_cidr_text(text).map(Value::Cidr),
        SqlTypeKind::MacAddr => parse_macaddr_text(text).map(Value::MacAddr),
        SqlTypeKind::MacAddr8 => parse_macaddr8_text(text).map(Value::MacAddr8),
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
        SqlTypeKind::PgLsn => parse_pg_lsn_text(text).map(Value::PgLsn),
        SqlTypeKind::Void => Err(ExecError::TypeMismatch {
            op: "::void",
            left: Value::Text(CompactString::new(text)),
            right: Value::Null,
        }),
        SqlTypeKind::RegProc
        | SqlTypeKind::RegClass
        | SqlTypeKind::RegRole
        | SqlTypeKind::RegNamespace
        | SqlTypeKind::RegOper
        | SqlTypeKind::RegOperator
        | SqlTypeKind::RegType
        | SqlTypeKind::RegProcedure
        | SqlTypeKind::RegCollation
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
        SqlTypeKind::Enum => cast_text_to_enum(text, ty, None),
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
        SqlTypeKind::Multirange => unreachable!("multirange handled above"),
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
    if ty.is_multirange() {
        return cast_text_value(&value.render(), ty, explicit);
    }
    match ty.kind {
        SqlTypeKind::AnyArray | SqlTypeKind::AnyCompatibleArray => {
            Err(unsupported_anyarray_input())
        }
        SqlTypeKind::AnyElement
        | SqlTypeKind::AnyRange
        | SqlTypeKind::AnyMultirange
        | SqlTypeKind::AnyCompatible
        | SqlTypeKind::AnyCompatibleRange
        | SqlTypeKind::AnyCompatibleMultirange
        | SqlTypeKind::AnyEnum => Ok(Value::Text(CompactString::from_owned(value.render()))),
        SqlTypeKind::Record | SqlTypeKind::Composite => Err(unsupported_record_input()),
        SqlTypeKind::Shell => Err(ExecError::TypeMismatch {
            op: "::shell",
            left: Value::Numeric(value.clone()),
            right: Value::Null,
        }),
        SqlTypeKind::Trigger => Err(unsupported_trigger_input()),
        SqlTypeKind::Internal => Err(ExecError::TypeMismatch {
            op: "::internal",
            left: Value::Numeric(value),
            right: Value::Null,
        }),
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
        | SqlTypeKind::Enum
        | SqlTypeKind::Cstring
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
        | SqlTypeKind::PgLsn
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
        SqlTypeKind::Oid
        | SqlTypeKind::RegProc
        | SqlTypeKind::RegClass
        | SqlTypeKind::RegType
        | SqlTypeKind::RegRole
        | SqlTypeKind::RegNamespace
        | SqlTypeKind::RegOper
        | SqlTypeKind::RegOperator
        | SqlTypeKind::RegProcedure
        | SqlTypeKind::RegCollation
        | SqlTypeKind::Xid => value
            .round_to_scale(0)
            .and_then(|rounded| rounded.render().parse::<u32>().ok())
            .and_then(|rounded| Some(Value::Int64(rounded as i64)))
            .ok_or(ExecError::OidOutOfRange),
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
        SqlTypeKind::Uuid => Err(ExecError::TypeMismatch {
            op: "::uuid",
            left: Value::Numeric(value),
            right: Value::Uuid([0; 16]),
        }),
        SqlTypeKind::Inet | SqlTypeKind::Cidr | SqlTypeKind::MacAddr | SqlTypeKind::MacAddr8 => {
            cast_text_value(&value.render(), ty, explicit)
        }
        SqlTypeKind::Range
        | SqlTypeKind::Int4Range
        | SqlTypeKind::Int8Range
        | SqlTypeKind::NumericRange
        | SqlTypeKind::DateRange
        | SqlTypeKind::TimestampRange
        | SqlTypeKind::TimestampTzRange => unreachable!("range handled above"),
        SqlTypeKind::Multirange => unreachable!("multirange handled above"),
    }
}

pub(crate) fn parse_uuid_text(text: &str) -> Result<[u8; 16], ExecError> {
    let mut src = text;
    let has_braces = src.starts_with('{');
    if has_braces {
        src = &src[1..];
    }

    let mut bytes = [0u8; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        if src.len() < 2 {
            return Err(ExecError::InvalidUuidInput { value: text.into() });
        }
        let pair = &src[..2];
        if !pair.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(ExecError::InvalidUuidInput { value: text.into() });
        }
        *byte = u8::from_str_radix(pair, 16)
            .map_err(|_| ExecError::InvalidUuidInput { value: text.into() })?;
        src = &src[2..];
        if src.starts_with('-') && index % 2 == 1 && index < 15 {
            src = &src[1..];
        }
    }

    if has_braces {
        let Some(rest) = src.strip_prefix('}') else {
            return Err(ExecError::InvalidUuidInput { value: text.into() });
        };
        src = rest;
    }
    if !src.is_empty() {
        return Err(ExecError::InvalidUuidInput { value: text.into() });
    }
    Ok(bytes)
}

fn coerce_character_string(text: &str, ty: SqlType, explicit: bool) -> Result<String, ExecError> {
    let max_chars = match ty.kind {
        SqlTypeKind::Name => return Ok(truncate_name_string(text)),
        SqlTypeKind::Char => match ty.char_len() {
            Some(max_chars) => max_chars,
            None => return Ok(text.to_string()),
        },
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

fn truncate_name_string(text: &str) -> String {
    const NAME_DATA_MAX_BYTES: usize = 63;

    if text.len() <= NAME_DATA_MAX_BYTES {
        return text.to_string();
    }

    let mut end = NAME_DATA_MAX_BYTES;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_string()
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
        return match ty.kind {
            SqlTypeKind::Int2 => Err(ExecError::Int2OutOfRange),
            SqlTypeKind::Int4 => Err(ExecError::Int4OutOfRange),
            SqlTypeKind::Int8 => Err(ExecError::Int8OutOfRange),
            SqlTypeKind::Oid
            | SqlTypeKind::RegClass
            | SqlTypeKind::RegType
            | SqlTypeKind::RegRole
            | SqlTypeKind::RegNamespace
            | SqlTypeKind::RegOperator
            | SqlTypeKind::RegProcedure => Err(ExecError::OidOutOfRange),
            _ => unreachable!(),
        };
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
        SqlTypeKind::Oid
        | SqlTypeKind::RegClass
        | SqlTypeKind::RegType
        | SqlTypeKind::RegRole
        | SqlTypeKind::RegNamespace
        | SqlTypeKind::RegOperator
        | SqlTypeKind::RegProcedure => {
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

fn float_to_numeric_text(value: f64, source_type: Option<SqlType>) -> String {
    let options = FloatFormatOptions {
        extra_float_digits: 0,
        ..FloatFormatOptions::default()
    };
    match source_type.map(|ty| ty.kind) {
        Some(SqlTypeKind::Float4) => format_float4_text(value, options),
        Some(SqlTypeKind::Float8) => format_float8_text(value, options),
        _ => value.to_string(),
    }
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
        cast_float_to_int, cast_value, cast_value_with_source_type_and_config,
        parse_input_type_name, parse_interval_text_value, parse_pg_float, parse_text_array_literal,
        render_interval_text_with_style, soft_input_error_info,
    };
    use crate::backend::executor::exec_expr::parse_numeric_text;
    use crate::backend::executor::{ExecError, Value};
    use crate::backend::parser::{SqlType, SqlTypeKind};
    use crate::backend::utils::misc::guc_datetime::{DateTimeConfig, IntervalStyle};
    use crate::include::nodes::datetime::{
        DateADT, TimeADT, TimeTzADT, TimestampADT, TimestampTzADT, USECS_PER_DAY, USECS_PER_SEC,
    };
    use crate::include::nodes::datum::{ArrayValue, IntervalValue};

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
    fn float_to_numeric_cast_uses_pg_precision_by_source_type() {
        let float4_numeric = cast_value_with_source_type_and_config(
            Value::Float64(1.23456789_f64),
            Some(SqlType::new(SqlTypeKind::Float4)),
            SqlType::new(SqlTypeKind::Numeric),
            &DateTimeConfig::default(),
        )
        .unwrap();
        let float8_numeric = cast_value_with_source_type_and_config(
            Value::Float64(1.2345678901234567_f64),
            Some(SqlType::new(SqlTypeKind::Float8)),
            SqlType::new(SqlTypeKind::Numeric),
            &DateTimeConfig::default(),
        )
        .unwrap();

        assert_eq!(
            float4_numeric,
            Value::Numeric(parse_numeric_text("1.23457").unwrap())
        );
        assert_eq!(
            float8_numeric,
            Value::Numeric(parse_numeric_text("1.23456789012346").unwrap())
        );
    }

    #[test]
    fn parse_input_type_name_uses_text_input_cast_surface() {
        assert_eq!(
            parse_input_type_name("jsonb", None).unwrap(),
            Some(SqlType::new(SqlTypeKind::Jsonb))
        );
        assert_eq!(
            parse_input_type_name("jsonpath", None).unwrap(),
            Some(SqlType::new(SqlTypeKind::JsonPath))
        );
        assert_eq!(
            parse_input_type_name("timestamp", None).unwrap(),
            Some(SqlType::new(SqlTypeKind::Timestamp))
        );
        assert_eq!(
            parse_input_type_name("varchar(4)", None).unwrap(),
            Some(SqlType::with_char_len(SqlTypeKind::Varchar, 4))
        );
        assert_eq!(
            parse_input_type_name("int4[]", None).unwrap(),
            Some(SqlType::array_of(SqlType::new(SqlTypeKind::Int4)))
        );
        assert_eq!(
            parse_input_type_name("varchar(4)[]", None).unwrap(),
            Some(SqlType::array_of(SqlType::with_char_len(
                SqlTypeKind::Varchar,
                4
            )))
        );
        assert_eq!(
            parse_input_type_name("macaddr", None).unwrap(),
            Some(SqlType::new(SqlTypeKind::MacAddr))
        );
        assert_eq!(
            parse_input_type_name("macaddr8[]", None).unwrap(),
            Some(SqlType::array_of(SqlType::new(SqlTypeKind::MacAddr8)))
        );
        assert_eq!(
            parse_input_type_name("_macaddr", None).unwrap(),
            Some(SqlType::array_of(SqlType::new(SqlTypeKind::MacAddr)))
        );
        assert_eq!(
            parse_input_type_name("int4[][]", None).unwrap(),
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
    fn cast_value_preserves_interval_array_element_oid() {
        assert_eq!(
            cast_value(
                Value::Text("{0 second,1 hour 42 minutes 20 seconds}".into()),
                SqlType::array_of(SqlType::new(SqlTypeKind::Interval))
            )
            .unwrap(),
            Value::PgArray(
                ArrayValue::from_1d(vec![
                    Value::Interval(IntervalValue {
                        time_micros: 0,
                        days: 0,
                        months: 0,
                    }),
                    Value::Interval(IntervalValue {
                        time_micros: 6_140_000_000,
                        days: 0,
                        months: 0,
                    }),
                ])
                .with_element_type_oid(crate::include::catalog::INTERVAL_TYPE_OID),
            )
        );
    }

    #[test]
    fn timestamp_casts_use_datetime_parts() {
        let timestamp =
            TimestampADT(USECS_PER_DAY + (12 * 3600 + 34 * 60 + 56) * USECS_PER_SEC + 789);
        let timestamptz = TimestampTzADT(timestamp.0);
        let config = DateTimeConfig::default();

        assert_eq!(
            cast_value(Value::Timestamp(timestamp), SqlType::new(SqlTypeKind::Date)).unwrap(),
            Value::Date(DateADT(1))
        );
        assert_eq!(
            cast_value(Value::Timestamp(timestamp), SqlType::new(SqlTypeKind::Time)).unwrap(),
            Value::Time(TimeADT((12 * 3600 + 34 * 60 + 56) * USECS_PER_SEC + 789))
        );
        assert_eq!(
            cast_value(
                Value::Timestamp(timestamp),
                SqlType::new(SqlTypeKind::TimeTz)
            )
            .unwrap(),
            Value::TimeTz(TimeTzADT {
                time: TimeADT((12 * 3600 + 34 * 60 + 56) * USECS_PER_SEC + 789),
                offset_seconds: 0,
            })
        );
        assert_eq!(
            cast_value_with_source_type_and_config(
                Value::TimestampTz(timestamptz),
                None,
                SqlType::new(SqlTypeKind::Timestamp),
                &config,
            )
            .unwrap(),
            Value::Timestamp(timestamp)
        );
    }

    #[test]
    fn bpchar_without_typmod_preserves_text_width() {
        assert_eq!(
            cast_value(
                Value::Text("WS.002.1a".into()),
                SqlType::new(SqlTypeKind::Char)
            )
            .unwrap(),
            Value::Text("WS.002.1a".into())
        );
        assert_eq!(
            cast_value(
                Value::Text("WS.002.1a".into()),
                SqlType::with_char_len(SqlTypeKind::Char, 2)
            )
            .unwrap(),
            Value::Text("WS".into())
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
    fn interval_input_errors_use_datetime_sqlstates() {
        let info = soft_input_error_info("garbage", "interval")
            .unwrap()
            .expect("invalid interval should return structured info");
        assert_eq!(info.sqlstate, "22007");

        let err = parse_interval_text_value("2147483648 days").unwrap_err();
        assert!(matches!(
            err,
            ExecError::DetailedError {
                message,
                sqlstate: "22008",
                ..
            } if message == "interval field value out of range: \"2147483648 days\""
        ));
    }

    #[test]
    fn interval_input_keeps_i64_microsecond_edge_values() {
        assert_eq!(
            parse_interval_text_value("-9223372036854775807 us").unwrap(),
            IntervalValue {
                time_micros: i64::MIN + 1,
                days: 0,
                months: 0,
            }
        );
        assert_eq!(
            parse_interval_text_value("-9223372036854775808 us").unwrap(),
            IntervalValue {
                time_micros: i64::MIN,
                days: 0,
                months: 0,
            }
        );
    }

    #[test]
    fn interval_rendering_respects_intervalstyle() {
        let value = IntervalValue {
            time_micros: 14_584_000_000,
            days: 3,
            months: 14,
        };
        assert_eq!(
            render_interval_text_with_style(value, IntervalStyle::Postgres),
            "1 year 2 mons 3 days 04:03:04"
        );
        assert_eq!(
            render_interval_text_with_style(value, IntervalStyle::PostgresVerbose),
            "@ 1 year 2 mons 3 days 4 hours 3 mins 4 secs"
        );
        assert_eq!(
            render_interval_text_with_style(value, IntervalStyle::SqlStandard),
            "+1-2 +3 +4:03:04"
        );
        assert_eq!(
            render_interval_text_with_style(value, IntervalStyle::Iso8601),
            "P1Y2M3DT4H3M4S"
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

    #[test]
    fn pg_input_error_info_reports_reg_object_cases() {
        let info = soft_input_error_info("-", "regoper")
            .unwrap()
            .expect("ambiguous operator should be soft");
        assert_eq!(info.message, "more than one operator named -");
        assert_eq!(info.sqlstate, "42725");

        let info = soft_input_error_info("-", "regoperator")
            .unwrap()
            .expect("missing operator signature should be soft");
        assert_eq!(info.message, "expected a left parenthesis");
        assert_eq!(info.sqlstate, "22P02");

        let info = soft_input_error_info("no_such_type", "regtype")
            .unwrap()
            .expect("missing type should be soft");
        assert_eq!(info.message, "type \"no_such_type\" does not exist");
        assert_eq!(info.sqlstate, "42704");

        assert!(soft_input_error_info("numeric(1,2,3)", "regtype").is_err());
    }
}
