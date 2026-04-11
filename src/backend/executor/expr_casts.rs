use super::exec_expr::parse_numeric_text;
use super::expr_bool::cast_integer_to_bool;
use super::expr_bool::parse_pg_bool_text;
use super::expr_json::{canonicalize_jsonpath_text, validate_json_text};
use super::node_types::*;
use super::ExecError;
use crate::backend::executor::jsonb::{parse_jsonb_text, render_jsonb_bytes};
use crate::backend::parser::{SqlType, SqlTypeKind, parse_type_name};
use crate::pgrust::compact_string::CompactString;

pub(crate) struct InputErrorInfo {
    pub(crate) message: String,
    pub(crate) detail: Option<String>,
    pub(crate) hint: Option<String>,
    pub(crate) sqlstate: &'static str,
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
    if normalized.is_empty()
        || !normalized
            .chars()
            .all(|ch| ch.is_digit(base))
    {
        return Err(ExecError::InvalidIntegerInput {
            ty,
            value: text.to_string(),
        });
    }

    let magnitude = i128::from_str_radix(&normalized, base).map_err(|_| ExecError::InvalidIntegerInput {
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
    if !(0..=i32::MAX as i128).contains(&value) {
        return Err(ExecError::OidOutOfRange);
    }
    Ok(Value::Int32(value as i32))
}

fn parse_internal_char_text(text: &str) -> u8 {
    let bytes = text.as_bytes();
    if bytes.is_empty() {
        return 0;
    }
    if bytes.len() == 4
        && bytes[0] == b'\\'
        && bytes[1..].iter().all(|b| (b'0'..=b'7').contains(b))
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

fn parse_input_type_name(type_name: &str) -> Result<Option<SqlType>, ExecError> {
    let parsed = match parse_type_name(type_name.trim()) {
        Ok(ty) => ty,
        Err(_) => return Ok(None),
    };
    let supported = matches!(
        parsed.kind,
        SqlTypeKind::Int2
            | SqlTypeKind::Int4
            | SqlTypeKind::Int8
            | SqlTypeKind::Oid
            | SqlTypeKind::Float4
            | SqlTypeKind::Float8
            | SqlTypeKind::Text
            | SqlTypeKind::Bool
            | SqlTypeKind::Numeric
            | SqlTypeKind::InternalChar
            | SqlTypeKind::Char
            | SqlTypeKind::Varchar
    ) && !parsed.is_array;
    Ok(supported.then_some(parsed))
}

fn input_error_message(err: &ExecError, text: &str) -> String {
    match err {
        ExecError::InvalidIntegerInput { ty, .. } => {
            format!("invalid input syntax for type {ty}: \"{text}\"")
        }
        ExecError::IntegerOutOfRange { ty, .. } => {
            format!("value \"{text}\" is out of range for type {ty}")
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
        ExecError::OidOutOfRange => "OID out of range".to_string(),
        ExecError::InvalidNumericInput(_) => {
            format!("invalid input syntax for type numeric: \"{text}\"")
        }
        ExecError::InvalidBooleanInput { .. } => {
            format!("invalid input syntax for type boolean: \"{text}\"")
        }
        ExecError::InvalidFloatInput { ty, .. } => {
            format!("invalid input syntax for type {ty}: \"{text}\"")
        }
        ExecError::FloatOutOfRange { ty, .. } => {
            format!("\"{text}\" is out of range for type {ty}")
        }
        ExecError::FloatOverflow => "value out of range: overflow".to_string(),
        ExecError::FloatUnderflow => "value out of range: underflow".to_string(),
        ExecError::StringDataRightTruncation { ty } => {
            format!("value too long for type {ty}")
        }
        other => format!("{other:?}"),
    }
}

fn input_error_sqlstate(err: &ExecError) -> &'static str {
    match err {
        ExecError::InvalidIntegerInput { .. }
        | ExecError::InvalidNumericInput(_)
        | ExecError::InvalidBooleanInput { .. } => "22P02",
        ExecError::IntegerOutOfRange { .. }
        | ExecError::Int2OutOfRange
        | ExecError::Int4OutOfRange
        | ExecError::Int8OutOfRange
        | ExecError::OidOutOfRange
        | ExecError::FloatOutOfRange { .. }
        | ExecError::FloatOverflow
        | ExecError::FloatUnderflow => {
            "22003"
        }
        ExecError::StringDataRightTruncation { .. } => "22001",
        ExecError::InvalidFloatInput { .. } => "22P02",
        _ => "XX000",
    }
}

pub(crate) fn soft_input_error_info(
    text: &str,
    type_name: &str,
) -> Result<Option<InputErrorInfo>, ExecError> {
    if type_name.trim().eq_ignore_ascii_case("int2vector") {
        for item in text.split_ascii_whitespace() {
            match cast_text_to_int2(item) {
                Ok(_) => {}
                Err(err) => {
                    return Ok(Some(InputErrorInfo {
                        message: input_error_message(&err, item),
                        detail: None,
                        hint: None,
                        sqlstate: input_error_sqlstate(&err),
                    }));
                }
            }
        }
        return Ok(None);
    }

    let ty = parse_input_type_name(type_name)?.ok_or_else(|| ExecError::InvalidStorageValue {
        column: type_name.to_string(),
        details: format!("unsupported type: {type_name}"),
    })?;
    match cast_text_value(text, ty, false) {
        Ok(_) => Ok(None),
        Err(err) => Ok(Some(InputErrorInfo {
            message: input_error_message(&err, text),
            detail: None,
            hint: None,
            sqlstate: input_error_sqlstate(&err),
        })),
    }
}

pub(crate) fn cast_value(value: Value, ty: SqlType) -> Result<Value, ExecError> {
    if ty.is_array {
        return match value {
            Value::Null => Ok(Value::Null),
            Value::Array(items) => {
                let element_type = ty.element_type();
                let mut casted = Vec::with_capacity(items.len());
                for item in items {
                    casted.push(cast_value(item, element_type)?);
                }
                Ok(Value::Array(casted))
            }
            other => Err(ExecError::TypeMismatch {
                op: "::array",
                left: other,
                right: Value::Null,
            }),
        };
    }

    match value {
        Value::Null => Ok(Value::Null),
        Value::Int16(v) => match ty {
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
                kind: SqlTypeKind::Oid,
                ..
            } => {
                if v < 0 {
                    Err(ExecError::OidOutOfRange)
                } else {
                    Ok(Value::Int32(v as i32))
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
                kind:
                    SqlTypeKind::Text
                    | SqlTypeKind::Timestamp
                    | SqlTypeKind::InternalChar
                    | SqlTypeKind::Char
                    | SqlTypeKind::Varchar
                    | SqlTypeKind::Json
                    | SqlTypeKind::Jsonb
                    | SqlTypeKind::JsonPath,
                ..
            } => cast_text_value(&v.to_string(), ty, true),
            SqlType {
                kind: SqlTypeKind::Bool,
                ..
            } => Ok(cast_integer_to_bool(v as i64)),
        },
        Value::Int32(v) => match ty {
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
                kind: SqlTypeKind::Oid,
                ..
            } => {
                if v < 0 {
                    Err(ExecError::OidOutOfRange)
                } else {
                    Ok(Value::Int32(v))
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
                kind:
                    SqlTypeKind::Text
                    | SqlTypeKind::Timestamp
                    | SqlTypeKind::InternalChar
                    | SqlTypeKind::Char
                    | SqlTypeKind::Varchar
                    | SqlTypeKind::Json
                    | SqlTypeKind::Jsonb
                    | SqlTypeKind::JsonPath,
                ..
            } => cast_text_value(&v.to_string(), ty, true),
            SqlType {
                kind: SqlTypeKind::Bool,
                ..
            } => Ok(cast_integer_to_bool(v as i64)),
        },
        Value::Bool(v) => match ty {
            SqlType {
                kind: SqlTypeKind::Bool,
                ..
            } => Ok(Value::Bool(v)),
            SqlType {
                kind:
                    SqlTypeKind::Text
                    | SqlTypeKind::Timestamp
                    | SqlTypeKind::InternalChar
                    | SqlTypeKind::Char
                    | SqlTypeKind::Varchar
                    | SqlTypeKind::Json
                    | SqlTypeKind::Jsonb
                    | SqlTypeKind::JsonPath,
                ..
            } => cast_text_value(if v { "true" } else { "false" }, ty, true),
            SqlType {
                kind:
                    SqlTypeKind::Int2
                    | SqlTypeKind::Int4
                    | SqlTypeKind::Int8
                    | SqlTypeKind::Oid
                    | SqlTypeKind::Float4
                    | SqlTypeKind::Float8
                    | SqlTypeKind::Numeric,
                ..
            } => Err(ExecError::TypeMismatch {
                op: "::int4",
                left: Value::Bool(v),
                right: Value::Int32(0),
            }),
        },
        Value::Text(text) => cast_text_value(text.as_str(), ty, true),
        Value::TextRef(ptr, len) => {
            let text = unsafe {
                std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len as usize))
            };
            cast_text_value(text, ty, true)
        }
        Value::InternalChar(byte) => match ty.kind {
            SqlTypeKind::InternalChar => Ok(Value::InternalChar(byte)),
            SqlTypeKind::Text | SqlTypeKind::Timestamp => Ok(Value::Text(
                CompactString::from_owned(render_internal_char_text(byte)),
            )),
            SqlTypeKind::Json => {
                let rendered = render_internal_char_text(byte);
                validate_json_text(&rendered)?;
                Ok(Value::Json(CompactString::from_owned(rendered)))
            }
            SqlTypeKind::Jsonb => {
                let rendered = render_internal_char_text(byte);
                Ok(Value::Jsonb(parse_jsonb_text(&rendered)?))
            }
            SqlTypeKind::JsonPath => {
                let rendered = render_internal_char_text(byte);
                Ok(Value::JsonPath(canonicalize_jsonpath_text(&rendered)?))
            }
            SqlTypeKind::Char | SqlTypeKind::Varchar => {
                cast_text_value(&render_internal_char_text(byte), ty, true)
            }
            _ => Err(ExecError::TypeMismatch {
                op: "::char",
                left: Value::InternalChar(byte),
                right: Value::Null,
            }),
        },
        Value::JsonPath(text) => cast_text_value(text.as_str(), ty, true),
        Value::Json(text) => cast_text_value(text.as_str(), ty, true),
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
            | SqlTypeKind::Timestamp
            | SqlTypeKind::Char
            | SqlTypeKind::Varchar => cast_text_value(&render_jsonb_bytes(&bytes)?, ty, true),
            _ => Err(ExecError::TypeMismatch {
                op: "::jsonb",
                left: Value::Jsonb(bytes),
                right: Value::Null,
            }),
        },
        Value::Int64(v) => match ty {
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
                kind: SqlTypeKind::Oid,
                ..
            } => {
                if !(0..=i32::MAX as i64).contains(&v) {
                    Err(ExecError::OidOutOfRange)
                } else {
                    Ok(Value::Int32(v as i32))
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
                kind:
                    SqlTypeKind::Text
                    | SqlTypeKind::Timestamp
                    | SqlTypeKind::InternalChar
                    | SqlTypeKind::Char
                    | SqlTypeKind::Varchar
                    | SqlTypeKind::Json
                    | SqlTypeKind::Jsonb
                    | SqlTypeKind::JsonPath,
                ..
            } => cast_text_value(&v.to_string(), ty, true),
            SqlType {
                kind: SqlTypeKind::Bool,
                ..
            } => Ok(cast_integer_to_bool(v)),
        },
        Value::Float64(v) => match ty {
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
            } => Ok(Value::Numeric(
                parse_numeric_text(&v.to_string())
                    .ok_or_else(|| ExecError::InvalidNumericInput(v.to_string()))?,
            )),
            SqlType {
                kind:
                    SqlTypeKind::Text
                    | SqlTypeKind::Timestamp
                    | SqlTypeKind::InternalChar
                    | SqlTypeKind::Char
                    | SqlTypeKind::Varchar
                    | SqlTypeKind::Json
                    | SqlTypeKind::Jsonb
                    | SqlTypeKind::JsonPath,
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
                kind: SqlTypeKind::Oid,
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
        },
        Value::Numeric(numeric) => cast_numeric_value(numeric, ty, true),
        Value::Array(items) => Ok(Value::Array(items)),
    }
}

pub(super) fn cast_text_value(text: &str, ty: SqlType, explicit: bool) -> Result<Value, ExecError> {
    match ty.kind {
        SqlTypeKind::Text | SqlTypeKind::Timestamp => Ok(Value::Text(CompactString::new(text))),
        SqlTypeKind::InternalChar => Ok(Value::InternalChar(parse_internal_char_text(text))),
        SqlTypeKind::Json => {
            validate_json_text(text)?;
            Ok(Value::Json(CompactString::new(text)))
        }
        SqlTypeKind::Jsonb => Ok(Value::Jsonb(parse_jsonb_text(text)?)),
        SqlTypeKind::JsonPath => Ok(Value::JsonPath(canonicalize_jsonpath_text(text)?)),
        SqlTypeKind::Char | SqlTypeKind::Varchar => Ok(Value::Text(CompactString::from_owned(
            coerce_character_string(text, ty, explicit)?,
        ))),
        SqlTypeKind::Int2 => cast_text_to_int2(text),
        SqlTypeKind::Int4 => cast_text_to_int4(text),
        SqlTypeKind::Int8 => cast_text_to_int8(text),
        SqlTypeKind::Oid => cast_text_to_oid(text),
        SqlTypeKind::Float4 | SqlTypeKind::Float8 => parse_pg_float(text, ty.kind)
            .map(|v| {
                Value::Float64(if matches!(ty.kind, SqlTypeKind::Float4) {
                    (v as f32) as f64
                } else {
                    v
                })
            }),
        SqlTypeKind::Numeric => Ok(Value::Numeric(coerce_numeric_value(
            parse_numeric_text(text)
                .ok_or_else(|| ExecError::InvalidNumericInput(text.to_string()))?,
            ty,
        )?)),
        SqlTypeKind::Bool => parse_pg_bool_text(text).map(Value::Bool),
    }
}

pub(super) fn cast_numeric_value(
    value: NumericValue,
    ty: SqlType,
    explicit: bool,
) -> Result<Value, ExecError> {
    match ty.kind {
        SqlTypeKind::Numeric => Ok(Value::Numeric(coerce_numeric_value(value, ty)?)),
        SqlTypeKind::Text | SqlTypeKind::Timestamp => {
            Ok(Value::Text(CompactString::from_owned(value.render())))
        }
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
        SqlTypeKind::Char | SqlTypeKind::Varchar => cast_text_value(&value.render(), ty, explicit),
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
        SqlTypeKind::Int2 => value
            .round_to_scale(0)
            .and_then(|rounded| rounded.render().parse::<i16>().ok())
            .map(Value::Int16)
            .ok_or(ExecError::Int2OutOfRange),
        SqlTypeKind::Int4 => value
            .round_to_scale(0)
            .and_then(|rounded| rounded.render().parse::<i32>().ok())
            .map(Value::Int32)
            .ok_or(ExecError::Int4OutOfRange),
        SqlTypeKind::Int8 => value
            .round_to_scale(0)
            .and_then(|rounded| rounded.render().parse::<i64>().ok())
            .map(Value::Int64)
            .ok_or(ExecError::Int8OutOfRange),
        SqlTypeKind::Oid => value
            .round_to_scale(0)
            .and_then(|rounded| rounded.render().parse::<i64>().ok())
            .and_then(|rounded| {
                if (0..=i32::MAX as i64).contains(&rounded) {
                    Some(Value::Int32(rounded as i32))
                } else {
                    None
                }
            })
            .ok_or(ExecError::OidOutOfRange),
        SqlTypeKind::Bool => Err(ExecError::TypeMismatch {
            op: "::bool",
            left: Value::Numeric(value),
            right: Value::Bool(false),
        }),
    }
}

fn coerce_character_string(text: &str, ty: SqlType, explicit: bool) -> Result<String, ExecError> {
    let max_chars = match ty.kind {
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
            if rounded < 0.0 || rounded > i32::MAX as f64 {
                Err(ExecError::OidOutOfRange)
            } else {
                Ok(Value::Int32(rounded as i32))
            }
        }
        _ => unreachable!(),
    }
}

fn coerce_numeric_value(parsed: NumericValue, ty: SqlType) -> Result<NumericValue, ExecError> {
    let Some((precision, scale)) = ty.numeric_precision_scale() else {
        return Ok(parsed);
    };

    let rounded = parsed
        .round_to_scale(scale as u32)
        .ok_or(ExecError::NumericFieldOverflow)?;

    if rounded.digit_count() > precision {
        return Err(ExecError::NumericFieldOverflow);
    }

    Ok(rounded)
}

fn parse_pg_float(text: &str, kind: SqlTypeKind) -> Result<f64, ExecError> {
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
        "nan" | "+nan" | "-nan" => return Ok(if matches!(kind, SqlTypeKind::Float4) {
            f32::NAN as f64
        } else {
            f64::NAN
        }),
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
            let parsed64 = trimmed.parse::<f64>().map_err(|_| ExecError::InvalidFloatInput {
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
    let parsed = trimmed.parse::<f64>().map_err(|_| ExecError::InvalidFloatInput {
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
    use super::{cast_float_to_int, parse_pg_float};
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
}
