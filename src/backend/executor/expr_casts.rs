use super::exec_expr::parse_numeric_text;
use super::expr_json::{canonicalize_jsonpath_text, validate_json_text};
use super::node_types::*;
use super::ExecError;
use crate::backend::executor::jsonb::{parse_jsonb_text, render_jsonb_bytes};
use crate::backend::parser::{SqlType, SqlTypeKind};
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

fn parse_input_type_name(type_name: &str) -> Result<Option<SqlType>, ExecError> {
    let ty = type_name.trim().to_ascii_lowercase();
    let sql_type = match ty.as_str() {
        "int2" | "smallint" => Some(SqlType::new(SqlTypeKind::Int2)),
        "int4" | "int" | "integer" => Some(SqlType::new(SqlTypeKind::Int4)),
        "int8" | "bigint" => Some(SqlType::new(SqlTypeKind::Int8)),
        "text" => Some(SqlType::new(SqlTypeKind::Text)),
        "bool" | "boolean" => Some(SqlType::new(SqlTypeKind::Bool)),
        "numeric" | "decimal" => Some(SqlType::new(SqlTypeKind::Numeric)),
        _ => None,
    };
    Ok(sql_type)
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
        ExecError::InvalidNumericInput(_) => {
            format!("invalid input syntax for type numeric: \"{text}\"")
        }
        other => format!("{other:?}"),
    }
}

fn input_error_sqlstate(err: &ExecError) -> &'static str {
    match err {
        ExecError::InvalidIntegerInput { .. } | ExecError::InvalidNumericInput(_) => "22P02",
        ExecError::IntegerOutOfRange { .. }
        | ExecError::Int2OutOfRange
        | ExecError::Int4OutOfRange
        | ExecError::Int8OutOfRange => {
            "22003"
        }
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
            } => Err(ExecError::TypeMismatch {
                op: "::bool",
                left: Value::Int16(v),
                right: Value::Bool(false),
            }),
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
            } => Err(ExecError::TypeMismatch {
                op: "::bool",
                left: Value::Int32(v),
                right: Value::Bool(false),
            }),
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
            } => Err(ExecError::TypeMismatch {
                op: "::bool",
                left: Value::Int64(v),
                right: Value::Bool(false),
            }),
        },
        Value::Float64(v) => match ty {
            SqlType {
                kind: SqlTypeKind::Float4 | SqlTypeKind::Float8,
                ..
            } => Ok(Value::Float64(if matches!(ty.kind, SqlTypeKind::Float4) {
                (v as f32) as f64
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
        SqlTypeKind::Float4 | SqlTypeKind::Float8 => parse_pg_float(text)
            .map(|v| {
                Value::Float64(if matches!(ty.kind, SqlTypeKind::Float4) {
                    (v as f32) as f64
                } else {
                    v
                })
            })
            .map_err(|_| ExecError::InvalidFloatInput(text.to_string())),
        SqlTypeKind::Numeric => Ok(Value::Numeric(coerce_numeric_value(
            parse_numeric_text(text)
                .ok_or_else(|| ExecError::InvalidNumericInput(text.to_string()))?,
            ty,
        )?)),
        SqlTypeKind::Bool => match text.to_ascii_lowercase().as_str() {
            "true" | "t" => Ok(Value::Bool(true)),
            "false" | "f" => Ok(Value::Bool(false)),
            _ => Err(ExecError::TypeMismatch {
                op: "::bool",
                left: Value::Text(CompactString::new(text)),
                right: Value::Bool(false),
            }),
        },
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
        SqlTypeKind::Char | SqlTypeKind::Varchar => cast_text_value(&value.render(), ty, explicit),
        SqlTypeKind::Float4 => {
            let rendered = value.render();
            let v = parse_pg_float(&rendered)
                .map_err(|_| ExecError::InvalidFloatInput(rendered.clone()))?;
            Ok(Value::Float64((v as f32) as f64))
        }
        SqlTypeKind::Float8 => {
            let rendered = value.render();
            let v = parse_pg_float(&rendered)
                .map_err(|_| ExecError::InvalidFloatInput(rendered.clone()))?;
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
        SqlTypeKind::Bool => Err(ExecError::TypeMismatch {
            op: "::bool",
            left: Value::Numeric(value),
            right: Value::Bool(false),
        }),
    }
}

fn coerce_character_string(text: &str, ty: SqlType, explicit: bool) -> Result<String, ExecError> {
    let Some(max_chars) = ty.char_len() else {
        return Ok(text.to_string());
    };

    let char_count = text.chars().count() as i32;
    if char_count <= max_chars {
        return Ok(text.to_string());
    }

    let clip_idx = text
        .char_indices()
        .nth(max_chars as usize)
        .map(|(idx, _)| idx)
        .unwrap_or(text.len());
    let truncated = &text[..clip_idx];
    let remainder = &text[clip_idx..];
    if explicit || remainder.chars().all(|ch| ch == ' ') {
        Ok(truncated.to_string())
    } else {
        Err(ExecError::StringDataRightTruncation {
            ty: format!("character varying({max_chars})"),
        })
    }
}

fn cast_float_to_int(value: f64, ty: SqlType) -> Result<Value, ExecError> {
    if !value.is_finite() {
        return Err(ExecError::InvalidFloatInput(value.to_string()));
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
            if rounded < i64::MIN as f64 || rounded > i64::MAX as f64 {
                Err(ExecError::Int8OutOfRange)
            } else {
                Ok(Value::Int64(rounded as i64))
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

fn parse_pg_float(text: &str) -> Result<f64, ()> {
    if text.eq_ignore_ascii_case("infinity") || text.eq_ignore_ascii_case("+infinity") {
        Ok(f64::INFINITY)
    } else if text.eq_ignore_ascii_case("-infinity") {
        Ok(f64::NEG_INFINITY)
    } else {
        text.parse::<f64>().map_err(|_| ())
    }
}
