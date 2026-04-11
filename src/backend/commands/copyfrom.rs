use crate::backend::executor::jsonpath::canonicalize_jsonpath;
use crate::backend::executor::{ExecError, Value, parse_bytea_text};
use crate::backend::parser::{ParseError, SqlType, SqlTypeKind};

pub fn parse_text_array_literal(raw: &str, element_type: SqlType) -> Result<Value, ExecError> {
    if raw == "{}" {
        return Ok(Value::Array(Vec::new()));
    }
    if !raw.starts_with('{') || !raw.ends_with('}') {
        return Err(ExecError::TypeMismatch {
            op: "copy assignment",
            left: Value::Null,
            right: Value::Text(raw.into()),
        });
    }

    let mut chars = raw[1..raw.len() - 1].chars().peekable();
    let mut items = Vec::new();
    while chars.peek().is_some() {
        let value = if chars.peek() == Some(&'"') {
            chars.next();
            let mut text = String::new();
            while let Some(ch) = chars.next() {
                match ch {
                    '"' => break,
                    '\\' => {
                        let escaped = chars.next().ok_or_else(|| ExecError::TypeMismatch {
                            op: "copy assignment",
                            left: Value::Null,
                            right: Value::Text(raw.into()),
                        })?;
                        text.push(escaped);
                    }
                    other => text.push(other),
                }
            }
            text
        } else {
            let mut text = String::new();
            while let Some(&ch) = chars.peek() {
                if ch == ',' {
                    break;
                }
                text.push(ch);
                chars.next();
            }
            text
        };

        let value = if value == "NULL" {
            Value::Null
        } else {
            match element_type.kind {
                SqlTypeKind::Int2 => value
                    .parse::<i16>()
                    .map(Value::Int16)
                    .map_err(|_| ExecError::Parse(ParseError::InvalidInteger(value.clone())))?,
                SqlTypeKind::Int4 => value
                    .parse::<i32>()
                    .map(Value::Int32)
                    .map_err(|_| ExecError::Parse(ParseError::InvalidInteger(value.clone())))?,
                SqlTypeKind::Oid => value
                    .parse::<i32>()
                    .map(Value::Int32)
                    .map_err(|_| ExecError::Parse(ParseError::InvalidInteger(value.clone())))?,
                SqlTypeKind::Int8 => value
                    .parse::<i64>()
                    .map(Value::Int64)
                    .map_err(|_| ExecError::Parse(ParseError::InvalidInteger(value.clone())))?,
                SqlTypeKind::Float4 | SqlTypeKind::Float8 => value
                    .parse::<f64>()
                    .map(Value::Float64)
                    .map_err(|_| ExecError::TypeMismatch {
                        op: "copy assignment",
                        left: Value::Null,
                        right: Value::Text(value.clone().into()),
                    })?,
                SqlTypeKind::Bool => match value.as_str() {
                    "t" | "true" | "1" => Value::Bool(true),
                    "f" | "false" | "0" => Value::Bool(false),
                    _ => {
                        return Err(ExecError::TypeMismatch {
                            op: "copy assignment",
                            left: Value::Null,
                            right: Value::Text(value.into()),
                        });
                    }
                },
                SqlTypeKind::Text
                | SqlTypeKind::Numeric
                | SqlTypeKind::Json
                | SqlTypeKind::Jsonb
                | SqlTypeKind::JsonPath
                | SqlTypeKind::Timestamp
                | SqlTypeKind::Bytea
                | SqlTypeKind::Char
                | SqlTypeKind::Varchar => {
                    if matches!(element_type.kind, SqlTypeKind::Numeric) {
                        Value::Numeric(value.as_str().into())
                    } else if matches!(element_type.kind, SqlTypeKind::Json) {
                        Value::Json(value.into())
                    } else if matches!(element_type.kind, SqlTypeKind::Jsonb) {
                        Value::Jsonb(crate::backend::executor::jsonb::parse_jsonb_text(&value)?)
                    } else if matches!(element_type.kind, SqlTypeKind::JsonPath) {
                        Value::JsonPath(
                            canonicalize_jsonpath(&value).map_err(|_| ExecError::InvalidStorageValue {
                                column: "<array>".into(),
                                details: format!(
                                    "invalid input syntax for type jsonpath: \"{value}\""
                                ),
                            })?.into()
                        )
                    } else if matches!(element_type.kind, SqlTypeKind::Bytea) {
                        Value::Bytea(parse_bytea_text(&value)?)
                    } else {
                        Value::Text(value.into())
                    }
                }
                SqlTypeKind::InternalChar => {
                    let bytes = value.as_bytes();
                    if bytes.is_empty() {
                        Value::InternalChar(0)
                    } else if bytes.len() == 4
                        && bytes[0] == b'\\'
                        && bytes[1..].iter().all(|b| (b'0'..=b'7').contains(b))
                    {
                        Value::InternalChar(
                            (bytes[1] - b'0') * 64 + (bytes[2] - b'0') * 8 + (bytes[3] - b'0'),
                        )
                    } else {
                        Value::InternalChar(bytes[0])
                    }
                }
            }
        };
        items.push(value);

        if chars.peek() == Some(&',') {
            chars.next();
        }
    }

    Ok(Value::Array(items))
}
