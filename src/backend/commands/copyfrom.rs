use crate::backend::executor::{ExecError, Value};
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
                SqlTypeKind::Int4 => value
                    .parse::<i32>()
                    .map(Value::Int32)
                    .map_err(|_| ExecError::Parse(ParseError::InvalidInteger(value.clone())))?,
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
                | SqlTypeKind::Timestamp
                | SqlTypeKind::Char
                | SqlTypeKind::Varchar => Value::Text(value.into()),
            }
        };
        items.push(value);

        if chars.peek() == Some(&',') {
            chars.next();
        }
    }

    Ok(Value::Array(items))
}
