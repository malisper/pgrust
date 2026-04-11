use super::exec_expr::parse_numeric_text;
use super::expr_casts::{cast_numeric_value, cast_text_value, cast_value};
use super::node_types::*;
use super::ExecError;
use crate::backend::executor::expr_json::{canonicalize_jsonpath_text, validate_json_text};
use crate::backend::executor::jsonb::{decode_jsonb, render_jsonb_bytes};
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::access::htup::{HeapTuple, TupleValue};
use crate::pgrust::compact_string::CompactString;

pub(crate) fn tuple_from_values(desc: &RelationDesc, values: &[Value]) -> Result<HeapTuple, ExecError> {
    let tuple_values = desc
        .columns
        .iter()
        .zip(values.iter())
        .map(|(column, value)| encode_value(column, value))
        .collect::<Result<Vec<_>, _>>()?;
    HeapTuple::from_values(&desc.attribute_descs(), &tuple_values).map_err(ExecError::from)
}

pub(crate) fn encode_value(column: &ColumnDesc, value: &Value) -> Result<TupleValue, ExecError> {
    if matches!(value, Value::Null) {
        return if !column.storage.nullable {
            Err(ExecError::MissingRequiredColumn(column.name.clone()))
        } else {
            Ok(TupleValue::Null)
        };
    }

    let coerced = coerce_assignment_value(value, column.sql_type)?;
    match (&column.ty, coerced) {
        (ScalarType::Int16, Value::Int16(v)) => Ok(TupleValue::Bytes(v.to_le_bytes().to_vec())),
        (ScalarType::Int32, Value::Int32(v)) => Ok(TupleValue::Bytes(v.to_le_bytes().to_vec())),
        (ScalarType::Int32, Value::Int64(v)) if matches!(column.sql_type.kind, SqlTypeKind::Oid) => {
            let oid = u32::try_from(v).map_err(|_| ExecError::OidOutOfRange)?;
            Ok(TupleValue::Bytes(oid.to_le_bytes().to_vec()))
        }
        (ScalarType::Int64, Value::Int64(v)) => Ok(TupleValue::Bytes(v.to_le_bytes().to_vec())),
        (ScalarType::Float32, Value::Float64(v)) => Ok(TupleValue::Bytes((v as f32).to_le_bytes().to_vec())),
        (ScalarType::Float64, Value::Float64(v)) => Ok(TupleValue::Bytes(v.to_le_bytes().to_vec())),
        (ScalarType::Numeric, Value::Numeric(numeric)) => Ok(TupleValue::Bytes(numeric.render().into_bytes())),
        (ScalarType::Json, Value::Json(text)) => Ok(TupleValue::Bytes(text.as_bytes().to_vec())),
        (ScalarType::Jsonb, Value::Jsonb(bytes)) => Ok(TupleValue::Bytes(bytes)),
        (ScalarType::JsonPath, Value::JsonPath(text)) => Ok(TupleValue::Bytes(text.as_bytes().to_vec())),
        (ScalarType::Text, value) => Ok(TupleValue::Bytes(value.as_text().unwrap().as_bytes().to_vec())),
        (ScalarType::Bool, Value::Bool(v)) => Ok(TupleValue::Bytes(vec![u8::from(v)])),
        (ScalarType::Array(_), Value::Array(items)) => {
            Ok(TupleValue::Bytes(encode_array_bytes(column.sql_type.element_type(), &items)?))
        }
        (_, other) => Err(ExecError::TypeMismatch { op: "assignment", left: Value::Null, right: other }),
    }
}

fn coerce_assignment_value(value: &Value, target: SqlType) -> Result<Value, ExecError> {
    if target.is_array {
        return match value {
            Value::Null => Ok(Value::Null),
            Value::Array(items) => {
                let element_type = target.element_type();
                let mut coerced = Vec::with_capacity(items.len());
                for item in items {
                    coerced.push(coerce_assignment_value(item, element_type)?);
                }
                Ok(Value::Array(coerced))
            }
            other => Err(ExecError::TypeMismatch {
                op: "copy assignment",
                left: Value::Null,
                right: other.clone(),
            }),
        };
    }

    match value {
        Value::Null => Ok(Value::Null),
        Value::Int16(v) => cast_text_value(&v.to_string(), target, false),
        Value::Int32(v) => cast_text_value(&v.to_string(), target, false),
        Value::Int64(v) => cast_text_value(&v.to_string(), target, false),
        Value::Bool(v) => cast_text_value(if *v { "true" } else { "false" }, target, false),
        Value::Float64(v) => match target.kind {
            SqlTypeKind::Float4
            | SqlTypeKind::Float8
            | SqlTypeKind::Numeric
            | SqlTypeKind::Int2
            | SqlTypeKind::Int4
            | SqlTypeKind::Int8
            | SqlTypeKind::Oid => cast_value(Value::Float64(*v), target),
            _ => cast_text_value(&v.to_string(), target, false),
        },
        Value::Numeric(numeric) => cast_numeric_value(numeric.clone(), target, false),
        Value::JsonPath(text) => cast_text_value(text.as_str(), target, false),
        Value::Json(text) => cast_text_value(text.as_str(), target, false),
        Value::Jsonb(bytes) => cast_text_value(&render_jsonb_bytes(bytes)?, target, false),
        Value::Text(text) => cast_text_value(text.as_str(), target, false),
        Value::TextRef(_, _) => cast_text_value(value.as_text().unwrap(), target, false),
        Value::InternalChar(byte) => cast_value(Value::InternalChar(*byte), target),
        Value::Array(items) => Ok(Value::Array(items.clone())),
    }
}

pub(crate) fn decode_value(column: &ColumnDesc, bytes: Option<&[u8]>) -> Result<Value, ExecError> {
    let Some(bytes) = bytes else {
        return Ok(Value::Null);
    };

    match column.ty {
        ScalarType::Int16 => {
            if column.storage.attlen != 2 || bytes.len() != 2 {
                return Err(ExecError::UnsupportedStorageType { column: column.name.clone(), ty: column.ty.clone(), attlen: column.storage.attlen });
            }
            Ok(Value::Int16(i16::from_le_bytes(bytes.try_into().map_err(|_| ExecError::InvalidStorageValue {
                column: column.name.clone(),
                details: "int2 must be exactly 2 bytes".into(),
            })?)))
        }
        ScalarType::Int32 => {
            if column.storage.attlen != 4 || bytes.len() != 4 {
                return Err(ExecError::UnsupportedStorageType { column: column.name.clone(), ty: column.ty.clone(), attlen: column.storage.attlen });
            }
            let raw = i32::from_le_bytes(bytes.try_into().map_err(|_| ExecError::InvalidStorageValue {
                column: column.name.clone(),
                details: "int4 must be exactly 4 bytes".into(),
            })?);
            if matches!(column.sql_type.kind, SqlTypeKind::Oid) {
                Ok(Value::Int64(raw as u32 as i64))
            } else {
                Ok(Value::Int32(raw))
            }
        }
        ScalarType::Int64 => {
            if column.storage.attlen != 8 || bytes.len() != 8 {
                return Err(ExecError::UnsupportedStorageType { column: column.name.clone(), ty: column.ty.clone(), attlen: column.storage.attlen });
            }
            Ok(Value::Int64(i64::from_le_bytes(bytes.try_into().map_err(|_| ExecError::InvalidStorageValue {
                column: column.name.clone(),
                details: "int8 must be exactly 8 bytes".into(),
            })?)))
        }
        ScalarType::Float32 => {
            if column.storage.attlen != 4 || bytes.len() != 4 {
                return Err(ExecError::UnsupportedStorageType { column: column.name.clone(), ty: column.ty.clone(), attlen: column.storage.attlen });
            }
            Ok(Value::Float64(f32::from_le_bytes(bytes.try_into().map_err(|_| ExecError::InvalidStorageValue {
                column: column.name.clone(),
                details: "float4 must be exactly 4 bytes".into(),
            })?) as f64))
        }
        ScalarType::Float64 => {
            if column.storage.attlen != 8 || bytes.len() != 8 {
                return Err(ExecError::UnsupportedStorageType { column: column.name.clone(), ty: column.ty.clone(), attlen: column.storage.attlen });
            }
            Ok(Value::Float64(f64::from_le_bytes(bytes.try_into().map_err(|_| ExecError::InvalidStorageValue {
                column: column.name.clone(),
                details: "float8 must be exactly 8 bytes".into(),
            })?)))
        }
        ScalarType::Numeric => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(ExecError::UnsupportedStorageType { column: column.name.clone(), ty: column.ty.clone(), attlen: column.storage.attlen });
            }
            Ok(Value::Numeric(parse_numeric_text(unsafe { std::str::from_utf8_unchecked(bytes) }).ok_or_else(|| ExecError::InvalidStorageValue {
                column: column.name.clone(),
                details: "invalid numeric text".into(),
            })?))
        }
        ScalarType::Json => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(ExecError::UnsupportedStorageType { column: column.name.clone(), ty: column.ty.clone(), attlen: column.storage.attlen });
            }
            let text = unsafe { std::str::from_utf8_unchecked(bytes) };
            validate_json_text(text)?;
            Ok(Value::Json(CompactString::new(text)))
        }
        ScalarType::Jsonb => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(ExecError::UnsupportedStorageType { column: column.name.clone(), ty: column.ty.clone(), attlen: column.storage.attlen });
            }
            decode_jsonb(bytes)?;
            Ok(Value::Jsonb(bytes.to_vec()))
        }
        ScalarType::JsonPath => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(ExecError::UnsupportedStorageType { column: column.name.clone(), ty: column.ty.clone(), attlen: column.storage.attlen });
            }
            let text = unsafe { std::str::from_utf8_unchecked(bytes) };
            Ok(Value::JsonPath(canonicalize_jsonpath_text(text)?))
        }
        ScalarType::Text => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(ExecError::UnsupportedStorageType { column: column.name.clone(), ty: column.ty.clone(), attlen: column.storage.attlen });
            }
            Ok(Value::Text(CompactString::new(unsafe { std::str::from_utf8_unchecked(bytes) })))
        }
        ScalarType::Bool => {
            if column.storage.attlen != 1 || bytes.len() != 1 {
                return Err(ExecError::UnsupportedStorageType { column: column.name.clone(), ty: column.ty.clone(), attlen: column.storage.attlen });
            }
            match bytes[0] {
                0 => Ok(Value::Bool(false)),
                1 => Ok(Value::Bool(true)),
                other => Err(ExecError::InvalidStorageValue {
                    column: column.name.clone(),
                    details: format!("invalid bool byte {}", other),
                }),
            }
        }
        ScalarType::Array(_) => {
            if column.storage.attlen != -1 {
                return Err(ExecError::UnsupportedStorageType { column: column.name.clone(), ty: column.ty.clone(), attlen: column.storage.attlen });
            }
            decode_array_bytes(column.sql_type.element_type(), bytes)
        }
    }
}

fn encode_array_bytes(element_type: SqlType, items: &[Value]) -> Result<Vec<u8>, ExecError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(items.len() as u32).to_le_bytes());
    for item in items {
        match item {
            Value::Null => bytes.extend_from_slice(&(-1_i32).to_le_bytes()),
            _ => {
                let payload = encode_array_element(element_type, item)?;
                bytes.extend_from_slice(&(payload.len() as i32).to_le_bytes());
                bytes.extend_from_slice(&payload);
            }
        }
    }
    Ok(bytes)
}

fn encode_array_element(element_type: SqlType, value: &Value) -> Result<Vec<u8>, ExecError> {
    let coerced = coerce_assignment_value(value, element_type)?;
    match coerced {
        Value::Null => Ok(Vec::new()),
        Value::Int16(v) => Ok(v.to_le_bytes().to_vec()),
        Value::Int32(v) => Ok(v.to_le_bytes().to_vec()),
        Value::Int64(v) => Ok(v.to_le_bytes().to_vec()),
        Value::Bool(v) => Ok(vec![u8::from(v)]),
        Value::Numeric(text) => Ok(text.render().into_bytes()),
        Value::Json(text) => Ok(text.as_bytes().to_vec()),
        Value::Text(text) => Ok(text.as_bytes().to_vec()),
        Value::TextRef(_, _) => Ok(coerced.as_text().unwrap().as_bytes().to_vec()),
        Value::InternalChar(v) => Ok(vec![v]),
        Value::Float64(v) => Ok(v.to_string().into_bytes()),
        Value::JsonPath(text) => Ok(text.as_bytes().to_vec()),
        Value::Array(_) => Err(ExecError::TypeMismatch { op: "array element", left: coerced, right: Value::Null }),
        Value::Jsonb(bytes) => Ok(bytes),
    }
}

fn decode_array_bytes(element_type: SqlType, bytes: &[u8]) -> Result<Value, ExecError> {
    if bytes.len() < 4 {
        return Err(ExecError::InvalidStorageValue { column: "<array>".into(), details: "array payload too short".into() });
    }
    let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let mut offset = 4usize;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        if offset + 4 > bytes.len() {
            return Err(ExecError::InvalidStorageValue { column: "<array>".into(), details: "array length header truncated".into() });
        }
        let len = i32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;
        if len == -1 {
            items.push(Value::Null);
            continue;
        }
        let len = len as usize;
        if offset + len > bytes.len() {
            return Err(ExecError::InvalidStorageValue { column: "<array>".into(), details: "array element payload truncated".into() });
        }
        items.push(decode_array_element(element_type, &bytes[offset..offset + len])?);
        offset += len;
    }
    Ok(Value::Array(items))
}

fn decode_array_element(element_type: SqlType, bytes: &[u8]) -> Result<Value, ExecError> {
    match element_type.kind {
        SqlTypeKind::Int2 => {
            if bytes.len() != 2 {
                return Err(ExecError::InvalidStorageValue { column: "<array>".into(), details: "int2 array element must be 2 bytes".into() });
            }
            Ok(Value::Int16(i16::from_le_bytes(bytes.try_into().unwrap())))
        }
        SqlTypeKind::Int4 | SqlTypeKind::Oid => {
            if bytes.len() != 4 {
                return Err(ExecError::InvalidStorageValue { column: "<array>".into(), details: "int4 array element must be 4 bytes".into() });
            }
            Ok(Value::Int32(i32::from_le_bytes(bytes.try_into().unwrap())))
        }
        SqlTypeKind::Int8 => {
            if bytes.len() != 8 {
                return Err(ExecError::InvalidStorageValue { column: "<array>".into(), details: "int8 array element must be 8 bytes".into() });
            }
            Ok(Value::Int64(i64::from_le_bytes(bytes.try_into().unwrap())))
        }
        SqlTypeKind::Float4 | SqlTypeKind::Float8 => {
            let width = if matches!(element_type.kind, SqlTypeKind::Float4) { 4 } else { 8 };
            if bytes.len() != width {
                return Err(ExecError::InvalidStorageValue { column: "<array>".into(), details: "float array element has wrong width".into() });
            }
            if matches!(element_type.kind, SqlTypeKind::Float4) {
                Ok(Value::Float64(f32::from_le_bytes(bytes.try_into().unwrap()) as f64))
            } else {
                Ok(Value::Float64(f64::from_le_bytes(bytes.try_into().unwrap())))
            }
        }
        SqlTypeKind::Numeric => Ok(Value::Numeric(parse_numeric_text(unsafe { std::str::from_utf8_unchecked(bytes) }).ok_or_else(|| ExecError::InvalidStorageValue {
            column: "<array>".into(),
            details: "invalid numeric array element".into(),
        })?)),
        SqlTypeKind::Json => {
            let text = unsafe { std::str::from_utf8_unchecked(bytes) };
            validate_json_text(text)?;
            Ok(Value::Json(CompactString::new(text)))
        }
        SqlTypeKind::Jsonb => {
            decode_jsonb(bytes)?;
            Ok(Value::Jsonb(bytes.to_vec()))
        }
        SqlTypeKind::JsonPath => {
            let text = unsafe { std::str::from_utf8_unchecked(bytes) };
            Ok(Value::JsonPath(canonicalize_jsonpath_text(text)?))
        }
        SqlTypeKind::Bool => {
            if bytes.len() != 1 {
                return Err(ExecError::InvalidStorageValue { column: "<array>".into(), details: "bool array element must be 1 byte".into() });
            }
            Ok(Value::Bool(bytes[0] != 0))
        }
        SqlTypeKind::Text
        | SqlTypeKind::Timestamp
        | SqlTypeKind::InternalChar
        | SqlTypeKind::Char
        | SqlTypeKind::Varchar => {
            Ok(Value::Text(CompactString::new(unsafe { std::str::from_utf8_unchecked(bytes) })))
        }
    }
}

pub(crate) fn format_array_text(items: &[Value]) -> String {
    let mut out = String::from("{");
    for (idx, item) in items.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        match item {
            Value::Null => out.push_str("NULL"),
            Value::Int16(v) => out.push_str(&v.to_string()),
            Value::Int32(v) => out.push_str(&v.to_string()),
            Value::Int64(v) => out.push_str(&v.to_string()),
            Value::Float64(v) => out.push_str(&v.to_string()),
            Value::Numeric(v) => out.push_str(&v.render()),
            Value::Json(v) => {
                out.push('"');
                for ch in v.chars() {
                    match ch {
                        '"' | '\\' => {
                            out.push('\\');
                            out.push(ch);
                        }
                        _ => out.push(ch),
                    }
                }
                out.push('"');
            }
            Value::JsonPath(v) => {
                out.push('"');
                for ch in v.chars() {
                    match ch {
                        '"' | '\\' => {
                            out.push('\\');
                            out.push(ch);
                        }
                        _ => out.push(ch),
                    }
                }
                out.push('"');
            }
            Value::Jsonb(v) => {
                let rendered = render_jsonb_bytes(v).unwrap_or_else(|_| "null".into());
                out.push('"');
                for ch in rendered.chars() {
                    match ch {
                        '"' | '\\' => {
                            out.push('\\');
                            out.push(ch);
                        }
                        _ => out.push(ch),
                    }
                }
                out.push('"');
            }
            Value::Bool(v) => out.push_str(if *v { "true" } else { "false" }),
            Value::Text(_) | Value::TextRef(_, _) => {
                out.push('"');
                for ch in item.as_text().unwrap().chars() {
                    match ch {
                        '"' | '\\' => {
                            out.push('\\');
                            out.push(ch);
                        }
                        _ => out.push(ch),
                    }
                }
                out.push('"');
            }
            Value::InternalChar(byte) => {
                let rendered = super::expr_casts::render_internal_char_text(*byte);
                out.push('"');
                for ch in rendered.chars() {
                    match ch {
                        '"' | '\\' => {
                            out.push('\\');
                            out.push(ch);
                        }
                        _ => out.push(ch),
                    }
                }
                out.push('"');
            }
            Value::Array(nested) => out.push_str(&format_array_text(nested)),
        }
    }
    out.push('}');
    out
}
