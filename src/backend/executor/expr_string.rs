use super::ExecError;
use super::expr_format::to_char_int;
use super::node_types::Value;
use crate::pgrust::compact_string::CompactString;
use encoding_rs::Encoding;

pub(super) fn eval_to_char_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(value) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(format) = values.get(1) else {
        return Ok(Value::Null);
    };
    let number = match value {
        Value::Int16(v) => *v as i128,
        Value::Int32(v) => *v as i128,
        Value::Int64(v) => *v as i128,
        _ => {
            return Err(ExecError::TypeMismatch {
                op: "to_char",
                left: value.clone(),
                right: Value::Text("".into()),
            });
        }
    };
    let fmt = format.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "to_char",
        left: format.clone(),
        right: Value::Text("".into()),
    })?;
    Ok(Value::Text(to_char_int(number, fmt)?.into()))
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
    let text = text_value.as_text().ok_or_else(|| ExecError::TypeMismatch {
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
    let text = text_value.as_text().ok_or_else(|| ExecError::TypeMismatch {
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
    let text = text_value.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "lower",
        left: text_value.clone(),
        right: Value::Text("".into()),
    })?;
    Ok(Value::Text(CompactString::from_owned(text.to_lowercase())))
}

pub(super) fn eval_bpchar_to_text_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(text_value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(text_value, Value::Null) {
        return Ok(Value::Null);
    }
    let text = text_value.as_text().ok_or_else(|| ExecError::TypeMismatch {
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
    let needle = needle_value.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "position",
        left: needle_value.clone(),
        right: haystack_value.clone(),
    })?;
    let haystack = haystack_value.as_text().ok_or_else(|| ExecError::TypeMismatch {
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
    let raw = bytes_value.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "convert_from",
        left: bytes_value.clone(),
        right: encoding_value.clone(),
    })?;
    let encoding_name = encoding_value.as_text().ok_or_else(|| ExecError::TypeMismatch {
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
    let encoding = Encoding::for_label(normalized.as_bytes()).ok_or_else(|| ExecError::TypeMismatch {
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

fn normalize_encoding_label(name: &str) -> String {
    name.trim().to_ascii_lowercase().replace('_', "-")
}
