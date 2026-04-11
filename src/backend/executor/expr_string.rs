use super::ExecError;
use super::expr_format::to_char_int;
use super::node_types::Value;
use crate::pgrust::compact_string::CompactString;

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
