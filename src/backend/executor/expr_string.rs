use super::ExecError;
use super::expr_bit::render_bit_text;
use super::expr_casts::{cast_value, parse_bytea_text, render_internal_char_text};
use super::expr_datetime::render_datetime_value_text;
use super::expr_format::{to_char_float, to_char_int, to_char_numeric, to_number_numeric};
use super::expr_range::render_range_text;
use super::node_types::Value;
use super::value_io::format_array_text;
use crate::backend::executor::jsonb::render_jsonb_bytes;
use crate::backend::libpq::pqformat::format_bytea_text;
use crate::backend::parser::{ParseError, SqlType, SqlTypeKind};
use crate::backend::utils::record::assign_anonymous_record_descriptor;
use crate::pgrust::compact_string::CompactString;
use crate::pgrust::session::ByteaOutputFormat;
use base64::Engine as _;
use encoding_rs::{DecoderResult, EncoderResult, Encoding};
use md5::{Digest, Md5};
use sha2::{Sha224, Sha256, Sha384, Sha512};

pub(super) fn eval_to_char_function(values: &[Value]) -> Result<Value, ExecError> {
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
        Value::Float64(v) => to_char_float(*v, fmt)?,
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

fn value_output_text(value: &Value) -> Result<String, ExecError> {
    Ok(match value {
        Value::Null => String::new(),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Money(v) => crate::backend::executor::money_format_text(*v),
        Value::Float64(v) => v.to_string(),
        Value::Numeric(v) => v.render(),
        Value::Bool(v) => {
            if *v {
                "t".into()
            } else {
                "f".into()
            }
        }
        Value::Text(_) | Value::TextRef(_, _) | Value::JsonPath(_) => {
            value.as_text().unwrap().into()
        }
        Value::Json(v) => v.as_str().into(),
        Value::Jsonb(bytes) => render_jsonb_bytes(bytes)?,
        Value::Bit(bits) => render_bit_text(bits),
        Value::Bytea(bytes) => format_bytea_text(bytes, ByteaOutputFormat::Hex),
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
        Value::Range(_) => render_range_text(value).unwrap_or_default(),
        Value::InternalChar(byte) => render_internal_char_text(*byte),
        Value::Date(_)
        | Value::Time(_)
        | Value::TimeTz(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_) => {
            render_datetime_value_text(value).expect("datetime values render")
        }
        Value::TsVector(vector) => crate::backend::executor::render_tsvector_text(vector),
        Value::TsQuery(query) => crate::backend::executor::render_tsquery_text(query),
        Value::Array(values) => format_array_text(values),
        Value::PgArray(array) => crate::backend::executor::value_io::format_array_value_text(array),
        Value::Record(record) => crate::backend::executor::expr_json::eval_json_builtin_function(
            crate::include::nodes::primnodes::BuiltinScalarFunction::RowToJson,
            &[Value::Record(record.clone())],
            false,
            &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        )
        .expect("row_to_json is a json builtin")?
        .as_text()
        .unwrap_or_default()
        .to_string(),
    })
}

fn quote_identifier(identifier: &str) -> String {
    if !identifier.is_empty()
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

fn format_arg_text(kind: char, value: &Value) -> Result<String, ExecError> {
    match kind {
        's' => {
            if matches!(value, Value::Null) {
                Ok(String::new())
            } else {
                value_output_text(value)
            }
        }
        'I' => {
            if matches!(value, Value::Null) {
                Err(ExecError::RaiseException(
                    "null values cannot be formatted as an SQL identifier".into(),
                ))
            } else {
                Ok(quote_identifier(&value_output_text(value)?))
            }
        }
        'L' => {
            if matches!(value, Value::Null) {
                Ok("NULL".into())
            } else {
                Ok(quote_literal_text(&value_output_text(value)?))
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

pub(super) fn eval_concat_function(values: &[Value]) -> Result<Value, ExecError> {
    let mut out = String::new();
    for value in values {
        if matches!(value, Value::Null) {
            continue;
        }
        out.push_str(&value_output_text(value)?);
    }
    Ok(Value::Text(CompactString::from_owned(out)))
}

pub(super) fn eval_concat_ws_function(values: &[Value]) -> Result<Value, ExecError> {
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
        out.push_str(&value_output_text(value)?);
    }
    Ok(Value::Text(CompactString::from_owned(out)))
}

pub(super) fn eval_format_function(values: &[Value]) -> Result<Value, ExecError> {
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
        let rendered = format_arg_text(kind, value)?;
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
    let text = value.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "length",
        left: value.clone(),
        right: Value::Null,
    })?;
    Ok(Value::Int32(text.chars().count() as i32))
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
    let parts: Vec<&str> = if delim.is_empty() {
        text.char_indices()
            .map(|(idx, ch)| &text[idx..idx + ch.len_utf8()])
            .collect()
    } else {
        text.split(delim).collect()
    };
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
    case_insensitive: bool,
    negated: bool,
) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(pattern, Value::Null) {
        return Ok(Value::Null);
    }
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
                None => None,
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
        "base64" => base64::engine::general_purpose::STANDARD.encode(bytes),
        _ => {
            return Err(ExecError::RaiseException(format!(
                "unrecognized encoding: \"{format}\""
            )));
        }
    };
    Ok(Value::Text(CompactString::from_owned(rendered)))
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
        "escape" => parse_bytea_text(text)?,
        "base64" => base64::engine::general_purpose::STANDARD
            .decode(text)
            .map_err(|_| ExecError::RaiseException("invalid base64 end sequence".into()))?,
        _ => {
            return Err(ExecError::RaiseException(format!(
                "unrecognized encoding: \"{format}\""
            )));
        }
    };
    Ok(Value::Bytea(bytes))
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
    let shift = 7 - (index % 8) as u8;
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
    let mask = 1u8 << (7 - (index % 8) as u8);
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

fn validate_bytea_index(bytes: &[u8], index: i32, op: &'static str) -> Result<(), ExecError> {
    if !(0..bytes.len() as i32).contains(&index) {
        return Err(ExecError::RaiseException(format!(
            "{op} index {index} out of valid range, 0..{}",
            bytes.len().saturating_sub(1)
        )));
    }
    Ok(())
}

fn validate_bytea_bit_index(bytes: &[u8], index: i32) -> Result<(), ExecError> {
    let max_index = (bytes.len() as i32).saturating_mul(8).saturating_sub(1);
    if index < 0 || index > max_index {
        return Err(ExecError::BitIndexOutOfRange { index, max_index });
    }
    Ok(())
}
