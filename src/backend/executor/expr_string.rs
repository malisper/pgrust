use super::ExecError;
use super::expr_format::{to_char_int, to_char_numeric, to_number_numeric};
use super::expr_ops::parse_numeric_text;
use super::node_types::Value;
use crate::pgrust::compact_string::CompactString;
use encoding_rs::Encoding;
use md5::{Digest, Md5};
use regex::{Regex, RegexBuilder};

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
        Value::Float64(v) => {
            let numeric =
                parse_numeric_text(&v.to_string()).ok_or_else(|| ExecError::TypeMismatch {
                    op: "to_char",
                    left: value.clone(),
                    right: Value::Text("".into()),
                })?;
            to_char_numeric(&numeric, fmt)?
        }
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

pub(super) fn eval_length_function(values: &[Value]) -> Result<Value, ExecError> {
    let Some(value) = values.first() else {
        return Ok(Value::Null);
    };
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
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

pub(super) fn eval_trim_function(
    op: &'static str,
    values: &[Value],
) -> Result<Value, ExecError> {
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

pub(super) fn eval_regexp_like(values: &[Value]) -> Result<Value, ExecError> {
    let Some(text) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(pattern) = values.get(1) else {
        return Ok(Value::Null);
    };
    if matches!(text, Value::Null) || matches!(pattern, Value::Null) {
        return Ok(Value::Null);
    }
    let flags = if let Some(Value::Null) = values.get(2) {
        return Ok(Value::Null);
    } else {
        values.get(2).and_then(Value::as_text).unwrap_or("")
    };
    let regex = build_regex(pattern.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "regexp_like",
        left: text.clone(),
        right: pattern.clone(),
    })?, flags)?;
    let haystack = text.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "regexp_like",
        left: text.clone(),
        right: pattern.clone(),
    })?;
    Ok(Value::Bool(regex.is_match(haystack)))
}

pub(super) fn eval_regexp_replace(values: &[Value]) -> Result<Value, ExecError> {
    let Some(text) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(pattern) = values.get(1) else {
        return Ok(Value::Null);
    };
    let Some(replacement) = values.get(2) else {
        return Ok(Value::Null);
    };
    if matches!(text, Value::Null) || matches!(pattern, Value::Null) || matches!(replacement, Value::Null) {
        return Ok(Value::Null);
    }
    let flags = if let Some(Value::Null) = values.get(3) {
        return Ok(Value::Null);
    } else {
        values.get(3).and_then(Value::as_text).unwrap_or("")
    };
    let regex = build_regex(pattern.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "regexp_replace",
        left: text.clone(),
        right: pattern.clone(),
    })?, flags)?;
    let replacement = replacement.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "regexp_replace",
        left: text.clone(),
        right: replacement.clone(),
    })?;
    let expanded = translate_regexp_replacement(replacement);
    let haystack = text.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "regexp_replace",
        left: text.clone(),
        right: pattern.clone(),
    })?;
    let replaced = if flags.contains('g') {
        regex
            .replace_all(haystack, expanded.as_str())
            .to_string()
    } else {
        regex.replace(haystack, expanded.as_str()).to_string()
    };
    Ok(Value::Text(CompactString::from_owned(replaced)))
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

fn like_match_text(text: &str, pattern: &str, escape: Option<char>, case_insensitive: bool) -> bool {
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

fn build_regex(pattern: &str, flags: &str) -> Result<Regex, ExecError> {
    let mut builder = RegexBuilder::new(pattern);
    for flag in flags.chars() {
        match flag {
            'i' => {
                builder.case_insensitive(true);
            }
            's' => {
                builder.dot_matches_new_line(true);
            }
            'm' => {
                builder.multi_line(true);
            }
            'x' => {
                builder.ignore_whitespace(true);
            }
            'n' => {
                builder.multi_line(true);
            }
            'g' => {}
            other => {
                return Err(ExecError::InvalidRegex(format!(
                    "invalid regular expression option: {other}"
                )));
            }
        };
    }
    builder
        .build()
        .map_err(|e| ExecError::InvalidRegex(e.to_string()))
}

fn translate_regexp_replacement(replacement: &str) -> String {
    let mut out = String::new();
    let chars: Vec<char> = replacement.chars().collect();
    let mut i = 0usize;
    while i < chars.len() {
        if chars[i] == '\\' {
            if i + 1 >= chars.len() {
                break;
            }
            match chars[i + 1] {
                '&' => out.push_str("${0}"),
                '1'..='9' => {
                    out.push('$');
                    out.push(chars[i + 1]);
                }
                '\\' => out.push('\\'),
                other => out.push(other),
            }
            i += 2;
        } else {
            out.push(chars[i]);
            i += 1;
        }
    }
    out
}

fn normalize_encoding_label(name: &str) -> String {
    name.trim().to_ascii_lowercase().replace('_', "-")
}
