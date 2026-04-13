use super::ExecError;
use super::expr_format::{to_char_int, to_char_numeric, to_number_numeric};
use super::expr_ops::parse_numeric_text;
use super::node_types::Value;
use crate::pgrust::compact_string::CompactString;
use encoding_rs::Encoding;
use md5::{Digest, Md5};
use regex::{Captures, Regex, RegexBuilder};

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
    let regex = build_regex_with_policy(
        pattern.as_text().ok_or_else(|| ExecError::TypeMismatch {
            op: "regexp_like",
            left: text.clone(),
            right: pattern.clone(),
        })?,
        flags,
        RegexGlobalPolicy::Reject,
    )?;
    let haystack = text.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "regexp_like",
        left: text.clone(),
        right: pattern.clone(),
    })?;
    Ok(Value::Bool(regex.is_match(haystack)))
}

pub(super) fn eval_regexp_count(values: &[Value]) -> Result<Value, ExecError> {
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let (text, pattern, start, flags) = regex_count_args(values)?;
    let regex = build_regex_with_policy(pattern, flags, RegexGlobalPolicy::Reject)?;
    let subject = slice_from_char_start(text, start)?;
    Ok(Value::Int32(regex.find_iter(subject).count() as i32))
}

pub(super) fn eval_regexp_instr(values: &[Value]) -> Result<Value, ExecError> {
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let (text, pattern, start, nth, return_end, flags, subexpr) = regex_instr_args(values)?;
    let regex = build_regex_with_policy(pattern, flags, RegexGlobalPolicy::Reject)?;
    let subject = slice_from_char_start(text, start)?;
    let Some((captures, base_offset)) =
        nth_capture_match(&regex, subject, nth, start, subexpr)?
    else {
        return Ok(Value::Int32(0));
    };
    let Some(matched) = capture_by_index(&captures, subexpr) else {
        return Ok(Value::Int32(0));
    };
    let pos = if return_end == 1 {
        char_index_from_byte(subject, matched.end()) + base_offset + 1
    } else {
        char_index_from_byte(subject, matched.start()) + base_offset + 1
    };
    Ok(Value::Int32(pos as i32))
}

pub(super) fn eval_regexp_substr(values: &[Value]) -> Result<Value, ExecError> {
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let (text, pattern, start, nth, flags, subexpr) = regex_substr_args(values)?;
    let regex = build_regex_with_policy(pattern, flags, RegexGlobalPolicy::Reject)?;
    let subject = slice_from_char_start(text, start)?;
    let Some((captures, _)) = nth_capture_match(&regex, subject, nth, start, subexpr)? else {
        return Ok(Value::Null);
    };
    let Some(matched) = capture_by_index(&captures, subexpr) else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(CompactString::from(matched.as_str())))
}

pub(super) fn eval_regexp_replace(values: &[Value]) -> Result<Value, ExecError> {
    let Some(text_value) = values.first() else {
        return Ok(Value::Null);
    };
    let Some(pattern_value) = values.get(1) else {
        return Ok(Value::Null);
    };
    let Some(replacement_value) = values.get(2) else {
        return Ok(Value::Null);
    };
    if matches!(text_value, Value::Null)
        || matches!(pattern_value, Value::Null)
        || matches!(replacement_value, Value::Null)
    {
        return Ok(Value::Null);
    }
    let text = text_value.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "regexp_replace",
        left: text_value.clone(),
        right: pattern_value.clone(),
    })?;
    let pattern = pattern_value.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "regexp_replace",
        left: text_value.clone(),
        right: pattern_value.clone(),
    })?;
    let replacement = replacement_value.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op: "regexp_replace",
        left: text_value.clone(),
        right: replacement_value.clone(),
    })?;
    let (start, nth, flags) = regexp_replace_options(values)?;
    let (regex, global) = build_regex_and_global(pattern, flags, RegexGlobalPolicy::Allow)?;
    let expanded = translate_regexp_replacement(replacement);
    let start_byte = byte_index_from_char_start(text, start)?;
    let prefix = &text[..start_byte];
    let subject = &text[start_byte..];
    let replaced_subject = if global || nth == 0 {
        regex.replace_all(subject, expanded.as_str()).to_string()
    } else {
        replace_nth_match(&regex, subject, nth, expanded.as_str())
    };
    let replaced = format!("{prefix}{replaced_subject}");
    Ok(Value::Text(CompactString::from_owned(replaced)))
}

pub(super) fn eval_regexp_split_to_array(values: &[Value]) -> Result<Value, ExecError> {
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let (text, pattern, flags) = regex_text_pattern_flags_only("regexp_split_to_array", values)?;
    let regex = build_regex_with_policy(pattern, flags, RegexGlobalPolicy::Reject)?;
    Ok(Value::Array(
        regex
            .split(text)
            .map(|part| Value::Text(CompactString::from(part)))
            .collect(),
    ))
}

pub(super) fn eval_regexp_matches_rows(values: &[Value]) -> Result<Vec<Value>, ExecError> {
    let (text, pattern, flags) = regex_text_pattern_flags_only("regexp_matches", values)?;
    let (regex, global) = build_regex_and_global(pattern, flags, RegexGlobalPolicy::Allow)?;
    let mut rows = Vec::new();
    for captures in regex.captures_iter(text) {
        rows.push(Value::Array(captures_to_array(&captures)));
        if !global {
            break;
        }
    }
    Ok(rows)
}

pub(super) fn eval_regexp_split_to_table_rows(values: &[Value]) -> Result<Vec<Value>, ExecError> {
    let (text, pattern, flags) = regex_text_pattern_flags_only("regexp_split_to_table", values)?;
    let regex = build_regex_with_policy(pattern, flags, RegexGlobalPolicy::Reject)?;
    Ok(regex
        .split(text)
        .map(|part| Value::Text(CompactString::from(part)))
        .collect())
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

#[derive(Clone, Copy)]
enum RegexGlobalPolicy {
    Allow,
    Reject,
}

fn build_regex_with_policy(
    pattern: &str,
    flags: &str,
    global_policy: RegexGlobalPolicy,
) -> Result<Regex, ExecError> {
    build_regex_and_global(pattern, flags, global_policy).map(|(regex, _)| regex)
}

fn build_regex_and_global(
    pattern: &str,
    flags: &str,
    global_policy: RegexGlobalPolicy,
) -> Result<(Regex, bool), ExecError> {
    let mut builder = RegexBuilder::new(pattern);
    let mut global = false;
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
            'g' => match global_policy {
                RegexGlobalPolicy::Allow => global = true,
                RegexGlobalPolicy::Reject => {
                    return Err(ExecError::InvalidRegex(
                        "regular expression option g is not supported for this function".into(),
                    ));
                }
            },
            other => {
                return Err(ExecError::InvalidRegex(format!(
                    "invalid regular expression option: {other}"
                )));
            }
        };
    }
    let regex = builder
        .build()
        .map_err(|e| ExecError::InvalidRegex(e.to_string()))?;
    Ok((regex, global))
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

fn regex_text_pattern_flags_only<'a>(
    op: &'static str,
    values: &'a [Value],
) -> Result<(&'a str, &'a str, &'a str), ExecError> {
    let Some(text) = values.first() else {
        return Ok(("", "", ""));
    };
    let Some(pattern) = values.get(1) else {
        return Ok(("", "", ""));
    };
    if matches!(text, Value::Null) || matches!(pattern, Value::Null) {
        return Ok(("", "", ""));
    }
    let flags = match values.get(2) {
        Some(Value::Null) => return Ok(("", "", "")),
        Some(value) => value.as_text().ok_or_else(|| ExecError::TypeMismatch {
            op,
            left: text.clone(),
            right: value.clone(),
        })?,
        None => "",
    };
    let text_value = text;
    let text = text_value.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op,
        left: text_value.clone(),
        right: pattern.clone(),
    })?;
    let pattern_value = pattern;
    let pattern = pattern_value.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op,
        left: text_value.clone(),
        right: pattern_value.clone(),
    })?;
    Ok((text, pattern, flags))
}

fn regex_count_args(values: &[Value]) -> Result<(&str, &str, i32, &str), ExecError> {
    let (text, pattern) = regex_text_pattern_pair("regexp_count", values)?;
    let start = optional_regex_i32_arg("regexp_count", values.get(2), 1)?;
    let flags = optional_regex_text_arg("regexp_count", values.get(3), "")?;
    if start <= 0 {
        return Err(ExecError::RaiseException(
            "regexp_count start position must be greater than zero".into(),
        ));
    }
    Ok((text, pattern, start, flags))
}

fn regex_instr_args(values: &[Value]) -> Result<(&str, &str, i32, i32, i32, &str, usize), ExecError> {
    let (text, pattern) = regex_text_pattern_pair("regexp_instr", values)?;
    let start = optional_regex_i32_arg("regexp_instr", values.get(2), 1)?;
    let nth = optional_regex_i32_arg("regexp_instr", values.get(3), 1)?;
    let return_end = optional_regex_i32_arg("regexp_instr", values.get(4), 0)?;
    let flags = optional_regex_text_arg("regexp_instr", values.get(5), "")?;
    let subexpr = optional_regex_i32_arg("regexp_instr", values.get(6), 0)?;
    if start <= 0 {
        return Err(ExecError::RaiseException(
            "regexp_instr start position must be greater than zero".into(),
        ));
    }
    if nth <= 0 {
        return Err(ExecError::RaiseException(
            "regexp_instr occurrence must be greater than zero".into(),
        ));
    }
    if !matches!(return_end, 0 | 1) {
        return Err(ExecError::RaiseException(
            "regexp_instr return option must be 0 or 1".into(),
        ));
    }
    if subexpr < 0 {
        return Err(ExecError::RaiseException(
            "regexp_instr subexpression must not be negative".into(),
        ));
    }
    Ok((text, pattern, start, nth, return_end, flags, subexpr as usize))
}

fn regex_substr_args(values: &[Value]) -> Result<(&str, &str, i32, i32, &str, usize), ExecError> {
    let (text, pattern) = regex_text_pattern_pair("regexp_substr", values)?;
    let start = optional_regex_i32_arg("regexp_substr", values.get(2), 1)?;
    let nth = optional_regex_i32_arg("regexp_substr", values.get(3), 1)?;
    let flags = optional_regex_text_arg("regexp_substr", values.get(4), "")?;
    let subexpr = optional_regex_i32_arg("regexp_substr", values.get(5), 0)?;
    if start <= 0 {
        return Err(ExecError::RaiseException(
            "regexp_substr start position must be greater than zero".into(),
        ));
    }
    if nth <= 0 {
        return Err(ExecError::RaiseException(
            "regexp_substr occurrence must be greater than zero".into(),
        ));
    }
    if subexpr < 0 {
        return Err(ExecError::RaiseException(
            "regexp_substr subexpression must not be negative".into(),
        ));
    }
    Ok((text, pattern, start, nth, flags, subexpr as usize))
}

fn regexp_replace_options(values: &[Value]) -> Result<(i32, i32, &str), ExecError> {
    let mut start = 1;
    let mut nth = 1;
    let mut flags = "";
    match values.len() {
        4 => match values[3] {
            Value::Int32(v) => start = v,
            Value::Null => return Ok((1, 1, "")),
            _ => flags = values[3].as_text().ok_or_else(|| ExecError::TypeMismatch {
                op: "regexp_replace",
                left: values[0].clone(),
                right: values[3].clone(),
            })?,
        },
        5 => {
            start = optional_regex_i32_arg("regexp_replace", values.get(3), 1)?;
            nth = optional_regex_i32_arg("regexp_replace", values.get(4), 1)?;
        }
        6 => {
            start = optional_regex_i32_arg("regexp_replace", values.get(3), 1)?;
            nth = optional_regex_i32_arg("regexp_replace", values.get(4), 1)?;
            flags = optional_regex_text_arg("regexp_replace", values.get(5), "")?;
        }
        _ => {}
    }
    if start <= 0 {
        return Err(ExecError::RaiseException(
            "regexp_replace start position must be greater than zero".into(),
        ));
    }
    if nth < 0 {
        return Err(ExecError::RaiseException(
            "regexp_replace occurrence must not be negative".into(),
        ));
    }
    Ok((start, nth, flags))
}

fn regex_text_pattern_pair<'a>(
    op: &'static str,
    values: &'a [Value],
) -> Result<(&'a str, &'a str), ExecError> {
    let Some(text_value) = values.first() else {
        return Ok(("", ""));
    };
    let Some(pattern_value) = values.get(1) else {
        return Ok(("", ""));
    };
    let text = text_value.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op,
        left: text_value.clone(),
        right: pattern_value.clone(),
    })?;
    let pattern = pattern_value.as_text().ok_or_else(|| ExecError::TypeMismatch {
        op,
        left: text_value.clone(),
        right: pattern_value.clone(),
    })?;
    Ok((text, pattern))
}

fn optional_regex_i32_arg(
    op: &'static str,
    value: Option<&Value>,
    default: i32,
) -> Result<i32, ExecError> {
    match value {
        None => Ok(default),
        Some(Value::Null) => Ok(default),
        Some(Value::Int32(v)) => Ok(*v),
        Some(other) => Err(ExecError::TypeMismatch {
            op,
            left: other.clone(),
            right: Value::Int32(default),
        }),
    }
}

fn optional_regex_text_arg<'a>(
    op: &'static str,
    value: Option<&'a Value>,
    default: &'a str,
) -> Result<&'a str, ExecError> {
    match value {
        None => Ok(default),
        Some(Value::Null) => Ok(default),
        Some(value) => value.as_text().ok_or_else(|| ExecError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::Text(default.into()),
        }),
    }
}

fn slice_from_char_start(text: &str, start: i32) -> Result<&str, ExecError> {
    let start_byte = byte_index_from_char_start(text, start)?;
    Ok(&text[start_byte..])
}

fn byte_index_from_char_start(text: &str, start: i32) -> Result<usize, ExecError> {
    if start <= 0 {
        return Err(ExecError::RaiseException(
            "regex start position must be greater than zero".into(),
        ));
    }
    if start == 1 {
        return Ok(0);
    }
    let char_index = (start - 1) as usize;
    Ok(text
        .char_indices()
        .nth(char_index)
        .map(|(idx, _)| idx)
        .unwrap_or(text.len()))
}

fn char_index_from_byte(text: &str, byte_index: usize) -> usize {
    text[..byte_index].chars().count()
}

fn nth_capture_match<'a>(
    regex: &Regex,
    subject: &'a str,
    nth: i32,
    start: i32,
    subexpr: usize,
) -> Result<Option<(Captures<'a>, usize)>, ExecError> {
    let base_offset = (start - 1) as usize;
    for (idx, captures) in regex.captures_iter(subject).enumerate() {
        if idx + 1 == nth as usize {
            if subexpr >= captures.len() {
                return Ok(None);
            }
            return Ok(Some((captures, base_offset)));
        }
    }
    Ok(None)
}

fn capture_by_index<'a>(captures: &'a Captures<'a>, index: usize) -> Option<regex::Match<'a>> {
    captures.get(index)
}

fn captures_to_array(captures: &Captures<'_>) -> Vec<Value> {
    if captures.len() <= 1 {
        return vec![Value::Text(CompactString::from(
            captures.get(0).map(|m| m.as_str()).unwrap_or(""),
        ))];
    }
    (1..captures.len())
        .map(|idx| {
            captures
                .get(idx)
                .map(|m| Value::Text(CompactString::from(m.as_str())))
                .unwrap_or(Value::Null)
        })
        .collect()
}

fn replace_nth_match(regex: &Regex, subject: &str, nth: i32, replacement: &str) -> String {
    for (idx, captures) in regex.captures_iter(subject).enumerate() {
        if idx + 1 == nth as usize {
            let matched = captures.get(0).unwrap();
            let mut out = String::with_capacity(subject.len() + replacement.len());
            out.push_str(&subject[..matched.start()]);
            captures.expand(replacement, &mut out);
            out.push_str(&subject[matched.end()..]);
            return out;
        }
    }
    subject.to_string()
}
