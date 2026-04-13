use super::ExecError;
use super::exec_expr::parse_numeric_text;
use super::expr_bit::{coerce_bit_string, parse_bit_text, render_bit_text};
use super::expr_bool::cast_integer_to_bool;
use super::expr_bool::parse_pg_bool_text;
use super::expr_json::{canonicalize_jsonpath_text, validate_json_text};
use super::node_types::*;
use crate::backend::executor::jsonb::{parse_jsonb_text, render_jsonb_bytes};
use crate::backend::parser::{SqlType, SqlTypeKind, parse_type_name};
use crate::include::catalog::{TEXT_TYPE_OID, bootstrap_pg_cast_rows, builtin_type_rows};
use crate::pgrust::compact_string::CompactString;
use num_integer::Integer;
use num_traits::Signed;
use std::collections::BTreeSet;
use std::sync::OnceLock;

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
    if normalized.is_empty() || !normalized.chars().all(|ch| ch.is_digit(base)) {
        return Err(ExecError::InvalidIntegerInput {
            ty,
            value: text.to_string(),
        });
    }

    let magnitude =
        i128::from_str_radix(&normalized, base).map_err(|_| ExecError::InvalidIntegerInput {
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
    let oid = if (0..=u32::MAX as i128).contains(&value) {
        value as u32
    } else if (i32::MIN as i128..=-1).contains(&value) {
        (value as i32) as u32
    } else {
        return Err(ExecError::IntegerOutOfRange {
            ty: "oid",
            value: text.to_string(),
        });
    };
    Ok(Value::Int64(oid as i64))
}

pub(crate) fn parse_bytea_text(text: &str) -> Result<Vec<u8>, ExecError> {
    if let Some(rest) = text.strip_prefix("\\x") {
        let normalized: String = rest
            .chars()
            .filter(|ch| !ch.is_ascii_whitespace())
            .collect();
        if normalized.len() % 2 != 0 {
            return Err(ExecError::InvalidByteaInput {
                value: text.to_string(),
            });
        }
        let mut out = Vec::with_capacity(normalized.len() / 2);
        for chunk in normalized.as_bytes().chunks(2) {
            let hex = std::str::from_utf8(chunk).map_err(|_| ExecError::InvalidByteaInput {
                value: text.to_string(),
            })?;
            out.push(
                u8::from_str_radix(hex, 16).map_err(|_| ExecError::InvalidByteaInput {
                    value: text.to_string(),
                })?,
            );
        }
        return Ok(out);
    }

    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut idx = 0usize;
    while idx < bytes.len() {
        if bytes[idx] != b'\\' {
            out.push(bytes[idx]);
            idx += 1;
            continue;
        }
        idx += 1;
        if idx >= bytes.len() {
            return Err(ExecError::InvalidByteaInput {
                value: text.to_string(),
            });
        }
        if bytes[idx] == b'\\' {
            out.push(b'\\');
            idx += 1;
            continue;
        }
        if idx + 2 >= bytes.len()
            || !(b'0'..=b'7').contains(&bytes[idx])
            || !(b'0'..=b'7').contains(&bytes[idx + 1])
            || !(b'0'..=b'7').contains(&bytes[idx + 2])
        {
            return Err(ExecError::InvalidByteaInput {
                value: text.to_string(),
            });
        }
        let value =
            (bytes[idx] - b'0') * 64 + (bytes[idx + 1] - b'0') * 8 + (bytes[idx + 2] - b'0');
        out.push(value);
        idx += 3;
    }
    Ok(out)
}

fn parse_oid_token_prefix(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    if bytes.is_empty() {
        return None;
    }
    let mut idx = 0;
    if matches!(bytes[0], b'+' | b'-') {
        idx += 1;
    }
    let start_digits = idx;
    while idx < bytes.len() && bytes[idx].is_ascii_digit() {
        idx += 1;
    }
    (idx > start_digits).then_some(idx)
}

fn soft_parse_oidvector_input(text: &str) -> Result<Option<InputErrorInfo>, ExecError> {
    let mut remaining = text;
    loop {
        remaining = remaining.trim_start_matches(|ch: char| ch.is_ascii_whitespace());
        if remaining.is_empty() {
            return Ok(None);
        }
        let Some(prefix_len) = parse_oid_token_prefix(remaining) else {
            let err = ExecError::InvalidIntegerInput {
                ty: "oid",
                value: remaining.to_string(),
            };
            return Ok(Some(InputErrorInfo {
                message: input_error_message(&err, remaining),
                detail: None,
                hint: None,
                sqlstate: input_error_sqlstate(&err),
            }));
        };
        let token = &remaining[..prefix_len];
        if let Err(err) = cast_text_to_oid(token) {
            return Ok(Some(InputErrorInfo {
                message: input_error_message(&err, token),
                detail: None,
                hint: None,
                sqlstate: input_error_sqlstate(&err),
            }));
        }
        remaining = &remaining[prefix_len..];
    }
}

fn parse_internal_char_text(text: &str) -> u8 {
    let bytes = text.as_bytes();
    if bytes.is_empty() {
        return 0;
    }
    if bytes.len() == 4 && bytes[0] == b'\\' && bytes[1..].iter().all(|b| (b'0'..=b'7').contains(b))
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

pub(crate) fn parse_text_array_literal(
    raw: &str,
    element_type: SqlType,
) -> Result<Value, ExecError> {
    parse_text_array_literal_with_options(raw, element_type, "::array", true)
}

pub(crate) fn parse_text_array_literal_with_op(
    raw: &str,
    element_type: SqlType,
    op: &'static str,
) -> Result<Value, ExecError> {
    parse_text_array_literal_with_options(raw, element_type, op, true)
}

pub(crate) fn parse_text_array_literal_with_options(
    raw: &str,
    element_type: SqlType,
    op: &'static str,
    explicit: bool,
) -> Result<Value, ExecError> {
    let (bounds, input) = parse_array_bounds_prefix(raw)?;
    if input == "{}" {
        return Ok(Value::PgArray(ArrayValue::empty()));
    }
    if !input.starts_with('{') || !input.ends_with('}') {
        return Err(invalid_array_literal(
            raw,
            Some("Array value must start with \"{\" or dimension information.".into()),
        ));
    }
    let mut parser = ArrayTextParser::new(input, element_type, explicit);
    let value = parser.parse_array()?;
    parser.skip_ws();
    if !parser.is_eof() {
        return Err(invalid_array_literal(
            raw,
            Some("Junk after closing right brace.".into()),
        ));
    }
    let nested = match value {
        Value::Array(values) => values,
        other => {
            return Err(ExecError::TypeMismatch {
                op,
                left: other,
                right: Value::Null,
            });
        }
    };
    let array = ArrayValue::from_nested_values(nested, bounds.lower_bounds.clone()).map_err(|_| {
        invalid_array_literal(
            raw,
            Some("Multidimensional arrays must have sub-arrays with matching dimensions.".into()),
        )
    })?;
    if let Some(expected_lengths) = &bounds.lengths
        && (expected_lengths.len() != array.dimensions.len()
            || expected_lengths
                .iter()
                .zip(array.dimensions.iter())
                .any(|(expected, actual)| *expected != actual.length))
    {
        return Err(invalid_array_literal(
            raw,
            Some("Specified array dimensions do not match array contents.".into()),
        ));
    }
    Ok(Value::PgArray(array))
}

#[derive(Default)]
struct ParsedArrayBounds {
    lower_bounds: Vec<i32>,
    lengths: Option<Vec<usize>>,
}

fn parse_array_bounds_prefix(raw: &str) -> Result<(ParsedArrayBounds, &str), ExecError> {
    if !raw.starts_with('[') {
        return Ok((ParsedArrayBounds::default(), raw));
    }
    let Some(equals) = raw.find('=') else {
        return Ok((ParsedArrayBounds::default(), raw));
    };
    let bounds = &raw[..equals];
    let mut lower_bounds = Vec::new();
    let mut lengths = Vec::new();
    let mut remaining = bounds;
    while let Some(rest) = remaining.strip_prefix('[') {
        let Some(end) = rest.find(']') else {
            return Err(invalid_array_literal(raw, None));
        };
        let part = &rest[..end];
        let Some((lower, upper)) = part.split_once(':') else {
            return Err(invalid_array_literal(
                raw,
                Some("Specified array dimensions do not match array contents.".into()),
            ));
        };
        if lower.trim().is_empty() {
            return Err(invalid_array_literal(
                raw,
                Some("\"[\" must introduce explicitly-specified array dimensions.".into()),
            ));
        };
        if upper.trim().is_empty() {
            return Err(invalid_array_literal(
                raw,
                Some("Missing array dimension value.".into()),
            ));
        }
        let lower = parse_array_bound(lower.trim(), raw)?;
        let upper = parse_array_bound(upper.trim(), raw)?;
        if upper < lower {
            return Err(ExecError::ArrayInput {
                message: "upper bound cannot be less than lower bound".into(),
                value: raw.into(),
                detail: None,
                sqlstate: "2202E",
            });
        }
        if upper >= i32::MAX as i64 {
            return Err(ExecError::ArrayInput {
                message: format!("array upper bound is too large: {upper}"),
                value: raw.into(),
                detail: None,
                sqlstate: "54000",
            });
        }
        lower_bounds.push(lower as i32);
        lengths.push((upper - lower + 1) as usize);
        remaining = &rest[end + 1..];
    }
    Ok((
        ParsedArrayBounds {
            lower_bounds,
            lengths: Some(lengths),
        },
        &raw[equals + 1..],
    ))
}

fn parse_array_bound(text: &str, raw: &str) -> Result<i64, ExecError> {
    text.parse::<i64>().map_err(|_| ExecError::ArrayInput {
        message: "array bound is out of integer range".into(),
        value: raw.into(),
        detail: None,
        sqlstate: "22003",
    })
}

fn invalid_array_literal(raw: &str, detail: Option<String>) -> ExecError {
    ExecError::ArrayInput {
        message: format!("malformed array literal: \"{raw}\""),
        value: raw.into(),
        detail,
        sqlstate: "22P02",
    }
}

struct ArrayTextParser<'a> {
    input: &'a str,
    offset: usize,
    element_type: SqlType,
    explicit: bool,
}

impl<'a> ArrayTextParser<'a> {
    fn new(input: &'a str, element_type: SqlType, explicit: bool) -> Self {
        Self {
            input,
            offset: 0,
            element_type,
            explicit,
        }
    }

    fn parse_array(&mut self) -> Result<Value, ExecError> {
        self.skip_ws();
        self.expect('{')?;
        let mut items = Vec::new();
        loop {
            self.skip_ws();
            if self.peek_char() == Some('}') {
                self.bump_char();
                break;
            }
            items.push(self.parse_item()?);
            self.skip_ws();
            match self.peek_char() {
                Some(',') => {
                    self.bump_char();
                    self.skip_ws();
                    if self.peek_char() == Some('}') {
                        return Err(invalid_array_literal(
                            self.input,
                            Some("Unexpected \"}\" character.".into()),
                        ));
                    }
                }
                Some('}') => {
                    self.bump_char();
                    break;
                }
                _ => return self.type_mismatch(),
            }
        }
        Ok(Value::Array(items))
    }

    fn parse_item(&mut self) -> Result<Value, ExecError> {
        self.skip_ws();
        match self.peek_char() {
            Some('{') => self.parse_array(),
            Some('"') => {
                let text = self.parse_quoted_string()?;
                self.skip_ws();
                if matches!(self.peek_char(), Some(ch) if !matches!(ch, ',' | '}')) {
                    return Err(invalid_array_literal(
                        self.input,
                        Some("Incorrectly quoted array element.".into()),
                    ));
                }
                cast_text_value(&text, self.element_type, self.explicit)
            }
            Some(_) => {
                let text = self.parse_unquoted_token();
                if text.is_empty() {
                    let detail = match self.peek_char() {
                        Some(',') => "Unexpected \",\" character.",
                        Some('}') => "Unexpected \"}\" character.",
                        _ => "Unexpected array element.",
                    };
                    return Err(invalid_array_literal(self.input, Some(detail.into())));
                }
                if text.contains('{') {
                    return Err(invalid_array_literal(
                        self.input,
                        Some("Unexpected \"{\" character.".into()),
                    ));
                }
                if text.eq_ignore_ascii_case("NULL") {
                    Ok(Value::Null)
                } else {
                    cast_text_value(text.trim_end(), self.element_type, self.explicit)
                }
            }
            None => self.type_mismatch(),
        }
    }

    fn parse_quoted_string(&mut self) -> Result<String, ExecError> {
        self.expect('"')?;
        let mut text = String::new();
        while let Some(ch) = self.bump_char() {
            match ch {
                '"' => return Ok(text),
                '\\' => {
                    let escaped = self
                        .bump_char()
                        .ok_or_else(|| invalid_array_literal(self.input, None))?;
                    text.push(escaped);
                }
                other => text.push(other),
            }
        }
        self.type_mismatch()
    }

    fn parse_unquoted_token(&mut self) -> &'a str {
        let start = self.offset;
        while let Some(ch) = self.peek_char() {
            if matches!(ch, ',' | '}') {
                break;
            }
            self.bump_char();
        }
        &self.input[start..self.offset]
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek_char(), Some(ch) if ch.is_ascii_whitespace()) {
            self.bump_char();
        }
    }

    fn expect(&mut self, expected: char) -> Result<(), ExecError> {
        if self.bump_char() == Some(expected) {
            Ok(())
        } else {
            self.type_mismatch()
        }
    }

    fn peek_char(&self) -> Option<char> {
        self.input[self.offset..].chars().next()
    }

    fn bump_char(&mut self) -> Option<char> {
        let ch = self.peek_char()?;
        self.offset += ch.len_utf8();
        Some(ch)
    }

    fn is_eof(&self) -> bool {
        self.offset >= self.input.len()
    }

    fn type_mismatch<T>(&self) -> Result<T, ExecError> {
        Err(invalid_array_literal(
            self.input,
            Some("Unexpected array element.".into()),
        ))
    }
}

fn parse_input_type_name(type_name: &str) -> Result<Option<SqlType>, ExecError> {
    let parsed = match parse_type_name(type_name.trim()) {
        Ok(ty) => ty,
        Err(_) => return Ok(None),
    };
    Ok(input_type_name_supported(parsed).then_some(parsed))
}

fn input_type_name_supported(parsed: SqlType) -> bool {
    if !parsed.is_array && matches!(parsed.kind, SqlTypeKind::Text) {
        return true;
    }
    let Some(type_oid) = builtin_type_oid(parsed) else {
        return false;
    };
    explicit_text_input_target_oids().contains(&type_oid)
}

fn builtin_type_oid(sql_type: SqlType) -> Option<u32> {
    builtin_type_rows().into_iter().find_map(|row| {
        (row.sql_type.is_array == sql_type.is_array && row.sql_type.kind == sql_type.kind)
            .then_some(row.oid)
    })
}

fn explicit_text_input_target_oids() -> &'static BTreeSet<u32> {
    static OIDS: OnceLock<BTreeSet<u32>> = OnceLock::new();
    OIDS.get_or_init(|| {
        bootstrap_pg_cast_rows()
            .into_iter()
            .filter(|row| row.castsource == TEXT_TYPE_OID && row.castmethod == 'i')
            .map(|row| row.casttarget)
            .collect()
    })
}

fn input_error_message(err: &ExecError, text: &str) -> String {
    match err {
        ExecError::InvalidIntegerInput { ty, value } => {
            format!("invalid input syntax for type {ty}: \"{value}\"")
        }
        ExecError::ArrayInput { message, .. } => message.clone(),
        ExecError::IntegerOutOfRange { ty, value } => {
            format!("value \"{value}\" is out of range for type {ty}")
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
        ExecError::OidOutOfRange => format!("value \"{text}\" is out of range for type oid"),
        ExecError::InvalidNumericInput(_) => {
            format!("invalid input syntax for type numeric: \"{text}\"")
        }
        ExecError::InvalidByteaInput { .. } => {
            format!("invalid input syntax for type bytea: \"{text}\"")
        }
        ExecError::InvalidBitInput { digit, is_hex } => {
            if *is_hex {
                format!("\"{digit}\" is not a valid hexadecimal digit")
            } else {
                format!("\"{digit}\" is not a valid binary digit")
            }
        }
        ExecError::BitStringLengthMismatch { actual, expected } => {
            format!("bit string length {actual} does not match type bit({expected})")
        }
        ExecError::BitStringTooLong { limit, .. } => {
            format!("bit string too long for type bit varying({limit})")
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
        | ExecError::ArrayInput { .. }
        | ExecError::InvalidNumericInput(_)
        | ExecError::InvalidByteaInput { .. }
        | ExecError::InvalidBitInput { .. }
        | ExecError::InvalidBooleanInput { .. } => "22P02",
        ExecError::BitStringLengthMismatch { .. } => "22026",
        ExecError::BitStringTooLong { .. } => "22001",
        ExecError::IntegerOutOfRange { .. }
        | ExecError::Int2OutOfRange
        | ExecError::Int4OutOfRange
        | ExecError::Int8OutOfRange
        | ExecError::OidOutOfRange
        | ExecError::FloatOutOfRange { .. }
        | ExecError::FloatOverflow
        | ExecError::FloatUnderflow => "22003",
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
    if type_name.trim().eq_ignore_ascii_case("oidvector") {
        return soft_parse_oidvector_input(text);
    }

    let ty = parse_input_type_name(type_name)?.ok_or_else(|| ExecError::InvalidStorageValue {
        column: type_name.to_string(),
        details: format!("unsupported type: {type_name}"),
    })?;
    let parsed = match ty.kind {
        // PostgreSQL's pg_input_* helpers use the type input function semantics,
        // not explicit-cast padding/truncation semantics for bit and typmod-
        // constrained text inputs.
        SqlTypeKind::Bit
        | SqlTypeKind::VarBit
        | SqlTypeKind::Name
        | SqlTypeKind::Char
        | SqlTypeKind::Varchar => cast_text_value(text, ty, false),
        _ => cast_value(Value::Text(text.into()), ty),
    };
    match parsed {
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
            Value::PgArray(array) => {
                let element_type = ty.element_type();
                let mut casted = Vec::with_capacity(array.elements.len());
                for item in array.elements {
                    casted.push(cast_value(item, element_type)?);
                }
                Ok(Value::PgArray(ArrayValue::from_dimensions(
                    array.dimensions,
                    casted,
                )))
            }
            other => match other.as_text() {
                Some(text) => parse_text_array_literal(text, ty.element_type()),
                None => Err(ExecError::TypeMismatch {
                    op: "::array",
                    left: other,
                    right: Value::Null,
                }),
            },
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
                    Ok(Value::Int64(v as u32 as i64))
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
                    | SqlTypeKind::Name
                    | SqlTypeKind::Int2Vector
                    | SqlTypeKind::OidVector
                    | SqlTypeKind::Timestamp
                    | SqlTypeKind::PgNodeTree
                    | SqlTypeKind::InternalChar
                    | SqlTypeKind::Bit
                    | SqlTypeKind::VarBit
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
            SqlType {
                kind: SqlTypeKind::Bytea,
                ..
            } => Err(ExecError::TypeMismatch {
                op: "::bytea",
                left: Value::Int16(v),
                right: Value::Bytea(Vec::new()),
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
                kind: SqlTypeKind::Oid,
                ..
            } => {
                if v < 0 {
                    Err(ExecError::OidOutOfRange)
                } else {
                    Ok(Value::Int64(v as u32 as i64))
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
                    | SqlTypeKind::Name
                    | SqlTypeKind::Int2Vector
                    | SqlTypeKind::OidVector
                    | SqlTypeKind::Timestamp
                    | SqlTypeKind::PgNodeTree
                    | SqlTypeKind::InternalChar
                    | SqlTypeKind::Bit
                    | SqlTypeKind::VarBit
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
            SqlType {
                kind: SqlTypeKind::Bytea,
                ..
            } => Err(ExecError::TypeMismatch {
                op: "::bytea",
                left: Value::Int32(v),
                right: Value::Bytea(Vec::new()),
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
                    | SqlTypeKind::Name
                    | SqlTypeKind::Int2Vector
                    | SqlTypeKind::OidVector
                    | SqlTypeKind::Timestamp
                    | SqlTypeKind::PgNodeTree
                    | SqlTypeKind::InternalChar
                    | SqlTypeKind::Bit
                    | SqlTypeKind::VarBit
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
                    | SqlTypeKind::Bytea
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
            SqlTypeKind::Text | SqlTypeKind::Timestamp | SqlTypeKind::Bit | SqlTypeKind::VarBit => {
                Ok(Value::Text(CompactString::from_owned(
                    render_internal_char_text(byte),
                )))
            }
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
        Value::Bytea(bytes) => match ty.kind {
            SqlTypeKind::Bytea => Ok(Value::Bytea(bytes)),
            _ => Err(ExecError::TypeMismatch {
                op: "::bytea",
                left: Value::Bytea(bytes),
                right: Value::Null,
            }),
        },
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
                    Ok(Value::Int64(v as u32 as i64))
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
                    | SqlTypeKind::Name
                    | SqlTypeKind::Int2Vector
                    | SqlTypeKind::OidVector
                    | SqlTypeKind::Timestamp
                    | SqlTypeKind::PgNodeTree
                    | SqlTypeKind::InternalChar
                    | SqlTypeKind::Bit
                    | SqlTypeKind::VarBit
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
            SqlType {
                kind: SqlTypeKind::Bytea,
                ..
            } => Err(ExecError::TypeMismatch {
                op: "::bytea",
                left: Value::Int64(v),
                right: Value::Bytea(Vec::new()),
            }),
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
                    | SqlTypeKind::Name
                    | SqlTypeKind::Int2Vector
                    | SqlTypeKind::OidVector
                    | SqlTypeKind::Timestamp
                    | SqlTypeKind::PgNodeTree
                    | SqlTypeKind::InternalChar
                    | SqlTypeKind::Bit
                    | SqlTypeKind::VarBit
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
            SqlType {
                kind: SqlTypeKind::Bytea,
                ..
            } => Err(ExecError::TypeMismatch {
                op: "::bytea",
                left: Value::Float64(v),
                right: Value::Bytea(Vec::new()),
            }),
        },
        Value::Numeric(numeric) => cast_numeric_value(numeric, ty, true),
        Value::Bit(bits) => match ty.kind {
            SqlTypeKind::Bit | SqlTypeKind::VarBit => {
                Ok(Value::Bit(coerce_bit_string(bits, ty, true)?))
            }
            SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Timestamp => Ok(Value::Text(
                CompactString::from_owned(render_bit_text(&bits)),
            )),
            _ => Err(ExecError::TypeMismatch {
                op: "::bit",
                left: Value::Bit(bits),
                right: Value::Null,
            }),
        },
        Value::Array(items) => Ok(Value::Array(items)),
        Value::PgArray(array) => Ok(Value::PgArray(array)),
    }
}

pub(super) fn cast_text_value(text: &str, ty: SqlType, explicit: bool) -> Result<Value, ExecError> {
    match ty.kind {
        SqlTypeKind::Text
        | SqlTypeKind::Int2Vector
        | SqlTypeKind::OidVector
        | SqlTypeKind::Timestamp
        | SqlTypeKind::PgNodeTree => Ok(Value::Text(CompactString::new(text))),
        SqlTypeKind::InternalChar => Ok(Value::InternalChar(parse_internal_char_text(text))),
        SqlTypeKind::Bit | SqlTypeKind::VarBit => Ok(Value::Bit(coerce_bit_string(
            parse_bit_text(text)?,
            ty,
            explicit,
        )?)),
        SqlTypeKind::Bytea => Ok(Value::Bytea(parse_bytea_text(text)?)),
        SqlTypeKind::Json => {
            validate_json_text(text)?;
            Ok(Value::Json(CompactString::new(text)))
        }
        SqlTypeKind::Jsonb => Ok(Value::Jsonb(parse_jsonb_text(text)?)),
        SqlTypeKind::JsonPath => Ok(Value::JsonPath(canonicalize_jsonpath_text(text)?)),
        SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar => Ok(Value::Text(
            CompactString::from_owned(coerce_character_string(text, ty, explicit)?),
        )),
        SqlTypeKind::Int2 => cast_text_to_int2(text),
        SqlTypeKind::Int4 => cast_text_to_int4(text),
        SqlTypeKind::Int8 => cast_text_to_int8(text),
        SqlTypeKind::Oid => cast_text_to_oid(text),
        SqlTypeKind::Float4 | SqlTypeKind::Float8 => parse_pg_float(text, ty.kind).map(|v| {
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
        SqlTypeKind::Text
        | SqlTypeKind::Int2Vector
        | SqlTypeKind::OidVector
        | SqlTypeKind::Timestamp
        | SqlTypeKind::PgNodeTree => Ok(Value::Text(CompactString::from_owned(value.render()))),
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
        SqlTypeKind::Bit | SqlTypeKind::VarBit => cast_text_value(&value.render(), ty, explicit),
        SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar => {
            cast_text_value(&value.render(), ty, explicit)
        }
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
            .and_then(|rounded| rounded.render().parse::<u32>().ok())
            .and_then(|rounded| Some(Value::Int64(rounded as i64)))
            .ok_or(ExecError::OidOutOfRange),
        SqlTypeKind::Bool => Err(ExecError::TypeMismatch {
            op: "::bool",
            left: Value::Numeric(value),
            right: Value::Bool(false),
        }),
        SqlTypeKind::Bytea => Err(ExecError::TypeMismatch {
            op: "::bytea",
            left: Value::Numeric(value),
            right: Value::Bytea(Vec::new()),
        }),
    }
}

fn coerce_character_string(text: &str, ty: SqlType, explicit: bool) -> Result<String, ExecError> {
    let max_chars = match ty.kind {
        SqlTypeKind::Name => return Ok(text.to_string()),
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
            if rounded < 0.0 || rounded > u32::MAX as f64 {
                Err(ExecError::OidOutOfRange)
            } else {
                Ok(Value::Int64(rounded as u32 as i64))
            }
        }
        _ => unreachable!(),
    }
}

fn coerce_numeric_value(parsed: NumericValue, ty: SqlType) -> Result<NumericValue, ExecError> {
    let Some((precision, scale)) = ty.numeric_precision_scale() else {
        return Ok(parsed);
    };

    let rounded = if scale >= 0 {
        parsed
            .round_to_scale(scale as u32)
            .ok_or(ExecError::NumericFieldOverflow)?
    } else {
        coerce_numeric_negative_scale(parsed, scale)?
    };

    match rounded {
        NumericValue::NaN => Ok(NumericValue::NaN),
        NumericValue::PosInf | NumericValue::NegInf => Err(ExecError::NumericFieldOverflow),
        NumericValue::Finite { .. } => {
            let max_digits_before_decimal = precision - scale;
            let digits_before_decimal = numeric_digits_before_decimal(&rounded);
            if digits_before_decimal > max_digits_before_decimal {
                return Err(ExecError::NumericFieldOverflow);
            }
            Ok(rounded)
        }
    }
}

fn coerce_numeric_negative_scale(
    parsed: NumericValue,
    scale: i32,
) -> Result<NumericValue, ExecError> {
    let shift = scale.unsigned_abs();
    match parsed {
        NumericValue::Finite {
            coeff,
            scale: current_scale,
        } => {
            let integer = coeff;
            let factor = pow10_bigint(current_scale.saturating_add(shift));
            let (quotient, remainder) = integer.div_rem(&factor);
            let twice = remainder.abs() * 2u8;
            let rounded = if twice >= factor.abs() {
                quotient + integer.signum()
            } else {
                quotient
            };
            Ok(NumericValue::Finite {
                coeff: rounded * pow10_bigint(shift),
                scale: 0,
            }
            .normalize())
        }
        other => Ok(other),
    }
}

fn numeric_digits_before_decimal(value: &NumericValue) -> i32 {
    match value {
        NumericValue::Finite { coeff, scale } => {
            let digits = coeff
                .to_str_radix(10)
                .trim_start_matches('-')
                .trim_start_matches('0')
                .len()
                .max(1) as i32;
            (digits - *scale as i32).max(0)
        }
        _ => 0,
    }
}

fn pow10_bigint(exp: u32) -> num_bigint::BigInt {
    let mut value = num_bigint::BigInt::from(1u8);
    for _ in 0..exp {
        value *= 10u8;
    }
    value
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
        "nan" | "+nan" | "-nan" => {
            return Ok(if matches!(kind, SqlTypeKind::Float4) {
                f32::NAN as f64
            } else {
                f64::NAN
            });
        }
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
            let parsed64 = trimmed
                .parse::<f64>()
                .map_err(|_| ExecError::InvalidFloatInput {
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
    let parsed = trimmed
        .parse::<f64>()
        .map_err(|_| ExecError::InvalidFloatInput {
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
    use super::{
        cast_float_to_int, cast_value, parse_input_type_name, parse_pg_float,
        parse_text_array_literal, soft_input_error_info,
    };
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

    #[test]
    fn parse_input_type_name_uses_text_input_cast_surface() {
        assert_eq!(
            parse_input_type_name("jsonb").unwrap(),
            Some(SqlType::new(SqlTypeKind::Jsonb))
        );
        assert_eq!(
            parse_input_type_name("jsonpath").unwrap(),
            Some(SqlType::new(SqlTypeKind::JsonPath))
        );
        assert_eq!(
            parse_input_type_name("timestamp").unwrap(),
            Some(SqlType::new(SqlTypeKind::Timestamp))
        );
        assert_eq!(
            parse_input_type_name("varchar(4)").unwrap(),
            Some(SqlType::with_char_len(SqlTypeKind::Varchar, 4))
        );
        assert_eq!(
            parse_input_type_name("int4[]").unwrap(),
            Some(SqlType::array_of(SqlType::new(SqlTypeKind::Int4)))
        );
        assert_eq!(
            parse_input_type_name("varchar(4)[]").unwrap(),
            Some(SqlType::array_of(SqlType::with_char_len(
                SqlTypeKind::Varchar,
                4
            )))
        );
        assert_eq!(
            parse_input_type_name("int4[][]").unwrap(),
            Some(SqlType::array_of(SqlType::new(SqlTypeKind::Int4)))
        );
    }

    #[test]
    fn parse_text_array_literal_uses_scalar_input_parsers() {
        assert_eq!(
            parse_text_array_literal("{1,2}", SqlType::new(SqlTypeKind::Int4)).unwrap(),
            Value::Array(vec![Value::Int32(1), Value::Int32(2)])
        );
        assert_eq!(
            parse_text_array_literal("{\"NULL\",NULL}", SqlType::new(SqlTypeKind::Text)).unwrap(),
            Value::Array(vec![Value::Text("NULL".into()), Value::Null])
        );
        assert_eq!(
            parse_text_array_literal("{true,false}", SqlType::new(SqlTypeKind::Bool)).unwrap(),
            Value::Array(vec![Value::Bool(true), Value::Bool(false)])
        );
        assert_eq!(
            parse_text_array_literal("{{1,4},{2,5},{3,6}}", SqlType::new(SqlTypeKind::Int4))
                .unwrap(),
            Value::Array(vec![
                Value::Array(vec![Value::Int32(1), Value::Int32(4)]),
                Value::Array(vec![Value::Int32(2), Value::Int32(5)]),
                Value::Array(vec![Value::Int32(3), Value::Int32(6)]),
            ])
        );
    }

    #[test]
    fn cast_value_supports_text_input_array_targets() {
        assert_eq!(
            cast_value(
                Value::Text("{1,2,3}".into()),
                SqlType::array_of(SqlType::new(SqlTypeKind::Int4))
            )
            .unwrap(),
            Value::Array(vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)])
        );
        assert_eq!(
            cast_value(
                Value::Text("{\"a\",\"b\"}".into()),
                SqlType::array_of(SqlType::new(SqlTypeKind::Varchar))
            )
            .unwrap(),
            Value::Array(vec![Value::Text("a".into()), Value::Text("b".into())])
        );
    }

    #[test]
    fn soft_input_error_info_supports_catalog_backed_input_types() {
        assert!(
            soft_input_error_info("{\"a\":1}", "jsonb")
                .unwrap()
                .is_none()
        );
        assert!(soft_input_error_info("{\"a\":", "jsonb").unwrap().is_some());
        assert!(soft_input_error_info("$.a", "jsonpath").unwrap().is_none());
        assert!(
            soft_input_error_info("{1,2,3}", "int4[]")
                .unwrap()
                .is_none()
        );
        assert!(
            soft_input_error_info("{1,nope}", "int4[]")
                .unwrap()
                .is_some()
        );
    }
}
