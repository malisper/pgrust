use std::cmp::Ordering;

use num_bigint::BigInt;
use num_traits::{Signed, Zero};
use serde_json::{Error as SerdeJsonError, Map, Value as SerdeJsonValue};

use crate::backend::executor::ExecError;
use crate::backend::executor::exec_expr::format_array_text;
use crate::backend::executor::render_bit_text;
use crate::backend::executor::render_datetime_value_text;
use crate::backend::libpq::pqformat::format_bytea_text;
use crate::include::nodes::execnodes::{NumericValue, Value};
use crate::pgrust::compact_string::CompactString;
use crate::pgrust::session::ByteaOutputFormat;

const JENTRY_OFFLENMASK: u32 = 0x0FFF_FFFF;
const JENTRY_TYPEMASK: u32 = 0x7000_0000;
const JENTRY_HAS_OFF: u32 = 0x8000_0000;

const JENTRY_ISSTRING: u32 = 0x0000_0000;
const JENTRY_ISNUMERIC: u32 = 0x1000_0000;
const JENTRY_ISBOOL_FALSE: u32 = 0x2000_0000;
const JENTRY_ISBOOL_TRUE: u32 = 0x3000_0000;
const JENTRY_ISNULL: u32 = 0x4000_0000;
const JENTRY_ISCONTAINER: u32 = 0x5000_0000;

const JB_OFFSET_STRIDE: usize = 32;
const JB_CMASK: u32 = 0x0FFF_FFFF;
const JB_FSCALAR: u32 = 0x1000_0000;
const JB_FOBJECT: u32 = 0x2000_0000;
const JB_FARRAY: u32 = 0x4000_0000;

const NUMERIC_NEG: u16 = 0x4000;
const NUMERIC_SHORT: u16 = 0x8000;
const NUMERIC_SPECIAL: u16 = 0xC000;
const NUMERIC_NAN: u16 = 0xC000;
const NUMERIC_PINF: u16 = 0xD000;
const NUMERIC_NINF: u16 = 0xF000;
const NUMERIC_DSCALE_MASK: u16 = 0x3FFF;
const NUMERIC_SHORT_SIGN_MASK: u16 = 0x2000;
const NUMERIC_SHORT_DSCALE_MASK: u16 = 0x1F80;
const NUMERIC_SHORT_DSCALE_SHIFT: u16 = 7;
const NUMERIC_SHORT_WEIGHT_SIGN_MASK: u16 = 0x0040;
const NUMERIC_SHORT_WEIGHT_MASK: u16 = 0x003F;
const NUMERIC_SHORT_WEIGHT_MAX: i16 = NUMERIC_SHORT_WEIGHT_MASK as i16;
const NUMERIC_SHORT_WEIGHT_MIN: i16 = -((NUMERIC_SHORT_WEIGHT_MASK as i16) + 1);
const NUMERIC_SHORT_DSCALE_MAX: u16 = NUMERIC_SHORT_DSCALE_MASK >> NUMERIC_SHORT_DSCALE_SHIFT;
const NBASE: u16 = 10000;
const DEC_DIGITS: usize = 4;
const APPROX_STACK_BYTES_PER_JSON_LEVEL: u32 = 100;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum JsonbValue {
    Null,
    String(String),
    Numeric(NumericValue),
    Bool(bool),
    Array(Vec<JsonbValue>),
    Object(Vec<(String, JsonbValue)>),
}

impl JsonbValue {
    pub(crate) fn from_serde(value: SerdeJsonValue) -> Result<Self, ExecError> {
        Ok(match value {
            SerdeJsonValue::Null => JsonbValue::Null,
            SerdeJsonValue::Bool(v) => JsonbValue::Bool(v),
            SerdeJsonValue::Number(v) => {
                let text = v.to_string();
                if jsonb_numeric_text_overflows(&text)? {
                    return Err(ExecError::NumericFieldOverflow);
                }
                let numeric = crate::backend::executor::exec_expr::parse_numeric_text(&text)
                    .ok_or_else(|| ExecError::InvalidStorageValue {
                        column: "jsonb".into(),
                        details: format!("invalid input syntax for type jsonb: \"{text}\""),
                    })?;
                validate_jsonb_numeric_value(&numeric)?;
                JsonbValue::Numeric(numeric)
            }
            SerdeJsonValue::String(v) => JsonbValue::String(v),
            SerdeJsonValue::Array(items) => JsonbValue::Array(
                items
                    .into_iter()
                    .map(JsonbValue::from_serde)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            SerdeJsonValue::Object(map) => {
                let pairs = map
                    .into_iter()
                    .map(|(k, v)| Ok((k, JsonbValue::from_serde(v)?)))
                    .collect::<Result<Vec<_>, ExecError>>()?;
                JsonbValue::Object(canonicalize_object_pairs(pairs))
            }
        })
    }

    pub(crate) fn to_serde(&self) -> SerdeJsonValue {
        match self {
            JsonbValue::Null => SerdeJsonValue::Null,
            JsonbValue::Bool(v) => SerdeJsonValue::Bool(*v),
            JsonbValue::Numeric(v) => {
                serde_json::from_str(&v.render()).unwrap_or(SerdeJsonValue::Null)
            }
            JsonbValue::String(v) => SerdeJsonValue::String(v.clone()),
            JsonbValue::Array(items) => {
                SerdeJsonValue::Array(items.iter().map(JsonbValue::to_serde).collect())
            }
            JsonbValue::Object(items) => {
                let mut map = Map::new();
                for (key, value) in items {
                    map.insert(key.clone(), value.to_serde());
                }
                SerdeJsonValue::Object(map)
            }
        }
    }

    pub(crate) fn render(&self) -> String {
        let mut out = String::new();
        render_jsonb_value(&mut out, self);
        out
    }
}

pub(crate) fn parse_jsonb_text(text: &str) -> Result<Vec<u8>, ExecError> {
    let value = parse_json_text_input_with_stack_limit(text, 100)?;
    Ok(encode_jsonb(&JsonbValue::from_serde(value)?))
}

pub(crate) fn parse_jsonb_text_with_limit(
    text: &str,
    max_stack_depth_kb: u32,
) -> Result<Vec<u8>, ExecError> {
    let value = parse_json_text_input_with_stack_limit(text, max_stack_depth_kb)?;
    Ok(encode_jsonb(&JsonbValue::from_serde(value)?))
}

pub(crate) fn parse_json_text_input(text: &str) -> Result<SerdeJsonValue, ExecError> {
    serde_json::from_str::<SerdeJsonValue>(text).map_err(|err| json_input_error(text, err))
}

fn parse_json_text_input_with_stack_limit(
    text: &str,
    max_stack_depth_kb: u32,
) -> Result<SerdeJsonValue, ExecError> {
    enforce_json_stack_limit(text, max_stack_depth_kb)?;
    parse_json_text_input(text)
}

fn enforce_json_stack_limit(text: &str, max_stack_depth_kb: u32) -> Result<(), ExecError> {
    let max_depth = max_stack_depth_kb
        .saturating_mul(1024)
        .checked_div(APPROX_STACK_BYTES_PER_JSON_LEVEL)
        .unwrap_or(u32::MAX)
        .max(1);
    let mut depth = 0_u32;
    let mut in_string = false;
    let mut escaping = false;

    for ch in text.chars() {
        if in_string {
            if escaping {
                escaping = false;
                continue;
            }
            match ch {
                '\\' => escaping = true,
                '"' => in_string = false,
                _ => {}
            }
            continue;
        }

        match ch {
            '"' => in_string = true,
            '[' | '{' => {
                depth = depth.saturating_add(1);
                if depth > max_depth {
                    return Err(stack_depth_limit_error(max_stack_depth_kb));
                }
            }
            ']' | '}' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }

    Ok(())
}

fn stack_depth_limit_error(max_stack_depth_kb: u32) -> ExecError {
    ExecError::DetailedError {
        message: "stack depth limit exceeded".into(),
        detail: None,
        hint: Some(format!(
            "Increase the configuration parameter \"max_stack_depth\" (currently {max_stack_depth_kb}kB), after ensuring the platform's stack depth limit is adequate."
        )),
        sqlstate: "54001",
    }
}

pub(crate) fn json_input_error(text: &str, err: SerdeJsonError) -> ExecError {
    let (detail, context) = match diagnose_json_input(text) {
        Some(diag) => (Some(diag.detail), Some(diag.context)),
        None => {
            let line = err.line();
            let column = err.column();
            let suffix = format!(" at line {line} column {column}");
            let rendered = err.to_string();
            let detail = match err.classify() {
                serde_json::error::Category::Io => None,
                serde_json::error::Category::Eof => {
                    Some("The input string ended unexpectedly.".into())
                }
                _ => Some(
                    rendered
                        .strip_suffix(&suffix)
                        .unwrap_or(rendered.as_str())
                        .to_string(),
                ),
            };
            (detail, json_error_context(text, line, column))
        }
    };
    ExecError::JsonInput {
        raw_input: text.to_string(),
        message: "invalid input syntax for type json".into(),
        detail,
        context,
        sqlstate: "22P02",
    }
}

fn json_error_context(text: &str, line: usize, column: usize) -> Option<String> {
    let line_text = text.lines().nth(line.saturating_sub(1))?;
    let snippet_start = column.saturating_sub(1).saturating_sub(15);
    let mut snippet: String = line_text.chars().skip(snippet_start).take(40).collect();
    if snippet_start > 0 {
        snippet.insert_str(0, "...");
    }
    if line_text.chars().skip(snippet_start).count() > 40 {
        snippet.push_str("...");
    }
    Some(format!("JSON data, line {line}: {snippet}"))
}

#[derive(Debug)]
struct JsonInputDiagnostic {
    detail: String,
    context: String,
}

fn diagnose_json_input(text: &str) -> Option<JsonInputDiagnostic> {
    JsonDiagnosticParser::new(text).parse().err()
}

struct JsonDiagnosticParser<'a> {
    text: &'a str,
    chars: Vec<char>,
    pos: usize,
}

impl<'a> JsonDiagnosticParser<'a> {
    fn new(text: &'a str) -> Self {
        Self {
            text,
            chars: text.chars().collect(),
            pos: 0,
        }
    }

    fn parse(mut self) -> Result<(), JsonInputDiagnostic> {
        self.skip_ws();
        self.parse_value()?;
        self.skip_ws();
        if self.pos == self.chars.len() {
            return Ok(());
        }
        Err(self.error_expected_end_of_input())
    }

    fn parse_value(&mut self) -> Result<(), JsonInputDiagnostic> {
        self.skip_ws();
        let Some(ch) = self.peek() else {
            return Err(self.error_unexpected_end());
        };
        match ch {
            '"' => self.parse_string(),
            '[' => self.parse_array(),
            '{' => self.parse_object(),
            '-' | '0'..='9' => self.parse_number(),
            't' => self.parse_literal("true"),
            'f' => self.parse_literal("false"),
            'n' => self.parse_literal("null"),
            _ => Err(self.error_expected_json_value()),
        }
    }

    fn parse_array(&mut self) -> Result<(), JsonInputDiagnostic> {
        self.bump();
        self.skip_ws();
        if self.peek() == Some(']') {
            self.bump();
            return Ok(());
        }
        loop {
            self.parse_value()?;
            self.skip_ws();
            match self.peek() {
                Some(',') => {
                    self.bump();
                    self.skip_ws();
                    if self.peek() == Some(']') {
                        return Err(self.error_expected_json_value());
                    }
                }
                Some(']') => {
                    self.bump();
                    return Ok(());
                }
                None => return Err(self.error_unexpected_end()),
                Some(_) => return Err(self.error_expected_one_of(&[",", "]"])),
            }
        }
    }

    fn parse_object(&mut self) -> Result<(), JsonInputDiagnostic> {
        self.bump();
        self.skip_ws();
        let mut allow_object_end = true;
        loop {
            match self.peek() {
                Some('"') => self.parse_string()?,
                Some('}') if allow_object_end => {
                    self.bump();
                    return Ok(());
                }
                None => return Err(self.error_unexpected_end()),
                Some(_) if allow_object_end => {
                    return Err(self.error_expected_one_of(&["string", "}"]));
                }
                Some(_) => return Err(self.error_expected_one_of(&["string"])),
            }
            self.skip_ws();
            match self.peek() {
                Some(':') => self.bump(),
                Some(',') | Some('}') => return Err(self.error_expected_found(":")),
                Some('=') => return Err(self.error_invalid_token()),
                Some(_) => return Err(self.error_invalid_token()),
                None => return Err(self.error_unexpected_end()),
            }
            self.skip_ws();
            self.parse_value()?;
            self.skip_ws();
            match self.peek() {
                Some(',') => {
                    self.bump();
                    self.skip_ws();
                    allow_object_end = false;
                }
                Some('}') => {
                    self.bump();
                    return Ok(());
                }
                None => return Err(self.error_unexpected_end()),
                Some(_) => return Err(self.error_expected_one_of(&[",", "}"])),
            }
        }
    }

    fn parse_string(&mut self) -> Result<(), JsonInputDiagnostic> {
        let start = self.pos;
        self.bump();
        while let Some(ch) = self.peek() {
            match ch {
                '"' => {
                    self.bump();
                    return Ok(());
                }
                '\\' => {
                    self.bump();
                    let Some(escaped) = self.peek() else {
                        return Err(self.error_token_invalid_range(start, self.pos));
                    };
                    match escaped {
                        '"' | '\\' | '/' | 'b' | 'f' | 'n' | 'r' | 't' => {
                            self.bump();
                        }
                        'u' => {
                            self.bump();
                            for _ in 0..4 {
                                let Some(hex) = self.peek() else {
                                    return Err(self.error_invalid_escape('u'));
                                };
                                if !hex.is_ascii_hexdigit() {
                                    return Err(self.error_invalid_escape('u'));
                                }
                                self.bump();
                            }
                        }
                        other => return Err(self.error_invalid_escape(other)),
                    }
                }
                ch if ch.is_control() => return Err(self.error_unescaped_control(ch)),
                _ => self.bump(),
            }
        }
        Err(self.error_token_invalid_range(start, self.pos))
    }

    fn parse_number(&mut self) -> Result<(), JsonInputDiagnostic> {
        let start = self.pos;
        if self.peek() == Some('-') {
            self.bump();
        }
        match self.peek() {
            Some('0') => {
                self.bump();
                if matches!(self.peek(), Some('0'..='9')) {
                    self.consume_token_tail();
                    return Err(self.error_token_invalid_range(start, self.pos));
                }
            }
            Some('1'..='9') => {
                self.bump();
                while matches!(self.peek(), Some('0'..='9')) {
                    self.bump();
                }
            }
            _ => return Err(self.error_token_invalid_range(start, self.pos)),
        }
        if self.peek() == Some('.') {
            self.bump();
            if !matches!(self.peek(), Some('0'..='9')) {
                self.consume_token_tail();
                return Err(self.error_token_invalid_range(start, self.pos));
            }
            while matches!(self.peek(), Some('0'..='9')) {
                self.bump();
            }
        }
        if matches!(self.peek(), Some('e' | 'E')) {
            self.bump();
            if matches!(self.peek(), Some('+' | '-')) {
                self.bump();
            }
            if !matches!(self.peek(), Some('0'..='9')) {
                self.consume_token_tail();
                return Err(self.error_token_invalid_range(start, self.pos));
            }
            while matches!(self.peek(), Some('0'..='9')) {
                self.bump();
            }
        }
        if self.peek().is_some_and(|ch| !is_json_delimiter(ch)) {
            self.consume_token_tail();
            return Err(self.error_token_invalid_range(start, self.pos));
        }
        Ok(())
    }

    fn parse_literal(&mut self, expected: &str) -> Result<(), JsonInputDiagnostic> {
        let start = self.pos;
        for expected_ch in expected.chars() {
            if self.peek() == Some(expected_ch) {
                self.bump();
            } else {
                self.consume_token_tail();
                return Err(self.error_token_invalid_range(start, self.pos));
            }
        }
        if self.peek().is_some_and(|ch| !is_json_delimiter(ch)) {
            self.consume_token_tail();
            return Err(self.error_token_invalid_range(start, self.pos));
        }
        Ok(())
    }

    fn error_unexpected_end(&self) -> JsonInputDiagnostic {
        self.error_with_position("The input string ended unexpectedly.".into(), self.pos)
    }

    fn error_invalid_escape(&self, escaped: char) -> JsonInputDiagnostic {
        self.error_with_position(
            format!("Escape sequence \"\\{escaped}\" is invalid."),
            self.pos.saturating_sub(1),
        )
    }

    fn error_unescaped_control(&self, ch: char) -> JsonInputDiagnostic {
        self.error_with_position(
            format!("Character with value 0x{:02x} must be escaped.", ch as u32),
            self.pos,
        )
    }

    fn error_expected_json_value(&self) -> JsonInputDiagnostic {
        if self
            .peek()
            .is_some_and(|ch| !matches!(ch, ':' | ',' | ']' | '}'))
        {
            return self.error_invalid_token();
        }
        self.error_expected_found("JSON value")
    }

    fn error_expected_end_of_input(&self) -> JsonInputDiagnostic {
        self.error_expected_found("end of input")
    }

    fn error_expected_found(&self, expected: &str) -> JsonInputDiagnostic {
        let token = self.current_token();
        let rendered = if expected == "end of input" {
            format!("Expected end of input, but found {token}.")
        } else if expected == "JSON value" {
            format!("Expected JSON value, but found {token}.")
        } else {
            format!("Expected \"{expected}\", but found {token}.")
        };
        self.error_with_position(rendered, self.pos)
    }

    fn error_expected_one_of(&self, expected: &[&str]) -> JsonInputDiagnostic {
        let rendered_expected = match expected {
            [single] => render_json_expected(single),
            [left, right] => format!(
                "{} or {}",
                render_json_expected(left),
                render_json_expected(right)
            ),
            _ => expected
                .iter()
                .map(|item| render_json_expected(item))
                .collect::<Vec<_>>()
                .join(", "),
        };
        let token = self.current_token();
        self.error_with_position(
            format!("Expected {rendered_expected}, but found {token}."),
            self.pos,
        )
    }

    fn error_invalid_token(&self) -> JsonInputDiagnostic {
        self.error_token_invalid_range(self.pos, self.token_end(self.pos))
    }

    fn error_token_invalid_range(&self, start: usize, end: usize) -> JsonInputDiagnostic {
        let token = self.chars[start..end].iter().collect::<String>();
        self.error_with_position(format!("Token \"{token}\" is invalid."), start)
    }

    fn error_with_position(&self, detail: String, pos: usize) -> JsonInputDiagnostic {
        let (line, column) = self.line_col_at(pos);
        JsonInputDiagnostic {
            detail,
            context: json_error_context(self.text, line, column)
                .unwrap_or_else(|| format!("JSON data, line {line}: ")),
        }
    }

    fn current_token(&self) -> String {
        match self.peek() {
            Some(ch) if is_json_delimiter(ch) && !ch.is_whitespace() => format!("\"{ch}\""),
            Some(_) => {
                let end = self.token_end(self.pos);
                let token = self.chars[self.pos..end].iter().collect::<String>();
                format!("\"{token}\"")
            }
            None => "end of input".into(),
        }
    }

    fn token_end(&self, start: usize) -> usize {
        if let Some(ch) = self.chars.get(start) {
            if *ch == '\'' || (!ch.is_ascii_alphanumeric() && !matches!(ch, '"' | '-')) {
                return start.saturating_add(1).min(self.chars.len());
            }
        }
        let mut end = start;
        while let Some(ch) = self.chars.get(end) {
            if is_json_delimiter(*ch) {
                break;
            }
            end += 1;
        }
        end.max(start.saturating_add(1).min(self.chars.len()))
    }

    fn consume_token_tail(&mut self) {
        while self.peek().is_some_and(|ch| !is_json_delimiter(ch)) {
            self.bump();
        }
    }

    fn skip_ws(&mut self) {
        while self.peek().is_some_and(|ch| ch.is_whitespace()) {
            self.bump();
        }
    }

    fn line_col_at(&self, pos: usize) -> (usize, usize) {
        let mut line = 1usize;
        let mut column = 1usize;
        for ch in self.chars.iter().take(pos) {
            if *ch == '\n' {
                line += 1;
                column = 1;
            } else {
                column += 1;
            }
        }
        (line, column)
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn bump(&mut self) {
        self.pos += 1;
    }
}

fn is_json_delimiter(ch: char) -> bool {
    ch.is_whitespace() || matches!(ch, ',' | ':' | ']' | '[' | '}' | '{')
}

fn render_json_expected(expected: &str) -> String {
    if expected == "string" {
        expected.to_string()
    } else {
        format!("\"{expected}\"")
    }
}

pub(crate) fn render_jsonb_bytes(bytes: &[u8]) -> Result<String, ExecError> {
    Ok(decode_jsonb(bytes)?.render())
}

pub(crate) fn decode_jsonb(bytes: &[u8]) -> Result<JsonbValue, ExecError> {
    let header = read_u32(bytes, 0)?;
    if header & JB_FARRAY == 0 && header & JB_FOBJECT == 0 {
        return Err(corrupt_jsonb());
    }
    let decoded = decode_container(bytes, 0, 0, header)?;
    if header & JB_FSCALAR != 0 {
        match decoded {
            JsonbValue::Array(mut items) if items.len() == 1 => Ok(items.remove(0)),
            _ => Err(corrupt_jsonb()),
        }
    } else {
        Ok(decoded)
    }
}

pub(crate) fn encode_jsonb(value: &JsonbValue) -> Vec<u8> {
    let mut out = Vec::new();
    let mut meta = 0u32;
    encode_jsonb_value(&mut out, &mut meta, value, 0, true);
    out
}

pub(crate) fn jsonb_from_value(value: &Value) -> Result<JsonbValue, ExecError> {
    Ok(match value {
        Value::Null => JsonbValue::Null,
        Value::Int16(v) => JsonbValue::Numeric(NumericValue::from_i64(*v as i64)),
        Value::Int32(v) => JsonbValue::Numeric(NumericValue::from_i64(*v as i64)),
        Value::Int64(v) => JsonbValue::Numeric(NumericValue::from_i64(*v)),
        Value::Money(v) => JsonbValue::String(crate::backend::executor::money_format_text(*v)),
        Value::Float64(v) => JsonbValue::Numeric({
            let numeric = crate::backend::executor::exec_expr::parse_numeric_text(&v.to_string())
                .ok_or_else(|| ExecError::InvalidNumericInput(v.to_string()))?;
            validate_jsonb_numeric_value(&numeric)?;
            numeric
        }),
        Value::Numeric(v) => {
            validate_jsonb_numeric_value(v)?;
            JsonbValue::Numeric(v.clone())
        }
        Value::Bool(v) => JsonbValue::Bool(*v),
        Value::Bit(v) => JsonbValue::String(render_bit_text(v)),
        Value::JsonPath(text) => JsonbValue::String(text.to_string()),
        Value::Text(text) => JsonbValue::String(text.to_string()),
        Value::TextRef(_, _) => JsonbValue::String(value.as_text().unwrap().to_string()),
        Value::Bytea(bytes) => JsonbValue::String(format_bytea_text(bytes, ByteaOutputFormat::Hex)),
        Value::InternalChar(v) => {
            JsonbValue::String(crate::backend::executor::render_internal_char_text(*v))
        }
        Value::Date(_)
        | Value::Time(_)
        | Value::TimeTz(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_) => {
            JsonbValue::String(render_datetime_value_text(value).expect("datetime values render"))
        }
        Value::Point(_)
        | Value::Lseg(_)
        | Value::Path(_)
        | Value::Line(_)
        | Value::Box(_)
        | Value::Polygon(_)
        | Value::Circle(_) => JsonbValue::String(
            crate::backend::executor::render_geometry_text(value, Default::default())
                .unwrap_or_default(),
        ),
        Value::Range(_) => JsonbValue::String(
            crate::backend::executor::render_range_text(value).unwrap_or_default(),
        ),
        Value::TsVector(v) => JsonbValue::String(crate::backend::executor::render_tsvector_text(v)),
        Value::TsQuery(v) => JsonbValue::String(crate::backend::executor::render_tsquery_text(v)),
        Value::Json(text) => JsonbValue::from_serde(parse_json_text_input(text.as_str())?)?,
        Value::Jsonb(bytes) => decode_jsonb(bytes)?,
        Value::Record(record) => JsonbValue::Object(
            record
                .iter()
                .map(|(field, value)| Ok((field.name.clone(), jsonb_from_value(value)?)))
                .collect::<Result<Vec<_>, ExecError>>()?,
        ),
        Value::Array(items) => JsonbValue::Array(
            items
                .iter()
                .map(jsonb_from_value)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Value::PgArray(array) => JsonbValue::Array(
            array
                .to_nested_values()
                .iter()
                .map(jsonb_from_value)
                .collect::<Result<Vec<_>, _>>()?,
        ),
    })
}

pub(crate) fn jsonb_to_value(value: &JsonbValue) -> Value {
    Value::Jsonb(encode_jsonb(value))
}

pub(crate) fn jsonb_to_text_value(value: &JsonbValue) -> Value {
    match value {
        JsonbValue::Null => Value::Null,
        JsonbValue::String(text) => Value::Text(CompactString::from_owned(text.clone())),
        other => Value::Text(CompactString::from_owned(other.render())),
    }
}

pub(crate) fn compare_jsonb(left: &JsonbValue, right: &JsonbValue) -> Ordering {
    let left_rank = jsonb_type_rank(left);
    let right_rank = jsonb_type_rank(right);
    if left_rank != right_rank {
        return left_rank.cmp(&right_rank);
    }
    match (left, right) {
        (JsonbValue::Null, JsonbValue::Null) => Ordering::Equal,
        (JsonbValue::String(l), JsonbValue::String(r)) => l.cmp(r),
        (JsonbValue::Numeric(l), JsonbValue::Numeric(r)) => l.cmp(r),
        (JsonbValue::Bool(l), JsonbValue::Bool(r)) => l.cmp(r),
        (JsonbValue::Array(l), JsonbValue::Array(r)) => {
            let len_cmp = l.len().cmp(&r.len());
            if len_cmp != Ordering::Equal {
                return len_cmp;
            }
            for (lv, rv) in l.iter().zip(r.iter()) {
                let cmp = compare_jsonb(lv, rv);
                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            Ordering::Equal
        }
        (JsonbValue::Object(l), JsonbValue::Object(r)) => {
            let len_cmp = l.len().cmp(&r.len());
            if len_cmp != Ordering::Equal {
                return len_cmp;
            }
            for ((lk, lv), (rk, rv)) in l.iter().zip(r.iter()) {
                let key_cmp = lk.cmp(rk);
                if key_cmp != Ordering::Equal {
                    return key_cmp;
                }
                let val_cmp = compare_jsonb(lv, rv);
                if val_cmp != Ordering::Equal {
                    return val_cmp;
                }
            }
            Ordering::Equal
        }
        _ => Ordering::Equal,
    }
}

pub(crate) fn jsonb_get<'a>(
    value: &'a JsonbValue,
    key: &Value,
) -> Result<Option<&'a JsonbValue>, ExecError> {
    Ok(match key {
        Value::Text(_) | Value::TextRef(_, _) => match value {
            JsonbValue::Object(items) => {
                let name = key.as_text().unwrap();
                items.iter().find(|(k, _)| k == name).map(|(_, v)| v)
            }
            _ => None,
        },
        Value::Int16(index) => jsonb_get_index(value, *index as i32),
        Value::Int32(index) => jsonb_get_index(value, *index),
        Value::Int64(index) => i32::try_from(*index)
            .ok()
            .and_then(|idx| jsonb_get_index(value, idx)),
        other => {
            return Err(ExecError::TypeMismatch {
                op: "jsonb ->",
                left: jsonb_to_value(value),
                right: other.clone(),
            });
        }
    })
}

pub(crate) fn jsonb_path<'a>(value: &'a JsonbValue, path: &[String]) -> Option<&'a JsonbValue> {
    let mut current = value;
    for step in path {
        current = match current {
            JsonbValue::Object(items) => items.iter().find(|(k, _)| k == step).map(|(_, v)| v)?,
            JsonbValue::Array(_) => {
                let idx = step.parse::<i32>().ok()?;
                jsonb_get_index(current, idx)?
            }
            _ => return None,
        };
    }
    Some(current)
}

pub(crate) fn jsonb_contains(left: &JsonbValue, right: &JsonbValue) -> bool {
    match (left, right) {
        (
            _,
            JsonbValue::Null | JsonbValue::String(_) | JsonbValue::Numeric(_) | JsonbValue::Bool(_),
        ) => {
            if let JsonbValue::Array(items) = left {
                items.iter().any(|item| jsonb_contains(item, right))
            } else {
                compare_jsonb(left, right) == Ordering::Equal
            }
        }
        (JsonbValue::Object(left_items), JsonbValue::Object(right_items)) => {
            right_items.iter().all(|(rk, rv)| {
                left_items
                    .iter()
                    .find(|(lk, _)| lk == rk)
                    .map(|(_, lv)| jsonb_contains(lv, rv))
                    .unwrap_or(false)
            })
        }
        (JsonbValue::Array(left_items), JsonbValue::Array(right_items)) => {
            right_items.iter().all(|right_item| {
                left_items
                    .iter()
                    .any(|left_item| jsonb_contains(left_item, right_item))
            })
        }
        _ => false,
    }
}

pub(crate) fn jsonb_concat(left: &JsonbValue, right: &JsonbValue) -> JsonbValue {
    match (left, right) {
        (JsonbValue::Object(left_items), JsonbValue::Object(right_items)) => {
            let mut merged = left_items.clone();
            merged.extend(right_items.iter().cloned());
            JsonbValue::Object(canonicalize_object_pairs(merged))
        }
        (JsonbValue::Array(left_items), JsonbValue::Array(right_items)) => {
            let mut items = left_items.clone();
            items.extend(right_items.iter().cloned());
            JsonbValue::Array(items)
        }
        _ => {
            let mut items = match left {
                JsonbValue::Array(items) => items.clone(),
                other => vec![other.clone()],
            };
            match right {
                JsonbValue::Array(right_items) => items.extend(right_items.iter().cloned()),
                other => items.push(other.clone()),
            }
            JsonbValue::Array(items)
        }
    }
}

pub(crate) fn jsonb_exists(value: &JsonbValue, key: &str) -> bool {
    match value {
        JsonbValue::Object(items) => items.iter().any(|(k, _)| k == key),
        JsonbValue::Array(items) => items
            .iter()
            .any(|item| matches!(item, JsonbValue::String(text) if text == key)),
        _ => false,
    }
}

pub(crate) fn jsonb_exists_any(value: &JsonbValue, keys: &[String]) -> bool {
    keys.iter().any(|key| jsonb_exists(value, key))
}

pub(crate) fn jsonb_exists_all(value: &JsonbValue, keys: &[String]) -> bool {
    keys.iter().all(|key| jsonb_exists(value, key))
}

pub(crate) fn jsonb_object_from_pairs(pairs: &[(String, Value)]) -> Result<JsonbValue, ExecError> {
    let items = pairs
        .iter()
        .map(|(k, v)| Ok((k.clone(), jsonb_from_value(v)?)))
        .collect::<Result<Vec<_>, ExecError>>()?;
    Ok(JsonbValue::Object(canonicalize_object_pairs(items)))
}

pub(crate) fn jsonb_builder_key(value: &Value) -> Result<String, ExecError> {
    match value {
        Value::Null => Err(ExecError::TypeMismatch {
            op: "jsonb_build_object key",
            left: Value::Null,
            right: Value::Text("non-null key".into()),
        }),
        Value::Int16(v) => Ok(v.to_string()),
        Value::Int32(v) => Ok(v.to_string()),
        Value::Int64(v) => Ok(v.to_string()),
        Value::Money(v) => Ok(crate::backend::executor::money_format_text(*v)),
        Value::Float64(v) => Ok(v.to_string()),
        Value::Numeric(v) => Ok(v.render()),
        Value::Bool(v) => Ok(if *v { "true".into() } else { "false".into() }),
        Value::Bit(v) => Ok(render_bit_text(v)),
        Value::Text(text) => Ok(text.to_string()),
        Value::TextRef(_, _) => Ok(value.as_text().unwrap().to_string()),
        Value::Bytea(bytes) => Ok(format_bytea_text(bytes, ByteaOutputFormat::Hex)),
        Value::InternalChar(v) => Ok(crate::backend::executor::render_internal_char_text(*v)),
        Value::JsonPath(text) => Ok(text.to_string()),
        Value::Json(text) => Ok(text.to_string()),
        Value::Jsonb(bytes) => render_jsonb_bytes(bytes),
        Value::Date(_)
        | Value::Time(_)
        | Value::TimeTz(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_) => {
            Ok(render_datetime_value_text(value).expect("datetime values render"))
        }
        Value::Point(_)
        | Value::Lseg(_)
        | Value::Path(_)
        | Value::Line(_)
        | Value::Box(_)
        | Value::Polygon(_)
        | Value::Circle(_) => Ok(crate::backend::executor::render_geometry_text(
            value,
            Default::default(),
        )
        .unwrap_or_default()),
        Value::Range(_) => {
            Ok(crate::backend::executor::render_range_text(value).unwrap_or_default())
        }
        Value::TsVector(v) => Ok(crate::backend::executor::render_tsvector_text(v)),
        Value::TsQuery(v) => Ok(crate::backend::executor::render_tsquery_text(v)),
        Value::Array(items) => Ok(format_array_text(items)),
        Value::PgArray(array) => Ok(crate::backend::executor::value_io::format_array_value_text(
            array,
        )),
        Value::Record(record) => Ok(JsonbValue::Object(
            record
                .iter()
                .map(|(field, value)| Ok((field.name.clone(), jsonb_from_value(value)?)))
                .collect::<Result<Vec<_>, ExecError>>()?,
        )
        .to_serde()
        .to_string()),
    }
}

fn encode_jsonb_value(
    out: &mut Vec<u8>,
    header: &mut u32,
    value: &JsonbValue,
    level: usize,
    is_root: bool,
) {
    match value {
        JsonbValue::Null | JsonbValue::String(_) | JsonbValue::Numeric(_) | JsonbValue::Bool(_) => {
            if is_root {
                encode_jsonb_array(out, header, std::slice::from_ref(value), level, true);
            } else {
                encode_jsonb_scalar(out, header, value);
            }
        }
        JsonbValue::Array(items) => encode_jsonb_array(out, header, items, level, false),
        JsonbValue::Object(items) => encode_jsonb_object(out, header, items, level),
    }
}

fn encode_jsonb_array(
    out: &mut Vec<u8>,
    header: &mut u32,
    items: &[JsonbValue],
    level: usize,
    raw_scalar: bool,
) {
    let base_offset = out.len();
    pad_to_int(out);
    let mut container_header = items.len() as u32 | JB_FARRAY;
    if raw_scalar {
        debug_assert_eq!(items.len(), 1);
        debug_assert_eq!(level, 0);
        container_header |= JB_FSCALAR;
    }
    push_u32(out, container_header);
    let jentry_offset = reserve(out, items.len() * 4);
    let mut total_data_len = 0usize;

    for (i, item) in items.iter().enumerate() {
        let mut meta = 0u32;
        encode_jsonb_value(out, &mut meta, item, level + 1, false);
        let len = jentry_len(meta) as usize;
        total_data_len += len;
        if total_data_len > JENTRY_OFFLENMASK as usize {
            panic!("jsonb array elements exceed maximum size");
        }
        if i % JB_OFFSET_STRIDE == 0 {
            meta = (meta & JENTRY_TYPEMASK) | total_data_len as u32 | JENTRY_HAS_OFF;
        }
        write_u32(out, jentry_offset + i * 4, meta);
    }

    let total_len = out.len() - base_offset;
    if total_len > JENTRY_OFFLENMASK as usize {
        panic!("jsonb array exceeds maximum size");
    }
    *header = JENTRY_ISCONTAINER | total_len as u32;
}

fn encode_jsonb_object(
    out: &mut Vec<u8>,
    header: &mut u32,
    items: &[(String, JsonbValue)],
    level: usize,
) {
    let items = canonicalize_object_pairs(items.to_vec());
    let base_offset = out.len();
    pad_to_int(out);
    push_u32(out, items.len() as u32 | JB_FOBJECT);
    let jentry_offset = reserve(out, items.len() * 8);
    let mut total_data_len = 0usize;

    for (i, (key, _)) in items.iter().enumerate() {
        let mut meta = 0u32;
        encode_jsonb_scalar(out, &mut meta, &JsonbValue::String(key.clone()));
        let len = jentry_len(meta) as usize;
        total_data_len += len;
        if total_data_len > JENTRY_OFFLENMASK as usize {
            panic!("jsonb object elements exceed maximum size");
        }
        if i % JB_OFFSET_STRIDE == 0 {
            meta = (meta & JENTRY_TYPEMASK) | total_data_len as u32 | JENTRY_HAS_OFF;
        }
        write_u32(out, jentry_offset + i * 4, meta);
    }

    for (i, (_, value)) in items.iter().enumerate() {
        let mut meta = 0u32;
        encode_jsonb_value(out, &mut meta, value, level + 1, false);
        let len = jentry_len(meta) as usize;
        total_data_len += len;
        if total_data_len > JENTRY_OFFLENMASK as usize {
            panic!("jsonb object elements exceed maximum size");
        }
        if (i + items.len()) % JB_OFFSET_STRIDE == 0 {
            meta = (meta & JENTRY_TYPEMASK) | total_data_len as u32 | JENTRY_HAS_OFF;
        }
        write_u32(out, jentry_offset + (i + items.len()) * 4, meta);
    }

    let total_len = out.len() - base_offset;
    if total_len > JENTRY_OFFLENMASK as usize {
        panic!("jsonb object exceeds maximum size");
    }
    *header = JENTRY_ISCONTAINER | total_len as u32;
}

fn encode_jsonb_scalar(out: &mut Vec<u8>, header: &mut u32, value: &JsonbValue) {
    match value {
        JsonbValue::Null => *header = JENTRY_ISNULL,
        JsonbValue::Bool(false) => *header = JENTRY_ISBOOL_FALSE,
        JsonbValue::Bool(true) => *header = JENTRY_ISBOOL_TRUE,
        JsonbValue::String(text) => {
            out.extend_from_slice(text.as_bytes());
            *header = JENTRY_ISSTRING | text.len() as u32;
        }
        JsonbValue::Numeric(numeric) => {
            let pad = pad_to_int(out);
            let bytes = encode_pg_numeric(numeric);
            out.extend_from_slice(&bytes);
            *header = JENTRY_ISNUMERIC | (pad + bytes.len()) as u32;
        }
        JsonbValue::Array(_) | JsonbValue::Object(_) => unreachable!(),
    }
}

fn decode_container(
    bytes: &[u8],
    start: usize,
    data_offset: usize,
    header: u32,
) -> Result<JsonbValue, ExecError> {
    let container_start = if start == 0 {
        0
    } else {
        align4(start + data_offset)
    };
    if container_start + 4 > bytes.len() {
        return Err(corrupt_jsonb());
    }
    let count = (header & JB_CMASK) as usize;
    let is_object = header & JB_FOBJECT != 0;
    let is_array = header & JB_FARRAY != 0;
    let is_scalar = header & JB_FSCALAR != 0;
    if !is_object && !is_array {
        return Err(corrupt_jsonb());
    }

    let jentry_count = if is_object { count * 2 } else { count };
    let jentry_start = container_start + 4;
    let data_base = jentry_start + jentry_count * 4;
    if data_base > bytes.len() {
        return Err(corrupt_jsonb());
    }

    let mut offsets = Vec::with_capacity(jentry_count);
    let mut current = 0usize;
    for i in 0..jentry_count {
        let meta = read_u32(bytes, jentry_start + i * 4)?;
        let len = jentry_len(meta) as usize;
        let end = if meta & JENTRY_HAS_OFF != 0 {
            len
        } else {
            current.checked_add(len).ok_or_else(corrupt_jsonb)?
        };
        offsets.push((meta, current, end));
        current = end;
    }

    if is_object {
        let mut items = Vec::with_capacity(count);
        for i in 0..count {
            let key =
                decode_jsonb_string(bytes, data_base, offsets[i].1, offsets[i].2, offsets[i].0)?;
            let value = decode_jsonb_entry(
                bytes,
                data_base,
                offsets[count + i].1,
                offsets[count + i].2,
                offsets[count + i].0,
            )?;
            items.push((key, value));
        }
        Ok(JsonbValue::Object(items))
    } else {
        let mut items = Vec::with_capacity(count);
        for (meta, start_off, end_off) in offsets {
            items.push(decode_jsonb_entry(
                bytes, data_base, start_off, end_off, meta,
            )?);
        }
        if is_scalar && items.len() == 1 {
            Ok(JsonbValue::Array(items))
        } else {
            Ok(JsonbValue::Array(items))
        }
    }
}

fn decode_jsonb_entry(
    bytes: &[u8],
    data_base: usize,
    start_off: usize,
    end_off: usize,
    meta: u32,
) -> Result<JsonbValue, ExecError> {
    let ty = meta & JENTRY_TYPEMASK;
    let raw_start = data_base.checked_add(start_off).ok_or_else(corrupt_jsonb)?;
    let raw_end = data_base.checked_add(end_off).ok_or_else(corrupt_jsonb)?;
    if raw_end > bytes.len() || raw_start > raw_end {
        return Err(corrupt_jsonb());
    }
    match ty {
        JENTRY_ISNULL => Ok(JsonbValue::Null),
        JENTRY_ISBOOL_FALSE => Ok(JsonbValue::Bool(false)),
        JENTRY_ISBOOL_TRUE => Ok(JsonbValue::Bool(true)),
        JENTRY_ISSTRING => {
            let text =
                std::str::from_utf8(&bytes[raw_start..raw_end]).map_err(|_| corrupt_jsonb())?;
            Ok(JsonbValue::String(text.to_string()))
        }
        JENTRY_ISNUMERIC => {
            let aligned = align4(raw_start);
            if aligned > raw_end {
                return Err(corrupt_jsonb());
            }
            Ok(JsonbValue::Numeric(decode_pg_numeric(
                &bytes[aligned..raw_end],
            )?))
        }
        JENTRY_ISCONTAINER => {
            let aligned = align4(raw_start);
            if aligned > raw_end {
                return Err(corrupt_jsonb());
            }
            let header = read_u32(bytes, aligned)?;
            decode_container(bytes, aligned, 0, header)
        }
        _ => Err(corrupt_jsonb()),
    }
}

fn decode_jsonb_string(
    bytes: &[u8],
    data_base: usize,
    start_off: usize,
    end_off: usize,
    meta: u32,
) -> Result<String, ExecError> {
    if meta & JENTRY_TYPEMASK != JENTRY_ISSTRING {
        return Err(corrupt_jsonb());
    }
    let start = data_base.checked_add(start_off).ok_or_else(corrupt_jsonb)?;
    let end = data_base.checked_add(end_off).ok_or_else(corrupt_jsonb)?;
    if end > bytes.len() || start > end {
        return Err(corrupt_jsonb());
    }
    Ok(std::str::from_utf8(&bytes[start..end])
        .map_err(|_| corrupt_jsonb())?
        .to_string())
}

fn encode_pg_numeric(value: &NumericValue) -> Vec<u8> {
    match value {
        NumericValue::NaN => {
            let mut out = Vec::with_capacity(6);
            push_i32(&mut out, 6);
            push_u16(&mut out, NUMERIC_NAN);
            out
        }
        NumericValue::PosInf => {
            let mut out = Vec::with_capacity(6);
            push_i32(&mut out, 6);
            push_u16(&mut out, NUMERIC_PINF);
            out
        }
        NumericValue::NegInf => {
            let mut out = Vec::with_capacity(6);
            push_i32(&mut out, 6);
            push_u16(&mut out, NUMERIC_NINF);
            out
        }
        NumericValue::Finite {
            coeff,
            scale,
            dscale,
        } => {
            let (sign, mut digits, weight) = decimal_to_pg_digits(coeff, *scale);
            debug_assert!(*scale <= NUMERIC_DSCALE_MASK as u32);
            debug_assert!((i16::MIN as i32..=i16::MAX as i32).contains(&weight));
            while matches!(digits.first(), Some(0)) {
                digits.remove(0);
            }
            while matches!(digits.last(), Some(0)) {
                digits.pop();
            }
            let weight = if digits.is_empty() { 0 } else { weight };
            let can_be_short = *dscale <= NUMERIC_SHORT_DSCALE_MAX as u32
                && weight >= NUMERIC_SHORT_WEIGHT_MIN as i32
                && weight <= NUMERIC_SHORT_WEIGHT_MAX as i32;

            let header_len = if can_be_short { 2 } else { 4 };
            let total_len = 4 + header_len + digits.len() * 2;
            let mut out = Vec::with_capacity(total_len);
            push_i32(&mut out, total_len as i32);
            if can_be_short {
                let mut short = NUMERIC_SHORT;
                if sign == NUMERIC_NEG {
                    short |= NUMERIC_SHORT_SIGN_MASK;
                }
                short |=
                    ((*dscale as u16) << NUMERIC_SHORT_DSCALE_SHIFT) & NUMERIC_SHORT_DSCALE_MASK;
                if weight < 0 {
                    short |= NUMERIC_SHORT_WEIGHT_SIGN_MASK;
                }
                short |= (weight as u16) & NUMERIC_SHORT_WEIGHT_MASK;
                push_u16(&mut out, short);
            } else {
                let sign_dscale = (if sign == NUMERIC_NEG { NUMERIC_NEG } else { 0 })
                    | ((*dscale as u16) & NUMERIC_DSCALE_MASK);
                push_u16(&mut out, sign_dscale);
                push_i16(&mut out, weight as i16);
            }
            for digit in digits {
                push_u16(&mut out, digit);
            }
            out
        }
    }
}

fn decode_pg_numeric(bytes: &[u8]) -> Result<NumericValue, ExecError> {
    if bytes.len() < 6 {
        return Err(corrupt_jsonb());
    }
    let total_len = read_i32_from(bytes, 0)? as usize;
    if total_len != bytes.len() {
        return Err(corrupt_jsonb());
    }
    let header = read_u16_from(bytes, 4)?;
    if header & NUMERIC_SPECIAL == NUMERIC_SPECIAL {
        return if header == NUMERIC_NAN {
            Ok(NumericValue::NaN)
        } else if header == NUMERIC_PINF {
            Ok(NumericValue::PosInf)
        } else if header == NUMERIC_NINF {
            Ok(NumericValue::NegInf)
        } else {
            Err(corrupt_jsonb())
        };
    }

    let (sign, dscale, weight, digits_start) = if header & NUMERIC_SHORT == NUMERIC_SHORT {
        let sign = if header & NUMERIC_SHORT_SIGN_MASK != 0 {
            NUMERIC_NEG
        } else {
            0
        };
        let dscale = ((header & NUMERIC_SHORT_DSCALE_MASK) >> NUMERIC_SHORT_DSCALE_SHIFT) as u32;
        let weight = if header & NUMERIC_SHORT_WEIGHT_SIGN_MASK != 0 {
            (header | !NUMERIC_SHORT_WEIGHT_MASK) as i16
        } else {
            (header & NUMERIC_SHORT_WEIGHT_MASK) as i16
        };
        (sign, dscale, weight, 6usize)
    } else {
        let sign = header & NUMERIC_NEG;
        let dscale = (header & NUMERIC_DSCALE_MASK) as u32;
        let weight = read_i16_from(bytes, 6)?;
        (sign, dscale, weight, 8usize)
    };

    if (bytes.len() - digits_start) % 2 != 0 {
        return Err(corrupt_jsonb());
    }
    let ndigits = (bytes.len() - digits_start) / 2;
    if ndigits == 0 {
        return Ok(NumericValue::finite(BigInt::zero(), dscale).with_dscale(dscale));
    }

    let mut coeff = BigInt::zero();
    for i in 0..ndigits {
        let digit = read_u16_from(bytes, digits_start + i * 2)? as u32;
        if digit >= NBASE as u32 {
            return Err(corrupt_jsonb());
        }
        coeff = coeff * BigInt::from(NBASE) + BigInt::from(digit);
    }
    let integer_group_gap = if (weight + 1) > ndigits as i16 {
        ((weight + 1) - ndigits as i16) as usize
    } else {
        0
    };
    if integer_group_gap > 0 {
        coeff *= pow10(integer_group_gap * DEC_DIGITS);
    }
    let fractional_digits =
        usize::saturating_sub(ndigits, (weight + 1).max(0) as usize) * DEC_DIGITS;
    let coeff = if dscale as usize >= fractional_digits {
        coeff * pow10((dscale as usize) - fractional_digits)
    } else {
        let divisor = pow10(fractional_digits - dscale as usize);
        if (&coeff % &divisor) != BigInt::zero() {
            return Err(corrupt_jsonb());
        }
        coeff / divisor
    };

    Ok(
        NumericValue::finite(if sign == NUMERIC_NEG { -coeff } else { coeff }, dscale)
            .with_dscale(dscale)
            .normalize(),
    )
}

fn decimal_to_pg_digits(coeff: &BigInt, scale: u32) -> (u16, Vec<u16>, i32) {
    let negative = coeff.is_negative();
    let digits = coeff.abs().to_str_radix(10);
    let scale = scale as usize;
    let integer_len = digits.len().saturating_sub(scale);
    let whole = if integer_len == 0 {
        ""
    } else {
        &digits[..integer_len]
    };
    let frac = if scale == 0 {
        String::new()
    } else if digits.len() >= scale {
        digits[digits.len() - scale..].to_string()
    } else {
        format!("{}{}", "0".repeat(scale - digits.len()), digits)
    };

    let mut pg_digits = Vec::new();
    if !whole.is_empty() {
        let first = whole.len() % DEC_DIGITS;
        let first = if first == 0 { DEC_DIGITS } else { first };
        pg_digits.push(whole[..first].parse::<u16>().unwrap());
        let mut idx = first;
        while idx < whole.len() {
            pg_digits.push(whole[idx..idx + DEC_DIGITS].parse::<u16>().unwrap());
            idx += DEC_DIGITS;
        }
    }
    let whole_groups = pg_digits.len();
    if !frac.is_empty() {
        let mut frac = frac;
        while frac.len() % DEC_DIGITS != 0 {
            frac.push('0');
        }
        let mut idx = 0;
        while idx < frac.len() {
            pg_digits.push(frac[idx..idx + DEC_DIGITS].parse::<u16>().unwrap());
            idx += DEC_DIGITS;
        }
    }

    while matches!(pg_digits.first(), Some(0)) {
        pg_digits.remove(0);
    }
    while matches!(pg_digits.last(), Some(0)) {
        pg_digits.pop();
    }

    let weight = if pg_digits.is_empty() {
        0
    } else {
        whole_groups as i32 - 1
    };
    (if negative { NUMERIC_NEG } else { 0 }, pg_digits, weight)
}

fn validate_jsonb_numeric_value(value: &NumericValue) -> Result<(), ExecError> {
    let NumericValue::Finite { coeff, scale, .. } = value else {
        return Ok(());
    };
    if *scale > NUMERIC_DSCALE_MASK as u32 {
        return Err(ExecError::NumericFieldOverflow);
    }
    let (_, digits, weight) = decimal_to_pg_digits(coeff, *scale);
    if !digits.is_empty() && !(i16::MIN as i32..=i16::MAX as i32).contains(&weight) {
        return Err(ExecError::NumericFieldOverflow);
    }
    Ok(())
}

fn jsonb_numeric_text_overflows(text: &str) -> Result<bool, ExecError> {
    let trimmed = text.trim();
    let unsigned = trimmed.strip_prefix(['+', '-']).unwrap_or(trimmed);
    let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
        Some(index) => {
            let exponent = unsigned[index + 1..]
                .parse::<i64>()
                .map_err(|_| ExecError::NumericFieldOverflow)?;
            (&unsigned[..index], exponent)
        }
        None => (unsigned, 0),
    };
    let mut parts = mantissa.split('.');
    let whole = parts.next().unwrap_or("");
    let frac = parts.next().unwrap_or("");
    if parts.next().is_some() {
        return Err(ExecError::InvalidStorageValue {
            column: "jsonb".into(),
            details: format!("invalid input syntax for type jsonb: \"{text}\""),
        });
    }
    let digits = format!("{whole}{frac}");
    let Some(first_nonzero) = digits.bytes().position(|b| b != b'0') else {
        return Ok(false);
    };
    let last_nonzero = digits.bytes().rposition(|b| b != b'0').unwrap();
    let decimal_pos = whole.len() as i64 + exponent;
    let digits_before_decimal = (decimal_pos - first_nonzero as i64).max(0);
    if digits_before_decimal > ((i16::MAX as i64) + 1) * DEC_DIGITS as i64 {
        return Ok(true);
    }
    let significant_end = last_nonzero as i64 + 1;
    let scale = if decimal_pos >= significant_end {
        0
    } else {
        significant_end - decimal_pos.max(first_nonzero as i64)
    };
    Ok(scale > NUMERIC_DSCALE_MASK as i64)
}

fn render_jsonb_value(out: &mut String, value: &JsonbValue) {
    match value {
        JsonbValue::Null => out.push_str("null"),
        JsonbValue::Bool(true) => out.push_str("true"),
        JsonbValue::Bool(false) => out.push_str("false"),
        JsonbValue::Numeric(numeric) => out.push_str(&numeric.render()),
        JsonbValue::String(text) => render_jsonb_string(out, text),
        JsonbValue::Array(items) => {
            out.push('[');
            for (idx, item) in items.iter().enumerate() {
                if idx > 0 {
                    out.push_str(", ");
                }
                render_jsonb_value(out, item);
            }
            out.push(']');
        }
        JsonbValue::Object(items) => {
            out.push('{');
            for (idx, (key, value)) in items.iter().enumerate() {
                if idx > 0 {
                    out.push_str(", ");
                }
                render_jsonb_string(out, key);
                out.push_str(": ");
                render_jsonb_value(out, value);
            }
            out.push('}');
        }
    }
}

fn render_jsonb_string(out: &mut String, text: &str) {
    out.push('"');
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c <= '\u{1f}' => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

fn canonicalize_object_pairs(items: Vec<(String, JsonbValue)>) -> Vec<(String, JsonbValue)> {
    let mut deduped: Vec<(String, JsonbValue)> = Vec::new();
    for (key, value) in items {
        if let Some(existing) = deduped
            .iter_mut()
            .find(|(existing_key, _)| existing_key == &key)
        {
            existing.1 = value;
        } else {
            deduped.push((key, value));
        }
    }
    deduped.sort_by(|(lk, _), (rk, _)| compare_jsonb_key(lk, rk));
    deduped
}

fn compare_jsonb_key(left: &str, right: &str) -> Ordering {
    left.len()
        .cmp(&right.len())
        .then_with(|| left.as_bytes().cmp(right.as_bytes()))
}

fn jsonb_type_rank(value: &JsonbValue) -> u8 {
    match value {
        JsonbValue::Null => 0,
        JsonbValue::String(_) => 1,
        JsonbValue::Numeric(_) => 2,
        JsonbValue::Bool(_) => 3,
        JsonbValue::Array(_) => 16,
        JsonbValue::Object(_) => 17,
    }
}

fn jsonb_get_index(value: &JsonbValue, index: i32) -> Option<&JsonbValue> {
    let items = match value {
        JsonbValue::Array(items) => items,
        _ => return None,
    };
    let len = items.len() as i32;
    let idx = if index < 0 { len + index } else { index };
    if idx < 0 {
        None
    } else {
        items.get(idx as usize)
    }
}

fn pad_to_int(out: &mut Vec<u8>) -> usize {
    let aligned = align4(out.len());
    let pad = aligned - out.len();
    if pad > 0 {
        out.resize(aligned, 0);
    }
    pad
}

fn align4(offset: usize) -> usize {
    (offset + 3) & !3
}

fn reserve(out: &mut Vec<u8>, len: usize) -> usize {
    let offset = out.len();
    out.resize(offset + len, 0);
    offset
}

fn write_u32(out: &mut [u8], offset: usize, value: u32) {
    out[offset..offset + 4].copy_from_slice(&value.to_ne_bytes());
}

fn push_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_ne_bytes());
}

fn push_i32(out: &mut Vec<u8>, value: i32) {
    out.extend_from_slice(&value.to_ne_bytes());
}

fn push_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_ne_bytes());
}

fn push_i16(out: &mut Vec<u8>, value: i16) {
    out.extend_from_slice(&value.to_ne_bytes());
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, ExecError> {
    read_u32_from(bytes, offset)
}

fn read_u32_from(bytes: &[u8], offset: usize) -> Result<u32, ExecError> {
    if offset + 4 > bytes.len() {
        return Err(corrupt_jsonb());
    }
    let mut raw = [0u8; 4];
    raw.copy_from_slice(&bytes[offset..offset + 4]);
    Ok(u32::from_ne_bytes(raw))
}

fn read_i32_from(bytes: &[u8], offset: usize) -> Result<i32, ExecError> {
    if offset + 4 > bytes.len() {
        return Err(corrupt_jsonb());
    }
    let mut raw = [0u8; 4];
    raw.copy_from_slice(&bytes[offset..offset + 4]);
    Ok(i32::from_ne_bytes(raw))
}

fn read_u16_from(bytes: &[u8], offset: usize) -> Result<u16, ExecError> {
    if offset + 2 > bytes.len() {
        return Err(corrupt_jsonb());
    }
    let mut raw = [0u8; 2];
    raw.copy_from_slice(&bytes[offset..offset + 2]);
    Ok(u16::from_ne_bytes(raw))
}

fn read_i16_from(bytes: &[u8], offset: usize) -> Result<i16, ExecError> {
    if offset + 2 > bytes.len() {
        return Err(corrupt_jsonb());
    }
    let mut raw = [0u8; 2];
    raw.copy_from_slice(&bytes[offset..offset + 2]);
    Ok(i16::from_ne_bytes(raw))
}

fn jentry_len(meta: u32) -> u32 {
    meta & JENTRY_OFFLENMASK
}

fn pow10(exp: usize) -> BigInt {
    let mut value = BigInt::from(1u8);
    for _ in 0..exp {
        value *= 10u8;
    }
    value
}

fn corrupt_jsonb() -> ExecError {
    ExecError::InvalidStorageValue {
        column: "jsonb".into(),
        details: "corrupt jsonb payload".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_input_error_keeps_structured_fields() {
        let err = parse_json_text_input("{\"a\":true").unwrap_err();
        assert!(matches!(
            err,
            ExecError::JsonInput {
                message,
                detail: Some(detail),
                context: Some(context),
                ..
            } if message == "invalid input syntax for type json"
                && detail == "The input string ended unexpectedly."
                && context == "JSON data, line 1: {\"a\":true"
        ));
    }

    #[test]
    fn json_input_error_uses_postgres_style_detail_messages() {
        let cases = [
            ("''", "Token \"'\" is invalid.", "JSON data, line 1: ''"),
            (
                "\"abc",
                "Token \"\"abc\" is invalid.",
                "JSON data, line 1: \"abc",
            ),
            (
                "\"abc\ndef\"",
                "Character with value 0x0a must be escaped.",
                "JSON data, line 1: \"abc",
            ),
            (
                "\"\\v\"",
                "Escape sequence \"\\v\" is invalid.",
                "JSON data, line 1: \"\\v\"",
            ),
            (
                "[1,2,]",
                "Expected JSON value, but found \"]\".",
                "JSON data, line 1: [1,2,]",
            ),
            (
                "{\"abc\"}",
                "Expected \":\", but found \"}\".",
                "JSON data, line 1: {\"abc\"}",
            ),
            (
                "{1:\"abc\"}",
                "Expected string or \"}\", but found \"1\".",
                "JSON data, line 1: {1:\"abc\"}",
            ),
            (
                "{\"abc\"=1}",
                "Token \"=\" is invalid.",
                "JSON data, line 1: {\"abc\"=1}",
            ),
            (
                "{\"abc\":1:2}",
                "Expected \",\" or \"}\", but found \":\".",
                "JSON data, line 1: {\"abc\":1:2}",
            ),
            (
                "{\"abc\":1,3}",
                "Expected string, but found \"3\".",
                "JSON data, line 1: {\"abc\":1,3}",
            ),
            (
                "true false",
                "Expected end of input, but found \"false\".",
                "JSON data, line 1: true false",
            ),
            (
                "trues",
                "Token \"trues\" is invalid.",
                "JSON data, line 1: trues",
            ),
            ("01", "Token \"01\" is invalid.", "JSON data, line 1: 01"),
        ];

        for (input, expected_detail, expected_context) in cases {
            let err = parse_json_text_input(input).unwrap_err();
            assert!(
                matches!(
                    err,
                    ExecError::JsonInput {
                        detail: Some(ref detail),
                        context: Some(ref context),
                        ..
                    } if detail == expected_detail && context == expected_context
                ),
                "unexpected diagnostic for input {input:?}: {err:?}"
            );
        }
    }

    #[test]
    fn jsonb_input_enforces_stack_depth_limit() {
        let err = parse_jsonb_text_with_limit(&"[".repeat(10_000), 100).unwrap_err();
        assert!(matches!(
            err,
            ExecError::DetailedError {
                message,
                hint: Some(hint),
                sqlstate,
                ..
            } if message == "stack depth limit exceeded"
                && sqlstate == "54001"
                && hint.contains("\"max_stack_depth\" (currently 100kB)")
        ));
    }

    #[test]
    fn scalar_root_uses_pg_scalar_array_wrapper() {
        let bytes = encode_jsonb(&JsonbValue::Numeric(NumericValue::from("42")));
        let header = u32::from_ne_bytes(bytes[0..4].try_into().unwrap());
        assert_eq!(header & (JB_FARRAY | JB_FSCALAR), JB_FARRAY | JB_FSCALAR);
        assert_eq!(
            decode_jsonb(&bytes).unwrap(),
            JsonbValue::Numeric(NumericValue::from("42"))
        );
    }

    #[test]
    fn object_keys_are_sorted_by_pg_length_then_bytes() {
        let value = JsonbValue::Object(vec![
            ("bbb".into(), JsonbValue::Null),
            ("aa".into(), JsonbValue::Null),
            ("b".into(), JsonbValue::Null),
            ("ab".into(), JsonbValue::Null),
        ]);
        let decoded = decode_jsonb(&encode_jsonb(&value)).unwrap();
        assert_eq!(
            decoded,
            JsonbValue::Object(vec![
                ("b".into(), JsonbValue::Null),
                ("aa".into(), JsonbValue::Null),
                ("ab".into(), JsonbValue::Null),
                ("bbb".into(), JsonbValue::Null),
            ])
        );
    }

    #[test]
    fn numeric_payload_round_trips_pg_numeric_bytes() {
        let value = JsonbValue::Numeric(NumericValue::finite(BigInt::from(12345u32), 2));
        let encoded = encode_jsonb(&value);
        let decoded = decode_jsonb(&encoded).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn numeric_payload_handles_large_exponents() {
        let value = JsonbValue::Numeric(NumericValue::from("1e100"));
        let encoded = encode_jsonb(&value);
        let decoded = decode_jsonb(&encoded).unwrap();
        assert_eq!(decoded, value);
        assert_eq!(
            decoded.render(),
            "10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
        );
    }

    #[test]
    fn render_uses_postgres_jsonb_spacing() {
        let value = JsonbValue::Object(vec![
            ("a".into(), JsonbValue::Numeric(NumericValue::from("1"))),
            (
                "b".into(),
                JsonbValue::Array(vec![
                    JsonbValue::Numeric(NumericValue::from("2")),
                    JsonbValue::String("x".into()),
                ]),
            ),
        ]);
        assert_eq!(value.render(), "{\"a\": 1, \"b\": [2, \"x\"]}");
    }
}
