//! Recursive-descent JSON validator (common/jsonapi.c, non-incremental,
//! need_escapes=false path used by json_in/json_recv). Validation-only: no
//! strval de-escaping, no surrogate combining, no server-encoding conversion —
//! those live on the need_escapes lanes (json_typeof/object-keys), loud there.

use stack_depth::check_stack_depth;
use types_error::PgResult;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum JsonToken {
    Invalid,
    String,
    Number,
    ObjectStart,
    ObjectEnd,
    ArrayStart,
    ArrayEnd,
    Comma,
    Colon,
    True,
    False,
    Null,
    End,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum JsonError {
    Success,
    EscapingInvalid,
    EscapingRequired,
    ExpectedArrayFirst,
    ExpectedArrayNext,
    ExpectedColon,
    ExpectedEnd,
    ExpectedJson,
    ExpectedMore,
    ExpectedObjectFirst,
    ExpectedObjectNext,
    ExpectedString,
    InvalidToken,
    UnicodeEscapeFormat,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ParseCtx {
    Value,
    String,
    ArrayStart,
    ArrayNext,
    ObjectStart,
    ObjectLabel,
    ObjectNext,
    End,
}

#[inline]
fn is_alnum(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c & 0x80 != 0
}

#[inline]
fn is_hex(c: u8) -> bool {
    c.is_ascii_digit() || (b'a'..=b'f').contains(&c) || (b'A'..=b'F').contains(&c)
}

pub struct JsonLex<'a> {
    input: &'a [u8],
    encoding: i32,
    // C's token_start is NULL only at EOF; None mirrors that.
    pub token_start: Option<usize>,
    pub token_terminator: usize,
    pub line_number: i32,
    line_start: usize,
    pub token_type: JsonToken,
}

impl<'a> JsonLex<'a> {
    pub fn new(input: &'a [u8], encoding: i32) -> Self {
        JsonLex {
            input,
            encoding,
            token_start: Some(0),
            token_terminator: 0,
            line_number: 1,
            line_start: 0,
            token_type: JsonToken::Invalid,
        }
    }

    #[inline]
    fn end(&self) -> usize {
        self.input.len()
    }

    fn fail_at_char_end(&mut self, s: usize, code: JsonError) -> JsonError {
        let end = self.end();
        let remaining = end - s;
        let charlen = wchar::pg_encoding_mblen_or_incomplete(self.encoding, &self.input[s..end]);
        self.token_terminator = if (charlen as usize) <= remaining {
            s + charlen as usize
        } else {
            end
        };
        code
    }

    pub fn lex(&mut self) -> JsonError {
        let end = self.end();
        let mut s = self.token_terminator;

        while s < end
            && matches!(self.input[s], b' ' | b'\t' | b'\n' | b'\r')
        {
            let c = self.input[s];
            s += 1;
            if c == b'\n' {
                self.line_number += 1;
                self.line_start = s;
            }
        }
        self.token_start = Some(s);

        if s >= end {
            self.token_start = None;
            self.token_terminator = s;
            self.token_type = JsonToken::End;
            return JsonError::Success;
        }

        match self.input[s] {
            b'{' => self.single(s, JsonToken::ObjectStart),
            b'}' => self.single(s, JsonToken::ObjectEnd),
            b'[' => self.single(s, JsonToken::ArrayStart),
            b']' => self.single(s, JsonToken::ArrayEnd),
            b',' => self.single(s, JsonToken::Comma),
            b':' => self.single(s, JsonToken::Colon),
            b'"' => {
                let r = self.lex_string();
                if r != JsonError::Success {
                    return r;
                }
                self.token_type = JsonToken::String;
                JsonError::Success
            }
            b'-' => {
                let r = self.lex_number(s + 1);
                if r != JsonError::Success {
                    return r;
                }
                self.token_type = JsonToken::Number;
                JsonError::Success
            }
            b'0'..=b'9' => {
                let r = self.lex_number(s);
                if r != JsonError::Success {
                    return r;
                }
                self.token_type = JsonToken::Number;
                JsonError::Success
            }
            _ => {
                let mut p = s;
                while p < end && is_alnum(self.input[p]) {
                    p += 1;
                }
                if p == s {
                    self.token_terminator = s + 1;
                    return JsonError::InvalidToken;
                }
                self.token_terminator = p;
                let word = &self.input[s..p];
                self.token_type = match word {
                    b"true" => JsonToken::True,
                    b"null" => JsonToken::Null,
                    b"false" => JsonToken::False,
                    _ => return JsonError::InvalidToken,
                };
                JsonError::Success
            }
        }
    }

    #[inline]
    fn single(&mut self, s: usize, tok: JsonToken) -> JsonError {
        self.token_terminator = s + 1;
        self.token_type = tok;
        JsonError::Success
    }

    fn lex_string(&mut self) -> JsonError {
        let end = self.end();
        let mut s = self.token_start.expect("lex_string entered at a token");
        loop {
            s += 1;
            if s >= end {
                self.token_terminator = s;
                return JsonError::InvalidToken;
            } else if self.input[s] == b'"' {
                break;
            } else if self.input[s] == b'\\' {
                s += 1;
                if s >= end {
                    self.token_terminator = s;
                    return JsonError::InvalidToken;
                } else if self.input[s] == b'u' {
                    for _ in 0..4 {
                        s += 1;
                        if s >= end {
                            self.token_terminator = s;
                            return JsonError::InvalidToken;
                        } else if !is_hex(self.input[s]) {
                            return self.fail_at_char_end(s, JsonError::UnicodeEscapeFormat);
                        }
                    }
                } else if !matches!(
                    self.input[s],
                    b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't'
                ) {
                    self.token_start = Some(s);
                    return self.fail_at_char_end(s, JsonError::EscapingInvalid);
                }
            } else {
                let mut p = s;
                // 16-byte clean-byte skip, C's pg_lfind8/pg_lfind8_le shape in
                // json_lex_string; the OR-reduction has no early exit so LLVM
                // vectorizes it (cmeq/cmhs + umaxv on aarch64).
                while p + 16 <= end {
                    let chunk: &[u8; 16] = self.input[p..p + 16].try_into().unwrap();
                    let mut hit = 0u8;
                    for &c in chunk {
                        hit |= u8::from(c == b'\\') | u8::from(c == b'"') | u8::from(c <= 0x1F);
                    }
                    if hit != 0 {
                        break;
                    }
                    p += 16;
                }
                while p < end {
                    let c = self.input[p];
                    if c == b'\\' || c == b'"' {
                        break;
                    } else if c <= 31 {
                        self.token_terminator = p;
                        return JsonError::EscapingRequired;
                    }
                    p += 1;
                }
                s = p - 1;
            }
        }
        self.token_terminator = s + 1;
        JsonError::Success
    }

    // C: json_lex_number with num_err=NULL, total_len=NULL. `s` is the index of
    // the first digit (after any '-', which the caller consumed).
    fn lex_number(&mut self, mut s: usize) -> JsonError {
        let input_length = self.input.len();
        let mut error = false;
        let mut len = s;

        if len < input_length && self.input[s] == b'0' {
            s += 1;
            len += 1;
        } else if len < input_length && (b'1'..=b'9').contains(&self.input[s]) {
            loop {
                s += 1;
                len += 1;
                if !(len < input_length && self.input[s].is_ascii_digit()) {
                    break;
                }
            }
        } else {
            error = true;
        }

        if len < input_length && self.input[s] == b'.' {
            s += 1;
            len += 1;
            if len == input_length || !self.input[s].is_ascii_digit() {
                error = true;
            } else {
                loop {
                    s += 1;
                    len += 1;
                    if !(len < input_length && self.input[s].is_ascii_digit()) {
                        break;
                    }
                }
            }
        }

        if len < input_length && matches!(self.input[s], b'e' | b'E') {
            s += 1;
            len += 1;
            if len < input_length && matches!(self.input[s], b'+' | b'-') {
                s += 1;
                len += 1;
            }
            if len == input_length || !self.input[s].is_ascii_digit() {
                error = true;
            } else {
                loop {
                    s += 1;
                    len += 1;
                    if !(len < input_length && self.input[s].is_ascii_digit()) {
                        break;
                    }
                }
            }
        }

        while len < input_length && is_alnum(self.input[s]) {
            error = true;
            s += 1;
            len += 1;
        }

        self.token_terminator = s;
        if error {
            JsonError::InvalidToken
        } else {
            JsonError::Success
        }
    }

    fn report_parse_error(&self, ctx: ParseCtx) -> JsonError {
        if self.token_start.is_none() || self.token_type == JsonToken::End {
            return JsonError::ExpectedMore;
        }
        match ctx {
            ParseCtx::Value => JsonError::ExpectedJson,
            ParseCtx::String => JsonError::ExpectedString,
            ParseCtx::ArrayStart => JsonError::ExpectedArrayFirst,
            ParseCtx::ArrayNext => JsonError::ExpectedArrayNext,
            ParseCtx::ObjectStart => JsonError::ExpectedObjectFirst,
            ParseCtx::ObjectLabel => JsonError::ExpectedColon,
            ParseCtx::ObjectNext => JsonError::ExpectedObjectNext,
            ParseCtx::End => JsonError::ExpectedEnd,
        }
    }

    fn lex_expect(&mut self, ctx: ParseCtx, token: JsonToken) -> JsonError {
        if self.token_type == token {
            self.lex()
        } else {
            self.report_parse_error(ctx)
        }
    }

    fn current_token(&self) -> &[u8] {
        let start = self.token_start.unwrap_or(self.token_terminator);
        &self.input[start..self.token_terminator]
    }

    // C: json_errdetail. The `%.*s` specifier prints the current token verbatim.
    pub fn errdetail(&self, error: JsonError) -> String {
        let tok = || String::from_utf8_lossy(self.current_token());
        match error {
            JsonError::EscapingInvalid => {
                format!("Escape sequence \"\\{}\" is invalid.", tok())
            }
            JsonError::EscapingRequired => format!(
                "Character with value 0x{:02x} must be escaped.",
                self.input[self.token_terminator]
            ),
            JsonError::ExpectedEnd => {
                format!("Expected end of input, but found \"{}\".", tok())
            }
            JsonError::ExpectedArrayFirst => {
                format!("Expected array element or \"]\", but found \"{}\".", tok())
            }
            JsonError::ExpectedArrayNext => {
                format!("Expected \",\" or \"]\", but found \"{}\".", tok())
            }
            JsonError::ExpectedColon => {
                format!("Expected \":\", but found \"{}\".", tok())
            }
            JsonError::ExpectedJson => {
                format!("Expected JSON value, but found \"{}\".", tok())
            }
            JsonError::ExpectedMore => "The input string ended unexpectedly.".to_string(),
            JsonError::ExpectedObjectFirst => {
                format!("Expected string or \"}}\", but found \"{}\".", tok())
            }
            JsonError::ExpectedObjectNext => {
                format!("Expected \",\" or \"}}\", but found \"{}\".", tok())
            }
            JsonError::ExpectedString => {
                format!("Expected string, but found \"{}\".", tok())
            }
            JsonError::InvalidToken => format!("Token \"{}\" is invalid.", tok()),
            JsonError::UnicodeEscapeFormat => {
                "\"\\u\" must be followed by four hexadecimal digits.".to_string()
            }
            JsonError::Success => String::new(),
        }
    }

    // C: report_json_context — the "JSON data, line N: ..." errcontext line.
    pub fn errcontext(&self) -> String {
        let line_start = self.line_start;
        let context_end = self.token_terminator;
        let mut context_start = line_start;

        while context_end - context_start >= 50 {
            if self.input[context_start] & 0x80 != 0 {
                context_start +=
                    wchar::pg_encoding_mblen(self.encoding, &self.input[context_start..context_end])
                        as usize;
            } else {
                context_start += 1;
            }
        }

        if context_start - line_start <= 3 {
            context_start = line_start;
        }

        let ctxt = String::from_utf8_lossy(&self.input[context_start..context_end]);
        let prefix = if context_start > line_start { "..." } else { "" };
        let suffix = if self.token_type != JsonToken::End
            && context_end < self.input.len()
            && self.input[context_end] != b'\n'
            && self.input[context_end] != b'\r'
        {
            "..."
        } else {
            ""
        };

        format!(
            "JSON data, line {}: {}{}{}",
            self.line_number, prefix, ctxt, suffix
        )
    }
}

pub fn parse(lex: &mut JsonLex<'_>) -> PgResult<JsonError> {
    let r = lex.lex();
    if r != JsonError::Success {
        return Ok(r);
    }
    let result = match lex.token_type {
        JsonToken::ObjectStart => parse_object(lex)?,
        JsonToken::ArrayStart => parse_array(lex)?,
        _ => parse_scalar(lex),
    };
    if result != JsonError::Success {
        return Ok(result);
    }
    Ok(lex.lex_expect(ParseCtx::End, JsonToken::End))
}

fn parse_scalar(lex: &mut JsonLex<'_>) -> JsonError {
    match lex.token_type {
        JsonToken::String
        | JsonToken::Number
        | JsonToken::True
        | JsonToken::False
        | JsonToken::Null => lex.lex(),
        _ => lex.report_parse_error(ParseCtx::Value),
    }
}

fn parse_object_field(lex: &mut JsonLex<'_>) -> PgResult<JsonError> {
    if lex.token_type != JsonToken::String {
        return Ok(lex.report_parse_error(ParseCtx::String));
    }
    let r = lex.lex();
    if r != JsonError::Success {
        return Ok(r);
    }
    let r = lex.lex_expect(ParseCtx::ObjectLabel, JsonToken::Colon);
    if r != JsonError::Success {
        return Ok(r);
    }
    match lex.token_type {
        JsonToken::ObjectStart => parse_object(lex),
        JsonToken::ArrayStart => parse_array(lex),
        _ => Ok(parse_scalar(lex)),
    }
}

fn parse_object(lex: &mut JsonLex<'_>) -> PgResult<JsonError> {
    check_stack_depth()?;

    let r = lex.lex();
    if r != JsonError::Success {
        return Ok(r);
    }

    let mut result = match lex.token_type {
        JsonToken::String => {
            let mut result = parse_object_field(lex)?;
            while result == JsonError::Success && lex.token_type == JsonToken::Comma {
                result = lex.lex();
                if result != JsonError::Success {
                    break;
                }
                result = parse_object_field(lex)?;
            }
            result
        }
        JsonToken::ObjectEnd => JsonError::Success,
        _ => lex.report_parse_error(ParseCtx::ObjectStart),
    };
    if result != JsonError::Success {
        return Ok(result);
    }

    result = lex.lex_expect(ParseCtx::ObjectNext, JsonToken::ObjectEnd);
    Ok(result)
}

fn parse_array_element(lex: &mut JsonLex<'_>) -> PgResult<JsonError> {
    match lex.token_type {
        JsonToken::ObjectStart => parse_object(lex),
        JsonToken::ArrayStart => parse_array(lex),
        _ => Ok(parse_scalar(lex)),
    }
}

fn parse_array(lex: &mut JsonLex<'_>) -> PgResult<JsonError> {
    check_stack_depth()?;

    let mut result = lex.lex_expect(ParseCtx::ArrayStart, JsonToken::ArrayStart);
    if result == JsonError::Success && lex.token_type != JsonToken::ArrayEnd {
        result = parse_array_element(lex)?;
        while result == JsonError::Success && lex.token_type == JsonToken::Comma {
            result = lex.lex();
            if result != JsonError::Success {
                break;
            }
            result = parse_array_element(lex)?;
        }
    }
    if result != JsonError::Success {
        return Ok(result);
    }

    result = lex.lex_expect(ParseCtx::ArrayNext, JsonToken::ArrayEnd);
    Ok(result)
}
