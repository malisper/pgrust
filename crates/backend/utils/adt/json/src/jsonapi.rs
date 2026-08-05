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
    SemActionFailed,
    UnicodeCodePointZero,
    UnicodeEscapeFormat,
    UnicodeHighSurrogate,
    UnicodeLowSurrogate,
    UnicodeUntranslatable,
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

#[derive(Clone)]
pub struct JsonLex<'a> {
    input: &'a [u8],
    encoding: i32,
    // C's token_start is NULL only at EOF; None mirrors that.
    pub token_start: Option<usize>,
    pub token_terminator: usize,
    pub prev_token_terminator: usize,
    // Maintained only by the sem-action drivers (parse_sem), as consumers
    // (jsonfuncs.c workers) read it; the validation lane leaves it 0.
    pub lex_level: i32,
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
            prev_token_terminator: 0,
            lex_level: 0,
            line_number: 1,
            line_start: 0,
            token_type: JsonToken::Invalid,
        }
    }

    pub fn input(&self) -> &'a [u8] {
        self.input
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
        self.prev_token_terminator = self.token_terminator;

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

    // lex() for the de-escape lane: identical dispatch except the string case
    // is left to JsonLexDe (None = token_start sits on the opening quote).
    fn lex_dispatch_no_string(&mut self) -> Option<JsonError> {
        let end = self.end();
        let mut s = self.token_terminator;
        self.prev_token_terminator = self.token_terminator;

        while s < end && matches!(self.input[s], b' ' | b'\t' | b'\n' | b'\r') {
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
            return Some(JsonError::Success);
        }

        match self.input[s] {
            b'{' => Some(self.single(s, JsonToken::ObjectStart)),
            b'}' => Some(self.single(s, JsonToken::ObjectEnd)),
            b'[' => Some(self.single(s, JsonToken::ArrayStart)),
            b']' => Some(self.single(s, JsonToken::ArrayEnd)),
            b',' => Some(self.single(s, JsonToken::Comma)),
            b':' => Some(self.single(s, JsonToken::Colon)),
            b'"' => None,
            b'-' => {
                let r = self.lex_number(s + 1);
                if r != JsonError::Success {
                    return Some(r);
                }
                self.token_type = JsonToken::Number;
                Some(JsonError::Success)
            }
            b'0'..=b'9' => {
                let r = self.lex_number(s);
                if r != JsonError::Success {
                    return Some(r);
                }
                self.token_type = JsonToken::Number;
                Some(JsonError::Success)
            }
            _ => {
                let mut p = s;
                while p < end && is_alnum(self.input[p]) {
                    p += 1;
                }
                if p == s {
                    self.token_terminator = s + 1;
                    return Some(JsonError::InvalidToken);
                }
                self.token_terminator = p;
                let word = &self.input[s..p];
                self.token_type = match word {
                    b"true" => JsonToken::True,
                    b"null" => JsonToken::Null,
                    b"false" => JsonToken::False,
                    _ => return Some(JsonError::InvalidToken),
                };
                Some(JsonError::Success)
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
            JsonError::UnicodeCodePointZero => {
                "\\u0000 cannot be converted to text.".to_string()
            }
            JsonError::UnicodeEscapeFormat => {
                "\"\\u\" must be followed by four hexadecimal digits.".to_string()
            }
            JsonError::UnicodeHighSurrogate => {
                "Unicode high surrogate must not follow a high surrogate.".to_string()
            }
            JsonError::UnicodeLowSurrogate => {
                "Unicode low surrogate must follow a high surrogate.".to_string()
            }
            JsonError::UnicodeUntranslatable => format!(
                "Unicode escape value could not be translated to the server's encoding {}.",
                mbutils::GetDatabaseEncodingName()
            ),
            JsonError::SemActionFailed | JsonError::Success => String::new(),
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

#[derive(Clone, Copy)]
pub enum JsonSemToken<'mcx> {
    String(&'mcx [u8]),
    Number(&'mcx [u8]),
    True,
    False,
    Null,
}

/// C: JsonSemAction. Hooks return Ok(false) for JSON_SEM_ACTION_FAILED after
/// recording a soft error; `lex` carries lex_level/token positions exactly as
/// C hooks read them through state->lex.
pub trait JsonSem<'mcx> {
    fn object_start(&mut self, _lex: &JsonLex<'_>) -> PgResult<bool> {
        Ok(true)
    }
    fn object_end(&mut self, _lex: &JsonLex<'_>) -> PgResult<bool> {
        Ok(true)
    }
    fn array_start(&mut self, _lex: &JsonLex<'_>) -> PgResult<bool> {
        Ok(true)
    }
    fn array_end(&mut self, _lex: &JsonLex<'_>) -> PgResult<bool> {
        Ok(true)
    }
    fn object_field_start(
        &mut self,
        _lex: &JsonLex<'_>,
        _fname: &'mcx [u8],
        _isnull: bool,
    ) -> PgResult<bool> {
        Ok(true)
    }
    fn object_field_end(
        &mut self,
        _lex: &JsonLex<'_>,
        _fname: &'mcx [u8],
        _isnull: bool,
    ) -> PgResult<bool> {
        Ok(true)
    }
    fn array_element_start(&mut self, _lex: &JsonLex<'_>, _isnull: bool) -> PgResult<bool> {
        Ok(true)
    }
    fn array_element_end(&mut self, _lex: &JsonLex<'_>, _isnull: bool) -> PgResult<bool> {
        Ok(true)
    }
    fn scalar(&mut self, _lex: &JsonLex<'_>, _token: JsonSemToken<'mcx>) -> PgResult<bool> {
        Ok(true)
    }
}

/// C: json_count_array_elements — counts the elements of the array whose '['
/// is the current token, over a throwaway need_escapes=false lexer copy.
pub fn json_count_array_elements(lex: &JsonLex<'_>) -> PgResult<Result<i32, JsonError>> {
    let mut copy = lex.clone();
    let mut count = 0i32;
    let mut r = copy.lex_expect(ParseCtx::ArrayStart, JsonToken::ArrayStart);
    if r == JsonError::Success && copy.token_type != JsonToken::ArrayEnd {
        loop {
            count += 1;
            r = parse_array_element(&mut copy)?;
            if r != JsonError::Success || copy.token_type != JsonToken::Comma {
                break;
            }
            r = copy.lex();
            if r != JsonError::Success {
                break;
            }
        }
    }
    if r != JsonError::Success {
        return Ok(Err(r));
    }
    r = copy.lex_expect(ParseCtx::ArrayNext, JsonToken::ArrayEnd);
    if r != JsonError::Success {
        return Ok(Err(r));
    }
    Ok(Ok(count))
}

/// C: JsonLexContext with need_escapes=true — `strval` carries the de-escaped
/// value of the current string token. The mcx feeds pg_unicode_to_server on
/// the non-UTF8 escape arm and the arena copies handed to sem hooks.
pub struct JsonLexDe<'src, 'mcx> {
    pub lex: JsonLex<'src>,
    mcx: mcx::Mcx<'mcx>,
    strval: mcx::PgVec<'mcx, u8>,
    need_escapes: bool,
}

impl<'src, 'mcx> JsonLexDe<'src, 'mcx> {
    pub fn new(mcx: mcx::Mcx<'mcx>, input: &'src [u8], encoding: i32) -> Self {
        Self::with_escapes(mcx, input, encoding, true)
    }

    /// C: makeJsonLexContext with an explicit need_escapes (false skips strval
    /// population; string-typed hook payloads are empty, mirroring C's NULL).
    pub fn with_escapes(
        mcx: mcx::Mcx<'mcx>,
        input: &'src [u8],
        encoding: i32,
        need_escapes: bool,
    ) -> Self {
        JsonLexDe {
            lex: JsonLex::new(input, encoding),
            mcx,
            strval: mcx::PgVec::new_in(mcx),
            need_escapes,
        }
    }

    fn lex(&mut self) -> PgResult<JsonError> {
        if !self.need_escapes {
            return Ok(self.lex.lex());
        }
        let r = self.lex.lex_dispatch_no_string();
        match r {
            Some(err) => Ok(err),
            None => {
                let r = self.lex_string_de()?;
                if r == JsonError::Success {
                    self.lex.token_type = JsonToken::String;
                }
                Ok(r)
            }
        }
    }

    // C: json_lex_string, need_escapes=true.
    fn lex_string_de(&mut self) -> PgResult<JsonError> {
        let input = self.lex.input;
        let end = input.len();
        let mut s = self.lex.token_start.expect("lex_string entered at a token");
        let mut hi_surrogate: i32 = -1;
        self.strval.clear();
        loop {
            s += 1;
            if s >= end {
                self.lex.token_terminator = s;
                return Ok(JsonError::InvalidToken);
            } else if input[s] == b'"' {
                break;
            } else if input[s] == b'\\' {
                s += 1;
                if s >= end {
                    self.lex.token_terminator = s;
                    return Ok(JsonError::InvalidToken);
                } else if input[s] == b'u' {
                    let mut ch: u32 = 0;
                    for _ in 0..4 {
                        s += 1;
                        if s >= end {
                            self.lex.token_terminator = s;
                            return Ok(JsonError::InvalidToken);
                        }
                        let c = input[s];
                        ch = match c {
                            b'0'..=b'9' => ch * 16 + u32::from(c - b'0'),
                            b'a'..=b'f' => ch * 16 + u32::from(c - b'a') + 10,
                            b'A'..=b'F' => ch * 16 + u32::from(c - b'A') + 10,
                            _ => {
                                return Ok(self
                                    .lex
                                    .fail_at_char_end(s, JsonError::UnicodeEscapeFormat))
                            }
                        };
                    }
                    if wchar::is_utf16_surrogate_first(ch) {
                        if hi_surrogate != -1 {
                            return Ok(self
                                .lex
                                .fail_at_char_end(s, JsonError::UnicodeHighSurrogate));
                        }
                        hi_surrogate = ch as i32;
                        continue;
                    } else if wchar::is_utf16_surrogate_second(ch) {
                        if hi_surrogate == -1 {
                            return Ok(self
                                .lex
                                .fail_at_char_end(s, JsonError::UnicodeLowSurrogate));
                        }
                        ch = wchar::surrogate_pair_to_codepoint(hi_surrogate as u32, ch);
                        hi_surrogate = -1;
                    }
                    if hi_surrogate != -1 {
                        return Ok(self
                            .lex
                            .fail_at_char_end(s, JsonError::UnicodeLowSurrogate));
                    }
                    if ch == 0 {
                        return Ok(self
                            .lex
                            .fail_at_char_end(s, JsonError::UnicodeCodePointZero));
                    }
                    // C: pg_unicode_to_server_noerror — its ASCII and UTF8
                    // server-encoding arms inlined to skip the arena round trip.
                    if ch <= 0x7F {
                        self.strval.push(ch as u8);
                    } else if mbutils::GetDatabaseEncoding() == wchar::PG_UTF8 {
                        let mut buf = [0u8; 4];
                        wchar::unicode_to_utf8(ch, &mut buf);
                        let n = wchar::pg_utf_mblen(&buf) as usize;
                        mcx::vec_append_bytes(&mut self.strval, &buf[..n])?;
                    } else {
                        match mbutils::pg_unicode_to_server_noerror(self.mcx, ch)? {
                            Some(converted) => {
                                mcx::vec_append_bytes(&mut self.strval, &converted)?
                            }
                            None => {
                                return Ok(self
                                    .lex
                                    .fail_at_char_end(s, JsonError::UnicodeUntranslatable))
                            }
                        }
                    }
                } else {
                    if hi_surrogate != -1 {
                        return Ok(self
                            .lex
                            .fail_at_char_end(s, JsonError::UnicodeLowSurrogate));
                    }
                    match input[s] {
                        c @ (b'"' | b'\\' | b'/') => self.strval.push(c),
                        b'b' => self.strval.push(0x08),
                        b'f' => self.strval.push(0x0c),
                        b'n' => self.strval.push(b'\n'),
                        b'r' => self.strval.push(b'\r'),
                        b't' => self.strval.push(b'\t'),
                        _ => {
                            self.lex.token_start = Some(s);
                            return Ok(self.lex.fail_at_char_end(s, JsonError::EscapingInvalid));
                        }
                    }
                }
            } else {
                if hi_surrogate != -1 {
                    return Ok(self
                        .lex
                        .fail_at_char_end(s, JsonError::UnicodeLowSurrogate));
                }
                let mut p = s;
                while p < end {
                    let c = input[p];
                    if c == b'\\' || c == b'"' {
                        break;
                    } else if c <= 31 {
                        self.lex.token_terminator = p;
                        return Ok(JsonError::EscapingRequired);
                    }
                    p += 1;
                }
                mcx::vec_append_bytes(&mut self.strval, &input[s..p])?;
                s = p - 1;
            }
        }
        if hi_surrogate != -1 {
            self.lex.token_terminator = s + 1;
            return Ok(JsonError::UnicodeLowSurrogate);
        }
        self.lex.token_terminator = s + 1;
        Ok(JsonError::Success)
    }

    fn strval_in_arena(&mut self) -> PgResult<&'mcx [u8]> {
        if !self.need_escapes {
            return Ok(&[]);
        }
        Ok(mcx::slice_in(self.mcx, &self.strval)?.leak())
    }

    fn token_in_arena(&mut self) -> PgResult<&'mcx [u8]> {
        let start = self.lex.token_start.unwrap_or(self.lex.token_terminator);
        Ok(mcx::slice_in(self.mcx, &self.lex.input[start..self.lex.token_terminator])?.leak())
    }

    fn lex_expect(&mut self, ctx: ParseCtx, token: JsonToken) -> PgResult<JsonError> {
        if self.lex.token_type == token {
            self.lex()
        } else {
            Ok(self.lex.report_parse_error(ctx))
        }
    }
}

/// C: pg_parse_json with semantic actions (need_escapes=true).
pub fn parse_sem<'mcx>(
    lex: &mut JsonLexDe<'_, 'mcx>,
    sem: &mut impl JsonSem<'mcx>,
) -> PgResult<JsonError> {
    let r = lex.lex()?;
    if r != JsonError::Success {
        return Ok(r);
    }
    let result = match lex.lex.token_type {
        JsonToken::ObjectStart => parse_object_sem(lex, sem)?,
        JsonToken::ArrayStart => parse_array_sem(lex, sem)?,
        _ => parse_scalar_sem(lex, sem)?,
    };
    if result != JsonError::Success {
        return Ok(result);
    }
    lex.lex_expect(ParseCtx::End, JsonToken::End)
}

fn parse_scalar_sem<'mcx>(
    lex: &mut JsonLexDe<'_, 'mcx>,
    sem: &mut impl JsonSem<'mcx>,
) -> PgResult<JsonError> {
    // C: parse_scalar copies the value before consuming the token (the next
    // lex clobbers strval), then invokes the callback.
    let tok = match lex.lex.token_type {
        JsonToken::String => JsonSemToken::String(lex.strval_in_arena()?),
        JsonToken::Number => JsonSemToken::Number(lex.token_in_arena()?),
        JsonToken::True => JsonSemToken::True,
        JsonToken::False => JsonSemToken::False,
        JsonToken::Null => JsonSemToken::Null,
        _ => return Ok(lex.lex.report_parse_error(ParseCtx::Value)),
    };
    let r = lex.lex()?;
    if r != JsonError::Success {
        return Ok(r);
    }
    if !sem.scalar(&lex.lex, tok)? {
        return Ok(JsonError::SemActionFailed);
    }
    Ok(JsonError::Success)
}

fn parse_object_field_sem<'mcx>(
    lex: &mut JsonLexDe<'_, 'mcx>,
    sem: &mut impl JsonSem<'mcx>,
) -> PgResult<JsonError> {
    if lex.lex.token_type != JsonToken::String {
        return Ok(lex.lex.report_parse_error(ParseCtx::String));
    }
    let fname = lex.strval_in_arena()?;
    let r = lex.lex()?;
    if r != JsonError::Success {
        return Ok(r);
    }
    let r = lex.lex_expect(ParseCtx::ObjectLabel, JsonToken::Colon)?;
    if r != JsonError::Success {
        return Ok(r);
    }
    let isnull = lex.lex.token_type == JsonToken::Null;
    if !sem.object_field_start(&lex.lex, fname, isnull)? {
        return Ok(JsonError::SemActionFailed);
    }
    let r = match lex.lex.token_type {
        JsonToken::ObjectStart => parse_object_sem(lex, sem)?,
        JsonToken::ArrayStart => parse_array_sem(lex, sem)?,
        _ => parse_scalar_sem(lex, sem)?,
    };
    if r != JsonError::Success {
        return Ok(r);
    }
    if !sem.object_field_end(&lex.lex, fname, isnull)? {
        return Ok(JsonError::SemActionFailed);
    }
    Ok(JsonError::Success)
}

fn parse_object_sem<'mcx>(
    lex: &mut JsonLexDe<'_, 'mcx>,
    sem: &mut impl JsonSem<'mcx>,
) -> PgResult<JsonError> {
    check_stack_depth()?;

    if !sem.object_start(&lex.lex)? {
        return Ok(JsonError::SemActionFailed);
    }
    lex.lex.lex_level += 1;

    let r = lex.lex()?;
    if r != JsonError::Success {
        return Ok(r);
    }

    let mut result = match lex.lex.token_type {
        JsonToken::String => {
            let mut result = parse_object_field_sem(lex, sem)?;
            while result == JsonError::Success && lex.lex.token_type == JsonToken::Comma {
                result = lex.lex()?;
                if result != JsonError::Success {
                    break;
                }
                result = parse_object_field_sem(lex, sem)?;
            }
            result
        }
        JsonToken::ObjectEnd => JsonError::Success,
        _ => lex.lex.report_parse_error(ParseCtx::ObjectStart),
    };
    if result != JsonError::Success {
        return Ok(result);
    }

    result = lex.lex_expect(ParseCtx::ObjectNext, JsonToken::ObjectEnd)?;
    if result != JsonError::Success {
        return Ok(result);
    }

    lex.lex.lex_level -= 1;
    if !sem.object_end(&lex.lex)? {
        return Ok(JsonError::SemActionFailed);
    }
    Ok(JsonError::Success)
}

fn parse_array_element_sem<'mcx>(
    lex: &mut JsonLexDe<'_, 'mcx>,
    sem: &mut impl JsonSem<'mcx>,
) -> PgResult<JsonError> {
    let isnull = lex.lex.token_type == JsonToken::Null;
    if !sem.array_element_start(&lex.lex, isnull)? {
        return Ok(JsonError::SemActionFailed);
    }
    let r = match lex.lex.token_type {
        JsonToken::ObjectStart => parse_object_sem(lex, sem)?,
        JsonToken::ArrayStart => parse_array_sem(lex, sem)?,
        _ => parse_scalar_sem(lex, sem)?,
    };
    if r != JsonError::Success {
        return Ok(r);
    }
    if !sem.array_element_end(&lex.lex, isnull)? {
        return Ok(JsonError::SemActionFailed);
    }
    Ok(JsonError::Success)
}

fn parse_array_sem<'mcx>(
    lex: &mut JsonLexDe<'_, 'mcx>,
    sem: &mut impl JsonSem<'mcx>,
) -> PgResult<JsonError> {
    check_stack_depth()?;

    if !sem.array_start(&lex.lex)? {
        return Ok(JsonError::SemActionFailed);
    }
    lex.lex.lex_level += 1;

    let mut result = lex.lex_expect(ParseCtx::ArrayStart, JsonToken::ArrayStart)?;
    if result == JsonError::Success && lex.lex.token_type != JsonToken::ArrayEnd {
        result = parse_array_element_sem(lex, sem)?;
        while result == JsonError::Success && lex.lex.token_type == JsonToken::Comma {
            result = lex.lex()?;
            if result != JsonError::Success {
                break;
            }
            result = parse_array_element_sem(lex, sem)?;
        }
    }
    if result != JsonError::Success {
        return Ok(result);
    }

    result = lex.lex_expect(ParseCtx::ArrayNext, JsonToken::ArrayEnd)?;
    if result != JsonError::Success {
        return Ok(result);
    }

    lex.lex.lex_level -= 1;
    if !sem.array_end(&lex.lex)? {
        return Ok(JsonError::SemActionFailed);
    }
    Ok(JsonError::Success)
}
