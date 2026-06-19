//! Port of PostgreSQL's `src/common/jsonapi.c` — the JSON lexer and
//! recursive-descent parser — wired for the **backend** build path
//! (`#ifndef FRONTEND`): `\uXXXX` escapes are converted to the server encoding
//! via `pg_unicode_to_server_noerror`, and the recursion guard
//! `check_stack_depth` is honored.
//!
//! The lexer keeps the input as a borrowed `&[u8]` and tracks positions as
//! `usize` byte offsets (C threads `char *` cursors into the immutable input).
//! The recursive-descent driver is generic over a [`SaxSink`] trait so the same
//! parse loop drives every consumer: the `types_json::JsonSemAction` boxed-
//! closure table (`pg_parse_json` seam), the `jsonb_in_*` assembly callbacks
//! (`parse_to_jsonb` seam), and pure validation / unique-key checking.
//!
//! The incremental parser (`pg_parse_json_incremental` and the table-driven
//! machinery) is the non-backend streaming path used only by the WAL/COPY
//! frontends; it is not reachable from the backend `json`/`jsonb` input
//! routines this unit exists to unblock, and is omitted here. The backend
//! drives only the recursive-descent `pg_parse_json`.

#![allow(clippy::needless_range_loop)]

extern crate alloc;

use alloc::vec::Vec;

use common_wchar::{pg_encoding_mblen_or_incomplete, pg_utf_mblen_private, PG_UTF8};
use mcx::{Mcx, PgVec};
use types_error::{PgError, PgResult};
use types_json::{
    JsonLexContext as TjLexContext, JsonParseErrorType as TjErr, JsonSemAction as TjSem,
    JsonTokenType as TjTok,
};

// ---------------------------------------------------------------------------
// Internal token / error / context enums (1:1 with src/common/jsonapi.h).
// These are the lexer's own working types; the seam boundary exchanges the
// `types_json` mirrors and conversion happens at the edges.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
enum JsonTokenType {
    Invalid = 0,
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
#[allow(dead_code)] // InvalidLexerType (incremental-only) and UnicodeHighEscape
                    // (frontend-only) round-trip through tj_err but the backend
                    // recursive path never produces them.
enum JsonParseErrorType {
    Success,
    InvalidLexerType,
    NestingTooDeep,
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
    OutOfMemory,
    UnicodeCodePointZero,
    UnicodeEscapeFormat,
    UnicodeHighEscape,
    UnicodeUntranslatable,
    UnicodeHighSurrogate,
    UnicodeLowSurrogate,
    SemActionFailed,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum JsonParseContext {
    Value,
    String,
    ArrayStart,
    ArrayNext,
    ObjectStart,
    ObjectLabel,
    ObjectNext,
    End,
}

// ---------------------------------------------------------------------------
// SaxSink — the internal semantic-action interface the recursive-descent
// driver calls. Mirrors C's `JsonSemAction` function-pointer table. Each
// concrete consumer (closures / jsonb / unique-check / null) implements it.
//
// String/scalar callbacks receive the de-escaped or raw token bytes as
// `Option<&[u8]>` (`None` mirrors the C `NULL` when `need_escapes` is false).
// The live `&JsonLexContext` is threaded so callbacks can snapshot position.
// ---------------------------------------------------------------------------

trait SaxSink {
    fn object_start(&mut self, _lex: &JsonLexContext) -> JsonParseErrorType {
        JsonParseErrorType::Success
    }
    fn object_end(&mut self, _lex: &JsonLexContext) -> JsonParseErrorType {
        JsonParseErrorType::Success
    }
    fn array_start(&mut self, _lex: &JsonLexContext) -> JsonParseErrorType {
        JsonParseErrorType::Success
    }
    fn array_end(&mut self, _lex: &JsonLexContext) -> JsonParseErrorType {
        JsonParseErrorType::Success
    }
    fn object_field_start(
        &mut self,
        _lex: &JsonLexContext,
        _fname: Option<&[u8]>,
        _isnull: bool,
    ) -> JsonParseErrorType {
        JsonParseErrorType::Success
    }
    fn object_field_end(
        &mut self,
        _lex: &JsonLexContext,
        _fname: Option<&[u8]>,
        _isnull: bool,
    ) -> JsonParseErrorType {
        JsonParseErrorType::Success
    }
    fn array_element_start(&mut self, _lex: &JsonLexContext, _isnull: bool) -> JsonParseErrorType {
        JsonParseErrorType::Success
    }
    fn array_element_end(&mut self, _lex: &JsonLexContext, _isnull: bool) -> JsonParseErrorType {
        JsonParseErrorType::Success
    }
    fn scalar(
        &mut self,
        _lex: &JsonLexContext,
        _token: Option<&[u8]>,
        _tokentype: JsonTokenType,
    ) -> JsonParseErrorType {
        JsonParseErrorType::Success
    }
}

/// The null action object used for pure validation (`nullSemAction`).
struct NullSink;
impl SaxSink for NullSink {}

// ---------------------------------------------------------------------------
// JsonLexContext (the lexer's running state).
// ---------------------------------------------------------------------------

struct JsonLexContext<'a> {
    input: &'a [u8],
    input_length: usize,
    input_encoding: i32,
    token_start: Option<usize>,
    token_terminator: usize,
    prev_token_terminator: Option<usize>,
    token_type: JsonTokenType,
    lex_level: i32,
    line_number: i32,
    line_start: usize,
    need_escapes: bool,
    /// De-escaped lexeme accumulator (only present when `need_escapes`).
    strval: Option<Vec<u8>>,
    /// Sticky out-of-memory sentinel (`&failed_oom` in C).
    failed_oom: bool,
}

impl<'a> JsonLexContext<'a> {
    #[inline]
    fn lex_peek(&self) -> JsonTokenType {
        self.token_type
    }
    #[inline]
    fn end(&self) -> usize {
        self.input_length
    }
}

/// `makeJsonLexContextCstringLen` — set up a context for recursive-descent
/// parsing over `json`.
fn make_json_lex_context_cstring_len(
    json: &[u8],
    encoding: i32,
    need_escapes: bool,
) -> JsonLexContext<'_> {
    JsonLexContext {
        input: json,
        input_length: json.len(),
        input_encoding: encoding,
        token_start: None,
        token_terminator: 0,
        prev_token_terminator: None,
        token_type: JsonTokenType::Invalid,
        lex_level: 0,
        line_number: 1,
        line_start: 0,
        need_escapes,
        strval: if need_escapes { Some(Vec::new()) } else { None },
        failed_oom: false,
    }
}

// ---------------------------------------------------------------------------
// JSON_ALPHANUMERIC_CHAR
// ---------------------------------------------------------------------------

#[inline]
fn json_alphanumeric_char(c: u8) -> bool {
    c.is_ascii_lowercase()
        || c.is_ascii_uppercase()
        || c.is_ascii_digit()
        || c == b'_'
        || (c & 0x80) != 0
}

// ---------------------------------------------------------------------------
// IsValidJsonNumber
// ---------------------------------------------------------------------------

/// `IsValidJsonNumber` — true if `s` is a valid JSON number.
pub fn is_valid_json_number(s: &[u8]) -> bool {
    let len = s.len();
    if len == 0 {
        return false;
    }
    let (input, input_length): (&[u8], usize) = if s[0] == b'-' {
        (&s[1..], len - 1)
    } else {
        (s, len)
    };

    let mut dummy_lex = JsonLexContext {
        input,
        input_length,
        input_encoding: 0,
        token_start: Some(0),
        token_terminator: 0,
        prev_token_terminator: None,
        token_type: JsonTokenType::Invalid,
        lex_level: 0,
        line_number: 0,
        line_start: 0,
        need_escapes: false,
        strval: None,
        failed_oom: false,
    };

    let mut numeric_error = false;
    let mut total_len: usize = 0;
    let _ = json_lex_number(
        &mut dummy_lex,
        0,
        Some(&mut numeric_error),
        Some(&mut total_len),
    );
    (!numeric_error) && (total_len == input_length)
}

// ---------------------------------------------------------------------------
// pg_parse_json — recursive descent entry point
// ---------------------------------------------------------------------------

fn pg_parse_json(lex: &mut JsonLexContext<'_>, sem: &mut dyn SaxSink) -> JsonParseErrorType {
    let _ = take_stack_error();
    if lex.failed_oom {
        return JsonParseErrorType::OutOfMemory;
    }

    let mut result = json_lex(lex);
    if result != JsonParseErrorType::Success {
        return result;
    }

    let tok = lex.lex_peek();
    result = match tok {
        JsonTokenType::ObjectStart => parse_object(lex, sem),
        JsonTokenType::ArrayStart => parse_array(lex, sem),
        _ => parse_scalar(lex, sem),
    };

    if result == JsonParseErrorType::Success {
        result = lex_expect(JsonParseContext::End, lex, JsonTokenType::End);
    }
    result
}

fn lex_expect(
    ctx: JsonParseContext,
    lex: &mut JsonLexContext<'_>,
    token: JsonTokenType,
) -> JsonParseErrorType {
    if lex.lex_peek() == token {
        json_lex(lex)
    } else {
        report_parse_error(ctx, lex)
    }
}

// ---------------------------------------------------------------------------
// json_count_array_elements
// ---------------------------------------------------------------------------

fn json_count_array_elements(
    lex: &JsonLexContext<'_>,
) -> Result<i32, JsonParseErrorType> {
    if lex.failed_oom {
        return Err(JsonParseErrorType::OutOfMemory);
    }

    let mut copylex = JsonLexContext {
        input: lex.input,
        input_length: lex.input_length,
        input_encoding: lex.input_encoding,
        token_start: lex.token_start,
        token_terminator: lex.token_terminator,
        prev_token_terminator: lex.prev_token_terminator,
        token_type: lex.token_type,
        lex_level: lex.lex_level + 1,
        line_number: lex.line_number,
        line_start: lex.line_start,
        need_escapes: false,
        strval: None,
        failed_oom: false,
    };

    let mut count: i32 = 0;
    let mut null_sem = NullSink;

    let mut result = lex_expect(
        JsonParseContext::ArrayStart,
        &mut copylex,
        JsonTokenType::ArrayStart,
    );
    if result != JsonParseErrorType::Success {
        return Err(result);
    }
    if copylex.lex_peek() != JsonTokenType::ArrayEnd {
        loop {
            count += 1;
            result = parse_array_element(&mut copylex, &mut null_sem);
            if result != JsonParseErrorType::Success {
                return Err(result);
            }
            if copylex.token_type != JsonTokenType::Comma {
                break;
            }
            result = json_lex(&mut copylex);
            if result != JsonParseErrorType::Success {
                return Err(result);
            }
        }
    }
    result = lex_expect(
        JsonParseContext::ArrayNext,
        &mut copylex,
        JsonTokenType::ArrayEnd,
    );
    if result != JsonParseErrorType::Success {
        return Err(result);
    }
    Ok(count)
}

// ---------------------------------------------------------------------------
// Recursive descent parse routines
// ---------------------------------------------------------------------------

/// Snapshot the raw token bytes (`token_start..token_terminator`). Returns
/// `None` on OOM (mirrors `ALLOC(tlen + 1)`).
fn raw_token(lex: &JsonLexContext<'_>) -> Option<Vec<u8>> {
    let start = lex.token_start?;
    let end = lex.token_terminator;
    let mut v: Vec<u8> = Vec::new();
    v.try_reserve(end - start).ok()?;
    v.extend_from_slice(&lex.input[start..end]);
    Some(v)
}

fn strval_copy(lex: &JsonLexContext<'_>) -> Option<Vec<u8>> {
    let bytes = lex.strval.as_ref()?;
    let mut v: Vec<u8> = Vec::new();
    v.try_reserve(bytes.len()).ok()?;
    v.extend_from_slice(bytes);
    Some(v)
}

fn parse_scalar(lex: &mut JsonLexContext<'_>, sem: &mut dyn SaxSink) -> JsonParseErrorType {
    let tok = lex.lex_peek();

    if tok != JsonTokenType::String
        && tok != JsonTokenType::Number
        && tok != JsonTokenType::True
        && tok != JsonTokenType::False
        && tok != JsonTokenType::Null
    {
        return report_parse_error(JsonParseContext::Value, lex);
    }

    let val: Option<Vec<u8>> = if lex.lex_peek() == JsonTokenType::String {
        if lex.need_escapes {
            match strval_copy(lex) {
                Some(s) => Some(s),
                None => return JsonParseErrorType::OutOfMemory,
            }
        } else {
            None
        }
    } else {
        match raw_token(lex) {
            Some(s) => Some(s),
            None => return JsonParseErrorType::OutOfMemory,
        }
    };

    let result = json_lex(lex);
    if result != JsonParseErrorType::Success {
        return result;
    }

    sem.scalar(lex, val.as_deref(), tok)
}

fn parse_object_field(lex: &mut JsonLexContext<'_>, sem: &mut dyn SaxSink) -> JsonParseErrorType {
    if lex.lex_peek() != JsonTokenType::String {
        return report_parse_error(JsonParseContext::String, lex);
    }

    let fname: Option<Vec<u8>> = if lex.need_escapes {
        match strval_copy(lex) {
            Some(s) => Some(s),
            None => return JsonParseErrorType::OutOfMemory,
        }
    } else {
        None
    };

    let mut result = json_lex(lex);
    if result != JsonParseErrorType::Success {
        return result;
    }

    result = lex_expect(JsonParseContext::ObjectLabel, lex, JsonTokenType::Colon);
    if result != JsonParseErrorType::Success {
        return result;
    }

    let tok = lex.lex_peek();
    let isnull = tok == JsonTokenType::Null;

    result = sem.object_field_start(lex, fname.as_deref(), isnull);
    if result != JsonParseErrorType::Success {
        return result;
    }

    result = match tok {
        JsonTokenType::ObjectStart => parse_object(lex, sem),
        JsonTokenType::ArrayStart => parse_array(lex, sem),
        _ => parse_scalar(lex, sem),
    };
    if result != JsonParseErrorType::Success {
        return result;
    }

    sem.object_field_end(lex, fname.as_deref(), isnull)
}

fn parse_object(lex: &mut JsonLexContext<'_>, sem: &mut dyn SaxSink) -> JsonParseErrorType {
    // Backend recursion guard (#ifndef FRONTEND check_stack_depth()).
    if let Err(()) = check_stack_depth() {
        return JsonParseErrorType::NestingTooDeep;
    }

    let mut result = sem.object_start(lex);
    if result != JsonParseErrorType::Success {
        return result;
    }

    lex.lex_level += 1;

    debug_assert!(lex.lex_peek() == JsonTokenType::ObjectStart);
    result = json_lex(lex);
    if result != JsonParseErrorType::Success {
        return result;
    }

    let tok = lex.lex_peek();
    match tok {
        JsonTokenType::String => {
            result = parse_object_field(lex, sem);
            while result == JsonParseErrorType::Success && lex.lex_peek() == JsonTokenType::Comma {
                result = json_lex(lex);
                if result != JsonParseErrorType::Success {
                    break;
                }
                result = parse_object_field(lex, sem);
            }
        }
        JsonTokenType::ObjectEnd => {}
        _ => {
            result = report_parse_error(JsonParseContext::ObjectStart, lex);
        }
    }
    if result != JsonParseErrorType::Success {
        return result;
    }

    result = lex_expect(JsonParseContext::ObjectNext, lex, JsonTokenType::ObjectEnd);
    if result != JsonParseErrorType::Success {
        return result;
    }

    lex.lex_level -= 1;
    sem.object_end(lex)
}

fn parse_array_element(lex: &mut JsonLexContext<'_>, sem: &mut dyn SaxSink) -> JsonParseErrorType {
    let tok = lex.lex_peek();
    let isnull = tok == JsonTokenType::Null;

    let mut result = sem.array_element_start(lex, isnull);
    if result != JsonParseErrorType::Success {
        return result;
    }

    result = match tok {
        JsonTokenType::ObjectStart => parse_object(lex, sem),
        JsonTokenType::ArrayStart => parse_array(lex, sem),
        _ => parse_scalar(lex, sem),
    };
    if result != JsonParseErrorType::Success {
        return result;
    }

    sem.array_element_end(lex, isnull)
}

fn parse_array(lex: &mut JsonLexContext<'_>, sem: &mut dyn SaxSink) -> JsonParseErrorType {
    // Backend recursion guard (#ifndef FRONTEND check_stack_depth()).
    if let Err(()) = check_stack_depth() {
        return JsonParseErrorType::NestingTooDeep;
    }

    let mut result = sem.array_start(lex);
    if result != JsonParseErrorType::Success {
        return result;
    }

    lex.lex_level += 1;

    result = lex_expect(JsonParseContext::ArrayStart, lex, JsonTokenType::ArrayStart);
    if result == JsonParseErrorType::Success && lex.lex_peek() != JsonTokenType::ArrayEnd {
        result = parse_array_element(lex, sem);

        while result == JsonParseErrorType::Success && lex.lex_peek() == JsonTokenType::Comma {
            result = json_lex(lex);
            if result != JsonParseErrorType::Success {
                break;
            }
            result = parse_array_element(lex, sem);
        }
    }
    if result != JsonParseErrorType::Success {
        return result;
    }

    result = lex_expect(JsonParseContext::ArrayNext, lex, JsonTokenType::ArrayEnd);
    if result != JsonParseErrorType::Success {
        return result;
    }

    lex.lex_level -= 1;
    sem.array_end(lex)
}

// ---------------------------------------------------------------------------
// json_lex — lex one token
// ---------------------------------------------------------------------------

fn json_lex(lex: &mut JsonLexContext<'_>) -> JsonParseErrorType {
    if lex.failed_oom {
        return JsonParseErrorType::OutOfMemory;
    }

    let s_start = lex.token_terminator;
    let end = lex.end();
    let mut s = s_start;

    // Skip leading whitespace.
    while s < end {
        let c = lex.input[s];
        if c == b' ' || c == b'\t' || c == b'\n' || c == b'\r' {
            if c == b'\n' {
                lex.line_number += 1;
                lex.line_start = s + 1;
            }
            s += 1;
        } else {
            break;
        }
    }
    lex.token_start = Some(s);

    if s >= end {
        lex.token_start = None;
        lex.prev_token_terminator = Some(lex.token_terminator);
        lex.token_terminator = s;
        lex.token_type = JsonTokenType::End;
    } else {
        let result = match lex.input[s] {
            b'{' => {
                single_char_token(lex, s, JsonTokenType::ObjectStart);
                JsonParseErrorType::Success
            }
            b'}' => {
                single_char_token(lex, s, JsonTokenType::ObjectEnd);
                JsonParseErrorType::Success
            }
            b'[' => {
                single_char_token(lex, s, JsonTokenType::ArrayStart);
                JsonParseErrorType::Success
            }
            b']' => {
                single_char_token(lex, s, JsonTokenType::ArrayEnd);
                JsonParseErrorType::Success
            }
            b',' => {
                single_char_token(lex, s, JsonTokenType::Comma);
                JsonParseErrorType::Success
            }
            b':' => {
                single_char_token(lex, s, JsonTokenType::Colon);
                JsonParseErrorType::Success
            }
            b'"' => {
                let r = json_lex_string(lex);
                if r != JsonParseErrorType::Success {
                    return r;
                }
                lex.token_type = JsonTokenType::String;
                JsonParseErrorType::Success
            }
            b'-' => {
                let r = json_lex_number(lex, s + 1, None, None);
                if r != JsonParseErrorType::Success {
                    return r;
                }
                lex.token_type = JsonTokenType::Number;
                JsonParseErrorType::Success
            }
            b'0'..=b'9' => {
                let r = json_lex_number(lex, s, None, None);
                if r != JsonParseErrorType::Success {
                    return r;
                }
                lex.token_type = JsonTokenType::Number;
                JsonParseErrorType::Success
            }
            _ => {
                let mut p = s;
                while p < end && json_alphanumeric_char(lex.input[p]) {
                    p += 1;
                }

                if p == s {
                    lex.prev_token_terminator = Some(lex.token_terminator);
                    lex.token_terminator = s + 1;
                    return JsonParseErrorType::InvalidToken;
                }

                lex.prev_token_terminator = Some(lex.token_terminator);
                lex.token_terminator = p;
                let word = &lex.input[s..p];
                if p - s == 4 {
                    if word == b"true" {
                        lex.token_type = JsonTokenType::True;
                    } else if word == b"null" {
                        lex.token_type = JsonTokenType::Null;
                    } else {
                        return JsonParseErrorType::InvalidToken;
                    }
                } else if p - s == 5 && word == b"false" {
                    lex.token_type = JsonTokenType::False;
                } else {
                    return JsonParseErrorType::InvalidToken;
                }
                JsonParseErrorType::Success
            }
        };
        if result != JsonParseErrorType::Success {
            return result;
        }
    }

    JsonParseErrorType::Success
}

#[inline]
fn single_char_token(lex: &mut JsonLexContext<'_>, s: usize, ty: JsonTokenType) {
    lex.prev_token_terminator = Some(lex.token_terminator);
    lex.token_terminator = s + 1;
    lex.token_type = ty;
}

// ---------------------------------------------------------------------------
// json_lex_string
// ---------------------------------------------------------------------------

fn json_lex_string(lex: &mut JsonLexContext<'_>) -> JsonParseErrorType {
    let end = lex.end();
    let mut hi_surrogate: i32 = -1;

    if lex.need_escapes {
        if let Some(sv) = lex.strval.as_mut() {
            sv.clear();
        } else {
            lex.strval = Some(Vec::new());
        }
    }

    debug_assert!(lex.input_length > 0);
    let mut s = lex.token_start.expect("token_start");

    loop {
        s += 1;
        if s >= end {
            return fail_at_char_end(lex, s, end, JsonParseErrorType::InvalidToken);
        } else if lex.input[s] == b'"' {
            break;
        } else if lex.input[s] == b'\\' {
            s += 1;
            if s >= end {
                return fail_at_char_end(lex, s, end, JsonParseErrorType::InvalidToken);
            } else if lex.input[s] == b'u' {
                let mut ch: i32 = 0;
                for _i in 1..=4 {
                    s += 1;
                    if s >= end {
                        return fail_at_char_end(lex, s, end, JsonParseErrorType::InvalidToken);
                    }
                    let c = lex.input[s];
                    if c.is_ascii_digit() {
                        ch = (ch * 16) + (c - b'0') as i32;
                    } else if (b'a'..=b'f').contains(&c) {
                        ch = (ch * 16) + (c - b'a') as i32 + 10;
                    } else if (b'A'..=b'F').contains(&c) {
                        ch = (ch * 16) + (c - b'A') as i32 + 10;
                    } else {
                        return fail_at_char_end(lex, s, end, JsonParseErrorType::UnicodeEscapeFormat);
                    }
                }
                if lex.need_escapes {
                    if is_utf16_surrogate_first(ch) {
                        if hi_surrogate != -1 {
                            return fail_at_char_end(lex, s, end, JsonParseErrorType::UnicodeHighSurrogate);
                        }
                        hi_surrogate = ch;
                        continue;
                    } else if is_utf16_surrogate_second(ch) {
                        if hi_surrogate == -1 {
                            return fail_at_char_end(lex, s, end, JsonParseErrorType::UnicodeLowSurrogate);
                        }
                        ch = surrogate_pair_to_codepoint(hi_surrogate, ch);
                        hi_surrogate = -1;
                    }

                    if hi_surrogate != -1 {
                        return fail_at_char_end(lex, s, end, JsonParseErrorType::UnicodeLowSurrogate);
                    }

                    if ch == 0 {
                        return fail_at_char_end(lex, s, end, JsonParseErrorType::UnicodeCodePointZero);
                    }

                    // Backend path: let pg_unicode_to_server_noerror handle any
                    // required character-set conversion (#ifndef FRONTEND).
                    match unicode_to_server(ch as u32) {
                        Some(bytes) => {
                            if append_binary(strval_mut(lex), &bytes).is_err() {
                                lex.failed_oom = true;
                                return JsonParseErrorType::OutOfMemory;
                            }
                        }
                        None => {
                            return fail_at_char_end(lex, s, end, JsonParseErrorType::UnicodeUntranslatable);
                        }
                    }
                }
            } else if lex.need_escapes {
                if hi_surrogate != -1 {
                    return fail_at_char_end(lex, s, end, JsonParseErrorType::UnicodeLowSurrogate);
                }

                let esc = lex.input[s];
                let appended = match esc {
                    b'"' | b'\\' | b'/' => esc,
                    b'b' => b'\x08',
                    b'f' => b'\x0c',
                    b'n' => b'\n',
                    b'r' => b'\r',
                    b't' => b'\t',
                    _ => {
                        lex.token_start = Some(s);
                        return fail_at_char_end(lex, s, end, JsonParseErrorType::EscapingInvalid);
                    }
                };
                if append_char(strval_mut(lex), appended).is_err() {
                    lex.failed_oom = true;
                    return JsonParseErrorType::OutOfMemory;
                }
            } else if !matches!(lex.input[s], b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't')
            {
                lex.token_start = Some(s);
                return fail_at_char_end(lex, s, end, JsonParseErrorType::EscapingInvalid);
            }
        } else {
            if hi_surrogate != -1 {
                return fail_at_char_end(lex, s, end, JsonParseErrorType::UnicodeLowSurrogate);
            }

            let mut p = s;
            while p < end {
                let c = lex.input[p];
                if c == b'\\' || c == b'"' {
                    break;
                } else if c <= 31 {
                    lex.token_terminator = p;
                    return JsonParseErrorType::EscapingRequired;
                }
                p += 1;
            }

            if lex.need_escapes {
                let input = lex.input;
                if append_binary(strval_mut(lex), &input[s..p]).is_err() {
                    lex.failed_oom = true;
                    return JsonParseErrorType::OutOfMemory;
                }
            }

            s = p - 1;
        }
    }

    if hi_surrogate != -1 {
        lex.token_terminator = s + 1;
        return JsonParseErrorType::UnicodeLowSurrogate;
    }

    lex.prev_token_terminator = Some(lex.token_terminator);
    lex.token_terminator = s + 1;
    JsonParseErrorType::Success
}

/// `FAIL_AT_CHAR_END(code)` macro.
fn fail_at_char_end(
    lex: &mut JsonLexContext<'_>,
    s: usize,
    end: usize,
    code: JsonParseErrorType,
) -> JsonParseErrorType {
    let remaining = end - s;
    let charlen = pg_encoding_mblen_or_incomplete(lex.input_encoding, &lex.input[s..end]) as usize;
    lex.token_terminator = if charlen <= remaining { s + charlen } else { end };
    code
}

#[inline]
fn strval_mut<'b>(lex: &'b mut JsonLexContext<'_>) -> &'b mut Vec<u8> {
    lex.strval.as_mut().expect("strval present when need_escapes")
}

// ---------------------------------------------------------------------------
// json_lex_number
// ---------------------------------------------------------------------------

fn json_lex_number(
    lex: &mut JsonLexContext<'_>,
    s_in: usize,
    num_err: Option<&mut bool>,
    total_len: Option<&mut usize>,
) -> JsonParseErrorType {
    let mut error = false;
    let mut s = s_in;
    let mut len = s;

    if len < lex.input_length && lex.input[s] == b'0' {
        s += 1;
        len += 1;
    } else if len < lex.input_length && (b'1'..=b'9').contains(&lex.input[s]) {
        loop {
            s += 1;
            len += 1;
            if !(len < lex.input_length && lex.input[s].is_ascii_digit()) {
                break;
            }
        }
    } else {
        error = true;
    }

    if len < lex.input_length && lex.input[s] == b'.' {
        s += 1;
        len += 1;
        if len == lex.input_length || !lex.input[s].is_ascii_digit() {
            error = true;
        } else {
            loop {
                s += 1;
                len += 1;
                if !(len < lex.input_length && lex.input[s].is_ascii_digit()) {
                    break;
                }
            }
        }
    }

    if len < lex.input_length && (lex.input[s] == b'e' || lex.input[s] == b'E') {
        s += 1;
        len += 1;
        if len < lex.input_length && (lex.input[s] == b'+' || lex.input[s] == b'-') {
            s += 1;
            len += 1;
        }
        if len == lex.input_length || !lex.input[s].is_ascii_digit() {
            error = true;
        } else {
            loop {
                s += 1;
                len += 1;
                if !(len < lex.input_length && lex.input[s].is_ascii_digit()) {
                    break;
                }
            }
        }
    }

    while len < lex.input_length && json_alphanumeric_char(lex.input[s]) {
        error = true;
        s += 1;
        len += 1;
    }

    if let Some(tl) = total_len {
        *tl = len;
    }

    if let Some(ne) = num_err {
        *ne = error;
    } else {
        lex.prev_token_terminator = Some(lex.token_terminator);
        lex.token_terminator = s;
        if error {
            return JsonParseErrorType::InvalidToken;
        }
    }

    JsonParseErrorType::Success
}

// ---------------------------------------------------------------------------
// report_parse_error
// ---------------------------------------------------------------------------

fn report_parse_error(ctx: JsonParseContext, lex: &JsonLexContext<'_>) -> JsonParseErrorType {
    if lex.token_start.is_none() || lex.token_type == JsonTokenType::End {
        return JsonParseErrorType::ExpectedMore;
    }
    match ctx {
        JsonParseContext::End => JsonParseErrorType::ExpectedEnd,
        JsonParseContext::Value => JsonParseErrorType::ExpectedJson,
        JsonParseContext::String => JsonParseErrorType::ExpectedString,
        JsonParseContext::ArrayStart => JsonParseErrorType::ExpectedArrayFirst,
        JsonParseContext::ArrayNext => JsonParseErrorType::ExpectedArrayNext,
        JsonParseContext::ObjectStart => JsonParseErrorType::ExpectedObjectFirst,
        JsonParseContext::ObjectLabel => JsonParseErrorType::ExpectedColon,
        JsonParseContext::ObjectNext => JsonParseErrorType::ExpectedObjectNext,
    }
}

// ---------------------------------------------------------------------------
// json_errdetail — construct a (translated) detail message for a JSON error.
// ---------------------------------------------------------------------------

/// `json_errdetail` (backend path) — the human-readable detail string. Operates
/// over a `types_json::JsonLexContext` snapshot (the form the seam carries).
fn json_errdetail_tj(error: TjErr, lex: &TjLexContext) -> Vec<u8> {
    if error == TjErr::JSON_OUT_OF_MEMORY {
        return b"out of memory".to_vec();
    }

    // Current-token text for "%.*s"-style messages.
    let token_text = current_token_text_tj(lex);

    let mut buf: Vec<u8> = Vec::new();
    match error {
        TjErr::JSON_INCOMPLETE | TjErr::JSON_SUCCESS => {}
        TjErr::JSON_INVALID_LEXER_TYPE => {
            // Backend recursive-descent path: this is the non-incremental case.
            buf.extend_from_slice(b"Recursive descent parser cannot use incremental lexer.");
        }
        TjErr::JSON_NESTING_TOO_DEEP => {
            buf.extend_from_slice(b"JSON nested too deep, maximum permitted depth is 6400.");
        }
        TjErr::JSON_ESCAPING_INVALID => {
            json_token_error(&mut buf, b"Escape sequence \"\\", b"\" is invalid.", &token_text);
        }
        TjErr::JSON_ESCAPING_REQUIRED => {
            let byte = lex.input.get(lex.token_terminator).copied().unwrap_or(0);
            buf.extend_from_slice(b"Character with value 0x");
            buf.push(hex_digit((byte >> 4) & 0xf));
            buf.push(hex_digit(byte & 0xf));
            buf.extend_from_slice(b" must be escaped.");
        }
        TjErr::JSON_EXPECTED_END => {
            json_token_error(&mut buf, b"Expected end of input, but found \"", b"\".", &token_text);
        }
        TjErr::JSON_EXPECTED_ARRAY_FIRST => {
            json_token_error(&mut buf, b"Expected array element or \"]\", but found \"", b"\".", &token_text);
        }
        TjErr::JSON_EXPECTED_ARRAY_NEXT => {
            json_token_error(&mut buf, b"Expected \",\" or \"]\", but found \"", b"\".", &token_text);
        }
        TjErr::JSON_EXPECTED_COLON => {
            json_token_error(&mut buf, b"Expected \":\", but found \"", b"\".", &token_text);
        }
        TjErr::JSON_EXPECTED_JSON => {
            json_token_error(&mut buf, b"Expected JSON value, but found \"", b"\".", &token_text);
        }
        TjErr::JSON_EXPECTED_MORE => {
            buf.extend_from_slice(b"The input string ended unexpectedly.");
        }
        TjErr::JSON_EXPECTED_OBJECT_FIRST => {
            json_token_error(&mut buf, b"Expected string or \"}\", but found \"", b"\".", &token_text);
        }
        TjErr::JSON_EXPECTED_OBJECT_NEXT => {
            json_token_error(&mut buf, b"Expected \",\" or \"}\", but found \"", b"\".", &token_text);
        }
        TjErr::JSON_EXPECTED_STRING => {
            json_token_error(&mut buf, b"Expected string, but found \"", b"\".", &token_text);
        }
        TjErr::JSON_INVALID_TOKEN => {
            json_token_error(&mut buf, b"Token \"", b"\" is invalid.", &token_text);
        }
        TjErr::JSON_OUT_OF_MEMORY => {} // handled above
        TjErr::JSON_UNICODE_CODE_POINT_ZERO => {
            buf.extend_from_slice(b"\\u0000 cannot be converted to text.");
        }
        TjErr::JSON_UNICODE_ESCAPE_FORMAT => {
            buf.extend_from_slice(b"\"\\u\" must be followed by four hexadecimal digits.");
        }
        TjErr::JSON_UNICODE_HIGH_ESCAPE => {
            buf.extend_from_slice(b"Unicode escape values cannot be used for code point values above 007F when the encoding is not UTF8.");
        }
        TjErr::JSON_UNICODE_UNTRANSLATABLE => {
            // Backend (#ifndef FRONTEND): names the database encoding.
            buf.extend_from_slice(b"Unicode escape value could not be translated to the server's encoding ");
            buf.extend_from_slice(backend_utils_mb_mbutils::GetDatabaseEncodingName().as_bytes());
            buf.push(b'.');
        }
        TjErr::JSON_UNICODE_HIGH_SURROGATE => {
            buf.extend_from_slice(b"Unicode high surrogate must not follow a high surrogate.");
        }
        TjErr::JSON_UNICODE_LOW_SURROGATE => {
            buf.extend_from_slice(b"Unicode low surrogate must follow a high surrogate.");
        }
        TjErr::JSON_SEM_ACTION_FAILED => {}
    }

    if buf.is_empty() {
        buf.extend_from_slice(b"unexpected json parse error type: ");
        push_i32(&mut buf, error_ordinal(error));
    }
    buf
}

/// Current token bytes (`token_start..token_terminator`) of a `types_json`
/// snapshot.
fn current_token_text_tj(lex: &TjLexContext) -> Vec<u8> {
    let start = lex.token_start;
    let end = lex.token_terminator.min(lex.input.len());
    if start <= end && start <= lex.input.len() {
        lex.input[start..end].to_vec()
    } else {
        Vec::new()
    }
}

fn json_token_error(buf: &mut Vec<u8>, prefix: &[u8], suffix: &[u8], token: &[u8]) {
    buf.extend_from_slice(prefix);
    buf.extend_from_slice(token);
    buf.extend_from_slice(suffix);
}

// ---------------------------------------------------------------------------
// Small byte-buffer helpers (StringInfo append family)
// ---------------------------------------------------------------------------

fn append_char(buf: &mut Vec<u8>, c: u8) -> Result<(), ()> {
    buf.try_reserve(1).map_err(|_| ())?;
    buf.push(c);
    Ok(())
}

fn append_binary(buf: &mut Vec<u8>, data: &[u8]) -> Result<(), ()> {
    buf.try_reserve(data.len()).map_err(|_| ())?;
    buf.extend_from_slice(data);
    Ok(())
}

#[inline]
fn hex_digit(n: u8) -> u8 {
    if n < 10 {
        b'0' + n
    } else {
        b'a' + (n - 10)
    }
}

fn push_i32(s: &mut Vec<u8>, mut n: i32) {
    if n == 0 {
        s.push(b'0');
        return;
    }
    if n < 0 {
        s.push(b'-');
    }
    let mut buf = [0u8; 12];
    let mut i = buf.len();
    let mut x = (n as i64).unsigned_abs();
    while x > 0 {
        i -= 1;
        buf[i] = b'0' + (x % 10) as u8;
        x /= 10;
    }
    s.extend_from_slice(&buf[i..]);
    let _ = &mut n;
}

fn error_ordinal(e: TjErr) -> i32 {
    e as i32
}

// ---------------------------------------------------------------------------
// pg_wchar.h helpers used here
// ---------------------------------------------------------------------------

#[inline]
fn is_utf16_surrogate_first(c: i32) -> bool {
    (0xD800..=0xDBFF).contains(&c)
}
#[inline]
fn is_utf16_surrogate_second(c: i32) -> bool {
    (0xDC00..=0xDFFF).contains(&c)
}
#[inline]
fn surrogate_pair_to_codepoint(first: i32, second: i32) -> i32 {
    ((first & 0x3FF) << 10) + 0x10000 + (second & 0x3FF)
}

/// `unicode_to_utf8` (wchar.c) — encode `c` into the start of `utf8string`.
fn unicode_to_utf8(c: u32, utf8string: &mut [u8]) {
    if c <= 0x7f {
        utf8string[0] = c as u8;
    } else if c <= 0x7ff {
        utf8string[0] = (0xc0 | ((c >> 6) & 0x1f)) as u8;
        utf8string[1] = (0x80 | (c & 0x3f)) as u8;
    } else if c <= 0xffff {
        utf8string[0] = (0xe0 | ((c >> 12) & 0x0f)) as u8;
        utf8string[1] = (0x80 | ((c >> 6) & 0x3f)) as u8;
        utf8string[2] = (0x80 | (c & 0x3f)) as u8;
    } else {
        utf8string[0] = (0xf0 | ((c >> 18) & 0x07)) as u8;
        utf8string[1] = (0x80 | ((c >> 12) & 0x3f)) as u8;
        utf8string[2] = (0x80 | ((c >> 6) & 0x3f)) as u8;
        utf8string[3] = (0x80 | (c & 0x3f)) as u8;
    }
}

/// Backend `\uXXXX` -> server-encoding conversion. UTF8 (the common case, and
/// the smoke fixture's encoding) is handled inline; other encodings go through
/// `pg_unicode_to_server_noerror`, which needs an allocation arena, so a
/// throwaway scratch context backs the conversion (the bytes are copied into
/// `strval` immediately). `None` mirrors the C conversion failure that maps to
/// `JSON_UNICODE_UNTRANSLATABLE`.
fn unicode_to_server(ch: u32) -> Option<Vec<u8>> {
    let encoding = backend_utils_mb_mbutils::GetDatabaseEncoding();
    if encoding == PG_UTF8 {
        let mut utf8str = [0u8; 5];
        unicode_to_utf8(ch, &mut utf8str);
        let utf8len = pg_utf_mblen_private(&utf8str).unwrap_or(0) as usize;
        return Some(utf8str[..utf8len].to_vec());
    }
    let ctx = mcx::MemoryContext::new("jsonapi-unicode");
    let mcx = ctx.mcx();
    let out = match backend_utils_mb_mbutils::pg_unicode_to_server_noerror(mcx, ch) {
        Ok(Some(bytes)) => Some(bytes.as_slice().to_vec()),
        _ => None,
    };
    out
}

use core::cell::RefCell;

thread_local! {
    /// Holds the hard error a `check_stack_depth()` failure raised during a
    /// parse, so the sink-agnostic recursive routines can signal it (they
    /// return `JsonParseErrorType`, not `PgResult`) and the entry point can
    /// surface it as `Err` — mirroring C's `ereport(ERROR)` longjmp out of the
    /// recursive descent.
    static STACK_ERROR: RefCell<Option<PgError>> = const { RefCell::new(None) };
}

/// Backend recursion guard, mirroring C `check_stack_depth()` in the recursive
/// `parse_object`/`parse_array`. The real depth check lives in
/// `backend-utils-misc-stack-depth`; on overflow it returns the
/// `ereport(ERROR, "stack depth limit exceeded")`, which we stash for the entry
/// point and signal upward as `Err(())`.
fn check_stack_depth() -> Result<(), ()> {
    match backend_utils_misc_stack_depth_seams::check_stack_depth::call() {
        Ok(()) => Ok(()),
        Err(e) => {
            STACK_ERROR.with(|s| *s.borrow_mut() = Some(e));
            Err(())
        }
    }
}

/// Take any stack-depth error stashed during the just-finished parse.
fn take_stack_error() -> Option<PgError> {
    STACK_ERROR.with(|s| s.borrow_mut().take())
}

// ===========================================================================
// types_json bridge: build a snapshot of the lexer state for the SAX seam.
// ===========================================================================

fn tj_token(t: JsonTokenType) -> TjTok {
    match t {
        JsonTokenType::Invalid => TjTok::JSON_TOKEN_INVALID,
        JsonTokenType::String => TjTok::JSON_TOKEN_STRING,
        JsonTokenType::Number => TjTok::JSON_TOKEN_NUMBER,
        JsonTokenType::ObjectStart => TjTok::JSON_TOKEN_OBJECT_START,
        JsonTokenType::ObjectEnd => TjTok::JSON_TOKEN_OBJECT_END,
        JsonTokenType::ArrayStart => TjTok::JSON_TOKEN_ARRAY_START,
        JsonTokenType::ArrayEnd => TjTok::JSON_TOKEN_ARRAY_END,
        JsonTokenType::Comma => TjTok::JSON_TOKEN_COMMA,
        JsonTokenType::Colon => TjTok::JSON_TOKEN_COLON,
        JsonTokenType::True => TjTok::JSON_TOKEN_TRUE,
        JsonTokenType::False => TjTok::JSON_TOKEN_FALSE,
        JsonTokenType::Null => TjTok::JSON_TOKEN_NULL,
        JsonTokenType::End => TjTok::JSON_TOKEN_END,
    }
}

fn tj_err(e: JsonParseErrorType) -> TjErr {
    match e {
        JsonParseErrorType::Success => TjErr::JSON_SUCCESS,
        JsonParseErrorType::InvalidLexerType => TjErr::JSON_INVALID_LEXER_TYPE,
        JsonParseErrorType::NestingTooDeep => TjErr::JSON_NESTING_TOO_DEEP,
        JsonParseErrorType::EscapingInvalid => TjErr::JSON_ESCAPING_INVALID,
        JsonParseErrorType::EscapingRequired => TjErr::JSON_ESCAPING_REQUIRED,
        JsonParseErrorType::ExpectedArrayFirst => TjErr::JSON_EXPECTED_ARRAY_FIRST,
        JsonParseErrorType::ExpectedArrayNext => TjErr::JSON_EXPECTED_ARRAY_NEXT,
        JsonParseErrorType::ExpectedColon => TjErr::JSON_EXPECTED_COLON,
        JsonParseErrorType::ExpectedEnd => TjErr::JSON_EXPECTED_END,
        JsonParseErrorType::ExpectedJson => TjErr::JSON_EXPECTED_JSON,
        JsonParseErrorType::ExpectedMore => TjErr::JSON_EXPECTED_MORE,
        JsonParseErrorType::ExpectedObjectFirst => TjErr::JSON_EXPECTED_OBJECT_FIRST,
        JsonParseErrorType::ExpectedObjectNext => TjErr::JSON_EXPECTED_OBJECT_NEXT,
        JsonParseErrorType::ExpectedString => TjErr::JSON_EXPECTED_STRING,
        JsonParseErrorType::InvalidToken => TjErr::JSON_INVALID_TOKEN,
        JsonParseErrorType::OutOfMemory => TjErr::JSON_OUT_OF_MEMORY,
        JsonParseErrorType::UnicodeCodePointZero => TjErr::JSON_UNICODE_CODE_POINT_ZERO,
        JsonParseErrorType::UnicodeEscapeFormat => TjErr::JSON_UNICODE_ESCAPE_FORMAT,
        JsonParseErrorType::UnicodeHighEscape => TjErr::JSON_UNICODE_HIGH_ESCAPE,
        JsonParseErrorType::UnicodeUntranslatable => TjErr::JSON_UNICODE_UNTRANSLATABLE,
        JsonParseErrorType::UnicodeHighSurrogate => TjErr::JSON_UNICODE_HIGH_SURROGATE,
        JsonParseErrorType::UnicodeLowSurrogate => TjErr::JSON_UNICODE_LOW_SURROGATE,
        JsonParseErrorType::SemActionFailed => TjErr::JSON_SEM_ACTION_FAILED,
    }
}

/// Build the `types_json::JsonLexContext` snapshot the SAX callbacks observe.
fn snapshot(lex: &JsonLexContext<'_>) -> TjLexContext {
    TjLexContext {
        input: lex.input.to_vec(),
        input_length: lex.input_length,
        input_encoding: lex.input_encoding,
        token_type: tj_token(lex.token_type),
        lex_level: lex.lex_level,
        token_start: lex.token_start.unwrap_or(lex.token_terminator),
        token_terminator: lex.token_terminator,
        prev_token_terminator: lex.prev_token_terminator.unwrap_or(0),
        line_number: lex.line_number,
        line_start: lex.line_start,
    }
}

// ===========================================================================
// Sink 1: the types_json closure table (the `pg_parse_json` seam).
// ===========================================================================

/// Adapter wrapping a borrowed `types_json::JsonSemAction`. Each fired callback
/// returns `PgResult<()>`; an `Err` (raised `ereport`) is captured here and the
/// parse abandoned with `JSON_SEM_ACTION_FAILED`, exactly as C's callbacks
/// signal a raised error.
struct ClosureSink<'s, 'a> {
    sem: &'s mut TjSem<'a>,
    raised: Option<PgError>,
}

impl<'s, 'a> ClosureSink<'s, 'a> {
    fn dispatch_struct(
        &mut self,
        lex: &JsonLexContext<'_>,
        which: for<'r> fn(&'r mut TjSem<'a>) -> &'r mut Option<types_json::JsonStructAction<'a>>,
    ) -> JsonParseErrorType {
        if which(self.sem).is_some() {
            let snap = snapshot(lex);
            let cb = which(self.sem).as_mut().unwrap();
            match cb(&snap) {
                Ok(()) => JsonParseErrorType::Success,
                Err(e) => {
                    self.raised = Some(e);
                    JsonParseErrorType::SemActionFailed
                }
            }
        } else {
            JsonParseErrorType::Success
        }
    }
}

impl<'s, 'a> SaxSink for ClosureSink<'s, 'a> {
    fn object_start(&mut self, lex: &JsonLexContext) -> JsonParseErrorType {
        self.dispatch_struct(lex, |s| &mut s.object_start)
    }
    fn object_end(&mut self, lex: &JsonLexContext) -> JsonParseErrorType {
        self.dispatch_struct(lex, |s| &mut s.object_end)
    }
    fn array_start(&mut self, lex: &JsonLexContext) -> JsonParseErrorType {
        self.dispatch_struct(lex, |s| &mut s.array_start)
    }
    fn array_end(&mut self, lex: &JsonLexContext) -> JsonParseErrorType {
        self.dispatch_struct(lex, |s| &mut s.array_end)
    }
    fn object_field_start(
        &mut self,
        lex: &JsonLexContext,
        fname: Option<&[u8]>,
        isnull: bool,
    ) -> JsonParseErrorType {
        if self.sem.object_field_start.is_some() {
            let snap = snapshot(lex);
            let name = fname.unwrap_or(&[]);
            let cb = self.sem.object_field_start.as_mut().unwrap();
            match cb(&snap, name, isnull) {
                Ok(()) => JsonParseErrorType::Success,
                Err(e) => {
                    self.raised = Some(e);
                    JsonParseErrorType::SemActionFailed
                }
            }
        } else {
            JsonParseErrorType::Success
        }
    }
    fn object_field_end(
        &mut self,
        lex: &JsonLexContext,
        fname: Option<&[u8]>,
        isnull: bool,
    ) -> JsonParseErrorType {
        if self.sem.object_field_end.is_some() {
            let snap = snapshot(lex);
            let name = fname.unwrap_or(&[]);
            let cb = self.sem.object_field_end.as_mut().unwrap();
            match cb(&snap, name, isnull) {
                Ok(()) => JsonParseErrorType::Success,
                Err(e) => {
                    self.raised = Some(e);
                    JsonParseErrorType::SemActionFailed
                }
            }
        } else {
            JsonParseErrorType::Success
        }
    }
    fn array_element_start(&mut self, lex: &JsonLexContext, isnull: bool) -> JsonParseErrorType {
        if self.sem.array_element_start.is_some() {
            let snap = snapshot(lex);
            let cb = self.sem.array_element_start.as_mut().unwrap();
            match cb(&snap, isnull) {
                Ok(()) => JsonParseErrorType::Success,
                Err(e) => {
                    self.raised = Some(e);
                    JsonParseErrorType::SemActionFailed
                }
            }
        } else {
            JsonParseErrorType::Success
        }
    }
    fn array_element_end(&mut self, lex: &JsonLexContext, isnull: bool) -> JsonParseErrorType {
        if self.sem.array_element_end.is_some() {
            let snap = snapshot(lex);
            let cb = self.sem.array_element_end.as_mut().unwrap();
            match cb(&snap, isnull) {
                Ok(()) => JsonParseErrorType::Success,
                Err(e) => {
                    self.raised = Some(e);
                    JsonParseErrorType::SemActionFailed
                }
            }
        } else {
            JsonParseErrorType::Success
        }
    }
    fn scalar(
        &mut self,
        lex: &JsonLexContext,
        token: Option<&[u8]>,
        tokentype: JsonTokenType,
    ) -> JsonParseErrorType {
        if self.sem.scalar.is_some() {
            let snap = snapshot(lex);
            let tok = token.unwrap_or(&[]);
            let cb = self.sem.scalar.as_mut().unwrap();
            match cb(&snap, tok, tj_token(tokentype)) {
                Ok(()) => JsonParseErrorType::Success,
                Err(e) => {
                    self.raised = Some(e);
                    JsonParseErrorType::SemActionFailed
                }
            }
        } else {
            JsonParseErrorType::Success
        }
    }
}

/// Seam body: `makeJsonLexContext(json, need_escapes)` + `pg_parse_json(sem)`.
/// Returns the first non-success error (or `JSON_SUCCESS`); a callback that
/// raised propagates as `Err`.
fn run_pg_parse_json(
    json: &[u8],
    encoding: i32,
    need_escapes: bool,
    sem: &mut TjSem<'_>,
) -> PgResult<TjErr> {
    let mut lex = make_json_lex_context_cstring_len(json, encoding, need_escapes);
    let mut sink = ClosureSink { sem, raised: None };
    let result = pg_parse_json(&mut lex, &mut sink);
    if let Some(e) = take_stack_error() {
        return Err(e);
    }
    if let Some(e) = sink.raised.take() {
        return Err(e);
    }
    Ok(tj_err(result))
}

// ===========================================================================
// Sink 2: jsonb_in_* assembly (the `parse_to_jsonb` seam).
// ===========================================================================

struct JsonbSink<'m, 'mcx> {
    mcx: Mcx<'mcx>,
    state: &'m mut backend_utils_adt_jsonb::JsonbInState,
    raised: Option<PgError>,
}

impl<'m, 'mcx> JsonbSink<'m, 'mcx> {
    fn note(&mut self, r: PgResult<()>) -> JsonParseErrorType {
        match r {
            Ok(()) => JsonParseErrorType::Success,
            Err(e) => {
                self.raised = Some(e);
                JsonParseErrorType::SemActionFailed
            }
        }
    }
}

impl<'m, 'mcx> SaxSink for JsonbSink<'m, 'mcx> {
    fn object_start(&mut self, _lex: &JsonLexContext) -> JsonParseErrorType {
        let r = backend_utils_adt_jsonb::jsonb_in_object_start(self.state);
        self.note(r)
    }
    fn object_end(&mut self, _lex: &JsonLexContext) -> JsonParseErrorType {
        let r = backend_utils_adt_jsonb::jsonb_in_object_end(self.state);
        self.note(r)
    }
    fn array_start(&mut self, _lex: &JsonLexContext) -> JsonParseErrorType {
        let r = backend_utils_adt_jsonb::jsonb_in_array_start(self.state);
        self.note(r)
    }
    fn array_end(&mut self, _lex: &JsonLexContext) -> JsonParseErrorType {
        let r = backend_utils_adt_jsonb::jsonb_in_array_end(self.state);
        self.note(r)
    }
    fn object_field_start(
        &mut self,
        _lex: &JsonLexContext,
        fname: Option<&[u8]>,
        _isnull: bool,
    ) -> JsonParseErrorType {
        // jsonb_from_cstring uses need_escapes=true, so fname is always present.
        let name = fname.unwrap_or(&[]);
        let r = backend_utils_adt_jsonb::jsonb_in_object_field_start(self.state, name);
        self.note(r)
    }
    fn scalar(
        &mut self,
        _lex: &JsonLexContext,
        token: Option<&[u8]>,
        tokentype: JsonTokenType,
    ) -> JsonParseErrorType {
        let r = backend_utils_adt_jsonb::jsonb_in_scalar(self.mcx, self.state, token, tj_token(tokentype));
        self.note(r)
    }
}

/// Seam body for `parse_to_jsonb` — C `jsonb_from_cstring` (jsonb.c:248).
fn run_parse_to_jsonb<'mcx>(
    mcx: Mcx<'mcx>,
    json: &[u8],
    unique_keys: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    let encoding = backend_utils_mb_mbutils::GetDatabaseEncoding();
    let mut lex = make_json_lex_context_cstring_len(json, encoding, true);

    let mut state = backend_utils_adt_jsonb::JsonbInState {
        unique_keys,
        ..Default::default()
    };

    let result = {
        let mut sink = JsonbSink {
            mcx,
            state: &mut state,
            raised: None,
        };
        let r = pg_parse_json(&mut lex, &mut sink);
        if let Some(e) = take_stack_error() {
            return Err(e);
        }
        if let Some(e) = sink.raised.take() {
            return Err(e);
        }
        r
    };

    if result != JsonParseErrorType::Success {
        // pg_parse_json_or_errsave with no escontext raises.
        let snap = snapshot(&lex);
        backend_utils_adt_jsonfuncs::lex::json_errsave_error(tj_err(result), &snap, None)?;
        // json_errsave_error with no escontext never returns Ok on error.
        return Err(PgError::error("invalid input syntax for type json"));
    }

    let res = state
        .res
        .ok_or_else(|| PgError::error("jsonb parse produced no value"))?;
    backend_utils_adt_jsonb_util::JsonbValueToJsonb(mcx, &res)
}

// ===========================================================================
// Sink 3: json unique-key check (the `parse_validate_unique` seam).
// ===========================================================================

struct UniqueSink<'m> {
    state: &'m mut backend_utils_adt_json::JsonUniqueParsingState,
}

fn tj_to_internal_err(e: types_json::JsonParseErrorType) -> JsonParseErrorType {
    // The json crate's unique callbacks return the types_json error enum;
    // they only ever return JSON_SUCCESS, so map that one and treat the rest as
    // SemActionFailed (defensive; unreachable in practice).
    match e {
        types_json::JsonParseErrorType::JSON_SUCCESS => JsonParseErrorType::Success,
        _ => JsonParseErrorType::SemActionFailed,
    }
}

impl<'m> SaxSink for UniqueSink<'m> {
    fn object_start(&mut self, _lex: &JsonLexContext) -> JsonParseErrorType {
        tj_to_internal_err(backend_utils_adt_json::json_unique_object_start(self.state))
    }
    fn object_end(&mut self, _lex: &JsonLexContext) -> JsonParseErrorType {
        tj_to_internal_err(backend_utils_adt_json::json_unique_object_end(self.state))
    }
    fn object_field_start(
        &mut self,
        _lex: &JsonLexContext,
        fname: Option<&[u8]>,
        isnull: bool,
    ) -> JsonParseErrorType {
        let name = fname.unwrap_or(&[]);
        tj_to_internal_err(backend_utils_adt_json::json_unique_object_field_start(
            self.state, name, isnull,
        ))
    }
}

// ===========================================================================
// Seam installers.
// ===========================================================================

/// `parse_validate(json)` — validate `json` with the null semantic action.
///
/// C's `check_stack_depth()` inside the recursive `parse_object`/`parse_array`
/// `ereport(ERROR, "stack depth limit exceeded")`s and unwinds at once. The
/// recursive descent here cannot return a `PgError` through the
/// `JsonParseErrorType` channel, so the guard stashes the real error and signals
/// `NestingTooDeep`; surface that stashed error as `Err` here (otherwise it
/// would be mis-rendered as the incremental parser's `JSON_NESTING_TOO_DEEP`
/// "max depth 6400" detail), mirroring C's immediate raise.
fn run_parse_validate(json: &[u8]) -> PgResult<TjErr> {
    let encoding = backend_utils_mb_mbutils::GetDatabaseEncoding();
    let mut lex = make_json_lex_context_cstring_len(json, encoding, false);
    let mut sink = NullSink;
    let result = pg_parse_json(&mut lex, &mut sink);
    if let Some(e) = take_stack_error() {
        return Err(e);
    }
    Ok(tj_err(result))
}

/// `parse_validate_unique(json)` — validate `json` and report key uniqueness
/// (json.c `json_validate(check_unique_keys=true)`).
fn run_parse_validate_unique(json: &[u8]) -> PgResult<(TjErr, bool)> {
    let encoding = backend_utils_mb_mbutils::GetDatabaseEncoding();
    // need_escapes=true to de-escape keys for the uniqueness comparison.
    let mut lex = make_json_lex_context_cstring_len(json, encoding, true);
    let mut state = backend_utils_adt_json::JsonUniqueParsingState::new();
    let result = {
        let mut sink = UniqueSink { state: &mut state };
        pg_parse_json(&mut lex, &mut sink)
    };
    // Surface the recursive descent's stack-depth hard error (see
    // `run_parse_validate`) ahead of any shallow parse result.
    if let Some(e) = take_stack_error() {
        return Err(e);
    }
    Ok((tj_err(result), state.unique))
}

/// `lex_first_token(json)` — lex the first token and report its type
/// (`json_typeof` / `json_get_first_token`).
fn run_lex_first_token(json: &[u8]) -> (TjErr, TjTok) {
    let encoding = backend_utils_mb_mbutils::GetDatabaseEncoding();
    let mut lex = make_json_lex_context_cstring_len(json, encoding, false);
    let result = json_lex(&mut lex);
    (tj_err(result), tj_token(lex.token_type))
}

/// `errsave_error(error, json)` — re-lex `json` to reconstruct the lexer state
/// at the failure point, then raise the user-facing parse error (no escontext:
/// hard error).
fn run_errsave_error(error: TjErr, json: &[u8], need_escapes: bool) -> PgResult<()> {
    let encoding = backend_utils_mb_mbutils::GetDatabaseEncoding();
    // Re-lex with the SAME need_escapes the failing parse used so the lexer
    // stops at the exact token_terminator the original failure landed on
    // (need_escapes governs whether json_lex_string detects unicode-escape /
    // surrogate errors at all; using the wrong value would reposition the
    // CONTEXT excerpt at a different spot, losing C's "...":-truncation).
    let mut lex = make_json_lex_context_cstring_len(json, encoding, need_escapes);
    // Drive a validation parse to reposition the lexer at the error spot; the
    // resulting position is what json_errsave_error renders the CONTEXT from.
    let mut sink = NullSink;
    let _ = pg_parse_json(&mut lex, &mut sink);
    let snap = snapshot(&lex);
    backend_utils_adt_jsonfuncs::lex::json_errsave_error(error, &snap, None)
}

/// `json_lex_first(json, encoding)` — lex the first token, returning the result
/// and a `types_json::JsonLexContext` snapshot (drives `json_get_first_token`).
fn run_json_lex_first(json: &[u8], encoding: i32) -> PgResult<(TjErr, TjLexContext)> {
    let mut lex = make_json_lex_context_cstring_len(json, encoding, false);
    let result = json_lex(&mut lex);
    Ok((tj_err(result), snapshot(&lex)))
}

/// `json_errdetail(error, lex)` — detail string for a parse error.
fn run_json_errdetail(error: TjErr, lex: &TjLexContext) -> PgResult<Vec<u8>> {
    Ok(json_errdetail_tj(error, lex))
}

/// `json_count_array_elements(json, encoding)` — count top-level array elements
/// (`json_array_length`).
fn run_json_count_array_elements(json: &[u8], encoding: i32) -> PgResult<i32> {
    let mut lex = make_json_lex_context_cstring_len(json, encoding, false);
    // Position at the first token (the array start), as json_array_length does
    // before calling json_count_array_elements from its array_start action.
    let r = json_lex(&mut lex);
    if r != JsonParseErrorType::Success {
        // json_array_length lexes with need_escapes=false.
        return run_errsave_error(tj_err(r), json, false).map(|_| 0);
    }
    match json_count_array_elements(&lex) {
        Ok(n) => Ok(n),
        Err(e) => run_errsave_error(tj_err(e), json, false).map(|_| 0),
    }
}

/// Install every seam this unit owns and the `parse_to_jsonb` bridge it drives.
pub fn init_seams() {
    common_jsonapi_seams::parse_validate::set(run_parse_validate);
    common_jsonapi_seams::parse_validate_unique::set(run_parse_validate_unique);
    common_jsonapi_seams::lex_first_token::set(run_lex_first_token);
    common_jsonapi_seams::errsave_error::set(run_errsave_error);
    common_jsonapi_seams::pg_parse_json::set(run_pg_parse_json);
    common_jsonapi_seams::json_lex_first::set(run_json_lex_first);
    common_jsonapi_seams::json_errdetail::set(run_json_errdetail);
    common_jsonapi_seams::json_count_array_elements::set(run_json_count_array_elements);
    common_jsonapi_seams::get_database_encoding::set(|| {
        backend_utils_mb_mbutils::GetDatabaseEncoding()
    });
    common_jsonapi_seams::pg_mblen::set(|s: &[u8]| backend_utils_mb_mbutils::pg_mblen(s) as usize);

    // parse_to_jsonb is owned by jsonb-seams but driven by this lexer unit.
    backend_utils_adt_jsonb_seams::parse_to_jsonb::set(run_parse_to_jsonb);
}
