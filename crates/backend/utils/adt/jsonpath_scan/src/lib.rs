//! Port of `src/backend/utils/adt/jsonpath_scan.l` (PostgreSQL 18.3) — the
//! flex lexical parser for the `jsonpath` datatype.
//!
//! This is a *faithful* hand-written re-implementation of the flex scanner.
//! Flex picks the longest match and, among equal-length matches, the rule that
//! appears first in the file; the scanner below reproduces that behaviour
//! rule-by-rule. The exclusive states (`xq`/`xnq`/`xvq`/`xc`), the keyword
//! table + binary search (`checkKeyword`), the `scanstring` literal buffer
//! (`addstring`/`addchar`/`resizeString`), the unicode/hex escape decoding
//! (`parseUnicode`/`parseHexChar`/`addUnicode`/`addUnicodeChar`), and the
//! numeric-literal patterns (decimal/real/hex/oct/bin in ECMAScript form with
//! `_` digit separators) all match the C scanner so the token stream is
//! byte-exact.
//!
//! Soft errors flow through `escontext` exactly as the C `yyterminate()` path
//! does (`jsonpath_yyerror` records the soft error and the lexer stops). The
//! grammar driver lives in `backend-utils-adt-jsonpath-gram`; this crate
//! exposes the token stream it consumes.
//!
//! Cross-subsystem helpers are reused from the ported crates:
//!   * `pg_strncasecmp` from `port-pgstrcasecmp` (`checkKeyword`),
//!   * `pg_unicode_to_server` / `pg_unicode_to_server_noerror` from
//!     `backend-utils-mb-mbutils` (`addUnicodeChar`).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(clippy::result_large_err)]

use ::mcx::MemoryContext;
use ::pgstrcasecmp::pg_strncasecmp;
use ::types_error::{ereturn, PgError, PgResult, SoftErrorContext};
use ::types_error::{
    ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_SYNTAX_ERROR, ERRCODE_UNTRANSLATABLE_CHARACTER,
};
use ::types_jsonpath::parse::JsonPathString;

/// The bison token kinds emitted by the scanner. These mirror the
/// `enum yytokentype` produced by bison from `jsonpath_gram.y`; the names match
/// the `%token` declarations there. Single-character punctuation that the
/// grammar matches as a literal character (`{special}` → `return *yytext`) is
/// carried as [`Token::Char`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Token {
    TO_P,
    NULL_P,
    TRUE_P,
    FALSE_P,
    IS_P,
    UNKNOWN_P,
    EXISTS_P,
    IDENT_P,
    STRING_P,
    NUMERIC_P,
    INT_P,
    VARIABLE_P,
    OR_P,
    AND_P,
    NOT_P,
    LESS_P,
    LESSEQUAL_P,
    EQUAL_P,
    NOTEQUAL_P,
    GREATEREQUAL_P,
    GREATER_P,
    ANY_P,
    STRICT_P,
    LAX_P,
    LAST_P,
    STARTS_P,
    WITH_P,
    LIKE_REGEX_P,
    FLAG_P,
    ABS_P,
    SIZE_P,
    TYPE_P,
    FLOOR_P,
    DOUBLE_P,
    CEILING_P,
    KEYVALUE_P,
    DATETIME_P,
    BIGINT_P,
    BOOLEAN_P,
    DATE_P,
    DECIMAL_P,
    INTEGER_P,
    NUMBER_P,
    STRINGFUNC_P,
    TIME_P,
    TIME_TZ_P,
    TIMESTAMP_P,
    TIMESTAMP_TZ_P,
    /// A `{special}` single-character literal (C: `return *yytext`).
    Char(u8),
}

/// One scanned token together with the `JsonPathString` semantic value the C
/// scanner stuffs into `yylval->str`. Only the value-carrying tokens populate
/// `value`; for the rest it is `None`.
#[derive(Clone, Debug)]
pub struct Lexeme {
    pub token: Token,
    pub value: Option<JsonPathString>,
    /// Byte span of the matched lexeme in the original input (`yytext`'s
    /// position). The scanner's `next_token` fills this after the scan rule
    /// emits; it is what `jsonpath_yyerror` uses to format the `"at or near
    /// \"%s\""` clause on a parse error.
    pub start: usize,
    pub end: usize,
}

/// The flex start condition (`INITIAL` plus the four exclusive states).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum State {
    Initial,
    Xq,
    Xnq,
    Xvq,
    Xc,
}

/// The scanner: input bytes, current cursor, start condition, and the
/// `scanstring` literal buffer that C keeps in `yyextra`.
pub struct JsonPathLexer<'a> {
    pub(crate) input: &'a [u8],
    pub(crate) pos: usize,
    pub(crate) state: State,
    /// C: `yyextra->scanstring`.
    pub(crate) scanstring: JsonPathString,
}

// ---------------------------------------------------------------------------
// Keyword table (jsonpath_scan.l: `keywords[]`), sorted by length then
// alphabetically, searched by `checkKeyword` with a binary search.
// ---------------------------------------------------------------------------

struct JsonPathKeyword {
    len: i16,
    lowercase: bool,
    val: Token,
    keyword: &'static [u8],
}

static KEYWORDS: &[JsonPathKeyword] = &[
    JsonPathKeyword { len: 2, lowercase: false, val: Token::IS_P, keyword: b"is" },
    JsonPathKeyword { len: 2, lowercase: false, val: Token::TO_P, keyword: b"to" },
    JsonPathKeyword { len: 3, lowercase: false, val: Token::ABS_P, keyword: b"abs" },
    JsonPathKeyword { len: 3, lowercase: false, val: Token::LAX_P, keyword: b"lax" },
    JsonPathKeyword { len: 4, lowercase: false, val: Token::DATE_P, keyword: b"date" },
    JsonPathKeyword { len: 4, lowercase: false, val: Token::FLAG_P, keyword: b"flag" },
    JsonPathKeyword { len: 4, lowercase: false, val: Token::LAST_P, keyword: b"last" },
    JsonPathKeyword { len: 4, lowercase: true, val: Token::NULL_P, keyword: b"null" },
    JsonPathKeyword { len: 4, lowercase: false, val: Token::SIZE_P, keyword: b"size" },
    JsonPathKeyword { len: 4, lowercase: false, val: Token::TIME_P, keyword: b"time" },
    JsonPathKeyword { len: 4, lowercase: true, val: Token::TRUE_P, keyword: b"true" },
    JsonPathKeyword { len: 4, lowercase: false, val: Token::TYPE_P, keyword: b"type" },
    JsonPathKeyword { len: 4, lowercase: false, val: Token::WITH_P, keyword: b"with" },
    JsonPathKeyword { len: 5, lowercase: true, val: Token::FALSE_P, keyword: b"false" },
    JsonPathKeyword { len: 5, lowercase: false, val: Token::FLOOR_P, keyword: b"floor" },
    JsonPathKeyword { len: 6, lowercase: false, val: Token::BIGINT_P, keyword: b"bigint" },
    JsonPathKeyword { len: 6, lowercase: false, val: Token::DOUBLE_P, keyword: b"double" },
    JsonPathKeyword { len: 6, lowercase: false, val: Token::EXISTS_P, keyword: b"exists" },
    JsonPathKeyword { len: 6, lowercase: false, val: Token::NUMBER_P, keyword: b"number" },
    JsonPathKeyword { len: 6, lowercase: false, val: Token::STARTS_P, keyword: b"starts" },
    JsonPathKeyword { len: 6, lowercase: false, val: Token::STRICT_P, keyword: b"strict" },
    JsonPathKeyword { len: 6, lowercase: false, val: Token::STRINGFUNC_P, keyword: b"string" },
    JsonPathKeyword { len: 7, lowercase: false, val: Token::BOOLEAN_P, keyword: b"boolean" },
    JsonPathKeyword { len: 7, lowercase: false, val: Token::CEILING_P, keyword: b"ceiling" },
    JsonPathKeyword { len: 7, lowercase: false, val: Token::DECIMAL_P, keyword: b"decimal" },
    JsonPathKeyword { len: 7, lowercase: false, val: Token::INTEGER_P, keyword: b"integer" },
    JsonPathKeyword { len: 7, lowercase: false, val: Token::TIME_TZ_P, keyword: b"time_tz" },
    JsonPathKeyword { len: 7, lowercase: false, val: Token::UNKNOWN_P, keyword: b"unknown" },
    JsonPathKeyword { len: 8, lowercase: false, val: Token::DATETIME_P, keyword: b"datetime" },
    JsonPathKeyword { len: 8, lowercase: false, val: Token::KEYVALUE_P, keyword: b"keyvalue" },
    JsonPathKeyword { len: 9, lowercase: false, val: Token::TIMESTAMP_P, keyword: b"timestamp" },
    JsonPathKeyword { len: 10, lowercase: false, val: Token::LIKE_REGEX_P, keyword: b"like_regex" },
    JsonPathKeyword {
        len: 12,
        lowercase: false,
        val: Token::TIMESTAMP_TZ_P,
        keyword: b"timestamp_tz",
    },
];

/// C: `checkKeyword(yyscanner)`.
pub(crate) fn check_keyword(scanstring: &JsonPathString) -> Token {
    let mut res = Token::IDENT_P;

    if scanstring.len > KEYWORDS[KEYWORDS.len() - 1].len as i32 {
        return res;
    }

    let mut stop_low = 0usize;
    let mut stop_high = KEYWORDS.len();

    while stop_low < stop_high {
        let stop_middle = stop_low + ((stop_high - stop_low) >> 1);
        let kw = &KEYWORDS[stop_middle];
        let s = scanstring.bytes();

        let diff: i32 = if kw.len as i32 == scanstring.len {
            pg_strncasecmp(kw.keyword, s, scanstring.len as usize)
        } else {
            kw.len as i32 - scanstring.len
        };

        if diff < 0 {
            stop_low = stop_middle + 1;
        } else if diff > 0 {
            stop_high = stop_middle;
        } else {
            let final_diff = if kw.lowercase {
                strncmp(kw.keyword, s, scanstring.len as usize)
            } else {
                0
            };
            if final_diff == 0 {
                res = kw.val;
            }
            break;
        }
    }

    res
}

/// C `strncmp` (case-sensitive byte compare of up to `n` bytes).
fn strncmp(a: &[u8], b: &[u8], n: usize) -> i32 {
    for i in 0..n {
        let ca = a.get(i).copied().unwrap_or(0);
        let cb = b.get(i).copied().unwrap_or(0);
        if ca != cb {
            return ca as i32 - cb as i32;
        }
        if ca == 0 {
            return 0;
        }
    }
    0
}

// ---------------------------------------------------------------------------
// scanstring literal-buffer machinery (resizeString/addstring/addchar).
//
// The C buffer is `{ char *val; int len; int total; }`. `addchar(false,'\0')`
// writes the NUL at val[len] *without* advancing len (so the buffer is
// NUL-terminated for the numeric/variable tokens fed to numeric_in). We model
// `val` as a Vec<u8> holding exactly the bytes written, keeping `len` as the
// meaningful length; the optional trailing NUL sits at val[len].
// ---------------------------------------------------------------------------

pub(crate) trait ScanBuf {
    fn resize(&mut self, init: bool, append_len: usize);
    fn addstring(&mut self, init: bool, s: &[u8]);
    fn addchar(&mut self, init: bool, c: u8);
}

impl ScanBuf for JsonPathString {
    fn resize(&mut self, init: bool, append_len: usize) {
        if init {
            self.total = core::cmp::max(32, append_len) as i32;
            self.val.clear();
            self.val.reserve(self.total as usize);
            self.len = 0;
        } else if (self.len as usize) + append_len >= self.total as usize {
            while (self.len as usize) + append_len >= self.total as usize {
                self.total = self.total.saturating_mul(2);
            }
        }
    }

    fn addstring(&mut self, init: bool, s: &[u8]) {
        self.resize(init, s.len() + 1);
        // Drop any in-place trailing NUL written past `len` before appending.
        self.val.truncate(self.len as usize);
        self.val.extend_from_slice(s);
        self.len += s.len() as i32;
    }

    fn addchar(&mut self, init: bool, c: u8) {
        self.resize(init, 1);
        self.val.truncate(self.len as usize);
        self.val.push(c);
        if c != 0 {
            self.len += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// Character classes mirroring the flex definitions.
// ---------------------------------------------------------------------------

/// flex `special` = `[\?\%\$\.\[\]\{\}\(\)\|\&\!\=\<\>\@\#\,\*:\-\+\/]`.
pub(crate) fn is_special(c: u8) -> bool {
    matches!(
        c,
        b'?' | b'%'
            | b'$'
            | b'.'
            | b'['
            | b']'
            | b'{'
            | b'}'
            | b'('
            | b')'
            | b'|'
            | b'&'
            | b'!'
            | b'='
            | b'<'
            | b'>'
            | b'@'
            | b'#'
            | b','
            | b'*'
            | b':'
            | b'-'
            | b'+'
            | b'/'
    )
}

/// flex `blank` = `[ \t\n\r\f]`.
pub(crate) fn is_blank(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0C)
}

/// flex `other` = anything not special, blank, `\` or `"`.
pub(crate) fn is_other(c: u8) -> bool {
    !is_special(c) && !is_blank(c) && c != b'\\' && c != b'"'
}

// ---------------------------------------------------------------------------
// Unicode / hex escape decoding (pg_wchar.h surrogate helpers + the local
// hexval/addUnicode/addUnicodeChar/parseUnicode/parseHexChar functions).
// ---------------------------------------------------------------------------

fn is_utf16_surrogate_first(c: i32) -> bool {
    (0xD800..=0xDBFF).contains(&c)
}
fn is_utf16_surrogate_second(c: i32) -> bool {
    (0xDC00..=0xDFFF).contains(&c)
}
fn surrogate_pair_to_codepoint(first: i32, second: i32) -> i32 {
    ((first & 0x3FF) << 10) + 0x10000 + (second & 0x3FF)
}

// ---------------------------------------------------------------------------
// The scanner.
// ---------------------------------------------------------------------------

impl<'a> JsonPathLexer<'a> {
    pub fn new(input: &'a [u8]) -> Self {
        JsonPathLexer { input, pos: 0, state: State::Initial, scanstring: JsonPathString::default() }
    }

    /// Record a soft jsonpath syntax error and signal lexer termination.
    fn yyerror(
        &self,
        escontext: &mut Option<&mut SoftErrorContext>,
        message: &str,
    ) -> PgResult<()> {
        jsonpath_yyerror(escontext.as_deref_mut(), self.input, self.pos, message)
    }

    /// Like `yyerror`, but with an explicit `yytext` span (`[start, end)` into
    /// `self.input`) — the text of the flex rule that matched. C's
    /// `jsonpath_yyerror` formats the "at or near \"%s\"" clause from `yytext`,
    /// i.e. the matched lexeme, not the whole remaining input.
    fn yyerror_yytext(
        &self,
        escontext: &mut Option<&mut SoftErrorContext>,
        start: usize,
        end: usize,
        message: &str,
    ) -> PgResult<()> {
        jsonpath_yyerror_yytext(escontext.as_deref_mut(), &self.input[start..end], message)
    }

    /// `addUnicodeChar(ch, escontext)`.
    fn add_unicode_char(
        &mut self,
        ch: i32,
        escontext: &mut Option<&mut SoftErrorContext>,
    ) -> PgResult<bool> {
        if ch == 0 {
            ereturn(
                escontext.as_deref_mut(),
                false,
                PgError::error("unsupported Unicode escape sequence")
                    .with_sqlstate(ERRCODE_UNTRANSLATABLE_CHARACTER)
                    .with_detail("\\u0000 cannot be converted to text."),
            )?;
            return Ok(false);
        }

        // C: noerror form when escontext is an ErrorSaveContext (soft mode),
        // throwing form otherwise. `Some(escontext)` == soft mode here.
        let scratch = MemoryContext::new("jsonpath unicode scratch");
        let mcx = scratch.mcx();
        if escontext.is_none() {
            let cbuf = mbutils::pg_unicode_to_server(mcx, ch as u32)?;
            self.scanstring.addstring(false, &cbuf);
        } else {
            match mbutils::pg_unicode_to_server_noerror(mcx, ch as u32)? {
                Some(cbuf) => self.scanstring.addstring(false, &cbuf),
                None => {
                    ereturn(
                        escontext.as_deref_mut(),
                        false,
                        PgError::error("could not convert Unicode to server encoding")
                            .with_sqlstate(ERRCODE_SYNTAX_ERROR),
                    )?;
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    /// `addUnicode(ch, &hi_surrogate, escontext)`.
    fn add_unicode(
        &mut self,
        ch: i32,
        hi_surrogate: &mut i32,
        escontext: &mut Option<&mut SoftErrorContext>,
    ) -> PgResult<bool> {
        let mut ch = ch;
        if is_utf16_surrogate_first(ch) {
            if *hi_surrogate != -1 {
                ereturn(
                    escontext.as_deref_mut(),
                    false,
                    PgError::error("invalid input syntax for type jsonpath")
                        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
                        .with_detail("Unicode high surrogate must not follow a high surrogate."),
                )?;
                return Ok(false);
            }
            *hi_surrogate = ch;
            return Ok(true);
        } else if is_utf16_surrogate_second(ch) {
            if *hi_surrogate == -1 {
                ereturn(
                    escontext.as_deref_mut(),
                    false,
                    PgError::error("invalid input syntax for type jsonpath")
                        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
                        .with_detail("Unicode low surrogate must follow a high surrogate."),
                )?;
                return Ok(false);
            }
            ch = surrogate_pair_to_codepoint(*hi_surrogate, ch);
            *hi_surrogate = -1;
        } else if *hi_surrogate != -1 {
            ereturn(
                escontext.as_deref_mut(),
                false,
                PgError::error("invalid input syntax for type jsonpath")
                    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
                    .with_detail("Unicode low surrogate must follow a high surrogate."),
            )?;
            return Ok(false);
        }

        self.add_unicode_char(ch, escontext)
    }

    /// `hexval(c, &result, escontext)`.
    fn hexval(
        &self,
        c: u8,
        escontext: &mut Option<&mut SoftErrorContext>,
    ) -> PgResult<Option<i32>> {
        if c.is_ascii_digit() {
            return Ok(Some((c - b'0') as i32));
        }
        if (b'a'..=b'f').contains(&c) {
            return Ok(Some((c - b'a') as i32 + 0xA));
        }
        if (b'A'..=b'F').contains(&c) {
            return Ok(Some((c - b'A') as i32 + 0xA));
        }
        self.yyerror(escontext, "invalid hexadecimal digit")?;
        Ok(None)
    }

    /// `parseUnicode(s, l, escontext)` — decode `\u`-escapes in `s[0..l]`.
    /// Returns `Ok(false)` on a soft error (lexer should terminate).
    fn parse_unicode(
        &mut self,
        s: &[u8],
        l: usize,
        escontext: &mut Option<&mut SoftErrorContext>,
    ) -> PgResult<bool> {
        // C: `for (i = 2; i < l; i += 2)` — the `i += 2` stride skips the next
        // `\u` between concatenated escapes (the inner loops leave `i` pointing
        // just after the previous escape body, then the stride steps over the
        // following backslash + 'u').
        let mut hi_surrogate = -1i32;
        let mut i = 2usize; // skip leading '\u'
        while i < l {
            let mut ch = 0i32;
            if s[i] == b'{' {
                // \u{XX...}
                loop {
                    i += 1;
                    if !(i < l && s[i] != b'}') {
                        break;
                    }
                    match self.hexval(s[i], escontext)? {
                        Some(si) => ch = (ch << 4) | si,
                        None => return Ok(false),
                    }
                }
                i += 1; // skip '}'
            } else {
                // \uXXXX
                let mut j = 0;
                while j < 4 && i < l {
                    match self.hexval(s[i], escontext)? {
                        Some(si) => ch = (ch << 4) | si,
                        None => return Ok(false),
                    }
                    i += 1;
                    j += 1;
                }
            }

            if !self.add_unicode(ch, &mut hi_surrogate, escontext)? {
                return Ok(false);
            }

            i += 2; // C stride: skip the next "\u".
        }

        if hi_surrogate != -1 {
            ereturn(
                escontext.as_deref_mut(),
                false,
                PgError::error("invalid input syntax for type jsonpath")
                    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
                    .with_detail("Unicode low surrogate must follow a high surrogate."),
            )?;
            return Ok(false);
        }

        Ok(true)
    }

    /// `parseHexChar(s, escontext)` — decode a `\xHH` escape at `s[0..]`.
    fn parse_hex_char(
        &mut self,
        s: &[u8],
        escontext: &mut Option<&mut SoftErrorContext>,
    ) -> PgResult<bool> {
        let s2 = match self.hexval(s[2], escontext)? {
            Some(v) => v,
            None => return Ok(false),
        };
        let s3 = match self.hexval(s[3], escontext)? {
            Some(v) => v,
            None => return Ok(false),
        };
        let ch = (s2 << 4) | s3;
        self.add_unicode_char(ch, escontext)
    }
}

mod scan_initial;
mod scan_states;

/// C: `jsonpath_yyerror(...)` — record a soft jsonpath syntax error keyed on
/// whether we are at end of input. `pos` is the current cursor (the start of
/// the offending text, i.e. flex `yytext`).
pub fn jsonpath_yyerror(
    escontext: Option<&mut SoftErrorContext>,
    input: &[u8],
    pos: usize,
    message: &str,
) -> PgResult<()> {
    // C: "don't overwrite escontext if it's already been set".
    if let Some(ctx) = escontext.as_ref() {
        if ctx.error_occurred() {
            return Ok(());
        }
    }

    let yytext: &[u8] = if pos >= input.len() { &[] } else { &input[pos..] };
    jsonpath_yyerror_yytext(escontext, yytext, message)
}

/// As `jsonpath_yyerror`, but the caller supplies `yytext` (the matched lexeme)
/// directly. C's `jsonpath_yyerror` keys the "at end" vs "at or near" choice on
/// whether `yytext` is empty (`*yytext == YY_END_OF_BUFFER_CHAR`) and formats
/// the near-text from `yytext` itself.
pub fn jsonpath_yyerror_yytext(
    escontext: Option<&mut SoftErrorContext>,
    yytext: &[u8],
    message: &str,
) -> PgResult<()> {
    if let Some(ctx) = escontext.as_ref() {
        if ctx.error_occurred() {
            return Ok(());
        }
    }

    if yytext.is_empty() {
        ereturn(
            escontext,
            (),
            PgError::error(alloc::format!("{} at end of jsonpath input", message))
                .with_sqlstate(ERRCODE_SYNTAX_ERROR),
        )
    } else {
        let near = alloc::string::String::from_utf8_lossy(yytext).into_owned();
        ereturn(
            escontext,
            (),
            PgError::error(alloc::format!(
                "{} at or near \"{}\" of jsonpath input",
                message, near
            ))
            .with_sqlstate(ERRCODE_SYNTAX_ERROR),
        )
    }
}

extern crate alloc;
