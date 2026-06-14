//! Port of `syncrep_scanner.l` — the lexical scanner for the
//! `synchronous_standby_names` GUC.
//!
//! This unit owns ONLY the lexer (`syncrep_scanner.l`): the scanner state
//! (`syncrep_yy_extra_type` + flex `yyscanner`), `syncrep_yylex`,
//! `syncrep_yyerror`, and `syncrep_scanner_init`/`syncrep_scanner_finish`.
//!
//! The Bison parser (`syncrep_gram.y`) — `create_syncrep_config`,
//! `SyncRepConfigData`, and the public `parse_synchronous_standby_names`
//! entry point — is the cycle-partner unit `backend-replication-syncrep-gram`
//! and is NOT ported here. The grammar drives this lexer (Bison calls
//! `syncrep_yylex`); the only thing crossing the cycle boundary is the set of
//! token codes the scanner returns (generated into `syncrep_gram.h`), kept
//! co-located here exactly as the C `return ANY;` / `return NAME;` statements
//! emit them.
//!
//! Mirroring the C scanner, `syncrep_yyerror` does NOT raise an error: it
//! collects the first error message into `*syncrep_parse_error_msg_p` and
//! leaves reporting to the ultimate caller (the GUC machinery). A genuine
//! `palloc` failure (e.g. `pstrdup(yytext)`) maps to a recoverable
//! [`types_error::PgError`] via [`PgResult`], exactly as the C `palloc`
//! `ereport(ERROR)`s and unwinds without killing the backend.

use mcx::{Mcx, PgString};
use types_error::PgResult;

/// Bison token codes for `syncrep_gram.y`, generated into `syncrep_gram.h`.
///
/// These are the values `syncrep_yylex` returns; single-character tokens
/// (`,`, `(`, `)`) use their ASCII byte value instead, and end-of-input is
/// reported as Bison's `YYEOF` (`0`). They form the ABI shared with the
/// cycle-partner grammar unit.
pub const NAME: i32 = 258;
pub const NUM: i32 = 259;
pub const JUNK: i32 = 260;
pub const ANY: i32 = 261;
pub const FIRST: i32 = 262;

/// A token produced by `syncrep_yylex`.
///
/// In the C scanner the token is the returned `int` code, with the semantic
/// value delivered separately through `yylval->str` (a `palloc`'d C string).
/// The idiomatic representation pairs the two: the `NAME`/`NUM`/`JUNK` arms
/// carry the `yylval->str` payload (a `ctx`-allocated [`PgString`]), and the
/// keyword/punctuation arms carry no payload (the C grammar never reads
/// `yylval` for them).
#[derive(Debug)]
pub enum SyncrepToken<'mcx> {
    /// `NAME` — an identifier, `*`, or double-quoted identifier; carries the
    /// `pstrdup(yytext)` / quoted-id buffer string in `yylval->str`.
    Name(PgString<'mcx>),
    /// `NUM` — a digit string; carries `pstrdup(yytext)`.
    Num(PgString<'mcx>),
    /// `ANY` keyword (case-insensitive). No `yylval` payload.
    Any,
    /// `FIRST` keyword (case-insensitive). No `yylval` payload.
    First,
    /// `','` punctuation token (returns the ASCII code `,`).
    Comma,
    /// `'('` punctuation token (returns the ASCII code `(`).
    LeftParen,
    /// `')'` punctuation token (returns the ASCII code `)`).
    RightParen,
    /// `JUNK` — any otherwise-unmatched single character, or the token
    /// returned from the `<xd><<EOF>>` (unterminated quoted identifier) rule.
    /// The C `.` rule sets no `yylval`, so this arm carries no payload.
    Junk,
    /// End of input. Bison's `YYEOF`, returned as the token code `0`.
    Eof,
}

impl SyncrepToken<'_> {
    /// The Bison token code this token returns, exactly as the C scanner's
    /// `return ...;` statements produce it.
    pub fn token_code(&self) -> i32 {
        match self {
            Self::Name(_) => NAME,
            Self::Num(_) => NUM,
            Self::Any => ANY,
            Self::First => FIRST,
            Self::Comma => b',' as i32,
            Self::LeftParen => b'(' as i32,
            Self::RightParen => b')' as i32,
            Self::Junk => JUNK,
            Self::Eof => 0,
        }
    }

    /// The `yylval->str` semantic value carried by `NAME` and `NUM` tokens
    /// (`None` for keyword/punctuation/`JUNK`/EOF, mirroring the C arms that
    /// never set `yylval`).
    pub fn str_value(&self) -> Option<&str> {
        match self {
            Self::Name(s) | Self::Num(s) => Some(s.as_str()),
            _ => None,
        }
    }
}

/// The scanner's reentrant state.
///
/// Mirrors flex's `yyscan_t` plus `struct syncrep_yy_extra_type`. The C
/// scanner keeps the input string (set up by `yy_scan_string`), the current
/// scan position, the `yytext` of the last matched rule, the `xdbuf`
/// `StringInfo` used to accumulate a double-quoted identifier, and — passed
/// through `syncrep_yylex`'s `syncrep_parse_error_msg_p` out-parameter — the
/// first collected error message. They are gathered here so a single owned
/// value carries the full reentrant scanner state.
///
/// `'mcx` is the parser's working memory context (the C parse
/// `CurrentMemoryContext`): the lifetime token under which `yylval->str`
/// strings are `palloc`'d.
pub struct SyncrepScanner<'mcx> {
    mcx: Mcx<'mcx>,
    input: &'mcx [u8],
    /// Current scan offset into `input` (the flex `yy_c_buf_p` cursor).
    pos: usize,
    /// `yytext` of the most-recently matched rule (the flex `yytext` macro).
    yytext: &'mcx [u8],
    /// `*syncrep_parse_error_msg_p`: the first collected error message, if any.
    parse_error_msg: Option<PgString<'mcx>>,
}

/// `syncrep_scanner_init(str, &yyscanner)`: set up the scanner over `str`.
///
/// The C function `palloc0`s the extra-type, runs `yylex_init`, attaches the
/// extra, and `yy_scan_string(str)`s the input. Here the equivalent state is
/// constructed directly over `input`. `input` and the produced token strings
/// are allocated/borrowed under `mcx`, the C parse memory context.
///
/// `input` must outlive the scanner (it is the GUC string the caller already
/// holds), matching `yy_scan_string`, which scans the caller's buffer in place.
pub fn syncrep_scanner_init<'mcx>(input: &'mcx str, mcx: Mcx<'mcx>) -> SyncrepScanner<'mcx> {
    SyncrepScanner {
        mcx,
        input: input.as_bytes(),
        pos: 0,
        yytext: &[],
        parse_error_msg: None,
    }
}

/// `syncrep_scanner_finish(yyscanner)`: tear down the scanner.
///
/// The C function `pfree`s the extra-type and `yylex_destroy`s the scanner.
/// In safe Rust the scanner owns all its state and is dropped here; the
/// `mcx`-allocated strings are reclaimed when the parse context is reset.
pub fn syncrep_scanner_finish(scanner: SyncrepScanner<'_>) {
    drop(scanner);
}

impl<'mcx> SyncrepScanner<'mcx> {
    /// `*syncrep_parse_error_msg_p` after the parse: the first collected error
    /// message, or `None` if no error was recorded.
    pub fn parse_error_msg(&self) -> Option<&str> {
        self.parse_error_msg.as_ref().map(PgString::as_str)
    }

    /// The flex `yytext` of the last matched rule.
    pub fn yytext(&self) -> &str {
        // `yytext` is always a sub-slice of a UTF-8 input on a char boundary
        // (rules match whole bytes/code points), so this never fails.
        core::str::from_utf8(self.yytext).unwrap_or("")
    }

    /// `syncrep_yylex(yylval, syncrep_parse_error_msg_p, yyscanner)`: return
    /// the next token.
    ///
    /// This is the flex DFA expressed as a hand-written matcher over the exact
    /// `syncrep_scanner.l` rules. A `pstrdup`/`appendStringInfo` allocation
    /// failure surfaces as a recoverable [`PgResult`] error, mirroring the C
    /// `palloc` `ereport(ERROR)`.
    pub fn syncrep_yylex(&mut self) -> PgResult<SyncrepToken<'mcx>> {
        // {space}+   { /* ignore */ }
        self.skip_space();

        let start = self.pos;
        if start >= self.input.len() {
            // flex matches <<EOF>> in the INITIAL start condition and returns
            // YY_NULL (0). `yytext` is empty at EOF.
            self.yytext = &self.input[start..start];
            return Ok(SyncrepToken::Eof);
        }

        let b = self.input[start];

        // {xdstart} -> BEGIN(xd): a double quote opens a delimited identifier.
        if b == b'"' {
            return self.scan_quoted_identifier(start);
        }

        // [Aa][Nn][Yy] / [Ff][Ii][Rr][Ss][Tt] / {identifier}
        //
        // flex's longest-match rule means an identifier run is scanned first
        // and then classified: a run that is exactly "any"/"first"
        // (case-insensitively) is the ANY/FIRST keyword, otherwise it is NAME.
        // (Crucially "anything" matches {identifier}, not [Aa][Nn][Yy], because
        // the latter is shorter — handled here by matching the full run first.)
        if is_ident_start(b) {
            return self.scan_identifier(start);
        }

        // {digit}+   { yylval->str = pstrdup(yytext); return NUM; }
        if b.is_ascii_digit() {
            return self.scan_number(start);
        }

        // "*"  { yylval->str = "*"; return NAME; }
        if b == b'*' {
            self.pos += 1;
            self.yytext = &self.input[start..self.pos];
            // The C rule sets yylval->str to the string literal "*"; pstrdup is
            // not used, but a ctx-allocated copy is the faithful safe analogue.
            let name = PgString::from_str_in("*", self.mcx)?;
            return Ok(SyncrepToken::Name(name));
        }

        // ","  { return ','; }   "("  { return '('; }   ")"  { return ')'; }
        if b == b',' {
            self.pos += 1;
            self.yytext = &self.input[start..self.pos];
            return Ok(SyncrepToken::Comma);
        }
        if b == b'(' {
            self.pos += 1;
            self.yytext = &self.input[start..self.pos];
            return Ok(SyncrepToken::LeftParen);
        }
        if b == b')' {
            self.pos += 1;
            self.yytext = &self.input[start..self.pos];
            return Ok(SyncrepToken::RightParen);
        }

        // .  { return JUNK; }
        //
        // flex's `.` matches any single character except newline; newline is
        // already consumed by {space}+, so any remaining byte here is JUNK.
        // Advance by one whole UTF-8 code point so `yytext` stays on a char
        // boundary (flex is 8-bit and would match one byte, but the only
        // observable effect — the `yytext` shown in an error message — must be
        // valid UTF-8 for the safe slice).
        self.pos += utf8_len(b);
        self.yytext = &self.input[start..self.pos];
        Ok(SyncrepToken::Junk)
    }

    /// The `{xdstart} {xddouble} {xdinside} {xdstop} <xd><<EOF>>` rule cluster:
    /// scan a double-quoted (delimited) identifier.
    ///
    /// `{xdstart}` consumes the opening quote and `BEGIN(xd)`. In the `xd`
    /// start condition, `{xddouble}` (`""`) appends a single `"` to `xdbuf`,
    /// `{xdinside}` (`[^"]+`) appends the literal run, `{xdstop}` (the closing
    /// `"`) hands `xdbuf.data` to `yylval->str` and returns `NAME`, and
    /// `<xd><<EOF>>` records "unterminated quoted identifier" and returns JUNK.
    fn scan_quoted_identifier(&mut self, _start: usize) -> PgResult<SyncrepToken<'mcx>> {
        // {xdstart}: consume the opening quote. (yytext becomes the lone quote,
        // but the NAME's value comes from xdbuf, not yytext.)
        self.pos += 1;
        // initStringInfo(&yyextra->xdbuf)
        let mut xdbuf = PgString::new_in(self.mcx);

        loop {
            if self.pos >= self.input.len() {
                // <xd><<EOF>>: matches the empty string at end of input, so
                // `yytext` is empty when syncrep_yyerror runs (it therefore
                // reports "... at end of input", NOT "at or near \"...\"").
                self.yytext = &self.input[self.pos..self.pos];
                self.syncrep_yyerror("unterminated quoted identifier")?;
                return Ok(SyncrepToken::Junk);
            }

            // {xddouble}: a doubled quote `""` collapses to a single `"`.
            if self.input[self.pos] == b'"' && self.input.get(self.pos + 1) == Some(&b'"') {
                xdbuf.try_push('"')?;
                self.pos += 2;
                continue;
            }

            // {xdstop}: a lone closing quote ends the identifier.
            if self.input[self.pos] == b'"' {
                self.pos += 1;
                // yylval->str = yyextra->xdbuf.data; BEGIN(INITIAL); return NAME;
                self.yytext = &self.input[self.pos - 1..self.pos];
                return Ok(SyncrepToken::Name(xdbuf));
            }

            // {xdinside}: the maximal run of non-quote bytes (`[^"]+`).
            let run_start = self.pos;
            while self.pos < self.input.len() && self.input[self.pos] != b'"' {
                self.pos += 1;
            }
            // appendStringInfoString(&yyextra->xdbuf, yytext)
            let run = &self.input[run_start..self.pos];
            // The input is valid UTF-8 and `"` is ASCII, so a non-quote run is
            // itself a valid-UTF-8 sub-slice on char boundaries.
            let run_str = core::str::from_utf8(run).unwrap_or("");
            xdbuf.try_push_str(run_str)?;
        }
    }

    /// `[Aa][Nn][Yy]` / `[Ff][Ii][Rr][Ss][Tt]` / `{identifier}`: scan an
    /// identifier run and classify it. The bare keywords `ANY`/`FIRST` are
    /// always recognized case-insensitively; everything else is `NAME` with a
    /// `pstrdup(yytext)` value.
    fn scan_identifier(&mut self, start: usize) -> PgResult<SyncrepToken<'mcx>> {
        self.pos += 1; // ident_start byte already known to be present
        while self.pos < self.input.len() && is_ident_cont(self.input[self.pos]) {
            self.pos += 1;
        }
        self.yytext = &self.input[start..self.pos];
        let text = core::str::from_utf8(self.yytext).unwrap_or("");

        // [Aa][Nn][Yy] / [Ff][Ii][Rr][Ss][Tt] — brute-force case-insensitive
        // keyword recognition, as in the C scanner. A keyword only matches when
        // it IS the whole run (flex longest-match: "anything" is {identifier}).
        if text.eq_ignore_ascii_case("any") {
            return Ok(SyncrepToken::Any);
        }
        if text.eq_ignore_ascii_case("first") {
            return Ok(SyncrepToken::First);
        }

        // {identifier}: yylval->str = pstrdup(yytext); return NAME;
        let name = PgString::from_str_in(text, self.mcx)?;
        Ok(SyncrepToken::Name(name))
    }

    /// `{digit}+`: scan a digit run. `yylval->str = pstrdup(yytext); return NUM;`
    fn scan_number(&mut self, start: usize) -> PgResult<SyncrepToken<'mcx>> {
        self.pos += 1; // first digit already known
        while self.pos < self.input.len() && self.input[self.pos].is_ascii_digit() {
            self.pos += 1;
        }
        self.yytext = &self.input[start..self.pos];
        let text = core::str::from_utf8(self.yytext).unwrap_or("");
        let num = PgString::from_str_in(text, self.mcx)?;
        Ok(SyncrepToken::Num(num))
    }

    /// `space  [ \t\n\r\f\v]`: `{space}+ { /* ignore */ }`.
    fn skip_space(&mut self) {
        while self.pos < self.input.len() && is_space(self.input[self.pos]) {
            self.pos += 1;
        }
    }

    /// `syncrep_yyerror(..., message)`: collect, but do NOT raise, an error.
    ///
    /// This mirrors the C `syncrep_yyerror` exactly. It does not `elog`; it
    /// records `message` into `*syncrep_parse_error_msg_p`, keeping only the
    /// first error in a parse operation, and formats it relative to the current
    /// `yytext`:
    ///   * `yytext[0]` set      → `"%s at or near \"%s\""`
    ///   * `yytext[0] == '\0'`  → `"%s at end of input"`
    ///
    /// The first argument of the C function (the Bison-mandated
    /// `syncrep_parse_result_p`) is unused; here the result is threaded by the
    /// grammar unit, not the scanner, so it is omitted. The only fallible step
    /// is the `psprintf` allocation (`palloc` `ereport(ERROR)` on OOM), surfaced
    /// as a [`PgResult`] error.
    pub fn syncrep_yyerror(&mut self, message: &str) -> PgResult<()> {
        // report only the first error in a parse operation
        if self.parse_error_msg.is_some() {
            return Ok(());
        }

        let mut buf = PgString::new_in(self.mcx);
        buf.try_push_str(message)?;
        if !self.yytext.is_empty() {
            // psprintf("%s at or near \"%s\"", message, yytext)
            buf.try_push_str(" at or near \"")?;
            buf.try_push_str(self.yytext())?;
            buf.try_push_str("\"")?;
        } else {
            // psprintf("%s at end of input", message)
            buf.try_push_str(" at end of input")?;
        }
        self.parse_error_msg = Some(buf);
        Ok(())
    }
}

/// `space  [ \t\n\r\f\v]` — the flex space character class.
fn is_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0c | 0x0b)
}

/// `ident_start  [A-Za-z\200-\377_]` — letters, underscore, and high-bit bytes.
fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_' || b >= 0x80
}

/// `ident_cont  [A-Za-z\200-\377_0-9\$]` — `ident_start` plus digits and `$`.
fn is_ident_cont(b: u8) -> bool {
    is_ident_start(b) || b.is_ascii_digit() || b == b'$'
}

/// Length, in bytes, of the UTF-8 code point whose leading byte is `b`.
///
/// Used only on the `.` (JUNK) path so that the recorded `yytext` stays on a
/// char boundary and remains valid UTF-8 for the error message. flex itself is
/// 8-bit and would advance one byte; for the high-bit `ident`/`xdinside`
/// classes whole multibyte runs are consumed elsewhere, so the only bytes that
/// reach here are isolated non-class characters.
fn utf8_len(b: u8) -> usize {
    if b < 0x80 {
        1
    } else if b >= 0xf0 {
        4
    } else if b >= 0xe0 {
        3
    } else if b >= 0xc0 {
        2
    } else {
        // A bare continuation byte: advance one byte (cannot start a code point).
        1
    }
}

/// Wire this crate's seams. The scanner owns no inward seams — the grammar
/// drives the lexer (Bison calls `syncrep_yylex`), so nothing here is exposed
/// as a `seam!` slot installed for another crate. Kept (empty) so `seams-init`
/// can list it uniformly.
pub fn init_seams() {}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;

    /// Drive the scanner over `input`, collecting `(token_code, str_value)`
    /// pairs (excluding the terminal EOF) plus any recorded error message.
    fn lex_all(input: &str) -> (Vec<(i32, Option<String>)>, Option<String>) {
        let ctx = MemoryContext::new("syncrep-scanner-test");
        let mut scanner = syncrep_scanner_init(input, ctx.mcx());
        let mut out = Vec::new();
        loop {
            let tok = scanner.syncrep_yylex().expect("no OOM in tests");
            if matches!(tok, SyncrepToken::Eof) {
                break;
            }
            out.push((tok.token_code(), tok.str_value().map(str::to_owned)));
        }
        let err = scanner.parse_error_msg().map(str::to_owned);
        (out, err)
    }

    #[test]
    fn lexes_first_priority_form_with_quote_escape() {
        let (toks, err) = lex_all("FIRST 2 (standby_a, \"standby\"\"b\", *)");
        assert_eq!(err, None);
        assert_eq!(
            toks,
            vec![
                (FIRST, None),
                (NUM, Some("2".to_owned())),
                (b'(' as i32, None),
                (NAME, Some("standby_a".to_owned())),
                (b',' as i32, None),
                (NAME, Some("standby\"b".to_owned())),
                (b',' as i32, None),
                (NAME, Some("*".to_owned())),
                (b')' as i32, None),
            ]
        );
    }

    #[test]
    fn lexes_any_quorum_form_with_numeric_and_dollar_names() {
        let (toks, err) = lex_all("any 1 (123, node$1)");
        assert_eq!(err, None);
        assert_eq!(
            toks,
            vec![
                (ANY, None),
                (NUM, Some("1".to_owned())),
                (b'(' as i32, None),
                (NUM, Some("123".to_owned())),
                (b',' as i32, None),
                (NAME, Some("node$1".to_owned())),
                (b')' as i32, None),
            ]
        );
    }

    #[test]
    fn any_and_first_are_keywords_only_when_unquoted() {
        let (toks, _) = lex_all("any first ANY FIRST");
        assert_eq!(
            toks,
            vec![(ANY, None), (FIRST, None), (ANY, None), (FIRST, None)]
        );

        let (toks, _) = lex_all("\"any\" \"first\"");
        assert_eq!(
            toks,
            vec![
                (NAME, Some("any".to_owned())),
                (NAME, Some("first".to_owned())),
            ]
        );
    }

    #[test]
    fn unmatched_character_returns_junk_without_error() {
        let (toks, err) = lex_all("@");
        assert_eq!(toks, vec![(JUNK, None)]);
        assert_eq!(err, None);
    }

    #[test]
    fn unterminated_quoted_identifier_reports_at_end_of_input() {
        // The <xd><<EOF>> rule fires after {xdstart} consumed `"` and
        // {xdinside} consumed `abc`; its empty match leaves yytext empty, so
        // the error is "... at end of input" and a JUNK token is returned.
        let ctx = MemoryContext::new("syncrep-scanner-test");
        let mut scanner = syncrep_scanner_init("\"abc", ctx.mcx());
        let tok = scanner.syncrep_yylex().unwrap();
        assert_eq!(tok.token_code(), JUNK);
        assert_eq!(scanner.yytext(), "");
        assert_eq!(
            scanner.parse_error_msg(),
            Some("unterminated quoted identifier at end of input")
        );

        // Only the first error in a parse operation is kept.
        scanner.syncrep_yyerror("syntax error").unwrap();
        assert_eq!(
            scanner.parse_error_msg(),
            Some("unterminated quoted identifier at end of input")
        );
    }

    #[test]
    fn unterminated_quoted_identifier_with_escape_still_end_of_input() {
        let (toks, err) = lex_all("\"ab\"\"cd");
        assert_eq!(toks, vec![(JUNK, None)]);
        assert_eq!(
            err.as_deref(),
            Some("unterminated quoted identifier at end of input")
        );
    }

    #[test]
    fn yyerror_reports_near_current_token_or_end_of_input() {
        let ctx = MemoryContext::new("syncrep-scanner-test");
        let mut scanner = syncrep_scanner_init("node", ctx.mcx());
        let tok = scanner.syncrep_yylex().unwrap();
        assert_eq!(tok.token_code(), NAME);
        assert_eq!(tok.str_value(), Some("node"));
        scanner.syncrep_yyerror("syntax error").unwrap();
        assert_eq!(
            scanner.parse_error_msg(),
            Some("syntax error at or near \"node\"")
        );

        let mut scanner = syncrep_scanner_init("", ctx.mcx());
        scanner.syncrep_yyerror("syntax error").unwrap();
        assert_eq!(
            scanner.parse_error_msg(),
            Some("syntax error at end of input")
        );
    }

    #[test]
    fn quoted_identifier_value_comes_from_xdbuf_not_yytext() {
        // After {xdstop} fires, yytext is the lone closing quote, but the NAME
        // value is the accumulated xdbuf contents (here with a `""` escape).
        let ctx = MemoryContext::new("syncrep-scanner-test");
        let mut scanner = syncrep_scanner_init("\"a\"\"b\"", ctx.mcx());
        let tok = scanner.syncrep_yylex().unwrap();
        assert_eq!(tok.token_code(), NAME);
        assert_eq!(tok.str_value(), Some("a\"b"));
        assert_eq!(scanner.yytext(), "\"");
    }

    #[test]
    fn finish_drops_scanner() {
        let ctx = MemoryContext::new("syncrep-scanner-test");
        let scanner = syncrep_scanner_init("a, b", ctx.mcx());
        syncrep_scanner_finish(scanner);
    }
}
