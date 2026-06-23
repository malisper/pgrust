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
//! [`::types_error::PgError`] via [`PgResult`], exactly as the C `palloc`
//! `ereport(ERROR)`s and unwinds without killing the backend.

use ::mcx::{Mcx, PgString};
use ::types_error::PgResult;

use scanner_seams as seams;

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
///
/// `'i` is the input buffer's lifetime, kept distinct from `'mcx` (the parse
/// memory context) so the handle-registry adapter can drive the scanner over an
/// input copy it owns for just the duration of one seam call while still
/// `palloc`ing token strings into a longer-lived caller `mcx`.
pub struct SyncrepScanner<'i, 'mcx> {
    mcx: Mcx<'mcx>,
    input: &'i [u8],
    /// Current scan offset into `input` (the flex `yy_c_buf_p` cursor).
    pos: usize,
    /// `yytext` of the most-recently matched rule (the flex `yytext` macro).
    yytext: &'i [u8],
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
pub fn syncrep_scanner_init<'i, 'mcx>(input: &'i str, mcx: Mcx<'mcx>) -> SyncrepScanner<'i, 'mcx> {
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
pub fn syncrep_scanner_finish(scanner: SyncrepScanner<'_, '_>) {
    drop(scanner);
}

impl<'i, 'mcx> SyncrepScanner<'i, 'mcx> {
    /// `*syncrep_parse_error_msg_p` after the parse: the first collected error
    /// message, or `None` if no error was recorded.
    pub fn parse_error_msg(&self) -> Option<&str> {
        self.parse_error_msg.as_ref().map(PgString::as_str)
    }

    /// Current scan cursor (`yy_c_buf_p`) as a byte offset into the input —
    /// the persistent reentrant state the handle-based seam carries across
    /// `syncrep_yylex` calls.
    pub(crate) fn pos(&self) -> usize {
        self.pos
    }

    /// `yytext` of the last matched rule expressed as a `start..end` byte range
    /// into the input, so the registry adapter can re-derive the live slice on
    /// the next reconstructed scanner (the C `yyscan_t` keeps `yytext` alive).
    pub(crate) fn yytext_range(&self) -> (usize, usize) {
        // SAFETY of arithmetic: `yytext` is always a sub-slice of `input`.
        let base = self.input.as_ptr() as usize;
        let start = self.yytext.as_ptr() as usize - base;
        (start, start + self.yytext.len())
    }

    /// Restore the persistent cursor + `yytext` window saved by a prior
    /// `syncrep_yylex`/`syncrep_yyerror` step on the same logical scanner.
    pub(crate) fn restore(&mut self, pos: usize, yytext: (usize, usize)) {
        self.pos = pos;
        self.yytext = &self.input[yytext.0..yytext.1];
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

    /// Restore the already-formatted first-error message saved by a prior step
    /// on the same logical (handle-based) scanner, so the C "first error wins"
    /// guard (`*syncrep_parse_error_msg_p`) carries across `syncrep_yylex` /
    /// `syncrep_yyerror` calls. The message is already qualified (it was
    /// produced by `syncrep_yyerror`), so it is stored verbatim.
    pub(crate) fn seed_error(&mut self, message: &str) {
        if self.parse_error_msg.is_some() {
            return;
        }
        if let Ok(buf) = PgString::from_str_in(message, self.mcx) {
            self.parse_error_msg = Some(buf);
        }
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

// ---------------------------------------------------------------------------
// Handle-registry adapter: bridge the handle-based seam contract to the
// value-typed `SyncrepScanner` impl above.
//
// The cycle-partner grammar (`backend-replication-syncrep-gram`) drives this
// lexer through the `backend-replication-syncrep-scanner-seams` contract, which
// models the reentrant flex `yyscan_t` as an opaque `SyncrepScannerHandle(u64)`
// (a `void *` in C). The scanner OWNS those inward seams; the grammar only
// `::call`s them. So this crate installs all five from `init_seams()`.
//
// flex's `yyscan_t` is heap state that outlives any single `syncrep_yylex`
// call, so the handle must too. We keep a backend-local registry of live
// scanners, exactly mirroring `syncrep_scanner_init`'s `palloc0` /
// `syncrep_scanner_finish`'s `pfree`. The persistent reentrant state is: the
// input (the C `yy_scan_string` copy), the scan cursor, the last `yytext`
// window, and the first recorded parse-error message. The `NAME`/`NUM`
// `yylval->str` payloads are `pstrdup`'d into the caller's `mcx` on each
// `syncrep_yylex`, so they are NOT held in the registry.
//
// The DFA itself is not re-implemented here: each step reconstructs a
// `SyncrepScanner` over the owned input, restores the saved cursor/`yytext`,
// runs exactly one rule, then saves the advanced state back — keeping a single
// source of truth for the scanner logic.
mod registry {
    extern crate std;
    use super::{seams, syncrep_scanner_init, SyncrepScanner};
    use ::mcx::{Mcx, PgString};
    use std::cell::RefCell;
    use std::string::String;
    use std::vec::Vec;
    use ::types_error::PgResult;

    /// One live scanner's persistent (cross-call) reentrant state.
    struct Entry {
        /// The C `yy_scan_string` input copy.
        input: String,
        /// `yy_c_buf_p` cursor.
        pos: usize,
        /// `yytext` window (`start..end`) into `input`.
        yytext: (usize, usize),
        /// `*syncrep_parse_error_msg_p` (owned in scanner memory; copied into
        /// the caller `mcx` only when read back).
        error_msg: Option<String>,
    }

    std::thread_local! {
        /// Slot table; a `None` slot is a finished/freed scanner. The handle is
        /// the slot index, matching the C `yyscan_t` pointer identity.
        static SCANNERS: RefCell<Vec<Option<Entry>>> = const { RefCell::new(Vec::new()) };
    }

    fn idx(handle: seams::SyncrepScannerHandle) -> usize {
        handle.0 as usize
    }

    /// `syncrep_scanner_init`: copy `input`, allocate a registry slot, return
    /// its handle.
    fn init(_mcx: Mcx<'_>, input: &str) -> PgResult<seams::SyncrepScannerHandle> {
        let entry = Entry {
            input: String::from(input),
            pos: 0,
            yytext: (0, 0),
            error_msg: None,
        };
        let handle = SCANNERS.with(|s| {
            let mut s = s.borrow_mut();
            // Reuse a freed slot if one exists, else push.
            if let Some(i) = s.iter().position(|e| e.is_none()) {
                s[i] = Some(entry);
                i as u64
            } else {
                s.push(Some(entry));
                (s.len() - 1) as u64
            }
        });
        Ok(seams::SyncrepScannerHandle(handle))
    }

    /// Reconstruct a borrowed `SyncrepScanner` over the slot's owned state,
    /// restore the cursor/`yytext`/error, run `f`, then save the advanced
    /// persistent state back into the slot.
    fn with_scanner<'mcx, R>(
        mcx: Mcx<'mcx>,
        handle: seams::SyncrepScannerHandle,
        f: impl FnOnce(&mut SyncrepScanner<'_, 'mcx>) -> PgResult<R>,
    ) -> PgResult<R> {
        // The input string must outlive the borrowed scanner. We pull the saved
        // state out of the slot (leaving it parked), build the scanner over a
        // local copy of the input held for the duration of the call, run the
        // step, then write the new persistent state back.
        let (input, pos, yytext, prev_err) = SCANNERS.with(|s| {
            let s = s.borrow();
            let e = s[idx(handle)].as_ref().expect("live scanner handle");
            (e.input.clone(), e.pos, e.yytext, e.error_msg.clone())
        });

        let mut scanner = syncrep_scanner_init(input.as_str(), mcx);
        scanner.restore(pos, yytext);
        if let Some(msg) = &prev_err {
            // Pre-seed the first-error guard so a second error is dropped and a
            // re-read of `yytext` after `f` still reflects this step.
            scanner.seed_error(msg);
        }

        let out = f(&mut scanner);

        // Persist advanced cursor / yytext / first-error message.
        let new_pos = scanner.pos();
        let new_yytext = scanner.yytext_range();
        let new_err = scanner.parse_error_msg().map(String::from);
        SCANNERS.with(|s| {
            let mut s = s.borrow_mut();
            if let Some(e) = s[idx(handle)].as_mut() {
                e.pos = new_pos;
                e.yytext = new_yytext;
                e.error_msg = new_err;
            }
        });

        out
    }

    /// `syncrep_yylex`: one token, with the `yylval->str` payload `pstrdup`'d
    /// into the caller `mcx`.
    fn yylex<'mcx>(
        mcx: Mcx<'mcx>,
        handle: seams::SyncrepScannerHandle,
    ) -> PgResult<seams::SyncrepLexeme<'mcx>> {
        with_scanner(mcx, handle, |scanner| {
            let tok = scanner.syncrep_yylex()?;
            // The lexeme string was allocated into `mcx` inside `syncrep_yylex`;
            // re-materialize it under `mcx` for the seam's `SyncrepLexeme`.
            let value = match tok.str_value() {
                Some(s) => Some(PgString::from_str_in(s, mcx)?),
                None => None,
            };
            Ok(seams::SyncrepLexeme {
                token: tok.token_code(),
                value,
            })
        })
    }

    /// `syncrep_yyerror`: record `message` against the scanner (first error
    /// wins). The OOM `PgResult` of the underlying format step is dropped here
    /// because the seam decl is infallible, matching the C `void` signature.
    fn yyerror(handle: seams::SyncrepScannerHandle, message: &str) {
        // `mcx` is only needed for the `psprintf` buffer; the result is held in
        // scanner-owned memory, so a throwaway context is fine and is discarded
        // with the borrowed scanner.
        let _ = with_scanner_no_mcx(handle, |scanner| scanner.syncrep_yyerror(message));
    }

    /// `syncrep_scanner_error_msg`: copy the first recorded message into `mcx`.
    fn error_msg<'mcx>(
        mcx: Mcx<'mcx>,
        handle: seams::SyncrepScannerHandle,
    ) -> PgResult<Option<PgString<'mcx>>> {
        let msg = SCANNERS.with(|s| {
            let s = s.borrow();
            s[idx(handle)]
                .as_ref()
                .expect("live scanner handle")
                .error_msg
                .clone()
        });
        match msg {
            Some(m) => Ok(Some(PgString::from_str_in(&m, mcx)?)),
            None => Ok(None),
        }
    }

    /// `syncrep_scanner_finish`: free the slot (the C `pfree` + `yylex_destroy`).
    fn finish(handle: seams::SyncrepScannerHandle) {
        SCANNERS.with(|s| {
            s.borrow_mut()[idx(handle)] = None;
        });
    }

    /// `syncrep_yyerror` runs against persistent scanner state and the live
    /// `yytext`, but needs no `mcx` of its own (the error string is owned).
    /// Build a transient context just to host the borrowed `SyncrepScanner`.
    fn with_scanner_no_mcx<R>(
        handle: seams::SyncrepScannerHandle,
        f: impl FnOnce(&mut SyncrepScanner<'_, '_>) -> PgResult<R>,
    ) -> PgResult<R> {
        let ctx = ::mcx::MemoryContext::new("syncrep_yyerror");
        with_scanner(ctx.mcx(), handle, f)
    }

    /// Install all five inward seams of `backend-replication-syncrep-scanner`.
    pub(super) fn install() {
        seams::syncrep_scanner_init::set(init);
        seams::syncrep_yylex::set(yylex);
        seams::syncrep_yyerror::set(yyerror);
        seams::syncrep_scanner_error_msg::set(error_msg);
        seams::syncrep_scanner_finish::set(finish);
    }
}

/// Wire this crate's inward seams. The grammar (`syncrep_gram.y`) drives this
/// lexer through the handle-based scanner-seams contract; the scanner owns and
/// installs those five seams here via a backend-local handle registry that
/// bridges the opaque `SyncrepScannerHandle` (C's reentrant `yyscan_t`) to the
/// value-typed `SyncrepScanner` impl above.
pub fn init_seams() {
    registry::install();
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::mcx::MemoryContext;

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
