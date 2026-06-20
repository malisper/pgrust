//! Port of `src/backend/parser/parser.c` (PostgreSQL 18.3): the main entry
//! point / driver for the PostgreSQL grammar.
//!
//! This crate ports:
//!   * [`base_yylex`] -- the intermediate filter between the bison grammar and
//!     the core lexer (`core_yylex` in scan.l).  It implements the one-token
//!     lookahead that merges multiword tokens (`NOT LIKE` -> `NOT_LA`,
//!     `WITH TIME` -> `WITH_LA`, `FORMAT JSON` -> `FORMAT_LA`, ...) and converts
//!     `UIDENT`/`USCONST` (Unicode-escaped) tokens into plain `IDENT`/`SCONST`
//!     via [`str_udeescape`].
//!   * [`raw_parser`] -- the per-query entry: set up the scanner, seed the
//!     lookahead with the `RawParseMode` mode token, drive the grammar, and
//!     return the list of `RawStmt` parse trees.
//!   * `str_udeescape`, `check_uescapechar`, `check_unicode_value`, `hexval`
//!     -- the Unicode de-escaping support routines.
//!
//! The flex/C scanner mutates a NUL-padded buffer and uses the
//! `lookahead_end`/`lookahead_hold_char` `\0` un-truncation trick purely so
//! error cursors point at the right token; this safe-Rust port tracks the
//! lookahead token directly (the scanner never mutates the input), which yields
//! the identical merged token stream and the same error locations.
//!
//! The grammar itself (`base_yyparse`) is a separate, much larger port
//! (gram.y); `raw_parser` invokes it through the [`Grammar`] seam, which the
//! grammar crate implements.  No `extern "C"`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use backend_parser_scan::{
    tokens, CoreYYSTYPE, LexError, Scanner, ScannerSettings, Token, UnicodeToServerSeam,
    Utf8UnicodeSeam, YY_NULL,
};
use pgrust_pg_ffi::spi::{
    RawParseMode, RAW_PARSE_DEFAULT, RAW_PARSE_PLPGSQL_ASSIGN1, RAW_PARSE_PLPGSQL_ASSIGN2,
    RAW_PARSE_PLPGSQL_ASSIGN3, RAW_PARSE_PLPGSQL_EXPR, RAW_PARSE_TYPE_NAME,
};

mod udeescape;
pub use udeescape::{check_uescapechar, str_udeescape};

// ===========================================================================
// pg_wchar.h surrogate helpers (used by str_udeescape).
// ===========================================================================

/// `pg_wchar` code point.
pub type PgWchar = u32;

fn is_valid_unicode_codepoint(c: PgWchar) -> bool {
    c > 0 && c <= 0x10FFFF
}
fn is_utf16_surrogate_first(c: PgWchar) -> bool {
    (0xD800..=0xDBFF).contains(&c)
}
fn is_utf16_surrogate_second(c: PgWchar) -> bool {
    (0xDC00..=0xDFFF).contains(&c)
}
fn surrogate_pair_to_codepoint(first: PgWchar, second: PgWchar) -> PgWchar {
    ((first & 0x3FF) << 10) + 0x10000 + (second & 0x3FF)
}

// ===========================================================================
// The grammar seam.
// ===========================================================================

/// `base_yyparse()` (gram.y) -- the bison parser, supplied by the grammar
/// crate.  `raw_parser` drives it with the configured [`BaseLexer`]; on success
/// it must leave the final parse result (the `List *parsetree`) available to be
/// returned.  This is a seam because gram.y is a separate large port.
pub trait Grammar {
    /// Parse the token stream produced by `lexer`, returning the raw parse-tree
    /// list (`yyextra.parsetree`).  `Err` mirrors a nonzero `base_yyparse`
    /// result (a syntax error), after which `raw_parser` returns `NIL`.
    fn base_yyparse(&self, lexer: &mut BaseLexer<'_>) -> Result<RawParseTree, ParseError>;
}

/// The raw parse-tree list returned by the grammar -- a `List *` of `RawStmt *`
/// in C.  Represented opaquely here as a vector of node pointers so the driver
/// is independent of the concrete node ABI (the grammar crate constructs the
/// real `RawStmt` nodes).
pub type RawParseTree = Vec<*mut core::ffi::c_void>;

/// A parse error (syntax error from the grammar, or a lexer error).
#[derive(Clone, Debug)]
pub struct ParseError {
    pub message: String,
    pub location: i32,
    /// SQLSTATE chars (e.g. `*b"42601"`) of the originating error.
    pub sqlstate: [u8; 5],
    /// True when the error came through scan.l's `yyerror()` shorthand (plain
    /// `ERRCODE_SYNTAX_ERROR`, no detail/hint/source), i.e. C renders it as
    /// `"%s at or near \"%s\""` via `scanner_yyerror`; false for the direct
    /// `ereport(...)` lexer errors, which C reports verbatim.
    pub yyerror: bool,
    /// `errdetail` text for the direct-`ereport` lexer errors (empty = none).
    pub detail: Option<String>,
    /// `errhint` text for the direct-`ereport` lexer errors (empty = none) —
    /// e.g. the Unicode-escape "Unicode escapes must be \\XXXX or \\+XXXXXX.".
    pub hint: Option<String>,
}

impl From<LexError> for ParseError {
    fn from(e: LexError) -> Self {
        // When the lexer error simply propagates an error raised by a called-out
        // routine (pg_verifymbstr / pg_unicode_to_server), carry that routine's
        // own dynamic message rather than the empty static placeholder.
        let message = match &e.source {
            Some(src) => src.message().to_string(),
            None => e.message.to_string(),
        };
        let yyerror = e.source.is_none()
            && e.detail.is_none()
            && e.hint.is_none()
            && e.sqlstate == pgrust_pg_ffi::ERRCODE_SYNTAX_ERROR;
        // The source-propagated errors carry their own detail/hint; otherwise
        // forward the scanner's own errdetail/errhint (scan.l).
        let (detail, hint) = if e.source.is_some() {
            (None, None)
        } else {
            (
                e.detail.map(|d| d.to_string()),
                e.hint.map(|h| h.to_string()),
            )
        };
        ParseError {
            message,
            location: e.location,
            sqlstate: pgrust_pg_ffi::error::unpack_sqlstate(e.sqlstate),
            yyerror,
            detail,
            hint,
        }
    }
}

/// `scanner_errposition(location, yyscanner)` (scan.l:1139) -- convert a byte
/// offset within the scan buffer into the 1-based *character* cursor position
/// that `errposition()` expects.  Returns 0 (no-op) when `location` is negative
/// (unknown), exactly as the C routine does.
///
/// `scanbuf` is the scanner's input buffer (`yyextra->scanbuf`).
pub fn scanner_errposition(location: i32, scanbuf: &[u8]) -> i32 {
    if location < 0 {
        return 0; // no-op if location is unknown
    }
    // Convert byte offset to character number (+1 for 1-based cursor).
    match backend_utils_mb::pg_mbstrlen_with_len(scanbuf, location) {
        Ok(pos) => pos + 1,
        // pg_mbstrlen_with_len only fails on an invalid encoding mid-buffer;
        // fall back to the byte offset's 1-based form to stay best-effort.
        Err(_) => location + 1,
    }
}

// ===========================================================================
// base_yylex: the grammar/scanner filter.
// ===========================================================================

/// Look-ahead state for [`base_yylex`] (`base_yy_extra_type`'s lookahead
/// fields).  Wraps the core [`Scanner`] and provides the token-merging filter.
pub struct BaseLexer<'a> {
    scanner: Scanner<'a>,
    /// `have_lookahead` + `lookahead_token`/`lookahead_yylval`/`lookahead_yylloc`.
    lookahead: Option<Token>,
    /// Unicode-to-server seam used by `str_udeescape`.
    unicode_seam: &'a dyn UnicodeToServerSeam,
}

impl<'a> BaseLexer<'a> {
    /// Create a lexer over `scanner` with the given lookahead seed (the mode
    /// token for non-default `RawParseMode`s) and Unicode seam.
    pub fn new(
        scanner: Scanner<'a>,
        seed: Option<Token>,
        unicode_seam: &'a dyn UnicodeToServerSeam,
    ) -> BaseLexer<'a> {
        BaseLexer {
            scanner,
            lookahead: seed,
            unicode_seam,
        }
    }

    /// Convert a byte `location` (as produced by the scanner) into the 1-based
    /// character cursor position used in user-facing error messages, via
    /// [`scanner_errposition`] over this lexer's scan buffer.
    fn errpos(&self, location: i32) -> i32 {
        scanner_errposition(location, self.scanner.scanbuf())
    }

    /// Run `core_yylex`, converting any lexer error's byte location into the
    /// 1-based character cursor position (matching C, where scanner errors are
    /// reported through `scanner_errposition`).
    fn core_yylex(&mut self) -> Result<Token, ParseError> {
        let warned = self.scanner.warnings.len();
        match self.scanner.core_yylex() {
            Ok(tok) => {
                // Emit any newly-collected scanner warnings. scan.l
                // `check_string_escape_warning`/`check_escape_warning` issue these
                // `ereport(WARNING)`s inline while scanning a string literal
                // ("nonstandard use of \\ / \' / escape in a string literal"); the
                // safe-Rust scanner instead defers them onto `scanner.warnings`, so
                // this token boundary is where we replay them, in scan order, with
                // the literal-start `errposition` (`lexer_errposition()`).
                if self.scanner.warnings.len() > warned {
                    self.emit_scanner_warnings(warned);
                }
                Ok(tok)
            }
            Err(e) => {
                let mut pe: ParseError = e.into();
                // A `yyerror`-path error is rendered by `scanner_yyerror`, which
                // needs the raw BYTE offset to slice the "at or near" snippet
                // (`scanbuf[lloc..match-end]`) and runs `scanner_errposition`
                // itself for the cursor. Converting here would double-apply it
                // (a char cursor offset slices off the leading byte, e.g. the
                // `\` of a Unicode escape). Only the direct-ereport path uses
                // `location` as a final cursor, so convert just that one.
                if !pe.yyerror {
                    pe.location = self.errpos(pe.location);
                }
                Err(pe)
            }
        }
    }

    /// Replay the deferred scanner warnings at indices `from..` as
    /// `ereport(WARNING, ...)`. Mirrors scan.l's
    /// `check_string_escape_warning`/`check_escape_warning`: SQLSTATE
    /// `ERRCODE_NONSTANDARD_USE_OF_ESCAPE_CHARACTER`, the scanner's static
    /// message/hint, and the literal-start byte `location` run through
    /// `scanner_errposition` (`lexer_errposition()`). A WARNING does not longjmp,
    /// so `finish` returns `Ok(())`; an unexpected error while emitting is dropped
    /// rather than aborting the parse (the C path cannot fail here either).
    fn emit_scanner_warnings(&self, from: usize) {
        for w in &self.scanner.warnings[from..] {
            let cursor = self.scanner.scanner_errposition(w.location);
            // Emit through the live client error path (`backend-utils-error`, the
            // non-fgram crate whose `ThrowErrorData`/report sink the backend wires
            // to the frontend — the same path `ereport(WARNING)` uses elsewhere).
            let _ = backend_utils_error_live::ereport(types_error_live::error::WARNING)
                .errcode(types_error_live::SqlState(w.sqlstate.0))
                .errmsg(w.message)
                .errhint(w.hint)
                .errposition(cursor)
                .finish(types_error_live::pg_error::ErrorLocation::new(
                    "scan.l",
                    1424,
                    "check_string_escape_warning",
                ));
        }
    }

    /// `base_yylex()` (parser.c:110) -- return the next (possibly merged) token.
    ///
    /// Returns the end-of-input token (`YY_NULL`) when the stream is exhausted.
    pub fn base_yylex(&mut self) -> Result<Token, ParseError> {
        // Get next token --- we might already have it (lookahead/mode seed).
        let mut cur = match self.lookahead.take() {
            Some(tok) => tok,
            None => self.core_yylex()?,
        };

        // If this token doesn't require lookahead, just return it.
        let needs_lookahead = matches!(
            cur.token,
            t if t == tokens::FORMAT
                || t == tokens::NOT
                || t == tokens::NULLS_P
                || t == tokens::WITH
                || t == tokens::WITHOUT
                || t == tokens::UIDENT
                || t == tokens::USCONST
        );
        if !needs_lookahead {
            return Ok(cur);
        }

        // Get next token, saving it as the lookahead.
        let next = self.core_yylex()?;
        let next_token = next.token;
        self.lookahead = Some(next.clone());

        // Replace cur_token if needed, based on lookahead.
        match cur.token {
            t if t == tokens::FORMAT => {
                if next_token == tokens::JSON {
                    cur.token = tokens::FORMAT_LA;
                }
            }
            t if t == tokens::NOT => {
                if next_token == tokens::BETWEEN
                    || next_token == tokens::IN_P
                    || next_token == tokens::LIKE
                    || next_token == tokens::ILIKE
                    || next_token == tokens::SIMILAR
                {
                    cur.token = tokens::NOT_LA;
                }
            }
            t if t == tokens::NULLS_P => {
                if next_token == tokens::FIRST_P || next_token == tokens::LAST_P {
                    cur.token = tokens::NULLS_LA;
                }
            }
            t if t == tokens::WITH => {
                if next_token == tokens::TIME || next_token == tokens::ORDINALITY {
                    cur.token = tokens::WITH_LA;
                }
            }
            t if t == tokens::WITHOUT => {
                if next_token == tokens::TIME {
                    cur.token = tokens::WITHOUT_LA;
                }
            }
            t if t == tokens::UIDENT || t == tokens::USCONST => {
                return self.finish_uident_usconst(cur);
            }
            _ => {}
        }

        Ok(cur)
    }

    /// The `UIDENT`/`USCONST` lookahead branch of `base_yylex` (parser.c:253).
    /// Looks ahead for `UESCAPE 'c'`, applies the Unicode de-escaping, and
    /// converts the token to `IDENT`/`SCONST`.
    fn finish_uident_usconst(&mut self, mut cur: Token) -> Result<Token, ParseError> {
        // The lookahead currently holds the token following UIDENT/USCONST.
        let location = cur.location;
        let next = self.lookahead.clone().ok_or_else(|| ParseError {
            message: "finish_uident_usconst: lookahead not set".to_string(),
            location,
            sqlstate: *b"XX000",
            yyerror: false,
            detail: None,
            hint: None,
        })?;

        let mut escape = b'\\';
        if next.token == tokens::UESCAPE {
            // Get the third token, which had better be SCONST.
            let third = self.core_yylex()?;
            if third.token != tokens::SCONST {
                return Err(ParseError {
                    message: "UESCAPE must be followed by a simple string literal".to_string(),
                    // Raw BYTE offset: `scanner_yyerror` (the yyerror renderer)
                    // runs `scanner_errposition` for the cursor itself.
                    location: third.location,
                    sqlstate: *b"42601",
                    yyerror: true,
                    detail: None,
                    hint: None,
                });
            }
            let escstr = match &third.value {
                CoreYYSTYPE::Str(s) => s.clone(),
                _ => Vec::new(),
            };
            if escstr.len() != 1 || !check_uescapechar(escstr[0]) {
                // C parser.c:278 un-truncates so this error points at the third
                // (UESCAPE string) token, like the sibling check above.
                return Err(ParseError {
                    message: "invalid Unicode escape character".to_string(),
                    // Raw BYTE offset (see sibling check above).
                    location: third.location,
                    sqlstate: *b"42601",
                    yyerror: true,
                    detail: None,
                    hint: None,
                });
            }
            escape = escstr[0];
            // Consume all three tokens.
            self.lookahead = None;
        }
        // else: keep the lookahead token for the next call (no UESCAPE).

        // Apply Unicode conversion to cur's string value.
        let raw = match &cur.value {
            CoreYYSTYPE::Str(s) => s.clone(),
            _ => Vec::new(),
        };
        // str_udeescape reports a raw byte offset (`in - str + position + 3`);
        // C runs every such error through scanner_errposition, so convert here.
        let deescaped =
            str_udeescape(&raw, escape, location, self.unicode_seam).map_err(|e| ParseError {
                message: e.message,
                location: self.errpos(e.location),
                // str_udeescape errors are direct ereport(ERRCODE_SYNTAX_ERROR)
                // calls in C (no "at or near" rendering).
                sqlstate: *b"42601",
                yyerror: false,
                detail: None,
                // The "invalid Unicode escape" error carries the
                // "\\XXXX or \\+XXXXXX." hint (C: errhint); forward it.
                hint: e.hint.map(|h| h.to_string()),
            })?;

        if cur.token == tokens::UIDENT {
            // Truncate as appropriate, then it's an IDENT.
            let truncated = truncate_ident_bytes(&deescaped);
            cur.value = CoreYYSTYPE::Str(truncated);
            cur.token = tokens::IDENT;
        } else {
            // USCONST -> SCONST
            cur.value = CoreYYSTYPE::Str(deescaped);
            cur.token = tokens::SCONST;
        }
        Ok(cur)
    }

    /// Borrow the underlying core scanner (e.g. to inspect warnings).
    pub fn scanner(&self) -> &Scanner<'a> {
        &self.scanner
    }
}

/// `truncate_identifier(str, strlen(str), true)` on a de-escaped `UIDENT`.
fn truncate_ident_bytes(bytes: &[u8]) -> Vec<u8> {
    let len = bytes.len();
    if len >= pgrust_pg_ffi::NAMEDATALEN as usize {
        let mut buf = bytes.to_vec();
        buf.push(0);
        if backend_parser_scansup::truncate_identifier(&mut buf, len as core::ffi::c_int, true)
            .is_ok()
        {
            let nul = buf.iter().position(|&b| b == 0).unwrap_or(len);
            return buf[..nul].to_vec();
        }
    }
    bytes.to_vec()
}

// ===========================================================================
// raw_parser.
// ===========================================================================

/// `raw_parser()` (parser.c:41) -- given a query in string form, do lexical and
/// grammatical analysis, returning the list of raw (un-analyzed) parse trees.
///
/// `grammar` supplies the bison parser (gram.y) via the [`Grammar`] seam;
/// `mode` selects the `RawParseMode` (seeding `base_yylex`'s lookahead with the
/// matching mode token for the non-default modes).  Uses the built-in
/// [`Utf8UnicodeSeam`] for Unicode de-escaping.
pub fn raw_parser(
    str_: &str,
    mode: RawParseMode,
    grammar: &dyn Grammar,
) -> Result<RawParseTree, ParseError> {
    raw_parser_with_seam(
        str_,
        mode,
        grammar,
        &Utf8UnicodeSeam,
        ScannerSettings::default(),
    )
}

/// As [`raw_parser`], with an explicit Unicode seam and scanner settings.
pub fn raw_parser_with_seam<'a>(
    str_: &'a str,
    mode: RawParseMode,
    grammar: &dyn Grammar,
    unicode_seam: &'a dyn UnicodeToServerSeam,
    settings: ScannerSettings,
) -> Result<RawParseTree, ParseError> {
    let scanner = Scanner::with_unicode_seam(str_.as_bytes(), settings, unicode_seam);

    // base_yylex() only needs us to initialize the lookahead token, if any.
    let seed = mode_token(mode).map(|tok| Token {
        token: tok,
        value: CoreYYSTYPE::None,
        location: 0,
    });

    let mut lexer = BaseLexer::new(scanner, seed, unicode_seam);

    // Parse! (yyresult != 0 -> error -> return NIL).
    match grammar.base_yyparse(&mut lexer) {
        Ok(tree) => Ok(tree),
        // On a grammar/syntax error, raw_parser returns NIL (the empty list).
        Err(_) => Ok(Vec::new()),
    }
}

/// The `mode_token[]` array (parser.c:58) -- the initial lookahead token for a
/// non-default `RawParseMode`, or `None` for `RAW_PARSE_DEFAULT`.
fn mode_token(mode: RawParseMode) -> Option<i32> {
    match mode {
        m if m == RAW_PARSE_DEFAULT => None,
        m if m == RAW_PARSE_TYPE_NAME => Some(tokens::MODE_TYPE_NAME),
        m if m == RAW_PARSE_PLPGSQL_EXPR => Some(tokens::MODE_PLPGSQL_EXPR),
        m if m == RAW_PARSE_PLPGSQL_ASSIGN1 => Some(tokens::MODE_PLPGSQL_ASSIGN1),
        m if m == RAW_PARSE_PLPGSQL_ASSIGN2 => Some(tokens::MODE_PLPGSQL_ASSIGN2),
        m if m == RAW_PARSE_PLPGSQL_ASSIGN3 => Some(tokens::MODE_PLPGSQL_ASSIGN3),
        _ => None,
    }
}

/// Convenience: lex `str_` to completion through `base_yylex`, returning the
/// merged token stream (excluding the terminating `YY_NULL`).  Used by tests
/// and by callers that want the filtered token stream without a grammar.
pub fn lex_tokens(str_: &str) -> Result<Vec<Token>, ParseError> {
    lex_tokens_with(str_, &Utf8UnicodeSeam, ScannerSettings::default())
}

/// As [`lex_tokens`], with an explicit Unicode seam and scanner settings.
pub fn lex_tokens_with<'a>(
    str_: &'a str,
    unicode_seam: &'a dyn UnicodeToServerSeam,
    settings: ScannerSettings,
) -> Result<Vec<Token>, ParseError> {
    let scanner = Scanner::with_unicode_seam(str_.as_bytes(), settings, unicode_seam);
    let mut lexer = BaseLexer::new(scanner, None, unicode_seam);
    let mut out = Vec::new();
    loop {
        let tok = lexer.base_yylex()?;
        if tok.token == YY_NULL {
            break;
        }
        out.push(tok);
        if out.len() > 1_000_000 {
            break;
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests;
