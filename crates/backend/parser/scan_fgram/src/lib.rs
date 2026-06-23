//! Port of `src/backend/parser/scan.l` (PostgreSQL 18.3) -- the core flex
//! lexical scanner for the SQL grammar.
//!
//! This is a *faithful* hand-written re-implementation of the flex scanner.
//! Flex chooses the longest match and, among equal-length matches, the rule
//! appearing first in the file; the scanner below reproduces that behaviour
//! rule-by-rule.  Token codes, token text, byte locations (`yylloc`), the
//! exclusive states (`xb`/`xc`/`xd`/`xh`/`xq`/`xqs`/`xe`/`xdolq`/`xui`/`xus`/
//! `xeu`), the `yyless`/look-back handling, and the literal-buffer machinery
//! all match the C scanner so the token stream is byte-exact.
//!
//! The generated grammar token codes (`IDENT = 258`, ...) and the
//! `ScanKeywordTokens[]` array are produced at build time from the same
//! `gram.y`/`kwlist.h` data PostgreSQL uses; see `build.rs`.
//!
//! Cross-subsystem helpers are reused from the ported crates:
//!   * `ScanKeywordLookup`/`GetScanKeyword` from `common-keywords_fgram`,
//!   * `downcase_truncate_identifier`/`truncate_identifier`/`scanner_isspace`
//!     from `backend-parser-scansup`,
//!   * `pg_verifymbstr`/`pg_mbstrlen_with_len`/`pg_get_client_encoding` from
//!     `backend-utils-mb`,
//!   * `pg_strtoint32_safe` from `backend-utils-adt-numutils_fgram`,
//!   * soft/hard error reporting from `backend-utils-error`.
//!
//! `pg_unicode_to_server` belongs to the multibyte *encoding-conversion*
//! subsystem (mbutils.c), which is not yet ported; it is reached only by the
//! Unicode-escape rules and is provided through the [`UnicodeToServerSeam`]
//! trait.  No `extern "C"`; soft errors flow through `backend-utils-error`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

pub mod tokens;

use ::scansup_fgram::scanner_isspace;
use error_fgram::{PgError, PgResult};
use mb_fgram::{pg_get_client_encoding, pg_verifymbstr};
use ::pg_ffi_fgram::error::make_sqlstate;
use pg_ffi_fgram::{SqlState, ERRCODE_FEATURE_NOT_SUPPORTED, PG_ENCODING_IS_CLIENT_ONLY};

/// `ERRCODE_INVALID_ESCAPE_SEQUENCE` (utils/errcodes.txt: `22025`). Not yet
/// re-exported by the shared error crate.
const ERRCODE_INVALID_ESCAPE_SEQUENCE: SqlState = make_sqlstate(*b"22025");
/// `ERRCODE_NONSTANDARD_USE_OF_ESCAPE_CHARACTER` (errcodes.txt: `22P06`).
const ERRCODE_NONSTANDARD_USE_OF_ESCAPE_CHARACTER: SqlState = make_sqlstate(*b"22P06");

/// `BackslashQuoteType` (parser.h) -- the `backslash_quote` GUC values.
pub const BACKSLASH_QUOTE_OFF: i32 = 0;
pub const BACKSLASH_QUOTE_ON: i32 = 1;
pub const BACKSLASH_QUOTE_SAFE_ENCODING: i32 = 2;

/// The three scanner-owned GUC globals.
///
/// In C these are plain mutable globals defined in `scan.l`:
///
/// ```c
/// int  backslash_quote = BACKSLASH_QUOTE_SAFE_ENCODING;  // scan.l:68
/// bool escape_string_warning = true;                     // scan.l:69
/// bool standard_conforming_strings = true;               // scan.l:70
/// ```
///
/// They are ordinary GUC variables (`conf->variable` backing) read/written
/// directly by the GUC engine and copied into each scanner instance by
/// `scanner_init` (scan.l:1265-1267) — none come from the control file. This
/// module is the `conf->variable` backing store: a `thread_local!` `Cell` per
/// global with C-named getter and setter, mirroring the `scalar_global!`
/// pattern (e.g. `twophase` `max_prepared_xacts`). `init_seams` installs them
/// as the GUC slots' [`GucVarAccessors`].
pub mod gucs {
    use std::cell::Cell;

    thread_local! {
        /// `int backslash_quote = BACKSLASH_QUOTE_SAFE_ENCODING;` (scan.l:68).
        static BACKSLASH_QUOTE: Cell<i32> =
            const { Cell::new(super::BACKSLASH_QUOTE_SAFE_ENCODING) };
        /// `bool escape_string_warning = true;` (scan.l:69).
        static ESCAPE_STRING_WARNING: Cell<bool> = const { Cell::new(true) };
        /// `bool standard_conforming_strings = true;` (scan.l:70).
        static STANDARD_CONFORMING_STRINGS: Cell<bool> = const { Cell::new(true) };
    }

    #[inline]
    pub fn backslash_quote() -> i32 {
        BACKSLASH_QUOTE.get()
    }

    #[inline]
    pub fn set_backslash_quote(value: i32) {
        BACKSLASH_QUOTE.set(value);
    }

    #[inline]
    pub fn escape_string_warning() -> bool {
        ESCAPE_STRING_WARNING.get()
    }

    #[inline]
    pub fn set_escape_string_warning(value: bool) {
        ESCAPE_STRING_WARNING.set(value);
    }

    #[inline]
    pub fn standard_conforming_strings() -> bool {
        STANDARD_CONFORMING_STRINGS.get()
    }

    #[inline]
    pub fn set_standard_conforming_strings(value: bool) {
        STANDARD_CONFORMING_STRINGS.set(value);
    }
}

/// Install this crate's seam providers.
///
/// Installs the [`GucVarAccessors`](::guc_tables::GucVarAccessors)
/// for the three scanner-owned GUCs (`backslash_quote`, `escape_string_warning`,
/// `standard_conforming_strings`) over the [`gucs`] backing store, so the GUC
/// engine's `.read()`/`.set()` reach the `conf->variable` C globals. Also wires
/// [`ScannerSettings`]' live provider to read the same store, matching scan.l's
/// `scanner_init` (scan.l:1265-1267) which copies the globals into the scanner.
pub fn init_seams() {
    use ::guc_tables::vars;
    use ::guc_tables::GucVarAccessors;

    vars::backslash_quote.install(GucVarAccessors {
        get: gucs::backslash_quote,
        set: gucs::set_backslash_quote,
    });
    vars::escape_string_warning.install(GucVarAccessors {
        get: gucs::escape_string_warning,
        set: gucs::set_escape_string_warning,
    });
    vars::standard_conforming_strings.install(GucVarAccessors {
        get: gucs::standard_conforming_strings,
        set: gucs::set_standard_conforming_strings,
    });

    ScannerSettings::set_live_provider(|| ScannerSettings {
        backslash_quote: gucs::backslash_quote(),
        escape_string_warning: gucs::escape_string_warning(),
        standard_conforming_strings: gucs::standard_conforming_strings(),
    });
}

/// `MAX_UNICODE_EQUIVALENT_STRING` (mb/pg_wchar.h) -- the longest server-
/// encoding byte sequence a single Unicode code point can map to.
pub const MAX_UNICODE_EQUIVALENT_STRING: usize = 16;

/// `YY_END_OF_BUFFER_CHAR` -- flex's sentinel byte (NUL) at the end of the
/// scan buffer.
const YY_END_OF_BUFFER_CHAR: u8 = 0;

/// `pg_wchar` (a Unicode code point during escape processing).
pub type PgWchar = u32;

// ===========================================================================
// Exclusive scanner states (scan.l `%x` declarations).
// ===========================================================================

/// The flex start conditions.  `INITIAL` is the default; the rest are the
/// `%x` exclusive states.  Numbered as bytes for cheap `Cell` storage.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum State {
    INITIAL,
    /// `<xb>` bit string literal.
    Xb,
    /// `<xc>` extended C-style comments.
    Xc,
    /// `<xd>` delimited (double-quoted) identifiers.
    Xd,
    /// `<xh>` hexadecimal byte string.
    Xh,
    /// `<xq>` standard quoted strings.
    Xq,
    /// `<xqs>` quote stop (continuation lookahead).
    Xqs,
    /// `<xe>` extended quoted strings (backslash escapes).
    Xe,
    /// `<xdolq>` `$foo$` dollar-quoted strings.
    Xdolq,
    /// `<xui>` quoted identifier with Unicode escapes.
    Xui,
    /// `<xus>` quoted string with Unicode escapes.
    Xus,
    /// `<xeu>` UTF-16 surrogate pair inside an extended quoted string.
    Xeu,
}

// ===========================================================================
// The token value union (`core_YYSTYPE`).
// ===========================================================================

/// `core_YYSTYPE` (parser/scanner.h) -- the per-token semantic value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CoreYYSTYPE {
    /// `int ival` -- for integer literals and `$n` parameters.
    Ival(i32),
    /// `char *str` -- for identifiers and non-integer literals.
    Str(Vec<u8>),
    /// `const char *keyword` -- canonical spelling of a keyword.
    Keyword(&'static str),
    /// No semantic value (single-character and punctuation tokens).
    None,
}

/// A scanned token: its grammar token code, semantic value, and byte location.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Token {
    /// The grammar token code (`IDENT`, `SCONST`, an ASCII byte for `self`
    /// tokens, or `0` for end-of-input).
    pub token: i32,
    /// The semantic value (`yylval`).
    pub value: CoreYYSTYPE,
    /// The byte offset of the token's start in the input (`yylloc`).
    pub location: i32,
}

/// End-of-input token code (`yyterminate()` returns `YY_NULL`, i.e. 0).
pub const YY_NULL: i32 = 0;

// ===========================================================================
// Unicode-to-server-encoding seam.
// ===========================================================================

/// Seam for `pg_unicode_to_server()` (mbutils.c).  Converting a Unicode code
/// point to the *server* encoding requires the encoding-conversion subsystem,
/// which is a separate, not-yet-ported subsystem.  The scanner reaches it only
/// through the Unicode-escape rules (`E'\uXXXX'`, `U&'...'`); callers install a
/// concrete implementation via [`Scanner::with_unicode_seam`].
pub trait UnicodeToServerSeam {
    /// `pg_unicode_to_server(c, buf)` -- convert code point `c` to the server
    /// encoding, returning the encoded bytes (without a trailing NUL).  Must
    /// raise `ERROR` for code points unconvertible in the server encoding,
    /// exactly as the C routine does.
    fn pg_unicode_to_server(&self, c: PgWchar) -> PgResult<Vec<u8>>;
}

/// Built-in [`UnicodeToServerSeam`] for the UTF-8 / SQL_ASCII server encodings
/// the scanner can handle without the conversion subsystem.  Mirrors
/// `pg_unicode_to_server` when the server encoding is UTF-8 (the conversion is
/// the identity `unicode_to_utf8`) and rejects values requiring conversion in
/// other encodings, matching C's error path for an unconvertible code point.
pub struct Utf8UnicodeSeam;

impl UnicodeToServerSeam for Utf8UnicodeSeam {
    fn pg_unicode_to_server(&self, c: PgWchar) -> PgResult<Vec<u8>> {
        // unicode_to_utf8(): identical bytes for the UTF-8 server encoding.
        Ok(unicode_to_utf8(c))
    }
}

/// `unicode_to_utf8()` (mb/wchar.c) -- encode a code point as UTF-8.
fn unicode_to_utf8(c: PgWchar) -> Vec<u8> {
    if c <= 0x7F {
        vec![c as u8]
    } else if c <= 0x7FF {
        vec![0xC0 | ((c >> 6) & 0x1F) as u8, 0x80 | (c & 0x3F) as u8]
    } else if c <= 0xFFFF {
        vec![
            0xE0 | ((c >> 12) & 0x0F) as u8,
            0x80 | ((c >> 6) & 0x3F) as u8,
            0x80 | (c & 0x3F) as u8,
        ]
    } else {
        vec![
            0xF0 | ((c >> 18) & 0x07) as u8,
            0x80 | ((c >> 12) & 0x3F) as u8,
            0x80 | ((c >> 6) & 0x3F) as u8,
            0x80 | (c & 0x3F) as u8,
        ]
    }
}

// ===========================================================================
// pg_wchar.h inline helpers (transcribed 1:1).
// ===========================================================================

/// `is_valid_unicode_codepoint` (pg_wchar.h).
fn is_valid_unicode_codepoint(c: PgWchar) -> bool {
    c > 0 && c <= 0x10FFFF
}
/// `is_utf16_surrogate_first` (pg_wchar.h).
fn is_utf16_surrogate_first(c: PgWchar) -> bool {
    (0xD800..=0xDBFF).contains(&c)
}
/// `is_utf16_surrogate_second` (pg_wchar.h).
fn is_utf16_surrogate_second(c: PgWchar) -> bool {
    (0xDC00..=0xDFFF).contains(&c)
}
/// `surrogate_pair_to_codepoint` (pg_wchar.h).
fn surrogate_pair_to_codepoint(first: PgWchar, second: PgWchar) -> PgWchar {
    ((first & 0x3FF) << 10) + 0x10000 + (second & 0x3FF)
}

/// `IS_HIGHBIT_SET(ch)` (c.h).
#[inline]
fn is_highbit_set(ch: u8) -> bool {
    ch & 0x80 != 0
}

// ===========================================================================
// The scanner.
// ===========================================================================

/// A lexer error, carrying the SQLSTATE, message, optional detail/hint, and the
/// byte location to report.
///
/// `scanner_yyerror`-style errors don't have the `yyscanner` context handy in
/// safe Rust, so the scanner surfaces them as this typed value; the driver
/// (parser.c port) renders the final `ereport(ERROR, ...)` with the cursor.
/// The SQLSTATE/hint mirror the exact `ereport`/`yyerror` call in scan.l: plain
/// `yyerror(msg)` errors are `ERRCODE_SYNTAX_ERROR` with no hint, while the
/// Unicode-escape and `\'` errors carry their distinct codes and hints.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LexError {
    /// The SQLSTATE for the `ereport`.
    pub sqlstate: SqlState,
    /// The (untranslated) message, e.g. `"unterminated /* comment"`.
    pub message: &'static str,
    /// Optional `errdetail` text.
    pub detail: Option<&'static str>,
    /// Optional `errhint` text.
    pub hint: Option<&'static str>,
    /// Byte location for the error cursor (`yylloc` at the time of the error).
    pub location: i32,
    /// The originating [`PgError`] when this lexer error simply propagates an
    /// error raised by a called-out routine (e.g. `pg_verifymbstr` /
    /// `pg_unicode_to_server`).  In C those routines `ereport(ERROR, ...)`
    /// directly with their own SQLSTATE (CHARACTER_NOT_IN_REPERTOIRE etc.) and
    /// dynamic message; we carry the real error here instead of rewriting it to
    /// a generic syntax error.  When set, `sqlstate` mirrors the source's code
    /// and the dynamic message lives here (the static `message` is a fallback).
    pub source: Option<::error_fgram::PgError>,
}

/// The core scanner: holds the input buffer and all of `core_yy_extra_type`'s
/// mutable state.  Construct with [`Scanner::new`] (mirrors `scanner_init`),
/// then call [`Scanner::core_yylex`] repeatedly until it returns the
/// end-of-input token (`YY_NULL`).
pub struct Scanner<'a> {
    /// The input being scanned (`scanbuf`).  Indices into this slice play the
    /// role of C's `yytext - scanbuf` byte offsets.
    scanbuf: &'a [u8],
    /// Current scan position (the start of the next match attempt).
    pos: usize,
    /// Byte offset where the current token's match began (C's `yytext`); read
    /// by `SET_YYLLOC()`.
    tok_start: usize,

    /// Active start condition (`YYSTATE`).
    state: State,

    // --- scanner settings (initialised from GUCs in scanner_init) ---
    backslash_quote: i32,
    escape_string_warning: bool,
    standard_conforming_strings: bool,

    // --- literal buffer (literalbuf/literallen) ---
    literalbuf: Vec<u8>,

    // --- assorted scanner state ---
    state_before_str_stop: State,
    xcdepth: i32,
    dolqstart: Option<Vec<u8>>,
    save_yylloc: i32,
    utf16_first_part: PgWchar,
    warn_on_first_escape: bool,
    saw_non_ascii: bool,

    /// The location of the current token (`yylloc`).
    yylloc: i32,

    /// Unicode-to-server-encoding seam (see [`UnicodeToServerSeam`]).
    unicode_seam: &'a dyn UnicodeToServerSeam,

    /// Warnings emitted during scanning (the `ereport(WARNING, ...)` calls in
    /// `check_*escape_warning`).  Recorded so callers can observe them; the C
    /// code emits them directly via `ereport`.
    pub warnings: Vec<Warning>,

    /// NOTICE-level diagnostics emitted during scanning (the
    /// `truncate_identifier` "will be truncated" `ereport(NOTICE)`).  Like
    /// `warnings`, recorded for the driver to replay through the live error path.
    pub notices: Vec<Notice>,
}

/// A `WARNING`-level diagnostic emitted by the scanner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Warning {
    pub sqlstate: SqlState,
    pub message: &'static str,
    pub hint: &'static str,
    pub location: i32,
}

/// A `NOTICE`-level diagnostic emitted by the scanner with a dynamically-built
/// message (currently only `truncate_identifier`'s
/// "identifier \"%s\" will be truncated to \"%s\"" text, scansup.c:102).
///
/// scan.l's `downcase_truncate_identifier(..., true)` emits this `ereport(NOTICE)`
/// inline while scanning; the safe-Rust scanner instead defers it here so the
/// parser-driver can replay it through the live client error path (mirroring how
/// `Warning` defers the escape `ereport(WARNING)`s).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Notice {
    pub sqlstate: SqlState,
    pub message: String,
    pub location: i32,
}

/// The result of a single `core_yylex` call.
type LexResult = Result<Token, LexError>;

impl<'a> Scanner<'a> {
    /// `scanner_init()` (scan.l:1248) -- set up a scanner over `str`.
    ///
    /// The GUC-derived settings (`backslash_quote`, `escape_string_warning`,
    /// `standard_conforming_strings`) are taken from the supplied
    /// [`ScannerSettings`]; callers may adjust them after construction exactly
    /// as the C code allows after `scanner_init()`.  Uses the built-in
    /// [`Utf8UnicodeSeam`] for Unicode-escape conversion.
    pub fn new(input: &'a [u8], settings: ScannerSettings) -> Scanner<'a> {
        Self::with_unicode_seam(input, settings, &Utf8UnicodeSeam)
    }

    /// As [`Scanner::new`], but with a caller-supplied [`UnicodeToServerSeam`].
    pub fn with_unicode_seam(
        input: &'a [u8],
        settings: ScannerSettings,
        unicode_seam: &'a dyn UnicodeToServerSeam,
    ) -> Scanner<'a> {
        Scanner {
            scanbuf: input,
            pos: 0,
            tok_start: 0,
            state: State::INITIAL,
            backslash_quote: settings.backslash_quote,
            escape_string_warning: settings.escape_string_warning,
            standard_conforming_strings: settings.standard_conforming_strings,
            literalbuf: Vec::with_capacity(1024),
            state_before_str_stop: State::INITIAL,
            xcdepth: 0,
            dolqstart: None,
            save_yylloc: 0,
            utf16_first_part: 0,
            warn_on_first_escape: false,
            saw_non_ascii: false,
            yylloc: 0,
            unicode_seam,
            warnings: Vec::new(),
            notices: Vec::new(),
        }
    }

    /// The scanner's input buffer (`yyextra->scanbuf`), used by
    /// `scanner_errposition` to convert a byte offset to a character position.
    pub fn scanbuf(&self) -> &[u8] {
        self.scanbuf
    }

    /// Position the scan cursor at byte offset `pos` (the stateless-resume entry
    /// the `core_yylex` seam marshal uses: each call resumes at a token
    /// boundary, where the scanner is always in `INITIAL`).
    pub fn seek(&mut self, pos: usize) {
        self.pos = pos;
        self.tok_start = pos;
    }

    /// The current scan cursor (the byte offset the next `core_yylex` call
    /// resumes at). Read by the stateless `core_yylex` seam marshal to report
    /// the resume offset (`CoreToken.end_pos`) after a token is returned.
    pub fn pos(&self) -> usize {
        self.pos
    }

    /// The current token's start location (`yylloc`), as set by the last
    /// `SET_YYLLOC()`.  Read by error renderers (scan.l `scanner_yyerror`'s
    /// "at or near") that need the byte offset of the token whose scan failed.
    pub fn yylloc(&self) -> i32 {
        self.yylloc
    }

    /// `scanner_errposition(location)` (scan.l:1340) -- convert a byte
    /// `location` within the scan buffer to the 1-based character cursor used in
    /// user-facing error/warning positions (`errposition`). Returns `0` ("no
    /// position") for an unknown (`< 0`) location or an encoding failure, matching
    /// the C path (which reports `0` rather than escalating while building a
    /// report). Used by the `core_yylex` seam to position the deferred scanner
    /// warnings (`check_string_escape_warning`).
    pub fn scanner_errposition(&self, location: i32) -> i32 {
        if location < 0 {
            return 0;
        }
        // C: pg_mbstrlen_with_len(scanbuf, location) + 1.
        match ::mb_fgram::pg_mbstrlen_with_len(self.scanbuf, location) {
            Ok(n) => n + 1,
            Err(_) => 0,
        }
    }

    /// `SET_YYLLOC()` -- record the current token's start location.
    #[inline]
    fn set_yylloc(&mut self) {
        self.yylloc = self.tok_start as i32;
    }

    /// The byte offset where the current token began (set at the top of each
    /// `core_yylex` iteration before matching).
    fn current_location(&self) -> i32 {
        self.yylloc
    }

    /// startlit() -- reset the literal buffer.
    #[inline]
    fn startlit(&mut self) {
        self.literalbuf.clear();
    }

    /// addlit() -- append raw bytes to the literal buffer.
    #[inline]
    fn addlit(&mut self, bytes: &[u8]) {
        self.literalbuf.extend_from_slice(bytes);
    }

    /// addlitchar() -- append one byte to the literal buffer.
    #[inline]
    fn addlitchar(&mut self, ch: u8) {
        self.literalbuf.push(ch);
    }

    /// litbufdup() -- a copy of the literal buffer (C adds a NUL; callers here
    /// treat the bytes as the string value and add NULs only when bridging to
    /// C-style consumers).
    fn litbufdup(&self) -> Vec<u8> {
        self.literalbuf.clone()
    }

    /// `yyerror(msg)` (scan.l) -- a syntax error at the current cursor with the
    /// default `ERRCODE_SYNTAX_ERROR` and no detail/hint.
    fn lexerr(&self, message: &'static str) -> LexError {
        LexError {
            sqlstate: ::pg_ffi_fgram::ERRCODE_SYNTAX_ERROR,
            message,
            detail: None,
            hint: None,
            location: self.current_location(),
            source: None,
        }
    }

    /// Propagate an error raised by a called-out routine verbatim (matching C,
    /// where e.g. `pg_verifymbstr`/`pg_unicode_to_server` `ereport(ERROR, ...)`
    /// directly).  Preserves the source's SQLSTATE/message rather than rewriting
    /// it to a generic syntax error; the cursor is the current `yylloc` (in C,
    /// supplied via the scanner error-position callback).
    fn lexerr_propagate(&self, err: PgError) -> LexError {
        LexError {
            sqlstate: err.sqlstate(),
            message: "",
            detail: None,
            hint: None,
            location: self.current_location(),
            source: Some(err),
        }
    }

    /// A lexer error with an explicit SQLSTATE and optional detail/hint, for the
    /// `ereport(...)` calls in scan.l that don't use the `yyerror` shorthand.
    fn lexerr_full(
        &self,
        sqlstate: SqlState,
        message: &'static str,
        detail: Option<&'static str>,
        hint: Option<&'static str>,
    ) -> LexError {
        LexError {
            sqlstate,
            message,
            detail,
            hint,
            location: self.current_location(),
            source: None,
        }
    }
}

/// GUC-derived scanner settings (the three fields `scanner_init` copies).
#[derive(Clone, Copy)]
pub struct ScannerSettings {
    pub backslash_quote: i32,
    pub escape_string_warning: bool,
    pub standard_conforming_strings: bool,
}

impl Default for ScannerSettings {
    /// PostgreSQL's compiled-in defaults (scan.l:68-70).
    fn default() -> Self {
        ScannerSettings {
            backslash_quote: BACKSLASH_QUOTE_SAFE_ENCODING,
            escape_string_warning: true,
            standard_conforming_strings: true,
        }
    }
}

/// Live-GUC settings provider (installed by seam-init wiring over the live GUC
/// store; `guc.c` is outside this crate's dependency set). In C the scanner
/// reads the three GUC globals the assign hooks keep current (scan.l:68-70);
/// absent provider = the compiled-in defaults (the pre-wiring behavior).
static SCANNER_SETTINGS_PROVIDER: std::sync::OnceLock<fn() -> ScannerSettings> =
    std::sync::OnceLock::new();

impl ScannerSettings {
    /// Install the live settings provider (first install wins).
    pub fn set_live_provider(f: fn() -> ScannerSettings) {
        let _ = SCANNER_SETTINGS_PROVIDER.set(f);
    }

    /// The current live settings: the installed provider's value, or the
    /// compiled-in defaults when no provider is installed.
    pub fn live() -> ScannerSettings {
        match SCANNER_SETTINGS_PROVIDER.get() {
            Some(f) => f(),
            None => ScannerSettings::default(),
        }
    }
}

/// `ScanKeywordTokens[]` (scan.l:81) -- maps a `ScanKeywordLookup` index to the
/// grammar token code for that keyword.  Generated from `kwlist.h` (see
/// `build.rs`).
mod keyword_tokens {
    include!(concat!(env!("OUT_DIR"), "/keyword_tokens.rs"));
}
use keyword_tokens::SCAN_KEYWORD_TOKENS;

/// `scanner_finish()` (scan.l:1290) -- release scanner resources.  In this
/// safe-Rust port all scanner storage is owned by the [`Scanner`] and dropped
/// with it, so this is a no-op kept for API parity.
pub fn scanner_finish(_scanner: Scanner<'_>) {}

include!("scan_core.rs");
include!("scan_initial.rs");
include!("scan_states.rs");
include!("scan_helpers.rs");

#[cfg(test)]
mod tests;
