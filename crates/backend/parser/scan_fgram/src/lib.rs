#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

mod dfa;
mod lex;
pub mod tokens {
    #![allow(dead_code)]
    include!(concat!(env!("OUT_DIR"), "/tokens.rs"));
}
mod keyword_tokens {
    include!(concat!(env!("OUT_DIR"), "/keyword_tokens.rs"));
}
pub use keyword_tokens::SCAN_KEYWORD_TOKENS;

use mcx::{Mcx, PgVec};
use types_error::{PgError, PgResult};
use wchar::pg_enc;

pub const BACKSLASH_QUOTE_OFF: i32 = 0;
pub const BACKSLASH_QUOTE_ON: i32 = 1;
pub const BACKSLASH_QUOTE_SAFE_ENCODING: i32 = 2;

pub const YY_NULL: i32 = 0;

// conf->variable backing for the three scanner-owned GUCs (scan.l:68-70).
pub mod gucs {
    use std::cell::Cell;

    thread_local! {
        static BACKSLASH_QUOTE: Cell<i32> = const { Cell::new(super::BACKSLASH_QUOTE_SAFE_ENCODING) };
        static ESCAPE_STRING_WARNING: Cell<bool> = const { Cell::new(true) };
        static STANDARD_CONFORMING_STRINGS: Cell<bool> = const { Cell::new(true) };
    }

    pub fn backslash_quote() -> i32 {
        BACKSLASH_QUOTE.get()
    }
    pub fn set_backslash_quote(v: i32) {
        BACKSLASH_QUOTE.set(v);
    }
    pub fn escape_string_warning() -> bool {
        ESCAPE_STRING_WARNING.get()
    }
    pub fn set_escape_string_warning(v: bool) {
        ESCAPE_STRING_WARNING.set(v);
    }
    pub fn standard_conforming_strings() -> bool {
        STANDARD_CONFORMING_STRINGS.get()
    }
    pub fn set_standard_conforming_strings(v: bool) {
        STANDARD_CONFORMING_STRINGS.set(v);
    }
}

pub fn init_seams() {
    use guc_tables::{vars, GucVarAccessors};
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
}

// Values match scan.c's start-condition defines (asserted by build.rs).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum State {
    Initial = 0,
    Xb = 1,
    Xc = 2,
    Xd = 3,
    Xh = 4,
    Xq = 5,
    Xqs = 6,
    Xe = 7,
    Xdolq = 8,
    Xui = 9,
    Xus = 10,
    Xeu = 11,
}

// core_YYSTYPE (parser/scanner.h), packed to 16 bytes: p + (tag | len<<32),
// crossing core_yylex's out-param like C's 8-byte union instead of a 24-byte
// enum (the V2 store-forwarding class, graviton.md §4.5). Tag values mirror
// gram_core YYSTYPE's low tags so the parser's repack is bit-identical. Str
// borrows the input or the scanner's mcx arena (C pallocs into the parser
// context).
#[derive(Clone, Copy)]
pub struct CoreYYSTYPE<'mcx> {
    p: *const u8,
    meta: u64,
    _arena: core::marker::PhantomData<&'mcx ()>,
}

const _: () = assert!(core::mem::size_of::<CoreYYSTYPE<'static>>() == 16);

const CT_NONE: u32 = 0;
const CT_IVAL: u32 = 1;
const CT_STR: u32 = 2;
const CT_KEYWORD: u32 = 3;

pub enum CoreVal<'mcx> {
    None,
    Ival(i32),
    Str(&'mcx [u8]),
    Keyword(&'static str),
}

impl<'mcx> CoreYYSTYPE<'mcx> {
    pub const None: CoreYYSTYPE<'mcx> = CoreYYSTYPE {
        p: core::ptr::null(),
        meta: CT_NONE as u64,
        _arena: core::marker::PhantomData,
    };

    #[inline(always)]
    fn mk(p: *const u8, tag: u32, aux: u32) -> Self {
        CoreYYSTYPE {
            p,
            meta: tag as u64 | ((aux as u64) << 32),
            _arena: core::marker::PhantomData,
        }
    }

    #[inline(always)]
    pub fn Ival(v: i32) -> Self {
        Self::mk(core::ptr::null(), CT_IVAL, v as u32)
    }

    #[inline(always)]
    pub fn Str(s: &'mcx [u8]) -> Self {
        Self::mk(s.as_ptr(), CT_STR, s.len() as u32)
    }

    #[inline(always)]
    pub fn Keyword(s: &'static str) -> Self {
        Self::mk(s.as_ptr(), CT_KEYWORD, s.len() as u32)
    }

    #[inline(always)]
    pub fn get(&self) -> CoreVal<'mcx> {
        match self.meta as u32 {
            CT_IVAL => CoreVal::Ival((self.meta >> 32) as i32),
            // SAFETY: built by Str from a &'mcx [u8] with this ptr/len.
            CT_STR => CoreVal::Str(unsafe {
                core::slice::from_raw_parts(self.p, (self.meta >> 32) as usize)
            }),
            // SAFETY: built by Keyword from a &'static str with this ptr/len.
            CT_KEYWORD => CoreVal::Keyword(unsafe {
                core::str::from_utf8_unchecked(core::slice::from_raw_parts(
                    self.p,
                    (self.meta >> 32) as usize,
                ))
            }),
            _ => CoreVal::None,
        }
    }
}

// Content equality (tokens re-lexed from different buffers must compare
// equal; gram_core's error-path binary search relies on it).
impl PartialEq for CoreYYSTYPE<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self.get(), other.get()) {
            (CoreVal::None, CoreVal::None) => true,
            (CoreVal::Ival(a), CoreVal::Ival(b)) => a == b,
            (CoreVal::Str(a), CoreVal::Str(b)) => a == b,
            (CoreVal::Keyword(a), CoreVal::Keyword(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for CoreYYSTYPE<'_> {}

impl core::fmt::Debug for CoreYYSTYPE<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self.get() {
            CoreVal::None => f.write_str("None"),
            CoreVal::Ival(v) => write!(f, "Ival({v})"),
            CoreVal::Str(s) => write!(f, "Str({:?})", core::str::from_utf8(s)),
            CoreVal::Keyword(k) => write!(f, "Keyword({k:?})"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Token<'mcx> {
    pub token: i32,
    pub value: CoreYYSTYPE<'mcx>,
    pub location: i32,
}

#[derive(Clone, Copy)]
pub struct ScannerSettings {
    pub backslash_quote: i32,
    pub escape_string_warning: bool,
    pub standard_conforming_strings: bool,
    // GetDatabaseEncoding() / pg_get_client_encoding(), threaded as
    // parameters (no ambient-global seams).
    pub encoding: pg_enc,
    pub client_encoding: pg_enc,
}

impl Default for ScannerSettings {
    fn default() -> Self {
        ScannerSettings {
            backslash_quote: gucs::backslash_quote(),
            escape_string_warning: gucs::escape_string_warning(),
            standard_conforming_strings: gucs::standard_conforming_strings(),
            encoding: wchar::PG_UTF8,
            client_encoding: wchar::PG_UTF8,
        }
    }
}

pub struct Scanner<'mcx> {
    scanbuf: &'mcx [u8],
    mcx: Mcx<'mcx>,
    pos: usize,
    tok_start: usize,
    state: State,
    backslash_quote: i32,
    escape_string_warning: bool,
    standard_conforming_strings: bool,
    encoding: pg_enc,
    client_encoding: pg_enc,
    literalbuf: PgVec<'mcx, u8>,
    state_before_str_stop: State,
    xcdepth: i32,
    dolqstart: Option<&'mcx [u8]>,
    utf16_first_part: u32,
    warn_on_first_escape: bool,
    saw_non_ascii: bool,
    yylloc: i32,
}

impl<'mcx> Scanner<'mcx> {
    // scanner_init (scan.l:1248); the literal buffer allocates lazily so a
    // string-free statement costs zero mallocs.
    pub fn new(input: &'mcx [u8], mcx: Mcx<'mcx>, settings: ScannerSettings) -> Scanner<'mcx> {
        Scanner {
            scanbuf: input,
            mcx,
            pos: 0,
            tok_start: 0,
            state: State::Initial,
            backslash_quote: settings.backslash_quote,
            escape_string_warning: settings.escape_string_warning,
            standard_conforming_strings: settings.standard_conforming_strings,
            encoding: settings.encoding,
            client_encoding: settings.client_encoding,
            literalbuf: PgVec::new_in(mcx),
            state_before_str_stop: State::Initial,
            xcdepth: 0,
            dolqstart: None,
            utf16_first_part: 0,
            warn_on_first_escape: false,
            saw_non_ascii: false,
            yylloc: 0,
        }
    }

    pub fn scanbuf(&self) -> &'mcx [u8] {
        self.scanbuf
    }

    pub fn yylloc(&self) -> i32 {
        self.yylloc
    }

    /// Byte position just past the last-returned token (pl_scanner's
    /// yytext-length reads; flex exposes this as yyleng).
    pub fn tok_end(&self) -> usize {
        self.pos
    }

    pub fn scanner_errposition(&self, location: i32) -> i32 {
        parser_small1::parser_errposition_source(Some(self.scanbuf), location, self.encoding)
    }

    #[inline]
    pub(crate) fn at(&self, p: usize) -> u8 {
        self.scanbuf.get(p).copied().unwrap_or(0)
    }

    #[inline]
    fn set_yylloc(&mut self) {
        self.yylloc = self.tok_start as i32;
    }

    #[inline]
    fn yytext(&self) -> &'mcx [u8] {
        &self.scanbuf[self.tok_start..self.pos]
    }

    #[inline]
    fn yyleng(&self) -> usize {
        self.pos - self.tok_start
    }

    #[inline]
    fn yyless(&mut self, n: usize) {
        self.pos = self.tok_start + n;
    }

    #[inline]
    fn startlit(&mut self) {
        self.literalbuf.clear();
    }

    fn addlit(&mut self, bytes: &[u8]) -> PgResult<()> {
        mcx::vec_append_bytes(&mut self.literalbuf, bytes)
    }

    fn addlitchar(&mut self, ch: u8) -> PgResult<()> {
        mcx::vec_append_bytes(&mut self.literalbuf, &[ch])
    }

    fn litbufdup(&self) -> PgResult<&'mcx [u8]> {
        mcx::slice_borrow_in(self.mcx, &self.literalbuf)
    }

    // scanner_yyerror (scan.l:1355): yyerror(msg) with the "at or near"
    // rendering and cursor position. flex's hold-char NUL bounds the quoted
    // text at the end of the current match.
    #[track_caller]
    #[cold]
    fn yyerr(&self, message: &str) -> Box<PgError> {
        let tail = &self.scanbuf[self.yylloc as usize..self.pos.max(self.yylloc as usize)];
        let tail = &tail[..tail.iter().position(|&b| b == 0).unwrap_or(tail.len())];
        let err = if tail.is_empty() {
            PgError::error(format!("{message} at end of input"))
        } else {
            PgError::error(format!(
                "{message} at or near \"{}\"",
                String::from_utf8_lossy(tail)
            ))
        };
        Box::new(
            err.with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR)
                .with_cursor_position(self.scanner_errposition(self.yylloc)),
        )
    }

    #[track_caller]
    #[cold]
    fn lexerr(
        &self,
        sqlstate: types_error::SqlState,
        message: &'static str,
        detail: Option<&'static str>,
        hint: Option<&'static str>,
    ) -> Box<PgError> {
        let mut err = PgError::error(message).with_sqlstate(sqlstate);
        if let Some(d) = detail {
            err = err.with_detail(d);
        }
        if let Some(h) = hint {
            err = err.with_hint(h);
        }
        Box::new(err.with_cursor_position(self.scanner_errposition(self.yylloc)))
    }
}

#[cfg(test)]
mod tests;
