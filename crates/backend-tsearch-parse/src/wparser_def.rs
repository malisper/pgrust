//! Idiomatic port of `src/backend/tsearch/wparser_def.c` (PostgreSQL 18.3) —
//! the default word parser.
//!
//! Implements the `default` text-search parser: the [`TParser`] state machine
//! that tokenizes text into the parser's token categories (words, numbers,
//! URLs, hosts, emails, tags, ...).  The tokenizing entry points are
//! [`prsd_lextype`], [`prsd_start`], [`prsd_nexttoken`], and [`prsd_end`].
//! The headline selector ([`prsd_headline`]) and its cover/fragment cores
//! ([`hlCover`], [`mark_hl_words`], [`mark_hl_fragments`], [`get_next_fragment`],
//! [`mark_fragment`]) are ported here 1:1.  Only the generic tsquery execution
//! engine they invoke (`TS_execute` / `TS_execute_locations` with the
//! `checkcondition_HL` callback) crosses [`crate::seam`], since that recursive
//! AND/OR/NOT/PHRASE walk belongs to the `tsvector_op` subsystem.
//!
//! # Idiomatic owned model (vs the faithful C-ABI port)
//!
//! The faithful port (`src/crates/backend-tsearch-parse`) keeps the C struct's
//! shape: `char *str`, a `wchar_t *` / `pg_wchar *` wide copy, and raw byte /
//! char offsets, with the encoding/locale helpers reached through a
//! `BackendTsearchParseRuntime` trait of fail-safe defaults.  This idiomatic
//! port instead owns its buffers in a per-parser [`MemoryContext`]:
//!
//!   * the input bytes and the wide-char copies live in context-charged
//!     [`PgVec`]s (NOT faithful `palloc`); [`prsd_end`] / [`TParser::free`]
//!     release every charge so the context's `used()` returns to `0`;
//!   * positions are bounds-checked byte / char indices into those vectors —
//!     **no raw pointers, no `unsafe`** anywhere in the crate;
//!   * the genuinely-external encoding / locale helpers cross the crate-local
//!     [`crate::seam`] module (function-pointer slots, loud-panic default)
//!     rather than trait methods.
//!
//! Everything else — the full state/action tables (in
//! [`wparser_def_tables`](mod@self) via `include!`) and the `TParserGet` driver
//! — is ported 1:1 from the C with the same branch order and token categories.

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use backend_utils_error::PgResult;

use crate::seam;

/* ----------------------------------------------------------------------
 * Output token categories (must match wparser_def.c exactly).
 * -------------------------------------------------------------------- */

pub const ASCIIWORD: i32 = 1;
pub const WORD_T: i32 = 2;
pub const NUMWORD: i32 = 3;
pub const EMAIL: i32 = 4;
pub const URL_T: i32 = 5;
pub const HOST: i32 = 6;
pub const SCIENTIFIC: i32 = 7;
pub const VERSIONNUMBER: i32 = 8;
pub const NUMPARTHWORD: i32 = 9;
pub const PARTHWORD: i32 = 10;
pub const ASCIIPARTHWORD: i32 = 11;
pub const SPACE: i32 = 12;
pub const TAG_T: i32 = 13;
pub const PROTOCOL: i32 = 14;
pub const NUMHWORD: i32 = 15;
pub const ASCIIHWORD: i32 = 16;
pub const HWORD: i32 = 17;
pub const URLPATH: i32 = 18;
pub const FILEPATH: i32 = 19;
pub const DECIMAL_T: i32 = 20;
pub const SIGNEDINT: i32 = 21;
pub const UNSIGNEDINT: i32 = 22;
pub const XMLENTITY: i32 = 23;

pub const LASTNUM: i32 = 23;

pub static TOK_ALIAS: [&str; (LASTNUM + 1) as usize] = [
    "",
    "asciiword",
    "word",
    "numword",
    "email",
    "url",
    "host",
    "sfloat",
    "version",
    "hword_numpart",
    "hword_part",
    "hword_asciipart",
    "blank",
    "tag",
    "protocol",
    "numhword",
    "asciihword",
    "hword",
    "url_path",
    "file",
    "float",
    "int",
    "uint",
    "entity",
];

pub static LEX_DESCR: [&str; (LASTNUM + 1) as usize] = [
    "",
    "Word, all ASCII",
    "Word, all letters",
    "Word, letters and digits",
    "Email address",
    "URL",
    "Host",
    "Scientific notation",
    "Version number",
    "Hyphenated word part, letters and digits",
    "Hyphenated word part, all letters",
    "Hyphenated word part, all ASCII",
    "Space symbols",
    "XML tag",
    "Protocol head",
    "Hyphenated word, letters and digits",
    "Hyphenated word, all ASCII",
    "Hyphenated word, all letters",
    "URL path",
    "File or path name",
    "Decimal notation",
    "Signed integer",
    "Unsigned integer",
    "XML entity",
];

/* ----------------------------------------------------------------------
 * Parser states (TParserState).
 * -------------------------------------------------------------------- */

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(usize)]
pub enum TParserState {
    Base = 0,
    InNumWord,
    InAsciiWord,
    InWord,
    InUnsignedInt,
    InSignedIntFirst,
    InSignedInt,
    InSpace,
    InUDecimalFirst,
    InUDecimal,
    InDecimalFirst,
    InDecimal,
    InVerVersion,
    InSVerVersion,
    InVersionFirst,
    InVersion,
    InMantissaFirst,
    InMantissaSign,
    InMantissa,
    InXMLEntityFirst,
    InXMLEntity,
    InXMLEntityNumFirst,
    InXMLEntityNum,
    InXMLEntityHexNumFirst,
    InXMLEntityHexNum,
    InXMLEntityEnd,
    InTagFirst,
    InXMLBegin,
    InTagCloseFirst,
    InTagName,
    InTagBeginEnd,
    InTag,
    InTagEscapeK,
    InTagEscapeKK,
    InTagBackSleshed,
    InTagEnd,
    InCommentFirst,
    InCommentLast,
    InComment,
    InCloseCommentFirst,
    InCloseCommentLast,
    InCommentEnd,
    InHostFirstDomain,
    InHostDomainSecond,
    InHostDomain,
    InPortFirst,
    InPort,
    InHostFirstAN,
    InHost,
    InEmail,
    InFileFirst,
    InFileTwiddle,
    InPathFirst,
    InPathFirstFirst,
    InPathSecond,
    InFile,
    InFileNext,
    InURLPathFirst,
    InURLPathStart,
    InURLPath,
    InFURL,
    InProtocolFirst,
    InProtocolSecond,
    InProtocolEnd,
    InHyphenAsciiWordFirst,
    InHyphenAsciiWord,
    InHyphenWordFirst,
    InHyphenWord,
    InHyphenNumWordFirst,
    InHyphenNumWord,
    InHyphenDigitLookahead,
    InParseHyphen,
    InParseHyphenHyphen,
    InHyphenWordPart,
    InHyphenAsciiWordPart,
    InHyphenNumWordPart,
    InHyphenUnsignedInt,
    Null, // last state (fake value)
}

/* Flag bits in TParserStateActionItem.flags */
const A_NEXT: u16 = 0x0000;
const A_BINGO: u16 = 0x0001;
const A_POP: u16 = 0x0002;
const A_PUSH: u16 = 0x0004;
const A_RERUN: u16 = 0x0008;
const A_CLEAR: u16 = 0x0010;
const A_MERGE: u16 = 0x0020;
const A_CLRALL: u16 = 0x0040;

/// Character-class test selector (the `isclass` function pointer in C).
///
/// `None_` marks the catch-all entry (C `NULL` isclass), which always matches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CharTest {
    None_,
    IsEOF,
    IsAlnum,
    IsNotAlnum,
    IsAlpha,
    IsDigit,
    IsSpace,
    IsAscLet,
    IsUrlChar,
    IsXdigit,
    IsSpecial,
    IsEqC,
    IsIgnore,
    IsStopHost,
    IsHost,
    IsURLPath,
}

/// Special handler selector (the `special` function pointer in C).
///
/// Variant names mirror the C `Special*` handlers (`SpecialFURL`, etc.).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(clippy::upper_case_acronyms)]
enum Special {
    None_,
    Tags,
    FURL,
    Hyphen,
    VerVersion,
}

#[derive(Clone, Copy)]
struct TParserStateActionItem {
    isclass: CharTest,
    c: u8,
    flags: u16,
    tostate: TParserState,
    type_: i32,
    special: Special,
}

#[derive(Clone, Copy, Debug)]
struct TParserPosition {
    posbyte: usize,
    poschar: usize,
    charlen: usize,
    lenbytetoken: usize,
    lenchartoken: usize,
    state: TParserState,
    /// Index into the active state's action array where this position was
    /// pushed (the C `pushedAtAction` pointer), used to resume after a POP.
    pushed_at_action: Option<usize>,
}

impl TParserPosition {
    fn zeroed() -> Self {
        TParserPosition {
            posbyte: 0,
            poschar: 0,
            charlen: 0,
            lenbytetoken: 0,
            lenchartoken: 0,
            state: TParserState::Base,
            pushed_at_action: None,
        }
    }
}

/// The default word parser, equivalent to the C `TParser`.
///
/// The C struct keeps a `char *str` and (for multibyte) a `wchar_t *` /
/// `pg_wchar *` copy.  Here the input is owned bytes plus an optional wide-char
/// vector, all charged to the parser's internal [`MemoryContext`]; positions
/// are byte / char indices instead of raw pointers.  [`prsd_end`] releases
/// every charge.
pub struct TParser {
    /* string and position information */
    str: Vec<u8>,
    lenstr: usize,
    /// wide character string (libc-locale path) or C-locale `pg_wchar` path.
    /// In both cases the values are stored as `u32` code points.
    wstr: Option<Vec<u32>>,
    pgwstr: Option<Vec<u32>>,
    usewide: bool,

    /* State of parse: stack of positions; top is the last element. */
    charmaxlen: i32,
    stack: Vec<TParserPosition>,
    ignore: bool,
    wanthost: bool,

    /* silly char */
    c: u8,

    /* out */
    /// Byte offset in `str` of the start of the current token.
    token: usize,
    lenbytetoken: usize,
    lenchartoken: usize,
    type_: i32,

}

impl TParser {
    #[inline]
    fn top(&self) -> &TParserPosition {
        self.stack.last().expect("TParser state stack non-empty")
    }
    #[inline]
    fn top_mut(&mut self) -> &mut TParserPosition {
        self.stack
            .last_mut()
            .expect("TParser state stack non-empty")
    }

    /// `TParserClose`: release the parser's owned buffers (the C `palloc`'d
    /// `str` / wide copies, freed by `TParserClose`).  Idempotent (a second
    /// call frees already-empty buffers); the buffers are also freed on drop.
    pub fn free(&mut self) {
        self.str = Vec::new();
        self.wstr = None;
        self.pgwstr = None;
    }

    /// Bytes currently held by the parser's owned buffers (test/leak-gate hook;
    /// the C struct has no such counter).
    #[doc(hidden)]
    pub fn charged_bytes(&self) -> usize {
        self.str.len()
            + self.wstr.as_ref().map_or(0, |w| w.len() * 4)
            + self.pgwstr.as_ref().map_or(0, |w| w.len() * 4)
    }
}

/* ----------------------------------------------------------------------
 * Unicode "strange letters": Mark, Spacing Combining characters that are
 * neither alpha nor word-breakers.  Must stay sorted for binary search.
 * -------------------------------------------------------------------- */

static STRANGE_LETTER: &[u32] = &[
    0x0903, 0x093E, 0x093F, 0x0940, 0x0949, 0x094A, 0x094B, 0x094C, 0x0982, 0x0983, 0x09BE, 0x09BF,
    0x09C0, 0x09C7, 0x09C8, 0x09CB, 0x09CC, 0x09D7, 0x0A03, 0x0A3E, 0x0A3F, 0x0A40, 0x0A83, 0x0ABE,
    0x0ABF, 0x0AC0, 0x0AC9, 0x0ACB, 0x0ACC, 0x0B02, 0x0B03, 0x0B3E, 0x0B40, 0x0B47, 0x0B48, 0x0B4B,
    0x0B4C, 0x0B57, 0x0BBE, 0x0BBF, 0x0BC1, 0x0BC2, 0x0BC6, 0x0BC7, 0x0BC8, 0x0BCA, 0x0BCB, 0x0BCC,
    0x0BD7, 0x0C01, 0x0C02, 0x0C03, 0x0C41, 0x0C42, 0x0C43, 0x0C44, 0x0C82, 0x0C83, 0x0CBE, 0x0CC0,
    0x0CC1, 0x0CC2, 0x0CC3, 0x0CC4, 0x0CC7, 0x0CC8, 0x0CCA, 0x0CCB, 0x0CD5, 0x0CD6, 0x0D02, 0x0D03,
    0x0D3E, 0x0D3F, 0x0D40, 0x0D46, 0x0D47, 0x0D48, 0x0D4A, 0x0D4B, 0x0D4C, 0x0D57, 0x0D82, 0x0D83,
    0x0DCF, 0x0DD0, 0x0DD1, 0x0DD8, 0x0DD9, 0x0DDA, 0x0DDB, 0x0DDC, 0x0DDD, 0x0DDE, 0x0DDF, 0x0DF2,
    0x0DF3, 0x0F3E, 0x0F3F, 0x0F7F, 0x102B, 0x102C, 0x1031, 0x1038, 0x103B, 0x103C, 0x1056, 0x1057,
    0x1062, 0x1063, 0x1064, 0x1067, 0x1068, 0x1069, 0x106A, 0x106B, 0x106C, 0x106D, 0x1083, 0x1084,
    0x1087, 0x1088, 0x1089, 0x108A, 0x108B, 0x108C, 0x108F, 0x17B6, 0x17BE, 0x17BF, 0x17C0, 0x17C1,
    0x17C2, 0x17C3, 0x17C4, 0x17C5, 0x17C7, 0x17C8, 0x1923, 0x1924, 0x1925, 0x1926, 0x1929, 0x192A,
    0x192B, 0x1930, 0x1931, 0x1933, 0x1934, 0x1935, 0x1936, 0x1937, 0x1938, 0x19B0, 0x19B1, 0x19B2,
    0x19B3, 0x19B4, 0x19B5, 0x19B6, 0x19B7, 0x19B8, 0x19B9, 0x19BA, 0x19BB, 0x19BC, 0x19BD, 0x19BE,
    0x19BF, 0x19C0, 0x19C8, 0x19C9, 0x1A19, 0x1A1A, 0x1A1B, 0x1B04, 0x1B35, 0x1B3B, 0x1B3D, 0x1B3E,
    0x1B3F, 0x1B40, 0x1B41, 0x1B43, 0x1B44, 0x1B82, 0x1BA1, 0x1BA6, 0x1BA7, 0x1BAA, 0x1C24, 0x1C25,
    0x1C26, 0x1C27, 0x1C28, 0x1C29, 0x1C2A, 0x1C2B, 0x1C34, 0x1C35, 0xA823, 0xA824, 0xA827, 0xA880,
    0xA881, 0xA8B4, 0xA8B5, 0xA8B6, 0xA8B7, 0xA8B8, 0xA8B9, 0xA8BA, 0xA8BB, 0xA8BC, 0xA8BD, 0xA8BE,
    0xA8BF, 0xA8C0, 0xA8C1, 0xA8C2, 0xA8C3, 0xA952, 0xA953, 0xAA2F, 0xAA30, 0xAA33, 0xAA34, 0xAA4D,
];

/* ----------------------------------------------------------------------
 * TParser lifecycle.
 * -------------------------------------------------------------------- */

fn new_tparser_position(prev: Option<&TParserPosition>) -> TParserPosition {
    let mut res = match prev {
        Some(p) => *p,
        None => TParserPosition::zeroed(),
    };
    res.pushed_at_action = None;
    res
}

/// `TParserInit`: allocate and initialize a parser over `str` (database
/// encoding bytes of length `len`).
///
/// Fallible: the wide-char conversion (`char2wchar` / `pg_mb2wchar_with_len`)
/// can raise on a bad multibyte sequence; that soft `PgError` is propagated
/// rather than being swallowed into an empty wide buffer (which would then
/// index out of bounds in the `p_iswhat` predicates).  On the error path the
/// already-charged input buffer is released so no charge leaks.
fn tparser_init(str: Vec<u8>, len: usize) -> PgResult<TParser> {
    let charmaxlen = crate::seam::pg_database_encoding_max_length::call();

    // The input is owned by the parser (C `palloc`'d `str`).
    let mut prs = TParser {
        str,
        lenstr: len,
        wstr: None,
        pgwstr: None,
        usewide: false,
        charmaxlen,
        stack: Vec::new(),
        ignore: false,
        wanthost: false,
        c: 0,
        token: 0,
        lenbytetoken: 0,
        lenchartoken: 0,
        type_: 0,
    };

    // Use wide char code only when max encoding length > 1.
    if prs.charmaxlen > 1 {
        prs.usewide = true;
        let head = prs.str[..prs.lenstr].to_vec();
        if seam::database_ctype_is_c::call() {
            // char2wchar doesn't work for C-locale; use the pg_wchar path.
            match seam::pg_mb2wchar_with_len::call(head) {
                Ok(w) => {
                    prs.pgwstr = Some(w);
                }
                Err(e) => {
                    prs.free();
                    return Err(e);
                }
            }
        } else {
            match seam::char2wchar::call(head) {
                Ok(w) => {
                    prs.wstr = Some(w);
                }
                Err(e) => {
                    prs.free();
                    return Err(e);
                }
            }
        }
    } else {
        prs.usewide = false;
    }

    let mut base = new_tparser_position(None);
    base.state = TParserState::Base;
    prs.stack.push(base);

    Ok(prs)
}

fn oom(what: &str) -> backend_utils_error::PgError {
    backend_utils_error::PgError::error(format!("out of memory in {what}"))
}

/// `TParserCopyInit`: a parser that shares the original's (wide) string and
/// starts at the original's current position.  Buffers are owned by the copy.
fn tparser_copy_init(orig: &TParser) -> PgResult<TParser> {
    let posbyte = orig.top().posbyte;
    let poschar = orig.top().poschar;

    let str = orig.str[posbyte..].to_vec();

    let mut prs = TParser {
        str,
        lenstr: orig.lenstr - posbyte,
        wstr: None,
        pgwstr: None,
        usewide: orig.usewide,
        charmaxlen: orig.charmaxlen,
        stack: Vec::new(),
        ignore: false,
        wanthost: false,
        c: 0,
        token: 0,
        lenbytetoken: 0,
        lenchartoken: 0,
        type_: 0,
    };

    if let Some(pw) = &orig.pgwstr {
        prs.pgwstr = Some(pw[poschar..].to_vec());
    }
    if let Some(w) = &orig.wstr {
        prs.wstr = Some(w[poschar..].to_vec());
    }

    let mut base = new_tparser_position(None);
    base.state = TParserState::Base;
    prs.stack.push(base);

    Ok(prs)
}

/* ----------------------------------------------------------------------
 * Character-type support functions, equivalent to is* macros.
 * -------------------------------------------------------------------- */

/// Which `p_iswhat` family the predicate dispatches to (the C `byte`/`wide`
/// function-pointer pair).  Replaces the faithful port's `fn(&R, u32) -> i32`
/// pair, since the idiomatic seams are module-level free functions.
#[derive(Clone, Copy)]
enum IsWhat {
    Alnum,
    Alpha,
    Digit,
    Space,
    Xdigit,
}

impl IsWhat {
    #[inline]
    fn byte_fn(self, c: u32) -> i32 {
        match self {
            IsWhat::Alnum => seam::isalnum::call(c),
            IsWhat::Alpha => seam::isalpha::call(c),
            IsWhat::Digit => seam::isdigit::call(c),
            IsWhat::Space => seam::isspace::call(c),
            IsWhat::Xdigit => seam::isxdigit::call(c),
        }
    }
    #[inline]
    fn wide_fn(self, c: u32) -> i32 {
        match self {
            IsWhat::Alnum => seam::iswalnum::call(c),
            IsWhat::Alpha => seam::iswalpha::call(c),
            IsWhat::Digit => seam::iswdigit::call(c),
            IsWhat::Space => seam::iswspace::call(c),
            IsWhat::Xdigit => seam::iswxdigit::call(c),
        }
    }
}

/// The expanded `p_iswhat` macro: dispatch a wide/byte class test, where
/// `nonascii` is the result for >0x7f code points under a C multibyte locale.
fn p_iswhat(prs: &TParser, which: IsWhat, nonascii: i32) -> i32 {
    let st = prs.top();
    if prs.usewide {
        if let Some(pw) = &prs.pgwstr {
            let c = pw[st.poschar];
            if c > 0x7f {
                return nonascii;
            }
            return which.byte_fn(c);
        }
        let w = prs.wstr.as_ref().expect("wstr present when usewide");
        return which.wide_fn(w[st.poschar]);
    }
    which.byte_fn(prs.str[st.posbyte] as u32)
}

fn p_isalnum(prs: &TParser) -> i32 {
    p_iswhat(prs, IsWhat::Alnum, 1)
}
fn p_isnotalnum(prs: &TParser) -> i32 {
    (p_isalnum(prs) == 0) as i32
}
fn p_isalpha(prs: &TParser) -> i32 {
    p_iswhat(prs, IsWhat::Alpha, 1)
}
fn p_isdigit(prs: &TParser) -> i32 {
    p_iswhat(prs, IsWhat::Digit, 0)
}
fn p_isspace(prs: &TParser) -> i32 {
    p_iswhat(prs, IsWhat::Space, 0)
}
fn p_isxdigit(prs: &TParser) -> i32 {
    p_iswhat(prs, IsWhat::Xdigit, 0)
}

/// `p_iseq`: only valid for single-byte ASCII symbols.
fn p_iseq(prs: &TParser, c: u8) -> i32 {
    let st = prs.top();
    (st.charlen == 1 && prs.str[st.posbyte] == c) as i32
}

fn p_iseof(prs: &TParser) -> i32 {
    let st = prs.top();
    (st.posbyte == prs.lenstr || st.charlen == 0) as i32
}

fn p_iseqc(prs: &TParser) -> i32 {
    p_iseq(prs, prs.c)
}

fn p_isascii(prs: &TParser) -> i32 {
    let st = prs.top();
    (st.charlen == 1 && prs.str[st.posbyte] < 0x80) as i32
}

fn p_isasclet(prs: &TParser) -> i32 {
    (p_isascii(prs) != 0 && p_isalpha(prs) != 0) as i32
}

fn p_isurlchar(prs: &TParser) -> i32 {
    let st = prs.top();
    // no non-ASCII need apply
    if st.charlen != 1 {
        return 0;
    }
    let ch = prs.str[st.posbyte];
    // no spaces or control characters
    if ch <= 0x20 || ch >= 0x7F {
        return 0;
    }
    // reject characters disallowed by RFC 3986
    match ch {
        b'"' | b'<' | b'>' | b'\\' | b'^' | b'`' | b'{' | b'|' | b'}' => 0,
        _ => 1,
    }
}

fn p_isstophost(prs: &mut TParser) -> i32 {
    if prs.wanthost {
        prs.wanthost = false;
        return 1;
    }
    0
}

fn p_isignore(prs: &TParser) -> i32 {
    prs.ignore as i32
}

/// `p_isspecial`: zero display length or special signs in several languages.
fn p_isspecial(prs: &TParser) -> i32 {
    let st = prs.top();
    // pg_dsplen could return -1 (error or control character).
    if seam::pg_dsplen::call(&prs.str[st.posbyte..]) == 0 {
        return 1;
    }

    // Unicode "Mark, Spacing Combining" characters: not alpha, not breakers.
    if crate::seam::get_database_encoding::call() == 6 /* PG_UTF8 */ && prs.usewide {
        let c = if let Some(pw) = &prs.pgwstr {
            pw[st.poschar]
        } else {
            prs.wstr.as_ref().expect("wstr present when usewide")[st.poschar]
        };
        if STRANGE_LETTER.binary_search(&c).is_ok() {
            return 1;
        }
    }

    0
}

fn p_ishost(prs: &mut TParser) -> PgResult<i32> {
    let mut tmpprs = tparser_copy_init(prs)?;
    let mut res = 0;

    tmpprs.wanthost = true;

    if tparser_get(&mut tmpprs)? && tmpprs.type_ == HOST {
        let lb = tmpprs.lenbytetoken;
        let lc = tmpprs.lenchartoken;
        let cl = tmpprs.top().charlen;
        let st = prs.top_mut();
        st.posbyte += lb;
        st.poschar += lc;
        st.lenbytetoken += lb;
        st.lenchartoken += lc;
        st.charlen = cl;
        res = 1;
    }

    tmpprs.free();
    Ok(res)
}

fn p_isurlpath(prs: &mut TParser) -> PgResult<i32> {
    let mut tmpprs = tparser_copy_init(prs)?;
    let mut res = 0;

    let top = *tmpprs.top();
    let mut pushed = new_tparser_position(Some(&top));
    pushed.state = TParserState::InURLPathFirst;
    tmpprs.stack.push(pushed);

    if tparser_get(&mut tmpprs)? && tmpprs.type_ == URLPATH {
        let lb = tmpprs.lenbytetoken;
        let lc = tmpprs.lenchartoken;
        let cl = tmpprs.top().charlen;
        let st = prs.top_mut();
        st.posbyte += lb;
        st.poschar += lc;
        st.lenbytetoken += lb;
        st.lenchartoken += lc;
        st.charlen = cl;
        res = 1;
    }

    tmpprs.free();
    Ok(res)
}

/* ----------------------------------------------------------------------
 * Special handlers.
 * -------------------------------------------------------------------- */

fn special_tags(prs: &mut TParser) {
    let lenchar = prs.top().lenchartoken;
    let tok = &prs.str[prs.token..];
    match lenchar {
        8 => {
            // </script
            if strncasecmp(tok, b"</script", 8) == 0 {
                prs.ignore = false;
            }
        }
        7 => {
            if strncasecmp(tok, b"</style", 7) == 0 {
                prs.ignore = false;
            } else if strncasecmp(tok, b"<script", 7) == 0 {
                prs.ignore = true;
            }
        }
        6 => {
            if strncasecmp(tok, b"<style", 6) == 0 {
                prs.ignore = true;
            }
        }
        _ => {}
    }
}

// In C, posbyte/poschar/lenbytetoken/lenchartoken are `int`, and these
// rewinds (`posbyte -= lenbytetoken`) are logically always >= 0 because a
// special only runs once the token of that length has been consumed.  The
// usize port uses `saturating_sub` to mirror C's non-panicking arithmetic
// rather than risk a debug underflow panic if that invariant were ever
// violated by unexpected state.
fn special_furl(prs: &mut TParser) {
    prs.wanthost = true;
    let lb = prs.top().lenbytetoken;
    let lc = prs.top().lenchartoken;
    let st = prs.top_mut();
    st.posbyte = st.posbyte.saturating_sub(lb);
    st.poschar = st.poschar.saturating_sub(lc);
}

fn special_hyphen(prs: &mut TParser) {
    let lb = prs.top().lenbytetoken;
    let lc = prs.top().lenchartoken;
    let st = prs.top_mut();
    st.posbyte = st.posbyte.saturating_sub(lb);
    st.poschar = st.poschar.saturating_sub(lc);
}

fn special_ver_version(prs: &mut TParser) {
    let lb = prs.top().lenbytetoken;
    let lc = prs.top().lenchartoken;
    let st = prs.top_mut();
    st.posbyte = st.posbyte.saturating_sub(lb);
    st.poschar = st.poschar.saturating_sub(lc);
    st.lenbytetoken = 0;
    st.lenchartoken = 0;
}

/// ASCII case-insensitive prefix compare over `n` bytes (`pg_strncasecmp`).
fn strncasecmp(a: &[u8], b: &[u8], n: usize) -> i32 {
    for i in 0..n {
        let ca = a.get(i).copied().unwrap_or(0).to_ascii_lower_or_self();
        let cb = b.get(i).copied().unwrap_or(0).to_ascii_lower_or_self();
        if ca != cb {
            return ca as i32 - cb as i32;
        }
        if ca == 0 {
            break;
        }
    }
    0
}

trait AsciiLower {
    fn to_ascii_lower_or_self(self) -> u8;
}
impl AsciiLower for u8 {
    #[inline]
    fn to_ascii_lower_or_self(self) -> u8 {
        if self.is_ascii_uppercase() {
            self + 32
        } else {
            self
        }
    }
}

/* ----------------------------------------------------------------------
 * State/action table.
 * -------------------------------------------------------------------- */

macro_rules! act {
    ($isclass:ident, $c:expr, $flags:expr, $tostate:ident, $type:expr, $special:ident) => {
        TParserStateActionItem {
            isclass: CharTest::$isclass,
            c: $c,
            flags: $flags,
            tostate: TParserState::$tostate,
            type_: $type,
            special: Special::$special,
        }
    };
}

include!("wparser_def_tables.rs");

/// Run one character-class test for the parser at the current position.
///
/// Fallible because the `IsHost`/`IsURLPath` tests recurse into
/// [`tparser_get`], whose `pg_mblen_range` can raise a soft encoding error.
fn run_char_test(prs: &mut TParser, test: CharTest, c: u8) -> PgResult<i32> {
    Ok(match test {
        CharTest::None_ => 1, // catch-all entry always matches
        CharTest::IsEOF => p_iseof(prs),
        CharTest::IsAlnum => p_isalnum(prs),
        CharTest::IsNotAlnum => p_isnotalnum(prs),
        CharTest::IsAlpha => p_isalpha(prs),
        CharTest::IsDigit => p_isdigit(prs),
        CharTest::IsSpace => p_isspace(prs),
        CharTest::IsAscLet => p_isasclet(prs),
        CharTest::IsUrlChar => p_isurlchar(prs),
        CharTest::IsXdigit => p_isxdigit(prs),
        CharTest::IsSpecial => p_isspecial(prs),
        CharTest::IsEqC => {
            prs.c = c;
            p_iseqc(prs)
        }
        CharTest::IsIgnore => p_isignore(prs),
        CharTest::IsStopHost => p_isstophost(prs),
        CharTest::IsHost => p_ishost(prs)?,
        CharTest::IsURLPath => p_isurlpath(prs)?,
    })
}

fn run_special(prs: &mut TParser, special: Special) {
    match special {
        Special::None_ => {}
        Special::Tags => special_tags(prs),
        Special::FURL => special_furl(prs),
        Special::Hyphen => special_hyphen(prs),
        Special::VerVersion => special_ver_version(prs),
    }
}

/* ----------------------------------------------------------------------
 * The state-machine driver.
 * -------------------------------------------------------------------- */

/// `TParserGet`: advance the parser and produce the next token; returns true
/// when a token (BINGO) was found.
///
/// Fallible: the current-char length uses `pg_mblen_range`, which raises a
/// soft encoding error (SQLSTATE `22021`) on a truncated/invalid multibyte
/// sequence at the end of the buffer, exactly as C's `pg_mblen_range` does.
fn tparser_get(prs: &mut TParser) -> PgResult<bool> {
    // CHECK_FOR_INTERRUPTS() — no-op in the port.

    if prs.top().posbyte >= prs.lenstr {
        return Ok(false);
    }

    prs.token = prs.top().posbyte;
    prs.top_mut().pushed_at_action = None;

    // The action item matched on the last iteration (index into the active
    // state's action table) and its flags, kept so the loop tail can decide
    // how to proceed.  `last_flags` mirrors the C `item` being non-NULL.
    let mut last_flags: Option<u16> = None;

    // look at string
    while prs.top().posbyte <= prs.lenstr {
        // compute current char length
        let charlen = if prs.top().posbyte == prs.lenstr {
            0
        } else if prs.charmaxlen == 1 {
            1
        } else {
            // C: pg_mblen_range(prs->str + posbyte, prs->str + lenstr).  The
            // remaining slice's length is `lenstr - posbyte == end - mbstr`,
            // so the seam's bounds check is exact.
            seam::pg_mblen_range::call(&prs.str[prs.top().posbyte..])? as usize
        };
        prs.top_mut().charlen = charlen;
        // C: Assert(prs->state->posbyte + prs->state->charlen <= prs->lenstr);
        debug_assert!(prs.top().posbyte + prs.top().charlen <= prs.lenstr);

        let state = prs.top().state;
        let action = actions_for(state);

        // Pick the starting action index: resume after a POP, or restart.
        let start_idx = match prs.top().pushed_at_action {
            Some(idx) => {
                prs.top_mut().pushed_at_action = None;
                idx + 1
            }
            None => 0,
        };

        // find action by character class
        let mut item_idx = start_idx;
        loop {
            let item = action[item_idx];
            if item.isclass == CharTest::None_ {
                break;
            }
            if run_char_test(prs, item.isclass, item.c)? != 0 {
                break;
            }
            item_idx += 1;
        }

        let item = action[item_idx];

        // call special handler if exists
        run_special(prs, item.special);

        // BINGO, token is found
        if item.flags & A_BINGO != 0 {
            let (lb, lc) = {
                let st = prs.top();
                (st.lenbytetoken, st.lenchartoken)
            };
            prs.lenbytetoken = lb;
            prs.lenchartoken = lc;
            let st = prs.top_mut();
            st.lenbytetoken = 0;
            st.lenchartoken = 0;
            prs.type_ = item.type_;
        }

        // do various actions by flags
        if item.flags & A_POP != 0 {
            // pop stored state in stack
            prs.stack.pop();
            debug_assert!(!prs.stack.is_empty());
        } else if item.flags & A_PUSH != 0 {
            // push (store) state in stack
            prs.top_mut().pushed_at_action = Some(item_idx);
            let top = *prs.top();
            prs.stack.push(new_tparser_position(Some(&top)));
        } else if item.flags & A_CLEAR != 0 {
            // clear previous pushed state: remove the element below the top
            debug_assert!(prs.stack.len() >= 2);
            let below = prs.stack.len() - 2;
            prs.stack.remove(below);
        } else if item.flags & A_CLRALL != 0 {
            // clear all previous pushed state: keep only the top element
            let top = prs.stack.pop().ok_or_else(|| {
                backend_utils_error::PgError::error("tparser_get: non-empty stack")
            })?;
            prs.stack.clear();
            prs.stack.push(top);
        } else if item.flags & A_MERGE != 0 {
            // merge posinfo with current and pushed state
            debug_assert!(prs.stack.len() >= 2);
            let ptr = prs.stack.pop().ok_or_else(|| {
                backend_utils_error::PgError::error("tparser_get: non-empty stack")
            })?;
            let st = prs.top_mut();
            st.posbyte = ptr.posbyte;
            st.poschar = ptr.poschar;
            st.charlen = ptr.charlen;
            st.lenbytetoken = ptr.lenbytetoken;
            st.lenchartoken = ptr.lenchartoken;
        }

        // set new state if pointed
        if item.tostate != TParserState::Null {
            prs.top_mut().state = item.tostate;
        }

        last_flags = Some(item.flags);

        // check for go away
        if (item.flags & A_BINGO != 0)
            || (prs.top().posbyte >= prs.lenstr && (item.flags & A_RERUN) == 0)
        {
            break;
        }

        // go to beginning of loop if we should rerun or just restored state
        if item.flags & (A_RERUN | A_POP) != 0 {
            continue;
        }

        // move forward
        let charlen = prs.top().charlen;
        if charlen != 0 {
            let st = prs.top_mut();
            st.posbyte += charlen;
            st.lenbytetoken += charlen;
            st.poschar += 1;
            st.lenchartoken += 1;
        }
    }

    Ok(matches!(last_flags, Some(f) if f & A_BINGO != 0))
}

/* ----------------------------------------------------------------------
 * Public parser interface used by ts_parse.c.
 * -------------------------------------------------------------------- */

/// `prsd_start`: create a parser over `str`.
///
/// Fallible: propagates the wide-char conversion error from `tparser_init`.
pub fn prsd_start(str: Vec<u8>, len: usize) -> PgResult<TParser> {
    tparser_init(str, len)
}

/// `prsd_nexttoken`: advance the parser; on success returns
/// `(type, token_bytes)`; type 0 means no more tokens.
///
/// Fallible: propagates the soft encoding error from `tparser_get`'s
/// `pg_mblen_range`.
pub fn prsd_nexttoken<'a>(prs: &'a mut TParser) -> PgResult<(i32, &'a [u8])> {
    if !tparser_get(prs)? {
        return Ok((0, &[]));
    }
    let start = prs.token;
    let end = start + prs.lenbytetoken;
    Ok((prs.type_, &prs.str[start..end]))
}

/// `prsd_end`: free the parser (releases its charged buffers).
pub fn prsd_end(mut prs: TParser) {
    prs.free();
}

/// `prsd_lextype`: the parser's token-type descriptors as `(lexid, alias,
/// descr)` triples, terminated by a `(0, "", "")` sentinel — matching the C
/// `LexDescr` array of length `LASTNUM + 1`.
pub fn prsd_lextype() -> Vec<(i32, String, String)> {
    let mut descr = Vec::with_capacity((LASTNUM + 1) as usize);
    for i in 1..=LASTNUM {
        descr.push((
            i,
            TOK_ALIAS[i as usize].to_string(),
            LEX_DESCR[i as usize].to_string(),
        ));
    }
    descr.push((0, String::new(), String::new()));
    descr
}

/* ======================================================================
 * ts_headline support (wparser_def.c lines 1928+)
 * ====================================================================== */

/* token type classification macros (wparser_def.c:1934-1936). */

/// `HLIDREPLACE(x)` (`wparser_def.c:1934`): token is to be replaced by a space.
#[inline]
fn hlidreplace(x: i32) -> bool {
    x == TAG_T
}

/// `HLIDSKIP(x)` (`wparser_def.c:1935`): token is to be skipped.
#[inline]
fn hlidskip(x: i32) -> bool {
    x == URL_T || x == NUMHWORD || x == ASCIIHWORD || x == HWORD
}

/// `XMLHLIDSKIP(x)` (`wparser_def.c:1936`): token to skip in highlight-all mode.
#[inline]
fn xmlhlidskip(x: i32) -> bool {
    x == URL_T || x == NUMHWORD || x == ASCIIHWORD || x == HWORD
}

/// `mark_fragment` (`wparser_def.c:2184`): apply highlight marking to the words
/// from `startpos` to `endpos` inclusive, per `highlightall`.  A pure in-memory
/// marking routine over `prs.words[]`, invoked only from the headline selector
/// (`hlCover`/`mark_hl_fragments`/`mark_hl_words`).
pub fn mark_fragment(
    prs: &mut crate::ts_parse::HeadlineParsedText,
    highlightall: bool,
    startpos: i32,
    endpos: i32,
) {
    let mut i = startpos;
    while i <= endpos {
        let idx = i as usize;
        if prs.words[idx].item.is_some() {
            prs.words[idx].selected = true;
        }
        if !highlightall {
            if hlidreplace(prs.words[idx].type_ as i32) {
                prs.words[idx].replace = true;
            } else if hlidskip(prs.words[idx].type_ as i32) {
                prs.words[idx].skip = true;
            }
        } else if xmlhlidskip(prs.words[idx].type_ as i32) {
            prs.words[idx].skip = true;
        }

        prs.words[idx].in_ = !prs.words[idx].repeated;

        i += 1;
    }
}

/* additional token-type classification macros (wparser_def.c:1933,1937,1938). */

/// `TS_IDIGNORE(x)` (`wparser_def.c:1933`): token to ignore for indexing.
#[inline]
fn ts_idignore(x: i32) -> bool {
    x == TAG_T || x == PROTOCOL || x == SPACE || x == XMLENTITY
}

/// `NONWORDTOKEN(x)` (`wparser_def.c:1937`).
#[inline]
fn nonwordtoken(x: i32) -> bool {
    x == SPACE || hlidreplace(x) || hlidskip(x)
}

/// `NOENDTOKEN(x)` (`wparser_def.c:1938`): don't want a fragment to end here.
#[inline]
fn noendtoken(x: i32) -> bool {
    nonwordtoken(x)
        || x == SCIENTIFIC
        || x == VERSIONNUMBER
        || x == DECIMAL_T
        || x == SIGNEDINT
        || x == UNSIGNEDINT
        || ts_idignore(x)
}

/// `INTERESTINGWORD(j)` (`wparser_def.c:1947`): interesting words are
/// non-repeated search terms.
#[inline]
fn interestingword(prs: &crate::ts_parse::HeadlineParsedText, j: usize) -> bool {
    prs.words[j].item.is_some() && !prs.words[j].repeated
}

/// `BADENDPOINT(j)` (`wparser_def.c:1951`): don't want to end at a non-word or
/// a short word, unless interesting.
#[inline]
fn badendpoint(prs: &crate::ts_parse::HeadlineParsedText, j: usize, shortword: i32) -> bool {
    (noendtoken(prs.words[j].type_ as i32) || prs.words[j].len as i32 <= shortword)
        && !interestingword(prs, j)
}

/// `CoverPos` (`wparser_def.c:1955`): one cover (well, really one fragment) for
/// [`mark_hl_fragments`].
#[derive(Clone, Copy, Default)]
struct CoverPos {
    startpos: i32,
    endpos: i32,
    poslen: i32,
    curlen: i32,
    chosen: bool,
    excluded: bool,
}

/// Build the `checkcondition_HL` match-table the tsquery-execute seams consume:
/// `match_table[i]` is `(prs.words[i].item, prs.words[i].pos)` for the words in
/// the (inclusive) index range `[base, last]`, indexed relative to `base`.
///
/// This is the faithful equivalent of passing `&(prs->words[idxb])` with
/// `len = idxe - idxb + 1` to `checkcondition_HL`: the callback scans these
/// entries, matches each query operand by identity (`words[i].item == val`, the
/// operand's query-item index), and reports `words[i].pos`.
fn hl_match_table(
    prs: &crate::ts_parse::HeadlineParsedText,
    base: i32,
    last: i32,
) -> Vec<(Option<usize>, u16)> {
    let mut table = Vec::with_capacity((last - base + 1).max(0) as usize);
    let mut i = base;
    while i <= last {
        let w = &prs.words[i as usize];
        table.push((w.item, w.pos));
        i += 1;
    }
    table
}

/// `hlCover` (`wparser_def.c:2032`): try to find a substring of `prs`' word
/// list that satisfies `query`.
///
/// `locations` is the result of [`crate::seam::ts_execute_locations_hl`] for the
/// query.  `*nextpos` is the lexeme position (NOT word index) to start at
/// (caller seeds it to 0).  On success advances `*nextpos`, sets `*p`/`*q` to
/// the first/last word indices of the (minimal) cover, returns `true`.
#[allow(clippy::too_many_arguments)]
fn hl_cover(
    prs: &crate::ts_parse::HeadlineParsedText,
    query: &crate::ts_parse::TSQuery,
    locations: &[crate::ts_parse::ExecPhraseData],
    nextpos: &mut i32,
    p: &mut i32,
    q: &mut i32,
) -> PgResult<bool> {
    let mut pos = *nextpos;

    // This loop repeats when our selected word-range fails the query.
    loop {
        // For each AND'ed query term or phrase, find its first occurrence at or
        // after pos; set pose to the maximum of those positions.
        let mut pose: i32 = -1;
        for pdata in locations {
            let mut first: i32 = -1;
            for i in 0..pdata.npos as usize {
                // For phrase matches, use the ending lexeme.
                let endp = pdata.pos[i];
                if endp >= pos {
                    first = endp;
                    break;
                }
            }
            if first < 0 {
                return Ok(false); // no more matches for this term
            }
            if first > pose {
                pose = first;
            }
        }

        if pose < 0 {
            return Ok(false); // we only get here if empty list
        }

        // For each AND'ed query term or phrase, find its last occurrence at or
        // before pose; set posb to the minimum of those positions.  We start
        // posb at INT_MAX - 1 to guarantee no overflow computing posb + 1.
        let mut posb: i32 = i32::MAX - 1;
        for pdata in locations {
            let mut last: i32 = -1;
            let mut i = pdata.npos - 1;
            while i >= 0 {
                // For phrase matches, use the starting lexeme.
                let startp = pdata.pos[i as usize] - pdata.width;
                if startp <= pose {
                    last = startp;
                    break;
                }
                i -= 1;
            }
            if last < posb {
                posb = last;
            }
        }

        // posb could be to the left of pos if a phrase match crosses pos.  Try
        // the match starting at pos anyway (TS_execute_locations is imprecise
        // for phrase matches OR'd with plain matches).
        posb = posb.max(pos);

        // This test probably always succeeds, but be paranoid.
        if posb <= pose {
            // posb .. pose is the shortest, earliest-after-pos lexeme range
            // containing all query terms.  Convert to prs->words[] indexes.
            let mut idxb: i32 = -1;
            let mut idxe: i32 = -1;

            for i in 0..prs.curwords as usize {
                if prs.words[i].item.is_none() {
                    continue;
                }
                if idxb < 0 && prs.words[i].pos as i32 >= posb {
                    idxb = i as i32;
                }
                if prs.words[i].pos as i32 <= pose {
                    idxe = i as i32;
                } else {
                    break;
                }
            }

            // This test probably always succeeds, but be paranoid.
            if idxb >= 0 && idxe >= idxb {
                // Check that the selected range satisfies the query.
                let table = hl_match_table(prs, idxb, idxe);
                if seam::ts_execute_hl::call(
                    query.items.clone(),
                    table,
                    crate::ts_parse::TS_EXEC_EMPTY,
                )? {
                    // Match!  Advance *nextpos and return the word range.
                    *nextpos = posb + 1;
                    *p = idxb;
                    *q = idxe;
                    return Ok(true);
                }
            }
        }

        // Advance pos and try again.  Any later workable match must start
        // beyond posb.
        pos = posb + 1;
    }
}

/// `get_next_fragment` (`wparser_def.c:2221`): split a cover substring into
/// fragments not longer than `max_words`.  At entry `*startpos`/`*endpos` are
/// the remaining bounds of the cover; they are updated to the next fragment.
/// `*curlen`/`*poslen` get the fragment's length in words / interesting words.
fn get_next_fragment(
    prs: &crate::ts_parse::HeadlineParsedText,
    startpos: &mut i32,
    endpos: &mut i32,
    curlen: &mut i32,
    poslen: &mut i32,
    max_words: i32,
) {
    // first move startpos to an item
    let mut i = *startpos;
    while i <= *endpos {
        *startpos = i;
        if interestingword(prs, i as usize) {
            break;
        }
        i += 1;
    }
    // cut endpos to have only max_words
    *curlen = 0;
    *poslen = 0;
    i = *startpos;
    while i <= *endpos && *curlen < max_words {
        if !nonwordtoken(prs.words[i as usize].type_ as i32) {
            *curlen += 1;
        }
        if interestingword(prs, i as usize) {
            *poslen += 1;
        }
        i += 1;
    }
    // if the cover was cut then move back endpos to a query item
    if *endpos > i {
        *endpos = i;
        let mut j = *endpos;
        while j >= *startpos {
            *endpos = j;
            if interestingword(prs, j as usize) {
                break;
            }
            if !nonwordtoken(prs.words[j as usize].type_ as i32) {
                *curlen -= 1;
            }
            j -= 1;
        }
    }
}

/// `mark_hl_fragments` (`wparser_def.c:2272`): headline selector used when
/// `MaxFragments > 0`.  (In this mode `highlightall` is disregarded for phrase
/// selection; it only controls presentation details.)
#[allow(clippy::too_many_arguments)]
fn mark_hl_fragments(
    prs: &mut crate::ts_parse::HeadlineParsedText,
    query: &crate::ts_parse::TSQuery,
    locations: &[crate::ts_parse::ExecPhraseData],
    highlightall: bool,
    shortword: i32,
    min_words: i32,
    max_words: i32,
    max_fragments: i32,
) -> PgResult<()> {
    let mut poslen: i32 = 0;
    let mut curlen: i32 = 0;
    let mut i: i32;
    let mut num_f: i32 = 0;
    let mut stretch: i32;
    let mut maxstretch: i32;
    let mut posmarker: i32;

    // C declares `startpos = 0, endpos = 0`; here they are only ever read after
    // assignment (from the cover bounds, or in the no-match fallback), so leave
    // them uninitialized to avoid a dead-store.
    let mut startpos: i32;
    let mut endpos: i32;
    let mut nextpos: i32 = 0;
    let mut p: i32 = 0;
    let mut q: i32 = 0;

    let mut covers: Vec<CoverPos> = Vec::new();

    // get all covers
    while hl_cover(prs, query, locations, &mut nextpos, &mut p, &mut q)? {
        startpos = p;
        endpos = q;

        // Break the cover into smaller fragments such that each fragment has at
        // most max_words and each end is a query word.
        while startpos <= endpos {
            get_next_fragment(
                prs,
                &mut startpos,
                &mut endpos,
                &mut curlen,
                &mut poslen,
                max_words,
            );
            covers.push(CoverPos {
                startpos,
                endpos,
                curlen,
                poslen,
                chosen: false,
                excluded: false,
            });
            startpos = endpos + 1;
            endpos = q;
        }
    }
    let numcovers = covers.len() as i32;

    // choose best covers
    for _f in 0..max_fragments {
        let mut maxitems: i32 = 0;
        let mut minwords: i32 = i32::MAX;
        let mut min_i: i32 = -1;

        // Choose the cover that contains max items; on a tie, the one with the
        // smaller number of words.
        i = 0;
        while i < numcovers {
            let c = &covers[i as usize];
            if !c.chosen
                && !c.excluded
                && (maxitems < c.poslen || (maxitems == c.poslen && minwords > c.curlen))
            {
                maxitems = c.poslen;
                minwords = c.curlen;
                min_i = i;
            }
            i += 1;
        }
        // if a cover was found mark it
        if min_i >= 0 {
            let mi = min_i as usize;
            covers[mi].chosen = true;
            // adjust the size of cover
            startpos = covers[mi].startpos;
            endpos = covers[mi].endpos;
            curlen = covers[mi].curlen;
            // stretch the cover if cover size is lower than max_words
            if curlen < max_words {
                // divide the stretch on both sides of cover
                maxstretch = (max_words - curlen) / 2;

                // first stretch the startpos; stop stretching if 1. we hit the
                // beginning of document 2. exceed maxstretch 3. we hit an
                // already marked fragment
                stretch = 0;
                posmarker = startpos;
                i = startpos - 1;
                while i >= 0 && stretch < maxstretch && !prs.words[i as usize].in_ {
                    if !nonwordtoken(prs.words[i as usize].type_ as i32) {
                        curlen += 1;
                        stretch += 1;
                    }
                    posmarker = i;
                    i -= 1;
                }
                // cut back startpos till we find a good endpoint
                i = posmarker;
                while i < startpos && badendpoint(prs, i as usize, shortword) {
                    if !nonwordtoken(prs.words[i as usize].type_ as i32) {
                        curlen -= 1;
                    }
                    i += 1;
                }
                startpos = i;
                // now stretch the endpos as much as possible
                posmarker = endpos;
                i = endpos + 1;
                while i < prs.curwords && curlen < max_words && !prs.words[i as usize].in_ {
                    if !nonwordtoken(prs.words[i as usize].type_ as i32) {
                        curlen += 1;
                    }
                    posmarker = i;
                    i += 1;
                }
                // cut back endpos till we find a good endpoint
                i = posmarker;
                while i > endpos && badendpoint(prs, i as usize, shortword) {
                    if !nonwordtoken(prs.words[i as usize].type_ as i32) {
                        curlen -= 1;
                    }
                    i -= 1;
                }
                endpos = i;
            }
            covers[mi].startpos = startpos;
            covers[mi].endpos = endpos;
            covers[mi].curlen = curlen;
            // Mark the chosen fragments (covers)
            mark_fragment(prs, highlightall, startpos, endpos);
            num_f += 1;
            // Exclude covers overlapping this one from future consideration
            i = 0;
            while i < numcovers {
                if i != min_i {
                    let c = &covers[i as usize];
                    if (c.startpos >= startpos && c.startpos <= endpos)
                        || (c.endpos >= startpos && c.endpos <= endpos)
                        || (c.startpos < startpos && c.endpos > endpos)
                    {
                        covers[i as usize].excluded = true;
                    }
                }
                i += 1;
            }
        } else {
            break; // no selectable covers remain
        }
    }

    // show the first min_words words if we have not marked anything
    if num_f <= 0 {
        startpos = 0;
        curlen = 0;
        endpos = -1;
        i = 0;
        while i < prs.curwords && curlen < min_words {
            if !nonwordtoken(prs.words[i as usize].type_ as i32) {
                curlen += 1;
            }
            endpos = i;
            i += 1;
        }
        mark_fragment(prs, highlightall, startpos, endpos);
    }

    Ok(())
}

/// `mark_hl_words` (`wparser_def.c:2455`): headline selector used when
/// `MaxFragments == 0`.
fn mark_hl_words(
    prs: &mut crate::ts_parse::HeadlineParsedText,
    query: &crate::ts_parse::TSQuery,
    locations: &[crate::ts_parse::ExecPhraseData],
    highlightall: bool,
    shortword: i32,
    min_words: i32,
    max_words: i32,
) -> PgResult<()> {
    let mut nextpos: i32 = 0;
    let mut p: i32 = 0;
    let mut q: i32 = 0;
    let mut bestb: i32 = -1;
    let mut beste: i32 = -1;
    let mut bestlen: i32 = -1;
    let mut bestcover = false;
    let mut pose: i32;
    let mut posb: i32;
    let mut poslen: i32;
    let mut curlen: i32;
    let mut poscover: bool;
    let mut i: i32;

    if !highlightall {
        // examine all covers, select a headline using the best one
        while hl_cover(prs, query, locations, &mut nextpos, &mut p, &mut q)? {
            // Count words (curlen) and interesting words (poslen) within the
            // cover, but stop once we reach max_words.
            curlen = 0;
            poslen = 0;
            posb = p;
            pose = p;
            i = p;
            while i <= q && curlen < max_words {
                if !nonwordtoken(prs.words[i as usize].type_ as i32) {
                    curlen += 1;
                }
                if interestingword(prs, i as usize) {
                    poslen += 1;
                }
                pose = i;
                i += 1;
            }

            if curlen < max_words {
                // We have room to lengthen the headline; search forward until
                // it's full or we find a good stopping point.  Reconsider "q",
                // then move forward.
                i -= 1;
                while i < prs.curwords && curlen < max_words {
                    if i > q {
                        if !nonwordtoken(prs.words[i as usize].type_ as i32) {
                            curlen += 1;
                        }
                        if interestingword(prs, i as usize) {
                            poslen += 1;
                        }
                    }
                    pose = i;
                    if badendpoint(prs, i as usize, shortword) {
                        i += 1;
                        continue;
                    }
                    if curlen >= min_words {
                        break;
                    }
                    i += 1;
                }
                if curlen < min_words {
                    // Reached end of text and headline still shorter than
                    // min_words; try to extend to the left.
                    i = p - 1;
                    while i >= 0 {
                        if !nonwordtoken(prs.words[i as usize].type_ as i32) {
                            curlen += 1;
                        }
                        if interestingword(prs, i as usize) {
                            poslen += 1;
                        }
                        if curlen >= max_words {
                            break;
                        }
                        if badendpoint(prs, i as usize, shortword) {
                            i -= 1;
                            continue;
                        }
                        if curlen >= min_words {
                            break;
                        }
                        i -= 1;
                    }
                    posb = if i >= 0 { i } else { 0 };
                }
            } else {
                // Can't make headline longer; consider making it shorter if
                // needed to avoid a bad endpoint.
                if i > q {
                    i = q;
                }
                while curlen > min_words {
                    if !badendpoint(prs, i as usize, shortword) {
                        break;
                    }
                    if !nonwordtoken(prs.words[i as usize].type_ as i32) {
                        curlen -= 1;
                    }
                    if interestingword(prs, i as usize) {
                        poslen -= 1;
                    }
                    pose = i - 1;
                    i -= 1;
                }
            }

            // Does the proposed headline include the original cover?
            poscover = posb <= p && pose >= q;

            // Adopt this headline if better than the last: prefer headlines
            // including the cover, then more interesting words, then good
            // stopping points.  (bestlen starts at -1, so the first is taken.)
            if (poscover & !bestcover)
                || (poscover == bestcover && poslen > bestlen)
                || (poscover == bestcover
                    && poslen == bestlen
                    && !badendpoint(prs, pose as usize, shortword)
                    && badendpoint(prs, beste as usize, shortword))
            {
                bestb = posb;
                beste = pose;
                bestlen = poslen;
                bestcover = poscover;
            }
        }

        // If nothing acceptable, select min_words words from the beginning.
        if bestlen < 0 {
            curlen = 0;
            pose = -1;
            i = 0;
            while i < prs.curwords && curlen < min_words {
                if !nonwordtoken(prs.words[i as usize].type_ as i32) {
                    curlen += 1;
                }
                pose = i;
                i += 1;
            }
            bestb = 0;
            beste = pose;
        }
    } else {
        // highlightall mode: headline is whole document
        bestb = 0;
        beste = prs.curwords - 1;
    }

    mark_fragment(prs, highlightall, bestb, beste);
    Ok(())
}

/// `prsd_headline` (`wparser_def.c:2616`): the default parser's prsheadline
/// function.  `prsoptions` is the headline option list as `(name, value)`
/// pairs (the idiomatic owned form of the C `List *` of `DefElem`); `query` is
/// the tsquery to highlight against.  Fills `prs.startsel`/`stopsel`/`fragdelim`
/// and marks the selected words.
pub fn prsd_headline(
    prs: &mut crate::ts_parse::HeadlineParsedText,
    prsoptions: &[(String, String)],
    query: &crate::ts_parse::TSQuery,
) -> PgResult<()> {
    use backend_utils_error::ereport;
    use types_error::{ErrorLocation, ERRCODE_INVALID_PARAMETER_VALUE, ERROR};

    // default option values:
    let mut min_words: i32 = 15;
    let mut max_words: i32 = 35;
    let mut shortword: i32 = 3;
    let mut max_fragments: i32 = 0;
    let mut highlightall = false;

    let errloc = || ErrorLocation::new("src/backend/tsearch/wparser_def.c", 0, "prsd_headline");

    // Extract configuration option values
    prs.startsel = Vec::new();
    prs.stopsel = Vec::new();
    prs.fragdelim = Vec::new();
    let mut startsel_set = false;
    let mut stopsel_set = false;
    let mut fragdelim_set = false;
    for (defname, val) in prsoptions {
        if pg_strcasecmp(defname, "MaxWords") {
            max_words = pg_strtoint32(val)?;
        } else if pg_strcasecmp(defname, "MinWords") {
            min_words = pg_strtoint32(val)?;
        } else if pg_strcasecmp(defname, "ShortWord") {
            shortword = pg_strtoint32(val)?;
        } else if pg_strcasecmp(defname, "MaxFragments") {
            max_fragments = pg_strtoint32(val)?;
        } else if pg_strcasecmp(defname, "StartSel") {
            prs.startsel = val.as_bytes().to_vec();
            startsel_set = true;
        } else if pg_strcasecmp(defname, "StopSel") {
            prs.stopsel = val.as_bytes().to_vec();
            stopsel_set = true;
        } else if pg_strcasecmp(defname, "FragmentDelimiter") {
            prs.fragdelim = val.as_bytes().to_vec();
            fragdelim_set = true;
        } else if pg_strcasecmp(defname, "HighlightAll") {
            highlightall = pg_strcasecmp(val, "1")
                || pg_strcasecmp(val, "on")
                || pg_strcasecmp(val, "true")
                || pg_strcasecmp(val, "t")
                || pg_strcasecmp(val, "y")
                || pg_strcasecmp(val, "yes");
        } else {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!("unrecognized headline parameter: \"{defname}\""))
                .finish(errloc());
        }
    }

    // in HighlightAll mode these parameters are ignored
    if !highlightall {
        if min_words >= max_words {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("MinWords must be less than MaxWords")
                .finish(errloc());
        }
        if min_words <= 0 {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("MinWords must be positive")
                .finish(errloc());
        }
        if shortword < 0 {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("ShortWord must be >= 0")
                .finish(errloc());
        }
        if max_fragments < 0 {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("MaxFragments must be >= 0")
                .finish(errloc());
        }
    }

    // Locate words and phrases matching the query
    let locations: Vec<crate::ts_parse::ExecPhraseData> = if query.size > 0 {
        let table = hl_match_table(prs, 0, prs.curwords - 1);
        seam::ts_execute_locations_hl::call(
            query.items.clone(),
            table,
            crate::ts_parse::TS_EXEC_EMPTY,
        )?
    } else {
        Vec::new() // empty query matches nothing
    };

    // Apply appropriate headline selector
    if max_fragments == 0 {
        mark_hl_words(
            prs,
            query,
            &locations,
            highlightall,
            shortword,
            min_words,
            max_words,
        )?;
    } else {
        mark_hl_fragments(
            prs,
            query,
            &locations,
            highlightall,
            shortword,
            min_words,
            max_words,
            max_fragments,
        )?;
    }

    // Fill in default values for string options
    if !startsel_set {
        prs.startsel = b"<b>".to_vec();
    }
    if !stopsel_set {
        prs.stopsel = b"</b>".to_vec();
    }
    if !fragdelim_set {
        prs.fragdelim = b" ... ".to_vec();
    }

    // Caller will need these lengths, too
    prs.startsellen = prs.startsel.len() as i16;
    prs.stopsellen = prs.stopsel.len() as i16;
    prs.fragdelimlen = prs.fragdelim.len() as i16;

    Ok(())
}

/// `pg_strcasecmp(a, b) == 0`: case-insensitive ASCII equality (the comparison
/// `prsd_headline` makes against fixed option names).
fn pg_strcasecmp(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b)
}

/// `pg_strtoint32(val)` (`numutils.c`): parse a base-10 `int32`, raising
/// `22003` (numeric value out of range) / `22P02` (invalid input syntax) on
/// failure, matching the C error the option parser would surface.
fn pg_strtoint32(val: &str) -> PgResult<i32> {
    use backend_utils_error::ereport;
    use types_error::{
        ErrorLocation, ERRCODE_INVALID_TEXT_REPRESENTATION,
        ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERROR,
    };
    let s = val.trim();
    match s.parse::<i32>() {
        Ok(v) => Ok(v),
        Err(_) => {
            // Distinguish overflow from a malformed literal, as pg_strtoint32 does.
            let looks_numeric = {
                let body = s.strip_prefix(['+', '-']).unwrap_or(s);
                !body.is_empty() && body.bytes().all(|c| c.is_ascii_digit())
            };
            if looks_numeric {
                Err(ereport(ERROR)
                    .errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
                    .errmsg(format!("value \"{val}\" is out of range for type integer"))
                    .finish(ErrorLocation::new(
                        "src/backend/utils/adt/numutils.c",
                        0,
                        "pg_strtoint32",
                    ))
                    .unwrap_err())
            } else {
                Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
                    .errmsg(format!("invalid input syntax for type integer: \"{val}\""))
                    .finish(ErrorLocation::new(
                        "src/backend/utils/adt/numutils.c",
                        0,
                        "pg_strtoint32",
                    ))
                    .unwrap_err())
            }
        }
    }
}
