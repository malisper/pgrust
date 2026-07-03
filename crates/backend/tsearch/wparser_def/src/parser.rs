use core::ffi::c_int;

use ::mcx::Mcx;
use ::types_error::PgResult;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TParserState {
    Base,
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
    Null,
}

const A_BINGO: u16 = 0x0001;
const A_POP: u16 = 0x0002;
const A_PUSH: u16 = 0x0004;
const A_RERUN: u16 = 0x0008;
const A_CLEAR: u16 = 0x0010;
const A_MERGE: u16 = 0x0020;
const A_CLRALL: u16 = 0x0040;
const A_NEXT: u16 = 0x0000;

#[derive(Clone, Copy, PartialEq, Eq)]
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

#[derive(Clone, Copy, PartialEq, Eq)]
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

#[derive(Clone, Copy)]
struct TParserPosition {
    posbyte: usize,
    poschar: usize,
    charlen: usize,
    lenbytetoken: usize,
    lenchartoken: usize,
    state: TParserState,
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

// C TParser: `str` is BORROWED from the caller (TParserInit stores the pointer
// and TParserClose never frees it); the caller keeps the buffer alive for the
// parser's lifetime. Wide copies are parser-owned.
pub struct TParser {
    str_ptr: *const u8,
    lenstr: usize,
    wstr: Option<Vec<u32>>,
    pgwstr: Option<Vec<u32>>,
    usewide: bool,
    charmaxlen: i32,
    stack: Vec<TParserPosition>,
    ignore: bool,
    wanthost: bool,
    c: u8,
    token: usize,
    pub lenbytetoken: usize,
    pub lenchartoken: usize,
    pub type_: i32,
}

extern "C" {
    fn isalnum(c: c_int) -> c_int;
    fn isalpha(c: c_int) -> c_int;
    fn isdigit(c: c_int) -> c_int;
    fn isspace(c: c_int) -> c_int;
    fn isxdigit(c: c_int) -> c_int;
    fn iswalnum(wc: u32) -> c_int;
    fn iswalpha(wc: u32) -> c_int;
    fn iswdigit(wc: u32) -> c_int;
    fn iswspace(wc: u32) -> c_int;
    fn iswxdigit(wc: u32) -> c_int;
    fn mbstowcs(dest: *mut libc::wchar_t, src: *const core::ffi::c_char, n: usize) -> usize;
}

impl TParser {
    fn str(&self) -> &[u8] {
        // SAFETY: caller contract (C TParserInit): the input buffer outlives
        // the parser and has lenstr valid bytes.
        unsafe { core::slice::from_raw_parts(self.str_ptr, self.lenstr) }
    }
    fn top(&self) -> &TParserPosition {
        self.stack.last().expect("TParser stack non-empty")
    }
    fn top_mut(&mut self) -> &mut TParserPosition {
        self.stack.last_mut().expect("TParser stack non-empty")
    }
    pub fn token_bytes(&self) -> &[u8] {
        &self.str()[self.token..self.token + self.lenbytetoken]
    }
    pub fn token_ptr(&self) -> *const u8 {
        // SAFETY: token is a valid byte offset into the borrowed input.
        unsafe { self.str_ptr.add(self.token) }
    }
}

fn new_tparser_position(prev: Option<&TParserPosition>) -> TParserPosition {
    let mut res = match prev {
        Some(p) => *p,
        None => TParserPosition::zeroed(),
    };
    res.pushed_at_action = None;
    res
}

// char2wchar with C's NULL pg_locale_t: plain mbstowcs in the current locale.
fn char2wchar_default(head: &[u8]) -> PgResult<Vec<u32>> {
    let mut nul = Vec::with_capacity(head.len() + 1);
    nul.extend_from_slice(head);
    nul.push(0);
    let mut out: Vec<libc::wchar_t> = vec![0; head.len() + 1];
    // SAFETY: nul is NUL-terminated; at most out.len() wchars written.
    let n = unsafe {
        mbstowcs(
            out.as_mut_ptr(),
            nul.as_ptr() as *const core::ffi::c_char,
            out.len(),
        )
    };
    if n == usize::MAX {
        ::mbutils::pg_verifymbstr(head, false)?;
        return Err(::types_error::PgError::error(
            "invalid multibyte character for locale",
        )
        .into());
    }
    out.truncate(n);
    Ok(out.into_iter().map(|w| w as u32).collect())
}

pub fn tparser_init(mcx: Mcx<'_>, str_ptr: *const u8, len: usize) -> PgResult<TParser> {
    let charmaxlen = ::mbutils::pg_database_encoding_max_length();
    let mut prs = TParser {
        str_ptr,
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
    if charmaxlen > 1 {
        prs.usewide = true;
        // SAFETY: caller contract (C TParserInit): str_ptr has len valid bytes.
        let head = unsafe { core::slice::from_raw_parts(str_ptr, len) };
        if ::pg_locale::database_ctype_is_c() {
            let w = ::mbutils::pg_mb2wchar_with_len(mcx, head)?;
            prs.pgwstr = Some(w.iter().map(|&c| c as u32).collect());
        } else {
            prs.wstr = Some(char2wchar_default(head)?);
        }
    }
    let mut base = new_tparser_position(None);
    base.state = TParserState::Base;
    prs.stack.push(base);
    Ok(prs)
}

fn tparser_copy_init(orig: &TParser) -> TParser {
    let posbyte = orig.top().posbyte;
    let poschar = orig.top().poschar;
    let mut prs = TParser {
        // SAFETY: offset stays inside the borrowed input buffer.
        str_ptr: unsafe { orig.str_ptr.add(posbyte) },
        lenstr: orig.lenstr - posbyte,
        wstr: orig.wstr.as_ref().map(|w| w[poschar..].to_vec()),
        pgwstr: orig.pgwstr.as_ref().map(|w| w[poschar..].to_vec()),
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
    let mut base = new_tparser_position(None);
    base.state = TParserState::Base;
    prs.stack.push(base);
    prs
}

#[derive(Clone, Copy)]
enum IsWhat {
    Alnum,
    Alpha,
    Digit,
    Space,
    Xdigit,
}

impl IsWhat {
    fn byte_fn(self, c: u32) -> i32 {
        // SAFETY: pure ctype calls.
        unsafe {
            match self {
                IsWhat::Alnum => isalnum(c as c_int),
                IsWhat::Alpha => isalpha(c as c_int),
                IsWhat::Digit => isdigit(c as c_int),
                IsWhat::Space => isspace(c as c_int),
                IsWhat::Xdigit => isxdigit(c as c_int),
            }
        }
    }
    fn wide_fn(self, c: u32) -> i32 {
        // SAFETY: pure wctype calls.
        unsafe {
            match self {
                IsWhat::Alnum => iswalnum(c),
                IsWhat::Alpha => iswalpha(c),
                IsWhat::Digit => iswdigit(c),
                IsWhat::Space => iswspace(c),
                IsWhat::Xdigit => iswxdigit(c),
            }
        }
    }
}

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
    which.byte_fn(prs.str()[st.posbyte] as u32)
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

fn p_iseq(prs: &TParser, c: u8) -> i32 {
    let st = prs.top();
    (st.charlen == 1 && prs.str()[st.posbyte] == c) as i32
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
    (st.charlen == 1 && prs.str()[st.posbyte] < 0x80) as i32
}

fn p_isasclet(prs: &TParser) -> i32 {
    (p_isascii(prs) != 0 && p_isalpha(prs) != 0) as i32
}

fn p_isurlchar(prs: &TParser) -> i32 {
    let st = prs.top();
    if st.charlen != 1 {
        return 0;
    }
    let ch = prs.str()[st.posbyte];
    if ch <= 0x20 || ch >= 0x7F {
        return 0;
    }
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

fn p_isspecial(prs: &TParser) -> i32 {
    let st = prs.top();
    if ::mbutils::pg_dsplen(&prs.str()[st.posbyte..]) == 0 {
        return 1;
    }
    if ::mbutils::GetDatabaseEncoding() == ::wchar::PG_UTF8 && prs.usewide {
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
    let mut tmpprs = tparser_copy_init(prs);
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
    Ok(res)
}

fn p_isurlpath(prs: &mut TParser) -> PgResult<i32> {
    let mut tmpprs = tparser_copy_init(prs);
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
    Ok(res)
}

fn special_tags(prs: &mut TParser) {
    let lenchar = prs.top().lenchartoken;
    let new_ignore = {
        let tok = &prs.str()[prs.token..];
        match lenchar {
            8 if strncasecmp(tok, b"</script", 8) == 0 => Some(false),
            7 if strncasecmp(tok, b"</style", 7) == 0 => Some(false),
            7 if strncasecmp(tok, b"<script", 7) == 0 => Some(true),
            6 if strncasecmp(tok, b"<style", 6) == 0 => Some(true),
            _ => None,
        }
    };
    if let Some(v) = new_ignore {
        prs.ignore = v;
    }
}

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

fn strncasecmp(a: &[u8], b: &[u8], n: usize) -> i32 {
    for i in 0..n {
        let ca = a.get(i).copied().unwrap_or(0).to_ascii_lowercase();
        let cb = b.get(i).copied().unwrap_or(0).to_ascii_lowercase();
        if ca != cb {
            return ca as i32 - cb as i32;
        }
        if ca == 0 {
            break;
        }
    }
    0
}

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

include!("tables.rs");

fn run_char_test(prs: &mut TParser, test: CharTest, c: u8) -> PgResult<i32> {
    Ok(match test {
        CharTest::None_ => 1,
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

pub fn tparser_get(prs: &mut TParser) -> PgResult<bool> {
    if prs.top().posbyte >= prs.lenstr {
        return Ok(false);
    }
    prs.token = prs.top().posbyte;
    prs.top_mut().pushed_at_action = None;

    let mut last_flags: Option<u16> = None;

    while prs.top().posbyte <= prs.lenstr {
        let charlen = if prs.top().posbyte == prs.lenstr {
            0
        } else if prs.charmaxlen == 1 {
            1
        } else {
            let pos = prs.top().posbyte;
            ::mbutils::pg_mblen_range(&prs.str()[pos..])? as usize
        };
        prs.top_mut().charlen = charlen;
        debug_assert!(prs.top().posbyte + prs.top().charlen <= prs.lenstr);

        let state = prs.top().state;
        let action = actions_for(state);

        let start_idx = match prs.top().pushed_at_action {
            Some(idx) => {
                prs.top_mut().pushed_at_action = None;
                idx + 1
            }
            None => 0,
        };

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

        run_special(prs, item.special);

        if item.flags & A_BINGO != 0 {
            prs.lenbytetoken = prs.top().lenbytetoken;
            prs.lenchartoken = prs.top().lenchartoken;
            let st = prs.top_mut();
            st.lenbytetoken = 0;
            st.lenchartoken = 0;
            prs.type_ = item.type_;
        }

        if item.flags & A_POP != 0 {
            prs.stack.pop();
            debug_assert!(!prs.stack.is_empty());
        } else if item.flags & A_PUSH != 0 {
            prs.top_mut().pushed_at_action = Some(item_idx);
            let top = *prs.top();
            prs.stack.push(new_tparser_position(Some(&top)));
        } else if item.flags & A_CLEAR != 0 {
            debug_assert!(prs.stack.len() >= 2);
            let below = prs.stack.len() - 2;
            prs.stack.remove(below);
        } else if item.flags & A_CLRALL != 0 {
            let top = *prs.top();
            prs.stack.clear();
            prs.stack.push(top);
        } else if item.flags & A_MERGE != 0 {
            let ptr = prs.stack.pop().expect("A_MERGE with pushed state");
            let st = prs.top_mut();
            st.posbyte = ptr.posbyte;
            st.poschar = ptr.poschar;
            st.charlen = ptr.charlen;
            st.lenbytetoken = ptr.lenbytetoken;
            st.lenchartoken = ptr.lenchartoken;
        }

        if item.tostate != TParserState::Null {
            prs.top_mut().state = item.tostate;
        }

        last_flags = Some(item.flags);

        if (item.flags & A_BINGO != 0)
            || (prs.top().posbyte >= prs.lenstr && (item.flags & A_RERUN) == 0)
        {
            break;
        }

        if item.flags & (A_RERUN | A_POP) != 0 {
            continue;
        }

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
