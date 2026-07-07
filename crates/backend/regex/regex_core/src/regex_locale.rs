
use core::cell::Cell;

use ::mcx::Mcx;
use ::types_core::{Oid, PgWChar, C_COLLATION_OID, OidIsValid};
use ::types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INDETERMINATE_COLLATION};

use ::pg_locale::{PgLocale, WcClass, COLLPROVIDER_BUILTIN, COLLPROVIDER_ICU, COLLPROVIDER_LIBC};
use mbutils_seams as mb_seams;

// Builtin provider ctype: unicode_category.c pg_u_is* over the codepoint,
// posix-restricted per regc_pg_locale.c's `!casemap_full` convention.
fn regex_wc_isclass_builtin(class: WcClass, c: PgWChar, posix: bool) -> bool {
    let code = c as u32;
    match class {
        WcClass::Digit => unicode_category::pg_u_isdigit(code, posix),
        WcClass::Alpha => unicode_category::pg_u_isalpha(code),
        WcClass::Alnum => unicode_category::pg_u_isalnum(code, posix),
        WcClass::Upper => unicode_category::pg_u_isupper(code),
        WcClass::Lower => unicode_category::pg_u_islower(code),
        WcClass::Graph => unicode_category::pg_u_isgraph(code),
        WcClass::Print => unicode_category::pg_u_isprint(code),
        WcClass::Punct => unicode_category::pg_u_ispunct(code, posix),
        WcClass::Space => unicode_category::pg_u_isspace(code),
    }
}

use crate::regex_consts::{NUM_CCLASSES, REG_ECOLLATE, REG_ECTYPE, REG_ERANGE, REG_ESPACE,
    REG_ETOOBIG, REG_FAKE, REG_ULOCALE};
use crate::regex_error::{RegError, RegResult};
use crate::regex_foundation::{addchr, addrange, getcvec};
use crate::regguts::{char_classes, chr, ColorMap, Cvec, CvecRange, MAX_SIMPLE_CHR};


#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PgLocaleStrategy {
    C,
    Builtin,
    LibcWide,
    Libc1Byte,
    Icu,
}

#[derive(Clone, Copy, Debug)]
struct RegexLocaleState {
    strategy: PgLocaleStrategy,
    collation: Oid,
    is_default: bool,
    // Builtin only: pg_u_isdigit/isalnum/ispunct's posix arg is !casemap_full.
    builtin_posix: bool,
    // C's pg_regex_locale: collation-cache entry pointer, live for the
    // Builtin/LibcWide/Libc1Byte/Icu strategies.
    locale: Option<&'static PgLocale>,
}

thread_local! {
    static REGEX_LOCALE_STATE: Cell<RegexLocaleState> = const {
        Cell::new(RegexLocaleState {
            strategy: PgLocaleStrategy::C,
            collation: 0, // InvalidOid
            is_default: false,
            builtin_posix: false,
            locale: None,
        })
    };
}

#[inline]
fn regex_locale_state() -> RegexLocaleState {
    REGEX_LOCALE_STATE.with(|s| s.get())
}

const PG_UTF8: i32 = 6;
const UCHAR_MAX: chr = 255;


const PG_ISDIGIT: u8 = 0x01;
const PG_ISALPHA: u8 = 0x02;
const PG_ISALNUM: u8 = PG_ISDIGIT | PG_ISALPHA;
const PG_ISUPPER: u8 = 0x04;
const PG_ISLOWER: u8 = 0x08;
const PG_ISGRAPH: u8 = 0x10;
const PG_ISPRINT: u8 = 0x20;
const PG_ISPUNCT: u8 = 0x40;
const PG_ISSPACE: u8 = 0x80;

const PG_CHAR_PROPERTIES: [u8; 128] = build_char_properties();

const fn build_char_properties() -> [u8; 128] {
    let mut t = [0u8; 128];
    let mut c = 0x09;
    while c <= 0x0d {
        t[c] = PG_ISSPACE;
        c += 1;
    }
    t[0x20] = PG_ISPRINT | PG_ISSPACE;
    let graph = PG_ISGRAPH | PG_ISPRINT | PG_ISPUNCT;
    let mut c = 0x21;
    while c <= 0x2f {
        t[c] = graph;
        c += 1;
    }
    let mut c = 0x30;
    while c <= 0x39 {
        t[c] = PG_ISDIGIT | PG_ISGRAPH | PG_ISPRINT;
        c += 1;
    }
    let mut c = 0x3a;
    while c <= 0x40 {
        t[c] = graph;
        c += 1;
    }
    let mut c = 0x41;
    while c <= 0x5a {
        t[c] = PG_ISALPHA | PG_ISUPPER | PG_ISGRAPH | PG_ISPRINT;
        c += 1;
    }
    let mut c = 0x5b;
    while c <= 0x60 {
        t[c] = graph;
        c += 1;
    }
    let mut c = 0x61;
    while c <= 0x7a {
        t[c] = PG_ISALPHA | PG_ISLOWER | PG_ISGRAPH | PG_ISPRINT;
        c += 1;
    }
    let mut c = 0x7b;
    while c <= 0x7e {
        t[c] = graph;
        c += 1;
    }
    t
}

#[inline]
fn pg_ascii_toupper(ch: u8) -> u8 {
    if ch.is_ascii_lowercase() {
        ch - b'a' + b'A'
    } else {
        ch
    }
}

#[inline]
fn pg_ascii_tolower(ch: u8) -> u8 {
    if ch.is_ascii_uppercase() {
        ch - b'A' + b'a'
    } else {
        ch
    }
}


pub fn pg_set_regex_collation<'mcx>(mcx: Mcx<'mcx>, collation: Oid) -> PgResult<()> {
    let strategy;
    let mut is_default = false;
    let mut builtin_posix = false;
    let mut active_collation = collation;
    let mut active_locale: Option<&'static PgLocale> = None;

    if !OidIsValid(collation) {
        return Err(PgError::error(
            "could not determine which collation to use for regular expression",
        )
        .with_sqlstate(ERRCODE_INDETERMINATE_COLLATION)
        .with_hint("Use the COLLATE clause to set the collation explicitly.").into());
    }

    if collation == C_COLLATION_OID {
        strategy = PgLocaleStrategy::C;
        active_collation = 0; // locale = 0 (C: pg_regex_locale = NULL)
    } else {
        let _ = mcx;
        let locale = ::pg_locale::pg_newlocale_from_collation(collation)?;

        if !locale.deterministic {
            return Err(PgError::error(
                "nondeterministic collations are not supported for regular expressions",
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED).into());
        }

        if locale.ctype_is_c {
            strategy = PgLocaleStrategy::C;
            active_collation = 0; // locale = 0
        } else {
            // C's pg_regex_locale assignment (regc_pg_locale.c).
            active_locale = Some(locale);
            if locale.provider == COLLPROVIDER_BUILTIN {
                debug_assert_eq!(mb_seams::get_database_encoding::call(), PG_UTF8);
                strategy = PgLocaleStrategy::Builtin;
                builtin_posix = !locale.builtin_casemap_full;
            } else if locale.provider == COLLPROVIDER_ICU {
                strategy = PgLocaleStrategy::Icu;
            } else {
                debug_assert_eq!(locale.provider, COLLPROVIDER_LIBC);
                if mb_seams::get_database_encoding::call() == PG_UTF8 {
                    strategy = PgLocaleStrategy::LibcWide;
                } else {
                    strategy = PgLocaleStrategy::Libc1Byte;
                }
            }
            is_default = locale.is_default;
        }
    }

    REGEX_LOCALE_STATE.with(|s| {
        s.set(RegexLocaleState {
            strategy,
            collation: active_collation,
            is_default,
            builtin_posix,
            locale: if matches!(strategy, PgLocaleStrategy::C) {
                None
            } else {
                active_locale
            },
        })
    });
    Ok(())
}

#[inline]
fn pg_wc_isclass(c: PgWChar, c_bit: u8, class: WcClass) -> bool {
    let st = regex_locale_state();
    match st.strategy {
        PgLocaleStrategy::C => c <= 127 && (PG_CHAR_PROPERTIES[c as usize] & c_bit) != 0,
        PgLocaleStrategy::Builtin => regex_wc_isclass_builtin(class, c, st.builtin_posix),
        PgLocaleStrategy::LibcWide => st.locale.unwrap().wc_isclass_wide(c, class),
        PgLocaleStrategy::Libc1Byte => st.locale.unwrap().wc_isclass_1byte(c, class),
        PgLocaleStrategy::Icu => ::pg_locale::icu_wc_isclass(c, class),
    }
}

pub fn pg_wc_isdigit(c: PgWChar) -> bool {
    pg_wc_isclass(c, PG_ISDIGIT, WcClass::Digit)
}

pub fn pg_wc_isalpha(c: PgWChar) -> bool {
    pg_wc_isclass(c, PG_ISALPHA, WcClass::Alpha)
}

pub fn pg_wc_isalnum(c: PgWChar) -> bool {
    pg_wc_isclass(c, PG_ISALNUM, WcClass::Alnum)
}

pub fn pg_wc_isword(c: PgWChar) -> bool {
    if c == chr_lit('_') {
        return true;
    }
    pg_wc_isalnum(c)
}

pub fn pg_wc_isupper(c: PgWChar) -> bool {
    pg_wc_isclass(c, PG_ISUPPER, WcClass::Upper)
}

pub fn pg_wc_islower(c: PgWChar) -> bool {
    pg_wc_isclass(c, PG_ISLOWER, WcClass::Lower)
}

pub fn pg_wc_isgraph(c: PgWChar) -> bool {
    pg_wc_isclass(c, PG_ISGRAPH, WcClass::Graph)
}

pub fn pg_wc_isprint(c: PgWChar) -> bool {
    pg_wc_isclass(c, PG_ISPRINT, WcClass::Print)
}

pub fn pg_wc_ispunct(c: PgWChar) -> bool {
    pg_wc_isclass(c, PG_ISPUNCT, WcClass::Punct)
}

pub fn pg_wc_isspace(c: PgWChar) -> bool {
    pg_wc_isclass(c, PG_ISSPACE, WcClass::Space)
}

pub fn pg_wc_toupper(c: PgWChar) -> PgWChar {
    let st = regex_locale_state();
    match st.strategy {
        PgLocaleStrategy::C => {
            if c <= 127 {
                pg_ascii_toupper(c as u8) as PgWChar
            } else {
                c
            }
        }
        PgLocaleStrategy::LibcWide => {
            if st.is_default && c <= 127 {
                pg_ascii_toupper(c as u8) as PgWChar
            } else {
                st.locale.unwrap().wc_toupper_wide(c)
            }
        }
        PgLocaleStrategy::Libc1Byte => {
            if st.is_default && c <= 127 {
                pg_ascii_toupper(c as u8) as PgWChar
            } else {
                st.locale.unwrap().wc_toupper_1byte(c)
            }
        }
        PgLocaleStrategy::Builtin => unicode_case::unicode_uppercase_simple(c as u32) as PgWChar,
        PgLocaleStrategy::Icu => ::pg_locale::icu_wc_toupper(c) as PgWChar,
    }
}

pub fn pg_wc_tolower(c: PgWChar) -> PgWChar {
    let st = regex_locale_state();
    match st.strategy {
        PgLocaleStrategy::C => {
            if c <= 127 {
                pg_ascii_tolower(c as u8) as PgWChar
            } else {
                c
            }
        }
        PgLocaleStrategy::LibcWide => {
            if st.is_default && c <= 127 {
                pg_ascii_tolower(c as u8) as PgWChar
            } else {
                st.locale.unwrap().wc_tolower_wide(c)
            }
        }
        PgLocaleStrategy::Libc1Byte => {
            if st.is_default && c <= 127 {
                pg_ascii_tolower(c as u8) as PgWChar
            } else {
                st.locale.unwrap().wc_tolower_1byte(c)
            }
        }
        PgLocaleStrategy::Builtin => unicode_case::unicode_lowercase_simple(c as u32) as PgWChar,
        PgLocaleStrategy::Icu => ::pg_locale::icu_wc_tolower(c) as PgWChar,
    }
}

pub type PgWcProbeFunc = fn(PgWChar) -> bool;

pub fn pg_ctype_get_cache<'mcx>(
    mcx: Mcx<'mcx>,
    probefunc: PgWcProbeFunc,
    cclasscode: i32,
) -> RegResult<Cvec> {
    let st = regex_locale_state();
    let mut effective_cclasscode = cclasscode;

    let max_chr: chr = match st.strategy {
        PgLocaleStrategy::C => {
            effective_cclasscode = -1;
            127
        }
        PgLocaleStrategy::Builtin => MAX_SIMPLE_CHR,
        PgLocaleStrategy::LibcWide => MAX_SIMPLE_CHR,
        PgLocaleStrategy::Libc1Byte => {
            effective_cclasscode = -1;
            UCHAR_MAX
        }
        PgLocaleStrategy::Icu => MAX_SIMPLE_CHR,
    };

    let mut cv = getcvec(mcx, None, 128, 64)?;
    cv.cclasscode = effective_cclasscode;

    let mut nmatches: u32 = 0; // number of consecutive matches
    let mut cur_chr: chr = 0;
    while cur_chr <= max_chr {
        if probefunc(cur_chr) {
            nmatches += 1;
        } else if nmatches > 0 {
            store_match(&mut cv, cur_chr - nmatches, nmatches);
            nmatches = 0;
        }
        cur_chr += 1;
    }

    if nmatches > 0 {
        store_match(&mut cv, cur_chr - nmatches, nmatches);
    }

    Ok(cv)
}

fn store_match(cv: &mut Cvec, chr1: chr, nchrs: u32) {
    if nchrs > 1 {
        cv.ranges.push(CvecRange {
            from: chr1,
            to: chr1 + nchrs - 1,
        });
    } else {
        debug_assert_eq!(nchrs, 1);
        cv.chrs.push(chr1);
    }
}


struct Cname {
    name: &'static str,
    code: u8,
}

const CNAMES: &[Cname] = &[
    Cname { name: "NUL", code: 0o0 },
    Cname { name: "SOH", code: 0o1 },
    Cname { name: "STX", code: 0o2 },
    Cname { name: "ETX", code: 0o3 },
    Cname { name: "EOT", code: 0o4 },
    Cname { name: "ENQ", code: 0o5 },
    Cname { name: "ACK", code: 0o6 },
    Cname { name: "BEL", code: 0o7 },
    Cname { name: "alert", code: 0o7 },
    Cname { name: "BS", code: 0o10 },
    Cname { name: "backspace", code: 0x08 }, // '\b'
    Cname { name: "HT", code: 0o11 },
    Cname { name: "tab", code: 0x09 }, // '\t'
    Cname { name: "LF", code: 0o12 },
    Cname { name: "newline", code: 0x0a }, // '\n'
    Cname { name: "VT", code: 0o13 },
    Cname { name: "vertical-tab", code: 0x0b }, // '\v'
    Cname { name: "FF", code: 0o14 },
    Cname { name: "form-feed", code: 0x0c }, // '\f'
    Cname { name: "CR", code: 0o15 },
    Cname { name: "carriage-return", code: 0x0d }, // '\r'
    Cname { name: "SO", code: 0o16 },
    Cname { name: "SI", code: 0o17 },
    Cname { name: "DLE", code: 0o20 },
    Cname { name: "DC1", code: 0o21 },
    Cname { name: "DC2", code: 0o22 },
    Cname { name: "DC3", code: 0o23 },
    Cname { name: "DC4", code: 0o24 },
    Cname { name: "NAK", code: 0o25 },
    Cname { name: "SYN", code: 0o26 },
    Cname { name: "ETB", code: 0o27 },
    Cname { name: "CAN", code: 0o30 },
    Cname { name: "EM", code: 0o31 },
    Cname { name: "SUB", code: 0o32 },
    Cname { name: "ESC", code: 0o33 },
    Cname { name: "IS4", code: 0o34 },
    Cname { name: "FS", code: 0o34 },
    Cname { name: "IS3", code: 0o35 },
    Cname { name: "GS", code: 0o35 },
    Cname { name: "IS2", code: 0o36 },
    Cname { name: "RS", code: 0o36 },
    Cname { name: "IS1", code: 0o37 },
    Cname { name: "US", code: 0o37 },
    Cname { name: "space", code: b' ' },
    Cname { name: "exclamation-mark", code: b'!' },
    Cname { name: "quotation-mark", code: b'"' },
    Cname { name: "number-sign", code: b'#' },
    Cname { name: "dollar-sign", code: b'$' },
    Cname { name: "percent-sign", code: b'%' },
    Cname { name: "ampersand", code: b'&' },
    Cname { name: "apostrophe", code: b'\'' },
    Cname { name: "left-parenthesis", code: b'(' },
    Cname { name: "right-parenthesis", code: b')' },
    Cname { name: "asterisk", code: b'*' },
    Cname { name: "plus-sign", code: b'+' },
    Cname { name: "comma", code: b',' },
    Cname { name: "hyphen", code: b'-' },
    Cname { name: "hyphen-minus", code: b'-' },
    Cname { name: "period", code: b'.' },
    Cname { name: "full-stop", code: b'.' },
    Cname { name: "slash", code: b'/' },
    Cname { name: "solidus", code: b'/' },
    Cname { name: "zero", code: b'0' },
    Cname { name: "one", code: b'1' },
    Cname { name: "two", code: b'2' },
    Cname { name: "three", code: b'3' },
    Cname { name: "four", code: b'4' },
    Cname { name: "five", code: b'5' },
    Cname { name: "six", code: b'6' },
    Cname { name: "seven", code: b'7' },
    Cname { name: "eight", code: b'8' },
    Cname { name: "nine", code: b'9' },
    Cname { name: "colon", code: b':' },
    Cname { name: "semicolon", code: b';' },
    Cname { name: "less-than-sign", code: b'<' },
    Cname { name: "equals-sign", code: b'=' },
    Cname { name: "greater-than-sign", code: b'>' },
    Cname { name: "question-mark", code: b'?' },
    Cname { name: "commercial-at", code: b'@' },
    Cname { name: "left-square-bracket", code: b'[' },
    Cname { name: "backslash", code: b'\\' },
    Cname { name: "reverse-solidus", code: b'\\' },
    Cname { name: "right-square-bracket", code: b']' },
    Cname { name: "circumflex", code: b'^' },
    Cname { name: "circumflex-accent", code: b'^' },
    Cname { name: "underscore", code: b'_' },
    Cname { name: "low-line", code: b'_' },
    Cname { name: "grave-accent", code: b'`' },
    Cname { name: "left-brace", code: b'{' },
    Cname { name: "left-curly-bracket", code: b'{' },
    Cname { name: "vertical-line", code: b'|' },
    Cname { name: "right-brace", code: b'}' },
    Cname { name: "right-curly-bracket", code: b'}' },
    Cname { name: "tilde", code: b'~' },
    Cname { name: "DEL", code: 0o177 },
];

const CLASS_NAMES: [&str; NUM_CCLASSES as usize] = [
    "alnum", "alpha", "ascii", "blank", "cntrl", "digit", "graph", "lower",
    "print", "punct", "space", "upper", "xdigit", "word",
];

#[inline]
const fn chr_lit(c: char) -> chr {
    c as chr
}

#[inline]
fn name_eq_chrs(name: &str, startp: &[chr]) -> bool {
    let bytes = name.as_bytes();
    if bytes.len() != startp.len() {
        return false;
    }
    bytes
        .iter()
        .zip(startp.iter())
        .all(|(&b, &c)| (b as chr) == c)
}


#[inline]
fn before(x: chr, y: chr) -> bool {
    x < y
}

pub fn element(startp: &[chr]) -> RegResult<ElementResult> {
    debug_assert!(!startp.is_empty());
    let len = startp.len();
    if len == 1 {
        return Ok(ElementResult { code: startp[0], note_ulocale: false });
    }

    for cn in CNAMES {
        if name_eq_chrs(cn.name, startp) {
            return Ok(ElementResult {
                code: chr_lit(cn.code as char),
                note_ulocale: true, // NOTE(REG_ULOCALE)
            });
        }
    }

    Err(RegError::new(REG_ECOLLATE))
}

#[derive(Clone, Copy, Debug)]
pub struct ElementResult {
    pub code: chr,
    pub note_ulocale: bool,
}

pub const ELEMENT_NOTE_ULOCALE: i32 = REG_ULOCALE;

pub fn range<'mcx>(mcx: Mcx<'mcx>, a: chr, b: chr, cases: i32) -> RegResult<Cvec> {
    if a != b && !before(a, b) {
        return Err(RegError::new(REG_ERANGE));
    }

    if cases == 0 {
        let mut cv = getcvec(mcx, None, 0, 1)?;
        addrange(&mut cv, a, b);
        return Ok(cv);
    }

    let nchrs_i: i64 = b as i64 - a as i64 + 1;
    let nchrs: i32 = if nchrs_i <= 0 || nchrs_i > 100000 {
        100000
    } else {
        nchrs_i as i32
    };

    let mut cv = getcvec(mcx, None, nchrs, 1)?;
    addrange(&mut cv, a, b);

    let chrspace = nchrs as usize;
    let mut c = a;
    loop {
        let cc = pg_wc_tolower(c);
        if cc != c && (before(cc, a) || before(b, cc)) {
            if cv.chrs.len() >= chrspace {
                return Err(RegError::new(REG_ETOOBIG));
            }
            addchr(&mut cv, cc);
        }
        let cc = pg_wc_toupper(c);
        if cc != c && (before(cc, a) || before(b, cc)) {
            if cv.chrs.len() >= chrspace {
                return Err(RegError::new(REG_ETOOBIG));
            }
            addchr(&mut cv, cc);
        }
        if c == b {
            break;
        }
        c += 1;
    }

    Ok(cv)
}

pub fn eclass<'mcx>(mcx: Mcx<'mcx>, cflags: i32, c: chr, cases: i32) -> RegResult<Cvec> {
    if (cflags & REG_FAKE) != 0 && c == chr_lit('x') {
        let mut cv = getcvec(mcx, None, 4, 0)?;
        addchr(&mut cv, chr_lit('x'));
        addchr(&mut cv, chr_lit('y'));
        if cases != 0 {
            addchr(&mut cv, chr_lit('X'));
            addchr(&mut cv, chr_lit('Y'));
        }
        return Ok(cv);
    }

    if cases != 0 {
        return allcases(mcx, c);
    }
    let mut cv = getcvec(mcx, None, 1, 0)?;
    addchr(&mut cv, c);
    Ok(cv)
}

pub fn lookupcclass(startp: &[chr]) -> RegResult<i32> {
    for (i, name) in CLASS_NAMES.iter().enumerate() {
        if name_eq_chrs(name, startp) {
            return Ok(i as i32);
        }
    }

    Err(RegError::new(REG_ECTYPE))
}

pub fn cclasscvec<'mcx>(mcx: Mcx<'mcx>, cclasscode: i32, cases: i32) -> RegResult<Cvec> {
    let mut cclasscode = cclasscode;
    if cases != 0
        && (cclasscode == char_classes::CC_LOWER as i32
            || cclasscode == char_classes::CC_UPPER as i32)
    {
        cclasscode = char_classes::CC_ALPHA as i32;
    }

    const CC_ALNUM: i32 = char_classes::CC_ALNUM as i32;
    const CC_ALPHA: i32 = char_classes::CC_ALPHA as i32;
    const CC_ASCII: i32 = char_classes::CC_ASCII as i32;
    const CC_BLANK: i32 = char_classes::CC_BLANK as i32;
    const CC_CNTRL: i32 = char_classes::CC_CNTRL as i32;
    const CC_DIGIT: i32 = char_classes::CC_DIGIT as i32;
    const CC_GRAPH: i32 = char_classes::CC_GRAPH as i32;
    const CC_LOWER: i32 = char_classes::CC_LOWER as i32;
    const CC_PRINT: i32 = char_classes::CC_PRINT as i32;
    const CC_PUNCT: i32 = char_classes::CC_PUNCT as i32;
    const CC_SPACE: i32 = char_classes::CC_SPACE as i32;
    const CC_UPPER: i32 = char_classes::CC_UPPER as i32;
    const CC_XDIGIT: i32 = char_classes::CC_XDIGIT as i32;
    const CC_WORD: i32 = char_classes::CC_WORD as i32;

    let cv: Cvec = match cclasscode {
        CC_PRINT => pg_ctype_get_cache(mcx, pg_wc_isprint, cclasscode)?,
        CC_ALNUM => pg_ctype_get_cache(mcx, pg_wc_isalnum, cclasscode)?,
        CC_ALPHA => pg_ctype_get_cache(mcx, pg_wc_isalpha, cclasscode)?,
        CC_WORD => pg_ctype_get_cache(mcx, pg_wc_isword, cclasscode)?,
        CC_ASCII => {
            let mut cv = getcvec(mcx, None, 0, 1)?;
            addrange(&mut cv, 0, 0x7f);
            cv
        }
        CC_BLANK => {
            let mut cv = getcvec(mcx, None, 2, 0)?;
            addchr(&mut cv, chr_lit('\t'));
            addchr(&mut cv, chr_lit(' '));
            cv
        }
        CC_CNTRL => {
            let mut cv = getcvec(mcx, None, 0, 2)?;
            addrange(&mut cv, 0x0, 0x1f);
            addrange(&mut cv, 0x7f, 0x9f);
            cv
        }
        CC_DIGIT => pg_ctype_get_cache(mcx, pg_wc_isdigit, cclasscode)?,
        CC_PUNCT => pg_ctype_get_cache(mcx, pg_wc_ispunct, cclasscode)?,
        CC_XDIGIT => {
            let mut cv = getcvec(mcx, None, 0, 3)?;
            addrange(&mut cv, chr_lit('0'), chr_lit('9'));
            addrange(&mut cv, chr_lit('a'), chr_lit('f'));
            addrange(&mut cv, chr_lit('A'), chr_lit('F'));
            cv
        }
        CC_SPACE => pg_ctype_get_cache(mcx, pg_wc_isspace, cclasscode)?,
        CC_LOWER => pg_ctype_get_cache(mcx, pg_wc_islower, cclasscode)?,
        CC_UPPER => pg_ctype_get_cache(mcx, pg_wc_isupper, cclasscode)?,
        CC_GRAPH => pg_ctype_get_cache(mcx, pg_wc_isgraph, cclasscode)?,
        _ => {
            return Err(RegError::new(REG_ESPACE));
        }
    };

    Ok(cv)
}

pub fn cclass_column_index(cm: &ColorMap, c: chr) -> i32 {
    let mut colnum: i32 = 0;

    debug_assert!(c > MAX_SIMPLE_CHR);

    let cb = &cm.classbits;
    let idx = |cc: char_classes| cc as usize;

    if cb[idx(char_classes::CC_PRINT)] != 0 && pg_wc_isprint(c) {
        colnum |= cb[idx(char_classes::CC_PRINT)];
    }
    if cb[idx(char_classes::CC_ALNUM)] != 0 && pg_wc_isalnum(c) {
        colnum |= cb[idx(char_classes::CC_ALNUM)];
    }
    if cb[idx(char_classes::CC_ALPHA)] != 0 && pg_wc_isalpha(c) {
        colnum |= cb[idx(char_classes::CC_ALPHA)];
    }
    if cb[idx(char_classes::CC_WORD)] != 0 && pg_wc_isword(c) {
        colnum |= cb[idx(char_classes::CC_WORD)];
    }
    debug_assert_eq!(cb[idx(char_classes::CC_ASCII)], 0);
    debug_assert_eq!(cb[idx(char_classes::CC_BLANK)], 0);
    debug_assert_eq!(cb[idx(char_classes::CC_CNTRL)], 0);
    if cb[idx(char_classes::CC_DIGIT)] != 0 && pg_wc_isdigit(c) {
        colnum |= cb[idx(char_classes::CC_DIGIT)];
    }
    if cb[idx(char_classes::CC_PUNCT)] != 0 && pg_wc_ispunct(c) {
        colnum |= cb[idx(char_classes::CC_PUNCT)];
    }
    debug_assert_eq!(cb[idx(char_classes::CC_XDIGIT)], 0);
    if cb[idx(char_classes::CC_SPACE)] != 0 && pg_wc_isspace(c) {
        colnum |= cb[idx(char_classes::CC_SPACE)];
    }
    if cb[idx(char_classes::CC_LOWER)] != 0 && pg_wc_islower(c) {
        colnum |= cb[idx(char_classes::CC_LOWER)];
    }
    if cb[idx(char_classes::CC_UPPER)] != 0 && pg_wc_isupper(c) {
        colnum |= cb[idx(char_classes::CC_UPPER)];
    }
    if cb[idx(char_classes::CC_GRAPH)] != 0 && pg_wc_isgraph(c) {
        colnum |= cb[idx(char_classes::CC_GRAPH)];
    }

    colnum
}

pub fn allcases<'mcx>(mcx: Mcx<'mcx>, c: chr) -> RegResult<Cvec> {
    let lc = pg_wc_tolower(c);
    let uc = pg_wc_toupper(c);

    let mut cv = getcvec(mcx, None, 2, 0)?;
    addchr(&mut cv, lc);
    if lc != uc {
        addchr(&mut cv, uc);
    }
    Ok(cv)
}

pub fn cmp(x: &[chr], y: &[chr], len: usize) -> i32 {
    if x[..len] == y[..len] {
        0
    } else {
        1
    }
}

pub fn casecmp(x: &[chr], y: &[chr], len: usize) -> i32 {
    for i in 0..len {
        let xc = x[i];
        let yc = y[i];
        if xc != yc && pg_wc_tolower(xc) != pg_wc_tolower(yc) {
            return 1;
        }
    }
    0
}
