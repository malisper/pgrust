//! Family: **regex-locale** — `regc_locale.c` + `regc_pg_locale.c`: the
//! locale-ctype boundary of the regex engine.
//!
//! This family OWNS the `pg_wc_is*` probe family, `pg_wc_toupper`/`tolower`,
//! `pg_set_regex_collation`, `pg_ctype_get_cache`, the column-index selector,
//! and the leaf collating-element / equivalence-class / named-class routines.
//! These are the functions `regc_color.c`/`regc_lex.c` reach through to learn
//! character classification.
//!
//! The probes CONSUME `pg_locale_t`, ICU/libc ctype, and the multibyte string
//! helpers, which are owned by `backend-utils-adt-pg-locale` /
//! `backend-utils-mb` (not yet ported). The locale-resolution and the
//! provider-specific ctype/case leaf operations route through that owner's
//! `backend-utils-adt-pg-locale-seams` crate; the database-encoding probe routes
//! through `backend-utils-mb-mbutils-seams`. Everything the regex engine itself
//! owns — the strategy choice, the hard-wired C-locale table, the
//! ASCII-forcing for the default collation, and the cvec/cache plumbing — is
//! implemented here.
//!
//! `pg_set_regex_collation` selects the active locale for the duration of one
//! compile/exec; per AGENTS.md backend-global rules the resolved locale state
//! is per-backend `thread_local!`, not a shared static (the C globals
//! `pg_regex_strategy` / `pg_regex_locale`).

use core::cell::Cell;

use ::mcx::Mcx;
use types_core::{Oid, PgWChar, C_COLLATION_OID, OidIsValid};
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INDETERMINATE_COLLATION};
use ::locale::CollProvider;

use pg_locale_seams as pg_locale_seams;
use mbutils_seams as mb_seams;
use pg_locale_seams::RegexWcClass;

use crate::regex_consts::{NUM_CCLASSES, REG_ECOLLATE, REG_ECTYPE, REG_ERANGE, REG_ESPACE,
    REG_ETOOBIG, REG_FAKE, REG_ULOCALE};
use crate::regex_error::{RegError, RegResult};
use crate::regex_foundation::{addchr, addrange, getcvec};
use crate::regguts::{char_classes, chr, ColorMap, Cvec, CvecRange, MAX_SIMPLE_CHR};

// ---------------------------------------------------------------------------
// regc_pg_locale.c — strategy / collation state (per-backend; C globals)
// ---------------------------------------------------------------------------

/// `PG_Locale_Strategy` (regc_pg_locale.c): the classification strategy chosen
/// for the active collation. Selected by [`pg_set_regex_collation`] and obeyed
/// by every `pg_wc_*` probe below.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PgLocaleStrategy {
    /// `PG_REGEX_STRATEGY_C` — C locale (encoding independent).
    C,
    /// `PG_REGEX_STRATEGY_BUILTIN` — built-in Unicode semantics.
    Builtin,
    /// `PG_REGEX_STRATEGY_LIBC_WIDE` — locale_t `<wctype.h>` functions.
    LibcWide,
    /// `PG_REGEX_STRATEGY_LIBC_1BYTE` — locale_t `<ctype.h>` functions.
    Libc1Byte,
    /// `PG_REGEX_STRATEGY_ICU` — ICU `uchar.h` functions.
    Icu,
}

/// Resolved active-collation state for the current backend.
///
/// Mirrors the C file's `static PG_Locale_Strategy pg_regex_strategy` and
/// `static pg_locale_t pg_regex_locale`. The C `pg_locale_t` is a pointer into
/// pg_locale.c's permanent cache, used both as a cache key (`pg_ctype_get_cache`)
/// and to reach the provider `info` union. Here we keep the engine-relevant
/// flag core inline (the `is_default` flag the LIBC strategies consult) plus the
/// active collation OID, which the leaf ctype/case seams use to reach the
/// owner's permanent locale cache. `collation == InvalidOid` marks the C-locale
/// path (C: `pg_regex_locale == NULL`).
#[derive(Clone, Copy, Debug)]
struct RegexLocaleState {
    strategy: PgLocaleStrategy,
    /// active non-C collation OID (`InvalidOid` for the C-locale path).
    collation: Oid,
    /// `pg_locale->is_default` for the active locale (LIBC ASCII-forcing).
    is_default: bool,
}

thread_local! {
    /// Per-backend active regex collation state. Begins in the C-locale strategy
    /// so the engine is usable before any collation is set (matching the C
    /// zero-initialized `pg_regex_strategy = PG_REGEX_STRATEGY_C`).
    static REGEX_LOCALE_STATE: Cell<RegexLocaleState> = const {
        Cell::new(RegexLocaleState {
            strategy: PgLocaleStrategy::C,
            collation: 0, // InvalidOid
            is_default: false,
        })
    };
}

#[inline]
fn regex_locale_state() -> RegexLocaleState {
    REGEX_LOCALE_STATE.with(|s| s.get())
}

/// `PG_UTF8` (`mb/pg_wchar.h` `pg_enc`): the encoding value the LIBC/BUILTIN
/// strategy selection compares the database encoding against.
const PG_UTF8: i32 = 6;
/// `UCHAR_MAX` (`limits.h`): largest `unsigned char` value, the 1-byte
/// `<ctype.h>` API's reach.
const UCHAR_MAX: chr = 255;

// ---------------------------------------------------------------------------
// Hard-wired character properties for C locale (regc_pg_locale.c)
// ---------------------------------------------------------------------------

const PG_ISDIGIT: u8 = 0x01;
const PG_ISALPHA: u8 = 0x02;
const PG_ISALNUM: u8 = PG_ISDIGIT | PG_ISALPHA;
const PG_ISUPPER: u8 = 0x04;
const PG_ISLOWER: u8 = 0x08;
const PG_ISGRAPH: u8 = 0x10;
const PG_ISPRINT: u8 = 0x20;
const PG_ISPUNCT: u8 = 0x40;
const PG_ISSPACE: u8 = 0x80;

/// `pg_char_properties[128]` (regc_pg_locale.c): the hard-wired ASCII ctype
/// table consulted by the C-locale strategy. Built from the same per-char
/// property bits as the C array; the table is the C array transcribed by class.
const PG_CHAR_PROPERTIES: [u8; 128] = build_char_properties();

const fn build_char_properties() -> [u8; 128] {
    let mut t = [0u8; 128];
    // control whitespace: ^I..^M (0x09..0x0d)
    let mut c = 0x09;
    while c <= 0x0d {
        t[c] = PG_ISSPACE;
        c += 1;
    }
    // space (0x20): print + space
    t[0x20] = PG_ISPRINT | PG_ISSPACE;
    // punctuation/graph ranges and digits/letters
    let graph = PG_ISGRAPH | PG_ISPRINT | PG_ISPUNCT;
    // 0x21..0x2f  ! " # $ % & ' ( ) * + , - . /
    let mut c = 0x21;
    while c <= 0x2f {
        t[c] = graph;
        c += 1;
    }
    // 0x30..0x39  digits 0-9
    let mut c = 0x30;
    while c <= 0x39 {
        t[c] = PG_ISDIGIT | PG_ISGRAPH | PG_ISPRINT;
        c += 1;
    }
    // 0x3a..0x40  : ; < = > ? @
    let mut c = 0x3a;
    while c <= 0x40 {
        t[c] = graph;
        c += 1;
    }
    // 0x41..0x5a  A-Z
    let mut c = 0x41;
    while c <= 0x5a {
        t[c] = PG_ISALPHA | PG_ISUPPER | PG_ISGRAPH | PG_ISPRINT;
        c += 1;
    }
    // 0x5b..0x60  [ \ ] ^ _ `
    let mut c = 0x5b;
    while c <= 0x60 {
        t[c] = graph;
        c += 1;
    }
    // 0x61..0x7a  a-z
    let mut c = 0x61;
    while c <= 0x7a {
        t[c] = PG_ISALPHA | PG_ISLOWER | PG_ISGRAPH | PG_ISPRINT;
        c += 1;
    }
    // 0x7b..0x7e  { | } ~
    let mut c = 0x7b;
    while c <= 0x7e {
        t[c] = graph;
        c += 1;
    }
    // 0x7f (DEL) stays 0
    t
}

/// `pg_ascii_toupper((unsigned char) c)` (`common/string.h`): upper-case a byte
/// using ASCII rules only.
#[inline]
fn pg_ascii_toupper(ch: u8) -> u8 {
    if ch.is_ascii_lowercase() {
        ch - b'a' + b'A'
    } else {
        ch
    }
}

/// `pg_ascii_tolower((unsigned char) c)` (`common/string.h`): lower-case a byte
/// using ASCII rules only.
#[inline]
fn pg_ascii_tolower(ch: u8) -> u8 {
    if ch.is_ascii_uppercase() {
        ch - b'A' + b'a'
    } else {
        ch
    }
}

// ---------------------------------------------------------------------------
// regc_pg_locale.c — collation selection + the pg_wc_* probe family
// ---------------------------------------------------------------------------

/// `pg_set_regex_collation(Oid collation)` — set the collation these probes
/// obey for the current compile/exec. Resolves the `pg_locale_t` from the
/// (unported) pg-locale owner; can ereport on a bad collation.
///
/// The C function takes only `(Oid)` and reads `GetDatabaseEncoding()` /
/// `pg_newlocale_from_collation()` from ambient state; the idiomatic port
/// threads `Mcx` because resolving the locale through the owner seam allocates
/// (C returns a pointer into a permanent cache). The resolved strategy + flags
/// are stored in the per-backend [`REGEX_LOCALE_STATE`].
pub fn pg_set_regex_collation<'mcx>(mcx: Mcx<'mcx>, collation: Oid) -> PgResult<()> {
    let strategy;
    let mut is_default = false;
    let mut active_collation = collation;

    if !OidIsValid(collation) {
        // This typically means that the parser could not resolve a conflict
        // of implicit collations, so report it that way.
        return Err(PgError::error(
            "could not determine which collation to use for regular expression",
        )
        .with_sqlstate(ERRCODE_INDETERMINATE_COLLATION)
        .with_hint("Use the COLLATE clause to set the collation explicitly."));
    }

    if collation == C_COLLATION_OID {
        // Some callers expect regexes to work for C_COLLATION_OID before
        // catalog access is available, so we can't call
        // pg_newlocale_from_collation().
        strategy = PgLocaleStrategy::C;
        active_collation = 0; // locale = 0 (C: pg_regex_locale = NULL)
    } else {
        let locale = pg_locale_seams::pg_newlocale_from_collation::call(mcx, collation)?;

        if !locale.deterministic {
            return Err(PgError::error(
                "nondeterministic collations are not supported for regular expressions",
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
        }

        if locale.ctype_is_c {
            // C/POSIX collations use this path regardless of database encoding.
            strategy = PgLocaleStrategy::C;
            active_collation = 0; // locale = 0
        } else if locale.provider == CollProvider::Builtin {
            debug_assert_eq!(mb_seams::get_database_encoding::call(), PG_UTF8);
            strategy = PgLocaleStrategy::Builtin;
            is_default = locale.is_default;
        } else if locale.provider == CollProvider::Icu {
            // USE_ICU: ICU support is disabled in this build profile, but the
            // strategy selection still names it for fidelity with the C switch.
            strategy = PgLocaleStrategy::Icu;
            is_default = locale.is_default;
        } else {
            debug_assert_eq!(locale.provider, CollProvider::Libc);
            if mb_seams::get_database_encoding::call() == PG_UTF8 {
                strategy = PgLocaleStrategy::LibcWide;
            } else {
                strategy = PgLocaleStrategy::Libc1Byte;
            }
            is_default = locale.is_default;
        }
    }

    REGEX_LOCALE_STATE.with(|s| {
        s.set(RegexLocaleState {
            strategy,
            collation: active_collation,
            is_default,
        })
    });
    Ok(())
}

/// Shared body for the `pg_wc_is*` probes that follow the standard
/// strategy-switch: the C-locale hard-wired table for the C strategy, and the
/// owner ctype seam (keyed by class + active collation) for the others.
#[inline]
fn pg_wc_isclass(c: PgWChar, c_bit: u8, class: RegexWcClass) -> bool {
    let st = regex_locale_state();
    match st.strategy {
        PgLocaleStrategy::C => c <= 127 && (PG_CHAR_PROPERTIES[c as usize] & c_bit) != 0,
        PgLocaleStrategy::Builtin
        | PgLocaleStrategy::LibcWide
        | PgLocaleStrategy::Libc1Byte
        | PgLocaleStrategy::Icu => {
            // The BUILTIN/LIBC_WIDE/LIBC_1BYTE/ICU code paths in C all reach the
            // provider `info` union (builtin Unicode tables, libc locale_t
            // iswX_l/isX_l, ICU uX) owned by pg_locale.c; route them through the
            // owner ctype seam, keyed by the active collation.
            pg_locale_seams::regex_wc_isclass::call(st.collation, class, c)
        }
    }
}

/// `pg_wc_isdigit(pg_wchar c)`.
pub fn pg_wc_isdigit(c: PgWChar) -> bool {
    pg_wc_isclass(c, PG_ISDIGIT, RegexWcClass::Digit)
}

/// `pg_wc_isalpha(pg_wchar c)`.
pub fn pg_wc_isalpha(c: PgWChar) -> bool {
    pg_wc_isclass(c, PG_ISALPHA, RegexWcClass::Alpha)
}

/// `pg_wc_isalnum(pg_wchar c)`.
pub fn pg_wc_isalnum(c: PgWChar) -> bool {
    pg_wc_isclass(c, PG_ISALNUM, RegexWcClass::Alnum)
}

/// `pg_wc_isword(pg_wchar c)`.
pub fn pg_wc_isword(c: PgWChar) -> bool {
    // We define word characters as alnum class plus underscore.
    if c == chr_lit('_') {
        return true;
    }
    pg_wc_isalnum(c)
}

/// `pg_wc_isupper(pg_wchar c)`.
pub fn pg_wc_isupper(c: PgWChar) -> bool {
    pg_wc_isclass(c, PG_ISUPPER, RegexWcClass::Upper)
}

/// `pg_wc_islower(pg_wchar c)`.
pub fn pg_wc_islower(c: PgWChar) -> bool {
    pg_wc_isclass(c, PG_ISLOWER, RegexWcClass::Lower)
}

/// `pg_wc_isgraph(pg_wchar c)`.
pub fn pg_wc_isgraph(c: PgWChar) -> bool {
    pg_wc_isclass(c, PG_ISGRAPH, RegexWcClass::Graph)
}

/// `pg_wc_isprint(pg_wchar c)`.
pub fn pg_wc_isprint(c: PgWChar) -> bool {
    pg_wc_isclass(c, PG_ISPRINT, RegexWcClass::Print)
}

/// `pg_wc_ispunct(pg_wchar c)`.
pub fn pg_wc_ispunct(c: PgWChar) -> bool {
    pg_wc_isclass(c, PG_ISPUNCT, RegexWcClass::Punct)
}

/// `pg_wc_isspace(pg_wchar c)`.
pub fn pg_wc_isspace(c: PgWChar) -> bool {
    pg_wc_isclass(c, PG_ISSPACE, RegexWcClass::Space)
}

/// `pg_wc_toupper(pg_wchar c)`.
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
        PgLocaleStrategy::LibcWide | PgLocaleStrategy::Libc1Byte => {
            // force C behavior for ASCII characters, per comments above
            if st.is_default && c <= 127 {
                pg_ascii_toupper(c as u8) as PgWChar
            } else {
                pg_locale_seams::regex_wc_toupper::call(st.collation, c)
            }
        }
        PgLocaleStrategy::Builtin | PgLocaleStrategy::Icu => {
            pg_locale_seams::regex_wc_toupper::call(st.collation, c)
        }
    }
}

/// `pg_wc_tolower(pg_wchar c)`.
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
        PgLocaleStrategy::LibcWide | PgLocaleStrategy::Libc1Byte => {
            // force C behavior for ASCII characters, per comments above
            if st.is_default && c <= 127 {
                pg_ascii_tolower(c as u8) as PgWChar
            } else {
                pg_locale_seams::regex_wc_tolower::call(st.collation, c)
            }
        }
        PgLocaleStrategy::Builtin | PgLocaleStrategy::Icu => {
            pg_locale_seams::regex_wc_tolower::call(st.collation, c)
        }
    }
}

/// A `pg_wc_probefunc`: one of the `pg_wc_is*` probes, used to populate a
/// ctype cache (`pg_ctype_get_cache`).
pub type PgWcProbeFunc = fn(PgWChar) -> bool;

/// `pg_ctype_get_cache(pg_wc_probefunc probefunc, int cclasscode)` — build (or
/// return the cached) cvec of all simple chrs matching a probe. Allocates the
/// cvec, hence `Mcx`/`RegResult` (NULL on OOM in C -> `REG_ESPACE`).
///
/// The C version caches the result in a process-permanent linked list keyed by
/// `(probefunc, pg_regex_locale)`. The idiomatic cvec stores chrs/ranges in
/// growable `Vec`s, so the C realloc/trim/store_match machinery becomes plain
/// `Vec::push`; the per-locale cache is the owner's concern (`pg_ctype_get_cache`
/// is documented "must not be freed by caller"), so here we rebuild the cvec on
/// each call from the same scan. The `cclasscode` is cleared to -1 when the scan
/// is not bounded by `MAX_SIMPLE_CHR` (no run-time locale checks needed).
pub fn pg_ctype_get_cache<'mcx>(
    mcx: Mcx<'mcx>,
    probefunc: PgWcProbeFunc,
    cclasscode: i32,
) -> RegResult<Cvec> {
    // getcvec(v, 0, 0) seed; we grow as we scan. Match C's initial split of the
    // class code: -1 when the scan limit makes run-time locale checks moot.
    let st = regex_locale_state();
    let mut effective_cclasscode = cclasscode;

    // Decide how many character codes we ought to look through. In general we
    // don't go past MAX_SIMPLE_CHR; in C locale there's no need to go past 127,
    // and with a 1-byte API no need past UCHAR_MAX. (The production MAX_SIMPLE_CHR
    // (0x7FF) is >= 127 and >= UCHAR_MAX, so those bounds always apply and the
    // cclasscode is cleared in both, matching the C `#if` arms.)
    let max_chr: chr = match st.strategy {
        PgLocaleStrategy::C => {
            // #if MAX_SIMPLE_CHR >= 127
            effective_cclasscode = -1;
            127
        }
        PgLocaleStrategy::Builtin => MAX_SIMPLE_CHR,
        PgLocaleStrategy::LibcWide => MAX_SIMPLE_CHR,
        PgLocaleStrategy::Libc1Byte => {
            // #if MAX_SIMPLE_CHR >= UCHAR_MAX
            effective_cclasscode = -1;
            UCHAR_MAX
        }
        PgLocaleStrategy::Icu => MAX_SIMPLE_CHR,
    };

    // C: pcc->cv.chrspace = 128; pcc->cv.rangespace = 64 (regc_pg_locale.c:719-723).
    // store_match grows these on overflow (its own realloc-doubling, NOT the
    // fixed-space addchr/addrange), so seed the initial reservation here.
    let mut cv = getcvec(mcx, None, 128, 64)?;
    cv.cclasscode = effective_cclasscode;

    // And scan 'em ... accumulating consecutive matches into ranges/singletons,
    // exactly as C's store_match does (nchrs > 1 -> range, == 1 -> chr).
    let mut nmatches: u32 = 0; // number of consecutive matches
    let mut cur_chr: chr = 0;
    while cur_chr <= max_chr {
        if probefunc(cur_chr) {
            nmatches += 1;
        } else if nmatches > 0 {
            store_match(&mut cv, cur_chr - nmatches, nmatches);
            nmatches = 0;
        }
        // Guard against overflow at the top of the chr range (max_chr <
        // CHR_MAX always holds for these bounds, so the increment is safe).
        cur_chr += 1;
    }

    if nmatches > 0 {
        store_match(&mut cv, cur_chr - nmatches, nmatches);
    }

    Ok(cv)
}

/// `store_match(pcc, chr1, nchrs)` (regc_pg_locale.c): append a run of `nchrs`
/// consecutive matching chrs starting at `chr1` to the cvec — as a range when
/// `nchrs > 1`, else as a single chr.
///
/// C's `store_match` does NOT use the fixed-space `addrange`/`addchr` (which
/// assert `n < space`); it owns a realloc-doubling grow loop
/// (regc_pg_locale.c:654-674: `if (nranges >= rangespace) { rangespace *= 2;
/// realloc(...) }`). Mirror that here by pushing onto the growable `Vec`
/// directly — `Vec::push` reallocates (doubling) on overflow, exactly the C
/// behavior — rather than routing through the capacity-asserting helpers.
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

// ---------------------------------------------------------------------------
// regc_locale.c — ASCII character-name + character-class tables
// ---------------------------------------------------------------------------

/// `struct cname` (regc_locale.c): an ASCII collating-element name and its code.
struct Cname {
    name: &'static str,
    code: u8,
}

/// `cnames[]` (regc_locale.c): the ASCII character-name table, used by
/// [`element`] to resolve `[.name.]` collating-element references.
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

/// `classNames[NUM_CCLASSES + 1]` (regc_locale.c): valid character-class names,
/// in the exact order of `enum char_classes` (regguts.h).
const CLASS_NAMES: [&str; NUM_CCLASSES as usize] = [
    "alnum", "alpha", "ascii", "blank", "cntrl", "digit", "graph", "lower",
    "print", "punct", "space", "upper", "xdigit", "word",
];

/// `CHR(c)` (regcustom.h): widen an ASCII byte literal to a `chr`.
#[inline]
const fn chr_lit(c: char) -> chr {
    c as chr
}

/// `pg_char_and_wchar_strncmp(name, startp, len) == 0` test, specialized to the
/// regex tables: do the `len` `chr`s in `startp` equal the bytes of the ASCII
/// `name` (each promoted as an unsigned char)? Matches the C idiom
/// `strlen(name) == len && pg_char_and_wchar_strncmp(name, startp, len) == 0`.
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

// ---------------------------------------------------------------------------
// regc_locale.c — collating elements, ranges, classes, casing
// ---------------------------------------------------------------------------

/// `before(chr x, chr y)` — is chr x before chr y, for purposes of range
/// legality?
#[inline]
fn before(x: chr, y: chr) -> bool {
    x < y
}

/// `element(struct vars *v, const chr *startp, const chr *endp)` — look up a
/// [.collating element.] by name. Returns the chr, and (via `note`) records
/// `REG_ULOCALE` when a multi-char name is used; the caller threads the note.
///
/// In C the `NOTE(REG_ULOCALE)` mutates `v->re->re_info`; the engine's `struct
/// vars` is owned by the [`crate::regex_compile`] family, so the locale note is
/// returned alongside the chr for the caller to OR in.
pub fn element(startp: &[chr]) -> RegResult<ElementResult> {
    // generic: one-chr names stand for themselves
    debug_assert!(!startp.is_empty());
    let len = startp.len();
    if len == 1 {
        return Ok(ElementResult { code: startp[0], note_ulocale: false });
    }

    // search table
    for cn in CNAMES {
        if name_eq_chrs(cn.name, startp) {
            return Ok(ElementResult {
                code: chr_lit(cn.code as char),
                note_ulocale: true, // NOTE(REG_ULOCALE)
            });
        }
    }

    // couldn't find it
    Err(RegError::new(REG_ECOLLATE))
}

/// Result of [`element`]: the resolved chr plus whether C would have recorded
/// `NOTE(REG_ULOCALE)` (a multi-char collating-element name was used).
#[derive(Clone, Copy, Debug)]
pub struct ElementResult {
    /// the resolved collating-element chr (`CHR(cn->code)`).
    pub code: chr,
    /// C `NOTE(REG_ULOCALE)`: a named (multi-char) collating element was used.
    pub note_ulocale: bool,
}

/// The `REG_ULOCALE` info bit, re-exported so callers threading [`element`]'s
/// note do not need to reach into [`crate::regex_consts`] directly.
pub const ELEMENT_NOTE_ULOCALE: i32 = REG_ULOCALE;

/// `range(struct vars *v, chr a, chr b, int cases)` — build the cvec for a
/// character range `a-b`, optionally case-expanded.
pub fn range<'mcx>(mcx: Mcx<'mcx>, a: chr, b: chr, cases: i32) -> RegResult<Cvec> {
    if a != b && !before(a, b) {
        return Err(RegError::new(REG_ERANGE));
    }

    if cases == 0 {
        // easy version
        let mut cv = getcvec(mcx, None, 0, 1)?;
        addrange(&mut cv, a, b);
        return Ok(cv);
    }

    // When case-independent, it's hard to decide when cvec ranges are usable,
    // so for now at least, we won't try. We use a range for the originally
    // specified chrs and then add on any case-equivalents that are outside that
    // range as individual chrs.
    //
    // To ensure sane behavior if someone specifies a very large range, limit the
    // allocation size to 100000 chrs (arbitrary) and check for overrun inside
    // the loop below.
    let nchrs_i: i64 = b as i64 - a as i64 + 1;
    let nchrs: i32 = if nchrs_i <= 0 || nchrs_i > 100000 {
        100000
    } else {
        nchrs_i as i32
    };

    let mut cv = getcvec(mcx, None, nchrs, 1)?;
    addrange(&mut cv, a, b);

    // The C loop ranges c from a..=b; the chrspace check (cv->nchrs >=
    // cv->chrspace) reports REG_ETOOBIG when the case-equivalents would exceed
    // the reserved nchrs slots. The idiomatic cvec tracks that capacity as the
    // reserved `nchrs` count from getcvec.
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
        // INTERRUPT(v->re): cancellation check is owned by the compile family's
        // `struct vars`; nothing to do at this leaf.
        if c == b {
            break;
        }
        c += 1;
    }

    Ok(cv)
}

/// `eclass(struct vars *v, chr c, int cases)` — build the cvec for the
/// equivalence class `[=c=]`. `cflags` carries the compile flags (for the
/// `REG_FAKE` test); it is owned by the compile family's `struct vars`.
pub fn eclass<'mcx>(mcx: Mcx<'mcx>, cflags: i32, c: chr, cases: i32) -> RegResult<Cvec> {
    // crude fake equivalence class for testing
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

    // otherwise, none
    if cases != 0 {
        return allcases(mcx, c);
    }
    let mut cv = getcvec(mcx, None, 1, 0)?;
    addchr(&mut cv, c);
    Ok(cv)
}

/// `lookupcclass(struct vars *v, const chr *startp, const chr *endp)` — map a
/// `[:name:]` to its `enum char_classes` code. On failure, returns
/// `REG_ECTYPE`.
pub fn lookupcclass(startp: &[chr]) -> RegResult<i32> {
    // Map the name to the corresponding enumerated value.
    for (i, name) in CLASS_NAMES.iter().enumerate() {
        if name_eq_chrs(name, startp) {
            return Ok(i as i32);
        }
    }

    Err(RegError::new(REG_ECTYPE))
}

/// `cclasscvec(struct vars *v, int cclasscode, int cases)` — build the cvec
/// for a named character class.
///
/// The returned cvec might be either a transient cvec gotten from getcvec(), or
/// a permanently cached one from pg_ctype_get_cache(); callers must not free it
/// either way. Returns `REG_ESPACE` on out-of-memory (C: cv == NULL).
pub fn cclasscvec<'mcx>(mcx: Mcx<'mcx>, cclasscode: i32, cases: i32) -> RegResult<Cvec> {
    // Remap lower and upper to alpha if the match is case insensitive.
    let mut cclasscode = cclasscode;
    if cases != 0
        && (cclasscode == char_classes::CC_LOWER as i32
            || cclasscode == char_classes::CC_UPPER as i32)
    {
        cclasscode = char_classes::CC_ALPHA as i32;
    }

    // Now compute the character class contents. For classes that are based on
    // the behavior of a <wctype.h> or <ctype.h> function, we use
    // pg_ctype_get_cache so that we can cache the results. Other classes have
    // definitions that are hard-wired here.
    //
    // NB: keep this code in sync with cclass_column_index(), below.
    // The C switch matches on `enum char_classes`; we match on the same integer
    // codes (regguts.h declaration order).
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
            // hard-wired meaning
            let mut cv = getcvec(mcx, None, 0, 1)?;
            addrange(&mut cv, 0, 0x7f);
            cv
        }
        CC_BLANK => {
            // hard-wired meaning
            let mut cv = getcvec(mcx, None, 2, 0)?;
            addchr(&mut cv, chr_lit('\t'));
            addchr(&mut cv, chr_lit(' '));
            cv
        }
        CC_CNTRL => {
            // hard-wired meaning
            let mut cv = getcvec(mcx, None, 0, 2)?;
            addrange(&mut cv, 0x0, 0x1f);
            addrange(&mut cv, 0x7f, 0x9f);
            cv
        }
        CC_DIGIT => pg_ctype_get_cache(mcx, pg_wc_isdigit, cclasscode)?,
        CC_PUNCT => pg_ctype_get_cache(mcx, pg_wc_ispunct, cclasscode)?,
        CC_XDIGIT => {
            // It's not clear how to define this in non-western locales, and even
            // less clear that there's any particular use in trying. So just
            // hard-wire the meaning.
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
            // C's switch has no default and leaves cv == NULL, which then maps
            // to REG_ESPACE below ("If cv is NULL now, the reason must be out
            // of memory"). An unknown class code cannot occur for a code from
            // lookupcclass(), so this mirrors the C fall-through.
            return Err(RegError::new(REG_ESPACE));
        }
    };

    Ok(cv)
}

/// `cclass_column_index(struct colormap *cm, chr c)` — the locale-dependent
/// high-colormap column selector: ORs together the classbits of every cclass
/// `c` belongs to. Dispatches to the `pg_wc_is*` probes above.
pub fn cclass_column_index(cm: &ColorMap, c: chr) -> i32 {
    let mut colnum: i32 = 0;

    // Shouldn't go through all these pushups for simple chrs
    debug_assert!(c > MAX_SIMPLE_CHR);

    let cb = &cm.classbits;
    let idx = |cc: char_classes| cc as usize;

    // Note: we should not see requests to consider cclasses that are not treated
    // as locale-specific by cclasscvec(), above.
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

/// `allcases(struct vars *v, chr c)` — build the cvec of all case variants of
/// `c`.
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

/// `cmp(const chr *x, const chr *y, size_t len)` — the case-sensitive chr-string
/// comparator (the default `g->compare`). 0 for equal, nonzero for unequal.
/// Compares exactly `len` chrs and does not stop at embedded NULs.
pub fn cmp(x: &[chr], y: &[chr], len: usize) -> i32 {
    // memcmp(VS(x), VS(y), len * sizeof(chr)): compares byte-for-byte over len
    // chrs. For equal/unequal the chr-wise comparison is equivalent.
    if x[..len] == y[..len] {
        0
    } else {
        1
    }
}

/// `casecmp(const chr *x, const chr *y, size_t len)` — the case-insensitive
/// chr-string comparator (the `REG_ICASE` `g->compare`). 0 for equal, nonzero
/// for unequal; compares exactly `len` chrs without stopping at embedded NULs.
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
