//! like.c + like_match.c. C stamps the matcher four ways via #include; here one
//! generic body is monomorphized per mode. Live: SB, UTF8, SB_I, ILIKE folds
//! (ASCII, libc tolower_l, non-C ctype via casemap). Loud: non-UTF8 multibyte
//! matchers, nondeterministic collations (ICU-only), like_support.c
//! (selfuncs lane).

use mcx::Mcx;
use pg_locale::{PgLocale, COLLPROVIDER_ICU};
use types_core::{Oid, OidIsValid};
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INDETERMINATE_COLLATION,
    ERRCODE_INVALID_ESCAPE_SEQUENCE,
};
use wchar::{pg_enc, PG_UTF8};

pub const LIKE_TRUE: i32 = 1;
pub const LIKE_FALSE: i32 = 0;
pub const LIKE_ABORT: i32 = -1;

// C's locale-0 callers (bytealike, lowered ILIKE): deterministic, never folded.
const LOCALE_NONE: PgLocale = pg_locale::C_LOCALE;

#[track_caller]
#[cold]
#[inline(never)]
fn like_pattern_ends_with_escape() -> Box<PgError> {
    PgError::error("LIKE pattern must not end with escape character")
        .with_sqlstate(ERRCODE_INVALID_ESCAPE_SEQUENCE)
        .into()
}

#[track_caller]
#[cold]
#[inline(never)]
fn invalid_escape_string() -> Box<PgError> {
    PgError::error("invalid escape string")
        .with_sqlstate(ERRCODE_INVALID_ESCAPE_SEQUENCE)
        .with_hint("Escape string must be empty or one character.")
        .into()
}

#[track_caller]
#[cold]
#[inline(never)]
fn indeterminate_collation(op: &str) -> Box<PgError> {
    PgError::error(format!("could not determine which collation to use for {op}"))
        .with_sqlstate(ERRCODE_INDETERMINATE_COLLATION)
        .with_hint("Use the COLLATE clause to set the collation explicitly.")
        .into()
}

#[track_caller]
#[cold]
#[inline(never)]
fn ilike_nondeterministic() -> Box<PgError> {
    PgError::error("nondeterministic collations are not supported for ILIKE")
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
        .into()
}

// unported: like_match.c MB arm for non-UTF8 multibyte database encodings;
// clean feature error (LIKE/ILIKE evaluation, safe unwind).
#[cold]
#[inline(never)]
fn mb_matchtext_unported(encoding: pg_enc) -> Box<::types_error::PgError> {
    Box::new(
        ::types_error::PgError::error(format!(
            "LIKE in a non-UTF8 multibyte database encoding ({encoding}) is not yet implemented"
        ))
        .with_sqlstate(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

#[inline]
fn pg_ascii_tolower(c: u8) -> u8 {
    c.to_ascii_lowercase()
}

#[inline]
fn sb_lower_char(c: u8, locale: &PgLocale) -> u8 {
    if locale.ctype_is_c {
        pg_ascii_tolower(c)
    } else if locale.is_default {
        pg_locale::pg_tolower(c)
    } else {
        locale.tolower_l(c)
    }
}

trait MatchMode {
    fn next_char(t: &[u8]) -> &[u8];
    fn getchar(c: u8, locale: &PgLocale) -> u8;
}

struct SbCs;
struct SbIc;
struct Utf8Cs;

impl MatchMode for SbCs {
    #[inline(always)]
    fn next_char(t: &[u8]) -> &[u8] {
        &t[1..]
    }
    #[inline(always)]
    fn getchar(c: u8, _locale: &PgLocale) -> u8 {
        c
    }
}

impl MatchMode for SbIc {
    #[inline(always)]
    fn next_char(t: &[u8]) -> &[u8] {
        &t[1..]
    }
    #[inline(always)]
    fn getchar(c: u8, locale: &PgLocale) -> u8 {
        sb_lower_char(c, locale)
    }
}

impl MatchMode for Utf8Cs {
    // C's do { p++; plen--; } while (plen > 0 && (*p & 0xC0) == 0x80) shape;
    // the index-then-reslice form cost 1.16x instr on the %-scan lane.
    #[inline(always)]
    fn next_char(mut t: &[u8]) -> &[u8] {
        loop {
            t = &t[1..];
            if t.is_empty() || (t[0] & 0xC0) != 0x80 {
                return t;
            }
        }
    }
    #[inline(always)]
    fn getchar(c: u8, _locale: &PgLocale) -> u8 {
        c
    }
}

fn match_text<M: MatchMode>(mut t: &[u8], mut p: &[u8], locale: &PgLocale) -> PgResult<i32> {
    if p.len() == 1 && p[0] == b'%' {
        return Ok(LIKE_TRUE);
    }

    stack_depth::check_stack_depth()?;

    let nondet = !locale.deterministic;

    // Byte-lockstep except after wildcards, exactly as C: '%'/'_' advance the
    // text by whole characters so recursion re-enters char-synced. The '\\'
    // stanza is only correct for deterministic matching, so it sits after the
    // nondeterministic arm (which handles escapes itself); at the top of the
    // loop we are never positioned immediately after an escape.
    while !t.is_empty() && !p.is_empty() {
        // Not positioned immediately after an escape here, so wildcards may
        // be taken at face value; the deterministic escape branch must come
        // after the nondeterministic arm (C 51652c4).
        if p[0] == b'%' {
            p = &p[1..];

            while !p.is_empty() {
                if p[0] == b'%' {
                    p = &p[1..];
                } else if p[0] == b'_' {
                    if t.is_empty() {
                        return Ok(LIKE_ABORT);
                    }
                    t = M::next_char(t);
                    p = &p[1..];
                } else {
                    break;
                }
            }

            if p.is_empty() {
                return Ok(LIKE_TRUE);
            }

            let firstpat = if p[0] == b'\\' {
                if p.len() < 2 {
                    return Err(like_pattern_ends_with_escape());
                }
                M::getchar(p[1], locale)
            } else {
                M::getchar(p[0], locale)
            };

            while !t.is_empty() {
                if M::getchar(t[0], locale) == firstpat || nondet {
                    let matched = match_text::<M>(t, p, locale)?;
                    if matched != LIKE_FALSE {
                        return Ok(matched);
                    }
                }
                t = M::next_char(t);
            }

            return Ok(LIKE_ABORT);
        } else if p[0] == b'_' {
            t = M::next_char(t);
            p = &p[1..];
            continue;
        } else if nondet {
            // like_match.c nondeterministic arm: match the next wildcard-free
            // subpattern against expanding text substrings via pg_strncoll
            // (per SQL standard, substring by substring — a matching
            // substring may differ in length from the subpattern).
            let mut p1 = p;
            let mut found_escape = false;
            while !p1.is_empty() {
                if p1[0] == b'\\' {
                    found_escape = true;
                    p1 = &p1[1..];
                    if p1.is_empty() {
                        return Err(like_pattern_ends_with_escape());
                    }
                } else if p1[0] == b'_' || p1[0] == b'%' {
                    break;
                }
                p1 = &p1[1..];
            }
            let sublen = p.len() - p1.len();
            let mut buf: Vec<u8>;
            let subpat: &[u8] = if found_escape {
                buf = Vec::with_capacity(sublen);
                let mut c = &p[..sublen];
                while !c.is_empty() {
                    if c[0] == b'\\' {
                        // The p1 scan already rejected a trailing escape.
                        c = &c[1..];
                    }
                    buf.push(c[0]);
                    c = &c[1..];
                }
                &buf
            } else {
                &p[..sublen]
            };

            if p1.is_empty() {
                return Ok(if locale.pg_strncoll(subpat, t) == 0 {
                    LIKE_TRUE
                } else {
                    LIKE_FALSE
                });
            }

            let mut t1 = t;
            loop {
                postgres_seams::check_for_interrupts::call()?;
                if locale.pg_strncoll(subpat, &t[..t.len() - t1.len()]) == 0 {
                    let matched = match_text::<M>(t1, p1, locale)?;
                    if matched == LIKE_TRUE {
                        return Ok(matched);
                    }
                }
                if t1.is_empty() {
                    return Ok(LIKE_FALSE);
                }
                t1 = M::next_char(t1);
            }
        } else if p[0] == b'\\' {
            p = &p[1..];
            if p.is_empty() {
                return Err(like_pattern_ends_with_escape());
            }
            if M::getchar(p[0], locale) != M::getchar(t[0], locale) {
                return Ok(LIKE_FALSE);
            }
        } else if M::getchar(p[0], locale) != M::getchar(t[0], locale) {
            return Ok(LIKE_FALSE);
        }

        t = &t[1..];
        p = &p[1..];
    }

    if !t.is_empty() {
        return Ok(LIKE_FALSE);
    }

    while !p.is_empty() && p[0] == b'%' {
        p = &p[1..];
    }
    if p.is_empty() {
        return Ok(LIKE_TRUE);
    }

    Ok(LIKE_ABORT)
}

pub fn sb_match_text(t: &[u8], p: &[u8], locale: Option<&PgLocale>) -> PgResult<i32> {
    match_text::<SbCs>(t, p, locale.unwrap_or(&LOCALE_NONE))
}

pub fn utf8_match_text(t: &[u8], p: &[u8], locale: Option<&PgLocale>) -> PgResult<i32> {
    match_text::<Utf8Cs>(t, p, locale.unwrap_or(&LOCALE_NONE))
}

pub fn sb_imatch_text(t: &[u8], p: &[u8], locale: &PgLocale) -> PgResult<i32> {
    match_text::<SbIc>(t, p, locale)
}

pub fn generic_match_text(s: &[u8], p: &[u8], collation: Oid) -> PgResult<i32> {
    if !OidIsValid(collation) {
        return Err(indeterminate_collation("LIKE"));
    }

    let locale = pg_locale::pg_newlocale_from_collation(collation)?;

    if mbutils::pg_database_encoding_max_length() == 1 {
        match_text::<SbCs>(s, p, locale)
    } else if mbutils::GetDatabaseEncoding() == PG_UTF8 {
        match_text::<Utf8Cs>(s, p, locale)
    } else {
        return Err(mb_matchtext_unported(mbutils::GetDatabaseEncoding()))
    }
}

/// Retained lowered-operand buffers for the ILIKE multibyte arm; owned by the
/// resolved FmgrInfo (C pallocs two texts per call via lower()).
#[derive(Default)]
pub struct IcScratch {
    s: Vec<u8>,
    p: Vec<u8>,
}

// C's lower() -> str_tolower(ctype_is_c) -> asc_tolower: pnstrdup truncates at
// an embedded NUL (adt_oracle_compat's casemap shape); text can't contain NUL.
fn lower_into(dst: &mut Vec<u8>, src: &[u8]) {
    dst.clear();
    let n = src.iter().position(|&b| b == 0).unwrap_or(src.len());
    dst.extend_from_slice(&src[..n]);
    for b in dst.iter_mut() {
        b.make_ascii_lowercase();
    }
}

pub fn generic_text_ic_like(
    mcx: Mcx<'_>,
    s: &[u8],
    p: &[u8],
    collation: Oid,
    scratch: &mut IcScratch,
) -> PgResult<i32> {
    if !OidIsValid(collation) {
        return Err(indeterminate_collation("ILIKE"));
    }

    let locale = pg_locale::pg_newlocale_from_collation(collation)?;

    if !locale.deterministic {
        return Err(ilike_nondeterministic());
    }

    if mbutils::pg_database_encoding_max_length() > 1 || locale.provider == COLLPROVIDER_ICU {
        if !locale.ctype_is_c {
            // C: DirectFunctionCall1Coll(lower) per operand (formatting.c
            // str_tolower non-C tail).
            let pat = adt_oracle_compat::casemap::str_tolower(mcx, p, collation)?;
            let str_ = adt_oracle_compat::casemap::str_tolower(mcx, s, collation)?;
            return if mbutils::GetDatabaseEncoding() == PG_UTF8 {
                match_text::<Utf8Cs>(&str_, &pat, &LOCALE_NONE)
            } else {
                return Err(mb_matchtext_unported(mbutils::GetDatabaseEncoding()))
            };
        }
        lower_into(&mut scratch.p, p);
        lower_into(&mut scratch.s, s);
        if mbutils::GetDatabaseEncoding() == PG_UTF8 {
            match_text::<Utf8Cs>(&scratch.s, &scratch.p, &LOCALE_NONE)
        } else {
            return Err(mb_matchtext_unported(mbutils::GetDatabaseEncoding()))
        }
    } else {
        match_text::<SbIc>(s, p, locale)
    }
}

// C: s = NameStr(*str); slen = strlen(s).
#[inline]
fn name_str(name: &[u8]) -> &[u8] {
    let end = name.iter().position(|&b| b == 0).unwrap_or(name.len());
    &name[..end]
}

pub fn namelike(name: &[u8], pat: &[u8], collation: Oid) -> PgResult<bool> {
    Ok(generic_match_text(name_str(name), pat, collation)? == LIKE_TRUE)
}

pub fn namenlike(name: &[u8], pat: &[u8], collation: Oid) -> PgResult<bool> {
    Ok(generic_match_text(name_str(name), pat, collation)? != LIKE_TRUE)
}

pub fn textlike(s: &[u8], pat: &[u8], collation: Oid) -> PgResult<bool> {
    Ok(generic_match_text(s, pat, collation)? == LIKE_TRUE)
}

pub fn textnlike(s: &[u8], pat: &[u8], collation: Oid) -> PgResult<bool> {
    Ok(generic_match_text(s, pat, collation)? != LIKE_TRUE)
}

pub fn bytealike(s: &[u8], pat: &[u8]) -> PgResult<bool> {
    Ok(match_text::<SbCs>(s, pat, &LOCALE_NONE)? == LIKE_TRUE)
}

pub fn byteanlike(s: &[u8], pat: &[u8]) -> PgResult<bool> {
    Ok(match_text::<SbCs>(s, pat, &LOCALE_NONE)? != LIKE_TRUE)
}

pub fn nameiclike(
    mcx: Mcx<'_>,
    name: &[u8],
    pat: &[u8],
    collation: Oid,
    scratch: &mut IcScratch,
) -> PgResult<bool> {
    Ok(generic_text_ic_like(mcx, name_str(name), pat, collation, scratch)? == LIKE_TRUE)
}

pub fn nameicnlike(
    mcx: Mcx<'_>,
    name: &[u8],
    pat: &[u8],
    collation: Oid,
    scratch: &mut IcScratch,
) -> PgResult<bool> {
    Ok(generic_text_ic_like(mcx, name_str(name), pat, collation, scratch)? != LIKE_TRUE)
}

pub fn texticlike(
    mcx: Mcx<'_>,
    s: &[u8],
    pat: &[u8],
    collation: Oid,
    scratch: &mut IcScratch,
) -> PgResult<bool> {
    Ok(generic_text_ic_like(mcx, s, pat, collation, scratch)? == LIKE_TRUE)
}

pub fn texticnlike(
    mcx: Mcx<'_>,
    s: &[u8],
    pat: &[u8],
    collation: Oid,
    scratch: &mut IcScratch,
) -> PgResult<bool> {
    Ok(generic_text_ic_like(mcx, s, pat, collation, scratch)? != LIKE_TRUE)
}

trait EscMode {
    fn char_len(s: &[u8]) -> PgResult<usize>;
    fn chareq(p: &[u8], e: &[u8]) -> PgResult<bool>;
}

struct SbEsc;
struct MbEsc;

impl EscMode for SbEsc {
    #[inline]
    fn char_len(_s: &[u8]) -> PgResult<usize> {
        Ok(1)
    }
    #[inline]
    fn chareq(p: &[u8], e: &[u8]) -> PgResult<bool> {
        Ok(p[0] == e[0])
    }
}

impl EscMode for MbEsc {
    #[inline]
    fn char_len(s: &[u8]) -> PgResult<usize> {
        Ok(mbutils::pg_mblen_with_len(s, s.len() as i32)? as usize)
    }
    #[inline]
    fn chareq(p: &[u8], e: &[u8]) -> PgResult<bool> {
        wchareq(p, e)
    }
}

// C: wchareq (like.c) with its first-byte fast test.
fn wchareq(p1: &[u8], p2: &[u8]) -> PgResult<bool> {
    if p1[0] != p2[0] {
        return Ok(false);
    }
    let l1 = mbutils::pg_mblen_with_len(p1, p1.len() as i32)? as usize;
    if mbutils::pg_mblen_with_len(p2, p2.len() as i32)? as usize != l1 {
        return Ok(false);
    }
    Ok(p1[..l1] == p2[..l1])
}

fn do_like_escape<E: EscMode>(pat: &[u8], esc: &[u8], r: &mut Vec<u8>) -> PgResult<()> {
    let mut p = pat;
    // C: palloc(plen * 2 + VARHDRSZ) — worst-case growth is 2x.
    r.reserve(pat.len().saturating_mul(2));

    if esc.is_empty() {
        // No escape character wanted: double backslashes so they act literal.
        while !p.is_empty() {
            if p[0] == b'\\' {
                r.push(b'\\');
            }
            let l = E::char_len(p)?;
            r.extend_from_slice(&p[..l]);
            p = &p[l..];
        }
        return Ok(());
    }

    if E::char_len(esc)? != esc.len() {
        return Err(invalid_escape_string());
    }

    if esc[0] == b'\\' {
        r.extend_from_slice(pat);
        return Ok(());
    }

    // Convert the specified escape to '\' and double '\' — unless they
    // immediately follow an escape character.
    let mut afterescape = false;
    while !p.is_empty() {
        if E::chareq(p, esc)? && !afterescape {
            r.push(b'\\');
            p = &p[E::char_len(p)?..];
            afterescape = true;
        } else if p[0] == b'\\' {
            r.push(b'\\');
            if !afterescape {
                r.push(b'\\');
            }
            p = &p[E::char_len(p)?..];
            afterescape = false;
        } else {
            let l = E::char_len(p)?;
            r.extend_from_slice(&p[..l]);
            p = &p[l..];
            afterescape = false;
        }
    }
    Ok(())
}

pub fn like_escape_into(pat: &[u8], esc: &[u8], out: &mut Vec<u8>) -> PgResult<()> {
    if mbutils::pg_database_encoding_max_length() == 1 {
        do_like_escape::<SbEsc>(pat, esc, out)
    } else {
        do_like_escape::<MbEsc>(pat, esc, out)
    }
}

pub fn like_escape_bytea_into(pat: &[u8], esc: &[u8], out: &mut Vec<u8>) -> PgResult<()> {
    do_like_escape::<SbEsc>(pat, esc, out)
}

pub mod builtins;

#[cfg(test)]
mod tests;
