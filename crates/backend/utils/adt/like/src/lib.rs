#![allow(non_snake_case)]
//! `LIKE`/`ILIKE` pattern matching for the `name`, `text`, and `bytea` types.
//!
//! Owned-tree port of PostgreSQL 18.3 `src/backend/utils/adt/like.c` together
//! with the matcher template `src/backend/utils/adt/like_match.c`.
//!
//! In C, `like_match.c` is `#include`d four times from `like.c`, each time with
//! different `NextChar`/`CHAREQ`/`GETCHAR` macros, producing the multibyte
//! ([`MB_MatchText`]/[`MB_do_like_escape`]), single-byte
//! ([`SB_MatchText`]/[`SB_do_like_escape`]), single-byte case-insensitive
//! ([`SB_IMatchText`]), and UTF-8 ([`UTF8_MatchText`]) instantiations.  Those
//! four instantiations are reproduced here as thin wrappers over a single
//! generic matcher [`match_text`] parameterized by the same `NextChar`/`GETCHAR`
//! behaviors, so each wrapper's control flow is the C body of that `#include`.
//!
//! # Owned-tree / idiomatic adaptations
//!
//! * C's `pg_locale_t` (a pointer into pg_locale.c's permanent cache) is the
//!   borrowed flag-core [`Locale`] = [`Option<&PgLocaleStruct>`]; C's locale-0
//!   (`NULL`) is [`None`] (treated as deterministic, no case fold).  The active
//!   collation `Oid` is threaded alongside it: the flag-core handle does not
//!   carry the provider-specific `info` union, so the libc single-byte fold
//!   (`tolower_l`) and the nondeterministic-collation comparison (`pg_strncoll`)
//!   re-key by `collation`, which the pg_locale.c owner re-resolves.
//! * The `text`/`bytea` payloads are plain `&[u8]` (the varlena payload C reads
//!   with `VARDATA_ANY`); `do_like_escape` returns an owned `PgVec<'mcx, u8>`
//!   payload (the layered varlena boundary attaches the header) instead of a
//!   `palloc`'d buffer.
//!
//! `like_support.c` (the planner support functions `textlike_support` etc. and
//! the selectivity entry points `likesel`/`prefixsel`/...) is **not** ported
//! here: its entire body operates on planner `Node`/`Const`/`PlannerInfo` and
//! the `supportnodes.h` `SupportRequest*` types, none of which are modeled yet,
//! and reaches the unported selfuncs / index-qual machinery.  Those functions
//! are also dispatched solely through the bare-word `PGFunction` registry, which
//! is deferred.  There is no in-repo caller for them; they will be ported when
//! the planner-support node infrastructure and selfuncs land.

extern crate alloc;

use pg_locale_seams as locale_seam;
use mbutils_seams as mb_seam;
use ::mcx::{Mcx, PgVec};
use ::pgstrcasecmp::{pg_ascii_tolower, pg_tolower};
use ::types_core::{Oid, C_COLLATION_OID};
use ::types_error::{make_sqlstate, PgError, PgResult, SqlState};
use ::locale::{CollProvider, PgLocaleStruct};
use ::types_wchar::encoding::PG_UTF8;

/// `#define LIKE_TRUE 1` -- they match.
pub const LIKE_TRUE: i32 = 1;
/// `#define LIKE_FALSE 0` -- they don't match.
pub const LIKE_FALSE: i32 = 0;
/// `#define LIKE_ABORT -1` -- they don't match and the text is too short, so no
/// suffix can match either.
pub const LIKE_ABORT: i32 = -1;

/// SQLSTATE `22025` -- `ERRCODE_INVALID_ESCAPE_SEQUENCE`.
const ERRCODE_INVALID_ESCAPE_SEQUENCE: SqlState = make_sqlstate(*b"22025");
/// SQLSTATE `42P22` -- `ERRCODE_INDETERMINATE_COLLATION`.
const ERRCODE_INDETERMINATE_COLLATION: SqlState = make_sqlstate(*b"42P22");
/// SQLSTATE `0A000` -- `ERRCODE_FEATURE_NOT_SUPPORTED`.
const ERRCODE_FEATURE_NOT_SUPPORTED: SqlState = make_sqlstate(*b"0A000");

/// The idiomatic `pg_locale_t`: the borrowed flag core, or [`None`] for C's
/// locale-0 (treated as deterministic, no case fold needed).
pub type Locale<'a> = Option<&'a PgLocaleStruct>;

/// `pg_locale_t` is occasionally tested for truth in C as
/// `(locale && !locale->deterministic)`; a `None` locale is deterministic.
#[inline]
fn locale_nondeterministic(locale: Locale<'_>) -> bool {
    match locale {
        Some(l) => !l.deterministic,
        None => false,
    }
}

/// C: `pg_mblen_with_len(p, plen)` -- the byte length of the leading encoded
/// character of `s`, clamped never to extend past the slice end (the mbutils
/// seam's `pg_mblen_range`).
#[inline]
fn pg_mblen_with_len(s: &[u8]) -> usize {
    // C's `pg_mblen` does not validate; the range-clamped seam only Errs when
    // the leading char would overrun the slice, where the clamped length is the
    // slice length (the dead error path falls back to `s.len()`).
    mb_seam::pg_mblen_range::call(s).unwrap_or(s.len() as i32) as usize
}

// ===========================================================================
// Helpers (like.c)
// ===========================================================================

/// C: `wchareq` (static inline, like.c:57) -- whole-multibyte-character equality
/// of the leading character of `p1` and `p2`.  Used only by `MB_do_like_escape`'s
/// `CHAREQ`.
#[inline]
pub fn wchareq(p1: &[u8], p2: &[u8]) -> bool {
    // Optimization: quickly compare the first byte.
    if p1[0] != p2[0] {
        return false;
    }

    let p1clen = pg_mblen_with_len(p1);
    if pg_mblen_with_len(p2) != p1clen {
        return false;
    }

    // They are the same length.
    for i in 0..p1clen {
        if p1[i] != p2[i] {
            return false;
        }
    }
    true
}

/// C: `SB_lower_char` (static, like.c:93) -- single-byte lowercasing honoring the
/// collation `locale` (and the libc/builtin provider behind it).
///
/// `collation` is the OID that produced `locale`; the libc-fold leg passes it to
/// the `char_tolower` (`tolower_l`) seam (the flag-core handle does not carry the
/// provider `info.lt`, so the owner re-resolves it).
pub fn SB_lower_char(c: u8, locale: &PgLocaleStruct, collation: Oid) -> u8 {
    if locale.ctype_is_c {
        pg_ascii_tolower(c)
    } else if locale.is_default {
        pg_tolower(c)
    } else {
        locale_seam::char_tolower::call(c, collation)
    }
}

// ===========================================================================
// Matcher template (like_match.c), instantiated four ways
// ===========================================================================

/// How to advance one "character" in the text, matching the `NextChar` macro for
/// each instantiation.
#[derive(Clone, Copy)]
enum NextCharMode {
    /// SB / SB_I: `NextByte` -- single byte.
    SingleByte,
    /// MB: `do { int __l = pg_mblen_with_len(p, plen); p += __l; plen -= __l; }`.
    MultiByte,
    /// UTF8: `do { p++; plen--; } while (plen > 0 && (*p & 0xC0) == 0x80)`.
    Utf8,
}

/// Number of bytes the text cursor advances for one `NextChar`, given the
/// remaining text slice `s` (`s[0]` is the current byte; `s` is non-empty).
#[inline]
fn next_char_len(mode: NextCharMode, s: &[u8]) -> usize {
    match mode {
        NextCharMode::SingleByte => 1,
        NextCharMode::MultiByte => pg_mblen_with_len(s),
        NextCharMode::Utf8 => {
            // p++; plen--; while (plen > 0 && (*p & 0xC0) == 0x80)
            let mut i = 1usize;
            while i < s.len() && (s[i] & 0xC0) == 0x80 {
                i += 1;
            }
            i
        }
    }
}

/// Whether this instantiation folds case on the fly (C: `#ifdef MATCH_LOWER`),
/// i.e. the `SB_I` path.  When `true`, `GETCHAR(t) == SB_lower_char(t, locale)`.
#[derive(Clone, Copy)]
enum CaseFold {
    /// `#define GETCHAR(t, locale) (t)`.
    None,
    /// `#define GETCHAR(t, locale) MATCH_LOWER(t, locale)` (SB_I).
    SbLower,
}

/// C: `GETCHAR(t, locale)` -- identity, or `SB_lower_char(t, locale)` for the
/// case-folding (SB_I) instantiation.  The `SbLower` arm is only ever reached on
/// the single-byte case-insensitive path, which always has a non-NULL `locale`
/// (mirroring C, where `MATCH_LOWER` dereferences `locale`).
#[inline]
fn getchar(fold: CaseFold, t: u8, locale: Locale<'_>, collation: Oid) -> u8 {
    match fold {
        CaseFold::None => t,
        CaseFold::SbLower => SB_lower_char(
            t,
            locale.expect("SB_IMatchText is only invoked with a non-NULL locale"),
            collation,
        ),
    }
}

/// C: `MatchText` (like_match.c:79) -- the shared matcher body.  The four
/// `#include`s differ only in `next` (the `NextChar` macro) and `fold` (the
/// `GETCHAR`/`MATCH_LOWER` macro); every branch below is the C control flow.
///
/// `t`/`p` are the remaining text/pattern bytes (C's `t`/`tlen`, `p`/`plen`).
/// `collation` is threaded only for the case-fold (SB_I) `char_tolower` seam and
/// the nondeterministic `pg_strncoll` seam.  `mcx` is needed only for the
/// nondeterministic escape-copy buffer.
fn match_text(
    mut t: &[u8],
    mut p: &[u8],
    locale: Locale<'_>,
    collation: Oid,
    next: NextCharMode,
    fold: CaseFold,
    mcx: Mcx<'_>,
) -> PgResult<i32> {
    // Fast path for match-everything pattern.
    if p.len() == 1 && p[0] == b'%' {
        return Ok(LIKE_TRUE);
    }

    // Since this function recurses, it could be driven to stack overflow.
    stack_depth_seams::check_stack_depth::call()?;

    while !t.is_empty() && !p.is_empty() {
        if p[0] == b'\\' {
            // Next pattern byte must match literally, whatever it is.
            p = &p[1..];
            // ... and there had better be one, per SQL standard.
            if p.is_empty() {
                return Err(like_pattern_ends_with_escape());
            }
            if getchar(fold, p[0], locale, collation) != getchar(fold, t[0], locale, collation) {
                return Ok(LIKE_FALSE);
            }
        } else if p[0] == b'%' {
            // % processing is essentially a search for a text position at which
            // the remainder of the text matches the remainder of the pattern,
            // using a recursive call to check each potential match.
            //
            // If there are wildcards immediately following the %, we can skip
            // over them first, using the idea that any sequence of N _'s and one
            // or more %'s is equivalent to N _'s and one %.
            p = &p[1..];

            while !p.is_empty() {
                if p[0] == b'%' {
                    p = &p[1..];
                } else if p[0] == b'_' {
                    // If not enough text left to match the pattern, ABORT.
                    if t.is_empty() {
                        return Ok(LIKE_ABORT);
                    }
                    let l = next_char_len(next, t);
                    t = &t[l..];
                    p = &p[1..];
                } else {
                    break; // Reached a non-wildcard pattern char.
                }
            }

            // If we're at end of pattern, match: we have a trailing % which
            // matches any remaining text string.
            if p.is_empty() {
                return Ok(LIKE_TRUE);
            }

            // Otherwise, scan for a text position at which we can match the rest
            // of the pattern.  The first remaining pattern char is known to be a
            // regular or escaped literal character.  With a nondeterministic
            // collation, we can't rely on the first bytes being equal, so we
            // have to recurse in any case.
            let firstpat: u8 = if p[0] == b'\\' {
                if p.len() < 2 {
                    return Err(like_pattern_ends_with_escape());
                }
                getchar(fold, p[1], locale, collation)
            } else {
                getchar(fold, p[0], locale, collation)
            };

            while !t.is_empty() {
                if getchar(fold, t[0], locale, collation) == firstpat
                    || locale_nondeterministic(locale)
                {
                    let matched = match_text(t, p, locale, collation, next, fold, mcx)?;

                    if matched != LIKE_FALSE {
                        return Ok(matched); // TRUE or ABORT
                    }
                }

                let l = next_char_len(next, t);
                t = &t[l..];
            }

            // End of text with no match, so no point in trying later places to
            // start matching this pattern.
            return Ok(LIKE_ABORT);
        } else if p[0] == b'_' {
            // _ matches any single character, and we know there is one.
            let l = next_char_len(next, t);
            t = &t[l..];
            p = &p[1..];
            continue;
        } else if locale_nondeterministic(locale) {
            // For nondeterministic locales, we find the next substring of the
            // pattern that does not contain wildcards and try to find a matching
            // substring in the text.  Crucially, we cannot do this character by
            // character, but must do it substring by substring, partitioned by
            // the wildcard characters.  (This is per SQL standard.)

            // Determine next substring of pattern without wildcards.  p is the
            // start of the subpattern, p1_idx is one past the last byte. Also
            // track if we found an escape character.
            let mut p1_idx = 0usize;
            let mut found_escape = false;
            while p1_idx < p.len() {
                if p[p1_idx] == b'\\' {
                    found_escape = true;
                    p1_idx += 1;
                    if p1_idx == p.len() {
                        return Err(like_pattern_ends_with_escape());
                    }
                } else if p[p1_idx] == b'_' || p[p1_idx] == b'%' {
                    break;
                }
                p1_idx += 1;
            }

            // If we found an escape character, then make an unescaped copy of
            // the subpattern.
            let buf: PgVec<'_, u8>;
            let subpat: &[u8] = if found_escape {
                let mut b = ::mcx::vec_with_capacity_in(mcx, p1_idx)?;
                let mut j = 0usize;
                while j < p1_idx {
                    if p[j] == b'\\' {
                        j += 1;
                    }
                    b.push(p[j]);
                    j += 1;
                }
                buf = b;
                &buf
            } else {
                &p[..p1_idx]
            };

            // Shortcut: If this is the end of the pattern, then the rest of the
            // text has to match the rest of the pattern.
            if p1_idx == p.len() {
                let cmp = locale_seam::pg_strncoll::call(collation, subpat, t)?;
                if cmp == 0 {
                    return Ok(LIKE_TRUE);
                } else {
                    return Ok(LIKE_FALSE);
                }
            }

            // Now build a substring of the text and try to match it against the
            // subpattern.  t is the start of the text, t1_idx is one past the
            // last byte.  We start with a zero-length string.
            let mut t1_idx = 0usize;
            loop {
                postgres_seams::check_for_interrupts::call()?;

                let cmp = locale_seam::pg_strncoll::call(collation, subpat, &t[..t1_idx])?;

                // If we found a match, we have to test if the rest of pattern
                // can match against the rest of the string.  Otherwise we have
                // to continue here and try matching with a longer substring.
                if cmp == 0 {
                    let matched = match_text(
                        &t[t1_idx..],
                        &p[p1_idx..],
                        locale,
                        collation,
                        next,
                        fold,
                        mcx,
                    )?;
                    if matched == LIKE_TRUE {
                        return Ok(matched);
                    }
                }

                // Didn't match.  If we used up the whole text, then the match
                // fails.  Otherwise, try again with a longer substring.
                if t1_idx == t.len() {
                    return Ok(LIKE_FALSE);
                } else {
                    let l = next_char_len(next, &t[t1_idx..]);
                    t1_idx += l;
                }
            }
        } else if getchar(fold, p[0], locale, collation) != getchar(fold, t[0], locale, collation) {
            // non-wildcard pattern char fails to match text char.
            return Ok(LIKE_FALSE);
        }

        // Pattern and text match, so advance.  It is safe to use NextByte
        // instead of NextChar here, even for multi-byte character sets, because
        // we are not following immediately after a wildcard character.
        t = &t[1..];
        p = &p[1..];
    }

    if !t.is_empty() {
        return Ok(LIKE_FALSE); // end of pattern, but not of text
    }

    // End of text, but perhaps not of pattern.  Match iff the remaining pattern
    // can match a zero-length string, ie, it's zero or more %'s.
    while !p.is_empty() && p[0] == b'%' {
        p = &p[1..];
    }
    if p.is_empty() {
        return Ok(LIKE_TRUE);
    }

    // End of text with no match, so no point in trying later places to start
    // matching this pattern.
    Ok(LIKE_ABORT)
}

/// C: `MB_MatchText` -- `like_match.c` compiled for multibyte characters
/// (`NextChar` via `pg_mblen_with_len`, no case folding).
pub fn MB_MatchText(t: &[u8], p: &[u8], locale: Locale<'_>, mcx: Mcx<'_>) -> PgResult<i32> {
    match_text(t, p, locale, C_COLLATION_OID, NextCharMode::MultiByte, CaseFold::None, mcx)
}

/// C: `SB_MatchText` -- `like_match.c` compiled for single-byte characters
/// (`NextChar == NextByte`, no case folding).
pub fn SB_MatchText(t: &[u8], p: &[u8], locale: Locale<'_>, mcx: Mcx<'_>) -> PgResult<i32> {
    match_text(t, p, locale, C_COLLATION_OID, NextCharMode::SingleByte, CaseFold::None, mcx)
}

/// C: `SB_IMatchText` -- `like_match.c` compiled for single-byte case-insensitive
/// matching (`NextChar == NextByte`, `MATCH_LOWER` via [`SB_lower_char`]).
///
/// `collation` produced `locale`; it is threaded to the `char_tolower` seam.
pub fn SB_IMatchText(
    t: &[u8],
    p: &[u8],
    locale: &PgLocaleStruct,
    collation: Oid,
    mcx: Mcx<'_>,
) -> PgResult<i32> {
    match_text(
        t,
        p,
        Some(locale),
        collation,
        NextCharMode::SingleByte,
        CaseFold::SbLower,
        mcx,
    )
}

/// C: `UTF8_MatchText` -- `like_match.c` compiled for UTF-8 with the fast
/// `NextChar` (no case folding).
pub fn UTF8_MatchText(t: &[u8], p: &[u8], locale: Locale<'_>, mcx: Mcx<'_>) -> PgResult<i32> {
    match_text(t, p, locale, C_COLLATION_OID, NextCharMode::Utf8, CaseFold::None, mcx)
}

// ===========================================================================
// Escape normalization (like_match.c do_like_escape), instantiated SB + MB
// ===========================================================================

/// C: `do_like_escape` (like_match.c:392) -- shared body of `SB_do_like_escape`
/// and `MB_do_like_escape`.  The two differ only in `NextChar`/`CopyAdvChar`
/// (single byte vs. `pg_mblen_with_len`) and the `CHAREQ` used to compare the
/// pattern char to the escape char.  `pat`/`esc` are the input payload bytes.
///
/// Returns the result text payload bytes (without the varlena header), charged
/// to `mcx`.
fn do_like_escape<'mcx>(
    pat: &[u8],
    esc: &[u8],
    next: NextCharMode,
    mcx: Mcx<'mcx>,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut p = pat;
    let mut e = esc;
    let elen = e.len();

    // Worst-case pattern growth is 2x.
    let mut r = ::mcx::vec_with_capacity_in(mcx, pat.len().saturating_mul(2))?;

    if elen == 0 {
        // No escape character is wanted.  Double any backslashes in the pattern
        // to make them act like ordinary characters.
        while !p.is_empty() {
            if p[0] == b'\\' {
                r.push(b'\\');
            }
            // CopyAdvChar(r, p, plen)
            let l = copy_adv_char_len(next, p);
            r.extend_from_slice(&p[..l]);
            p = &p[l..];
        }
    } else {
        // The specified escape must be only a single character.
        let l = next_char_len(next, e);
        e = &e[l..];
        if !e.is_empty() {
            return Err(invalid_escape_string());
        }

        e = esc;

        // If specified escape is '\', just copy the pattern as-is.
        if e[0] == b'\\' {
            r.extend_from_slice(pat);
            return Ok(r);
        }

        // Otherwise, convert occurrences of the specified escape character to
        // '\', and double occurrences of '\' --- unless they immediately follow
        // an escape character!
        let mut afterescape = false;
        while !p.is_empty() {
            if chareq(next, p, e) && !afterescape {
                r.push(b'\\');
                let l = next_char_len(next, p);
                p = &p[l..];
                afterescape = true;
            } else if p[0] == b'\\' {
                r.push(b'\\');
                if !afterescape {
                    r.push(b'\\');
                }
                let l = next_char_len(next, p);
                p = &p[l..];
                afterescape = false;
            } else {
                let l = copy_adv_char_len(next, p);
                r.extend_from_slice(&p[..l]);
                p = &p[l..];
                afterescape = false;
            }
        }
    }

    Ok(r)
}

/// C: `CopyAdvChar(dst, src, srclen)` byte count.  SB copies one byte; MB copies
/// `pg_mblen_with_len(src, srclen)` bytes.
#[inline]
fn copy_adv_char_len(next: NextCharMode, s: &[u8]) -> usize {
    match next {
        NextCharMode::SingleByte => 1,
        _ => pg_mblen_with_len(s),
    }
}

/// C: `CHAREQ(p1, p1len, p2, p2len)` for `do_like_escape`.  SB: `*p1 == *p2`;
/// MB: `wchareq(...)`.
#[inline]
fn chareq(next: NextCharMode, p1: &[u8], p2: &[u8]) -> bool {
    match next {
        NextCharMode::SingleByte => p1[0] == p2[0],
        _ => wchareq(p1, p2),
    }
}

/// C: `SB_do_like_escape` -- escape-normalizing pass for single-byte patterns.
/// Returns the owned result payload, charged to `mcx`.
pub fn SB_do_like_escape<'mcx>(pat: &[u8], esc: &[u8], mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, u8>> {
    do_like_escape(pat, esc, NextCharMode::SingleByte, mcx)
}

/// C: `MB_do_like_escape` -- escape-normalizing pass for multibyte patterns.
pub fn MB_do_like_escape<'mcx>(pat: &[u8], esc: &[u8], mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, u8>> {
    do_like_escape(pat, esc, NextCharMode::MultiByte, mcx)
}

// ===========================================================================
// Generic dispatch (like.c)
// ===========================================================================

/// C: `GenericMatchText` (static, like.c:149) -- choose SB/MB/UTF8 matcher by
/// database encoding and collation, validating the collation first.
pub fn GenericMatchText(s: &[u8], p: &[u8], collation: Oid, mcx: Mcx<'_>) -> PgResult<i32> {
    if collation == 0 {
        // This typically means that the parser could not resolve a conflict of
        // implicit collations, so report it that way.
        return Err(indeterminate_collation("LIKE"));
    }

    let locale = locale_seam::pg_newlocale_from_collation::call(mcx, collation)?;

    if mb_seam::pg_database_encoding_max_length::call() == 1 {
        SB_MatchText(s, p, Some(&locale), mcx)
    } else if mb_seam::get_database_encoding::call() == PG_UTF8 {
        UTF8_MatchText(s, p, Some(&locale), mcx)
    } else {
        MB_MatchText(s, p, Some(&locale), mcx)
    }
}

/// C: `Generic_Text_IC_like` (static, like.c:176) -- case-insensitive (`ILIKE`)
/// entry that lowercases both operands (multibyte/ICU path) or uses
/// [`SB_IMatchText`] (single-byte fold-on-the-fly path).  `str`/`pat` are the
/// input payload bytes.
pub fn Generic_Text_IC_like(str: &[u8], pat: &[u8], collation: Oid, mcx: Mcx<'_>) -> PgResult<i32> {
    if collation == 0 {
        // This typically means that the parser could not resolve a conflict of
        // implicit collations, so report it that way.
        return Err(indeterminate_collation("ILIKE"));
    }

    let locale = locale_seam::pg_newlocale_from_collation::call(mcx, collation)?;

    if !locale.deterministic {
        return Err(PgError::error("nondeterministic collations are not supported for ILIKE")
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    // For efficiency reasons, in the single byte case we don't call lower() on
    // the pattern and text, but instead call SB_lower_char on each character.
    // In the multi-byte case we don't have much choice.  Also, ICU does not
    // support single-character case folding, so we go the long way.
    if mb_seam::pg_database_encoding_max_length::call() > 1 || locale.provider == CollProvider::Icu
    {
        // C: pat = lower(collation, pat); str = lower(collation, str).
        let lpat = formatting::case::str_tolower(mcx, pat, collation)?;
        let lstr = formatting::case::str_tolower(mcx, str, collation)?;
        // C passes locale NULL to the lowered-text match.
        if mb_seam::get_database_encoding::call() == PG_UTF8 {
            UTF8_MatchText(&lstr, &lpat, None, mcx)
        } else {
            MB_MatchText(&lstr, &lpat, None, mcx)
        }
    } else {
        SB_IMatchText(str, pat, &locale, collation, mcx)
    }
}

// ===========================================================================
// SQL functions (like.c)
// ===========================================================================

/// C: `s = NameStr(*str); slen = strlen(s)` -- the in-body NUL-trim of a
/// fixed-size `Name` buffer to its logical bytes.  A caller may pass the full
/// NUL-padded `NAMEDATALEN` buffer, exactly as C receives a `Name`.
#[inline]
fn name_str(name: &[u8]) -> &[u8] {
    let end = name.iter().position(|&b| b == 0).unwrap_or(name.len());
    &name[..end]
}

/// C: `namelike` (like.c:241) -- `name LIKE text`.  `str` is the `Name` buffer
/// bytes (NUL-terminated, possibly NUL-padded); `pat` is the pattern text
/// payload.
pub fn namelike(str: &[u8], pat: &[u8], collation: Oid, mcx: Mcx<'_>) -> PgResult<bool> {
    let s = name_str(str);
    Ok(GenericMatchText(s, pat, collation, mcx)? == LIKE_TRUE)
}

/// C: `namenlike` (like.c:262) -- `name NOT LIKE text`.
pub fn namenlike(str: &[u8], pat: &[u8], collation: Oid, mcx: Mcx<'_>) -> PgResult<bool> {
    let s = name_str(str);
    Ok(GenericMatchText(s, pat, collation, mcx)? != LIKE_TRUE)
}

/// C: `textlike` (like.c:283) -- `text LIKE text`.  `str`/`pat` are the varlena
/// payload bytes (C's `VARDATA_ANY`).
pub fn textlike(str: &[u8], pat: &[u8], collation: Oid, mcx: Mcx<'_>) -> PgResult<bool> {
    Ok(GenericMatchText(str, pat, collation, mcx)? == LIKE_TRUE)
}

/// C: `textnlike` (like.c:304) -- `text NOT LIKE text`.
pub fn textnlike(str: &[u8], pat: &[u8], collation: Oid, mcx: Mcx<'_>) -> PgResult<bool> {
    Ok(GenericMatchText(str, pat, collation, mcx)? != LIKE_TRUE)
}

/// C: `bytealike` (like.c:325) -- `bytea LIKE bytea`.  No collation: matches with
/// a NULL locale.
pub fn bytealike(str: &[u8], pat: &[u8], mcx: Mcx<'_>) -> PgResult<bool> {
    Ok(SB_MatchText(str, pat, None, mcx)? == LIKE_TRUE)
}

/// C: `byteanlike` (like.c:346) -- `bytea NOT LIKE bytea`.
pub fn byteanlike(str: &[u8], pat: &[u8], mcx: Mcx<'_>) -> PgResult<bool> {
    Ok(SB_MatchText(str, pat, None, mcx)? != LIKE_TRUE)
}

/// C: `nameiclike` (like.c:371) -- `name ILIKE text`.
pub fn nameiclike(str: &[u8], pat: &[u8], collation: Oid, mcx: Mcx<'_>) -> PgResult<bool> {
    // C: strtext = name_text(NameGetDatum(str)).
    let strtext = varlena::wire_io::name_text(mcx, str)?;
    Ok(Generic_Text_IC_like(&strtext, pat, collation, mcx)? == LIKE_TRUE)
}

/// C: `nameicnlike` (like.c:386) -- `name NOT ILIKE text`.
pub fn nameicnlike(str: &[u8], pat: &[u8], collation: Oid, mcx: Mcx<'_>) -> PgResult<bool> {
    let strtext = varlena::wire_io::name_text(mcx, str)?;
    Ok(Generic_Text_IC_like(&strtext, pat, collation, mcx)? != LIKE_TRUE)
}

/// C: `texticlike` (like.c:401) -- `text ILIKE text`.
pub fn texticlike(str: &[u8], pat: &[u8], collation: Oid, mcx: Mcx<'_>) -> PgResult<bool> {
    Ok(Generic_Text_IC_like(str, pat, collation, mcx)? == LIKE_TRUE)
}

/// C: `texticnlike` (like.c:413) -- `text NOT ILIKE text`.
pub fn texticnlike(str: &[u8], pat: &[u8], collation: Oid, mcx: Mcx<'_>) -> PgResult<bool> {
    Ok(Generic_Text_IC_like(str, pat, collation, mcx)? != LIKE_TRUE)
}

/// C: `like_escape` (like.c:429) -- `like_escape(text, text)`; normalizes a
/// pattern for a given ESCAPE character to the standard backslash convention.
/// `pat`/`esc` are the varlena payload bytes; the result is the owned payload.
pub fn like_escape<'mcx>(pat: &[u8], esc: &[u8], mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, u8>> {
    if mb_seam::pg_database_encoding_max_length::call() == 1 {
        SB_do_like_escape(pat, esc, mcx)
    } else {
        MB_do_like_escape(pat, esc, mcx)
    }
}

/// C: `like_escape_bytea` (like.c:448) -- `like_escape(bytea, bytea)` variant;
/// always single-byte.
pub fn like_escape_bytea<'mcx>(pat: &[u8], esc: &[u8], mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, u8>> {
    SB_do_like_escape(pat, esc, mcx)
}

// ===========================================================================
// Error constructors (faithful ereport messages / SQLSTATEs)
// ===========================================================================

/// C: `ereport(ERROR, errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE),
/// errmsg("LIKE pattern must not end with escape character"))`.
fn like_pattern_ends_with_escape() -> PgError {
    PgError::error("LIKE pattern must not end with escape character")
        .with_sqlstate(ERRCODE_INVALID_ESCAPE_SEQUENCE)
}

/// C: `ereport(ERROR, errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE),
/// errmsg("invalid escape string"),
/// errhint("Escape string must be empty or one character."))`.
fn invalid_escape_string() -> PgError {
    PgError::error("invalid escape string")
        .with_sqlstate(ERRCODE_INVALID_ESCAPE_SEQUENCE)
        .with_hint("Escape string must be empty or one character.")
}

/// C: `ereport(ERROR, errcode(ERRCODE_INDETERMINATE_COLLATION),
/// errmsg("could not determine which collation to use for {op}"),
/// errhint("Use the COLLATE clause to set the collation explicitly."))`.
fn indeterminate_collation(op: &str) -> PgError {
    PgError::error(alloc::format!(
        "could not determine which collation to use for {op}"
    ))
    .with_sqlstate(ERRCODE_INDETERMINATE_COLLATION)
    .with_hint("Use the COLLATE clause to set the collation explicitly.")
}

/// The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
/// `LIKE`/`ILIKE` operators and `like_escape` normalizers.
pub mod fmgr_builtins;

/// This unit owns no inward seams (its value cores are called by the fmgr
/// dispatcher, which depends on this crate directly).  It does register its
/// `fmgr_builtins[]` rows into the fmgr-core builtin table so by-OID dispatch
/// resolves them.  Called by the `seams-init` aggregator.
pub fn init_seams() {
    fmgr_builtins::register_like_builtins();
}

#[cfg(test)]
mod tests;
