//! Locale-aware and ASCII-only case-folding routines, plus the small shared
//! string helpers (`index_seq_search`, `suff_search`, `is_separator_char`,
//! `get_th`, `str_numth`).
//!
//! Faithful port of formatting.c:1148-2016 (PG 18.3).
//!
//! The locale lookup (`pg_newlocale_from_collation`), the locale-aware case
//! transforms (`pg_strlower`/`pg_strupper`/`pg_strtitle`/`pg_strfold`, all in
//! `pg_locale.c`) go through the `pg_locale_seams` slots, and
//! `GetDatabaseEncoding` (`mbutils.c`) through `mbutils_seams`.
//! The C/POSIX `ctype_is_c` fast path, the `InvalidOid` guard, and the
//! grow-and-retry buffer discipline stay in-crate, byte-for-byte with the C
//! source.

use mcx::{Mcx, PgVec};
use types_error::{PgError, PgResult};
use types_error::{
    ERRCODE_INDETERMINATE_COLLATION, ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_SYNTAX_ERROR,
};
use locale::PgLocale;
use types_core::{InvalidOid, Oid};
use types_wchar::encoding::PG_UTF8;

use crate::tables::{
    keyword_index_filter, KeySuffix, KeyWord, NUM_TH_LOWER, NUM_TH_UPPER, TH_UPPER,
};

/// C: `pg_ascii_tolower` (pg_locale.c) -- map A-Z to a-z, leave others.
#[inline]
pub fn pg_ascii_tolower(ch: u8) -> u8 {
    if ch.is_ascii_uppercase() {
        ch + (b'a' - b'A')
    } else {
        ch
    }
}

/// C: `pg_ascii_toupper` (pg_locale.c) -- map a-z to A-Z, leave others.
#[inline]
pub fn pg_ascii_toupper(ch: u8) -> u8 {
    if ch.is_ascii_lowercase() {
        ch - (b'a' - b'A')
    } else {
        ch
    }
}

// ===========================================================================
// Fast sequential search routines (formatting.c:1148)
// ===========================================================================

/// C: `index_seq_search` (formatting.c:1148).
///
/// `str` is the remaining (NUL-free) input bytes.  Returns the index into `kw`
/// of a matching keyword, or `None`.
pub fn index_seq_search(str: &[u8], kw: &[KeyWord], index: &[i32]) -> Option<usize> {
    let first = *str.first()?;
    if !keyword_index_filter(first) {
        return None;
    }

    let poz = index[(first - b' ') as usize];
    if poz > -1 {
        let mut k = poz as usize;
        loop {
            let name = kw[k].name.as_bytes();
            // C: strncmp(str, k->name, k->len) == 0
            if str.len() >= name.len() && &str[..name.len()] == name {
                return Some(k);
            }
            k += 1;
            // C: if (!k->name) return NULL;  (sentinel terminator)
            if k >= kw.len() {
                return None;
            }
            // C: while (*str == *k->name)
            if first != kw[k].name.as_bytes()[0] {
                break;
            }
        }
    }
    None
}

/// C: `suff_search` (formatting.c:1172).
pub fn suff_search(str: &[u8], suf: &[KeySuffix], typ: i32) -> Option<usize> {
    for (i, s) in suf.iter().enumerate() {
        if s.typ != typ {
            continue;
        }
        let name = s.name.as_bytes();
        if str.len() >= name.len() && &str[..name.len()] == name {
            return Some(i);
        }
    }
    None
}

/// C: `is_separator_char` (formatting.c:1188).
#[inline]
pub fn is_separator_char(c: u8) -> bool {
    // ASCII printable character, but not letter or digit
    c > 0x20
        && c < 0x7F
        && !c.is_ascii_uppercase()
        && !c.is_ascii_lowercase()
        && !c.is_ascii_digit()
}

// ===========================================================================
// Ordinal-suffix helpers (formatting.c:1576)
// ===========================================================================

/// C: `get_th` (formatting.c:1576) -- return ST/ND/RD/TH for a number string.
/// `type` is `TH_UPPER` or `TH_LOWER`.
pub fn get_th(num: &[u8], typ: i32) -> PgResult<&'static str> {
    let len = num.len();
    // C: last = *(num + (len - 1));
    let mut last = if len > 0 { num[len - 1] } else { 0 };
    if !last.is_ascii_digit() {
        return Err(PgError::error(format!(
            "\"{}\" is not a number",
            String::from_utf8_lossy(num)
        ))
        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION));
    }

    // All "teens" (<x>1[0-9]) get 'TH/th'.
    if len > 1 && num[len - 2] == b'1' {
        last = 0;
    }

    let tbl = if typ == TH_UPPER {
        &NUM_TH_UPPER
    } else {
        &NUM_TH_LOWER
    };
    Ok(match last {
        b'1' => tbl[0],
        b'2' => tbl[1],
        b'3' => tbl[2],
        _ => tbl[3],
    })
}

/// C: `str_numth` (formatting.c:1622) -- append the ordinal suffix of `num` to
/// `dest`.  In C, `dest` and `num` may alias; here we always build a fresh
/// string (the C aliasing case `str_numth(s, s, ...)` is just append).
pub fn str_numth(dest: &mut Vec<u8>, num: &[u8], typ: i32) -> PgResult<()> {
    // C: if (dest != num) strcpy(dest, num);  -- we model the common in-place
    // append by requiring the caller to pass `dest` already holding `num`.
    let th = get_th(num, typ)?;
    dest.extend_from_slice(th.as_bytes());
    Ok(())
}

// ===========================================================================
// upper/lower/initcap/casefold (formatting.c:1650)
// ===========================================================================

fn indeterminate_collation(func: &str) -> PgError {
    PgError::error(format!(
        "could not determine which collation to use for {func} function"
    ))
    .with_sqlstate(ERRCODE_INDETERMINATE_COLLATION)
    .with_hint("Use the COLLATE clause to set the collation explicitly.")
}

/// The locale-aware provider transform: one of the `pg_str*` seams. Keyed by
/// collation OID (symmetric with the comparison family); the pg_locale owner
/// re-resolves the locale's `info` union from `collid`.
type ProviderFn = for<'mcx> fn(Mcx<'mcx>, Oid, &[u8]) -> PgResult<PgVec<'mcx, u8>>;

/// Copy an ASCII fast-path `Vec<u8>` result into a `PgVec` allocated in `mcx`,
/// mirroring the palloc'd C result the locale path also produces.
fn asc_into_mcx<'mcx>(mcx: Mcx<'mcx>, v: Vec<u8>) -> PgResult<PgVec<'mcx, u8>> {
    mcx::slice_in(mcx, &v)
}

/// Shared engine for the four locale-aware case routines: validate collation,
/// fetch the locale, take the C/POSIX fast path or call the provider routine.
///
/// The C source uses a grow-and-retry buffer: it sizes a `srclen + 1` buffer,
/// calls the provider, and retries with the exact needed size if it was too
/// small. Our provider seam returns the fully-grown owned `PgVec` directly,
/// so the result already has the final length; the in-crate caller no longer
/// needs the explicit retry, and never truncates a NUL (the seam returns the
/// bytes without a terminator). This is behavior-identical to the C path.
fn case_via_locale<'mcx>(
    mcx: Mcx<'mcx>,
    buff: &[u8],
    collid: Oid,
    func: &str,
    asc: fn(&[u8]) -> Vec<u8>,
    provider: ProviderFn,
) -> PgResult<PgVec<'mcx, u8>> {
    if collid == InvalidOid {
        return Err(indeterminate_collation(func));
    }

    let mylocale = pg_newlocale_from_collation(mcx, collid)?;

    // C: if (mylocale->ctype_is_c) ... asc path.
    if mylocale.ctype_is_c {
        asc_into_mcx(mcx, asc(buff))
    } else {
        provider(mcx, collid, buff)
    }
}

fn pg_strlower<'mcx>(mcx: Mcx<'mcx>, collid: Oid, src: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    pg_locale_seams::pg_strlower::call(mcx, collid, src)
}
fn pg_strupper<'mcx>(mcx: Mcx<'mcx>, collid: Oid, src: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    pg_locale_seams::pg_strupper::call(mcx, collid, src)
}
fn pg_strtitle<'mcx>(mcx: Mcx<'mcx>, collid: Oid, src: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    pg_locale_seams::pg_strtitle::call(mcx, collid, src)
}
fn pg_strfold<'mcx>(mcx: Mcx<'mcx>, collid: Oid, src: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    pg_locale_seams::pg_strfold::call(mcx, collid, src)
}

/// C: `pg_newlocale_from_collation` via the pg_locale seam.
fn pg_newlocale_from_collation<'mcx>(mcx: Mcx<'mcx>, collid: Oid) -> PgResult<PgLocale<'mcx>> {
    pg_locale_seams::pg_newlocale_from_collation::call(mcx, collid)
}

/// C: `GetDatabaseEncoding` via the mbutils seam.
fn get_database_encoding() -> i32 {
    mbutils_seams::get_database_encoding::call()
}

/// C: `str_tolower` (formatting.c:1650).
pub fn str_tolower<'mcx>(mcx: Mcx<'mcx>, buff: &[u8], collid: Oid) -> PgResult<PgVec<'mcx, u8>> {
    case_via_locale(mcx, buff, collid, "lower()", asc_tolower, pg_strlower)
}

/// C: `str_toupper` (formatting.c:1714).
pub fn str_toupper<'mcx>(mcx: Mcx<'mcx>, buff: &[u8], collid: Oid) -> PgResult<PgVec<'mcx, u8>> {
    case_via_locale(mcx, buff, collid, "upper()", asc_toupper, pg_strupper)
}

/// C: `str_initcap` (formatting.c:1778).
pub fn str_initcap<'mcx>(mcx: Mcx<'mcx>, buff: &[u8], collid: Oid) -> PgResult<PgVec<'mcx, u8>> {
    case_via_locale(mcx, buff, collid, "initcap()", asc_initcap, pg_strtitle)
}

/// C: `str_casefold` (formatting.c:1842).
pub fn str_casefold<'mcx>(mcx: Mcx<'mcx>, buff: &[u8], collid: Oid) -> PgResult<PgVec<'mcx, u8>> {
    if collid == InvalidOid {
        return Err(indeterminate_collation("lower()"));
    }

    if get_database_encoding() != PG_UTF8 {
        return Err(PgError::error(
            "Unicode case folding can only be performed if server encoding is UTF8".to_string(),
        )
        .with_sqlstate(ERRCODE_SYNTAX_ERROR));
    }

    let mylocale = pg_newlocale_from_collation(mcx, collid)?;
    if mylocale.ctype_is_c {
        asc_into_mcx(mcx, asc_tolower(buff))
    } else {
        pg_strfold(mcx, collid, buff)
    }
}

// ===========================================================================
// ASCII-only case routines (formatting.c:1911)
// ===========================================================================

/// C: `asc_tolower` (formatting.c:1911).
pub fn asc_tolower(buff: &[u8]) -> Vec<u8> {
    buff.iter().map(|&c| pg_ascii_tolower(c)).collect()
}

/// C: `asc_toupper` (formatting.c:1934).
pub fn asc_toupper(buff: &[u8]) -> Vec<u8> {
    buff.iter().map(|&c| pg_ascii_toupper(c)).collect()
}

/// C: `asc_initcap` (formatting.c:1957).
pub fn asc_initcap(buff: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(buff.len());
    let mut wasalnum = false;
    for &b in buff {
        let c = if wasalnum {
            pg_ascii_tolower(b)
        } else {
            pg_ascii_toupper(b)
        };
        result.push(c);
        // we don't trust isalnum() here
        wasalnum = c.is_ascii_uppercase() || c.is_ascii_lowercase() || c.is_ascii_digit();
    }
    result
}

// ----------
// convenience routines for null-terminated input (formatting.c:1988)
// ----------

/// C: `str_tolower_z` (formatting.c:1988).
pub fn str_tolower_z<'mcx>(mcx: Mcx<'mcx>, buff: &[u8], collid: Oid) -> PgResult<PgVec<'mcx, u8>> {
    str_tolower(mcx, buff, collid)
}
/// C: `str_toupper_z` (formatting.c:1994).
pub fn str_toupper_z<'mcx>(mcx: Mcx<'mcx>, buff: &[u8], collid: Oid) -> PgResult<PgVec<'mcx, u8>> {
    str_toupper(mcx, buff, collid)
}
/// C: `str_initcap_z` (formatting.c:2000).
pub fn str_initcap_z<'mcx>(mcx: Mcx<'mcx>, buff: &[u8], collid: Oid) -> PgResult<PgVec<'mcx, u8>> {
    str_initcap(mcx, buff, collid)
}
/// C: `asc_tolower_z` (formatting.c:2006).
pub fn asc_tolower_z(buff: &[u8]) -> Vec<u8> {
    asc_tolower(buff)
}
/// C: `asc_toupper_z` (formatting.c:2012).
pub fn asc_toupper_z(buff: &[u8]) -> Vec<u8> {
    asc_toupper(buff)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ascii_case_routines_match_c() {
        assert_eq!(asc_tolower(b"Hello WORLD 9"), b"hello world 9");
        assert_eq!(asc_toupper(b"Hello world 9"), b"HELLO WORLD 9");
        assert_eq!(asc_initcap(b"hello world-foo"), b"Hello World-Foo");
        assert_eq!(asc_initcap(b"john's cafe"), b"John'S Cafe");
    }

    #[test]
    fn invalid_collation_errors() {
        let ctx = mcx::MemoryContext::new("test");
        let err = str_tolower(ctx.mcx(), b"abc", InvalidOid).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INDETERMINATE_COLLATION);
        assert!(err.message().contains("lower()"));
    }

    #[test]
    fn get_th_matches_c() {
        use crate::tables::TH_LOWER;
        assert_eq!(get_th(b"1", TH_UPPER).unwrap(), "ST");
        assert_eq!(get_th(b"2", TH_LOWER).unwrap(), "nd");
        assert_eq!(get_th(b"3", TH_UPPER).unwrap(), "RD");
        assert_eq!(get_th(b"11", TH_UPPER).unwrap(), "TH");
        assert_eq!(get_th(b"21", TH_UPPER).unwrap(), "ST");
        assert_eq!(get_th(b"4", TH_UPPER).unwrap(), "TH");
    }

    #[test]
    fn separator_char_classification() {
        assert!(is_separator_char(b'-'));
        assert!(is_separator_char(b'.'));
        assert!(!is_separator_char(b'a'));
        assert!(!is_separator_char(b'5'));
        assert!(!is_separator_char(b' '));
    }
}
