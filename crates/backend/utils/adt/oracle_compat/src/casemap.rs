//! str_tolower/str_toupper/str_initcap/str_casefold case kernels, decomposed
//! out of formatting.c for their oracle_compat.c consumers (lower/upper/
//! initcap/casefold). ctype_is_c collations take the asc_* fast path; the
//! non-C arms dispatch through pg_locale's pg_strlower/strtitle/strupper/
//! strfold (builtin + libc providers; ICU loud there).

use mcx::{Mcx, PgVec};
use pg_locale::PgLocale;
use types_core::{Oid, OidIsValid};
use types_error::{PgError, PgResult, ERRCODE_INDETERMINATE_COLLATION, ERRCODE_SYNTAX_ERROR};

#[cold]
#[inline(never)]
fn indeterminate_collation_err(fname: &str) -> PgError {
    PgError::error(format!(
        "could not determine which collation to use for {fname} function"
    ))
    .with_sqlstate(ERRCODE_INDETERMINATE_COLLATION)
    .with_hint("Use the COLLATE clause to set the collation explicitly.")
}

#[cold]
#[inline(never)]
fn casefold_encoding_err() -> PgError {
    PgError::error("Unicode case folding can only be performed if server encoding is UTF8")
        .with_sqlstate(ERRCODE_SYNTAX_ERROR)
}

fn locale_for(collid: Oid, fname: &str) -> PgResult<&'static PgLocale> {
    if !OidIsValid(collid) {
        return Err(indeterminate_collation_err(fname).into());
    }
    pg_locale::pg_newlocale_from_collation(collid)
}

// formatting.c's non-ctype_is_c tail: equal-size buffer first, one grow-and-
// retry using the returned length; result is the cstring prefix (first NUL).
fn pg_case<'mcx>(
    mcx: Mcx<'mcx>,
    buff: &[u8],
    locale: &PgLocale,
    worker: fn(Mcx<'mcx>, &mut [u8], &[u8], &PgLocale) -> PgResult<usize>,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut dstsize = buff.len() + 1;
    let mut dst: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, dstsize)?;
    dst.resize(dstsize, 0);
    let mut needed = worker(mcx, &mut dst, buff, locale)?;
    if needed + 1 > dstsize {
        dstsize = needed + 1;
        dst.resize(dstsize, 0);
        needed = worker(mcx, &mut dst, buff, locale)?;
        debug_assert!(needed + 1 <= dstsize);
    }
    dst.truncate(nul_pos(&dst[..needed]));
    Ok(dst)
}

// strnlen word-at-a-time (C pays glibc's SIMD strnlen inside pnstrdup;
// a bytewise scan is real measured overhead on the initcap lane).
fn nul_pos(s: &[u8]) -> usize {
    const LO: u64 = 0x0101_0101_0101_0101;
    const HI: u64 = 0x8080_8080_8080_8080;
    let mut i = 0;
    while i + 8 <= s.len() {
        let w = u64::from_le_bytes(s[i..i + 8].try_into().unwrap());
        let zero = w.wrapping_sub(LO) & !w & HI;
        if zero != 0 {
            return i + (zero.trailing_zeros() >> 3) as usize;
        }
        i += 8;
    }
    while i < s.len() && s[i] != 0 {
        i += 1;
    }
    i
}

// C's asc_* shape: pnstrdup (strnlen — the copy itself stops at an embedded
// NUL and drops the tail), then an in-place *p walk over the copy.
fn dup<'mcx>(mcx: Mcx<'mcx>, buff: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let nul = nul_pos(buff);
    let mut out = mcx::vec_with_capacity_in(mcx, nul)?;
    mcx::vec_append_bytes(&mut out, &buff[..nul])?;
    Ok(out)
}

pub fn asc_tolower<'mcx>(mcx: Mcx<'mcx>, buff: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let mut out = dup(mcx, buff)?;
    out.make_ascii_lowercase();
    Ok(out)
}

pub fn asc_toupper<'mcx>(mcx: Mcx<'mcx>, buff: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let mut out = dup(mcx, buff)?;
    out.make_ascii_uppercase();
    Ok(out)
}

pub fn asc_initcap<'mcx>(mcx: Mcx<'mcx>, buff: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let mut out = dup(mcx, buff)?;
    let mut wasalnum = false;
    for b in &mut out[..] {
        let c = if wasalnum {
            b.to_ascii_lowercase()
        } else {
            b.to_ascii_uppercase()
        };
        *b = c;
        // C: "we don't trust isalnum() here"
        wasalnum = c.is_ascii_alphanumeric();
    }
    Ok(out)
}

pub fn str_tolower<'mcx>(mcx: Mcx<'mcx>, buff: &[u8], collid: Oid) -> PgResult<PgVec<'mcx, u8>> {
    let locale = locale_for(collid, "lower()")?;
    if locale.ctype_is_c {
        asc_tolower(mcx, buff)
    } else {
        pg_case(mcx, buff, locale, pg_locale::pg_strlower)
    }
}

pub fn str_toupper<'mcx>(mcx: Mcx<'mcx>, buff: &[u8], collid: Oid) -> PgResult<PgVec<'mcx, u8>> {
    let locale = locale_for(collid, "upper()")?;
    if locale.ctype_is_c {
        asc_toupper(mcx, buff)
    } else {
        pg_case(mcx, buff, locale, pg_locale::pg_strupper)
    }
}

pub fn str_initcap<'mcx>(mcx: Mcx<'mcx>, buff: &[u8], collid: Oid) -> PgResult<PgVec<'mcx, u8>> {
    let locale = locale_for(collid, "initcap()")?;
    if locale.ctype_is_c {
        asc_initcap(mcx, buff)
    } else {
        pg_case(mcx, buff, locale, pg_locale::pg_strtitle)
    }
}

// C spells the casefold indeterminate-collation message with "lower()".
pub fn str_casefold<'mcx>(mcx: Mcx<'mcx>, buff: &[u8], collid: Oid) -> PgResult<PgVec<'mcx, u8>> {
    if !OidIsValid(collid) {
        return Err(indeterminate_collation_err("lower()").into());
    }
    if mbutils::GetDatabaseEncoding() != wchar::PG_UTF8 {
        return Err(casefold_encoding_err().into());
    }
    let locale = pg_locale::pg_newlocale_from_collation(collid)?;
    if locale.ctype_is_c {
        asc_tolower(mcx, buff)
    } else {
        pg_case(mcx, buff, locale, pg_locale::pg_strfold)
    }
}
