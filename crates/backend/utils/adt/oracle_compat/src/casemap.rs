//! str_tolower/str_toupper/str_initcap/str_casefold case kernels, decomposed
//! out of formatting.c for their oracle_compat.c consumers (lower/upper/
//! initcap/casefold). ctype_is_c collations take the asc_* fast path; the
//! non-C arms (pg_strlower/pg_strupper/pg_strtitle/pg_strfold provider
//! dispatch) are loud until the pg_locale casemap lane lands.

use mcx::{Mcx, PgVec};
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

#[cold]
#[inline(never)]
fn non_c_ctype_unported(fname: &str) -> ! {
    panic!("{fname}: non-C ctype arm requires the pg_locale casemap lane (pg_strlower/upper/title/fold unported)")
}

fn ctype_is_c(collid: Oid, fname: &str) -> PgResult<bool> {
    if !OidIsValid(collid) {
        return Err(indeterminate_collation_err(fname).into());
    }
    Ok(pg_locale::pg_newlocale_from_collation(collid)?.ctype_is_c)
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

// C's asc_* shape: one copy (pnstrdup), then an in-place *p walk that stops
// at an embedded NUL.
fn dup<'mcx>(mcx: Mcx<'mcx>, buff: &[u8]) -> PgResult<(PgVec<'mcx, u8>, usize)> {
    let mut out = mcx::vec_with_capacity_in(mcx, buff.len())?;
    mcx::vec_append_bytes(&mut out, buff)?;
    Ok((out, nul_pos(buff)))
}

pub fn asc_tolower<'mcx>(mcx: Mcx<'mcx>, buff: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let (mut out, nul) = dup(mcx, buff)?;
    out[..nul].make_ascii_lowercase();
    Ok(out)
}

pub fn asc_toupper<'mcx>(mcx: Mcx<'mcx>, buff: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let (mut out, nul) = dup(mcx, buff)?;
    out[..nul].make_ascii_uppercase();
    Ok(out)
}

pub fn asc_initcap<'mcx>(mcx: Mcx<'mcx>, buff: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let (mut out, nul) = dup(mcx, buff)?;
    let mut wasalnum = false;
    for b in &mut out[..nul] {
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
    if ctype_is_c(collid, "lower()")? {
        asc_tolower(mcx, buff)
    } else {
        non_c_ctype_unported("str_tolower")
    }
}

pub fn str_toupper<'mcx>(mcx: Mcx<'mcx>, buff: &[u8], collid: Oid) -> PgResult<PgVec<'mcx, u8>> {
    if ctype_is_c(collid, "upper()")? {
        asc_toupper(mcx, buff)
    } else {
        non_c_ctype_unported("str_toupper")
    }
}

pub fn str_initcap<'mcx>(mcx: Mcx<'mcx>, buff: &[u8], collid: Oid) -> PgResult<PgVec<'mcx, u8>> {
    if ctype_is_c(collid, "initcap()")? {
        asc_initcap(mcx, buff)
    } else {
        non_c_ctype_unported("str_initcap")
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
    if pg_locale::pg_newlocale_from_collation(collid)?.ctype_is_c {
        asc_tolower(mcx, buff)
    } else {
        non_c_ctype_unported("str_casefold")
    }
}
