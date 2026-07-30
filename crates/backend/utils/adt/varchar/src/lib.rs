//! varchar.c: bpchar (char(n)) and varchar(n) — I/O with typmod
//! blank-padding/truncation, length-coercion casts, char/name conversions,
//! trailing-blank-insensitive comparisons/hashing, pattern ops. Values follow
//! the varlena crate carrier: detoasted payload bytes in, 4B-header
//! [`Varlena`] images out. varchar_support and the sortsupport installs are
//! loud until their substrates land.

#![allow(clippy::result_large_err)]

pub mod builtins;
#[cfg(test)]
mod tests;

use core::ffi::CStr;

use datum::{Bytea, Varlena};
use mcx::{Mcx, PgVec};
use stringinfo::StringInfo;
use types_core::{Oid, C_COLLATION_OID, CSTRINGOID, POSIX_COLLATION_OID};
use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_ARRAY_ELEMENT_ERROR,
    ERRCODE_ARRAY_SUBSCRIPT_ERROR, ERRCODE_INDETERMINATE_COLLATION,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_NULL_VALUE_NOT_ALLOWED,
    ERRCODE_STRING_DATA_RIGHT_TRUNCATION,
};

pub const VARHDRSZ: usize = varlena::VARHDRSZ;
// MaxAttrSize (htup_details.h).
const MAX_ATTR_SIZE: i32 = 10 * 1024 * 1024;
pub const NAMEDATALEN: usize = 64;

#[cold]
#[inline(never)]
fn value_too_long_bpchar(maxlen: i32) -> PgError {
    PgError::error(format!("value too long for type character({maxlen})"))
        .with_sqlstate(ERRCODE_STRING_DATA_RIGHT_TRUNCATION)
}

#[cold]
#[inline(never)]
fn value_too_long_varchar(maxlen: i32) -> PgError {
    PgError::error(format!(
        "value too long for type character varying({maxlen})"
    ))
    .with_sqlstate(ERRCODE_STRING_DATA_RIGHT_TRUNCATION)
}

fn pad_spaces(v: &mut PgVec<'_, u8>, n: usize) -> PgResult<()> {
    if n == 0 {
        return Ok(());
    }
    let mcx = *v.allocator();
    v.try_reserve(n).map_err(|_| mcx.oom(n))?;
    let old = v.len();
    // SAFETY: capacity >= old + n after try_reserve; set_len covers the fill.
    unsafe {
        core::ptr::write_bytes(v.as_mut_ptr().add(old), b' ', n);
        v.set_len(old + n);
    }
    Ok(())
}

pub struct BpClip {
    pub copy: usize,
    pub total: usize,
}

// bpchar_input's clip/pad decision: copy `copy` input bytes, blank-pad to
// `total` payload bytes. Ok(None) = soft error saved into escontext.
pub fn bpchar_clip(
    s: &[u8],
    atttypmod: i32,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<BpClip>> {
    let len = s.len();
    if atttypmod < VARHDRSZ as i32 {
        return Ok(Some(BpClip {
            copy: len,
            total: len,
        }));
    }
    // atttypmod counts characters, not bytes.
    let maxchars = atttypmod as usize - VARHDRSZ;
    let charlen = mbutils::pg_mbstrlen_with_len(s)? as usize;
    if charlen > maxchars {
        let mbmaxlen = mbutils::pg_mbcharcliplen(s, len as i32, maxchars as i32)? as usize;
        if s[mbmaxlen..].iter().any(|&b| b != b' ') {
            return ereturn(
                escontext.as_deref_mut(),
                None,
                value_too_long_bpchar(maxchars as i32),
            );
        }
        Ok(Some(BpClip {
            copy: mbmaxlen,
            total: mbmaxlen,
        }))
    } else {
        Ok(Some(BpClip {
            copy: len,
            total: len + (maxchars - charlen),
        }))
    }
}

pub fn bpchar_input<'mcx>(
    mcx: Mcx<'mcx>,
    s: &[u8],
    atttypmod: i32,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<Varlena<'mcx>>> {
    let Some(clip) = bpchar_clip(s, atttypmod, escontext)? else {
        return Ok(None);
    };
    let mut image = varlena::image_with_header(mcx, clip.total)?;
    mcx::vec_append_bytes(&mut image, &s[..clip.copy])?;
    pad_spaces(&mut image, clip.total - clip.copy)?;
    Ok(Some(Varlena::from_image(image)))
}

pub fn bpcharout<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    varlena::text_to_cstring(mcx, s)
}

pub fn bpcharrecv<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &mut StringInfo<'_>,
    atttypmod: i32,
) -> PgResult<Varlena<'mcx>> {
    let rawbytes = buf.len().saturating_sub(buf.cursor);
    let str = pqformat::pq_getmsgtext(mcx, buf, rawbytes)?;
    Ok(bpchar_input(mcx, &str, atttypmod, None)?
        .expect("bpchar_input: soft-error escape without an escontext"))
}

pub fn bpcharsend<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<Bytea<'mcx>> {
    varlena::textsend(mcx, s)
}

// bpchar() length coercion (OID 668). Ok(None) = return the source datum.
pub fn bpchar<'mcx>(
    mcx: Mcx<'mcx>,
    source: &[u8],
    maxlen: i32,
    is_explicit: bool,
) -> PgResult<Option<Varlena<'mcx>>> {
    if maxlen < VARHDRSZ as i32 {
        return Ok(None);
    }
    let maxchars = maxlen as usize - VARHDRSZ;
    let len = source.len();
    let charlen = mbutils::pg_mbstrlen_with_len(source)? as usize;
    if charlen == maxchars {
        return Ok(None);
    }
    let (copy, total) = if charlen > maxchars {
        let maxmblen = mbutils::pg_mbcharcliplen(source, len as i32, maxchars as i32)? as usize;
        if !is_explicit && source[maxmblen..].iter().any(|&b| b != b' ') {
            return Err(value_too_long_bpchar(maxchars as i32).into());
        }
        (maxmblen, maxmblen as i32)
    } else {
        // C (varchar.c bpchar): `maxlen = len + (maxlen - charlen)` in int
        // arithmetic under -fwrapv.
        (
            len,
            (len as i32).wrapping_add(maxchars as i32 - charlen as i32),
        )
    };
    // C pallocs `maxlen + VARHDRSZ` — int arithmetic that wraps for typmods
    // near INT32_MAX; palloc's Size (u64) parameter then sign-extends, and
    // the alloc guard reports that wrapped size (e.g. 18446744071562067969
    // for typmod = INT32_MAX). Reproduce the exact request C makes.
    let request = (total as i32).wrapping_add(VARHDRSZ as i32) as i64 as u64;
    mcx::check_alloc_size(request as usize)?;
    // Guard passed → the C int arithmetic did not wrap; safe as usize.
    let total = total as usize;
    let mut image = varlena::image_with_header(mcx, total)?;
    mcx::vec_append_bytes(&mut image, &source[..copy])?;
    pad_spaces(&mut image, total - copy)?;
    Ok(Some(Varlena::from_image(image)))
}

pub fn char_bpchar<'mcx>(mcx: Mcx<'mcx>, c: i8) -> PgResult<Varlena<'mcx>> {
    varlena::cstring_to_text(mcx, &[c as u8])
}

pub fn bpchar_name(s: &[u8]) -> [u8; NAMEDATALEN] {
    let mut len = s.len();
    if len >= NAMEDATALEN {
        len = mbutils::pg_mbcliplen(s, len as i32, NAMEDATALEN as i32 - 1) as usize;
    }
    while len > 0 && s[len - 1] == b' ' {
        len -= 1;
    }
    let mut out = [0u8; NAMEDATALEN];
    out[..len].copy_from_slice(&s[..len]);
    out
}

pub fn name_bpchar<'mcx>(mcx: Mcx<'mcx>, name: &[u8]) -> PgResult<Varlena<'mcx>> {
    let end = name.iter().position(|&b| b == 0).unwrap_or(name.len());
    varlena::cstring_to_text(mcx, &name[..end])
}

#[track_caller]
#[cold]
#[inline(never)]
fn typmod_array_err(msg: &'static str, sqlstate: types_error::SqlState) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(sqlstate))
}

// ArrayGetIntegerTypmods (arrayutils.c) — no shared port exists yet.
fn array_get_integer_typmods<'mcx>(mcx: Mcx<'mcx>, arr: &[u8]) -> PgResult<PgVec<'mcx, i32>> {
    if arrayfuncs::arr_elemtype(arr) != CSTRINGOID {
        return Err(typmod_array_err(
            "typmod array must be type cstring[]",
            ERRCODE_ARRAY_ELEMENT_ERROR,
        ));
    }
    if arrayfuncs::arr_ndim(arr) != 1 {
        return Err(typmod_array_err(
            "typmod array must be one-dimensional",
            ERRCODE_ARRAY_SUBSCRIPT_ERROR,
        ));
    }
    if arrayfuncs::array_contains_nulls(arr) {
        return Err(typmod_array_err(
            "typmod array must not contain nulls",
            ERRCODE_NULL_VALUE_NOT_ALLOWED,
        ));
    }
    let (elems, _nulls) = arrayfuncs::deconstruct_array_builtin(mcx, arr, CSTRINGOID, false)?;
    let mut out = mcx::vec_with_capacity_in(mcx, elems.len())?;
    for d in elems.iter() {
        // SAFETY: non-null cstring element datum pointing into `arr`.
        let c = unsafe { CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) };
        let s = core::str::from_utf8(c.to_bytes()).map_err(|_| invalid_type_modifier())?;
        out.push(numutils::pg_strtoint32(s)?);
    }
    Ok(out)
}

#[track_caller]
#[cold]
#[inline(never)]
fn invalid_type_modifier() -> Box<PgError> {
    Box::new(PgError::error("invalid type modifier").with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE))
}

// pub for proofs/oracle-compat (Kani check-core harness; visibility-only
// change per the 2026-07-28 shipped-edits ruling).
pub fn anychar_typmodin(tl: &[i32], typename: &str) -> PgResult<i32> {
    if tl.len() != 1 {
        return Err(invalid_type_modifier());
    }
    if tl[0] < 1 {
        return Err(Box::new(
            PgError::error(format!("length for type {typename} must be at least 1"))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    if tl[0] > MAX_ATTR_SIZE {
        return Err(Box::new(
            PgError::error(format!(
                "length for type {typename} cannot exceed {MAX_ATTR_SIZE}"
            ))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    // Typmod is VARHDRSZ plus the character count, for historical reasons.
    Ok(VARHDRSZ as i32 + tl[0])
}

pub fn bpchartypmodin<'mcx>(mcx: Mcx<'mcx>, arr: &[u8]) -> PgResult<i32> {
    anychar_typmodin(&array_get_integer_typmods(mcx, arr)?, "char")
}

pub fn varchartypmodin<'mcx>(mcx: Mcx<'mcx>, arr: &[u8]) -> PgResult<i32> {
    anychar_typmodin(&array_get_integer_typmods(mcx, arr)?, "varchar")
}

// anychar_typmodout: "(n)" into `buf`, or 0 bytes when typmod <= VARHDRSZ.
pub fn anychar_typmodout(typmod: i32, buf: &mut [u8; 16]) -> usize {
    if typmod > VARHDRSZ as i32 {
        buf[0] = b'(';
        let n = numutils::pg_ltoa(typmod - VARHDRSZ as i32, &mut buf[1..]);
        buf[1 + n] = b')';
        n + 2
    } else {
        0
    }
}

pub fn varchar_clip(
    s: &[u8],
    atttypmod: i32,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<usize>> {
    let len = s.len();
    if atttypmod >= VARHDRSZ as i32 && len > atttypmod as usize - VARHDRSZ {
        let maxlen = atttypmod as usize - VARHDRSZ;
        let mbmaxlen = mbutils::pg_mbcharcliplen(s, len as i32, maxlen as i32)? as usize;
        if s[mbmaxlen..].iter().any(|&b| b != b' ') {
            return ereturn(
                escontext.as_deref_mut(),
                None,
                value_too_long_varchar(maxlen as i32),
            );
        }
        return Ok(Some(mbmaxlen));
    }
    Ok(Some(len))
}

pub fn varchar_input<'mcx>(
    mcx: Mcx<'mcx>,
    s: &[u8],
    atttypmod: i32,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<Varlena<'mcx>>> {
    let Some(len) = varchar_clip(s, atttypmod, escontext)? else {
        return Ok(None);
    };
    Ok(Some(varlena::cstring_to_text(mcx, &s[..len])?))
}

pub fn varcharout<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    varlena::text_to_cstring(mcx, s)
}

pub fn varcharrecv<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &mut StringInfo<'_>,
    atttypmod: i32,
) -> PgResult<Varlena<'mcx>> {
    let rawbytes = buf.len().saturating_sub(buf.cursor);
    let str = pqformat::pq_getmsgtext(mcx, buf, rawbytes)?;
    Ok(varchar_input(mcx, &str, atttypmod, None)?
        .expect("varchar_input: soft-error escape without an escontext"))
}

pub fn varcharsend<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<Bytea<'mcx>> {
    varlena::textsend(mcx, s)
}

// varchar() length coercion (OID 669). Ok(None) = return the source datum.
pub fn varchar<'mcx>(
    mcx: Mcx<'mcx>,
    source: &[u8],
    typmod: i32,
    is_explicit: bool,
) -> PgResult<Option<Varlena<'mcx>>> {
    let len = source.len() as i32;
    // C (varchar.c varchar): `maxlen = typmod - VARHDRSZ` in int arithmetic
    // under -fwrapv, so typmod = INT32_MIN wraps to a huge positive maxlen
    // and falls into the "supplied data fits" return of the source datum.
    let maxlen = typmod.wrapping_sub(VARHDRSZ as i32);
    if maxlen < 0 || len <= maxlen {
        return Ok(None);
    }
    let maxmblen = mbutils::pg_mbcharcliplen(source, len, maxlen)? as usize;
    if !is_explicit && source[maxmblen..].iter().any(|&b| b != b' ') {
        return Err(value_too_long_varchar(maxlen).into());
    }
    Ok(Some(varlena::cstring_to_text(mcx, &source[..maxmblen])?))
}

pub fn bpchartruelen(s: &[u8]) -> usize {
    s.len() - s.iter().rev().take_while(|&&b| b == b' ').count()
}

fn bc_trim(s: &[u8]) -> &[u8] {
    &s[..bpchartruelen(s)]
}

pub fn bpcharlen(arg: &[u8]) -> PgResult<i32> {
    let len = bpchartruelen(arg);
    if mbutils::pg_database_encoding_max_length() != 1 {
        mbutils::pg_mbstrlen_with_len(&arg[..len])
    } else {
        Ok(len as i32)
    }
}

pub fn bpcharoctetlen(arg: &[u8]) -> i32 {
    arg.len() as i32
}

pub fn bpchareq(arg1: &[u8], arg2: &[u8], collid: Oid) -> PgResult<bool> {
    varlena::texteq(bc_trim(arg1), bc_trim(arg2), collid)
}

pub fn bpcharne(arg1: &[u8], arg2: &[u8], collid: Oid) -> PgResult<bool> {
    varlena::textne(bc_trim(arg1), bc_trim(arg2), collid)
}

pub fn bpcharcmp(arg1: &[u8], arg2: &[u8], collid: Oid) -> PgResult<i32> {
    varlena::varstr_cmp(bc_trim(arg1), bc_trim(arg2), collid)
}

pub fn bpcharlt(arg1: &[u8], arg2: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(bpcharcmp(arg1, arg2, collid)? < 0)
}

pub fn bpcharle(arg1: &[u8], arg2: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(bpcharcmp(arg1, arg2, collid)? <= 0)
}

pub fn bpchargt(arg1: &[u8], arg2: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(bpcharcmp(arg1, arg2, collid)? > 0)
}

pub fn bpcharge(arg1: &[u8], arg2: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(bpcharcmp(arg1, arg2, collid)? >= 0)
}

#[track_caller]
#[cold]
#[inline(never)]
fn hash_collation_err() -> Box<PgError> {
    Box::new(
        PgError::error("could not determine which collation to use for string hashing")
            .with_sqlstate(ERRCODE_INDETERMINATE_COLLATION)
            .with_hint("Use the COLLATE clause to set the collation explicitly."),
    )
}

fn bpchar_nondeterministic_hash(
    collid: Oid,
    k: &[u8],
    seed: Option<u64>,
) -> PgResult<Option<u64>> {
    if collid == C_COLLATION_OID || collid == POSIX_COLLATION_OID {
        return Ok(None);
    }
    pg_locale_seams::varstr_nondeterministic_hash::call(collid, k, seed)
}

pub fn hashbpchar(key: &[u8], collid: Oid) -> PgResult<u32> {
    if collid == 0 {
        return Err(hash_collation_err());
    }
    let k = bc_trim(key);
    if let Some(h) = bpchar_nondeterministic_hash(collid, k, None)? {
        return Ok(h as u32);
    }
    Ok(hashfn::hash_bytes(k))
}

pub fn hashbpcharextended(key: &[u8], collid: Oid, seed: u64) -> PgResult<u64> {
    if collid == 0 {
        return Err(hash_collation_err());
    }
    let k = bc_trim(key);
    if let Some(h) = bpchar_nondeterministic_hash(collid, k, Some(seed))? {
        return Ok(h);
    }
    Ok(hashfn::hash_bytes_extended(k, seed))
}

// C returns the raw memcmp value; sign-normalized here (varlena precedent).
pub fn internal_bpchar_pattern_compare(arg1: &[u8], arg2: &[u8]) -> i32 {
    varlena::bpcharfastcmp_c(arg1, arg2)
}

pub fn bpchar_pattern_lt(arg1: &[u8], arg2: &[u8]) -> bool {
    internal_bpchar_pattern_compare(arg1, arg2) < 0
}

pub fn bpchar_pattern_le(arg1: &[u8], arg2: &[u8]) -> bool {
    internal_bpchar_pattern_compare(arg1, arg2) <= 0
}

pub fn bpchar_pattern_ge(arg1: &[u8], arg2: &[u8]) -> bool {
    internal_bpchar_pattern_compare(arg1, arg2) >= 0
}

pub fn bpchar_pattern_gt(arg1: &[u8], arg2: &[u8]) -> bool {
    internal_bpchar_pattern_compare(arg1, arg2) > 0
}

pub fn btbpchar_pattern_cmp(arg1: &[u8], arg2: &[u8]) -> i32 {
    internal_bpchar_pattern_compare(arg1, arg2)
}
