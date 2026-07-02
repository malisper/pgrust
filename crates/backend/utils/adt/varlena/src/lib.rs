//! varlena.c, boot+SELECT-spine lane: text/bytea/unknown I/O, eq/cmp
//! collation core (C-collation memcmp fast path, seam-free), length,
//! catenate, C-collation sort comparator cores. Carrier: detoasted payload
//! bytes in, full 4B-header [`Varlena`] images out (one allocation). Output
//! is direct-to-wire (no per-row UTF-8 revalidation). Deferred to their
//! catalog rows: position/substring/overlay/replace, split/format/concat/
//! string_agg, name<->text + pattern ops, sortsupport abbreviation, regex
//! tails, misc encoding. External/compressed images and non-C collations go
//! through detoast_seams / pg_locale_seams (loud until those units land).

pub mod builtins;
pub mod bytea;
#[cfg(test)]
mod tests;

use core::cmp::Ordering;

use datum::{Bytea, Varlena};
use mcx::{Mcx, PgVec};
use stringinfo::StringInfo;
use types_core::{C_COLLATION_OID, Oid, OidIsValid, POSIX_COLLATION_OID};
use types_error::{PgError, PgResult, ERRCODE_INDETERMINATE_COLLATION};
use types_tuple::varatt;

pub const VARHDRSZ: usize = datum::varlena::VARHDRSZ;

pub(crate) fn image_with_header<'mcx>(mcx: Mcx<'mcx>, payload_len: usize) -> PgResult<PgVec<'mcx, u8>> {
    mcx::check_alloc_size(payload_len + VARHDRSZ)?;
    let mut image = mcx::vec_with_capacity_in(mcx, VARHDRSZ + payload_len)?;
    mcx::vec_append_bytes(&mut image, &[0u8; VARHDRSZ])?;
    Ok(image)
}

// C: cstring_to_text[_with_len] — a slice carries its length.
pub fn cstring_to_text<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<Varlena<'mcx>> {
    let mut image = image_with_header(mcx, s.len())?;
    mcx::vec_append_bytes(&mut image, s)?;
    Ok(Varlena::from_image(image))
}

// C: text_to_cstring post-detoast tail (palloc(len+1) + memcpy + NUL).
pub fn text_to_cstring<'mcx>(mcx: Mcx<'mcx>, t: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let mut out = mcx::vec_with_capacity_in(mcx, t.len() + 1)?;
    mcx::vec_append_bytes(&mut out, t)?;
    out.push(0);
    Ok(out)
}

// C: pg_detoast_datum_packed + VARDATA_ANY over a bounded image.
pub enum VarPayload<'a, 'mcx> {
    Inline(&'a [u8]),
    Detoasted(PgVec<'mcx, u8>),
}

impl VarPayload<'_, '_> {
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            VarPayload::Inline(s) => s,
            VarPayload::Detoasted(v) => &v[VARHDRSZ..],
        }
    }
}

pub fn open_image<'a, 'mcx>(mcx: Mcx<'mcx>, image: &'a [u8]) -> PgResult<VarPayload<'a, 'mcx>> {
    let b0 = image[0];
    if b0 == 0x01 || (b0 & 0x03) == 0x02 {
        return Ok(VarPayload::Detoasted(detoast_seams::detoast_attr::call(
            mcx, image,
        )?));
    }
    if b0 & 0x01 == 0x01 {
        let total = ((b0 >> 1) & 0x7F) as usize;
        return Ok(VarPayload::Inline(&image[1..total]));
    }
    let word = u32::from_ne_bytes([image[0], image[1], image[2], image[3]]);
    let total = varatt::varsize_4b_word(word) as usize;
    Ok(VarPayload::Inline(&image[VARHDRSZ..total]))
}

#[cold]
#[inline(never)]
fn indeterminate_collation_err() -> PgError {
    PgError::error("could not determine which collation to use for string comparison")
        .with_sqlstate(ERRCODE_INDETERMINATE_COLLATION)
        .with_hint("Use the COLLATE clause to set the collation explicitly.")
}

#[inline]
pub fn check_collation_set(collid: Oid) -> PgResult<()> {
    if !OidIsValid(collid) {
        return Err(indeterminate_collation_err().into());
    }
    Ok(())
}

// C's lc_collate_is_c fast cases; every other collid is the seam's truth.
#[inline(always)]
fn collation_is_c_known(collid: Oid) -> bool {
    collid == C_COLLATION_OID || collid == POSIX_COLLATION_OID
}

#[inline]
fn collation_is_deterministic(collid: Oid) -> PgResult<bool> {
    if collation_is_c_known(collid) {
        Ok(true)
    } else {
        pg_locale_seams::collation_is_deterministic::call(collid)
    }
}

// C: varstrfastcmp_c — memcmp + length tiebreak; also the comparator core
// varstr_sortsupport installs for C collations (abbreviation arms on top).
#[inline]
pub fn varstrfastcmp_c(a1: &[u8], a2: &[u8]) -> i32 {
    let n = a1.len().min(a2.len());
    match a1[..n].cmp(&a2[..n]) {
        Ordering::Less => -1,
        Ordering::Greater => 1,
        Ordering::Equal => {
            if a1.len() == a2.len() {
                0
            } else if a1.len() < a2.len() {
                -1
            } else {
                1
            }
        }
    }
}

// C: bpcharfastcmp_c — trailing-blank-trimmed memcmp + tiebreak.
#[inline]
pub fn bpcharfastcmp_c(a1: &[u8], a2: &[u8]) -> i32 {
    let t1 = &a1[..a1.len() - a1.iter().rev().take_while(|&&b| b == b' ').count()];
    let t2 = &a2[..a2.len() - a2.iter().rev().take_while(|&&b| b == b' ').count()];
    varstrfastcmp_c(t1, t2)
}

pub fn varstr_cmp(arg1: &[u8], arg2: &[u8], collid: Oid) -> PgResult<i32> {
    check_collation_set(collid)?;
    if collation_is_c_known(collid) {
        return Ok(varstrfastcmp_c(arg1, arg2));
    }
    varstr_cmp_locale(arg1, arg2, collid)
}

#[cold]
#[inline(never)]
fn varstr_cmp_locale(arg1: &[u8], arg2: &[u8], collid: Oid) -> PgResult<i32> {
    if arg1.len() == arg2.len() && arg1 == arg2 {
        return Ok(0);
    }
    pg_locale_seams::varstr_cmp_locale::call(collid, arg1, arg2)
}

pub fn text_cmp(arg1: &[u8], arg2: &[u8], collid: Oid) -> PgResult<i32> {
    varstr_cmp(arg1, arg2, collid)
}

pub fn texteq(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<bool> {
    check_collation_set(collid)?;
    if collation_is_c_known(collid) {
        return Ok(t1.len() == t2.len() && t1 == t2);
    }
    texteq_slow(t1, t2, collid)
}

#[cold]
#[inline(never)]
fn texteq_slow(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<bool> {
    if pg_locale_seams::collation_is_deterministic::call(collid)? {
        Ok(t1.len() == t2.len() && t1 == t2)
    } else {
        Ok(text_cmp(t1, t2, collid)? == 0)
    }
}

pub fn textne(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(!texteq(t1, t2, collid)?)
}

pub fn text_lt(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(text_cmp(t1, t2, collid)? < 0)
}

pub fn text_le(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(text_cmp(t1, t2, collid)? <= 0)
}

pub fn text_gt(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(text_cmp(t1, t2, collid)? > 0)
}

pub fn text_ge(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(text_cmp(t1, t2, collid)? >= 0)
}

pub fn bttextcmp(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<i32> {
    text_cmp(t1, t2, collid)
}

// C returns one of the argument pointers; the winner is the borrowed input.
pub fn text_larger<'a>(t1: &'a [u8], t2: &'a [u8], collid: Oid) -> PgResult<&'a [u8]> {
    Ok(if text_cmp(t1, t2, collid)? > 0 { t1 } else { t2 })
}

pub fn text_smaller<'a>(t1: &'a [u8], t2: &'a [u8], collid: Oid) -> PgResult<&'a [u8]> {
    Ok(if text_cmp(t1, t2, collid)? < 0 { t1 } else { t2 })
}

pub fn btvarstrequalimage(collid: Oid) -> PgResult<bool> {
    check_collation_set(collid)?;
    collation_is_deterministic(collid)
}

pub fn text_length(t: &[u8]) -> i32 {
    if mbutils_seams::pg_database_encoding_max_length::call() == 1 {
        t.len() as i32
    } else {
        mbutils_seams::pg_mbstrlen_with_len::call(t)
    }
}

pub fn textoctetlen(t: &[u8]) -> i32 {
    t.len() as i32
}

pub fn text_catenate<'mcx>(mcx: Mcx<'mcx>, t1: &[u8], t2: &[u8]) -> PgResult<Varlena<'mcx>> {
    let mut image = image_with_header(mcx, t1.len() + t2.len())?;
    mcx::vec_append_bytes(&mut image, t1)?;
    mcx::vec_append_bytes(&mut image, t2)?;
    Ok(Varlena::from_image(image))
}

pub fn textin<'mcx>(mcx: Mcx<'mcx>, input: &[u8]) -> PgResult<Varlena<'mcx>> {
    cstring_to_text(mcx, input)
}

pub fn textout<'mcx>(mcx: Mcx<'mcx>, t: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    text_to_cstring(mcx, t)
}

pub fn textrecv<'mcx>(mcx: Mcx<'mcx>, buf: &mut StringInfo<'_>) -> PgResult<Varlena<'mcx>> {
    let rawbytes = buf.len().saturating_sub(buf.cursor);
    let str = pqformat::pq_getmsgtext(mcx, buf, rawbytes)?;
    cstring_to_text(mcx, &str)
}

pub fn textsend<'mcx>(mcx: Mcx<'mcx>, t: &[u8]) -> PgResult<Bytea<'mcx>> {
    let mut buf = pqformat::pq_begintypsend(mcx)?;
    pqformat::pq_sendtext(&mut buf, t)?;
    Ok(pqformat::pq_endtypsend(buf))
}

// C: pstrdup — bytes up to the first NUL, re-terminated.
fn pstrdup<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let len = s.iter().position(|&b| b == 0).unwrap_or(s.len());
    text_to_cstring(mcx, &s[..len])
}

pub fn unknownin<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    pstrdup(mcx, s)
}

pub fn unknownout<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    pstrdup(mcx, s)
}

pub fn unknownrecv<'mcx>(mcx: Mcx<'mcx>, buf: &mut StringInfo<'_>) -> PgResult<PgVec<'mcx, u8>> {
    let rawbytes = buf.len().saturating_sub(buf.cursor);
    let mut str = pqformat::pq_getmsgtext(mcx, buf, rawbytes)?;
    str.push(0);
    Ok(str)
}

pub fn unknownsend<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<Bytea<'mcx>> {
    let len = s.iter().position(|&b| b == 0).unwrap_or(s.len());
    let mut buf = pqformat::pq_begintypsend(mcx)?;
    pqformat::pq_sendtext(&mut buf, &s[..len])?;
    Ok(pqformat::pq_endtypsend(buf))
}

// C: int bytea_output = BYTEA_OUTPUT_HEX (guc_tables binds its variable here).
use std::cell::Cell;

thread_local! {
    static BYTEA_OUTPUT: Cell<i32> = const { Cell::new(guc_tables::consts::BYTEA_OUTPUT_HEX) };
}

pub fn get_bytea_output() -> i32 {
    BYTEA_OUTPUT.with(|v| v.get())
}

pub fn set_bytea_output(value: i32) {
    BYTEA_OUTPUT.with(|v| v.set(value));
}

pub fn init_seams() {
    guc_tables::vars::bytea_output.install(guc_tables::GucVarAccessors {
        get: get_bytea_output,
        set: set_bytea_output,
    });
}
