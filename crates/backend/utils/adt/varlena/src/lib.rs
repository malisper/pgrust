//! varlena.c, boot+SELECT-spine lane: text/bytea/unknown I/O, eq/cmp
//! collation core (C-collation memcmp fast path, seam-free), length,
//! catenate, position/substring/overlay/replace, to_hex/to_bin/to_oct,
//! C-collation sort comparator cores. Carrier: detoasted payload bytes in,
//! full 4B-header [`Varlena`] images out (one allocation). Output is
//! direct-to-wire (no per-row UTF-8 revalidation). Deferred to their catalog
//! rows: split/format/concat/string_agg, name<->text + pattern ops,
//! sortsupport abbreviation, regex tails, misc encoding. External/compressed
//! images and non-C collations go through detoast_seams / pg_locale_seams.

pub mod builtins;
pub mod abbrev;
pub mod bytea;
pub mod concat_format;
pub mod levenshtein;
pub mod split_text;
pub mod replace_regexp;
pub mod string_agg;
pub mod unicode;
#[cfg(test)]
mod tests;

use core::cmp::Ordering;

use datum::{Bytea, Varlena};
use mcx::{Mcx, PgVec};
use stringinfo::StringInfo;
use types_core::{C_COLLATION_OID, Oid, OidIsValid, POSIX_COLLATION_OID};
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INDETERMINATE_COLLATION};
use types_tuple::varatt;

pub const VARHDRSZ: usize = datum::varlena::VARHDRSZ;

pub fn image_with_header<'mcx>(mcx: Mcx<'mcx>, payload_len: usize) -> PgResult<PgVec<'mcx, u8>> {
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
//
// C (varlena.c varstr_cmp / varstrfastcmp_c) returns the RAW memcmp result —
// libc memcmp reports the difference of the first mismatching bytes (e.g.
// 'a' vs 'c' → -2), and that magnitude is byte-visible at the SQL level
// through bttextcmp/btnametextcmp/bttextnamecmp (fnconf batch-1, OIDs
// 246/253). Only the equal-prefix length tie-break normalizes to ±1, as in C.
#[inline]
pub fn varstrfastcmp_c(a1: &[u8], a2: &[u8]) -> i32 {
    let n = a1.len().min(a2.len());
    if let Some(i) = a1[..n].iter().zip(&a2[..n]).position(|(x, y)| x != y) {
        return a1[i] as i32 - a2[i] as i32;
    }
    if a1.len() == a2.len() {
        0
    } else if a1.len() < a2.len() {
        -1
    } else {
        1
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

pub(crate) fn collation_is_c_known_pub(collid: Oid) -> bool {
    collation_is_c_known(collid)
}

/// hashtext (hashfunc.c) over detoasted text bytes — `fc_hashtext`'s core for
/// executor step paths that carry no fcinfo (execgrouping's text probe
/// kernel). Nondeterministic collations hash the pg_strnxfrm sort key,
/// deterministic (incl. C-known) hash the raw bytes — bit-identical to the
/// fmgr entry point.
pub fn hashtext_bytes(collid: Oid, data: &[u8]) -> PgResult<u32> {
    if let Some(h) = builtins::hashtext_nondeterministic(collid, data, None)? {
        return Ok(h as u32);
    }
    Ok(::hashfn::hash_bytes(data))
}

/// Whether text hashing/equality under this collation reduces to raw bytes
/// (hashtext = hash_any, texteq = length + memcmp): valid AND deterministic.
/// Resolve-once gate for execgrouping's text probe kernel — the per-row fmgr
/// entry points make exactly this decision per call, with the same inputs
/// and the same catalog truth, so hoisting it is value-invisible.
pub fn text_collation_is_raw_bytes(collid: Oid) -> PgResult<bool> {
    if !OidIsValid(collid) {
        return Ok(false);
    }
    collation_is_deterministic(collid)
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

pub fn text_starts_with(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<bool> {
    check_collation_set(collid)?;
    if !collation_is_deterministic(collid)? {
        return Err(Box::new(
            PgError::error(
                "nondeterministic collations are not supported for substring searches",
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    Ok(t2.len() <= t1.len() && &t1[..t2.len()] == t2)
}

// internal_text_pattern_compare: raw memcmp + length tiebreak, sign-normalized.
pub fn text_pattern_lt(t1: &[u8], t2: &[u8]) -> bool {
    varstrfastcmp_c(t1, t2) < 0
}

pub fn text_pattern_le(t1: &[u8], t2: &[u8]) -> bool {
    varstrfastcmp_c(t1, t2) <= 0
}

pub fn text_pattern_ge(t1: &[u8], t2: &[u8]) -> bool {
    varstrfastcmp_c(t1, t2) >= 0
}

pub fn text_pattern_gt(t1: &[u8], t2: &[u8]) -> bool {
    varstrfastcmp_c(t1, t2) > 0
}

pub fn bttext_pattern_cmp(t1: &[u8], t2: &[u8]) -> i32 {
    varstrfastcmp_c(t1, t2)
}

pub fn btvarstrequalimage(collid: Oid) -> PgResult<bool> {
    check_collation_set(collid)?;
    collation_is_deterministic(collid)
}

pub fn text_length(t: &[u8]) -> PgResult<i32> {
    if mbutils_seams::pg_database_encoding_max_length::call() == 1 {
        Ok(t.len() as i32)
    } else {
        mbutils_seams::pg_mbstrlen_with_len::call(t)
    }
}

pub fn textoctetlen(t: &[u8]) -> i32 {
    t.len() as i32
}

// VARDATA_ANY over a guaranteed-inline (short or plain 4B) image.
fn inline_payload(img: &[u8]) -> &[u8] {
    debug_assert!(img[0] != 0x01 && (img[0] & 0x03) != 0x02);
    if img[0] & 0x01 == 0x01 {
        &img[1..((img[0] >> 1) & 0x7F) as usize]
    } else {
        let word = u32::from_ne_bytes([img[0], img[1], img[2], img[3]]);
        &img[VARHDRSZ..varatt::varsize_4b_word(word) as usize]
    }
}

#[cold]
#[inline(never)]
fn negative_substring_len() -> PgError {
    PgError::error("negative substring length not allowed")
        .with_sqlstate(types_error::ERRCODE_SUBSTRING_ERROR)
}

// C: text_substring — `image` is the RAW argument image; toasted sources go
// through the detoast_attr_slice fetch, C-exact in both encoding arms.
pub fn text_substring<'mcx>(
    mcx: Mcx<'mcx>,
    image: &[u8],
    start: i32,
    length: i32,
    length_not_specified: bool,
) -> PgResult<Varlena<'mcx>> {
    let eml = mbutils_seams::pg_database_encoding_max_length::call();
    let s1 = start.max(1);

    if eml == 1 {
        let l1 = if length_not_specified {
            -1
        } else if length < 0 {
            return Err(negative_substring_len().into());
        } else {
            match start.checked_add(length) {
                None => -1,
                Some(e) => {
                    if e < 1 {
                        return cstring_to_text(mcx, b"");
                    }
                    e - s1
                }
            }
        };
        return Ok(Varlena::from_image(
            detoast_seams::detoast_attr_slice::call(mcx, image, s1 - 1, l1)?,
        ));
    }
    assert!(eml > 1, "invalid backend encoding: encoding max length < 1");

    let slice_start = 0i32;
    let (slice_size, l1);
    if length_not_specified {
        slice_size = -1;
        l1 = -1;
    } else if length < 0 {
        return Err(negative_substring_len().into());
    } else {
        match start.checked_add(length) {
            None => {
                slice_size = -1;
                l1 = -1;
            }
            Some(e) => {
                if e <= 1 {
                    return cstring_to_text(mcx, b"");
                }
                l1 = e - s1;
                slice_size = (e - 1).checked_mul(eml).unwrap_or(-1);
            }
        }
    }

    let sliced: Option<PgVec<'mcx, u8>> = if image[0] == 0x01 || (image[0] & 0x03) == 0x02 {
        Some(detoast_seams::detoast_attr_slice::call(
            mcx,
            image,
            slice_start,
            slice_size,
        )?)
    } else {
        None
    };
    let data = inline_payload(sliced.as_deref().unwrap_or(image));

    if data.is_empty() {
        return cstring_to_text(mcx, b"");
    }
    // Validation must stop at the substring's last character: a char cut off
    // by the slice fetch past that point is not diagnosable (varlena.c
    // pg_mbcharcliplen_chars). When slice_size != -1, s1 + l1 - 1 == E - 1.
    let slice_strlen = if slice_size == -1 {
        mbutils_seams::pg_mbstrlen_with_len::call(data)?
    } else {
        pg_mbcharcliplen_chars(data, s1 + l1 - 1)?
    };
    if s1 > slice_strlen {
        return cstring_to_text(mcx, b"");
    }

    let e1 = if l1 > -1 {
        (s1 + l1).min(slice_start + 1 + slice_strlen)
    } else {
        slice_start + 1 + slice_strlen
    };

    let mut p = 0usize;
    for _ in 0..(s1 - 1) {
        p += mbutils_seams::pg_mblen_range::call(&data[p..])? as usize;
    }
    let sstart = p;
    for _ in s1..e1 {
        p += mbutils_seams::pg_mblen_range::call(&data[p..])? as usize;
    }

    cstring_to_text(mcx, &data[sstart..p])
}

// C: pg_mbcharcliplen_chars (varlena.c) — pg_mbcharcliplen with the return
// unit in chars; stops counting (and validating) once `limit` chars are seen.
fn pg_mbcharcliplen_chars(mbstr: &[u8], limit: i32) -> PgResult<i32> {
    debug_assert!(!mbstr.is_empty());
    debug_assert!(limit > 0);
    let mut nch = 0;
    let mut pos = 0usize;
    while pos < mbstr.len() && mbstr[pos] != 0 {
        let l = mbutils_seams::pg_mblen_range::call(&mbstr[pos..])?;
        nch += 1;
        if nch == limit {
            break;
        }
        pos += l as usize;
    }
    Ok(nch)
}

pub fn text_catenate<'mcx>(mcx: Mcx<'mcx>, t1: &[u8], t2: &[u8]) -> PgResult<Varlena<'mcx>> {
    let mut image = image_with_header(mcx, t1.len() + t2.len())?;
    mcx::vec_append_bytes(&mut image, t1)?;
    mcx::vec_append_bytes(&mut image, t2)?;
    Ok(Varlena::from_image(image))
}

#[cold]
#[inline(never)]
fn integer_out_of_range() -> PgError {
    PgError::error("integer out of range")
        .with_sqlstate(types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

// C: text_overlay (SQL standard OVERLAY() as substring + concatenation).
pub fn text_overlay<'mcx>(
    mcx: Mcx<'mcx>,
    t1: &[u8],
    t2: &[u8],
    sp: i32,
    sl: i32,
) -> PgResult<Varlena<'mcx>> {
    if sp <= 0 {
        return Err(negative_substring_len().into());
    }
    let sp_pl_sl = sp.checked_add(sl).ok_or_else(integer_out_of_range)?;
    let s1 = text_substring(mcx, t1, 1, sp - 1, false)?;
    let s2 = text_substring(mcx, t1, sp_pl_sl, -1, true)?;
    let result = text_catenate(mcx, s1.data(), t2)?;
    text_catenate(mcx, result.data(), s2.data())
}

// C: convert_to_base (workhorse for to_bin/to_oct/to_hex); base in 2..=16.
pub fn convert_to_base<'mcx>(mcx: Mcx<'mcx>, mut value: u64, base: u64) -> PgResult<Varlena<'mcx>> {
    debug_assert!(base > 1 && base <= 16);
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut buf = [0u8; u64::BITS as usize];
    let mut i = buf.len();
    loop {
        i -= 1;
        buf[i] = DIGITS[(value % base) as usize];
        value /= base;
        if i == 0 || value == 0 {
            break;
        }
    }
    cstring_to_text(mcx, &buf[i..])
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

#[cold]
#[inline(never)]
fn field_position_zero() -> PgError {
    PgError::error("field position must not be zero")
        .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE)
}

pub struct TextPositionState<'a> {
    str1: &'a [u8],
    str2: &'a [u8],
    is_multibyte_char_in_char: bool,
    nondeterministic: bool,
    collid: Oid,
    greedy: bool,
    last_match: Option<usize>,
    last_match_len: usize,
    refpoint: usize,
    refpos: i32,
    skiptablemask: usize,
    // Only 0..=skiptablemask is written; reads mask into that range (C leaves
    // the tail uninitialized too).
    skiptable: [core::mem::MaybeUninit<i32>; 256],
}

pub fn text_position_setup<'a>(
    t1: &'a [u8],
    t2: &'a [u8],
    collid: Oid,
) -> PgResult<TextPositionState<'a>> {
    check_collation_set(collid)?;
    let nondeterministic = !collation_is_deterministic(collid)?;
    let (len1, len2) = (t1.len(), t2.len());
    debug_assert!(len2 > 0);
    let is_multibyte_char_in_char = mbutils::pg_database_encoding_max_length() != 1
        && mbutils::GetDatabaseEncoding() != wchar::PG_UTF8;

    let mut state = TextPositionState {
        str1: t1,
        str2: t2,
        is_multibyte_char_in_char,
        nondeterministic,
        collid,
        greedy: true,
        last_match: None,
        last_match_len: 0,
        refpoint: 0,
        refpos: 0,
        skiptablemask: 0,
        skiptable: [core::mem::MaybeUninit::uninit(); 256],
    };

    // (Nondeterministic search is substring-by-substring; no B-M-H table.)
    if len1 >= len2 && len2 > 1 && !nondeterministic {
        let searchlength = len1 - len2;
        let skiptablemask: usize = if searchlength < 16 {
            3
        } else if searchlength < 64 {
            7
        } else if searchlength < 128 {
            15
        } else if searchlength < 512 {
            31
        } else if searchlength < 2048 {
            63
        } else if searchlength < 4096 {
            127
        } else {
            255
        };
        state.skiptablemask = skiptablemask;
        for i in 0..=skiptablemask {
            state.skiptable[i] = core::mem::MaybeUninit::new(len2 as i32);
        }
        let last = len2 - 1;
        for i in 0..last {
            state.skiptable[t2[i] as usize & skiptablemask] =
                core::mem::MaybeUninit::new((last - i) as i32);
        }
    }
    Ok(state)
}

// Returns (match offset, matched-substring length): with a nondeterministic
// collation the found substring's length may differ from the needle's.
fn text_position_next_internal(
    state: &TextPositionState<'_>,
    start: usize,
) -> PgResult<Option<(usize, usize)>> {
    let haystack = state.str1;
    let needle = state.str2;
    let needle_len = needle.len();
    debug_assert!(needle_len > 0);

    if state.nondeterministic {
        return text_position_next_nondeterministic(state, start);
    }

    if needle_len == 1 {
        let nchar = needle[0];
        return Ok(haystack[start..]
            .iter()
            .position(|&b| b == nchar)
            .map(|i| (start + i, needle_len)));
    }

    let mask = state.skiptablemask;
    let last = needle_len - 1;
    let mut hptr = start + last;
    while hptr < haystack.len() {
        let mut nptr = last;
        let mut p = hptr;
        while haystack[p] == needle[nptr] {
            if nptr == 0 {
                return Ok(Some((p, needle_len)));
            }
            nptr -= 1;
            p -= 1;
        }
        // SAFETY: the masked index is <= skiptablemask; setup initialized
        // 0..=skiptablemask whenever this arm runs (len1 >= len2 && len2 > 1).
        hptr +=
            unsafe { state.skiptable[haystack[hptr] as usize & mask].assume_init() } as usize;
    }
    Ok(None)
}

// text_position_next_internal nondeterministic arm (varlena.c:1478-1537):
// walk the haystack character-wise; at each position probe every non-empty
// prefix of the remainder for pg_strncoll-equality with the needle; greedy
// mode keeps the longest match at that position.
fn text_position_next_nondeterministic(
    state: &TextPositionState<'_>,
    start: usize,
) -> PgResult<Option<(usize, usize)>> {
    let haystack = state.str1;
    let needle = state.str2;
    let needle_len = needle.len();
    let eq = |cand: &[u8]| -> PgResult<bool> {
        Ok(pg_locale_seams::varstr_cmp_locale::call(state.collid, cand, needle)? == 0)
    };
    let mut hptr = start;
    while hptr < haystack.len() {
        if !state.greedy && haystack.len() - hptr >= needle_len && eq(&haystack[hptr..hptr + needle_len])? {
            return Ok(Some((hptr, needle_len)));
        }
        let mut result: Option<(usize, usize)> = None;
        let mut test_end = hptr;
        loop {
            test_end += mbutils::pg_mblen_range(&haystack[test_end..])? as usize;
            let test_end_c = test_end.min(haystack.len());
            if eq(&haystack[hptr..test_end_c])? {
                result = Some((hptr, test_end_c - hptr));
                if !state.greedy {
                    break;
                }
            }
            if test_end_c >= haystack.len() {
                break;
            }
        }
        if result.is_some() {
            return Ok(result);
        }
        hptr += mbutils::pg_mblen_range(&haystack[hptr..])? as usize;
    }
    Ok(None)
}

pub fn text_position_next(state: &mut TextPositionState<'_>) -> PgResult<bool> {
    let needle_len = state.str2.len();
    if needle_len == 0 {
        return Ok(false);
    }
    let mut start_ptr = match state.last_match {
        Some(m) => m + state.last_match_len,
        None => 0,
    };

    'retry: loop {
        let Some((matchptr, matchlen)) = text_position_next_internal(state, start_ptr)? else {
            return Ok(false);
        };
        if state.is_multibyte_char_in_char && !state.nondeterministic {
            debug_assert!(state.refpoint <= matchptr);
            while state.refpoint < matchptr {
                state.refpoint += mbutils::pg_mblen_range(&state.str1[state.refpoint..])? as usize;
                state.refpos += 1;
                if state.refpoint > matchptr {
                    start_ptr = state.refpoint;
                    continue 'retry;
                }
            }
        }
        state.last_match = Some(matchptr);
        state.last_match_len = matchlen;
        return Ok(true);
    }
}

pub fn text_position_get_match_off(state: &TextPositionState<'_>) -> usize {
    state.last_match.expect("no match recorded")
}

pub fn text_position_get_match_len(state: &TextPositionState<'_>) -> usize {
    state.last_match_len
}

pub fn text_position_get_match_pos(state: &mut TextPositionState<'_>) -> PgResult<i32> {
    let m = state.last_match.expect("no match recorded");
    state.refpos += mbutils::pg_mbstrlen_with_len(&state.str1[state.refpoint..m])?;
    state.refpoint = m;
    Ok(state.refpos + 1)
}

pub fn text_position_reset(state: &mut TextPositionState<'_>) {
    state.last_match = None;
    state.refpoint = 0;
    state.refpos = 0;
}

pub fn text_position(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<i32> {
    check_collation_set(collid)?;
    if t2.is_empty() {
        return Ok(1);
    }
    if t1.len() < t2.len() && collation_is_deterministic(collid)? {
        return Ok(0);
    }
    let mut state = text_position_setup(t1, t2, collid)?;
    state.greedy = false;
    if !text_position_next(&mut state)? {
        return Ok(0);
    }
    text_position_get_match_pos(&mut state)
}

pub fn textpos(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<i32> {
    text_position(t1, t2, collid)
}

// replace_text (varlena.c): replace all occurrences of from_sub with to_sub.
pub fn replace_text<'mcx>(
    mcx: Mcx<'mcx>,
    src: &[u8],
    from_sub: &[u8],
    to_sub: &[u8],
    collid: Oid,
) -> PgResult<Varlena<'mcx>> {
    if src.is_empty() || from_sub.is_empty() {
        return cstring_to_text(mcx, src);
    }

    let mut state = text_position_setup(src, from_sub, collid)?;
    if !text_position_next(&mut state)? {
        return cstring_to_text(mcx, src);
    }
    let mut curr_ptr = text_position_get_match_off(&state);
    let mut start_ptr = 0usize;
    let mut str: Vec<u8> = Vec::new();

    loop {
        str.extend_from_slice(&src[start_ptr..curr_ptr]);
        str.extend_from_slice(to_sub);
        start_ptr = curr_ptr + state.last_match_len;
        if !text_position_next(&mut state)? {
            break;
        }
        curr_ptr = text_position_get_match_off(&state);
    }
    str.extend_from_slice(&src[start_ptr..]);

    cstring_to_text(mcx, &str)
}

pub fn split_part<'mcx>(
    mcx: Mcx<'mcx>,
    inputstring: &[u8],
    fldsep: &[u8],
    fldnum: i32,
    collid: Oid,
) -> PgResult<Varlena<'mcx>> {
    let mut fldnum = fldnum;
    if fldnum == 0 {
        return Err(field_position_zero().into());
    }
    if inputstring.is_empty() {
        return cstring_to_text(mcx, b"");
    }
    if fldsep.is_empty() {
        return if fldnum == 1 || fldnum == -1 {
            cstring_to_text(mcx, inputstring)
        } else {
            cstring_to_text(mcx, b"")
        };
    }

    let mut state = text_position_setup(inputstring, fldsep, collid)?;
    let mut found = text_position_next(&mut state)?;
    if !found {
        return if fldnum == 1 || fldnum == -1 {
            cstring_to_text(mcx, inputstring)
        } else {
            cstring_to_text(mcx, b"")
        };
    }

    if fldnum < 0 {
        let mut numfields = 2i32;
        while text_position_next(&mut state)? {
            numfields += 1;
        }
        if fldnum == -1 {
            let start = text_position_get_match_off(&state) + state.last_match_len;
            return cstring_to_text(mcx, &inputstring[start..]);
        }
        fldnum += numfields + 1;
        if fldnum <= 0 {
            return cstring_to_text(mcx, b"");
        }
        text_position_reset(&mut state);
        found = text_position_next(&mut state)?;
        debug_assert!(found);
    }

    let mut start_ptr = 0usize;
    let mut end_ptr = text_position_get_match_off(&state);
    loop {
        if !found {
            break;
        }
        fldnum -= 1;
        if fldnum <= 0 {
            break;
        }
        start_ptr = end_ptr + state.last_match_len;
        found = text_position_next(&mut state)?;
        if found {
            end_ptr = text_position_get_match_off(&state);
        }
    }

    if fldnum > 0 {
        if fldnum == 1 {
            cstring_to_text(mcx, &inputstring[start_ptr..])
        } else {
            cstring_to_text(mcx, b"")
        }
    } else {
        cstring_to_text(mcx, &inputstring[start_ptr..end_ptr])
    }
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
    regexp_alt::install();
}

// SplitIdentifierString (varlena.c). Owned std strings: cold GUC-list parsing,
// C builds a palloc'd List the caller frees. None is C's `return false`.
pub fn split_identifier_string(
    mcx: Mcx<'_>,
    rawstring: &str,
    separator: u8,
    encoding: wchar::pg_enc,
) -> PgResult<Option<Vec<String>>> {
    use parser_small1::{downcase_truncate_identifier, scanner_isspace, truncate_identifier};

    let s = rawstring.as_bytes();
    let mut namelist: Vec<String> = Vec::new();
    let mut p = 0usize;

    while p < s.len() && scanner_isspace(s[p]) {
        p += 1;
    }
    if p == s.len() {
        return Ok(Some(namelist));
    }

    loop {
        let mut curname: PgVec<'_, u8>;
        if s[p] == b'"' {
            curname = mcx::vec_with_capacity_in(mcx, 0)?;
            let mut q = p + 1;
            loop {
                let Some(rel) = s[q..].iter().position(|&b| b == b'"') else {
                    return Ok(None);
                };
                let endp = q + rel;
                mcx::vec_append_bytes(&mut curname, &s[q..endp])?;
                if s.get(endp + 1) == Some(&b'"') {
                    mcx::vec_append_bytes(&mut curname, b"\"")?;
                    q = endp + 2;
                } else {
                    p = endp + 1;
                    break;
                }
            }
        } else {
            let start = p;
            while p < s.len() && s[p] != separator && !scanner_isspace(s[p]) {
                p += 1;
            }
            if p == start {
                return Ok(None);
            }
            curname = downcase_truncate_identifier(mcx, &s[start..p], false, encoding)?;
        }

        while p < s.len() && scanner_isspace(s[p]) {
            p += 1;
        }

        let done = if p < s.len() && s[p] == separator {
            p += 1;
            while p < s.len() && scanner_isspace(s[p]) {
                p += 1;
            }
            false
        } else if p == s.len() {
            true
        } else {
            return Ok(None);
        };

        truncate_identifier(&mut curname, false, encoding)?;
        namelist.push(String::from_utf8_lossy(&curname).into_owned());

        if done {
            return Ok(Some(namelist));
        }
    }
}

// textToQualifiedNameList (varlena.c); caller detoasts to &str. Owned
// strings, like split_identifier_string (C returns a list of String nodes).
#[allow(non_snake_case)]
pub fn textToQualifiedNameList(mcx: Mcx<'_>, rawname: &str) -> PgResult<Vec<String>> {
    match split_identifier_string(mcx, rawname, b'.', mbutils::GetDatabaseEncoding())? {
        Some(names) if !names.is_empty() => Ok(names),
        _ => Err(Box::new(
            PgError::error("invalid name syntax")
                .with_sqlstate(types_error::ERRCODE_INVALID_NAME),
        )),
    }
}

// SplitGUCList (varlena.c): like SplitIdentifierString but never downcases
// or truncates. None is C's `return false`.
pub fn split_guc_list(rawstring: &str, separator: u8) -> Option<Vec<String>> {
    use parser_small1::scanner_isspace;

    let s = rawstring.as_bytes();
    let mut namelist: Vec<String> = Vec::new();
    let mut p = 0usize;

    while p < s.len() && scanner_isspace(s[p]) {
        p += 1;
    }
    if p == s.len() {
        return Some(namelist);
    }

    loop {
        let mut curname: Vec<u8> = Vec::new();
        if s[p] == b'"' {
            let mut q = p + 1;
            loop {
                let rel = s[q..].iter().position(|&b| b == b'"')?;
                let endp = q + rel;
                curname.extend_from_slice(&s[q..endp]);
                if s.get(endp + 1) == Some(&b'"') {
                    curname.push(b'"');
                    q = endp + 2;
                } else {
                    p = endp + 1;
                    break;
                }
            }
        } else {
            let start = p;
            while p < s.len() && s[p] != separator && !scanner_isspace(s[p]) {
                p += 1;
            }
            if p == start {
                return None;
            }
            curname.extend_from_slice(&s[start..p]);
        }

        while p < s.len() && scanner_isspace(s[p]) {
            p += 1;
        }

        let done = if p < s.len() && s[p] == separator {
            p += 1;
            while p < s.len() && scanner_isspace(s[p]) {
                p += 1;
            }
            false
        } else if p == s.len() {
            true
        } else {
            return None;
        };

        namelist.push(String::from_utf8_lossy(&curname).into_owned());

        if done {
            return Some(namelist);
        }
    }
}

#[track_caller]
#[cold]
fn invalid_surrogate_pair() -> Box<PgError> {
    Box::new(
        PgError::error("invalid Unicode surrogate pair")
            .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR),
    )
}

#[track_caller]
#[cold]
fn invalid_codepoint(unicode: u32) -> Box<PgError> {
    Box::new(
        PgError::error(format!("invalid Unicode code point: {unicode:04X}"))
            .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

#[track_caller]
#[cold]
fn invalid_unicode_escape() -> Box<PgError> {
    Box::new(
        PgError::error("invalid Unicode escape")
            .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR)
            .with_hint("Unicode escapes must be \\XXXX, \\+XXXXXX, \\uXXXX, or \\UXXXXXXXX."),
    )
}

fn hexval_n(s: &[u8], n: usize) -> Option<u32> {
    let mut v = 0u32;
    for &c in s.get(..n)? {
        v = (v << 4) | (c as char).to_digit(16)?;
    }
    Some(v)
}

pub fn unistr<'mcx>(mcx: Mcx<'mcx>, input: &[u8]) -> PgResult<Varlena<'mcx>> {
    use wchar::{
        is_utf16_surrogate_first, is_utf16_surrogate_second, is_valid_unicode_codepoint,
        surrogate_pair_to_codepoint,
    };
    let mut out: PgVec<'mcx, u8> = image_with_header(mcx, 0)?;
    let mut s = input;
    let mut pair_first: u32 = 0;

    while let Some(&c0) = s.first() {
        if c0 == b'\\' {
            let (unicode, adv) = if s.get(1) == Some(&b'\\') {
                if pair_first != 0 {
                    return Err(invalid_surrogate_pair());
                }
                mcx::vec_append_bytes(&mut out, b"\\")?;
                s = &s[2..];
                continue;
            } else if let Some(u) = hexval_n(&s[1..], 4) {
                (u, 5)
            } else if s.get(1) == Some(&b'u') && hexval_n(&s[2..], 4).is_some() {
                (hexval_n(&s[2..], 4).expect("checked"), 6)
            } else if s.get(1) == Some(&b'+') && hexval_n(&s[2..], 6).is_some() {
                (hexval_n(&s[2..], 6).expect("checked"), 8)
            } else if s.get(1) == Some(&b'U') && hexval_n(&s[2..], 8).is_some() {
                (hexval_n(&s[2..], 8).expect("checked"), 10)
            } else {
                return Err(invalid_unicode_escape());
            };

            if !is_valid_unicode_codepoint(unicode) {
                return Err(invalid_codepoint(unicode));
            }
            let mut unicode = unicode;
            if pair_first != 0 {
                if is_utf16_surrogate_second(unicode) {
                    unicode = surrogate_pair_to_codepoint(pair_first, unicode);
                    pair_first = 0;
                } else {
                    return Err(invalid_surrogate_pair());
                }
            } else if is_utf16_surrogate_second(unicode) {
                return Err(invalid_surrogate_pair());
            }

            if is_utf16_surrogate_first(unicode) {
                pair_first = unicode;
            } else {
                let cbuf = mbutils::pg_unicode_to_server(mcx, unicode)?;
                mcx::vec_append_bytes(&mut out, &cbuf)?;
            }
            s = &s[adv..];
        } else {
            if pair_first != 0 {
                return Err(invalid_surrogate_pair());
            }
            mcx::vec_append_bytes(&mut out, &[c0])?;
            s = &s[1..];
        }
    }

    if pair_first != 0 {
        return Err(invalid_surrogate_pair());
    }
    Ok(Varlena::from_image(out))
}
