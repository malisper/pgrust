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
pub mod concat_format;
pub mod levenshtein;
pub mod string_agg;
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

pub(crate) fn check_collation_set_pub(collid: Oid) -> PgResult<()> {
    check_collation_set(collid)
}

pub(crate) fn collation_is_c_known_pub(collid: Oid) -> bool {
    collation_is_c_known(collid)
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
    let (slice_size, mut l1);
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
                if e < 1 {
                    return cstring_to_text(mcx, b"");
                }
                l1 = e - s1;
                match e.checked_mul(eml) {
                    Some(sz) => slice_size = sz,
                    None => {
                        slice_size = -1;
                        l1 = -1;
                    }
                }
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
    let slice_strlen = mbutils_seams::pg_mbstrlen_with_len::call(data);
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
    if !collation_is_deterministic(collid)? {
        panic!(
            "text_position_setup (varlena.c): nondeterministic-collation search unported (pg_strncoll)"
        );
    }
    let (len1, len2) = (t1.len(), t2.len());
    debug_assert!(len2 > 0);
    let is_multibyte_char_in_char = mbutils::pg_database_encoding_max_length() != 1
        && mbutils::GetDatabaseEncoding() != wchar::PG_UTF8;

    let mut state = TextPositionState {
        str1: t1,
        str2: t2,
        is_multibyte_char_in_char,
        last_match: None,
        last_match_len: 0,
        refpoint: 0,
        refpos: 0,
        skiptablemask: 0,
        skiptable: [core::mem::MaybeUninit::uninit(); 256],
    };

    if len1 >= len2 && len2 > 1 {
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

fn text_position_next_internal(state: &TextPositionState<'_>, start: usize) -> Option<usize> {
    let haystack = state.str1;
    let needle = state.str2;
    let needle_len = needle.len();
    debug_assert!(needle_len > 0);

    if needle_len == 1 {
        let nchar = needle[0];
        return haystack[start..].iter().position(|&b| b == nchar).map(|i| start + i);
    }

    let mask = state.skiptablemask;
    let last = needle_len - 1;
    let mut hptr = start + last;
    while hptr < haystack.len() {
        let mut nptr = last;
        let mut p = hptr;
        while haystack[p] == needle[nptr] {
            if nptr == 0 {
                return Some(p);
            }
            nptr -= 1;
            p -= 1;
        }
        // SAFETY: the masked index is <= skiptablemask; setup initialized
        // 0..=skiptablemask whenever this arm runs (len1 >= len2 && len2 > 1).
        hptr +=
            unsafe { state.skiptable[haystack[hptr] as usize & mask].assume_init() } as usize;
    }
    None
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
        let Some(matchptr) = text_position_next_internal(state, start_ptr) else {
            return Ok(false);
        };
        if state.is_multibyte_char_in_char {
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
        state.last_match_len = needle_len;
        return Ok(true);
    }
}

pub fn text_position_get_match_off(state: &TextPositionState<'_>) -> usize {
    state.last_match.expect("no match recorded")
}

pub fn text_position_get_match_len(state: &TextPositionState<'_>) -> usize {
    state.last_match_len
}

pub fn text_position_get_match_pos(state: &mut TextPositionState<'_>) -> i32 {
    let m = state.last_match.expect("no match recorded");
    state.refpos += mbutils::pg_mbstrlen_with_len(&state.str1[state.refpoint..m]);
    state.refpoint = m;
    state.refpos + 1
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
    if !text_position_next(&mut state)? {
        return Ok(0);
    }
    Ok(text_position_get_match_pos(&mut state))
}

pub fn textpos(t1: &[u8], t2: &[u8], collid: Oid) -> PgResult<i32> {
    text_position(t1, t2, collid)
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
