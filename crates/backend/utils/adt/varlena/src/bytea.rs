//! FAMILY: bytea I/O + scalar ops + comparison + int casts.
//!
//! `byteain`/`byteaout`/`bytearecv`/`byteasend`, `byteacat`/`bytea_catenate`,
//! `byteaoctetlen`, `byteaoverlay*`/`bytea_overlay`, `byteapos`,
//! `byteaGetByte`/`byteaGetBit`/`byteaSetByte`/`byteaSetBit`, `bytea_reverse`,
//! `bytea_bit_count`, the bytea relational ops
//! (`byteaeq`/`byteane`/`bytealt`/`byteale`/`byteagt`/`byteage`/`byteacmp`,
//! `bytea_larger`/`bytea_smaller`), and the bytea<->int casts
//! (`bytea_int2`/`int4`/`int8`, `int2_bytea`/`int4_bytea`/`int8_bytea`).
//!
//! `bytea` comparison is always raw `memcmp` + length tiebreak (no
//! collation). Depends on the keystone carrier conventions only.
//!
//! ## Carrier
//!
//! A `bytea` value crosses this family's surface as its **payload bytes**
//! (`&[u8]`, already detoasted by the caller); a freshly built value is a
//! [`PgVec<'mcx, u8>`] charged to the caller's [`Mcx`] (C: `palloc` +
//! `SET_VARSIZE` + `memcpy` of the payload — the 4-byte header is the layered
//! Datum boundary's job).
//!
//! ## External owners (mirror-pg-and-panic until ported)
//!
//! - `backend-utils-adt-encode-seams` — `hex_encode` / `hex_decode_safe`
//!   (encode.c) for the `\x` hex `bytea` I/O paths.
//! - `backend-utils-misc-guc-tables` — the `bytea_output` GUC consulted by
//!   `byteaout` (a real ported global, read directly, not seamed).

#![allow(unused_variables)]

use mcx::{Mcx, PgVec};
use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_ARRAY_SUBSCRIPT_ERROR,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_SUBSTRING_ERROR,
};

use encode_seams as encode;
use ::guc_tables::consts::{BYTEA_OUTPUT_ESCAPE, BYTEA_OUTPUT_HEX};
use ::guc_tables::vars::bytea_output;

/// C: `MaxAllocSize` (memutils.h) — the 1GB-minus-a-header palloc ceiling that
/// `SET_VARSIZE`/`palloc` enforce. Mirrored here for the `byteaout` escape
/// overflow guard.
const MAX_ALLOC_SIZE: u64 = 0x3fff_ffff;

/// C: `byteain(PG_FUNCTION_ARGS)` — parse `\x...` hex or traditional escaped
/// input into a `bytea` payload. `input` is the NUL-terminated C string's
/// bytes (without the trailing NUL).
///
/// C raises `ERRCODE_INVALID_TEXT_REPRESENTATION` for a malformed escape (the
/// hex path's bad-digit/odd-length cases are `ERRCODE_INVALID_PARAMETER_VALUE`).
/// Both go through `escontext` (C's `fcinfo->context`): with a soft-error sink
/// the error is saved and `Ok(None)` is returned (C `ereturn(escontext, 0,
/// ...)` → SQL NULL); with `None` it is a hard `Err`.
pub fn byteain<'mcx>(
    mcx: Mcx<'mcx>,
    input: &[u8],
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // C: if (inputText[0] == '\\' && inputText[1] == 'x') — recognize hex.
    // The cstring contract guarantees a terminating NUL, so reading index 1
    // when index 0 is '\\' is in-bounds (worst case it is the NUL, != 'x').
    if input.first() == Some(&b'\\') && input.get(1) == Some(&b'x') {
        // C: hex_decode_safe(inputText + 2, len - 2, VARDATA(result), escontext).
        // The seam carries `escontext != NULL` as `soft`; a recoverable hex
        // error comes back as the inner `Err`, which we route through this
        // frame's escontext exactly as the escape branch does.
        return match encode::hex_decode_safe::call(mcx, &input[2..], escontext.is_some())? {
            Ok(result) => Ok(Some(result)),
            Err(hexerr) => ereturn(escontext.as_deref_mut(), None, hexerr),
        };
    }

    // Else, the traditional escaped style. First pass: count the result bytes
    // (C scans twice) and validate the escapes.
    let mut bc: usize = 0;
    let mut i = 0usize;
    while i < input.len() {
        let tp = &input[i..];
        if tp[0] != b'\\' {
            i += 1;
        } else if tp.len() >= 4
            && tp[0] == b'\\'
            && (tp[1] >= b'0' && tp[1] <= b'3')
            && (tp[2] >= b'0' && tp[2] <= b'7')
            && (tp[3] >= b'0' && tp[3] <= b'7')
        {
            i += 4;
        } else if tp.len() >= 2 && tp[0] == b'\\' && tp[1] == b'\\' {
            i += 2;
        } else {
            // C: one backslash, not followed by another or valid octal —
            // `ereturn(escontext, (Datum) 0, ...)`.
            return ereturn(escontext.as_deref_mut(), None, invalid_bytea_input());
        }
        bc += 1;
    }

    // C: result = palloc(bc + VARHDRSZ); the carrier is the header-less payload.
    let mut result = ::mcx::vec_with_capacity_in(mcx, bc)?;

    // Second pass: decode.
    let mut i = 0usize;
    while i < input.len() {
        let tp = &input[i..];
        if tp[0] != b'\\' {
            result.push(tp[0]);
            i += 1;
        } else if tp.len() >= 4
            && tp[0] == b'\\'
            && (tp[1] >= b'0' && tp[1] <= b'3')
            && (tp[2] >= b'0' && tp[2] <= b'7')
            && (tp[3] >= b'0' && tp[3] <= b'7')
        {
            // C: bc = VAL(tp[1]); bc <<= 3; bc += VAL(tp[2]); bc <<= 3;
            //    *rp++ = bc + VAL(tp[3]); where VAL(CH) = ((CH) - '0').
            let mut byte: i32 = (tp[1] - b'0') as i32;
            byte <<= 3;
            byte += (tp[2] - b'0') as i32;
            byte <<= 3;
            result.push((byte + (tp[3] - b'0') as i32) as u8);
            i += 4;
        } else if tp.len() >= 2 && tp[0] == b'\\' && tp[1] == b'\\' {
            result.push(b'\\');
            i += 2;
        } else {
            // C: "We should never get here. The first pass should not allow it."
            return Err(invalid_bytea_input());
        }
    }

    Ok(Some(result))
}

/// C: `byteaout(PG_FUNCTION_ARGS)` — render a `bytea` payload as the `\x` hex
/// or escaped form per the `bytea_output` GUC. The returned buffer is the
/// printable C-string bytes including the trailing NUL (C's cstring contract).
pub fn byteaout<'mcx>(mcx: Mcx<'mcx>, v: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let output = bytea_output.read();
    if output == BYTEA_OUTPUT_HEX {
        // C: palloc(len*2 + 2 + 1); *rp++='\\'; *rp++='x'; rp += hex_encode(...).
        let hex = encode::hex_encode::call(mcx, v)?;
        let mut result = ::mcx::vec_with_capacity_in(mcx, hex.len() + 3)?;
        result.push(b'\\');
        result.push(b'x');
        result.extend_from_slice(&hex);
        result.push(0);
        Ok(result)
    } else if output == BYTEA_OUTPUT_ESCAPE {
        // C: len starts at 1 ("empty string has 1 char"); count the encoded size.
        let mut len: u64 = 1;
        for &c in v {
            if c == b'\\' {
                len += 2;
            } else if c < 0x20 || c > 0x7e {
                len += 4;
            } else {
                len += 1;
            }
        }
        // C: if (len > MaxAllocSize) ereport(PROGRAM_LIMIT_EXCEEDED).
        if len > MAX_ALLOC_SIZE {
            return Err(PgError::error(
                "result of bytea output conversion is too large",
            )
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
        }
        // C: rp = result = palloc(len); ... *rp = '\0';
        let mut result = ::mcx::vec_with_capacity_in(mcx, len as usize)?;
        for &c in v {
            if c == b'\\' {
                result.push(b'\\');
                result.push(b'\\');
            } else if c < 0x20 || c > 0x7e {
                // C: octal escape \nnn; DIG(VAL) = ((VAL) + '0').
                let mut val = c as i32;
                let d3 = b'0' + (val & 0o7) as u8;
                val >>= 3;
                let d2 = b'0' + (val & 0o7) as u8;
                val >>= 3;
                let d1 = b'0' + (val & 0o3) as u8;
                result.push(b'\\');
                result.push(d1);
                result.push(d2);
                result.push(d3);
            } else {
                result.push(c);
            }
        }
        result.push(0);
        Ok(result)
    } else {
        // C: elog(ERROR, "unrecognized \"bytea_output\" setting: %d", ...).
        Err(PgError::error(format!(
            "unrecognized \"bytea_output\" setting: {output}"
        )))
    }
}

/// C: `bytearecv(PG_FUNCTION_ARGS)` — read the remaining wire-buffer bytes into
/// a fresh `bytea` payload (C: `nbytes = buf->len - buf->cursor`,
/// `pq_copymsgbytes`). `buf` is exactly those remaining message bytes.
pub fn bytearecv<'mcx>(mcx: Mcx<'mcx>, buf: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    ::mcx::slice_in(mcx, buf)
}

/// C: `byteasend(PG_FUNCTION_ARGS)` — "just copy the input" (C: a verbatim copy
/// of the datum). Returns a copy of the payload charged to `mcx`.
pub fn byteasend<'mcx>(mcx: Mcx<'mcx>, v: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    ::mcx::slice_in(mcx, v)
}

/// C: `byteaoctetlen(PG_FUNCTION_ARGS)` — byte count of a `bytea` (C derives it
/// from `toast_raw_datum_size - VARHDRSZ` without detoasting; the carrier here
/// is the already-detoasted payload, so it is the payload length).
pub fn byteaoctetlen(v: &[u8]) -> PgResult<i32> {
    checked_i32(v.len())
}

/// C: `byteacat(PG_FUNCTION_ARGS)` -> [`bytea_catenate`].
pub fn byteacat<'mcx>(mcx: Mcx<'mcx>, t1: &[u8], t2: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    bytea_catenate(mcx, t1, t2)
}

/// C: `bytea_catenate(bytea *t1, bytea *t2)` (guts of `byteacat`).
pub fn bytea_catenate<'mcx>(mcx: Mcx<'mcx>, t1: &[u8], t2: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // C clamps negative VARSIZE_ANY_EXHDR to 0; a Rust slice length is never
    // negative, so the clamp is a no-op here.
    let len1 = t1.len();
    let len2 = t2.len();
    // C: len = len1 + len2 + VARHDRSZ; result = palloc(len).
    let total = len1.checked_add(len2).ok_or_else(out_of_memory)?;
    checked_i32(total)?;
    let mut out = ::mcx::vec_with_capacity_in(mcx, total)?;
    if len1 > 0 {
        out.extend_from_slice(t1);
    }
    if len2 > 0 {
        out.extend_from_slice(t2);
    }
    Ok(out)
}

/// C: `bytea_substring(Datum str, int S, int L, bool length_not_specified)` —
/// shared substring core for `bytea_substr` and `bytea_overlay`. Mirrors
/// `text_substring()` index math. The carrier is the detoasted payload.
fn bytea_substring<'mcx>(
    mcx: Mcx<'mcx>,
    str: &[u8],
    s: i32,
    l: i32,
    length_not_specified: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    // C: S1 = Max(S, 1);
    let s1 = s.max(1);

    // C: compute L1 (adjusted length).
    let l1: i32;
    if length_not_specified {
        // C: L1 = -1 (grab everything to the end).
        l1 = -1;
    } else if l < 0 {
        // C: SQL99 says to throw an error for E < S, i.e., negative length.
        return Err(PgError::error("negative substring length not allowed")
            .with_sqlstate(ERRCODE_SUBSTRING_ERROR));
    } else if let Some(e) = s.checked_add(l) {
        // C: A zero or negative end position -> zero-length string.
        if e < 1 {
            return ::mcx::vec_with_capacity_in(mcx, 0);
        }
        l1 = e - s1;
    } else {
        // C: S + L overflowed; the substring must run to the end of string.
        l1 = -1;
    }

    // C: DatumGetByteaPSlice(str, S1 - 1, L1) — a 0-based slice of the payload.
    // S1 >= 1, so the 0-based start is S1 - 1 >= 0.
    let start = (s1 - 1) as usize;
    if start >= str.len() {
        // Past the end -> zero-length (C's PSlice does this).
        return ::mcx::vec_with_capacity_in(mcx, 0);
    }
    let avail = str.len() - start;
    // L1 < 0 means "to the end"; otherwise clamp to what's available.
    let take = if l1 < 0 {
        avail
    } else {
        (l1 as usize).min(avail)
    };
    ::mcx::slice_in(mcx, &str[start..start + take])
}

/// C: `bytea_substr(PG_FUNCTION_ARGS)` — 1-based substring with explicit length.
pub fn bytea_substr<'mcx>(
    mcx: Mcx<'mcx>,
    str: &[u8],
    s: i32,
    l: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    bytea_substring(mcx, str, s, l, false)
}

/// C: `bytea_substr_no_len(PG_FUNCTION_ARGS)` — 1-based substring to the end.
pub fn bytea_substr_no_len<'mcx>(mcx: Mcx<'mcx>, str: &[u8], s: i32) -> PgResult<PgVec<'mcx, u8>> {
    bytea_substring(mcx, str, s, -1, true)
}

/// C: `byteaoverlay(PG_FUNCTION_ARGS)` — replace the substring of `t1` starting
/// at `sp` (1-based) of length `sl` with `t2`.
pub fn byteaoverlay<'mcx>(
    mcx: Mcx<'mcx>,
    t1: &[u8],
    t2: &[u8],
    sp: i32,
    sl: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    bytea_overlay(mcx, t1, t2, sp, sl)
}

/// C: `byteaoverlay_no_len(PG_FUNCTION_ARGS)` — `sl` defaults to `length(t2)`.
pub fn byteaoverlay_no_len<'mcx>(
    mcx: Mcx<'mcx>,
    t1: &[u8],
    t2: &[u8],
    sp: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    // C: sl = VARSIZE_ANY_EXHDR(t2);
    let sl = checked_i32(t2.len())?;
    bytea_overlay(mcx, t1, t2, sp, sl)
}

/// C: `bytea_overlay(bytea *t1, bytea *t2, int sp, int sl)` — direct
/// implementation of OVERLAY() per the SQL standard (substring + concat).
fn bytea_overlay<'mcx>(
    mcx: Mcx<'mcx>,
    t1: &[u8],
    t2: &[u8],
    sp: i32,
    sl: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    // C: if (sp <= 0) ereport(SUBSTRING_ERROR, "negative substring length...").
    if sp <= 0 {
        return Err(PgError::error("negative substring length not allowed")
            .with_sqlstate(ERRCODE_SUBSTRING_ERROR));
    }
    // C: if (pg_add_s32_overflow(sp, sl, &sp_pl_sl)) ereport(NUMERIC_VALUE_OUT_OF_RANGE).
    let sp_pl_sl = sp
        .checked_add(sl)
        .ok_or_else(|| PgError::error("integer out of range").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE))?;

    // C: s1 = bytea_substring(t1, 1, sp - 1, false);
    let s1 = bytea_substring(mcx, t1, 1, sp - 1, false)?;
    // C: s2 = bytea_substring(t1, sp_pl_sl, -1, true);
    let s2 = bytea_substring(mcx, t1, sp_pl_sl, -1, true)?;
    // C: result = bytea_catenate(s1, t2); result = bytea_catenate(result, s2);
    let result = bytea_catenate(mcx, &s1, t2)?;
    bytea_catenate(mcx, &result, &s2)
}

/// C: `bytea_bit_count(PG_FUNCTION_ARGS)` — population count (set bits) of a
/// `bytea` payload (C: `pg_popcount`).
pub fn bytea_bit_count(v: &[u8]) -> PgResult<i64> {
    Ok(v.iter().map(|b| b.count_ones() as i64).sum())
}

/// C: `byteapos(PG_FUNCTION_ARGS)` — 1-based position of `t2` within `t1`, or 0
/// if not found; an empty needle returns 1.
pub fn byteapos(t1: &[u8], t2: &[u8]) -> PgResult<i32> {
    let len1 = t1.len();
    let len2 = t2.len();
    // C: if (len2 <= 0) PG_RETURN_INT32(1);
    if len2 == 0 {
        return Ok(1);
    }
    // C: px = (len1 - len2); for (p = 0; p <= px; p++) ...
    if len2 > len1 {
        return Ok(0);
    }
    let px = len1 - len2;
    for p in 0..=px {
        // C: if ((*p2 == *p1) && (memcmp(p1, p2, len2) == 0))
        if t1[p] == t2[0] && t1[p..p + len2] == *t2 {
            return checked_i32(p + 1);
        }
    }
    Ok(0)
}

/// C: `byteaGetByte(PG_FUNCTION_ARGS)` — 0-based byte fetch (the value 0..255).
pub fn bytea_get_byte(v: &[u8], n: i32) -> PgResult<i32> {
    let len = v.len();
    // C: if (n < 0 || n >= len) ereport(ARRAY_SUBSCRIPT_ERROR).
    if n < 0 || (n as usize) >= len {
        return Err(byte_index_out_of_range(n as i64, len as i64 - 1));
    }
    Ok(v[n as usize] as i32)
}

/// C: `byteaGetBit(PG_FUNCTION_ARGS)` — 0-based bit fetch (0 or 1).
pub fn bytea_get_bit(v: &[u8], n: i64) -> PgResult<i32> {
    let len = v.len() as i64;
    // C: if (n < 0 || n >= (int64) len * 8) ereport(ARRAY_SUBSCRIPT_ERROR).
    if n < 0 || n >= len * 8 {
        return Err(byte_index_out_of_range(n, len * 8 - 1));
    }
    let byte_no = (n / 8) as usize;
    let bit_no = (n % 8) as u32;
    let byte = v[byte_no];
    if byte & (1 << bit_no) != 0 {
        Ok(1)
    } else {
        Ok(0)
    }
}

/// C: `byteaSetByte(PG_FUNCTION_ARGS)` — a copy of `v` with the `n`th byte set
/// to the low 8 bits of `newbyte` (C copies the datum first).
pub fn bytea_set_byte<'mcx>(
    mcx: Mcx<'mcx>,
    v: &[u8],
    n: i32,
    newbyte: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    let len = v.len();
    // C: if (n < 0 || n >= len) ereport(ARRAY_SUBSCRIPT_ERROR).
    if n < 0 || (n as usize) >= len {
        return Err(byte_index_out_of_range(n as i64, len as i64 - 1));
    }
    let mut res = ::mcx::slice_in(mcx, v)?;
    res[n as usize] = newbyte as u8;
    Ok(res)
}

/// C: `byteaSetBit(PG_FUNCTION_ARGS)` — a copy of `v` with the `n`th bit set to
/// `newbit` (which must be 0 or 1).
pub fn bytea_set_bit<'mcx>(
    mcx: Mcx<'mcx>,
    v: &[u8],
    n: i64,
    newbit: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    let len = v.len() as i64;
    // C: if (n < 0 || n >= (int64) len * 8) ereport(ARRAY_SUBSCRIPT_ERROR).
    if n < 0 || n >= len * 8 {
        return Err(byte_index_out_of_range(n, len * 8 - 1));
    }
    let byte_no = (n / 8) as usize;
    let bit_no = (n % 8) as u32;
    // C: if (newBit != 0 && newBit != 1) ereport(INVALID_PARAMETER_VALUE).
    if newbit != 0 && newbit != 1 {
        return Err(PgError::error("new bit must be 0 or 1")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }
    let mut res = ::mcx::slice_in(mcx, v)?;
    let old_byte = res[byte_no];
    let new_byte = if newbit == 0 {
        old_byte & !(1 << bit_no)
    } else {
        old_byte | (1 << bit_no)
    };
    res[byte_no] = new_byte;
    Ok(res)
}

/// C: `bytea_reverse(PG_FUNCTION_ARGS)` — the `bytea` payload reversed.
pub fn bytea_reverse<'mcx>(mcx: Mcx<'mcx>, v: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let mut out = ::mcx::vec_with_capacity_in(mcx, v.len())?;
    out.extend(v.iter().rev().copied());
    Ok(out)
}

// ===========================================================================
// Comparison functions (raw memcmp + length tiebreak; no collation).
//
// In C the relational ops use a fast path on toast_raw_datum_size to avoid
// detoasting unequal-length values; the carrier here is the already-detoasted
// payload, so the length is just the payload length (the optimization is moot,
// the result is identical).
// ===========================================================================

/// C: `byteaeq(PG_FUNCTION_ARGS)`.
pub fn byteaeq(a: &[u8], b: &[u8]) -> PgResult<bool> {
    // C: if (len1 != len2) result = false; else memcmp == 0.
    Ok(a == b)
}

/// C: `byteane(PG_FUNCTION_ARGS)`.
pub fn byteane(a: &[u8], b: &[u8]) -> PgResult<bool> {
    Ok(a != b)
}

/// C: `bytealt(PG_FUNCTION_ARGS)`.
pub fn bytealt(a: &[u8], b: &[u8]) -> PgResult<bool> {
    let cmp = memcmp_min(a, b);
    Ok(cmp < 0 || (cmp == 0 && a.len() < b.len()))
}

/// C: `byteale(PG_FUNCTION_ARGS)`.
pub fn byteale(a: &[u8], b: &[u8]) -> PgResult<bool> {
    let cmp = memcmp_min(a, b);
    Ok(cmp < 0 || (cmp == 0 && a.len() <= b.len()))
}

/// C: `byteagt(PG_FUNCTION_ARGS)`.
pub fn byteagt(a: &[u8], b: &[u8]) -> PgResult<bool> {
    let cmp = memcmp_min(a, b);
    Ok(cmp > 0 || (cmp == 0 && a.len() > b.len()))
}

/// C: `byteage(PG_FUNCTION_ARGS)`.
pub fn byteage(a: &[u8], b: &[u8]) -> PgResult<bool> {
    let cmp = memcmp_min(a, b);
    Ok(cmp > 0 || (cmp == 0 && a.len() >= b.len()))
}

/// C: `byteacmp(PG_FUNCTION_ARGS)` — raw `memcmp` + length tiebreak.
pub fn byteacmp(a: &[u8], b: &[u8]) -> PgResult<i32> {
    let mut cmp = memcmp_min(a, b);
    // C: if ((cmp == 0) && (len1 != len2)) cmp = (len1 < len2) ? -1 : 1;
    if cmp == 0 && a.len() != b.len() {
        cmp = if a.len() < b.len() { -1 } else { 1 };
    }
    Ok(cmp)
}

/// C: `bytea_larger(PG_FUNCTION_ARGS)` — returns whichever of `a`/`b` sorts
/// greater (a copy charged to `mcx`).
pub fn bytea_larger<'mcx>(mcx: Mcx<'mcx>, a: &[u8], b: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let cmp = memcmp_min(a, b);
    // C: result = ((cmp > 0) || ((cmp == 0) && (len1 > len2)) ? arg1 : arg2);
    let result = if cmp > 0 || (cmp == 0 && a.len() > b.len()) {
        a
    } else {
        b
    };
    ::mcx::slice_in(mcx, result)
}

/// C: `bytea_smaller(PG_FUNCTION_ARGS)` — returns whichever sorts smaller.
pub fn bytea_smaller<'mcx>(mcx: Mcx<'mcx>, a: &[u8], b: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let cmp = memcmp_min(a, b);
    // C: result = ((cmp < 0) || ((cmp == 0) && (len1 < len2)) ? arg1 : arg2);
    let result = if cmp < 0 || (cmp == 0 && a.len() < b.len()) {
        a
    } else {
        b
    };
    ::mcx::slice_in(mcx, result)
}

// ===========================================================================
// bytea <-> int casts (varlena.c:4139-4234).
//
// bytea -> int decodes big-endian (most significant byte first); int -> bytea
// is int2send/int4send/int8send, which emit the value's big-endian bytes.
// ===========================================================================

/// C: `bytea_int2(PG_FUNCTION_ARGS)` — big-endian decode into an `int2`.
pub fn bytea_int2(v: &[u8]) -> PgResult<i16> {
    let len = v.len();
    // C: if (len > sizeof(result)) ereport(NUMERIC_VALUE_OUT_OF_RANGE, "smallint...").
    if len > 2 {
        return Err(int_out_of_range("smallint"));
    }
    // C: result = 0; for (i..len) { result <<= 8; result |= v[i]; }  (uint16)
    let mut result: u16 = 0;
    for &byte in v.iter().take(len) {
        result = (result << 8) | byte as u16;
    }
    Ok(result as i16)
}

/// C: `bytea_int4(PG_FUNCTION_ARGS)` — big-endian decode into an `int4`.
pub fn bytea_int4(v: &[u8]) -> PgResult<i32> {
    let len = v.len();
    if len > 4 {
        return Err(int_out_of_range("integer"));
    }
    let mut result: u32 = 0;
    for &byte in v.iter().take(len) {
        result = (result << 8) | byte as u32;
    }
    Ok(result as i32)
}

/// C: `bytea_int8(PG_FUNCTION_ARGS)` — big-endian decode into an `int8`.
pub fn bytea_int8(v: &[u8]) -> PgResult<i64> {
    let len = v.len();
    if len > 8 {
        return Err(int_out_of_range("bigint"));
    }
    let mut result: u64 = 0;
    for &byte in v.iter().take(len) {
        result = (result << 8) | byte as u64;
    }
    Ok(result as i64)
}

/// C: `int2_bytea(PG_FUNCTION_ARGS)` — `int2send`: the value's big-endian bytes.
pub fn int2_bytea<'mcx>(mcx: Mcx<'mcx>, val: i16) -> PgResult<PgVec<'mcx, u8>> {
    ::mcx::slice_in(mcx, &val.to_be_bytes())
}

/// C: `int4_bytea(PG_FUNCTION_ARGS)` — `int4send`: the value's big-endian bytes.
pub fn int4_bytea<'mcx>(mcx: Mcx<'mcx>, val: i32) -> PgResult<PgVec<'mcx, u8>> {
    ::mcx::slice_in(mcx, &val.to_be_bytes())
}

/// C: `int8_bytea(PG_FUNCTION_ARGS)` — `int8send`: the value's big-endian bytes.
pub fn int8_bytea<'mcx>(mcx: Mcx<'mcx>, val: i64) -> PgResult<PgVec<'mcx, u8>> {
    ::mcx::slice_in(mcx, &val.to_be_bytes())
}

// ===========================================================================
// Shared helpers.
// ===========================================================================

/// C: `memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2))` — compare
/// the overlapping prefix, returning the sign of the first differing byte.
fn memcmp_min(a: &[u8], b: &[u8]) -> i32 {
    let n = a.len().min(b.len());
    match a[..n].cmp(&b[..n]) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }
}

/// Validate that a `usize` fits the varlena `int32` payload-length domain (what
/// C's `SET_VARSIZE` requires) and narrow it.
fn checked_i32(value: usize) -> PgResult<i32> {
    i32::try_from(value).map_err(|_| {
        PgError::error("requested length too large").with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
    })
}

/// C: `ereport(ERRCODE_INVALID_TEXT_REPRESENTATION, "invalid input syntax for
/// type %s", "bytea")`.
fn invalid_bytea_input() -> PgError {
    PgError::error("invalid input syntax for type bytea")
        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
}

/// C: the `ereport(ERRCODE_ARRAY_SUBSCRIPT_ERROR, "index %d out of valid range,
/// 0..%d")` shared by `byteaGetByte`/`byteaGetBit`/`byteaSetByte`/`byteaSetBit`.
fn byte_index_out_of_range(index: i64, max: i64) -> PgError {
    PgError::error(format!("index {index} out of valid range, 0..{max}"))
        .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
}

/// C: `ereport(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, "%s out of range")` for the
/// over-long `bytea -> int` casts.
fn int_out_of_range(typename: &str) -> PgError {
    PgError::error(format!("{typename} out of range"))
        .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

/// Shared "out of memory" used by the size-overflow guards.
fn out_of_memory() -> PgError {
    PgError::error("out of memory").with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
}
