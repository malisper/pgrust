//! bytea I/O + comparison (memcmp, no collation). The `\x` hex codec is
//! encode.c's hex lane, carried here until backend-utils-adt-encode lands.

use datum::Varlena;
use mcx::{Mcx, PgVec};
use stringinfo::StringInfo;
use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_ARRAY_SUBSCRIPT_ERROR,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_SUBSTRING_ERROR,
};

use crate::{image_with_header, varstrfastcmp_c, VARHDRSZ};

pub const HEXTBL: &[u8; 16] = b"0123456789abcdef";

// C encode.c hextbl[512]: both output bytes for each input byte, one 2-byte store.
static HEXTBL2: [u8; 512] = {
    let mut t = [0u8; 512];
    let mut b = 0usize;
    while b < 256 {
        t[2 * b] = HEXTBL[b >> 4];
        t[2 * b + 1] = HEXTBL[b & 0xf];
        b += 1;
    }
    t
};

// C encode.c hexlookup via get_hex: digits + both cases, -1 otherwise
// (widened to 256 entries to drop C's c < 127 guard).
static HEXLOOKUP: [i8; 256] = {
    let mut t = [-1i8; 256];
    let mut c = 0usize;
    while c < 256 {
        t[c] = match c as u8 {
            b'0'..=b'9' => (c as u8 - b'0') as i8,
            b'a'..=b'f' => (c as u8 - b'a' + 10) as i8,
            b'A'..=b'F' => (c as u8 - b'A' + 10) as i8,
            _ => -1,
        };
        c += 1;
    }
    t
};

#[inline]
fn get_hex(c: u8) -> i8 {
    HEXLOOKUP[c as usize]
}

#[cold]
#[inline(never)]
fn invalid_bytea_input() -> PgError {
    PgError::error("invalid input syntax for type bytea")
        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
}

// C: encode.c error path renders the offending mbchar (pg_mblen_range).
#[cold]
#[inline(never)]
fn invalid_hex_digit(s: &[u8]) -> PgResult<PgError> {
    let n = (mbutils_seams::pg_mblen_range::call(s)? as usize).min(s.len());
    Ok(
        PgError::error(format!(
            "invalid hexadecimal digit: \"{}\"",
            String::from_utf8_lossy(&s[..n])
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

#[cold]
#[inline(never)]
fn odd_hex_digits() -> PgError {
    PgError::error("invalid hexadecimal data: odd number of digits")
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

// C: encode.c hex_encode — two lowercase nibbles per byte, into reserved space.
pub fn hex_encode_into(src: &[u8], out: &mut PgVec<'_, u8>) {
    let old = out.len();
    assert!(out.capacity() - old >= 2 * src.len());
    // SAFETY: capacity holds 2*src.len() bytes past old (asserted); the loop
    // writes exactly 2 bytes per input byte; set_len covers exactly those.
    unsafe {
        let mut p = out.as_mut_ptr().add(old);
        for &b in src {
            core::ptr::copy_nonoverlapping(HEXTBL2.as_ptr().add(2 * b as usize), p, 2);
            p = p.add(2);
        }
        out.set_len(old + 2 * src.len());
    }
}

// C: encode.c hex_decode_safe — whitespace-skipping, mblen-aware digit error,
// odd-length error, soft-error channel. Shared by byteain and encode's `decode`.
pub fn hex_decode_into(
    src: &[u8],
    mut escontext: Option<&mut SoftErrorContext>,
    out: &mut PgVec<'_, u8>,
) -> PgResult<Option<()>> {
    let old = out.len();
    assert!(out.capacity() - old >= src.len() / 2);
    let mut written = 0usize;
    let mut i = 0usize;
    while i < src.len() {
        let c = src[i];
        if c == b' ' || c == b'\n' || c == b'\t' || c == b'\r' {
            i += 1;
            continue;
        }
        let v1 = get_hex(c);
        if v1 < 0 {
            return ereturn(escontext.as_deref_mut(), None, invalid_hex_digit(&src[i..])?);
        }
        i += 1;
        if i >= src.len() {
            return ereturn(escontext.as_deref_mut(), None, odd_hex_digits());
        }
        let v2 = get_hex(src[i]);
        if v2 < 0 {
            return ereturn(escontext.as_deref_mut(), None, invalid_hex_digit(&src[i..])?);
        }
        i += 1;
        // SAFETY: one output byte per digit pair; pairs <= src.len()/2, within
        // the asserted spare capacity past old.
        unsafe {
            out.as_mut_ptr().add(old + written).write(((v1 as u8) << 4) | v2 as u8);
        }
        written += 1;
    }
    // SAFETY: the first `written` bytes past old were initialized above.
    unsafe { out.set_len(old + written) };
    Ok(Some(()))
}

pub fn byteain<'mcx>(
    mcx: Mcx<'mcx>,
    input: &[u8],
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<Varlena<'mcx>>> {
    if input.first() == Some(&b'\\') && input.get(1) == Some(&b'x') {
        // C: palloc((len-2)/2 + VARHDRSZ) then decode to the actual length.
        let mut image = image_with_header(mcx, (input.len() - 2) / 2)?;
        return match hex_decode_into(&input[2..], escontext.as_deref_mut(), &mut image)? {
            Some(()) => Ok(Some(Varlena::from_image(image))),
            None => Ok(None),
        };
    }

    // Escaped style: C's two passes — count + validate, then decode.
    let mut bc = 0usize;
    let mut i = 0usize;
    while i < input.len() {
        let tp = &input[i..];
        if tp[0] != b'\\' {
            i += 1;
        } else if tp.len() >= 4
            && (b'0'..=b'3').contains(&tp[1])
            && (b'0'..=b'7').contains(&tp[2])
            && (b'0'..=b'7').contains(&tp[3])
        {
            i += 4;
        } else if tp.len() >= 2 && tp[1] == b'\\' {
            i += 2;
        } else {
            return ereturn(escontext.as_deref_mut(), None, invalid_bytea_input());
        }
        bc += 1;
    }

    let mut image = image_with_header(mcx, bc)?;
    let old = image.len();
    // SAFETY: pass one counted exactly bc output bytes for this input and the
    // image was reserved for bc past the header; pass two writes one byte per
    // counted unit; set_len covers exactly those bytes.
    unsafe {
        let mut p = image.as_mut_ptr().add(old);
        let mut i = 0usize;
        while i < input.len() {
            let tp = &input[i..];
            if tp[0] != b'\\' {
                p.write(tp[0]);
                i += 1;
            } else if tp.len() >= 4
                && (b'0'..=b'3').contains(&tp[1])
                && (b'0'..=b'7').contains(&tp[2])
                && (b'0'..=b'7').contains(&tp[3])
            {
                p.write(((tp[1] - b'0') << 6) | ((tp[2] - b'0') << 3) | (tp[3] - b'0'));
                i += 4;
            } else {
                p.write(b'\\');
                i += 2;
            }
            p = p.add(1);
        }
        image.set_len(old + bc);
    }
    Ok(Some(Varlena::from_image(image)))
}

// C: MaxAllocSize guard on the escape-format length count.
const MAX_ALLOC_SIZE: u64 = 0x3fff_ffff;

// Cstring output (incl. NUL) into retained fn_extra scratch (rule 7).
pub fn byteaout_into(v: &[u8], mode: i32, out: &mut Vec<u8>) -> PgResult<()> {
    out.clear();
    if mode == guc_tables::consts::BYTEA_OUTPUT_HEX {
        out.reserve(v.len() * 2 + 3);
        out.push(b'\\');
        out.push(b'x');
        // SAFETY: reserve above covers 2 bytes per input byte past the "\x";
        // the loop writes exactly that; set_len covers exactly those bytes.
        unsafe {
            let mut p = out.as_mut_ptr().add(2);
            for &b in v {
                core::ptr::copy_nonoverlapping(HEXTBL2.as_ptr().add(2 * b as usize), p, 2);
                p = p.add(2);
            }
            out.set_len(2 + 2 * v.len());
        }
    } else if mode == guc_tables::consts::BYTEA_OUTPUT_ESCAPE {
        let mut len: u64 = 1;
        for &c in v {
            len += match c {
                b'\\' => 2,
                0x20..=0x7e => 1,
                _ => 4,
            };
        }
        if len > MAX_ALLOC_SIZE {
            return Err(
                PgError::error("result of bytea output conversion is too large")
                    .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                    .into(),
            );
        }
        out.reserve(len as usize);
        for &c in v {
            match c {
                b'\\' => out.extend_from_slice(b"\\\\"),
                0x20..=0x7e => out.push(c),
                _ => {
                    out.push(b'\\');
                    out.push(b'0' + ((c >> 6) & 0o3));
                    out.push(b'0' + ((c >> 3) & 0o7));
                    out.push(b'0' + (c & 0o7));
                }
            }
        }
    } else {
        return Err(PgError::error(format!("unrecognized \"bytea_output\" setting: {mode}")).into());
    }
    out.push(0);
    Ok(())
}

pub fn bytearecv<'mcx>(mcx: Mcx<'mcx>, buf: &mut StringInfo<'_>) -> PgResult<Varlena<'mcx>> {
    let nbytes = buf.len().saturating_sub(buf.cursor);
    let mut image = image_with_header(mcx, nbytes)?;
    mcx::vec_append_bytes(&mut image, pqformat::pq_getmsgbytes(buf, nbytes)?)?;
    Ok(Varlena::from_image(image))
}

pub fn byteasend<'mcx>(mcx: Mcx<'mcx>, v: &[u8]) -> PgResult<Varlena<'mcx>> {
    // C: "just copy the input".
    let mut image = image_with_header(mcx, v.len())?;
    mcx::vec_append_bytes(&mut image, v)?;
    Ok(Varlena::from_image(image))
}

pub fn byteaoctetlen(v: &[u8]) -> i32 {
    v.len() as i32
}

pub fn bytea_catenate<'mcx>(mcx: Mcx<'mcx>, v1: &[u8], v2: &[u8]) -> PgResult<Varlena<'mcx>> {
    crate::text_catenate(mcx, v1, v2)
}

pub fn byteacmp(v1: &[u8], v2: &[u8]) -> i32 {
    varstrfastcmp_c(v1, v2)
}

pub fn byteaeq(v1: &[u8], v2: &[u8]) -> bool {
    v1.len() == v2.len() && v1 == v2
}

pub fn byteane(v1: &[u8], v2: &[u8]) -> bool {
    !byteaeq(v1, v2)
}

pub fn bytealt(v1: &[u8], v2: &[u8]) -> bool {
    byteacmp(v1, v2) < 0
}

pub fn byteale(v1: &[u8], v2: &[u8]) -> bool {
    byteacmp(v1, v2) <= 0
}

pub fn byteagt(v1: &[u8], v2: &[u8]) -> bool {
    byteacmp(v1, v2) > 0
}

pub fn byteage(v1: &[u8], v2: &[u8]) -> bool {
    byteacmp(v1, v2) >= 0
}

pub fn bytea_larger<'a>(v1: &'a [u8], v2: &'a [u8]) -> &'a [u8] {
    if byteacmp(v1, v2) > 0 {
        v1
    } else {
        v2
    }
}

pub fn bytea_smaller<'a>(v1: &'a [u8], v2: &'a [u8]) -> &'a [u8] {
    if byteacmp(v1, v2) < 0 {
        v1
    } else {
        v2
    }
}

#[cold]
#[inline(never)]
fn index_out_of_range_i32(n: i32, len: i32) -> PgError {
    PgError::error(format!("index {n} out of valid range, 0..{}", len - 1))
        .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
}

#[cold]
#[inline(never)]
fn index_out_of_range_i64(n: i64, hi: i64) -> PgError {
    PgError::error(format!("index {n} out of valid range, 0..{hi}"))
        .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
}

#[cold]
#[inline(never)]
pub(crate) fn negative_substring() -> PgError {
    PgError::error("negative substring length not allowed").with_sqlstate(ERRCODE_SUBSTRING_ERROR)
}

// C: bytea_substring — SQL substring math, then DatumGetByteaPSlice(str, S1-1, L1);
// `image` is the raw argument image (the slice fetch is the toast read path).
pub fn bytea_substring<'mcx>(
    mcx: Mcx<'mcx>,
    image: &[u8],
    s: i32,
    l: i32,
    length_not_specified: bool,
) -> PgResult<Varlena<'mcx>> {
    let s1 = s.max(1);
    let l1: i32 = if length_not_specified {
        -1
    } else if l < 0 {
        return Err(negative_substring().into());
    } else {
        match s.checked_add(l) {
            None => -1,
            Some(e) => {
                if e < 1 {
                    return byteasend(mcx, b"");
                }
                e - s1
            }
        }
    };
    Ok(Varlena::from_image(
        detoast_seams::detoast_attr_slice::call(mcx, image, s1 - 1, l1)?,
    ))
}

// C: byteapos — POSITION(); 1-based, 0 if absent, 1 for the empty pattern.
pub fn byteapos(t1: &[u8], t2: &[u8]) -> i32 {
    let len1 = t1.len() as i32;
    let len2 = t2.len() as i32;
    if len2 <= 0 {
        return 1;
    }
    let px = len1 - len2;
    let n2 = len2 as usize;
    let mut p = 0i32;
    while p <= px {
        let pi = p as usize;
        if t1[pi] == t2[0] && t1[pi..pi + n2] == *t2 {
            return p + 1;
        }
        p += 1;
    }
    0
}

pub fn bytea_get_byte(v: &[u8], n: i32) -> PgResult<i32> {
    let len = v.len() as i32;
    if n < 0 || n >= len {
        return Err(index_out_of_range_i32(n, len).into());
    }
    Ok(v[n as usize] as i32)
}

pub fn bytea_get_bit(v: &[u8], n: i64) -> PgResult<i32> {
    let len = v.len() as i64;
    if n < 0 || n >= len * 8 {
        return Err(index_out_of_range_i64(n, len * 8 - 1).into());
    }
    let byte_no = (n / 8) as usize;
    let bit_no = (n % 8) as u32;
    Ok(((v[byte_no] >> bit_no) & 1) as i32)
}

pub fn bytea_set_byte<'mcx>(
    mcx: Mcx<'mcx>,
    v: &[u8],
    n: i32,
    new_byte: i32,
) -> PgResult<Varlena<'mcx>> {
    let len = v.len() as i32;
    if n < 0 || n >= len {
        return Err(index_out_of_range_i32(n, len).into());
    }
    let mut image = image_with_header(mcx, v.len())?;
    mcx::vec_append_bytes(&mut image, v)?;
    image[VARHDRSZ + n as usize] = new_byte as u8;
    Ok(Varlena::from_image(image))
}

pub fn bytea_set_bit<'mcx>(
    mcx: Mcx<'mcx>,
    v: &[u8],
    n: i64,
    new_bit: i32,
) -> PgResult<Varlena<'mcx>> {
    let len = v.len() as i64;
    if n < 0 || n >= len * 8 {
        return Err(index_out_of_range_i64(n, len * 8 - 1).into());
    }
    let byte_no = (n / 8) as usize;
    let bit_no = (n % 8) as u32;
    if new_bit != 0 && new_bit != 1 {
        return Err(PgError::error("new bit must be 0 or 1")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .into());
    }
    let mut image = image_with_header(mcx, v.len())?;
    mcx::vec_append_bytes(&mut image, v)?;
    let idx = VARHDRSZ + byte_no;
    let old = image[idx];
    image[idx] = if new_bit == 0 {
        old & !(1 << bit_no)
    } else {
        old | (1 << bit_no)
    };
    Ok(Varlena::from_image(image))
}

#[cold]
#[inline(never)]
fn integer_out_of_range() -> PgError {
    PgError::error("integer out of range")
        .with_sqlstate(::types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

// C: byteaoverlay (SQL standard OVERLAY() as substring + concatenation).
pub fn bytea_overlay<'mcx>(
    mcx: Mcx<'mcx>,
    t1: &[u8],
    t2: &[u8],
    sp: i32,
    sl: i32,
) -> PgResult<Varlena<'mcx>> {
    if sp <= 0 {
        return Err(negative_substring().into());
    }
    let sp_pl_sl = sp.checked_add(sl).ok_or_else(integer_out_of_range)?;
    let s1 = bytea_substring(mcx, t1, 1, sp - 1, false)?;
    let s2 = bytea_substring(mcx, t1, sp_pl_sl, -1, true)?;
    let result = bytea_catenate(mcx, s1.data(), t2)?;
    bytea_catenate(mcx, result.data(), s2.data())
}

// C: bytea_bit_count.
pub fn bytea_bit_count(v: &[u8]) -> i64 {
    ::pg_bitutils::pg_popcount(v) as i64
}

#[track_caller]
#[cold]
#[inline(never)]
fn int_out_of_range(type_name: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("{type_name} out of range"))
            .with_sqlstate(::types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
    )
}

// bytea_int2/4/8: big-endian fold of at most size_of bytes.
fn bytea_uint_be(v: &[u8], width: usize, type_name: &str) -> PgResult<u64> {
    if v.len() > width {
        return Err(int_out_of_range(type_name));
    }
    let mut result: u64 = 0;
    for &b in v {
        result = (result << 8) | b as u64;
    }
    Ok(result)
}

pub fn bytea_int2(v: &[u8]) -> PgResult<i16> {
    Ok(bytea_uint_be(v, 2, "smallint")? as u16 as i16)
}

pub fn bytea_int4(v: &[u8]) -> PgResult<i32> {
    Ok(bytea_uint_be(v, 4, "integer")? as u32 as i32)
}

pub fn bytea_int8(v: &[u8]) -> PgResult<i64> {
    Ok(bytea_uint_be(v, 8, "bigint")? as i64)
}

// int2/4/8_bytea alias intNsend: the big-endian byte image.
pub fn int_bytea<'mcx>(mcx: Mcx<'mcx>, be_bytes: &[u8]) -> PgResult<Varlena<'mcx>> {
    let mut image = image_with_header(mcx, be_bytes.len())?;
    mcx::vec_append_bytes(&mut image, be_bytes)?;
    Ok(Varlena::from_image(image))
}

pub fn bytea_reverse<'mcx>(mcx: Mcx<'mcx>, v: &[u8]) -> PgResult<Varlena<'mcx>> {
    let mut image = image_with_header(mcx, v.len())?;
    for &b in v.iter().rev() {
        image.push(b);
    }
    Ok(Varlena::from_image(image))
}
