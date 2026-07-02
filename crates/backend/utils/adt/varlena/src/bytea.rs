//! bytea I/O + comparison (memcmp, no collation). The `\x` hex codec is
//! encode.c's hex lane, carried here until backend-utils-adt-encode lands.

use datum::Varlena;
use mcx::{Mcx, PgVec};
use stringinfo::StringInfo;
use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
};

use crate::{image_with_header, varstrfastcmp_c};

const HEXTBL: &[u8; 16] = b"0123456789abcdef";

#[inline]
fn hexval(c: u8) -> i8 {
    match c {
        b'0'..=b'9' => (c - b'0') as i8,
        b'a'..=b'f' => (c - b'a' + 10) as i8,
        b'A'..=b'F' => (c - b'A' + 10) as i8,
        _ => -1,
    }
}

#[cold]
#[inline(never)]
fn invalid_bytea_input() -> PgError {
    PgError::error("invalid input syntax for type bytea")
        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
}

#[cold]
#[inline(never)]
fn invalid_hex_digit() -> PgError {
    PgError::error("invalid hexadecimal digit").with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

#[cold]
#[inline(never)]
fn odd_hex_digits() -> PgError {
    PgError::error("invalid hexadecimal data: odd number of digits")
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

// C: hex_decode_safe (encode.c) appending into the reserved-capacity image.
fn hex_decode_append(src: &[u8], out: &mut PgVec<'_, u8>) -> Result<(), PgError> {
    let mut i = 0usize;
    while i < src.len() {
        let c = src[i];
        if c == b' ' || c == b'\n' || c == b'\t' || c == b'\r' {
            i += 1;
            continue;
        }
        let v1 = hexval(c);
        if v1 < 0 {
            return Err(invalid_hex_digit());
        }
        i += 1;
        if i >= src.len() {
            return Err(odd_hex_digits());
        }
        let v2 = hexval(src[i]);
        if v2 < 0 {
            return Err(invalid_hex_digit());
        }
        i += 1;
        out.push(((v1 as u8) << 4) | v2 as u8);
    }
    Ok(())
}

pub fn byteain<'mcx>(
    mcx: Mcx<'mcx>,
    input: &[u8],
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<Varlena<'mcx>>> {
    if input.first() == Some(&b'\\') && input.get(1) == Some(&b'x') {
        // C: palloc((len-2)/2 + VARHDRSZ) then decode to the actual length.
        let mut image = image_with_header(mcx, (input.len() - 2) / 2)?;
        return match hex_decode_append(&input[2..], &mut image) {
            Ok(()) => Ok(Some(Varlena::from_image(image))),
            Err(e) => ereturn(escontext.as_deref_mut(), None, e),
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
    let mut i = 0usize;
    while i < input.len() {
        let tp = &input[i..];
        if tp[0] != b'\\' {
            image.push(tp[0]);
            i += 1;
        } else if tp.len() >= 4
            && (b'0'..=b'3').contains(&tp[1])
            && (b'0'..=b'7').contains(&tp[2])
            && (b'0'..=b'7').contains(&tp[3])
        {
            image.push(((tp[1] - b'0') << 6) | ((tp[2] - b'0') << 3) | (tp[3] - b'0'));
            i += 4;
        } else {
            image.push(b'\\');
            i += 2;
        }
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
        for &b in v {
            // C hex_encode: 2-byte table copy per input byte.
            out.push(HEXTBL[(b >> 4) as usize]);
            out.push(HEXTBL[(b & 0xf) as usize]);
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
