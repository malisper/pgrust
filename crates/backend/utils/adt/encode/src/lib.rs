//! encode.c: encode(bytea,text)->text and decode(text,bytea)->bytea over the
//! hex/base64/escape codecs. Hex reuses varlena's shared C hex lane; base64
//! and escape are transcribed C-exact here. Codec dispatch is a closed enum
//! (rule 4), not C's per-name fn-pointer table.

pub mod builtins;
#[cfg(test)]
mod tests;

use datum::Varlena;
use mcx::{Mcx, PgVec};
use types_error::{
    PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED,
};
use varlena::image_with_header;

const VARHDRSZ: u64 = datum::varlena::VARHDRSZ as u64;
const MAX_ALLOC_SIZE: u64 = 0x3fff_ffff;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Codec {
    Hex,
    Base64,
    Escape,
}

// C: TextDatumGetCString stops the name at the first embedded NUL.
fn name_cstr(name: &[u8]) -> &[u8] {
    match name.iter().position(|&b| b == 0) {
        Some(n) => &name[..n],
        None => name,
    }
}

// C: pg_strcasecmp — C-locale ASCII fold (only 'A'..='Z').
fn ascii_ci_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|(&x, &y)| x.to_ascii_lowercase() == y.to_ascii_lowercase())
}

pub fn pg_find_encoding(name: &[u8]) -> Option<Codec> {
    let name = name_cstr(name);
    if ascii_ci_eq(name, b"hex") {
        Some(Codec::Hex)
    } else if ascii_ci_eq(name, b"base64") {
        Some(Codec::Base64)
    } else if ascii_ci_eq(name, b"escape") {
        Some(Codec::Escape)
    } else {
        None
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn unrecognized_encoding(name: &[u8]) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "unrecognized encoding: \"{}\"",
            String::from_utf8_lossy(name_cstr(name))
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn too_large(which: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("result of {which} conversion is too large"))
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
    )
}

pub fn binary_encode<'mcx>(mcx: Mcx<'mcx>, data: &[u8], name: &[u8]) -> PgResult<Varlena<'mcx>> {
    let Some(codec) = pg_find_encoding(name) else {
        return Err(unrecognized_encoding(name));
    };
    let resultlen = codec.encode_len(data);
    if resultlen > MAX_ALLOC_SIZE - VARHDRSZ {
        return Err(too_large("encoding"));
    }
    let mut image = image_with_header(mcx, resultlen as usize)?;
    codec.encode(data, &mut image);
    // C makes the estimate-too-small case FATAL — we've trodden on memory.
    assert!(
        (image.len() as u64 - VARHDRSZ) <= resultlen,
        "encode overflow - estimate too small"
    );
    Ok(Varlena::from_image(image))
}

pub fn binary_decode<'mcx>(mcx: Mcx<'mcx>, data: &[u8], name: &[u8]) -> PgResult<Varlena<'mcx>> {
    let Some(codec) = pg_find_encoding(name) else {
        return Err(unrecognized_encoding(name));
    };
    let resultlen = codec.decode_len(data)?;
    if resultlen > MAX_ALLOC_SIZE - VARHDRSZ {
        return Err(too_large("decoding"));
    }
    let mut image = image_with_header(mcx, resultlen as usize)?;
    codec.decode(data, &mut image)?;
    assert!(
        (image.len() as u64 - VARHDRSZ) <= resultlen,
        "decode overflow - estimate too small"
    );
    Ok(Varlena::from_image(image))
}

impl Codec {
    pub fn encode_len(self, data: &[u8]) -> u64 {
        match self {
            Codec::Hex => (data.len() as u64) << 1,
            Codec::Base64 => b64_enc_len(data.len()),
            Codec::Escape => esc_enc_len(data),
        }
    }

    pub fn decode_len(self, data: &[u8]) -> PgResult<u64> {
        match self {
            Codec::Hex => Ok((data.len() as u64) >> 1),
            Codec::Base64 => Ok(((data.len() as u64) * 3) >> 2),
            Codec::Escape => esc_dec_len(data),
        }
    }

    pub fn encode(self, data: &[u8], out: &mut PgVec<'_, u8>) {
        match self {
            Codec::Hex => varlena::bytea::hex_encode_into(data, out),
            Codec::Base64 => b64_encode(data, out),
            Codec::Escape => esc_encode(data, out),
        }
    }

    pub fn decode(self, data: &[u8], out: &mut PgVec<'_, u8>) -> PgResult<()> {
        match self {
            Codec::Hex => varlena::bytea::hex_decode_into(data, None, out).map(|_| ()),
            Codec::Base64 => b64_decode(data, out),
            Codec::Escape => esc_decode(data, out),
        }
    }
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

// C's b64lookup widened to 256 (high half -1): raw-byte index, no 127 guard.
const B64LOOKUP: [i8; 256] = {
    let mut t = [-1i8; 256];
    let mut i = 0;
    while i < 64 {
        t[B64[i] as usize] = i as i8;
        i += 1;
    }
    t
};

// C: 3 bytes -> 4 chars, a linefeed after each 76 output chars (upper bound).
fn b64_enc_len(srclen: usize) -> u64 {
    let s = srclen as u64;
    (s + 2) / 3 * 4 + s / (76 * 3 / 4)
}

fn b64_encode(src: &[u8], out: &mut PgVec<'_, u8>) {
    let old = out.len();
    assert!(out.capacity() - old >= b64_enc_len(src.len()) as usize);
    let mut pos: i32 = 2;
    let mut buf: u32 = 0;
    let mut linelen: usize = 0;
    // SAFETY: output is bounded by b64_enc_len — 4 chars per started 3-byte
    // group plus one LF per 57 input bytes — asserted against spare capacity;
    // set_len covers exactly the bytes written through p.
    unsafe {
        let base = out.as_mut_ptr().add(old);
        let mut p = base;
        for &c in src {
            buf |= (c as u32) << (pos << 3);
            pos -= 1;
            if pos < 0 {
                p.write(B64[((buf >> 18) & 0x3f) as usize]);
                p.add(1).write(B64[((buf >> 12) & 0x3f) as usize]);
                p.add(2).write(B64[((buf >> 6) & 0x3f) as usize]);
                p.add(3).write(B64[(buf & 0x3f) as usize]);
                p = p.add(4);
                linelen += 4;
                pos = 2;
                buf = 0;
            }
            if linelen >= 76 {
                p.write(b'\n');
                p = p.add(1);
                linelen = 0;
            }
        }
        if pos != 2 {
            p.write(B64[((buf >> 18) & 0x3f) as usize]);
            p.add(1).write(B64[((buf >> 12) & 0x3f) as usize]);
            p.add(2).write(if pos == 0 {
                B64[((buf >> 6) & 0x3f) as usize]
            } else {
                b'='
            });
            p.add(3).write(b'=');
            p = p.add(4);
        }
        out.set_len(old + p.offset_from(base) as usize);
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn b64_unexpected_eq() -> Box<PgError> {
    Box::new(
        PgError::error("unexpected \"=\" while decoding base64 sequence")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

#[cold]
#[inline(never)]
fn b64_invalid_symbol(s: &[u8]) -> PgResult<Box<PgError>> {
    let n = (mbutils_seams::pg_mblen_range::call(s)? as usize).min(s.len());
    Ok(Box::new(
        PgError::error(format!(
            "invalid symbol \"{}\" found while decoding base64 sequence",
            String::from_utf8_lossy(&s[..n])
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
    ))
}

#[track_caller]
#[cold]
#[inline(never)]
fn b64_invalid_end() -> Box<PgError> {
    Box::new(
        PgError::error("invalid base64 end sequence")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .with_hint("Input data is missing padding, is truncated, or is otherwise corrupted."),
    )
}

fn b64_decode(src: &[u8], out: &mut PgVec<'_, u8>) -> PgResult<()> {
    let old = out.len();
    let spare = out.capacity() - old;
    let mut written = 0usize;
    let mut buf: u32 = 0;
    let mut pos: i32 = 0;
    let mut end: i32 = 0;
    let mut i = 0usize;
    while i < src.len() {
        // 4 independent lookups per triple (one-symbol shift-or is latency-
        // bound on V2); negative lookups defer to the scalar arm — C-exact.
        if pos == 0 && end == 0 {
            while i + 4 <= src.len() {
                let b0 = B64LOOKUP[src[i] as usize] as i32;
                let b1 = B64LOOKUP[src[i + 1] as usize] as i32;
                let b2 = B64LOOKUP[src[i + 2] as usize] as i32;
                let b3 = B64LOOKUP[src[i + 3] as usize] as i32;
                if (b0 | b1 | b2 | b3) < 0 {
                    break;
                }
                assert!(written + 3 <= spare);
                let v = ((b0 as u32) << 18) | ((b1 as u32) << 12) | ((b2 as u32) << 6) | b3 as u32;
                // SAFETY: 3 bytes at old+written, within the asserted spare.
                unsafe {
                    let p = out.as_mut_ptr().add(old + written);
                    p.write((v >> 16) as u8);
                    p.add(1).write((v >> 8) as u8);
                    p.add(2).write(v as u8);
                }
                written += 3;
                i += 4;
            }
            if i >= src.len() {
                break;
            }
        }
        let c = src[i];
        i += 1;
        if c == b' ' || c == b'\t' || c == b'\n' || c == b'\r' {
            continue;
        }
        let b: i32;
        if c == b'=' {
            if end == 0 {
                if pos == 2 {
                    end = 1;
                } else if pos == 3 {
                    end = 2;
                } else {
                    return Err(b64_unexpected_eq());
                }
            }
            b = 0;
        } else {
            let bb = B64LOOKUP[c as usize] as i32;
            if bb < 0 {
                return Err(b64_invalid_symbol(&src[i - 1..])?);
            }
            b = bb;
        }
        buf = (buf << 6).wrapping_add(b as u32);
        pos += 1;
        if pos == 4 {
            assert!(written + 3 <= spare);
            // SAFETY: this quad writes at most 3 bytes at old+written, within
            // the asserted spare capacity past old.
            unsafe {
                let p = out.as_mut_ptr().add(old + written);
                p.write(((buf >> 16) & 255) as u8);
                written += 1;
                if end == 0 || end > 1 {
                    p.add(1).write(((buf >> 8) & 255) as u8);
                    written += 1;
                }
                if end == 0 || end > 2 {
                    p.add(2).write((buf & 255) as u8);
                    written += 1;
                }
            }
            buf = 0;
            pos = 0;
        }
    }
    if pos != 0 {
        return Err(b64_invalid_end());
    }
    // SAFETY: the first `written` bytes past old were initialized above.
    unsafe { out.set_len(old + written) };
    Ok(())
}

#[track_caller]
#[cold]
#[inline(never)]
fn esc_invalid_bytea() -> Box<PgError> {
    Box::new(
        PgError::error("invalid input syntax for type bytea")
            .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION),
    )
}

// Pub for proofs/bytea-varbit (Kani C-equivalence harness).
pub fn esc_enc_len(src: &[u8]) -> u64 {
    let mut len = 0u64;
    for &c in src {
        if c == 0 || (c & 0x80) != 0 {
            len += 4;
        } else if c == b'\\' {
            len += 2;
        } else {
            len += 1;
        }
    }
    len
}

fn esc_encode(src: &[u8], out: &mut PgVec<'_, u8>) {
    let old = out.len();
    let spare = out.capacity() - old;
    assert!(spare >= esc_enc_len(src) as usize);
    // SAFETY: the spare capacity past old is valid (possibly uninitialized)
    // memory; the body initializes exactly the first `written` bytes of it,
    // which set_len then covers.
    unsafe {
        let dst = core::slice::from_raw_parts_mut(
            out.as_mut_ptr().add(old).cast::<core::mem::MaybeUninit<u8>>(),
            spare,
        );
        let written = esc_encode_body(src, dst);
        out.set_len(old + written);
    }
}

// Pure slice core factored out of esc_encode for proofs/bytea-varbit (Kani
// C-equivalence harness). Caller must supply dst.len() >= esc_enc_len(src).
pub fn esc_encode_body(src: &[u8], dst: &mut [core::mem::MaybeUninit<u8>]) -> usize {
    debug_assert!(dst.len() as u64 >= esc_enc_len(src));
    // SAFETY: each byte emits the same 1/2/4 bytes esc_enc_len counted for it,
    // within the dst length the caller guarantees.
    unsafe {
        let base = dst.as_mut_ptr().cast::<u8>();
        let mut p = base;
        for &c in src {
            if c == 0 || (c & 0x80) != 0 {
                p.write(b'\\');
                p.add(1).write(b'0' + (c >> 6));
                p.add(2).write(b'0' + ((c >> 3) & 7));
                p.add(3).write(b'0' + (c & 7));
                p = p.add(4);
            } else if c == b'\\' {
                p.write(b'\\');
                p.add(1).write(b'\\');
                p = p.add(2);
            } else {
                p.write(c);
                p = p.add(1);
            }
        }
        p.offset_from(base) as usize
    }
}

// Pub for proofs/bytea-varbit (Kani C-equivalence harness).
pub fn esc_dec_len(src: &[u8]) -> PgResult<u64> {
    let n = src.len();
    let mut i = 0usize;
    let mut len = 0u64;
    while i < n {
        if src[i] != b'\\' {
            i += 1;
        } else if i + 3 < n
            && (b'0'..=b'3').contains(&src[i + 1])
            && (b'0'..=b'7').contains(&src[i + 2])
            && (b'0'..=b'7').contains(&src[i + 3])
        {
            i += 4;
        } else if i + 1 < n && src[i + 1] == b'\\' {
            i += 2;
        } else {
            return Err(esc_invalid_bytea());
        }
        len += 1;
    }
    Ok(len)
}

fn esc_decode(src: &[u8], out: &mut PgVec<'_, u8>) -> PgResult<()> {
    let old = out.len();
    let spare = out.capacity() - old;
    // Revalidates (same checks, same error) to bound the raw writes below.
    assert!(spare >= esc_dec_len(src)? as usize);
    // SAFETY: the spare capacity past old is valid (possibly uninitialized)
    // memory; the body initializes exactly the first `written` bytes of it,
    // which set_len then covers.
    unsafe {
        let dst = core::slice::from_raw_parts_mut(
            out.as_mut_ptr().add(old).cast::<core::mem::MaybeUninit<u8>>(),
            spare,
        );
        let written = esc_decode_body(src, dst)?;
        out.set_len(old + written);
    }
    Ok(())
}

// Pure slice core factored out of esc_decode for proofs/bytea-varbit (Kani
// C-equivalence harness). Caller must supply dst.len() >= esc_dec_len(src).
pub fn esc_decode_body(src: &[u8], dst: &mut [core::mem::MaybeUninit<u8>]) -> PgResult<usize> {
    let n = src.len();
    let mut written = 0usize;
    let mut i = 0usize;
    while i < n {
        let val;
        if src[i] != b'\\' {
            val = src[i];
            i += 1;
        } else if i + 3 < n
            && (b'0'..=b'3').contains(&src[i + 1])
            && (b'0'..=b'7').contains(&src[i + 2])
            && (b'0'..=b'7').contains(&src[i + 3])
        {
            val = ((src[i + 1] - b'0') << 6) | ((src[i + 2] - b'0') << 3) | (src[i + 3] - b'0');
            i += 4;
        } else if i + 1 < n && src[i + 1] == b'\\' {
            val = b'\\';
            i += 2;
        } else {
            return Err(esc_invalid_bytea());
        }
        // SAFETY: one output byte per unit esc_dec_len counted, within the
        // dst length the caller guarantees.
        unsafe { dst.as_mut_ptr().cast::<u8>().add(written).write(val) };
        written += 1;
    }
    Ok(written)
}
