//! Little-endian wire plumbing: bounded cursor reads, append writes, crc32c,
//! and the PG varlena 4B-U header helpers (spec §1: LE throughout; every
//! decode bounds-validated with typed errors).

use crate::{FormatError, FormatResult};

/// One-shot crc32c digest (C-parity Castagnoli via the crc32c port crate).
pub fn crc32c(bytes: &[u8]) -> u32 {
    ::crc32c::fin_crc32c(::crc32c::pg_comp_crc32c(::crc32c::CRC32C_INIT, bytes))
}

// ---------------------------------------------------------------------------
// append writers (encode side; staging buffers per the format-crate precedent)
// ---------------------------------------------------------------------------

pub fn put_u8(b: &mut Vec<u8>, v: u8) {
    b.push(v);
}
pub fn put_u16(b: &mut Vec<u8>, v: u16) {
    b.extend_from_slice(&v.to_le_bytes());
}
pub fn put_u32(b: &mut Vec<u8>, v: u32) {
    b.extend_from_slice(&v.to_le_bytes());
}
pub fn put_u64(b: &mut Vec<u8>, v: u64) {
    b.extend_from_slice(&v.to_le_bytes());
}
pub fn put_i64(b: &mut Vec<u8>, v: i64) {
    b.extend_from_slice(&v.to_le_bytes());
}
pub fn put_i128(b: &mut Vec<u8>, v: i128) {
    b.extend_from_slice(&v.to_le_bytes());
}
pub fn put_bytes(b: &mut Vec<u8>, v: &[u8]) {
    b.extend_from_slice(v);
}
/// Zero-pad `b` to the next multiple of `align` (entry-alignment law, spec §1).
pub fn pad_to(b: &mut Vec<u8>, align: usize) {
    let rem = b.len() % align;
    if rem != 0 {
        b.resize(b.len() + (align - rem), 0);
    }
}

// ---------------------------------------------------------------------------
// bounded cursor (decode side)
// ---------------------------------------------------------------------------

/// Bounded LE reader. Every getter carries the structure name so truncation
/// errors are self-locating.
pub struct Cur<'a> {
    b: &'a [u8],
    off: usize,
}

impl<'a> Cur<'a> {
    pub fn new(b: &'a [u8]) -> Cur<'a> {
        Cur { b, off: 0 }
    }
    pub fn off(&self) -> usize {
        self.off
    }
    pub fn remaining(&self) -> usize {
        self.b.len() - self.off
    }
    pub fn take(&mut self, n: usize, at: &'static str) -> FormatResult<&'a [u8]> {
        if self.remaining() < n {
            return Err(FormatError::Truncated { at });
        }
        let s = &self.b[self.off..self.off + n];
        self.off += n;
        Ok(s)
    }
    pub fn u8(&mut self, at: &'static str) -> FormatResult<u8> {
        Ok(self.take(1, at)?[0])
    }
    pub fn u16(&mut self, at: &'static str) -> FormatResult<u16> {
        Ok(u16::from_le_bytes(
            self.take(2, at)?.try_into().expect("len 2"),
        ))
    }
    pub fn u32(&mut self, at: &'static str) -> FormatResult<u32> {
        Ok(u32::from_le_bytes(
            self.take(4, at)?.try_into().expect("len 4"),
        ))
    }
    pub fn u64(&mut self, at: &'static str) -> FormatResult<u64> {
        Ok(u64::from_le_bytes(
            self.take(8, at)?.try_into().expect("len 8"),
        ))
    }
    pub fn i64(&mut self, at: &'static str) -> FormatResult<i64> {
        Ok(i64::from_le_bytes(
            self.take(8, at)?.try_into().expect("len 8"),
        ))
    }
    pub fn i128(&mut self, at: &'static str) -> FormatResult<i128> {
        Ok(i128::from_le_bytes(
            self.take(16, at)?.try_into().expect("len 16"),
        ))
    }
}

// ---------------------------------------------------------------------------
// varlena 4B-U helpers (spec §1: the StrView invariant-4 shape)
// ---------------------------------------------------------------------------

/// PG 4-byte varlena header size.
pub const VARHDRSZ: u32 = 4;

/// Build the 4B-uncompressed varlena header word for `payload_len` payload
/// bytes (LE bit layout: low 2 bits 00, size = total length << 2, size
/// includes the header itself — C's `SET_VARSIZE`).
pub fn varlena_header_4b_u(payload_len: u32) -> u32 {
    debug_assert!(payload_len <= (u32::MAX >> 2) - VARHDRSZ);
    (payload_len + VARHDRSZ) << 2
}

/// Parse a 4B-U header word back to the payload length; typed refusal if the
/// flag bits say it is not a plain 4B-U header.
pub fn varlena_4b_u_payload_len(header: u32, at: &'static str) -> FormatResult<u32> {
    if header & 0b11 != 0 {
        return Err(FormatError::Corrupt { at });
    }
    let total = header >> 2;
    if total < VARHDRSZ {
        return Err(FormatError::Corrupt { at });
    }
    Ok(total - VARHDRSZ)
}

/// Append one varlena-shaped entry (header + payload) at 8-byte entry
/// alignment (spec §1); returns the entry's buffer-relative offset (of the
/// header word).
pub fn put_varlena_entry(b: &mut Vec<u8>, payload: &[u8]) -> u64 {
    pad_to(b, 8);
    let off = b.len() as u64;
    put_u32(b, varlena_header_4b_u(payload.len() as u32));
    put_bytes(b, payload);
    off
}

/// Read the varlena-shaped entry at `off`: returns (whole image incl. header,
/// payload). Bounds-validated.
pub fn varlena_entry_at<'a>(
    b: &'a [u8],
    off: usize,
    at: &'static str,
) -> FormatResult<(&'a [u8], &'a [u8])> {
    if off + 4 > b.len() {
        return Err(FormatError::Bounds { at });
    }
    let header = u32::from_le_bytes(b[off..off + 4].try_into().expect("len 4"));
    let payload_len = varlena_4b_u_payload_len(header, at)? as usize;
    let end = off + 4 + payload_len;
    if end > b.len() {
        return Err(FormatError::Bounds { at });
    }
    Ok((&b[off..end], &b[off + 4..end]))
}
