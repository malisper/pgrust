//! Byte-buffer access layer reproducing the `ts_type.h` macros over an
//! already-detoasted `tsvector` datum held as `&[u8]` / `Vec<u8>`.
//!
//! A `tsvector` datum is laid out exactly as PostgreSQL's `TSVectorData`: a
//! 4-byte varlena header, an `int32 size`, then `size` `WordEntry` records,
//! then the lexeme + position storage. These helpers are the faithful Rust
//! analogues of `VARSIZE`/`SET_VARSIZE`/`ARRPTR`/`STRPTR`/`_POSVECPTR`/
//! `POSDATALEN`/`POSDATAPTR` and the `SHORTALIGN` rounding macro. All reads are
//! bounds-checked; well-formed PostgreSQL datums behave bit-identically to the
//! C macros on little-endian targets.

use tsearch::tsearch::{WordEntry, WordEntryPos, DATAHDRSIZE};

/// `VARHDRSZ` — size of the 4-byte varlena header.
pub const VARHDRSZ: usize = 4;
/// `sizeof(WordEntry)` (a single packed `uint32`).
pub const SIZEOF_WORDENTRY: usize = 4;
/// `sizeof(WordEntryPos)` (`uint16`).
pub const SIZEOF_WEP: usize = 2;
/// `sizeof(uint16)` — the `WordEntryPosVector.npos` header.
pub const SIZEOF_NPOS: usize = 2;

/// `SHORTALIGN(LEN)` (c.h) — round up to the next multiple of 2.
#[inline]
pub fn shortalign(len: usize) -> usize {
    (len + 1) & !1
}

/// `VARSIZE(p)` — total datum size in bytes from the 4-byte varlena header.
#[inline]
pub fn varsize(buf: &[u8]) -> usize {
    let header = u32::from_ne_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if cfg!(target_endian = "big") {
        (header & 0x3FFF_FFFF) as usize
    } else {
        ((header >> 2) & 0x3FFF_FFFF) as usize
    }
}

/// `SET_VARSIZE(p, len)` — write the 4-byte varlena header for `len` bytes.
#[inline]
pub fn set_varsize(buf: &mut [u8], len: usize) {
    let header: u32 = if cfg!(target_endian = "big") {
        len as u32
    } else {
        (len as u32) << 2
    };
    buf[0..4].copy_from_slice(&header.to_ne_bytes());
}

/// Read `TSVectorData.size` (the `WordEntry` count).
#[inline]
pub fn tsv_size(buf: &[u8]) -> i32 {
    i32::from_ne_bytes([buf[4], buf[5], buf[6], buf[7]])
}

/// Write `TSVectorData.size`.
#[inline]
pub fn set_tsv_size(buf: &mut [u8], size: i32) {
    buf[4..8].copy_from_slice(&size.to_ne_bytes());
}

/// `ARRPTR(x)[i]` — read the `i`th [`WordEntry`].
#[inline]
pub fn arrptr(buf: &[u8], i: usize) -> WordEntry {
    let off = DATAHDRSIZE + i * SIZEOF_WORDENTRY;
    let w = u32::from_ne_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]]);
    WordEntry { word: w }
}

/// Write the `i`th [`WordEntry`].
#[inline]
pub fn set_arrptr(buf: &mut [u8], i: usize, entry: WordEntry) {
    let off = DATAHDRSIZE + i * SIZEOF_WORDENTRY;
    buf[off..off + 4].copy_from_slice(&entry.word.to_ne_bytes());
}

/// `STRPTR(x)` — byte offset of the lexeme/position storage area.
#[inline]
pub fn strptr_off(size: i32) -> usize {
    DATAHDRSIZE + (size as usize) * SIZEOF_WORDENTRY
}

/// `_POSVECPTR(x, e)` — byte offset of the `WordEntryPosVector` header that
/// follows lexeme `e`'s string.
#[inline]
pub fn posvecptr_off(size: i32, e: WordEntry) -> usize {
    strptr_off(size) + shortalign((e.pos() + e.len()) as usize)
}

/// `_POSVECPTR(x, e)->npos` — the position count.
#[inline]
pub fn posvec_npos(buf: &[u8], size: i32, e: WordEntry) -> u16 {
    let off = posvecptr_off(size, e);
    u16::from_ne_bytes([buf[off], buf[off + 1]])
}

/// `POSDATALEN(x, e)` — number of positions for entry `e` (0 if `!haspos`).
#[inline]
pub fn posdatalen(buf: &[u8], size: i32, e: WordEntry) -> u16 {
    if e.haspos() != 0 {
        posvec_npos(buf, size, e)
    } else {
        0
    }
}

/// `POSDATAPTR(x, e)[j]` — read the `j`th [`WordEntryPos`] of entry `e`.
#[inline]
pub fn posdataptr(buf: &[u8], size: i32, e: WordEntry, j: usize) -> WordEntryPos {
    let base = posvecptr_off(size, e) + SIZEOF_NPOS;
    let off = base + j * SIZEOF_WEP;
    u16::from_ne_bytes([buf[off], buf[off + 1]])
}

/// The lexeme bytes for entry `e` within tsvector datum `buf` (size `size`).
#[inline]
pub fn lexeme(buf: &[u8], size: i32, e: WordEntry) -> &[u8] {
    let start = strptr_off(size) + e.pos() as usize;
    &buf[start..start + e.len() as usize]
}
