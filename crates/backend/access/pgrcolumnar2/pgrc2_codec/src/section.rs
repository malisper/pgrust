//! Decode-side section plumbing shared by every hot kernel (spec §6.4/§6.5).
//!
//! The reference codec (`pgrc2_format::verbatim`) keeps its equivalents
//! private; the behavior is FROZEN by spec §6.4 (section geometry) and §6.5
//! (child-stream value counts), so this module re-states it for the hot
//! kernels and the differential suites prove both readings agree
//! (`tests/roundtrip.rs` decodes reference-encoded sections through these
//! helpers on every class).
//!
//! Everything here is allocation-free and bounds-validated with typed
//! errors (spec §1: typed refusal, never UB, even after CRC passes).

use pgrc2_format::geom::{FRAMES_PER_GRANULE, FRAME_VALUES, GRANULE_ROWS};
use pgrc2_format::part::{StreamSectionHdr, STREAM_SECTION_HDR_LEN};
use pgrc2_format::{FormatError, FormatResult};

/// The payload region of a section (past the header, before the tables).
pub fn payload_region<'a>(hdr: &StreamSectionHdr, section: &'a [u8]) -> FormatResult<&'a [u8]> {
    let end = if hdr.frame_table_off != 0 {
        hdr.frame_table_off as usize
    } else if hdr.gcount_table_off != 0 {
        hdr.gcount_table_off as usize
    } else {
        section.len()
    };
    if end < STREAM_SECTION_HDR_LEN || end > section.len() {
        return Err(FormatError::Bounds {
            at: "payload region",
        });
    }
    Ok(&section[STREAM_SECTION_HDR_LEN..end])
}

/// Per-granule value count `i` from the gcount table (child streams).
pub fn gcount(hdr: &StreamSectionHdr, section: &[u8], i: u32) -> FormatResult<u32> {
    let off = hdr.gcount_table_off as usize + i as usize * 4;
    let b = section
        .get(off..off + 4)
        .ok_or(FormatError::Bounds { at: "gcount table" })?;
    Ok(u32::from_le_bytes(b.try_into().expect("len 4")))
}

/// Values before granule `g` in this extent (spec §6.5: closed-form for
/// root streams, gcount-table-driven for child streams).
pub fn value_base(hdr: &StreamSectionHdr, section: &[u8], g: u32) -> FormatResult<u64> {
    if hdr.gcount_table_off == 0 {
        return Ok(g as u64 * GRANULE_ROWS as u64);
    }
    let mut sum = 0u64;
    for i in 0..g {
        sum += gcount(hdr, section, i)? as u64;
    }
    Ok(sum)
}

/// Frames before granule `g` in this extent (granule-major frame numbering,
/// at the frame grain the ENCODER marked — spec §6.4's uniform mechanism).
pub fn frame_base(hdr: &StreamSectionHdr, section: &[u8], g: u32) -> FormatResult<u32> {
    // Accumulate in u64 so a crafted gcount table (or granule ordinal) can
    // never wrap the frame index into an in-bounds-but-wrong frame-table slot
    // (spec §1 typed-ref invariant). The true, unwrapped base is then
    // range-checked against the frame-table length (`frame_count`, the exact
    // entry count `StreamSectionHdr::frame_table` materializes): anything past
    // it is refused as corruption — a wrapping u32 sum would instead alias a
    // real slot and serve wrong granule data. Callers add a small per-granule
    // `fbase + f` (f < frames-in-granule) before `frame_start`, whose bounds
    // check validates the final index; a base bounded by `frame_count` keeps
    // that addition in range.
    let base: u64 = if hdr.gcount_table_off == 0 {
        g as u64 * FRAMES_PER_GRANULE as u64
    } else {
        let mut sum = 0u64;
        for i in 0..g {
            sum += gcount(hdr, section, i)?.div_ceil(FRAME_VALUES) as u64;
        }
        sum
    };
    if base > hdr.frame_count as u64 {
        return Err(FormatError::Corrupt {
            at: "frame_base out of range",
        });
    }
    Ok(base as u32)
}

/// Granule-per-frame variant of [`frame_base`] for encodings that mark ONE
/// frame per granule (the ALP family: the vendored self-describing granule
/// frame is the addressing unit — module doc in `alpc.rs`).
pub fn granule_frame_base(hdr: &StreamSectionHdr, _section: &[u8], g: u32) -> FormatResult<u32> {
    if hdr.gcount_table_off != 0 {
        return Err(FormatError::Corrupt {
            at: "granule-framed child stream",
        });
    }
    Ok(g)
}

/// Frame-table entry `i` (payload-relative start), bounds-validated.
pub fn frame_start(frame_table: Option<&[u32]>, i: u32) -> FormatResult<usize> {
    let ft = frame_table.ok_or(FormatError::Corrupt {
        at: "missing frame table",
    })?;
    Ok(*ft
        .get(i as usize)
        .ok_or(FormatError::Bounds { at: "frame index" })? as usize)
}

/// Decode-time header open: validates magic and the expected encoding id.
pub fn open_section(bytes: &[u8], expect_encoding: u16) -> FormatResult<StreamSectionHdr> {
    let hdr = StreamSectionHdr::decode(bytes)?;
    if hdr.encoding != expect_encoding {
        return Err(FormatError::Corrupt {
            at: "section encoding id",
        });
    }
    Ok(hdr)
}

/// Read 1..=8 LE bytes, zero-extended.
#[inline]
pub fn read_le(bytes: &[u8]) -> u64 {
    let mut w = [0u8; 8];
    w[..bytes.len()].copy_from_slice(bytes);
    u64::from_le_bytes(w)
}

/// Width-monomorphized LE load of exactly `W` bytes, zero-extended (the
/// gate-3 kernel idiom: paired with `chunks_exact(W)` this compiles to a
/// flat widening load the autovectorizer turns into NEON `ld1`+`ushll`
/// chains — the old format's proven loop shape, `pgrcolumnar` reader.rs
/// widen loops). `W` is const so the match folds away per instantiation;
/// the dead arms' `unwrap`s never execute. The SB-3 widths (3/5/6/7) take
/// the const-unrolled byte assembly — `W` is a monomorphization constant,
/// so no runtime width dispatch survives into the loop body.
#[inline(always)]
pub fn read_le_w<const W: usize>(chunk: &[u8]) -> u64 {
    match W {
        1 => chunk[0] as u64,
        2 => u16::from_le_bytes(chunk.try_into().expect("len 2")) as u64,
        4 => u32::from_le_bytes(chunk.try_into().expect("len 4")) as u64,
        8 => u64::from_le_bytes(chunk.try_into().expect("len 8")),
        _ => {
            let mut w = 0u64;
            let mut i = 0;
            while i < W {
                w |= (chunk[i] as u64) << (8 * i);
                i += 1;
            }
            w
        }
    }
}

/// Word-class datum extension (spec §6.7): sign-extend iff `signed`.
#[inline]
pub fn extend_word(raw: u64, width: u8, signed: bool) -> u64 {
    if !signed || width == 8 {
        return raw;
    }
    let shift = 64 - width as u32 * 8;
    (((raw << shift) as i64) >> shift) as u64
}

/// Unaligned typed load of a full LE u64 at `off` (the S4 byte-FOR kernel
/// shape: full-word loads, mask by width — never a byte gather loop).
#[inline]
pub fn load_u64_le(bytes: &[u8], off: usize) -> FormatResult<u64> {
    let b = bytes
        .get(off..off + 8)
        .ok_or(FormatError::Bounds { at: "u64 load" })?;
    Ok(u64::from_le_bytes(b.try_into().expect("len 8")))
}

/// Payload bytes (header excluded) of an encode-side varlena datum.
///
/// # Safety
///
/// `datum` must point at a live, valid 4B-U varlena image that outlives the
/// returned slice — the `EncodeInput` pointer-class contract (spec §19.6).
/// This is the crate's single encode-side deref site (the decode faces
/// never dereference datums).
pub unsafe fn varlena_payload<'a>(datum: u64) -> FormatResult<&'a [u8]> {
    let p = datum as *const u8;
    // SAFETY: caller contract — 4 readable header bytes.
    let header =
        u32::from_le_bytes(unsafe { core::slice::from_raw_parts(p, 4).try_into().expect("len 4") });
    let len = pgrc2_format::wire::varlena_4b_u_payload_len(header, "encode varlena")? as usize;
    // SAFETY: caller contract — the image is len + 4 bytes.
    Ok(unsafe { core::slice::from_raw_parts(p.add(4), len) })
}
