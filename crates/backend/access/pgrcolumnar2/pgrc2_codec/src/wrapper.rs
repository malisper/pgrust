//! Wrapper block assembly (spec §6.4; delegated to M3-C by spec §20).
//!
//! A wrapped stream section (`wrapper != 0` in the header) replaces the
//! plain payload region with **per-granule compressed blocks** prefixed by a
//! `u32 × (granule_count + 1)` block-offset table; the frame table then
//! addresses the *uncompressed* payload image and `uncompressed_len` sizes
//! the scratch. Wrappers carry DISK-ONLY semantics (the two-layer law,
//! O-CMP-3(a), ruled 2026-08-10): the offer extends to every election — hot
//! fixed-width included — and wrapped extents decompress to the ENCODED
//! image at extent open (once per part residency, O-CMP-5(a)), so the hot
//! kernels stay wrapper-oblivious and allocation-free and the in-memory
//! tier never sees a wrapper.
//!
//! - [`wrap_section`] rewrites an unwrapped section (as emitted by
//!   `StreamSectionWriter`) into the wrapped form, given the per-granule
//!   payload boundaries the encode driver recorded.
//! - [`unwrap_section`] rebuilds the unwrapped section image in a
//!   caller-owned scratch buffer; kernels then run on the scratch exactly as
//!   on an unwrapped section.
//! - LZ4 (wrapper id 1) is the in-repo block codec (`lz4.rs`); Zstd
//!   (wrapper id 2) is the CMP-A slot-fill over the workspace `zstd` dep
//!   per O-CMP-2(a) — per-granule single-frame blocks at a pinned level, so
//!   wrapped bytes stay a pure function of the input bytes (the
//!   byte-identical-parts law, at a pinned library version). On wasm32 the
//!   zstd arm refuses TYPED (`WrapperUnsupported`) both directions — the C
//!   binding has no wasm build (the v1 reader's documented caveat) and
//!   parity-honest degradation beats a silent wrong answer.
//!
//! The election (`election.rs`) engages a wrapper only under the
//! wrapper-layer ≥20%-win law (O-CMP-4(a)) against the unwrapped bytes;
//! encodings keep the standing ≥10% law.

use crate::lz4;
use pgrc2_format::enc::Wrapper;
use pgrc2_format::part::{StreamSectionHdr, STREAM_SECTION_HDR_LEN, STREAM_SECTION_MAGIC};
use pgrc2_format::{FormatError, FormatResult};

/// The pinned zstd compression level for the wrapper layer. Disk-only layer:
/// the election prices the EXACT wrapped bytes, so the level is a size/CPU
/// trade only — pinned so part bytes stay a deterministic function of input
/// (the level the v1 store pins for its own zstd frames).
const ZSTD_LEVEL: i32 = 3;

/// Whether this build can ENCODE/DECODE the given wrapper arm. The seal
/// election consults this before pricing an arm (a build without the codec
/// never elects it — parity-honest degradation); the decode side needs no
/// probe because [`unwrap_section`] refuses typed on the missing arm.
pub const fn wrapper_available(w: Wrapper) -> bool {
    match w {
        Wrapper::None => false,
        Wrapper::Lz4 => true,
        Wrapper::Zstd => cfg!(not(target_family = "wasm")),
    }
}

/// Compress one granule block under `wrapper`, appended to `out`.
fn compress_block(wrapper: Wrapper, input: &[u8], out: &mut Vec<u8>) -> FormatResult<()> {
    match wrapper {
        Wrapper::Lz4 => {
            lz4::compress(input, out);
            Ok(())
        }
        Wrapper::Zstd => zstd_compress_block(input, out),
        Wrapper::None => Err(FormatError::EncodeContract {
            detail: "compress_block with Wrapper::None",
        }),
    }
}

#[cfg(not(target_family = "wasm"))]
fn zstd_compress_block(input: &[u8], out: &mut Vec<u8>) -> FormatResult<()> {
    // One zstd frame per granule block (`ZSTD_compress`: content size stamped
    // in the frame header; deterministic at a pinned level + library).
    let frame = zstd::bulk::compress(input, ZSTD_LEVEL).map_err(|_| {
        FormatError::EncodeContract {
            detail: "zstd compress",
        }
    })?;
    out.extend_from_slice(&frame);
    Ok(())
}

#[cfg(target_family = "wasm")]
fn zstd_compress_block(_input: &[u8], _out: &mut Vec<u8>) -> FormatResult<()> {
    Err(FormatError::WrapperUnsupported {
        wrapper: Wrapper::Zstd.as_u8(),
    })
}

/// Decompress one zstd granule block into the exact remaining image span;
/// returns the decoded length. Bounds-validated by the zstd frame machinery
/// (a frame larger than `dst` errors); typed errors, never a panic.
#[cfg(not(target_family = "wasm"))]
fn zstd_decompress_block(src: &[u8], dst: &mut [u8]) -> FormatResult<usize> {
    zstd::bulk::decompress_to_buffer(src, dst)
        .map_err(|_| FormatError::Corrupt { at: "zstd block" })
}

#[cfg(target_family = "wasm")]
fn zstd_decompress_block(_src: &[u8], _dst: &mut [u8]) -> FormatResult<usize> {
    Err(FormatError::WrapperUnsupported {
        wrapper: Wrapper::Zstd.as_u8(),
    })
}

/// Byte length of the wrapped form of `section` (exact), or None when the
/// wrapper loses (never smaller). The election consumes this.
pub fn wrapped_len(
    section: &[u8],
    granule_payload_ends: &[u32],
    wrapper: Wrapper,
) -> FormatResult<usize> {
    let mut out = Vec::new();
    wrap_section(section, granule_payload_ends, wrapper, &mut out)?;
    Ok(out.len())
}

/// Rewrite an UNWRAPPED section into the wrapped form (spec §6.4).
///
/// `granule_payload_ends[g]` = payload-relative end offset of granule `g`'s
/// payload bytes (ascending; last entry == payload image length). The frame
/// table and gcount table are carried verbatim — their offsets address the
/// uncompressed image by spec.
pub fn wrap_section(
    section: &[u8],
    granule_payload_ends: &[u32],
    wrapper: Wrapper,
    out: &mut Vec<u8>,
) -> FormatResult<()> {
    match wrapper {
        Wrapper::Lz4 | Wrapper::Zstd => {}
        Wrapper::None => {
            return Err(FormatError::EncodeContract {
                detail: "wrap_section with Wrapper::None",
            })
        }
    }
    let hdr = StreamSectionHdr::decode(section)?;
    if hdr.wrapper != Wrapper::None.as_u8() {
        return Err(FormatError::EncodeContract {
            detail: "wrap_section input already wrapped",
        });
    }
    let payload_end = if hdr.frame_table_off != 0 {
        hdr.frame_table_off as usize
    } else if hdr.gcount_table_off != 0 {
        hdr.gcount_table_off as usize
    } else {
        section.len()
    };
    if payload_end < STREAM_SECTION_HDR_LEN || payload_end > section.len() {
        return Err(FormatError::Bounds {
            at: "payload region",
        });
    }
    let payload = &section[STREAM_SECTION_HDR_LEN..payload_end];
    let tables = &section[payload_end..];
    let nblocks = granule_payload_ends.len();
    if nblocks == 0
        || granule_payload_ends[nblocks - 1] as usize != payload.len()
        || granule_payload_ends.windows(2).any(|w| w[0] > w[1])
    {
        return Err(FormatError::EncodeContract {
            detail: "granule payload boundaries",
        });
    }

    let start = out.len();
    out.resize(start + STREAM_SECTION_HDR_LEN, 0);
    // Block-offset table placeholder: nblocks + 1 u32s, payload-relative.
    let btab_off = out.len();
    out.resize(btab_off + (nblocks + 1) * 4, 0);
    // The payload region (block table + blocks) starts right after the
    // header; block offsets are payload-relative per spec §6.4.
    let payload_region_start = btab_off;
    let mut block_offs: Vec<u32> = Vec::with_capacity(nblocks + 1);
    block_offs.push((out.len() - payload_region_start) as u32);
    let mut g0 = 0usize;
    for &end in granule_payload_ends {
        compress_block(wrapper, &payload[g0..end as usize], out)?;
        block_offs.push((out.len() - payload_region_start) as u32);
        g0 = end as usize;
    }
    for (i, &o) in block_offs.iter().enumerate() {
        out[btab_off + i * 4..btab_off + i * 4 + 4].copy_from_slice(&o.to_le_bytes());
    }
    let frame_table_off = if hdr.frame_table_off != 0 {
        (out.len() - start) as u32
    } else {
        0
    };
    // Frame table then gcount table were contiguous in the source; copy the
    // whole table region and recompute both offsets by their relative order.
    let gcount_delta = if hdr.gcount_table_off != 0 {
        hdr.gcount_table_off - payload_end as u32
    } else {
        0
    };
    let tables_start = (out.len() - start) as u32;
    out.extend_from_slice(tables);
    let gcount_table_off = if hdr.gcount_table_off != 0 {
        tables_start + gcount_delta
    } else {
        0
    };

    // Header: same frozen field order as `StreamSectionHdr` (spec §6.4).
    let h = &mut out[start..start + STREAM_SECTION_HDR_LEN];
    h[0..4].copy_from_slice(&STREAM_SECTION_MAGIC.to_le_bytes());
    h[4..6].copy_from_slice(&hdr.encoding.to_le_bytes());
    h[6] = hdr.width;
    h[7] = wrapper.as_u8();
    h[8..12].copy_from_slice(&hdr.frame_count.to_le_bytes());
    h[12..16].copy_from_slice(&frame_table_off.to_le_bytes());
    h[16..20].copy_from_slice(&gcount_table_off.to_le_bytes());
    h[20..24].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    h[24..28].copy_from_slice(&hdr.value_count.to_le_bytes());
    h[28..32].copy_from_slice(&0u32.to_le_bytes());
    Ok(())
}

/// Rebuild the unwrapped section image from a wrapped one into `scratch`
/// (cleared first). After this, `scratch` is byte-usable by every kernel as
/// `KernelCtx::bytes`. The wrapped section's CRC was already validated by
/// the reader (spec §5.2); the rebuilt image needs no second CRC.
pub fn unwrap_section(section: &[u8], scratch: &mut Vec<u8>) -> FormatResult<()> {
    let hdr = StreamSectionHdr::decode(section)?;
    let wrapper = Wrapper::from_u8(hdr.wrapper)?;
    match wrapper {
        Wrapper::Lz4 | Wrapper::Zstd => {}
        Wrapper::None => {
            return Err(FormatError::Corrupt {
                at: "unwrap of unwrapped section",
            })
        }
    }
    let payload_end = if hdr.frame_table_off != 0 {
        hdr.frame_table_off as usize
    } else if hdr.gcount_table_off != 0 {
        hdr.gcount_table_off as usize
    } else {
        section.len()
    };
    if payload_end < STREAM_SECTION_HDR_LEN || payload_end > section.len() {
        return Err(FormatError::Bounds {
            at: "payload region",
        });
    }
    let wrapped_payload = &section[STREAM_SECTION_HDR_LEN..payload_end];
    let tables = &section[payload_end..];

    // Block-offset table: u32 × (nblocks + 1), payload-relative; the first
    // entry is the table's own length, which yields the block count.
    if wrapped_payload.len() < 4 {
        return Err(FormatError::Truncated {
            at: "wrapper block table",
        });
    }
    let first = u32::from_le_bytes(wrapped_payload[..4].try_into().expect("len 4")) as usize;
    if first < 4 || first % 4 != 0 || first > wrapped_payload.len() {
        return Err(FormatError::Corrupt {
            at: "wrapper block table",
        });
    }
    let nblocks = first / 4 - 1;
    let mut offs = Vec::with_capacity(nblocks + 1);
    for i in 0..=nblocks {
        let b = wrapped_payload
            .get(i * 4..i * 4 + 4)
            .ok_or(FormatError::Truncated {
                at: "wrapper block table",
            })?;
        offs.push(u32::from_le_bytes(b.try_into().expect("len 4")) as usize);
    }
    if offs.windows(2).any(|w| w[0] > w[1])
        || *offs.last().expect("nonempty") > wrapped_payload.len()
    {
        return Err(FormatError::Corrupt {
            at: "wrapper block offsets",
        });
    }

    scratch.clear();
    scratch.resize(
        STREAM_SECTION_HDR_LEN + hdr.uncompressed_len as usize + tables.len(),
        0,
    );
    // Header: unwrapped twin.
    let frame_table_off = if hdr.frame_table_off != 0 {
        STREAM_SECTION_HDR_LEN as u32 + hdr.uncompressed_len
    } else {
        0
    };
    let gcount_table_off = if hdr.gcount_table_off != 0 {
        let tables_start = if hdr.frame_table_off != 0 {
            hdr.frame_table_off
        } else {
            hdr.gcount_table_off
        };
        STREAM_SECTION_HDR_LEN as u32 + hdr.uncompressed_len + (hdr.gcount_table_off - tables_start)
    } else {
        0
    };
    let h = &mut scratch[..STREAM_SECTION_HDR_LEN];
    h[0..4].copy_from_slice(&STREAM_SECTION_MAGIC.to_le_bytes());
    h[4..6].copy_from_slice(&hdr.encoding.to_le_bytes());
    h[6] = hdr.width;
    h[7] = Wrapper::None.as_u8();
    h[8..12].copy_from_slice(&hdr.frame_count.to_le_bytes());
    h[12..16].copy_from_slice(&frame_table_off.to_le_bytes());
    h[16..20].copy_from_slice(&gcount_table_off.to_le_bytes());
    h[20..24].copy_from_slice(&0u32.to_le_bytes());
    h[24..28].copy_from_slice(&hdr.value_count.to_le_bytes());
    h[28..32].copy_from_slice(&0u32.to_le_bytes());

    // Blocks decompress back-to-back into the payload image. Block sizes are
    // not recorded per block (the image is contiguous); blocks and granules
    // are 1:1, blocks tile the image in granule order, and exact fit is
    // enforced on the final cursor. Per-block image length: LZ4 blocks carry
    // no length, so a dry parse (`block_image_len`) recovers it before the
    // copy; zstd frames bound their own output against the remaining span
    // and report the decoded length.
    let img_len = hdr.uncompressed_len as usize;
    let mut d = 0usize;
    for b in 0..nblocks {
        let src = &wrapped_payload[offs[b]..offs[b + 1]];
        let take = match wrapper {
            Wrapper::Lz4 => {
                let take = block_image_len(src)?;
                if d + take > img_len {
                    return Err(FormatError::Corrupt {
                        at: "wrapper image overflow",
                    });
                }
                let dst =
                    &mut scratch[STREAM_SECTION_HDR_LEN + d..STREAM_SECTION_HDR_LEN + d + take];
                lz4::decompress_into(src, dst)?;
                take
            }
            Wrapper::Zstd => {
                let dst =
                    &mut scratch[STREAM_SECTION_HDR_LEN + d..STREAM_SECTION_HDR_LEN + img_len];
                zstd_decompress_block(src, dst)?
            }
            // Adjudicated at entry; kept typed (never a panic on any input).
            Wrapper::None => {
                return Err(FormatError::Corrupt {
                    at: "unwrap of unwrapped section",
                })
            }
        };
        d += take;
    }
    if d != img_len {
        return Err(FormatError::Corrupt {
            at: "wrapper image short",
        });
    }
    let tstart = STREAM_SECTION_HDR_LEN + img_len;
    scratch[tstart..].copy_from_slice(tables);
    Ok(())
}

/// [stack] Decompress ONE §6.4 wrapper block into exactly `dst` — the
/// block-lazy dict-payload serving primitive (RESULTS-FMTLAND §B.3): the
/// caller resolved the block's compressed span from the block-offset table
/// and its uncompressed span from the frame table; a decode that does not
/// fill `dst` exactly is typed corruption. No section header involved.
pub fn unwrap_block_into(wrapper: Wrapper, src: &[u8], dst: &mut [u8]) -> FormatResult<()> {
    let take = match wrapper {
        Wrapper::Lz4 => {
            let take = block_image_len(src)?;
            if take != dst.len() {
                return Err(FormatError::Corrupt {
                    at: "wrapper block image len",
                });
            }
            lz4::decompress_into(src, dst)?;
            take
        }
        Wrapper::Zstd => zstd_decompress_block(src, dst)?,
        Wrapper::None => {
            return Err(FormatError::Corrupt {
                at: "unwrap of unwrapped block",
            })
        }
    };
    if take != dst.len() {
        return Err(FormatError::Corrupt {
            at: "wrapper block image len",
        });
    }
    Ok(())
}

/// Exact decoded length of one LZ4 block (a dry parse: lengths only, no
/// byte copies). Bounds-validated; typed errors.
fn block_image_len(src: &[u8]) -> FormatResult<usize> {
    let mut s = 0usize;
    let mut d = 0usize;
    loop {
        let token = *src
            .get(s)
            .ok_or(FormatError::Truncated { at: "lz4 token" })?;
        s += 1;
        let mut lit = (token >> 4) as usize;
        if lit == 15 {
            loop {
                let b = *src
                    .get(s)
                    .ok_or(FormatError::Truncated { at: "lz4 litlen" })?;
                s += 1;
                lit += b as usize;
                if b != 255 {
                    break;
                }
                if lit > isize::MAX as usize / 2 {
                    return Err(FormatError::Corrupt { at: "lz4 litlen" });
                }
            }
        }
        if s + lit > src.len() {
            return Err(FormatError::Truncated { at: "lz4 literals" });
        }
        s += lit;
        d += lit;
        if s == src.len() {
            return Ok(d);
        }
        s += 2; // offset
        if s > src.len() {
            return Err(FormatError::Truncated { at: "lz4 offset" });
        }
        let mut mlen = (token & 0x0F) as usize + 4;
        if mlen == 19 {
            loop {
                let b = *src
                    .get(s)
                    .ok_or(FormatError::Truncated { at: "lz4 matlen" })?;
                s += 1;
                mlen += b as usize;
                if b != 255 {
                    break;
                }
                if mlen > isize::MAX as usize / 2 {
                    return Err(FormatError::Corrupt { at: "lz4 matlen" });
                }
            }
        }
        d += mlen;
    }
}

// ---------------------------------------------------------------------------
// CMP-F meta-plane envelope (SB-6)
// ---------------------------------------------------------------------------

/// Wrap a META-section body (Stats/Psma/Bloom/NdvRegisters — the CMP-F
/// plane) in the `[raw_len: u32 LE][zstd frame]` envelope. Returns `None`
/// when zstd is unavailable in this build (the wasm parity-honest
/// degradation: the section ships raw, exactly like the value path).
/// The SB-2 ≥20% gate is the CALLER's — this function only builds the
/// candidate envelope.
pub fn meta_wrap_body(raw: &[u8]) -> Option<Vec<u8>> {
    if !wrapper_available(Wrapper::Zstd) || raw.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(4 + raw.len() / 2);
    out.extend_from_slice(&(raw.len() as u32).to_le_bytes());
    zstd_compress_block(raw, &mut out).ok()?;
    Some(out)
}

/// Unwrap a `SECTIONF_META_ZSTD` meta-section envelope back to the raw
/// body. The stored bytes' CRC was already validated by the section reader
/// (spec §5.2); this validates the envelope's own length claim.
pub fn meta_unwrap_body(envelope: &[u8]) -> FormatResult<Vec<u8>> {
    if envelope.len() < 4 {
        return Err(FormatError::Truncated {
            at: "meta envelope raw_len",
        });
    }
    let raw_len = u32::from_le_bytes(envelope[..4].try_into().expect("len 4")) as usize;
    let mut out = vec![0u8; raw_len];
    let n = zstd_decompress_block(&envelope[4..], &mut out)
        .map_err(|_| FormatError::Corrupt {
            at: "meta envelope zstd frame",
        })?;
    if n != raw_len {
        return Err(FormatError::Corrupt {
            at: "meta envelope raw_len claim",
        });
    }
    Ok(out)
}
