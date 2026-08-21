//! FFOR_INTERLEAVE — FastLanes-style transposed FFOR, the ELECTED
//! narrow-width tier for FUSED consumers only (spec §4 id 3; S4 verdict:
//! wins 1.16–1.53× fused at 25–47% fewer bits, loses 1.9–2.7× on flat
//! decode against BYTE_FOR — the election tables in `election.rs` price
//! exactly that, so this tier engages only where the writer's caller asks
//! for the fused posture).
//!
//! Payload layout (authority: the vendored `alp::bitpack` interleave layout,
//! M3-B provenance — value `i` in lane `i % 16`, packed words
//! lane-interleaved): per 1024-value frame —
//!
//! ```text
//! frame := base i64 | bit_width u32 | reserved u32
//!        | packed (bit_width × 16 u64, the vendored pack() image)
//! ```
//!
//! A short tail frame pads its missing slots with delta 0 (the base — the
//! spec §6.6 canonical placeholder) and still stores the full packed image
//! (the interleave is a fixed 1024-slot shape by construction).

use crate::section::{frame_base, frame_start, open_section, payload_region};
use alp::bitpack;
use pgrc2_format::abi::{
    refuse_decode_codes, refuse_dict_handle, validity_from_ctx, CodecVtable, DecodeOut,
    EncodeInput, GranuleEncoder, KernelCtx, KernelKey, Selection, ValidityVerdict,
};
use pgrc2_format::class::CLASS_BYVAL;
use pgrc2_format::enc::EncodingId;
use pgrc2_format::geom::{FRAME_VALUES, GRANULE_ROWS};
use pgrc2_format::meta::{MetaAnswer, MetaProbe};
use pgrc2_format::part::{OverflowSink, StreamSectionWriter};
use pgrc2_format::{FormatError, FormatResult};

pub(crate) const FRAME_HDR_LEN: usize = 16; // base(8) + bit_width(4) + reserved(4)
const VEC: usize = FRAME_VALUES as usize;

pub(crate) fn bits_for(range: u64) -> u32 {
    64 - range.leading_zeros()
}

/// Exact payload bytes for one granule (the election's size arm).
pub fn granule_payload_bytes(input: &EncodeInput<'_>, signed: bool) -> usize {
    let rows = input.rows as usize;
    let mut total = 0usize;
    let mut f0 = 0usize;
    while f0 < rows {
        let n = (rows - f0).min(VEC);
        let (_, bw) = frame_base_bits(input, f0, n, signed);
        total += FRAME_HDR_LEN + bitpack::packed_words(bw) * 8;
        f0 += n;
    }
    total
}

fn frame_base_bits(input: &EncodeInput<'_>, f0: usize, n: usize, signed: bool) -> (u64, u32) {
    let mut any = false;
    let (mut min_s, mut max_s) = (i64::MAX, i64::MIN);
    let (mut min_u, mut max_u) = (u64::MAX, u64::MIN);
    for r in f0..f0 + n {
        if !input.valid(r as u32) {
            continue;
        }
        any = true;
        let d = input.datums[r];
        min_s = min_s.min(d as i64);
        max_s = max_s.max(d as i64);
        min_u = min_u.min(d);
        max_u = max_u.max(d);
    }
    if !any {
        return (0, 0);
    }
    if signed {
        (
            min_s as u64,
            bits_for((max_s as u64).wrapping_sub(min_s as u64)),
        )
    } else {
        (min_u, bits_for(max_u - min_u))
    }
}

#[derive(Default)]
pub struct FforEncoder {
    pub signed: bool,
    /// SEAL-FUSION: the election's per-frame facts (base = refw, bits =
    /// `bits_for(range)`); None = re-reduce per frame (pre-fusion path).
    pub carry: Option<crate::bytefor::IntCarryCursor>,
}

impl GranuleEncoder for FforEncoder {
    fn key(&self) -> KernelKey {
        KernelKey {
            encoding: EncodingId::FforInterleave.as_u16(),
            class: CLASS_BYVAL,
            width: 0,
        }
    }

    fn encode_granule(
        &mut self,
        input: &EncodeInput<'_>,
        w: &mut StreamSectionWriter<'_>,
        _ovf: &mut OverflowSink<'_>,
    ) -> FormatResult<()> {
        let rows = input.rows as usize;
        if input.datums.len() < rows {
            return Err(FormatError::EncodeContract {
                detail: "datums shorter than rows",
            });
        }
        let mut deltas = [0u64; VEC];
        let mut packed = [0u64; 64 * bitpack::LANES];
        let mut f0 = 0usize;
        while f0 < rows {
            let n = (rows - f0).min(VEC);
            let (base, bw) = match self.carry.as_mut() {
                Some(c) => {
                    let f = c.take()?;
                    // `frame_base_bits` semantics carried: all-null = (0, 0).
                    if f.any {
                        (f.refw, bits_for(f.range))
                    } else {
                        (0, 0)
                    }
                }
                None => frame_base_bits(input, f0, n, self.signed),
            };
            for i in 0..VEC {
                let r = f0 + i;
                deltas[i] = if i < n && input.valid(r as u32) {
                    input.datums[r].wrapping_sub(base)
                } else {
                    0 // null / tail placeholder (spec §6.6)
                };
            }
            let nwords = bitpack::packed_words(bw);
            bitpack::pack(&deltas, bw, &mut packed[..nwords]);
            w.begin_frame();
            let buf = w.payload();
            buf.extend_from_slice(&(base as i64).to_le_bytes());
            buf.extend_from_slice(&bw.to_le_bytes());
            buf.extend_from_slice(&0u32.to_le_bytes());
            for &word in &packed[..nwords] {
                buf.extend_from_slice(&word.to_le_bytes());
            }
            f0 += n;
        }
        w.end_granule(input.rows);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// decode
// ---------------------------------------------------------------------------

struct Frame<'a> {
    base: u64,
    bw: u32,
    words: &'a [u8],
}

fn open_frame<'a>(payload: &'a [u8], fs: usize) -> FormatResult<Frame<'a>> {
    let hdr = payload
        .get(fs..fs + FRAME_HDR_LEN)
        .ok_or(FormatError::Bounds {
            at: "ffor frame header",
        })?;
    let base = u64::from_le_bytes(hdr[..8].try_into().expect("len 8"));
    let bw = u32::from_le_bytes(hdr[8..12].try_into().expect("len 4"));
    if bw > 64 {
        return Err(FormatError::Corrupt {
            at: "ffor bit width",
        });
    }
    let nbytes = bitpack::packed_words(bw) * 8;
    let words = payload
        .get(fs + FRAME_HDR_LEN..fs + FRAME_HDR_LEN + nbytes)
        .ok_or(FormatError::Bounds {
            at: "ffor frame payload",
        })?;
    Ok(Frame { base, bw, words })
}

/// Word-scratch for one unpack: the vendored layout's max packed image.
/// Granule-scoped (gate-3 shape, M3 exit §2.3): callers allocate ONCE per
/// granule so the 8 KiB zero-init is amortized over every frame, and full
/// frames unpack STRAIGHT into the caller's output slice — no intermediate
/// vector buffer, no extra 8 KiB copy per frame.
type WordScratch = [u64; 64 * bitpack::LANES];

fn unpack_frame(f: &Frame<'_>, words: &mut WordScratch, out: &mut [u64; VEC]) {
    let nwords = bitpack::packed_words(f.bw);
    for (slot, ch) in words[..nwords].iter_mut().zip(f.words.chunks_exact(8)) {
        *slot = u64::from_le_bytes(ch.try_into().expect("len 8"));
    }
    bitpack::unpack(&words[..nwords], f.bw, out);
    for v in out.iter_mut() {
        *v = f.base.wrapping_add(*v);
    }
}

fn ff_dec_full(ctx: &KernelCtx<'_>, out: &mut DecodeOut<'_>) -> FormatResult<u32> {
    let hdr = open_section(ctx.bytes, EncodingId::FforInterleave.as_u16())?;
    let payload = payload_region(&hdr, ctx.bytes)?;
    let fbase = frame_base(&hdr, ctx.bytes, ctx.granule_in_extent)?;
    if out.datums.len() < ctx.values as usize {
        return Err(FormatError::Bounds {
            at: "decode_full out",
        });
    }
    let mut words: WordScratch = [0; 64 * bitpack::LANES];
    let mut r = 0usize;
    let mut f = 0u32;
    while r < ctx.values as usize {
        let n = (ctx.values as usize - r).min(VEC);
        let fs = frame_start(ctx.frame_table, fbase + f)?;
        let frame = open_frame(payload, fs)?;
        if n == VEC {
            let dst: &mut [u64; VEC] = (&mut out.datums[r..r + VEC]).try_into().expect("len VEC");
            unpack_frame(&frame, &mut words, dst);
        } else {
            // Tail frame: the packed image is a fixed 1024-slot shape, so
            // unpack to a scratch vector and copy the real prefix.
            let mut buf = [0u64; VEC];
            unpack_frame(&frame, &mut words, &mut buf);
            out.datums[r..r + n].copy_from_slice(&buf[..n]);
        }
        r += n;
        f += 1;
    }
    Ok(ctx.values)
}

fn ff_dec_sel(
    ctx: &KernelCtx<'_>,
    sel: &Selection<'_>,
    out: &mut DecodeOut<'_>,
) -> FormatResult<u32> {
    let hdr = open_section(ctx.bytes, EncodingId::FforInterleave.as_u16())?;
    let payload = payload_region(&hdr, ctx.bytes)?;
    let fbase = frame_base(&hdr, ctx.bytes, ctx.granule_in_extent)?;
    if out.datums.len() < sel.rows.len() {
        return Err(FormatError::Bounds {
            at: "decode_sel out",
        });
    }
    let mut words: WordScratch = [0; 64 * bitpack::LANES];
    let mut buf = [0u64; VEC];
    let mut cur_frame = u32::MAX;
    for (i, &r16) in sel.rows.iter().enumerate() {
        let r = r16 as u32;
        if r >= ctx.values {
            return Err(FormatError::Bounds {
                at: "decode_sel row",
            });
        }
        let f = r / FRAME_VALUES;
        if f != cur_frame {
            let fs = frame_start(ctx.frame_table, fbase + f)?;
            let frame = open_frame(payload, fs)?;
            unpack_frame(&frame, &mut words, &mut buf);
            cur_frame = f;
        }
        out.datums[i] = buf[(r % FRAME_VALUES) as usize];
    }
    Ok(sel.rows.len() as u32)
}

fn ff_meta(ctx: &KernelCtx<'_>, probe: &MetaProbe) -> FormatResult<MetaAnswer> {
    Ok(match probe {
        MetaProbe::RowCount => MetaAnswer::Count(ctx.rows as u64),
        MetaProbe::NonNullCount => {
            let mut scratch = [0u64; (GRANULE_ROWS as usize).div_ceil(64)];
            match validity_from_ctx(ctx, &mut scratch)? {
                ValidityVerdict::AllValid => MetaAnswer::Count(ctx.rows as u64),
                ValidityVerdict::Mixed { nonnull } => MetaAnswer::Count(nonnull as u64),
            }
        }
        _ => MetaAnswer::Absent,
    })
}

fn ff_validity(ctx: &KernelCtx<'_>, out: &mut [u64]) -> FormatResult<ValidityVerdict> {
    validity_from_ctx(ctx, out)
}

pub static VT_FFOR: CodecVtable = CodecVtable {
    key: KernelKey {
        encoding: EncodingId::FforInterleave as u16,
        class: CLASS_BYVAL,
        width: 0,
    },
    decode_full: ff_dec_full,
    decode_sel: ff_dec_sel,
    decode_codes: refuse_decode_codes,
    dict_handle: refuse_dict_handle,
    meta_probe: ff_meta,
    validity: ff_validity,
};
