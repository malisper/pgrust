//! ALP / ALP-RD — float primary-by-election (spec §4 ids 5/6; charter §7:
//! self-describing frames, bit-exact exceptions — every NaN payload, ±0.0
//! and out-of-domain value round-trips exactly; the B1/B2 float laws).
//!
//! Payload layout authority: the vendored `alp::granule` frame (M3-B
//! provenance) — ONE self-describing frame per granule
//! (`scheme u8 | nvectors u8 | nvalues u16 | payload`, layout doc in
//! `crates/_support/alp/src/granule.rs`). The frame table marks one entry
//! per granule (the vendored frame is the addressing unit; spec §6.4's
//! frame table is the uniform mechanism and the marked grain is the
//! encoder's).
//!
//! Encode calls the vendored `alp::granule::encode` (per-granule exact-size
//! election among classic / RD / raw inside the stream — frames
//! self-describe, so mixed granules are native). Decode is THIS crate's
//! allocation-free reimplementation of the frame walk over the vendored
//! layout (spec §20 delegates hot kernels to M3-C; the vendored
//! `decode_frame` requires a `Vec` sink and stays the differential ORACLE —
//! `tests/roundtrip.rs` pins byte-agreement between both readers on every
//! corpus). The decode arithmetic `(d as f64) * 10^f * 10^-e` and its
//! constant tables replicate the vendored `classic.rs`/`constants.rs`
//! evaluation order EXACTLY (each product rounds once; the tables are the
//! same nearest-double literals) — drift is caught by the oracle
//! differential and by `verify_roundtrip` at every encode.
//!
//! Two encoding IDs share this family: the writer stamps ALP_RD (id 6) when
//! the stream's dominant elected scheme is RD, ALP (id 5) otherwise — a
//! stats-visible distinction; both decode identically since frames
//! self-describe.
//!
//! **F32 (SB-5 / CMP-D, lanev4):** the family is complete at the 4-byte
//! width through the vendored `alp::granule32` frames (classic + raw arms,
//! no RD; `alp/src/granule32.rs` is the layout doc) under the SAME laws —
//! decode(encode(x)) reproduces x by `f32::to_bits` equality (every NaN
//! payload, ±0.0, subnormal), exact-bytes pricing, verbatim fallback when
//! the ≥10% election loses. F32 streams stamp ALP (id 5) at CLASS_F32 —
//! ALP_RD never appears at this width (a non-conforming granule stores raw
//! bit images inside the ALP stream; frames self-describe). The v3 typed
//! demotion (`Demotion::FloatClassUnsupported` on F32) is retired.
//!
//! Null slots encode 0.0 (deterministic placeholder, spec §6.6).

use crate::section::{frame_start, granule_frame_base, open_section, payload_region};
use alp::bitpack;
use pgrc2_format::abi::{
    refuse_decode_codes, refuse_dict_handle, validity_from_ctx, CodecVtable, DecodeOut,
    EncodeInput, GranuleEncoder, KernelCtx, KernelKey, Selection, ValidityVerdict,
};
use pgrc2_format::class::{CLASS_F32, CLASS_F64};
use pgrc2_format::enc::EncodingId;
use pgrc2_format::geom::{FRAME_VALUES, GRANULE_ROWS};
use pgrc2_format::meta::{MetaAnswer, MetaProbe};
use pgrc2_format::part::{OverflowSink, StreamSectionWriter};
use pgrc2_format::{FormatError, FormatResult};

const VEC: usize = alp::VECTOR_SIZE; // 1024 == FRAME_VALUES
const GRANULE_VECTORS: usize = alp::granule::GRANULE_VECTORS; // 8
const TAG_ALP: u8 = 0;
const TAG_ALP_RD: u8 = 1;
const TAG_RAW: u8 = 2;
const MAX_EXPONENT: u8 = 18;
/// The f32 profile's exponent ceiling (vendored `classic32.rs`).
const MAX_EXPONENT_F32: u8 = 10;
const MAX_RD_DICT: usize = 8;

/// 10^f as exact i64 (the vendored FACT_ARR values).
const FACT: [i64; 19] = [
    1,
    10,
    100,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
    10_000_000_000,
    100_000_000_000,
    1_000_000_000_000,
    10_000_000_000_000,
    100_000_000_000_000,
    1_000_000_000_000_000,
    10_000_000_000_000_000,
    100_000_000_000_000_000,
    1_000_000_000_000_000_000,
];

/// 10^-e as the same nearest-double literals the vendored FRAC_ARR uses.
const FRAC: [f64; 19] = [
    1.0, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9, 1e-10, 1e-11, 1e-12, 1e-13, 1e-14,
    1e-15, 1e-16, 1e-17, 1e-18,
];

// The F32 arm's decode constants are the SAME f64 tables above (indices
// 0..=MAX_EXPONENT_F32): the vendored `classic32.rs` transform evaluates
// `(d * 10^f) * 10^-e` in f64 and rounds ONCE to f32 — pure-f32
// evaluation has no near-zero-exception (e,f) pairs on decimal data
// (vendored module doc), so the f64 domain is the bit-frozen form of
// record at this width too.

const _: () = {
    assert!(VEC == FRAME_VALUES as usize);
    assert!(GRANULE_VECTORS * VEC == GRANULE_ROWS as usize);
};

// ---------------------------------------------------------------------------
// encode
// ---------------------------------------------------------------------------

/// Exact frame bytes + election report for one granule (the election arm;
/// derived from real encoding so it can never drift — vendored contract).
pub fn analyze_granule(input: &EncodeInput<'_>) -> alp::granule::ElectionReport {
    let vals = granule_f64s(input);
    alp::granule::analyze(&vals)
}

/// Exact election facts AND the encoded frame for one granule (SEAL-FUSION:
/// the election prices by ENCODING once and carries the frame to the seal
/// instead of discarding the search — `analyze` is contractually "always
/// exactly `encode`'s report", so the outcome cannot differ).
pub fn encode_granule_carry(input: &EncodeInput<'_>) -> alp::granule::GranuleEncoded {
    let vals = granule_f64s(input);
    alp::granule::encode(&vals)
}

/// The SB-5 f32 twin of [`encode_granule_carry`].
pub fn encode_granule_f32_carry(input: &EncodeInput<'_>) -> alp::granule32::GranuleEncodedF32 {
    let vals = granule_f32s(input);
    alp::granule32::encode(&vals)
}

/// Carried-frame cursor for the ALP encoders (SEAL-FUSION): the election's
/// encoded frames, consumed one per granule in granule order. Valid ONLY at
/// the election's own granule slicing (the default grain) — the factory
/// (`pgrc2_write::elect::AlpFactory::make_at`) enforces that; the encoder
/// additionally refuses on any nvalues mismatch (typed, never silent).
#[derive(Debug, Clone)]
pub struct AlpCarryCursor {
    pub frames: std::sync::Arc<Vec<Vec<u8>>>,
    /// Next granule ordinal to emit.
    pub next: usize,
}

/// Frame nvalues (LE u16 at bytes 2..4 — the vendored frame header).
fn carry_frame_nvalues(frame: &[u8]) -> FormatResult<u32> {
    let b = frame.get(2..4).ok_or(FormatError::EncodeContract {
        detail: "carried alp frame shorter than its header",
    })?;
    Ok(u16::from_le_bytes(b.try_into().expect("len 2")) as u32)
}

/// Emit one carried frame, validating the granule shape matches the
/// election's (a mismatch is a seal-driver bug — typed refusal, and the
/// mandatory verify_roundtrip behind it would catch any byte drift).
fn emit_carried_frame(
    carry: &mut AlpCarryCursor,
    input: &EncodeInput<'_>,
    w: &mut StreamSectionWriter<'_>,
) -> FormatResult<()> {
    let frame = carry
        .frames
        .get(carry.next)
        .ok_or(FormatError::EncodeContract {
            detail: "alp carry exhausted",
        })?;
    if carry_frame_nvalues(frame)? != input.rows {
        return Err(FormatError::EncodeContract {
            detail: "alp carry granule shape mismatch",
        });
    }
    w.begin_frame();
    w.payload().extend_from_slice(frame);
    w.end_granule(input.rows);
    carry.next += 1;
    Ok(())
}

fn granule_f64s(input: &EncodeInput<'_>) -> Vec<f64> {
    (0..input.rows as usize)
        .map(|r| {
            if input.valid(r as u32) {
                f64::from_bits(input.datums[r])
            } else {
                0.0 // canonical placeholder (spec §6.6)
            }
        })
        .collect()
}

pub struct AlpEncoder {
    /// The id stamped in section headers (ALP or ALP_RD; decode-identical).
    pub encoding: EncodingId,
    /// SEAL-FUSION: the election's carried frames (None = re-encode, the
    /// pre-fusion path — non-default grains and non-seal callers).
    pub carry: Option<AlpCarryCursor>,
}

impl GranuleEncoder for AlpEncoder {
    fn key(&self) -> KernelKey {
        KernelKey {
            encoding: self.encoding.as_u16(),
            class: CLASS_F64,
            width: 0,
        }
    }

    fn encode_granule(
        &mut self,
        input: &EncodeInput<'_>,
        w: &mut StreamSectionWriter<'_>,
        _ovf: &mut OverflowSink<'_>,
    ) -> FormatResult<()> {
        if input.rows as usize > GRANULE_ROWS as usize {
            return Err(FormatError::EncodeContract {
                detail: "granule overflow",
            });
        }
        if input.datums.len() < input.rows as usize {
            return Err(FormatError::EncodeContract {
                detail: "datums shorter than rows",
            });
        }
        if let Some(c) = &mut self.carry {
            return emit_carried_frame(c, input, w);
        }
        let vals = granule_f64s(input);
        let enc = alp::granule::encode(&vals);
        if enc.frames.len() != 1 {
            return Err(FormatError::EncodeContract {
                detail: "one vendored frame per granule",
            });
        }
        w.begin_frame();
        w.payload().extend_from_slice(&enc.frames[0]);
        w.end_granule(input.rows);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// F32 encode (SB-5): vendored granule32 frames, one per granule
// ---------------------------------------------------------------------------

/// Exact frame bytes + election report for one F32 granule (the SB-5
/// election arm; derived from real encoding so it can never drift).
pub fn analyze_granule_f32(input: &EncodeInput<'_>) -> alp::granule32::ElectionReportF32 {
    let vals = granule_f32s(input);
    alp::granule32::analyze(&vals)
}

fn granule_f32s(input: &EncodeInput<'_>) -> Vec<f32> {
    (0..input.rows as usize)
        .map(|r| {
            if input.valid(r as u32) {
                // F32 datum words carry the bit image zero-extended
                // (spec §6.7's unsigned w4 widen — verbhot's F32 law).
                f32::from_bits(input.datums[r] as u32)
            } else {
                0.0 // canonical placeholder (spec §6.6)
            }
        })
        .collect()
}

/// The F32 ALP granule encoder (always stamps ALP, id 5 — no RD arm at
/// this width, module doc).
#[derive(Default)]
pub struct AlpF32Encoder {
    /// SEAL-FUSION: the election's carried frames (None = re-encode).
    pub carry: Option<AlpCarryCursor>,
}

impl GranuleEncoder for AlpF32Encoder {
    fn key(&self) -> KernelKey {
        KernelKey {
            encoding: EncodingId::Alp.as_u16(),
            class: CLASS_F32,
            width: 0,
        }
    }

    fn encode_granule(
        &mut self,
        input: &EncodeInput<'_>,
        w: &mut StreamSectionWriter<'_>,
        _ovf: &mut OverflowSink<'_>,
    ) -> FormatResult<()> {
        if input.rows as usize > GRANULE_ROWS as usize {
            return Err(FormatError::EncodeContract {
                detail: "granule overflow",
            });
        }
        if input.datums.len() < input.rows as usize {
            return Err(FormatError::EncodeContract {
                detail: "datums shorter than rows",
            });
        }
        if let Some(c) = &mut self.carry {
            return emit_carried_frame(c, input, w);
        }
        let vals = granule_f32s(input);
        let enc = alp::granule32::encode(&vals);
        if enc.frames.len() != 1 {
            return Err(FormatError::EncodeContract {
                detail: "one vendored frame per granule",
            });
        }
        w.begin_frame();
        w.payload().extend_from_slice(&enc.frames[0]);
        w.end_granule(input.rows);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// allocation-free decode over the vendored frame layout
// ---------------------------------------------------------------------------

/// Bounded LE cursor (no allocation; typed errors).
struct Cur<'a> {
    b: &'a [u8],
    off: usize,
}

impl<'a> Cur<'a> {
    fn new(b: &'a [u8]) -> Cur<'a> {
        Cur { b, off: 0 }
    }
    fn take(&mut self, n: usize) -> FormatResult<&'a [u8]> {
        let s = self
            .b
            .get(self.off..self.off + n)
            .ok_or(FormatError::Truncated { at: "alp frame" })?;
        self.off += n;
        Ok(s)
    }
    fn u8(&mut self) -> FormatResult<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> FormatResult<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().expect("len 2")))
    }
    fn i32(&mut self) -> FormatResult<i32> {
        Ok(i32::from_le_bytes(self.take(4)?.try_into().expect("len 4")))
    }
    fn i64(&mut self) -> FormatResult<i64> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().expect("len 8")))
    }
    fn words_into(&mut self, n: usize, buf: &mut [u64]) -> FormatResult<()> {
        let bytes = self.take(n * 8)?;
        for (slot, ch) in buf[..n].iter_mut().zip(bytes.chunks_exact(8)) {
            *slot = u64::from_le_bytes(ch.try_into().expect("len 8"));
        }
        Ok(())
    }
}

/// One decoded vector's bit images land here (stack; 8 KiB).
type VecBuf = [u64; VEC];

/// Sink for decoded vectors: either dense (full decode) or gather
/// (selection); keeps ONE frame walker for both faces.
enum Sink<'s> {
    Dense {
        out: &'s mut [u64],
        cursor: usize,
    },
    /// Selection gather: rows strictly ascending (granule-local), `out`
    /// parallel to rows.
    Gather {
        rows: &'s [u16],
        out: &'s mut [u64],
        next: usize,
    },
}

impl<'s> Sink<'s> {
    /// Rows [v0, v0+len) decoded in `buf[..len]`; consume what applies.
    /// (Gather-only after the gate-3 reshape — dense spans go through
    /// [`Sink::dense_span`] and never touch a scratch vector.)
    fn vector(&mut self, v0: usize, len: usize, buf: &VecBuf) -> FormatResult<()> {
        match self {
            Sink::Dense { out, cursor } => {
                let dst = out
                    .get_mut(*cursor..*cursor + len)
                    .ok_or(FormatError::Bounds { at: "decode out" })?;
                dst.copy_from_slice(&buf[..len]);
                *cursor += len;
                Ok(())
            }
            Sink::Gather { rows, out, next } => {
                while *next < rows.len() {
                    let r = rows[*next] as usize;
                    if r < v0 {
                        return Err(FormatError::Corrupt {
                            at: "Selection order",
                        });
                    }
                    if r >= v0 + len {
                        break;
                    }
                    out[*next] = buf[r - v0];
                    *next += 1;
                }
                Ok(())
            }
        }
    }

    /// Dense fast path (gate-3 shape, M3 exit §2.3): the output span for
    /// the next `len` rows when this sink is contiguous — vectors decode
    /// STRAIGHT into the caller's buffer instead of round-tripping an 8 KiB
    /// scratch vector per vector. Advances the cursor; `None` = gather.
    fn dense_span(&mut self, len: usize) -> FormatResult<Option<&mut [u64]>> {
        match self {
            Sink::Dense { out, cursor } => {
                let dst = out
                    .get_mut(*cursor..*cursor + len)
                    .ok_or(FormatError::Bounds { at: "decode out" })?;
                *cursor += len;
                Ok(Some(dst))
            }
            Sink::Gather { .. } => Ok(None),
        }
    }

    /// TRUE when rows [v0, v0+len) contain no survivors (skip decode).
    fn skippable(&self, v0: usize, len: usize) -> bool {
        match self {
            Sink::Dense { .. } => false,
            Sink::Gather { rows, next, .. } => match rows.get(*next) {
                None => true,
                Some(&r) => (r as usize) >= v0 + len,
            },
        }
    }
}

/// The classic-ALP vector transform: `(d * 10^f) * 10^-e`, each product
/// rounding once (the vendored evaluation order, bit-frozen).
#[inline]
fn alp_transform(dst: &mut [u64], deltas: &[u64], for_base: i64, fact: f64, frac: f64) {
    for (o, &z) in dst.iter_mut().zip(deltas) {
        let d = for_base.wrapping_add(z as i64);
        *o = ((d as f64) * fact * frac).to_bits();
    }
}

/// Same transform in place (full-vector dense spans: unpack landed the
/// deltas directly in `dst`).
#[inline]
fn alp_transform_inplace(dst: &mut [u64], for_base: i64, fact: f64, frac: f64) {
    for o in dst.iter_mut() {
        let d = for_base.wrapping_add(*o as i64);
        *o = ((d as f64) * fact * frac).to_bits();
    }
}

/// Patch classic-ALP exceptions (bit-exact raw images) into `dst`.
#[inline]
fn alp_patch(dst: &mut [u64], pos_bytes: &[u8], val_bytes: &[u8]) -> FormatResult<()> {
    for (p, v) in pos_bytes.chunks_exact(2).zip(val_bytes.chunks_exact(8)) {
        let pos = u16::from_le_bytes(p.try_into().expect("len 2")) as usize;
        if pos >= dst.len() {
            return Err(FormatError::Corrupt { at: "alp exc pos" });
        }
        dst[pos] = u64::from_le_bytes(v.try_into().expect("len 8"));
    }
    Ok(())
}

/// Walk one vendored granule frame, decoding into the sink. Every field is
/// validated (typed error, never UB) — the same checks as the vendored
/// oracle reader.
fn walk_frame(frame: &[u8], expect_values: u32, sink: &mut Sink<'_>) -> FormatResult<()> {
    let mut c = Cur::new(frame);
    let tag = c.u8()?;
    let nvectors = c.u8()? as usize;
    let nvalues = c.u16()? as usize;
    if nvectors == 0 || nvectors > GRANULE_VECTORS {
        return Err(FormatError::Corrupt { at: "alp nvectors" });
    }
    if nvalues == 0 || nvalues > GRANULE_VECTORS * VEC || nvalues != expect_values as usize {
        return Err(FormatError::Corrupt { at: "alp nvalues" });
    }
    let mut words = [0u64; 64 * bitpack::LANES];
    let mut deltas: VecBuf = [0; VEC];
    let mut buf: VecBuf = [0; VEC];
    let mut v0 = 0usize;
    match tag {
        TAG_RAW => {
            let bytes = c.take(nvalues * 8)?;
            // Raw frames are contiguous bit images; feed per-vector spans.
            while v0 < nvalues {
                let len = (nvalues - v0).min(VEC);
                if !sink.skippable(v0, len) {
                    let span = &bytes[v0 * 8..(v0 + len) * 8];
                    if let Some(dst) = sink.dense_span(len)? {
                        // Dense: one flat widen straight into the output.
                        for (o, ch) in dst.iter_mut().zip(span.chunks_exact(8)) {
                            *o = u64::from_le_bytes(ch.try_into().expect("len 8"));
                        }
                    } else {
                        for (o, ch) in buf[..len].iter_mut().zip(span.chunks_exact(8)) {
                            *o = u64::from_le_bytes(ch.try_into().expect("len 8"));
                        }
                        sink.vector(v0, len, &buf)?;
                    }
                }
                v0 += len;
            }
        }
        TAG_ALP => {
            let mut total = 0usize;
            for _ in 0..nvectors {
                let len = c.u16()? as usize;
                if len == 0 || len > VEC {
                    return Err(FormatError::Corrupt {
                        at: "alp vector len",
                    });
                }
                let exponent = c.u8()?;
                let factor = c.u8()?;
                if exponent > MAX_EXPONENT || factor > MAX_EXPONENT {
                    return Err(FormatError::Corrupt { at: "alp e/f" });
                }
                let bit_width = c.u8()? as u32;
                if bit_width > 64 {
                    return Err(FormatError::Corrupt {
                        at: "alp bit width",
                    });
                }
                let for_base = c.i64()?;
                let exc = c.u16()? as usize;
                if exc > len {
                    return Err(FormatError::Corrupt {
                        at: "alp exc count",
                    });
                }
                let nwords = bitpack::packed_words(bit_width);
                let skip = sink.skippable(v0, len);
                if skip {
                    c.take(nwords * 8)?;
                    c.take(exc * 2)?;
                    c.take(exc * 8)?;
                } else {
                    c.words_into(nwords, &mut words)?;
                    let fact = FACT[factor as usize] as f64;
                    let frac = FRAC[exponent as usize];
                    let pos_bytes = c.take(exc * 2)?;
                    let val_bytes = c.take(exc * 8)?;
                    if let Some(dst) = sink.dense_span(len)? {
                        if len == VEC {
                            // Full-vector dense span: unpack lands the
                            // deltas IN the output; transform in place.
                            let arr: &mut VecBuf = dst.try_into().expect("len VEC");
                            bitpack::unpack(&words[..nwords], bit_width, arr);
                            alp_transform_inplace(arr, for_base, fact, frac);
                            alp_patch(arr, pos_bytes, val_bytes)?;
                        } else {
                            bitpack::unpack(&words[..nwords], bit_width, &mut deltas);
                            alp_transform(dst, &deltas[..len], for_base, fact, frac);
                            alp_patch(dst, pos_bytes, val_bytes)?;
                        }
                    } else {
                        bitpack::unpack(&words[..nwords], bit_width, &mut deltas);
                        alp_transform(&mut buf[..len], &deltas[..len], for_base, fact, frac);
                        alp_patch(&mut buf[..len], pos_bytes, val_bytes)?;
                        sink.vector(v0, len, &buf)?;
                    }
                }
                v0 += len;
                total += len;
            }
            if total != nvalues {
                return Err(FormatError::Corrupt {
                    at: "alp nvalues sum",
                });
            }
        }
        TAG_ALP_RD => {
            let right_bw = c.u8()?;
            if !(48..=63).contains(&right_bw) {
                return Err(FormatError::Corrupt {
                    at: "rd right width",
                });
            }
            let left_bw = c.u8()?;
            if left_bw == 0 || left_bw > 3 {
                return Err(FormatError::Corrupt {
                    at: "rd left width",
                });
            }
            let dict_len = c.u8()? as usize;
            if dict_len == 0 || dict_len > MAX_RD_DICT {
                return Err(FormatError::Corrupt { at: "rd dict len" });
            }
            let dict_bytes = c.take(dict_len * 2)?;
            // Pre-shifted 8-slot gather table (the vendored decode shape:
            // padded slots repeat the last real entry; only exception
            // positions can reach them and those are patched).
            let mut table = [0u64; MAX_RD_DICT];
            for (j, slot) in table.iter_mut().enumerate() {
                let k = j.min(dict_len - 1);
                let e = u16::from_le_bytes(dict_bytes[k * 2..k * 2 + 2].try_into().expect("len 2"));
                *slot = (e as u64) << right_bw;
            }
            let right_mask = (1u64 << right_bw) - 1;
            let nr = bitpack::packed_words(right_bw as u32);
            let nl = bitpack::packed_words(left_bw as u32);
            let mut rwords = [0u64; 63 * bitpack::LANES];
            let mut lwords = [0u64; 3 * bitpack::LANES];
            let mut rights: VecBuf = [0; VEC];
            let mut lefts: VecBuf = [0; VEC];
            let mut total = 0usize;
            for _ in 0..nvectors {
                let len = c.u16()? as usize;
                if len == 0 || len > VEC {
                    return Err(FormatError::Corrupt {
                        at: "rd vector len",
                    });
                }
                let exc = c.u16()? as usize;
                if exc > len {
                    return Err(FormatError::Corrupt { at: "rd exc count" });
                }
                let skip = sink.skippable(v0, len);
                if skip {
                    c.take(nr * 8)?;
                    c.take(nl * 8)?;
                    c.take(exc * 2)?;
                    c.take(exc * 2)?;
                } else {
                    c.words_into(nr, &mut rwords)?;
                    c.words_into(nl, &mut lwords)?;
                    bitpack::unpack(&lwords[..nl], left_bw as u32, &mut lefts);
                    let pos_bytes = c.take(exc * 2)?;
                    let left_bytes = c.take(exc * 2)?;
                    let rd_patch = |dst: &mut [u64]| -> FormatResult<()> {
                        for (p, l) in pos_bytes.chunks_exact(2).zip(left_bytes.chunks_exact(2)) {
                            let pos = u16::from_le_bytes(p.try_into().expect("len 2")) as usize;
                            if pos >= dst.len() {
                                return Err(FormatError::Corrupt { at: "rd exc pos" });
                            }
                            let left = u16::from_le_bytes(l.try_into().expect("len 2")) as u64;
                            let right = dst[pos] & right_mask;
                            dst[pos] = (left << right_bw) | right;
                        }
                        Ok(())
                    };
                    if let Some(dst) = sink.dense_span(len)? {
                        if len == VEC {
                            // Full-vector dense span: rights unpack lands
                            // IN the output; combine in place.
                            let arr: &mut VecBuf = dst.try_into().expect("len VEC");
                            bitpack::unpack(&rwords[..nr], right_bw as u32, arr);
                            for (o, &l) in arr.iter_mut().zip(lefts.iter()) {
                                *o |= table[l as usize & (MAX_RD_DICT - 1)];
                            }
                            rd_patch(arr)?;
                        } else {
                            bitpack::unpack(&rwords[..nr], right_bw as u32, &mut rights);
                            for ((o, &r), &l) in dst.iter_mut().zip(rights.iter()).zip(lefts.iter())
                            {
                                *o = table[l as usize & (MAX_RD_DICT - 1)] | r;
                            }
                            rd_patch(dst)?;
                        }
                    } else {
                        bitpack::unpack(&rwords[..nr], right_bw as u32, &mut rights);
                        for ((o, &r), &l) in
                            buf[..len].iter_mut().zip(rights.iter()).zip(lefts.iter())
                        {
                            *o = table[l as usize & (MAX_RD_DICT - 1)] | r;
                        }
                        rd_patch(&mut buf[..len])?;
                        sink.vector(v0, len, &buf)?;
                    }
                }
                v0 += len;
                total += len;
            }
            if total != nvalues {
                return Err(FormatError::Corrupt {
                    at: "rd nvalues sum",
                });
            }
        }
        _ => {
            return Err(FormatError::Corrupt {
                at: "alp scheme tag",
            })
        }
    }
    Ok(())
}

/// The classic-ALP f32 vector transform: `(d * 10^f) * 10^-e` evaluated
/// in f64, rounded ONCE to f32 (the vendored `classic32.rs` evaluation
/// order and domain, bit-frozen — see the table note above). Datum words
/// are bit images zero-extended.
#[inline]
fn alp32_transform(dst: &mut [u64], deltas: &[u64], for_base: i32, fact: f64, frac: f64) {
    for (o, &z) in dst.iter_mut().zip(deltas) {
        let d = for_base.wrapping_add(z as u32 as i32);
        *o = (((d as f64) * fact * frac) as f32).to_bits() as u64;
    }
}

/// Same transform in place (full-vector dense spans).
#[inline]
fn alp32_transform_inplace(dst: &mut [u64], for_base: i32, fact: f64, frac: f64) {
    for o in dst.iter_mut() {
        let d = for_base.wrapping_add(*o as u32 as i32);
        *o = (((d as f64) * fact * frac) as f32).to_bits() as u64;
    }
}

/// Patch f32 classic-ALP exceptions (bit-exact raw images) into `dst`.
#[inline]
fn alp32_patch(dst: &mut [u64], pos_bytes: &[u8], val_bytes: &[u8]) -> FormatResult<()> {
    for (p, v) in pos_bytes.chunks_exact(2).zip(val_bytes.chunks_exact(4)) {
        let pos = u16::from_le_bytes(p.try_into().expect("len 2")) as usize;
        if pos >= dst.len() {
            return Err(FormatError::Corrupt { at: "alp exc pos" });
        }
        dst[pos] = u32::from_le_bytes(v.try_into().expect("len 4")) as u64;
    }
    Ok(())
}

/// Walk one vendored F32 granule frame (`alp::granule32` layout doc),
/// decoding into the sink — the SB-5 twin of [`walk_frame`], sharing its
/// [`Sink`] machinery. Every field is validated (typed error, never UB);
/// the RD tag refuses at this width (no f32 RD arm — module doc).
fn walk_frame32(frame: &[u8], expect_values: u32, sink: &mut Sink<'_>) -> FormatResult<()> {
    let mut c = Cur::new(frame);
    let tag = c.u8()?;
    let nvectors = c.u8()? as usize;
    let nvalues = c.u16()? as usize;
    if nvectors == 0 || nvectors > GRANULE_VECTORS {
        return Err(FormatError::Corrupt { at: "alp nvectors" });
    }
    if nvalues == 0 || nvalues > GRANULE_VECTORS * VEC || nvalues != expect_values as usize {
        return Err(FormatError::Corrupt { at: "alp nvalues" });
    }
    let mut words = [0u64; 32 * bitpack::LANES];
    let mut deltas: VecBuf = [0; VEC];
    let mut buf: VecBuf = [0; VEC];
    let mut v0 = 0usize;
    match tag {
        TAG_RAW => {
            let bytes = c.take(nvalues * 4)?;
            // Raw frames are contiguous 4-byte bit images; per-vector spans.
            while v0 < nvalues {
                let len = (nvalues - v0).min(VEC);
                if !sink.skippable(v0, len) {
                    let span = &bytes[v0 * 4..(v0 + len) * 4];
                    if let Some(dst) = sink.dense_span(len)? {
                        // Dense: one flat u32 widen straight into the output.
                        for (o, ch) in dst.iter_mut().zip(span.chunks_exact(4)) {
                            *o = u32::from_le_bytes(ch.try_into().expect("len 4")) as u64;
                        }
                    } else {
                        for (o, ch) in buf[..len].iter_mut().zip(span.chunks_exact(4)) {
                            *o = u32::from_le_bytes(ch.try_into().expect("len 4")) as u64;
                        }
                        sink.vector(v0, len, &buf)?;
                    }
                }
                v0 += len;
            }
        }
        TAG_ALP => {
            let mut total = 0usize;
            for _ in 0..nvectors {
                let len = c.u16()? as usize;
                if len == 0 || len > VEC {
                    return Err(FormatError::Corrupt {
                        at: "alp vector len",
                    });
                }
                let exponent = c.u8()?;
                let factor = c.u8()?;
                if exponent > MAX_EXPONENT_F32 || factor > MAX_EXPONENT_F32 {
                    return Err(FormatError::Corrupt { at: "alp e/f" });
                }
                let bit_width = c.u8()? as u32;
                if bit_width > 32 {
                    return Err(FormatError::Corrupt {
                        at: "alp bit width",
                    });
                }
                let for_base = c.i32()?;
                let exc = c.u16()? as usize;
                if exc > len {
                    return Err(FormatError::Corrupt {
                        at: "alp exc count",
                    });
                }
                let nwords = bitpack::packed_words(bit_width);
                let skip = sink.skippable(v0, len);
                if skip {
                    c.take(nwords * 8)?;
                    c.take(exc * 2)?;
                    c.take(exc * 4)?;
                } else {
                    c.words_into(nwords, &mut words)?;
                    let fact = FACT[factor as usize] as f64;
                    let frac = FRAC[exponent as usize];
                    let pos_bytes = c.take(exc * 2)?;
                    let val_bytes = c.take(exc * 4)?;
                    if let Some(dst) = sink.dense_span(len)? {
                        if len == VEC {
                            // Full-vector dense span: unpack lands the
                            // deltas IN the output; transform in place.
                            let arr: &mut VecBuf = dst.try_into().expect("len VEC");
                            bitpack::unpack(&words[..nwords], bit_width, arr);
                            alp32_transform_inplace(arr, for_base, fact, frac);
                            alp32_patch(arr, pos_bytes, val_bytes)?;
                        } else {
                            bitpack::unpack(&words[..nwords], bit_width, &mut deltas);
                            alp32_transform(dst, &deltas[..len], for_base, fact, frac);
                            alp32_patch(dst, pos_bytes, val_bytes)?;
                        }
                    } else {
                        bitpack::unpack(&words[..nwords], bit_width, &mut deltas);
                        alp32_transform(&mut buf[..len], &deltas[..len], for_base, fact, frac);
                        alp32_patch(&mut buf[..len], pos_bytes, val_bytes)?;
                        sink.vector(v0, len, &buf)?;
                    }
                }
                v0 += len;
                total += len;
            }
            if total != nvalues {
                return Err(FormatError::Corrupt {
                    at: "alp nvalues sum",
                });
            }
        }
        // TAG_ALP_RD deliberately included: no RD arm at the f32 width.
        _ => {
            return Err(FormatError::Corrupt {
                at: "alp scheme tag",
            })
        }
    }
    Ok(())
}

fn granule_frame<'a>(ctx: &KernelCtx<'a>, encoding: u16) -> FormatResult<&'a [u8]> {
    let hdr = open_section(ctx.bytes, encoding)?;
    let payload = payload_region(&hdr, ctx.bytes)?;
    let f = granule_frame_base(&hdr, ctx.bytes, ctx.granule_in_extent)?;
    let fs = frame_start(ctx.frame_table, f)?;
    // Frame extends to the next frame's start (or the payload end).
    let fe = match ctx.frame_table.and_then(|ft| ft.get(f as usize + 1)) {
        Some(&next) => next as usize,
        None => payload.len(),
    };
    payload.get(fs..fe).ok_or(FormatError::Bounds {
        at: "alp granule frame",
    })
}

fn alp_dec_full_for(ctx: &KernelCtx<'_>, encoding: u16, out: &mut [u64]) -> FormatResult<u32> {
    let frame = granule_frame(ctx, encoding)?;
    if out.len() < ctx.values as usize {
        return Err(FormatError::Bounds {
            at: "decode_full out",
        });
    }
    let mut sink = Sink::Dense {
        out: &mut out[..ctx.values as usize],
        cursor: 0,
    };
    walk_frame(frame, ctx.values, &mut sink)?;
    Ok(ctx.values)
}

fn alp_dec_sel_for(
    ctx: &KernelCtx<'_>,
    encoding: u16,
    sel: &Selection<'_>,
    out: &mut [u64],
) -> FormatResult<u32> {
    let frame = granule_frame(ctx, encoding)?;
    if out.len() < sel.rows.len() {
        return Err(FormatError::Bounds {
            at: "decode_sel out",
        });
    }
    if let Some(&last) = sel.rows.last() {
        if last as u32 >= ctx.values {
            return Err(FormatError::Bounds {
                at: "decode_sel row",
            });
        }
    }
    let n = sel.rows.len();
    let mut sink = Sink::Gather {
        rows: sel.rows,
        out: &mut out[..n],
        next: 0,
    };
    walk_frame(frame, ctx.values, &mut sink)?;
    match sink {
        Sink::Gather { next, .. } if next == n => Ok(n as u32),
        _ => Err(FormatError::Corrupt {
            at: "selection past frame",
        }),
    }
}

fn alp_dec_full(ctx: &KernelCtx<'_>, out: &mut DecodeOut<'_>) -> FormatResult<u32> {
    alp_dec_full_for(ctx, EncodingId::Alp.as_u16(), out.datums)
}
fn alp_dec_sel(
    ctx: &KernelCtx<'_>,
    sel: &Selection<'_>,
    out: &mut DecodeOut<'_>,
) -> FormatResult<u32> {
    alp_dec_sel_for(ctx, EncodingId::Alp.as_u16(), sel, out.datums)
}
fn rd_dec_full(ctx: &KernelCtx<'_>, out: &mut DecodeOut<'_>) -> FormatResult<u32> {
    alp_dec_full_for(ctx, EncodingId::AlpRd.as_u16(), out.datums)
}
fn rd_dec_sel(
    ctx: &KernelCtx<'_>,
    sel: &Selection<'_>,
    out: &mut DecodeOut<'_>,
) -> FormatResult<u32> {
    alp_dec_sel_for(ctx, EncodingId::AlpRd.as_u16(), sel, out.datums)
}

fn alp32_dec_full(ctx: &KernelCtx<'_>, out: &mut DecodeOut<'_>) -> FormatResult<u32> {
    let frame = granule_frame(ctx, EncodingId::Alp.as_u16())?;
    if out.datums.len() < ctx.values as usize {
        return Err(FormatError::Bounds {
            at: "decode_full out",
        });
    }
    let mut sink = Sink::Dense {
        out: &mut out.datums[..ctx.values as usize],
        cursor: 0,
    };
    walk_frame32(frame, ctx.values, &mut sink)?;
    Ok(ctx.values)
}

fn alp32_dec_sel(
    ctx: &KernelCtx<'_>,
    sel: &Selection<'_>,
    out: &mut DecodeOut<'_>,
) -> FormatResult<u32> {
    let frame = granule_frame(ctx, EncodingId::Alp.as_u16())?;
    if out.datums.len() < sel.rows.len() {
        return Err(FormatError::Bounds {
            at: "decode_sel out",
        });
    }
    if let Some(&last) = sel.rows.last() {
        if last as u32 >= ctx.values {
            return Err(FormatError::Bounds {
                at: "decode_sel row",
            });
        }
    }
    let n = sel.rows.len();
    let mut sink = Sink::Gather {
        rows: sel.rows,
        out: &mut out.datums[..n],
        next: 0,
    };
    walk_frame32(frame, ctx.values, &mut sink)?;
    match sink {
        Sink::Gather { next, .. } if next == n => Ok(n as u32),
        _ => Err(FormatError::Corrupt {
            at: "selection past frame",
        }),
    }
}

fn alp_meta(ctx: &KernelCtx<'_>, probe: &MetaProbe) -> FormatResult<MetaAnswer> {
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

fn alp_validity(ctx: &KernelCtx<'_>, out: &mut [u64]) -> FormatResult<ValidityVerdict> {
    validity_from_ctx(ctx, out)
}

pub static VT_ALP: CodecVtable = CodecVtable {
    key: KernelKey {
        encoding: EncodingId::Alp as u16,
        class: CLASS_F64,
        width: 0,
    },
    decode_full: alp_dec_full,
    decode_sel: alp_dec_sel,
    decode_codes: refuse_decode_codes,
    dict_handle: refuse_dict_handle,
    meta_probe: alp_meta,
    validity: alp_validity,
};

pub static VT_ALP_RD: CodecVtable = CodecVtable {
    key: KernelKey {
        encoding: EncodingId::AlpRd as u16,
        class: CLASS_F64,
        width: 0,
    },
    decode_full: rd_dec_full,
    decode_sel: rd_dec_sel,
    decode_codes: refuse_decode_codes,
    dict_handle: refuse_dict_handle,
    meta_probe: alp_meta,
    validity: alp_validity,
};

/// The SB-5 F32 arm: ALP (id 5) at CLASS_F32 over the vendored granule32
/// frames. No RD vtable exists at this width — the writer never stamps
/// ALP_RD on an F32 stream (a stream entry claiming it finds no kernel).
pub static VT_ALP_F32: CodecVtable = CodecVtable {
    key: KernelKey {
        encoding: EncodingId::Alp as u16,
        class: CLASS_F32,
        width: 0,
    },
    decode_full: alp32_dec_full,
    decode_sel: alp32_dec_sel,
    decode_codes: refuse_decode_codes,
    dict_handle: refuse_dict_handle,
    meta_probe: alp_meta,
    validity: alp_validity,
};
