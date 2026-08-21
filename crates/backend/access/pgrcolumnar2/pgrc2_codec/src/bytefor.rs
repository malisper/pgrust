//! BYTE_FOR — byte-aligned frame-of-reference, the int-family primary
//! (spec §6.10; M0-S4 verdict). Frame = `[ref: i64][deltas:
//! values_in_frame × w]`, w ∈ {1..=8} elected per stream, deltas unsigned
//! LE, `value = ref +(wrapping) delta`.
//!
//! Width set: v3 shipped the pow2 widths {1,2,4,8}; the SB-3 / CMP-B
//! signed disposition (lanev4 format ledger, SIGNED 2026-08-12) adds the
//! missing byte-aligned widths {3,5,6,7} end-to-end, so the int/µs-
//! timestamp family (22-bit-seconds at µs grain ⇒ ~42-bit ranges ⇒ width
//! 6; time-of-day µs ⇒ width 5) stops falling to VERBATIM. Timestamps
//! ride this ordinary ByvalWord election — no timestamp-specific code.
//!
//! Kernel-shape laws carried (S4):
//! - one monomorphized kernel per width behind the fn-pointer vtable —
//!   never a width switch in or near the hot loop (the pow2-switch trap);
//! - flat slice-widen bodies (gate-3 ruling, M3 exit §2.3): bounds proven
//!   once per frame, then `zip(chunks_exact(W))` widening loads with no
//!   `Result` in the loop — the old format's proven NEON shape (the prior
//!   masked-u64-with-per-value-`?` body priced 0.10–0.41× against it);
//! - decode emits exact datum words (the ref is the full extended word and
//!   deltas are exact differences), so no per-value extension runs at all.
//!
//! Null slots encode delta 0 (the frame ref — spec §6.6 canonical
//! placeholder); validity is the only truth on decode.

use crate::section::{frame_base, frame_start, open_section, payload_region, read_le, read_le_w};
use pgrc2_format::abi::{
    refuse_decode_codes, refuse_dict_handle, validity_from_ctx, CodecVtable, DecodeOut,
    EncodeInput, GranuleEncoder, KernelCtx, KernelKey, Selection, ValidityVerdict,
};
use pgrc2_format::class::CLASS_BYVAL;
use pgrc2_format::enc::EncodingId;
use pgrc2_format::geom::FRAME_VALUES;
use pgrc2_format::meta::{MetaAnswer, MetaProbe};
use pgrc2_format::part::{OverflowSink, StreamSectionWriter};
use pgrc2_format::{FormatError, FormatResult};

/// Bytes of frame header (the i64 reference).
pub const FRAME_REF_LEN: usize = 8;

// ---------------------------------------------------------------------------
// analyze (election input: exact, input-decidable)
// ---------------------------------------------------------------------------

/// Smallest legal delta width for one granule's frames, or None when even
/// w=8 cannot hold a frame range (never happens: u64 ranges always fit 8).
/// `signed` picks the min/max domain; the width is a pure function of the
/// input (input-decidable election, charter §3).
pub fn granule_min_width(input: &EncodeInput<'_>, signed: bool) -> u8 {
    let mut need: u8 = 1;
    let rows = input.rows as usize;
    let mut f0 = 0usize;
    while f0 < rows {
        let n = (rows - f0).min(FRAME_VALUES as usize);
        if let Some(range) = frame_range(input, f0, n, signed) {
            let w = width_for_range(range);
            if w > need {
                need = w;
            }
        }
        f0 += n;
    }
    need
}

/// Exact BYTE_FOR payload bytes for `rows` values at width `w` (frames of
/// 1024): the election's candidate size.
pub fn payload_bytes(rows: u32, w: u8) -> usize {
    let full = (rows / FRAME_VALUES) as usize;
    let tail = (rows % FRAME_VALUES) as usize;
    let mut b = full * (FRAME_REF_LEN + FRAME_VALUES as usize * w as usize);
    if tail > 0 {
        b += FRAME_REF_LEN + tail * w as usize;
    }
    b
}

fn frame_range(input: &EncodeInput<'_>, f0: usize, n: usize, signed: bool) -> Option<u64> {
    frame_min_range(input, f0, n, signed).map(|(_, range)| range)
}

/// One frame's (min-in-domain, range) in a SINGLE walk (SEAL-FUSION: the
/// pre-fusion encode ran `frame_range` then `frame_min` — two reductions
/// over the same in-cache values). `None` = all-null frame.
fn frame_min_range(
    input: &EncodeInput<'_>,
    f0: usize,
    n: usize,
    signed: bool,
) -> Option<(u64, u64)> {
    let mut any = false;
    let (mut min_s, mut max_s) = (i64::MAX, i64::MIN);
    let (mut min_u, mut max_u) = (u64::MAX, u64::MIN);
    for r in f0..f0 + n {
        if !input.valid(r as u32) {
            continue;
        }
        any = true;
        let d = input.datums[r];
        if signed {
            min_s = min_s.min(d as i64);
            max_s = max_s.max(d as i64);
        } else {
            min_u = min_u.min(d);
            max_u = max_u.max(d);
        }
    }
    if !any {
        return None;
    }
    Some(if signed {
        (min_s as u64, (max_s as u64).wrapping_sub(min_s as u64))
    } else {
        (min_u, max_u - min_u)
    })
}

/// Smallest byte-aligned width holding `range` (full {1..=8} ladder — the
/// SB-3 widening; pure, input-decidable).
pub fn width_for_range(range: u64) -> u8 {
    if range <= 0xFF {
        1
    } else if range <= 0xFFFF {
        2
    } else if range <= 0xFF_FFFF {
        3
    } else if range <= 0xFFFF_FFFF {
        4
    } else if range <= 0xFF_FFFF_FFFF {
        5
    } else if range <= 0xFFFF_FFFF_FFFF {
        6
    } else if range <= 0xFF_FFFF_FFFF_FFFF {
        7
    } else {
        8
    }
}

// ---------------------------------------------------------------------------
// encode (spec §19.6 faces)
// ---------------------------------------------------------------------------

/// The BYTE_FOR granule encoder for byval word streams. Also reused by
/// PACKED_NUMERIC for its mantissa payload (`packednum.rs`), which is why
/// the frame emission is exposed as [`encode_i64_frames`].
pub struct ByteForEncoder {
    /// Elected delta width (stream-level; from [`granule_min_width`] over
    /// every granule).
    pub width: u8,
    /// Election domain for frame refs.
    pub signed: bool,
    /// The key this encoder writes under (BYTE_FOR for byval words;
    /// PACKED_NUMERIC delegates with its own key).
    pub key: KernelKey,
    /// SEAL-FUSION: the election's per-frame facts, consumed from `next`
    /// in frame order (None = re-reduce per frame, the pre-fusion path).
    pub carry: Option<IntCarryCursor>,
}

/// Per-frame carried facts + cursor (SEAL-FUSION; shared by the BYTE_FOR /
/// FFOR / DELTA_FOR encoders — the facts are the fused int analyzer's
/// [`crate::election::IntFrameFact`] table, frame ordinal = row-dense part
/// offset / 1024, exact at every SB-10 grain).
#[derive(Debug, Clone)]
pub struct IntCarryCursor {
    pub facts: std::sync::Arc<Vec<crate::election::IntFrameFact>>,
    pub next: usize,
}

impl IntCarryCursor {
    pub(crate) fn take(&mut self) -> FormatResult<crate::election::IntFrameFact> {
        let f = self
            .facts
            .get(self.next)
            .copied()
            .ok_or(FormatError::EncodeContract {
                detail: "int frame carry exhausted",
            })?;
        self.next += 1;
        Ok(f)
    }
}

impl ByteForEncoder {
    pub fn new_bytefor(byval_width: u8, delta_width: u8, signed: bool) -> ByteForEncoder {
        // The kernel key (and the §6.3 width byte) carry the DELTA width —
        // the dispatch axis; the byval width is immaterial to decode
        // (datum words are exact) and is recorded in the storage class.
        let _ = byval_width;
        ByteForEncoder {
            width: delta_width,
            signed,
            key: KernelKey {
                encoding: EncodingId::ByteFor.as_u16(),
                class: CLASS_BYVAL,
                width: delta_width,
            },
            carry: None,
        }
    }
}

/// Emit one granule of `rows` datum words as BYTE_FOR frames (ref + deltas)
/// at width `w`. Shared by BYTE_FOR and PACKED_NUMERIC. `carry` = the
/// election's per-frame facts (SEAL-FUSION): the ref/width reduction is a
/// table read instead of a re-walk; the emit loop and the elected-width
/// refusal are unchanged, and `verify_roundtrip` stands behind both paths.
pub fn encode_i64_frames(
    input: &EncodeInput<'_>,
    w: &mut StreamSectionWriter<'_>,
    width: u8,
    signed: bool,
) -> FormatResult<()> {
    encode_i64_frames_carried(input, w, width, signed, None)
}

/// [`encode_i64_frames`] with an optional carried-facts cursor.
pub fn encode_i64_frames_carried(
    input: &EncodeInput<'_>,
    w: &mut StreamSectionWriter<'_>,
    width: u8,
    signed: bool,
    carry: Option<&mut IntCarryCursor>,
) -> FormatResult<()> {
    encode_i64_frames_fused(input, w, width, signed, carry, None)
}

/// [`encode_i64_frames_carried`] with the SEAL-SPEED-2 fold-fusion hook:
/// the delta-emit loop feeds the meta builder per row (`obs` = the
/// streaming face, caller-opened), so the separate observe walk over the
/// same datums disappears. `None` = the classic emit; bytes identical
/// either way.
pub fn encode_i64_frames_fused(
    input: &EncodeInput<'_>,
    w: &mut StreamSectionWriter<'_>,
    width: u8,
    signed: bool,
    mut carry: Option<&mut IntCarryCursor>,
    mut obs: Option<&mut dyn pgrc2_format::abi::ColumnMetaBuilder>,
) -> FormatResult<()> {
    let rows = input.rows as usize;
    if input.datums.len() < rows {
        return Err(FormatError::EncodeContract {
            detail: "datums shorter than rows",
        });
    }
    let mut f0 = 0usize;
    while f0 < rows {
        let n = (rows - f0).min(FRAME_VALUES as usize);
        // Ref = the frame's min datum word in the elected domain; an
        // all-null frame uses ref 0 (every slot is a placeholder).
        let mr: Option<(u64, u64)> = match carry.as_mut() {
            Some(c) => {
                let f = c.take()?;
                f.any.then_some((f.refw, f.range))
            }
            None => frame_min_range(input, f0, n, signed),
        };
        let refw: u64 = match mr {
            Some((minw, range)) => {
                if width_for_range(range) > width {
                    return Err(FormatError::EncodeContract {
                        detail: "frame range exceeds elected width",
                    });
                }
                minw
            }
            None => 0,
        };
        // SEAL-SPEED-2 fold fusion, SPAN grain: fold this frame's rows in
        // one virtual call (the builder's tight loop inside); the emit
        // below re-reads the same cache-warm datums.
        if let Some(o) = obs.as_deref_mut() {
            o.observe_rows(input, f0 as u32, n as u32);
        }
        w.begin_frame();
        let buf = w.payload();
        buf.extend_from_slice(&(refw as i64).to_le_bytes());
        for r in f0..f0 + n {
            let delta = if input.valid(r as u32) {
                input.datums[r].wrapping_sub(refw)
            } else {
                0 // canonical placeholder (spec §6.6)
            };
            buf.extend_from_slice(&delta.to_le_bytes()[..width as usize]);
        }
        f0 += n;
    }
    w.end_granule(input.rows);
    Ok(())
}

impl GranuleEncoder for ByteForEncoder {
    fn key(&self) -> KernelKey {
        self.key
    }

    fn encode_granule(
        &mut self,
        input: &EncodeInput<'_>,
        w: &mut StreamSectionWriter<'_>,
        _ovf: &mut OverflowSink<'_>,
    ) -> FormatResult<()> {
        encode_i64_frames_carried(input, w, self.width, self.signed, self.carry.as_mut())
    }

    /// SEAL-SPEED-2 fold fusion: the delta emit walks the datums row-dense
    /// anyway — the folds ride it (the observe walk disappears).
    fn encode_granule_observed(
        &mut self,
        input: &EncodeInput<'_>,
        w: &mut StreamSectionWriter<'_>,
        ovf: &mut OverflowSink<'_>,
        meta: &mut dyn pgrc2_format::abi::ColumnMetaBuilder,
        granule: u32,
    ) -> FormatResult<()> {
        if !meta.row_observe_supported() {
            self.encode_granule(input, w, ovf)?;
            meta.observe_granule(input, granule);
            return Ok(());
        }
        meta.begin_granule_rows(granule);
        encode_i64_frames_fused(
            input,
            w,
            self.width,
            self.signed,
            self.carry.as_mut(),
            Some(meta),
        )?;
        meta.end_granule_rows(input.rows);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// decode kernels (monomorphized per delta width W)
// ---------------------------------------------------------------------------

/// Shared open: the granule's first frame index + the payload slice.
struct ByteForView<'a> {
    payload: &'a [u8],
    frame_table: Option<&'a [u32]>,
    fbase: u32,
    values: u32,
}

fn open_view<'a>(ctx: &KernelCtx<'a>, encoding: u16) -> FormatResult<ByteForView<'a>> {
    let hdr = open_section(ctx.bytes, encoding)?;
    Ok(ByteForView {
        payload: payload_region(&hdr, ctx.bytes)?,
        frame_table: ctx.frame_table,
        fbase: frame_base(&hdr, ctx.bytes, ctx.granule_in_extent)?,
        values: ctx.values,
    })
}

/// Decode all values of the granule at delta width W. Gate-3 kernel shape
/// (M3 exit §2.3 ruling): frame addressing is resolved ONCE per frame, the
/// bounds are proven ONCE per frame, and the inner loop is a flat
/// `zip(chunks_exact(W))` slice widen with no `Result` and no per-value
/// bounds check — the old format's proven NEON idiom (`pgrcolumnar`
/// reader.rs widen loops). The prior body's per-value `load_u64_le(..)?`
/// carried an early-exit branch per iteration that blocked vectorization
/// (0.10–0.41× vs old at the decode_full face).
fn dec_full_width<const W: usize>(v: &ByteForView<'_>, out: &mut [u64]) -> FormatResult<u32> {
    if out.len() < v.values as usize {
        return Err(FormatError::Bounds {
            at: "decode_full out",
        });
    }
    let mut r = 0usize;
    let mut f = 0u32;
    while r < v.values as usize {
        let n = (v.values as usize - r).min(FRAME_VALUES as usize);
        let fs = frame_start(v.frame_table, v.fbase + f)?;
        let refw = load_frame_ref(v.payload, fs)?;
        let base = fs + FRAME_REF_LEN;
        let body = v
            .payload
            .get(base..base + n * W)
            .ok_or(FormatError::Bounds {
                at: "byte-for frame payload",
            })?;
        if W == 1 {
            // chunks_exact(1) carries remainder plumbing the vectorizer
            // dislikes; the byte iterator is the clean u8 widen.
            for (o, &b) in out[r..r + n].iter_mut().zip(body.iter()) {
                *o = refw.wrapping_add(b as u64);
            }
        } else {
            for (o, c) in out[r..r + n].iter_mut().zip(body.chunks_exact(W)) {
                *o = refw.wrapping_add(read_le_w::<W>(c));
            }
        }
        r += n;
        f += 1;
    }
    Ok(v.values)
}

fn load_frame_ref(payload: &[u8], fs: usize) -> FormatResult<u64> {
    let b = payload
        .get(fs..fs + FRAME_REF_LEN)
        .ok_or(FormatError::Bounds {
            at: "byte-for frame ref",
        })?;
    Ok(u64::from_le_bytes(b.try_into().expect("len 8")))
}

fn dec_sel_width<const W: usize>(
    v: &ByteForView<'_>,
    sel: &Selection<'_>,
    out: &mut [u64],
) -> FormatResult<u32> {
    if out.len() < sel.rows.len() {
        return Err(FormatError::Bounds {
            at: "decode_sel out",
        });
    }
    // Survivor-only completion: touch only the frames selections land in.
    let mut cur_frame = u32::MAX;
    let (mut refw, mut base) = (0u64, 0usize);
    for (i, &r16) in sel.rows.iter().enumerate() {
        let r = r16 as u32;
        if r >= v.values {
            return Err(FormatError::Bounds {
                at: "decode_sel row",
            });
        }
        let f = r / FRAME_VALUES;
        if f != cur_frame {
            let fs = frame_start(v.frame_table, v.fbase + f)?;
            refw = load_frame_ref(v.payload, fs)?;
            base = fs + FRAME_REF_LEN;
            cur_frame = f;
        }
        let off = base + (r % FRAME_VALUES) as usize * W;
        let b = v.payload.get(off..off + W).ok_or(FormatError::Bounds {
            at: "byte-for frame payload",
        })?;
        out[i] = refw.wrapping_add(read_le(b));
    }
    Ok(sel.rows.len() as u32)
}

fn bf_dec_full<const W: usize>(ctx: &KernelCtx<'_>, out: &mut DecodeOut<'_>) -> FormatResult<u32> {
    let v = open_view(ctx, EncodingId::ByteFor.as_u16())?;
    dec_full_width::<W>(&v, out.datums)
}

fn bf_dec_sel<const W: usize>(
    ctx: &KernelCtx<'_>,
    sel: &Selection<'_>,
    out: &mut DecodeOut<'_>,
) -> FormatResult<u32> {
    let v = open_view(ctx, EncodingId::ByteFor.as_u16())?;
    dec_sel_width::<W>(&v, sel, out.datums)
}

fn bf_meta(ctx: &KernelCtx<'_>, probe: &MetaProbe) -> FormatResult<MetaAnswer> {
    Ok(match probe {
        MetaProbe::RowCount => MetaAnswer::Count(ctx.rows as u64),
        MetaProbe::NonNullCount => {
            let mut scratch = [0u64; (pgrc2_format::geom::GRANULE_ROWS as usize).div_ceil(64)];
            match validity_from_ctx(ctx, &mut scratch)? {
                ValidityVerdict::AllValid => MetaAnswer::Count(ctx.rows as u64),
                ValidityVerdict::Mixed { nonnull } => MetaAnswer::Count(nonnull as u64),
            }
        }
        _ => MetaAnswer::Absent,
    })
}

fn bf_validity(ctx: &KernelCtx<'_>, out: &mut [u64]) -> FormatResult<ValidityVerdict> {
    validity_from_ctx(ctx, out)
}

/// The PACKED_NUMERIC payload shares this frame layout; its kernels reuse
/// these bodies through open-by-id (`packednum.rs`).
pub(crate) fn dec_full_for<const W: usize>(
    ctx: &KernelCtx<'_>,
    encoding: u16,
    out: &mut [u64],
) -> FormatResult<u32> {
    let v = open_view(ctx, encoding)?;
    dec_full_width::<W>(&v, out)
}

pub(crate) fn dec_sel_for<const W: usize>(
    ctx: &KernelCtx<'_>,
    encoding: u16,
    sel: &Selection<'_>,
    out: &mut [u64],
) -> FormatResult<u32> {
    let v = open_view(ctx, encoding)?;
    dec_sel_width::<W>(&v, sel, out)
}

// ---------------------------------------------------------------------------
// vtables: (BYTE_FOR × byval width × delta width) — the stream entry's
// `width` byte carries the DELTA width (spec §6.3 for BYTE_FOR), so the
// kernel key is (encoding=2, class=0, width=delta_w) and the byval width is
// immaterial to decode (datum words are exact).
// ---------------------------------------------------------------------------

const fn bf_vt<const W: usize>() -> CodecVtable {
    CodecVtable {
        key: KernelKey {
            encoding: EncodingId::ByteFor as u16,
            class: CLASS_BYVAL,
            width: W as u8,
        },
        decode_full: bf_dec_full::<W>,
        decode_sel: bf_dec_sel::<W>,
        decode_codes: refuse_decode_codes,
        dict_handle: refuse_dict_handle,
        meta_probe: bf_meta,
        validity: bf_validity,
    }
}

/// One vtable per legal delta width — the full {1..=8} ladder (SB-3), each
/// entry a DISTINCT monomorphized kernel pair (pinned by
/// `tests/dispatch_shape.rs`).
pub static VT_BYTE_FOR: [CodecVtable; 8] = [
    bf_vt::<1>(),
    bf_vt::<2>(),
    bf_vt::<3>(),
    bf_vt::<4>(),
    bf_vt::<5>(),
    bf_vt::<6>(),
    bf_vt::<7>(),
    bf_vt::<8>(),
];
