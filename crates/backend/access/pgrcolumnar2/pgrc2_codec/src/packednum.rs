//! PACKED_NUMERIC — the A6b fixed-scale packed-i64 election (spec §4 id 9;
//! payload semantics authority: the vendored `adt_numeric::fixed_scale`,
//! M3-B provenance). A chunk elects a single shared display scale S; every
//! value must `fixed_scale_fit` at S EXACTLY (uniform-dscale rule — a value
//! that would need rounding REFUSES, never rounds), and reconstruction
//! (`fixed_scale_unpack`) is byte-identical to the stored image by the
//! vendored canonical-digit argument. Refusal demotes the chunk to
//! VERBATIM (election quadruple; `election.rs`).
//!
//! On-disk payload (delegated to M3-C): per granule —
//!
//! ```text
//! granule := scale i32 | pad u32 | BYTE_FOR frames over the mantissas
//! ```
//!
//! The mantissa frames are EXACTLY the spec §6.10 frame layout at the
//! elected per-stream delta width (`bytefor.rs` emits and decodes them);
//! the 8-byte scale block sits before each granule's first frame, mirroring
//! the DICT_CODES block-header pattern. The elected scale ALSO rides the
//! stream entry's `aux32` (spec §6.3) for the metadata plane; the payload
//! copy makes decode self-contained (`KernelCtx` carries no aux32 — the
//! kernel reads the payload's, and M3-K's corrupt suites cover a mismatch
//! via CRC + the round-trip gate).
//!
//! Packed order == `cmp_numerics` order at a shared scale (vendored pin),
//! which is what makes the i64 zone plane sound over these chunks (M3-E).
//!
//! Null slots encode delta 0 (the frame ref — spec §6.6).

use crate::bytefor::{dec_full_for, dec_sel_for, encode_i64_frames};
use crate::section::{frame_base, frame_start, open_section, payload_region, varlena_payload};
use adt_numeric::{fixed_scale_fit, fixed_scale_unpack, Num, FIXED_SCALE_MANT_ABS_MAX};
use pgrc2_format::abi::{
    refuse_decode_codes, refuse_dict_handle, validity_from_ctx, ByteArena, CodecVtable, DecodeOut,
    EncodeInput, GranuleEncoder, KernelCtx, KernelKey, Selection, ValidityVerdict,
};
use pgrc2_format::class::{StorageClass, CLASS_VARLENA};
use pgrc2_format::enc::EncodingId;
use pgrc2_format::geom::GRANULE_ROWS;
use pgrc2_format::meta::{MetaAnswer, MetaProbe};
use pgrc2_format::part::{OverflowSink, StreamSectionWriter};
use pgrc2_format::{FormatError, FormatResult};

/// PG's NUMERIC_DSCALE_MAX (the vendored unpack's domain; validated at
/// decode so corrupt scales refuse typed instead of tripping debug asserts).
const DSCALE_MAX: i32 = 0x3FFF;

const SCALE_HDR_LEN: usize = 8; // scale(4) + pad(4)

/// The mantissa of one encode-side numeric datum at scale S, or None
/// (refusal — the election demotes). Pure; input-decidable.
///
/// # Safety-free surface: the unsafe deref is confined to
/// [`varlena_payload`].
pub fn mantissa_at(datum: u64, scale: i32) -> FormatResult<Option<i64>> {
    // SAFETY: EncodeInput pointer-class contract (spec §19.6).
    let payload = unsafe { varlena_payload(datum)? };
    Ok(fixed_scale_fit(Num::from_payload(payload), scale))
}

/// The elected scale of the FIRST valid value (the vendored election rule:
/// S = first value's dscale), or None for an all-null/empty input.
pub fn elect_scale(input: &EncodeInput<'_>) -> FormatResult<Option<i32>> {
    for r in 0..input.rows as usize {
        if !input.valid(r as u32) {
            continue;
        }
        // SAFETY: EncodeInput pointer-class contract.
        let payload = unsafe { varlena_payload(input.datums[r as usize])? };
        let num = Num::from_payload(payload);
        if num.is_special() {
            return Ok(None);
        }
        return Ok(Some(num.dscale()));
    }
    Ok(None)
}

pub struct PackedNumericEncoder {
    pub scale: i32,
    /// Elected mantissa delta width (stream-level).
    pub width: u8,
    /// Encode-side staging for the granule's mantissa words.
    mantissas: Vec<u64>,
}

impl PackedNumericEncoder {
    pub fn new(scale: i32, width: u8) -> PackedNumericEncoder {
        PackedNumericEncoder {
            scale,
            width,
            mantissas: Vec::with_capacity(GRANULE_ROWS as usize),
        }
    }
}

impl GranuleEncoder for PackedNumericEncoder {
    fn key(&self) -> KernelKey {
        KernelKey {
            encoding: EncodingId::PackedNumeric.as_u16(),
            class: CLASS_VARLENA,
            width: self.width,
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
        self.mantissas.clear();
        for r in 0..rows {
            let m = if input.valid(r as u32) {
                mantissa_at(input.datums[r], self.scale)?.ok_or(FormatError::EncodeContract {
                    detail: "value refused the elected scale",
                })?
            } else {
                0
            };
            self.mantissas.push(m as u64);
        }
        {
            let buf = w.payload();
            buf.extend_from_slice(&self.scale.to_le_bytes());
            buf.extend_from_slice(&0u32.to_le_bytes());
        }
        let mant_input = EncodeInput {
            class: StorageClass::ByvalWord {
                width: 8,
                signed: true,
            },
            rows: input.rows,
            datums: &self.mantissas,
            validity: input.validity,
        };
        encode_i64_frames(&mant_input, w, self.width, true)
    }
}

// ---------------------------------------------------------------------------
// decode
// ---------------------------------------------------------------------------

fn granule_scale(ctx: &KernelCtx<'_>) -> FormatResult<i32> {
    let hdr = open_section(ctx.bytes, EncodingId::PackedNumeric.as_u16())?;
    let payload = payload_region(&hdr, ctx.bytes)?;
    let fbase = frame_base(&hdr, ctx.bytes, ctx.granule_in_extent)?;
    let first = frame_start(ctx.frame_table, fbase)?;
    if first < SCALE_HDR_LEN {
        return Err(FormatError::Bounds {
            at: "packed-numeric scale header",
        });
    }
    let sh = &payload[first - SCALE_HDR_LEN..first];
    let scale = i32::from_le_bytes(sh[..4].try_into().expect("len 4"));
    if !(0..=DSCALE_MAX).contains(&scale) {
        return Err(FormatError::Corrupt {
            at: "packed-numeric scale",
        });
    }
    Ok(scale)
}

/// Rebuild numeric images for the decoded mantissa words in `datums[..n]`,
/// arena-out. Null rows (per `row_of`) are skipped — their slots stay
/// unspecified (validity is the only truth).
fn rebuild(
    ctx: &KernelCtx<'_>,
    scale: i32,
    n: usize,
    row_of: impl Fn(usize) -> u32,
    datums: &mut [u64],
    arena: &mut ByteArena<'_>,
) -> FormatResult<()> {
    let valid = |row: u32| -> bool {
        match ctx.validity_bytes {
            None => true,
            Some(bits) => bits
                .get((row / 8) as usize)
                .is_some_and(|b| b >> (row % 8) & 1 == 1),
        }
    };
    for i in 0..n {
        if !valid(row_of(i)) {
            continue;
        }
        let m = datums[i] as i64;
        if m.unsigned_abs() > FIXED_SCALE_MANT_ABS_MAX as u64 {
            return Err(FormatError::Corrupt {
                at: "packed-numeric mantissa",
            });
        }
        let image = fixed_scale_unpack(m, scale).map_err(|_| FormatError::Corrupt {
            at: "packed-numeric unpack",
        })?;
        datums[i] = arena.alloc_varlena(image.payload())?;
    }
    Ok(())
}

fn pn_dec_full<const W: usize>(ctx: &KernelCtx<'_>, out: &mut DecodeOut<'_>) -> FormatResult<u32> {
    let scale = granule_scale(ctx)?;
    let n = dec_full_for::<W>(ctx, EncodingId::PackedNumeric.as_u16(), out.datums)? as usize;
    rebuild(ctx, scale, n, |i| i as u32, out.datums, &mut out.arena)?;
    Ok(n as u32)
}

fn pn_dec_sel<const W: usize>(
    ctx: &KernelCtx<'_>,
    sel: &Selection<'_>,
    out: &mut DecodeOut<'_>,
) -> FormatResult<u32> {
    let scale = granule_scale(ctx)?;
    let n = dec_sel_for::<W>(ctx, EncodingId::PackedNumeric.as_u16(), sel, out.datums)? as usize;
    rebuild(
        ctx,
        scale,
        n,
        |i| sel.rows[i] as u32,
        out.datums,
        &mut out.arena,
    )?;
    Ok(n as u32)
}

fn pn_meta(ctx: &KernelCtx<'_>, probe: &MetaProbe) -> FormatResult<MetaAnswer> {
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

fn pn_validity(ctx: &KernelCtx<'_>, out: &mut [u64]) -> FormatResult<ValidityVerdict> {
    validity_from_ctx(ctx, out)
}

const fn pn_vt<const W: usize>() -> CodecVtable {
    CodecVtable {
        key: KernelKey {
            encoding: EncodingId::PackedNumeric as u16,
            class: CLASS_VARLENA,
            width: W as u8,
        },
        decode_full: pn_dec_full::<W>,
        decode_sel: pn_dec_sel::<W>,
        decode_codes: refuse_decode_codes,
        dict_handle: refuse_dict_handle,
        meta_probe: pn_meta,
        validity: pn_validity,
    }
}

/// One vtable per legal mantissa width — the full {1..=8} ladder: the
/// mantissa frames ARE BYTE_FOR frames, so the SB-3 width widening carries
/// through here (the election derives widths from `granule_min_width`).
pub static VT_PACKED_NUMERIC: [CodecVtable; 8] = [
    pn_vt::<1>(),
    pn_vt::<2>(),
    pn_vt::<3>(),
    pn_vt::<4>(),
    pn_vt::<5>(),
    pn_vt::<6>(),
    pn_vt::<7>(),
    pn_vt::<8>(),
];
