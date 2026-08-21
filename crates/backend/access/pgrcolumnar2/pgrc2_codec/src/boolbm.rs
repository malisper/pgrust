//! BOOL_BITMAP — 1-bit bool payload (spec §4 id 7; layout = spec §6.6: one
//! bit per value, LSB-first, laid per-granule contiguously and byte-padded
//! at each granule boundary — the same frozen layout validity substreams
//! use). The frame table marks one entry per granule (the granule bitmap
//! start; the uniform §6.4 mechanism).
//!
//! Null slots encode bit 0 (spec §6.6 canonical placeholder); validity is
//! the only truth.

use crate::section::{frame_start, granule_frame_base, open_section, payload_region};
use pgrc2_format::abi::{
    refuse_decode_codes, refuse_dict_handle, validity_from_ctx, CodecVtable, DecodeOut,
    EncodeInput, GranuleEncoder, KernelCtx, KernelKey, Selection, ValidityVerdict,
};
use pgrc2_format::class::CLASS_BOOL;
use pgrc2_format::enc::EncodingId;
use pgrc2_format::geom::GRANULE_ROWS;
use pgrc2_format::meta::{MetaAnswer, MetaProbe};
use pgrc2_format::part::{OverflowSink, StreamSectionWriter};
use pgrc2_format::{FormatError, FormatResult};

pub struct BoolBitmapEncoder;

impl GranuleEncoder for BoolBitmapEncoder {
    fn key(&self) -> KernelKey {
        KernelKey {
            encoding: EncodingId::BoolBitmap.as_u16(),
            class: CLASS_BOOL,
            width: 1,
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
        w.begin_frame();
        let buf = w.payload();
        for byte0 in (0..rows).step_by(8) {
            let mut b: u8 = 0;
            for bit in 0..8 {
                let r = byte0 + bit;
                if r < rows && input.valid(r as u32) && input.datums[r] != 0 {
                    b |= 1 << bit;
                }
            }
            buf.push(b);
        }
        w.end_granule(input.rows);
        Ok(())
    }
}

struct BitmapView<'a> {
    bits: &'a [u8],
    values: u32,
}

fn open_view<'a>(ctx: &KernelCtx<'a>) -> FormatResult<BitmapView<'a>> {
    let hdr = open_section(ctx.bytes, EncodingId::BoolBitmap.as_u16())?;
    let payload = payload_region(&hdr, ctx.bytes)?;
    let f = granule_frame_base(&hdr, ctx.bytes, ctx.granule_in_extent)?;
    let fs = frame_start(ctx.frame_table, f)?;
    let need = (ctx.values as usize).div_ceil(8);
    let bits = payload.get(fs..fs + need).ok_or(FormatError::Bounds {
        at: "bool bitmap payload",
    })?;
    Ok(BitmapView {
        bits,
        values: ctx.values,
    })
}

fn bb_dec_full(ctx: &KernelCtx<'_>, out: &mut DecodeOut<'_>) -> FormatResult<u32> {
    let v = open_view(ctx)?;
    if out.datums.len() < v.values as usize {
        return Err(FormatError::Bounds {
            at: "decode_full out",
        });
    }
    for r in 0..v.values as usize {
        out.datums[r] = (v.bits[r / 8] >> (r % 8) & 1) as u64;
    }
    Ok(v.values)
}

fn bb_dec_sel(
    ctx: &KernelCtx<'_>,
    sel: &Selection<'_>,
    out: &mut DecodeOut<'_>,
) -> FormatResult<u32> {
    let v = open_view(ctx)?;
    if out.datums.len() < sel.rows.len() {
        return Err(FormatError::Bounds {
            at: "decode_sel out",
        });
    }
    for (i, &r16) in sel.rows.iter().enumerate() {
        let r = r16 as usize;
        if r as u32 >= v.values {
            return Err(FormatError::Bounds {
                at: "decode_sel row",
            });
        }
        out.datums[i] = (v.bits[r / 8] >> (r % 8) & 1) as u64;
    }
    Ok(sel.rows.len() as u32)
}

fn bb_meta(ctx: &KernelCtx<'_>, probe: &MetaProbe) -> FormatResult<MetaAnswer> {
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

fn bb_validity(ctx: &KernelCtx<'_>, out: &mut [u64]) -> FormatResult<ValidityVerdict> {
    validity_from_ctx(ctx, out)
}

pub static VT_BOOL_BITMAP: CodecVtable = CodecVtable {
    key: KernelKey {
        encoding: EncodingId::BoolBitmap as u16,
        class: CLASS_BOOL,
        width: 1,
    },
    decode_full: bb_dec_full,
    decode_sel: bb_dec_sel,
    decode_codes: refuse_decode_codes,
    dict_handle: refuse_dict_handle,
    meta_probe: bb_meta,
    validity: bb_validity,
};
