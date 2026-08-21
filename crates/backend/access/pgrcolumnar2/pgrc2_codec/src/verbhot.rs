//! Hot VERBATIM decode kernels (gate-3, M3 exit §2.3 — the licensed
//! rewrite the lib.rs stop-rule note reserved for exactly this microbench
//! evidence: verbatim int8/f64 priced 0.03× through the reference witness).
//!
//! Wire behavior is FROZEN (spec §6.7/§6.8) and stays with the reference
//! codec (`pgrc2_format::verbatim`) — the ABI witness keeps encode AND its
//! naive decode bodies, which remain the differential ORACLE
//! (`tests/kernel_diff.rs` pins hot ≡ reference per (class × width) on
//! random + adversarial corpora). This module only re-reads the same frozen
//! layout in the gate-3 kernel shape:
//!
//! - one WIDTH-MONOMORPHIZED kernel per vtable entry (the reference wired
//!   every per-width entry to ONE generic body doing a class match + a
//!   runtime-width `read_le` memcpy per value);
//! - addressing hoisted out of the inner loop: `value_base`/`frame_base`
//!   resolve once per granule, frames once per frame — the inner loop is a
//!   flat `zip(chunks_exact(W))` slice widen (the old format's proven NEON
//!   idiom, `pgrcolumnar` reader.rs widen loops);
//! - varlena stays MATERIALIZED per the arena law (R1–R6 + StrView §7b —
//!   the zero-copy gather is the phase-2 charter, NOT this lane): the hot
//!   kernel prices one bulk region copy per frame instead of a per-value
//!   frame-table walk, with a per-value fallback that replicates reference
//!   semantics exactly whenever a frame is non-canonical (overflow refs,
//!   unaligned/irregular entries), so hot ≡ reference on every input.
//!
//! CONST stays on the reference vtables (0.98× parity at the exit face —
//! nothing to recover).

use crate::section::{
    extend_word, frame_base, frame_start, open_section, payload_region, read_le_w, value_base,
};
use pgrc2_format::abi::{
    refuse_decode_codes, refuse_dict_handle, validity_from_ctx, ByteArena, CodecVtable, DecodeOut,
    KernelCtx, KernelKey, Selection, ValidityVerdict,
};
use pgrc2_format::class::{CLASS_BOOL, CLASS_BYVAL, CLASS_F32, CLASS_F64, CLASS_FIXED};
use pgrc2_format::enc::EncodingId;
use pgrc2_format::geom::{FRAME_VALUES, GRANULE_ROWS};
use pgrc2_format::meta::{MetaAnswer, MetaProbe};
use pgrc2_format::part::{OVERFLOW_MARK, STREAMF_SIGNED};
use pgrc2_format::wire::{varlena_4b_u_payload_len, varlena_entry_at};
use pgrc2_format::{FormatError, FormatResult};

// ---------------------------------------------------------------------------
// shared open (once per kernel call = once per granule face)
// ---------------------------------------------------------------------------

struct WordView<'a> {
    payload: &'a [u8],
    /// Values before this granule (element index base).
    vbase: u64,
    values: u32,
}

fn open_words<'a>(ctx: &KernelCtx<'a>) -> FormatResult<WordView<'a>> {
    let hdr = open_section(ctx.bytes, EncodingId::Verbatim.as_u16())?;
    Ok(WordView {
        payload: payload_region(&hdr, ctx.bytes)?,
        vbase: value_base(&hdr, ctx.bytes, ctx.granule_in_extent)?,
        values: ctx.values,
    })
}

/// The granule's contiguous stride-`S` body, bounds proven once.
fn word_body<'a, const S: usize>(v: &WordView<'a>, n: usize) -> FormatResult<&'a [u8]> {
    let start = (v.vbase as usize)
        .checked_mul(S)
        .ok_or(FormatError::Bounds {
            at: "verbatim word payload",
        })?;
    let end = start.checked_add(n * S).ok_or(FormatError::Bounds {
        at: "verbatim word payload",
    })?;
    v.payload.get(start..end).ok_or(FormatError::Bounds {
        at: "verbatim word payload",
    })
}

// ---------------------------------------------------------------------------
// byval / f32 / f64 (word classes): flat widen, sign resolved per granule
// ---------------------------------------------------------------------------

fn vh_full_byval<const W: usize>(
    ctx: &KernelCtx<'_>,
    out: &mut DecodeOut<'_>,
) -> FormatResult<u32> {
    let v = open_words(ctx)?;
    let n = v.values as usize;
    if out.datums.len() < n {
        return Err(FormatError::Bounds {
            at: "decode_full out",
        });
    }
    let body = word_body::<W>(&v, n)?;
    let signed = ctx.flags & STREAMF_SIGNED != 0;
    let dst = &mut out.datums[..n];
    if W == 8 || !signed {
        for (o, c) in dst.iter_mut().zip(body.chunks_exact(W)) {
            *o = read_le_w::<W>(c);
        }
    } else {
        for (o, c) in dst.iter_mut().zip(body.chunks_exact(W)) {
            *o = extend_word(read_le_w::<W>(c), W as u8, true);
        }
    }
    Ok(v.values)
}

fn vh_sel_byval<const W: usize>(
    ctx: &KernelCtx<'_>,
    sel: &Selection<'_>,
    out: &mut DecodeOut<'_>,
) -> FormatResult<u32> {
    let v = open_words(ctx)?;
    if out.datums.len() < sel.rows.len() {
        return Err(FormatError::Bounds {
            at: "decode_sel out",
        });
    }
    let start = (v.vbase as usize)
        .checked_mul(W)
        .ok_or(FormatError::Bounds {
            at: "verbatim word payload",
        })?;
    let body = v.payload.get(start..).ok_or(FormatError::Bounds {
        at: "verbatim word payload",
    })?;
    let signed = ctx.flags & STREAMF_SIGNED != 0;
    for (o, &r16) in out.datums.iter_mut().zip(sel.rows.iter()) {
        let r = r16 as u32;
        if r >= v.values {
            return Err(FormatError::Bounds { at: "verbatim row" });
        }
        let off = r as usize * W;
        let c = body.get(off..off + W).ok_or(FormatError::Bounds {
            at: "verbatim word payload",
        })?;
        let raw = read_le_w::<W>(c);
        *o = if W == 8 || !signed {
            raw
        } else {
            extend_word(raw, W as u8, true)
        };
    }
    Ok(sel.rows.len() as u32)
}

// ---------------------------------------------------------------------------
// bool: byte -> 0/1 normalization (spec §6.7)
// ---------------------------------------------------------------------------

fn vh_full_bool(ctx: &KernelCtx<'_>, out: &mut DecodeOut<'_>) -> FormatResult<u32> {
    let v = open_words(ctx)?;
    let n = v.values as usize;
    if out.datums.len() < n {
        return Err(FormatError::Bounds {
            at: "decode_full out",
        });
    }
    let body = word_body::<1>(&v, n)?;
    for (o, &b) in out.datums[..n].iter_mut().zip(body.iter()) {
        *o = (b != 0) as u64;
    }
    Ok(v.values)
}

fn vh_sel_bool(
    ctx: &KernelCtx<'_>,
    sel: &Selection<'_>,
    out: &mut DecodeOut<'_>,
) -> FormatResult<u32> {
    let v = open_words(ctx)?;
    if out.datums.len() < sel.rows.len() {
        return Err(FormatError::Bounds {
            at: "decode_sel out",
        });
    }
    let start = v.vbase as usize;
    let body = v.payload.get(start..).ok_or(FormatError::Bounds {
        at: "verbatim word payload",
    })?;
    for (o, &r16) in out.datums.iter_mut().zip(sel.rows.iter()) {
        let r = r16 as u32;
        if r >= v.values {
            return Err(FormatError::Bounds { at: "verbatim row" });
        }
        let b = *body.get(r as usize).ok_or(FormatError::Bounds {
            at: "verbatim word payload",
        })?;
        *o = (b != 0) as u64;
    }
    Ok(sel.rows.len() as u32)
}

// ---------------------------------------------------------------------------
// fixed(N): arena images, stride hoisted (no class match per value)
// ---------------------------------------------------------------------------

fn vh_full_fixed(ctx: &KernelCtx<'_>, out: &mut DecodeOut<'_>) -> FormatResult<u32> {
    let v = open_words(ctx)?;
    let n = v.values as usize;
    if out.datums.len() < n {
        return Err(FormatError::Bounds {
            at: "decode_full out",
        });
    }
    let s = ctx.fixed_len as usize;
    let start = (v.vbase as usize)
        .checked_mul(s)
        .ok_or(FormatError::Bounds {
            at: "verbatim fixed payload",
        })?;
    let end = start.checked_add(n * s).ok_or(FormatError::Bounds {
        at: "verbatim fixed payload",
    })?;
    let body = v.payload.get(start..end).ok_or(FormatError::Bounds {
        at: "verbatim fixed payload",
    })?;
    for (o, img) in out.datums[..n].iter_mut().zip(body.chunks_exact(s.max(1))) {
        *o = out.arena.alloc_fixed(&img[..s])?;
    }
    // chunks_exact(s.max(1)) yields n chunks only when s > 0; the s == 0
    // degenerate (never encoded) still needs its datums populated.
    if s == 0 {
        for o in out.datums[..n].iter_mut() {
            *o = out.arena.alloc_fixed(&[])?;
        }
    }
    Ok(v.values)
}

fn vh_sel_fixed(
    ctx: &KernelCtx<'_>,
    sel: &Selection<'_>,
    out: &mut DecodeOut<'_>,
) -> FormatResult<u32> {
    let v = open_words(ctx)?;
    if out.datums.len() < sel.rows.len() {
        return Err(FormatError::Bounds {
            at: "decode_sel out",
        });
    }
    let s = ctx.fixed_len as usize;
    for (o, &r16) in out.datums.iter_mut().zip(sel.rows.iter()) {
        let r = r16 as u32;
        if r >= v.values {
            return Err(FormatError::Bounds { at: "verbatim row" });
        }
        let off = (v.vbase as usize + r as usize) * s;
        let img = v.payload.get(off..off + s).ok_or(FormatError::Bounds {
            at: "verbatim fixed payload",
        })?;
        *o = out.arena.alloc_fixed(img)?;
    }
    Ok(sel.rows.len() as u32)
}

// ---------------------------------------------------------------------------
// varlena: frame hoisted; canonical frames bulk-copy, everything else takes
// the reference-identical per-value path
// ---------------------------------------------------------------------------

struct VarlenaView<'a> {
    payload: &'a [u8],
    fbase: u32,
    values: u32,
    frame_table: Option<&'a [u32]>,
    overflow: Option<&'a [u8]>,
}

fn open_varlena<'a>(ctx: &KernelCtx<'a>) -> FormatResult<VarlenaView<'a>> {
    let hdr = open_section(ctx.bytes, EncodingId::Verbatim.as_u16())?;
    Ok(VarlenaView {
        payload: payload_region(&hdr, ctx.bytes)?,
        fbase: frame_base(&hdr, ctx.bytes, ctx.granule_in_extent)?,
        values: ctx.values,
        frame_table: ctx.frame_table,
        overflow: ctx.overflow,
    })
}

/// Slot-table entry `i` of a frame slice (frame-relative entry offset).
#[inline]
fn slot(frame: &[u8], i: usize) -> FormatResult<u32> {
    let b = frame
        .get(i * 4..i * 4 + 4)
        .ok_or(FormatError::Bounds { at: "slot table" })?;
    Ok(u32::from_le_bytes(b.try_into().expect("len 4")))
}

/// One value through the reference-identical per-value path (inline entry
/// or overflow ref) — the fallback arm and the `decode_sel` body.
fn varlena_value(
    frame: &[u8],
    entry_off: usize,
    overflow: Option<&[u8]>,
    arena: &mut ByteArena<'_>,
) -> FormatResult<u64> {
    let head = frame
        .get(entry_off..entry_off + 4)
        .ok_or(FormatError::Bounds {
            at: "varlena entry",
        })?;
    let first = u32::from_le_bytes(head.try_into().expect("len 4"));
    if first == OVERFLOW_MARK {
        // OverflowRef: { mark, total_len, ovf_off } (spec §6.7).
        let rec = frame
            .get(entry_off..entry_off + 16)
            .ok_or(FormatError::Bounds { at: "overflow ref" })?;
        let total_len = u32::from_le_bytes(rec[4..8].try_into().expect("len 4")) as usize;
        let ovf_off = u64::from_le_bytes(rec[8..16].try_into().expect("len 8")) as usize;
        let ovf = overflow.ok_or(FormatError::Corrupt {
            at: "overflow ref without stream",
        })?;
        let (_, payload) = varlena_entry_at(ovf, ovf_off, "overflow entry")?;
        if payload.len() != total_len {
            return Err(FormatError::Corrupt {
                at: "overflow entry length",
            });
        }
        arena.alloc_varlena(payload)
    } else {
        let (_, payload) = varlena_entry_at(frame, entry_off, "varlena entry")?;
        arena.alloc_varlena(payload)
    }
}

/// Decode one frame's `vif` values into `out`, arena-out.
///
/// Canonical frames (what the frozen encoder emits: 8-aligned inline
/// entries, in slot order inside `[entries_base, end)`, no overflow refs)
/// take ONE arena alloc + ONE region memcpy + a flat datum-fixup loop;
/// entry 8-alignment is preserved because both the arena base and every
/// canonical slot offset are ≡ 0 (mod 8) relative to their regions
/// (StrView §7b holds). ANY irregularity — overflow refs included — falls
/// back to the per-value path, which replicates the reference codec
/// byte-for-byte (same values, same typed refusals).
fn varlena_frame_full(
    frame: &[u8],
    vif: usize,
    overflow: Option<&[u8]>,
    out: &mut [u64],
    arena: &mut ByteArena<'_>,
) -> FormatResult<()> {
    let entries_base = ((vif + 1) * 4).div_ceil(8) * 8;
    // Canonical-shape scan: cheap u32 reads, no value bytes touched. The
    // slot offsets are buffered so the fixup pass never re-reads the
    // table.
    let mut offs = [0u32; FRAME_VALUES as usize];
    let end = slot(frame, vif)? as usize;
    let mut canonical = end >= entries_base && end <= frame.len();
    if canonical {
        for (i, off) in offs[..vif].iter_mut().enumerate() {
            let eo = slot(frame, i)? as usize;
            *off = eo as u32;
            if eo % 8 != 0 || eo < entries_base || eo + 4 > end {
                canonical = false;
                break;
            }
            let head = u32::from_le_bytes(frame[eo..eo + 4].try_into().expect("len 4"));
            if head == OVERFLOW_MARK {
                canonical = false;
                break;
            }
            let Ok(len) = varlena_4b_u_payload_len(head, "varlena entry") else {
                canonical = false;
                break;
            };
            if eo + 4 + len as usize > end {
                canonical = false;
                break;
            }
        }
    }
    if canonical {
        let region = &frame[entries_base..end];
        let dst = arena.alloc(region.len())?;
        dst.copy_from_slice(region);
        let base_ptr = dst.as_ptr() as u64;
        for (o, &eo) in out[..vif].iter_mut().zip(offs[..vif].iter()) {
            *o = base_ptr + (eo as usize - entries_base) as u64;
        }
    } else {
        for (i, o) in out[..vif].iter_mut().enumerate() {
            let eo = slot(frame, i)? as usize;
            *o = varlena_value(frame, eo, overflow, arena)?;
        }
    }
    Ok(())
}

fn vh_full_varlena(ctx: &KernelCtx<'_>, out: &mut DecodeOut<'_>) -> FormatResult<u32> {
    let v = open_varlena(ctx)?;
    let n = v.values as usize;
    if out.datums.len() < n {
        return Err(FormatError::Bounds {
            at: "decode_full out",
        });
    }
    let ft = v.frame_table.ok_or(FormatError::Corrupt {
        at: "varlena needs frame table",
    })?;
    let mut v0 = 0usize;
    let mut f = 0u32;
    while v0 < n {
        let vif = (n - v0).min(FRAME_VALUES as usize);
        let fs = frame_start(Some(ft), v.fbase + f)?;
        let frame = v
            .payload
            .get(fs..)
            .ok_or(FormatError::Bounds { at: "frame offset" })?;
        varlena_frame_full(
            frame,
            vif,
            v.overflow,
            &mut out.datums[v0..v0 + vif],
            &mut out.arena,
        )?;
        v0 += vif;
        f += 1;
    }
    Ok(v.values)
}

fn vh_sel_varlena(
    ctx: &KernelCtx<'_>,
    sel: &Selection<'_>,
    out: &mut DecodeOut<'_>,
) -> FormatResult<u32> {
    let v = open_varlena(ctx)?;
    if out.datums.len() < sel.rows.len() {
        return Err(FormatError::Bounds {
            at: "decode_sel out",
        });
    }
    let ft = v.frame_table.ok_or(FormatError::Corrupt {
        at: "varlena needs frame table",
    })?;
    // Survivor-only: resolve each touched frame once (ascending selection).
    let mut cur_frame = u32::MAX;
    let mut frame: &[u8] = &[];
    for (o, &r16) in out.datums.iter_mut().zip(sel.rows.iter()) {
        let r = r16 as u32;
        if r >= v.values {
            return Err(FormatError::Bounds { at: "verbatim row" });
        }
        let f = r / FRAME_VALUES;
        if f != cur_frame {
            let fs = frame_start(Some(ft), v.fbase + f)?;
            frame = v
                .payload
                .get(fs..)
                .ok_or(FormatError::Bounds { at: "frame offset" })?;
            cur_frame = f;
        }
        let eo = slot(frame, (r % FRAME_VALUES) as usize)? as usize;
        *o = varlena_value(frame, eo, v.overflow, &mut out.arena)?;
    }
    Ok(sel.rows.len() as u32)
}

// ---------------------------------------------------------------------------
// meta / validity (reference-identical semantics)
// ---------------------------------------------------------------------------

fn vh_meta(ctx: &KernelCtx<'_>, probe: &MetaProbe) -> FormatResult<MetaAnswer> {
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

fn vh_validity(ctx: &KernelCtx<'_>, out: &mut [u64]) -> FormatResult<ValidityVerdict> {
    validity_from_ctx(ctx, out)
}

// ---------------------------------------------------------------------------
// vtables — same keys the reference registered; the registry now points
// them here (dispatch.rs), the reference stays the oracle
// ---------------------------------------------------------------------------

const fn vh_vt_byval<const W: usize>() -> CodecVtable {
    CodecVtable {
        key: KernelKey {
            encoding: EncodingId::Verbatim as u16,
            class: CLASS_BYVAL,
            width: W as u8,
        },
        decode_full: vh_full_byval::<W>,
        decode_sel: vh_sel_byval::<W>,
        decode_codes: refuse_decode_codes,
        dict_handle: refuse_dict_handle,
        meta_probe: vh_meta,
        validity: vh_validity,
    }
}

const fn vh_vt(class: u8, width: u8) -> CodecVtable {
    CodecVtable {
        key: KernelKey {
            encoding: EncodingId::Verbatim as u16,
            class,
            width,
        },
        decode_full: vh_full_byval::<8>,
        decode_sel: vh_sel_byval::<8>,
        decode_codes: refuse_decode_codes,
        dict_handle: refuse_dict_handle,
        meta_probe: vh_meta,
        validity: vh_validity,
    }
}

pub static VT_VERBATIM_HOT: [CodecVtable; 8] = [
    vh_vt_byval::<1>(),
    vh_vt_byval::<2>(),
    vh_vt_byval::<4>(),
    vh_vt_byval::<8>(),
    // F32: raw 4-byte image zero-extended — the unsigned w4 widen (spec
    // §6.7; float streams never set STREAMF_SIGNED).
    {
        let mut vt = vh_vt(CLASS_F32, 4);
        vt.decode_full = vh_full_byval::<4>;
        vt.decode_sel = vh_sel_byval::<4>;
        vt
    },
    // F64: raw 8-byte image — the w8 copy.
    vh_vt(CLASS_F64, 8),
    // Bool: byte -> 0/1 normalization.
    {
        let mut vt = vh_vt(CLASS_BOOL, 1);
        vt.decode_full = vh_full_bool;
        vt.decode_sel = vh_sel_bool;
        vt
    },
    // Fixed(N): arena images, stride hoisted.
    {
        let mut vt = vh_vt(CLASS_FIXED, 0);
        vt.decode_full = vh_full_fixed;
        vt.decode_sel = vh_sel_fixed;
        vt
    },
];

pub static VT_VERBATIM_VARLENA_HOT: CodecVtable = {
    let mut vt = vh_vt(pgrc2_format::class::CLASS_VARLENA, 0);
    vt.decode_full = vh_full_varlena;
    vt.decode_sel = vh_sel_varlena;
    vt
};

// The bulk path's alignment argument is frame-relative (slot offsets and
// entries_base are both ≡ 0 mod 8 relative to the frame start), so it
// holds for any source buffer alignment; nothing further to pin here —
// the arena base is 8-aligned by `ByteArena`'s own debug assertion.
