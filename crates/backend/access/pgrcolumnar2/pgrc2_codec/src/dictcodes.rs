//! DICT_CODES — bit-packed GLOBAL codes, per-granule base+width (spec §6.11
//! FROZEN). Block = `{ base: u32, width: u8 (0..=32), pad: [u8;3], packed
//! bits }`, LSB-first, `code = base + extracted`; width 0 ⇒ no payload (all
//! rows = base). Codes are global (spec §7): stored codes are final — no
//! stitch, no remap; epoch identity is part-scoped, so no claim splits one.
//!
//! Frame addressing is byte-exact by the freeze (1024 × width ≡ 0 mod 8):
//! frame f of a granule starts at block payload byte `f × 128 × width`, and
//! the frame table records exactly those starts (uniform mechanism).
//!
//! Faces:
//! - `decode_codes` extracts global codes with NO dict bytes touched — the
//!   dict-lane late-materialization face (`lx_vec` `DictCodes` overlays);
//! - `decode_full`/`decode_sel` GATHER through the ctx dict sections
//!   (byte-rank-sorted entries, varlena-shaped per StrView §7b). Varlena
//!   class is ZERO-COPY (dekern phase 2): datums are validated pointers to
//!   the entry images inside the resident dict payload section
//!   (`dict::dict_entry_datum` — see its aliasing law and the `DecodeOut`
//!   pointer-class output law); fixed class still copies through the arena.
//! - `dict_handle` reports the layout facts for M3-F's lazy framed handle.
//!
//! The publishability lattice (`STREAMF_DICT_EXEC`) and the O-6 zero-null
//! posture bind at the execution boundary (M3-G) — this codec records and
//! serves; it never decides publishability.
//!
//! Null slots encode the granule base (delta 0 — spec §6.6).

use crate::section::{frame_base, frame_start, open_section, payload_region, read_le};
use pgrc2_format::abi::{
    validity_from_ctx, CodecVtable, DecodeOut, EncodeInput, GranuleEncoder, KernelCtx, KernelKey,
    Selection, ValidityVerdict,
};
use pgrc2_format::class::{CLASS_FIXED, CLASS_VARLENA};
use pgrc2_format::dict::{dict_entry, dict_entry_datum, DictLayout, DictSections};
use pgrc2_format::enc::EncodingId;
use pgrc2_format::geom::{DICT_FRAME_ENTRIES, FRAME_VALUES, GRANULE_ROWS};
use pgrc2_format::meta::{MetaAnswer, MetaProbe};
use pgrc2_format::part::{OverflowSink, StreamSectionWriter};
use pgrc2_format::{FormatError, FormatResult};

const BLOCK_HDR_LEN: usize = 8; // base(4) + width(1) + pad(3)

fn code_bits(range: u32) -> u8 {
    (32 - range.leading_zeros()) as u8
}

/// The granule's elected width: exact `ceil(log2(range+1))`, optionally
/// rounded UP to the next byte boundary (8/16/24/32 — the pgrc21-widths
/// experiment: byte-aligned streams decode via plain slice widens with no
/// shift/mask, at a bounded size cost). Width 0 (constant granule) stays 0
/// in both postures — it carries no payload and is already the fastest
/// path. Byte-aligned widths are a SUBSET of the frozen §6.11 envelope
/// (width 0..=32): readers need no change.
fn elected_width(base: u32, max: u32, byte_align: bool) -> u8 {
    if max == base {
        return 0;
    }
    let w = code_bits(max - base);
    if byte_align {
        w.div_ceil(8) * 8
    } else {
        w
    }
}

/// Exact payload bytes for one granule of codes (election arm) and the
/// granule's width — pure function of the input. `byte_align` mirrors the
/// encoder's posture so the election prices the bytes actually sealed.
pub fn granule_block_facts(input: &EncodeInput<'_>, byte_align: bool) -> (usize, u8) {
    let (base, max) = base_max(input);
    let width = elected_width(base, max, byte_align);
    let bits = input.rows as usize * width as usize;
    (BLOCK_HDR_LEN + bits.div_ceil(8), width)
}

fn base_max(input: &EncodeInput<'_>) -> (u32, u32) {
    let mut base = u32::MAX;
    let mut max = 0u32;
    let mut any = false;
    for r in 0..input.rows as usize {
        if !input.valid(r as u32) {
            continue;
        }
        any = true;
        let c = input.datums[r] as u32;
        base = base.min(c);
        max = max.max(c);
    }
    if !any {
        (0, 0)
    } else {
        (base, max)
    }
}

/// [`base_max`] with the u32 code-domain check fused into the same walk
/// (SEAL-FUSION — encode's separate bounds pass folded in).
fn base_max_checked(input: &EncodeInput<'_>) -> FormatResult<(u32, u32)> {
    let mut base = u32::MAX;
    let mut max = 0u32;
    let mut any = false;
    for r in 0..input.rows as usize {
        if !input.valid(r as u32) {
            continue;
        }
        let d = input.datums[r];
        if d > u32::MAX as u64 {
            return Err(FormatError::EncodeContract {
                detail: "dict code exceeds u32",
            });
        }
        any = true;
        let c = d as u32;
        base = base.min(c);
        max = max.max(c);
    }
    Ok(if !any { (0, 0) } else { (base, max) })
}

/// The dict-code granule encoder: datum words carry GLOBAL codes (the
/// writer resolved them against its byte-rank-sorted dictionary build —
/// M3-D's seal path).
pub struct DictCodesEncoder {
    /// Class of the materialized values (varlena or fixed) — the key echo.
    pub class: u8,
    /// Stream-level max code width (the §6.3 width byte); granule blocks
    /// carry their own widths and may be narrower.
    pub max_width: u8,
    /// Round every granule width up to the next byte boundary (8/16/24/32;
    /// width 0 stays 0). The election passed the same posture to
    /// [`granule_block_facts`], so `max_width` already reflects it.
    pub byte_align: bool,
}

impl GranuleEncoder for DictCodesEncoder {
    fn key(&self) -> KernelKey {
        KernelKey {
            encoding: EncodingId::DictCodes.as_u16(),
            class: self.class,
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
        // SEAL-FUSION: the u32 bounds check rides the base/max reduction —
        // one walk over the codes instead of two (same refusal, same
        // outcome: the pre-fusion bounds pass ran first but any oversized
        // code fails the granule identically either way).
        let (base, max) = base_max_checked(input)?;
        let width = elected_width(base, max, self.byte_align);
        if width > self.max_width {
            return Err(FormatError::EncodeContract {
                detail: "granule width exceeds stream width",
            });
        }
        {
            let buf = w.payload();
            buf.extend_from_slice(&base.to_le_bytes());
            buf.push(width);
            buf.extend_from_slice(&[0u8; 3]);
        }
        // Frame-by-frame bit emission: full frames end byte-aligned
        // (1024 × width ≡ 0 mod 8), so per-frame packing is byte-identical
        // to continuous packing and each frame start is markable.
        let mut f0 = 0usize;
        while f0 < rows {
            let n = (rows - f0).min(FRAME_VALUES as usize);
            w.begin_frame();
            if width > 0 {
                let buf = w.payload();
                let mut acc: u64 = 0;
                let mut nbits: u32 = 0;
                for r in f0..f0 + n {
                    let delta = if input.valid(r as u32) {
                        input.datums[r] as u32 - base
                    } else {
                        0 // placeholder: the granule base (spec §6.6)
                    };
                    acc |= (delta as u64) << nbits;
                    nbits += width as u32;
                    while nbits >= 8 {
                        buf.push(acc as u8);
                        acc >>= 8;
                        nbits -= 8;
                    }
                }
                if nbits > 0 {
                    buf.push(acc as u8);
                }
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

struct BlockView<'a> {
    base: u32,
    width: u8,
    /// Packed bits of the whole granule block.
    bits: &'a [u8],
    values: u32,
}

fn open_block<'a>(ctx: &KernelCtx<'a>) -> FormatResult<BlockView<'a>> {
    let hdr = open_section(ctx.bytes, EncodingId::DictCodes.as_u16())?;
    let payload = payload_region(&hdr, ctx.bytes)?;
    let fbase = frame_base(&hdr, ctx.bytes, ctx.granule_in_extent)?;
    let first = frame_start(ctx.frame_table, fbase)?;
    if first < BLOCK_HDR_LEN {
        return Err(FormatError::Bounds {
            at: "dict block header",
        });
    }
    let bh = payload
        .get(first - BLOCK_HDR_LEN..first)
        .ok_or(FormatError::Bounds {
            at: "dict block header",
        })?;
    let base = u32::from_le_bytes(bh[..4].try_into().expect("len 4"));
    let width = bh[4];
    if width > 32 {
        return Err(FormatError::Corrupt {
            at: "dict code width",
        });
    }
    let need = (ctx.values as usize * width as usize).div_ceil(8);
    let bits = payload
        .get(first..first + need)
        .ok_or(FormatError::Bounds {
            at: "dict block payload",
        })?;
    Ok(BlockView {
        base,
        width,
        bits,
        values: ctx.values,
    })
}

#[inline]
fn extract_code(v: &BlockView<'_>, i: u32) -> u32 {
    if v.width == 0 {
        return v.base;
    }
    let bitoff = i as usize * v.width as usize;
    let byte = bitoff / 8;
    let shift = (bitoff % 8) as u32;
    let end = (byte + 8).min(v.bits.len());
    let raw = read_le(&v.bits[byte..end]) >> shift;
    let mask = (1u64 << v.width) - 1;
    v.base + (raw & mask) as u32
}

/// Gate-3 code unpack (M3 exit §2.3 ruling: specialize the dict-code
/// unpack — the on-disk §6.11 block does NOT change): ONE dispatch on the
/// granule's width, then flat monomorphic loops. Byte-aligned widths are
/// pure slice widens (the old format's u16-code idiom); other widths run a
/// fixed 8-byte-window extract per value (the window always covers a code:
/// byte-granular offsets leave ≤7 slack bits and 7 + 32 ≤ 64) — no
/// variable-length `read_le` memcpy per value, which is what priced the
/// codes face at 0.03×.
fn unpack_codes(v: &BlockView<'_>, out: &mut [u32]) {
    unpack_codes_raw(v.base, v.width, v.bits, out)
}

/// The raw-slice unpack (same kernels): `bits` starts at the FIRST value's
/// bit 0 — callers slicing at a frame boundary get byte offsets for free
/// (frame law: 1024 × width ≡ 0 mod 8, so every frame starts byte-aligned).
fn unpack_codes_raw(base: u32, width: u8, bits: &[u8], out: &mut [u32]) {
    match width {
        0 => out.fill(base),
        8 => {
            for (o, &b) in out.iter_mut().zip(bits.iter()) {
                *o = base + b as u32;
            }
        }
        16 => {
            for (o, c) in out.iter_mut().zip(bits.chunks_exact(2)) {
                *o = base + u16::from_le_bytes(c.try_into().expect("len 2")) as u32;
            }
        }
        24 => {
            for (o, c) in out.iter_mut().zip(bits.chunks_exact(3)) {
                *o = base + (c[0] as u32 | (c[1] as u32) << 8 | (c[2] as u32) << 16);
            }
        }
        32 => {
            for (o, c) in out.iter_mut().zip(bits.chunks_exact(4)) {
                *o = base + u32::from_le_bytes(c.try_into().expect("len 4"));
            }
        }
        w => unpack_codes_bitwise(base, w, bits, out),
    }
}

fn unpack_codes_bitwise(base: u32, w: u8, bits: &[u8], out: &mut [u32]) {
    let wu = w as usize;
    let mask = (1u64 << w) - 1;
    // Fast region: every value whose fixed 8-byte load window is in
    // bounds (i*w/8 + 8 ≤ len ⟺ i ≤ (8·(len−8)+7)/w).
    let fast = if bits.len() >= 8 {
        (((bits.len() - 8) * 8 + 7) / wu + 1).min(out.len())
    } else {
        0
    };
    // Multi-value windows amortize the load: a u64 window at a byte
    // offset spans ≤7 slack bits + k·w payload bits, so k values fit
    // whenever 7 + k·w ≤ 64 — k=4 for w ≤ 14, k=2 for w ≤ 28. The
    // window byte of the group's FIRST value bounds the whole group.
    let group: usize = if wu <= 14 {
        4
    } else if wu <= 28 {
        2
    } else {
        1
    };
    let mut i = 0usize;
    if group > 1 {
        while i + group <= fast {
            let bitoff = i * wu;
            let byte = bitoff >> 3;
            let win =
                u64::from_le_bytes(bits[byte..byte + 8].try_into().expect("len 8")) >> (bitoff & 7);
            for (k, o) in out[i..i + group].iter_mut().enumerate() {
                *o = base + ((win >> (k * wu)) & mask) as u32;
            }
            i += group;
        }
    }
    let mut bitoff = i * wu;
    for o in out[i..fast].iter_mut() {
        let byte = bitoff >> 3;
        let win = u64::from_le_bytes(bits[byte..byte + 8].try_into().expect("len 8"));
        *o = base + ((win >> (bitoff & 7)) & mask) as u32;
        bitoff += wu;
    }
    // Bounded tail (identical math to `extract_code`).
    for o in out[fast..].iter_mut() {
        let byte = bitoff >> 3;
        let end = (byte + 8).min(bits.len());
        let raw = read_le(&bits[byte..end]) >> (bitoff & 7);
        *o = base + (raw & mask) as u32;
        bitoff += wu;
    }
}

fn dc_dec_codes(ctx: &KernelCtx<'_>, out: &mut [u32]) -> FormatResult<u32> {
    let v = open_block(ctx)?;
    let n = v.values as usize;
    if out.len() < n {
        return Err(FormatError::Bounds {
            at: "decode_codes out",
        });
    }
    unpack_codes(&v, &mut out[..n]);
    Ok(v.values)
}

fn need_dict<'a>(ctx: &KernelCtx<'a>) -> FormatResult<DictSections<'a>> {
    ctx.dict.ok_or(FormatError::Corrupt {
        at: "dict stream without dict sections",
    })
}

/// Fixed-class materialization: the arena-copy path (fixed-class dict
/// streams are format-legal; the writer's dict election is varlena-only
/// today, so this path is refusal/parity coverage, not a hot face).
fn materialize_fixed(
    ctx: &KernelCtx<'_>,
    d: &DictSections<'_>,
    code: u32,
    arena: &mut pgrc2_format::abi::ByteArena<'_>,
) -> FormatResult<u64> {
    let e = dict_entry(d, code)?;
    if e.bytes.len() != ctx.fixed_len as usize {
        return Err(FormatError::Corrupt {
            at: "dict entry fixed length",
        });
    }
    arena.alloc_fixed(e.bytes)
}

/// The zero-copy varlena gather over one already-unpacked code run: datums
/// are validated pointers to entry images inside `d.payload` (StrView §7b
/// views into the resident dictionary — no arena write, no byte copy). The
/// one-entry memo turns code runs (sorted banks are run-heavy) into a
/// compare + copy of the previous datum word.
#[inline]
fn gather_varlena(
    d: &DictSections<'_>,
    codes: &[u32],
    out: &mut [u64],
    memo: &mut (u32, u64),
) -> FormatResult<()> {
    for (o, &code) in out.iter_mut().zip(codes.iter()) {
        if code != memo.0 {
            *memo = (code, dict_entry_datum(d, code)?);
        }
        *o = memo.1;
    }
    Ok(())
}

/// One frame of codes on the stack: the gather working set stays L1-resident
/// and the kernel stays allocation-free (spec §19.2).
const GATHER_CHUNK: usize = FRAME_VALUES as usize;

fn dc_dec_full(ctx: &KernelCtx<'_>, out: &mut DecodeOut<'_>) -> FormatResult<u32> {
    let v = open_block(ctx)?;
    let d = need_dict(ctx)?;
    let n = v.values as usize;
    if out.datums.len() < n {
        return Err(FormatError::Bounds {
            at: "decode_full out",
        });
    }
    match ctx.key.class {
        CLASS_VARLENA => {
            // Frame-chunked: monomorphized code unpack into a stack buffer,
            // then the zero-copy gather. Frames start byte-aligned in the
            // bit stream (module doc), so each chunk is a plain re-slice.
            let mut codes = [0u32; GATHER_CHUNK];
            // memo.0 = u32::MAX can never be a live code (codes are
            // `< entry_count <= u32::MAX`), so the first row always faults
            // the memo in.
            let mut memo = (u32::MAX, 0u64);
            let mut done = 0usize;
            while done < n {
                let m = (n - done).min(GATHER_CHUNK);
                let frame_bytes = done * v.width as usize / 8;
                unpack_codes_raw(v.base, v.width, &v.bits[frame_bytes..], &mut codes[..m]);
                gather_varlena(&d, &codes[..m], &mut out.datums[done..done + m], &mut memo)?;
                done += m;
            }
        }
        CLASS_FIXED => {
            for i in 0..v.values {
                let code = extract_code(&v, i);
                out.datums[i as usize] = materialize_fixed(ctx, &d, code, &mut out.arena)?;
            }
        }
        other => return Err(FormatError::UnknownStorageClass { class: other }),
    }
    Ok(v.values)
}

fn dc_dec_sel(
    ctx: &KernelCtx<'_>,
    sel: &Selection<'_>,
    out: &mut DecodeOut<'_>,
) -> FormatResult<u32> {
    let v = open_block(ctx)?;
    let d = need_dict(ctx)?;
    if out.datums.len() < sel.rows.len() {
        return Err(FormatError::Bounds {
            at: "decode_sel out",
        });
    }
    let varlena = match ctx.key.class {
        CLASS_VARLENA => true,
        CLASS_FIXED => false,
        other => return Err(FormatError::UnknownStorageClass { class: other }),
    };
    let mut memo = (u32::MAX, 0u64);
    for (i, &r16) in sel.rows.iter().enumerate() {
        let r = r16 as u32;
        if r >= v.values {
            return Err(FormatError::Bounds {
                at: "decode_sel row",
            });
        }
        let code = extract_code(&v, r);
        out.datums[i] = if varlena {
            // Zero-copy view into the resident dictionary (same law as the
            // full face) — selection sparsity keeps the per-row extract.
            if code != memo.0 {
                memo = (code, dict_entry_datum(&d, code)?);
            }
            memo.1
        } else {
            materialize_fixed(ctx, &d, code, &mut out.arena)?
        };
    }
    Ok(sel.rows.len() as u32)
}

fn dc_dict_handle(ctx: &KernelCtx<'_>) -> FormatResult<DictLayout> {
    let d = need_dict(ctx)?;
    Ok(DictLayout {
        entry_count: d.entry_count,
        frame_entries: DICT_FRAME_ENTRIES,
    })
}

fn dc_meta(ctx: &KernelCtx<'_>, probe: &MetaProbe) -> FormatResult<MetaAnswer> {
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

fn dc_validity(ctx: &KernelCtx<'_>, out: &mut [u64]) -> FormatResult<ValidityVerdict> {
    validity_from_ctx(ctx, out)
}

const fn dc_vt(class: u8) -> CodecVtable {
    CodecVtable {
        key: KernelKey {
            encoding: EncodingId::DictCodes as u16,
            class,
            width: 0,
        },
        decode_full: dc_dec_full,
        decode_sel: dc_dec_sel,
        decode_codes: dc_dec_codes,
        dict_handle: dc_dict_handle,
        meta_probe: dc_meta,
        validity: dc_validity,
    }
}

pub static VT_DICT_VARLENA: CodecVtable = dc_vt(CLASS_VARLENA);
pub static VT_DICT_FIXED: CodecVtable = dc_vt(CLASS_FIXED);
