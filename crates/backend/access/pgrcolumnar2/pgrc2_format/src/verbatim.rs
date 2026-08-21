//! The reference `VERBATIM` + `CONST` codec (spec §19.1): the ABI WITNESS.
//!
//! This codec exists to prove the six-face ABI round-trips over every
//! storage class — correctness, not speed. It may read class/width from ctx
//! (a match in a cold body); M3-C's hot kernels monomorphize per width
//! behind the fn-pointer tables (the S4 pow2-switch law governs THEM, not
//! this witness). Wire behavior here is FROZEN: spec §6.7 (verbatim),
//! §6.8 (overflow), §6.9 (const).

use crate::abi::{
    refuse_decode_codes, refuse_dict_handle, validity_from_ctx, ByteArena, CodecVtable, DecodeOut,
    EncodeInput, GranuleEncoder, KernelCtx, KernelKey, Selection, ValidityVerdict,
};
use crate::class::{
    StorageClass, CLASS_BOOL, CLASS_BYVAL, CLASS_F32, CLASS_F64, CLASS_FIXED, CLASS_VARLENA,
};
use crate::enc::EncodingId;
use crate::geom::{FRAMES_PER_GRANULE, FRAME_VALUES, GRANULE_ROWS, OVERSIZE_THRESHOLD};
use crate::meta::{MetaAnswer, MetaProbe, Sortedness};
use crate::part::{
    OverflowSink, StreamSectionHdr, StreamSectionWriter, OVERFLOW_MARK, STREAMF_SIGNED,
    STREAM_SECTION_HDR_LEN,
};
use crate::wire::{varlena_entry_at, varlena_header_4b_u};
use crate::{FormatError, FormatResult};

// ---------------------------------------------------------------------------
// shared section plumbing (allocation-free)
// ---------------------------------------------------------------------------

/// The payload region of a section (past the header, before the tables).
fn payload_region<'a>(hdr: &StreamSectionHdr, section: &'a [u8]) -> FormatResult<&'a [u8]> {
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
fn gcount(hdr: &StreamSectionHdr, section: &[u8], i: u32) -> FormatResult<u32> {
    let off = hdr.gcount_table_off as usize + i as usize * 4;
    let b = section
        .get(off..off + 4)
        .ok_or(FormatError::Bounds { at: "gcount table" })?;
    Ok(u32::from_le_bytes(b.try_into().expect("len 4")))
}

/// Values before granule `g` in this extent (spec §6.5: closed-form for root
/// streams, gcount-table-driven for child streams).
fn value_base(hdr: &StreamSectionHdr, section: &[u8], g: u32) -> FormatResult<u64> {
    if hdr.gcount_table_off == 0 {
        return Ok(g as u64 * GRANULE_ROWS as u64);
    }
    let mut sum = 0u64;
    for i in 0..g {
        sum += gcount(hdr, section, i)? as u64;
    }
    Ok(sum)
}

/// Frames before granule `g` in this extent (granule-major frame numbering).
fn frame_base(hdr: &StreamSectionHdr, section: &[u8], g: u32) -> FormatResult<u32> {
    if hdr.gcount_table_off == 0 {
        return Ok(g * FRAMES_PER_GRANULE);
    }
    let mut sum = 0u32;
    for i in 0..g {
        sum += gcount(hdr, section, i)?.div_ceil(FRAME_VALUES);
    }
    Ok(sum)
}

/// Word-class datum extension (spec §6.7).
pub(crate) fn extend_word(raw: u64, width: u8, signed: bool) -> u64 {
    if !signed || width == 8 {
        return raw;
    }
    let shift = 64 - width as u32 * 8;
    (((raw << shift) as i64) >> shift) as u64
}

/// Read 1..=8 LE bytes, zero-extended.
fn read_le(bytes: &[u8]) -> u64 {
    let mut w = [0u8; 8];
    w[..bytes.len()].copy_from_slice(bytes);
    u64::from_le_bytes(w)
}

// ---------------------------------------------------------------------------
// VERBATIM decode (spec §6.7/§6.8)
// ---------------------------------------------------------------------------

/// Per-granule verbatim view: resolves any row to a datum word.
struct VerbatimView<'a> {
    class_id: u8,
    width: u8,
    signed: bool,
    fixed_len: usize,
    payload: &'a [u8],
    /// Values before this granule (element index base).
    vbase: u64,
    /// Frames before this granule (varlena frame numbering).
    fbase: u32,
    values: u32,
    frame_table: Option<&'a [u32]>,
    overflow: Option<&'a [u8]>,
}

impl<'a> VerbatimView<'a> {
    fn open(ctx: &KernelCtx<'a>) -> FormatResult<VerbatimView<'a>> {
        let hdr = StreamSectionHdr::decode(ctx.bytes)?;
        if hdr.encoding != EncodingId::Verbatim.as_u16() {
            return Err(FormatError::Corrupt {
                at: "verbatim section encoding",
            });
        }
        let payload = payload_region(&hdr, ctx.bytes)?;
        Ok(VerbatimView {
            class_id: ctx.key.class,
            width: ctx.key.width,
            signed: ctx.flags & STREAMF_SIGNED != 0,
            fixed_len: ctx.fixed_len as usize,
            payload,
            vbase: value_base(&hdr, ctx.bytes, ctx.granule_in_extent)?,
            fbase: frame_base(&hdr, ctx.bytes, ctx.granule_in_extent)?,
            values: ctx.values,
            frame_table: ctx.frame_table,
            overflow: ctx.overflow,
        })
    }

    /// The stride of word/fixed classes.
    fn stride(&self) -> FormatResult<usize> {
        Ok(match self.class_id {
            CLASS_BYVAL => self.width as usize,
            CLASS_F32 => 4,
            CLASS_F64 => 8,
            CLASS_BOOL => 1,
            CLASS_FIXED => self.fixed_len,
            _ => {
                return Err(FormatError::Corrupt {
                    at: "verbatim stride class",
                })
            }
        })
    }

    /// Decode row `r` (granule-local) to a datum word.
    fn datum_at(&self, r: u32, arena: &mut ByteArena<'_>) -> FormatResult<u64> {
        if r >= self.values {
            return Err(FormatError::Bounds { at: "verbatim row" });
        }
        match self.class_id {
            CLASS_BYVAL | CLASS_F32 | CLASS_F64 | CLASS_BOOL => {
                let s = self.stride()?;
                let off = (self.vbase + r as u64) as usize * s;
                let raw = self.payload.get(off..off + s).ok_or(FormatError::Bounds {
                    at: "verbatim word payload",
                })?;
                let w = read_le(raw);
                Ok(match self.class_id {
                    CLASS_BOOL => (w != 0) as u64,
                    CLASS_BYVAL => extend_word(w, self.width, self.signed),
                    _ => w,
                })
            }
            CLASS_FIXED => {
                let s = self.fixed_len;
                let off = (self.vbase + r as u64) as usize * s;
                let img = self.payload.get(off..off + s).ok_or(FormatError::Bounds {
                    at: "verbatim fixed payload",
                })?;
                arena.alloc_fixed(img)
            }
            CLASS_VARLENA => {
                let ft = self.frame_table.ok_or(FormatError::Corrupt {
                    at: "varlena needs frame table",
                })?;
                let fi = (self.fbase + r / FRAME_VALUES) as usize;
                let frame_off = *ft
                    .get(fi)
                    .ok_or(FormatError::Bounds { at: "frame index" })?
                    as usize;
                let frame = self
                    .payload
                    .get(frame_off..)
                    .ok_or(FormatError::Bounds { at: "frame offset" })?;
                let slot = (r % FRAME_VALUES) as usize;
                let so = slot * 4;
                let entry_off = u32::from_le_bytes(
                    frame
                        .get(so..so + 4)
                        .ok_or(FormatError::Bounds { at: "slot table" })?
                        .try_into()
                        .expect("len 4"),
                ) as usize;
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
                    let total_len =
                        u32::from_le_bytes(rec[4..8].try_into().expect("len 4")) as usize;
                    let ovf_off =
                        u64::from_le_bytes(rec[8..16].try_into().expect("len 8")) as usize;
                    let ovf = self.overflow.ok_or(FormatError::Corrupt {
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
            other => Err(FormatError::UnknownStorageClass { class: other }),
        }
    }
}

fn vdec_full(ctx: &KernelCtx<'_>, out: &mut DecodeOut<'_>) -> FormatResult<u32> {
    let v = VerbatimView::open(ctx)?;
    if out.datums.len() < v.values as usize {
        return Err(FormatError::Bounds {
            at: "decode_full out",
        });
    }
    for r in 0..v.values {
        out.datums[r as usize] = v.datum_at(r, &mut out.arena)?;
    }
    Ok(v.values)
}

fn vdec_sel(
    ctx: &KernelCtx<'_>,
    sel: &Selection<'_>,
    out: &mut DecodeOut<'_>,
) -> FormatResult<u32> {
    let v = VerbatimView::open(ctx)?;
    if out.datums.len() < sel.rows.len() {
        return Err(FormatError::Bounds {
            at: "decode_sel out",
        });
    }
    for (i, &r) in sel.rows.iter().enumerate() {
        out.datums[i] = v.datum_at(r as u32, &mut out.arena)?;
    }
    Ok(sel.rows.len() as u32)
}

/// NonNullCount from the ctx validity slice without an output buffer.
fn nonnull_count(ctx: &KernelCtx<'_>) -> FormatResult<u32> {
    let Some(bits) = ctx.validity_bytes else {
        return Ok(ctx.rows);
    };
    let rows = ctx.rows as usize;
    let need = rows.div_ceil(8);
    if bits.len() < need {
        return Err(FormatError::Bounds {
            at: "validity bitmap",
        });
    }
    let mut n = 0u32;
    for (i, &b) in bits[..need].iter().enumerate() {
        let mut byte = b;
        if (i + 1) * 8 > rows {
            byte &= (1u8 << (rows - i * 8)) - 1;
        }
        n += byte.count_ones();
    }
    Ok(n)
}

fn vmeta(ctx: &KernelCtx<'_>, probe: &MetaProbe) -> FormatResult<MetaAnswer> {
    Ok(match probe {
        MetaProbe::RowCount => MetaAnswer::Count(ctx.rows as u64),
        MetaProbe::NonNullCount => MetaAnswer::Count(nonnull_count(ctx)? as u64),
        _ => MetaAnswer::Absent,
    })
}

fn vvalidity(ctx: &KernelCtx<'_>, out: &mut [u64]) -> FormatResult<ValidityVerdict> {
    validity_from_ctx(ctx, out)
}

// ---------------------------------------------------------------------------
// CONST decode (spec §6.9)
// ---------------------------------------------------------------------------

struct ConstRecord<'a> {
    all_null: bool,
    bytes: &'a [u8],
}

fn const_record<'a>(ctx: &KernelCtx<'a>) -> FormatResult<ConstRecord<'a>> {
    let hdr = StreamSectionHdr::decode(ctx.bytes)?;
    if hdr.encoding != EncodingId::Const.as_u16() {
        return Err(FormatError::Corrupt {
            at: "const section encoding",
        });
    }
    let payload = payload_region(&hdr, ctx.bytes)?;
    if payload.len() < 8 {
        return Err(FormatError::Truncated { at: "const record" });
    }
    let all_null = payload[0] == 1;
    if payload[0] > 1 {
        return Err(FormatError::Corrupt { at: "const flags" });
    }
    let len = u32::from_le_bytes(payload[4..8].try_into().expect("len 4")) as usize;
    let bytes = payload
        .get(8..8 + len)
        .ok_or(FormatError::Bounds { at: "const bytes" })?;
    if all_null && len != 0 {
        return Err(FormatError::Corrupt {
            at: "const all-null with bytes",
        });
    }
    Ok(ConstRecord { all_null, bytes })
}

fn const_datum(
    ctx: &KernelCtx<'_>,
    rec: &ConstRecord<'_>,
    arena: &mut ByteArena<'_>,
) -> FormatResult<u64> {
    if rec.all_null {
        return Ok(0);
    }
    match ctx.key.class {
        CLASS_BYVAL | CLASS_F32 | CLASS_F64 | CLASS_BOOL => {
            if rec.bytes.len() != 8 {
                return Err(FormatError::Corrupt {
                    at: "const word length",
                });
            }
            Ok(read_le(rec.bytes))
        }
        CLASS_FIXED => {
            if rec.bytes.len() != ctx.fixed_len as usize {
                return Err(FormatError::Corrupt {
                    at: "const fixed length",
                });
            }
            arena.alloc_fixed(rec.bytes)
        }
        CLASS_VARLENA => arena.alloc_varlena(rec.bytes),
        other => Err(FormatError::UnknownStorageClass { class: other }),
    }
}

fn cdec_full(ctx: &KernelCtx<'_>, out: &mut DecodeOut<'_>) -> FormatResult<u32> {
    let rec = const_record(ctx)?;
    if out.datums.len() < ctx.values as usize {
        return Err(FormatError::Bounds {
            at: "decode_full out",
        });
    }
    let d = const_datum(ctx, &rec, &mut out.arena)?;
    for r in 0..ctx.values as usize {
        out.datums[r] = d;
    }
    Ok(ctx.values)
}

fn cdec_sel(
    ctx: &KernelCtx<'_>,
    sel: &Selection<'_>,
    out: &mut DecodeOut<'_>,
) -> FormatResult<u32> {
    let rec = const_record(ctx)?;
    if out.datums.len() < sel.rows.len() {
        return Err(FormatError::Bounds {
            at: "decode_sel out",
        });
    }
    let d = const_datum(ctx, &rec, &mut out.arena)?;
    for (i, &r) in sel.rows.iter().enumerate() {
        if r as u32 >= ctx.values {
            return Err(FormatError::Bounds {
                at: "decode_sel row",
            });
        }
        out.datums[i] = d;
    }
    Ok(sel.rows.len() as u32)
}

fn cmeta(ctx: &KernelCtx<'_>, probe: &MetaProbe) -> FormatResult<MetaAnswer> {
    Ok(match probe {
        MetaProbe::RowCount => MetaAnswer::Count(ctx.rows as u64),
        MetaProbe::NonNullCount => {
            let rec = const_record(ctx)?;
            if rec.all_null {
                MetaAnswer::Count(0)
            } else {
                MetaAnswer::Count(nonnull_count(ctx)? as u64)
            }
        }
        MetaProbe::Sortedness => MetaAnswer::Sorted(Sortedness::Constant),
        _ => MetaAnswer::Absent,
    })
}

// ---------------------------------------------------------------------------
// vtables (spec §19.5)
// ---------------------------------------------------------------------------

const fn verbatim_vt(class: u8, width: u8) -> CodecVtable {
    CodecVtable {
        key: KernelKey {
            encoding: EncodingId::Verbatim as u16,
            class,
            width,
        },
        decode_full: vdec_full,
        decode_sel: vdec_sel,
        decode_codes: refuse_decode_codes,
        dict_handle: refuse_dict_handle,
        meta_probe: vmeta,
        validity: vvalidity,
    }
}

const fn const_vt(class: u8, width: u8) -> CodecVtable {
    CodecVtable {
        key: KernelKey {
            encoding: EncodingId::Const as u16,
            class,
            width,
        },
        decode_full: cdec_full,
        decode_sel: cdec_sel,
        decode_codes: refuse_decode_codes,
        dict_handle: refuse_dict_handle,
        meta_probe: cmeta,
        validity: vvalidity,
    }
}

static VT_VERBATIM: [CodecVtable; 8] = [
    verbatim_vt(CLASS_BYVAL, 1),
    verbatim_vt(CLASS_BYVAL, 2),
    verbatim_vt(CLASS_BYVAL, 4),
    verbatim_vt(CLASS_BYVAL, 8),
    verbatim_vt(CLASS_F32, 4),
    verbatim_vt(CLASS_F64, 8),
    verbatim_vt(CLASS_BOOL, 1),
    verbatim_vt(CLASS_FIXED, 0),
];
static VT_VERBATIM_VARLENA: CodecVtable = verbatim_vt(CLASS_VARLENA, 0);

static VT_CONST: [CodecVtable; 8] = [
    const_vt(CLASS_BYVAL, 1),
    const_vt(CLASS_BYVAL, 2),
    const_vt(CLASS_BYVAL, 4),
    const_vt(CLASS_BYVAL, 8),
    const_vt(CLASS_F32, 4),
    const_vt(CLASS_F64, 8),
    const_vt(CLASS_BOOL, 1),
    const_vt(CLASS_FIXED, 0),
];
static VT_CONST_VARLENA: CodecVtable = const_vt(CLASS_VARLENA, 0);

/// The CONST half of the reference vtables. The production registry
/// (`pgrc2_codec::dispatch`) seeds CONST from here but wires VERBATIM to
/// its gate-3 hot kernels (M3 exit §2.3 license); the full
/// [`reference_vtables`] set stays available as the ABI witness, the
/// reference read binding, and the hot kernels' differential oracle.
pub fn const_reference_vtables() -> [&'static CodecVtable; 9] {
    [
        &VT_CONST[0],
        &VT_CONST[1],
        &VT_CONST[2],
        &VT_CONST[3],
        &VT_CONST[4],
        &VT_CONST[5],
        &VT_CONST[6],
        &VT_CONST[7],
        &VT_CONST_VARLENA,
    ]
}

/// The VERBATIM half of the reference vtables (the naive per-value witness
/// bodies — the hot kernels' differential oracle).
pub fn verbatim_reference_vtables() -> [&'static CodecVtable; 9] {
    [
        &VT_VERBATIM[0],
        &VT_VERBATIM[1],
        &VT_VERBATIM[2],
        &VT_VERBATIM[3],
        &VT_VERBATIM[4],
        &VT_VERBATIM[5],
        &VT_VERBATIM[6],
        &VT_VERBATIM[7],
        &VT_VERBATIM_VARLENA,
    ]
}

/// The reference vtables (registry seed; M3-C appends its kernel lists).
pub fn reference_vtables() -> [&'static CodecVtable; 18] {
    [
        &VT_VERBATIM[0],
        &VT_VERBATIM[1],
        &VT_VERBATIM[2],
        &VT_VERBATIM[3],
        &VT_VERBATIM[4],
        &VT_VERBATIM[5],
        &VT_VERBATIM[6],
        &VT_VERBATIM[7],
        &VT_VERBATIM_VARLENA,
        &VT_CONST[0],
        &VT_CONST[1],
        &VT_CONST[2],
        &VT_CONST[3],
        &VT_CONST[4],
        &VT_CONST[5],
        &VT_CONST[6],
        &VT_CONST[7],
        &VT_CONST_VARLENA,
    ]
}

// ---------------------------------------------------------------------------
// VERBATIM encode (spec §6.7/§6.8)
// ---------------------------------------------------------------------------

/// # Safety
///
/// Pointer-class datums must obey the EncodeInput contract (spec §19.6):
/// Fixed datums point at `len` readable bytes; Varlena datums at valid 4B-U
/// images. This helper is the crate's single deref site for encode.
unsafe fn input_image<'a>(class: StorageClass, datum: u64) -> FormatResult<&'a [u8]> {
    match class {
        StorageClass::Fixed { len } => {
            // SAFETY: caller contract.
            Ok(unsafe { core::slice::from_raw_parts(datum as *const u8, len as usize) })
        }
        StorageClass::VarlenaVerbatim => {
            let p = datum as *const u8;
            // SAFETY: caller contract.
            let header = u32::from_le_bytes(unsafe {
                core::slice::from_raw_parts(p, 4).try_into().expect("len 4")
            });
            let len = crate::wire::varlena_4b_u_payload_len(header, "encode varlena")? as usize;
            // SAFETY: caller contract — image is len + 4 bytes.
            Ok(unsafe { core::slice::from_raw_parts(p.add(4), len) })
        }
        _ => Err(FormatError::EncodeContract {
            detail: "input_image on word class",
        }),
    }
}

/// The reference VERBATIM encoder: one struct per stream, class-directed.
pub struct VerbatimEncoder {
    pub class: StorageClass,
}

impl GranuleEncoder for VerbatimEncoder {
    fn key(&self) -> KernelKey {
        KernelKey {
            encoding: EncodingId::Verbatim.as_u16(),
            class: self.class.id(),
            width: self.class.width(),
        }
    }

    fn encode_granule(
        &mut self,
        input: &EncodeInput<'_>,
        w: &mut StreamSectionWriter<'_>,
        ovf: &mut OverflowSink<'_>,
    ) -> FormatResult<()> {
        if input.class != self.class {
            return Err(FormatError::EncodeContract {
                detail: "encoder/input class",
            });
        }
        if input.datums.len() < input.rows as usize {
            return Err(FormatError::EncodeContract {
                detail: "datums shorter than rows",
            });
        }
        if input.rows > GRANULE_ROWS {
            return Err(FormatError::EncodeContract {
                detail: "granule overflow",
            });
        }
        match self.class {
            StorageClass::ByvalWord { width, .. } => {
                encode_words(input, w, width as usize)?;
            }
            StorageClass::F32 => encode_words(input, w, 4)?,
            StorageClass::F64 => encode_words(input, w, 8)?,
            StorageClass::Bool => {
                let buf = w.payload();
                for r in 0..input.rows {
                    let b = if input.valid(r) {
                        (input.datums[r as usize] != 0) as u8
                    } else {
                        0
                    };
                    buf.push(b);
                }
            }
            StorageClass::Fixed { len } => {
                for r in 0..input.rows {
                    if input.valid(r) {
                        // SAFETY: EncodeInput pointer-class contract.
                        let img = unsafe { input_image(self.class, input.datums[r as usize])? };
                        w.payload().extend_from_slice(img);
                    } else {
                        // Canonical placeholder: zero bytes (spec §6.6).
                        let new_len = w.payload().len() + len as usize;
                        w.payload().resize(new_len, 0);
                    }
                }
            }
            StorageClass::VarlenaVerbatim => {
                encode_varlena_granule(input, w, ovf)?;
            }
        }
        w.end_granule(input.rows);
        Ok(())
    }

    /// SEAL-SPEED-2 fold fusion: the VARLENA arm folds at frame-span grain
    /// from inside the emit walk — the observe pass's second image deref
    /// per value disappears (the fold bill's home: pointer chase + byte-
    /// grain hashing per string). Word/bool/fixed classes keep the classic
    /// pair: their emit is a bulk copy and the observe walk over in-cache
    /// words is the cheap half.
    fn encode_granule_observed(
        &mut self,
        input: &EncodeInput<'_>,
        w: &mut StreamSectionWriter<'_>,
        ovf: &mut OverflowSink<'_>,
        meta: &mut dyn crate::abi::ColumnMetaBuilder,
        granule: u32,
    ) -> FormatResult<()> {
        if !meta.row_observe_supported() || !matches!(self.class, StorageClass::VarlenaVerbatim)
        {
            self.encode_granule(input, w, ovf)?;
            meta.observe_granule(input, granule);
            return Ok(());
        }
        if input.class != self.class {
            return Err(FormatError::EncodeContract {
                detail: "encoder/input class",
            });
        }
        if input.datums.len() < input.rows as usize {
            return Err(FormatError::EncodeContract {
                detail: "datums shorter than rows",
            });
        }
        if input.rows > GRANULE_ROWS {
            return Err(FormatError::EncodeContract {
                detail: "granule overflow",
            });
        }
        meta.begin_granule_rows(granule);
        encode_varlena_granule_observed(input, w, ovf, Some(meta))?;
        meta.end_granule_rows(input.rows);
        w.end_granule(input.rows);
        Ok(())
    }
}

fn encode_words(
    input: &EncodeInput<'_>,
    w: &mut StreamSectionWriter<'_>,
    stride: usize,
) -> FormatResult<()> {
    let buf = w.payload();
    for r in 0..input.rows {
        // Canonical placeholder for nulls: zero bytes (spec §6.6).
        let d = if input.valid(r) {
            input.datums[r as usize]
        } else {
            0
        };
        buf.extend_from_slice(&d.to_le_bytes()[..stride]);
    }
    Ok(())
}

fn encode_varlena_granule(
    input: &EncodeInput<'_>,
    w: &mut StreamSectionWriter<'_>,
    ovf: &mut OverflowSink<'_>,
) -> FormatResult<()> {
    encode_varlena_granule_observed(input, w, ovf, None)
}

/// [`encode_varlena_granule`] with the SEAL-SPEED-2 fold-fusion hook: the
/// SAME emit loop feeds the meta builder per row (`obs` = the streaming
/// face, already `begin_granule_rows`-opened by the caller) — the varlena
/// deref the folds need is the one the encoder just performed, so the
/// separate observe walk (its second pointer chase per value) disappears.
/// `None` = the classic emit (byte-identical either way).
fn encode_varlena_granule_observed(
    input: &EncodeInput<'_>,
    w: &mut StreamSectionWriter<'_>,
    ovf: &mut OverflowSink<'_>,
    mut obs: Option<&mut dyn crate::abi::ColumnMetaBuilder>,
) -> FormatResult<()> {
    let mut r0: u32 = 0;
    while r0 < input.rows {
        let vif = (input.rows - r0).min(FRAME_VALUES);
        // SEAL-SPEED-2 fold fusion, SPAN grain: fold this frame's rows
        // right before the emit walks the same images — one virtual call
        // per frame, the builder's tight monomorphic loop inside, and the
        // emit re-reads cache-warm payloads. Fold order stays row order.
        if let Some(o) = obs.as_deref_mut() {
            o.observe_rows(input, r0, vif);
        }
        // Frames start 8-aligned so entry alignment is absolute (spec §6.7).
        w.align_payload(8);
        w.begin_frame();
        // Slot table: (vif + 1) u32 entry offsets, frame-start-relative;
        // entries begin at the next 8-aligned offset past the table.
        let slot_table_len = (vif as usize + 1) * 4;
        let entries_base = slot_table_len.div_ceil(8) * 8;
        let mut entries: Vec<u8> = Vec::new();
        let mut slots: Vec<u32> = Vec::with_capacity(vif as usize + 1);
        for i in 0..vif {
            let r = r0 + i;
            // 8-align each entry within the entry region (base is 8-aligned).
            let pad = (8 - entries.len() % 8) % 8;
            entries.resize(entries.len() + pad, 0);
            slots.push((entries_base + entries.len()) as u32);
            if !input.valid(r) {
                // Canonical placeholder: a zero-length varlena entry.
                entries.extend_from_slice(&varlena_header_4b_u(0).to_le_bytes());
                continue;
            }
            // SAFETY: EncodeInput pointer-class contract.
            let payload =
                unsafe { input_image(StorageClass::VarlenaVerbatim, input.datums[r as usize])? };
            if payload.len() as u32 >= OVERSIZE_THRESHOLD {
                let off = ovf.put_entry(payload);
                entries.extend_from_slice(&OVERFLOW_MARK.to_le_bytes());
                entries.extend_from_slice(&(payload.len() as u32).to_le_bytes());
                entries.extend_from_slice(&off.to_le_bytes());
            } else {
                entries.extend_from_slice(&varlena_header_4b_u(payload.len() as u32).to_le_bytes());
                entries.extend_from_slice(payload);
            }
        }
        slots.push((entries_base + entries.len()) as u32);
        let buf = w.payload();
        for &s in &slots {
            buf.extend_from_slice(&s.to_le_bytes());
        }
        // Pad the slot table out to the 8-aligned entry base.
        let pad = entries_base - slot_table_len;
        buf.resize(buf.len() + pad, 0);
        buf.extend_from_slice(&entries);
        r0 += vif;
    }
    Ok(())
}

/// Canonical validity-bitmap producer (spec §6.6): per-granule, LSB-first,
/// byte-padded, zeroed past `rows`. The writer emits validity streams
/// through this (BOOL_BITMAP layout).
pub fn encode_validity_bitmap(validity: Option<&[u64]>, rows: u32, out: &mut Vec<u8>) {
    let bytes = (rows as usize).div_ceil(8);
    for i in 0..bytes {
        let mut b: u8 = 0;
        for bit in 0..8 {
            let r = (i * 8 + bit) as u32;
            if r < rows {
                let valid = match validity {
                    None => true,
                    Some(words) => {
                        let w = (r / 64) as usize;
                        w < words.len() && (words[w] >> (r % 64)) & 1 == 1
                    }
                };
                if valid {
                    b |= 1 << bit;
                }
            }
        }
        out.push(b);
    }
}

// ---------------------------------------------------------------------------
// CONST encode (spec §6.9)
// ---------------------------------------------------------------------------

enum ConstState {
    /// Nothing seen yet (no record written).
    Empty,
    /// Only nulls so far; no record written yet (finish_stream writes
    /// ALL_NULL if it stays that way).
    AllNullSoFar,
    /// The extent's value record is written; canonical bytes retained for
    /// the constancy check.
    Value(Vec<u8>),
}

/// The reference CONST encoder: extent-scoped single value; the election is
/// the caller's — this encoder VERIFIES constancy (typed error otherwise).
pub struct ConstEncoder {
    pub class: StorageClass,
    state: ConstState,
}

impl ConstEncoder {
    pub fn new(class: StorageClass) -> ConstEncoder {
        ConstEncoder {
            class,
            state: ConstState::Empty,
        }
    }

    fn canonical(&self, datum: u64) -> FormatResult<Vec<u8>> {
        let mut scratch = [0u8; 8];
        // SAFETY: EncodeInput pointer-class contract (spec §19.6).
        let bytes = unsafe { crate::abi::datum_canonical_bytes(self.class, datum, &mut scratch)? };
        Ok(bytes.to_vec())
    }

    /// The record payload for the stored value (word classes: the full
    /// 8-byte datum word; fixed/varlena: the image/payload bytes).
    fn record_bytes(&self, datum: u64) -> FormatResult<Vec<u8>> {
        match self.class {
            StorageClass::ByvalWord { .. }
            | StorageClass::F32
            | StorageClass::F64
            | StorageClass::Bool => Ok(datum.to_le_bytes().to_vec()),
            StorageClass::Fixed { .. } | StorageClass::VarlenaVerbatim => {
                // SAFETY: EncodeInput pointer-class contract.
                unsafe { input_image(self.class, datum).map(|s| s.to_vec()) }
            }
        }
    }

    fn write_record(w: &mut StreamSectionWriter<'_>, all_null: bool, bytes: &[u8]) {
        let buf = w.payload();
        buf.push(all_null as u8);
        buf.extend_from_slice(&[0u8; 3]);
        buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(bytes);
    }
}

impl GranuleEncoder for ConstEncoder {
    fn key(&self) -> KernelKey {
        KernelKey {
            encoding: EncodingId::Const.as_u16(),
            class: self.class.id(),
            width: self.class.width(),
        }
    }

    fn encode_granule(
        &mut self,
        input: &EncodeInput<'_>,
        w: &mut StreamSectionWriter<'_>,
        _ovf: &mut OverflowSink<'_>,
    ) -> FormatResult<()> {
        if input.class != self.class {
            return Err(FormatError::EncodeContract {
                detail: "encoder/input class",
            });
        }
        for r in 0..input.rows {
            if !input.valid(r) {
                continue;
            }
            let canon = self.canonical(input.datums[r as usize])?;
            match &self.state {
                ConstState::Empty | ConstState::AllNullSoFar => {
                    let rec = self.record_bytes(input.datums[r as usize])?;
                    ConstEncoder::write_record(w, false, &rec);
                    self.state = ConstState::Value(canon);
                }
                ConstState::Value(stored) => {
                    if *stored != canon {
                        return Err(FormatError::EncodeContract {
                            detail: "CONST not constant",
                        });
                    }
                }
            }
        }
        if matches!(self.state, ConstState::Empty) {
            self.state = ConstState::AllNullSoFar;
        }
        w.end_granule(input.rows);
        Ok(())
    }

    fn finish_stream(&mut self, w: &mut StreamSectionWriter<'_>) -> FormatResult<()> {
        if matches!(self.state, ConstState::Empty | ConstState::AllNullSoFar) {
            ConstEncoder::write_record(w, true, &[]);
        }
        Ok(())
    }
}
