//! The codec + meta-builder ABI (spec §19; charter §8 — the C2/C4 seam).
//!
//! Laws carried in the signatures:
//! - **Pre-compiled kernels, fn-pointer dispatch** per
//!   `(encoding × class × width)` — never per-query-JITted (Data Blocks
//!   law), never a width switch in or near the hot path (the S4 pow2-switch
//!   trap is live in LLVM-22). Resolution happens at stream open (M3-F),
//!   once per (stream, extent).
//! - **Ctx-relative, no address baking** (M0-S3 soundness law): every byte a
//!   kernel reads arrives through [`KernelCtx`]; kernels hold no statics, so
//!   fragment-cached T1 bodies may call them through stable pointers.
//! - **Allocation-free, no-panic, arena-out, worker-confined** (R1–R6):
//!   failures are typed [`FormatError`]s; outputs land in caller-owned
//!   [`DecodeOut`] buffers; [`ByteArena`] exhaustion is typed, never a
//!   resize. Arenas are per-granule-lifetime (the stale-datum trap).
//! - **One position list per granule**: visibility ∩ delete-vector ∩ qual
//!   survivors compose into a single [`Selection`] — `decode_sel` is the
//!   native late-materialization face and must equal
//!   `decode_full ∘ select` (M3-C property gate).
//! - Value-level decode errors fire lazily in row order at C's row.
//!
//! The reference `Verbatim`/`Const` codec ([`crate::verbatim`]) proves this
//! ABI round-trips; M3-C's kernels are the hot implementations.

use crate::class::{StorageClass, CLASS_BOOL, CLASS_BYVAL, CLASS_F32, CLASS_F64};
use crate::dict::{DictLayout, DictSections};
use crate::enc::EncodingId;
use crate::meta::{MetaAnswer, MetaProbe, StatsRecord};
use crate::part::{OverflowSink, SectionKind, StreamSectionWriter};
use crate::wire::varlena_4b_u_payload_len;
use crate::{FormatError, FormatResult};

/// The six decode faces (spec §19.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Face {
    DecodeFull,
    DecodeSel,
    DecodeCodes,
    DictHandle,
    MetaProbe,
    Validity,
}

/// Kernel dispatch key (spec §19.1): resolved at stream open, never per row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct KernelKey {
    pub encoding: u16,
    pub class: u8,
    pub width: u8,
}

/// ONE position list per granule: strictly-ascending row ordinals
/// (0..rows). Built once from visibility ∩ DV ∩ quals.
#[derive(Debug, Clone, Copy)]
pub struct Selection<'a> {
    pub rows: &'a [u16],
}

impl<'a> Selection<'a> {
    /// Structural validation (debug-tier; kernels bounds-check anyway).
    pub fn validate(&self, rows_in_granule: u32) -> FormatResult<()> {
        let mut prev: Option<u16> = None;
        for &r in self.rows {
            if r as u32 >= rows_in_granule {
                return Err(FormatError::Bounds {
                    at: "Selection row",
                });
            }
            if let Some(p) = prev {
                if r <= p {
                    return Err(FormatError::Corrupt {
                        at: "Selection order",
                    });
                }
            }
            prev = Some(r);
        }
        Ok(())
    }
}

/// Caller-owned bump arena for pointer-class outputs (spec §19.4):
/// ≥8-byte per-value alignment (the jsonb/numeric container law),
/// per-granule lifetime, typed exhaustion. The backing buffer must itself be
/// 8-aligned (debug-asserted) so entry alignment is absolute.
pub struct ByteArena<'a> {
    buf: &'a mut [u8],
    used: usize,
}

impl<'a> ByteArena<'a> {
    pub fn new(buf: &'a mut [u8]) -> ByteArena<'a> {
        debug_assert!(
            buf.as_ptr() as usize % 8 == 0,
            "arena buffers are 8-aligned"
        );
        ByteArena { buf, used: 0 }
    }

    pub fn used(&self) -> usize {
        self.used
    }

    /// Per-granule lifetime: the caller resets between granules.
    pub fn reset(&mut self) {
        self.used = 0;
    }

    /// 8-aligned allocation; typed exhaustion, never a resize.
    pub fn alloc(&mut self, len: usize) -> FormatResult<&mut [u8]> {
        let aligned = self.used.div_ceil(8) * 8;
        let end = aligned
            .checked_add(len)
            .ok_or(FormatError::ArenaExhausted { needed: len })?;
        if end > self.buf.len() {
            return Err(FormatError::ArenaExhausted { needed: len });
        }
        self.used = end;
        Ok(&mut self.buf[aligned..end])
    }

    /// Write a varlena-shaped entry (4B-U header + payload, spec §1) and
    /// return its datum word (the pointer to the header).
    pub fn alloc_varlena(&mut self, payload: &[u8]) -> FormatResult<u64> {
        let slot = self.alloc(4 + payload.len())?;
        slot[..4]
            .copy_from_slice(&crate::wire::varlena_header_4b_u(payload.len() as u32).to_le_bytes());
        slot[4..].copy_from_slice(payload);
        Ok(slot.as_ptr() as u64)
    }

    /// Copy a raw fixed-length image and return its datum word.
    pub fn alloc_fixed(&mut self, image: &[u8]) -> FormatResult<u64> {
        let slot = self.alloc(image.len())?;
        slot.copy_from_slice(image);
        Ok(slot.as_ptr() as u64)
    }
}

/// Decode output (spec §19.4): datum words in the lx_vec base-column
/// currency + the pointer-class arena.
///
/// **Pointer-class output law (spec §19.4 + StrView §7b zero-copy gather):**
/// pointer-class datums live in `arena` — EXCEPT dict-encoded varlena
/// streams, whose `decode_full`/`decode_sel` datums are zero-copy views
/// into the ctx's dict payload section (`dict::dict_entry_datum`). Every
/// caller already keeps those sections alive to build the ctx; the datums
/// stay valid exactly as long as that backing does (on the read path: the
/// part's insert-only segment cache, i.e. the `OpenPart`'s life — a
/// superset of every granule-claim consumption window).
pub struct DecodeOut<'a> {
    pub datums: &'a mut [u64],
    pub arena: ByteArena<'a>,
}

/// The `validity` face's answer: `AllValid` short-circuits with the output
/// words untouched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidityVerdict {
    AllValid,
    Mixed { nonnull: u32 },
}

/// Everything a kernel reads (spec §19.2). `bytes` is the whole extent
/// section (header included), CRC-validated by the reader — kernels still
/// bounds-check (typed error, never UB). `frame_table` is the reader's
/// parsed copy (kernels are allocation-free and never parse it themselves).
pub struct KernelCtx<'a> {
    pub key: KernelKey,
    /// Stream flags echo (`STREAMF_*`, spec §6.3).
    pub flags: u16,
    /// Fixed(N)'s N; 0 otherwise.
    pub fixed_len: u32,
    pub bytes: &'a [u8],
    pub frame_table: Option<&'a [u32]>,
    /// Granule ordinal within the part.
    pub granule: u32,
    /// Granule ordinal within this extent.
    pub granule_in_extent: u32,
    /// Logical rows in this granule.
    pub rows: u32,
    /// Values in this granule (child streams: may differ from rows).
    pub values: u32,
    /// This granule's validity-bitmap slice; None = all-valid.
    pub validity_bytes: Option<&'a [u8]>,
    /// The column's overflow-stream payload region (spec §6.8), when any.
    pub overflow: Option<&'a [u8]>,
    /// Dict section payloads for dict-encoded streams (spec §7).
    pub dict: Option<DictSections<'a>>,
}

// ---------------------------------------------------------------------------
// the vtable (spec §19.3)
// ---------------------------------------------------------------------------

pub type DecodeFullFn = fn(&KernelCtx<'_>, &mut DecodeOut<'_>) -> FormatResult<u32>;
pub type DecodeSelFn = fn(&KernelCtx<'_>, &Selection<'_>, &mut DecodeOut<'_>) -> FormatResult<u32>;
pub type DecodeCodesFn = fn(&KernelCtx<'_>, &mut [u32]) -> FormatResult<u32>;
pub type DictHandleFn = fn(&KernelCtx<'_>) -> FormatResult<DictLayout>;
pub type MetaProbeFn = fn(&KernelCtx<'_>, &MetaProbe) -> FormatResult<MetaAnswer>;
pub type ValidityFn = fn(&KernelCtx<'_>, &mut [u64]) -> FormatResult<ValidityVerdict>;

/// One kernel set. Faces an encoding cannot serve point at the canonical
/// refusal kernels (typed `FaceUnsupported`) — no `Option`, no extra branch
/// shape at call sites.
pub struct CodecVtable {
    pub key: KernelKey,
    pub decode_full: DecodeFullFn,
    pub decode_sel: DecodeSelFn,
    pub decode_codes: DecodeCodesFn,
    pub dict_handle: DictHandleFn,
    pub meta_probe: MetaProbeFn,
    pub validity: ValidityFn,
}

/// The dispatch registry (spec §19.5): assembled at init from kernel lists,
/// consulted at stream open only. Unknown/reserved encoding IDs refuse
/// BEFORE any table walk (born-RED-tested).
pub struct CodecRegistry {
    entries: &'static [&'static CodecVtable],
}

impl CodecRegistry {
    pub const fn new(entries: &'static [&'static CodecVtable]) -> CodecRegistry {
        CodecRegistry { entries }
    }

    pub fn resolve(&self, key: KernelKey) -> FormatResult<&'static CodecVtable> {
        // Adjudicate the ID first: unknown vs reserved vs assigned.
        EncodingId::resolve(key.encoding)?;
        for vt in self.entries {
            if vt.key == key {
                return Ok(vt);
            }
        }
        Err(FormatError::KernelMissing {
            encoding: key.encoding,
            class: key.class,
            width: key.width,
        })
    }
}

// ---------------------------------------------------------------------------
// canonical refusal + validity kernels
// ---------------------------------------------------------------------------

pub fn refuse_decode_codes(ctx: &KernelCtx<'_>, _out: &mut [u32]) -> FormatResult<u32> {
    Err(FormatError::FaceUnsupported {
        face: Face::DecodeCodes,
        encoding: ctx.key.encoding,
    })
}

pub fn refuse_dict_handle(ctx: &KernelCtx<'_>) -> FormatResult<DictLayout> {
    Err(FormatError::FaceUnsupported {
        face: Face::DictHandle,
        encoding: ctx.key.encoding,
    })
}

/// The canonical validity kernel (spec §6.6): every encoding's `validity`
/// face — the bitmap layout is encoding-independent. Fills `out` mask words
/// (LSB-first) and masks the tail defensively.
pub fn validity_from_ctx(ctx: &KernelCtx<'_>, out: &mut [u64]) -> FormatResult<ValidityVerdict> {
    let Some(bits) = ctx.validity_bytes else {
        return Ok(ValidityVerdict::AllValid);
    };
    let rows = ctx.rows as usize;
    let need_bytes = rows.div_ceil(8);
    if bits.len() < need_bytes {
        return Err(FormatError::Bounds {
            at: "validity bitmap",
        });
    }
    let words = rows.div_ceil(64);
    if out.len() < words {
        return Err(FormatError::Bounds { at: "validity out" });
    }
    let mut nonnull: u32 = 0;
    for w in 0..words {
        let mut word: u64 = 0;
        let base = w * 8;
        for b in 0..8 {
            if base + b < need_bytes {
                word |= (bits[base + b] as u64) << (b * 8);
            }
        }
        // Mask bits past `rows` (canonical encoders zero them; readers never
        // trust that).
        if (w + 1) * 64 > rows {
            let keep = rows - w * 64;
            if keep < 64 {
                word &= (1u64 << keep) - 1;
            }
        }
        nonnull += word.count_ones();
        out[w] = word;
    }
    Ok(ValidityVerdict::Mixed { nonnull })
}

// ---------------------------------------------------------------------------
// encode faces (spec §19.6)
// ---------------------------------------------------------------------------

/// The encode-side currency — the SAME datum-word representation decode
/// emits, closing round-trip verification over the ABI.
pub struct EncodeInput<'a> {
    pub class: StorageClass,
    pub rows: u32,
    pub datums: &'a [u64],
    /// Mask words, LSB-first; None = all valid.
    pub validity: Option<&'a [u64]>,
}

impl<'a> EncodeInput<'a> {
    pub fn valid(&self, row: u32) -> bool {
        match self.validity {
            None => true,
            Some(words) => {
                let w = (row / 64) as usize;
                w < words.len() && (words[w] >> (row % 64)) & 1 == 1
            }
        }
    }
}

/// The granule encode face (spec §19.6). `StreamSectionWriter` owns §6.4
/// framing so every codec emits identical sections; oversize varlena values
/// go through the `OverflowSink` (spec §6.8).
pub trait GranuleEncoder {
    fn key(&self) -> KernelKey;
    fn encode_granule(
        &mut self,
        input: &EncodeInput<'_>,
        w: &mut StreamSectionWriter<'_>,
        ovf: &mut OverflowSink<'_>,
    ) -> FormatResult<()>;
    /// SEAL-SPEED-2 fold fusion: encode granule `granule` AND feed the meta
    /// builder in ONE value iteration where this encoder's emit loop walks
    /// the value currency row-dense (the fused overrides). The DEFAULT is
    /// the classic pair — encode walk then observe walk — byte-identical
    /// witnesses either way (the fold sequence per row is the same; only
    /// the number of passes over the values differs). Caller contract:
    /// `input` must be the VALUE currency (never dict codes) when this face
    /// is used; dict streams keep the explicit pair at the driver.
    fn encode_granule_observed(
        &mut self,
        input: &EncodeInput<'_>,
        w: &mut StreamSectionWriter<'_>,
        ovf: &mut OverflowSink<'_>,
        meta: &mut dyn ColumnMetaBuilder,
        granule: u32,
    ) -> FormatResult<()> {
        self.encode_granule(input, w, ovf)?;
        meta.observe_granule(input, granule);
        Ok(())
    }
    /// Called once after the last granule, before `StreamSectionWriter::
    /// finish` — encodings with extent-scoped records (CONST) close out here.
    fn finish_stream(&mut self, _w: &mut StreamSectionWriter<'_>) -> FormatResult<()> {
        Ok(())
    }
}

/// The meta-builder face (spec §19.7): M3-E implements, M3-D drives at seal.
/// Stats records are format structs (spec §8.1) at all three grains; aux
/// bodies (Psma/Bloom/NdvRegisters) are section images per spec §8.2–§8.4.
pub trait ColumnMetaBuilder {
    fn observe_granule(&mut self, input: &EncodeInput<'_>, granule: u32);

    // ---- SEAL-SPEED-2 fold fusion (ruled 2026-08-12): the streaming
    // per-row face. The 100m phase timers attributed fold_observe at 14.7x
    // the encode walk — a whole separate pass over values the encoder just
    // iterated. Encoders that walk values row-dense call THESE from inside
    // their own emit loops (one iteration produces encoded bytes AND feeds
    // the folds — same cache-warm values); the fold sequence is identical
    // to `observe_granule`'s by construction, so witness values stay pure
    // functions of the data (the dirsha law). Builders that do not
    // implement the streaming face keep the classic granule walk (defaults
    // below), as do encode paths whose emit currency is not the value
    // currency (dict code streams; carried memcpy arms).

    /// True when this builder implements the streaming face (the fused
    /// encoders consult it and fall back to the classic pair otherwise).
    fn row_observe_supported(&self) -> bool {
        false
    }
    /// Open granule `granule` for streamed spans (the `observe_granule`
    /// prelude). Driver contract: same ordering laws as `observe_granule`.
    fn begin_granule_rows(&mut self, granule: u32) {
        let _ = granule;
    }
    /// Fold rows `[first, first+n)` of `input`, in row order — SPAN grain
    /// (one virtual call per encoder frame, ~1024 rows), so the fold loop
    /// stays tight and monomorphic inside the builder while the encoder's
    /// emit keeps the values cache-warm between the two. Spans must be
    /// called in ascending, non-overlapping row order covering the input
    /// exactly (the fused encoders' frame walk does by construction).
    fn observe_rows(&mut self, input: &EncodeInput<'_>, first: u32, n: u32) {
        let _ = (input, first, n);
    }
    /// Close the streamed input span (`rows` = rows streamed since begin).
    fn end_granule_rows(&mut self, rows: u32) {
        let _ = rows;
    }

    fn seal_granule(&mut self, granule: u32) -> StatsRecord;
    fn seal_band(&mut self, band: u32) -> StatsRecord;
    fn seal_part(&mut self) -> StatsRecord;
    /// Aux section bodies to append (seal-time staging buffers).
    fn aux_sections(&mut self) -> Vec<(SectionKind, Vec<u8>)>;
    /// ST-1 (OD-2): the part-grain distribution sketch for the Stats
    /// sidecar — MCV top-k with EXACT part counts + equi-depth histogram
    /// bounds over canonical value bytes (HLL NDV stays in-footer, spec
    /// §8.4). Called after [`ColumnMetaBuilder::seal_part`]. Default
    /// `None`: stand-ins and profiles whose value identity is unsound
    /// (nondeterministic collations) compute none — the fold declines
    /// honestly instead of reading wrong numbers.
    fn distribution(&mut self) -> Option<crate::sidecar::ColDistribution> {
        None
    }

    /// pgrc2.1 §2.2: the part's EXACT non-null distinct count, when the
    /// builder's distinct structure observed EVERY non-null value's
    /// identity — the counted distinct set with no long-value residue
    /// (values over the MCV eligibility bound are tallied, not deduped, so
    /// any residue makes the exact count unknowable and this returns
    /// `None`; the dict feed arm is always complete). O(1) — a length
    /// read, never a finalize. Called after [`ColumnMetaBuilder::seal_part`].
    fn distinct_exact(&self) -> Option<u64> {
        None
    }

    /// DICT-DEDUP: hand over the part's exact counted distinct set the
    /// dict election already maintained (strictly byte-ascending entries;
    /// each count = the value's exact non-null occurrence count, so
    /// Σ counts = the stream's non-null rows). Driver contract: called at
    /// most once, BEFORE any observation. A builder that accepts the feed
    /// serves [`ColumnMetaBuilder::distribution`] from it and MUST stop
    /// its own distribution accumulation (the feed exists precisely so
    /// the distinct set is maintained ONCE — one structure, two
    /// consumers); a builder whose distribution is unsound (stand-ins,
    /// nondeterministic collations) ignores it and keeps declining. The
    /// default drops the feed.
    fn set_distribution_feed(&mut self, entries: Vec<(Vec<u8>, u64)>) {
        let _ = entries;
    }

    // ---- D-STATS dict fold-from-codes (seal-fusion charter §4 cut 3) ----
    //
    // Dict codes are byte-rank ranks — code order == entry byte order (the
    // dict build's contractual, emit-verified order certificate) — and the
    // DICT-DEDUP feed already handed the builder the byte-rank entry list
    // (index == global code). A builder that accepted that feed can
    // therefore fold a dict stream's granule facts FROM THE CODES the seal
    // already materialized (integer lookups through once-per-stream
    // tables) instead of re-walking the hydrated varlena values — the
    // fold-side twin of the #971/#983 dedup. Witness values stay pure
    // functions of the data (identical to the value walk; the equivalence
    // battery + rig dirshas pin it), so this is a perf face, never a
    // bytes face.

    /// True when this builder can serve
    /// [`ColumnMetaBuilder::observe_granule_dict_codes`] for the current
    /// stream (the D-STATS batched shell is on AND the DICT-DEDUP feed was
    /// accepted). The
    /// driver consults this per stream and falls back to the classic
    /// hydrated-value `observe_granule` otherwise (D2 inherit, control
    /// arms, stand-ins).
    fn dict_code_observe_supported(&self) -> bool {
        false
    }

    /// Observe one dict-stream granule FROM ITS CODES: `input` carries the
    /// value class but its `datums` are the granule's row-dense GLOBAL
    /// codes (null slots 0, skipped via validity) — exactly the encode
    /// currency the seal already built. Same ordering laws as
    /// `observe_granule`. Driver contract: called only when
    /// [`ColumnMetaBuilder::dict_code_observe_supported`] returned true.
    fn observe_granule_dict_codes(&mut self, input: &EncodeInput<'_>, granule: u32) {
        let _ = (input, granule);
        unreachable!(
            "observe_granule_dict_codes without support (driver protocol: \
             gate on dict_code_observe_supported)"
        );
    }
}

// ---------------------------------------------------------------------------
// canonical value bytes + round-trip verify (spec §18.1/§19.6)
// ---------------------------------------------------------------------------

/// Canonical value bytes of one datum word (spec §18.1) — the ONE
/// canonicalization serving round-trip verify and the bank logical hash.
///
/// # Safety
///
/// For pointer classes (`Fixed`, `VarlenaVerbatim`) `datum` must point at a
/// live, correctly-shaped image (Fixed: `len` readable bytes; Varlena: a
/// valid 4B-U varlena image) that outlives the returned slice. Byval classes
/// return a slice of `scratch`.
pub unsafe fn datum_canonical_bytes<'a>(
    class: StorageClass,
    datum: u64,
    scratch: &'a mut [u8; 8],
) -> FormatResult<&'a [u8]> {
    match class {
        StorageClass::ByvalWord { width, .. } => {
            scratch.copy_from_slice(&datum.to_le_bytes());
            Ok(&scratch[..width as usize])
        }
        StorageClass::F32 => {
            scratch.copy_from_slice(&datum.to_le_bytes());
            Ok(&scratch[..4])
        }
        StorageClass::F64 => {
            scratch.copy_from_slice(&datum.to_le_bytes());
            Ok(&scratch[..8])
        }
        StorageClass::Bool => {
            scratch[0] = (datum != 0) as u8;
            Ok(&scratch[..1])
        }
        StorageClass::Fixed { len } => {
            // SAFETY: caller contract — datum points at `len` readable bytes.
            Ok(unsafe { core::slice::from_raw_parts(datum as *const u8, len as usize) })
        }
        StorageClass::VarlenaVerbatim => {
            let p = datum as *const u8;
            // SAFETY: caller contract — datum points at a valid 4B-U image.
            let header = u32::from_le_bytes(unsafe {
                core::slice::from_raw_parts(p, 4).try_into().expect("len 4")
            });
            let payload_len = varlena_4b_u_payload_len(header, "datum varlena")? as usize;
            // SAFETY: caller contract — the image is payload_len + 4 bytes.
            Ok(unsafe { core::slice::from_raw_parts(p.add(4), payload_len) })
        }
    }
}

/// Round-trip verify at encode (the election quadruple's fixed leg, spec
/// §19.6): decode the just-written section and compare every NON-NULL row's
/// canonical bytes against the input. Null slots are placeholder-encoded and
/// not value-compared (validity is the only truth, spec §6.6).
pub fn verify_roundtrip(
    vt: &CodecVtable,
    ctx: &KernelCtx<'_>,
    input: &EncodeInput<'_>,
    datum_scratch: &mut [u64],
    arena_scratch: &mut [u8],
) -> FormatResult<()> {
    let rows = input.rows as usize;
    if datum_scratch.len() < rows {
        return Err(FormatError::Bounds {
            at: "verify datum scratch",
        });
    }
    let mut out = DecodeOut {
        datums: &mut datum_scratch[..rows],
        arena: ByteArena::new(arena_scratch),
    };
    let wrote = (vt.decode_full)(ctx, &mut out)?;
    if wrote != input.rows {
        return Err(FormatError::EncodeContract {
            detail: "round-trip row count",
        });
    }
    for r in 0..input.rows {
        if !input.valid(r) {
            continue;
        }
        let mut s_in = [0u8; 8];
        let mut s_out = [0u8; 8];
        // SAFETY: EncodeInput datums obey the pointer-class contract by
        // construction (the writer built them); decode outputs point into
        // the live arena_scratch or — dict streams — into the live ctx
        // dict sections (the zero-copy gather law on `DecodeOut`).
        let a = unsafe { datum_canonical_bytes(input.class, input.datums[r as usize], &mut s_in)? };
        let b = unsafe { datum_canonical_bytes(input.class, out.datums[r as usize], &mut s_out)? };
        if a != b {
            return Err(FormatError::EncodeContract {
                detail: "round-trip value mismatch",
            });
        }
    }
    Ok(())
}

// The datum-word extension conventions (spec §6.7) are class facts; pin the
// class-id constants the vtables key on so a class renumbering cannot slip
// through silently.
const _: () = {
    assert!(CLASS_BYVAL == 0);
    assert!(CLASS_F32 == 1);
    assert!(CLASS_F64 == 2);
    assert!(CLASS_BOOL == 3);
};
