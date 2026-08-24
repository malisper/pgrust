//! The decode-kernel invocation layer (spec §19, read side): vtable
//! resolution ONCE at stream open (never in or near the hot loop — the S4
//! pow2-switch law), per-extent parsed state (header, frame table, gcount
//! prefix), per-face section faulting (crate-doc policy table), and the six
//! face calls over CRC-validated bytes.
//!
//! Contract notes set by this layer (cited by the M3-F report; binding on
//! M3-C's kernels because the reader builds every ctx):
//!
//! - **`validity` face ctx carries `bytes = &[]`.** The validity bitmap
//!   layout is encoding-independent (spec §6.6) and the canonical kernel
//!   reads only `ctx.validity_bytes`; faulting the values extent for a
//!   bitmap question would violate O(streams-touched). A validity kernel
//!   must not consult section bytes.
//! - **Wrapped sections**: `entry.wrapper != 0` resolves an unwrapper from
//!   the [`CodecBinding`] at cursor open — absent unwrapper is a typed
//!   `WrapperUnsupported` refusal BEFORE any payload fault. The unwrapper
//!   returns a rebuilt UNWRAPPED section image (header `wrapper = 0`,
//!   uncompressed payload, tables), so kernels stay wrapper-oblivious
//!   (wrappers are orthogonal to encodings, spec §4).
//! - **Single-slice ABI limits**: `KernelCtx.overflow` and `DictSections`
//!   are single slices, so overflow/dict streams split across extents refuse
//!   typed (`Unsupported`) — an A-lane report item, not a silent skip.
//! - **`ctx.rows` carries the stream's PER-ENTRY dimension** (== `values`).
//!   The frozen reference kernels size the validity bitmap and mask output
//!   by `ctx.rows` (`validity_from_ctx`, `nonnull_count`), while a child
//!   stream's bitmap covers its VALUES (spec §6.6) — for root streams the
//!   two are the same number (the §2 closed form), for child streams only
//!   the value count is coherent. The rows-vs-values asymmetry in the
//!   reference kernels is an A-lane ABI report item; until ruled, this
//!   layer feeds every kernel the entry count in both fields.
//! - **The `validity` face self-counts from the validity stream** (its own
//!   gcount table for child streams, the §2 closed form for root streams) —
//!   consulting the values extent for a bitmap question would violate
//!   O(streams-touched). The values-vs-validity count cross-witness lives
//!   on the decode faces, where the values extent is faulted anyway.

use std::sync::Arc;

use pgrc2_format::abi::{
    CodecRegistry, CodecVtable, DecodeOut, KernelCtx, KernelKey, MetaProbeFn, Selection,
    ValidityVerdict,
};
use pgrc2_format::dict::{DictLayout, DictSections};
use pgrc2_format::enc::{EncodingId, Wrapper};
use pgrc2_format::geom;
use pgrc2_format::meta::{MetaAnswer, MetaProbe};
use pgrc2_format::part::{
    StreamRole, StreamSectionHdr, STREAMF_HAS_OVERFLOW, STREAM_SECTION_HDR_LEN,
};
use pgrc2_format::{FormatError, FormatResult};

use crate::openpart::{OpenPart, SegBuf};
use crate::streams::ParsedStream;
use crate::{ReadError, ReadResult};

// ---------------------------------------------------------------------------
// the binding seam (kernels + unwrappers arrive together, this crate frozen)
// ---------------------------------------------------------------------------

/// Decode-side section unwrap (spec §6.4 `wrapper != 0`). Implemented by the
/// codec crate next to its kernels; registered in the [`CodecBinding`].
pub trait SectionUnwrapper: Sync {
    fn wrapper(&self) -> Wrapper;
    /// Rebuild the unwrapped section image: `StreamSectionHdr` with
    /// `wrapper = 0`, uncompressed payload, frame/gcount tables addressing
    /// it. Bounds-validated, typed errors, no panic.
    fn unwrap_section(&self, hdr: &StreamSectionHdr, section: &[u8]) -> FormatResult<Vec<u8>>;
    /// [stack] Decompress ONE §6.4 wrapper block into exactly `dst`
    /// (compressed span from the block-offset table, uncompressed span from
    /// the frame table). `Ok(false)` = this binding cannot serve block
    /// grain — callers fall back to the whole-section unwrap (the default,
    /// so existing bindings keep today's behavior verbatim).
    fn unwrap_block(&self, _src: &[u8], _dst: &mut [u8]) -> FormatResult<bool> {
        Ok(false)
    }
}

/// Everything the reader dispatches through: the kernel registry (spec
/// §19.5) plus the section unwrappers. M3-C ships a static binding with its
/// kernel lists and LZ4/Zstd unwrappers; this crate never changes to admit
/// them.
pub struct CodecBinding<'a> {
    pub registry: &'a CodecRegistry,
    pub unwrappers: &'a [&'a dyn SectionUnwrapper],
}

impl<'a> CodecBinding<'a> {
    fn unwrapper_for(&self, wrapper: Wrapper) -> Option<&'a dyn SectionUnwrapper> {
        self.unwrappers
            .iter()
            .copied()
            .find(|u| u.wrapper() == wrapper)
    }
}

/// A leaked binding over the reference Verbatim/Const vtables with no
/// unwrappers — test/tool scaffolding (leaks three small allocations per
/// call; product bindings are statics assembled by the codec crate).
pub fn reference_binding_leaked() -> &'static CodecBinding<'static> {
    let vts: &'static [&'static CodecVtable] =
        Box::leak(Box::new(pgrc2_format::verbatim::reference_vtables()));
    let reg: &'static CodecRegistry = Box::leak(Box::new(CodecRegistry::new(vts)));
    Box::leak(Box::new(CodecBinding {
        registry: reg,
        unwrappers: &[],
    }))
}

// ---------------------------------------------------------------------------
// per-extent parsed state
// ---------------------------------------------------------------------------

/// One loaded, validated extent of a granule-organized stream.
struct ExtentState {
    bytes: SegBuf,
    /// Parsed once (kernels are allocation-free and never parse it).
    frame_table: Option<Vec<u32>>,
    /// Child streams: per-granule value counts from the gcount table;
    /// `None` = root stream (closed-form from part rows).
    gcounts: Option<Vec<u32>>,
    /// Validity streams: byte offset of each granule's bitmap inside the
    /// payload region (`prefix[i+1] - prefix[i]` = that granule's bytes).
    vbyte_prefix: Option<Vec<u32>>,
    payload_start: u32,
    payload_end: u32,
}

impl ExtentState {
    fn payload(&self) -> &[u8] {
        &self.bytes.bytes()[self.payload_start as usize..self.payload_end as usize]
    }
}

/// A single-extent byte-run stream (overflow / dict index / dict payload):
/// the payload region inside its one loaded section. Shared with the lazy
/// dict handle (`crate::dicthandle`).
pub(crate) struct RegionState {
    pub(crate) bytes: SegBuf,
    pub(crate) payload_start: u32,
    pub(crate) payload_end: u32,
}

impl RegionState {
    pub(crate) fn payload(&self) -> &[u8] {
        &self.bytes.bytes()[self.payload_start as usize..self.payload_end as usize]
    }
}

/// Payload bounds of a section (spec §6.4): past the header, before the
/// first table. Mirrors the reference codec's region math. `pub(crate)`
/// since M3-L3: the frame-lazy dict payload region (`crate::dicthandle`)
/// computes the same bounds from its extent-0 fault.
pub(crate) fn payload_bounds(hdr: &StreamSectionHdr, section_len: usize) -> FormatResult<(u32, u32)> {
    let end = if hdr.frame_table_off != 0 {
        hdr.frame_table_off as usize
    } else if hdr.gcount_table_off != 0 {
        hdr.gcount_table_off as usize
    } else {
        section_len
    };
    if end < STREAM_SECTION_HDR_LEN || end > section_len {
        return Err(FormatError::Bounds {
            at: "payload region",
        });
    }
    // Width-safe narrowing (spec §6.4, idx-196): the section-relative table
    // offsets are u32-typed fields, but an ASSEMBLED multi-extent byte-run
    // region (`load_region`) sums per-extent lengths — each capped at
    // u32::MAX, the SUM is not — so the `section_len` fallback can exceed
    // 2^32. A bare `end as u32` would truncate to a value BELOW payload_start
    // (an inverted window that panics `payload()` or underflows downstream
    // pointer math). Reject any payload end that does not fit the returned
    // u32 with a typed, catchable error; valid (<=4 GiB) regions are
    // unaffected. `end >= STREAM_SECTION_HDR_LEN` is already proven, so the
    // returned pair is ordered by construction.
    let end = u32::try_from(end).map_err(|_| FormatError::Bounds {
        at: "payload region exceeds u32",
    })?;
    Ok((STREAM_SECTION_HDR_LEN as u32, end))
}

// ---------------------------------------------------------------------------
// StreamCursor
// ---------------------------------------------------------------------------

/// The per-(column, path) decode cursor: vtable + companion streams resolved
/// at open, extents faulted lazily per face call. Worker-confined (`&mut
/// self` faces, R1–R6); the underlying part is shared.
pub struct StreamCursor<'b> {
    part: Arc<OpenPart>,
    binding: &'b CodecBinding<'b>,
    vt: &'b CodecVtable,
    /// The NORMALIZED dispatch key (`enc::stream_kernel_key` over the entry
    /// fields) — every ctx this cursor builds carries it, never raw fields.
    key: KernelKey,
    attno: u32,
    path_ord: u32,
    /// Cloned stream facts (entry + extents) for the values stream and
    /// companions — parsed-directory content, small.
    values: ParsedStream,
    validity: Option<ParsedStream>,
    overflow: Option<ParsedStream>,
    dict_index: Option<ParsedStream>,
    dict_payload: Option<ParsedStream>,
    vstate: Vec<Option<ExtentState>>,
    valstate: Vec<Option<ExtentState>>,
    ovf_state: Option<RegionState>,
    dict_state: Option<(RegionState, RegionState, u32, pgrc2_format::dict::DictCharLenForm)>,
}

impl core::fmt::Debug for StreamCursor<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("StreamCursor")
            .field("attno", &self.attno)
            .field("path_ord", &self.path_ord)
            .finish_non_exhaustive()
    }
}

impl<'b> StreamCursor<'b> {
    /// Open the cursor for (attno, path_ord) over the `Values` role.
    pub fn open(
        part: Arc<OpenPart>,
        binding: &'b CodecBinding<'b>,
        attno: u32,
        path_ord: u32,
    ) -> ReadResult<StreamCursor<'b>> {
        StreamCursor::open_role(part, binding, attno, path_ord, StreamRole::Values)
    }

    /// Open the cursor over a value-bearing role (`Values`, `ChildValues`,
    /// `Sizes` — the nesting currency, spec §6.5): resolve the vtable from
    /// the registry (unknown/reserved encoding IDs refuse first, spec
    /// §19.5), resolve companion streams, resolve the unwrapper if the
    /// stream is wrapped. Faults at most the StreamDir section.
    pub fn open_role(
        part: Arc<OpenPart>,
        binding: &'b CodecBinding<'b>,
        attno: u32,
        path_ord: u32,
        role: StreamRole,
    ) -> ReadResult<StreamCursor<'b>> {
        if !matches!(
            role,
            StreamRole::Values | StreamRole::ChildValues | StreamRole::Sizes
        ) {
            return Err(ReadError::Unsupported {
                what: "cursor over a non-value role",
            });
        }
        let dir = part.stream_directory()?;
        let values = dir
            .lookup(attno, path_ord, role)
            .ok_or(ReadError::StreamMissing {
                attno,
                path_ord,
                role: role.as_u8(),
            })?
            .clone();
        // Adjudicate the encoding ID before any kernel-table walk (typed
        // unknown-vs-reserved split, born-RED-tested upstream).
        EncodingId::resolve(values.entry.encoding)?;
        // A STRUCTURAL parent entry (ArrayDual — TY-1) is an election
        // marker, not a decodable stream: readers compose through the
        // `Sizes`/`ChildValues` substream cursors + the codec's
        // `assemble_array_datums`. Refuse typed BEFORE the kernel-key
        // normalization (which would report it as corruption).
        if values.entry.encoding == EncodingId::ArrayDual.as_u16() {
            return Err(ReadError::Unsupported {
                what: "structural parent stream (compose via Sizes/ChildValues)",
            });
        }
        // Wrapper resolution at open: a wrapped stream without a registered
        // unwrapper refuses before any payload fault.
        let wrapper = Wrapper::from_u8(values.entry.wrapper)?;
        if wrapper != Wrapper::None && binding.unwrapper_for(wrapper).is_none() {
            return Err(ReadError::Format(FormatError::WrapperUnsupported {
                wrapper: wrapper.as_u8(),
            }));
        }
        // Resolve through the ONE normalization (spec §6.3/§19.5, A-lane
        // amendment M3-A2): the entry width byte is per-encoding vocabulary
        // (DICT_CODES: max code width — a stats fact), the dispatch width is
        // normalized. A raw-field key cannot resolve a §6.3-conformant dict
        // stream (the M3-G gap-1 seam, closed here).
        let key = pgrc2_format::enc::stream_kernel_key(
            values.entry.encoding,
            values.entry.class,
            values.entry.width,
        )?;
        let vt = binding.registry.resolve(key)?;
        // Validity binding: a child stream's OWN bitmap is VALUE-aligned
        // (spec §6.6 — "a child stream's bitmap covers its VALUES") and
        // binds exactly as in v3. The ONE exception is a ChildValues stream
        // under an ArrayDual STRUCTURAL parent (TY-1): there the validity
        // stream at this (attno, path_ord) is the PARENT'S row-aligned
        // bitmap — elements carry no validity by construction (null
        // elements refuse the split at seal) — and slicing a row-aligned
        // bitmap against an element count would refuse typed. Discriminate
        // on the directory fact itself: the structural parent entry.
        let under_array_dual_parent = role == StreamRole::ChildValues
            && dir
                .lookup(attno, path_ord, StreamRole::Values)
                .is_some_and(|p| p.entry.encoding == EncodingId::ArrayDual.as_u16());
        let validity = if under_array_dual_parent {
            None
        } else {
            dir.lookup(attno, path_ord, StreamRole::Validity).cloned()
        };
        let overflow = if values.entry.flags & STREAMF_HAS_OVERFLOW != 0 {
            Some(
                dir.lookup(attno, path_ord, StreamRole::Overflow)
                    .ok_or(ReadError::Format(FormatError::Corrupt {
                        at: "HAS_OVERFLOW without Overflow stream",
                    }))?
                    .clone(),
            )
        } else {
            None
        };
        let is_dict = values.entry.encoding == EncodingId::DictCodes.as_u16();
        let (dict_index, dict_payload) = if is_dict {
            let di = dir
                .lookup(attno, path_ord, StreamRole::DictIndex)
                .ok_or(ReadError::Format(FormatError::Corrupt {
                    at: "dict column without DictIndex stream",
                }))?
                .clone();
            let dp = dir
                .lookup(attno, path_ord, StreamRole::DictPayload)
                .ok_or(ReadError::Format(FormatError::Corrupt {
                    at: "dict column without DictPayload stream",
                }))?
                .clone();
            (Some(di), Some(dp))
        } else {
            (None, None)
        };
        // Companion streams share the refusal-before-fault pin (CMP-B: the
        // byte-run stream class can be wrapped now — a wrapped dict/overflow
        // stream without its unwrapper refuses TYPED at open, before any
        // payload fault; validity is checked under the same law).
        for companion in [
            validity.as_ref(),
            overflow.as_ref(),
            dict_index.as_ref(),
            dict_payload.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            let w = Wrapper::from_u8(companion.entry.wrapper)?;
            if w != Wrapper::None && binding.unwrapper_for(w).is_none() {
                return Err(ReadError::Format(FormatError::WrapperUnsupported {
                    wrapper: w.as_u8(),
                }));
            }
        }
        let n_v = values.extents.len();
        let n_val = validity.as_ref().map(|s| s.extents.len()).unwrap_or(0);
        Ok(StreamCursor {
            part,
            binding,
            vt,
            key,
            attno,
            path_ord,
            values,
            validity,
            overflow,
            dict_index,
            dict_payload,
            vstate: (0..n_v).map(|_| None).collect(),
            valstate: (0..n_val).map(|_| None).collect(),
            ovf_state: None,
            dict_state: None,
        })
    }

    /// The values stream entry (encoding, flags, width — the resolved key).
    pub fn entry(&self) -> &pgrc2_format::part::StreamEntry {
        &self.values.entry
    }

    pub fn attno(&self) -> u32 {
        self.attno
    }

    pub fn path_ord(&self) -> u32 {
        self.path_ord
    }

    /// Logical rows in part-granule `g` (closed form in (rows, grain) —
    /// spec §2 + SB-10; the grain is the part footer's).
    pub fn rows_in_granule(&self, g: u32) -> u32 {
        geom::rows_in_granule_at(self.part.rows(), self.part.grain(), g)
    }

    pub fn granule_count(&self) -> u32 {
        self.part.footer().granule_count
    }

    /// Whether the column stores a validity stream in this part (spec §6.1:
    /// present iff ≥ 1 NULL). The O-6 zero-null proof input for dict lanes.
    pub fn has_validity_stream(&self) -> bool {
        self.validity.is_some()
    }

    // -- the six faces ----------------------------------------------------

    /// `decode_full` (spec §19.3): all rows of granule `g` into `out`.
    pub fn decode_full(&mut self, g: u32, out: &mut DecodeOut<'_>) -> ReadResult<u32> {
        let vt = self.vt;
        let (ei, gie, values_g, _rows_g) = self.ensure_values(g)?;
        let vslice = self.ensure_validity_slice(g, values_g)?;
        self.ensure_overflow()?;
        self.ensure_dict()?;
        let ctx = self.build_ctx(g, ei, gie, values_g, vslice, true, true)?;
        (vt.decode_full)(&ctx, out).map_err(Into::into)
    }

    /// `decode_sel` (spec §19.3): survivor-only completion, dense outputs in
    /// selection order. Must equal `decode_full ∘ select` — this layer
    /// builds bit-identical ctxs for both faces, so the kernel property
    /// survives composition (pinned in `src/tests/decode_props.rs`).
    pub fn decode_sel(
        &mut self,
        g: u32,
        sel: &Selection<'_>,
        out: &mut DecodeOut<'_>,
    ) -> ReadResult<u32> {
        let vt = self.vt;
        let (ei, gie, values_g, _rows_g) = self.ensure_values(g)?;
        sel.validate(values_g)?;
        let vslice = self.ensure_validity_slice(g, values_g)?;
        self.ensure_overflow()?;
        self.ensure_dict()?;
        let ctx = self.build_ctx(g, ei, gie, values_g, vslice, true, true)?;
        (vt.decode_sel)(&ctx, sel, out).map_err(Into::into)
    }

    /// `decode_codes` (spec §19.3): global codes without dict bytes.
    pub fn decode_codes(&mut self, g: u32, out: &mut [u32]) -> ReadResult<u32> {
        let vt = self.vt;
        let (ei, gie, values_g, _rows_g) = self.ensure_values(g)?;
        let vslice = self.ensure_validity_slice(g, values_g)?;
        let ctx = self.build_ctx(g, ei, gie, values_g, vslice, false, false)?;
        (vt.decode_codes)(&ctx, out).map_err(Into::into)
    }

    /// `validity` (spec §19.3): mask words for granule `g`. Faults ONLY the
    /// validity extent (ctx carries empty section bytes — module doc); the
    /// per-entry count comes from the validity stream ITSELF (its own
    /// gcount table for child streams, the §2 closed form for root
    /// streams) — never from the values extent.
    pub fn validity(&mut self, g: u32, out: &mut [u64]) -> ReadResult<ValidityVerdict> {
        let vt = self.vt;
        if g >= self.granule_count() {
            return Err(ReadError::Format(FormatError::Bounds {
                at: "granule ordinal",
            }));
        }
        let (values_g, vslice) = match self.load_validity_extent(g)? {
            // No validity stream: all-valid (spec §6.1). The kernel
            // short-circuits on `validity_bytes = None` before any count
            // geometry, so the closed form serves both root and child
            // streams here.
            None => (self.rows_in_granule(g), None),
            Some((ei, gie)) => {
                let st = self.valstate[ei].as_ref().expect("just loaded");
                let values_g = match &st.gcounts {
                    Some(gc) => *gc.get(gie as usize).ok_or(ReadError::Format(
                        FormatError::Bounds { at: "gcount index" },
                    ))?,
                    None => self.rows_in_granule(g),
                };
                (values_g, self.validity_slice_at(ei, gie, values_g)?)
            }
        };
        let ctx = self.build_empty_ctx(g, values_g, vslice);
        (vt.validity)(&ctx, out).map_err(Into::into)
    }

    /// `meta_probe` (spec §19.3): encoding-local stat answers.
    pub fn meta_probe(&mut self, g: u32, probe: &MetaProbe) -> ReadResult<MetaAnswer> {
        let vt = self.vt;
        let (ei, gie, values_g, _rows_g) = self.ensure_values(g)?;
        let vslice = self.ensure_validity_slice(g, values_g)?;
        let ctx = self.build_ctx(g, ei, gie, values_g, vslice, false, false)?;
        let f: MetaProbeFn = vt.meta_probe;
        f(&ctx, probe).map_err(Into::into)
    }

    /// `dict_handle` (spec §19.3): the ABI face — dict geometry facts from
    /// the kernel. (The reader-level lazy handle is
    /// [`crate::dicthandle::DictHandle`].)
    pub fn dict_handle_face(&mut self, g: u32) -> ReadResult<DictLayout> {
        let vt = self.vt;
        let (ei, gie, values_g, _rows_g) = self.ensure_values(g)?;
        let vslice = self.ensure_validity_slice(g, values_g)?;
        self.ensure_dict()?;
        let ctx = self.build_ctx(g, ei, gie, values_g, vslice, false, true)?;
        (vt.dict_handle)(&ctx).map_err(Into::into)
    }

    /// Values in part-granule `g` of THIS stream (child streams: from the
    /// gcount table; root: closed form).
    pub fn values_in_granule(&mut self, g: u32) -> ReadResult<u32> {
        Ok(self.ensure_values(g)?.2)
    }

    /// The resident dict PAYLOAD region as (base address, byte length);
    /// `None` for non-dict streams. This is the zero-copy aliasing target of
    /// dict-varlena `decode_full`/`decode_sel` datums (the `DecodeOut`
    /// pointer-class output law): the region is a part-cached `SegBuf`
    /// (8-aligned, insert-only — resident for the `OpenPart`'s life), so
    /// datums into it outlive every granule-claim consumption window.
    /// Containment gates (pgrc2_qa) use these bounds.
    pub fn dict_payload_bounds(&mut self) -> ReadResult<Option<(usize, usize)>> {
        if self.dict_payload.is_none() {
            return Ok(None);
        }
        self.ensure_dict()?;
        Ok(self.dict_state.as_ref().map(|(_, p, _, _)| {
            let pl = p.payload();
            (pl.as_ptr() as usize, pl.len())
        }))
    }

    // -- extent state -----------------------------------------------------

    /// Ensure the values extent covering `g` is loaded; returns
    /// (extent idx, granule-in-extent, values_g, rows_g).
    fn ensure_values(&mut self, g: u32) -> ReadResult<(usize, u32, u32, u32)> {
        if g >= self.granule_count() {
            return Err(ReadError::Format(FormatError::Bounds {
                at: "granule ordinal",
            }));
        }
        let (ei, rec) = self.values.extent_for_granule(g)?;
        let ei = ei as usize;
        let gie = g - self.values.extents[ei].granule_start;
        if self.vstate[ei].is_none() {
            let st = load_extent(
                &self.part,
                self.binding,
                &self.values,
                ei as u32,
                &rec.clone(),
                false,
            )?;
            self.vstate[ei] = Some(st);
        }
        let rows_g = self.rows_in_granule(g);
        let st = self.vstate[ei].as_ref().expect("just loaded");
        let values_g = match &st.gcounts {
            Some(gc) => *gc.get(gie as usize).ok_or(ReadError::Format(FormatError::Bounds {
                at: "gcount index",
            }))?,
            None => rows_g,
        };
        Ok((ei, gie, values_g, rows_g))
    }

    /// Load the validity extent covering `g` (iff the stream exists);
    /// returns (extent idx, granule-in-extent).
    fn load_validity_extent(&mut self, g: u32) -> ReadResult<Option<(usize, u32)>> {
        let Some(vs) = &self.validity else {
            return Ok(None);
        };
        let (ei, rec) = vs.extent_for_granule(g)?;
        let ei = ei as usize;
        let gie = g - vs.extents[ei].granule_start;
        if self.valstate[ei].is_none() {
            let vs_cloned = vs.clone();
            let st = load_extent(
                &self.part,
                self.binding,
                &vs_cloned,
                ei as u32,
                &rec.clone(),
                true,
            )?;
            self.valstate[ei] = Some(st);
        }
        Ok(Some((ei, gie)))
    }

    /// The granule's bitmap slice coordinates inside a loaded validity
    /// extent, cross-checked against the caller's value count.
    fn validity_slice_at(
        &self,
        ei: usize,
        gie: u32,
        values_g: u32,
    ) -> ReadResult<Option<(usize, u32, u32)>> {
        let st = self.valstate[ei].as_ref().expect("validity extent loaded");
        let prefix = st.vbyte_prefix.as_ref().expect("validity prefix built");
        let (b0, b1) = (
            *prefix
                .get(gie as usize)
                .ok_or(ReadError::Format(FormatError::Bounds { at: "validity prefix" }))?,
            *prefix
                .get(gie as usize + 1)
                .ok_or(ReadError::Format(FormatError::Bounds { at: "validity prefix" }))?,
        );
        // Two-witness discipline at the slice level: the bitmap must be
        // sized for exactly this granule's values (spec §6.6). Real on the
        // decode faces (values_g comes from the values extent); structural
        // on the validity face (same-source counts).
        if (b1 - b0) as usize != (values_g as usize).div_ceil(8) {
            return Err(ReadError::Format(FormatError::Corrupt {
                at: "validity bitmap size vs values",
            }));
        }
        Ok(Some((ei, b0, b1)))
    }

    /// Ensure the validity extent covering `g` (iff the stream exists) and
    /// return the granule's bitmap slice coordinates.
    fn ensure_validity_slice(
        &mut self,
        g: u32,
        values_g: u32,
    ) -> ReadResult<Option<(usize, u32, u32)>> {
        match self.load_validity_extent(g)? {
            None => Ok(None),
            Some((ei, gie)) => self.validity_slice_at(ei, gie, values_g),
        }
    }

    fn ensure_overflow(&mut self) -> ReadResult<()> {
        let Some(os) = &self.overflow else {
            return Ok(());
        };
        if self.ovf_state.is_some() {
            return Ok(());
        }
        self.ovf_state = Some(load_region(
            &self.part,
            self.binding.unwrappers,
            os,
            "overflow stream",
        )?);
        Ok(())
    }

    fn ensure_dict(&mut self) -> ReadResult<()> {
        let (Some(di), Some(dp)) = (&self.dict_index, &self.dict_payload) else {
            return Ok(());
        };
        if self.dict_state.is_some() {
            return Ok(());
        }
        let idx = load_region(&self.part, self.binding.unwrappers, di, "dict index stream")?;
        let pay = load_region(&self.part, self.binding.unwrappers, dp, "dict payload stream")?;
        // [cold2] census: a cursor-side whole-payload assembly (decode-path
        // dict_state / is_dict-class probes) — the attribution witness.
        crate::dicthandle::DICT_WHOLE_FAULT_BYTES
            .fetch_add(pay.payload().len() as u64, core::sync::atomic::Ordering::Relaxed);
        let entry_count =
            u32::try_from(di.entry.values).map_err(|_| ReadError::Format(FormatError::Corrupt {
                at: "dict entry count",
            }))?;
        if idx.payload().len() != entry_count as usize * pgrc2_format::dict::DICT_INDEX_ENTRY_LEN {
            return Err(ReadError::Format(FormatError::Corrupt {
                at: "dict index size vs entry count",
            }));
        }
        // M5d char-len record: the DictIndex dir entry names the
        // `char_field` form; the ctx carries it so `dict_entry` stays pure.
        let charlen_form =
            pgrc2_format::dict::DictCharLenForm::from_flags(di.entry.flags)
                .map_err(ReadError::Format)?;
        self.dict_state = Some((idx, pay, entry_count, charlen_form));
        Ok(())
    }

    // -- ctx assembly -----------------------------------------------------

    #[allow(clippy::too_many_arguments)]
    fn build_ctx(
        &self,
        g: u32,
        ei: usize,
        gie: u32,
        values_g: u32,
        vslice: Option<(usize, u32, u32)>,
        with_overflow: bool,
        with_dict: bool,
    ) -> ReadResult<KernelCtx<'_>> {
        let st = self.vstate[ei].as_ref().expect("values extent loaded");
        let validity_bytes = vslice.map(|(vei, b0, b1)| {
            let vst = self.valstate[vei].as_ref().expect("validity extent loaded");
            &vst.payload()[b0 as usize..b1 as usize]
        });
        let overflow = if with_overflow {
            self.ovf_state.as_ref().map(|r| r.payload())
        } else {
            None
        };
        let dict = if with_dict {
            self.dict_state.as_ref().map(|(i, p, n, form)| DictSections {
                index: i.payload(),
                payload: p.payload(),
                entry_count: *n,
                charlen_form: *form,
            })
        } else {
            None
        };
        Ok(KernelCtx {
            key: self.key,
            flags: self.values.entry.flags,
            fixed_len: self.values.entry.fixed_len,
            bytes: st.bytes.bytes(),
            frame_table: st.frame_table.as_deref(),
            granule: g,
            granule_in_extent: gie,
            // The per-entry dimension in BOTH fields (module doc: the
            // reference kernels size validity geometry by `rows`; for root
            // streams this IS the §2 closed form).
            rows: values_g,
            values: values_g,
            validity_bytes,
            overflow,
            dict,
        })
    }

    /// Ctx for the validity face: empty section bytes (module doc).
    fn build_empty_ctx(
        &self,
        g: u32,
        values_g: u32,
        vslice: Option<(usize, u32, u32)>,
    ) -> KernelCtx<'_> {
        let validity_bytes = vslice.map(|(vei, b0, b1)| {
            let vst = self.valstate[vei].as_ref().expect("validity extent loaded");
            &vst.payload()[b0 as usize..b1 as usize]
        });
        KernelCtx {
            key: self.key,
            flags: self.values.entry.flags,
            fixed_len: self.values.entry.fixed_len,
            bytes: &[],
            frame_table: None,
            granule: g,
            granule_in_extent: 0,
            rows: values_g,
            values: values_g,
            validity_bytes,
            overflow: None,
            dict: None,
        }
    }
}

/// Hard ceiling on one extent's granule count, independent of any
/// file-supplied footer (a corrupt footer could itself over-report). Kept
/// well above any realistic part geometry; its role is only to stop a crafted
/// 4-byte count from committing gigabytes before the payload-size witnesses
/// (gcount-table bounds, validity `payload_len`) run.
const MAX_EXTENT_GRANULES: u32 = 1 << 28;

/// Validate a file-supplied per-extent granule count against an independent
/// budget BEFORE it drives any allocation or count-driven loop (idx-198).
///
/// The class-level budget: an extent covers a SUBSET of the part's granules,
/// so its `granule_count` can never exceed the part footer's `granule_count`
/// (itself cross-checked against `rows`/`grain` at part open — the independent
/// witness), and never the hard [`MAX_EXTENT_GRANULES`] cap. A 4-byte lie
/// thus refuses catchably (`ERRCODE_DATA_CORRUPTED` via `FormatError::Corrupt`)
/// here, instead of sizing a multi-gigabyte `Vec` or spinning a
/// multi-billion-iteration loop. Returns the validated count as `usize` for
/// direct use as an allocation/loop bound.
fn checked_extent_granules(
    part: &OpenPart,
    rec: &pgrc2_format::part::ExtentRecord,
) -> ReadResult<usize> {
    if rec.granule_count > part.footer().granule_count
        || rec.granule_count > MAX_EXTENT_GRANULES
    {
        return Err(ReadError::Format(FormatError::Corrupt {
            at: "extent granule_count exceeds part budget",
        }));
    }
    Ok(rec.granule_count as usize)
}

/// Load + validate one granule-organized extent: fault (CRC under the part's
/// segment cache), header cross-witness against the stream entry, unwrap if
/// wrapped, parse frame table, parse the gcount table (child streams), build
/// the validity byte prefix (validity streams).
///
/// Wrapped extents (O-CMP-5(a), ruled 2026-08-10) resolve their unwrapper
/// from the stream ENTRY before any payload fault (the refusal-before-fault
/// pin, mirrored from cursor open) and go through the part's UNWRAPPED-image
/// cache: decompress once per part residency, every later cursor over this
/// extent hits the rebuilt encoded image.
fn load_extent(
    part: &Arc<OpenPart>,
    binding: &CodecBinding<'_>,
    ps: &ParsedStream,
    extent_idx: u32,
    rec: &pgrc2_format::part::ExtentRecord,
    is_validity: bool,
) -> ReadResult<ExtentState> {
    let entry_wrapper = Wrapper::from_u8(ps.entry.wrapper)?;
    let bytes = if entry_wrapper == Wrapper::None {
        let bytes = part.extent_bytes(&ps.entry, rec, extent_idx)?;
        let hdr = StreamSectionHdr::decode(bytes.bytes())?;
        if hdr.encoding != ps.entry.encoding
            || hdr.width != ps.entry.width
            || hdr.wrapper != ps.entry.wrapper
        {
            return Err(ReadError::Format(FormatError::Corrupt {
                at: "section header vs stream entry",
            }));
        }
        bytes
    } else {
        // Typed refusal BEFORE any payload fault (the cursor-open pin,
        // repeated here so the predicate is correct standalone).
        let uw = binding.unwrapper_for(entry_wrapper).ok_or(ReadError::Format(
            FormatError::WrapperUnsupported {
                wrapper: entry_wrapper.as_u8(),
            },
        ))?;
        part.unwrapped_extent_bytes(&ps.entry, rec, extent_idx, &mut |raw: &[u8]| {
            let hdr = StreamSectionHdr::decode(raw)?;
            if hdr.encoding != ps.entry.encoding
                || hdr.width != ps.entry.width
                || hdr.wrapper != ps.entry.wrapper
            {
                return Err(ReadError::Format(FormatError::Corrupt {
                    at: "section header vs stream entry",
                }));
            }
            let rebuilt = uw.unwrap_section(&hdr, raw)?;
            let twin = StreamSectionHdr::decode(&rebuilt)?;
            if twin.wrapper != Wrapper::None.as_u8() {
                return Err(ReadError::Format(FormatError::Corrupt {
                    at: "unwrapped section still wrapped",
                }));
            }
            Ok(rebuilt)
        })?
    };
    let hdr = StreamSectionHdr::decode(bytes.bytes())?;
    let frame_table = hdr.frame_table(bytes.bytes())?;
    let (payload_start, payload_end) = payload_bounds(&hdr, bytes.len())?;
    let gcounts = if hdr.gcount_table_off != 0 {
        // Class-level budget check BEFORE the table-bytes math or the
        // allocation/loop (idx-198): a file-supplied count can never exceed
        // the part footer / hard cap.
        let ng = checked_extent_granules(part, rec)?;
        let off = hdr.gcount_table_off as usize;
        let need = ng * 4;
        let end = off.checked_add(need).ok_or(ReadError::Format(FormatError::Bounds {
            at: "gcount table",
        }))?;
        if off < STREAM_SECTION_HDR_LEN || end > bytes.len() {
            return Err(ReadError::Format(FormatError::Bounds {
                at: "gcount table",
            }));
        }
        let mut v: Vec<u32> = Vec::new();
        v.try_reserve(ng).map_err(|_| {
            ReadError::Format(FormatError::Corrupt {
                at: "gcount table allocation",
            })
        })?;
        let mut sum: u64 = 0;
        for i in 0..ng {
            let b = &bytes.bytes()[off + i * 4..off + i * 4 + 4];
            let c = u32::from_le_bytes(b.try_into().expect("len 4"));
            sum += c as u64;
            v.push(c);
        }
        if sum != rec.values {
            return Err(ReadError::Format(FormatError::Corrupt {
                at: "gcount sum vs extent values",
            }));
        }
        Some(v)
    } else {
        None
    };
    let vbyte_prefix = if is_validity {
        // Class-level budget check BEFORE the allocation/loop (idx-198): the
        // validity prefix is sized and built from the extent's file-supplied
        // granule_count with no gcount-table byte witness to cap it (root
        // streams have no gcount table), so a 4-byte lie would otherwise
        // commit gigabytes / spin for billions of iterations before the
        // `acc > payload_len` witness below ever runs.
        let ng = checked_extent_granules(part, rec)?;
        let payload_len = (payload_end - payload_start) as usize;
        let mut prefix: Vec<u32> = Vec::new();
        prefix.try_reserve(ng + 1).map_err(|_| {
            ReadError::Format(FormatError::Corrupt {
                at: "validity prefix allocation",
            })
        })?;
        prefix.push(0u32);
        let mut acc: u64 = 0;
        for i in 0..ng as u32 {
            let vals = match &gcounts {
                Some(gc) => gc[i as usize],
                None => geom::rows_in_granule_at(part.rows(), part.grain(), rec.granule_start + i),
            };
            acc += (vals as u64).div_ceil(8);
            let acc32 = u32::try_from(acc).map_err(|_| {
                ReadError::Format(FormatError::Corrupt {
                    at: "validity prefix overflow",
                })
            })?;
            prefix.push(acc32);
        }
        if acc as usize > payload_len {
            return Err(ReadError::Format(FormatError::Corrupt {
                at: "validity payload short",
            }));
        }
        Some(prefix)
    } else {
        None
    };
    Ok(ExtentState {
        bytes,
        frame_table,
        gcounts,
        vbyte_prefix,
        payload_start,
        payload_end,
    })
}

/// Load a single-extent byte-run stream (overflow / dict): the payload
/// region of its one section. Multi-extent runs cannot flow through the
/// frozen single-slice ctx — typed `Unsupported` (crate doc).
pub(crate) fn load_region(
    part: &Arc<OpenPart>,
    unwrappers: &[&dyn crate::SectionUnwrapper],
    ps: &ParsedStream,
    what: &'static str,
) -> ReadResult<RegionState> {
    let entry_wrapper = Wrapper::from_u8(ps.entry.wrapper)?;
    if ps.extents.len() != 1 {
        // SB-7 frame-boundary dict extents: an UNWRAPPED byte-run stream
        // whose extent table partitions one contiguous section (the writer
        // cuts the DictPayload extents at dict-frame boundaries, each with
        // its own CRC) assembles back into the single region the frozen
        // ctx shape needs. Realized fault grain HERE is still the whole
        // region (every extent faults, CRC-validated individually); the
        // per-frame lazy fault rides the M3-L3 reader rebuild on exactly
        // this geometry — the format is ready, the laziness is the
        // consumer's. Wrapped multi-extent byte runs stay refused (the
        // O-CMP-5(a) unwrap is whole-section by design).
        if entry_wrapper != Wrapper::None {
            return Err(ReadError::Unsupported {
                what: "multi-extent wrapped byte-run stream",
            });
        }
        for w in ps.extents.windows(2) {
            if w[0].file_off + w[0].len != w[1].file_off {
                return Err(ReadError::Format(FormatError::Corrupt {
                    at: "byte-run extent contiguity",
                }));
            }
        }
        let total: usize = ps.extents.iter().map(|e| e.len as usize).sum();
        let mut off = 0usize;
        let mut segs: Vec<SegBuf> = Vec::with_capacity(ps.extents.len());
        for (i, rec) in ps.extents.iter().enumerate() {
            segs.push(part.extent_bytes(&ps.entry, rec, i as u32)?);
        }
        let bytes = SegBuf::assemble(total, |b| {
            for seg in &segs {
                b[off..off + seg.len()].copy_from_slice(seg.bytes());
                off += seg.len();
            }
            Ok(())
        })?;
        let hdr = StreamSectionHdr::decode(bytes.bytes())?;
        if hdr.encoding != ps.entry.encoding || hdr.wrapper != ps.entry.wrapper {
            return Err(ReadError::Format(FormatError::Corrupt { at: what }));
        }
        let (payload_start, payload_end) = payload_bounds(&hdr, bytes.len())?;
        return Ok(RegionState {
            bytes,
            payload_start,
            payload_end,
        });
    }
    let rec = ps.extents[0];
    let bytes = if entry_wrapper == Wrapper::None {
        let bytes = part.extent_bytes(&ps.entry, &rec, 0)?;
        let hdr = StreamSectionHdr::decode(bytes.bytes())?;
        if hdr.encoding != ps.entry.encoding || hdr.wrapper != ps.entry.wrapper {
            return Err(ReadError::Format(FormatError::Corrupt { at: what }));
        }
        bytes
    } else {
        // CMP-B (wrapper-offer class widening): byte-run streams unwrap
        // through the SAME part-resident unwrapped-image cache as value
        // extents (O-CMP-5(a)) — decompress once per part residency; the
        // rebuilt region is a part-lifetime `SegBuf`, so the StrView §7b
        // dict-payload region-stability law holds for wrapped payloads
        // exactly as for unwrapped ones. Typed refusal BEFORE any payload
        // fault when this binding lacks the arm (the old-binary posture —
        // this refusal replaces the v1 "byte-run streams are never wrapped"
        // refusal, which now applies only through it).
        let uw = unwrappers
            .iter()
            .find(|u| u.wrapper() == entry_wrapper)
            .ok_or(ReadError::Format(FormatError::WrapperUnsupported {
                wrapper: entry_wrapper.as_u8(),
            }))?;
        part.unwrapped_extent_bytes(&ps.entry, &rec, 0, &mut |raw: &[u8]| {
            let hdr = StreamSectionHdr::decode(raw)?;
            if hdr.encoding != ps.entry.encoding || hdr.wrapper != ps.entry.wrapper {
                return Err(ReadError::Format(FormatError::Corrupt { at: what }));
            }
            let rebuilt = uw.unwrap_section(&hdr, raw)?;
            let twin = StreamSectionHdr::decode(&rebuilt)?;
            if twin.wrapper != Wrapper::None.as_u8() {
                return Err(ReadError::Format(FormatError::Corrupt {
                    at: "unwrapped section still wrapped",
                }));
            }
            Ok(rebuilt)
        })?
    };
    let hdr = StreamSectionHdr::decode(bytes.bytes())?;
    let (payload_start, payload_end) = payload_bounds(&hdr, bytes.len())?;
    Ok(RegionState {
        bytes,
        payload_start,
        payload_end,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plain_hdr() -> StreamSectionHdr {
        // Closed-form/root stream with no tables: the `end = section_len`
        // fallback arm of `payload_bounds` (idx-196 truncation path).
        StreamSectionHdr {
            magic: 0,
            encoding: 0,
            width: 0,
            wrapper: 0,
            frame_count: 0,
            frame_table_off: 0,
            gcount_table_off: 0,
            uncompressed_len: 0,
            value_count: 0,
            reserved: 0,
        }
    }

    #[test]
    fn payload_bounds_ok_for_in_range_region() {
        let hdr = plain_hdr();
        let (start, end) = payload_bounds(&hdr, STREAM_SECTION_HDR_LEN + 100).expect("valid");
        assert_eq!(start, STREAM_SECTION_HDR_LEN as u32);
        assert_eq!(end, (STREAM_SECTION_HDR_LEN + 100) as u32);
        assert!(start <= end, "returned window must be ordered");
    }

    #[test]
    fn payload_bounds_rejects_over_u32_assembled_region() {
        // An assembled multi-extent byte-run region of 2^32 + 16 bytes: the
        // old `end as u32` truncated to 16 (< payload_start = 32), an
        // inverted window. Must now surface a typed, catchable error rather
        // than a truncated (32, 16) pair.
        let hdr = plain_hdr();
        let big = (u32::MAX as usize) + 1 + 16; // 2^32 + 16
        let err = payload_bounds(&hdr, big).err().unwrap();
        assert!(matches!(err, FormatError::Bounds { .. }));
    }

    #[test]
    fn payload_bounds_never_inverts_across_u32_boundary() {
        // Any accepted result is ordered; the boundary case (exactly u32::MAX
        // total) either accepts an ordered pair or refuses — never inverts.
        let hdr = plain_hdr();
        match payload_bounds(&hdr, u32::MAX as usize) {
            Ok((start, end)) => assert!(start <= end),
            Err(FormatError::Bounds { .. }) => {}
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }
}
