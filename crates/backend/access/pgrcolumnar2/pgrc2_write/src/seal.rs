//! The ONE seal implementation (chunk M3-D row): chunk stats → election →
//! encode → footer/meta assembly → part file.
//!
//! One part = one call to [`seal_part`]. The part image is assembled
//! deterministically (part bytes are a pure function of (schema, elections,
//! input row partition) — spec §1) and written to a `tmp-<fxid>-<seq>.pgrc2t`
//! file, HEADER FIRST (spec §5: a crashed partial file is identifiable as
//! pgrc2 and never readable — the tmp name additionally keeps it out of
//! every reader's namespace until publish renames it). `part_no` is 0 in the
//! sealed image and is patched at publish (spec §5.1: assigned at publish,
//! monotone).
//!
//! Laws enforced on this path, every seal, no opt-outs:
//!
//! - **`verify_roundtrip` per granule per value stream** (spec §19.6, the
//!   election quadruple's fixed leg): the just-encoded section is decoded
//!   through the SAME vtable a reader resolves and compared value-by-value.
//!   Failure is a typed [`WriteError::RoundTrip`] — the part never reaches
//!   a file.
//! - **Two-witness null law** (spec §6.6): every granule/band/part stats
//!   record's `nonnull` is cross-checked against the EMITTED validity
//!   bitmap's popcount (not the input's — the emitted bytes are the second
//!   witness). Skew is a typed refusal; the check count is witnessed in the
//!   [`SealReport`].
//! - **O(streams-touched) discipline**: sealing reads nothing back but the
//!   in-memory section images it just wrote; no whole-part re-reads, no
//!   file reads at all before publish.
//!
//! File section order: per-stream value extents (per band) with their
//! overflow extents, then validity streams, then per-stream Stats (+ any
//! builder aux sections), PathTable (when shredded), SortKey, StreamDir,
//! the section table, FooterFixed, PartTail. Sections start 8-aligned
//! (writer law, spec §1 — makes entry alignment absolute).

use pgrc2_format::abi::{
    verify_roundtrip, ByteArena, CodecVtable, ColumnMetaBuilder, EncodeInput, GranuleEncoder,
    KernelCtx, KernelKey,
};
use pgrc2_format::class::{StorageClass, CLASS_BYVAL};
use pgrc2_format::enc::{EncodingId, Wrapper};
use pgrc2_format::geom::{self, GranuleGrain, GRANULES_PER_BAND, GRANULE_ROWS};
use pgrc2_format::rowid::MAX_GRANULES_PER_PART;
use pgrc2_format::meta::StatsRecord;
use pgrc2_format::part::{
    ExtentRecord, FooterFixed, OverflowSink, PartHeader, PartTail, SectionEntry, SectionKind,
    StreamCloseout, StreamEntry, StreamRole, StreamSectionHdr, StreamSectionWriter, FOOTER_MAGIC,
    SECTIONF_META_ZSTD, STREAMF_HAS_OVERFLOW, STREAMF_SIGNED,
};
use pgrc2_format::sortkey::{NullsOrder, SortDir, SortKeyEntry, SortKeyRecord};
use pgrc2_format::verbatim::{encode_validity_bitmap, reference_vtables, VerbatimEncoder};
use pgrc2_format::wire::{pad_to, varlena_entry_at};

use pgrc2_format::dict::DictSections;

use pgrc2_codec::arraydual::{
    assemble_array_datums, assembled_arena_bytes, elect_array_split, ArrayElemFacts, ArraySplit,
};

use crate::elect::{
    elect_stream, verbatim_baseline_len, wins_twenty_pct, CandidateSource, Elected, ElectPlan,
    ElectionWitness, ExtentShape, FullElectInput, FullElection,
};
use crate::ingest::ColBuffer;
use crate::shred::{encode_path_table, ShredLane};
use crate::structural::{ClusterKeyDecl, StructuralPolicy};
use crate::wvfs::WriteVfs;
use crate::{WriteError, WriteResult};

/// Part-level identity facts (schema fingerprint per spec §5.5, computed by
/// the writer once per table via `pgrc2_format::ident::schema_fingerprint`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartSpec {
    pub spc: u32,
    pub db: u32,
    pub relfilenumber: u64,
    pub schema_fingerprint: u64,
}

/// Resolves decode vtables for round-trip verification. M3-C's kernels join
/// through a composite resolver; the reference resolver serves the M3-D
/// election set (VERBATIM/CONST).
pub trait VerifyResolver {
    fn resolve(&self, key: KernelKey) -> WriteResult<&'static CodecVtable>;
}

/// Resolver over the frozen reference vtables.
#[derive(Debug, Default, Clone, Copy)]
pub struct ReferenceResolver;

impl VerifyResolver for ReferenceResolver {
    fn resolve(&self, key: KernelKey) -> WriteResult<&'static CodecVtable> {
        for vt in reference_vtables() {
            if vt.key == key {
                return Ok(vt);
            }
        }
        Err(WriteError::Format(
            pgrc2_format::FormatError::KernelMissing {
                encoding: key.encoding,
                class: key.class,
                width: key.width,
            },
        ))
    }
}

/// Resolver over the full codec-family registry (M3-C's kernels + the
/// reference vtables): the production resolver for full-registry seals —
/// verify resolves exactly the vtable a reader will.
#[derive(Debug, Default, Clone, Copy)]
pub struct CodecResolver;

impl VerifyResolver for CodecResolver {
    fn resolve(&self, key: KernelKey) -> WriteResult<&'static CodecVtable> {
        pgrc2_codec::registry()
            .resolve(key)
            .map_err(WriteError::Format)
    }
}

/// A sealed (unpublished) part: the facts publish needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedPart {
    /// File name within the table directory (`tmp-<fxid>-<seq>.pgrc2t`).
    pub tmp_name: String,
    pub rows: u64,
    pub file_len: u64,
    pub footer_off: u64,
    pub granule_count: u32,
    pub band_count: u32,
    /// The sealed 96-B footer image (part_no still 0). Publish patches
    /// part_no through this copy — no file read-back, O(streams-touched)
    /// preserved.
    pub footer_image: Vec<u8>,
    /// ST-1 (OD-2): the encoded Stats-sidecar payload (per-stream
    /// distribution sketches, `pgrc2_format::sidecar::encode_stats_payload`
    /// form). Empty = no stream computed one. Publish writes it as the
    /// part's `stats` sidecar companion — stats are a SEAL byproduct, so
    /// "banks ship stats-built" is structural, never a follow-up pass.
    pub stats_payload: Vec<u8>,
    /// [fmt-land] Bank-grain stats-plane slices, captured AS THE PART
    /// SEALS (Michael 2026-08-16: "we should generate it as we copy data
    /// in"): per top-level column (path_ord 0), the UNWRAPPED §8.1 Stats
    /// body + the 24-B PartDigest record byte-for-byte as sealed into the
    /// part. Publish folds them into the bank's `bankstats-<gen>.pgrc2bs`
    /// sidecar (`bankplane.rs`) — a derived artifact, byte-identical to an
    /// offline rebuild from the sealed parts by construction (same bodies,
    /// same attno order).
    pub plane_slices: Vec<PlaneSlice>,
}

/// [fmt-land] One (column, part) stats-plane slice — the exact bytes the
/// `pgrc2_format::bankstats` column payload carries for this part.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaneSlice {
    pub attno: u32,
    /// The UNWRAPPED §8.1 Stats section body (granule records ‖ band
    /// records ‖ part record) — captured BEFORE the CMP-F meta wrap.
    pub stats_body: Vec<u8>,
    /// The 24-B PartDigest record bytes; `None` when the digest section
    /// was not sealed (PGRUST_PGRC2_PARTDIGEST=0 arm).
    pub digest: Option<[u8; pgrc2_format::bankstats::BANKSTATS_DIGEST_LEN]>,
}

/// The seal witnesses (gates prove they RAN, not just that nothing fired).
#[derive(Debug, Default, Clone)]
pub struct SealReport {
    pub elections: Vec<ElectionWitness>,
    /// verify_roundtrip completions (== granules × value streams).
    pub granules_verified: u64,
    /// Two-witness nonnull cross-checks performed (granule+band+part grain).
    pub nonnull_crosschecks: u64,
    /// O-10 logical identity per stream: (attno, path_ord, (lo, hi, rows)).
    pub col_hashes: Vec<(u32, u32, (u64, u64, u64))>,
    /// FT-6 clustered witness (report leg): the declared cluster-key
    /// attnos this part was sorted-and-verified on (empty = undeclared,
    /// OD-8's legal-unlicensed posture). The on-disk leg is the spec-§9
    /// SortKey section with `nkeys > 0`.
    pub cluster_keys: Vec<u32>,
    /// SEAL-FUSION phase timers (measurement witnesses, never part bytes):
    /// the cluster-sort wall (IN-1; the EXT-SORT lane's domain) and the
    /// post-sort seal wall (election + encode + verify + meta + assembly —
    /// the fusion's own phase).
    pub sort_secs: f64,
    pub seal_secs: f64,
    /// FSST-UNLOCK: typed census notes minted by the elections (e.g. a
    /// Utf8Chars claim that failed per-part verification and ran
    /// BytesOnly). Measurement witnesses, never part bytes.
    pub notes: Vec<crate::elect::SealNote>,
}

pub fn band_count_u64(rows: u64) -> u64 {
    geom::band_count(rows) as u64
}

/// SEAL-SPEED-2 fold-fusion switch (default ON; `PGRUST_SEAL_FUSE_FOLD=0`
/// kills back to the classic encode-then-observe pair). A perf dial, never
/// a bytes dial: witness values are identical either way (the per-row fold
/// sequence is shared code — `observe_granule` delegates to the same
/// streaming face the fused encoders drive). Read once per process.
fn fuse_fold_enabled() -> bool {
    static ON: pgsync::OnceLock<bool> = pgsync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_SEAL_FUSE_FOLD").map(|v| v.trim() != "0").unwrap_or(true)
    })
}

/// Wrapper election for a finished BYTE-RUN section (overflow / dict index /
/// dict payload) — CMP-B's wrapper-offer CLASS widening: the value-stream
/// offer (CMP-A, O-CMP-3(a)) extends to the byte-run stream class under the
/// SAME laws. SB-1 (v4 DEFAULT, not an offer): the arm is zstd-3
/// block-grain — the election table of `9b6ff701ae` adopted verbatim (zstd
/// wins every offered family; LZ4 dominated on BOTH axes — worse ratio
/// everywhere AND slower unwrap — so it is no longer priced; the enum id
/// stays reader vocabulary). The wrapped image ships iff it clears the
/// wrapper-layer ≥20% stream-grain law (O-CMP-4(a)). Byte-run sections are
/// single-granule, so the wrapped form is one block; readers rebuild the
/// byte-exact region at first region fault and it stays part-resident
/// (O-CMP-5(a) — the StrView §7b dict-payload region-stability law is
/// preserved by residency, not by refusing the wrapper). Returns `None`
/// when the gate refuses (the section ships unwrapped, exactly as before).
fn wrap_byte_run_section(section: &[u8]) -> WriteResult<Option<(Wrapper, Vec<u8>)>> {
    let hdr = StreamSectionHdr::decode(section).map_err(WriteError::Format)?;
    let payload_end = if hdr.frame_table_off != 0 {
        hdr.frame_table_off as usize
    } else if hdr.gcount_table_off != 0 {
        hdr.gcount_table_off as usize
    } else {
        section.len()
    };
    let payload_len = payload_end
        .checked_sub(pgrc2_format::part::STREAM_SECTION_HDR_LEN)
        .ok_or(WriteError::Contract {
            detail: "byte-run section shorter than its header",
        })?;
    // SB-7 under the wrapper: a FRAMED byte-run section (DictPayload — the
    // only byte-run stream carrying a frame table) wraps with ONE zstd
    // block PER DICT FRAME, so the wrapped image's block-offset table is
    // frame-addressable — the partial-decompress geometry the M3-L3
    // frame-grain residency consumes (the whole-section unwrap + image
    // cache stays TODAY's realized answer per P-6; the CMP-E cold cells
    // price the difference). Unframed byte runs (overflow, DictIndex) stay
    // one block.
    let ends: Vec<u32> = match hdr.frame_table(section).map_err(WriteError::Format)? {
        Some(offs) if !offs.is_empty() => offs
            .iter()
            .skip(1)
            .copied()
            .chain(std::iter::once(payload_len as u32))
            .collect(),
        _ => vec![payload_len as u32],
    };
    let mut best: Option<(Wrapper, Vec<u8>)> = None;
    for arm in [Wrapper::Zstd] {
        if !pgrc2_codec::wrapper::wrapper_available(arm) {
            // A build without the codec never elects it (the wasm arm's
            // parity-honest degradation, mirrored from the value path).
            continue;
        }
        let mut out: Vec<u8> = Vec::new();
        pgrc2_codec::wrapper::wrap_section(section, &ends, arm, &mut out)
            .map_err(WriteError::Format)?;
        if !wins_twenty_pct(out.len() as u64, section.len() as u64) {
            continue;
        }
        let better = match &best {
            None => true,
            Some((_, b)) => out.len() < b.len(),
        };
        if better {
            best = Some((arm, out));
        }
    }
    Ok(best)
}

/// One stream's directory facts, accumulated during emission.
struct StreamPlan {
    attno: u32,
    path_ord: u32,
    role: StreamRole,
    encoding: u16,
    flags: u16,
    class_id: u8,
    width: u8,
    fixed_len: u32,
    aux32: u32,
    wrapper: u8,
    values: u64,
    extents: Vec<ExtentRecord>,
}

/// Seal one part from accumulated column buffers.
///
/// `roots` are the table's columns in attno order; `lanes` are derived
/// shred substreams (validated row-aligned); `builders` is parallel to
/// `roots ++ lanes` (the §19.7 faces this lane drives — M3-E's real
/// builders replace the stand-ins through the same trait). `structural`
/// carries the caller-declared structural-election facts (TY-1 ArrayDual;
/// see [`StructuralPolicy`]).
///
/// SB-10: this function ELECTS the part's granule grain first — the largest
/// [`geom::GRAIN_LADDER`] grain at which every stream's exact whole-part
/// value bytes average under [`geom::GRANULE_BYTE_BOUND_PROVISIONAL`] per
/// granule (oversize varlenas price at their 16-B inline stub; the ladder
/// floor is taken when even 1024 misses the bound). All granule/band
/// geometry below is closed-form in (rows, grain); the grain lands in the
/// footer.
#[allow(clippy::too_many_arguments)]
pub fn seal_part(
    vfs: &mut dyn WriteVfs,
    table_dir: &str,
    spec: &PartSpec,
    roots: &[ColBuffer],
    lanes: &[ShredLane],
    builders: &mut [Box<dyn ColumnMetaBuilder>],
    sources: &[&dyn CandidateSource],
    resolver: &dyn VerifyResolver,
    structural: &StructuralPolicy,
    fxid: u64,
    seq: u32,
) -> WriteResult<(SealedPart, SealReport)> {
    // ---- contracts ---------------------------------------------------------
    let Some(first) = roots.first() else {
        return Err(WriteError::Contract {
            detail: "seal with zero columns",
        });
    };
    let rows = first.rows();
    if rows == 0 {
        return Err(WriteError::Contract {
            detail: "seal of an empty part",
        });
    }
    for c in roots {
        if c.rows() != rows {
            return Err(WriteError::Contract {
                detail: "column row counts diverge",
            });
        }
    }
    for l in lanes {
        if l.col.rows() != rows {
            return Err(WriteError::Contract {
                detail: "shred lane not row-aligned",
            });
        }
    }

    let t_sort0 = std::time::Instant::now();

    // ---- IN-1/FT-6/OD-8: declared-cluster-key sorted ingest ----------------
    // Applied BEFORE stream assembly so every stream — roots AND derived
    // lanes — carries the same row order. One implementation for both
    // ingest paths (serial `cut_part` and the parallel `seal_one` both land
    // here), so the sort preserves the byte-identical-parts law by
    // construction. Already-ordered input takes the verify-only fast path
    // (identity permutation is skipped — the rebuilt state would be
    // bit-identical anyway). Order is a pure function of the part's rows
    // (STABLE sort: equal keys keep input order), so the sealed bytes are
    // a pure function of the input MULTISET + declared key — a shuffled
    // feed and a presorted feed seal byte-identical parts.
    let cluster_hold;
    let (roots, lanes): (&[ColBuffer], &[ShredLane]) = if structural.cluster_key().is_empty()
    {
        (roots, lanes)
    } else {
        match cluster_sort(roots, lanes, structural.cluster_key())? {
            None => (roots, lanes), // verified already ordered
            Some(sorted) => {
                cluster_hold = sorted;
                (&cluster_hold.0, &cluster_hold.1)
            }
        }
    };

    let t_seal0 = std::time::Instant::now();

    // Streams: roots (path_ord 0) then lanes (part-global path_ord 1..).
    // The fourth slot is the NumericFs lane scale (RULED 2026-08-14:
    // persisted in the Values entry's aux32 under STREAMF_LANE_SCALE;
    // None on roots and non-numeric lanes).
    let streams: Vec<(u32, u32, &ColBuffer, Option<i32>)> = roots
        .iter()
        .map(|c| (c.schema.attno, 0u32, c, None))
        .chain(
            lanes
                .iter()
                .enumerate()
                .map(|(i, l)| (l.parent_attno, i as u32 + 1, &l.col, l.scale)),
        )
        .collect();
    if builders.len() != streams.len() {
        return Err(WriteError::Contract {
            detail: "one meta builder per stream",
        });
    }

    // ---- SB-10 grain election (closed-form over exact chunk stats) ---------
    let grain = elect_part_grain(rows, &streams);
    // The closed forms are untruncated u64 (geom §, idx 253). Bound the
    // granule count in u64 first; a legitimate part is well within u32, so
    // only after the MAX_GRANULES_PER_PART guard do we narrow to the u32 the
    // footer/manifest fields (and the downstream loops) carry.
    let granule_count = geom::granule_count_at(rows, grain);
    let band_count = geom::band_count_at(rows, grain);
    if granule_count > MAX_GRANULES_PER_PART as u64 {
        return Err(WriteError::Contract {
            detail: "part exceeds the rowid granule budget (cut policy breach)",
        });
    }
    let granule_count = granule_count as u32;
    let band_count = band_count as u32;

    let mut buf: Vec<u8> = Vec::new();
    PartHeader::new(
        0, // part_no: assigned at publish (spec §5.1)
        spec.schema_fingerprint,
        spec.spc,
        spec.db,
        spec.relfilenumber,
    )
    .encode_into(&mut buf);

    let mut sections: Vec<SectionEntry> = Vec::new();
    let mut plans: Vec<StreamPlan> = Vec::new();
    let mut report = SealReport::default();
    // [fmt-land] stats-plane slices, collected as each column's stats seal.
    let mut plane_slices: Vec<PlaneSlice> = Vec::new();

    // Scratch reused across streams. The verify arena is u64-BACKED and
    // byte-viewed ([`words_as_bytes`]): ByteArena bases must be 8-aligned
    // (the abi.rs §19.4 base-alignment law, debug-asserted). The v3-verbatim
    // ordinary path used a bare `Vec<u8>` here and survived only by malloc
    // alignment accident — the structural arm below always carried the
    // sanctioned idiom; now both arms do (L2 A-lane hardening).
    let mut datum_scratch: Vec<u64> = vec![0; GRANULE_ROWS as usize];
    let mut arena_words: Vec<u64> = vec![0; 8];

    // ---- per-stream emission ----------------------------------------------
    for (si, &(attno, path_ord, col, lane_scale)) in streams.iter().enumerate() {
        let class = col.schema.class;
        let stats = col.stream_stats();
        let shape = ExtentShape {
            extent_count: band_count as u64,
            frame_count: rows.div_ceil(geom::FRAME_VALUES as u64),
        };

        // Materialize the whole stream's row-dense datum currency ONCE
        // (SEAL-FUSION): byval classes BORROW the staged words (zero copy);
        // byref classes materialize heap pointers in one pass. Granule
        // inputs at the elected grain — and, below, the election's
        // default-grain inputs — are SLICES of this one array: the M3-C
        // analyzers are exact whole-part pure functions (analyze-then-
        // elect, charter §1), and the seal reuses the same inputs for
        // encode + verify + meta observation.
        let flat = col.part_ptrs();
        let ginputs: Vec<pgrc2_format::abi::EncodeInput<'_>> = (0..granule_count)
            .map(|g| {
                let start = g as usize * grain.rows() as usize;
                let rows_g = geom::rows_in_granule_at(rows, grain, g);
                col.encode_input(g, grain, &flat[start..start + rows_g as usize], rows_g)
            })
            .collect();

        // ---- TY-1 structural arm: ArrayDual (ENC 10) ----------------------
        // A declared array column routes through the codec's whole-part
        // accept/refuse: acceptance on EVERY granule seals sizes+elements
        // dual substreams (per-part permanence); any refusal (null
        // elements, multidim, byref/text payloads, exotic shapes) falls
        // through to the ordinary election, which demotes to VERBATIM.
        if path_ord == 0 && class == StorageClass::VarlenaVerbatim {
            if let Some(facts) = structural.array_facts(attno) {
                if let Some(splits) = split_all_granules(&ginputs, facts)? {
                    let baseline = verbatim_baseline_len(&stats, &shape);
                    seal_array_dual(
                        &mut buf,
                        &mut sections,
                        &mut plans,
                        &mut report,
                        &mut builders[si],
                        resolver,
                        col,
                        &ginputs,
                        &splits,
                        facts,
                        attno,
                        rows,
                        grain,
                        granule_count,
                        band_count,
                        baseline,
                        &mut plane_slices,
                    )?;
                    continue;
                }
            }
        }

        // Election inputs: the FROZEN election analyzers and the dict arm
        // slice at the DEFAULT grain (their intra-granule closed forms are
        // capacity-keyed at 8192). The part grain is uniform, so both
        // slicings cover the identical row-dense value sequence — the
        // election is judged on the same values, and dict code arrays are
        // re-sliced to the part grain below. SEAL-FUSION: the default-grain
        // inputs are SLICES of the one materialized array above — the old
        // second whole-column materialization (#1b) is gone.
        let e_ginputs: Vec<pgrc2_format::abi::EncodeInput<'_>> = if grain.is_default() {
            Vec::new()
        } else {
            let dgc = geom::granule_count(rows);
            (0..dgc)
                .map(|g| {
                    let start = g as usize * GranuleGrain::DEFAULT.rows() as usize;
                    let rows_g = geom::rows_in_granule(rows, g);
                    col.encode_input(
                        g,
                        GranuleGrain::DEFAULT,
                        &flat[start..start + rows_g as usize],
                        rows_g,
                    )
                })
                .collect()
        };
        let elect_granules: &[pgrc2_format::abi::EncodeInput<'_>] = if grain.is_default() {
            &ginputs
        } else {
            &e_ginputs
        };

        // ---- election: full-registry arm first (source order), stats-only
        // fallback (A-lane amendment M3-A2) ---------------------------------
        let fin = FullElectInput {
            attno,
            path_ord,
            stats: &stats,
            shape: &shape,
            granules: elect_granules,
            col: Some(col),
        };
        let mut full: Option<FullElection> = None;
        for src in sources {
            if let Some(r) = src.elect_full(&fin) {
                full = Some(r?);
                break;
            }
        }
        let mut full = match full {
            Some(f) => f,
            None => {
                let (elected, witness) = elect_stream(&fin, sources);
                match elected {
                    Elected::Verbatim => FullElection {
                        encoding: EncodingId::Verbatim.as_u16(),
                        width: class.width(),
                        aux32: 0,
                        extra_flags: 0,
                        offer_wrapper: false,
                        plan: ElectPlan::Verbatim,
                        witness,
                        note: None,
                    },
                    Elected::Candidate(f) => FullElection {
                        encoding: f.key().encoding,
                        width: f.key().width,
                        aux32: 0,
                        extra_flags: 0,
                        offer_wrapper: false,
                        plan: ElectPlan::Encoder(f),
                        witness,
                        note: None,
                    },
                }
            }
        };
        let encoding = full.encoding;
        let width = full.width;
        let mut flags: u16 = full.extra_flags;
        if class.signed() {
            flags |= STREAMF_SIGNED;
        }
        // The NumericFs lane-scale slot (JSON routing, RULED 2026-08-14):
        // a shredded numeric lane's chunk-shared decimal scale lands in
        // the Values entry's aux32 under STREAMF_LANE_SCALE (the
        // ArrayDual-aux32 precedent) — readers adjudicate scale soundness
        // from the part alone; flag-absent (pre-ruling) numeric lanes
        // stay reader-refused.
        if let Some(scale) = lane_scale {
            full.aux32 = scale as u32;
            flags |= pgrc2_format::part::STREAMF_LANE_SCALE;
        }
        // Resolve the verify vtable through the ONE normalization (spec
        // §6.3/§19.5): the entry width byte is per-encoding vocabulary
        // (DICT_CODES: max code width — a stats fact), never the dispatch
        // axis. The verify path resolves exactly as a reader will.
        let key = pgrc2_format::enc::stream_kernel_key(encoding, class.id(), width)
            .map_err(WriteError::Format)?;
        let vt = resolver.resolve(key)?;

        // Validity bytes for the WHOLE stream, built up front: the verify
        // ctx and the two-witness check both read the emitted bytes (the
        // per-granule popcounts ride the build — SEAL-FUSION #15/#17).
        let (val_bytes, val_pops) = build_validity_bytes(col, granule_count, grain);

        // DICT-DEDUP: the dict election already maintained this part's
        // exact counted distinct set (the byte-rank dict build). Hand it to
        // the meta builder BEFORE the folds below, so `distribution()`
        // serves from it and the builder's own accumulator never runs on
        // this stream — the distinct set is maintained ONCE, two consumers.
        // Non-dict streams, demoted arms, the count-free D2 inherit path,
        // and the `PGRUST_PGRC2_DICT_DIST_FEED=0` control arm take the
        // `None` branch and keep the accumulator (#971) path; sidecar
        // bytes are identical either way (pure functions of the data — the
        // sketch equivalence battery + the rig dirshas pin it).
        if let ElectPlan::Dict(d) = &mut full.plan {
            if let Some(feed) = d.dist_feed.take() {
                builders[si].set_distribution_feed(feed);
            }
        }
        let dict_plan = match &full.plan {
            ElectPlan::Dict(d) => Some(d),
            _ => None,
        };
        // Dict codes were built by the FROZEN election arm at the DEFAULT
        // grain; at a smaller part grain, re-slice the row-dense whole-part
        // code stream at the elected grain (default-grain granule
        // concatenation IS row-dense part order; null slots carry 0 either
        // way).
        let codes_regrain: Option<Vec<Vec<u64>>> = match (dict_plan, grain.is_default()) {
            (Some(d), false) => {
                let flat: Vec<u64> = d.codes.iter().flat_map(|v| v.iter().copied()).collect();
                let mut out = Vec::with_capacity(granule_count as usize);
                for g in 0..granule_count {
                    let start = g as usize * grain.rows() as usize;
                    let rows_g = geom::rows_in_granule_at(rows, grain, g) as usize;
                    out.push(flat[start..start + rows_g].to_vec());
                }
                Some(out)
            }
            _ => None,
        };
        // Value-stream encode currency: global codes for dict streams
        // (their datum currency IS the code, spec §7), datum pointers/words
        // otherwise.
        let enc_inputs: Vec<pgrc2_format::abi::EncodeInput<'_>> = (0..granule_count)
            .map(|g| {
                let rows_g = geom::rows_in_granule_at(rows, grain, g);
                let datums: &[u64] = match (&codes_regrain, dict_plan) {
                    (Some(c), _) => &c[g as usize],
                    (None, Some(d)) => &d.codes[g as usize],
                    (None, None) => {
                        let start = g as usize * grain.rows() as usize;
                        &flat[start..start + rows_g as usize]
                    }
                };
                pgrc2_format::abi::EncodeInput {
                    class,
                    rows: rows_g,
                    datums,
                    validity: col.granule_validity(g, grain),
                }
            })
            .collect();

        // ---- band emission into scratch sections; ONE overflow region per
        // stream (the v1 single-extent law, spec §6.8) ----------------------
        let mut ovf_buf: Vec<u8> = Vec::new();
        let mut band_secs: Vec<(Vec<u8>, StreamCloseout, Vec<u32>)> = Vec::new();
        let ovf_entries: u64;
        {
            let mut ovf = OverflowSink::new(&mut ovf_buf);
            for b in 0..band_count {
                let g0 = b * GRANULES_PER_BAND;
                let g1 = (g0 + GRANULES_PER_BAND).min(granule_count);
                let mut sec: Vec<u8> = Vec::new();
                let mut w = StreamSectionWriter::begin(&mut sec, encoding, width, Wrapper::None)?;
                let mut enc: Box<dyn GranuleEncoder> = match &full.plan {
                    ElectPlan::Verbatim => Box::new(VerbatimEncoder { class }),
                    // SEAL-FUSION: the POSITIONED face — carry-aware
                    // factories hand the encoder its cursor into the
                    // election's carried facts (per-frame refs/widths, ALP
                    // frames, FSST buffers); carry-free factories fall
                    // through to `make`.
                    ElectPlan::Encoder(f) => f.make_at(g0, grain.rows()),
                    ElectPlan::Dict(d) => Box::new(pgrc2_codec::dictcodes::DictCodesEncoder {
                        class: class.id(),
                        max_width: d.max_width,
                        byte_align: d.byte_align,
                    }),
                };
                let mut ends: Vec<u32> = Vec::with_capacity((g1 - g0) as usize);
                for g in g0..g1 {
                    // SEAL-SPEED-2 fold fusion (ruled 2026-08-12): the meta
                    // folds RIDE the encode walk for value-currency streams
                    // — one iteration produces encoded bytes AND feeds the
                    // builder (fused overrides in verbatim/bytefor/deltafor;
                    // families whose emit is a carried memcpy fall through
                    // to the classic pair inside the default face). Dict
                    // streams observe the VALUE currency — via the D-STATS
                    // fold-from-codes face when the builder offers it (the
                    // codes ARE the values through the byte-rank order
                    // certificate + the DICT-DEDUP feed's entry table;
                    // seal-fusion charter §4 cut 3), via the classic
                    // hydrated-value pair otherwise (D2 inherit, feed-off
                    // control, `PGRUST_SEAL_BATCH_FOLD=0`, stand-ins).
                    // Witness bytes are switch-invisible on every arm (the
                    // fold facts are pure functions of the data);
                    // `PGRUST_SEAL_FUSE_FOLD=0` kills back to the classic
                    // pair.
                    if dict_plan.is_none() && fuse_fold_enabled() {
                        enc.encode_granule_observed(
                            &enc_inputs[g as usize],
                            &mut w,
                            &mut ovf,
                            builders[si].as_mut(),
                            g,
                        )?;
                    } else {
                        enc.encode_granule(&enc_inputs[g as usize], &mut w, &mut ovf)?;
                        if dict_plan.is_some() && builders[si].dict_code_observe_supported() {
                            // enc_inputs for a dict stream IS the codes
                            // currency (rows + validity + global codes).
                            builders[si].observe_granule_dict_codes(&enc_inputs[g as usize], g);
                        } else {
                            builders[si].observe_granule(&ginputs[g as usize], g);
                        }
                    }
                    ends.push(w.payload_off());
                }
                enc.finish_stream(&mut w)?;
                if let Some(last) = ends.last_mut() {
                    // CONST closes out in finish_stream; fold trailing bytes
                    // into the final granule's block for wrapper purposes.
                    *last = w.payload_off();
                }
                // SB-10: at a NON-DEFAULT grain, multi-frame-per-granule
                // families get the gcount table on ROOT sections too — the
                // frozen kernels' `value_base`/`frame_base` closed forms are
                // capacity-keyed at 8192, while their gcount arm is
                // grain-exact. Granule-framed families (CONST/ALP/ALP_RD/
                // BOOL_BITMAP/FSST — FSST granule-framed since the L1
                // wave-6 SB-10 x SB-4 fix) address by granule ordinal —
                // grain-proof without (and `granule_frame_base` refuses) a
                // table. At the default grain nothing changes: v3 bytes
                // exactly.
                let emit_gcounts = !grain.is_default() && root_gcounts_required(encoding);
                let closeout = w.finish(emit_gcounts)?;
                band_secs.push((sec, closeout, ends));
            }
            ovf_entries = ovf.entries();
        }

        // ---- MANDATORY round-trip verify, per granule, over the UNWRAPPED
        // sections (the exact image kernels see post-unwrap). Dict streams
        // verify FULL MATERIALIZATION against the original varlena inputs
        // (ctx carries the emitted dict sections) plus a decode_codes
        // cross-witness against the elected code arrays. -------------------
        let dict_payloads = match dict_plan {
            Some(d) => Some((
                crate::dict::section_payload(&d.images.index_section)?,
                crate::dict::section_payload(&d.images.payload_section)?,
                d.entry_count,
                d.images.charlen_form,
            )),
            None => None,
        };
        for (bi, (sec, _closeout, _ends)) in band_secs.iter().enumerate() {
            let b = bi as u32;
            let g0 = b * GRANULES_PER_BAND;
            let g1 = (g0 + GRANULES_PER_BAND).min(granule_count);
            let hdr = StreamSectionHdr::decode(sec)?;
            let ft = hdr.frame_table(sec)?;
            for g in g0..g1 {
                let rows_g = geom::rows_in_granule_at(rows, grain, g);
                let input = &ginputs[g as usize];
                let bound = (col.granule_arena_bound(g, grain) + 64) as usize;
                if arena_words.len() * 8 < bound {
                    arena_words.resize(bound.div_ceil(8), 0);
                }
                let arena_buf: &mut [u8] = words_as_bytes(&mut arena_words);
                let ctx = KernelCtx {
                    key,
                    flags,
                    fixed_len: class.fixed_len(),
                    bytes: sec,
                    frame_table: ft.as_deref(),
                    granule: g,
                    granule_in_extent: g - g0,
                    rows: rows_g,
                    values: rows_g,
                    validity_bytes: validity_slice(col, &val_bytes, g, rows_g, grain),
                    overflow: if ovf_buf.is_empty() {
                        None
                    } else {
                        Some(&ovf_buf)
                    },
                    dict: dict_payloads.map(|(index, payload, entry_count, charlen_form)| {
                        DictSections {
                            index,
                            payload,
                            entry_count,
                            charlen_form,
                        }
                    }),
                };
                verify_roundtrip(vt, &ctx, input, &mut datum_scratch, arena_buf).map_err(
                    |cause| WriteError::RoundTrip {
                        attno,
                        path_ord,
                        granule: g,
                        cause,
                    },
                )?;
                // SEAL-FUSION (walks #13/#14 dropped): the dict
                // `decode_codes` cross-witness re-decoded every granule and
                // compared decoded codes against the elected code arrays.
                // The full-materialization verify above already proves more:
                // through the reader's own kernel it resolves every STORED
                // code to its value and compares canonical bytes against the
                // original varlena inputs — and dict entries are strictly
                // distinct (byte-rank sorted, `verify_sections`-proven), so
                // value identity IMPLIES code identity. The reader-kernel
                // decode_full+compare leg is untouched — that structure is
                // the bank-integrity tooth.
                report.granules_verified += 1;
            }
        }

        // ---- wrapper decision (stream grain, exact bytes, the wrapper-
        // layer ≥20% law — O-CMP-4(a); the v4 DEFAULT on every full
        // election — SB-1: zstd-3 block-grain per data family, the
        // election table of 9b6ff701ae adopted verbatim; LZ4 dominated on
        // both axes and is no longer priced. Disk-only semantics: readers
        // rebuild the ENCODED image at extent open, kernels never see a
        // wrapper (O-CMP-5(a) unwrapped-image residency).
        let mut wrapped_secs: Option<(Wrapper, Vec<Vec<u8>>)> = None;
        if full.offer_wrapper {
            let unwrapped_total: usize = band_secs.iter().map(|(sec, _, _)| sec.len()).sum();
            let mut best: Option<(Wrapper, Vec<Vec<u8>>, usize)> = None;
            for arm in [Wrapper::Zstd] {
                if !pgrc2_codec::wrapper::wrapper_available(arm) {
                    // A build without the codec never elects it (the wasm
                    // arm's parity-honest degradation).
                    continue;
                }
                let mut images: Vec<Vec<u8>> = Vec::with_capacity(band_secs.len());
                let mut wrapped_total = 0usize;
                for (sec, _c, ends) in &band_secs {
                    let mut out: Vec<u8> = Vec::new();
                    pgrc2_codec::wrapper::wrap_section(sec, ends, arm, &mut out)
                        .map_err(WriteError::Format)?;
                    wrapped_total += out.len();
                    images.push(out);
                }
                if !wins_twenty_pct(wrapped_total as u64, unwrapped_total as u64) {
                    continue;
                }
                let better = match &best {
                    None => true,
                    Some((_, _, t)) => wrapped_total < *t,
                };
                if better {
                    best = Some((arm, images, wrapped_total));
                }
            }
            if let Some((arm, images, _)) = best {
                wrapped_secs = Some((arm, images));
            }
        }
        let wrapper_byte: u8 = match &wrapped_secs {
            Some((arm, _)) => arm.as_u8(),
            None => Wrapper::None.as_u8(),
        };

        // ---- append value extents -----------------------------------------
        let mut value_extents: Vec<ExtentRecord> = Vec::new();
        for (bi, (sec, closeout, _ends)) in band_secs.iter().enumerate() {
            let b = bi as u32;
            let g0 = b * GRANULES_PER_BAND;
            let g1 = (g0 + GRANULES_PER_BAND).min(granule_count);
            // The wrapper CRC law: CRC over the WRAPPED bytes — exactly the
            // bytes the extent record addresses on disk.
            let (bytes, crc): (&[u8], u32) = match &wrapped_secs {
                Some((_, images)) => (&images[bi], pgrc2_format::wire::crc32c(&images[bi])),
                None => (sec.as_slice(), closeout.crc),
            };
            pad_to(&mut buf, 8);
            let sec_start = buf.len();
            buf.extend_from_slice(bytes);
            value_extents.push(ExtentRecord {
                file_off: sec_start as u64,
                len: bytes.len() as u64,
                values: closeout.values,
                granule_start: g0,
                granule_count: g1 - g0,
                crc,
                flags: 0,
            });
            sections.push(SectionEntry {
                off: sec_start as u64,
                len: bytes.len() as u64,
                kind: SectionKind::Stream.as_u16(),
                flags: 0,
                attno,
                path_ord,
                crc,
            });
        }

        // ---- the ONE overflow extent (spec §6.8 v1 single-extent law).
        // Built in scratch so the byte-run wrapper offer (CMP-B class
        // widening) can price the finished section; gated on the same
        // `offer_wrapper` the value stream carries (the stats-only fallback
        // never wraps — reference-grade bindings keep reading its parts). --
        let mut overflow_extents: Vec<ExtentRecord> = Vec::new();
        let mut overflow_entries_total: u64 = 0;
        let mut overflow_wrapper: u8 = Wrapper::None.as_u8();
        if !ovf_buf.is_empty() {
            // SEAL-FUSION (#22): the count was accumulated at append time;
            // the header re-walk survives only as a debug self-check.
            let entries = ovf_entries;
            debug_assert_eq!(
                entries,
                count_overflow_entries(&ovf_buf)?,
                "overflow sink count drifted from the region census"
            );
            overflow_entries_total = entries;
            let mut osec: Vec<u8> = Vec::new();
            let closeout = {
                let mut w = StreamSectionWriter::begin(
                    &mut osec,
                    EncodingId::Verbatim.as_u16(),
                    0,
                    Wrapper::None,
                )?;
                w.payload().extend_from_slice(&ovf_buf);
                w.end_granule(entries as u32);
                w.finish(false)?
            };
            let wrapped = if full.offer_wrapper {
                wrap_byte_run_section(&osec)?
            } else {
                None
            };
            // The wrapper CRC law: CRC over the bytes the extent record
            // addresses on disk.
            let (bytes, crc): (&[u8], u32) = match &wrapped {
                Some((arm, image)) => {
                    overflow_wrapper = arm.as_u8();
                    (image.as_slice(), pgrc2_format::wire::crc32c(image))
                }
                None => (osec.as_slice(), closeout.crc),
            };
            pad_to(&mut buf, 8);
            let osec_start = buf.len();
            buf.extend_from_slice(bytes);
            overflow_extents.push(ExtentRecord {
                file_off: osec_start as u64,
                len: bytes.len() as u64,
                values: entries,
                granule_start: 0,
                granule_count,
                crc,
                flags: 0,
            });
            sections.push(SectionEntry {
                off: osec_start as u64,
                len: bytes.len() as u64,
                kind: SectionKind::Stream.as_u16(),
                flags: 0,
                attno,
                path_ord,
                crc,
            });
            flags |= STREAMF_HAS_OVERFLOW;
        }

        // ---- dict sections (single extents each, spec §7; section headers
        // stamp DICT_CODES — the family id; layout is role-driven) ----------
        let mut dict_stream_plans: Vec<StreamPlan> = Vec::new();
        if let Some(d) = dict_plan {
            let pairs: [(StreamRole, &Vec<u8>, &pgrc2_format::part::StreamCloseout); 2] = [
                (
                    StreamRole::DictIndex,
                    &d.images.index_section,
                    &d.images.index_closeout,
                ),
                (
                    StreamRole::DictPayload,
                    &d.images.payload_section,
                    &d.images.payload_closeout,
                ),
            ];
            for (role, image, closeout) in pairs {
                // CMP-B wrapper-offer class widening: the byte-run dict
                // sections take the same per-stream offer as the value
                // stream (dict PAYLOAD is the bank's single largest byte
                // sink — §9.2 of the compression doc). Verify ran over the
                // UNWRAPPED images above (`verify_sections`); wrapping is
                // strictly downstream of verify, like the value path.
                let wrapped = if full.offer_wrapper {
                    wrap_byte_run_section(image)?
                } else {
                    None
                };
                let (dict_wrapper, bytes, crc): (u8, &[u8], u32) = match &wrapped {
                    Some((arm, w_image)) => (
                        arm.as_u8(),
                        w_image.as_slice(),
                        pgrc2_format::wire::crc32c(w_image),
                    ),
                    None => (Wrapper::None.as_u8(), image.as_slice(), closeout.crc),
                };
                pad_to(&mut buf, 8);
                let s = buf.len();
                buf.extend_from_slice(bytes);
                sections.push(SectionEntry {
                    off: s as u64,
                    len: bytes.len() as u64,
                    kind: SectionKind::Stream.as_u16(),
                    flags: 0,
                    attno,
                    path_ord,
                    crc,
                });
                // SB-7 (frame-boundary dict extents): the UNWRAPPED
                // DictPayload stream's extent table is cut at dict-frame
                // boundaries — extent i covers frame i's byte run (extent 0
                // additionally carries the section header; the last extent
                // carries the frame-table tail), each with its own CRC, so
                // `ensure_frame` has a REAL per-frame fault-and-validate
                // grain from birth (PC-3.2: a claimed span faults frames,
                // never whole payloads). A WRAPPED payload stays
                // single-extent by design: O-CMP-5(a) unwrap is
                // whole-section + part-resident image cache (P-6), so the
                // frame grain there is the unwrapped image's, not the
                // disk's. DictIndex stays single-extent (12-B closed-form
                // stride; it is the small always-resident half).
                let extents: Vec<ExtentRecord> = if role == StreamRole::DictPayload
                    && wrapped.is_none()
                {
                    dict_frame_extents(image, s as u64, d.entry_count)?
                } else {
                    vec![ExtentRecord {
                        file_off: s as u64,
                        len: bytes.len() as u64,
                        values: d.entry_count as u64,
                        granule_start: 0,
                        granule_count,
                        crc,
                        flags: 0,
                    }]
                };
                dict_stream_plans.push(StreamPlan {
                    attno,
                    path_ord,
                    role,
                    // M5d char-len record: the DictIndex stream self-
                    // describes its `char_field` form (spec §6.3) — readers
                    // consult the flags, never a writer posture. Absolute
                    // (the disarmed default) stamps 0, byte-for-byte the
                    // pre-M5d dir entry.
                    flags: if role == StreamRole::DictIndex {
                        d.images.charlen_form.flag_bits()
                    } else {
                        0
                    },
                    encoding: EncodingId::DictCodes.as_u16(),
                    class_id: class.id(),
                    width: 0,
                    fixed_len: 0,
                    aux32: 0,
                    wrapper: dict_wrapper,
                    values: d.entry_count as u64,
                    extents,
                });
            }
        }

        // Validity stream: present iff ≥1 NULL in this part (spec §6.1).
        let validity_extent = emit_validity_stream(
            &mut buf,
            &mut sections,
            col,
            &val_bytes,
            attno,
            path_ord,
            rows,
            grain,
            granule_count,
        )?;

        // ---- two-witness null law + stats section (spec §6.6/§8.1) --------
        emit_stats_sections(
            &mut buf,
            &mut sections,
            &mut report,
            &mut builders[si],
            col,
            &val_pops,
            attno,
            path_ord,
            rows,
            grain,
            granule_count,
            band_count,
            // pgrc2.1 §2.2: the dict entry count IS the exact per-part NDV
            // when this stream elected DICT_CODES.
            dict_plan.map(|d| d.entry_count as u64),
            &mut plane_slices,
        )?;

        // ---- stream plans (directory rows) ---------------------------------
        plans.push(StreamPlan {
            attno,
            path_ord,
            role: StreamRole::Values,
            encoding,
            flags,
            class_id: class.id(),
            width,
            fixed_len: class.fixed_len(),
            aux32: full.aux32,
            wrapper: wrapper_byte,
            values: rows,
            extents: value_extents,
        });
        if let Some(ve) = validity_extent {
            plans.push(StreamPlan {
                attno,
                path_ord,
                role: StreamRole::Validity,
                encoding: EncodingId::BoolBitmap.as_u16(),
                flags: 0,
                class_id: class.id(),
                width: 0,
                fixed_len: 0,
                aux32: 0,
                wrapper: 0,
                values: rows,
                extents: vec![ve],
            });
        }
        if !overflow_extents.is_empty() {
            plans.push(StreamPlan {
                attno,
                path_ord,
                role: StreamRole::Overflow,
                encoding: EncodingId::Verbatim.as_u16(),
                flags: 0,
                class_id: class.id(),
                width: 0,
                fixed_len: 0,
                aux32: 0,
                wrapper: overflow_wrapper,
                values: overflow_entries_total,
                extents: overflow_extents,
            });
        }
        plans.append(&mut dict_stream_plans);

        // TY-3 (OD-4 two-arm election): a jsonb root that DERIVED typed
        // shred lanes this part records the STRUCTURAL JsonbShred election
        // as its witness — honestly: the lanes were actually emitted (they
        // ride this very stream list at path_ord ≥ 1), the image lane
        // remains the read truth (dual-store law, O-4), and the on-disk
        // parent StreamEntry keeps its text-plane encoding. baseline/chosen
        // stay the image-lane election's exact bytes.
        let mut witness = full.witness;
        if path_ord == 0 && lanes.iter().any(|l| l.parent_attno == attno) {
            witness.encoding = EncodingId::JsonbShred.as_u16();
        }
        report.elections.push(witness);
        if let Some(n) = full.note {
            report.notes.push(n);
        }
        report
            .col_hashes
            .push((attno, path_ord, col.logical_hash().digest()));
    }

    // ---- PathTable (spec §6.5; shredded parts only) ------------------------
    if !lanes.is_empty() {
        let paths: Vec<&str> = lanes.iter().map(|l| l.path.as_str()).collect();
        let body = encode_path_table(&paths)?;
        push_raw_section(&mut buf, &mut sections, SectionKind::PathTable, 0, 0, &body);
    }

    // ---- SortKey (spec §9): with a declared cluster key the v4 writer IS
    // a verified producer of the ordering (IN-1): it sorted — or verified —
    // this very part above, so the section attests honestly (FT-6's
    // per-part clustered witness: key + sorted-within-part, both legs the
    // writer's own; an attested-but-unverified key would poison sort
    // elision, which is why nkeys stayed 0 until this producer existed).
    // Undeclared tables stay nkeys = 0 (OD-8: legal, unlicensed).
    let sort_body = if structural.cluster_key().is_empty() {
        SortKeyRecord::default().encode()
    } else {
        let keys = structural
            .cluster_key()
            .iter()
            .map(|k| {
                let col = roots
                    .iter()
                    .find(|c| c.schema.attno == k.attno)
                    .expect("cluster_sort validated the key attnos");
                SortKeyEntry {
                    attno: k.attno,
                    dir: k.dir as u8,
                    nulls: k.nulls as u8,
                    collation_class: col.schema.collation_class.as_u8(),
                    pad: 0,
                }
            })
            .collect();
        report.cluster_keys = structural.cluster_key().iter().map(|k| k.attno).collect();
        SortKeyRecord { keys }.encode()
    };
    push_raw_section(
        &mut buf,
        &mut sections,
        SectionKind::SortKey,
        0,
        0,
        &sort_body,
    );

    // ---- StreamDir (spec §6.2/§6.3) ---------------------------------------
    plans.sort_by_key(|p| (p.attno, p.path_ord, p.role.as_u8()));
    let dir_body = encode_stream_dir(&plans);
    let stream_count = plans.len() as u32;
    push_raw_section(
        &mut buf,
        &mut sections,
        SectionKind::StreamDir,
        0,
        0,
        &dir_body,
    );

    // ---- section table + footer + tail (spec §5.2–§5.4) --------------------
    pad_to(&mut buf, 8);
    let section_table_off = buf.len() as u64;
    let mut table_bytes: Vec<u8> = Vec::with_capacity(sections.len() * 32);
    for s in &sections {
        s.encode_into(&mut table_bytes);
    }
    let section_table_crc = pgrc2_format::wire::crc32c(&table_bytes);
    buf.extend_from_slice(&table_bytes);

    pad_to(&mut buf, 8);
    let footer_off = buf.len() as u64;
    FooterFixed {
        magic: FOOTER_MAGIC,
        format_version: pgrc2_format::FORMAT_VERSION,
        rows,
        granule_count,
        band_count,
        section_count: sections.len() as u32,
        flags: 0,
        section_table_off,
        section_table_crc,
        part_no: 0, // patched at publish
        schema_fingerprint: spec.schema_fingerprint,
        stream_count,
        granule_rows: grain.rows(), // SB-10: the part's elected grain
        reserved: [0; 28],
        footer_crc: 0, // computed by encode_into
    }
    .encode_into(&mut buf);
    let footer_image =
        buf[footer_off as usize..footer_off as usize + pgrc2_format::part::FOOTER_FIXED_LEN]
            .to_vec();
    PartTail::new(footer_off).encode_into(&mut buf);

    // ---- ST-1 (OD-2): collect the per-stream distribution sketches ---------
    // (after every stream's seal_part ran in the emission loop above). The
    // payload is DETERMINISTIC — sketches iterate canonical-byte order and
    // streams ride the (attno, path_ord) order of the stream list — so
    // sealed parts stay a pure function of their inputs, sidecar included.
    let mut dist_cols: Vec<(u32, u32, pgrc2_format::sidecar::ColDistribution)> = Vec::new();
    for (si, &(attno, path_ord, _, _)) in streams.iter().enumerate() {
        if let Some(d) = builders[si].distribution() {
            dist_cols.push((attno, path_ord, d));
        }
    }
    let stats_payload = if dist_cols.is_empty() {
        Vec::new()
    } else {
        pgrc2_format::sidecar::encode_stats_payload(&dist_cols)
    };

    // ---- write the tmp file, header first (spec §5) ------------------------
    let tmp_name = pgrc2_format::dirlayout::temp_file_name(fxid, seq);
    let path = format!("{table_dir}/{tmp_name}");
    let fd = vfs.create_rw(&path)?;
    vfs.pwrite_at(&fd, 0, &buf[..pgrc2_format::part::PART_HEADER_LEN])?;
    vfs.pwrite_at(
        &fd,
        pgrc2_format::part::PART_HEADER_LEN as u64,
        &buf[pgrc2_format::part::PART_HEADER_LEN..],
    )?;
    vfs.close_file(fd)?;

    report.sort_secs = (t_seal0 - t_sort0).as_secs_f64();
    report.seal_secs = t_seal0.elapsed().as_secs_f64();

    Ok((
        SealedPart {
            tmp_name,
            rows,
            file_len: buf.len() as u64,
            footer_off,
            granule_count,
            band_count,
            footer_image,
            stats_payload,
            plane_slices: {
                // Canonical attno order (the offline builder walks the
                // schema in attno order; emission order already is, the
                // sort is the invariant's belt).
                let mut ps = plane_slices;
                ps.sort_by_key(|s| s.attno);
                ps
            },
        },
        report,
    ))
}

/// Append one META-PLANE section (Stats / Psma / Bloom / NdvRegisters —
/// the CMP-F wrapped meta-section class, SB-6): the body ships in the
/// zstd meta envelope iff the wrap clears the SB-2 ≥20% gate at this
/// section's own grain; `SECTIONF_META_ZSTD` marks the stored form and the
/// CRC stays over the STORED bytes (the wrapper CRC law). The structural
/// plane (PathTable / SortKey / StreamDir / section table / footer) stays
/// raw — CMP-F's claw is the census's measured meta-section class
/// (~2.1GB at 100m), not the struct-arithmetic residual; the two byte
/// classes are deliberately not conflated. A build without zstd ships raw
/// (the wasm parity-honest degradation, exactly the value path's).
fn push_meta_section(
    buf: &mut Vec<u8>,
    sections: &mut Vec<SectionEntry>,
    kind: SectionKind,
    attno: u32,
    path_ord: u32,
    body: &[u8],
) {
    if let Some(env) = pgrc2_codec::wrapper::meta_wrap_body(body) {
        if wins_twenty_pct(env.len() as u64, body.len() as u64) {
            pad_to(buf, 8);
            let off = buf.len() as u64;
            buf.extend_from_slice(&env);
            sections.push(SectionEntry {
                off,
                len: env.len() as u64,
                kind: kind.as_u16(),
                flags: SECTIONF_META_ZSTD,
                attno,
                path_ord,
                crc: pgrc2_format::wire::crc32c(&env),
            });
            return;
        }
    }
    push_raw_section(buf, sections, kind, attno, path_ord, body)
}

/// Append one raw OPTIONAL section (pgrc2.1 new kinds — FlatStats/…): the
/// `SECTION_OPTIONAL` flag makes pre-2.1 readers skip it typed instead of
/// refusing the part (spec §5.2 forward-compat law); the body stays raw so
/// mmap-cast consumers see the arrays directly (never `SECTIONF_META_ZSTD`).
fn push_optional_raw_section(
    buf: &mut Vec<u8>,
    sections: &mut Vec<SectionEntry>,
    kind: SectionKind,
    attno: u32,
    path_ord: u32,
    body: &[u8],
) {
    pad_to(buf, 8);
    let off = buf.len() as u64;
    buf.extend_from_slice(body);
    sections.push(SectionEntry {
        off,
        len: body.len() as u64,
        kind: kind.as_u16(),
        flags: pgrc2_format::part::SECTION_OPTIONAL,
        attno,
        path_ord,
        crc: pgrc2_format::wire::crc32c(body),
    });
}

/// Append one raw-body section (Stats/PathTable/SortKey/StreamDir…): 8-align,
/// write body, record the SectionEntry with its crc.
fn push_raw_section(
    buf: &mut Vec<u8>,
    sections: &mut Vec<SectionEntry>,
    kind: SectionKind,
    attno: u32,
    path_ord: u32,
    body: &[u8],
) {
    pad_to(buf, 8);
    let off = buf.len() as u64;
    buf.extend_from_slice(body);
    sections.push(SectionEntry {
        off,
        len: body.len() as u64,
        kind: kind.as_u16(),
        flags: 0,
        attno,
        path_ord,
        crc: pgrc2_format::wire::crc32c(body),
    });
}

/// StreamDir body: StreamEntry × n, then the extent tables each entry's
/// `extent_table_off` points into (section-relative offsets, spec §6.2).
fn encode_stream_dir(plans: &[StreamPlan]) -> Vec<u8> {
    let entries_len = plans.len() * pgrc2_format::part::STREAM_ENTRY_LEN;
    let mut tables: Vec<u8> = Vec::new();
    let mut entry_offs: Vec<u64> = Vec::with_capacity(plans.len());
    for p in plans {
        entry_offs.push((entries_len + tables.len()) as u64);
        for e in &p.extents {
            e.encode_into(&mut tables);
        }
    }
    let mut out = Vec::with_capacity(entries_len + tables.len());
    for (i, p) in plans.iter().enumerate() {
        StreamEntry {
            extent_table_off: entry_offs[i],
            values: p.values,
            attno: p.attno,
            path_ord: p.path_ord,
            fixed_len: p.fixed_len,
            aux32: p.aux32,
            extent_count: p.extents.len() as u32,
            encoding: p.encoding,
            flags: p.flags,
            role: p.role.as_u8(),
            class: p.class_id,
            width: p.width,
            wrapper: p.wrapper,
            reserved: 0,
        }
        .encode_into(&mut out);
    }
    out.extend_from_slice(&tables);
    out
}

/// SB-10 grain election (OD-13 RULED "let's byte bound granules"): the
/// LARGEST ladder grain at which EVERY stream's exact whole-part value
/// bytes, averaged per granule (ceiling division — conservative), stay at
/// or under the provisional byte bound. Oversize varlenas already price at
/// their 16-B inline stub ([`ColBuffer::granule_pricing_bytes`] — their
/// payloads route to the overflow stream, not the granule). When even the
/// ladder floor misses the bound, the floor is taken: the bound is a
/// geometry target, never a refusal surface. Pure function of the exact
/// chunk stats — serial and parallel seals elect identically by
/// construction.
fn elect_part_grain(rows: u64, streams: &[(u32, u32, &ColBuffer, Option<i32>)]) -> GranuleGrain {
    for &g in &geom::GRAIN_LADDER {
        let grain = GranuleGrain::from_rows(g).expect("ladder grain");
        let gc = geom::granule_count_at(rows, grain).max(1);
        let fits = streams.iter().all(|&(_, _, col, _)| {
            col.granule_pricing_bytes().div_ceil(gc) <= geom::GRANULE_BYTE_BOUND_PROVISIONAL
        });
        if fits {
            return grain;
        }
    }
    let floor = *geom::GRAIN_LADDER.last().expect("nonempty ladder");
    GranuleGrain::from_rows(floor).expect("ladder grain")
}

/// Which encoding families need the per-granule gcount table on ROOT
/// sections at a NON-DEFAULT grain (SB-10): the multi-frame-per-granule
/// families address intra-extent values/frames through `value_base`/
/// `frame_base`, whose no-table closed forms are capacity-keyed at the
/// default 8192 grain; their gcount arm is grain-exact. The granule-framed
/// families (CONST/ALP/ALP_RD/BOOL_BITMAP/FSST — FSST joined at the M3 L1
/// wave-6 fix: its capacity-keyed frame_base misaddressed at non-default
/// grains, the SB-10 x SB-4 interaction defect) address by granule ordinal
/// and stay table-free (`granule_frame_base` refuses a table by contract).
fn root_gcounts_required(encoding: u16) -> bool {
    matches!(
        EncodingId::resolve(encoding),
        Ok(EncodingId::Verbatim
            | EncodingId::ByteFor
            | EncodingId::FforInterleave
            | EncodingId::DeltaFor
            | EncodingId::DictCodes
            | EncodingId::PackedNumeric)
    )
}

/// Run the codec's ArrayDual accept/refuse over EVERY granule of the part
/// (per-part permanence): `Some(splits)` iff every granule accepts; ONE
/// refusal demotes the whole column (`None`).
fn split_all_granules(
    ginputs: &[EncodeInput<'_>],
    facts: ArrayElemFacts,
) -> WriteResult<Option<Vec<ArraySplit>>> {
    let mut out = Vec::with_capacity(ginputs.len());
    for gi in ginputs {
        match elect_array_split(gi, facts).map_err(WriteError::Format)? {
            Some(s) => out.push(s),
            None => return Ok(None),
        }
    }
    Ok(Some(out))
}

/// Emit the validity stream for one (attno, path_ord) — present iff ≥1 NULL
/// in this part (spec §6.1). Shared by the ordinary and structural arms.
#[allow(clippy::too_many_arguments)]
fn emit_validity_stream(
    buf: &mut Vec<u8>,
    sections: &mut Vec<SectionEntry>,
    col: &ColBuffer,
    val_bytes: &[u8],
    attno: u32,
    path_ord: u32,
    rows: u64,
    grain: GranuleGrain,
    granule_count: u32,
) -> WriteResult<Option<ExtentRecord>> {
    if !col.has_null() {
        return Ok(None);
    }
    pad_to(buf, 8);
    let vsec_start = buf.len();
    let mut w =
        StreamSectionWriter::begin(buf, EncodingId::BoolBitmap.as_u16(), 0, Wrapper::None)?;
    w.payload().extend_from_slice(val_bytes);
    for g in 0..granule_count {
        w.end_granule(geom::rows_in_granule_at(rows, grain, g));
    }
    let closeout = w.finish(false)?;
    sections.push(SectionEntry {
        off: vsec_start as u64,
        len: closeout.len,
        kind: SectionKind::Stream.as_u16(),
        flags: 0,
        attno,
        path_ord,
        crc: closeout.crc,
    });
    Ok(Some(ExtentRecord {
        file_off: vsec_start as u64,
        len: closeout.len,
        values: rows,
        granule_start: 0,
        granule_count,
        crc: closeout.crc,
        flags: 0,
    }))
}

/// The two-witness null law + Stats section + builder aux sections for one
/// (attno, path_ord) (spec §6.6/§8.1). Shared by the ordinary and
/// structural arms — the builder observed the VALUE currency either way.
#[allow(clippy::too_many_arguments)]
fn emit_stats_sections(
    buf: &mut Vec<u8>,
    sections: &mut Vec<SectionEntry>,
    report: &mut SealReport,
    builder: &mut Box<dyn ColumnMetaBuilder>,
    col: &ColBuffer,
    popcounts: &[u32],
    attno: u32,
    path_ord: u32,
    rows: u64,
    grain: GranuleGrain,
    granule_count: u32,
    band_count: u32,
    dict_ndv: Option<u64>,
    plane: &mut Vec<PlaneSlice>,
) -> WriteResult<()> {
    let mut stats_body: Vec<u8> = Vec::new();
    let mut band_sums: Vec<u64> = vec![0; band_count as usize];
    let mut part_sum: u64 = 0;
    // Collected once, encoded twice: the §8.1 record stream AND (pgrc2.1
    // §2.1) its SoA transposition — one builder walk, two section bodies,
    // facts byte-identical by construction.
    let mut granule_recs: Vec<StatsRecord> = Vec::with_capacity(granule_count as usize);
    for g in 0..granule_count {
        let rows_g = geom::rows_in_granule_at(rows, grain, g);
        let rec = builder.seal_granule(g);
        let bitmap_nonnull = bitmap_popcount(popcounts, g, rows_g, col.has_null());
        if rec.nonnull != bitmap_nonnull {
            return Err(WriteError::TwoWitnessSkew {
                attno,
                granule: g,
                stats_nonnull: rec.nonnull,
                bitmap_nonnull,
            });
        }
        report.nonnull_crosschecks += 1;
        band_sums[(g / GRANULES_PER_BAND) as usize] += bitmap_nonnull as u64;
        part_sum += bitmap_nonnull as u64;
        rec.encode_into(&mut stats_body);
        granule_recs.push(rec);
    }
    let mut band_recs: Vec<StatsRecord> = Vec::with_capacity(band_count as usize);
    for b in 0..band_count {
        let rec = builder.seal_band(b);
        if rec.nonnull as u64 != band_sums[b as usize] {
            return Err(WriteError::TwoWitnessSkew {
                attno,
                granule: b * GRANULES_PER_BAND, // band grain, first granule cited
                stats_nonnull: rec.nonnull,
                bitmap_nonnull: band_sums[b as usize] as u32,
            });
        }
        report.nonnull_crosschecks += 1;
        band_recs.push(rec);
    }
    for rec in &band_recs {
        rec.encode_into(&mut stats_body);
    }
    let part_rec = builder.seal_part();
    if part_rec.nonnull as u64 != part_sum {
        return Err(WriteError::TwoWitnessSkew {
            attno,
            granule: 0,
            stats_nonnull: part_rec.nonnull,
            bitmap_nonnull: part_sum as u32,
        });
    }
    report.nonnull_crosschecks += 1;
    part_rec.encode_into(&mut stats_body);

    // CMP-F (SB-6): the meta plane rides the wrapper machinery from the
    // start — Stats + every builder aux section (Psma/Bloom/NdvRegisters).
    push_meta_section(buf, sections, SectionKind::Stats, attno, path_ord, &stats_body);
    // pgrc2.1 §2.1: the FlatStats SoA transposition of the SAME records —
    // OPTIONAL (old readers skip typed), always RAW (mmap-cast law, never
    // zstd-wrapped), granule ordinals + one part rollup, ~44B/granule.
    // Kill switch PGRUST_PGRC2_FLATSTATS=0 seals without it (the A/B
    // byte-attribution arm + the revert hatch); default ON — the section
    // is additive and §8.1 stays authoritative for band grain.
    if !matches!(
        std::env::var("PGRUST_PGRC2_FLATSTATS").as_deref(),
        Ok("0") | Ok("off")
    ) {
        let flat = pgrc2_format::meta::flatstats_encode(&granule_recs, &part_rec);
        push_optional_raw_section(buf, sections, SectionKind::FlatStats, attno, path_ord, &flat);
    }
    // pgrc2.1 §2.2: the per-(column, part) exact digest — exact NDV (the
    // dict entry count, free when this stream elected DICT_CODES) + the
    // part-grain zero tally under its computed witness. 24 B, OPTIONAL,
    // raw. Kill switch PGRUST_PGRC2_PARTDIGEST=0.
    let mut plane_digest: Option<[u8; pgrc2_format::bankstats::BANKSTATS_DIGEST_LEN]> = None;
    if !matches!(
        std::env::var("PGRUST_PGRC2_PARTDIGEST").as_deref(),
        Ok("0") | Ok("off")
    ) {
        use pgrc2_format::meta::{
            PartDigest, PARTDIGESTF_NDV_EXACT, PARTDIGESTF_ZERO_COMPUTED, STATSF_COMPUTED,
        };
        // Exact NDV source ladder: the dict entry count (the dictionary IS
        // the distinct set) → the meta builder's complete counted distinct
        // set (`distinct_exact` — O(1), None on long-value residue or
        // ndv-unsound profiles) → absent (readers fall back to ndv_est).
        let ndv_exact = dict_ndv.or_else(|| builder.distinct_exact());
        let mut flags = 0u16;
        if ndv_exact.is_some() {
            flags |= PARTDIGESTF_NDV_EXACT;
        }
        if part_rec.flags & STATSF_COMPUTED != 0 {
            flags |= PARTDIGESTF_ZERO_COMPUTED;
        }
        let digest = PartDigest {
            flags,
            ndv: ndv_exact.unwrap_or(0),
            zero_count: part_rec.zero_count,
        };
        let digest_bytes = digest.encode();
        push_optional_raw_section(
            buf,
            sections,
            SectionKind::PartDigest,
            attno,
            path_ord,
            &digest_bytes,
        );
        plane_digest = <[u8; pgrc2_format::bankstats::BANKSTATS_DIGEST_LEN]>::try_from(
            digest_bytes.as_slice(),
        )
        .ok();
    }
    // OD-10/OD-11 populate kill switches (the M3.psma-ab populate half's
    // A/B apparatus): an OFF arm seals WITHOUT the aux section — the byte
    // attribution + seal-time delta the FT-9/FT-10 decline triggers price.
    // Default ON; production never sets these. SEAL-FUSION measurement-
    // honesty fix: the COMPUTE half (per-value bloom inserts, PSMA key
    // staging + block build) is skipped by the real builder under the same
    // switches (`pgrc2_meta::builder::ColumnMeta::new`), so the OFF arm now
    // prices populate CPU honestly, not just section bytes; this section
    // skip stays as the byte half and as the belt for stand-in builders.
    let populate_off = |name: &str| {
        matches!(std::env::var(name).as_deref(), Ok("0") | Ok("off"))
    };
    let psma_off = populate_off("PGRUST_PGRC2_PSMA_POPULATE");
    let bloom_off = populate_off("PGRUST_PGRC2_BLOOM_POPULATE");
    for (kind, body) in builder.aux_sections() {
        if (kind == SectionKind::Psma && psma_off) || (kind == SectionKind::Bloom && bloom_off) {
            continue;
        }
        push_meta_section(buf, sections, kind, attno, path_ord, &body);
    }
    // [fmt-land] Stats-plane capture — the seal byproduct the bankstats
    // sidecar is assembled from at publish (top-level columns only; the
    // plane's column payloads carry exactly the path_ord-0 §8.1 bodies +
    // PartDigest records the offline builder would re-read from the part).
    if path_ord == 0 {
        plane.push(PlaneSlice {
            attno,
            stats_body,
            digest: plane_digest,
        });
    }
    Ok(())
}

/// Byte view over a u64-backed buffer — the sanctioned way to hand
/// [`ByteArena`] an 8-ALIGNED base (abi.rs §19.4 debug-asserts it; a
/// `Vec<u8>`'s base carries no alignment guarantee). Mirrors the
/// `pgrc2_read` test-battery / `pgrc2_qa::corpus` idiom.
fn words_as_bytes(words: &mut Vec<u64>) -> &mut [u8] {
    // SAFETY: u64 → u8 reinterpret of an exclusively borrowed buffer;
    // alignment only loosens and the length is exact.
    unsafe {
        core::slice::from_raw_parts_mut(words.as_mut_ptr() as *mut u8, words.len() * 8)
    }
}

/// One emitted dual-substream's per-band facts.
struct DualBandSec {
    sec: Vec<u8>,
    closeout: StreamCloseout,
}

/// Emit one word-currency dual substream (sizes or elements) per band in
/// the frozen §6.7 verbatim byval layout (dense LE words at `width`
/// stride), with the gcount table ALWAYS present — these are structural
/// children (spec §6.5: child streams carry their own value counts), which
/// also makes their addressing grain-proof by construction. Emitted by
/// hand rather than through [`VerbatimEncoder`] because a granule of
/// ELEMENTS may hold far more than 8192 values (ragged arrays) — the
/// reference encoder's capacity check is row-granule-shaped; the decode
/// side has no such cap (it is bounded by the per-granule value count).
fn emit_dual_word_sections(
    values_per_granule: impl Fn(u32) -> u32,
    word_at: impl Fn(u32, usize) -> u64,
    width: u8,
    granule_count: u32,
    band_count: u32,
) -> WriteResult<Vec<DualBandSec>> {
    let mut out = Vec::with_capacity(band_count as usize);
    for b in 0..band_count {
        let g0 = b * GRANULES_PER_BAND;
        let g1 = (g0 + GRANULES_PER_BAND).min(granule_count);
        let mut sec: Vec<u8> = Vec::new();
        let mut w = StreamSectionWriter::begin(
            &mut sec,
            EncodingId::Verbatim.as_u16(),
            width,
            Wrapper::None,
        )?;
        for g in g0..g1 {
            let n = values_per_granule(g);
            let buf = w.payload();
            for i in 0..n as usize {
                let d = word_at(g, i);
                buf.extend_from_slice(&d.to_le_bytes()[..width as usize]);
            }
            w.end_granule(n);
        }
        let closeout = w.finish(true)?;
        out.push(DualBandSec { sec, closeout });
    }
    Ok(out)
}

/// Append emitted dual-substream sections to the part image; returns the
/// extent records.
fn append_dual_sections(
    buf: &mut Vec<u8>,
    sections: &mut Vec<SectionEntry>,
    band_secs: &[DualBandSec],
    attno: u32,
    granule_count: u32,
) -> Vec<ExtentRecord> {
    let mut extents = Vec::with_capacity(band_secs.len());
    for (bi, bs) in band_secs.iter().enumerate() {
        let b = bi as u32;
        let g0 = b * GRANULES_PER_BAND;
        let g1 = (g0 + GRANULES_PER_BAND).min(granule_count);
        pad_to(buf, 8);
        let start = buf.len();
        buf.extend_from_slice(&bs.sec);
        extents.push(ExtentRecord {
            file_off: start as u64,
            len: bs.sec.len() as u64,
            values: bs.closeout.values,
            granule_start: g0,
            granule_count: g1 - g0,
            crc: bs.closeout.crc,
            flags: 0,
        });
        sections.push(SectionEntry {
            off: start as u64,
            len: bs.sec.len() as u64,
            kind: SectionKind::Stream.as_u16(),
            flags: 0,
            attno,
            path_ord: 0,
            crc: bs.closeout.crc,
        });
    }
    extents
}

/// Seal one accepted array column as the TY-1 ArrayDual structural election
/// (ENC 10): a row-aligned `Sizes` substream (u32 counts; null rows 0 — the
/// split's placeholder law) plus a dense `ChildValues` substream (elements
/// as zero-extended words at the catalog element width), each in the frozen
/// VERBATIM byval layout with its own gcount table; the parent
/// `StreamEntry` records `encoding = ArrayDual` with ZERO extents (the
/// structural marker — spec §4: no §6.4 section header ever carries id 10;
/// `width`/`aux32` echo the catalog facts: element width / elemtype oid).
///
/// Verification is two-legged and mandatory: (1) each substream round-trips
/// per granule through the SAME vtable a reader resolves; (2) the split
/// COMPOSES back — `assemble_array_datums` over the split arrays must be
/// byte-identical to every stored array image (the honesty leg: a wrong
/// acceptance cannot reach a file).
#[allow(clippy::too_many_arguments)]
fn seal_array_dual(
    buf: &mut Vec<u8>,
    sections: &mut Vec<SectionEntry>,
    plans: &mut Vec<StreamPlan>,
    report: &mut SealReport,
    builder: &mut Box<dyn ColumnMetaBuilder>,
    resolver: &dyn VerifyResolver,
    col: &ColBuffer,
    ginputs: &[EncodeInput<'_>],
    splits: &[ArraySplit],
    facts: ArrayElemFacts,
    attno: u32,
    rows: u64,
    grain: GranuleGrain,
    granule_count: u32,
    band_count: u32,
    baseline: u64,
    plane: &mut Vec<PlaneSlice>,
) -> WriteResult<()> {
    let sizes_class = StorageClass::ByvalWord {
        width: 4,
        signed: false,
    };
    let elem_class = StorageClass::ByvalWord {
        width: facts.elem_len,
        signed: false,
    };
    let sizes_key = pgrc2_format::enc::stream_kernel_key(
        EncodingId::Verbatim.as_u16(),
        CLASS_BYVAL,
        4,
    )
    .map_err(WriteError::Format)?;
    let elems_key = pgrc2_format::enc::stream_kernel_key(
        EncodingId::Verbatim.as_u16(),
        CLASS_BYVAL,
        facts.elem_len,
    )
    .map_err(WriteError::Format)?;
    let sizes_vt = resolver.resolve(sizes_key)?;
    let elems_vt = resolver.resolve(elems_key)?;

    // ---- emit both substreams (scratch sections, per band) -----------------
    let sizes_secs = emit_dual_word_sections(
        |g| splits[g as usize].sizes.len() as u32,
        |g, i| splits[g as usize].sizes[i],
        4,
        granule_count,
        band_count,
    )?;
    let elems_secs = emit_dual_word_sections(
        |g| splits[g as usize].elems.len() as u32,
        |g, i| splits[g as usize].elems[i],
        facts.elem_len,
        granule_count,
        band_count,
    )?;

    // ---- mandatory verify: substream round-trips + composition -------------
    let mut datum_scratch: Vec<u64> = Vec::new();
    // ByteArena backings must be 8-aligned (the abi.rs §19.4 base-alignment
    // law, debug-asserted) — u64-backed buffers, byte-viewed (the
    // pgrc2_read test-battery / qa corpus idiom). A `Vec<u8>` base carries
    // no alignment guarantee.
    let mut verify_arena_words: Vec<u64> = vec![0; 8];
    let arena_scratch: &mut [u8] = words_as_bytes(&mut verify_arena_words);
    let mut compose_out: Vec<u64> = Vec::new();
    for b in 0..band_count {
        let g0 = b * GRANULES_PER_BAND;
        let g1 = (g0 + GRANULES_PER_BAND).min(granule_count);
        for g in g0..g1 {
            let s = &splits[g as usize];
            let legs: [(&DualBandSec, KernelKey, &CodecVtable, &[u64], StorageClass); 2] = [
                (&sizes_secs[b as usize], sizes_key, sizes_vt, &s.sizes, sizes_class),
                (&elems_secs[b as usize], elems_key, elems_vt, &s.elems, elem_class),
            ];
            for (bs, key, vt, datums, class) in legs {
                let n = datums.len() as u32;
                if datum_scratch.len() < datums.len() {
                    datum_scratch.resize(datums.len(), 0);
                }
                let ctx = KernelCtx {
                    key,
                    flags: 0,
                    fixed_len: 0,
                    bytes: &bs.sec,
                    frame_table: None,
                    granule: g,
                    granule_in_extent: g - g0,
                    rows: n,
                    values: n,
                    validity_bytes: None,
                    overflow: None,
                    dict: None,
                };
                let input = EncodeInput {
                    class,
                    rows: n,
                    datums,
                    validity: None,
                };
                verify_roundtrip(vt, &ctx, &input, &mut datum_scratch, &mut *arena_scratch)
                    .map_err(|cause| WriteError::RoundTrip {
                        attno,
                        path_ord: 0,
                        granule: g,
                        cause,
                    })?;
                report.granules_verified += 1;
            }
            // Composition leg: split arrays must rebuild every stored image
            // byte-identically (empty arrays included). u64-backed arena
            // (the 8-aligned-base law again).
            let input = &ginputs[g as usize];
            let need = assembled_arena_bytes(facts, &s.sizes) + 64;
            let mut compose_arena_words: Vec<u64> = vec![0; need.div_ceil(8)];
            let mut arena = ByteArena::new(words_as_bytes(&mut compose_arena_words));
            if compose_out.len() < s.sizes.len() {
                compose_out.resize(s.sizes.len(), 0);
            }
            assemble_array_datums(
                facts,
                &s.sizes,
                &s.elems,
                |r| input.valid(r),
                &mut compose_out,
                &mut arena,
            )
            .map_err(|cause| WriteError::RoundTrip {
                attno,
                path_ord: 0,
                granule: g,
                cause,
            })?;
            for r in 0..input.rows {
                if !input.valid(r) {
                    continue;
                }
                let mut s_in = [0u8; 8];
                let mut s_out = [0u8; 8];
                // SAFETY: input datums obey the EncodeInput pointer-class
                // contract (writer-built); compose_out datums point into
                // the live `arena_bytes`.
                let a = unsafe {
                    pgrc2_format::abi::datum_canonical_bytes(
                        StorageClass::VarlenaVerbatim,
                        input.datums[r as usize],
                        &mut s_in,
                    )
                    .map_err(WriteError::Format)?
                };
                let bcanon = unsafe {
                    pgrc2_format::abi::datum_canonical_bytes(
                        StorageClass::VarlenaVerbatim,
                        compose_out[r as usize],
                        &mut s_out,
                    )
                    .map_err(WriteError::Format)?
                };
                if a != bcanon {
                    return Err(WriteError::Contract {
                        detail: "ArrayDual composition is not byte-identical",
                    });
                }
            }
            // Meta builders observe the VALUE currency (the arrays).
            builder.observe_granule(input, g);
        }
    }

    // ---- append substream extents ------------------------------------------
    let sizes_extents = append_dual_sections(buf, sections, &sizes_secs, attno, granule_count);
    let elems_extents = append_dual_sections(buf, sections, &elems_secs, attno, granule_count);
    let chosen_len: u64 = sizes_extents.iter().map(|e| e.len).sum::<u64>()
        + elems_extents.iter().map(|e| e.len).sum::<u64>();
    let elems_total: u64 = elems_extents.iter().map(|e| e.values).sum();

    // ---- validity + stats (shared emission) --------------------------------
    let (val_bytes, val_pops) = build_validity_bytes(col, granule_count, grain);
    let validity_extent = emit_validity_stream(
        buf,
        sections,
        col,
        &val_bytes,
        attno,
        0,
        rows,
        grain,
        granule_count,
    )?;
    emit_stats_sections(
        buf,
        sections,
        report,
        builder,
        col,
        &val_pops,
        attno,
        0,
        rows,
        grain,
        granule_count,
        band_count,
        None,
        plane,
    )?;

    // ---- directory rows -----------------------------------------------------
    // Parent: the structural marker — encoding ArrayDual, ZERO extents;
    // `width` echoes the element width and `aux32` the elemtype oid (the
    // caller-declared catalog facts, recorded so a reader rebuilds
    // `ArrayElemFacts` from the entry alone).
    plans.push(StreamPlan {
        attno,
        path_ord: 0,
        role: StreamRole::Values,
        encoding: EncodingId::ArrayDual.as_u16(),
        flags: 0,
        class_id: col.schema.class.id(),
        width: facts.elem_len,
        fixed_len: 0,
        aux32: facts.elemtype,
        wrapper: Wrapper::None.as_u8(),
        values: rows,
        extents: Vec::new(),
    });
    plans.push(StreamPlan {
        attno,
        path_ord: 0,
        role: StreamRole::Sizes,
        encoding: EncodingId::Verbatim.as_u16(),
        flags: 0,
        class_id: CLASS_BYVAL,
        width: 4,
        fixed_len: 0,
        aux32: 0,
        wrapper: Wrapper::None.as_u8(),
        values: rows,
        extents: sizes_extents,
    });
    plans.push(StreamPlan {
        attno,
        path_ord: 0,
        role: StreamRole::ChildValues,
        encoding: EncodingId::Verbatim.as_u16(),
        flags: 0,
        class_id: CLASS_BYVAL,
        width: facts.elem_len,
        fixed_len: 0,
        aux32: 0,
        wrapper: Wrapper::None.as_u8(),
        values: elems_total,
        extents: elems_extents,
    });
    if let Some(ve) = validity_extent {
        plans.push(StreamPlan {
            attno,
            path_ord: 0,
            role: StreamRole::Validity,
            encoding: EncodingId::BoolBitmap.as_u16(),
            flags: 0,
            class_id: col.schema.class.id(),
            width: 0,
            fixed_len: 0,
            aux32: 0,
            wrapper: 0,
            values: rows,
            extents: vec![ve],
        });
    }

    // The structural witness: encoding ArrayDual, priced honestly (exact
    // emitted substream bytes vs the verbatim baseline).
    report.elections.push(ElectionWitness {
        attno,
        path_ord: 0,
        encoding: EncodingId::ArrayDual.as_u16(),
        baseline_len: baseline,
        chosen_len,
    });
    report
        .col_hashes
        .push((attno, 0, col.logical_hash().digest()));
    Ok(())
}

/// The stream's full validity bitmap in the §6.6 layout (per-granule
/// contiguous, byte-padded per granule) PLUS the per-granule popcounts of
/// the just-emitted bytes. Empty when the column is all-valid.
///
/// SEAL-FUSION (walks #15/#17): the second-witness popcount is accumulated
/// per granule right after that granule's bytes are ENCODED — cache-warm,
/// one pass instead of a build pass plus a later popcount pass. The
/// two-witness law is intact: the popcount still reads the EMITTED bytes
/// (this very buffer is what `emit_validity_stream` writes to the file),
/// never the input bitset.
fn build_validity_bytes(
    col: &ColBuffer,
    granule_count: u32,
    grain: GranuleGrain,
) -> (Vec<u8>, Vec<u32>) {
    if !col.has_null() {
        return (Vec::new(), Vec::new());
    }
    let mut out = Vec::new();
    let mut pops = Vec::with_capacity(granule_count as usize);
    for g in 0..granule_count {
        let rows_g = geom::rows_in_granule_at(col.rows(), grain, g);
        let at = out.len();
        encode_validity_bitmap(col.granule_validity(g, grain), rows_g, &mut out);
        pops.push(out[at..].iter().map(|b| b.count_ones()).sum());
    }
    (out, pops)
}

/// Granule `g`'s slice of the emitted validity bytes (full granules before
/// `g` each occupy exactly `grain / 8` bytes — every ladder grain is a
/// multiple of 8).
fn validity_slice<'a>(
    col: &ColBuffer,
    val_bytes: &'a [u8],
    g: u32,
    rows_g: u32,
    grain: GranuleGrain,
) -> Option<&'a [u8]> {
    if !col.has_null() {
        return None;
    }
    let start = g as usize * (grain.rows() / 8) as usize;
    let len = (rows_g as usize).div_ceil(8);
    Some(&val_bytes[start..start + len])
}

/// The second witness: popcount of the EMITTED bitmap bytes (all-valid
/// streams count rows — the absent-bitmap arm of spec §6.1). SEAL-FUSION:
/// the counts were accumulated while the bitmap bytes were built
/// ([`build_validity_bytes`]) — same emitted bytes, one pass.
fn bitmap_popcount(popcounts: &[u32], g: u32, rows_g: u32, has_null: bool) -> u32 {
    if has_null {
        popcounts[g as usize]
    } else {
        rows_g
    }
}

/// SB-7: cut the UNWRAPPED DictPayload section's extent table at dict-frame
/// boundaries. The section image is untouched (the frame table was already
/// written every `DICT_FRAME_ENTRIES` entries — ONLY extent geometry
/// changes, the ledger row's own scoping): extent i covers frame i's byte
/// run of the STORED section (extent 0 includes the 32-B header; the last
/// extent includes the frame-table tail), `values` = entries in the frame,
/// `granule_start` = frame ordinal, per-extent CRC over exactly the
/// addressed bytes — the fault-and-validate grain `ensure_frame` needs.
fn dict_frame_extents(
    image: &[u8],
    sec_start: u64,
    entry_count: u32,
) -> WriteResult<Vec<ExtentRecord>> {
    let hdr = StreamSectionHdr::decode(image).map_err(WriteError::Format)?;
    let ft = hdr
        .frame_table(image)
        .map_err(WriteError::Format)?
        .ok_or(WriteError::Contract {
            detail: "dict payload section without a frame table",
        })?;
    if ft.is_empty() {
        return Err(WriteError::Contract {
            detail: "dict payload frame table empty",
        });
    }
    // Frame starts are payload-relative; absolute (section-relative) start
    // of frame i is header + off. Extent 0 starts at 0 (header rides with
    // frame 0); the last extent runs to the section end (frame table tail).
    let nf = ft.len();
    let mut bounds: Vec<usize> = Vec::with_capacity(nf + 1);
    bounds.push(0);
    for &off in ft.iter().skip(1) {
        bounds.push(pgrc2_format::part::STREAM_SECTION_HDR_LEN + off as usize);
    }
    bounds.push(image.len());
    let mut out = Vec::with_capacity(nf);
    for f in 0..nf {
        let (lo, hi) = (bounds[f], bounds[f + 1]);
        if hi <= lo || hi > image.len() {
            return Err(WriteError::Contract {
                detail: "dict frame extent bounds",
            });
        }
        let entries_in_frame = if f + 1 < nf {
            pgrc2_format::geom::DICT_FRAME_ENTRIES as u64
        } else {
            entry_count as u64 - (nf as u64 - 1) * pgrc2_format::geom::DICT_FRAME_ENTRIES as u64
        };
        out.push(ExtentRecord {
            file_off: sec_start + lo as u64,
            len: (hi - lo) as u64,
            values: entries_in_frame,
            granule_start: f as u32,
            granule_count: 1,
            crc: pgrc2_format::wire::crc32c(&image[lo..hi]),
            flags: 0,
        });
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Cluster-key sorted ingest (IN-1 / FT-6 / OD-8)
// ---------------------------------------------------------------------------

/// One prepared key cell (comparator currency — extracted with typed error
/// propagation BEFORE sorting, so the comparator itself is infallible).
enum KeySlot<'a> {
    Null,
    I(i64),
    U(u64),
    F(f64),
    B(&'a [u8]),
}

/// PG float ordering: NaN == NaN, NaN greater than everything else
/// (float8_cmp), -0 == +0.
fn pg_float_cmp(a: f64, b: f64) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Equal,
        (true, false) => Greater,
        (false, true) => Less,
        (false, false) => a.partial_cmp(&b).expect("no NaN"),
    }
}

/// Is `schema` cluster-sortable from its STORED form? The declared key must
/// sort in the order the stored canonical form exposes: byval word classes
/// (sign per the class), floats (PG NaN law), memcmp-ordered byref, and
/// C-collated text (strcmp IS the collation). Everything else — non-C
/// collations, packed/unpacked numeric via varlena images, opaque
/// semantics — refuses typed: a wrong order silently poisons every
/// consumer that gates on the clustered witness.
fn cluster_sortable(schema: &pgrc2_format::class::ColSchema) -> bool {
    use pgrc2_format::class::{CollationClass, TypeSemantics};
    match schema.class {
        StorageClass::ByvalWord { .. }
        | StorageClass::Bool
        | StorageClass::F32
        | StorageClass::F64 => true,
        StorageClass::Fixed { .. } | StorageClass::VarlenaVerbatim => {
            match schema.semantics {
                TypeSemantics::MemcmpOrdered => true,
                TypeSemantics::TextCollated => {
                    schema.collation_class == CollationClass::C
                }
                _ => false,
            }
        }
    }
}

/// Extract one key column's comparator cells.
fn key_slots<'a>(col: &'a ColBuffer) -> WriteResult<Vec<KeySlot<'a>>> {
    let rows = col.rows();
    let mut out = Vec::with_capacity(rows as usize);
    for r in 0..rows {
        if !col.valid_at(r) {
            out.push(KeySlot::Null);
            continue;
        }
        let slot = match col.schema.class {
            StorageClass::ByvalWord { signed: true, .. } => {
                KeySlot::I(col.word_at(r) as i64)
            }
            StorageClass::ByvalWord { signed: false, .. } | StorageClass::Bool => {
                KeySlot::U(col.word_at(r))
            }
            StorageClass::F32 => KeySlot::F(f32::from_bits(col.word_at(r) as u32) as f64),
            StorageClass::F64 => KeySlot::F(f64::from_bits(col.word_at(r))),
            StorageClass::Fixed { .. } => KeySlot::B(col.fixed_at(r)?.expect("valid row")),
            StorageClass::VarlenaVerbatim => {
                KeySlot::B(col.varlena_payload(r)?.expect("valid row"))
            }
        };
        out.push(slot);
    }
    Ok(out)
}

/// Compare rows `a` and `b` under one key declaration. NULLS FIRST/LAST is
/// absolute placement (PG semantics: independent of ASC/DESC); direction
/// inverts value comparisons only.
fn key_cmp(
    slots: &[KeySlot<'_>],
    decl: &ClusterKeyDecl,
    a: usize,
    b: usize,
) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    let ord = match (&slots[a], &slots[b]) {
        (KeySlot::Null, KeySlot::Null) => return Equal,
        (KeySlot::Null, _) => {
            return if decl.nulls == NullsOrder::First { Less } else { Greater }
        }
        (_, KeySlot::Null) => {
            return if decl.nulls == NullsOrder::First { Greater } else { Less }
        }
        (KeySlot::I(x), KeySlot::I(y)) => x.cmp(y),
        (KeySlot::U(x), KeySlot::U(y)) => x.cmp(y),
        (KeySlot::F(x), KeySlot::F(y)) => pg_float_cmp(*x, *y),
        (KeySlot::B(x), KeySlot::B(y)) => x.cmp(y),
        _ => unreachable!("one column, one slot shape"),
    };
    if decl.dir == SortDir::Desc {
        ord.reverse()
    } else {
        ord
    }
}

/// Sort the part by the declared cluster key (IN-1): validate the key,
/// extract comparator cells, VERIFY-ONLY when the input is already
/// ordered (`Ok(None)`), else apply the STABLE permutation to every root
/// and lane buffer and re-verify the produced order (the attestation's
/// second leg — a broken sort cannot reach a file).
fn cluster_sort(
    roots: &[ColBuffer],
    lanes: &[ShredLane],
    decls: &[ClusterKeyDecl],
) -> WriteResult<Option<(Vec<ColBuffer>, Vec<ShredLane>)>> {
    let rows = roots.first().map(|c| c.rows()).unwrap_or(0);
    // Resolve + validate the key columns (roots only: a lane is a derived
    // stream, never a declared key).
    let mut keys: Vec<(&ColBuffer, &ClusterKeyDecl)> = Vec::with_capacity(decls.len());
    for d in decls {
        let col = roots
            .iter()
            .find(|c| c.schema.attno == d.attno)
            .ok_or(WriteError::Contract {
                detail: "declared cluster-key attno is not a table column",
            })?;
        if !cluster_sortable(&col.schema) {
            return Err(WriteError::Refused {
                what: "cluster key over a column whose stored form is not its sort order",
            });
        }
        keys.push((col, d));
    }
    let slot_cols: Vec<(Vec<KeySlot<'_>>, &ClusterKeyDecl)> = keys
        .iter()
        .map(|(col, d)| Ok((key_slots(col)?, *d)))
        .collect::<WriteResult<_>>()?;
    let cmp_rows = |a: usize, b: usize| {
        for (slots, d) in &slot_cols {
            let ord = key_cmp(slots, d, a, b);
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    };
    // Fast path: already ordered ⇒ the verification IS the attestation.
    let already = (1..rows as usize).all(|r| cmp_rows(r - 1, r) != std::cmp::Ordering::Greater);
    if already {
        return Ok(None);
    }
    // STABLE sort (equal keys keep input order — the deterministic
    // tiebreak; sealed bytes become a pure function of multiset + key).
    let mut perm: Vec<u32> = (0..rows as u32).collect();
    perm.sort_by(|&a, &b| cmp_rows(a as usize, b as usize));
    let sorted_roots: Vec<ColBuffer> = roots
        .iter()
        .map(|c| c.permuted(&perm))
        .collect::<WriteResult<_>>()?;
    let sorted_lanes: Vec<ShredLane> = lanes
        .iter()
        .map(|l| {
            Ok(ShredLane {
                scale: l.scale,
                parent_attno: l.parent_attno,
                path: l.path.clone(),
                col: l.col.permuted(&perm)?,
            })
        })
        .collect::<WriteResult<_>>()?;
    // Post-sort verification (belt): the produced order must hold on the
    // PERMUTED buffers — the attestation never rides the sort's own
    // correctness alone.
    {
        let mut vkeys: Vec<(Vec<KeySlot<'_>>, &ClusterKeyDecl)> = Vec::new();
        for d in decls {
            let col = sorted_roots
                .iter()
                .find(|c| c.schema.attno == d.attno)
                .expect("validated above");
            vkeys.push((key_slots(col)?, d));
        }
        let vcmp = |a: usize, b: usize| {
            for (slots, d) in &vkeys {
                let ord = key_cmp(slots, d, a, b);
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        };
        for r in 1..rows as usize {
            if vcmp(r - 1, r) == std::cmp::Ordering::Greater {
                return Err(WriteError::Contract {
                    detail: "cluster sort postcondition violated",
                });
            }
        }
    }
    Ok(Some((sorted_roots, sorted_lanes)))
}

/// Count varlena entries in an overflow payload region (sequential
/// 8-aligned varlena images — spec §6.8).
fn count_overflow_entries(ovf: &[u8]) -> WriteResult<u64> {
    let mut off = 0usize;
    let mut n = 0u64;
    while off < ovf.len() {
        off = off.div_ceil(8) * 8;
        if off >= ovf.len() {
            break;
        }
        let (image, _) = varlena_entry_at(ovf, off, "overflow census")?;
        off += image.len();
        n += 1;
    }
    Ok(n)
}
