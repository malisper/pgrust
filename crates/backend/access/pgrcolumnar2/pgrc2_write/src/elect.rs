//! Election framework (chunk M3-D row: "chunk stats → election → encode";
//! spec §19.6): analyze-then-elect over EXACT whole-part chunk stats, the
//! ≥10%-win gate, and the incompressible guard.
//!
//! Division of labor (spec §20 + `lanev3-m3-chunks.md` §2): the writer owns
//! the election DRIVER — the acceptance law deciding whether a candidate
//! encoding displaces the VERBATIM baseline — while the hot candidate
//! analyzers/encoders are M3-C's, plugged in through [`CandidateSource`].
//! At M3-D the built-in candidate set is exactly what the reference codec
//! serves: `CONST` when the stream is constant over the part (an exact chunk
//! stat, not a sample). Refusal demotes to VERBATIM, never normalizes
//! (charter §3).
//!
//! Elections are **per stream, per part** (per-part permanence, O-9 spirit):
//! every extent of a stream carries the same encoding, so the
//! `StreamEntry.encoding` witness is unambiguous. Per-extent divergence is
//! format-legal (each extent header carries its own encoding, spec §6.4) but
//! this writer never emits it. Encoders are per-EXTENT objects (CONST's
//! record is extent-scoped, spec §6.9), so an election yields a FACTORY and
//! the seal loop makes a fresh encoder per extent.
//!
//! The gate, in integer math (pinned by property tests):
//! a candidate WINS iff `10 * candidate_len <= 9 * baseline_len`
//! (the candidate is at least 10% smaller than the byte-exact verbatim
//! baseline). The incompressible guard IS the default arm: no winning
//! candidate ⇒ the stream stays VERBATIM.

use pgrc2_format::abi::{EncodeInput, GranuleEncoder, KernelKey};
use pgrc2_format::class::StorageClass;
use pgrc2_format::enc::EncodingId;
use pgrc2_format::part::STREAMF_DICT_EXEC;
use pgrc2_format::verbatim::ConstEncoder;

use crate::dict::{BuiltDict, DictBuilder, DictSectionImages, TextSemantics};
use crate::ingest::ColBuffer;
use crate::WriteResult;

/// Exact whole-part chunk stats for one stream — the election input
/// (analyze-then-elect with exact stats; charter §1 "exact chunk stats").
#[derive(Debug, Clone, PartialEq)]
pub struct StreamStats {
    pub class: StorageClass,
    pub rows: u64,
    pub nonnull: u64,
    /// All non-null values share one canonical image (exact; vacuously true
    /// when `nonnull == 0`).
    pub constant: bool,
    /// Total canonical value bytes of non-null rows (varlena payload bytes;
    /// word classes: width × nonnull).
    pub value_bytes: u64,
    /// Count of values routed to the overflow stream (≥ OVERSIZE_THRESHOLD).
    pub oversize_values: u64,
}

/// The ≥10%-win law (spec §19.6), integer-exact.
#[inline]
pub fn wins_ten_pct(candidate_len: u64, baseline_len: u64) -> bool {
    // (baseline - candidate) / baseline >= 10%  ⇔  10*candidate <= 9*baseline
    candidate_len
        .checked_mul(10)
        .is_some_and(|c| baseline_len.checked_mul(9).is_some_and(|b| c <= b))
}

/// The WRAPPER-layer ≥20%-win law (O-CMP-4(a), ruled 2026-08-10),
/// integer-exact: the wrapper uniquely adds decode CPU on every cold read,
/// so its gate prices that asymmetry. Encodings keep [`wins_ten_pct`] —
/// two distinct laws on purpose (mirrors
/// `pgrc2_codec::election::wins_by_twenty_percent`).
#[inline]
pub fn wins_twenty_pct(candidate_len: u64, baseline_len: u64) -> bool {
    // (baseline - candidate) / baseline >= 20%  ⇔  5*candidate <= 4*baseline
    candidate_len
        .checked_mul(5)
        .is_some_and(|c| baseline_len.checked_mul(4).is_some_and(|b| c <= b))
}

/// Makes one fresh [`GranuleEncoder`] per extent (encoders are
/// extent-scoped objects — CONST closes its record per section).
pub trait EncoderFactory {
    fn key(&self) -> KernelKey;
    fn make(&self) -> Box<dyn GranuleEncoder>;

    /// SEAL-FUSION: make an encoder POSITIONED at granule `g0` of a part
    /// whose elected grain is `grain_rows` rows per granule. Carry-aware
    /// factories override this to hand the encoder its cursor into the
    /// election's carried per-frame/per-row facts; the default ignores the
    /// position (carry-free encoders are position-independent). The seal
    /// driver calls THIS face; `make` remains the position-free face for
    /// stats-only and test callers.
    fn make_at(&self, g0: u32, grain_rows: u32) -> Box<dyn GranuleEncoder> {
        let _ = (g0, grain_rows);
        self.make()
    }
}

/// One candidate proposal: the factory plus its EXACT encoded size for the
/// whole stream (candidates that cannot price themselves closed-form must
/// trial-encode before proposing).
pub struct Candidate {
    pub factory: Box<dyn EncoderFactory>,
    pub encoded_len: u64,
}

/// The candidate seam M3-C's election machinery plugs into (sampled
/// analyzers, dict/FOR/ALP candidates). The writer consults sources in
/// order; proposal order within a source must be deterministic (the
/// byte-identical-parts law).
///
/// Both faces receive the SAME [`FullElectInput`] — column identity
/// (attno/path_ord), exact stats, extent shape, AND the materialized
/// granule inputs — closing issue #463's seam gap ("propose carries no
/// column identity/data"): a data-priced candidate plugs in through either
/// face without workarounds.
pub trait CandidateSource {
    /// Stats-priced proposals judged by the ≥10% gate in
    /// [`elect_stream`]. Sources that fully own the decision use
    /// [`CandidateSource::elect_full`] instead and leave this empty.
    fn propose(&self, input: &FullElectInput<'_>) -> Vec<Candidate>;

    /// The full-registry election face (A-lane amendment M3-A2): sources
    /// that judge granule inputs (M3-C's exact analyzers) override this and
    /// return a complete [`FullElection`]; `None` falls back to the
    /// gate-adjudicated [`CandidateSource::propose`] path above. Defaulted
    /// so simple sources implement one face. The seal driver takes the
    /// FIRST source that answers — source order is part of the
    /// deterministic contract.
    fn elect_full(&self, input: &FullElectInput<'_>) -> Option<WriteResult<FullElection>> {
        let _ = input;
        None
    }
}

struct ConstFactory {
    class: StorageClass,
}

impl EncoderFactory for ConstFactory {
    fn key(&self) -> KernelKey {
        KernelKey {
            encoding: EncodingId::Const.as_u16(),
            class: self.class.id(),
            width: self.class.width(),
        }
    }
    fn make(&self) -> Box<dyn GranuleEncoder> {
        Box::new(ConstEncoder::new(self.class))
    }
}

/// The M3-D built-in source: CONST for constant streams.
pub struct ReferenceCandidates;

impl CandidateSource for ReferenceCandidates {
    fn propose(&self, input: &FullElectInput<'_>) -> Vec<Candidate> {
        let stats = input.stats;
        if !stats.constant || stats.rows == 0 {
            return Vec::new();
        }
        // CONST stream size, closed-form (spec §6.9): per extent, a 32-B
        // section header + record {flags u8, pad [3], len u32} + bytes.
        // Word classes store the full 8-B datum word; all-null extents
        // store zero bytes.
        let rec_bytes: u64 = if stats.nonnull == 0 {
            0
        } else {
            match stats.class {
                StorageClass::ByvalWord { .. }
                | StorageClass::F32
                | StorageClass::F64
                | StorageClass::Bool => 8,
                StorageClass::Fixed { len } => len as u64,
                // Constant varlena: value_bytes / nonnull is exact.
                StorageClass::VarlenaVerbatim => stats.value_bytes / stats.nonnull.max(1),
            }
        };
        let extents = crate::seal::band_count_u64(stats.rows);
        let encoded_len = extents * (32 + 8 + rec_bytes);
        vec![Candidate {
            factory: Box::new(ConstFactory { class: stats.class }),
            encoded_len,
        }]
    }
}

/// The election result: VERBATIM (the demotion default) or a winning
/// candidate's factory.
pub enum Elected {
    Verbatim,
    Candidate(Box<dyn EncoderFactory>),
}

impl Elected {
    pub fn encoding(&self) -> u16 {
        match self {
            Elected::Verbatim => EncodingId::Verbatim.as_u16(),
            Elected::Candidate(f) => f.key().encoding,
        }
    }
}

/// The election witness recorded per stream in the seal report (the on-disk
/// witness is the `StreamEntry.encoding` byte itself).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ElectionWitness {
    pub attno: u32,
    pub path_ord: u32,
    pub encoding: u16,
    pub baseline_len: u64,
    pub chosen_len: u64,
}

/// Extent/frame shape facts the baseline formula needs (closed-form from
/// part geometry).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExtentShape {
    pub extent_count: u64,
    pub frame_count: u64,
}

/// The verbatim baseline size for a stream, closed-form (spec §6.7) — the
/// denominator of the ≥10% gate. Word/fixed classes are EXACT; varlena
/// deliberately UNDERESTIMATES (4-B headers, no inter-entry padding) so the
/// gate stays conservative: a candidate can only lose margin against an
/// underestimated baseline, never gain it.
pub fn verbatim_baseline_len(stats: &StreamStats, shape: &ExtentShape) -> u64 {
    let hdrs = shape.extent_count * 32;
    match stats.class {
        StorageClass::ByvalWord { width, .. } => hdrs + stats.rows * width as u64,
        StorageClass::F32 => hdrs + stats.rows * 4,
        StorageClass::F64 => hdrs + stats.rows * 8,
        StorageClass::Bool => hdrs + stats.rows,
        StorageClass::Fixed { len } => hdrs + stats.rows * len as u64,
        StorageClass::VarlenaVerbatim => {
            let inline_values = stats.nonnull.saturating_sub(stats.oversize_values);
            let slot_tables = stats.rows * 4 + shape.frame_count * 4;
            let entries = stats.value_bytes + inline_values * 4 + stats.oversize_values * 16;
            hdrs + slot_tables + entries + shape.frame_count * 4
        }
    }
}

/// Run the election for one stream: consult sources in order, keep the
/// smallest candidate that passes the gate, else VERBATIM. Ties keep the
/// FIRST proposal at the winning size (source order is part of the
/// deterministic contract).
pub fn elect_stream(
    input: &FullElectInput<'_>,
    sources: &[&dyn CandidateSource],
) -> (Elected, ElectionWitness) {
    let (attno, path_ord) = (input.attno, input.path_ord);
    let stats = input.stats;
    let baseline = verbatim_baseline_len(stats, input.shape);
    let mut best: Option<(Box<dyn EncoderFactory>, u64)> = None;
    for src in sources {
        for cand in src.propose(input) {
            if !wins_ten_pct(cand.encoded_len, baseline) {
                continue; // incompressible guard: not a ≥10% win
            }
            let better = match &best {
                None => true,
                Some((_, len)) => cand.encoded_len < *len,
            };
            if better {
                best = Some((cand.factory, cand.encoded_len));
            }
        }
    }
    match best {
        Some((factory, len)) => {
            let encoding = factory.key().encoding;
            (
                Elected::Candidate(factory),
                ElectionWitness {
                    attno,
                    path_ord,
                    encoding,
                    baseline_len: baseline,
                    chosen_len: len,
                },
            )
        }
        None => (
            Elected::Verbatim,
            ElectionWitness {
                attno,
                path_ord,
                encoding: EncodingId::Verbatim.as_u16(),
                baseline_len: baseline,
                chosen_len: baseline,
            },
        ),
    }
}

// ---------------------------------------------------------------------------
// Full-registry election (A-lane amendment M3-A2): the seam that wires
// M3-C's exact analyzers + encoders (BYTE_FOR / FFOR / DELTA_FOR / ALP /
// ALP_RD — f64 AND the SB-5 f32 arm — / BOOL_BITMAP / PACKED_NUMERIC /
// DICT_CODES / FSST + the LZ4/Zstd stream wrapper) into the seal driver.
// The acceptance law is unchanged — exact candidate sizes, the ≥10%-win
// gate, incompressible guard, refusal demotes to VERBATIM — only the
// candidate set grows to the §4 matrix (FSST first-class per SB-4/OD-5).
// ---------------------------------------------------------------------------

/// The dict election policy for one column (caller-supplied: the
/// publishability lattice is CATALOG knowledge — bpchar/numeric/interval/
/// float/jsonb-as-value classes must never assert `exec_ok`, spec §7; the
/// writer cannot derive type identity from `ColSchema` and does not guess).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DictPolicy {
    /// NDV cap (a caller parameter — the §9 ledger marks it RE-MEASURE
    /// before any default ships).
    pub ndv_cap: u64,
    /// Caller-asserted code-eq == value-eq (the `DICT_EXEC` lattice, spec
    /// §7). The flag is additionally gated on the zero-null proof at seal.
    pub exec_ok: bool,
    pub sem: TextSemantics,
}

/// The DICT-DEDUP feed switch (read once per elected dict column — the
/// `sketch.rs` DISTACC parse verbatim: unset or anything but "0"/"off" =
/// stage the feed). The OFF arm is the A/B control: the meta accumulator
/// keeps its own distinct structure (the pre-dedup double-accounting),
/// sidecar bytes identical either way.
fn dict_dist_feed_on() -> bool {
    !matches!(
        std::env::var("PGRUST_PGRC2_DICT_DIST_FEED").as_deref(),
        Ok("0") | Ok("off")
    )
}

/// pgrc21-widths (STORAGE-PROPOSAL §6, adjudicated on the metal leg —
/// RESULTS-PGRC21-WIDTHS "METAL VERDICT"): seal dict code streams at
/// byte-aligned widths (8/16/24/32 per granule) so plain decode loops run
/// the slice-widen arms with no shift/mask. **Default ON** (flipped
/// 2026-08-16, approved by Michael): smaller banks at every measured
/// scale (−1.56%/−0.41%/−0.54%), walls neutral-to-better, 2.0–4.8×
/// unpack floor on Graviton, answers byte-identical everywhere.
/// `PGRUST_PGRC2_CODE_WIDTH_BYTE=0` (or `off`) restores the legacy
/// bit-packed widths — the pre-flip blessed lineage reproduces exactly
/// under that override. Read once, cached. The rounded widths stay inside
/// the frozen §6.11 envelope (0..=32), so readers need no change.
fn dict_byte_widths_on() -> bool {
    static F: pgsync::OnceLock<bool> = pgsync::OnceLock::new();
    *F.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_PGRC2_CODE_WIDTH_BYTE").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// Per-column election posture (caller-supplied through [`CodecCandidates`];
/// deterministic input to the byte-identical-parts law).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnPosture {
    /// Unlocks the FFOR_INTERLEAVE tier (S4: wins fused, loses flat).
    pub fused: bool,
    /// Cold/size posture: unlocks DELTA_FOR. (The wrapper offer no longer
    /// rides this flag: per the two-layer law O-CMP-3(a), ruled 2026-08-10,
    /// the wrapper is offered on EVERY election with disk-only semantics —
    /// the seal prices exact bytes under the ≥20% wrapper law.)
    pub cold: bool,
    /// The column is numeric-typed (unlocks the PACKED_NUMERIC arm; catalog
    /// knowledge the writer cannot derive from class facts).
    pub numeric: bool,
    /// Dict election policy; `None` = the column never dict-elects.
    pub dict: Option<DictPolicy>,
}

impl Default for ColumnPosture {
    fn default() -> ColumnPosture {
        ColumnPosture {
            fused: false,
            cold: false,
            numeric: false,
            dict: None,
        }
    }
}

/// Everything a full-registry election judges: exact whole-part stats plus
/// the materialized per-granule inputs (the analyzers are input-decidable
/// pure functions — same input ⇒ same election ⇒ same bytes).
pub struct FullElectInput<'a> {
    pub attno: u32,
    pub path_ord: u32,
    pub stats: &'a StreamStats,
    pub shape: &'a ExtentShape,
    /// One `EncodeInput` per part granule, in granule order (empty for
    /// stats-only judging, e.g. unit harnesses).
    pub granules: &'a [EncodeInput<'a>],
    /// Payload access for the dict build (read-only; ingest.rs faces).
    /// `None` = no value data available: data-priced arms must demote.
    pub col: Option<&'a ColBuffer>,
}

/// A dict election's complete output: the emitted dict section images
/// (verified against the builder), the per-granule global-code arrays the
/// value stream encodes, and the stream-level max code width (the §6.3
/// width-byte stats fact).
pub struct DictPlan {
    pub images: DictSectionImages,
    pub entry_count: u32,
    pub max_width: u8,
    /// Per granule, row-dense; null slots carry 0 (the encoder writes the
    /// granule base for null slots per spec §6.6 — the placeholder law).
    pub codes: Vec<Vec<u64>>,
    /// DICT-DEDUP: the build's counted distinct set — (payload bytes,
    /// exact non-null count) in byte-rank order — for the seal driver to
    /// hand to the meta builder (`set_distribution_feed`) BEFORE the
    /// folds, so the Stats-sidecar distribution rides the dict build's
    /// one distinct structure instead of a second accumulation over the
    /// same bytes. `None` on the D2 inherit path (count-free by
    /// construction) and under `PGRUST_PGRC2_DICT_DIST_FEED=0|off` (the
    /// A/B control arm) — the meta accumulator then serves the SAME
    /// sidecar bytes (both are pure functions of the data; the sketch
    /// equivalence battery + the rig dirshas pin it).
    pub dist_feed: Option<Vec<(Vec<u8>, u64)>>,
    /// pgrc21-widths: round every granule code width up to the next
    /// byte boundary (8/16/24/32). `max_width` and the election's priced
    /// `code_stream_bytes` already reflect the posture. Default TRUE
    /// (flip 2026-08-16); `PGRUST_PGRC2_CODE_WIDTH_BYTE=0` restores the
    /// legacy bit-packed widths.
    pub byte_align: bool,
}

/// How the value stream is produced under a full election.
pub enum ElectPlan {
    /// The demotion default (byte-exact by construction).
    Verbatim,
    /// A winning encoder family; fresh encoder per extent.
    Encoder(Box<dyn EncoderFactory>),
    /// DICT_CODES: the value stream encodes [`DictPlan::codes`]; the dict
    /// sections ride along.
    Dict(DictPlan),
}

/// One stream's complete full-registry election.
pub struct FullElection {
    /// The §6.3 `encoding` field of the value stream entry.
    pub encoding: u16,
    /// The §6.3 `width` byte (BYTE_FOR delta width, PACKED_NUMERIC mantissa
    /// width, DICT_CODES max code width, ByvalWord class width for
    /// VERBATIM/CONST; else 0). Dispatch NEVER keys on this raw byte — the
    /// resolver normalizes via `pgrc2_format::enc::stream_kernel_key`.
    pub width: u8,
    /// The §6.3 `aux32` field (PACKED_NUMERIC: elected scale as i32).
    pub aux32: u32,
    /// Extra stream-entry flags (`DICT_EXEC` when the lattice + zero-null
    /// proof both hold).
    pub extra_flags: u16,
    /// Offer the section wrapper (LZ4/Zstd) at seal. The production
    /// election face ([`CodecCandidates`]) offers on EVERY election —
    /// O-CMP-3(a)'s two-layer law, disk-only semantics — and the seal
    /// driver prices the built sections exactly per arm and applies the
    /// wrapper-layer ≥20% law ([`wins_twenty_pct`]) at stream grain. The
    /// stats-only fallback path keeps `false` (reference/demotion grade —
    /// bindings without unwrappers must keep reading its parts).
    pub offer_wrapper: bool,
    pub plan: ElectPlan,
    pub witness: ElectionWitness,
    /// FSST-UNLOCK: typed seal-census note riding this election (None on
    /// every path but a text-semantics degrade — see [`SealNote`]).
    pub note: Option<SealNote>,
}

/// A typed census fact minted by the election that is NOT an encoding
/// choice (FSST-UNLOCK). Carried on [`FullElection::note`] into
/// `SealReport::notes` — the census consumer's channel; never part bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SealNote {
    /// The posture CLAIMED `TextSemantics::Utf8Chars` but this part's
    /// payload bytes failed UTF-8 validation — the election ran the
    /// stream as BytesOnly (the claim is verified per part, never
    /// trusted; char-length facts and the FSST arm stayed gated).
    Utf8ClaimDegraded { attno: u32, path_ord: u32 },
}

// ---- factories over M3-C's encoders ---------------------------------------

/// Frame ordinal of granule `g0` at `grain_rows` rows per granule (exact:
/// every SB-10 ladder grain is a multiple of `FRAME_VALUES` — geom.rs
/// compile-time law).
fn first_frame_of(g0: u32, grain_rows: u32) -> usize {
    (g0 as u64 * grain_rows as u64 / pgrc2_format::geom::FRAME_VALUES as u64) as usize
}

struct ByteForFactory {
    byval_width: u8,
    delta_width: u8,
    signed: bool,
    /// SEAL-FUSION: the fused analyzer's per-frame facts.
    carry: Option<std::sync::Arc<Vec<pgrc2_codec::election::IntFrameFact>>>,
}

impl EncoderFactory for ByteForFactory {
    fn key(&self) -> KernelKey {
        KernelKey {
            encoding: EncodingId::ByteFor.as_u16(),
            class: pgrc2_format::class::CLASS_BYVAL,
            width: self.delta_width,
        }
    }
    fn make(&self) -> Box<dyn GranuleEncoder> {
        Box::new(pgrc2_codec::bytefor::ByteForEncoder::new_bytefor(
            self.byval_width,
            self.delta_width,
            self.signed,
        ))
    }
    fn make_at(&self, g0: u32, grain_rows: u32) -> Box<dyn GranuleEncoder> {
        let mut enc = pgrc2_codec::bytefor::ByteForEncoder::new_bytefor(
            self.byval_width,
            self.delta_width,
            self.signed,
        );
        if let Some(facts) = &self.carry {
            enc.carry = Some(pgrc2_codec::bytefor::IntCarryCursor {
                facts: facts.clone(),
                next: first_frame_of(g0, grain_rows),
            });
        }
        Box::new(enc)
    }
}

struct FforFactory {
    signed: bool,
    carry: Option<std::sync::Arc<Vec<pgrc2_codec::election::IntFrameFact>>>,
}

impl EncoderFactory for FforFactory {
    fn key(&self) -> KernelKey {
        KernelKey {
            encoding: EncodingId::FforInterleave.as_u16(),
            class: pgrc2_format::class::CLASS_BYVAL,
            width: 0,
        }
    }
    fn make(&self) -> Box<dyn GranuleEncoder> {
        Box::new(pgrc2_codec::ffor::FforEncoder {
            signed: self.signed,
            carry: None,
        })
    }
    fn make_at(&self, g0: u32, grain_rows: u32) -> Box<dyn GranuleEncoder> {
        Box::new(pgrc2_codec::ffor::FforEncoder {
            signed: self.signed,
            carry: self.carry.as_ref().map(|facts| {
                pgrc2_codec::bytefor::IntCarryCursor {
                    facts: facts.clone(),
                    next: first_frame_of(g0, grain_rows),
                }
            }),
        })
    }
}

struct DeltaForFactory {
    carry: Option<std::sync::Arc<Vec<pgrc2_codec::election::IntFrameFact>>>,
}

impl EncoderFactory for DeltaForFactory {
    fn key(&self) -> KernelKey {
        KernelKey {
            encoding: EncodingId::DeltaFor.as_u16(),
            class: pgrc2_format::class::CLASS_BYVAL,
            width: 0,
        }
    }
    fn make(&self) -> Box<dyn GranuleEncoder> {
        Box::new(pgrc2_codec::deltafor::DeltaForEncoder::default())
    }
    fn make_at(&self, g0: u32, grain_rows: u32) -> Box<dyn GranuleEncoder> {
        Box::new(pgrc2_codec::deltafor::DeltaForEncoder {
            carry: self.carry.as_ref().map(|facts| {
                pgrc2_codec::bytefor::IntCarryCursor {
                    facts: facts.clone(),
                    next: first_frame_of(g0, grain_rows),
                }
            }),
        })
    }
}

struct AlpFactory {
    encoding: EncodingId,
    /// CLASS_F64 or CLASS_F32 — the SB-5 f32 arm rides the same factory
    /// (the encoder type follows the class; the F32 arm always stamps ALP).
    class: u8,
    /// SEAL-FUSION: the election's encoded frames (one per DEFAULT-grain
    /// granule — the election's slicing).
    carry: Option<std::sync::Arc<Vec<Vec<u8>>>>,
}

impl EncoderFactory for AlpFactory {
    fn key(&self) -> KernelKey {
        KernelKey {
            encoding: self.encoding.as_u16(),
            class: self.class,
            width: 0,
        }
    }
    fn make(&self) -> Box<dyn GranuleEncoder> {
        if self.class == pgrc2_format::class::CLASS_F32 {
            Box::new(pgrc2_codec::alpc::AlpF32Encoder::default())
        } else {
            Box::new(pgrc2_codec::alpc::AlpEncoder {
                encoding: self.encoding,
                carry: None,
            })
        }
    }
    fn make_at(&self, g0: u32, grain_rows: u32) -> Box<dyn GranuleEncoder> {
        // ALP is granule-framed and the vendored sampling is granule-slice
        // scoped, so carried frames are byte-valid ONLY when the seal's
        // grain IS the election's (the default). Non-default grains
        // re-encode — exactly the pre-fusion path.
        let carry = if grain_rows == pgrc2_format::geom::GRANULE_ROWS {
            self.carry
                .as_ref()
                .map(|frames| pgrc2_codec::alpc::AlpCarryCursor {
                    frames: frames.clone(),
                    next: g0 as usize,
                })
        } else {
            None
        };
        if self.class == pgrc2_format::class::CLASS_F32 {
            Box::new(pgrc2_codec::alpc::AlpF32Encoder { carry })
        } else {
            Box::new(pgrc2_codec::alpc::AlpEncoder {
                encoding: self.encoding,
                carry,
            })
        }
    }
}

/// SB-4: the FSST factory carries the per-(column,part) symbol table the
/// election built; each extent's encoder embeds the SAME table image
/// (OD-5 scope — one table per column per part). SEAL-FUSION: it also
/// carries the election's trial compression (row currency — grain-proof),
/// so the seal's encode is a slot-table + memcpy emit.
struct FsstFactory {
    table: pgrc2_codec::fsst::FsstSymbolTable,
    carry: Option<pgrc2_codec::fsst::FsstCarry>,
}

impl EncoderFactory for FsstFactory {
    fn key(&self) -> KernelKey {
        KernelKey {
            encoding: EncodingId::Fsst.as_u16(),
            class: pgrc2_format::class::CLASS_VARLENA,
            width: 0,
        }
    }
    fn make(&self) -> Box<dyn GranuleEncoder> {
        Box::new(pgrc2_codec::fsst::FsstEncoder::new(self.table.clone()))
    }
    fn make_at(&self, g0: u32, grain_rows: u32) -> Box<dyn GranuleEncoder> {
        match &self.carry {
            Some(c) => Box::new(pgrc2_codec::fsst::FsstEncoder::new_carried(
                self.table.clone(),
                c.clone(),
                g0 as usize * grain_rows as usize,
            )),
            None => self.make(),
        }
    }
}

struct BoolBitmapFactory;

impl EncoderFactory for BoolBitmapFactory {
    fn key(&self) -> KernelKey {
        KernelKey {
            encoding: EncodingId::BoolBitmap.as_u16(),
            class: pgrc2_format::class::CLASS_BOOL,
            width: 1,
        }
    }
    fn make(&self) -> Box<dyn GranuleEncoder> {
        Box::new(pgrc2_codec::boolbm::BoolBitmapEncoder)
    }
}

struct PackedNumericFactory {
    scale: i32,
    width: u8,
}

impl EncoderFactory for PackedNumericFactory {
    fn key(&self) -> KernelKey {
        KernelKey {
            encoding: EncodingId::PackedNumeric.as_u16(),
            class: pgrc2_format::class::CLASS_VARLENA,
            width: self.width,
        }
    }
    fn make(&self) -> Box<dyn GranuleEncoder> {
        Box::new(pgrc2_codec::packednum::PackedNumericEncoder::new(
            self.scale, self.width,
        ))
    }
}

/// SEAL-FUSION: what an election carried for encode (nothing, the fused
/// int analyzer's per-frame facts, or the float election's encoded ALP
/// frames).
enum ElectCarry {
    None,
    Int(std::sync::Arc<Vec<pgrc2_codec::election::IntFrameFact>>),
    Alp(std::sync::Arc<Vec<Vec<u8>>>),
}

// ---- the source ------------------------------------------------------------

/// The full-registry candidate source: per-column postures over M3-C's
/// analyzers. Answers `elect_full` for EVERY stream (so a seal driven by
/// this source never falls back to the stats-only path); `propose` is empty
/// by design.
pub struct CodecCandidates {
    default_posture: ColumnPosture,
    per_column: Vec<((u32, u32), ColumnPosture)>,
    /// [json-rung1] Per-parent DERIVED-LANE posture: applies to every
    /// `path_ord ≥ 1` stream of the attno (shred lane path ordinals are
    /// minted per part at shred time, so exact (attno, path_ord) keys
    /// cannot be known when postures are derived from the catalog). An
    /// exact `per_column` entry still wins.
    per_lane: Vec<(u32, ColumnPosture)>,
    /// SEAL-SPEED-2 D3: which families elect from the deterministic sample.
    /// DEFAULT-ON since the coordinator ruling of 2026-08-13 (the
    /// measured-decision class: the adoption A/B showed +0.000% regret on
    /// EVERY family at 1m AND 10m with zero changed elections — sampled
    /// banks dirsha-identical to the blessed identities). The full census
    /// stays the flagged exact/control arm: `PGRUST_SEAL_SAMPLE_ELECT=0`
    /// is the kill switch (see [`default_sample_families`]).
    sample: pgrc2_codec::election::SampleFamilies,
}

/// The D3 default (read once per process): unset = ALL sampled families
/// (the ruled default), `0` = full census (the kill switch / control arm),
/// else the `SampleFamilies::parse` vocabulary (`1`, `int,float,fsst`).
pub fn default_sample_families() -> pgrc2_codec::election::SampleFamilies {
    use pgrc2_codec::election::SampleFamilies;
    static F: pgsync::OnceLock<SampleFamilies> = pgsync::OnceLock::new();
    *F.get_or_init(|| match std::env::var("PGRUST_SEAL_SAMPLE_ELECT") {
        Ok(v) => SampleFamilies::parse(&v),
        Err(_) => SampleFamilies::ALL,
    })
}

/// FSST-UNLOCK — **RETIRED 2026-08-14 (the M5d DECLINE, Michael's
/// conditional ruling resolved NO).** History: Michael's 2026-08-12 ruling
/// ("we never had it engage? ... let's do it!") landed the unlock (#997,
/// `PGRUST_FSST_UNLOCK`, default OFF); the S8 100m ceremony adjudicated
/// the candidate a size LOSS as-cut (+0.234%); the M5d evidence package
/// (PR #1056, `docs/design/lanev4-m5d-fsst-package.md`) then measured the
/// recorded remedies and the query-side case: delta-form char_len
/// reclaims only 3.3–4.5% of the DictIndex entropy (§3, corpus
/// Cyrillic-dominated), and match-on-compressed runs 2.55× the NO-FSST
/// control on the like band's shape of record (§8). Michael's condition
/// — "could it help speed up queries even if it doesn't save space? If
/// not let's not do it" — resolved NO; the switch is RETIRED: production
/// claim suppliers stamp BytesOnly unconditionally (byte-for-byte the
/// blessed lineage; every anchor default-cuttable forever). What REMAINS
/// live: #997's verify-don't-trust UTF-8 validation + typed degrade
/// (correctness surface — an explicit `Utf8Chars` posture from QA
/// manifests/unit fixtures is still honored and still verified per
/// part), the FSST codec + MoC kernels + teeth (the decline's retained
/// evidence), and the §8.3 re-pose triggers (multi-byte-needle corpora;
/// resident-memory-constrained deployments; cold-I/O text walls) — any
/// one reopens the decision WITH numbers, never by flipping a constant.
pub const fn fsst_unlock() -> bool {
    false
}

/// M5d.char-len-form — **RECORDED DECLINE 2026-08-14 (rode the unlock's
/// retirement).** The S8 §2(a) delta form (`char_field = byte_len −
/// char_len`) was measured at the 1m/10m ladder: it reclaims only
/// 4.5%/3.3% of the as-cut DictIndex entropy growth — the hits corpus's
/// big dict-index columns are Cyrillic-heavy, so the delta carries the
/// same information as absolute char_len (ASCII columns DO zero out —
/// the mechanism is correct, the premise was corpus-wrong; package §3).
/// ABSOLUTE char_len stays the form of record; the emit dial is retired
/// with the unlock (this constant), while the SELF-DESCRIBING decode
/// surface (`STREAMF_CHARLEN_DELTA` + reconstruction + teeth) stays —
/// harmless, tooth-covered, and the honest record of the measured arm.
pub const fn fsst_charlen_delta() -> bool {
    false
}

/// OPTION-C RE-POSE PROBE (`PGRUST_FSST_OPTC_PROBE=1`, default OFF —
/// Michael's follow-up on the decline: "but doesn't it cost fsst? Could
/// we try fsst without char_len?"). Arms the S8 §2(b) lengths-prepass
/// candidate for MEASUREMENT ONLY (M5d package §9): production text
/// suppliers mint `Utf8Chars` claims (via [`utf8_claims_born`]) AND dict
/// cuts emit the ABSENT char-len form (nothing stored; zeros under the
/// wrapper; consumers recompute — `STREAMF_CHARLEN_ABSENT`). The merged
/// DECLINE STANDS: default OFF keeps every blessed anchor
/// default-cuttable; no bank carrying this form is ever blessed by this
/// probe. Read ONCE, `pgsync::OnceLock`-cached (the house probe class);
/// production never sets.
pub fn fsst_optc_probe() -> bool {
    static F: pgsync::OnceLock<bool> = pgsync::OnceLock::new();
    *F.get_or_init(|| {
        matches!(std::env::var("PGRUST_FSST_OPTC_PROBE"), Ok(v) if v.trim() == "1")
    })
}

/// Should production claim suppliers mint `Utf8Chars` claims? The one
/// gate the suppliers consult: the retired unlock (const false — the
/// decline's record) OR the Option-C re-pose probe. Explicit postures
/// (QA manifests, fixtures) bypass this and are always honored+verified.
pub fn utf8_claims_born() -> bool {
    fsst_unlock() || fsst_optc_probe()
}

/// The dict emit's char-len form: Absent under the Option-C probe,
/// Absolute otherwise (Delta stays a measured decline — no dial mints it).
pub fn fsst_charlen_form() -> pgrc2_format::dict::DictCharLenForm {
    use pgrc2_format::dict::DictCharLenForm;
    if fsst_optc_probe() {
        DictCharLenForm::Absent
    } else if fsst_charlen_delta() {
        DictCharLenForm::Delta
    } else {
        DictCharLenForm::Absolute
    }
}


/// FORMAT-LAYOUT lever A (Michael, 2026-08-16: "stop double-storing text
/// ... don't elect dict where the payload approximates the column").
/// `PGRUST_PGRC2_DICT_PAYLOAD_CAP=<pct>` demotes the dict arm whenever the
/// dictionary PAYLOAD section alone is >= pct% of the verbatim baseline —
/// on near-unique text (url/referer-shaped parts) the payload IS the
/// column, so the codes/index bytes ride on top of an already-full copy
/// and the wrapper compresses the deduplicated payload WORSE than the
/// repeat-rich verbatim image. Default OFF (unset/0/off) = today's
/// elections byte-for-byte; the blessed lineage never sees this knob.
/// Measurement arm only — a default flip is Michael's call, priced by the
/// fmt-layout election sweep. Read ONCE, `pgsync::OnceLock`-cached;
/// production never sets.
fn dict_payload_cap_pct() -> Option<u64> {
    static F: pgsync::OnceLock<Option<u64>> = pgsync::OnceLock::new();
    *F.get_or_init(|| match std::env::var("PGRUST_PGRC2_DICT_PAYLOAD_CAP") {
        Ok(v) => match v.trim() {
            "" | "0" | "off" => None,
            t => t.parse::<u64>().ok().filter(|p| (1..=100).contains(p)),
        },
        Err(_) => None,
    })
}

impl CodecCandidates {
    pub fn new(default_posture: ColumnPosture) -> CodecCandidates {
        CodecCandidates {
            default_posture,
            per_column: Vec::new(),
            per_lane: Vec::new(),
            sample: default_sample_families(),
        }
    }

    /// D3: pick the sampled-election families (per-family adoption law).
    pub fn with_sampling(
        mut self,
        f: pgrc2_codec::election::SampleFamilies,
    ) -> CodecCandidates {
        self.sample = f;
        self
    }

    /// Override the posture for one (attno, path_ord) stream.
    pub fn with_column(mut self, attno: u32, path_ord: u32, p: ColumnPosture) -> CodecCandidates {
        self.per_column.push(((attno, path_ord), p));
        self
    }

    /// [json-rung1] Posture for every DERIVED shred-lane stream
    /// (`path_ord ≥ 1`) of one parent column (see `per_lane`).
    pub fn with_lane_default(mut self, attno: u32, p: ColumnPosture) -> CodecCandidates {
        self.per_lane.push((attno, p));
        self
    }

    fn posture(&self, attno: u32, path_ord: u32) -> ColumnPosture {
        if let Some(p) = self
            .per_column
            .iter()
            .find(|((a, p), _)| *a == attno && *p == path_ord)
            .map(|(_, p)| *p)
        {
            return p;
        }
        if path_ord != 0 {
            if let Some(p) = self
                .per_lane
                .iter()
                .find(|(a, _)| *a == attno)
                .map(|(_, p)| *p)
            {
                return p;
            }
        }
        self.default_posture
    }

    fn run(&self, input: &FullElectInput<'_>) -> WriteResult<FullElection> {
        use pgrc2_codec::election as ce;
        let p = self.posture(input.attno, input.path_ord);
        let stats = input.stats;
        let baseline = verbatim_baseline_len(stats, input.shape);
        // O-CMP-3(a) (ruled 2026-08-10): the wrapper is OFFERED on every
        // election this face makes — hot fixed-width included. Disk-only
        // semantics: the offer never changes what kernels see (unwrap
        // rebuilds the encoded image at extent open), and the seal's exact
        // per-arm pricing under the ≥20% wrapper law is the only decider.
        let verbatim = |w: ElectionWitness| FullElection {
            encoding: EncodingId::Verbatim.as_u16(),
            width: stats.class.width(),
            aux32: 0,
            extra_flags: 0,
            offer_wrapper: true,
            plan: ElectPlan::Verbatim,
            witness: w,
            note: None,
        };
        let vwitness = ElectionWitness {
            attno: input.attno,
            path_ord: input.path_ord,
            encoding: EncodingId::Verbatim.as_u16(),
            baseline_len: baseline,
            chosen_len: baseline,
        };
        // FSST-UNLOCK: minted by the text-semantics resolution below and
        // attached to WHATEVER election this stream ends with.
        let mut sem_note: Option<SealNote> = None;
        let outcome = match stats.class {
            StorageClass::ByvalWord { width, signed } => {
                // SEAL-FUSION: the fused analyzer — one pass serves the
                // any/const probes + every int arm's reductions, and the
                // per-frame facts are carried into encode. D3: the sampled
                // arm picks the FAMILY from a ~1% frame sample and computes
                // only the winner's facts exactly (same carry currency).
                let (e, ic) = if self.sample.int {
                    ce::elect_int_sampled_carry(input.granules, width, signed, p.fused, p.cold)
                } else {
                    ce::elect_int_carry(input.granules, width, signed, p.fused, p.cold)
                };
                self.word_outcome(
                    input,
                    e,
                    signed,
                    ElectCarry::Int(std::sync::Arc::new(ic.frames)),
                )
            }
            StorageClass::F32 | StorageClass::F64 => {
                // SEAL-FUSION: the election prices by ENCODING once and the
                // frames are carried — encode never re-runs the identical
                // ALP scheme search. D3: the sampled arm pre-gates on a ~1%
                // granule sample so hopeless streams never pay the full
                // pricing encode.
                let (e, fc) = if self.sample.float {
                    ce::elect_float_sampled_carry(input.granules, stats.class)
                } else {
                    ce::elect_float_carry(input.granules, stats.class)
                };
                let carry = match fc {
                    Some(f) => ElectCarry::Alp(std::sync::Arc::new(f.frames)),
                    None => ElectCarry::None,
                };
                self.word_outcome(input, e, false, carry)
            }
            StorageClass::Bool => {
                self.word_outcome(input, ce::elect_bool(input.granules), false, ElectCarry::None)
            }
            StorageClass::Fixed { .. } => None,
            StorageClass::VarlenaVerbatim => {
                if p.numeric {
                    let e = ce::elect_numeric(input.granules, baseline as usize)
                        .map_err(crate::WriteError::Format)?;
                    self.word_outcome(input, e, false, ElectCarry::None)
                } else if let Some(dp) = p.dict {
                    // FSST-UNLOCK: resolve the text-semantics CLAIM into
                    // the verified per-part fact BEFORE either text arm
                    // consumes it (dict char-length facts + the FSST
                    // gate both ride `dp.sem`).
                    let (dp, note) = resolve_text_policy(input, dp)?;
                    sem_note = note;
                    match self.dict_arm(input, dp, baseline)? {
                        Some(f) => Some(f),
                        // SB-4: the FSST arm competes exactly on the
                        // dict-loser text families (NDV-cap breach,
                        // BelowWinGate, url/log-shaped near-unique) under
                        // the same exact-bytes ≥10% law.
                        None => self.fsst_arm(input, dp, baseline)?,
                    }
                } else {
                    None
                }
            }
        };
        if let Some(mut f) = outcome {
            f.note = sem_note;
            return Ok(f);
        }
        // Demotion / pointer-class arm: CONST when the exact chunk stat
        // proves constancy AND clears the gate, else VERBATIM (+wrapper
        // offer under the cold posture).
        for cand in ReferenceCandidates.propose(input) {
            if wins_ten_pct(cand.encoded_len, baseline) {
                let encoding = cand.factory.key().encoding;
                let width = cand.factory.key().width;
                return Ok(FullElection {
                    encoding,
                    width,
                    aux32: 0,
                    extra_flags: 0,
                    offer_wrapper: true,
                    plan: ElectPlan::Encoder(cand.factory),
                    witness: ElectionWitness {
                        attno: input.attno,
                        path_ord: input.path_ord,
                        encoding,
                        baseline_len: baseline,
                        chosen_len: cand.encoded_len,
                    },
                    note: sem_note,
                });
            }
        }
        let mut f = verbatim(vwitness);
        f.note = sem_note;
        Ok(f)
    }

    /// Map a codec-crate word-class election onto the seal plan. `None` =
    /// demoted (the caller's VERBATIM arm runs).
    fn word_outcome(
        &self,
        input: &FullElectInput<'_>,
        e: pgrc2_codec::election::Election,
        signed: bool,
        carry: ElectCarry,
    ) -> Option<FullElection> {
        use pgrc2_codec::election::Election;
        let Election::Elected {
            encoding,
            width,
            aux32,
            candidate_bytes,
            baseline_bytes,
        } = e
        else {
            return None;
        };
        let class = input.stats.class;
        let int_carry = match &carry {
            ElectCarry::Int(f) => Some(f.clone()),
            _ => None,
        };
        let alp_carry = match &carry {
            ElectCarry::Alp(f) => Some(f.clone()),
            _ => None,
        };
        let (entry_width, factory): (u8, Box<dyn EncoderFactory>) = match encoding {
            EncodingId::Const => (class.width(), Box::new(ConstFactory { class })),
            EncodingId::ByteFor => (
                width,
                Box::new(ByteForFactory {
                    byval_width: class.width(),
                    delta_width: width,
                    signed,
                    carry: int_carry,
                }),
            ),
            EncodingId::FforInterleave => (
                0,
                Box::new(FforFactory {
                    signed,
                    carry: int_carry,
                }),
            ),
            EncodingId::DeltaFor => (0, Box::new(DeltaForFactory { carry: int_carry })),
            EncodingId::Alp | EncodingId::AlpRd => (
                0,
                Box::new(AlpFactory {
                    encoding,
                    class: class.id(),
                    carry: alp_carry,
                }),
            ),
            EncodingId::BoolBitmap => (0, Box::new(BoolBitmapFactory)),
            EncodingId::PackedNumeric => (
                width,
                Box::new(PackedNumericFactory {
                    scale: aux32 as i32,
                    width,
                }),
            ),
            // The codec elections never propose these here.
            _ => return None,
        };
        Some(FullElection {
            encoding: encoding.as_u16(),
            width: entry_width,
            aux32,
            extra_flags: 0,
            offer_wrapper: true,
            plan: ElectPlan::Encoder(factory),
            witness: ElectionWitness {
                attno: input.attno,
                path_ord: input.path_ord,
                encoding: encoding.as_u16(),
                baseline_len: baseline_bytes as u64,
                chosen_len: candidate_bytes as u64,
            },
            note: None,
        })
    }

    /// The DICT_CODES arm: whole-part dict build (byte-rank-sorted global
    /// codes), exact dict + code-stream pricing, the codec's decision
    /// function, and the emitted-section self-verify. `None` = demoted.
    ///
    /// SEAL-SPEED-2 D2: when the ingest staged an inherited-dictionary side
    /// channel covering the part (parquet dict pages), the build routes
    /// through [`CodecCandidates::dict_arm_inherit`] — same election
    /// OUTCOME and same emitted bytes by the canonical-form law, minus the
    /// row-grain hash observe + rank assignment.
    fn dict_arm(
        &self,
        input: &FullElectInput<'_>,
        p: DictPolicy,
        baseline: u64,
    ) -> WriteResult<Option<FullElection>> {
        use pgrc2_codec::election as ce;
        let stats = input.stats;
        // Oversize values cannot live in a dictionary entry (the overflow
        // stream is the varlena path's own mechanism): demote.
        if stats.oversize_values > 0 || stats.nonnull == 0 {
            return Ok(None);
        }
        // Data-priced arm: no value data ⇒ demote (issue-#463 contract).
        let Some(col) = input.col else {
            return Ok(None);
        };
        if col.dict_side().is_some() {
            return self.dict_arm_inherit(input, p, baseline, col);
        }
        let mut b = DictBuilder::new(p.sem);
        for row in 0..col.rows() {
            if let Some(payload) = col.varlena_payload(row)? {
                b.observe(payload);
            }
        }
        let ndv = b.distinct() as u64;
        if ndv > p.ndv_cap {
            return Ok(None);
        }
        let built = b.build();
        // Global codes per granule + exact code-stream pricing. SEAL-FUSION
        // (walk #7b): code assignment is a rank read through the
        // observe-time row handle — the second whole-column payload walk +
        // per-row binary search are gone. The observe loop and this loop
        // visit non-null rows in the same row-dense order (the granule
        // validity IS the column's bitset), so the k-th observed row here
        // is the k-th observed row there.
        let mut codes: Vec<Vec<u64>> = Vec::with_capacity(input.granules.len());
        let mut code_bytes = 0usize;
        let mut max_width = 0u8;
        let byte_align = dict_byte_widths_on();
        let mut observed = 0usize;
        for gi in input.granules.iter() {
            let mut gc: Vec<u64> = Vec::with_capacity(gi.rows as usize);
            for r in 0..gi.rows {
                let code = if gi.valid(r) {
                    let c = built.code_of_observed(observed).ok_or(
                        crate::WriteError::Contract {
                            detail: "dict build lost a value",
                        },
                    )? as u64;
                    observed += 1;
                    c
                } else {
                    0
                };
                gc.push(code);
            }
            let ci = EncodeInput {
                class: stats.class,
                rows: gi.rows,
                datums: &gc,
                validity: gi.validity,
            };
            let (bytes, w) = pgrc2_codec::dictcodes::granule_block_facts(&ci, byte_align);
            code_bytes += bytes;
            max_width = max_width.max(w);
            codes.push(gc);
        }
        let images = built.emit_sections(fsst_charlen_form())?;
        built.verify_sections(&images)?;
        let dict_bytes = images.index_section.len() + images.payload_section.len();
        // FORMAT-LAYOUT lever A: payload-ratio demotion (see
        // `dict_payload_cap_pct`). Exact bytes both sides — the payload
        // SECTION image vs the same verbatim baseline the ≥10% law prices.
        // Demote = fall through to the FSST arm / verbatim catch-all, the
        // standard dict-loser path; answers are encoding-invariant.
        if let Some(pct) = dict_payload_cap_pct() {
            if (images.payload_section.len() as u64) * 100 >= pct * baseline {
                return Ok(None);
            }
        }
        let arm = ce::DictArm {
            rows: stats.rows,
            ndv,
            ndv_cap: p.ndv_cap,
            dict_bytes,
            code_stream_bytes: code_bytes,
            baseline_bytes: baseline as usize,
        };
        let ce::Election::Elected {
            candidate_bytes, ..
        } = ce::elect_text_dict(arm)
        else {
            return Ok(None);
        };
        // DICT_EXEC: the caller-asserted lattice AND the zero-null proof
        // (O-6 posture; nullable dict lanes are format-legal but never
        // execution-published at M3).
        let extra_flags = if p.exec_ok && !col.has_null() {
            STREAMF_DICT_EXEC
        } else {
            0
        };
        // DICT-DEDUP: the elected build's counted distinct set becomes the
        // Stats-sidecar distribution feed (sections already emitted +
        // verified above — the entries move out, no copy). The switch is
        // the A/B control: =0|off stages nothing and the meta accumulator
        // keeps the pre-dedup double-accounting arm (a perf dial, never a
        // bytes dial).
        let dist_feed = if dict_dist_feed_on() {
            built.into_counted_entries()
        } else {
            None
        };
        Ok(Some(FullElection {
            encoding: EncodingId::DictCodes.as_u16(),
            width: max_width,
            aux32: 0,
            extra_flags,
            offer_wrapper: true,
            plan: ElectPlan::Dict(DictPlan {
                entry_count: images.entry_count,
                images,
                max_width,
                codes,
                dist_feed,
                byte_align,
            }),
            witness: ElectionWitness {
                attno: input.attno,
                path_ord: input.path_ord,
                encoding: EncodingId::DictCodes.as_u16(),
                baseline_len: baseline,
                chosen_len: candidate_bytes as u64,
            },
            note: None,
        }))
    }

    /// SEAL-SPEED-2 D2 — the inherited-dictionary dict arm (merge / remap /
    /// verify). Replaces the row-grain hash observe + rank assignment with:
    ///
    /// 1. **referenced filter** — one integer-grain pass over the part's
    ///    `(source, code)` rows marks which source entries the part
    ///    actually uses (inherited dicts may carry unreferenced entries;
    ///    keeping them would break entry-set equality with the rebuild);
    /// 2. **hybrid hash** — PLAIN-fallback rows (code-free by the source's
    ///    own structure) hash their VALUES into the merge, exactly the
    ///    sweep's Arrow-lineage law — the only remaining byte-grain work,
    ///    proportional to the fallback tail, not the part;
    /// 3. **merge + dedup during the byte-rank sort** — referenced entries
    ///    from every source + plain distinct values sort by raw byte rank;
    ///    adjacent-equal collapses; the result feeds
    ///    [`BuiltDict::from_sorted_entries`], whose strict-ascending check
    ///    IS the strict-distinctness cert re-proven per part;
    /// 4. **NDV before any value walk** — the exact NDV is the merge's
    ///    entry count (entry grain), so the cap decision and the election
    ///    pricing never walk the part's values;
    /// 5. **remap + translate** — old→new tables per source; the part's
    ///    code streams translate by indexed lookup (integer grain).
    ///
    /// SAFETY (the canonical-form argument, held in code): the byte-rank
    /// dict is a pure function of the part's distinct value SET; the
    /// referenced filter + hybrid hash reproduce exactly the set the
    /// rebuild's observe walk would collect (every kept entry is some
    /// row's value; every row's value is kept — dict rows through their
    /// source entry, plain rows through their hashed payload), so entries,
    /// codes, sections, and the election decision are all byte-identical
    /// to the rebuild. Mechanically gated by: the strict-ascending cert
    /// (3), `verify_sections` round-trip, the seal's `verify_codes`, the
    /// per-row entry/staged-payload equality check below, and the rig's
    /// dirsha gates (serial arm rebuilds — serial==DOP IS the proof run).
    fn dict_arm_inherit(
        &self,
        input: &FullElectInput<'_>,
        p: DictPolicy,
        baseline: u64,
        col: &ColBuffer,
    ) -> WriteResult<Option<FullElection>> {
        use pgrc2_codec::election as ce;
        let stats = input.stats;
        let side = col.dict_side().expect("caller checked");
        let contract = |detail: &'static str| crate::WriteError::Contract { detail };

        // ---- (1) referenced filter + (2) plain distinct set --------------
        let mut used: Vec<Vec<bool>> = side
            .sources
            .iter()
            .map(|s| vec![false; s.entry_count() as usize])
            .collect();
        let mut plain: std::collections::BTreeSet<&[u8]> = std::collections::BTreeSet::new();
        for row in 0..col.rows() {
            if !col.valid_at(row) {
                continue;
            }
            let slot = side.rows[row as usize];
            if slot == crate::ingest::DICT_ROW_PLAIN {
                let payload = col
                    .varlena_payload(row)?
                    .ok_or_else(|| contract("valid row without payload"))?;
                plain.insert(payload);
            } else {
                let (src, code) = ((slot >> 32) as usize, slot as u32 as usize);
                let lane = used
                    .get_mut(src)
                    .ok_or_else(|| contract("dict side source out of range"))?;
                *lane
                    .get_mut(code)
                    .ok_or_else(|| contract("dict side code out of range"))? = true;
            }
        }

        // ---- (3) merge + dedup during the byte-rank sort -----------------
        // Item = (bytes, provenance). Provenance: source ordinal + code for
        // remap fill; u32::MAX marks a plain-hashed value.
        let mut items: Vec<(&[u8], u32, u32)> = Vec::new();
        for (si, (s, lane)) in side.sources.iter().zip(used.iter()).enumerate() {
            for (code, &u) in lane.iter().enumerate() {
                if u {
                    items.push((s.entry(code as u32), si as u32, code as u32));
                }
            }
        }
        for v in plain.iter() {
            items.push((*v, u32::MAX, 0));
        }
        items.sort_unstable_by(|a, b| a.0.cmp(b.0));

        // ---- (4) exact NDV at entry grain, before any value walk ---------
        let mut ndv = 0u64;
        {
            let mut prev: Option<&[u8]> = None;
            for &(bytes, _, _) in &items {
                if prev != Some(bytes) {
                    ndv += 1;
                    prev = Some(bytes);
                }
            }
        }
        if ndv > p.ndv_cap {
            return Ok(None); // NdvAboveCap — exactly the rebuild's demotion
        }

        // ---- (5) rank assignment + remap tables --------------------------
        let mut entries: Vec<Vec<u8>> = Vec::with_capacity(ndv as usize);
        let mut remap: Vec<Vec<u32>> = side
            .sources
            .iter()
            .map(|s| vec![u32::MAX; s.entry_count() as usize])
            .collect();
        let mut plain_ranks: std::collections::BTreeMap<&[u8], u32> =
            std::collections::BTreeMap::new();
        for &(bytes, si, code) in &items {
            let is_new = entries.last().map(|e| e.as_slice()) != Some(bytes);
            if is_new {
                entries.push(bytes.to_vec());
            }
            let rank = (entries.len() - 1) as u32;
            if si == u32::MAX {
                plain_ranks.insert(bytes, rank);
            } else {
                remap[si as usize][code as usize] = rank;
            }
        }
        let built = BuiltDict::from_sorted_entries(entries, p.sem)?;

        // ---- translate: code streams by indexed lookup -------------------
        // Row-dense walk mirroring the rebuild's granule loop; per-row work
        // is one remap read (dict rows) or one map lookup (plain tail).
        // The per-row entry==staged-payload equality is NOT re-checked here
        // (it would be the row-grain walk this arm deletes) — the decode
        // kernels staged the flat images FROM these same source entries, so
        // inequality is unreachable without a decoder defect, and the twin
        // gates (round-trip verify + serial-rebuild dirsha) stand behind.
        let mut codes: Vec<Vec<u64>> = Vec::with_capacity(input.granules.len());
        let mut code_bytes = 0usize;
        let mut max_width = 0u8;
        let byte_align = dict_byte_widths_on();
        let mut row = 0u64;
        for gi in input.granules.iter() {
            let mut gc: Vec<u64> = Vec::with_capacity(gi.rows as usize);
            for r in 0..gi.rows {
                let code = if gi.valid(r) {
                    let slot = side.rows[row as usize];
                    let new = if slot == crate::ingest::DICT_ROW_PLAIN {
                        let payload = col
                            .varlena_payload(row)?
                            .ok_or_else(|| contract("valid row without payload"))?;
                        *plain_ranks
                            .get(payload)
                            .ok_or_else(|| contract("plain value missing from merge"))?
                    } else {
                        let (src, c) = ((slot >> 32) as usize, slot as u32 as usize);
                        remap[src][c]
                    };
                    if new == u32::MAX {
                        return Err(contract("unreferenced code reached translate"));
                    }
                    new as u64
                } else {
                    0
                };
                gc.push(code);
                row += 1;
            }
            let ci = EncodeInput {
                class: stats.class,
                rows: gi.rows,
                datums: &gc,
                validity: gi.validity,
            };
            let (bytes, w) = pgrc2_codec::dictcodes::granule_block_facts(&ci, byte_align);
            code_bytes += bytes;
            max_width = max_width.max(w);
            codes.push(gc);
        }

        // ---- emit + verify + decide: the rebuild path verbatim -----------
        let images = built.emit_sections(fsst_charlen_form())?;
        built.verify_sections(&images)?;
        let dict_bytes = images.index_section.len() + images.payload_section.len();
        // FORMAT-LAYOUT lever A: payload-ratio demotion (see
        // `dict_payload_cap_pct`). Exact bytes both sides — the payload
        // SECTION image vs the same verbatim baseline the ≥10% law prices.
        // Demote = fall through to the FSST arm / verbatim catch-all, the
        // standard dict-loser path; answers are encoding-invariant.
        if let Some(pct) = dict_payload_cap_pct() {
            if (images.payload_section.len() as u64) * 100 >= pct * baseline {
                return Ok(None);
            }
        }
        let arm = ce::DictArm {
            rows: stats.rows,
            ndv,
            ndv_cap: p.ndv_cap,
            dict_bytes,
            code_stream_bytes: code_bytes,
            baseline_bytes: baseline as usize,
        };
        let ce::Election::Elected {
            candidate_bytes, ..
        } = ce::elect_text_dict(arm)
        else {
            return Ok(None);
        };
        let extra_flags = if p.exec_ok && !col.has_null() {
            STREAMF_DICT_EXEC
        } else {
            0
        };
        Ok(Some(FullElection {
            encoding: EncodingId::DictCodes.as_u16(),
            width: max_width,
            aux32: 0,
            extra_flags,
            offer_wrapper: true,
            plan: ElectPlan::Dict(DictPlan {
                entry_count: images.entry_count,
                images,
                max_width,
                codes,
                // Count-free by construction (the merge never walks rows);
                // the meta accumulator serves this part's distribution —
                // the same sidecar bytes (pure function of the data), so
                // the D2 inherit arm's bank identity with the rebuild is
                // untouched.
                dist_feed: None,
                byte_align,
            }),
            witness: ElectionWitness {
                attno: input.attno,
                path_ord: input.path_ord,
                encoding: EncodingId::DictCodes.as_u16(),
                baseline_len: baseline,
                chosen_len: candidate_bytes as u64,
            },
            note: None,
        }))
    }

    /// The SB-4 FSST arm — offered ONLY after the dict arm demoted (the
    /// dict-loser text families are its chartered target class). Utf8
    /// text semantics only (bytea and other byte-semantics classes keep
    /// the verbatim+wrapper catch-all law); oversize values demote (they
    /// live in the overflow stream, which FSST frames do not carry).
    ///
    /// Deterministic and data-priced: the per-(column,part) symbol table
    /// is built from a fixed-stride sample of the column's payloads, and
    /// the candidate is priced by trial-compressing EVERY value — exact
    /// bytes into the standing ≥10% law
    /// (`pgrc2_codec::election::elect_text_fsst`). `None` = demoted.
    fn fsst_arm(
        &self,
        input: &FullElectInput<'_>,
        p: DictPolicy,
        baseline: u64,
    ) -> WriteResult<Option<FullElection>> {
        use pgrc2_codec::election as ce;
        let stats = input.stats;
        if p.sem != TextSemantics::Utf8Chars {
            return Ok(None);
        }
        if stats.oversize_values > 0 || stats.nonnull == 0 {
            return Ok(None);
        }
        // Data-priced arm: no value data ⇒ demote (issue-#463 contract).
        let Some(col) = input.col else {
            return Ok(None);
        };
        // Fixed-stride sample (pure function of the column data — the
        // byte-identical-parts law).
        let stride = (col.rows() / FSST_SAMPLE_VALUES).max(1);
        let mut sample: Vec<&[u8]> = Vec::new();
        let mut row = 0u64;
        while row < col.rows() {
            if let Some(payload) = col.varlena_payload(row)? {
                sample.push(payload);
            }
            row += stride;
        }
        let table = pgrc2_codec::fsst::FsstSymbolTable::build(&sample);
        // D3 sampled pricing: trial-compress ONLY deterministic contiguous
        // 64-row runs (~1%) and ratio-scale the code bytes over the exact
        // whole-part `value_bytes` stat. The SYMBOL TABLE build above is
        // untouched (same fixed-stride sample as the census arm), so an
        // election that goes FSST under sampling emits byte-identical
        // streams to a census FSST election — sampling can only move the
        // DECISION near the gate (the D3 regret class), never the bytes of
        // an agreed election. No carry rides the sampled arm (the seal's
        // encoder re-compresses); demoted streams skip the whole-part
        // trial the census pays — the walk win lives there.
        if self.sample.fsst {
            let unit_count = (col.rows() as usize).div_ceil(64);
            let ords = pgrc2_codec::election::sample_ords(unit_count);
            let mut scratch: Vec<u8> = Vec::new();
            let mut s_code = 0u64;
            let mut s_value = 0u64;
            for &u in &ords {
                let r0 = u as u64 * 64;
                let r1 = (r0 + 64).min(col.rows());
                for r in r0..r1 {
                    if let Some(payload) = col.varlena_payload(r)? {
                        s_value += payload.len() as u64;
                        scratch.clear();
                        table.compress_into(payload, &mut scratch);
                        s_code += scratch.len() as u64;
                    }
                }
            }
            let est_code: usize = if s_value == 0 {
                // Sample saw no bytes: claim no compression win (neutral,
                // deterministic; the gate demotes).
                stats.value_bytes as usize
            } else {
                ((s_code as u128) * (stats.value_bytes as u128) / (s_value as u128)) as usize
            };
            let shape = input.shape;
            let table_bytes = table.serialized_len() * shape.extent_count as usize;
            let framing_bytes = stats.rows as usize * 4
                + shape.frame_count as usize * 8
                + shape.extent_count as usize * 32;
            let arm = ce::FsstArm {
                rows: stats.rows,
                table_bytes,
                code_bytes: est_code,
                framing_bytes,
                value_bytes: stats.value_bytes as usize,
                baseline_bytes: baseline as usize,
            };
            let ce::Election::Elected {
                candidate_bytes, ..
            } = ce::elect_text_fsst(arm)
            else {
                return Ok(None);
            };
            return Ok(Some(FullElection {
                encoding: EncodingId::Fsst.as_u16(),
                width: 0,
                aux32: 0,
                extra_flags: 0,
                offer_wrapper: true,
                plan: ElectPlan::Encoder(Box::new(FsstFactory { table, carry: None })),
                witness: ElectionWitness {
                    attno: input.attno,
                    path_ord: input.path_ord,
                    encoding: EncodingId::Fsst.as_u16(),
                    baseline_len: baseline,
                    // Sample-estimated (stats-only witness; the on-disk
                    // sections carry exact sizes regardless).
                    chosen_len: candidate_bytes as u64,
                },
                note: None,
            }));
        }
        // Exact pricing: trial-compress every non-null value. The bare
        // code total feeds the code-grain incompressible guard (the win
        // must be COMPRESSION — never the frame layout's smaller
        // per-value overhead; `elect_text_fsst` doc).
        //
        // SEAL-FUSION (walk #7f vs #9): the trial compression WRITES its
        // bytes once (row-dense, per-row ends — the encoder's own layout
        // law: null rows and empty strings are zero-length) and the
        // buffers are carried to encode, which becomes a slot-table +
        // memcpy emit instead of a second greedy-match walk over every
        // value. Memory price documented on `fsst::FsstCarry` (the
        // keep-vs-recompress ruling: KEEP — ≈ the compressed payload +
        // 4·(rows+1) ends, freed at stream end; a demoted stream drops
        // the buffers at return, paying only the transient allocation).
        let mut carry_bytes: Vec<u8> = Vec::new();
        let mut carry_ends: Vec<u32> = Vec::with_capacity(col.rows() as usize + 1);
        carry_ends.push(0);
        for r in 0..col.rows() {
            if let Some(payload) = col.varlena_payload(r)? {
                table.compress_into(payload, &mut carry_bytes);
            }
            carry_ends.push(carry_bytes.len() as u32);
        }
        let code_bytes = carry_bytes.len();
        // Framing bytes mirror `verbatim_baseline_len`'s varlena shape:
        // per-extent section header + table copy (OD-5) + per-row slot
        // entry. FSST is granule-framed (one frame per granule), so the
        // per-frame term (`frame_count × 8` for slot-end + frame-table
        // entries) conservatively OVERCOUNTS the per-granule actuals at
        // every SB-10 grain (granule_count ≤ frame_count) — the gate can
        // only lose margin, never gain it.
        let shape = input.shape;
        let table_bytes = table.serialized_len() * shape.extent_count as usize;
        let framing_bytes = stats.rows as usize * 4
            + shape.frame_count as usize * 8
            + shape.extent_count as usize * 32;
        let arm = ce::FsstArm {
            rows: stats.rows,
            table_bytes,
            code_bytes,
            framing_bytes,
            // Oversize demoted above, so value_bytes is exactly the
            // trial-compressed set's raw payload bytes.
            value_bytes: stats.value_bytes as usize,
            baseline_bytes: baseline as usize,
        };
        let ce::Election::Elected {
            candidate_bytes, ..
        } = ce::elect_text_fsst(arm)
        else {
            return Ok(None);
        };
        Ok(Some(FullElection {
            encoding: EncodingId::Fsst.as_u16(),
            width: 0,
            aux32: 0,
            extra_flags: 0,
            offer_wrapper: true, // FSST-under-zstd: two layers, legal (SB-2)
            plan: ElectPlan::Encoder(Box::new(FsstFactory {
                table,
                carry: Some(pgrc2_codec::fsst::FsstCarry {
                    bytes: std::sync::Arc::new(carry_bytes),
                    ends: std::sync::Arc::new(carry_ends),
                }),
            })),
            witness: ElectionWitness {
                attno: input.attno,
                path_ord: input.path_ord,
                encoding: EncodingId::Fsst.as_u16(),
                baseline_len: baseline,
                chosen_len: candidate_bytes as u64,
            },
            note: None,
        }))
    }
}

/// FSST sample budget: values sampled for the symbol-table build (a fixed
/// stride over the part — deterministic; the PRICING still walks every
/// value exactly).
const FSST_SAMPLE_VALUES: u64 = 4096;

/// FSST-UNLOCK: resolve a text column's CLAIMED semantics into the
/// verified per-part fact both text arms consume.
///
/// The law: `Utf8Chars` in a posture is a CLAIM (catalog/schema
/// knowledge — atttypid under a UTF-8 database, or the parquet UTF8/
/// STRING annotation); the election verifies it against this part's own
/// payload bytes before any char-length fact or FSST arm rides it —
/// verified, never trusted, UNCONDITIONALLY (there is no trust mode:
/// [`fsst_unlock`] gates where production CLAIMS are minted, never
/// whether a claim is checked). The verdict is a pure function of the
/// part's payloads (UTF-8 validity of bytes), so the byte-identical-
/// parts law holds by construction: serial == DOP-skew, parquet == TSV
/// twins, dict-page twin == plain twin (referenced-entries-only
/// validation — an UNREFERENCED side-channel entry never enters the
/// part and must never flip its bytes).
///
/// Degrade shape: any invalid payload ⇒ the whole stream runs BytesOnly
/// for this part (exactly the pre-unlock posture) and a typed
/// [`SealNote::Utf8ClaimDegraded`] rides the election into the census.
/// Ingest NEVER fails on invalid UTF-8 under a claim — the
/// byte-transparent staging law stands (ClickHouse String semantics).
///
/// Cost shape: `simdutf8::basic` per payload, short-circuit on first
/// invalid; parts covered by the D2 dict-page side channel validate at
/// DISTINCT grain (each referenced entry once + plain rows individually)
/// — the SEAL-SPEED-2 walk-avoidance win carries over.
fn resolve_text_policy(
    input: &FullElectInput<'_>,
    dp: DictPolicy,
) -> WriteResult<(DictPolicy, Option<SealNote>)> {
    if dp.sem != TextSemantics::Utf8Chars {
        return Ok((dp, None));
    }
    let bytes_only = DictPolicy {
        sem: TextSemantics::BytesOnly,
        ..dp
    };
    // Both text arms demote on oversize/empty regardless of semantics, so
    // the verdict is inert there — skip the walk, keep the old stamp, no
    // note (nothing degraded; nothing was reachable).
    if input.stats.oversize_values > 0 || input.stats.nonnull == 0 {
        return Ok((bytes_only, None));
    }
    // Data-priced resolution: no value data ⇒ nothing to verify and both
    // arms demote anyway (issue-#463 contract).
    let Some(col) = input.col else {
        return Ok((bytes_only, None));
    };
    let valid = if let Some(side) = col.dict_side() {
        utf8_valid_side(col, side)?
    } else {
        utf8_valid_col(col)?
    };
    if valid {
        Ok((dp, None))
    } else {
        Ok((
            bytes_only,
            Some(SealNote::Utf8ClaimDegraded {
                attno: input.attno,
                path_ord: input.path_ord,
            }),
        ))
    }
}

/// Whole-column UTF-8 validation (the rebuild-path arm): every non-null
/// payload, short-circuit on first invalid.
fn utf8_valid_col(col: &ColBuffer) -> WriteResult<bool> {
    for row in 0..col.rows() {
        if let Some(payload) = col.varlena_payload(row)? {
            if simdutf8::basic::from_utf8(payload).is_err() {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// Side-channel UTF-8 validation at distinct grain: mark the REFERENCED
/// entries (a coded row's payload IS its entry bytes), validate each such
/// entry once, and validate plain rows individually. Verdict-equal to
/// [`utf8_valid_col`] on the same part data by construction.
fn utf8_valid_side(col: &ColBuffer, side: &crate::ingest::DictSide) -> WriteResult<bool> {
    let contract = |detail: &'static str| crate::WriteError::Contract { detail };
    let mut used: Vec<Vec<bool>> = side
        .sources
        .iter()
        .map(|s| vec![false; s.entry_count() as usize])
        .collect();
    for row in 0..col.rows() {
        if !col.valid_at(row) {
            continue;
        }
        let slot = side.rows[row as usize];
        if slot == crate::ingest::DICT_ROW_PLAIN {
            let payload = col
                .varlena_payload(row)?
                .ok_or_else(|| contract("valid row without payload"))?;
            if simdutf8::basic::from_utf8(payload).is_err() {
                return Ok(false);
            }
        } else {
            let (src, code) = ((slot >> 32) as usize, slot as u32 as usize);
            let lane = used
                .get_mut(src)
                .ok_or_else(|| contract("dict side source out of range"))?;
            *lane
                .get_mut(code)
                .ok_or_else(|| contract("dict side code out of range"))? = true;
        }
    }
    for (s, lane) in side.sources.iter().zip(used.iter()) {
        for (code, &u) in lane.iter().enumerate() {
            if u && simdutf8::basic::from_utf8(s.entry(code as u32)).is_err() {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

impl CandidateSource for CodecCandidates {
    fn propose(&self, _input: &FullElectInput<'_>) -> Vec<Candidate> {
        Vec::new()
    }
    fn elect_full(&self, input: &FullElectInput<'_>) -> Option<WriteResult<FullElection>> {
        Some(self.run(input))
    }
}
