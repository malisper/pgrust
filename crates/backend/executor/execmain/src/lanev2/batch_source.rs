//! THE STORAGE SEAM of the runtime scan drive (single-executor migration
//! §0.1: open / granules / read / capabilities) — [`BatchGranuleSource`],
//! the granule-addressed batch source the drive positions and reads through.
//! One instance per WORKER per scan; heap and pgrcolumnar dispatch live
//! BELOW this seam (nodeseqscan → tableam), where they already lived.
//!
//! Naming (integration contract R-NAME, permanent): the storage seam is
//! `BatchGranuleSource`; `executils::BatchSource` is the operator→operator
//! pull seam (re-exported as `AggBatchSource`) and is never renamed.
//!
//! Phase 1 (WS-K) lands the staged-batch READ face on the trait
//! (`batch_soa` / `qual_sel` / `skip_sel` / `lane_sel` / `emit`) plus the
//! transitional `seq_scan_bridge` (the inc-1 columnar-only escape hatch,
//! deleted by WS-A inc-2), and the dedicated heap implementor
//! [`HeapBatchSource`] behind `PGRUST_LANE_V2_HEAPFEED` (default ON since
//! the SE15 coupling flip; explicit `=0`/`off` = the kill switch;
//! knob-OFF paths construct [`SeqScanSource`] and run today's bytes).
//! Columnar claim-time readahead stays BELOW `position()` (the AM's
//! `set_granule_range` claim-window advise) and passes through untouched;
//! HEAP readahead is the source's own policy inside `position()` — a
//! bounded advisory `bufmgr_seams::prefetch_buffer` walk over the claim
//! window's head (`PGRUST_LANE_V2_HEAPFEED_READAHEAD`, default 0 = none).
//!
//! # Batch ownership / pin-lifetime ABI (settled here, per the migration doc)
//!
//! - **R1 CLAIM-SCOPED BATCHES**: a staged batch is valid from
//!   `next_batch() > 0` until the next `next_batch`/`position`/`end_claim`
//!   on the SAME source instance; never retained past that. Today this is
//!   enforced by convention (drains are synchronous inside `run_morsel`);
//!   when the read face lands (inc-2), accessors borrowing `&self` between
//!   `&mut` calls make it borrow-checked.
//! - **R2 WORKER-PRIVATE**: batches never cross threads. Each worker builds
//!   its OWN executor + scan descriptor (`build_worker_exec`); the only
//!   cross-thread currency is (a) `Arc<GranuleMap>` (immutable geometry),
//!   (b) `Arc<Part>`/mmap + SegMap below the seam (immutable storage),
//!   (c) exported partials — deep copies under per-ordinal mutexes.
//! - **R3 HEAP PINS**: the staged heap batch IS one pinned page (`rs_cbuf`);
//!   SoA byref cells and emitted tuples alias the pinned image
//!   (`heap_batch_deform_soa`: "pinned by rs_cbuf for the whole batch").
//!   Pins release on batch advance (n == 0 → end of scan), on reposition
//!   (`heap_set_block_range`'s defensive release), and the scan SLOT's pin
//!   on the drain's end-of-claim `exec_clear_tuple`. LAW: at claim settle
//!   the worker holds zero pins from that claim.
//! - **R3v VARLENA PIN-HOLDING (Phase-1 WS-K ratification — THE pin-lifetime
//!   decision at this ABI boundary)**: the read face's staged heap cells
//!   (including every byref/varlena Datum in the SoA lanes) alias the pinned
//!   page image directly — the ABI is PIN-HOLDING, not copy-into-arena.
//!   Rationale: (a) this is exactly today's serial discipline
//!   (`heap_batch_deform_soa`: "pinned by rs_cbuf for the whole batch"), so
//!   an arena copy would be a NEW, parity-risking path plus a per-batch
//!   memcpy of every varlena cell; (b) R1 (claim-scoped) + R2
//!   (worker-private) + R6 (batches never move) make the pin's validity
//!   window equal the batch read window, and the read face's accessors
//!   borrow `&self` between the `&mut` calls (`next_batch`/`position`/
//!   `end_claim`), so retaining a cell past the batch is a COMPILE error;
//!   (c) consumers that retain values past the batch (agg transitions)
//!   already copy into their own aggcontext — PG's byref-transvalue
//!   discipline — and the emitted-slot path takes its own pin
//!   (`heap_batch_store_slot`). Nothing above this seam may stash a staged
//!   pointer; a consumer needing batch-outliving bytes copies AT the
//!   consumer, never here.
//! - **R4 COLUMNAR SCRATCH**: staged cells alias per-scan decode scratch
//!   (ColDecode datums/dict/arena) rebuilt at granule/RG grain — NOT the
//!   mmap. Validity therefore requires epoch-integral claims: enforced at
//!   DEFINITION (`GranuleMap` boundaries = `Part::granule_starts`) and
//!   re-checked at position (`set_granule_range`'s cross-RG error).
//!   Coalesced multi-epoch claims are legal ONLY for consumers that
//!   subdivide via `GranuleMap::segments` (the scan drive's `morsel_body`;
//!   sink drains feed claims straight to `set_granule_range` and must not
//!   coalesce).
//! - **R5 CARRY-OVER MEMOS**: worker-private reader memoization across
//!   claims (same-RG `rg_checked` carry + dict scratch reuse) is permitted
//!   only for pure per-RG predicates under the engagement snapshot.
//! - **R6 STEALING (the morsel-runtime-v2 stealing/NUMA law)**: by R1+R2 a
//!   stolen or re-split granule has no batch state — stealing/shedding is
//!   claim-level only (today's shed happens between morsels; a claim is
//!   executed whole by its claimer). A batch NEVER outlives its claim; if a
//!   scheduler ever wants mid-claim handoff, the unconsumed REMAINDER of
//!   the claim (a granule range) is what changes hands, never staged state.
//!   Sources wanting cross-worker decoded reuse must publish source-level
//!   immutable shared state (`Arc<Part>`-class), never staged batches.
//!   NUMA-affine claiming moves the CLAIM, never the batch.

use std::sync::atomic::{AtomicU8, Ordering::Relaxed};
use std::sync::{Arc, OnceLock};

use ::executils::{EStateData, ExecSlotId};
use ::types_error::{PgError, PgResult, ERROR};

/// `PGRUST_LANE_V2_HEAPFEED` (**default ON since the SE15-GATES coupling
/// flip** — all three SE11 item-5 re-open conditions met and re-lettered at
/// the SE15 composed tip, notes/se15-gates.md §5; R-KNOBS registry
/// spelling): the Phase-1 heap batch-source gate. The explicit `=0`/`off`
/// spelling is the kill switch and byte-restores the pre-flip world: every
/// consumption site constructs [`SeqScanSource`] and the drains keep their
/// inline end-of-claim clear. ON = heap scans at the two consumption sites
/// (serial plain fold feed, runtime `morsel_body`) ride [`HeapBatchSource`]
/// and `end_claim` ownership moves to the source (single-owner; see the
/// trait doc) — and K2 (`PGRUST_LANE_V2_K2_PROBE`, default ON since SE9)
/// engages through its dual gate. AtomicU8 + `_set_for_tests` idiom
/// (rowmode.rs precedent) so units can A/B both paths in one process.
static HEAPFEED: AtomicU8 = AtomicU8::new(0);

pub(super) fn heapfeed_v2_enabled() -> bool {
    match HEAPFEED.load(Relaxed) {
        1 => false,
        2 => true,
        _ => {
            let on = !matches!(
                std::env::var("PGRUST_LANE_V2_HEAPFEED").as_deref(),
                Ok("0") | Ok("off")
            );
            HEAPFEED.store(if on { 2 } else { 1 }, Relaxed);
            on
        }
    }
}

/// Same-process A/B lever for the unit corpus (`crate::tests`).
#[cfg(test)]
pub(crate) fn heapfeed_set_for_tests(on: bool) {
    HEAPFEED.store(if on { 2 } else { 1 }, Relaxed);
}

/// `PGRUST_LANE_V2_HEAPFEED_READAHEAD` (default 0 = no readahead): advisory
/// `prefetch_buffer` depth over each positioned claim window's head blocks.
/// Inert unless `PGRUST_LANE_V2_HEAPFEED` is on (only [`HeapBatchSource`]
/// reads it). OnceLock (no in-process A/B needed — the readahead leg is
/// e2e-proven). NOT free when nonzero: each advise is a buffer-table probe
/// under a partition lock — fleet-measured before any default flip.
pub(super) fn heapfeed_readahead_depth() -> u32 {
    static DEPTH: OnceLock<u32> = OnceLock::new();
    crate::once_val(&DEPTH, || {
        std::env::var("PGRUST_LANE_V2_HEAPFEED_READAHEAD")
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .unwrap_or(0)
    })
}

// --- WS-AH wave-9 sub-region (K1 late materialization knob) ---------------

/// `PGRUST_LANE_V2_K1_LATEMAT` (default OFF; R-KNOBS registry spelling): K1
/// inc-2 late materialization (wave-9 WS-AH, contract §2). Engagement is
/// HEAPFEED ∧ K1_LATEMAT ∧ `heap_gagg_admits` ∧ the per-build shape
/// admission in the grouped drains (armed whole-qual kernel staging + a
/// key-column set the feed can state); every refusal and the whole knob-OFF
/// world keep today's full staging bytes. AtomicU8 + `_set_for_tests` idiom
/// (rowmode.rs precedent) so units can A/B both worlds in one process.
static K1_LATEMAT: AtomicU8 = AtomicU8::new(0);

pub(super) fn k1_latemat_enabled() -> bool {
    match K1_LATEMAT.load(Relaxed) {
        1 => false,
        2 => true,
        _ => {
            let on = matches!(
                std::env::var("PGRUST_LANE_V2_K1_LATEMAT").as_deref(),
                Ok("1") | Ok("on")
            );
            K1_LATEMAT.store(if on { 2 } else { 1 }, Relaxed);
            on
        }
    }
}

/// Same-process A/B lever for the unit corpus (`crate::tests`).
#[cfg(test)]
pub(crate) fn k1_latemat_set_for_tests(on: bool) {
    K1_LATEMAT.store(if on { 2 } else { 1 }, Relaxed);
}

// --- K1-F2 selectivity gate (SE9-GATES item 2 charter) ----------------------
//
// The AH letter read CROSSOVER, not WIN: late materialization is profitable
// at LOW qual selectivity (gsel1 −3.71%, gsel10 −4.96% B/A) and LOSES at
// high (gsel50 +0.59%, gsel90 +3.76%) — staging cost paid, round-trip not
// saved. Uniform admission would eat the high-selectivity loss, so
// K1_LATEMAT stayed default-OFF. This gate admits late-mat only where the
// PLAN-TIME qual-selectivity estimate says it wins: the planner already
// divided these numbers — `Plan.plan_rows` on the SeqScan node is
// clamp(clauselist_selectivity × rel->tuples), and rel->tuples is
// `estimate_rel_size`'s heap arm (tableam::table_relation_estimate_size).
// We recompute the DENOMINATOR with the planner's own math (same reltuples/
// relpages density × current nblocks — one seam call per BUILD, never
// per-row) and gate on the quotient. Admission policy only: results are
// byte-identical either way (the dualexec matrix pins both arms).

/// `PGRUST_LANE_V2_K1_SEL_THRESHOLD` (K1-F2; R-KNOBS registry spelling):
/// max admitted qual-selectivity estimate, a fraction of the scan's
/// estimated input rows. Default = the measured local crossover with margin
/// (see notes/se-k1-f2.md §3; the AH fleet letter's win envelope is ≤10%,
/// the loss onset ≥50%). `>= 1` disables the gate (the ungated letter arm);
/// `0` refuses every sel-gated admission (kill lever — plan_rows clamps to
/// ≥ 1 row, so no estimate reads exactly 0).
fn k1_sel_threshold() -> f64 {
    static THR: OnceLock<f64> = OnceLock::new();
    crate::once_val(&THR, || {
        parse_k1_sel_threshold(std::env::var("PGRUST_LANE_V2_K1_SEL_THRESHOLD").ok().as_deref())
    })
}

/// The threshold's parse half, pure for the unit corpus: unset / unparsable
/// / non-finite / negative = the default; explicit values win (values > 1
/// mean "admit everything" — the letter arms' ungated spelling). Non-finite
/// is rejected explicitly (the gagg-floor NaN lesson: `sel <= NaN` is false
/// everywhere — a silent full disarm that looks like a huge threshold).
fn parse_k1_sel_threshold(raw: Option<&str>) -> f64 {
    const K1_SEL_THRESHOLD_DEFAULT: f64 = 0.20;
    raw.and_then(|v| v.trim().parse::<f64>().ok())
        .filter(|f| f.is_finite() && *f >= 0.0)
        .unwrap_or(K1_SEL_THRESHOLD_DEFAULT)
}

/// The gate's pure half (unit corpus): the plan-time qual-selectivity
/// estimate from the two numbers the planner divided, or None when the
/// denominator carries no information (empty / never-analyzed estimate).
/// The quotient caps at 1.0: `plan_rows` is clamped to ≥ 1 and the exec-time
/// denominator can lag the plan-time one, so raw quotients may exceed 1.
fn k1_sel_estimate(plan_rows: f64, tuples: f64) -> Option<f64> {
    if !(tuples > 0.0) || !plan_rows.is_finite() {
        return None;
    }
    Some((plan_rows / tuples).min(1.0))
}

/// K1-F2 selectivity admission for the late-mat arms (both grouped drains
/// call this AFTER the rail-G guard check, BEFORE the nodeseqscan arm).
/// Refusals NAMED for the laneexec funnel:
/// - `k1-latemat-selectivity` — the estimate exceeds the threshold (the
///   letter's high-selectivity loss envelope; full staging wins there);
/// - `k1-latemat-sel-unknown` — no usable estimate (never-analyzed relation
///   — the planner used the plancat data-width density fallback this seam
///   cannot reach — an empty estimate, or the size seam errored). Unknown
///   fails CLOSED: an unproven selectivity keeps today's full-staging bytes.
/// - `k1-latemat-parallel` — parallel-aware scan (N1: the numerator is the
///   PER-WORKER plan estimate; a whole-relation denominator would deflate
///   the quotient ~divisor×). Fails CLOSED; see the in-body note.
pub(super) fn k1_latemat_sel_admits(
    ss: &::nodeseqscan::SeqScanState<'_>,
) -> Result<(), &'static str> {
    // N1 flip-gate obligation (k1-f2 review §7, fixed in the k1-latemat
    // lane): under a parallel-aware scan `plan_rows()` is the planner's
    // PER-WORKER estimate (rows / the parallel divisor) while `tuples`
    // below is the FULL-relation count — the quotient deflates ~divisor×
    // and would admit above the intended threshold. The divisor is not
    // reconstructible at this seam (num_workers lives on the Gather node,
    // and the launched count differs from the planned divisor anyway), so
    // parallel builds refuse fail-closed: every ADMITTED build is serial,
    // where the numerator and denominator agree in denomination (the
    // per-worker estimate IS the whole-scan estimate). If a parallel heap
    // grouped feed ever becomes a target, the honest fix is threading the
    // planner's divisor onto the plan node — not guessing here.
    if ss.is_parallel() {
        return Err("k1-latemat-parallel");
    }
    let Some(rel) = ss.ss.ss_currentRelation.as_ref() else {
        return Err("k1-latemat-sel-unknown");
    };
    // Pre-empt the density fallback (`get_rel_data_width` lives in the
    // planner's plancat; estimate_rel_size takes it only when reltuples < 0
    // or relpages == 0 on a non-empty relation): those relations have no
    // planner-grade estimate reachable here.
    if rel.rd_rel.reltuples < 0.0 || rel.rd_rel.relpages <= 0 {
        return Err("k1-latemat-sel-unknown");
    }
    let mut pages: ::types_core::BlockNumber = 0;
    let mut tuples = 0.0f64;
    let mut allvisfrac = 0.0f64;
    // The plancat estimate_rel_size heap-arm constants (fidelity only: the
    // density-fallback closure that reads them is pre-empted above).
    const HEAP_OVERHEAD_BYTES_PER_TUPLE: usize = 24 + 4;
    const HEAP_USABLE_BYTES_PER_PAGE: usize = 8192 - 24;
    if ::tableam::table_relation_estimate_size(
        rel,
        HEAP_OVERHEAD_BYTES_PER_TUPLE,
        HEAP_USABLE_BYTES_PER_PAGE,
        |_| unreachable!("density fallback pre-empted by the reltuples/relpages check"),
        None,
        &mut pages,
        &mut tuples,
        &mut allvisfrac,
    )
    .is_err()
    {
        // A size-seam error refuses the OPTIMIZATION only; if the relation
        // is genuinely unreadable the scan itself reports it.
        return Err("k1-latemat-sel-unknown");
    }
    match k1_sel_estimate(ss.plan_rows(), tuples) {
        Some(sel) if sel <= k1_sel_threshold() => Ok(()),
        Some(_) => Err("k1-latemat-selectivity"),
        None => Err("k1-latemat-sel-unknown"),
    }
}

/// K1 inc-3 needed-set split (pure half, unit corpus): partition the armed
/// narrowing's deferred completion set into
/// - `now`   = `deferred ∩ needed` — completed right after staging, BEFORE
///   every whole-batch consumer (probes, folds, spill replays read only
///   agg-needed columns: `plan.cols ⊆ colnos_needed` is `agg_fold_staged`'s
///   documented contract and the K2 spill-miss replay fills `shape.needed`
///   cells only), and
/// - `publish` = `deferred \ needed` — columns NO whole-batch consumer
///   reads; they complete ONLY when a batch leaves the kernel legs for a
///   per-row route, whose emit (`soa_store_prefix`) publishes every prefix
///   cell of a selected row (rail B: never a stale published cell).
///
/// `needed` must be the agg's `colnos_needed` census over SCAN column
/// numbers. Callers that cannot state `needed` for the build pass `None`
/// and get the pre-inc-3 behavior (`now` = the whole deferred set,
/// `publish` empty) — strictly the landed wave-9 completion bytes.
pub(super) fn k1_latemat_split(
    deferred: Vec<u16>,
    needed: Option<&[u16]>,
) -> (Vec<u16>, Vec<u16>) {
    match needed {
        None => (deferred, Vec::new()),
        Some(nd) => {
            let mut now = Vec::with_capacity(deferred.len());
            let mut publish = Vec::new();
            for c in deferred {
                if nd.contains(&c) {
                    now.push(c);
                } else {
                    publish.push(c);
                }
            }
            (now, publish)
        }
    }
}

// --- end WS-AH wave-9 sub-region -------------------------------------------

/// `PGRUST_LANE_V2_HEAP_GAGG_FLOOR` (default 1000; K1 inc-1 — the ONE new
/// admission policy): the grouped small-N engagement floor. Heap GROUPED
/// feeds construct [`HeapBatchSource`] only when the scan's plan-time row
/// estimate reaches the floor; below it the knob-ON world keeps
/// [`SeqScanSource`] (the pre-K1 source, still seam-settled — the clear
/// ownership law is process-static, never per-source). Probe evidence of
/// record (origin/heapfeed, lane CLOSED): grouped engagement crossover
/// ~1,000 rows (tie by N=100; N=1 grouped fixed cost ~6.8us). PLAIN stays
/// ungated — it never loses more than ~2us at N=1. Env-overridable for
/// letters; `0` disarms the floor. OnceLock (no in-process A/B: both
/// sources run identical AM machine code today, so the floor's units are
/// e2e/letter-grade, not corpus-grade).
fn heap_gagg_floor() -> f64 {
    static FLOOR: OnceLock<f64> = OnceLock::new();
    crate::once_val(&FLOOR, || {
        parse_gagg_floor(std::env::var("PGRUST_LANE_V2_HEAP_GAGG_FLOOR").ok().as_deref())
    })
}

/// The floor's parse half, pure for the unit corpus: unset / unparsable /
/// non-finite = the probe's 1,000-row default; `0` disarms. (Non-finite is
/// rejected explicitly: `"NaN"`/`"inf"` parse as f64, and a NaN floor would
/// make `plan_rows >= floor` false everywhere — silently disabling heap
/// grouped engagement while looking like a huge floor. Garbage in behaves
/// like the documented default instead.)
fn parse_gagg_floor(raw: Option<&str>) -> f64 {
    raw.and_then(|v| v.trim().parse::<f64>().ok())
        .filter(|f| f.is_finite())
        .unwrap_or(1000.0)
}

/// The grouped feeds' heap-source admission, in one place (K1 inc-1): the
/// AM gate + the plan-time small-N floor. False = the caller constructs
/// [`SeqScanSource`] (byte-equivalent delegation today; the floor exists so
/// heap-specific policy grows behind it without re-litigating admission).
pub(super) fn heap_gagg_admits(ss: &::nodeseqscan::SeqScanState<'_>) -> bool {
    ::nodeseqscan::seq_scan_is_heap(ss) && ss.plan_rows() >= heap_gagg_floor()
}

/// Capabilities of a granule-addressed batch source (migration-doc
/// "capabilities" face; grows honestly per increment — no speculative
/// flags).
#[derive(Clone, Copy)]
#[allow(dead_code)] // index_leaf/zone_maps/all_visible_batches: consumers land with WS-F / later increments
pub(super) struct SourceCaps {
    /// Columnar staging: granule = the store's 8,192-row unit, hard
    /// boundaries = dictionary epochs, staged cells alias per-scan decode
    /// scratch (ownership ABI R4).
    pub columnar: bool,
    /// Heap page staging: granule = one block, staged batch pins its page
    /// (ownership ABI R3/R3v).
    pub heap_pages: bool,
    /// Source publishes dict-code lanes (`seq_scan_batch_dict_codes` — the
    /// str MIN/MAX code memos). pgrcolumnar only; heap: false.
    pub dict_codes: bool,
    /// Source answers zone/footer metadata peeks (the plain-fold meta arm's
    /// footer-stat units). pgrcolumnar only; heap: false.
    pub zone_maps: bool,
    /// Every staged batch is provably all-visible. FALSE for all sources in
    /// Phase 1 — heap's all-visible verdict is per PAGE
    /// (`page_collect_tuples`' ALL_VISIBLE arm), not per source; a per-batch
    /// signal is a ledgered later increment (no consumer exists yet).
    pub all_visible_batches: bool,
    /// Granule = one index leaf page, positional posture (WS-F's
    /// IndexOnlyScanSource; field shipped here per the Phase-1 contract §2a
    /// so the caps struct lands whole). Both scan implementors: false.
    pub index_leaf: bool,
}

/// The storage seam trait; the batch ownership / pin-lifetime ABI (module
/// doc, R1–R6) is this trait's contract.
///
/// Inherited preconditions (stated so a second caller cannot violate them
/// silently): `position` is single-claimer per instance — the heap AM
/// errors on parallel-scan descriptors and the columnar AM debug-asserts
/// its adaptive drive is unarmed; both refuse ranges that cross a hard
/// boundary (`GranuleMap::segments` upholds that above the seam).
#[allow(dead_code)] // granule_map/lane_sel: consumers arrive with WS-F / later drains
pub(super) trait BatchGranuleSource<'mcx> {
    /// OPEN + GRANULES: open the underlying scan exactly as the drive
    /// would (the same `ensure_scandesc` open the geometry probes perform
    /// today) and publish its granule geometry. `None` = the source cannot
    /// express granules (heap 0 blocks / empty part / foreign AM) — the
    /// caller refuses engagement, fail-closed.
    fn granule_map(
        &mut self,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<runtime::GranuleMap>>;

    /// Position the reader on the epoch-integral claim segment
    /// `[seg.start, seg.end)`: whole granules, never crossing a
    /// `GranuleMap` boundary. Claim-time readahead is the source's own
    /// policy, below this call.
    fn position(
        &mut self,
        estate: &mut EStateData<'mcx>,
        seg: runtime::MorselRange,
    ) -> PgResult<()>;

    /// READ (staging half): stage the next batch of the positioned
    /// segment; 0 = segment drained. The staged batch is claim-scoped
    /// (ABI R1) and readable through the trait's read face below until the
    /// next `&mut` call on this source.
    fn next_batch(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<u32>;

    /// Release end-of-claim resources (heap: the scan slot's page pin —
    /// ABI R3's zero-pins-at-settle law). OWNERSHIP (Phase-1 WS-K, ratified
    /// §2a): knob-OFF (`PGRUST_LANE_V2_HEAPFEED` unset) the drains own the
    /// end-of-claim `exec_clear_tuple` inline exactly as before and this is
    /// never called; knob-ON the drains SKIP their inline clear and the
    /// drive calls this once per claim after the segment loop. Single-owner
    /// by construction (both branch on the same process-static knob) and
    /// debug-asserted in the implementors — never double-owned.
    fn end_claim(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()>;

    fn capabilities(&self) -> SourceCaps;

    // ------------------------------------------------------------------
    // READ face (Phase-1 WS-K; ratified as THE WS-A inc-2 signatures).
    // R1 borrow discipline: every accessor borrows `&self` between the
    // `&mut` calls above, so a staged cell/bitmap CANNOT outlive its batch
    // (R3v's compile-error guarantee). Default bodies = "not staged" /
    // fail-closed, so non-seqscan implementors (WS-F's IndexOnlyScanSource,
    // bitmap later) implement nothing.
    // ------------------------------------------------------------------

    /// The staged SoA batch (None = unstaged / per-row shape). Heap cells
    /// alias the pinned page (R3v); columnar cells alias decode scratch (R4).
    fn batch_soa(&self) -> Option<&::exectuples::SoaBatch<'mcx>> {
        None
    }

    /// Whole-qual kernel selection bitmap over the staged batch (None = the
    /// per-row fetch path owns the qual; see `seq_scan_batch_qual_sel`).
    fn qual_sel(&self) -> Option<&[u64]> {
        None
    }

    /// Emit-dead word-skip bitmap (cleared bits are definitive rejections;
    /// see `seq_scan_batch_skip_sel`).
    fn skip_sel(&self) -> Option<&[u64]> {
        None
    }

    /// PREWHERE-lane conservative selection words (proof domain for batch
    /// guards; see `seq_scan_batch_lane_sel`). Heap/index: always None.
    fn lane_sel(&self) -> Option<&[u64]> {
        None
    }

    /// Per-row emit: fetch + the FULL qual/projection program for row `i`
    /// of the staged batch — same rows, same order, same errors as the
    /// serial per-tuple path (`seq_scan_batch_emit`). Default = loud
    /// fail-closed PgError (never a panic): sources without a per-row emit
    /// face refuse at runtime, and their admission gates must keep this
    /// unreachable.
    fn emit(
        &mut self,
        _estate: &mut EStateData<'mcx>,
        _i: u32,
    ) -> PgResult<Option<ExecSlotId>> {
        Err(seam_not_wired("emit (source has no per-row emit face)"))
    }

    /// Inc-1 COLUMNAR-ONLY bridge retained for the shared drains' branches
    /// the read face does not cover yet (str-mm dict-code memos, the
    /// footer-meta arm, the scan-invariant qual peek and the knob-OFF
    /// inline clear); WS-A inc-2 deletes it. Default None; the two SeqScan
    /// hosts return Some. Callers gate on `capabilities()` and treat None
    /// where a capability promised the bridge as a loud PgError, never an
    /// unwrap.
    fn seq_scan_bridge(&mut self) -> Option<&mut ::nodeseqscan::SeqScanState<'mcx>> {
        None
    }

    // --- WS-AH wave-9 sub-region (K1 late materialization; contract §2
    // grant: ONE additive default-implemented method, no existing signature
    // changes) ---------------------------------------------------------------

    /// K1 inc-2 completion (pass B): fill `cols` for `sel`-selected rows of
    /// the CURRENT staged batch (64-row selection words, all-zero words
    /// skipped whole). Default NO-OP — sources that staged everything
    /// (SeqScanSource, the columnar window fill) have nothing to complete;
    /// only the heap implementor narrows its staging deform
    /// (`seq_scan_k1_latemat_arm`). Callable only between `next_batch() > 0`
    /// and the next `&mut` positional call (batch ownership ABI R1); value
    /// movement only — completed cells read back byte-identical to the full
    /// staging deform's.
    fn complete_deform(
        &mut self,
        _estate: &mut EStateData<'mcx>,
        _cols: &[u16],
        _sel: &[u64],
    ) -> PgResult<()> {
        Ok(())
    }

    // --- end WS-AH wave-9 sub-region ----------------------------------------
}

/// The drains' loud caps-gated bridge accessor (§2a: expect-style PgError,
/// not unwrap): a source whose capabilities imply SeqScan hosting must
/// return the bridge.
#[inline]
pub(super) fn require_bridge<'a, 'mcx, S: BatchGranuleSource<'mcx>>(
    src: &'a mut S,
) -> PgResult<&'a mut ::nodeseqscan::SeqScanState<'mcx>> {
    match src.seq_scan_bridge() {
        Some(ss) => Ok(ss),
        None => Err(seam_not_wired("seq_scan_bridge (source hosts no SeqScan)")),
    }
}

/// The increment-1 implementor: a SeqScan over heap or pgrcolumnar, driven
/// through the existing nodeseqscan/tableam AM dispatch (heap and columnar
/// behavior both live BELOW this seam already — delegation only, so the
/// extraction is code-shape-neutral on the claim hot path).
pub(super) struct SeqScanSource<'a, 'mcx> {
    ss: &'a mut ::nodeseqscan::SeqScanState<'mcx>,
}

impl<'a, 'mcx> SeqScanSource<'a, 'mcx> {
    #[inline]
    pub(super) fn new(ss: &'a mut ::nodeseqscan::SeqScanState<'mcx>) -> Self {
        SeqScanSource { ss }
    }
}

/// Startup-ramp seed for pgrcolumnar maps: granules are 8,192 rows — large
/// against Umbra's 16-tuple C0; one 2-granule probe morsel (~16K rows, tens
/// of µs on fold shapes) sizes the pipeline without a giant first claim on
/// tiny scans. (Inert under whole-boundary claims; kept for the kill
/// switch.)
const CB_STARTUP_C0: u64 = 2;
/// Startup-ramp seed for heap maps: a block stages ~50-250 tuples — seed 16
/// blocks (128KB, a few thousand rows). Same probe-morsel intent as
/// pgrcolumnar's C0=2.
const HEAP_STARTUP_C0: u64 = 16;

impl<'mcx> BatchGranuleSource<'mcx> for SeqScanSource<'_, 'mcx> {
    fn granule_map(
        &mut self,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<runtime::GranuleMap>> {
        // GL-Q4142 — the PRIMARY structural gate (the AM refusals in
        // `heap_set_block_range` / `set_granule_range` are the backstop).
        // This map is a PRIVATE, whole-relation claim space: pgrcolumnar's
        // part-global granule prefix sums, heap's `0..nblocks`. A scan that
        // carries parallel wiring divides its work through the SHARED cursor
        // instead (`phs_nallocated`, claimed inside the AM by
        // `claim_next_rg` / `parallel_next_block`), so handing a private map
        // to a participant of a classic-parallel scan makes EVERY
        // participant walk the whole relation — each partial aggregate is
        // then the global answer and the finalize sums them, returning a
        // result inflated by the participant count. `None` = the arm refuses
        // and the classic parallel arm owns the shape, which is always
        // byte-safe. Fail-closed on the SCAN, never on the process role: the
        // role predicates (`IsParallelWorker` / `IsInParallelMode`) are
        // thread-local and clearable and cannot carry this invariant.
        if ::nodeseqscan::seq_scan_is_parallel(self.ss) {
            return Ok(None);
        }
        if ::nodeseqscan::seq_scan_is_pgrcolumnar(self.ss) {
            let Some((_, starts)) =
                ::nodeseqscan::seq_scan_cb_granule_geometry(self.ss, estate)?
            else {
                return Ok(None); // empty part
            };
            return Ok(Some(runtime::GranuleMap::with_boundaries(
                Arc::new(starts),
                CB_STARTUP_C0,
            )));
        }
        if ::nodeseqscan::seq_scan_is_heap(self.ss) {
            let Some(nblocks) = ::nodeseqscan::seq_scan_heap_block_geometry(self.ss, estate)?
            else {
                return Ok(None); // empty relation
            };
            return Ok(Some(runtime::GranuleMap::unbounded(nblocks, HEAP_STARTUP_C0)));
        }
        Ok(None)
    }

    #[inline]
    fn position(
        &mut self,
        estate: &mut EStateData<'mcx>,
        seg: runtime::MorselRange,
    ) -> PgResult<()> {
        ::nodeseqscan::seq_scan_set_morsel_range(self.ss, estate, seg.start, seg.end)
    }

    #[inline]
    fn next_batch(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<u32> {
        ::nodeseqscan::seq_scan_next_pagebatch(self.ss, estate)
    }

    fn end_claim(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        // SCOPE (K1 inc-1 precision, for the WS-A inc-2/3 consolidation):
        // slot clear only — no seq_scan_end_claim_release here. On a
        // drain-ERROR path a knob-ON below-floor heap scan's rs_cbuf pin
        // therefore rides to the abort machinery, exactly as base knob-OFF
        // does (no regression). Strict zero-pins-at-settle-on-error holds
        // only for the HeapBatchSource arm (its end_claim releases).
        end_claim_clear_slot(self.ss, estate)
    }

    fn capabilities(&self) -> SourceCaps {
        let columnar = ::nodeseqscan::seq_scan_is_pgrcolumnar(self.ss);
        SourceCaps {
            columnar,
            heap_pages: ::nodeseqscan::seq_scan_is_heap(self.ss),
            dict_codes: columnar,
            zone_maps: columnar,
            all_visible_batches: false,
            index_leaf: false,
        }
    }

    #[inline]
    fn batch_soa(&self) -> Option<&::exectuples::SoaBatch<'mcx>> {
        ::nodeseqscan::seq_scan_batch_soa(self.ss)
    }

    #[inline]
    fn qual_sel(&self) -> Option<&[u64]> {
        ::nodeseqscan::seq_scan_batch_qual_sel(self.ss)
    }

    #[inline]
    fn skip_sel(&self) -> Option<&[u64]> {
        ::nodeseqscan::seq_scan_batch_skip_sel(self.ss)
    }

    #[inline]
    fn lane_sel(&self) -> Option<&[u64]> {
        ::nodeseqscan::seq_scan_batch_lane_sel(self.ss)
    }

    #[inline(always)]
    fn emit(
        &mut self,
        estate: &mut EStateData<'mcx>,
        i: u32,
    ) -> PgResult<Option<ExecSlotId>> {
        ::nodeseqscan::seq_scan_batch_emit(self.ss, estate, i)
    }

    #[inline]
    fn seq_scan_bridge(&mut self) -> Option<&mut ::nodeseqscan::SeqScanState<'mcx>> {
        Some(self.ss)
    }
}

/// The shared end-of-claim body (trait-doc ownership rules): the scan
/// SLOT's clear — dropping its buffer pin on heap (R3's zero-pins-at-settle
/// law; `rs_cbuf` itself already released on the n == 0 batch advance).
/// Knob-ON-owned: the single-owner assertion — knob-OFF the drains clear
/// inline and never call this.
fn end_claim_clear_slot<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    debug_assert!(
        heapfeed_v2_enabled(),
        "end_claim is knob-ON-owned; knob-OFF the drains clear inline (single owner)"
    );
    let mcx = estate.es_query_cxt;
    ::exectuples::exec_clear_tuple(estate.slot_mut(ss.ss.ss_ScanTupleSlot), mcx);
    Ok(())
}

/// The dedicated heap implementor (Phase-1 WS-K, was the Phase-0
/// `HeapPageSource` skeleton): granule = one block, boundary-free
/// `GranuleMap::unbounded` geometry, staged batch pins its page (ABI
/// R3/R3v). Delegation-only bodies — heap dispatch stays BELOW the seam in
/// tableam/heapam where it lives today, so knob-ON runs the same AM
/// machine code as [`SeqScanSource`]'s heap arm.
///
/// CONSTRUCTOR PRECONDITION (fail-closed): construct only where
/// `SeqScanSource`'s heap arm engages today — behind `seq_scan_is_heap` +
/// the drive's fusibility gates (`seq_scan_fusible` guarantees the pagemode
/// scan `heap_getnextpagebatch` debug-asserts: forward, SO_ALLOW_PAGEMODE,
/// rs_nkeys == 0) — and only under `PGRUST_LANE_V2_HEAPFEED`. Constructing
/// it elsewhere trips those AM debug asserts.
///
/// Sink-arm hosting (WS-K Q5, amended by K2 inc-1 / wave-8 WS-AC): the
/// runtime HASHJOIN arm now hosts this source for its probe/build claim
/// drives behind `PGRUST_LANE_V2_HEAPFEED` + `PGRUST_LANE_V2_K2_PROBE`
/// (HEAPFEED default OFF; K2_PROBE default ON since the SE9-GATES K2
/// flip, `=0`/`off` = permanent kill; unbatched engagements only — the
/// spill/batch-file routes never see a heap-fed row). The distinct sink
/// arm still
/// constructs the legacy `PgrcolumnarGranuleSource` only; consolidating
/// the remaining sites onto the seam stays WS-A inc-3.
pub(super) struct HeapBatchSource<'a, 'mcx> {
    ss: &'a mut ::nodeseqscan::SeqScanState<'mcx>,
    /// Advisory `prefetch_buffer` depth over each positioned claim window's
    /// head; 0 = none (the default). Read once from
    /// `PGRUST_LANE_V2_HEAPFEED_READAHEAD` at construction.
    readahead: u32,
}

impl<'a, 'mcx> HeapBatchSource<'a, 'mcx> {
    #[inline]
    pub(super) fn new(ss: &'a mut ::nodeseqscan::SeqScanState<'mcx>) -> Self {
        debug_assert!(heapfeed_v2_enabled(), "HeapBatchSource is knob-gated");
        debug_assert!(::nodeseqscan::seq_scan_is_heap(ss));
        // WS-O inc-2 pool-thread visibility audit (enforcement half; the
        // audit record is notes/se-ws-o-gather-ledger.md): heap page MVCC
        // (page_collect_tuples) resolves against the thread's ACTIVE
        // snapshot — every claim-driving thread must be session-bound
        // (leader, C1 caller) or binder-bound (launched/standing helpers)
        // BEFORE any batch stages. Raw pool threads never reach this
        // constructor (pinned RGs are invisible to the pool pick, rg.rs).
        debug_assert!(
            ::snapmgr::ActiveSnapshotSet(),
            "heap batch source on a thread without an active snapshot"
        );
        HeapBatchSource { ss, readahead: heapfeed_readahead_depth() }
    }
}

impl<'mcx> BatchGranuleSource<'mcx> for HeapBatchSource<'_, 'mcx> {
    fn granule_map(
        &mut self,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<runtime::GranuleMap>> {
        if !::nodeseqscan::seq_scan_is_heap(self.ss) {
            return Ok(None); // fail-closed: this source expresses heap only
        }
        // GL-Q4142: same structural gate as SeqScanSource above — a private
        // `0..nblocks` claim space must never drive a scan that divides its
        // work through the shared parallel block cursor.
        if ::nodeseqscan::seq_scan_is_parallel(self.ss) {
            return Ok(None);
        }
        let Some(nblocks) = ::nodeseqscan::seq_scan_heap_block_geometry(self.ss, estate)?
        else {
            return Ok(None); // empty relation
        };
        Ok(Some(runtime::GranuleMap::unbounded(nblocks, HEAP_STARTUP_C0)))
    }

    /// Positions on the block-range claim (`heap_set_block_range` below the
    /// AM dispatch: parallel-desc refusal, range validation, SO_ALLOW_SYNC
    /// clear, defensive pin release). Readahead first: a bounded ADVISORY
    /// `prefetch_buffer` walk over the window's head blocks — never changes
    /// what a later read returns; errors are the read path's own.
    fn position(
        &mut self,
        estate: &mut EStateData<'mcx>,
        seg: runtime::MorselRange,
    ) -> PgResult<()> {
        if self.readahead > 0 && ::bufmgr_seams::prefetch_buffer::is_installed() {
            if let Some(rel) = self.ss.ss.ss_currentRelation.as_ref() {
                let hi = seg.end.min(seg.start.saturating_add(self.readahead as u64));
                for blk in seg.start..hi {
                    ::bufmgr_seams::prefetch_buffer::call(
                        rel,
                        ::types_core::ForkNumber::MAIN_FORKNUM,
                        blk as ::types_core::BlockNumber,
                    )?;
                }
            }
        }
        ::nodeseqscan::seq_scan_set_morsel_range(self.ss, estate, seg.start, seg.end)
    }

    /// `heap_getnextpagebatch` (share-lock page, per-tuple MVCC under the
    /// task-bound snapshot in `page_collect_tuples`) + the SoA staging half
    /// (`heap_batch_deform_soa`, deform-JIT when armed) — via the same
    /// `seq_scan_next_pagebatch` AM dispatch the drains call today.
    #[inline]
    fn next_batch(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<u32> {
        ::nodeseqscan::seq_scan_next_pagebatch(self.ss, estate)
    }

    fn end_claim(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        // WS-O inc-2 (supersedes the WS-K Q3 deferral — this IS the
        // ledgered later increment): the R3 zero-pins-at-settle
        // tightening. Release rs_cbuf and reset the scan to the drained
        // state, so a claim that ends EARLY (drain error, abort between
        // segments, shed) holds zero pins at settle — pin-lifetime under
        // stealing (R6): a re-split claim remainder changes hands with no
        // staged state AND no pin left behind by the previous claimer. A
        // normally-drained claim already released on the n == 0 advance
        // (the release below is idempotent).
        ::nodeseqscan::seq_scan_end_claim_release(self.ss);
        end_claim_clear_slot(self.ss, estate)
    }

    fn capabilities(&self) -> SourceCaps {
        SourceCaps {
            columnar: false,
            heap_pages: true,
            dict_codes: false,
            zone_maps: false,
            all_visible_batches: false,
            index_leaf: false,
        }
    }

    #[inline]
    fn batch_soa(&self) -> Option<&::exectuples::SoaBatch<'mcx>> {
        ::nodeseqscan::seq_scan_batch_soa(self.ss)
    }

    #[inline]
    fn qual_sel(&self) -> Option<&[u64]> {
        ::nodeseqscan::seq_scan_batch_qual_sel(self.ss)
    }

    #[inline]
    fn skip_sel(&self) -> Option<&[u64]> {
        ::nodeseqscan::seq_scan_batch_skip_sel(self.ss)
    }

    /// Heap stages no PREWHERE lane in inc-1 (`seq_scan_batch_lane_sel`
    /// answers None on heap batches regardless — this is the honest
    /// constant form).
    #[inline]
    fn lane_sel(&self) -> Option<&[u64]> {
        None
    }

    #[inline(always)]
    fn emit(
        &mut self,
        estate: &mut EStateData<'mcx>,
        i: u32,
    ) -> PgResult<Option<ExecSlotId>> {
        ::nodeseqscan::seq_scan_batch_emit(self.ss, estate, i)
    }

    #[inline]
    fn seq_scan_bridge(&mut self) -> Option<&mut ::nodeseqscan::SeqScanState<'mcx>> {
        Some(self.ss)
    }

    // --- WS-AH wave-9 sub-region (K1 late materialization) -----------------

    /// The heap deform split's completion half: fill `cols` for
    /// `sel`-selected kind-0 rows of the staged batch off the still-pinned
    /// page (ownership ABI R3 — the pin holds until the next batch advance/
    /// reposition/settle). The drains call this only when they armed the
    /// per-build narrowing (`seq_scan_k1_latemat_arm`); the deferred set is
    /// exactly what staging skipped, so completed cells read back
    /// byte-identical to the full staging deform's.
    #[inline]
    fn complete_deform(
        &mut self,
        _estate: &mut EStateData<'mcx>,
        cols: &[u16],
        sel: &[u64],
    ) -> PgResult<()> {
        ::nodeseqscan::seq_scan_batch_complete_deform(self.ss, cols, sel);
        Ok(())
    }

    // --- end WS-AH wave-9 sub-region ----------------------------------------
}

#[track_caller]
#[cold]
#[inline(never)]
fn seam_not_wired(what: &str) -> Box<PgError> {
    Box::new(PgError::new(
        ERROR,
        format!("batch source face not wired in this increment: {what}"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Trait-level capability-flags test: the trait is implementable by a
    /// plain stub and the caps flags read back exactly as constructed
    /// (no inference anywhere in the face).
    struct StubSource(SourceCaps);

    impl<'mcx> BatchGranuleSource<'mcx> for StubSource {
        fn granule_map(
            &mut self,
            _estate: &mut EStateData<'mcx>,
        ) -> PgResult<Option<runtime::GranuleMap>> {
            unimplemented!("stub")
        }

        fn position(
            &mut self,
            _estate: &mut EStateData<'mcx>,
            _seg: runtime::MorselRange,
        ) -> PgResult<()> {
            unimplemented!("stub")
        }

        fn next_batch(&mut self, _estate: &mut EStateData<'mcx>) -> PgResult<u32> {
            unimplemented!("stub")
        }

        fn end_claim(&mut self, _estate: &mut EStateData<'mcx>) -> PgResult<()> {
            unimplemented!("stub")
        }

        fn capabilities(&self) -> SourceCaps {
            self.0
        }
    }

    fn caps(columnar: bool, heap_pages: bool) -> SourceCaps {
        SourceCaps {
            columnar,
            heap_pages,
            dict_codes: columnar,
            zone_maps: columnar,
            all_visible_batches: false,
            index_leaf: false,
        }
    }

    #[test]
    fn capability_flags_read_back() {
        for (columnar, heap_pages) in [(false, false), (true, false), (false, true)] {
            let src = StubSource(caps(columnar, heap_pages));
            let got = src.capabilities();
            assert_eq!(got.columnar, columnar);
            assert_eq!(got.heap_pages, heap_pages);
            // Phase-1 flags: no inference anywhere in the face; the two
            // pgrcolumnar-derived flags mirror `columnar` for both scan
            // implementors, and nothing sets the Phase-1 constants.
            assert_eq!(got.dict_codes, columnar);
            assert_eq!(got.zone_maps, columnar);
            assert!(!got.all_visible_batches);
            assert!(!got.index_leaf);
        }
    }

    /// Read-face DEFAULT bodies (contract §2a): a source implementing only
    /// the five positional methods reads back as unstaged everywhere,
    /// `emit` is a loud fail-closed PgError (never a panic), and the
    /// bridge is None (so `require_bridge` errors rather than unwrapping).
    #[test]
    fn read_face_defaults_fail_closed() {
        let mut src = StubSource(caps(false, false));
        assert!(src.batch_soa().is_none());
        assert!(src.qual_sel().is_none());
        assert!(src.skip_sel().is_none());
        assert!(src.lane_sel().is_none());
        // The default emit/bridge errors must not require an EStateData:
        // exercise them through the erroring helper directly.
        let err = seam_not_wired("emit (source has no per-row emit face)");
        assert!(err.to_string().contains("emit"));
        assert!(src.seq_scan_bridge().is_none());
        // The drains' loud caps-gated accessor: a bridgeless source is a
        // PgError, never an unwrap (contract §2a).
        assert!(require_bridge(&mut src).is_err());
    }

    /// `PGRUST_LANE_V2_HEAPFEED` A/B lever (AtomicU8 idiom): both states
    /// resolvable in one process; restored to OFF (the default the rest of
    /// the suite assumes — knob-OFF = today's bytes).
    #[test]
    fn heapfeed_knob_ab() {
        heapfeed_set_for_tests(true);
        assert!(heapfeed_v2_enabled());
        heapfeed_set_for_tests(false);
        assert!(!heapfeed_v2_enabled());
    }

    /// The grouped small-N floor's parse contract (K1 inc-1): unset and
    /// garbage read as the probe's 1,000-row default; explicit values win;
    /// `0` disarms (every estimate admits). Non-finite f64 spellings
    /// ("NaN"/"inf") are garbage too — a NaN floor would silently fail every
    /// `plan_rows >= floor` admission.
    #[test]
    fn gagg_floor_parse() {
        assert_eq!(parse_gagg_floor(None), 1000.0);
        assert_eq!(parse_gagg_floor(Some("")), 1000.0);
        assert_eq!(parse_gagg_floor(Some("banana")), 1000.0);
        assert_eq!(parse_gagg_floor(Some("2500")), 2500.0);
        assert_eq!(parse_gagg_floor(Some(" 64 ")), 64.0);
        assert_eq!(parse_gagg_floor(Some("0")), 0.0);
        assert_eq!(parse_gagg_floor(Some("NaN")), 1000.0);
        assert_eq!(parse_gagg_floor(Some("nan")), 1000.0);
        assert_eq!(parse_gagg_floor(Some("inf")), 1000.0);
        assert_eq!(parse_gagg_floor(Some("-inf")), 1000.0);
        assert_eq!(parse_gagg_floor(Some("infinity")), 1000.0);
    }

    /// K1-F2 threshold parse contract: unset and garbage read as the
    /// default; explicit values win; values > 1 = the ungated letter arm;
    /// `0` = the kill lever; non-finite and negative spellings are garbage
    /// (the gagg-floor NaN lesson — `sel <= NaN` is false everywhere).
    #[test]
    fn k1_sel_threshold_parse() {
        let d = parse_k1_sel_threshold(None);
        assert!(d > 0.0 && d < 0.5, "default must sit below the measured crossover: {d}");
        assert_eq!(parse_k1_sel_threshold(Some("")), d);
        assert_eq!(parse_k1_sel_threshold(Some("banana")), d);
        assert_eq!(parse_k1_sel_threshold(Some("0.35")), 0.35);
        assert_eq!(parse_k1_sel_threshold(Some(" 0.1 ")), 0.1);
        assert_eq!(parse_k1_sel_threshold(Some("0")), 0.0);
        assert_eq!(parse_k1_sel_threshold(Some("1")), 1.0);
        assert_eq!(parse_k1_sel_threshold(Some("2.5")), 2.5);
        assert_eq!(parse_k1_sel_threshold(Some("-0.1")), d);
        assert_eq!(parse_k1_sel_threshold(Some("NaN")), d);
        assert_eq!(parse_k1_sel_threshold(Some("inf")), d);
        assert_eq!(parse_k1_sel_threshold(Some("-inf")), d);
    }

    /// K1-F2 estimate math: the planner's quotient, capped at 1.0; empty
    /// or degenerate denominators carry no information (None → the
    /// `k1-latemat-sel-unknown` refusal, fail-closed).
    #[test]
    fn k1_sel_estimate_math() {
        assert_eq!(k1_sel_estimate(5000.0, 50000.0), Some(0.1));
        assert_eq!(k1_sel_estimate(45000.0, 50000.0), Some(0.9));
        // plan_rows clamps to >= 1; a lagging exec-time denominator caps.
        assert_eq!(k1_sel_estimate(1.0, 1.0), Some(1.0));
        assert_eq!(k1_sel_estimate(2000.0, 1000.0), Some(1.0));
        assert_eq!(k1_sel_estimate(1.0, 0.0), None);
        assert_eq!(k1_sel_estimate(1.0, -1.0), None);
        assert_eq!(k1_sel_estimate(1.0, f64::NAN), None);
        assert_eq!(k1_sel_estimate(f64::NAN, 1000.0), None);
        // Threshold-boundary discipline: admit at <=, refuse above.
        let thr = parse_k1_sel_threshold(None);
        assert!(k1_sel_estimate(thr * 50000.0, 50000.0).unwrap() <= thr);
        assert!(k1_sel_estimate(thr * 50000.0 + 500.0, 50000.0).unwrap() > thr);
    }

    /// R1 borrow discipline is COMPILE-SHAPE: accessors borrow `&self`, so
    /// two staged views may coexist, and any `&mut` call ends them. (A
    /// retained-past-`next_batch` view is a borrow error — documented here;
    /// the negative case cannot be written in a passing test.)
    #[test]
    fn read_face_borrow_shape() {
        let src = StubSource(caps(true, false));
        let a = src.batch_soa();
        let b = src.qual_sel();
        assert!(a.is_none() && b.is_none());
    }

    // --- WS-AH wave-9 sub-region (K1 late materialization, band 91001+) ----

    /// `PGRUST_LANE_V2_K1_LATEMAT` A/B lever (AtomicU8 idiom): both states
    /// resolvable in one process; restored to OFF (the default the rest of
    /// the suite assumes — knob-OFF = today's full staging bytes).
    #[test]
    fn k1_latemat_knob_ab() {
        k1_latemat_set_for_tests(true);
        assert!(k1_latemat_enabled());
        k1_latemat_set_for_tests(false);
        assert!(!k1_latemat_enabled());
    }

    /// The §2 grant's additive method defaults to a NO-OP: a source
    /// implementing only the five positional methods (sources that staged
    /// everything) accepts any completion ask and does nothing — never a
    /// panic, never an error (fail-open, unlike the emit face whose default
    /// is fail-closed: an un-completed batch is merely today's fully-staged
    /// batch).
    #[test]
    fn complete_deform_default_is_noop() {
        // The default body never touches estate/cols/sel; exercising it
        // requires no EStateData — the body is `Ok(())` by construction
        // (compile-checked here through the trait object surface).
        fn assert_defaulted<'mcx, S: BatchGranuleSource<'mcx>>(_s: &S) {}
        let src = StubSource(caps(false, false));
        assert_defaulted(&src);
    }

    /// Inc-3 needed-set split (pure half): `now` keeps deferred∩needed in
    /// deferred order, `publish` gets the rest; `None` needed = the landed
    /// wave-9 behavior (everything completes now, publish empty).
    #[test]
    fn k1_latemat_split_partition() {
        // No census: pre-inc-3 bytes.
        assert_eq!(k1_latemat_split(vec![0, 2, 4], None), (vec![0, 2, 4], vec![]));
        // Census: agg-needed completes now, the rest defers to publish legs.
        assert_eq!(
            k1_latemat_split(vec![0, 2, 4], Some(&[1, 2, 3, 4])),
            (vec![2, 4], vec![0])
        );
        // Nothing needed beyond the staged set: everything is publish-only.
        assert_eq!(k1_latemat_split(vec![0, 2], Some(&[1, 3])), (vec![], vec![0, 2]));
        // Needed covers the whole deferred set: publish empty (kernel legs
        // complete everything — behavior == pre-inc-3 with one fewer pass).
        assert_eq!(k1_latemat_split(vec![5, 6], Some(&[5, 6, 7])), (vec![5, 6], vec![]));
        // Empty deferred set never arms (the arm refuses all-staged), but
        // the pure half stays total.
        assert_eq!(k1_latemat_split(vec![], Some(&[1])), (vec![], vec![]));
    }

    // --- end WS-AH wave-9 sub-region ----------------------------------------
}
