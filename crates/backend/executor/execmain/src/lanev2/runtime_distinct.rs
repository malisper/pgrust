//! M2 DISTINCT SINK — parallel exact-DISTINCT / COUNT(DISTINCT) on the
//! morsel runtime (docs/design/m2-sinks.md §3 donor B re-homed;
//! docs/design/parallelism-redesign-2026-07.md §2.2/§5-M2).
//!
//! Shape: the SERIAL-plan grouped distinct pipeline `Agg(AGG_SORTED) ← Sort
//! ← SeqScan(pgrcolumnar)` (the sorted grouped exact-DISTINCT class — and, with the
//! distinct-bytes car, the near-unique text-group-key donor-B class), executed as
//! one SealedParallelSink on the runtime: ACCEPT (granule-morsel scan →
//! PREWHERE → per-worker `PdBuilder` partial: compact int group keys +
//! canonical-bytes text keys (content identity, arena spans),
//! (acc,count) vocab words, exact `DistinctSet`s) → SEAL (parallel
//! per-worker freeze into `PdHandedTable`s) → COMBINE (256 group-partition
//! bucket-claim merges — disjoint partitions, single writer per output
//! cell) → finalize (concatenate buckets, publish). The parked leader
//! adopts the merged result through the UNCHANGED serial emit tail
//! (`agg_hashgroup_adopt_merged` → hashgroup emit): groups in the plan
//! Sort's prefix order, byte-identical to the serial arm by the donor's
//! identity argument (exact representational set equality;
//! order-insensitive-exact transitions; count/sum reassociation
//! unobservable).
//!
//! vs the Gather-era donor (pardistinct): the registry/handoff, the leader's
//! own partial, the stray-row queue drain, and the `spent` flag are all
//! GONE (no tuple queues exist); the vocabulary refusal is DROPPED — the
//! vocab companion-agg shape rides the sink (the donor's refusal priced the
//! per-row vocab accept against the fused classic GatherMerge drives, a
//! comparison that no longer exists here).
//!
//! Budget law (m2-sinks.md R3/R5; M3.5 §4): each Local gets the derived
//! `worker_budget` (C-parity per participant; participants = launched
//! helpers ≤ dop, so the memory envelope is the plan-shaped one, never
//! nthreads-shaped). A worker CROSSING its budget SPILLS an epoch of its
//! set values to its FileSet spill file (grouped int-set shapes; the
//! docs/design/m3.5-spill.md §4 arm, `PGRUST_RUNTIME_DISTINCT_SPILL=0`
//! restores phase 1) and keeps accepting bounded; a combine partition
//! whose pre-count crosses the budget SPLITS its spilled records by
//! `mix64(value)` bytes and merges bounded slices in sequence (inc-3b,
//! `PGRUST_RUNTIME_DISTINCT_SPILL_DEPTH` caps the recursion). Shapes the
//! spill cannot carry exactly — and split refusals (depth cap, or a merged
//! bucket whose TRUE deduplicated size cannot fit) — fall back to the
//! phase-1 law: the arm aborts the RG and the leader RERUNS THE SERIAL
//! ARM: exact, nothing consumed, bounded memory at every arm.
//!
//! Engagement layering (all cheap; absent = today's serial path, byte- and
//! perf-identical): PGRUST_RUNTIME=1 (pool spawned) + SET
//! pgrust.runtime_distinct_pool = <dop> (falling back to
//! pgrust.runtime_scan_pool — the lane's booked instrument vocabulary) +
//! PGRUST_RUNTIME_DISTINCT != 0 (arm kill switch, decoupled from the scan
//! arm's at m2-integration); see guc_tables::runtime_pool for the reconciled
//! three-arm surface. The plan surface stays the serial plan; EXPLAIN
//! unchanged; instrumented runs refuse (EXPLAIN ANALYZE stays C-exact).

use std::cell::UnsafeCell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::nodeagg::{
    pd_bucket_precount, pd_concat_buckets, pd_emit_bucket, pd_empty_grouped_table,
    pd_merge_bucket_refs, pd_route_value_records, pd_spill_record_width, pd_table_from_spill,
    pd_vec_plan, PdBucketMerger, PdEmitBucket, PdEmitRecipe, PdFeed, PdHandedTable, PdInt,
    PdMerged, PdSinkLocal, PdSpec, PdTopnCand, PdTopnSpec, PdVecScratch, PD_SINK_GROUP_PARTS,
};
use ::types_error::{PgError, PgResult, ERROR};
use ::types_nodes::plannodes::PlannedStmt;
use ::types_nodes::NodeTag;

use super::router::{self, ArmClass, ArmCounter};
use super::runtime_instr::{self, EaRowTally, InstrumentPartial};
use super::stats::{self, RefuseReason, ShapeClass};
use super::{lane_trace, seq_scan_fusible, seq_scan_fusible_runtime_ea, trace_feed};
use super::{drain_pipeline, BatchEmit, BatchSink, SeqScanFilterProject, SeqScanSource, Sink, SinkFeed};

// ---------------------------------------------------------------------------
// Shared state: the parallel context's private payload AND the sink body
// (one struct, one Arc — the runtime_scan discipline).
// ---------------------------------------------------------------------------

struct SendConstPstmt(*const PlannedStmt<'static>);
// SAFETY: read-only erased reference into the leader's executor arena; the
// leader keeps it alive until DestroyParallelContext has joined every helper
// (the execparallel SendConst contract, verbatim).
unsafe impl Send for SendConstPstmt {}
// SAFETY: as above; helpers only read.
unsafe impl Sync for SendConstPstmt {}

/// Per-worker sink Local: the donor `PdSinkLocal` plus the M3.5 spill face —
/// its single-writer spill file (epochs of partition-contiguous value
/// records), created lazily at the first budget crossing when the spill arm
/// is enabled. Plain data between flush events; rides SEAL like everything
/// else. `seen_null`, vocab words, and the group table itself never spill —
/// they stay inside the `PdSinkLocal` (design §4).
pub(super) struct DistinctSinkLocal {
    pd: PdSinkLocal,
    spill: Option<::spillset::SpillFile>,
}

/// A sealed Local: the frozen in-memory remainder + the (frozen) spill
/// directory the combine pre-counts and replays from (design §4: Sealed =
/// PdHandedTable + spill directory).
pub(super) struct DistinctSealed {
    table: PdHandedTable,
    spill: Option<::spillset::SpillFile>,
}

pub(super) struct RuntimeDistinctShared {
    rt: &'static Arc<runtime::Runtime>,
    /// Weak: the RG's task sets hold this struct as their sink — a strong
    /// handle here would leak the cycle.
    rg: OnceLock<runtime::WeakRgHandle>,
    pcxt_shared: OnceLock<Arc<parallel::ParallelShared>>,
    pstmt: SendConstPstmt,
    query_text: String,
    eflags: i32,
    /// The leader-derived build recipe (plain data; helpers fork Locals
    /// from it in-process — no DSM transfer).
    spec: Arc<PdSpec>,
    /// PAREMIT recipe (pardistinct.rs section doc): Some = every combine
    /// claim materializes its partition's ordered, fully-projected emit
    /// bucket and the leader merges buckets instead of adopting a merged
    /// table. Chosen once at admission — one engagement-level mode, so the
    /// out cells hold one variant uniformly.
    paremit: Option<Arc<PdEmitRecipe>>,
    /// Kernel-2 bounded selection (pardistinct.rs topn section doc): Some =
    /// every paremit combine claim materializes only its partition's
    /// top-`bound` candidates and the leader emits the truncate-merged
    /// global winners. Resolved once at admission beside the recipe (never
    /// armed without `paremit`); a `None` is the full drain exactly.
    topn: Option<PdTopnSpec>,
    /// Helpers whose binder validate() refused (before any claim).
    refused: AtomicUsize,
    /// Helpers that bound and entered the drive.
    started: AtomicUsize,
    /// Helpers that have EXITED `helper_drive` (every exit path bumps
    /// exactly once, by drop guard) — the leader's liveness-reap input
    /// (inc-2c; see runtime_agg, the identical hole).
    exited: AtomicUsize,
    /// First worker-phase error (the entry-phase errors ride the ordinary
    /// parallel message channel).
    error: Mutex<Option<Box<PgError>>>,
    /// Set when any worker recorded an error (fast skip for later morsels).
    failed: AtomicBool,
    /// A worker budget crossed mid-accept: NOT an error — the RG aborts and
    /// the leader falls back to the serial arm (m2-sinks.md R5 phase 1).
    crossed: AtomicBool,
    /// Combine-phase retained CONTENT bytes (merged bucket outputs, summed
    /// across claims) — m2-integration R3 accounting for the merged RESULT,
    /// checked against the ADMITTED envelope (forked Locals × worker_budget;
    /// see the check site for why not one worker_budget). Crossing = the
    /// same `crossed` fallback.
    merged_bytes: AtomicUsize,
    /// M3.5 spill arm: the engagement's spill set (None = spill disabled →
    /// budget crossings refuse exactly as before).
    spill_set: Option<Arc<::spillset::SpillSet>>,
    /// LOCALITY CAP (distinct-sidecar-cap lane — the radix-cap medicine on
    /// the DISTINCT-sidecar working set): Some(bytes) = a worker whose
    /// per-group distinct sets hold >= cap bytes drains them through the
    /// EXISTING spill-epoch machinery (256-partition contiguous value
    /// records) and resets the sets, keeping every accept-side set probe
    /// cache-resident; the combine's per-partition replay re-dedups
    /// cross-epoch duplicates into cache-resident bucket tables. Resolved
    /// once at engage: DOP>1 AND the spill arm is live (the epoch drain is
    /// the pressure law's own machinery — a locality flush is just an early
    /// epoch). None = today's behavior exactly (budget-crossing epochs
    /// only). The pressure law itself is untouched: a real budget crossing
    /// still spills (or refuses) through the same path.
    locality_cap: Option<usize>,
    /// GL-LOWDIST-1: this engagement seals LIVE-form tables and its combine
    /// takes the size-asymmetric steal path (knob ON and resolved dop
    /// within the band bound — decided once at engage).
    lowwidth: bool,
    /// Spill observability (gate-record counters, the R4 line).
    spill_epochs: AtomicU64,
    spilled_bytes: AtomicU64,
    /// Combine-split observability (inc-3b): split events, deepest level
    /// reached, and a per-engagement uniquifier for split-file names.
    combine_splits: AtomicU64,
    split_depth_max: AtomicU64,
    split_uniq: AtomicU64,
    /// Combine output cells, one per group partition. Single writer each:
    /// partition p is claimed exactly once by the combine task set. The
    /// variant is uniform per engagement (`paremit`).
    out: Vec<UnsafeCell<Option<DstCombined>>>,
    /// The published result (finalize writes, the leader takes).
    merged: Mutex<Option<DstPublished>>,
    /// EA-on-morsels (ea-morsels.md §2): Some(scan plan_node_id) ONLY when
    /// engaged under EXPLAIN ANALYZE — the sink's single EA flag; None on
    /// every other path (dead-when-off).
    ea_scan_node: Option<i32>,
    /// EA instrument partials, worker-indexed (the scan arm's per-ordinal
    /// overwrite channel — the Local type is nodeagg's, so the partial rides
    /// beside it, not inside it; same claim-end overwrite discipline, read
    /// by the leader on clean Completed only). Some iff `ea_scan_node`.
    ea_instr_slots: Option<Vec<Mutex<Option<InstrumentPartial>>>>,
    /// TIMER mode (inc-3): one clock pair per claim against `ea_epoch`
    /// (shared engagement origin — cross-worker comparable). false in ROWS
    /// mode and on every non-EA path: zero clock reads.
    ea_timer: bool,
    ea_epoch: std::time::Instant,
    /// M2 inc-1 standing channel: the live board entry, held for the
    /// PRIVATE_SHUTDOWN standing join (standing_channel, scan discipline).
    standing: Mutex<Option<Arc<parallel::standing::StandingEngagement>>>,
    /// GL-VECACCEPT-1 (PGRUST_RUNTIME_AGG_VECACCEPT, default OFF): Some =
    /// every accept claim runs the vectorized whole-granule drive
    /// ([`PdAcceptSink::vec_claim`]) — direct decoded lanes, batch hash,
    /// prefetched batch probe/resolve, columnar rider folds, the staged
    /// set feed in bulk. None = the incumbent per-row emit/accept pipeline
    /// byte-for-byte. Resolved once at engage (fail-closed admission:
    /// `vec_cols`).
    vec: Option<VecCols>,
    /// Vec-accept census (the mechanism witness: rows/granules through the
    /// direct lanes; printed at finalize under the trace channel).
    vec_rows: AtomicU64,
    vec_granules: AtomicU64,
}

/// GL-VECACCEPT-1 lane geometry, scan-column space: the [`pd_vec_plan`]
/// atts mapped through the (Var-only) projection census onto 0-based scan
/// columns the direct granule feed can hand out whole.
struct VecCols {
    key_col: u16,
    key_kind: PdInt,
    set_col: u16,
    set_kind: PdInt,
    /// Aligned with `spec.vocab`: Some = a value lane to canonicalize +
    /// fold; None = count-only rider (no lane read).
    riders: Vec<Option<(u16, PdInt)>>,
}

// SAFETY: (i) each `out` cell has a single writer — the sink contract
// visits every partition exactly once — and is read only by `finalize`,
// which the runtime's last-worker-out orders after every combine; (ii) the
// PdMerged values held in `out`/`merged` are never-spilled bucket-merge
// outputs (owned plain data — the PdHandedTable self-contained-buffer
// argument), and the paremit PdEmitBucket values are arena-self-contained
// projected rows (Send + Sync by the same argument); (iii) every other
// member is Send/Sync by composition.
unsafe impl Send for RuntimeDistinctShared {}
unsafe impl Sync for RuntimeDistinctShared {}

impl RuntimeDistinctShared {
    fn fail(&self, e: Box<PgError>) {
        {
            let mut g = self.error.lock().unwrap_or_else(|p| p.into_inner());
            if g.is_none() {
                *g = Some(e);
            }
        }
        self.failed.store(true, Ordering::SeqCst);
        self.abort_rg();
    }

    /// Budget crossing: no degrade target under the runtime — abort the RG;
    /// the leader observes `crossed` and reruns the serial arm.
    fn cross(&self) {
        self.crossed.store(true, Ordering::SeqCst);
        self.abort_rg();
    }

    fn abort_rg(&self) {
        if let Some(rg) = self.rg.get().and_then(|w| w.upgrade()) {
            rg.abort();
        }
    }

    fn take_error(&self) -> Option<Box<PgError>> {
        self.error.lock().unwrap_or_else(|p| p.into_inner()).take()
    }

    fn take_merged(&self) -> Option<DstPublished> {
        self.merged.lock().unwrap_or_else(|p| p.into_inner()).take()
    }

    /// COMBINE-claim tail: convert the merged partition into the retained
    /// form the engagement's mode publishes — the paremit bucket (ordered,
    /// fully-projected; the merged table and its sets drop with the claim)
    /// or the merged table itself (adopt mode, unchanged).
    fn finish_combine(&self, m: PdMerged<'static>) -> PgResult<DstCombined> {
        if let Some(r) = &self.paremit {
            let (b, cands) = pd_emit_bucket(&self.spec, r, &m, self.topn.as_ref())?;
            return Ok(DstCombined::Emit(b, cands));
        }
        Ok(DstCombined::Merged(m))
    }
}

/// One combine claim's retained output (variant uniform per engagement).
enum DstCombined {
    Merged(PdMerged<'static>),
    /// Paremit bucket + (topn-armed only) its candidate list.
    Emit(PdEmitBucket, Option<Vec<PdTopnCand>>),
}

impl DstCombined {
    /// Retained CONTENT bytes (the R3 merged-result metering input).
    fn mem_bytes(&self) -> usize {
        match self {
            DstCombined::Merged(m) => m.mem_bytes(),
            DstCombined::Emit(b, cands) => {
                b.mem_bytes()
                    + cands.as_ref().map_or(0, |c| {
                        c.len() * core::mem::size_of::<PdTopnCand>()
                    })
            }
        }
    }
}

/// The finalize-published result the leader consumes. The paremit
/// candidate lists are bucket-aligned; `Some` iff the engagement armed the
/// bounded selection (uniform per engagement, like the mode itself).
enum DstPublished {
    Merged(PdMerged<'static>),
    Emit(Vec<PdEmitBucket>, Option<Vec<Vec<PdTopnCand>>>),
}

// ---------------------------------------------------------------------------
// The SealedParallelSink implementation. accept_local/seal are INFALLIBLE BY
// CONTRACT: errors and panics are caught, recorded (first wins), and turn
// into an RG abort — the runtime protocol never sees an unwind.
// ---------------------------------------------------------------------------

impl runtime::SealedParallelSink for RuntimeDistinctShared {
    type Local = DistinctSinkLocal;
    type Sealed = DistinctSealed;

    fn fork(&self, _worker: usize) -> DistinctSinkLocal {
        let pd = PdSinkLocal::new(Arc::clone(&self.spec), self.spec.worker_budget);
        if _worker == 0 && pd.batch_insert_armed() {
            // batch-insert lane engagement trace (e2e leg pin; once per
            // engagement — worker 0's fork).
            trace_feed("runtime-distinct: batched set-insert armed");
        }
        if self.vec.is_some() {
            // GL-VECACCEPT-1 invariant: the engagement-level admission
            // (`vec_cols`) is the Local-side gate's superset — a Local
            // that cannot run the vec schedule under an armed engagement
            // is a contract breach, never a silent per-row fallback.
            debug_assert!(pd.vec_admissible(), "vec engagement forked a non-vec Local");
            if _worker == 0 {
                // The armed-witness line (e2e leg pin; once per engagement).
                trace_feed("runtime-distinct: vecaccept armed");
            }
        }
        DistinctSinkLocal { pd, spill: None }
    }

    fn accept_local(
        &self,
        local: &mut DistinctSinkLocal,
        worker: usize,
        range: runtime::MorselRange,
    ) {
        if self.failed.load(Ordering::SeqCst) || self.crossed.load(Ordering::SeqCst) {
            // Already aborting: drain the claim without work.
            return;
        }
        let r = catch_unwind(AssertUnwindSafe(|| self.morsel_body(local, worker, range)));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                mark_self_errored();
                self.fail(e);
            }
            Err(_panic) => {
                mark_self_errored();
                self.fail(
                    PgError::new(ERROR, "runtime distinct worker panicked in a morsel").into(),
                );
            }
        }
    }

    fn seal(&self, _worker: usize, local: DistinctSinkLocal) -> DistinctSealed {
        let DistinctSinkLocal { pd, spill } = local;
        if self.failed.load(Ordering::SeqCst) || self.crossed.load(Ordering::SeqCst) {
            return DistinctSealed { table: pd_empty_grouped_table(&self.spec), spill: None };
        }
        let r = catch_unwind(AssertUnwindSafe(|| {
            if self.lowwidth {
                // GL-LOWDIST-1: LIVE-form seal — the combine steals whole
                // sets instead of re-inserting every donor's values.
                pd.freeze_live()
            } else {
                pd.freeze()
            }
        }));
        match r {
            // freeze() sees a never-spilled builder (its `!ever_spilled`
            // invariant holds: the M3.5 spill drains set VALUES only and
            // never touches the builder's own Mcx-bound machinery); the
            // spill directory rides alongside the frozen remainder.
            Ok(Ok(t)) => DistinctSealed { table: t, spill },
            Ok(Err(e)) => {
                self.fail(e);
                DistinctSealed { table: pd_empty_grouped_table(&self.spec), spill: None }
            }
            Err(_panic) => {
                self.fail(PgError::new(ERROR, "runtime distinct worker panicked in seal").into());
                DistinctSealed { table: pd_empty_grouped_table(&self.spec), spill: None }
            }
        }
    }

    fn partitions(&self) -> u64 {
        PD_SINK_GROUP_PARTS
    }

    fn combine(&self, part: u64, sealed: &[DistinctSealed]) {
        if self.failed.load(Ordering::SeqCst) || self.crossed.load(Ordering::SeqCst) {
            return;
        }
        let r = catch_unwind(AssertUnwindSafe(|| self.combine_body(part as usize, sealed)));
        match r {
            Ok(Ok(DstCombine::Done(m))) => {
                // R3 accounting (m2-integration audit): the merged bucket is
                // RETAINED until the leader adopts — meter it against the
                // ADMITTED engagement envelope (forked Locals x per-Local
                // budget: the merged union is bounded by the sum of the
                // sealed tables' content, so this trips only on real
                // overhead/accounting surprises — fail-closed, visible).
                // NOT one worker_budget: the union legitimately exceeds a
                // single Local's budget (the grouped-distinct @100M rt1-crosses/rt2-fits
                // booked behavior). Crossing takes the same bounded fallback
                // as an accept-phase crossing.
                let b = m.mem_bytes();
                let total = self.merged_bytes.fetch_add(b, Ordering::Relaxed) + b;
                // COMPOSITION (train-13, m35 spill x train-12 R3; the agg
                // arm's retain_bucket law verbatim): the in-memory envelope
                // holds for spill-DISABLED engagements only — with the spill
                // arm armed the merged result is legitimately bounded by the
                // spilled content (combine_body's directory-only pre-count
                // bounds each claim's transient footprint). Metering stays
                // on for observability either way.
                if self.spill_set.is_none()
                    && total > self.spec.worker_budget.saturating_mul(sealed.len().max(1))
                {
                    self.cross();
                    return;
                }
                // SAFETY: partition `part` is handed to this claimer alone
                // (sink contract); finalize reads happen-after every combine.
                unsafe { *self.out[part as usize].get() = Some(m) };
            }
            Ok(Ok(DstCombine::OverBudget)) => {
                // Bounded-memory refusal, not an error: the merged bucket
                // cannot be carried under the worker budget (split depth
                // cap, spill disarmed, or the TRUE deduplicated bucket
                // itself cannot fit) — abort to the serial rerun, which
                // spills through its own C-parity machinery.
                lane_trace(
                    "runtime-distinct: combine partition over budget (split depth cap, spill disarmed, or merged set cannot fit) — serial rerun",
                );
                self.cross();
            }
            Ok(Err(e)) => self.fail(e),
            Err(_panic) => {
                self.fail(
                    PgError::new(ERROR, "runtime distinct worker panicked in combine").into(),
                );
            }
        }
    }

    fn finalize(&self, _sealed: &[DistinctSealed]) {
        if self.failed.load(Ordering::SeqCst) || self.crossed.load(Ordering::SeqCst) {
            return;
        }
        if self.vec.is_some() {
            // GL-VECACCEPT-1 accept census (the mechanism witness: every
            // accepted row rode the direct lanes, none the per-row path).
            trace_feed(&format!(
                "runtime-distinct: vecaccept rows={} granules={}",
                self.vec_rows.load(Ordering::Relaxed),
                self.vec_granules.load(Ordering::Relaxed),
            ));
        }
        // SAFETY: single-threaded under last-worker-out, after every combine.
        let cells: Vec<DstCombined> = self
            .out
            .iter()
            .filter_map(|c| unsafe { (*c.get()).take() })
            .collect();
        // Uniform-mode invariant: the recipe is fixed before submit and
        // finish_combine branches on the same field, so mixed variants are
        // structurally impossible. Fail CLOSED if ever violated (publish
        // nothing — the leader's "completed without a merged result"
        // protocol check fires) rather than unwinding into the runtime
        // (finalize runs outside the claim catch_unwind wrappers).
        let mut bufs: Vec<PdEmitBucket> = Vec::new();
        let mut bufcands: Vec<Option<Vec<PdTopnCand>>> = Vec::new();
        let mut merged_cells: Vec<PdMerged<'static>> = Vec::new();
        for c in cells {
            match c {
                DstCombined::Emit(b, cands) => {
                    bufs.push(b);
                    bufcands.push(cands);
                }
                DstCombined::Merged(m) => merged_cells.push(m),
            }
        }
        let published = if self.paremit.is_some() {
            debug_assert!(merged_cells.is_empty(), "adopt cell in a paremit engagement");
            if !merged_cells.is_empty() {
                return;
            }
            // Uniform-mode invariant, selection face: topn-armed
            // engagements carry a candidate list on EVERY bucket (the
            // recipe and spec are fixed pre-submit). Fail CLOSED on a
            // violation, as above.
            let cands = if self.topn.is_some() {
                if bufcands.iter().any(Option::is_none) {
                    return;
                }
                Some(bufcands.into_iter().map(|c| c.expect("checked")).collect())
            } else {
                debug_assert!(bufcands.iter().all(Option::is_none));
                None
            };
            // Paremit mode: publish the ordered buckets themselves (bucket
            // position is a non-surface — the leader merge orders rows).
            DstPublished::Emit(bufs, cands)
        } else {
            debug_assert!(bufs.is_empty(), "paremit cell in an adopt engagement");
            if !bufs.is_empty() {
                return;
            }
            DstPublished::Merged(pd_concat_buckets(&self.spec, merged_cells))
        };
        *self.merged.lock().unwrap_or_else(|p| p.into_inner()) = Some(published);
    }
}

/// Combine verdict: `OverBudget` = bounded-memory refusal → serial rerun
/// (spill disarmed, the in-memory merge alone cannot fit, split depth cap,
/// or the merged bucket's exact deduplicated size crossed the budget). The
/// SIZE decision itself is directory-only (M3.5 §4/§7 — nothing is read
/// from disk before it); the split's own refusals come after bounded I/O.
enum DstCombine {
    Done(DstCombined),
    OverBudget,
}

impl RuntimeDistinctShared {
    /// COMBINE(part b), the M3.5 spill-aware path: pre-count b's spilled
    /// bytes from the spill-file DIRECTORIES + the in-memory tables'
    /// partition indexes; SPLIT by value hash (inc-3b, [`Self::split_combine`])
    /// if the merged bucket's estimated bytes cross the worker budget —
    /// refusal (→ serial rerun) remains for the disarmed/cannot-fit faces;
    /// otherwise read b's records
    /// (open-by-name on THIS thread — the files are frozen: combine
    /// deps-follows accept), rebuild them into merge-compatible tables
    /// through the donor builder kernel, and run the donor bucket merge
    /// over in-memory + synthesized tables. Set-insert idempotence makes
    /// replay order immaterial (cross-epoch duplicates re-dedup here).
    fn combine_body(&self, b: usize, sealed: &[DistinctSealed]) -> PgResult<DstCombine> {
        let spilled_bytes: u64 = sealed
            .iter()
            .filter_map(|s| s.spill.as_ref())
            .map(|f| f.part_len(b as u32))
            .sum();
        if spilled_bytes == 0 {
            // Nothing spilled into this partition: the donor merge verbatim.
            let mut refs: Vec<&PdHandedTable> = sealed.iter().map(|s| &s.table).collect();
            if self.lowwidth {
                // GL-LOWDIST-1: LARGEST donor first — the bucket merger
                // steals the first live donor's set per group (its values
                // never re-hash), so ordering by per-bucket value count
                // maximizes the stolen volume. O(donors x bucket groups)
                // partition-index reads; group order within the bucket is
                // first-seen (a non-surface — the adopt/emit tails order
                // groups themselves; set replays are order-insensitive).
                refs.sort_by_cached_key(|t| {
                    core::cmp::Reverse(pd_bucket_precount(&self.spec, t, b).1)
                });
            }
            return Ok(DstCombine::Done(
                self.finish_combine(pd_merge_bucket_refs(&self.spec, &refs, b))?,
            ));
        }
        // Pre-count size check (M3.5 §4): spilled record count from the
        // directory alone; in-memory groups/values from the partition
        // indexes. Every term over-counts duplicates, so this only ever
        // refuses conservatively. Estimate: values cost ~16B each in a
        // merged set (i64 + probe slot), spilled values are transiently
        // held TWICE (synth table + merged output), groups carry the
        // fixed per-group block; 3/2 headroom on the value term. Bytes-key
        // specs (distinct-bytes car): records are variable-width, so the
        // row count divides by the MINIMUM width (over-counts rows —
        // conservative), and the merged bucket's key arena is bounded by
        // the in-memory key-content pre-count (×2: merged output + one
        // transient synth table).
        let bytes_mode = ::nodeagg::pd_spill_bytes_mode(&self.spec);
        let width = if bytes_mode {
            ::nodeagg::pd_spill_min_record_width(&self.spec) as u64
        } else {
            pd_spill_record_width(&self.spec) as u64
        };
        let spilled_vals = (spilled_bytes / width) as usize;
        let mut groups = 0usize;
        let mut inmem_vals = 0usize;
        let mut inmem_key_bytes = 0usize;
        for s in sealed {
            let (g, v, kb) = pd_bucket_precount(&self.spec, &s.table, b);
            groups += g;
            inmem_vals += v;
            inmem_key_bytes += kb;
        }
        let per_group = self.spec.nkeys() * 8
            + 2 * self.spec.vocab.len() * 8
            + self.spec.sets.len() * 48
            + 64;
        let est = (inmem_vals + 2 * spilled_vals)
            .saturating_mul(16)
            .saturating_mul(3)
            / 2
            + groups.saturating_mul(per_group)
            + inmem_key_bytes.saturating_mul(2);
        if est > self.spec.worker_budget {
            // inc-3b: recursive COMBINE-SPLIT by value hash — the estimate
            // over-counts cross-epoch/cross-Local duplicates, and the split
            // converts exactly that inflation (design §4). No spill set =
            // the disarmed refusal, exactly as before.
            let Some(set) = &self.spill_set else {
                return Ok(DstCombine::OverBudget);
            };
            // The one-pass in-memory merge is NOT value-sliced (group-level
            // facts must merge exactly once, see PdBucketMerger): if IT
            // alone cannot fit, no recursion helps — the final merged
            // bucket is a superset of the in-memory merge and must fit to
            // be emitted at all.
            let est_inmem = inmem_vals.saturating_mul(16).saturating_mul(3) / 2
                + groups.saturating_mul(per_group)
                + inmem_key_bytes;
            if est_inmem > self.spec.worker_budget {
                return Ok(DstCombine::OverBudget);
            }
            return self.split_combine(b, sealed, set, groups, per_group);
        }
        // Read + rebuild each Local's spilled partition, then merge.
        let ctx = ::mcx::MemoryContext::new("m35-dst-spill-read");
        let mut synth: Vec<PdHandedTable> = Vec::new();
        for s in sealed {
            let Some(f) = &s.spill else { continue };
            if let Some(mut r) = f.read_part(ctx.mcx(), b as u32)? {
                let bytes = r.read_to_end()?;
                r.close()?;
                synth.push(pd_table_from_spill(&self.spec, &bytes)?);
            }
        }
        let refs: Vec<&PdHandedTable> =
            sealed.iter().map(|s| &s.table).chain(synth.iter()).collect();
        Ok(DstCombine::Done(
            self.finish_combine(pd_merge_bucket_refs(&self.spec, &refs, b))?,
        ))
    }

    /// inc-3b COMBINE-SPLIT (design §4, the agg inc-2b twin on the VALUE
    /// axis): route partition `b`'s spilled records from every Local by the
    /// top byte of `mix64(value)` into a combine-task-owned split file,
    /// then merge bounded: the sealed IN-MEMORY tables in ONE pass (they
    /// carry ALL group-level state — vocab words, seen_null, group
    /// existence — and are not value-sliced, so nothing merges twice; see
    /// PdBucketMerger's exactly-once law), then each slice's synthesized
    /// table in sequence (states all zero, set_null all false — pure
    /// idempotent set-value insertions over disjoint value slices), dropped
    /// between absorbs. A slice whose synth table would cross the budget
    /// recurses one mix64 byte deeper into a fresh file, depth-capped →
    /// refusal → serial rerun. After every slice absorb the merged bucket's
    /// EXACT capacity-based size is checked — the dedup-aware bound no
    /// directory pre-count can compute: duplicate-inflation crossings
    /// convert (dedup keeps the bucket small), TRUE-cardinality overflows
    /// refuse there (wasted routing I/O, never unbounded growth).
    fn split_combine(
        &self,
        b: usize,
        sealed: &[DistinctSealed],
        set: &Arc<::spillset::SpillSet>,
        groups: usize,
        per_group: usize,
    ) -> PgResult<DstCombine> {
        self.combine_splits.fetch_add(1, Ordering::Relaxed);
        self.split_depth_max.fetch_max(1, Ordering::Relaxed);
        // Route every Local's partition-b records (record-aligned streaming;
        // torn records fail closed) into the depth-1 slice file.
        let mut router = DstSubRouter::new(self, set, b, 1);
        for s in sealed {
            let Some(f) = &s.spill else { continue };
            stream_part_dst(&self.spec, f, b as u32, |chunk| {
                router.absorb(&self.spec, chunk)
            })?;
        }
        router.flush()?;
        // In-memory tables merge EXACTLY ONCE, before any slice.
        let mut merger = PdBucketMerger::new(&self.spec);
        // dedupsub reserve wave: spilled records can never reference a
        // group the in-memory remainders lack (exactly-once law above), so
        // the in-memory pre-count bounds the merged bucket's group count.
        merger.seed_groups(groups);
        for s in sealed {
            merger.absorb(&s.table, b);
        }
        if !self.split_slices_into(&mut merger, b, set, &router.file, 1, groups, per_group)? {
            return Ok(DstCombine::OverBudget);
        }
        Ok(DstCombine::Done(self.finish_combine(merger.finish())?))
    }

    /// Merge each value slice of a routed split file into `merger`; slices
    /// whose synth table would cross the budget recurse one mix64 byte
    /// deeper (fresh file), depth-capped. Returns false on depth-cap
    /// overflow or when the merged bucket's exact size crosses the budget
    /// (the caller refuses → R5 serial rerun).
    #[allow(clippy::too_many_arguments)]
    fn split_slices_into(
        &self,
        merger: &mut PdBucketMerger<'_>,
        b: usize,
        set: &Arc<::spillset::SpillSet>,
        file: &::spillset::SpillFile,
        depth: u32,
        groups: usize,
        per_group: usize,
    ) -> PgResult<bool> {
        // Bytes-key specs: variable-width records — rows divide by the
        // MINIMUM width (over-counts rows → conservative slice estimates;
        // the raw slice bytes term below bounds the synth arena).
        let bytes_mode = ::nodeagg::pd_spill_bytes_mode(&self.spec);
        let width = if bytes_mode {
            ::nodeagg::pd_spill_min_record_width(&self.spec)
        } else {
            pd_spill_record_width(&self.spec)
        };
        let budget = self.spec.worker_budget;
        for sl in 0..DST_SPLIT_SLICES {
            // Abort responsiveness: a split is the longest single combine
            // task this sink can run (routing I/O + up to 256^depth slice
            // merges) — if the RG is already failing/crossed, stop here
            // instead of finishing the loop (the verdict no longer
            // matters; the leader's DestroyParallelContext join is waiting
            // on this task). Recorded hazard: the inc-2b agg SWEEP
            // DeadlineExceeded diagnosis names exactly this surface.
            if self.failed.load(Ordering::SeqCst) || self.crossed.load(Ordering::SeqCst) {
                return Ok(false);
            }
            let blen = file.part_len(sl as u32) as usize;
            if blen == 0 {
                continue;
            }
            let rows = blen / width;
            // Slice TRANSIENT bound (the synth table alone; the merged
            // bucket has its own exact check below). Rows over-count
            // duplicates → conservative; same-VALUE duplicates never slice
            // apart (they share every mix64 byte), so a slice dominated by
            // copies of few values recurses to the cap and refuses even
            // though its deduplicated table would fit — the inc-2b
            // limitation, value-inverted (ledger item: streaming replay).
            let est_slice = rows.saturating_mul(16).saturating_mul(3) / 2
                + rows.min(groups).saturating_mul(per_group)
                + if bytes_mode { blen } else { 0 };
            if est_slice > budget {
                if depth + 1 > distinct_split_depth_cap() {
                    return Ok(false);
                }
                self.combine_splits.fetch_add(1, Ordering::Relaxed);
                self.split_depth_max.fetch_max((depth + 1) as u64, Ordering::Relaxed);
                let mut router = DstSubRouter::new(self, set, b, depth + 1);
                stream_part_dst(&self.spec, file, sl as u32, |chunk| {
                    router.absorb(&self.spec, chunk)
                })?;
                router.flush()?;
                if !self.split_slices_into(
                    merger,
                    b,
                    set,
                    &router.file,
                    depth + 1,
                    groups,
                    per_group,
                )? {
                    return Ok(false);
                }
                continue;
            }
            let ctx = ::mcx::MemoryContext::new("m35-dst-split-read");
            let Some(mut rd) = file.read_part(ctx.mcx(), sl as u32)? else { continue };
            let bytes = rd.read_to_end()?;
            rd.close()?;
            let synth = pd_table_from_spill(&self.spec, &bytes)?;
            merger.absorb(&synth, b);
            drop(synth);
            // The DEDUP-AWARE final bound: exact, capacity-based. The
            // merged bucket must fit to be emitted at all — a crossing
            // here is a TRUE-cardinality overflow no slicing can convert.
            if merger.mem_bytes() > budget {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// One-byte value-slice routing vocabulary (each recursion level consumes
/// one mix64 byte).
const DST_SPLIT_SLICES: usize = 256;
/// Router epoch-flush threshold (mirrors the agg SubRouter's).
const DST_SPLIT_FLUSH_BYTES: usize = 16 << 20;

/// Combine-split depth cap: mix64(value) bytes (top-down) the recursion may
/// consume (depth 1 = the first split). Default 3; clamped to the routing
/// vocabulary (≤6).
fn distinct_split_depth_cap() -> u32 {
    static N: OnceLock<u32> = OnceLock::new();
    crate::once_val(&N, || {
        std::env::var("PGRUST_RUNTIME_DISTINCT_SPILL_DEPTH")
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .unwrap_or(3)
            .clamp(1, 6)
    })
}

/// Stream one spill partition's records to `f`, RECORD-ALIGNED: int-mode
/// specs ride the fixed-width `stream_part_rows` chunker verbatim;
/// bytes-key specs (distinct-bytes car) parse the self-describing `rec_len`
/// prefixes and carry the partial tail across reads, so `f` only ever sees
/// whole records (the router's parse contract). Fail-closed on torn tails
/// and malformed lengths.
fn stream_part_dst(
    spec: &PdSpec,
    file: &::spillset::SpillFile,
    part: u32,
    mut f: impl FnMut(&[u8]) -> PgResult<()>,
) -> PgResult<()> {
    if !::nodeagg::pd_spill_bytes_mode(spec) {
        let width = pd_spill_record_width(spec);
        return super::runtime_agg::stream_part_rows(file, part, width, |chunk| f(chunk));
    }
    let min_width = ::nodeagg::pd_spill_min_record_width(spec);
    let ctx = ::mcx::MemoryContext::new("m35-dst-split-read");
    let Some(mut rd) = file.read_part(ctx.mcx(), part)? else { return Ok(()) };
    let mut buf: Vec<u8> = vec![0u8; 1 << 20];
    let mut filled = 0usize;
    loop {
        if filled == buf.len() {
            // One record larger than the buffer: grow (bounded by the
            // spill writer's own epoch buffering; a corrupt rec_len fails
            // the length checks below before unbounded growth).
            buf.resize(buf.len() * 2, 0);
        }
        let n = rd.read(&mut buf[filled..])?;
        if n == 0 {
            rd.close()?;
            if filled != 0 {
                return Err(Box::new(PgError::new(
                    ERROR,
                    "torn distinct bytes spill record (partial tail) in split stream",
                )));
            }
            return Ok(());
        }
        filled += n;
        // Longest prefix of COMPLETE records.
        let mut usable = 0usize;
        loop {
            if filled - usable < 8 {
                break;
            }
            let rec_len =
                u64::from_ne_bytes(buf[usable..usable + 8].try_into().unwrap()) as usize;
            if rec_len < min_width || rec_len % 8 != 0 {
                return Err(Box::new(PgError::new(
                    ERROR,
                    "torn distinct bytes spill record (rec_len) in split stream",
                )));
            }
            if filled - usable < rec_len {
                break;
            }
            usable += rec_len;
        }
        if usable > 0 {
            f(&buf[..usable])?;
            buf.copy_within(usable..filled, 0);
            filled -= usable;
        }
    }
}

/// Bounded value-slice router (inc-3b, the agg SubRouter's twin): records
/// absorb into 256 in-memory buffers by the `mix64(value)` byte at `depth`
/// and epoch-flush to a combine-task-owned spill file when the staged total
/// crosses [`DST_SPLIT_FLUSH_BYTES`] — partition-ascending per epoch,
/// extents accumulating across epochs (the substrate contract).
struct DstSubRouter {
    file: ::spillset::SpillFile,
    bufs: Vec<Vec<u8>>,
    staged: usize,
    depth: u32,
}

impl DstSubRouter {
    fn new(
        shared: &RuntimeDistinctShared,
        set: &Arc<::spillset::SpillSet>,
        b: usize,
        depth: u32,
    ) -> DstSubRouter {
        let uniq = shared.split_uniq.fetch_add(1, Ordering::Relaxed);
        DstSubRouter {
            file: ::spillset::SpillFile::new(
                Arc::clone(set),
                format!("m35-dstcmb-p{b}-d{depth}-u{uniq}"),
                DST_SPLIT_SLICES as u32,
            ),
            bufs: vec![Vec::new(); DST_SPLIT_SLICES],
            staged: 0,
            depth,
        }
    }

    fn absorb(&mut self, spec: &PdSpec, records: &[u8]) -> PgResult<()> {
        if records.is_empty() {
            return Ok(());
        }
        pd_route_value_records(spec, records, self.depth, &mut self.bufs)?;
        self.staged += records.len();
        if self.staged >= DST_SPLIT_FLUSH_BYTES {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> PgResult<()> {
        if self.staged == 0 {
            return Ok(());
        }
        let ctx = ::mcx::MemoryContext::new("m35-dst-split-write");
        let mut w = self.file.begin_epoch(ctx.mcx())?;
        for (s, buf) in self.bufs.iter_mut().enumerate() {
            if !buf.is_empty() {
                w.write_part(s as u32, buf)?;
                buf.clear();
            }
        }
        w.finish()?;
        self.staged = 0;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Worker (helper) side: thread-local executor + the accept morsel body.
// ---------------------------------------------------------------------------

struct WorkerExec {
    qd: ::types_portal::QueryDescHandle,
    /// Per-helper detoast scratch context (reset per row when a bytes set
    /// detoasts into per-tuple memory).
    tmp: EcxtId,
    reset_tmp: bool,
    /// THIS helper contributed an error (take the release/abort teardown).
    errored: std::cell::Cell<bool>,
    /// EA-on-morsels: this worker's cumulative instrument partial (written
    /// only when the engagement carries `ea_instr_slots`).
    instr: std::cell::RefCell<InstrumentPartial>,
    /// GL-VECACCEPT-1 per-worker lane scratch (canonicalized granule
    /// lanes + the builder's hash/gid lanes) — allocated once, reused
    /// across claims; unused (empty) when the engagement runs the
    /// incumbent per-row accept.
    vec_scratch: std::cell::RefCell<VecClaimScratch>,
}

/// GL-VECACCEPT-1 canonicalization scratch: one i64 lane per read column
/// (the lanes borrow `&mut ss` one at a time — the topn-heap two-phase
/// borrow discipline — so each is copied out before the fused passes run).
#[derive(Default)]
struct VecClaimScratch {
    keys: Vec<i64>,
    vals: Vec<i64>,
    riders: Vec<Vec<i64>>,
    pd: PdVecScratch,
}

thread_local! {
    static WORKER_EXEC: std::cell::RefCell<Option<WorkerExec>> =
        const { std::cell::RefCell::new(None) };
}

fn mark_self_errored() {
    WORKER_EXEC.with(|cell| {
        if let Some(ex) = cell.borrow().as_ref() {
            ex.errored.set(true);
        }
    });
}

/// The per-morsel accept feed: rows into the worker's `PdSinkLocal`. A
/// budget crossing SPILLS an epoch when the M3.5 arm is on and the shape is
/// exactly spillable; otherwise it flips `crossed` and drops the remainder
/// of the morsel (the RG is aborting; nothing is emitted anywhere).
struct PdAcceptSink<'a> {
    shared: &'a RuntimeDistinctShared,
    local: &'a mut DistinctSinkLocal,
    worker: usize,
    tmp: EcxtId,
    reset_tmp: bool,
    crossed: bool,
    /// Locality-cap memo: the cap is armed on the engagement but THIS
    /// Local's shape refused the epoch drain (not spill-eligible / arm
    /// off) — stop re-probing per row; behavior degrades to today's
    /// budget-only cadence exactly.
    locality_denied: bool,
    /// EA-on-morsels row funnel (Some only under EXPLAIN ANALYZE): scanned
    /// tallied window-grain in accept_batch, survivors per emitted row.
    tally: Option<&'a mut EaRowTally>,
}

impl PdAcceptSink<'_> {
    /// M3.5 accept-side spill (design §4): on `PdFeed::Crossed`, write the
    /// Local's accumulated set values to its spill file as ONE epoch —
    /// partitions 0..255 contiguous in the freeze partition law's order,
    /// `seen_null`/vocab/group table kept in memory — then reset the sets'
    /// values so accept continues bounded. `Ok(false)` = refused (arm off,
    /// or a shape/economics face we cannot spill exactly): the caller falls
    /// through to the phase-1 Crossed abort, fail-closed.
    ///
    /// `locality` = the flush was cap-triggered, not budget-triggered
    /// (distinct-sidecar-cap lane): the group-table-dominated
    /// worthwhileness gate is vacated — the cap on `spill_freeable_bytes`
    /// IS the worthwhileness predicate (the flush releases exactly the
    /// capped set memory by construction) — and a refusal is benign (the
    /// caller keeps accepting under the budget law; never a crossing).
    fn try_spill_epoch(&mut self, locality: bool) -> PgResult<bool> {
        // Every refusal below names its branch on the trace channel (the agg
        // arm's refuse(why) pattern — inc-3a followup: the battery -82184
        // fail-closed was invisible without a reason line).
        let Some(set) = &self.shared.spill_set else {
            trace_feed("runtime-distinct: spill refused (arm off / no spill set)");
            return Ok(false);
        };
        let DistinctSinkLocal { pd, spill } = &mut *self.local;
        if !pd.pd_spill_eligible() {
            trace_feed("runtime-distinct: spill refused (shape not spill-eligible)");
            return Ok(false);
        }
        // Worthwhileness (fail-closed): a group-table-dominated crossing
        // cannot be helped by value spill — the epoch must RELEASE a
        // meaningful fraction of the budget or the arm refuses. The yardstick
        // is the capacity bytes the flush frees (`spill_freeable_bytes` =
        // total set memory; the reset shrinks the sets), NOT the value
        // payload: crossings land right after capacity doublings, where
        // payload is only ~1/6..1/3 of set memory and a payload-based gate
        // deterministically refused legitimate value-dominated shapes (the
        // grouped-distinct lockstep corpus; see the PdBuilder doc).
        let budget = self.shared.spec.worker_budget;
        if !locality && pd.pd_spill_freeable_bytes() < budget / 4 {
            trace_feed("runtime-distinct: spill refused (group-table-dominated crossing)");
            return Ok(false);
        }
        let file = spill.get_or_insert_with(|| {
            ::spillset::SpillFile::new(
                Arc::clone(set),
                ::spillset::SpillSet::file_name("dst", 0, self.worker),
                PD_SINK_GROUP_PARTS as u32,
            )
        });
        let before = file.spilled_bytes();
        // Open-per-event on the owning worker thread (§2 amendment): the
        // BufFile handle lives inside this flush event alone. Values reset
        // only after the epoch COMMITS — an error path loses nothing.
        let ctx = ::mcx::MemoryContext::new("m35-dst-spill-write");
        let mut w = file.begin_epoch(ctx.mcx())?;
        pd.pd_spill_emit(&mut |p, bytes| w.write_part(p, bytes))?;
        w.finish()?;
        pd.pd_spill_reset_values();
        self.shared.spill_epochs.fetch_add(1, Ordering::Relaxed);
        self.shared
            .spilled_bytes
            .fetch_add(file.spilled_bytes() - before, Ordering::Relaxed);
        if locality {
            trace_feed(&format!(
                "runtime-distinct: locality cap engaged (cap={})",
                self.shared.locality_cap.unwrap_or(0)
            ));
        }
        Ok(true)
    }

    /// GL-VECACCEPT-1: the vectorized whole-granule accept for one morsel
    /// claim (the incumbent `drain_pipeline` per-row emit/deform/accept is
    /// bypassed wholesale). Per granule: hand the decoded lanes out whole
    /// (the topn-heap direct feed), canonicalize each into worker scratch
    /// (one `&mut ss` lane borrow at a time), then the builder's fused
    /// passes — batch hash → prefetched batch probe/resolve → columnar
    /// rider folds → the staged distinct-set feed (dup-skip, window
    /// flushes, and the window-grain budget law all unchanged; a crossing
    /// runs the SAME spill-epoch law and resumes the set feed — phases 1-3
    /// never spill). The budget/locality checks move to granule grain (a
    /// documented one-granule overshoot of the incumbent's per-row check —
    /// same law family as the staged one-batch-overshoot contract).
    ///
    /// A lane the part cannot serve directly (dict-coded — impossible for
    /// the admitted int-family columns, kept fail-closed) flips `crossed`:
    /// RG abort → serial rerun, nothing consumed.
    fn vec_claim<'mcx>(
        &mut self,
        cols: &VecCols,
        vs: &mut VecClaimScratch,
        ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
        range: runtime::MorselRange,
    ) -> PgResult<()> {
        ::nodeseqscan::seq_scan_set_morsel_range(ss, estate, range.start, range.end)?;
        vs.riders.resize_with(cols.riders.len(), Vec::new);
        let mut rows = 0u64;
        let mut granules = 0u64;
        'claim: loop {
            ::postgres_seams::check_for_interrupts::call()?;
            let Some((nrows, _base)) = ::nodeseqscan::seq_scan_topn_direct_next_granule(ss)?
            else {
                break;
            };
            let n = nrows as usize;
            // Canonicalize the granule's lanes (borrow-scoped, one at a
            // time; decode-on-demand is granule-memoized underneath).
            let mut lane_into = |col: u16, kind: PdInt, out: &mut Vec<i64>| -> bool {
                let Some(lane) = ::nodeseqscan::seq_scan_topn_direct_lane(ss, col as usize)
                else {
                    return false;
                };
                out.clear();
                out.extend(lane[..n].iter().map(|&d| kind.read(d)));
                true
            };
            let mut ok = lane_into(cols.key_col, cols.key_kind, &mut vs.keys)
                && lane_into(cols.set_col, cols.set_kind, &mut vs.vals);
            for (vi, r) in cols.riders.iter().enumerate() {
                if !ok {
                    break;
                }
                if let Some((col, kind)) = r {
                    ok = lane_into(*col, *kind, &mut vs.riders[vi]);
                }
            }
            if !ok {
                // Fail-closed: the direct feed cannot serve a lane —
                // refuse to the serial arm (never an error, nothing kept).
                trace_feed("runtime-distinct: vecaccept lane refused — serial fallback");
                self.crossed = true;
                return Ok(());
            }
            // Phases 1-3 (group resolve + rider folds; infallible wrt the
            // budget — group growth is metered at the staged flush below,
            // the incumbent staged law).
            let riders: Vec<Option<&[i64]>> = cols
                .riders
                .iter()
                .enumerate()
                .map(|(vi, r)| r.as_ref().map(|_| vs.riders[vi].as_slice()))
                .collect();
            self.local.pd.vec_resolve_fold(&vs.keys, &riders, &mut vs.pd);
            // Phase 4: the staged set feed, resumable across spill epochs.
            let mut at = 0usize;
            loop {
                let (feed, consumed) =
                    self.local.pd.vec_stage_sets(&vs.pd.gids, &vs.vals, at)?;
                at = consumed;
                if feed == PdFeed::Ok {
                    break;
                }
                if !self.try_spill_epoch(false)? {
                    self.crossed = true;
                    break 'claim;
                }
            }
            // Locality flush at granule grain (the incumbent's per-row
            // check, one-granule overshoot; same cap, same epoch law).
            if let Some(cap) = self.shared.locality_cap {
                if !self.locality_denied
                    && self.local.pd.pd_spill_freeable_bytes() >= cap
                    && !self.try_spill_epoch(true)?
                {
                    self.locality_denied = true;
                }
            }
            rows += n as u64;
            granules += 1;
        }
        self.shared.vec_rows.fetch_add(rows, Ordering::Relaxed);
        self.shared.vec_granules.fetch_add(granules, Ordering::Relaxed);
        Ok(())
    }
}

impl<'mcx> Sink<'mcx> for PdAcceptSink<'_> {
    fn accept(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<SinkFeed> {
        if self.crossed {
            return Ok(SinkFeed::NeedMore);
        }
        let crossed = self.local.pd.accept(estate, tuple, self.tmp)? == PdFeed::Crossed;
        if self.reset_tmp {
            estate.reset_expr_context(self.tmp);
        }
        if crossed {
            if !self.try_spill_epoch(false)? {
                self.crossed = true;
            }
        } else if let Some(cap) = self.shared.locality_cap {
            // Locality flush (distinct-sidecar-cap): bound the per-worker
            // set working footprint at `cap` bytes through an EARLY spill
            // epoch. A refusal (shape not spill-eligible) is memoized and
            // benign — accept continues under the budget law exactly as
            // today. One field load + compare per row when disarmed-by-shape
            // or under the cap.
            if !self.locality_denied
                && self.local.pd.pd_spill_freeable_bytes() >= cap
                && !self.try_spill_epoch(true)?
            {
                self.locality_denied = true;
            }
        }
        Ok(SinkFeed::NeedMore)
    }

    fn finish(&mut self, _estate: &mut EStateData<'mcx>) -> PgResult<()> {
        Ok(())
    }
}

impl<'mcx> BatchSink<'mcx> for PdAcceptSink<'_> {
    /// The default loop verbatim (same emit, same accept, same order — the
    /// trait's byte-identity rule), plus the EA row-funnel tallies, which
    /// are branch-dead when `tally` is None (every non-EA path).
    fn accept_batch<E: BatchEmit<'mcx>>(
        &mut self,
        emit: &mut E,
        pos: u32,
        n: u32,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()> {
        if let Some(t) = self.tally.as_deref_mut() {
            // Window-grain: the staged batch rows are the pre-qual funnel
            // stage (emit re-checks the qual per row below); skipped
            // emit-dead rows still count as scanned, exactly as before.
            t.scanned += (n - pos) as u64;
        }
        // Emit-dead word skip (`live_sel` — the BatchSink default's
        // contract): cleared bits answer `emit` with None, so the surviving
        // feed (and the `survived` tally) is identical.
        let live = emit.live_sel();
        ::exectuples::for_each_live(live.as_ref().map(|w| &w[..]), pos, n, |i| -> PgResult<()> {
            if let Some(slot) = emit.emit(i, estate)? {
                if let Some(t) = self.tally.as_deref_mut() {
                    t.survived += 1;
                }
                match self.accept(slot, estate)? {
                    SinkFeed::NeedMore => {}
                    // A breaker never fills; see `drain_pipeline`'s Paused arm.
                    SinkFeed::Full => {
                        return Err(Box::new(::types_error::PgError::error(
                            "lane-v2 batch feed: breaker sink returned Full".to_string(),
                        )))
                    }
                }
            }
            Ok(())
        })
    }
}

impl RuntimeDistinctShared {
    fn morsel_body(
        &self,
        local: &mut DistinctSinkLocal,
        worker: usize,
        range: runtime::MorselRange,
    ) -> PgResult<()> {
        // TIMER mode: the claim's clock pair (§5 — the ONLY TIMING ON cost).
        let ea_t0 = (self.ea_timer && self.ea_instr_slots.is_some())
            .then(|| self.ea_epoch.elapsed().as_nanos() as u64);
        WORKER_EXEC.with(|cell| {
            let b = cell.borrow();
            let Some(ex) = b.as_ref() else {
                return Err(Box::new(PgError::new(
                    ERROR,
                    "runtime distinct morsel without a bound executor",
                )));
            };
            let (tmp, reset_tmp) = (ex.tmp, ex.reset_tmp);
            crate::querydesc::with_qd(ex.qd, |q| {
                let x = q.exec.as_mut().expect("runtime distinct worker executor state");
                x.with_mut(|d| -> PgResult<()> {
                    let estate = &mut d.estate;
                    let ss = distinct_worker_scan(d.planstate.as_mut())?;
                    // train-12 composition: the heap lane generalized the
                    // positioner to AM-dispatched seq_scan_set_morsel_range
                    // (PgResult<()>); this arm admits only pgrcolumnar scans, so
                    // the former not-pgrcolumnar branch is unreachable by
                    // construction.
                    ::nodeseqscan::seq_scan_set_morsel_range(
                        ss,
                        estate,
                        range.start,
                        range.end,
                    )?;
                    // EA-on-morsels: borrow the worker's cumulative partial
                    // for the drain's row funnel (None on every non-EA path).
                    let ea = self.ea_instr_slots.is_some();
                    let mut ipb = ex.instr.borrow_mut();
                    let mut sink = PdAcceptSink {
                        shared: self,
                        local,
                        worker,
                        tmp,
                        reset_tmp,
                        crossed: false,
                        locality_denied: false,
                        tally: ea.then_some(&mut ipb.rows),
                    };
                    // GL-VECACCEPT-1: the vectorized whole-granule drive
                    // owns the claim when armed; the incumbent per-row
                    // emit/accept pipeline is the default, byte-for-byte.
                    let fed = match &self.vec {
                        Some(cols) => {
                            let mut vsb = ex.vec_scratch.borrow_mut();
                            sink.vec_claim(cols, &mut vsb, ss, estate, range.clone())
                        }
                        None => drain_pipeline(
                            ss,
                            &mut SeqScanSource,
                            &mut SeqScanFilterProject,
                            &mut sink,
                            estate,
                        ),
                    };
                    let crossed = sink.crossed;
                    fed?;
                    // Claim fold + overwrite export (EXACT — accumulate in
                    // the worker state, export at claim end; the dop1-tax
                    // contract, ea-morsels.md §2).
                    if let Some(slots) = &self.ea_instr_slots {
                        ipb.claims += 1;
                        ipb.granules += range.end - range.start;
                        if let Some(c) = ::nodeseqscan::seq_scan_cb_ea_counters(ss) {
                            ipb.prune = c;
                        }
                        if let Some(t0) = ea_t0 {
                            let t1 = self.ea_epoch.elapsed().as_nanos() as u64;
                            runtime_instr::ea_claim_time(&mut ipb, t0, t1);
                        }
                        *slots[worker].lock().unwrap_or_else(|p| p.into_inner()) = Some(*ipb);
                    }
                    if crossed {
                        trace_feed(
                            "runtime distinct worker budget crossed; aborting to serial fallback",
                        );
                        self.cross();
                    }
                    Ok(())
                })
            })
        })
    }
}

/// The worker plan tree is the SCAN SUBTREE alone (workers never run the
/// Agg or the Sort — accept_local drives scan → PREWHERE → project into the
/// PdBuilder; the worker pstmt's planTree is the SeqScan node).
fn distinct_worker_scan<'a, 'mcx>(
    planstate: Option<&'a mut crate::procnode::PlanStateNode<'mcx>>,
) -> PgResult<&'a mut ::nodeseqscan::SeqScanState<'mcx>> {
    let Some(crate::procnode::PlanStateNode::SeqScan(ss)) = planstate else {
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime distinct worker plan is not a SeqScan root",
        )));
    };
    Ok(ss)
}

// ---------------------------------------------------------------------------
// Helper entry + POST_TASK_PARK drive (the runtime_scan ceremony, with this
// arm's payload type; the hook registries are multi-registrant and every
// hook no-ops on foreign payloads).
// ---------------------------------------------------------------------------

fn runtime_distinct_worker_main(_shared: &parallel::ParallelShared) -> PgResult<()> {
    Ok(())
}

fn runtime_distinct_post_task_park(shared: &parallel::ParallelShared) {
    let Some(private) = shared.private() else {
        // F1 observability: a context with NO private payload can never be
        // driven by any arm — trace it (foreign-payload downcast misses stay
        // silent below: every arm's hook runs for every worker by design).
        lane_trace("runtime-distinct: post-task-park without a private payload");
        return;
    };
    let Ok(payload) = private.downcast::<RuntimeDistinctShared>() else { return };
    // Every LAUNCHED helper bumps `exited` exactly once, on EVERY exit path
    // (the leader's liveness reap counts these against `launched`).
    // HOOK-frame placement (the scan arm's law): the standing driver reuses
    // helper_drive and must NOT bump — standing exits ride the board's
    // claimed/detached accounting.
    let _exit = super::runtime_agg::ExitBump(&payload.exited);
    let r = catch_unwind(AssertUnwindSafe(|| helper_drive(shared, &payload)));
    if r.is_err() {
        payload.fail(PgError::new(ERROR, "runtime distinct helper panicked").into());
    }
    latch::SetLatch(::types_storage::latch::LatchHandle::proc(
        shared.parallel_leader_proc_number,
    ));
}

/// The standing driver (M2 inc-1, parallel::set_standing_driver): the
/// POST_TASK_PARK body minus the ExitBump; exit-committed unwinds (FATAL)
/// rethrow to the gang glue (a terminated worker must die).
fn runtime_distinct_standing_driver(shared: &parallel::ParallelShared) {
    let Some(private) = shared.private() else { return };
    let Ok(payload) = private.downcast::<RuntimeDistinctShared>() else { return };
    let r = catch_unwind(AssertUnwindSafe(|| helper_drive(shared, &payload)));
    if let Err(unwind) = r {
        payload
            .fail(PgError::new(ERROR, "runtime distinct standing executor panicked").into());
        latch::SetLatch(::types_storage::latch::LatchHandle::proc(
            shared.parallel_leader_proc_number,
        ));
        if parallel::standing::is_exit_unwind(&*unwind) {
            std::panic::resume_unwind(unwind);
        }
        return;
    }
    latch::SetLatch(::types_storage::latch::LatchHandle::proc(
        shared.parallel_leader_proc_number,
    ));
}

fn helper_drive(shared: &parallel::ParallelShared, payload: &Arc<RuntimeDistinctShared>) {
    let _ = shared;
    // Liveness-battery injection (test-only, default-off): the wedge-class
    // exit — panic before binding or driving; the reap must convert it into
    // a prompt error (scripts/runtime-liveness-e2e.sh).
    super::test_helper_panic("distinct");
    // F1 fail-closed accounting: a helper that cannot participate must NEVER
    // vanish silently — every early exit below counts itself as a refusal
    // (the leader's started==0 && refused>=launched probe is its fallback
    // signal) and traces why.
    let Some(target) = payload.pcxt_shared.get() else {
        lane_trace("runtime-distinct: helper refused (no pcxt shared)");
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) else {
        lane_trace("runtime-distinct: helper refused (rg gone)");
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let Some(lane) = payload.rt.acquire_external_lane() else {
        lane_trace("runtime-distinct: helper refused (no external lane)");
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let mut local = lane.local();
    let lane = std::cell::RefCell::new(Some(lane));
    let entered = std::cell::Cell::new(false);
    let bound = parallel::with_query_task_binding(target, || {
        entered.set(true);
        payload.started.fetch_add(1, Ordering::SeqCst);
        drive_bound(payload, &mut local, &rg, &mut lane.borrow_mut())
    });
    match bound {
        Ok(()) => {}
        Err(e) => {
            if entered.get() {
                payload.fail(e);
                // F1 liveness (the agg-arm wedge mechanism, closed here
                // too): a helper that errored BEFORE joining the drive
                // (build_worker_exec failure) has aborted the RG via
                // fail() — but an aborted PINNED RG still needs a driver to
                // run invalidate/finalize/complete, or the leader's waiter
                // parks forever. Drive the closed generation to completion
                // (pure protocol cleanup); post-drive errors find it already
                // complete and skip.
                if rg.try_outcome().is_none() {
                    rg.abort();
                    let _ = payload.rt.drive_pinned(&mut local, &rg);
                }
            } else {
                lane_trace(&format!(
                    "runtime-distinct: helper bind refused: {}",
                    e.message()
                ));
                payload.refused.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

fn drive_bound(
    payload: &Arc<RuntimeDistinctShared>,
    local: &mut runtime::WorkerLocal,
    rg: &runtime::RgHandle,
    lane: &mut Option<runtime::ExternalLane>,
) -> PgResult<()> {
    build_worker_exec(payload)?;
    let _end = super::standing_channel::drive_pool_serve(&payload.rt, local, rg, lane);
    let self_errored =
        WORKER_EXEC.with(|cell| cell.borrow().as_ref().is_some_and(|ex| ex.errored.get()));
    let teardown = teardown_worker_exec(!self_errored);
    if self_errored {
        // m2-integration port of the agg lane's binder abort-path fix (also
        // applied to the scan arm): a released (not finished) executor may
        // still hold registered snapshots — the binder's NORMAL unbind
        // asserts a cleared xmin, so route through its transaction-ABORT
        // path by returning an error. The real error was recorded first
        // (fail() is first-wins), so this marker never surfaces; budget
        // crossings do not set the errored flag and keep their serial
        // fallback path.
        teardown?;
        return Err(PgError::new(
            ERROR,
            "runtime distinct worker unwound (recorded upstream)",
        )
        .into());
    }
    teardown
}

fn build_worker_exec(payload: &Arc<RuntimeDistinctShared>) -> PgResult<()> {
    WORKER_EXEC.with(|cell| -> PgResult<()> {
        if let Some(stale) = cell.borrow_mut().take() {
            crate::querydesc::release_query_desc_seam(stale.qd);
        }
        // SAFETY: leader-arena pstmt, alive until DestroyParallelContext
        // joins this helper (SendConst contract).
        let pstmt: &PlannedStmt<'_> = unsafe { &*payload.pstmt.0 };
        let qd = crate::querydesc::create_query_desc_seam(
            pstmt,
            &payload.query_text,
            Some(::snapmgr::GetActiveSnapshot()),
            None,
            ::types_dest::CommandDest::None,
            ::types_portal::ParamListHandle::NULL,
            ::types_portal::QueryEnvHandle::NULL,
            0,
        )?;
        let armed = (|| -> PgResult<EcxtId> {
            crate::execmain::executor_start_seam(qd, payload.eflags)?;
            crate::querydesc::with_qd(qd, |q| {
                let x = q.exec.as_mut().expect("runtime distinct worker ExecutorStart");
                x.with_mut(|d| -> PgResult<EcxtId> {
                    let estate = &mut d.estate;
                    let ss = distinct_worker_scan(d.planstate.as_mut())?;
                    super::arm_scan_staging(
                        ss,
                        estate,
                        super::ScanFeedShape::RowFeed {
                            ctx: "runtime distinct worker feed",
                            stitch: true,
                        },
                    )?;
                    Ok(estate.exec_assign_expr_context())
                })
            })
        })();
        match armed {
            Ok(tmp) => {
                *cell.borrow_mut() = Some(WorkerExec {
                    qd,
                    tmp,
                    // Bytes anywhere (set values or the distinct-bytes car's
                    // text group keys): the per-row detoast scratch resets.
                    reset_tmp: payload.spec.any_bytes(),
                    errored: std::cell::Cell::new(false),
                    instr: Default::default(),
                    vec_scratch: Default::default(),
                });
                Ok(())
            }
            Err(e) => {
                crate::querydesc::release_query_desc_seam(qd);
                Err(e)
            }
        }
    })
}

fn teardown_worker_exec(clean: bool) -> PgResult<()> {
    WORKER_EXEC.with(|cell| -> PgResult<()> {
        let Some(ex) = cell.borrow_mut().take() else { return Ok(()) };
        if clean {
            let r = crate::execmain::executor_finish_seam(ex.qd)
                .and_then(|()| crate::execmain::executor_end_seam(ex.qd));
            match r {
                Ok(()) => {
                    crate::querydesc::free_query_desc_seam(ex.qd);
                    Ok(())
                }
                Err(e) => {
                    crate::querydesc::release_query_desc_seam(ex.qd);
                    Err(e)
                }
            }
        } else {
            crate::querydesc::release_query_desc_seam(ex.qd);
            Ok(())
        }
    })
}

fn runtime_distinct_private_shutdown(private: &(dyn std::any::Any + Send + Sync)) {
    let Some(payload) = private.downcast_ref::<RuntimeDistinctShared>() else { return };
    let rg = payload.rg.get().and_then(|w| w.upgrade());
    if let Some(rg) = &rg {
        rg.abort();
    }
    // Standing channel (M2 inc-1): complete the standing join on leader
    // unwind paths (standing_channel::shutdown_standing_join).
    super::standing_channel::shutdown_standing_join(&payload.standing, rg.as_ref(), &|rg| {
        drain_rg(payload.rt, rg)
    });
}

fn ensure_hooks_registered() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        parallel::register_parallel_worker_entrypoint(
            "pgrust_runtime_distinct_main",
            runtime_distinct_worker_main,
        );
        parallel::register_parallel_post_task_park(runtime_distinct_post_task_park);
        parallel::register_parallel_private_shutdown(runtime_distinct_private_shutdown);
    });
}

/// GL-VECACCEPT-1 knob — DEFAULT ON (flip staged off the 100M verdict:
/// vec/base 0.64-0.72 at every measured (dop, shape) cell on fast-profile
/// AND the shipped dist binary; flip-preview at unpinned defaults −6..−10%;
/// parity byte-equal on every arm of every job; census exact; the
/// symbolized profile moved the accept-substrate samples exactly as
/// chartered — letter GL-VECACCEPT-1 §5). t35 flipped-kill spelling:
/// `PGRUST_RUNTIME_AGG_VECACCEPT=0|off` restores the incumbent per-row
/// accept pipeline byte-identically (the vectorized schedule feeds the
/// same kernels in the same row order — set bytes identical by
/// construction, unit-pinned).
fn runtime_agg_vecaccept_enabled() -> bool {
    // Knob unification (GL-VECACCEPT-2 flip prep): the lane posture is
    // shared with the K2 agg drain — one default, one kill.
    super::vecaccept_lane_enabled()
}

/// GL-VECACCEPT-1 admission (fail-closed; `None` = the incumbent per-row
/// accept, byte-for-byte): knob armed; non-EA session (the EA row funnel
/// is per-row machinery — a named residual, not a law); the staged
/// set-insert lane armed ([`::nodeagg::pd_batch_insert_enabled`] — the
/// Local-side `vec_admissible` twin); the [`pd_vec_plan`] shape (single
/// int-family group key, single int-family distinct set, int-family
/// riders); a QUAL-FREE scan (the direct lane walk applies no filters);
/// and a Var-only projection census mapping every referenced att onto a
/// scan column the direct feed can serve (the topn-heap tlist_map law).
fn vec_cols(
    spec: &PdSpec,
    ss: &::nodeseqscan::SeqScanState<'_>,
    scan_plan: &::types_nodes::plannodes::SeqScan<'_>,
    outer_desc: &::types_tuple::TupleDescData<'static>,
    ea: bool,
) -> Option<VecCols> {
    if !runtime_agg_vecaccept_enabled() || ea || !::nodeagg::pd_batch_insert_enabled() {
        return None;
    }
    // The direct granule feed serves pgrcolumnar parts only (the tableam
    // drive ERRORS on heap scans — "the arm admits pgrcolumnar only"). On
    // the member's base this was structural: the distinct arm itself only
    // engaged columnar granule starts. The t43 stack generalized the arm's
    // task source to heap morsels, so the vec rider must now refuse heap
    // itself — None = the incumbent per-row accept, the member's original
    // heap semantics (caught by the cbstore-lane off-arm heap corpus).
    if !::nodeseqscan::seq_scan_is_pgrcolumnar(ss) {
        return None;
    }
    if scan_plan.scan.plan.qual.iter().next().is_some() {
        return None;
    }
    let plan = pd_vec_plan(spec)?;
    let natts = outer_desc.natts as usize;
    let tlist_map: Vec<u16> = match ss.ss.ps_ProjInfo.as_ref() {
        // No projection: outer resno j is scan attno j (physical tlist).
        None => (0..natts as u16).collect(),
        // Projected scans admit only the pure Var-copy census.
        Some(p) => match p.pi_state.scan_proj_cols() {
            Some(cols) => {
                if cols.any_arith() || cols.n as usize != natts {
                    return None;
                }
                cols.cols[..natts]
                    .iter()
                    .map(|c| match *c {
                        ::execexpr::ScanProjCol::Var { attnum } => Some(attnum),
                        _ => None,
                    })
                    .collect::<Option<Vec<u16>>>()?
            }
            // Single-column pure-Var projection compiles to a kernel (no
            // step program => no census) — the topn-heap admission's arm.
            None => match p.pi_state.kernel() {
                ::execexpr::Kernel::JustAssignVar {
                    src: ::execexpr::SlotSrc::Scan,
                    attnum,
                    resultnum: 0,
                }
                | ::execexpr::Kernel::JustAssignVarVirt {
                    src: ::execexpr::SlotSrc::Scan,
                    attnum,
                    resultnum: 0,
                } if natts == 1 => vec![attnum],
                _ => return None,
            },
        },
    };
    let map = |att: u16| -> Option<u16> { tlist_map.get(att as usize).copied() };
    Some(VecCols {
        key_col: map(plan.key_att)?,
        key_kind: plan.key_kind,
        set_col: map(plan.set_att)?,
        set_kind: plan.set_kind,
        riders: plan
            .riders
            .iter()
            .map(|r| match r {
                None => Some(None),
                Some((att, kind)) => map(*att).map(|c| Some((c, *kind))),
            })
            .collect::<Option<Vec<_>>>()?,
    })
}

/// M3.5 spill arm kill switch: ON by default when the sink engages
/// (refusal→engagement is the charter); `PGRUST_RUNTIME_DISTINCT_SPILL=0`
/// restores the phase-1 budget refusal exactly.
fn distinct_spill_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || std::env::var("PGRUST_RUNTIME_DISTINCT_SPILL").as_deref() != Ok("0"))
}

/// GL-LOWDIST-4 B1 heap-feed knob (t35 law: DEFAULT OFF for the letter; ON
/// iff exactly `1`/`on`; the flip rides the measured verdict). Widens BOTH
/// distinct sinks' admission from pgrcolumnar-only to heap seq scans: the
/// morsel positioner is already AM-dispatched (`seq_scan_set_morsel_range`
/// — the morsel bodies never needed cbstore), so the widening is the
/// leader-side geometry/source fork below plus the probe's heap
/// classification (same spelling there — GROUPSINK coherence). Heap
/// engagements ride the GENERIC accept lanes (RowFeed / collected
/// `emit_key` batches — the columnar dict/int-SoA fast lanes have no heap
/// producer) and refuse under EXPLAIN ANALYZE (the scan arm's
/// heap-not-instrumented posture).
pub(super) fn distinct_heap_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    // DEFAULT ON since the GL-LOWDIST-4 flip (Michael's B1 GO; kill
    // spellings exactly 0|off, the t35 flipped-kill idiom).
    crate::once_val(&ON, || {
        !matches!(
            std::env::var("PGRUST_RUNTIME_DISTINCT_HEAP").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// GL-LOWDIST-4 B1: per-AM morsel space for the distinct sinks — the
/// hashjoin `k2_task_source` fork verbatim. cbstore → RG-boundary granule
/// source (the historical wire; claims feed straight into
/// `set_granule_range`, never coalesce); heap → the boundary-free block
/// source (granule = ONE heap block, sizer-truncated, non-coalescing — the
/// scan arm's `GranuleMap::unbounded` posture through the storage seam; a
/// worker positions exactly blocks [a,b) via the AM-dispatched
/// `seq_scan_set_morsel_range`). `None` = empty/unsupported rel — silent
/// serial fallback, exactly the cb geometry contract.
pub(super) fn distinct_task_source<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<(u64, Arc<dyn runtime::MorselSource>)>> {
    if ::nodeseqscan::seq_scan_is_pgrcolumnar(ss) {
        let Some((granules, starts)) =
            ::nodeseqscan::seq_scan_cb_granule_geometry(ss, estate)?
        else {
            return Ok(None);
        };
        return Ok(Some((
            granules,
            Arc::new(super::runtime_scan::PgrcolumnarGranuleSource {
                starts: Arc::new(starts),
                coalesce: false,
            }) as Arc<dyn runtime::MorselSource>,
        )));
    }
    // Heap (admission guarantees the B1 knob gates this arm): the storage
    // seam's block geometry — no new geometry policy.
    use super::batch_source::BatchGranuleSource as _;
    let Some(map) = super::batch_source::SeqScanSource::new(ss).granule_map(estate)? else {
        return Ok(None);
    };
    let total = map.total();
    Ok(Some((
        total,
        Arc::new(runtime::GranuleMapSource::new(Arc::new(map), false, false))
            as Arc<dyn runtime::MorselSource>,
    )))
}

/// GL-LOWDIST-1 low-width combine — **DEFAULT ON** since the GL-LOWDIST-1
/// flip (letter: scratchpad/night/GL-LOWDIST-1-letter.md; fleet fix A/B @
/// a3d09b8ff: the sink beats the forced-legacy GM+pardistinct hybrid at
/// 23/24 band cells, grouped 0.67-0.96 / plain 0.33-0.44, sole residual
/// 5M-class dop8 grouped = 1.008 parity). Kill spellings exactly
/// `PGRUST_RUNTIME_DISTINCT_LOWWIDTH=0|off` (the t35 flipped-kill idiom) —
/// the kill restores BOTH the combine strategy and (via the planner's
/// same-spelling guard term, m5_suppress::distinct_lowwidth_live) the
/// pre-flip keep-Gather routing, byte-for-byte. Applies ONLY at
/// engagements whose routed dop is within the band bound
/// (`PGRUST_RUNTIME_DISTINCT_LOWWIDTH_MAXDOP`, default 8 — the pardistinct
/// low-DOP band is dop<12; dop-12+ engagements are already runtime-won and
/// stay byte-untouched by admission). Returns the band bound when armed.
pub(super) fn distinct_lowwidth_maxdop() -> Option<i32> {
    static V: OnceLock<Option<i32>> = OnceLock::new();
    crate::once_val(&V, || {
        if matches!(
            std::env::var("PGRUST_RUNTIME_DISTINCT_LOWWIDTH").as_deref(),
            Ok("0") | Ok("off")
        ) {
            return None;
        }
        Some(
            std::env::var("PGRUST_RUNTIME_DISTINCT_LOWWIDTH_MAXDOP")
                .ok()
                .and_then(|v| v.trim().parse::<i32>().ok())
                .filter(|&n| n > 0)
                .unwrap_or(8),
        )
    })
}

/// GL-LOWDIST-1 leader-parity participant bump (knob-gated, band-only —
/// above the band or knob-off returns `dop` untouched). The legacy
/// comparison arm at mpwpg=N runs N workers PLUS a participating leader
/// (the pardistinct hybrid's leader builds its own partial on the shared
/// claim; plain per-tuple GM leaders drain and fold), while the runtime
/// sink PARKS the leader in the waiter loop — an engagement at dop=N
/// spends N participants against legacy's N+1. The baseline attribution
/// leg (GLLOWDIST rtp1 rows, jobs @ 65e5390ac) measured that asymmetry as
/// MOST of the grouped low-DOP band: dop2 1.25-1.33x -> 0.83-0.92x, dop4
/// 1.07-1.18x -> 0.89-0.99x at participant parity. The bump restores
/// parity — and the C-shaped memory envelope (N+1 processes x work_mem is
/// exactly what the legacy plan spends). Clamped to the pool width.
/// Returns `(admitted dop, in_band)` — the band predicate is evaluated on
/// the PRE-bump dop (the routed engagement width), so a dop-8 engagement
/// bumped to 9 participants still rides the low-width combine. dop-1
/// engagements are EXCLUDED (below the measured band; bumping 1->2 turns a
/// one-gang engagement into a real parallel one — e.g. the locality-cap
/// DOP1 law would flip — an unmeasured corner this lane does not claim).
pub(super) fn lowwidth_leader_parity_dop(
    rt: &runtime::Runtime,
    dop: i32,
    arm: &str,
) -> (i32, bool) {
    match distinct_lowwidth_maxdop() {
        // GL-LOWDIST-5 lever 2: dop1 joins the leader-parity bump — the
        // hybrid at dop1 runs TWO participants (its GM worker's partial
        // AND the leader's own partial on the shared claim), so the sink
        // spending 1 was the same asymmetry GL-LOWDIST-1 measured at dop
        // 2-8 (fourth appearance of the pattern). The original dop1
        // exclusion protected a TEST expectation, not an invariant: at
        // bumped width 2 the locality cap engaging is CORRECT (two Locals
        // have a real duplicate-group tax) — the DOP1 law is re-derived at
        // the cap gate and leg4L-dop1 (runtime-distinct-e2e) accordingly.
        // Reached only from the two distinct sinks' admissions — no other
        // arm's dop1 behavior moves.
        Some(max) if dop >= 1 && dop <= max => {
            let bumped = (dop + 1).min(rt.nthreads() as i32).max(dop);
            if bumped != dop {
                lane_trace(&format!("{arm}: low-width leader-parity dop {dop}->{bumped}"));
            }
            (bumped, true)
        }
        _ => (dop, false),
    }
}

/// `PGRUST_RUNTIME_DISTINCT_LOCALITY_CAP` (bytes): unset/0 = OFF (the
/// budget-epoch cadence exactly — the DEFAULT), N = working-set bound
/// (floored at 64KB). Engagement is further gated at engage(): DOP>1 AND
/// the spill arm live (the epoch drain is the spill machinery; a dead arm
/// means no flush target, and a single Local has no duplicate-group tax to
/// convert — the radix-cap DOP1 law).
///
/// MEASURED REFUSAL as a default (2026-07-14 grouped-distinct trio @100M mt16 cap
/// ladder, notes/distinct-sidecar-cap.md): every cap point 2MB-32MB is
/// +17..21% WORSE than off on the narrow-sort pair (hot 0.79/0.93 -> 0.94-0.99/
/// 1.09-1.13) and cap-FLAT — no knee. The distinct-sidecar working set is
/// not a latency lever there: at matched memory the baseline never spills
/// and its sets merge ONCE through the freeze partition law, while the
/// locality regime adds epoch write + full replay re-probe for near-unique
/// (group,value) pairs whose accept probes were not DRAM-bound enough to
/// repay (the bandwidth study's instruction-parity reading). Outputs
/// byte-MATCHED on all four cap arms at 100M — the mechanism is correct,
/// just unprofitable; the env channel stays for shapes that may differ
/// (C3/radix-cap measured-refusal precedent).
fn distinct_locality_cap() -> Option<usize> {
    static N: OnceLock<Option<usize>> = OnceLock::new();
    crate::once_val(&N, || {
        match std::env::var("PGRUST_RUNTIME_DISTINCT_LOCALITY_CAP")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
        {
            None | Some(0) => None,
            Some(c) => Some(c.max(64 << 10)),
        }
    })
}

/// Runtime-sink per-Local budget (bytes) — the FULL chartered R3 envelope:
/// `work_mem × hash_mem_multiplier` per participant (C's
/// `get_hash_memory_limit`, m2-sinks.md §5 R3 — "each Local gets the full
/// work_mem × hash_mem_multiplier budget exactly as each PG worker
/// instance does"), clamped like the donor's `distinct_set_budget`. The
/// derived `PdSpec::worker_budget` is `distinct_set_budget()/2` = raw
/// work_mem HALVED — the Gather-era split (the leader's OWN partial shared
/// the envelope with each worker) with no multiplier; the sink has no
/// leader partial, so that sizing under-budgets every Local 4× at default
/// hash_mem_multiplier and fires the accept spill / seal refusal / combine
/// value-hash splits that much early (train-14 spill-envelope ledger).
/// `PGRUST_RUNTIME_DISTINCT_BUDGET_KB` pins a fixed per-Local budget (the
/// A/B attribution arm; absent/0 = derived).
fn runtime_distinct_worker_budget() -> usize {
    static KB: OnceLock<Option<usize>> = OnceLock::new();
    let ov = crate::once_val(&KB, || {
        std::env::var("PGRUST_RUNTIME_DISTINCT_BUDGET_KB")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|&n| n > 0)
    });
    match ov {
        Some(kb) => kb.saturating_mul(1024).min(1 << 31),
        None => ::nodehash::get_hash_memory_limit().min(1 << 31),
    }
}

/// PAREMIT kill switch (default ON): `PGRUST_RUNTIME_DISTINCT_PAREMIT=0`
/// keeps every engagement on the adopt tail — the A/B attribution channel
/// (results are byte-identical either way; see the pardistinct.rs paremit
/// section doc).
fn distinct_paremit_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || std::env::var("PGRUST_RUNTIME_DISTINCT_PAREMIT").as_deref() != Ok("0"))
}

/// `PGRUST_RUNTIME_DISTINCT_TOPN` kill switch (default ON): 0/off = the
/// paremit full drain everywhere (pre-kernel-2 behavior exactly — the A/B
/// attribution and rollback channel; results byte-identical either way by
/// the winners superset lemma, pardistinct.rs topn section doc).
fn distinct_topn_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(
            std::env::var("PGRUST_RUNTIME_DISTINCT_TOPN").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// `PGRUST_RUNTIME_DISTINCT_TOPN_DOPBUDGET` kill switch (default ON): 0/off
/// restores the serial-halved budget-fit admission exactly (the K2 100M
/// budget refusal — the near-unique shape @100M falls back to the vector's non-runtime arm;
/// the rollback/A-B attribution channel for the dop-budget face).
fn distinct_topn_dopbudget_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(
            std::env::var("PGRUST_RUNTIME_DISTINCT_TOPN_DOPBUDGET").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// Kernel-2 admission — arm the distinct sink's bounded selection from the
/// PLAN-SIDE consumer shape: the leader pstmt's root must be exactly
/// `Limit(COUNT-option, Const count, no offset) → Sort(single int8-kernel
/// order key) → THIS Agg`, and the order column must resolve to a
/// state-comparable, never-NULL paremit column (count(DISTINCT x) — the
/// merged set's value count — or count(*)/count(x)'s sidecar word;
/// sum(int2/4) can be SQL-NULL, whose rank depends on NULLS placement, so
/// it stays the full drain: no mid-combine decline face exists at all).
///
/// Plan reads only, before any build state exists; every refusal is the
/// paremit full drain (never a lane refusal, never a plan change). The
/// exec-time bound the serial arm sees is the same plan Const (Limit's
/// `ExecSetTupleBound` pushes count+offset; offset-bearing shapes refuse
/// here, so bound == count).
fn distinct_topn_arm(
    agg: &::nodeagg::AggStateData<'_>,
    estate: &EStateData<'_>,
    spec: &PdSpec,
    cols: &[::nodeagg::PdParemitCol],
) -> Option<PdTopnSpec> {
    use ::nodeagg::{PdParemitCol, PdTopnKey};
    let decline = |why: &str| {
        lane_trace(&format!("runtime-distinct topn: declined ({why})"));
        None
    };
    if !distinct_topn_enabled() {
        return None;
    }
    // --- Consumer shape: root Limit → Sort → this Agg (the near-unique text top-n class).
    let pstmt = estate.es_plannedstmt?;
    let root = pstmt.planTree?;
    let limit = root.as_limit()?;
    if limit.limitOption != ::types_nodes::nodes_enums::LimitOption::LIMIT_OPTION_COUNT {
        return decline("limit option");
    }
    if limit.limitOffset.is_some() {
        return decline("limit offset");
    }
    let count = limit.limitCount?.as_const()?;
    if count.constisnull {
        return decline("LIMIT ALL");
    }
    let Ok(bound) = u32::try_from(count.constvalue.as_i64()) else {
        return decline("bound range");
    };
    if bound == 0 || bound > ::nodeagg::sink::SINK_TOPN_MAX_BOUND {
        return decline("bound cap");
    }
    let sort = limit.plan.lefttree?.as_sort()?;
    let agg_node = sort.plan.lefttree?.as_agg()?;
    if agg_node as *const _ as usize != agg.plan as *const _ as usize {
        return decline("sort child not this Agg");
    }
    // --- Order key: single column, int8 kernel operator, resolving to a
    // never-NULL paremit column (tie-break law: the selection total order's
    // secondary key is the GROUP order, which single-column consumers
    // cannot contradict).
    if sort.numCols != 1 || sort.sortColIdx.is_empty() {
        return decline("multi-column order");
    }
    let oc = sort.sortColIdx[0];
    if oc < 1 || (oc as usize) > cols.len() {
        return decline("order column resno");
    }
    let opfn = ::lsyscache::get_opcode(sort.sortOperators[0]).ok()?;
    let desc = match ::execexpr::CmpOp::for_fn_oid(opfn) {
        Some(::execexpr::CmpOp::Int8Gt) => true,
        Some(::execexpr::CmpOp::Int8Lt) => false,
        _ => return decline("order operator kernel"),
    };
    let key = match cols[(oc - 1) as usize] {
        PdParemitCol::SetCount(si) => PdTopnKey::SetCount(si),
        PdParemitCol::Vocab { transno, sum: false } => {
            let vi = spec.vocab.iter().position(|v| v.transno == transno)?;
            PdTopnKey::VocabCount(vi)
        }
        // Key columns are not order values; sum can be NULL (doc above).
        _ => return decline("order column not state-comparable"),
    };
    lane_trace(&format!("runtime-distinct topn: armed (bound={bound})"));
    Some(PdTopnSpec { key, desc, bound })
}

// ---------------------------------------------------------------------------
// Leader-side engagement. Arming layering (kill switch + DOP option + lane
// master) lives in guc_tables::runtime_pool::runtime_distinct_pool_dop —
// the reconciled three-arm surface (PGRUST_RUNTIME_DISTINCT is this arm's
// dedicated kill; the scan arm's kill no longer disarms it).
// ---------------------------------------------------------------------------

/// Refusal diagnosis trace (PGRUST_LANE_V2_TRACE only; emitted only once the
/// arm is ARMED — dop set + runtime on — so unarmed sessions stay silent) +
/// under EA the per-node transparency record (ea-morsels.md §6).
#[cold]
fn refused(
    estate: &mut EStateData<'_>,
    ea: bool,
    node_id: i32,
    reason: &'static str,
) {
    // M5-1: every distinct-arm refusal feeds the router's consolidated
    // taxonomy alongside the trace / EA transparency line.
    router::tick_refused(ArmClass::Distinct, reason);
    lane_trace(&format!("runtime-distinct: refused ({reason})"));
    if ea {
        estate.runtime_ea_record_refusal(node_id, "distinct", reason);
    }
}

/// The runtime distinct-sink arm, probed from the sorted-agg narrow branch
/// (set-mode already armed by the caller — the last-refusal ordering law is
/// satisfied there). `None` = refused or fell back (nothing consumed; the
/// serial arms run byte-identically). `Some(row)` = the arm owns the node
/// (merged result adopted; emit chain active).
pub(super) fn try_own_sorted_distinct_runtime<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    sort: &mut ::nodesort::SortState<'mcx>,
    outer: &mut crate::procnode::PlanStateNode<'mcx>,
    outer_desc: &Option<std::rc::Rc<::types_tuple::TupleDescData<'static>>>,
    rd_shape_refused: &mut bool,
    k: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // --- Arming + kill-switch layering (all cheap; absent = today's path).
    // M5-1: the router is the DOP source (bench GUC verbatim when set; else
    // engine=runtime arms at pgrust.runtime_dop; else 0 = today's path).
    let dop = router::arm_dop(ArmClass::Distinct);
    if dop <= 0 || !runtime::runtime_enabled() {
        return Ok(None);
    }
    let Some(rt) = runtime::global() else { return Ok(None) };
    // Static shape refusal memo: the plan-shape gates below cannot flip for
    // this node; skip the whole probe (incl. spec derivation) on re-pulls.
    if *rd_shape_refused {
        return Ok(None);
    }
    router::tick(ArmClass::Distinct, ArmCounter::Offered);
    lane_trace("runtime-distinct: probed");

    // EA-on-morsels (ea-morsels.md §5/§6): from here the session is ARMED —
    // under EXPLAIN ANALYZE every refusal records its first failing gate for
    // the transparency line.
    let ea = runtime_instr::ea_active(estate);
    let node_id = agg.plan.plan.plan_node_id;

    // --- Shape + session gates (fail-closed; every refusal is the serial arm).
    let crate::procnode::PlanStateNode::SeqScan(ss) = outer else {
        refused(estate, ea, node_id, "outer not SeqScan");
        return Ok(None);
    };
    // Under EA the leader node carries an instr slot, which the serial-lane
    // fusibility memo rightly refuses — the sink's workers run
    // uninstrumented, so EA admission walks the same gates with only the
    // instrument check vacated (E4).
    let fusible = if ea {
        seq_scan_fusible_runtime_ea(ss, estate)?
    } else {
        seq_scan_fusible(ss, estate)?
    };
    let is_cb = ::nodeseqscan::seq_scan_is_pgrcolumnar(ss);
    // GL-LOWDIST-4 B1: heap seq scans admit under the knob (the morsel
    // bodies were already AM-generic; see distinct_task_source).
    let heap_ok = ::nodeseqscan::seq_scan_is_heap(ss) && distinct_heap_enabled();
    if !fusible || !(is_cb || heap_ok) {
        refused(estate, ea, node_id, "scan not fusible/cbstore");
        return Ok(None);
    }
    if !is_cb && ea {
        // The scan arm's posture: heap engagements are uninstrumented.
        refused(estate, true, node_id, "heap-not-instrumented");
        return Ok(None);
    }
    if estate.es_epq_active {
        router::tick_refused(ArmClass::Distinct, "epq");
        return Ok(None);
    }
    // Instrument MODE gate: INSTRUMENT_ROWS (TIMING OFF, inc-1) or
    // INSTRUMENT_TIMER (BUFFERS OFF, inc-3 — one clock pair per claim)
    // engage; BUFFERS/WAL combinations refuse until threaded.
    if ea && !runtime_instr::ea_mode_admissible(estate) {
        refused(estate, true, node_id, runtime_instr::ea_mode_refuse_reason(estate));
        return Ok(None);
    }
    if super::runtime_in_parallel_machinery(ss) {
        refused(estate, ea, node_id, "already in parallel machinery");
        return Ok(None);
    }
    // Agg-side admission: the hash-grouped arm's int/text-key exact-set
    // vocabulary and the SINK economics tier (a refusal falls back to the
    // serial arms, byte-identically). Vocab shapes (companion aggs) are
    // ADMITTED — see the module doc. The sink reads its own density tier
    // (`agg_hashgroup_economical_sink`): the serial 8×-rows-per-group
    // refusal priced the SERIAL hashgroup build vs the narrow sort, but
    // near-unique text shapes (~2 rows/group, qual on the key) are exactly this
    // arm's DOP-parallel conversion targets — the budget-fit term stays.
    // PAREMIT shape probe BEFORE the economics tier: paremit-admitted
    // shapes read the 1.0 density default — the serial adopt/emit tail the
    // 8.0 tier priced is exactly what the parallel emit removes (the
    // near-unique class's conversion; hashgrouped.rs economics doc).
    let paremit_cols =
        if distinct_paremit_enabled() { ::nodeagg::pd_paremit_cols(agg) } else { None };
    if !::nodeagg::agg_hashgroup_admissible(agg) {
        refused(estate, ea, node_id, "hashgroup admission/economics");
        return Ok(None);
    }
    // Economics, serial-halved fit term first (the pre-lane sizing): a PASS
    // here is final. A FAIL is only provisional when the K2 dop-budget face
    // (economical_sink doc) can still apply — the paremit/topn probes below
    // are plan reads only, so deferring the refusal costs nothing and keeps
    // every non-K2 shape refusing exactly as before, same reason string.
    let econ_serial = ::nodeagg::agg_hashgroup_economical_sink(
        agg,
        super::pardistinct_force(),
        sort.plan.plan.plan_rows,
        paremit_cols.is_some(),
        None,
    );
    if !econ_serial
        && !(distinct_topn_dopbudget_enabled()
            && distinct_topn_enabled()
            && distinct_spill_enabled()
            && paremit_cols.is_some())
    {
        refused(estate, ea, node_id, "hashgroup admission/economics");
        return Ok(None);
    }
    let Some(order) = super::hashgroup_order_spec(agg, sort.plan, k) else {
        refused(estate, ea, node_id, "order spec");
        *rd_shape_refused = true;
        return Ok(None);
    };
    let Some(desc) = outer_desc.as_ref() else {
        refused(estate, ea, node_id, "no outer desc");
        return Ok(None);
    };
    // admit_text_keys = true (distinct-bytes car): `agg_hashgroup_admissible`
    // above proved `group_eq_representational` texteq under a deterministic
    // collation for every text/varchar grouping column — byte equality IS
    // the grouping operator's verdict, so the canonical-bytes key image is
    // sound here. The Gather-era arms keep passing false (leader-side twin
    // of the same gate: no surface without a bytes-comparable image ever
    // sees a bytes key — the m2-sinks §1 rule-5 selection-order totality
    // law's admission discipline).
    let Some(mut spec) = ::nodeagg::pd_derive_spec(agg, desc, true, ::nodeagg::distinct_datetime_enabled()) else {
        refused(estate, ea, node_id, "spec derivation");
        *rd_shape_refused = true;
        return Ok(None);
    };
    // Envelope right-sizing: the RUNTIME sink re-budgets the fresh spec to
    // the full R3 per-Local envelope (fn doc above). The Gather-era
    // pardistinct arm keeps the derived /2 budget untouched — its leader
    // partial still shares the envelope.
    if let Some(s) = Arc::get_mut(&mut spec) {
        s.worker_budget = runtime_distinct_worker_budget();
        // dedupsub I3: per-worker row-share expectation for the distinct-set
        // projection reserve (plan's post-qual scan estimate / dop; 0 stays
        // inert). An estimate error only moves probe-table GEOMETRY — the
        // ratio clamp and expected-cap bound in flush_staged bound the
        // overshoot, and the capacity-based budget metering stays honest.
        s.expected_worker_rows =
            (sort.plan.plan.plan_rows / f64::from(dop.max(1))).max(0.0) as u64;
    }
    if spec.max_att > desc.natts {
        refused(estate, ea, node_id, "att bound");
        *rd_shape_refused = true;
        return Ok(None);
    }
    // PAREMIT recipe (mode fixed HERE, before submit — one engagement-level
    // choice; combine and finalize branch on it uniformly). Resolution
    // against the derived spec is structurally total for shapes the cols
    // probe admitted; a None falls back to the adopt tail (correct, merely
    // priced at the paremit density tier — comment at pd_paremit_recipe).
    let paremit = paremit_cols
        .as_deref()
        .and_then(|cols| ::nodeagg::pd_paremit_recipe(&spec, cols, &order))
        .map(Arc::new);
    if paremit.is_some() {
        lane_trace("runtime-distinct: paremit armed");
    }
    // Kernel-2 bounded selection (topn section doc): plan-side consumer
    // resolution, armed only beside a live paremit recipe (the selection
    // rides the ordered emit buckets). A None is the paremit full drain.
    let topn = match (&paremit, paremit_cols.as_deref()) {
        (Some(_), Some(cols)) => distinct_topn_arm(agg, estate, &spec, cols),
        _ => None,
    };
    // Deferred economics (the K2 dop-budget face — economical_sink doc): a
    // serial-halved-term FAIL above survives only if the full bounded-memory
    // stack actually resolved (live paremit recipe + armed K2 selection) AND
    // the estimate fits the sink's real union bound (per-Local R3 envelope ×
    // dop — the same bound the combine enforces dynamically; splits + value
    // spill bound each partition, the selection bounds the leader).
    if !econ_serial {
        let admit = paremit.is_some()
            && topn.is_some()
            && ::nodeagg::agg_hashgroup_economical_sink(
                agg,
                super::pardistinct_force(),
                sort.plan.plan.plan_rows,
                true,
                Some((runtime_distinct_worker_budget(), dop as u32)),
            );
        if !admit {
            refused(estate, ea, node_id, "hashgroup admission/economics");
            return Ok(None);
        }
        lane_trace(&format!("runtime-distinct: topn dop-budget admission (dop={dop})"));
    }
    // No params, either kind (the binder refuses Params; the worker pstmt
    // carries none).
    if estate.es_param_list_info.is_some_and(|p| !p.is_empty()) {
        refused(estate, ea, node_id, "extern params");
        return Ok(None);
    }
    let Some(leader_pstmt) = estate.es_plannedstmt else { return Ok(None) };
    if leader_pstmt.paramExecTypes.iter().next().is_some() {
        refused(estate, ea, node_id, "exec params");
        return Ok(None);
    }
    // Plan shape below the Agg: exactly THIS Sort → SeqScan (the workers
    // receive the SCAN SUBTREE as their pstmt — the Agg need not be the
    // plan root, so ORDER BY/LIMIT above it, the real bank grouped-distinct shape,
    // stays engageable).
    let Some(sort_node) = agg.plan.plan.lefttree else { return Ok(None) };
    if sort_node.node_tag() != NodeTag::T_Sort
        || !std::ptr::eq(sort_node.as_sort().expect("Sort tag"), sort.plan)
    {
        refused(estate, ea, node_id, "agg child not this Sort");
        *rd_shape_refused = true;
        return Ok(None);
    }
    let Some(scan_node) = sort.plan.plan.lefttree else { return Ok(None) };
    if scan_node.node_tag() != NodeTag::T_SeqScan {
        refused(estate, ea, node_id, "sort child not SeqScan");
        *rd_shape_refused = true;
        return Ok(None);
    }
    let scan_plan = scan_node.as_seq_scan().expect("SeqScan tag");
    if !super::runtime_scan::exprs_parallel_safe(scan_plan.scan.plan.qual.iter())?
        || !super::runtime_scan::exprs_parallel_safe(scan_plan.scan.plan.targetlist.iter())?
    {
        refused(estate, ea, node_id, "parallel-unsafe scan exprs");
        *rd_shape_refused = true;
        return Ok(None);
    }
    // GL-VECACCEPT-1 (knob-gated, default OFF; fail-closed to the incumbent
    // per-row accept — a None changes NOTHING).
    let vec = vec_cols(&spec, ss, scan_plan, desc, ea);
    if !estate
        .es_snapshot
        .as_deref()
        .is_some_and(::types_snapshot::IsMVCCSnapshot)
    {
        refused(estate, ea, node_id, "non-MVCC snapshot");
        return Ok(None);
    }
    let policy = parallel::query_task_policy_probe();
    if policy.has_params
        || policy.temp_state
        || policy.serializable
        || policy.pending_invalidations
    {
        refused(estate, ea, node_id, "binder policy sources");
        return Ok(None);
    }

    // --- Geometry: enough granules to be worth a gang.
    let Some((total_granules, source)) = distinct_task_source(ss, estate)?
    else {
        return Ok(None);
    };
    if total_granules < super::runtime_scan::min_granules().max(2 * dop as u64) {
        refused(estate, ea, node_id, "granule floor");
        return Ok(None);
    }
    // DOP-elastic admission (tails192 #5): floors above ran against the
    // POOL dop; arm only what the work can feed (kill: PGRUST_RUNTIME_ELASTIC_DOP=0).
    let dop = super::runtime_scan::elastic_dop(dop, total_granules);
    // GL-LOWDIST-1 leader-parity bump (knob-gated, band-only).
    let (dop, lowwidth) = lowwidth_leader_parity_dop(rt, dop, "runtime-distinct");
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }

    // --- Engage.
    engage(
        agg, estate, rt, dop, lowwidth, total_granules, source, spec, order, paremit, topn,
        vec, scan_node, ea,
    )
}

#[allow(clippy::too_many_arguments)]
fn engage<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    lowwidth: bool,
    total_granules: u64,
    source: Arc<dyn runtime::MorselSource>,
    spec: Arc<PdSpec>,
    order: Vec<::nodeagg::HashGroupOrderKey>,
    paremit: Option<Arc<PdEmitRecipe>>,
    topn: Option<PdTopnSpec>,
    vec: Option<VecCols>,
    scan_node: ::types_nodes::node_tree::Node<'mcx>,
    ea: bool,
) -> PgResult<Option<Option<ExecSlotId>>> {
    ensure_hooks_registered();
    crate::execparallel::register_parallel_query_main();

    // The worker pstmt carries ONLY the scan subtree (ExecSerializePlan's
    // fragment-transfer shape; the helpers drive scan → PREWHERE → project
    // into their PdBuilder Locals — no Agg, no Sort).
    let pstmt = crate::execparallel::build_worker_pstmt(estate, scan_node)?;

    // M3.5 spill arm: ON by default when the sink engages (the
    // refusal→engagement charter); PGRUST_RUNTIME_DISTINCT_SPILL=0 restores
    // the phase-1 refusal exactly. SpillSet creation is leader-side (fd
    // substrate guaranteed); a creation failure fail-closes to refusal.
    let spill_set = if distinct_spill_enabled() {
        match ::spillset::SpillSet::create() {
            Ok(s) => Some(s),
            Err(_) => {
                lane_trace("runtime-distinct: spill set creation failed — spill disarmed");
                None
            }
        }
    } else {
        None
    };
    // Locality cap (distinct-sidecar-cap lane): resolved once per
    // engagement — DOP>1 with a live spill arm only (fn doc).
    // GL-LOWDIST-5 lever-2 re-derivation of the DOP1 law: `dop` here is
    // the RESOLVED participant count (post leader-parity bump), so a
    // requested dop1 lowwidth engagement arrives as dop=2 and the cap
    // rightly resolves — two Locals have a real duplicate-group tax. The
    // dop>1 gate still protects the true single-Local case (lowwidth
    // killed, or the pool clamped to 1).
    let locality_cap = if dop > 1 && spill_set.is_some() {
        distinct_locality_cap()
    } else {
        None
    };

    let payload = Arc::new(RuntimeDistinctShared {
        rt,
        rg: OnceLock::new(),
        pcxt_shared: OnceLock::new(),
        // SAFETY (lifetime erasure): leader executor arena, held across the
        // whole engagement; DestroyParallelContext joins helpers before this
        // frame returns on every path.
        pstmt: SendConstPstmt(unsafe {
            core::mem::transmute::<*const PlannedStmt<'mcx>, *const PlannedStmt<'static>>(
                pstmt as *const PlannedStmt<'mcx>,
            )
        }),
        query_text: estate.es_sourceText.unwrap_or("").to_string(),
        eflags: estate.es_top_eflags,
        spec: Arc::clone(&spec),
        paremit,
        topn,
        refused: AtomicUsize::new(0),
        started: AtomicUsize::new(0),
        exited: AtomicUsize::new(0),
        error: Mutex::new(None),
        failed: AtomicBool::new(false),
        crossed: AtomicBool::new(false),
        merged_bytes: AtomicUsize::new(0),
        spill_set,
        locality_cap,
        lowwidth: {
            if lowwidth {
                lane_trace(&format!("runtime-distinct: low-width combine armed (dop={dop})"));
            }
            lowwidth
        },
        spill_epochs: AtomicU64::new(0),
        spilled_bytes: AtomicU64::new(0),
        combine_splits: AtomicU64::new(0),
        split_depth_max: AtomicU64::new(0),
        split_uniq: AtomicU64::new(0),
        out: (0..PD_SINK_GROUP_PARTS as usize).map(|_| UnsafeCell::new(None)).collect(),
        merged: Mutex::new(None),
        ea_scan_node: if ea {
            scan_node.as_seq_scan().map(|s| s.scan.plan.plan_node_id)
        } else {
            None
        },
        ea_instr_slots: ea.then(|| {
            (0..rt.nthreads() + runtime::MAX_EXTERNAL_LANES)
                .map(|_| Mutex::new(None))
                .collect()
        }),
        ea_timer: ea && runtime_instr::ea_timer(estate),
        ea_epoch: std::time::Instant::now(),
        standing: Mutex::new(None),
        vec: {
            if vec.is_some() {
                lane_trace(&format!("runtime-distinct: vecaccept engaged (dop={dop})"));
            }
            vec
        },
        vec_rows: AtomicU64::new(0),
        vec_granules: AtomicU64::new(0),
    });

    xact::EnterParallelMode();
    // Router counter choke point (M5-1): Engaged = ceremony entered;
    // Completed = the runtime answered; Fallback = R5 serial rerun.
    router::tick(ArmClass::Distinct, ArmCounter::Engaged);
    let engaged =
        engage_ceremony(agg, estate, rt, dop, total_granules, source, &payload, spec, order);
    xact::ExitParallelMode();
    if let Ok(r) = &engaged {
        router::tick(
            ArmClass::Distinct,
            if r.is_some() { ArmCounter::Completed } else { ArmCounter::Fallback },
        );
    }
    engaged
}

enum EngageOutcome {
    Fallback,
    Completed,
}

/// This arm's standing-channel constants (M2 inc-1; see
/// standing_channel::StandingArm — sinks_gate: PGRUST_RUNTIME_POOLBIND_SINKS).
static STANDING_ARM: super::standing_channel::StandingArm =
    super::standing_channel::StandingArm {
        label: "runtime-distinct",
        died: "runtime distinct standing executors exited before completing the build",
        sinks_gate: true,
    };

/// Shared post-outcome tail (standing and launched channels): worker-phase
/// errors rethrow PLAIN; a budget crossing takes the bounded-memory serial
/// rerun; an unexplained abort surfaces the pending interrupt or reports;
/// completed-but-nobody-participated falls back serially.
fn finish_outcome(
    payload: &Arc<RuntimeDistinctShared>,
    outcome: runtime::RgOutcome,
) -> PgResult<EngageOutcome> {
    if let Some(e) = payload.take_error() {
        lane_trace(&format!(
            "runtime-distinct: worker-phase error: {}",
            e.message()
        ));
        return Err(e);
    }
    if outcome == runtime::RgOutcome::Aborted {
        if payload.crossed.load(Ordering::SeqCst) {
            // Worker budget crossed: bounded-memory refusal — rerun the
            // serial arm (nothing was emitted; the leader's scan is
            // untouched).
            lane_trace("runtime-distinct: worker budget crossed; serial fallback");
            stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AdmissionEconomicsFusedDrive);
            return Ok(EngageOutcome::Fallback);
        }
        ::postgres_seams::check_for_interrupts::call()?;
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime distinct pipeline aborted",
        )));
    }
    if payload.started.load(Ordering::SeqCst) == 0 {
        return Ok(EngageOutcome::Fallback);
    }
    Ok(EngageOutcome::Completed)
}

#[allow(clippy::too_many_arguments)]
fn engage_ceremony<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    total_granules: u64,
    source: Arc<dyn runtime::MorselSource>,
    payload: &Arc<RuntimeDistinctShared>,
    spec: Arc<PdSpec>,
    order: Vec<::nodeagg::HashGroupOrderKey>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    let pcxt = parallel::CreateParallelContext("postgres", "pgrust_runtime_distinct_main", dop)?;
    let mut submitted: Option<runtime::RgHandle> = None;
    // SinkProbe surface (M5-1, the §3.5 lane_trace remainder): captured out
    // of the ceremony body and reported at RG completion.
    let mut sink_probe: Option<Arc<runtime::SinkProbe>> = None;
    let probe_out = &mut sink_probe;

    let body = (|mut_submitted: &mut Option<runtime::RgHandle>| -> PgResult<EngageOutcome> {
        parallel::InitializeParallelDSM(pcxt)?;
        let nworkers = parallel::nworkers(pcxt);
        if nworkers <= 0 {
            return Ok(EngageOutcome::Fallback);
        }
        parallel::InstallQueryTaskBinding(pcxt, parallel::QueryTaskBindingPolicy::default())?;
        payload
            .pcxt_shared
            .set(parallel::shared_for(pcxt))
            .unwrap_or_else(|_| unreachable!("pcxt shared set once"));
        parallel::set_private(pcxt, Arc::clone(payload) as _);
        // Standing driver dispatch (M2 inc-1): deferred_bind false — this
        // arm binds EAGERLY (with_query_task_binding); the standing serve
        // re-establishes visibility up front and evicts parked sticky.
        parallel::set_standing_driver(pcxt, parallel::standing::StandingDriver {
            drive: runtime_distinct_standing_driver,
            deferred_bind: false,
        });
        // M2 inc-2: the POOL-DB channel — built BEFORE submit (the bound
        // descriptor must ride the submission: publication keys the
        // pool-visible active bit off it); sinks_gate: POOLBIND_SINKS=0
        // retires this channel with the gang's. None = plain pinned
        // submit, inc-1 byte-exactly.
        let pool = super::standing_channel::try_pool_channel(
            payload.pcxt_shared.get().expect("pcxt shared set above"),
            dop,
            /* sinks_gate */ true,
        );


        // Submit the pinned RG (accept → freeze → combine) before launch.
        // The per-AM morsel source was built at admission
        // (distinct_task_source — GL-LOWDIST-4 B1): cbstore RG-boundary
        // claims fed straight into set_granule_range, or heap block-range
        // claims through the same AM-dispatched positioner.
        let runtime::SealedSinkTaskSets { accept, freeze, combine, probe } =
            runtime::sealed_sink_tasksets(
                Arc::clone(payload),
                source,
                rt.nthreads() + runtime::MAX_EXTERNAL_LANES,
                0,
            );
        *probe_out = Some(probe);
        static NEXT_QUERY_ID: AtomicUsize = AtomicUsize::new(1);
        let qspec = runtime::QuerySpec {
            query_id: NEXT_QUERY_ID.fetch_add(1, Ordering::SeqCst) as u64,
            tasksets: vec![accept, freeze, combine],
        };
        // rg-set-BEFORE-publish (M2 inc-3 rung 3): payload.rg is stored by
        // on_rg before the bound submission can become pool-visible — no
        // "rg gone" refusal churn window.
        let set_rg = |rg: &runtime::RgHandle| {
            payload
                .rg
                .set(rg.downgrade())
                .unwrap_or_else(|_| unreachable!("rg set once"));
        };
        let (rg, waiter) = match &pool {
            Some((_, descriptor)) => rt.submit_pinned_bound(
                qspec,
                router::session_affinity_token(),
                descriptor.clone(),
                set_rg,
            ),
            None => {
                let (rg, waiter) =
                    rt.submit_pinned_with_affinity(qspec, router::session_affinity_token());
                set_rg(&rg);
                (rg, waiter)
            }
        };
        *mut_submitted = Some(rg.clone());

        // M2 inc-1: STANDING engagement first — no worker launch, one
        // binder bind per participant; fallback leaves the RG untouched
        // for the launched path below.
        let census = format!("vocab={} sets={}", spec.vocab.len(), spec.sets.len());
        match super::standing_channel::standing_wait(
            &STANDING_ARM,
            super::standing_channel::StandingLeader {
                // M2 inc-2: the pool-db board attached at submit (None =
                // gang-first, inc-1 exactly).
                pool: pool.as_ref().map(|(entry, _)| Arc::clone(entry)),
                shared: payload.pcxt_shared.get().expect("pcxt shared set above"),
                slot: &payload.standing,
                started: &payload.started,
                refused: &payload.refused,
                take_error: &|| payload.take_error(),
                drain: &|rg| drain_rg(rt, rg),
                census: &census,
            },
            dop,
            total_granules,
            &rg,
            &waiter,
        )? {
            super::standing_channel::StandingWait::Done(outcome) => {
                return finish_outcome(payload, outcome);
            }
            super::standing_channel::StandingWait::Fallback => {}
        }

        // M2 inc-3 rung 4: the launched-bgworker fallback is DELETED — a
        // board decline goes straight to the serial arm (pool → gang →
        // serial; the NOLAUNCH posture made permanent). Cause attribution
        // ticks the nolaunch-serial floor row inside the shared helper.
        super::standing_channel::launched_fallback_retired(&STANDING_ARM);
        drain_rg(rt, &rg);
        Ok(EngageOutcome::Fallback)
    })(&mut submitted);

    // Teardown tail (every path): a submitted RG must be COMPLETE before the
    // parallel context is destroyed and this frame's arena can unwind.
    if let Some(rg) = &submitted {
        if rg.try_outcome().is_none() {
            drain_rg(rt, rg);
        }
    }
    let destroy = parallel::DestroyParallelContext(pcxt);
    let outcome = body?;
    destroy?;

    // SinkProbe report (M5-1): stale_locals_dropped / combine_refusals now
    // have a surface — router counters + a lane_trace line per engagement.
    if let Some(probe) = &sink_probe {
        router::sink_probe_complete(ArmClass::Distinct, probe);
    }

    match outcome {
        EngageOutcome::Fallback => {
            stats::tick_engaged(STANDING_ARM.label, stats::EngageChannel::Serial);
            lane_trace("runtime-distinct: fallback to serial arm");
            Ok(None)
        }
        EngageOutcome::Completed => {
            let Some(published) = payload.take_merged() else {
                // Completed with participants but no published result: a
                // protocol violation, never silently wrong output.
                return Err(Box::new(PgError::new(
                    ERROR,
                    "runtime distinct completed without a merged result",
                )));
            };
            stats::tick_owned(ShapeClass::AggBuild);
            let spill_epochs = payload.spill_epochs.load(Ordering::Relaxed);
            if spill_epochs > 0 {
                // The R4 spill-rate observability line (e2e + gate records).
                lane_trace(&format!(
                    "runtime-distinct: SPILLED epochs={spill_epochs} bytes={}",
                    payload.spilled_bytes.load(Ordering::Relaxed)
                ));
            }
            let splits = payload.combine_splits.load(Ordering::Relaxed);
            if splits > 0 {
                lane_trace(&format!(
                    "runtime-distinct: COMBINE-SPLIT splits={splits} max_depth={}",
                    payload.split_depth_max.load(Ordering::Relaxed)
                ));
            }
            let groups = match &published {
                DstPublished::Merged(m) => m.ngroups,
                DstPublished::Emit(bufs, _) => bufs.iter().map(|b| b.nrows).sum(),
            };
            lane_trace(&format!("runtime-distinct: complete, groups={groups}"));
            // EA-on-morsels merge (clean Completed only): fold every
            // worker's final instrument export; write the bypassed
            // SeqScan's rows/nfiltered/loops and the bypassed Sort's
            // pass-through rows (ea-morsels.md §3 — node-exact rows; the
            // engaged Agg root ticks through its procnode wrapper).
            if let (Some(scan_id), Some(slots)) =
                (payload.ea_scan_node, &payload.ea_instr_slots)
            {
                let ips: Vec<InstrumentPartial> = slots
                    .iter()
                    .filter_map(|m| m.lock().unwrap_or_else(|p| p.into_inner()).take())
                    .collect();
                let m = runtime_instr::merge(ips.iter());
                runtime_instr::ea_fill_scan_node(estate, scan_id, &m.rows);
                let sort_id = agg
                    .plan
                    .plan
                    .lefttree
                    .and_then(::types_nodes::node_tree::Node::as_sort)
                    .map(|s| s.plan.plan_node_id);
                if let Some(sort_id) = sort_id {
                    runtime_instr::ea_fill_passthrough_node(estate, sort_id, m.rows.survived);
                }
                // Pipeline report for the inc-2 EXPLAIN block (ACCEPT +
                // SEALCVT + COMBINE task sets on this arm; the skipped Sort
                // is the second member; partials = workers).
                estate.es_runtime_ea_pipelines.push(runtime_instr::ea_pipeline_report(
                    "distinct",
                    agg.plan.plan.plan_node_id,
                    scan_id,
                    sort_id.unwrap_or(-1),
                    3,
                    m.workers as u64,
                    &m,
                ));
                lane_trace(&format!(
                    "runtime-distinct: EA merged workers={} claims={} granules={} \
                     scanned={} survived={}",
                    m.workers, m.claims, m.granules, m.rows.scanned, m.rows.survived
                ));
            }
            match published {
                DstPublished::Merged(merged) => {
                    trace_feed("runtime distinct sink adopt + hashgroup emit engaged");
                    ::nodeagg::agg_hashgroup_adopt_merged(
                        agg,
                        estate,
                        merged.into_lt(),
                        &spec.vocab,
                        order,
                    )?;
                    Ok(Some(super::hashgroup_emit(agg, estate)?))
                }
                DstPublished::Emit(bufs, cands) => {
                    // PAREMIT adoption: rows were finalized, projected, and
                    // ordered inside the combine claims; the leader's tail
                    // is the cross-bucket merge + a datum memcpy per pull.
                    let Some(recipe) = payload.paremit.as_deref() else {
                        // Structurally impossible (finalize published Emit
                        // only under Some) — protocol violation, never
                        // silently wrong output.
                        return Err(Box::new(PgError::new(
                            ERROR,
                            "runtime distinct published paremit buckets without a recipe",
                        )));
                    };
                    trace_feed("runtime distinct sink paremit emit engaged");
                    // Kernel-2 winner direction: truncate-merge the
                    // partition candidate lists to the global winner set;
                    // the merge then emits winners alone, in group order.
                    let topn = match (&payload.topn, &cands) {
                        (Some(t), Some(c)) => Some((&c[..], t.bound)),
                        (None, None) => None,
                        _ => {
                            return Err(Box::new(PgError::new(
                                ERROR,
                                "runtime distinct topn arming/candidate mismatch",
                            )))
                        }
                    };
                    let st = ::nodeagg::pd_paremit_state(recipe, bufs, topn)?;
                    if let Some(w) = st.kept_rows() {
                        let mat: usize = cands
                            .as_ref()
                            .map(|c| c.iter().map(Vec::len).sum())
                            .unwrap_or(0);
                        lane_trace(&format!(
                            "runtime-distinct: topn composed (winners={w}) mode=winners-only materialized={mat}"
                        ));
                    }
                    ::nodeagg::agg_pdemit_install(agg, st);
                    Ok(Some(super::pdemit_emit(agg, estate)?))
                }
            }
        }
    }
}

/// Abort + BOUNDED drain of a pinned RG no helper will drive
/// (abort/fallback paths) — protocol cleanup driving, not leader work
/// execution (§2.5; runtime_scan's hardened drain, verbatim — F1 port).
/// True = the RG completed. False = it could not be completed (a
/// participant died holding an unsettled pin): the RG and its slot are
/// deliberately LEAKED and the caller must surface an error rather than
/// wait forever — the previous unbounded `loop {{ acquire }} + drive_pinned`
/// shape could itself wedge on exactly the helper-death cases this lane
/// fixes.
fn drain_rg(rt: &'static Arc<runtime::Runtime>, rg: &runtime::RgHandle) -> bool {
    rg.abort();
    // Bounded lane wait (~2s): helper drives settle within a morsel.
    let mut lane = None;
    for _ in 0..4000 {
        if let Some(l) = rt.acquire_external_lane() {
            lane = Some(l);
            break;
        }
        std::thread::sleep(std::time::Duration::from_micros(500));
    }
    let Some(lane) = lane else {
        lane_trace("runtime-distinct: LEAKED pinned RG (no external lane for the drain)");
        return false;
    };
    let mut local = lane.local();
    let drained = rt.try_drain_pinned(&mut local, rg, 4000).is_some();
    if !drained {
        lane_trace("runtime-distinct: LEAKED pinned RG (drain gave up — dead participant?)");
    }
    drained
}
