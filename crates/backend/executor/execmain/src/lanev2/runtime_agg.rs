//! M2 RUNTIME AGGREGATION SINK — the parallel GROUP BY engagement
//! (docs/design/m2-sinks.md §2 donor A, notes/m2-agg-sink.md).
//!
//! Shape (phase 1): a SERIAL-plan hashed Agg (AGGSPLIT_SIMPLE) over an
//! unprojected pgrcolumnar SeqScan, K2 single-int-key compact class, byval
//! whitelist transitions with catalog combine functions, identity emit —
//! executed as one runtime ParallelSink (ACCEPT + COMBINE task sets) at
//! DOP N on the M1 pinned-RG machinery. The plan surface stays the serial
//! plan; engagement is FORCED/explicit:
//!
//!   PGRUST_RUNTIME=1  (pool spawned at postmaster start, M0 kill switch)
//!   SET pgrust.runtime_agg_pool = <dop>   (never consulted by the planner)
//!
//! Execution model:
//!  * LEADER: admission (fail-closed — every refusal is the serial arm,
//!    byte-identically) → parallel context + query-task binding policy →
//!    submit a PINNED RG with the sink's two task sets → launch N helpers →
//!    park (WaitForParallelWorkersToFinish-shaped loop). On completion it
//!    adopts the published per-bucket EmitBufs and becomes a pure emitter.
//!  * HELPERS (bound, at POST_TASK_PARK): build a thread-local executor
//!    over the worker PlannedStmt (root = the Agg subtree), arm the SINK
//!    build (staging + K2 + compact table under the sink cap), then drive
//!    the pinned RG: ACCEPT morsels run the narrow ranged drain below
//!    (survivor collect → compact batch probe → whole-batch fold — the
//!    serial lane's own kernels over the claimed granule range); COMBINE
//!    morsels merge one radix bucket across all sealed Locals and
//!    finalize+project it (paremit) into a self-contained EmitBuf.
//!  * Local discipline (R3/R5): the worker's compact table lives in its
//!    sink Local between morsels (lend/reclaim by move); at the sink cap it
//!    flushes into a radix-partitioned SinkRun; table + run bytes are
//!    budgeted against `work_mem × hash_mem_multiplier` per Local — a
//!    crossing records a BUDGET REFUSAL, aborts the RG, and the leader
//!    falls back to the serial arm (whole-attempt rerun; nothing consumed
//!    twice).
//!
//! WFIN markers (M0 acceptance instrument contract): emitted by the
//! runtime's generic sched.rs channel under `PGRUST_MORSEL_MARKERS=1` —
//! `MORSEL|WFIN|qid=..|pipe=..|worker=..|t_us=..|tasks=..|task_avg_us=..`
//! per (worker, task set); pipe = task-set index. Under the default 3-set
//! sealed plumbing (combine-parallel lane): 0 = ACCEPT, 1 = FREEZE (parallel
//! per-Local SEAL), 2 = COMBINE; under `PGRUST_RUNTIME_AGG_PARSEAL=0` the
//! 2-set layout is 0 = ACCEPT, 1 = COMBINE (and the single-threaded SEAL
//! emits its own `MORSEL|AGGSEAL|arm=2set|...|dur_us=..` line when markers
//! are armed). The arm's own duplicate WFIN emitter was removed at
//! m2-integration: with the sched channel armed, double emission (different
//! time bases) garbled the instrument parser's spread verdicts.

use core::cell::UnsafeCell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use ::executils::EStateData;
use ::nodeagg::sink::{
    sink_build_emit_plan, sink_combine_bucket, sink_emit_bucket, sink_null_only_run,
    sink_partition_remainder, sink_remainder_null_block, sink_remainder_spill_bucket,
    sink_resolve_combines, sink_route_records, sink_run_from_bucket_table, sink_run_from_spill,
    sink_run_spill_bucket, sink_spill_row_bytes, sink_topn_candidates, sink_topn_merge,
    sink_topn_merge_fragments,
    LaneAggTable, SinkCombineFn, SinkEmitAcc, SinkEmitBuf, SinkEmitPlan, SinkKeySpec,
    SinkLocalView, SinkPart, SinkRun, SinkTableHandle, SinkTopnCand, SinkTopnSpec, SINK_NBUCKETS,
    SINK_NULL_BUCKET,
};
use ::types_error::{PgError, PgResult, ERROR};
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::PlannedStmt;
use ::types_nodes::NodeTag;

use super::router::{self, ArmClass, ArmCounter};
use super::stats::{self, RefuseReason, ShapeClass};
use super::{lane_trace, lane_trace_enabled, seq_scan_fusible, ScanFeedShape, ScanK2Scratch};

// ---------------------------------------------------------------------------
// The sink: ParallelSink impl + engagement-shared control state.
// ---------------------------------------------------------------------------

/// Per-worker sink Local: flushed runs + the owned compact table between
/// morsels (+ its SEAL partition) + the M3.5 spill state (created lazily at
/// the first budget crossing when the spill arm is enabled).
#[derive(Default)]
pub(super) struct AggSinkLocal {
    runs: Vec<SinkRun>,
    run_bytes: usize,
    /// Process-ledger mirror of `run_bytes` (GL-CONCMEM-1: the flushed-run
    /// backlog + SEAL partition are per-Local plain-Rust estates that live
    /// through the combine phase — charge them like the tables). Settled at
    /// the same block-grain boundaries `run_bytes` moves at; Drop balances.
    ledger_runs: usize,
    table: Option<SinkTableHandle>,
    part: Option<SinkPart>,
    spill: Option<AggSpillState>,
    /// EA-on-morsels instrument partial (ea-morsels.md §2): written only when
    /// the sink is EA-armed (`sink.ea_scan_node.is_some()`); rides the Local
    /// through SEAL exactly like the agg state it sits beside.
    instr: super::runtime_instr::InstrumentPartial,
    /// numa-combine diagnostic (sampled only when the two-level arm is
    /// engaged): per-morsel socket-half votes of the ACCEPT worker driving
    /// this Local — measures how well the pool-half locals split tracks
    /// real socket placement (the finalize NUMAC marker's agreement term).
    /// Behavior never reads it.
    numa_votes: [u32; 2],
    /// agg192-contention CPROBE: lifetime cap/pressure flushes of this Local
    /// and their bytes (runs are drained by spill epochs, so `runs.len()` at
    /// seal undercounts). Thread-owned; emitted on the AGGSEAL marker line —
    /// discriminates DOP-scaled bounded-cap flush amplification (real extra
    /// work) from shared-line contention (stall-only inflation).
    probe_flushes: u64,
    probe_flush_bytes: u64,
    /// α-gate controller state (cachebudget lane; see the module section by
    /// [`alpha_gate_floor`]). Thread-owned like everything else here; rides
    /// the Local through SEAL for the AGGSEAL observability sums only —
    /// behavior after seal never reads it.
    alpha: AlphaGate,
    /// SCATTER ACCEPT (GL-RADIX-3, [`AggSink::scatter`]): the fold-bypass
    /// per-worker bucket buffers. Built lazily at the first drained batch of
    /// a scatter-armed engagement; its buffered rows flush as ordinary runs
    /// at the sink cap and at SEAL ([`AggSink::seal_partition_local`]).
    scatter: Option<Box<::nodeagg::sink::SinkScatter>>,
}

impl AggSinkLocal {
    /// Settle the process-ledger mirror to the current `run_bytes`
    /// (GL-CONCMEM-1; block-grain call sites only — flush pushes, the
    /// SEAL-partition charge, the spill-epoch drain).
    fn settle_run_ledger(&mut self) {
        if self.run_bytes > self.ledger_runs {
            ::mcx::global_footprint::charge_engine_estate(self.run_bytes - self.ledger_runs);
        } else if self.run_bytes < self.ledger_runs {
            ::mcx::global_footprint::uncharge_engine_estate(self.ledger_runs - self.run_bytes);
        }
        self.ledger_runs = self.run_bytes;
    }
}

impl Drop for AggSinkLocal {
    fn drop(&mut self) {
        // Ledger balance for `settle_run_ledger` (the combine phase reads
        // the runs in place; they die with the Local).
        ::mcx::global_footprint::uncharge_engine_estate(self.ledger_runs);
    }
}

/// Per-Local α-gate state (Müller ADAPTIVE): a pure function of this
/// worker's OWN fold/flush stream — no shared reads, no shared writes.
#[derive(Default)]
struct AlphaGate {
    /// Rows folded into the table since the last flush (any kind).
    window_rows: u64,
    /// Demoted: this Local's flush threshold is the sink's `alpha_floor`.
    demoted: bool,
    /// Rows folded since the demote (the re-probe budget's clock).
    rows_since_demote: u64,
    /// Observability (AGGSEAL marker sums): demote / collapse-restore /
    /// hysteresis re-probe transition counts.
    demotes: u64,
    restores: u64,
    reprobes: u64,
}

/// Summed α-gate transition counts for an AGGSEAL marker line.
fn alpha_sums(locals: &[AggSinkLocal]) -> (u64, u64, u64) {
    locals.iter().fold((0, 0, 0), |(d, r, p), l| {
        (d + l.alpha.demotes, r + l.alpha.restores, p + l.alpha.reprobes)
    })
}

impl AlphaGate {
    /// The effective flush threshold for this Local right now. Undemoted
    /// (or controller unarmed): exactly `sink.cap` — the incumbent cadence.
    #[inline]
    fn cap(&self, sink: &AggSink) -> u32 {
        match sink.alpha_floor {
            Some(f) if self.demoted => f,
            _ => sink.cap,
        }
    }

    /// Fold accounting, once per staged batch (survivor count — rows that
    /// actually probed the table; qual-dropped rows never touch it).
    #[inline]
    fn absorbed(&mut self, rows: usize) {
        self.window_rows += rows as u64;
        if self.demoted {
            self.rows_since_demote += rows as u64;
        }
    }

    /// A CAP flush fired with `entries` distinct groups in the run:
    /// adjudicate α = window_rows / entries against α₀ and transition.
    #[inline]
    fn on_cap_flush(&mut self, entries: usize, sink: &AggSink) {
        self.adjudicate(
            entries,
            sink.alpha_floor,
            sink.cap,
            agg_alpha0_x100(),
            agg_alpha_reprobe_mult(),
        );
    }

    /// The controller core (env-free for the unit tests; the knobs arrive
    /// as arguments).
    fn adjudicate(
        &mut self,
        entries: usize,
        alpha_floor: Option<u32>,
        full_cap: u32,
        alpha0_x100: u64,
        reprobe_mult: u64,
    ) {
        let rows = self.window_rows;
        self.window_rows = 0;
        if alpha_floor.is_none() || entries == 0 {
            return;
        }
        // α ≥ α₀ ⟺ rows × 100 ≥ entries × α₀×100 (integer, no fp).
        let collapse_ok = rows.saturating_mul(100) >= (entries as u64) * alpha0_x100;
        if self.demoted {
            if collapse_ok {
                // The floor window itself collapsed — the phase changed;
                // give the table its budget share back immediately.
                self.demoted = false;
                self.restores += 1;
            } else if reprobe_mult > 0
                && self.rows_since_demote >= reprobe_mult.saturating_mul(full_cap as u64)
            {
                // Müller hysteresis: one full-cap probe window — the NEXT
                // fill re-adjudicates (and re-demotes if α is still low,
                // restarting this clock).
                self.demoted = false;
                self.reprobes += 1;
            }
        } else if !collapse_ok {
            self.demoted = true;
            self.rows_since_demote = 0;
            self.demotes += 1;
        }
    }

    /// A non-cap (budget-pressure) flush emptied the table: the fill window
    /// restarts; no adjudication (the window didn't reach the threshold —
    /// its α is not the fill-window statistic the controller is defined on).
    #[inline]
    fn on_pressure_flush(&mut self) {
        self.window_rows = 0;
    }
}

/// A Local's spill face: its single-writer spill file (epochs of
/// bucket-contiguous run records) plus the spilled epochs' NULL-group
/// blocks, which never touch the file (design §3). Plain data between
/// events; rides the Local through SEAL like everything else.
struct AggSpillState {
    file: ::spillset::SpillFile,
    null_blocks: Vec<Vec<u64>>,
}

/// Which worker drain feeds the sink build.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SinkDrain {
    /// Unprojected scan, K2 single-int-key batch probe.
    K2,
    /// Unprojected scan, packed multi-int composite key (Mk car, one/two-word-image
    /// class) — `scan_mk_batch` per staged batch, fail-closed off-compact.
    /// Int components only: the packed image is value-derived, so worker
    /// tables merge on the canonical key words verbatim (no per-worker
    /// intern state, no numeric pack legality mid-build).
    Mk,
    /// Projected scan, expr-key feed (Arith/TsTrunc/Reduced kinds) —
    /// `exprkey_sink_batch` per staged batch, fail-closed off-compact.
    ExprKey,
}

struct AggSink {
    drain: SinkDrain,
    /// Reduced-key shape (worker arm re-derives and must match; the emit
    /// plan's Derived columns came from it). None = single-key.
    red: Option<::nodeagg::RedShape>,
    /// Packed multi-key shape (SinkDrain::Mk; worker arm re-derives and
    /// must match — the emit plan's MultiComp columns came from it).
    mk: Option<::nodeagg::MkShape>,
    cap: u32,
    /// α-gate demoted flush threshold (cachebudget lane; [`alpha_gate_floor`]
    /// — Some arms the per-Local controller, None = structurally off for
    /// this engagement). Fixed at construction like `cap`.
    alpha_floor: Option<u32>,
    /// Per-socket shared-table EXPERIMENT face (cachebudget D2, hard
    /// default-OFF; see [`SharedAggFace`]). None on every default boot.
    shared: Option<SharedAggFace>,
    /// Per-Local budget: work_mem × hash_mem_multiplier (R3 envelope).
    budget: usize,
    key_words: usize,
    state_bytes: usize,
    width: u8,
    /// Any byref state class present (PolyInt128/AvgInt8): the drain's
    /// budget accounting adds the aggcontext subtree (states live there).
    byref_states: bool,
    combines: Vec<SinkCombineFn>,
    emit: SinkEmitPlan,
    /// Combine-phase top-N composition (m3-sort-b car 1): armed when the
    /// sink's consumer is a bounded single-int8-column Sort (the drive
    /// chain resolved the spec at engagement). Selection is an EXTRA pass
    /// per combine claim; the emit buffers stay full, so a degrade (NULL
    /// order transvalue) publishes the plain full drain — no abort.
    topn: Option<SinkTopnSpec>,
    /// 256 per-partition winner candidate lists; slot b written only by
    /// partition b's combine task (single writer, as `out_emit`).
    topn_cands: Vec<UnsafeCell<Vec<SinkTopnCand>>>,
    /// A combine declined the selection (NULL order transvalue): global
    /// degrade to the full drain (correct either way — winners are a drain
    /// filter, never a data transform).
    topn_degraded: AtomicBool,
    /// Top-N materialization mode (topn-winners-only inc-2; winners-phase2
    /// lifts the spill-armed exclusion), meaningful only when `topn` is
    /// armed. Resolved by the §3.2 ladder: leader admission (kill switch, or
    /// spill-armed under `WINNERS_SPILL=0`, → FullDrain) then SEAL
    /// (pass-through shape → FullDrain) — IMMUTABLE once the first combine
    /// claim runs (SEAL happens-before every combine by last-worker-out).
    /// Encoded as `TOPN_MODE_*` in an AtomicU8 because SEAL writes through
    /// `&self`.
    topn_mode: AtomicU8,
    /// WinnersOnly selection declined (NULL/pending order transvalue): the
    /// whole attempt is REFUSED → R5 serial rerun (demote=refusal doctrine;
    /// design §3.2 step 3). Fail-closed and count-gated ≈0: the pgrcolumnar
    /// envelope (sort-b decision 6) makes the trigger structurally
    /// unreachable on every admitted feed.
    topn_refused: AtomicBool,
    /// inc-1 winners-only evidence counters (docs/design/topn-winners-only.md
    /// §6): attribute the combine phase's cost between the merged-table
    /// build, the selection pass, and the emit materialization, plus the
    /// materialized-row vs candidate-row split. Populated ONLY when `topn`
    /// is armed (off-path-free: unarmed engagements read no clocks); read
    /// once at adopt for the trace line. Nanos are summed raw claim time
    /// across all workers (worker-time, not wall — divide by the engaged
    /// DOP for a critical-path estimate).
    topn_ctr: TopnCounters,
    /// LIMIT-k-no-ORDER group-admission freeze (band-2a freeze class; see the
    /// sink.rs section doc): armed only on the Mk drain from a bare
    /// Limit-over-Agg bound; structurally never co-armed with `topn`.
    /// Workers install/consult it through [`scan_mk_batch`]; the combine
    /// filters merged buckets to set members; seal/passthrough/adopt fast
    /// paths are skipped once FROZEN (their tables may carry stragglers).
    freeze: Option<Arc<::nodeagg::sink::SinkFreeze>>,
    /// SEAL-FLUSH (radix seal) arm — GL-RADIX-1 (the groupby-high port):
    /// on the admitted high-NDV band the SEAL pass flushes each Local's
    /// remainder table into ONE final bucket-contiguous SinkRun (the
    /// cap-flush bodies verbatim; [`agg_sealflush_enabled`]) instead of
    /// building the SEAL index — the combine then streams EVERY face
    /// sequentially (the incumbent parallel-finalize lane's raw-exchange
    /// merge shape), where the SEAL index random-accesses up to DOP × cap
    /// entries across the Locals' tables at combine (the high-NDV combine's
    /// last latency-bound term). Resolved at construction; false = the
    /// incumbent SEAL partition exactly.
    seal_flush: bool,
    /// Seal-flush engagement witness: remainder rows handed through
    /// seal-flush runs (non-NULL; summed across Locals). Read by the
    /// AGGSEAL marker lines.
    sealflush_rows: AtomicU64,
    /// SCATTER ACCEPT arm (GL-RADIX-3, the fold-bypass drain): on the
    /// admitted low-α band (α_est ≤ [`agg_scatter_alpha`], est_groups ≥
    /// [`agg_scatter_floor`], K2 byval-POD, DOP>1, no topn/freeze/shared/
    /// dict composition; kill switch [`agg_scatter_enabled`] DEFAULT OFF)
    /// the K2 drain skips the worker table entirely and radix-scatters each
    /// surviving row as a single-row state block into bucket-contiguous
    /// [`SinkRun`]s (::nodeagg::sink::SinkScatter). Deletes the α≈1 accept's
    /// probe-miss + insert + cap-flush cycle; the combine consumes the runs
    /// unchanged (a scatter run is a run with α = 1). Resolved at
    /// construction; false = the incumbent drain exactly.
    scatter: bool,
    /// VEC ACCEPT arm (GL-VECACCEPT-2, the K2 substrate port of the
    /// vecaccept lane): the accept claims run the whole-granule direct-lane
    /// drive ([`sink_drain_range_vec`]) — no window staging, no SoA deform,
    /// no survivor collection, no per-row key gather; the probe
    /// (`agg_hash_compact_batch`, prefetched) and the fold
    /// (`agg_fold_staged`) are the incumbent kernels fed 1024-row chunks
    /// from per-granule lane copies. Resolved at admission (kill switch
    /// [`agg_vecaccept_k2_enabled`] DEFAULT OFF; fail-closed shape gate at
    /// the construction site). Off = the incumbent staged drain,
    /// branch-for-branch.
    vec_accept: bool,
    /// Vec-accept census (rows through the direct-lane drive — the
    /// engagement witness; equals the scan's accepted rows when armed).
    vec_rows: AtomicU64,
    /// Scatter engagement witness: rows handed through scatter-built runs
    /// (non-NULL; summed across Locals). Read by the AGGSEAL marker lines —
    /// the sealflush_rows discipline exactly.
    scatter_rows: AtomicU64,
    /// 256 per-bucket outputs; slot b is written only by the combine task
    /// that claimed partition b (single writer by the sink contract).
    out_emit: Vec<UnsafeCell<SinkEmitBuf>>,
    /// finalize's published output (leader consumes after completion).
    published: Mutex<Option<SinkPublished>>,
    /// TRUE TABLE ADOPT (dop1-tax2 inc-1) shape gate, fixed at construction:
    /// every emit column byval AND no byref combine state class — a byref
    /// transvalue points into a WORKER aggcontext, which dies with the
    /// helpers; byref shapes keep the EmitBuf arms (whose arena copy is what
    /// makes them self-contained).
    adopt_shape: bool,
    /// Seal-time hand-off: the single sealed Local's whole table (no SEAL
    /// partition — the leader drains it linearly). Set only when the LIVE
    /// seal census admits (exactly one sealed Local, zero flushed runs,
    /// adopt_shape) — every combine claim then no-ops and finalize
    /// publishes the table wholesale (the ledger's literal "adopt its
    /// table (pointer hand-off)").
    adopted: Mutex<Option<SinkTableHandle>>,
    /// Lock-free mirror of `adopted` for the per-claim combine check
    /// (written once at SEAL, which happens-before every combine claim).
    adopted_flag: AtomicBool,
    /// Forked-Local census for the 3-set parallel seal's adopt-skip: a seal
    /// claim may skip its partition pass only when it can PROVE the adopt
    /// census will take the table wholesale (exactly one fork). Counted at
    /// fork (accept set), read at seal (deps=[accept] — final by then).
    /// Overcounting (a stale-generation re-fork) only costs a wasted
    /// partition pass — the safe direction.
    forks: AtomicUsize,
    /// Abort/observability control (shared with the engagement payload).
    rg: OnceLock<runtime::WeakRgHandle>,
    failed: AtomicBool,
    error: Mutex<Option<Box<PgError>>>,
    /// A Local crossed its memory budget: not an error — the leader falls
    /// back to the serial arm (R5 whole-attempt rerun).
    budget_refused: AtomicBool,
    /// Combine-phase retained CONTENT bytes (the per-bucket emit buffers,
    /// summed across claims) — the m2-integration R3 accounting for the
    /// merged RESULT, checked against the ADMITTED envelope (forked Locals
    /// × per-Local budget; see the check site). Crossing = budget refusal.
    /// LIFETIME NOTE: this sink object is strictly per-engagement
    /// (constructed in try_engage_hashagg_runtime); if sink regeneration
    /// (the M1+ re-publish regime sink.rs documents) ever reuses one sink
    /// across generations, this counter — like the distinct arm's
    /// merged_bytes — must reset at re-publish or regenerated engagements
    /// double-count.
    combined_bytes: AtomicUsize,
    /// M3.5 spill arm: the engagement's spill set (None = spill disabled →
    /// budget crossings refuse exactly as before).
    spill_set: Option<Arc<::spillset::SpillSet>>,
    /// Spill observability (gate-record counters, R4 line).
    spill_epochs: AtomicU64,
    spilled_bytes: AtomicU64,
    /// Combine-split observability (inc-2b): split events, deepest level
    /// reached, and a per-sink uniquifier for split-file names.
    combine_splits: AtomicU64,
    split_depth_max: AtomicU64,
    split_uniq: AtomicU64,
    /// combine16 observability: in-memory merge claims and the merged
    /// tables' entry-set grow / two-level-convert counts (flat presized
    /// tables must show 0/0 — the engagement gate's evidence line).
    combine16_claims: AtomicU64,
    combine16_grows: AtomicU64,
    combine16_converts: AtomicU64,
    /// EA-on-morsels (ea-morsels.md §2): Some(scan plan_node_id) ONLY when
    /// engaged under EXPLAIN ANALYZE — the single EA flag for this sink;
    /// None on every other path (dead-when-off).
    ea_scan_node: Option<i32>,
    /// The accept-phase instrument merge, written at finalize (last-worker-
    /// out) from the sealed Locals; leader reads on clean Completed only.
    ea_instr: Mutex<Option<super::runtime_instr::InstrumentMerged>>,
    /// TIMER mode (inc-3): one clock pair per claim against `ea_epoch`
    /// (shared engagement origin — cross-worker comparable). false in ROWS
    /// mode and on every non-EA path: zero clock reads.
    ea_timer: bool,
    ea_epoch: std::time::Instant,
    /// Two-level socket-local combine (numa-combine item 1): Some = the
    /// engaged claims-shape (armed at construction — kill switch + DOP
    /// threshold + all-byval combines). None = the flat 256-partition pass,
    /// byte-and-time identical to before the lane.
    numa: Option<NumaCombine>,
}

// SAFETY: out_emit cells are written only by the exclusive claimer of their
// partition (the runtime's exactly-once combine claim) and read only by
// finalize, which happens-after every combine by last-worker-out. The
// numa-combine partial slots have the same discipline one level down: slot
// (h,b) is written only by the pass-A claim that popped (h,b) (cursor
// fetch_add = exactly-once ownership) and consumed only by bucket b's
// elected FINAL claim, whose counter Acquire pairs with the writers'
// Release increments.
unsafe impl Sync for AggSink {}

// ---------------------------------------------------------------------------
// Two-level socket-local combine (numa-combine lane, item 1).
// ---------------------------------------------------------------------------

/// The engaged claims-shape's shared state. The partition space doubles to
/// `2 × SINK_NBUCKETS` CLAIM CREDITS; each credit pops one pass-A item
/// (half h, bucket b) from the per-half cursors — SELF-STEERED: the claiming
/// worker samples its own socket and drains its own half's cursor first,
/// then steals. Pass-A merges bucket b across ONLY half h's sealed locals
/// and freezes the result into a single-bucket partial [`SinkRun`]; the
/// claim that completes a bucket's SECOND partial is elected to run the
/// FINAL stage (2-way partial merge + the unchanged combine tail) at once.
///
/// Credit/item accounting: 512 credits, 512 items; a `fetch_add` pop < 256
/// owns its item exactly once, and a credit that finds both cursors ≥ 256
/// has proven every item is already popped — no loss, no dup, no waiting.
///
/// Byte identity vs the flat pass: first-seen order COMPOSES across
/// contiguous locals halves (partial-0 replays half 0's arrivals in the
/// flat order, partial-1 follows — a key first seen in half 1 trails every
/// half-0-first key in both shapes), and the all-byval gate keeps state
/// regrouping bit-exact (the whitelist is int adds / min-max / bool —
/// associative). Covered by sink.rs `two_level_partial_runs_match_flat_*`.
struct NumaCombine {
    /// Pass-A claim cursors, one per locals half.
    cursors: [AtomicU32; 2],
    /// Per-bucket completion counters; 1→2 elects the FINAL claim.
    done: Vec<AtomicU8>,
    /// `2 × SINK_NBUCKETS` partial slots (h-major); single writer per slot
    /// (the popping claim), single consumer (the elected final). The run's
    /// state blocks are copied VERBATIM out of the pass-A table, so a
    /// min/max(text) shape's text pointers reference that table's own store —
    /// which therefore rides in the slot and is released by the final.
    partials: Vec<UnsafeCell<Option<(SinkRun, Option<::lanefold::StrStateArena>)>>>,
    /// Observability (NUMAC finalize marker; behavior never reads these).
    steer_hit: AtomicU64,
    steer_miss: AtomicU64,
    /// Buckets whose final ran the FLAT body (ineligible: NULL bucket,
    /// <2 locals, frozen engagement, over-budget verdict, or a pass-A
    /// asymmetry after an error).
    finals_flat: AtomicU64,
    partial_ns: AtomicU64,
    final_ns: AtomicU64,
    /// Live partial-run bytes (transient retained state between a bucket's
    /// pass-A and its final) + the observed peak.
    partial_bytes: AtomicUsize,
    partial_bytes_peak: AtomicUsize,
}

// SAFETY: the partial slots are single-writer / single-consumer by the
// claims discipline (cursor-pop = exactly-once slot ownership; the 1→2
// counter election = exactly-once consumption, Acquire-paired with the
// writers' Release increments — see the AggSink Sync comment); every other
// field is an atomic.
unsafe impl Sync for NumaCombine {}

impl NumaCombine {
    fn new() -> NumaCombine {
        NumaCombine {
            cursors: [AtomicU32::new(0), AtomicU32::new(0)],
            done: (0..SINK_NBUCKETS).map(|_| AtomicU8::new(0)).collect(),
            partials: (0..2 * SINK_NBUCKETS).map(|_| UnsafeCell::new(None)).collect(),
            steer_hit: AtomicU64::new(0),
            steer_miss: AtomicU64::new(0),
            finals_flat: AtomicU64::new(0),
            partial_ns: AtomicU64::new(0),
            final_ns: AtomicU64::new(0),
            partial_bytes: AtomicUsize::new(0),
            partial_bytes_peak: AtomicUsize::new(0),
        }
    }

    /// Pop one pass-A item, own half first. `None` = every item is popped
    /// (this credit's work is done).
    fn pop(&self, my: usize) -> Option<(usize, usize)> {
        debug_assert!(my < 2);
        for (attempt, h) in [my, 1 - my].into_iter().enumerate() {
            let b = self.cursors[h].fetch_add(1, Ordering::Relaxed) as usize;
            if b < SINK_NBUCKETS {
                if attempt == 0 {
                    self.steer_hit.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.steer_miss.fetch_add(1, Ordering::Relaxed);
                }
                return Some((h, b));
            }
        }
        None
    }

    fn note_partial_bytes(&self, n: usize) {
        let live = self.partial_bytes.fetch_add(n, Ordering::Relaxed) + n;
        self.partial_bytes_peak.fetch_max(live, Ordering::Relaxed);
    }

    fn release_partial_bytes(&self, n: usize) {
        self.partial_bytes.fetch_sub(n, Ordering::Relaxed);
    }
}

/// `PGRUST_RUNTIME_AGG_NUMA_COMBINE` kill switch (default ON): 0/off = the
/// flat 256-partition combine everywhere, byte-and-time identical to the
/// pre-lane binary.
fn numa_combine_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(
            std::env::var("PGRUST_RUNTIME_AGG_NUMA_COMBINE").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// `PGRUST_RUNTIME_AGG_NUMA_COMBINE_DOP` engagement threshold (default 96 —
/// the 48xl two-socket regime; the flat pass is byte-identical below it, so
/// small DOPs keep the exact t21 shape). Lower it to force the engaged
/// shape through the 16-thread byte gates.
fn numa_combine_dop_min() -> i32 {
    static N: OnceLock<i32> = OnceLock::new();
    crate::once_val(&N, || {
        std::env::var("PGRUST_RUNTIME_AGG_NUMA_COMBINE_DOP")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(96)
    })
}

/// The claiming thread's socket HALF (0/1), for claim steering and the
/// locals-split agreement diagnostic. Linux: `sched_getcpu` + the
/// /sys/devices/system/node/*/cpulist map (nodes split into two halves for
/// >2-node topologies; single-node machines map everything to 0 — the
/// steering degrades to a plain shared cursor, still correct). Elsewhere:
/// `None` (callers fall back to the claim credit's own half — deterministic,
/// which is what the non-Linux unit/e2e environments want).
fn numa_current_half() -> Option<usize> {
    #[cfg(target_os = "linux")]
    {
        extern "C" {
            fn sched_getcpu() -> core::ffi::c_int;
        }
        static MAP: OnceLock<Vec<u8>> = OnceLock::new();
        let map = MAP.get_or_init(|| {
            let mut nodes: Vec<Vec<usize>> = Vec::new();
            for node in 0..1024usize {
                let Ok(list) = std::fs::read_to_string(format!(
                    "/sys/devices/system/node/node{node}/cpulist"
                )) else {
                    break;
                };
                let mut cpus = Vec::new();
                for part in list.trim().split(',').filter(|s| !s.is_empty()) {
                    let mut ends = part.splitn(2, '-');
                    let lo: usize = match ends.next().and_then(|s| s.parse().ok()) {
                        Some(v) => v,
                        None => continue,
                    };
                    let hi: usize =
                        ends.next().and_then(|s| s.parse().ok()).unwrap_or(lo);
                    cpus.extend(lo..=hi);
                }
                nodes.push(cpus);
            }
            let nnodes = nodes.len().max(1);
            let ncpus = nodes.iter().map(|c| c.iter().max().map_or(0, |m| m + 1)).max().unwrap_or(0);
            let mut map = vec![0u8; ncpus];
            for (n, cpus) in nodes.iter().enumerate() {
                let half = u8::from(n >= nnodes.div_ceil(2));
                for &c in cpus {
                    map[c] = half;
                }
            }
            map
        });
        // SAFETY: no preconditions; vDSO-backed on Linux.
        let cpu = unsafe { sched_getcpu() };
        if cpu >= 0 {
            return Some(map.get(cpu as usize).copied().unwrap_or(0) as usize);
        }
        None
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// The engaged shape's locals split: CONTIGUOUS halves of the sealed slice
/// (slot order). Contiguity is load-bearing — it is what makes first-seen
/// order compose (see [`NumaCombine`]); a sampled per-Local socket grouping
/// would be nondeterministic run-to-run AND break the flat-order identity.
fn numa_half_slice(locals: &[AggSinkLocal], h: usize) -> &[AggSinkLocal] {
    let mid = locals.len() / 2;
    if h == 0 {
        &locals[..mid]
    } else {
        &locals[mid..]
    }
}

/// Top-N materialization modes (topn-winners-only §3.2). `WinnersOnly` is
/// the product default when the spec arms: each combine claim materializes
/// ONLY its partition's ≤bound candidate rows; degrade is NOT free, so every
/// degrade trigger is resolved before the first combine claim and the one
/// runtime trigger left (NULL order transvalue) is a refusal → R5 serial
/// rerun. `FullDrain` is the landed decision-1 behavior verbatim (full
/// buffers, selection = drain filter, mid-combine declines degrade globally)
/// — the permanent compat/spill/oracle mode.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TopnMode {
    WinnersOnly,
    FullDrain,
}

const TOPN_MODE_FULL: u8 = 0;
const TOPN_MODE_WINNERS: u8 = 1;

impl TopnMode {
    fn decode(v: u8) -> TopnMode {
        if v == TOPN_MODE_WINNERS { TopnMode::WinnersOnly } else { TopnMode::FullDrain }
    }

    fn encode(self) -> u8 {
        match self {
            TopnMode::WinnersOnly => TOPN_MODE_WINNERS,
            TopnMode::FullDrain => TOPN_MODE_FULL,
        }
    }
}

/// §3.2 step 1 — leader-admission mode resolution. PHASE 2 (winners-phase2,
/// split×selection): spill-armed engagements now resolve WinnersOnly too —
/// the m35 combine-split composes with the selection (per-fragment candidate
/// lists, merged before truncation; see `split_leaf_emit`), so the phase-1
/// H3 exclusion is lifted. `PGRUST_RUNTIME_AGG_TOPN_WINNERS_SPILL=0`
/// restores the phase-1 arm exactly (spill-armed → FullDrain — the A/B
/// attribution channel); the kill switch still forces FullDrain everywhere.
fn resolve_topn_mode_admission(
    spill_armed: bool,
    winners_enabled: bool,
    winners_spill_enabled: bool,
) -> TopnMode {
    if !winners_enabled || (spill_armed && !winners_spill_enabled) {
        TopnMode::FullDrain
    } else {
        TopnMode::WinnersOnly
    }
}

/// §3.2 step 2 — SEAL mode resolution: the single-Local pass-through shape
/// (exactly one sealed Local, no runs, no spill face) never builds a merged
/// table, so selection has nothing to run on — resolve FullDrain BEFORE any
/// combine claim instead of degrading mid-claim. A pure function of the
/// sealed census (uniform across all 256 claims).
fn resolve_topn_mode_seal(admission: TopnMode, passthrough_shape: bool) -> TopnMode {
    if passthrough_shape {
        TopnMode::FullDrain
    } else {
        admission
    }
}

/// `PGRUST_RUNTIME_AGG_TOPN_WINNERS` kill switch (default ON): 0/off =
/// FullDrain everywhere (decision-1 behavior exactly). The outer
/// `PGRUST_RUNTIME_AGG_TOPN=0` still kills the whole composition.
fn topn_winners_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(
            std::env::var("PGRUST_RUNTIME_AGG_TOPN_WINNERS").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// `PGRUST_RUNTIME_AGG_TOPN_WINNERS_SPILL` (default ON): 0/off restores the
/// ratified phase-1 spill-armed exclusion (spill-armed engagements ride
/// FullDrain) — the winners-phase2 A/B and rollback channel.
fn topn_winners_spill_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(
            std::env::var("PGRUST_RUNTIME_AGG_TOPN_WINNERS_SPILL").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// `PGRUST_RUNTIME_AGG_PARSEAL` (default ON): the 3-set sealed plumbing —
/// SEAL's per-Local partition pass runs parallel across Local slots (its own
/// task set between ACCEPT and COMBINE, the distinct sink's SEALCVT shape).
/// 0/off restores the 2-set plumbing (single-threaded SEAL) exactly — the
/// A/B and rollback channel (combine-parallel lane).
fn parseal_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(
            std::env::var("PGRUST_RUNTIME_AGG_PARSEAL").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// `PGRUST_MORSEL_MARKERS=1` (the sched.rs WFIN channel's env, re-read here
/// for the arm's own AGGSEAL line): default OFF — zero cost beyond one
/// branch per SEAL.
fn agg_markers_on() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        matches!(std::env::var("PGRUST_MORSEL_MARKERS").as_deref(), Ok("1") | Ok("on"))
    })
}

/// Leg-R fault injection (`PGRUST_RUNTIME_AGG_TOPN_FAULT=decline`): simulate
/// the NULL-order-transvalue selection decline, which is structurally
/// unreachable on real pgrcolumnar feeds (sort-b decision 6) — the e2e refusal
/// gate needs a trigger the corpus cannot produce. Read once; consulted only
/// on topn-armed combine claims (off-path-free).
fn topn_fault_decline() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        std::env::var("PGRUST_RUNTIME_AGG_TOPN_FAULT").as_deref() == Ok("decline")
    })
}

/// Combine-phase cost-attribution counters for topn-armed engagements
/// (winners-only inc-1 — the design's stop-rule evidence: what share of the
/// combine phase is the FULL emit materialization, and how many of the
/// materialized rows are selection losers). Trace-only observability; no
/// behavior reads these.
#[derive(Default)]
struct TopnCounters {
    /// Merged-table build time (`sink_combine_bucket`), ns, summed claims.
    build_ns: AtomicU64,
    /// Selection pass time (`sink_topn_candidates`), ns, summed claims.
    select_ns: AtomicU64,
    /// Emit materialization time (`sink_emit_bucket` / pass-through), ns.
    emit_ns: AtomicU64,
    /// Rows materialized through `emit_row` (all merged groups today).
    mat_rows: AtomicU64,
    /// Winner-candidate rows selected (≤ 256 × bound) — what winners-only
    /// materialization would materialize instead.
    cand_rows: AtomicU64,
}

/// What finalize hands the leader.
enum SinkPublished {
    /// Combine-materialized per-bucket EmitBufs (the general arm), plus the
    /// composed top-N winner list (m3-sort-b car 1; `None` = full drain).
    Emit(Vec<SinkEmitBuf>, Option<Vec<(u16, u32)>>),
    /// TRUE TABLE ADOPT: the single sealed Local's whole table — no SEAL
    /// partition, no merge, no re-insert, no EmitBuf materialization; the
    /// leader drains the table LINEARLY (insertion order = the DOP1
    /// build's serial-equivalent order).
    Table(SinkTableHandle),
}

impl AggSink {
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

    fn refuse_budget(&self) {
        self.budget_refused.store(true, Ordering::SeqCst);
        self.failed.store(true, Ordering::SeqCst);
        self.abort_rg();
    }

    /// The armed top-N materialization mode (§3.2). Only consulted when
    /// `topn` is armed.
    fn topn_mode(&self) -> TopnMode {
        TopnMode::decode(self.topn_mode.load(Ordering::Acquire))
    }

    /// WinnersOnly refusal (NULL/pending order transvalue mid-combine): the
    /// attempt dies wholesale — same R5 whole-attempt serial-rerun semantics
    /// as a budget refusal, under its own named reason (observability +
    /// count-gate ≈0). Never a mid-flight mode flip.
    fn refuse_topn(&self) {
        self.topn_refused.store(true, Ordering::SeqCst);
        self.failed.store(true, Ordering::SeqCst);
        self.abort_rg();
    }

    /// Any non-error refusal reason (leader falls back to the serial arm;
    /// helpers must not convert the aborted drive into a query error).
    fn refused_any(&self) -> bool {
        self.budget_refused.load(Ordering::SeqCst) || self.topn_refused.load(Ordering::SeqCst)
    }

    fn abort_rg(&self) {
        if let Some(rg) = self.rg.get().and_then(|w| w.upgrade()) {
            rg.abort();
        }
    }

    fn take_error(&self) -> Option<Box<PgError>> {
        self.error.lock().unwrap_or_else(|p| p.into_inner()).take()
    }

    /// Shared combine tail (merge and pass-through arms): meter the RETAINED
    /// emit buffer against the admitted envelope (R3, m2-integration audit —
    /// the emit buffers are the merged result, held until the leader
    /// drains; the union is bounded by the admitted Locals' content, so a
    /// crossing is a real accounting surprise → budget refusal, fail-closed)
    /// and store it in the claimed partition's slot.
    fn retain_bucket(&self, part: u64, buf: SinkEmitBuf, nlocals: usize) -> PgResult<()> {
        let retained = buf.bytes();
        let total = self.combined_bytes.fetch_add(retained, Ordering::Relaxed) + retained;
        // COMPOSITION (train-13, m35 spill x train-12 R3): the in-memory
        // envelope (admitted Locals x per-Local budget) is the LAW for
        // spill-disabled engagements — with the spill arm ON, the merged
        // result is legitimately bounded by the SPILLED content (the m35
        // ratified behavior: the combine's per-partition pre-build check
        // bounds each claim's transient table; the retained emit is the
        // result itself). Metering stays on for observability either way.
        if self.spill_set.is_none()
            && total > self.budget.saturating_mul(nlocals.max(1))
        {
            self.refuse_budget();
            return Ok(());
        }
        // SAFETY: partition `part` is claimed exactly once (runtime
        // contract); this is its single writer.
        unsafe { *self.out_emit[part as usize].get() = buf };
        Ok(())
    }

    // --- SEAL decomposition (combine-parallel lane) ---------------------
    // The 2-set arm's single-threaded seal and the 3-set arm's parallel
    // per-Local seal share these pieces; the ORDER differs (2-set: census →
    // partition loop → topn resolution; 3-set: per-claim partition with an
    // adopt-shape skip, then census + topn in sealed_ready). Outcomes are
    // byte-identical: partition_remainder is a pure function of one Local's
    // table, and both census decisions read post-accept state that no seal
    // work mutates.

    /// TRUE TABLE ADOPT census (dop1-tax2 inc-1b), verbatim from the 2-set
    /// seal. True = adopted (callers skip partition/combine work).
    fn try_adopt_census(&self, locals: &mut [AggSinkLocal]) -> bool {
        let frozen = self.freeze.as_ref().is_some_and(|f| f.frozen());
        if self.adopt_shape && !frozen {
            if let [l] = &mut *locals {
                // Scatter composition: a scatter-armed Local's rows live in
                // its scatter buffers/runs, NOT its (empty) table — the
                // wholesale table hand-off would drop them. Scatter admits
                // DOP>1 only, so this census is structurally a non-scatter
                // shape; fail closed if a degenerate launch ever lands here.
                if l.runs.is_empty()
                    && l.spill.is_none()
                    && l.table.is_some()
                    && l.scatter.is_none()
                {
                    let t = l.table.take().expect("checked Some");
                    *self.adopted.lock().unwrap_or_else(|g| g.into_inner()) = Some(t);
                    self.adopted_flag.store(true, Ordering::SeqCst);
                    return true;
                }
            }
        }
        false
    }

    /// Partition ONE Local's remainder table + the R3 SEAL-index accounting
    /// (per-Local, so safely parallel across seal claims — the refusal flag
    /// is idempotent and fail-closed).
    fn seal_partition_local(&self, l: &mut AggSinkLocal) {
        // SCATTER ACCEPT remainder (GL-RADIX-3): the Local's buffered
        // scatter rows leave as one final bucket-contiguous run, appended
        // after the cap-flushed scatter runs — chronological face order, so
        // first-seen order is preserved (the seal-flush arm's own cadence
        // argument). The worker table stayed EMPTY under scatter (nothing
        // ever folds into it), so the branches below are no-ops on its
        // remainder. R3: the run's bytes ride `run_bytes` into the same
        // budget checks below.
        if let Some(mut sc) = l.scatter.take() {
            if let Some(run) = sc.take_run() {
                self.scatter_rows.fetch_add(run.nrows() as u64, Ordering::Relaxed);
                l.run_bytes += run.bytes();
                l.runs.push(run);
                l.settle_run_ledger();
            }
        }
        if self.seal_flush {
            // Radix seal-flush arm ([`AggSink::seal_flush`]): the remainder
            // leaves as one final bucket-contiguous run, appended LAST — the
            // SEAL face's own visit position in the combine's first-seen
            // order, so the merge is byte-identical (flush cadence is
            // semantics-free; unit-pinned by
            // seal_flush_run_matches_remainder_view). No SEAL index is
            // built. R3: the run's bytes replace the table + index charge
            // (same content, contiguous layout); crossing = budget refusal,
            // exactly the incumbent arm's discipline.
            if let Some(mut t) = l.table.take() {
                if let Some(run) = t.flush_remainder() {
                    self.sealflush_rows.fetch_add(run.nrows() as u64, Ordering::Relaxed);
                    l.run_bytes += run.bytes();
                    l.runs.push(run);
                    l.settle_run_ledger();
                }
            }
            l.part = None;
            if l.run_bytes > self.budget {
                self.refuse_budget();
            }
            return;
        }
        // Canonical (text-bearing) shapes partition by canonical bytes;
        // word shapes by key words — the handle dispatches.
        l.part = l.table.as_mut().map(::nodeagg::sink::SinkTableHandle::partition_remainder);
        // R3 accounting (m2-integration audit): the SEAL index is per-Local
        // retained memory that lives through the whole combine phase —
        // charge it like a run. Crossing = budget refusal (R5 whole-attempt
        // rerun), never an error. Table mem includes the intern table (text
        // shapes) — it lives through combine too.
        if let Some(p) = &l.part {
            l.run_bytes += p.bytes();
            l.settle_run_ledger();
            let table_mem = l.table.as_ref().map_or(0, |t| t.mem_used());
            if l.run_bytes + table_mem > self.budget {
                self.refuse_budget();
            }
        }
    }

    /// combine16 evidence: fold one merged bucket table's construction
    /// counters into the sink totals (three relaxed adds per claim — 256
    /// claims per engagement, noise). Flat presized tables must report
    /// grows == converts == 0; the incumbent path on a large canonical
    /// shape reports the degenerate-top-byte growth this lane removes.
    fn note_combine16(&self, t: &LaneAggTable) {
        self.combine16_claims.fetch_add(1, Ordering::Relaxed);
        self.combine16_grows.fetch_add(t.grow_count() as u64, Ordering::Relaxed);
        self.combine16_converts.fetch_add(t.convert_count() as u64, Ordering::Relaxed);
    }

    /// §3.2 step 2 — SEAL mode resolution (topn-winners-only inc-2),
    /// verbatim from the 2-set seal: the single-Local pass-through shape
    /// never builds a merged table, so an armed selection resolves
    /// FullDrain HERE, before the first combine claim.
    fn resolve_topn_at_seal(&self, locals: &[AggSinkLocal]) {
        if self.topn.is_some() {
            // Keying-class term (GL-SINKSHAPE-1): a Local whose table
            // representation refuses the pass-through emit takes the merge
            // arm at combine, so it is NOT a pass-through shape here — the
            // predicate must match the combine's actual arm choice.
            let passthrough = matches!(
                locals,
                [l] if l.runs.is_empty()
                    && l.spill.is_none()
                    && l.table.as_ref().is_some_and(|t| {
                        ::nodeagg::sink::sink_passthrough_admits(&self.emit, t.table())
                    })
            );
            let mode = resolve_topn_mode_seal(self.topn_mode(), passthrough);
            self.topn_mode.store(mode.encode(), Ordering::Release);
            if passthrough {
                lane_trace("runtime-agg topn: pass-through shape at SEAL — mode=full");
            }
        }
    }
}

impl runtime::ParallelSink for AggSink {
    type Local = AggSinkLocal;

    fn fork(&self, _worker: usize) -> AggSinkLocal {
        self.forks.fetch_add(1, Ordering::SeqCst);
        AggSinkLocal::default()
    }

    fn accept_local(&self, local: &mut AggSinkLocal, worker: usize, range: runtime::MorselRange) {
        if self.failed.load(Ordering::SeqCst) {
            return;
        }
        // numa two-level diagnostic (engaged only): one socket-half vote per
        // morsel — the finalize NUMAC marker's locals-split agreement term.
        if self.numa.is_some() {
            if let Some(h) = numa_current_half() {
                local.numa_votes[h] += 1;
            }
        }
        let r = catch_unwind(AssertUnwindSafe(|| accept_morsel_body(self, local, worker, range)));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(AcceptFail::Budget)) => {
                mark_self_errored();
                self.refuse_budget();
            }
            Ok(Err(AcceptFail::Error(e))) => {
                mark_self_errored();
                // Estimate-failure class (the compact backstop tripping
                // UNDER the sink cap — a planner-underestimate shape the
                // admission gate could not see): a REFUSAL, not an error.
                // The leader reruns serially, byte-identically (the leg-4
                // budget-refusal path). Every other error stays an error.
                if ::nodeagg::sink::is_sink_cap_breach(&e) {
                    self.refuse_budget();
                } else {
                    self.fail(e);
                }
            }
            Err(_panic) => {
                mark_self_errored();
                self.fail(PgError::new(ERROR, "runtime agg sink worker panicked").into());
            }
        }
    }

    /// SEAL: partition every Local's remainder table (single-threaded by
    /// the last-worker-out protocol; counting sort, one pass per Local).
    /// This is the PGRUST_RUNTIME_AGG_PARSEAL=0 arm — the 3-set sealed
    /// plumbing runs the same pieces with the partition pass parallel
    /// across Local slots (see the SealedParallelSink impl below).
    fn seal(&self, locals: &mut [AggSinkLocal]) {
        if self.failed.load(Ordering::SeqCst) {
            return;
        }
        // Shared-table experiment drain (single-threaded here; before the
        // adopt census — injected runs correctly decline the adopt).
        if let Some(sh) = self.shared.as_ref() {
            sh.drain_into(locals);
        }
        let t0 = agg_markers_on().then(std::time::Instant::now);
        // TRUE TABLE ADOPT decision (dop1-tax2 inc-1b) — LIVE STATE at SEAL
        // (the sealed-Local census is final: last-worker-out; a widened
        // engagement forked >=2 Locals and takes the merge arms below).
        // Exactly one sealed Local, zero flushed runs, all-byval shape:
        // hand the table to finalize WHOLESALE — no SEAL partition (the
        // leader drains the table LINEARLY: for a DOP1 build the insertion
        // order IS the serial build's own order — sequential claims — so
        // the drain is serial-faithful AND cache-linear), no combine work,
        // no emit materialization. Memory: the table was charged during
        // accept; no partition index is ever built.
        // Freeze composition: a FROZEN engagement's tables can carry
        // pre-freeze straggler groups (undercounted past the freeze point) —
        // the wholesale table hand-off cannot filter them, so it stands
        // down and the combine's member filter runs instead. An armed-but-
        // never-frozen freeze dropped nothing — the adopt stays valid.
        if self.try_adopt_census(locals) {
            return;
        }
        for l in locals.iter_mut() {
            self.seal_partition_local(l);
            if self.failed.load(Ordering::SeqCst) {
                return;
            }
        }
        self.resolve_topn_at_seal(locals);
        if let Some(t0) = t0 {
            let rows: usize =
                locals.iter().map(|l| l.table.as_ref().map_or(0, |t| t.table().nrows())).sum();
            // Marker channel (PGRUST_MORSEL_MARKERS=1, the WFIN sibling):
            // the single-threaded seal's duration — the phase the 3-set arm
            // parallelizes; its A/B evidence line.
            let flushes: u64 = locals.iter().map(|l| l.probe_flushes).sum();
            let flush_bytes: u64 = locals.iter().map(|l| l.probe_flush_bytes).sum();
            let (ad, ar, ap) = alpha_sums(locals);
            eprintln!(
                "MORSEL|AGGSEAL|arm=2set|locals={}|rows={rows}|flushes={flushes}|flush_bytes={flush_bytes}|alpha_demotes={ad}|alpha_restores={ar}|alpha_reprobes={ap}|dur_us={}|sealflush_rows={}|scatter_rows={}|vec_rows={}",
                locals.len(),
                t0.elapsed().as_micros(),
                self.sealflush_rows.load(Ordering::Relaxed),
                self.scatter_rows.load(Ordering::Relaxed),
                self.vec_rows.load(Ordering::Relaxed),
            );
        }
    }

    fn partitions(&self) -> u64 {
        // numa two-level (item 1): the partition space doubles to CLAIM
        // CREDITS — one per (half, bucket) pass-A item; bucket finals are
        // elected inline by the second finisher.
        if self.numa.is_some() {
            2 * SINK_NBUCKETS as u64
        } else {
            SINK_NBUCKETS as u64
        }
    }

    fn combine(&self, part: u64, _worker: usize, locals: &[AggSinkLocal]) {
        if self.failed.load(Ordering::SeqCst) {
            return;
        }
        // TRUE TABLE ADOPT: seal took the single Local's table — there is
        // nothing to merge and nothing to materialize; finalize publishes
        // the table itself. (Set at SEAL, which happens-before every
        // combine claim; SeqCst pairs with the seal store.)
        if self.adopted_flag.load(Ordering::SeqCst) {
            return;
        }
        let r = catch_unwind(AssertUnwindSafe(|| -> PgResult<CombineOutcome> {
            // Two-level socket-local shape (numa-combine item 1): `part` is
            // a CLAIM CREDIT, not a bucket — the pass-A pop decides the
            // (half, bucket) this claim serves. Flat shape: `part` IS the
            // bucket, exactly the t21 body.
            match &self.numa {
                Some(nc) => self.combine_numa(nc, part, locals),
                None => self.combine_bucket_flat(part as usize, locals),
            }
        }));
        match r {
            Ok(Ok(CombineOutcome::Done)) => {}
            Ok(Ok(CombineOutcome::OverBudget)) => {
                lane_trace("runtime-agg: combine partition over budget (split depth cap or spill disarmed) — serial rerun");
                self.refuse_budget();
            }
            Ok(Ok(CombineOutcome::TopnDeclined)) => {
                // Winners-only refusal (§3.2 step 3): fail-closed, count-
                // gated ≈0 (structurally unreachable on pgrcolumnar feeds —
                // sort-b decision 6). The e2e leg-R gate greps this line.
                lane_trace(
                    "runtime-agg: topn-winners-refused (NULL order transvalue) — serial rerun",
                );
                self.refuse_topn();
            }
            Ok(Err(e)) => self.fail(e),
            Err(_panic) => {
                self.fail(PgError::new(ERROR, "runtime agg sink combine panicked").into())
            }
        }
    }

    /// Publish: the adopted table (TRUE TABLE ADOPT — the pointer hand-off)
    /// or the 256 emit buffers, moved out (O(partitions), the §6 contract).
    /// Locals drop with the plumbing right after.
    fn finalize(&self, locals: &[AggSinkLocal]) {
        if self.failed.load(Ordering::SeqCst) {
            return;
        }
        // numa-combine NUMAC marker (WFIN sibling, PGRUST_MORSEL_MARKERS=1):
        // steering + phase attribution + the locals-split/socket agreement
        // votes, read before the Locals drop.
        if let Some(nc) = &self.numa {
            if agg_markers_on() {
                let mid = locals.len() / 2;
                let (mut agree, mut votes) = (0u64, 0u64);
                for (i, l) in locals.iter().enumerate() {
                    let h = usize::from(i >= mid);
                    agree += u64::from(l.numa_votes[h]);
                    votes += u64::from(l.numa_votes[0]) + u64::from(l.numa_votes[1]);
                }
                eprintln!(
                    "MORSEL|NUMAC|locals={}|steer_hit={}|steer_miss={}|finals_flat={}|partial_ms={}|final_ms={}|partial_peak_bytes={}|half_agree={agree}/{votes}",
                    locals.len(),
                    nc.steer_hit.load(Ordering::Relaxed),
                    nc.steer_miss.load(Ordering::Relaxed),
                    nc.finals_flat.load(Ordering::Relaxed),
                    nc.partial_ns.load(Ordering::Relaxed) / 1_000_000,
                    nc.final_ns.load(Ordering::Relaxed) / 1_000_000,
                    nc.partial_bytes_peak.load(Ordering::Relaxed),
                );
            }
        }
        self.finalize_publish(locals)
    }
}

impl AggSink {
    /// One credit of the engaged two-level shape: pop a pass-A item (own
    /// socket half first), build that half's single-bucket partial, and —
    /// when this claim completes the bucket's SECOND partial — run the
    /// bucket's FINAL stage immediately (election by counter; exactly once).
    fn combine_numa(
        &self,
        nc: &NumaCombine,
        credit: u64,
        locals: &[AggSinkLocal],
    ) -> PgResult<CombineOutcome> {
        // Steering: real socket where we can sample it; the credit's own
        // half elsewhere (deterministic — the non-Linux test environments).
        let my = numa_current_half()
            .unwrap_or(usize::from(credit >= SINK_NBUCKETS as u64));
        let Some((h, b)) = nc.pop(my) else {
            return Ok(CombineOutcome::Done);
        };
        let t0 = std::time::Instant::now();
        if self.numa_bucket_eligible(b, locals) {
            let merged = self.merge_bucket_subset(b, numa_half_slice(locals, h))?;
            let run = sink_run_from_bucket_table(b, &merged);
            let store_bytes = merged.str_store_bytes();
            let store = merged.into_str_store();
            nc.note_partial_bytes(run.bytes() + store_bytes);
            // SAFETY: (h,b) was popped exactly once (cursor fetch_add) —
            // this claim is the slot's single writer; the elected final's
            // counter Acquire pairs with our Release increment below.
            unsafe { *nc.partials[h * SINK_NBUCKETS + b].get() = Some((run, store)) };
        }
        nc.partial_ns.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
        // Election: the claim that brings the bucket to 2 partials runs the
        // final. AcqRel: Release publishes our partial store, Acquire sees
        // the sibling's.
        if nc.done[b].fetch_add(1, Ordering::AcqRel) + 1 < 2 {
            return Ok(CombineOutcome::Done);
        }
        if self.failed.load(Ordering::SeqCst) {
            return Ok(CombineOutcome::Done);
        }
        let t0 = std::time::Instant::now();
        // SAFETY: bucket b's final runs exactly once (the 1→2 election);
        // both pass-A writers have released their slots.
        let p0 = unsafe { (*nc.partials[b].get()).take() };
        let p1 = unsafe { (*nc.partials[SINK_NBUCKETS + b].get()).take() };
        let out = match (p0, p1) {
            (Some((r0, s0)), Some((r1, s1))) => {
                nc.release_partial_bytes(
                    r0.bytes()
                        + r1.bytes()
                        + s0.as_ref().map_or(0, ::lanefold::StrStateArena::bytes)
                        + s1.as_ref().map_or(0, ::lanefold::StrStateArena::bytes),
                );
                let views = [
                    SinkLocalView {
                        spilled: &[],
                        runs: core::slice::from_ref(&r0),
                        remainder: None,
                    },
                    SinkLocalView {
                        spilled: &[],
                        runs: core::slice::from_ref(&r1),
                        remainder: None,
                    },
                ];
                let ctr = self.topn.is_some();
                let tb = ctr.then(std::time::Instant::now);
                // t22 merge graft: spankey combine_ns also meters the numa
                // final-stage merge (observational CTR band; the flat path's
                // timer in merge_bucket_subset reproduces spankey's original
                // placement byte-for-byte at <96 DOP).
                let spk_tb = ::nodeagg::spankey::spankey_t0();
                let merged = sink_combine_bucket(
                    b,
                    self.key_words,
                    self.state_bytes,
                    &views,
                    &self.combines,
                )?;
                ::nodeagg::spankey::spankey_lap(
                    &::nodeagg::spankey::SPANKEY_CTRS.combine_ns,
                    spk_tb,
                );
                if let Some(tb) = tb {
                    self.topn_ctr
                        .build_ns
                        .fetch_add(tb.elapsed().as_nanos() as u64, Ordering::Relaxed);
                }
                self.combine_tail(b, merged, locals.len())
            }
            (p0, p1) => {
                // Ineligible bucket (both claims computed the same verdict:
                // NULL bucket, <2 locals, frozen, over budget) — or a pass-A
                // asymmetry behind an in-flight failure. Either way the flat
                // body is correct and self-contained: pass A only READS the
                // sealed locals, so nothing was consumed.
                if let Some((r, s)) = &p0 {
                    nc.release_partial_bytes(
                        r.bytes() + s.as_ref().map_or(0, ::lanefold::StrStateArena::bytes),
                    );
                }
                if let Some((r, s)) = &p1 {
                    nc.release_partial_bytes(
                        r.bytes() + s.as_ref().map_or(0, ::lanefold::StrStateArena::bytes),
                    );
                }
                drop((p0, p1));
                nc.finals_flat.fetch_add(1, Ordering::Relaxed);
                self.combine_bucket_flat(b, locals)
            }
        };
        nc.final_ns.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
        out
    }

    /// Whether bucket `b` takes the two-level pass. MUST be a pure function
    /// of the sealed state (both halves' claims compute it independently and
    /// their verdicts have to agree): the NULL bucket is routed flat (a
    /// partial run carries the NULL group out-of-band, which would move its
    /// first-seen slot), single-local shapes keep the pass-through arm,
    /// FROZEN engagements keep the member-filter arm on tiny tables, and an
    /// over-budget verdict routes to the flat body's combine-split.
    fn numa_bucket_eligible(&self, b: usize, locals: &[AggSinkLocal]) -> bool {
        if b == SINK_NULL_BUCKET || locals.len() < 2 {
            return false;
        }
        if self.freeze.as_ref().is_some_and(|f| f.frozen()) {
            return false;
        }
        let (rows, content) = self.bucket_estimate(b, locals);
        est_table_bytes(self, rows).saturating_add(content.saturating_mul(3) / 2)
            <= self.budget
    }

    /// The flat (t21) per-bucket combine body: pass-through arm, pre-build
    /// size check, combine-split fallback, in-memory merge, tail.
    fn combine_bucket_flat(
        &self,
        b: usize,
        locals: &[AggSinkLocal],
    ) -> PgResult<CombineOutcome> {
        {
            // SINGLE-LOCAL PASS-THROUGH (dop1-tax fix 3): exactly one sealed
            // Local and zero flushed runs — the merged bucket table would be
            // a verbatim re-insert of the Local's rows, so emit straight
            // from its table through the SEAL partition index (no 256-way
            // rebuild, no double insert; byte-identical order by
            // construction — see sink_emit_bucket_passthrough). LIVE-STATE
            // decision: a widened engagement (≥2 Locals) or a flushed Local
            // takes the merge arm below; no plan/DOP special-casing.
            // M3.5 composition: a spilled face disqualifies the arm too —
            // spilled epochs live on the Local's file, not its table.
            // Freeze composition: a FROZEN engagement's Local tables can
            // carry pre-freeze stragglers — the pass-through emits verbatim
            // and cannot filter, so it stands down (the merge arm below
            // runs the member filter; frozen tables are tiny).
            // Keying-class composition (GL-SINKSHAPE-1): the pass-through
            // emits from the Local's OWN table, so the table's key
            // representation must serve the emit plan — an INTERN-ARMED
            // canonical table (word-keyed; canonical bytes only through
            // the intern chase) REFUSES to the merge arm below, whose
            // remainder face runs that chase. Reachable exactly when a
            // concurrent-saturated pool (the QPS window) collapses a
            // dop-N engagement to one sealed Local (fork-on-first-touch).
            let frozen = self.freeze.as_ref().is_some_and(|f| f.frozen());
            if let [l] = locals {
                if l.runs.is_empty() && l.spill.is_none() && !frozen {
                    if let (Some(t), Some(p)) = (&l.table, &l.part) {
                        if ::nodeagg::sink::sink_passthrough_admits(&self.emit, t.table()) {
                            // Top-N composition (m3-sort-b car 1) selects on the
                            // MERGED table; the pass-through never builds one, so
                            // an armed spec degrades globally to the full drain
                            // (decision 1: winners are a drain filter — a miss
                            // must never drop groups). Winners-only inc-2: this
                            // shape is resolved to FullDrain at SEAL (§3.2 step
                            // 2), so the mid-claim store below only ever runs in
                            // FullDrain mode — a WinnersOnly sighting here would
                            // mean partial compact bufs elsewhere.
                            if self.topn.is_some() {
                                debug_assert_eq!(
                                    self.topn_mode(),
                                    TopnMode::FullDrain,
                                    "pass-through shape must resolve FullDrain at SEAL"
                                );
                                self.topn_degraded.store(true, Ordering::Release);
                            }
                            let t0 = self.topn.is_some().then(std::time::Instant::now);
                            let buf = ::nodeagg::sink::sink_emit_bucket_passthrough(
                                &self.emit,
                                t.table(),
                                p,
                                b,
                            )?;
                            if let Some(t0) = t0 {
                                self.topn_ctr
                                    .emit_ns
                                    .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
                                self.topn_ctr
                                    .mat_rows
                                    .fetch_add(buf.nrows as u64, Ordering::Relaxed);
                            }
                            self.retain_bucket(b as u64, buf, locals.len())?;
                            return Ok(CombineOutcome::Done);
                        }
                    }
                }
            }
            // Pre-build size check (M3.5 §3) — see [`Self::bucket_estimate`].
            let (rows, content) = self.bucket_estimate(b, locals);
            if est_table_bytes(self, rows).saturating_add(content.saturating_mul(3) / 2)
                > self.budget
            {
                // inc-2b: recursive combine-split by deeper hash bits —
                // stream every face through sub-bucket routing files and
                // combine each sub-partition bounded; depth cap → refusal.
                let Some(set) = &self.spill_set else {
                    return Ok(CombineOutcome::OverBudget);
                };
                // Freeze composition: the split emits sub-partition tables
                // piecemeal and cannot run the member filter — a FROZEN
                // engagement refuses instead (unreachable by arithmetic:
                // frozen tables hold <= bound + first-batch stragglers,
                // orders below any budget; fail-closed, never
                // silent-wrong).
                if self.freeze.as_ref().is_some_and(|f| f.frozen()) {
                    return Ok(CombineOutcome::OverBudget);
                }
                // Top-N × split composition (winners-phase2 §split×selection):
                //  * WinnersOnly — each split LEAF (a disjoint sub-partition
                //    of this partition's groups) runs the selection on its
                //    own merged fragment table and materializes ONLY its
                //    candidates; the fragment candidate lists merge BEFORE
                //    truncation into this partition's local list (superset
                //    lemma one level deeper — sink_topn_merge_fragments).
                //    A leaf decline REFUSES the attempt (R5), same as the
                //    in-memory WinnersOnly arm — rows are already gone from
                //    other claims' compact bufs.
                //  * FullDrain — decision-1 verbatim: the split emits
                //    piecemeal with no selection, and the armed spec
                //    degrades globally to the plain full drain.
                let winners_split =
                    self.topn.is_some() && self.topn_mode() == TopnMode::WinnersOnly;
                if self.topn.is_some() && !winners_split {
                    self.topn_degraded.store(true, Ordering::Release);
                }
                let mut sel = winners_split.then(|| SplitSel {
                    spec: self.topn.as_ref().expect("winners split has a spec"),
                    part: b as u16,
                    lists: Vec::new(),
                });
                let mut acc = SinkEmitAcc::default();
                match split_views_and_emit(self, b, set, locals, &mut acc, &mut sel)? {
                    SplitOutcome::DepthCap => return Ok(CombineOutcome::OverBudget),
                    SplitOutcome::Declined => return Ok(CombineOutcome::TopnDeclined),
                    SplitOutcome::Done => {}
                }
                if let Some(sel) = sel {
                    let bound = sel.spec.bound as usize;
                    // SAFETY: bucket `b` is combined exactly once (runtime
                    // claim / numa final election); this is its single
                    // writer.
                    unsafe {
                        *self.topn_cands[b].get() =
                            sink_topn_merge_fragments(sel.lists, bound);
                    }
                }
                // R3: the split result is retained emit content like any
                // other combine result — meter it (retain_bucket is the
                // single writer of the claimed partition's slot).
                self.retain_bucket(b as u64, acc.finish(), locals.len())?;
                return Ok(CombineOutcome::Done);
            }
            // In-memory path — see [`Self::merge_bucket_subset`].
            let merged = self.merge_bucket_subset(b, locals)?;
            self.combine_tail(b, merged, locals.len())
        }
    }

    /// Pre-build size estimate of bucket `b` across `locals` — (rows,
    /// canonical key content), from the DIRECTORY + in-memory counts only;
    /// nothing is read from disk before the caller's decision. Rows
    /// over-count duplicates across faces, so the check is conservative in
    /// the safe direction. Canonical shapes add a KEY-CONTENT term (their
    /// merged table owns the byte images): spill part lengths and run
    /// key-byte ranges are O(1) directory reads; the remainder's share is
    /// approximated by its table's retained memory scaled to the bucket's
    /// row fraction — over-counting (entry+state overhead included), never
    /// under. Pure function of the sealed state (the numa two-level arm
    /// relies on both halves' claims computing the same verdict).
    fn bucket_estimate(&self, b: usize, locals: &[AggSinkLocal]) -> (usize, usize) {
        let state_words = self.state_bytes / 8;
        let canon = self.key_words == 0;
        // Canonical (bytes-mode) spill faces carry variable-width records:
        // divide by the MINIMUM record width (over-counts rows — the safe
        // direction, the distinct record's discipline).
        let row_bytes = if canon {
            ::nodeagg::sink::sink_canon_min_record_bytes(state_words)
        } else {
            sink_spill_row_bytes(self.key_words, state_words)
        };
        let mut rows = 0usize;
        let mut content = 0usize;
        for l in locals {
            if let Some(sp) = &l.spill {
                let blen = sp.file.part_len(b as u32) as usize;
                rows += blen / row_bytes;
                if canon {
                    content += blen;
                }
            }
            for r in &l.runs {
                rows += (r.starts[b + 1] - r.starts[b]) as usize;
                if canon {
                    // bucket_key_bytes dispatches the contiguous/stolen
                    // key-store law (arena-strings inc-1).
                    content += r.bucket_key_bytes(b);
                }
            }
            if let (Some(t), Some(p)) = (&l.table, &l.part) {
                let brows = (p.starts[b + 1] - p.starts[b]) as usize;
                rows += brows;
                if canon && brows > 0 {
                    let n = t.table().nrows().max(1);
                    content += t.mem_used() / n * brows;
                }
            }
        }
        (rows, content)
    }

    /// In-memory merge of bucket `b` across `locals` — a CONTIGUOUS slice of
    /// the sealed vec (the whole slice = the flat pass; a half = the numa
    /// two-level pass A; first-seen order composes only because the slices
    /// are contiguous). Rebuilds each Local's spilled face for this bucket —
    /// open-by-name on THIS thread (the file is frozen: combine
    /// deps-follows accept), one synthesized run per Local plus its
    /// in-memory NULL blocks in the NULL bucket.
    fn merge_bucket_subset(
        &self,
        b: usize,
        locals: &[AggSinkLocal],
    ) -> PgResult<::nodeagg::sink::CombinedBucket> {
        let state_words = self.state_bytes / 8;
        let canon = self.key_words == 0;
        let mut synth: Vec<Vec<SinkRun>> = Vec::with_capacity(locals.len());
        for l in locals {
            let mut v: Vec<SinkRun> = Vec::new();
            if let Some(sp) = &l.spill {
                let ctx = ::mcx::MemoryContext::new("m35-agg-spill-read");
                if let Some(mut r) = sp.file.read_part(ctx.mcx(), b as u32)? {
                    let bytes = r.read_to_end()?;
                    r.close()?;
                    v.push(if canon {
                        ::nodeagg::sink::sink_run_from_spill_bytes(b, state_words, &bytes)?
                    } else {
                        sink_run_from_spill(b, self.key_words, state_words, &bytes)?
                    });
                }
                if b == SINK_NULL_BUCKET {
                    for nb in &sp.null_blocks {
                        v.push(sink_null_only_run(self.key_words, state_words, nb.clone()));
                    }
                }
            }
            synth.push(v);
        }
        // Std-collections audit note: this views Vec is a per-claim
        // allocation, but the combine morsel space is a FIXED 256
        // partitions x dop-sized views — bounded per engagement,
        // independent of data volume (accepted; a borrowed view cannot
        // be retained across claims without lifetime erasure).
        let views: Vec<SinkLocalView<'_>> = locals
            .iter()
            .zip(synth.iter())
            .map(|(l, s)| SinkLocalView {
                spilled: s,
                runs: &l.runs,
                remainder: match (&l.table, &l.part) {
                    (Some(t), Some(p)) => Some(t.remainder_view(p)),
                    _ => None,
                },
            })
            .collect();
        let t0 = self.topn.is_some().then(std::time::Instant::now);
        let spk_t0 = ::nodeagg::spankey::spankey_t0();
        let merged = sink_combine_bucket(
            b,
            self.key_words,
            self.state_bytes,
            &views,
            &self.combines,
        )?;
        ::nodeagg::spankey::spankey_lap(
            &::nodeagg::spankey::SPANKEY_CTRS.combine_ns,
            spk_t0,
        );
        self.note_combine16(&merged);
        if let Some(t0) = t0 {
            self.topn_ctr
                .build_ns
                .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
        Ok(merged)
    }

    /// The combine tail, shared by the flat body and the numa final stage:
    /// freeze member filter → top-N selection → emit materialization →
    /// retain. Single writer of bucket `b`'s output slots (the caller holds
    /// the exactly-once claim/election).
    fn combine_tail(
        &self,
        b: usize,
        merged: ::nodeagg::sink::CombinedBucket,
        nlocals: usize,
    ) -> PgResult<CombineOutcome> {
        {
            let locals_len = nlocals;
            // Freeze member filter (band-2a): a FROZEN engagement emits
            // ONLY set members — pre-freeze stragglers are undercounted
            // past the freeze point and must never leave the sink. Rows
            // ascend (sink_emit_bucket_rows contract). Structurally
            // disjoint from the topn arm below (never co-armed).
            if let Some(fz) = &self.freeze {
                if let Some(entries) = fz.entries() {
                    let shape = self
                        .mk
                        .as_ref()
                        .expect("freeze arms only on the Mk drain");
                    let rows = ::nodeagg::sink::sink_freeze_member_rows(
                        &merged,
                        self.key_words,
                        shape,
                        entries,
                    );
                    fz.note_stragglers((merged.nrows() - rows.len()) as u64);
                    let buf =
                        ::nodeagg::sink::sink_emit_bucket_rows(&self.emit, &merged, &rows)?;
                    self.retain_bucket(b as u64, buf, locals_len)?;
                    return Ok(CombineOutcome::Done);
                }
            }
            // Combine-phase top-N (car 1 + the winners-only amendment):
            // select this partition's winners on the merged raw states
            // BEFORE any emit walks the rows. Mode dispatch (§3.2, fixed
            // before the first claim):
            //  * WinnersOnly — materialize ONLY the ≤bound candidate rows
            //    (compact buf; candidate `row` remapped to the compact
            //    index). A decline (NULL order transvalue) REFUSES the
            //    whole attempt (R5 serial rerun) — rows are already gone
            //    from other claims' compact bufs, so degrade is not free.
            //  * FullDrain — decision-1 verbatim: selection is a drain
            //    filter, the buf stays full, a decline degrades globally.
            if let Some(spec) = &self.topn {
                if self.topn_mode() == TopnMode::WinnersOnly {
                    let ts = std::time::Instant::now();
                    let selected = if topn_fault_decline() {
                        None // leg-R fault injection: the unreachable decline
                    } else {
                        sink_topn_candidates(&merged, spec, b as u16)
                    };
                    let Some(mut cands) = selected else {
                        return Ok(CombineOutcome::TopnDeclined);
                    };
                    self.topn_ctr
                        .select_ns
                        .fetch_add(ts.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    self.topn_ctr.cand_rows.fetch_add(cands.len() as u64, Ordering::Relaxed);
                    // Candidate row remap: materialize the candidate rows in
                    // ascending TABLE order (one ordered emit walk), and
                    // point each candidate at its compact-buf index. Rows
                    // are unique (one candidate per group row), so the
                    // binary search is exact.
                    let mut rows: Vec<u32> = cands.iter().map(|c| c.row).collect();
                    rows.sort_unstable();
                    for c in &mut cands {
                        c.row = rows
                            .binary_search(&c.row)
                            .expect("candidate row present in its own row set")
                            as u32;
                    }
                    // SAFETY: bucket `b` is combined exactly once (runtime
                    // claim / numa final election); this is its single
                    // writer.
                    unsafe { *self.topn_cands[b].get() = cands };
                    let t0 = std::time::Instant::now();
                    let buf = ::nodeagg::sink::sink_emit_bucket_rows(&self.emit, &merged, &rows)?;
                    self.topn_ctr
                        .emit_ns
                        .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    self.topn_ctr.mat_rows.fetch_add(buf.nrows as u64, Ordering::Relaxed);
                    self.retain_bucket(b as u64, buf, locals_len)?;
                    return Ok(CombineOutcome::Done);
                }
                // FullDrain: candidate row indices == full emit buf row
                // indices (both iterate table rows 0..n in order).
                if !self.topn_degraded.load(Ordering::Acquire) {
                    let ts = std::time::Instant::now();
                    let selected = if topn_fault_decline() {
                        None // leg-R fault injection: FullDrain must degrade
                    } else {
                        sink_topn_candidates(&merged, spec, b as u16)
                    };
                    match selected {
                        // SAFETY: bucket `b` is combined exactly once
                        // (runtime claim / numa final election); this is its
                        // single writer.
                        Some(c) => {
                            self.topn_ctr
                                .cand_rows
                                .fetch_add(c.len() as u64, Ordering::Relaxed);
                            unsafe { *self.topn_cands[b].get() = c }
                        }
                        None => self.topn_degraded.store(true, Ordering::Release),
                    }
                    self.topn_ctr
                        .select_ns
                        .fetch_add(ts.elapsed().as_nanos() as u64, Ordering::Relaxed);
                }
            }
            let t0 = self.topn.is_some().then(std::time::Instant::now);
            let buf = sink_emit_bucket(&self.emit, &merged)?;
            if let Some(t0) = t0 {
                self.topn_ctr
                    .emit_ns
                    .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
                self.topn_ctr.mat_rows.fetch_add(buf.nrows as u64, Ordering::Relaxed);
            }
            self.retain_bucket(b as u64, buf, locals_len)?;
            Ok(CombineOutcome::Done)
        }
    }

    /// Publish (the ParallelSink::finalize body): the adopted table (TRUE
    /// TABLE ADOPT — the pointer hand-off) or the 256 emit buffers, moved
    /// out (O(partitions), the §6 contract). Locals drop with the plumbing
    /// right after.
    fn finalize_publish(&self, locals: &[AggSinkLocal]) {
        // EA-on-morsels: merge the accept-phase instrument partials before
        // the Locals drop (O(workers) sums — the §6-of-m2-sinks minimal-
        // finalize ruling holds). Runs on the adopt path too — the
        // instrument partial rides the Local either way.
        if self.ea_scan_node.is_some() {
            *self.ea_instr.lock().unwrap_or_else(|p| p.into_inner()) =
                Some(super::runtime_instr::merge(locals.iter().map(|l| &l.instr)));
        }
        if self.adopted_flag.load(Ordering::SeqCst) {
            if let Some(t) = self.adopted.lock().unwrap_or_else(|g| g.into_inner()).take() {
                *self.published.lock().unwrap_or_else(|p| p.into_inner()) =
                    Some(SinkPublished::Table(t));
                return;
            }
            // Unreachable by construction (flag implies content); fall
            // through fail-closed to the buf publish (empty → leader errors
            // on "published nothing"-class checks rather than wedging).
        }
        let bufs: Vec<SinkEmitBuf> = self
            .out_emit
            .iter()
            .map(|c| {
                // SAFETY: all combine claims settled (last-worker-out);
                // finalize is the single reader.
                unsafe { std::mem::take(&mut *c.get()) }
            })
            .collect();
        // Composed top-N: truncate-merge the per-partition winner lists
        // (O((P + bound)·log P) — the finalize's O(partitions) envelope).
        // A degrade publishes `None` = the plain full drain.
        let winners = match &self.topn {
            Some(spec) if !self.topn_degraded.load(Ordering::Acquire) => {
                let lists: Vec<Vec<SinkTopnCand>> = self
                    .topn_cands
                    .iter()
                    // SAFETY: single reader after all combine claims settled.
                    .map(|c| unsafe { std::mem::take(&mut *c.get()) })
                    .collect();
                Some(sink_topn_merge(&lists, spec.bound as usize))
            }
            _ => None,
        };
        *self.published.lock().unwrap_or_else(|p| p.into_inner()) =
            Some(SinkPublished::Emit(bufs, winners));
    }
}

/// The 3-set arm (combine-parallel lane, PGRUST_RUNTIME_AGG_PARSEAL — default
/// ON): identical sink semantics to the 2-set ParallelSink impl above, with
/// the SEAL partition pass PARALLEL across Local slots (one freeze claim per
/// slot; the distinct sink's SEALCVT precedent). Rationale: the 2-set SEAL is
/// single-threaded and O(forked Locals × remainder rows) — a per-Local
/// locality-capped table makes that O(DOP × cap), a serial term that GROWS
/// with DOP while the parallel combine share shrinks (measured DOP-15 gap
/// 5-10ms on the two-key and wide-vocabulary top-n shapes; Amdahl-fatal at DOP 192 — notes/combine-parallel-lane.md).
/// Byte identity: partition_remainder is a pure per-Local function, and the
/// whole-census decisions (TRUE TABLE ADOPT, top-N SEAL resolution) run
/// exactly once in `sealed_ready` (single-threaded, happens-before every
/// combine claim) — same inputs, same order, same outputs as the 2-set seal.
impl runtime::SealedParallelSink for AggSink {
    type Local = AggSinkLocal;
    type Sealed = AggSinkLocal;

    fn fork(&self, worker: usize) -> AggSinkLocal {
        <Self as runtime::ParallelSink>::fork(self, worker)
    }

    fn accept_local(&self, local: &mut AggSinkLocal, worker: usize, range: runtime::MorselRange) {
        <Self as runtime::ParallelSink>::accept_local(self, local, worker, range)
    }

    /// Freeze one Local: the per-Local partition pass, parallel across
    /// slots. The TRUE-TABLE-ADOPT shape SKIPS the pass when it can prove
    /// the census will take the table wholesale (exactly one fork — the
    /// dop1-tax2 "no partition index is ever built" property preserved);
    /// an overcounted census (stale re-fork) partitions anyway and the
    /// adopt in `sealed_ready` simply ignores the index — the safe
    /// direction, never an unpartitioned Local reaching the merge arms.
    fn seal(&self, _worker: usize, mut local: AggSinkLocal) -> AggSinkLocal {
        if self.failed.load(Ordering::SeqCst) {
            return local;
        }
        let frozen = self.freeze.as_ref().is_some_and(|f| f.frozen());
        let adopt_skip = self.adopt_shape
            && !frozen
            && self.forks.load(Ordering::SeqCst) == 1
            && local.runs.is_empty()
            && local.spill.is_none()
            && local.table.is_some()
            // Scatter Locals must partition (their remainder leaves in
            // seal_partition_local) — see try_adopt_census.
            && local.scatter.is_none();
        if !adopt_skip {
            let r = catch_unwind(AssertUnwindSafe(|| self.seal_partition_local(&mut local)));
            if r.is_err() {
                self.fail(PgError::new(ERROR, "runtime agg sink seal panicked").into());
            }
        }
        local
    }

    /// Exactly-once whole-census decisions, single-threaded under the
    /// freeze set's last-worker-out, strictly before any combine claim:
    /// the 2-set seal's census pieces verbatim (partitioning already done
    /// per claim above).
    fn sealed_ready(&self, sealed: &mut Vec<AggSinkLocal>) {
        if self.failed.load(Ordering::SeqCst) {
            return;
        }
        // Shared-table experiment drain (single-threaded here by
        // last-worker-out; before the adopt census).
        if let Some(sh) = self.shared.as_ref() {
            sh.drain_into(sealed);
        }
        if agg_markers_on() {
            // The 3-set arm's AGGSEAL sibling line (single-threaded here by
            // last-worker-out of the freeze set): the flush-amplification
            // census the CPROBE channel reads (see AggSinkLocal doc).
            let rows: usize =
                sealed.iter().map(|l| l.table.as_ref().map_or(0, |t| t.table().nrows())).sum();
            let flushes: u64 = sealed.iter().map(|l| l.probe_flushes).sum();
            let flush_bytes: u64 = sealed.iter().map(|l| l.probe_flush_bytes).sum();
            let (ad, ar, ap) = alpha_sums(sealed);
            eprintln!(
                "MORSEL|AGGSEAL|arm=3set|locals={}|rows={rows}|flushes={flushes}|flush_bytes={flush_bytes}|alpha_demotes={ad}|alpha_restores={ar}|alpha_reprobes={ap}|dur_us=0|sealflush_rows={}|scatter_rows={}|vec_rows={}",
                sealed.len(),
                self.sealflush_rows.load(Ordering::Relaxed),
                self.scatter_rows.load(Ordering::Relaxed),
                self.vec_rows.load(Ordering::Relaxed),
            );
        }
        if self.try_adopt_census(sealed) {
            return;
        }
        self.resolve_topn_at_seal(sealed);
    }

    fn partitions(&self) -> u64 {
        <Self as runtime::ParallelSink>::partitions(self)
    }

    fn combine(&self, part: u64, sealed: &[AggSinkLocal]) {
        <Self as runtime::ParallelSink>::combine(self, part, 0, sealed)
    }

    fn finalize(&self, sealed: &[AggSinkLocal]) {
        <Self as runtime::ParallelSink>::finalize(self, sealed)
    }
}

enum CombineOutcome {
    Done,
    OverBudget,
    /// WinnersOnly selection declined (NULL order transvalue) → refusal.
    TopnDeclined,
}

enum AcceptFail {
    Budget,
    Error(Box<PgError>),
}

impl From<Box<PgError>> for AcceptFail {
    fn from(e: Box<PgError>) -> AcceptFail {
        AcceptFail::Error(e)
    }
}

// ---------------------------------------------------------------------------
// Worker-side executor (helper thread-local) + the narrow ranged drain.
// ---------------------------------------------------------------------------

struct SendConstPstmt(*const PlannedStmt<'static>);
// SAFETY: read-only erased reference into the leader's executor arena; the
// leader keeps it alive until DestroyParallelContext has joined every helper
// (the execparallel SendConst contract, verbatim — runtime_scan precedent).
unsafe impl Send for SendConstPstmt {}
// SAFETY: as above; helpers only read.
unsafe impl Sync for SendConstPstmt {}

pub(super) struct RuntimeAggShared {
    rt: &'static Arc<runtime::Runtime>,
    rg: OnceLock<runtime::WeakRgHandle>,
    pcxt_shared: OnceLock<Arc<parallel::ParallelShared>>,
    pstmt: SendConstPstmt,
    query_text: String,
    eflags: i32,
    refused: AtomicUsize,
    started: AtomicUsize,
    /// Helpers that have EXITED `helper_drive` (every exit path — refused
    /// bind, errored, drove to completion — bumps exactly once, by drop
    /// guard). Liveness reap input (inc-2c): a pinned RG is invisible to
    /// pool workers, so once `exited >= launched` with the RG incomplete,
    /// nobody will ever step it — the leader must reap or park forever.
    exited: AtomicUsize,
    sink: Arc<AggSink>,
    query_id: AtomicU64,
    /// M2 inc-1 standing channel: the live board entry, held so the
    /// PRIVATE_SHUTDOWN hook can complete the standing join (abort + drain
    /// + await detach) on leader unwind paths — the scan arm's discipline,
    /// verbatim (standing_channel::shutdown_standing_join).
    standing: Mutex<Option<Arc<parallel::standing::StandingEngagement>>>,
}

/// Bump-on-drop exit counter: rides `helper_drive`'s frame so EVERY exit
/// path (including a panic unwinding into the hook's catch_unwind) counts
/// exactly once. `pub(super)`: runtime_distinct's helper hook has the
/// identical liveness hole and shares this guard.
pub(super) struct ExitBump<'a>(pub(super) &'a AtomicUsize);

impl Drop for ExitBump<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct WorkerExec {
    qd: ::types_portal::QueryDescHandle,
    errored: std::cell::Cell<bool>,
    /// Per-worker reusable drain scratch.
    k2s: ScanK2Scratch,
    /// Dict-code sink feed cache (DictFeed::Code; K2 drain only).
    dgs: SinkDictScratch,
    idxs: Vec<u32>,
    groups: Vec<core::ptr::NonNull<::execexpr::AggPerGroup>>,
    /// ExprKey drain state (SinkDrain::ExprKey only): the worker's own
    /// decide + the spill-replay stage slot.
    xk: Option<Box<super::ExprKeyState>>,
    stage_slot: Option<::executils::ExecSlotId>,
    /// Mk drain state (SinkDrain::Mk only): the worker's own armed shape +
    /// the reusable pack scratch.
    mk: Option<super::ScanMk>,
    mks: super::MkScratch,
    /// GL-VECACCEPT-2 direct-lane scratch (vec-accept engagements only).
    vs: SinkVecScratch,
    /// Process-ledger mirror of the drive-scratch estate (GL-CONCMEM-1):
    /// the k2s/dgs/groups/xk/mk/mks/vs families' backing stores, settled
    /// once per claim in `accept_morsel_body` — block grain, never per
    /// row. Drop balances (the field's own Drop).
    scratch_ledger: ScratchLedger,
}

/// Delta-settled process-ledger mirror for a plain-Rust engine estate
/// (GL-CONCMEM-1): `settle(now)` charges/uncharges the movement against
/// `mcx::global_footprint`; Drop unwinds the residue — a leaked charge is
/// impossible by construction.
#[derive(Default)]
struct ScratchLedger(usize);

impl ScratchLedger {
    fn settle(&mut self, now: usize) {
        if now > self.0 {
            ::mcx::global_footprint::charge_engine_estate(now - self.0);
        } else if now < self.0 {
            ::mcx::global_footprint::uncharge_engine_estate(self.0 - now);
        }
        self.0 = now;
    }
}

impl Drop for ScratchLedger {
    fn drop(&mut self) {
        ::mcx::global_footprint::uncharge_engine_estate(self.0);
    }
}

impl WorkerExec {
    /// The per-worker drive-scratch estate (heap backing stores only; the
    /// estate-map families of the GL-CONCMEM-1 letter). Capacity-based.
    fn scratch_estate_bytes(&self) -> usize {
        self.k2s.estate_bytes()
            + self.dgs.estate_bytes()
            + super::vec_estate_bytes(&self.idxs)
            + super::vec_estate_bytes(&self.groups)
            + self.xk.as_ref().map_or(0, |xk| {
                core::mem::size_of::<super::ExprKeyState>() + xk.estate_bytes()
            })
            + self.mk.as_ref().map_or(0, super::ScanMk::estate_bytes)
            + self.mks.estate_bytes()
            + self.vs.estate_bytes()
    }

    /// Settle the process-ledger mirror to the current scratch estate
    /// (called once per claim — the block-grain boundary).
    fn settle_scratch_ledger(&mut self) {
        let now = self.scratch_estate_bytes();
        self.scratch_ledger.settle(now);
    }
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

/// One accept morsel: position the worker's scan on the claimed granule
/// range, lend the Local's table to the executor, run the narrow drain,
/// reclaim the table.
fn accept_morsel_body(
    sink: &AggSink,
    local: &mut AggSinkLocal,
    worker: usize,
    range: runtime::MorselRange,
) -> Result<(), AcceptFail> {
    // TIMER mode: the claim's clock pair (§5 — the ONLY TIMING ON cost).
    let ea_t0 = (sink.ea_timer && sink.ea_scan_node.is_some())
        .then(|| sink.ea_epoch.elapsed().as_nanos() as u64);
    WORKER_EXEC.with(|cell| -> Result<(), AcceptFail> {
        let mut b = cell.borrow_mut();
        let Some(ex) = b.as_mut() else {
            return Err(AcceptFail::Error(Box::new(PgError::new(
                ERROR,
                "runtime agg morsel without a bound executor",
            ))));
        };
        let WorkerExec { qd, k2s, dgs, idxs, groups, xk, stage_slot, mk, mks, vs, .. } = ex;
        let (k2s, dgs, idxs, groups) = (&mut *k2s, &mut *dgs, &mut *idxs, &mut *groups);
        let (xk, stage_slot) = (&mut *xk, &mut *stage_slot);
        let (mk, mks) = (&mut *mk, &mut *mks);
        let vs = &mut *vs;
        let r = crate::querydesc::with_qd(*qd, |q| {
            let x = q.exec.as_mut().expect("runtime agg worker executor state");
            x.with_mut(|d| -> Result<(), AcceptFail> {
                let estate = &mut d.estate;
                let Some(crate::procnode::PlanStateNode::Agg(aps)) = d.planstate.as_mut()
                else {
                    return Err(AcceptFail::Error(Box::new(PgError::new(
                        ERROR,
                        "runtime agg worker plan root is not an Agg",
                    ))));
                };
                let aps = &mut **aps;
                let crate::procnode::PlanStateNode::SeqScan(ss) = &mut aps.outer else {
                    return Err(AcceptFail::Error(Box::new(PgError::new(
                        ERROR,
                        "runtime agg worker outer node is not a SeqScan",
                    ))));
                };
                // train-12 composition: the heap lane generalized the
                // positioner to AM-dispatched seq_scan_set_morsel_range
                // (PgResult<()>); this arm admits only pgrcolumnar scans (its
                // admission requires cb granule geometry), so the former
                // not-pgrcolumnar false branch is unreachable by construction.
                ::nodeseqscan::seq_scan_set_morsel_range(
                    ss,
                    estate,
                    range.start,
                    range.end,
                )?;
                // Lend the Local's table to the executor for this range
                // (first morsel: the armed table is already in place).
                if let Some(t) = local.table.take() {
                    ::nodeagg::sink::agg_sink_put_table(&mut aps.agg, t);
                }
                // Sink-owned table: arm the batch-tail canonical hashing
                // (idempotent; the first morsel's table arms during the
                // drain below, so mark again before reclaiming it).
                ::nodeagg::sink::agg_sink_mark_sink_mode(&mut aps.agg);
                // GL-VECACCEPT-2: the whole-granule direct-lane drive owns
                // the claim when armed; the staged drain is the default,
                // branch-for-branch.
                let drained = if sink.vec_accept {
                    sink_drain_range_vec(
                        sink, local, worker, &mut aps.agg, ss, vs, idxs, groups, estate,
                    )
                } else {
                    sink_drain_range(
                        sink, local, worker, &mut aps.agg, ss, k2s, dgs, idxs, groups, xk,
                        stage_slot, mk, mks,
                        estate,
                    )
                };
                // EA-on-morsels claim fold (EXACT — accumulate in the Local,
                // never sampled; the dop1-tax contract).
                if sink.ea_scan_node.is_some() && drained.is_ok() {
                    local.instr.claims += 1;
                    local.instr.granules += range.end - range.start;
                    // Per-worker cumulative scan-desc counters: the snapshot
                    // IS the running total (prune fold, ea-morsels.md §1).
                    if let Some(c) = ::nodeseqscan::seq_scan_cb_ea_counters(ss) {
                        local.instr.prune = c;
                    }
                    if let Some(t0) = ea_t0 {
                        let t1 = sink.ea_epoch.elapsed().as_nanos() as u64;
                        super::runtime_instr::ea_claim_time(&mut local.instr, t0, t1);
                    }
                }
                // Reclaim on EVERY path — the Local owns the table between
                // morsels and at SEAL.
                ::nodeagg::sink::agg_sink_mark_sink_mode(&mut aps.agg);
                if let Some(t) = ::nodeagg::sink::agg_sink_take_table(&mut aps.agg) {
                    local.table = Some(t);
                }
                drained
            })
        });
        // GL-CONCMEM-1: settle the drive-scratch estate into the process
        // ledger once per claim (block grain — a claim is thousands of
        // rows; the whale lanes are the gndv-sized per-epoch code caches).
        // Error paths converge at teardown: the ledger field's Drop
        // balances whatever the last settle left charged.
        if let Some(ex) = b.as_mut() {
            ex.settle_scratch_ledger();
        }
        r
    })
}

/// M3.5 accept-side spill (design §3): write the Local's accumulated runs
/// to its spill file as ONE epoch — buckets 0..255 contiguous (each run's
/// bucket rows are already counting-sorted), NULL blocks kept in memory —
/// then drop the runs. Runs on the owning worker thread only; the BufFile
/// handle lives inside this event (open-per-event, §2 amendment).
fn spill_epoch(
    sink: &AggSink,
    local: &mut AggSinkLocal,
    set: &Arc<::spillset::SpillSet>,
    worker: usize,
) -> Result<(), Box<PgError>> {
    let sp = local.spill.get_or_insert_with(|| AggSpillState {
        file: ::spillset::SpillFile::new(
            Arc::clone(set),
            ::spillset::SpillSet::file_name("agg", 0, worker),
            SINK_NBUCKETS as u32,
        ),
        null_blocks: Vec::new(),
    });
    let before = sp.file.spilled_bytes();
    let ctx = ::mcx::MemoryContext::new("m35-agg-spill-write");
    let mut w = sp.file.begin_epoch(ctx.mcx())?;
    let mut buf: Vec<u8> = Vec::with_capacity(256 * 1024);
    for b in 0..SINK_NBUCKETS {
        buf.clear();
        for run in &local.runs {
            sink_run_spill_bucket(run, b, &mut buf);
        }
        w.write_part(b as u32, &buf)?;
    }
    w.finish()?;
    for mut run in local.runs.drain(..) {
        if let Some(nb) = run.null_states.take() {
            sp.null_blocks.push(nb);
        }
    }
    local.run_bytes = 0;
    // GL-CONCMEM-1 ledger settle, inlined: `sp` still borrows local.spill,
    // so the &mut-self helper can't run here — the drained runs' charge
    // unwinds directly (field-only access is disjoint-borrow legal).
    ::mcx::global_footprint::uncharge_engine_estate(local.ledger_runs);
    local.ledger_runs = 0;
    sink.spill_epochs.fetch_add(1, Ordering::Relaxed);
    sink.spilled_bytes
        .fetch_add(sp.file.spilled_bytes() - before, Ordering::Relaxed);
    Ok(())
}

/// Merged-table byte estimate for `rows` input rows (entry overhead + key +
/// state, ×1.5 headroom) — the combine pre-build check and the split loop
/// read the SAME estimator.
fn est_table_bytes(sink: &AggSink, rows: usize) -> usize {
    rows.saturating_mul(sink.key_words * 8 + sink.state_bytes + 32)
        .saturating_mul(3)
        / 2
}

/// Combine-split depth cap: hash bytes below the top-8 the recursion may
/// consume (depth 1 = the first split). Default 3; clamped to the routing
/// vocabulary (≤6).
fn spill_split_depth_cap() -> u32 {
    static N: OnceLock<u32> = OnceLock::new();
    crate::once_val(&N, || {
        std::env::var("PGRUST_RUNTIME_AGG_SPILL_DEPTH")
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .unwrap_or(3)
            .clamp(1, 6)
    })
}

const SPLIT_FLUSH_BYTES: usize = 16 << 20;
const SPLIT_READ_CHUNK: usize = 1 << 20;

/// Bounded sub-bucket router (inc-2b): records absorb into 256 in-memory
/// buffers and epoch-flush to a combine-task-owned spill file when the
/// staged total crosses [`SPLIT_FLUSH_BYTES`] — partition-ascending per
/// epoch, extents accumulate across epochs (the substrate contract).
struct SubRouter {
    file: ::spillset::SpillFile,
    bufs: Vec<Vec<u8>>,
    staged: usize,
    key_words: usize,
    state_words: usize,
    depth: u32,
}

impl SubRouter {
    fn new(
        sink: &AggSink,
        set: &Arc<::spillset::SpillSet>,
        b: usize,
        depth: u32,
    ) -> SubRouter {
        let uniq = sink.split_uniq.fetch_add(1, Ordering::Relaxed);
        SubRouter {
            file: ::spillset::SpillFile::new(
                Arc::clone(set),
                format!("m35-cmb-p{b}-d{depth}-u{uniq}"),
                SINK_NBUCKETS as u32,
            ),
            bufs: vec![Vec::new(); SINK_NBUCKETS],
            staged: 0,
            key_words: sink.key_words,
            state_words: sink.state_bytes / 8,
            depth,
        }
    }

    fn absorb(&mut self, records: &[u8]) -> PgResult<()> {
        if records.is_empty() {
            return Ok(());
        }
        if self.key_words == 0 {
            // Canonical bytes records route by their STORED hash (the C2
            // record carries it — value-derived, deeper bits of the same
            // hash partition groups exactly).
            ::nodeagg::sink::sink_route_records_bytes(
                records,
                self.state_words,
                self.depth,
                &mut self.bufs,
            )?;
        } else {
            sink_route_records(
                records,
                self.key_words,
                self.state_words,
                self.depth,
                &mut self.bufs,
            )?;
        }
        self.staged += records.len();
        if self.staged >= SPLIT_FLUSH_BYTES {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> PgResult<()> {
        if self.staged == 0 {
            return Ok(());
        }
        let ctx = ::mcx::MemoryContext::new("m35-agg-split-write");
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

/// Stream one spilled partition in ROW-ALIGNED chunks (fixed-width records;
/// a torn tail fails closed). `pub(super)`: the runtime-distinct combine
/// split (inc-3b) streams its fixed-width value records through the same
/// discipline.
pub(super) fn stream_part_rows(
    file: &::spillset::SpillFile,
    part: u32,
    row_bytes: usize,
    mut f: impl FnMut(&[u8]) -> PgResult<()>,
) -> PgResult<()> {
    let ctx = ::mcx::MemoryContext::new("m35-agg-split-read");
    let Some(mut rd) = file.read_part(ctx.mcx(), part)? else { return Ok(()) };
    let cap = (SPLIT_READ_CHUNK / row_bytes).max(1) * row_bytes;
    let mut buf = vec![0u8; cap];
    let mut filled = 0usize;
    loop {
        let n = rd.read(&mut buf[filled..])?;
        if n == 0 {
            rd.close()?;
            if filled != 0 {
                return Err(::nodeagg::sink::sink_shape_error(
                    "torn spill record (partial row) in split stream",
                ));
            }
            return Ok(());
        }
        filled += n;
        let usable = filled / row_bytes * row_bytes;
        if usable > 0 {
            f(&buf[..usable])?;
            buf.copy_within(usable..filled, 0);
            filled -= usable;
        }
    }
}

/// Stream one spilled partition of CANONICAL BYTES records in RECORD-ALIGNED
/// chunks: each record self-describes its length (`rec_len` at offset 0,
/// 8-aligned), so the reader carries partial tails across reads (the
/// distinct sink's `stream_part_dst` discipline) and hands `f` only whole
/// records. Fail-closed on a torn tail or a malformed header.
fn stream_part_records(
    file: &::spillset::SpillFile,
    part: u32,
    state_words: usize,
    mut f: impl FnMut(&[u8]) -> PgResult<()>,
) -> PgResult<()> {
    let ctx = ::mcx::MemoryContext::new("m35-agg-split-read");
    let Some(mut rd) = file.read_part(ctx.mcx(), part)? else { return Ok(()) };
    let min_rec = ::nodeagg::sink::sink_canon_min_record_bytes(state_words);
    let mut buf = vec![0u8; SPLIT_READ_CHUNK.max(min_rec * 2)];
    let mut filled = 0usize;
    loop {
        if filled == buf.len() {
            // One record larger than the buffer: grow (bounded by the
            // record's own rec_len validation downstream).
            buf.resize(buf.len() * 2, 0);
        }
        let n = rd.read(&mut buf[filled..])?;
        if n == 0 {
            rd.close()?;
            if filled != 0 {
                return Err(::nodeagg::sink::sink_shape_error(
                    "torn canonical spill record (partial tail) in split stream",
                ));
            }
            return Ok(());
        }
        filled += n;
        // Usable prefix: whole records only (each header read fail-closed).
        let mut usable = 0usize;
        while filled - usable >= 8 {
            let rec_len = u64::from_ne_bytes(
                buf[usable..usable + 8].try_into().expect("8 bytes"),
            ) as usize;
            // Sanity cap: a legit record is a ≤1GB varlena + states; a
            // larger rec_len is corruption — fail closed before the grow
            // loop could chase it.
            if rec_len < min_rec || rec_len % 8 != 0 || rec_len > (1usize << 31) {
                return Err(::nodeagg::sink::sink_shape_error(
                    "malformed canonical spill record header in split stream",
                ));
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

/// Split verdicts, threaded up the recursion.
enum SplitOutcome {
    Done,
    /// Depth-cap overflow — the caller refuses (OverBudget → R5 rerun).
    DepthCap,
    /// A split leaf's winners-only selection declined (fault injection; the
    /// NULL-order decline is structurally unreachable on pgrcolumnar feeds) —
    /// the caller refuses through the topn channel (R5 rerun).
    Declined,
}

/// Split×selection context (winners-phase2): the per-FRAGMENT candidate
/// lists of one split partition, collected across leaves and truncate-merged
/// by the caller into the partition's local candidate list.
struct SplitSel<'a> {
    spec: &'a SinkTopnSpec,
    part: u16,
    lists: Vec<Vec<SinkTopnCand>>,
}

/// One split LEAF's emit: winners-only (`sel` Some) selects the fragment
/// table's candidates first and materializes ONLY those rows — candidate
/// `row` payloads remapped to the accumulator's compact indices (fragment
/// rows land at `acc.nrows() + i` in `emit_rows`'s ascending order) — while
/// FullDrain (`sel` None) materializes the whole fragment. Returns false on
/// a selection decline. Counters mirror the in-memory arm: `cand_rows`
/// counts MATERIALIZED candidates (so the `mat_rows == cand_rows` compact-
/// materialization law holds on split shapes too; the partition's stored
/// list is truncated separately by the fragment merge).
fn split_leaf_emit(
    sink: &AggSink,
    t: &LaneAggTable,
    acc: &mut SinkEmitAcc,
    sel: &mut Option<SplitSel<'_>>,
) -> PgResult<bool> {
    let ctr = sink.topn.is_some();
    let Some(s) = sel else {
        let t0 = ctr.then(std::time::Instant::now);
        acc.emit_table(&sink.emit, t)?;
        if let Some(t0) = t0 {
            sink.topn_ctr.emit_ns.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
            sink.topn_ctr.mat_rows.fetch_add(t.nrows() as u64, Ordering::Relaxed);
        }
        return Ok(true);
    };
    let ts = std::time::Instant::now();
    let selected = if topn_fault_decline() {
        None // leg fault injection: the unreachable decline, split leaf form
    } else {
        sink_topn_candidates(t, s.spec, s.part)
    };
    sink.topn_ctr.select_ns.fetch_add(ts.elapsed().as_nanos() as u64, Ordering::Relaxed);
    let Some(mut cands) = selected else {
        return Ok(false);
    };
    let mut rows: Vec<u32> = cands.iter().map(|c| c.row).collect();
    rows.sort_unstable();
    let base = acc.nrows() as u32;
    for c in &mut cands {
        c.row = base
            + rows
                .binary_search(&c.row)
                .expect("candidate row present in its own row set") as u32;
    }
    let t0 = std::time::Instant::now();
    acc.emit_rows(&sink.emit, t, &rows)?;
    sink.topn_ctr.emit_ns.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
    sink.topn_ctr.mat_rows.fetch_add(rows.len() as u64, Ordering::Relaxed);
    sink.topn_ctr.cand_rows.fetch_add(cands.len() as u64, Ordering::Relaxed);
    s.lists.push(cands);
    Ok(true)
}

/// inc-2b top level: route every face of over-budget partition `b` into a
/// depth-1 sub-bucket file, then combine each sub-partition (recursing where
/// still too big), emitting each leaf into `acc` (winners-only leaves emit
/// candidates only — `split_leaf_emit`). NULL faces never route — they merge
/// through one bounded mini-combine at the end (a leaf like any other: the
/// NULL group rides the selection's null tier).
fn split_views_and_emit(
    sink: &AggSink,
    b: usize,
    set: &Arc<::spillset::SpillSet>,
    locals: &[AggSinkLocal],
    acc: &mut SinkEmitAcc,
    sel: &mut Option<SplitSel<'_>>,
) -> PgResult<SplitOutcome> {
    sink.combine_splits.fetch_add(1, Ordering::Relaxed);
    sink.split_depth_max.fetch_max(1, Ordering::Relaxed);
    let state_words = sink.state_bytes / 8;
    let canon = sink.key_words == 0;
    let row_bytes = sink_spill_row_bytes(sink.key_words, state_words);
    let mut router = SubRouter::new(sink, set, b, 1);
    let mut scratch: Vec<u8> = Vec::new();
    let mut null_runs: Vec<SinkRun> = Vec::new();
    for l in locals {
        for r in &l.runs {
            scratch.clear();
            // Bytes-mode runs serialize the canonical record (the encode
            // dispatches on the run's own key_words).
            sink_run_spill_bucket(r, b, &mut scratch);
            router.absorb(&scratch)?;
            if b == SINK_NULL_BUCKET {
                if let Some(nb) = &r.null_states {
                    null_runs.push(sink_null_only_run(sink.key_words, state_words, nb.clone()));
                }
            }
        }
        if let (Some(t), Some(p)) = (&l.table, &l.part) {
            scratch.clear();
            if canon {
                ::nodeagg::sink::sink_remainder_spill_bucket_canon(
                    &t.remainder_view(p),
                    b,
                    &mut scratch,
                )?;
            } else {
                sink_remainder_spill_bucket(t.table(), p, b, &mut scratch);
            }
            router.absorb(&scratch)?;
            if b == SINK_NULL_BUCKET && !canon {
                if let Some(nb) = sink_remainder_null_block(t.table()) {
                    null_runs.push(sink_null_only_run(sink.key_words, state_words, nb));
                }
            }
        }
        if let Some(sp) = &l.spill {
            if canon {
                stream_part_records(&sp.file, b as u32, state_words, |chunk| {
                    router.absorb(chunk)
                })?;
            } else {
                stream_part_rows(&sp.file, b as u32, row_bytes, |chunk| router.absorb(chunk))?;
            }
            if b == SINK_NULL_BUCKET {
                for nb in &sp.null_blocks {
                    null_runs.push(sink_null_only_run(sink.key_words, state_words, nb.clone()));
                }
            }
        }
    }
    router.flush()?;
    match split_subparts_and_emit(sink, b, set, &router.file, 1, acc, sel)? {
        SplitOutcome::Done => {}
        other => return Ok(other),
    }
    if !null_runs.is_empty() {
        // The NULL group: one bounded mini-combine over its blocks only.
        let ctr = sink.topn.is_some();
        let t0 = ctr.then(std::time::Instant::now);
        let view = [SinkLocalView { spilled: &null_runs, runs: &[], remainder: None }];
        let t = sink_combine_bucket(b, sink.key_words, sink.state_bytes, &view, &sink.combines)?;
        sink.note_combine16(&t);
        if let Some(t0) = t0 {
            sink.topn_ctr.build_ns.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
        if !split_leaf_emit(sink, &t, acc, sel)? {
            return Ok(SplitOutcome::Declined);
        }
    }
    Ok(SplitOutcome::Done)
}

/// Combine each sub-partition of a routed split file; sub-partitions still
/// over budget recurse one hash byte deeper (fresh file), depth-capped.
fn split_subparts_and_emit(
    sink: &AggSink,
    b: usize,
    set: &Arc<::spillset::SpillSet>,
    file: &::spillset::SpillFile,
    depth: u32,
    acc: &mut SinkEmitAcc,
    sel: &mut Option<SplitSel<'_>>,
) -> PgResult<SplitOutcome> {
    let state_words = sink.state_bytes / 8;
    let canon = sink.key_words == 0;
    let row_bytes = if canon {
        ::nodeagg::sink::sink_canon_min_record_bytes(state_words)
    } else {
        sink_spill_row_bytes(sink.key_words, state_words)
    };
    for s in 0..SINK_NBUCKETS {
        let blen = file.part_len(s as u32) as usize;
        if blen == 0 {
            continue;
        }
        let rows = blen / row_bytes;
        // Canonical sub-partitions add the content term (blen bounds the
        // key content — headers/states over-count, the safe direction).
        let est = est_table_bytes(sink, rows)
            .saturating_add(if canon { blen.saturating_mul(3) / 2 } else { 0 });
        if est > sink.budget {
            if depth + 1 > spill_split_depth_cap() {
                return Ok(SplitOutcome::DepthCap);
            }
            sink.combine_splits.fetch_add(1, Ordering::Relaxed);
            sink.split_depth_max.fetch_max((depth + 1) as u64, Ordering::Relaxed);
            let mut router = SubRouter::new(sink, set, b, depth + 1);
            if canon {
                stream_part_records(file, s as u32, state_words, |chunk| {
                    router.absorb(chunk)
                })?;
            } else {
                stream_part_rows(file, s as u32, row_bytes, |chunk| router.absorb(chunk))?;
            }
            router.flush()?;
            match split_subparts_and_emit(sink, b, set, &router.file, depth + 1, acc, sel)? {
                SplitOutcome::Done => {}
                other => return Ok(other),
            }
            continue;
        }
        let ctx = ::mcx::MemoryContext::new("m35-agg-split-read");
        let Some(mut rd) = file.read_part(ctx.mcx(), s as u32)? else { continue };
        let bytes = rd.read_to_end()?;
        rd.close()?;
        let synth = if canon {
            ::nodeagg::sink::sink_run_from_spill_bytes(b, state_words, &bytes)?
        } else {
            sink_run_from_spill(b, sink.key_words, state_words, &bytes)?
        };
        let view = [SinkLocalView {
            spilled: core::slice::from_ref(&synth),
            runs: &[],
            remainder: None,
        }];
        let ctr = sink.topn.is_some();
        let t0 = ctr.then(std::time::Instant::now);
        let t = sink_combine_bucket(b, sink.key_words, sink.state_bytes, &view, &sink.combines)?;
        sink.note_combine16(&t);
        if let Some(t0) = t0 {
            sink.topn_ctr.build_ns.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
        if !split_leaf_emit(sink, &t, acc, sel)? {
            return Ok(SplitOutcome::Declined);
        }
    }
    Ok(SplitOutcome::Done)
}

/// The dict-int-key-class dict-code sink feed mode (`PGRUST_RUNTIME_AGG_DICTFEED`):
/// what the K2 sink does when the scan staging is dict-group armed (the
/// single-int-key GROUP BY over a dict-encoded pgrcolumnar column whose
/// fixed-width prefix deform is unarmable — UserID past varlena columns).
#[derive(Clone, Copy, PartialEq, Eq)]
enum DictFeed {
    /// Keep the dict-group registration and admit. MEASURED FINDING
    /// (dict-columnar-feed lane): pgrcolumnar NEVER dict-encodes INT chunks
    /// (`encode_int_chunk`: Const/For/Raw/DeltaFor only — Encoding::Dict is
    /// the varlena encoder's arm), and the K2 sink admits int keys only
    /// (`agg_sink_key_width`), so dict windows cannot reach this drain
    /// today: every window fills decoded Datums and the plain keys path
    /// runs. The dict-window branch (`sink_dict_batch` — CH
    /// LowCardinality-style per-epoch code -> state cache) is the
    /// fail-closed guard that makes keeping the registration SOUND (a dict
    /// window's key Datum cells are stale by the set_dict_lane contract),
    /// and the ready lane if int dict encoding ever lands.
    Code,
    /// Dict-free columnar re-arm (the int-distinct-serial precedent): rebuild the
    /// scan staging with NO dict registration so windows fill decoded
    /// Datums, then the plain K2 per-row probe drain runs unchanged.
    Raw,
    /// Refuse the engagement exactly as before this lane (kill switch).
    Off,
}

fn dict_feed_mode() -> DictFeed {
    static MODE: OnceLock<DictFeed> = OnceLock::new();
    crate::once_val(&MODE, || match std::env::var("PGRUST_RUNTIME_AGG_DICTFEED").as_deref() {
        Ok("0") | Ok("off") => DictFeed::Off,
        Ok("raw") => DictFeed::Raw,
        _ => DictFeed::Code,
    })
}

/// Per-worker dict-code sink scratch (DictFeed::Code): the direct-indexed
/// code -> live compact-table state map, keyed on the serial dict-group
/// arm's exact identity tuple (is_global, epoch/gepoch). The cached pointers
/// are LaneAggTable row-state addresses — allocation-stable across inserts
/// and across the morsel take/put hand-off (chunked row storage; the table
/// handle moves, its rows do not) — and they die at every sink flush
/// (`sink_flush_table` resets the table), which must `invalidate` this
/// scratch exactly like the mk intern-id cache beside it.
#[derive(Default)]
pub(super) struct SinkDictScratch {
    ident: Option<(bool, u64)>,
    slots: Vec<Option<core::ptr::NonNull<::execexpr::AggPerGroup>>>,
    /// This batch's first-appearance-ordered unresolved codes (parallel to
    /// the miss probe batch).
    miss_codes: Vec<u32>,
}

impl SinkDictScratch {
    fn invalidate(&mut self) {
        self.ident = None;
        self.slots.clear();
    }

    /// Heap backing-store bytes for the process estate ledger
    /// (GL-CONCMEM-1): `slots` is gndv-sized under a v7 stitch — the
    /// family's whale lane at high-NDV dict shapes.
    fn estate_bytes(&self) -> usize {
        super::vec_estate_bytes(&self.slots) + super::vec_estate_bytes(&self.miss_codes)
    }
}

/// One dict-answered staged batch through the CODE sink feed: pass 1 marks
/// each first-appearing unresolved code (first-arrival order preserved:
/// the miss batch probes in first-appearance order, which is row order),
/// one compact batch probe resolves all of this batch's misses, pass 2
/// hands every surviving row its cached state for the whole-batch fold.
/// Mirrors the serial `scan_dictgroup_batch` against the WORKER's bounded
/// compact table instead of the global C tuplehash; there is no spill leg
/// (the sink table never spills — it flushes at the cap, which invalidates
/// this cache in the drain loop).
#[allow(clippy::too_many_arguments)]
fn sink_dict_batch<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    dgs: &mut SinkDictScratch,
    rows: &[u32],
    keys: &mut Vec<::datum::Datum>,
    knull: &mut Vec<bool>,
    idxs: &mut Vec<u32>,
    groups: &mut Vec<core::ptr::NonNull<::execexpr::AggPerGroup>>,
    lane: ::exectuples::SoaDictLane,
    estate: &mut EStateData<'mcx>,
) -> Result<(), AcceptFail> {
    let ndict = lane.table.ndict as usize;
    let global = lane.table.has_stitch();
    let (ident, size) = if global {
        ((true, lane.table.gepoch), lane.table.gndv as usize)
    } else {
        ((false, lane.table.epoch), ndict)
    };
    if dgs.ident != Some(ident) {
        dgs.ident = Some(ident);
        dgs.slots.clear();
        dgs.slots.resize(size, None);
        lane_trace(&format!(
            "sink dict-group {} {} (n={size})",
            if global { "gepoch" } else { "epoch" },
            ident.1
        ));
    }
    debug_assert!(dgs.slots.len() >= size, "dict size is fixed per identity");
    // PENDING sentinel: marks a code queued in THIS batch's miss list. A
    // dangling NonNull can never equal a live table row address.
    let pending = core::ptr::NonNull::<::execexpr::AggPerGroup>::dangling();
    dgs.miss_codes.clear();
    keys.clear();
    knull.clear();
    for &i in rows {
        let local = lane.code(i as usize);
        debug_assert!((local as usize) < ndict, "filler contract: code < ndict");
        let code =
            if global { lane.table.global_code(local) as usize } else { local as usize };
        debug_assert!(code < size, "stitch contract: global code < gndv");
        if dgs.slots[code].is_none() {
            dgs.slots[code] = Some(pending);
            dgs.miss_codes.push(code as u32);
            // NULL discipline: dict codes have no NULL representation and
            // pgrcolumnar stores no NULLs (per-chunk proof) — as the serial arm.
            keys.push(lane.table.datum(local));
            knull.push(false);
        }
    }
    if !keys.is_empty() {
        groups.clear();
        if !::nodeagg::agg_hash_compact_batch(agg, estate, keys, knull, groups)? {
            // The sink-mode backstop errors before migrating; belt-and-braces
            // (the raw K2 leg's exact treatment).
            return Err(AcceptFail::Error(::nodeagg::sink::sink_shape_error(
                "worker compact table disarmed mid-build",
            )));
        }
        for (k, &code) in dgs.miss_codes.iter().enumerate() {
            dgs.slots[code as usize] = Some(groups[k]);
        }
    }
    idxs.clear();
    groups.clear();
    for &i in rows {
        let local = lane.code(i as usize);
        let code =
            if global { lane.table.global_code(local) as usize } else { local as usize };
        let pg = dgs.slots[code].expect("every survivor code resolved above");
        debug_assert!(pg != pending, "pending sentinel must have been installed");
        idxs.push(i);
        groups.push(pg);
    }
    let soa =
        ::nodeseqscan::seq_scan_batch_soa(ss).expect("sink dict feed requires the armed SoA");
    // SAFETY: as the raw K2 sink fold — every probed row is non-fallback
    // (pgrcolumnar stages none; the caller admits all-lane batches only) with
    // valid lane values for every plan column (the key column is NOT in
    // `plan.cols`: the dict-group arm refuses that shape, and only that arm
    // registers the dict lane); the plan is unguarded (sink admission);
    // each state is a live compact-table row installed by a probe since the
    // last flush (flushes invalidate this cache before the next batch).
    unsafe { super::agg_fold_staged(agg, soa, idxs, groups)? };
    Ok(())
}

/// GL-VECACCEPT-2 per-worker lane scratch (reused across granules; the vec
/// drive's only steady-state allocations): one Datum lane per referenced
/// scan column, copied whole-granule from the direct feed (the lanes
/// borrow `&mut ss` one at a time — the topn-heap two-phase borrow law),
/// plus the shared all-false null lane. Unmetered scratch, the
/// ScanK2Scratch class (≤ (1+ncols) × 8192 × 8B ≈ tens of KB).
#[derive(Default)]
struct SinkVecScratch {
    /// (scan col, granule lane) pairs — `cols[0]` is always the key column.
    cols: Vec<(u16, Vec<::datum::Datum>)>,
    /// All-false isnull lane (columnar parts store no NULLs — the direct
    /// feed contract), sized to the largest granule seen.
    knull: Vec<bool>,
    /// Chunk row indices 0..VEC_CHUNK (agg_fold_staged's idx vocabulary).
    idxv: Vec<u32>,
}

impl SinkVecScratch {
    /// Heap backing-store bytes for the process estate ledger
    /// (GL-CONCMEM-1): the granule lane copies (one Datum lane per
    /// referenced scan column) plus the null/idx lanes.
    fn estate_bytes(&self) -> usize {
        super::vec_estate_bytes(&self.cols)
            + self.cols.iter().map(|(_, l)| super::vec_estate_bytes(l)).sum::<usize>()
            + super::vec_estate_bytes(&self.knull)
            + super::vec_estate_bytes(&self.idxv)
    }
}

/// One chunk's [`::lanefold::LaneCols`] view over the scratch lanes: the
/// fold kernels read `col_values(c)` in scan-column space, exactly the
/// SoA's vocabulary — here answered from the granule lane copies, sliced
/// to the chunk. No dict/len side channels (admission refuses those
/// compositions), no NULLs (part-lane law).
struct VecChunkCols<'a> {
    vs: &'a SinkVecScratch,
    lo: usize,
    n: usize,
}

impl ::lanefold::LaneCols for VecChunkCols<'_> {
    fn col_values(&self, c: usize) -> &[::datum::Datum] {
        let lane = &self
            .vs
            .cols
            .iter()
            .find(|(col, _)| *col as usize == c)
            .expect("vec drive staged every fold-plan column")
            .1;
        &lane[self.lo..self.lo + self.n]
    }
    fn col_isnull(&self, _c: usize) -> &[bool] {
        &self.vs.knull[..self.n]
    }
}

/// GL-VECACCEPT-2: the whole-granule direct-lane K2 drain — the vecaccept
/// schedule ported to the agg sink. Per granule (the topn-heap direct
/// feed; no window staging, no SoA deform, no survivor collection, no
/// per-row key gather): copy the key + fold-plan lanes into scratch, then
/// per 1024-row chunk run the incumbent laws and kernels — the loop-head
/// cap-flush/pressure/spill block (verbatim minus the mk/xk/dict caches
/// this drain structurally lacks), `agg_hash_compact_batch` (the
/// prefetched batch probe), and `agg_fold_staged` over a chunk LaneCols
/// view. Same probe order, same group-creation order, same fold order as
/// the staged drain — byte-identical downstream by construction; only the
/// flush-check grain moves (window 256 → chunk 1024, the documented
/// one-chunk overshoot).
fn sink_drain_range_vec<'mcx>(
    sink: &AggSink,
    local: &mut AggSinkLocal,
    worker: usize,
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    vs: &mut SinkVecScratch,
    idxs: &mut Vec<u32>,
    groups: &mut Vec<core::ptr::NonNull<::execexpr::AggPerGroup>>,
    estate: &mut EStateData<'mcx>,
) -> Result<(), AcceptFail> {
    let key_col = ::nodeagg::agg_hash_staged_probe_col(agg).ok_or_else(|| {
        AcceptFail::Error(::nodeagg::sink::sink_shape_error(
            "worker build lost its staged key column",
        ))
    })? as usize;
    // Column census: key first, then the fold plan's lanes (deduped —
    // admission proved the plan unguarded/vguard-free/filter-free).
    {
        let plan = ::nodeagg::agg_lanefold_plan(agg).ok_or_else(|| {
            AcceptFail::Error(::nodeagg::sink::sink_shape_error(
                "vec drain without a fold plan",
            ))
        })?;
        vs.cols.clear();
        vs.cols.push((key_col as u16, Vec::new()));
        for &c in plan.cols.iter() {
            if !vs.cols.iter().any(|(col, _)| *col == c) {
                vs.cols.push((c, Vec::new()));
            }
        }
    }
    let mut rows_total = 0u64;
    loop {
        ::postgres_seams::check_for_interrupts::call()?;
        let Some((nrows, _base)) = ::nodeseqscan::seq_scan_topn_direct_next_granule(ss)?
        else {
            break;
        };
        let n = nrows as usize;
        // Lane copies (one `&mut ss` borrow at a time). A lane the part
        // cannot serve directly is a contract breach for the admitted
        // int-family shape — fail closed, nothing half-consumed (the RG
        // aborts; the serial rerun consumes nothing).
        for i in 0..vs.cols.len() {
            let col = vs.cols[i].0 as usize;
            let Some(lane) = ::nodeseqscan::seq_scan_topn_direct_lane(ss, col) else {
                return Err(AcceptFail::Error(::nodeagg::sink::sink_shape_error(
                    "vec drain lane not directly servable",
                )));
            };
            vs.cols[i].1.clear();
            vs.cols[i].1.extend_from_slice(&lane[..n]);
        }
        if vs.knull.len() < n {
            vs.knull.resize(n, false);
        }
        // Chunked probe + fold under the incumbent flush/pressure laws.
        let mut lo = 0usize;
        while lo < n {
            let len = (n - lo).min(VEC_CHUNK);
            // --- Loop-head flush block: the staged drain's law, verbatim
            // minus the dict/mk/intern caches this drain structurally
            // lacks (K2 int keys never intern; an intern_reset here is a
            // contract breach, fail-closed).
            if let Some((run, intern_reset)) =
                ::nodeagg::sink::agg_sink_flush_if_due(agg, local.alpha.cap(sink))
            {
                if intern_reset {
                    return Err(AcceptFail::Error(::nodeagg::sink::sink_shape_error(
                        "intern reset on a vec K2 drain",
                    )));
                }
                local.alpha.on_cap_flush(run.nrows(), sink);
                local.probe_flushes += 1;
                local.probe_flush_bytes += run.bytes() as u64;
                if !sink.shared.as_ref().is_some_and(|sh| sh.absorb(&run)) {
                    local.run_bytes += run.bytes();
                    local.runs.push(run);
                }
                // byval-POD admission: the byref aggctx term is
                // structurally 0 (sink.byref_states refused).
                if local.run_bytes + ::nodeagg::sink::agg_sink_table_mem(agg) > sink.budget
                {
                    match &sink.spill_set {
                        Some(set) => spill_epoch(sink, local, set, worker)
                            .map_err(AcceptFail::Error)?,
                        None => return Err(AcceptFail::Budget),
                    }
                }
            }
            if ::nodeagg::sink::agg_sink_budget_pressure(agg) {
                match &sink.spill_set {
                    Some(set) => {
                        if let Some((run, intern_reset)) =
                            ::nodeagg::sink::agg_sink_flush_now(agg)
                        {
                            if intern_reset {
                                return Err(AcceptFail::Error(
                                    ::nodeagg::sink::sink_shape_error(
                                        "intern reset on a vec K2 drain",
                                    ),
                                ));
                            }
                            local.alpha.on_pressure_flush();
                            local.probe_flushes += 1;
                            local.probe_flush_bytes += run.bytes() as u64;
                            if !sink.shared.as_ref().is_some_and(|sh| sh.absorb(&run)) {
                                local.run_bytes += run.bytes();
                                local.runs.push(run);
                            }
                        }
                        if !local.runs.is_empty()
                            && (agg_spill_eager()
                                || local.run_bytes
                                    + ::nodeagg::sink::agg_sink_table_mem(agg)
                                    > sink.budget)
                        {
                            spill_epoch(sink, local, set, worker)
                                .map_err(AcceptFail::Error)?;
                        }
                        if ::nodeagg::sink::agg_sink_budget_pressure(agg) {
                            if lane_trace_enabled() {
                                lane_trace(&format!(
                                    "runtime-agg: budget-refused (residual, vec) worker={worker} run_bytes={} table_mem={} budget={}",
                                    local.run_bytes,
                                    ::nodeagg::sink::agg_sink_table_mem(agg),
                                    sink.budget,
                                ));
                            }
                            return Err(AcceptFail::Budget);
                        }
                    }
                    None => {
                        if lane_trace_enabled() {
                            lane_trace(&format!(
                                "runtime-agg: budget-refused (spill disarmed, vec) worker={worker} run_bytes={} table_mem={} budget={}",
                                local.run_bytes,
                                ::nodeagg::sink::agg_sink_table_mem(agg),
                                sink.budget,
                            ));
                        }
                        return Err(AcceptFail::Budget);
                    }
                }
            }
            // --- Probe (the incumbent prefetched batch kernel) + fold.
            let keys = &vs.cols[0].1[lo..lo + len];
            if !::nodeagg::agg_hash_compact_batch(
                agg,
                estate,
                keys,
                &vs.knull[..len],
                groups,
            )? {
                return Err(AcceptFail::Error(::nodeagg::sink::sink_shape_error(
                    "worker compact table disarmed mid-build",
                )));
            }
            local.alpha.absorbed(len);
            if vs.idxv.len() < len {
                vs.idxv = (0..VEC_CHUNK as u32).collect();
            }
            idxs.clear();
            idxs.extend_from_slice(&vs.idxv[..len]);
            let cols = VecChunkCols { vs, lo, n: len };
            // SAFETY: every chunk row carries live decoded lane values for
            // every fold-plan column (the granule lane copies above cover
            // rows lo..lo+len, and idxs ⊆ 0..len index the chunk view);
            // the plan is unguarded (vec admission); each pergroup was
            // installed by the compact probe within this chunk
            // (agg_fold_staged contract).
            unsafe { super::agg_fold_staged(agg, &cols, idxs, groups)? };
            lo += len;
        }
        rows_total += n as u64;
    }
    sink.vec_rows.fetch_add(rows_total, Ordering::Relaxed);
    Ok(())
}

/// The narrow sink drain over the positioned claim: per staged page batch —
/// cap-flush check, survivor collection, canonical key gather, compact
/// batch probe (never the C table), whole-batch fold.
#[allow(clippy::too_many_arguments)]
fn sink_drain_range<'mcx>(
    sink: &AggSink,
    local: &mut AggSinkLocal,
    worker: usize,
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    k2s: &mut ScanK2Scratch,
    dgs: &mut SinkDictScratch,
    idxs: &mut Vec<u32>,
    groups: &mut Vec<core::ptr::NonNull<::execexpr::AggPerGroup>>,
    xk: &mut Option<Box<super::ExprKeyState>>,
    stage_slot: &mut Option<::executils::ExecSlotId>,
    mk: &mut Option<super::ScanMk>,
    mks: &mut super::MkScratch,
    estate: &mut EStateData<'mcx>,
) -> Result<(), AcceptFail> {
    let key_col = match sink.drain {
        // Unused: the expr-key feed derives keys; the mk feed packs its own.
        SinkDrain::ExprKey | SinkDrain::Mk => 0,
        SinkDrain::K2 => ::nodeagg::agg_hash_staged_probe_col(agg).ok_or_else(|| {
            AcceptFail::Error(::nodeagg::sink::sink_shape_error(
                "worker build lost its staged key column",
            ))
        })? as usize,
    };
    // SCATTER ACCEPT (GL-RADIX-3): build the Local's scatter state at the
    // first drained batch. The worker re-derives the leader's whitelist
    // verdict on its own plan (F1 both-sides law) — a divergence is a
    // contract breach, fail-closed.
    if sink.scatter && local.scatter.is_none() {
        let Some(sc) = ::nodeagg::sink::sink_scatter_new(agg, sink.width) else {
            return Err(AcceptFail::Error(::nodeagg::sink::sink_shape_error(
                "scatter worker shape diverged from admission",
            )));
        };
        local.scatter = Some(Box::new(sc));
    }
    loop {
        // Scatter flush discipline (before the batch, the table's own law):
        // at the sink cap the buffered single-row blocks leave as ONE
        // ordinary bucket-contiguous run; R3 budget crossing afterwards
        // spills the accumulated runs (scatter-built included — the spill
        // record is key/state-word framing either way) or refuses exactly
        // like the table's cap-flush path below.
        if local.scatter.as_deref().is_some_and(|s| s.nrows() >= sink.cap as usize) {
            if let Some(run) = local.scatter.as_deref_mut().and_then(|s| s.take_run()) {
                sink.scatter_rows.fetch_add(run.nrows() as u64, Ordering::Relaxed);
                local.probe_flushes += 1;
                local.probe_flush_bytes += run.bytes() as u64;
                local.run_bytes += run.bytes();
                local.runs.push(run);
            }
            if local.run_bytes + local.scatter.as_deref().map_or(0, |s| s.bytes())
                > sink.budget
            {
                match &sink.spill_set {
                    Some(set) => {
                        spill_epoch(sink, local, set, worker).map_err(AcceptFail::Error)?
                    }
                    None => return Err(AcceptFail::Budget),
                }
            }
        }
        // Bounded-Local discipline: flush BEFORE the batch (no group pointer
        // held across this point), budget-check table + runs. The threshold
        // is the α-gate's per-Local effective cap (== sink.cap until a fill
        // window adjudicates a demote — flush WHEN only, never results).
        if let Some((run, intern_reset)) =
            ::nodeagg::sink::agg_sink_flush_if_due(agg, local.alpha.cap(sink))
        {
            local.alpha.on_cap_flush(run.nrows(), sink);
            local.probe_flushes += 1;
            local.probe_flush_bytes += run.bytes() as u64;
            // Shared-table EXPERIMENT (D2, default OFF): an absorbed run's
            // rows live in the socket table — the run is DROPPED, holding
            // no Local memory; a refusal (closed face) is the spill
            // fallback: the run rides the incumbent path below.
            if !sink.shared.as_ref().is_some_and(|sh| sh.absorb(&run)) {
                local.run_bytes += run.bytes();
                local.runs.push(run);
            }
            // The flush RESET the compact table: every cached code -> state
            // pointer is a dangling table row — drop the dict-code cache
            // (K2 feed) AND the expr-key drain's code→pergroup cache
            // (GL-DICTDRAIN-1 — the dict-coded resolve caches live table
            // pointers per (epoch, code); the 830320fed law).
            dgs.invalidate();
            if let Some(xk) = xk.as_deref_mut() {
                xk.invalidate_group_caches();
            }
            if intern_reset {
                // The flush RESET the intern table (wide-vocabulary
                // bounding): every code→intern-id cache is now stale — a
                // cached id would materialize the WRONG canonical bytes.
                // DIRECT tables (arena-strings inc-3) raise this signal on
                // EVERY flush — the table itself reset, so the code→state
                // pointers dangle (the 830320fed law, fail-closed).
                mks.epoch = None;
                mks.code_ids.clear();
                mks.code_states.clear();
                if let Some(xk) = xk.as_deref_mut() {
                    xk.invalidate_mk_intern_cache();
                }
            }
            let aggctx = if sink.byref_states {
                ::nodeagg::sink::agg_sink_aggctx_mem(agg)
            } else {
                0
            };
            if local.run_bytes + ::nodeagg::sink::agg_sink_table_mem(agg) + aggctx
                > sink.budget
            {
                // M3.5: the crossing SPILLS when the arm is enabled (the
                // accumulated runs go to the Local's file as one epoch);
                // disabled = today's R5 refusal exactly.
                match &sink.spill_set {
                    Some(set) => {
                        spill_epoch(sink, local, set, worker).map_err(AcceptFail::Error)?
                    }
                    None => return Err(AcceptFail::Budget),
                }
            }
        }
        // Demote = refusal: at half-limit pressure (table + intern +
        // aggcontext vs the compact backstop's own thresholds) REFUSE — RG
        // abort -> serial rerun — before the backstop's sink-mode belt
        // would raise its hard error (the wide-vocabulary @100M class).
        //
        // M3.5 x mt16-cliffs (the two-int-key @100M hmm=2 cliff — measured: the
        // engaged arm hit THIS demote with ZERO spill epochs and fell to a
        // 150s serial spill): with a live spill arm the pressure is
        // table-driven (the mem leg counts the cap-bounded table, which the
        // mem-derived cap sizes to ~half the limit ALONE; cumulative byref
        // aggcontext eats the margin) — a budget the spill arm can absorb.
        // Law: force-flush the bounded table into a run (identical
        // intern-reset handling to the cap flush above) and spill the
        // accumulated runs as one epoch; only RESIDUAL pressure after the
        // flush+spill (the aggcontext floor the runs still reference)
        // refuses. Spill-disarmed (canonical/kill-switch) keeps the plain
        // refusal exactly.
        //
        // EPOCH SIZING add-on (spill-envelopes lane, on the law above): the
        // flush already drained the table-driven pressure — the run's bytes
        // were live table bytes a moment ago, so HOLDING them to the R3
        // budget crossing (the cap-flush path's own spill law above) keeps
        // the per-Local envelope intact while cutting FEWER, BIGGER epochs.
        // The pressure trip sits at the ~half-limit altitude (pinned by the
        // compact backstop's sink-mode belt — raising it means moving the
        // belt's hard error), so spill-per-trip wrote one ~(half-limit −
        // aggctx) run per epoch (two-int-key @100M: 15 × ~400MB per Local); the
        // budget-crossing law accumulates ~2 trips per epoch — half the
        // epoch brackets, half the combine-replay extents, same bytes.
        // PGRUST_RUNTIME_AGG_SPILL_EAGER=1 restores spill-per-trip (the
        // A/B attribution arm).
        if ::nodeagg::sink::agg_sink_budget_pressure(agg) {
            match &sink.spill_set {
                Some(set) => {
                    if let Some((run, intern_reset)) =
                        ::nodeagg::sink::agg_sink_flush_now(agg)
                    {
                        local.alpha.on_pressure_flush();
                        local.probe_flushes += 1;
                        local.probe_flush_bytes += run.bytes() as u64;
                        // Shared-table experiment: as at the cap flush —
                        // absorption drains the pressure outright.
                        if !sink.shared.as_ref().is_some_and(|sh| sh.absorb(&run)) {
                            local.run_bytes += run.bytes();
                            local.runs.push(run);
                        }
                        // The flush RESET the compact table — drop the
                        // expr-key drain's code→pergroup cache (as at the
                        // cap flush above; GL-DICTDRAIN-1).
                        if let Some(xk) = xk.as_deref_mut() {
                            xk.invalidate_group_caches();
                        }
                        if intern_reset {
                            mks.epoch = None;
                            mks.code_ids.clear();
                            // DIRECT tables: the table reset — dangling
                            // code→state pointers (see the cap flush above).
                            mks.code_states.clear();
                            if let Some(xk) = xk.as_deref_mut() {
                                xk.invalidate_mk_intern_cache();
                            }
                        }
                    }
                    let aggctx = if sink.byref_states {
                        ::nodeagg::sink::agg_sink_aggctx_mem(agg)
                    } else {
                        0
                    };
                    if !local.runs.is_empty()
                        && (agg_spill_eager()
                            || local.run_bytes
                                + ::nodeagg::sink::agg_sink_table_mem(agg)
                                + aggctx
                                > sink.budget)
                    {
                        spill_epoch(sink, local, set, worker).map_err(AcceptFail::Error)?;
                    }
                    if ::nodeagg::sink::agg_sink_budget_pressure(agg) {
                        // RESIDUAL pressure after flush+spill = the
                        // unspillable floor (the two-int-key byref aggcontext class,
                        // proportionality-audit). One counted line makes
                        // every future envelope cliff self-diagnosing —
                        // without it the refusal is silent and only shows
                        // as a 10-15x serial-rerun wall.
                        if lane_trace_enabled() {
                            lane_trace(&format!(
                                "runtime-agg: budget-refused (residual) worker={worker} run_bytes={} table_mem={} aggctx={} budget={}",
                                local.run_bytes,
                                ::nodeagg::sink::agg_sink_table_mem(agg),
                                ::nodeagg::sink::agg_sink_aggctx_mem(agg),
                                sink.budget,
                            ));
                        }
                        return Err(AcceptFail::Budget);
                    }
                }
                None => {
                    if lane_trace_enabled() {
                        lane_trace(&format!(
                            "runtime-agg: budget-refused (spill disarmed) worker={worker} run_bytes={} table_mem={} aggctx={} budget={}",
                            local.run_bytes,
                            ::nodeagg::sink::agg_sink_table_mem(agg),
                            ::nodeagg::sink::agg_sink_aggctx_mem(agg),
                            sink.budget,
                        ));
                    }
                    return Err(AcceptFail::Budget);
                }
            }
        }
        let n = ::nodeseqscan::seq_scan_next_pagebatch(ss, estate)?;
        if n == 0 {
            // End of claim: drop the scan slot's buffer pin (SeqScanSource
            // end-of-stream parity).
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(ss.ss.ss_ScanTupleSlot), mcx);
            return Ok(());
        }
        ::postgres_seams::check_for_interrupts::call()?;
        let ea = sink.ea_scan_node.is_some();
        if ea {
            local.instr.rows.scanned += n as u64;
        }
        if sink.drain == SinkDrain::ExprKey {
            // Expr-key feed: keys derived per batch. A route off the compact
            // table (sticky range-guard/arith trap, a numeric pack demote's
            // disarm) is a REFUSAL, not an error: RG abort → serial
            // whole-attempt rerun (a data-borne C error then surfaces from
            // the serial replay with C's exact error identity).
            let xk = xk.as_deref_mut().ok_or_else(|| {
                AcceptFail::Error(::nodeagg::sink::sink_shape_error(
                    "expr-key drain without a worker decide",
                ))
            })?;
            if !super::exprkey::exprkey_sink_batch(
                agg, ss, xk, sink.mk.as_ref(), stage_slot, idxs, groups, n, estate,
            )? {
                return Err(AcceptFail::Budget);
            }
            // α numerator (batched route contract as the EA count below:
            // idxs holds this batch's survivors — the rows that probed).
            local.alpha.absorbed(idxs.len());
            if ea {
                // The sink-legal expr-key route is the batched one (per-row
                // routing errors above): idxs holds this batch's survivors.
                local.instr.rows.survived += idxs.len() as u64;
            }
            continue;
        }
        // Fail-closed: a fallback row has no staged key — the sink cannot
        // route it (no C-table leg exists here).
        let all_lane = ::nodeseqscan::seq_scan_batch_soa(ss)
            .is_some_and(|soa| soa.fallback_words().iter().all(|&w| w == 0));
        if !all_lane {
            return Err(AcceptFail::Error(::nodeagg::sink::sink_shape_error(
                "fallback rows in a sink accept batch",
            )));
        }
        // SE-T2AGG CAR B: guarded (vguard/uguard) plans prove the batch's
        // SELECTED rows inline BEFORE any consumer touches a varlena payload
        // (the serial feed's check_guards discipline; unselected prewhere
        // cells may be stale, so the domain is the qual selection). The
        // serial arm DEMOTES to its checked per-row program on a failed
        // proof; the sink has no per-row leg, so a demote is a REFUSAL —
        // RG abort → serial whole-attempt rerun (the mk numeric-demote
        // discipline). Unreachable unless the vguard admission
        // (sink_vguard_plan_ok, knob-gated default OFF) let the plan in.
        {
            let plan =
                ::nodeagg::agg_lanefold_plan(agg).expect("sink drain without a fold plan");
            if plan.guarded {
                let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                    .expect("sink drain requires the armed SoA");
                let nwords = (n as usize).div_ceil(64);
                let mut sel = [0u64; ::exectuples::SOA_BM_WORDS];
                match ::nodeseqscan::seq_scan_batch_qual_sel(ss) {
                    Some(q) => sel[..nwords].copy_from_slice(&q[..nwords]),
                    None => sel[..nwords].fill(u64::MAX),
                }
                if n % 64 != 0 {
                    sel[nwords - 1] &= (1u64 << (n % 64)) - 1;
                }
                // SAFETY: selected rows of an all-lane batch carry live
                // deformed lane values for every plan column (the staging
                // contract the serial proof site rides — survivor windows'
                // completing deform filled every prefix column).
                let demote = unsafe {
                    ::lanefold::check_guards(plan, soa, &sel[..nwords], |_| None)
                } == ::lanefold::GuardCheck::Demote;
                if demote {
                    lane_trace("runtime-agg: vguard proof demoted — refusing to serial");
                    return Err(AcceptFail::Budget);
                }
            }
        }
        if sink.drain == SinkDrain::Mk {
            // The serial lane's own packed multi-key batch (survivors →
            // pack pre-pass → mk1/mk2 compact probe → whole-batch fold).
            // Under the sink cap the compact backstop ERRORS instead of
            // migrating. A `false` = the feed demoted mid-build: Numeric
            // components carry a per-value pack-legality demote (and the
            // C2 shapes ride the same batch) — that is a REFUSAL (RG abort
            // → serial whole-attempt rerun), never silent wrong-table
            // routing. Int-only components cannot demote — a `false` there
            // is a contract breach and stays an error.
            let mk = mk.as_ref().ok_or_else(|| {
                AcceptFail::Error(::nodeagg::sink::sink_shape_error(
                    "mk drain without a worker shape",
                ))
            })?;
            // α numerator staging: scan_mk_batch's survivor-less PREWHERE
            // window returns BEFORE survivor collection — pre-clear so the
            // count below can never re-read a previous batch's survivors.
            mks.rows.clear();
            if !super::scan_mk_batch(
                agg,
                ss,
                mk,
                mks,
                idxs,
                groups,
                n,
                sink.freeze.as_deref(),
                estate,
            )? {
                let demotable = mk
                    .shape
                    .comps
                    .iter()
                    .any(|c| !matches!(c.kind, ::nodeagg::MkCompKind::Int { .. }));
                if demotable {
                    return Err(AcceptFail::Budget);
                }
                return Err(AcceptFail::Error(::nodeagg::sink::sink_shape_error(
                    "worker mk feed demoted mid-build",
                )));
            }
            // α numerator: the mk feed's own survivor collection (freeze-
            // filtered rows never probed — they were dropped before the
            // pack; survivor-less windows counted 0 via the pre-clear).
            local.alpha.absorbed(mks.rows.len());
            continue;
        }
        let ScanK2Scratch { rows, keys, knull, .. } = k2s;
        super::scan_collect_survivors(ss, estate, n, rows)?;
        // α numerator: both K2 legs (dict window and plain keys) fold
        // exactly these survivors.
        local.alpha.absorbed(rows.len());
        if ea {
            local.instr.rows.survived += rows.len() as u64;
        }
        // Dict-answered window under the CODE feed (dict-group staging on a
        // sink build): group on the u32 codes through the per-epoch cache —
        // the key column's Datum cells are STALE while the dict lane
        // answers, so this branch must own every dict window. Raw-answered
        // windows (non-dict key chunks) take the plain keys path below;
        // both resolve into the same worker table in the same row order.
        if let Some(lane) = ::nodeseqscan::seq_scan_batch_soa(ss)
            .and_then(|soa| soa.dict_lane(key_col))
        {
            // Scatter admission excluded the dict-code feed (the only arm
            // that registers a dict lane) — a sighting here is a contract
            // breach, never a silent path mix (which would reorder
            // first-seen arrivals across the buffer/table faces).
            if local.scatter.is_some() {
                return Err(AcceptFail::Error(::nodeagg::sink::sink_shape_error(
                    "dict window on a scatter-armed drain",
                )));
            }
            sink_dict_batch(agg, ss, dgs, rows, keys, knull, idxs, groups, lane, estate)?;
            continue;
        }
        keys.clear();
        knull.clear();
        {
            let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                .expect("sink drain requires the armed SoA");
            let (kv, kn) = (soa.col_values(key_col), soa.col_isnull(key_col));
            for &i in rows.iter() {
                keys.push(kv[i as usize]);
                knull.push(kn[i as usize]);
            }
        }
        // SCATTER ACCEPT (GL-RADIX-3): bypass the worker table wholesale —
        // each survivor becomes a single-row state block radix-routed into
        // the Local's bucket buffers (::nodeagg::sink::SinkScatter doc; the
        // cap/flush/budget discipline is at the loop top). No probe, no
        // insert, no fold.
        if let Some(sc) = local.scatter.as_deref_mut() {
            let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                .expect("sink drain requires the armed SoA");
            sc.absorb_batch(soa, rows, keys, knull);
            continue;
        }
        if !::nodeagg::agg_hash_compact_batch(agg, estate, keys, knull, groups)? {
            // The compact table migrated (backstop) — unexportable. The
            // sink-mode backstop errors before this; belt-and-braces.
            return Err(AcceptFail::Error(::nodeagg::sink::sink_shape_error(
                "worker compact table disarmed mid-build",
            )));
        }
        idxs.clear();
        idxs.extend_from_slice(rows);
        let soa =
            ::nodeseqscan::seq_scan_batch_soa(ss).expect("sink drain requires the armed SoA");
        // SAFETY: every probed row is non-fallback (all-lane batch), so the
        // SoA lanes carry valid deformed values for every plan column; the
        // plan is unguarded (sink admission); each pergroup was installed by
        // the compact probe within this batch (agg_fold_staged contract).
        unsafe { super::agg_fold_staged(agg, soa, idxs, groups)? };
    }
}

// ---------------------------------------------------------------------------
// Helper (worker) side: entry task + POST_TASK_PARK drive.
// ---------------------------------------------------------------------------

fn runtime_agg_worker_main(_shared: &parallel::ParallelShared) -> PgResult<()> {
    Ok(())
}

fn runtime_agg_post_task_park(shared: &parallel::ParallelShared) {
    let Some(private) = shared.private() else {
        // F1 observability: a context with NO private payload can never be
        // driven by any arm — trace it (foreign-payload downcast misses stay
        // silent below: every arm's hook runs for every worker by design).
        lane_trace("runtime-agg: post-task-park without a private payload");
        return;
    };
    let Ok(payload) = private.downcast::<RuntimeAggShared>() else { return };
    // Every LAUNCHED helper bumps `exited` exactly once, on EVERY exit path
    // (the leader's liveness reap counts these against `launched`).
    // HOOK-frame placement (the scan arm's law): the standing driver reuses
    // helper_drive and must NOT bump — standing exits are accounted by the
    // board's claimed/detached counters, and stale standing bumps would
    // poison the launched loop's reap threshold.
    let _exit = ExitBump(&payload.exited);
    let r = catch_unwind(AssertUnwindSafe(|| helper_drive(shared, &payload)));
    if r.is_err() {
        payload.sink.fail(PgError::new(ERROR, "runtime agg helper panicked").into());
    }
    latch::SetLatch(::types_storage::latch::LatchHandle::proc(
        shared.parallel_leader_proc_number,
    ));
}

/// The standing driver (M2 inc-1, parallel::set_standing_driver): runs ON
/// a standing executor, already impersonated (worker number + lock group).
/// Identical body to the POST_TASK_PARK hook minus the ExitBump (standing
/// exits ride the board's claimed/detached accounting); exit-committed
/// unwinds (FATAL) are rethrown to the gang glue — a terminated worker
/// must die, and swallowing one would resurrect it into the standing pool.
fn runtime_agg_standing_driver(shared: &parallel::ParallelShared) {
    let Some(private) = shared.private() else { return };
    let Ok(payload) = private.downcast::<RuntimeAggShared>() else { return };
    let r = catch_unwind(AssertUnwindSafe(|| helper_drive(shared, &payload)));
    if let Err(unwind) = r {
        payload
            .sink
            .fail(PgError::new(ERROR, "runtime agg standing executor panicked").into());
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

fn helper_drive(shared: &parallel::ParallelShared, payload: &Arc<RuntimeAggShared>) {
    let _ = shared;
    // Liveness-battery injection (test-only, default-off): the wedge-class
    // exit — panic before binding or driving; the reap must convert it into
    // a prompt error (scripts/runtime-liveness-e2e.sh).
    super::test_helper_panic("agg");
    // F1 fail-closed accounting: a helper that cannot participate must NEVER
    // vanish silently — every early exit below counts itself as a refusal
    // (the leader's started==0 && refused>=launched probe is its fallback
    // signal) and traces why.
    let Some(target) = payload.pcxt_shared.get() else {
        lane_trace("runtime-agg: helper refused (no pcxt shared)");
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) else {
        lane_trace("runtime-agg: helper refused (rg gone)");
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let Some(lane) = payload.rt.acquire_external_lane() else {
        lane_trace("runtime-agg: helper refused (no external lane)");
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let mut local = lane.local();
    let worker = payload.rt.nthreads() + lane.ordinal();
    let lane = std::cell::RefCell::new(Some(lane));
    let entered = std::cell::Cell::new(false);
    let bound = parallel::with_query_task_binding(target, || {
        entered.set(true);
        payload.started.fetch_add(1, Ordering::SeqCst);
        drive_bound(payload, &mut local, &rg, worker, &mut lane.borrow_mut())
    });
    match bound {
        Ok(()) => {}
        Err(e) => {
            if entered.get() {
                // Refusals (budget / topn-winners) are NOT query errors
                // (the leader falls back to the serial arm); the Err only
                // routed the binder through its abort-side cleanup.
                if !payload.sink.refused_any() {
                    payload.sink.fail(e);
                }
                // F1 liveness (the wedge mechanism): a helper that errored
                // BEFORE joining the drive (build_worker_exec failure) has
                // aborted the RG via fail()/refuse_budget() — but an aborted
                // PINNED RG still needs a driver to run invalidate/finalize/
                // complete, or the leader's waiter parks forever. Drive the
                // closed generation to completion here (pure protocol
                // cleanup, the drain_rg discipline); post-drive errors find
                // it already complete and skip.
                if rg.try_outcome().is_none() {
                    rg.abort();
                    let _ = payload.rt.drive_pinned(&mut local, &rg);
                }
            } else {
                lane_trace(&format!("runtime-agg: helper bind refused: {}", e.message()));
                payload.refused.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
    // WFIN markers: emitted by the runtime's generic channel (sched.rs,
    // PGRUST_MORSEL_MARKERS=1) — one line per (worker, task set). The arm's
    // own duplicate emitter was removed at m2-integration: with the sched
    // channel armed the double emission (different time bases) garbled the
    // instrument parser's spread verdicts.
}

/// M3.5 P1 substrate probe (env-gated, inc-2 opening move): prove on a
/// REAL binder-bound helper thread that the fd substrate supports the spill
/// design — create a FileSet segment, write an epoch, read it back on this
/// thread, verify bytes. Emits one marker line the e2e tranche parses.
fn spill_substrate_probe(payload: &Arc<RuntimeAggShared>, worker: usize) {
    if std::env::var("PGRUST_SPILL_SUBSTRATE_PROBE").as_deref() != Ok("1") {
        return;
    }
    let Some(set) = payload.sink.spill_set.as_ref() else {
        eprintln!("M35|SPILLPROBE|worker={worker}|ok=0|why=no-spill-set");
        return;
    };
    let r = (|| -> PgResult<bool> {
        let ctx = ::mcx::MemoryContext::new("m35-spill-probe");
        let mut f = ::spillset::SpillFile::new(
            Arc::clone(set),
            ::spillset::SpillSet::file_name("probe", 0, worker),
            4,
        );
        let payload_bytes: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
        let mut w = f.begin_epoch(ctx.mcx())?;
        w.write_part(1, &payload_bytes)?;
        w.finish()?;
        let Some(mut r) = f.read_part(ctx.mcx(), 1)? else { return Ok(false) };
        let got = r.read_to_end()?;
        r.close()?;
        Ok(got == payload_bytes)
    })();
    match r {
        Ok(true) => eprintln!("M35|SPILLPROBE|worker={worker}|ok=1"),
        Ok(false) => eprintln!("M35|SPILLPROBE|worker={worker}|ok=0|why=mismatch"),
        Err(e) => eprintln!("M35|SPILLPROBE|worker={worker}|ok=0|why={}", e.message()),
    }
}

fn drive_bound(
    payload: &Arc<RuntimeAggShared>,
    local: &mut runtime::WorkerLocal,
    rg: &runtime::RgHandle,
    worker: usize,
    lane: &mut Option<runtime::ExternalLane>,
) -> PgResult<()> {
    build_worker_exec(payload)?;
    spill_substrate_probe(payload, worker);
    let _end = super::standing_channel::drive_pool_serve(&payload.rt, local, rg, lane);
    let self_errored =
        WORKER_EXEC.with(|cell| cell.borrow().as_ref().is_some_and(|ex| ex.errored.get()));
    let teardown = teardown_worker_exec(!self_errored);
    if self_errored {
        // A released (not finished) executor may still hold registered
        // snapshots — the binder's NORMAL unbind asserts a cleared xmin, so
        // route through its transaction-ABORT path by returning an error
        // (observed live: snapmgr xmin assertion at worker slot teardown
        // after a budget refusal). The real error (if any) was recorded
        // first (fail() is first-wins); budget refusals record none and
        // helper_drive swallows this marker.
        teardown?;
        return Err(PgError::new(
            ERROR,
            "runtime agg worker unwound (recorded upstream)",
        )
        .into());
    }
    teardown
}

/// Build + SINK-ARM this helper's executor over the shared worker
/// PlannedStmt. Divergence from the leader's admission is an ERROR (the
/// leader proved the shape; a worker that cannot reproduce it must not
/// silently build something else).
fn build_worker_exec(payload: &Arc<RuntimeAggShared>) -> PgResult<()> {
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
        let armed = (|| -> PgResult<ArmedDrain> {
            crate::execmain::executor_start_seam(qd, payload.eflags)?;
            crate::querydesc::with_qd(qd, |q| {
                let x = q.exec.as_mut().expect("runtime agg worker ExecutorStart");
                x.with_mut(|d| -> PgResult<ArmedDrain> {
                    let estate = &mut d.estate;
                    let Some(crate::procnode::PlanStateNode::Agg(aps)) = d.planstate.as_mut()
                    else {
                        return Err(Box::new(PgError::new(
                            ERROR,
                            "runtime agg worker plan root is not an Agg",
                        )));
                    };
                    let aps = &mut **aps;
                    let crate::procnode::PlanStateNode::SeqScan(ss) = &mut aps.outer else {
                        return Err(Box::new(PgError::new(
                            ERROR,
                            "runtime agg worker outer node is not a SeqScan",
                        )));
                    };
                    arm_sink_build(&payload.sink, &mut aps.agg, ss, estate)
                })
            })
        })();
        match armed {
            Ok(drain) => {
                let (xk, mk) = match drain {
                    ArmedDrain::K2 => (None, None),
                    ArmedDrain::ExprKey(xk) => (Some(xk), None),
                    ArmedDrain::Mk(mk) => (None, Some(mk)),
                };
                *cell.borrow_mut() = Some(WorkerExec {
                    qd,
                    errored: std::cell::Cell::new(false),
                    k2s: ScanK2Scratch::default(),
                    dgs: SinkDictScratch::default(),
                    idxs: Vec::new(),
                    groups: Vec::new(),
                    xk,
                    stage_slot: None,
                    mk,
                    mks: super::MkScratch::default(),
                    vs: SinkVecScratch::default(),
                    scratch_ledger: ScratchLedger::default(),
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

/// DictFeed::Raw: rebuild the scan staging as the dict-FREE columnar arm —
/// the same offset-free window deform with NO column opted into dict lanes
/// (`seq_scan_cb_columnar_arm(.., None)`; the int-distinct-serial `arm_key_soa`
/// precedent generalized to the fold prefix) — so every window fills
/// decoded Datums and the plain K2 drain runs untouched. Fail-closed: the
/// re-arm must actually shed the dict registration (a PREWHERE-owned batch
/// keeps its co-consumers; single-key dict co-arms don't exist on that
/// path today, but the check is what makes this safe, not the today).
/// A later serial fallback re-arms dict-group idempotently — the fold
/// feed's `arm_scan_staging` re-runs its whole ladder.
fn sink_rearm_dictfree<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> bool {
    let Some(prefix) = super::fused_agg_soa_prefix(agg, ss) else { return false };
    ::nodeseqscan::seq_scan_cb_columnar_arm(ss, estate, prefix, None)
        && ::nodeseqscan::seq_scan_batch_dictgroup_col(ss).is_none()
        && ::nodeseqscan::seq_scan_batch_soa(ss).is_some()
}

/// The worker arm's drain-specific state (see [`arm_sink_build`]).
enum ArmedDrain {
    K2,
    ExprKey(Box<super::ExprKeyState>),
    Mk(super::ScanMk),
}

/// The worker's sink-build arm (see [`arm_sink_build_inner`]) plus the ONE
/// place the by-ref str transvalue store is armed.
///
/// GL-SINKCRASH-2 — the class fix. `min/max(text)` transvalues are plain
/// varlenas that the fold copies on install and copy-then-frees on replace.
/// The table they live in is Local-owned and LENT to whichever pool thread
/// serves each morsel, and it is read again LATER by the combine and emit
/// phases — so the only sound home for those copies is a store that travels
/// WITH the table and lives as long as the table does
/// ([`::lanefold::StrStateArena`]). Until now the store was armed inside a
/// single drain arm (the DictCoded expr-key kind), and the other str-capable
/// drains — K2 and Mk — copied into `::nodeagg::agg_aggcontext(agg)`: the bump
/// aggcontext of a pool thread's bound executor, whose lifetime is the
/// THREAD's binding, not the table's. That is what crashed the release
/// candidate, witnessed e2e (the crashing statement's own engagement prints
/// `drain=Mk … str_arena_armed=0`).
///
/// Note on the precise failure, because the lane's first reading of it was
/// wrong and the correction matters to anyone extending this: an in-pod census
/// of the aggcontext home per table found **zero** changes of home during the
/// build, i.e. a Local's table is served by ONE thread for the whole
/// engagement and its transvalues do NOT get scattered across several
/// contexts. The failure is therefore a LIFETIME failure, not a
/// multiple-allocator failure: one home, retired (thread unbound, aggcontext
/// released or rebound by the next statement) while the combine/emit phase
/// still reads through the pointers — which is why the observed errors are
/// `runtime agg sink combine panicked` and a shape violation "in a sink
/// combine/emit", both AFTER the build. A table-owned store fixes it under
/// either reading, since it both travels with the table and outlives any
/// thread's context; do not weaken it to a per-thread home on the argument
/// that tables turn out not to migrate mid-build.
///
/// So the arming lives HERE, at the single exit every drain passes through,
/// keyed on the class predicate ([`::lanefold::plan_has_str_trans`]) rather
/// than on a drain identity. A drain added later inherits it; a drain arm that
/// forgets to opt in cannot exist. Shapes with no by-ref str transvalue arm
/// nothing and are allocation-identical to before.
///
/// This is one half of the fix. The other half is the fail-closed check in
/// `agg_fold_staged_mm`: reaching a str advance on a sink build with no store
/// armed is a shape error, not a silent fall-back to the aggcontext. Together
/// they make the discipline provable instead of hopeful — the arming cannot be
/// incomplete without being loud.
fn arm_sink_build<'mcx>(
    sink: &AggSink,
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<ArmedDrain> {
    let armed = arm_sink_build_inner(sink, agg, ss, estate)?;
    if ::nodeagg::agg_lanefold_plan(agg).is_some_and(::lanefold::plan_has_str_trans) {
        ::nodeagg::sink::agg_sink_arm_str_state(agg);
        // Fail closed on the arming itself: every sink drain arms a compact
        // table before returning, so a plan with a str transvalue that finds no
        // store here has a shape this code does not understand. Refusing sends
        // the whole attempt to the serial arm, which is always correct.
        if ::nodeagg::sink::agg_sink_str_arena(agg).is_none() {
            return Err(::nodeagg::sink::sink_shape_error(
                "byref str transvalue on a sink drain whose table has no state store",
            ));
        }
    }
    Ok(armed)
}

/// The worker's sink-build arm: the serial lane's own staging + key-shape +
/// compact arm sequence, under the sink cap, with every admission the leader
/// proved re-checked (divergence = error). Returns the ExprKey drain's
/// worker decide (None for the K2 drain).
fn arm_sink_build_inner<'mcx>(
    sink: &AggSink,
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<ArmedDrain> {
    let shape_err = ::nodeagg::sink::sink_shape_error;
    // SE-T2AGG CAR B: the vguard-only widening mirrors the leader's gate
    // exactly (F1 leader/worker-verdict law; the knob is process-constant).
    // GL-DICTDRAIN-1: the dict-class expr-key drain additionally admits
    // vguard-bearing PROJECTED plans (`sink_exprkey_dict_vguard_ok`) — the
    // DictCoded kind arm below is the ONLY consumer (belted per kind).
    let base_plan_ok = ::nodeagg::agg_lanefold_plan(agg)
        .is_some_and(|p| !p.guarded && p.vguards.is_empty() && p.resid.is_empty())
        || super::sink_vguard_plan_ok(agg, ss);
    let plan_ok = base_plan_ok || super::sink_exprkey_dict_vguard_ok(agg, ss);
    if !plan_ok || ::nodeagg::agg_lanefold_has_resid(agg) {
        return Err(shape_err("worker fold plan diverged from the leader's"));
    }
    if sink.drain == SinkDrain::ExprKey {
        // The worker's own decide (same plan tree — same census result).
        let Some(mut xk) = super::exprkey::decide_exprkey(agg, ss, estate) else {
            return Err(shape_err("worker expr-key decide diverged from the leader's"));
        };
        // Sink-build marker: the drain adapter's demote exits REFUSE
        // instead of migrating (`ExprKeyState::sink_build` doc).
        xk.set_sink_build();
        if xk.sink_refused() {
            return Err(shape_err("worker expr-key decide starts refused"));
        }
        let kind = xk.sink_key_kind();
        // Belt (GL-DICTDRAIN-1): the widened vguard plan admission serves
        // the DictCoded kind ONLY — every other kind requires the base
        // plan gate it always had (F1 both-sides law).
        if !base_plan_ok && !matches!(kind, Some(super::exprkey::SinkXkKind::DictCoded)) {
            return Err(shape_err("worker fold plan diverged from the leader's"));
        }
        // Spill-armed admission flag mirrors the leader's exactly
        // (spill_set exists only on word-keyed spill-armed engagements).
        ::nodeagg::sink::agg_sink_set_cap_spill(agg, sink.cap, sink.spill_set.is_some());
        match (&sink.red, &sink.mk, kind) {
            (None, None, Some(super::exprkey::SinkXkKind::Single)) => {
                if ::nodeagg::agg_hash_compact_try_arm(agg) != ::nodeagg::CompactArm::Armed {
                    return Err(shape_err("worker compact arm refused under the sink cap"));
                }
            }
            (Some(shape), None, Some(super::exprkey::SinkXkKind::Reduced(wshape))) => {
                if wshape.width != shape.width || wshape.keys.len() != shape.keys.len() {
                    return Err(shape_err("worker reduced shape diverged from the leader's"));
                }
                if ::nodeagg::agg_hash_compact_try_arm_reduced(agg, wshape)
                    != ::nodeagg::CompactArm::Armed
                {
                    return Err(shape_err("worker reduced arm refused under the sink cap"));
                }
            }
            (
                None,
                Some(lshape),
                Some(super::exprkey::SinkXkKind::Multi { dict_input_att: _ }),
            ) => {
                // The ts-extract/CaseDict class: the serial build's own mk arm sequence
                // under the sink cap (CaseDict shapes arm their two Intern
                // atts through the shared pool — the serial feed's exact
                // call); every divergence from the leader's snapshot is an
                // error (combine + emit plans were built off that exact
                // shape).
                let (atts, n_atts) = xk
                    .sink_mk_intern_atts()
                    .expect("Multi kind carries intern atts");
                if ::nodeagg::agg_hash_compact_try_arm_mk_multi(agg, false, &atts[..n_atts])
                    != ::nodeagg::CompactArm::Armed
                {
                    return Err(shape_err("worker mk arm refused under the sink cap"));
                }
                let wshape = ::nodeagg::agg_hash_compact_mk_shape(agg)
                    .ok_or_else(|| shape_err("armed mk table lost its shape"))?;
                if &wshape != lshape {
                    return Err(shape_err("worker mk shape diverged from the leader's"));
                }
            }
            (None, Some(lshape), Some(super::exprkey::SinkXkKind::DictCoded)) => {
                // GL-DICTDRAIN-1: the Dict key class through the 1-Intern
                // compact spec — the serial coded arm's exact mk1 arm
                // (`try_arm_mk1(key_out)`), under the sink cap (the arm
                // elects intern-armed or DIRECT per `text_direct_enabled`;
                // both flush identical canonical-bytes runs, so the
                // leader's shape-only snapshot is arm-agnostic — the C2
                // single-text law verbatim). Divergence = error.
                let (atts, n_atts) =
                    xk.sink_mk_intern_atts().expect("Dict kind carries the key-out att");
                debug_assert_eq!(n_atts, 1, "the dict drain is the 1-Intern spec");
                if ::nodeagg::agg_hash_compact_try_arm_mk1(agg, Some(atts[0]))
                    != ::nodeagg::CompactArm::Armed
                {
                    return Err(shape_err("worker dict-coded arm refused under the sink cap"));
                }
                let wshape = ::nodeagg::agg_hash_compact_mk_shape(agg)
                    .ok_or_else(|| shape_err("armed dict-coded table lost its shape"))?;
                if &wshape != lshape {
                    return Err(shape_err("worker dict-coded shape diverged from the leader's"));
                }
                // The TABLE-OWNED byref str state store used to be armed
                // HERE, on this one drain arm of three str-capable ones
                // (GL-DICTDRAIN-3). GL-SINKCRASH-2 moved it to
                // `arm_sink_build`, the single exit every drain passes
                // through, keyed on the class predicate instead of on this
                // drain's identity — the per-arm arming is exactly how K2 and
                // Mk came to copy min/max(text) transvalues into a per-thread
                // aggcontext. Do not re-add an arming call here.
                lane_trace("runtime-agg: dict-coded sink drain armed (worker)");
            }
            _ => return Err(shape_err("worker expr-key kind diverged from the leader's")),
        }
        let spec_ok = match ::nodeagg::sink::agg_sink_key_spec(agg) {
            Some(SinkKeySpec::Single { width }) => {
                sink.red.is_none() && sink.mk.is_none() && width == sink.width
            }
            Some(SinkKeySpec::Reduced(sh)) => {
                sink.red.as_ref().is_some_and(|r| r.width == sh.width) && sh.width == sink.width
            }
            Some(SinkKeySpec::Multi(sh)) => sink.mk.as_ref() == Some(&sh),
            None => false,
        };
        if !spec_ok {
            return Err(shape_err("worker key spec diverged from the leader's"));
        }
        if ::nodeagg::sink::agg_sink_state_bytes(agg) != Some(sink.state_bytes) {
            return Err(shape_err("worker state layout diverged from the leader's"));
        }
        return Ok(ArmedDrain::ExprKey(xk));
    }
    super::arm_scan_staging(ss, estate, ScanFeedShape::HashAggFold { agg })?;
    // SE-T2AGG CAR B: the worker mirrors the leader's vguard columnar
    // re-arm (direct-index staging law; F1 leader/worker-verdict).
    if super::sink_vguard_plan_ok(agg, ss)
        && !super::sink_rearm_vguard_columnar(agg, ss, estate)
    {
        return Err(shape_err("worker vguard columnar staging refused"));
    }
    if sink.drain == SinkDrain::Mk {
        // Packed multi-key arm: the same decide the leader probed, this time
        // arming the compact table under the sink cap. Every divergence from
        // the leader's snapshot is an error (the sink's combine + emit plans
        // were built off that exact shape). Single-text shapes (one Intern
        // component) re-run the C2 admission; text-bearing shapes NEED their
        // dict/intern lane — only pure-int shapes refuse dict staging.
        // Spill-armed admission flag mirrors the leader's exactly
        // (spill_set exists only on word-keyed spill-armed engagements).
        ::nodeagg::sink::agg_sink_set_cap_spill(agg, sink.cap, sink.spill_set.is_some());
        let lshape = sink
            .mk
            .as_ref()
            .ok_or_else(|| shape_err("mk drain without a leader shape"))?;
        let single_text = lshape.comps.len() == 1 && lshape.intern_comp().is_some();
        let mk = if single_text {
            super::scan_mk1_text_shape(agg, ss, estate)
        } else {
            super::scan_mk_shape(agg, ss, estate)
        };
        let Some(mk) = mk else {
            return Err(shape_err("worker mk shape diverged from the leader's"));
        };
        if mk.shape.intern_comp().is_none()
            && (mk.dict_att.is_some()
                || ::nodeseqscan::seq_scan_batch_dictgroup_col(ss).is_some())
        {
            return Err(shape_err("dict component on a pure-int sink mk worker"));
        }
        if &mk.shape != lshape {
            return Err(shape_err("worker mk shape diverged from the leader's"));
        }
        match ::nodeagg::sink::agg_sink_key_spec(agg) {
            Some(SinkKeySpec::Multi(sh)) if &sh == lshape => {}
            _ => return Err(shape_err("worker key spec diverged from the leader's")),
        }
        if ::nodeagg::sink::agg_sink_state_bytes(agg) != Some(sink.state_bytes) {
            return Err(shape_err("worker state layout diverged from the leader's"));
        }
        return Ok(ArmedDrain::Mk(mk));
    }
    if super::scan_k2_shape_sink(agg, ss, estate).is_none() {
        return Err(shape_err("worker K2 shape diverged from the leader's"));
    }
    // Dict-group staging on the K2 build (the dict-int-key class): the same mode
    // decision the leader made. Code = keep the dict registration (the
    // drain's dict-window branch consumes the codes); Raw = dict-free
    // columnar re-arm (windows fill decoded Datums; the plain drain runs);
    // Off = the pre-lane refusal. Leader/worker verdicts agree because the
    // mode is a process-wide constant and both sides arm the same store.
    if ::nodeseqscan::seq_scan_batch_dictgroup_col(ss).is_some() {
        match dict_feed_mode() {
            DictFeed::Code => {}
            DictFeed::Raw => {
                if !sink_rearm_dictfree(agg, ss, estate) {
                    return Err(shape_err("dict-free columnar re-arm on a sink worker"));
                }
            }
            DictFeed::Off => {
                return Err(shape_err("dict-group staging on a sink worker"));
            }
        }
    }
    // Spill-armed admission flag mirrors the leader's exactly
    // (spill_set exists only on word-keyed spill-armed engagements).
    ::nodeagg::sink::agg_sink_set_cap_spill(agg, sink.cap, sink.spill_set.is_some());
    if ::nodeagg::agg_hash_compact_try_arm(agg) != ::nodeagg::CompactArm::Armed {
        return Err(shape_err("worker compact arm refused under the sink cap"));
    }
    match ::nodeagg::sink::agg_sink_key_spec(agg) {
        Some(SinkKeySpec::Single { width }) if width == sink.width => {}
        _ => return Err(shape_err("worker key spec diverged from the leader's")),
    }
    if ::nodeagg::sink::agg_sink_state_bytes(agg) != Some(sink.state_bytes) {
        return Err(shape_err("worker state layout diverged from the leader's"));
    }
    Ok(ArmedDrain::K2)
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

fn runtime_agg_private_shutdown(private: &(dyn std::any::Any + Send + Sync)) {
    let Some(payload) = private.downcast_ref::<RuntimeAggShared>() else { return };
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
            "pgrust_runtime_agg_main",
            runtime_agg_worker_main,
        );
        parallel::register_parallel_post_task_park(runtime_agg_post_task_park);
        parallel::register_parallel_private_shutdown(runtime_agg_private_shutdown);
    });
}

// ---------------------------------------------------------------------------
// Leader-side admission + engagement.
// ---------------------------------------------------------------------------

/// Env override for the sink flush cap (entries); None = budget-derived.
fn sink_cap_override() -> Option<u32> {
    static N: OnceLock<Option<u32>> = OnceLock::new();
    crate::once_val(&N, || {
        std::env::var("PGRUST_RUNTIME_AGG_CAP")
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .filter(|&c| c >= 1024)
    })
}

/// LOCALITY CAP (radix-cap lane): the high-NDV accept-latency bound. At
/// 100M/dop15 the budget-derived cap (~22M entries at matched memory) lets a
/// reduced-key-class Local grow to ~5-6M Salt8 entries (hundreds of MB): every
/// accept probe is a dependent DRAM miss (the dop16-bandwidth-ceiling study's
/// memory-LATENCY class — 55% stall_backend_mem at 3.6% bandwidth), and the
/// combine then re-merges ~dop× duplicated groups from the SEAL remainders
/// at the same latency. Bounding the worker table keeps accept probes
/// cache-resident and converts the surplus into bucket-partitioned SinkRuns
/// (sequential writes) that the 256-bucket combine merges into per-bucket
/// cache-resident tables — the radix/partition-first program on the
/// machinery that already exists (flush-at-cap + counting-sorted runs +
/// bucket-claim combine).
///
/// Engagement: WORD-KEYED shapes at DOP>1 only.
///   * Canonical (Intern-bearing) shapes keep the budget cap: their flush
///     materializes canonical bytes and resets the intern table (wide-
///     vocabulary bounding) — the canon-sink lane owns that surface.
///   * DOP1 keeps the budget cap: the single-Local pass-through/adopt fast
///     path requires zero flushed runs (the dop1-tax ledger), and one Local
///     has no duplicate-group tax to convert.
/// Low-NDV shapes are unaffected by construction (tables below the bound
/// never flush; the bound only engages where the table would outgrow it).
/// `PGRUST_RUNTIME_AGG_LOCALITY_CAP`: 0 = off (budget cap exactly), N =
/// entry bound override. Default = the wave-1 ladder knee (see
/// notes/q36-radix-lane.md).
fn sink_locality_cap() -> LocalityCap {
    static N: OnceLock<LocalityCap> = OnceLock::new();
    crate::once_val(&N, || {
        match std::env::var("PGRUST_RUNTIME_AGG_LOCALITY_CAP")
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
        {
            Some(0) => LocalityCap::Off,
            Some(c) => LocalityCap::Fixed(c.max(1024)),
            None => LocalityCap::Default,
        }
    })
}

/// The locality-cap env verdict: `Fixed` = an explicit
/// `PGRUST_RUNTIME_AGG_LOCALITY_CAP=N` (the pure A/B channel — the
/// NDV-adaptive rule never rewrites it), `Default` = unset (the adaptive
/// rule applies), `Off` = the 0 kill switch.
#[derive(Clone, Copy)]
enum LocalityCap {
    Off,
    Default,
    Fixed(u32),
}

/// NDV-ADAPTIVE locality cap (distinct-sidecar-cap lane — the radix-cap
/// close-out's flagged refinement): the 64K-vs-1M adjudication (repeat pair
/// -3aef/-2af4, reproduced against r2 exactly) showed a REAL per-query
/// split, not pod drift — the reduced-key shape (9.76M est groups) runs ~10% faster at 1M,
/// the dict-key shape (17.6M) ~16% faster at 64K, the two-int-key (~1e8) cap-flat 256K-1M. Mechanism
/// reading: at the higher NDV the worker table re-fills quickly at ANY cap,
/// so the smallest (L2-resident) table wins outright; in the mid band the
/// duplicate-absorption of a larger (L3-resident) table repays its slower
/// probes. Rule (defended by the ladder in notes/distinct-sidecar-cap.md):
///   est >= 12M            -> 64K (SINK_LOCALITY_CAP_DEFAULT; today's cap)
///   2M <= est < 12M       -> 1M  (SINK_LOCALITY_CAP_WIDE; the mid-NDV band)
///   est < 2M              -> 64K (cap barely binds; tranche-proven regime)
/// `PGRUST_RUNTIME_AGG_LOCALITY_NDV=0` kills adaptivity (fixed 64K default,
/// train-19 behavior exactly); an explicit LOCALITY_CAP=N is authoritative.
fn agg_locality_ndv_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_RUNTIME_AGG_LOCALITY_NDV").as_deref(), Ok("0") | Ok("off"))
    })
}

/// The mid-NDV-band locality cap — see [`agg_locality_ndv_enabled`].
const SINK_LOCALITY_CAP_WIDE: u32 = 1 << 20;

/// `PGRUST_RUNTIME_AGG_LOCALITY_CANON` (default ON since train-20): extend
/// the locality cap to CANONICAL (Intern-bearing) Mk shapes (the two-key int+text class:
/// 17M UserID×SearchPhrase groups; the CaseDict residual). The canonical
/// flush/SEAL/combine machinery exists since train-17 text-kernels (canon
/// shapes already flush at the BUDGET cap); the train-19 exclusion was the
/// car3/car4 seam, not a mechanism gap. Default flipped ON at the train-20
/// merge: the lane's ladder measured the two-key shape -28% (byte-MATCH) and the
/// canon-family guard pair (the six canon-key shapes @100M mt16, jobs
/// -1784092204-0df8 vs -1784092208-06ce) adjudicated ZERO regressions with
/// the ts-extract shape -30%. `=0`/`off` restores the exclusion.
fn agg_locality_canon_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(
            std::env::var("PGRUST_RUNTIME_AGG_LOCALITY_CANON").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// CAP-BAND v2 (GL-RADIX-2, the D2-completion increment): est+collapse
/// adaptive locality bands, replacing the v1 NDV-only rule where armed.
/// Two GL-RADIX-1-measured defects of the v1 curve on the plain word-key
/// class:
///   * the (64K, 2M) est band paid 1.6-3.4x vs NO cap at high collapse
///     ratios (100M-scale α = rows/groups ≥ 100): the 64K table holds a
///     few percent of the key space, so its fill-window statistic never
///     sees the global fold potential — an UNCAPPED table folds every
///     repeat in place (one probe per row) while the capped build pays
///     probe + flush + a full combine re-pass over ~every row. The α-gate
///     cannot adjudicate this (it only demotes, is anchored off at
///     dop ≤ 16, and its window-α is blind to out-of-table repeats), so
///     the band rule reads the PLANNER's α estimate instead: est under
///     the WIDE floor with α_est at/above [`agg_capband_alpha_min`]
///     drops the locality bound entirely (the budget cap still applies;
///     the spill/pressure machinery still bounds estimate misses). Low-α
///     small-scale points (α ≈ 10 at 10M rows measured the cap WINNING
///     ~10%) keep the incumbent 64K — the α_min default (16) splits the
///     measured cells.
///   * the [2M, 12M) WIDE band's 1M constant lost to 256K by 7-10% at the
///     1e7-class point on the same-pod hybrid head-to-head (GL-RADIX-1
///     decision job; with seal-flush ON the residual cap sensitivity is
///     small, but 256K is the measured band winner and its per-worker
///     table is SLC-friendlier at width) — v2 moves the band constant to
///     [`SINK_LOCALITY_CAP_MID`].
/// est ≥ 12M keeps 64K verbatim (the q16-class banked evidence).
/// DEFAULT OFF — armed iff `PGRUST_RUNTIME_AGG_CAP_BAND_V2` is `1`/`on`;
/// the GL-RADIX-2 ladder owns the flip. An explicit LOCALITY_CAP=N stays
/// authoritative over both curves; LOCALITY_NDV=0 keeps its meaning
/// (v1 flat-64K) when v2 is unarmed.
fn agg_capband_v2_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        matches!(std::env::var("PGRUST_RUNTIME_AGG_CAP_BAND_V2").as_deref(), Ok("1") | Ok("on"))
    })
}

/// v2 collapse-ratio floor for dropping the cap under the WIDE band
/// (`PGRUST_RUNTIME_AGG_CAP_BAND_ALPHA`, default 16): α_est =
/// plan rows / plan groups. 10M-scale cells measured the cap winning at
/// α = 10 and losing 1.6-3.4x at α ≥ 100; 16 splits the evidence with
/// margin on the regression side (a wrong uncap is bounded by the budget
/// cap + pressure/spill machinery; a wrong cap is the multi-x misroute).
fn agg_capband_alpha_min() -> u64 {
    static N: OnceLock<u64> = OnceLock::new();
    crate::once_val(&N, || {
        std::env::var("PGRUST_RUNTIME_AGG_CAP_BAND_ALPHA")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|&a| a >= 1)
            .unwrap_or(16)
    })
}

/// v2 mid-band ([2M, 12M) est) locality cap — see [`agg_capband_v2_enabled`].
const SINK_LOCALITY_CAP_MID: u32 = 1 << 18;

/// Resolve the engaged locality bound for this shape's plan-estimated group
/// count (+ the scan's estimated rows — the v2 curve's collapse term).
/// `None` = no locality bound (kill switch / v2 high-α band).
fn sink_locality_cap_for(est_groups: u64, est_rows: u64) -> Option<u32> {
    match sink_locality_cap() {
        LocalityCap::Off => None,
        LocalityCap::Fixed(c) => Some(c),
        LocalityCap::Default => {
            if agg_capband_v2_enabled() {
                let alpha_est = est_rows / est_groups.max(1);
                return if est_groups >= 12_000_000 {
                    Some(SINK_LOCALITY_CAP_DEFAULT)
                } else if est_groups >= 2_000_000 {
                    Some(SINK_LOCALITY_CAP_MID)
                } else if alpha_est >= agg_capband_alpha_min() {
                    // High-collapse sub-WIDE band: uncapped (budget bound
                    // only) — the GL-RADIX-1 side-finding cells.
                    None
                } else {
                    Some(SINK_LOCALITY_CAP_DEFAULT)
                };
            }
            if agg_locality_ndv_enabled() && (2_000_000..12_000_000).contains(&est_groups) {
                Some(SINK_LOCALITY_CAP_WIDE)
            } else {
                Some(SINK_LOCALITY_CAP_DEFAULT)
            }
        }
    }
}

/// SEAL-FLUSH (radix seal) arm — GL-RADIX-1, the groupby-high port's
/// runtime-side increment. On the admitted band the SEAL pass flushes each
/// Local's remainder table into ONE final bucket-contiguous SinkRun (the
/// cap-flush bodies verbatim, [`::nodeagg::sink::SinkTableHandle::flush_remainder`])
/// instead of building the SEAL index. Mechanism: at high NDV the combine's
/// remainder face random-accesses up to DOP × cap entries through the SEAL
/// index across the Locals' live tables (dependent DRAM loads — the same
/// memory-LATENCY class the locality cap bounded on the ACCEPT side), while
/// every other face already streams bucket-contiguous runs; the incumbent
/// parallel-finalize lane's raw exchange has NO such face — its final
/// install radix-partitions contiguously. Seal-flush closes that structural
/// gap: after it, the combine input is 100% sequential runs. Byte identity
/// rides the ratified flush-cadence law (runs merge first-seen; the
/// remainder run lands LAST — the SEAL face's own visit position) and is
/// unit-pinned (seal_flush_run_matches_remainder_view). DEFAULT ON since
/// the GL-RADIX-1 witnessed ladder (scratchpad/night/GL-RADIX-1-letter.md,
/// 2026-07-21: within-pod win vs the incumbent combine on the admitted
/// band at every scale/DOP/cap measured, byte-equal everywhere, zero
/// off-band engagements incl. a 43q ON-arm census of zero — the flip is
/// behavior-inert at product defaults while the m5 groupby_high hold
/// routes the band legacy, and positions the D2 recipe).
/// `PGRUST_RUNTIME_AGG_SEALFLUSH=0`/`off` is the kill switch.
fn agg_sealflush_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_RUNTIME_AGG_SEALFLUSH").as_deref(), Ok("0") | Ok("off"))
    })
}

/// Seal-flush admission floor in plan-estimated groups
/// (`PGRUST_RUNTIME_AGG_SEALFLUSH_FLOOR`, default 4e6 — the m5
/// groupby-high hold's own class boundary): below it the remainder is a
/// minority combine face over small tables and the SEAL index is already
/// cheap; the arm targets exactly the band the hold keeps legacy. Low-card
/// shapes are structurally untouched (admission never fires; the sink code
/// path is the incumbent's, branch-for-branch).
fn agg_sealflush_floor() -> u64 {
    static N: OnceLock<u64> = OnceLock::new();
    crate::once_val(&N, || {
        std::env::var("PGRUST_RUNTIME_AGG_SEALFLUSH_FLOOR")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|&f| f > 0)
            .unwrap_or(4_000_000)
    })
}

/// `PGRUST_RUNTIME_AGG_SCATTER` kill switch (GL-RADIX-3, DEFAULT OFF —
/// armed iff exactly `1`/`on`): the fold-bypass SCATTER ACCEPT
/// ([`AggSink::scatter`]). Off = the incumbent drain, branch-for-branch.
fn agg_scatter_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        matches!(std::env::var("PGRUST_RUNTIME_AGG_SCATTER").as_deref(), Ok("1") | Ok("on"))
    })
}

/// Scatter admission α ceiling (`PGRUST_RUNTIME_AGG_SCATTER_ALPHA`, default
/// 2): admit iff α_est = est_rows / est_groups ≤ it — the band where accept
/// rows are dominated by probe misses and the table folds ~nothing. This is
/// PLANNER-EST admission (the cap-band v2 α term's discipline), disjoint
/// from the cachebudget lane's runtime α-gate controller.
fn agg_scatter_alpha() -> u64 {
    static N: OnceLock<u64> = OnceLock::new();
    crate::once_val(&N, || {
        std::env::var("PGRUST_RUNTIME_AGG_SCATTER_ALPHA")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|&a| a > 0)
            .unwrap_or(2)
    })
}

/// Scatter admission floor in plan-estimated groups
/// (`PGRUST_RUNTIME_AGG_SCATTER_FLOOR`, default 4e6 — the seal-flush /
/// groupby-high class boundary): below it the worker tables are
/// cache-resident and the fold path already wins; the arm targets exactly
/// the high-NDV band where the α≈1 accept loses to the exchange hybrid.
fn agg_scatter_floor() -> u64 {
    static N: OnceLock<u64> = OnceLock::new();
    crate::once_val(&N, || {
        std::env::var("PGRUST_RUNTIME_AGG_SCATTER_FLOOR")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|&f| f > 0)
            .unwrap_or(4_000_000)
    })
}

/// GL-VECACCEPT-2 K2 arm — DEFAULT rides the unified lane posture
/// ([`super::vecaccept_lane_enabled`], default ON; flip basis: the 100M
/// grid never loses a cell, −4..−11% vs the incumbent drain everywhere,
/// and flips the 1e6-group parity band to runtime-won — letter §4;
/// α≈1 is NEUTRAL vs the incumbent, the carve stands). t35 flipped-kill
/// granularity: `PGRUST_RUNTIME_AGG_VECACCEPT=0|off` kills the whole
/// lane; `PGRUST_RUNTIME_AGG_VECACCEPT_K2=0|off` kills the K2 side alone
/// (adjudication). Killed = the incumbent staged drain, branch-for-branch.
fn agg_vecaccept_k2_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        super::vecaccept_lane_enabled()
            && !matches!(
                std::env::var("PGRUST_RUNTIME_AGG_VECACCEPT_K2").as_deref(),
                Ok("0") | Ok("off")
            )
    })
}

/// GL-VECACCEPT-2 chunk width: probe/fold batch length over the granule
/// lanes. 4x the staged window (256 rows) — fewer per-batch overheads —
/// while the loop-head flush/pressure laws run per chunk, so the cap
/// overshoot is bounded at one chunk of entries (the staged law's own
/// one-batch-overshoot family, coarser by 4x; the cap is a locality bound
/// in the tens-of-thousands, so ≤1024 extra entries is inside its noise).
const VEC_CHUNK: usize = 1024;

/// Wave-1 r2 ladder verdict (reduced-key/dict-key @100M, notes/q36-radix-lane.md):
/// monotone improvement control→64K (reduced-key 0.378→0.234 = 2.30x→1.40x ref-mt16;
/// dict-key 0.468→0.270), knee flattening 256K→64K, guards (selective-qual, two-key-sort) flat-or-
/// better. 64K = the historical exchange-class cap (tranche-proven).
/// 32K/16K extension arms recorded in the lane note; env overrides.
const SINK_LOCALITY_CAP_DEFAULT: u32 = 1 << 16;

/// The ENGAGED sink cap: the budget-derived cap ([`sink_cap_for`]) bounded
/// by the locality cap on word-keyed DOP>1 engagements. This is the single
/// authority for the leader admissibility gate AND the sink's constructed
/// `cap` (workers arm off `sink.cap`) — the F1 invariant: leader and worker
/// verdicts must be computed from the SAME cap. The early cap-aware Mk
/// admission probes keep the plain budget cap (the shape — and with it
/// word-keyedness — is not yet known there); the divergence is one-sided:
/// a larger probe cap can only manufacture a REFUSAL at the probe's
/// estimate gate (fail-closed to serial), never an engagement the final
/// gate would reject, and word-keyed spill-armed shapes vacate that gate
/// entirely.
/// agg192-contention (48xl window-B verdict 2): the locality cap was
/// DOP-INDEPENDENT — ~64K entries ≈ 3.6-3.9MB per worker table, which is
/// locality-correct for ONE worker but aggregates to ~690MB of live
/// random-probed table bytes at 191 workers, spilling the shared SLC
/// between 64 and 128 workers. The measured signature: instructions and
/// LLC-miss COUNTS flat, stall_backend_mem x4-5, system-wide miss RATE
/// saturating at ~1.7-1.9 G/s and then COLLAPSING at 191 (ts-extract 1.24 G/s <
/// its own 128-dop rate — queueing overload = the 191<128 regression).
/// Fix: above the 16-worker anchor, scale the locality bound so the
/// AGGREGATE stays at the anchor's working set (cap x 16 / dop), floored
/// (default 16K entries ≈ 0.9MB — near-L2-resident per worker at any DOP)
/// so flush cadence and the per-flush dict/intern-cache resets stay sane.
/// dop <= 16 is UNCHANGED by construction (the mt16 official channel is
/// byte-flat). `PGRUST_RUNTIME_AGG_DOPCAP=0` restores the DOP-independent
/// bound; `PGRUST_RUNTIME_AGG_DOPCAP_FLOOR` is the ladder A/B knob.
fn agg_dopcap_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_RUNTIME_AGG_DOPCAP").as_deref(), Ok("0") | Ok("off"))
    })
}

/// Per-worker floor for the DOP-scaled locality bound (see
/// [`agg_dopcap_enabled`]) — BYTE-denominated (cache-budget lit-review
/// refinement 2, Schuhknecht VLDB15 class: the flush scatter's output
/// shares the private cache with the table, so the floor must be a byte
/// budget, not an entry count — an entry floor silently overflows L2 on
/// state-heavy shapes). Default 1MB of table bytes at the sink's own
/// entry estimate (16+8+state+16): ≈18K entries on the ts-extract-class 56B
/// entry, proportionally fewer on heavy states — table + flush output +
/// scan batch fit a Neoverse-V2 core's private 2MB L2 with headroom.
/// `PGRUST_RUNTIME_AGG_DOPCAP_FLOOR` overrides the BYTE budget (A/B knob).
fn agg_dopcap_floor_bytes() -> u64 {
    static N: OnceLock<u64> = OnceLock::new();
    crate::once_val(&N, || {
        std::env::var("PGRUST_RUNTIME_AGG_DOPCAP_FLOOR")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|f| *f >= 64 * 1024)
            .unwrap_or(1 << 20)
    })
}

/// The DOP anchor: at or below this width the locality bound is exactly
/// the calibrated per-worker cap (the radix-ladder 64K / mid-NDV 1M bands);
/// above it the AGGREGATE working set is held at the anchor's.
const DOPCAP_ANCHOR: u32 = 16;

/// Budget-size ladder knob (`PGRUST_RUNTIME_AGG_DOPCAP_ANCHOR`, default
/// [`DOPCAP_ANCHOR`]): scales the AGGREGATE budget for the calibration
/// ladder Michael chartered (8 = 0.5x, 32 = 2x the anchor working set) —
/// the budget-response curve adjudicates whether the anchor sits at the
/// knee per width, and a 2x-helps-at-96-but-hurts-at-191 shape is the
/// per-socket-split signature. CALIBRATION/A-B channel on the stride/
/// decay env precedent, not product surface — the ratified anchor stays
/// the compiled constant. Clamped to [2, 191] so a typo cannot disable
/// the budget outright (the kill switch is DOPCAP=0). The dop<=anchor
/// early-out uses the SAME value, so a widened anchor also widens the
/// unscaled band (exactly the 2x-aggregate semantics).
fn agg_dopcap_anchor() -> u32 {
    static N: OnceLock<u32> = OnceLock::new();
    crate::once_val(&N, || {
        std::env::var("PGRUST_RUNTIME_AGG_DOPCAP_ANCHOR")
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .map(|a| a.clamp(2, 191))
            .unwrap_or(DOPCAP_ANCHOR)
    })
}

// ---------------------------------------------------------------------------
// α-gate controller (cachebudget lane — Müller et al., SIGMOD 2015 "Cache-
// Efficient Aggregation: Hashing Is Sorting", the ADAPTIVE controller,
// adapted to the sink's bounded-Local flush discipline).
//
// The DOP-aware budget (agg192-contention) sizes every worker's locality
// table as a share of the shared cache — but a share is only WORTH holding
// when the table actually collapses rows (repeat keys fold in place). When
// a worker's key stream is near-unique, the table is a WRITE BUFFER: every
// row inserts, nothing folds, and its cache residency buys nothing while
// still charging the aggregate SLC budget the other workers' tables need
// (the window-B root cause, at any width). Müller's runtime signal
// adjudicates this per worker with zero synchronization: at table FILL
// compute the collapse ratio α = rows-absorbed / distinct-entries-flushed.
// α below α₀ ⇒ demote that worker's flush threshold to the byte-denominated
// L2 floor (agg192's — the flush scatter shares the private cache, so the
// floor is bytes at the shape's entry estimate) and flush through; α at or
// above α₀ ⇒ keep the budget share. Re-probe after ~10× the full table's
// row volume (Müller's hysteresis: one full-cap fill window re-adjudicates
// a phase change; a wrong re-probe costs footprint only — the retained
// capacity means no realloc).
//
// Byte-identity law: the effective cap changes only WHEN flushes happen —
// flush cadence is semantics-free (the dopcap precedent: runs merge
// first-seen, W≡F≡D and selection totality are cadence-independent) — and
// the controller is per-Local thread-owned state with ZERO new shared
// writes (the condcache lesson). Demotion requires a first full-cap FILL,
// so engagements that never fill (low NDV, adopt/pass-through shapes) are
// byte-and-time identical by construction. Freeze-armed (LIMIT-k) shapes
// are structurally excluded at engagement (bounded output, nothing to buy).
// Kill switch: PGRUST_RUNTIME_AGG_ALPHAGATE=0.
// ---------------------------------------------------------------------------

fn agg_alphagate_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_RUNTIME_AGG_ALPHAGATE").as_deref(), Ok("0") | Ok("off"))
    })
}

/// α₀ ×100 (`PGRUST_RUNTIME_AGG_ALPHA0`, default 2.0): the collapse ratio
/// below which a filled table is adjudicated a write buffer. 2.0 = each
/// entry absorbed under two rows across the fill window — the table at most
/// halved its flush volume, too little to earn a shared-cache share (the
/// demote trades ≤2× sequential flush bytes for the aggregate footprint).
/// The 48xl calibration ladder sweeps this knob (composes with agg192's
/// DOPCAP_ANCHOR ladder).
fn agg_alpha0_x100() -> u64 {
    static N: OnceLock<u64> = OnceLock::new();
    crate::once_val(&N, || {
        std::env::var("PGRUST_RUNTIME_AGG_ALPHA0")
            .ok()
            .and_then(|v| v.trim().parse::<f64>().ok())
            .filter(|a| a.is_finite() && *a >= 1.0 && *a <= 100.0)
            .map(|a| (a * 100.0) as u64)
            .unwrap_or(200)
    })
}

/// Re-probe row multiple (`PGRUST_RUNTIME_AGG_ALPHA_REPROBE`, default 10 —
/// Müller's "periodically re-probe after processing ~10× the cache volume
/// of input"): a demoted Local restores the full threshold for one probe
/// window after `mult × full-cap` rows. 0 = never re-probe (a demote is
/// then sticky until a floor window shows collapse).
fn agg_alpha_reprobe_mult() -> u64 {
    static N: OnceLock<u64> = OnceLock::new();
    crate::once_val(&N, || {
        std::env::var("PGRUST_RUNTIME_AGG_ALPHA_REPROBE")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(10)
    })
}

/// The α-gate's demoted flush threshold for this engagement, resolved ONCE
/// at construction: `Some(floor_entries)` arms the controller. Reuses
/// agg192's byte-denominated L2 floor (`PGRUST_RUNTIME_AGG_DOPCAP_FLOOR`,
/// default 1MB at the shape's own 16+8+state+16 entry estimate) — the same
/// "table + flush scatter fit the private L2" constant, deliberately shared
/// so the calibration ladder moves both together. None (controller off):
/// kill switch; dop at or under the DOPCAP anchor (see below);
/// fixed-cap A/B override (PGRUST_RUNTIME_AGG_CAP stays authoritative, the
/// locality-bound law); freeze-armed shapes (caller gates); or a floor at/
/// above the engaged cap (nothing to demote to).
///
/// ANCHOR GATE (this lane's mt16 100M ON/OFF pair verdict): at or under
/// the anchor width the aggregate working set already sits at the ratified
/// SLC constant — demoting write-buffer tables there paid flush-EVENT
/// overhead (dict-code cache invalidation + run setup at ~3.5× the
/// cadence: high-card two-int-key hot +9%, ts-extract +2.5%, suite otherwise flat 1.0026) and
/// bought no residency anyone needed at 16 workers. Above the anchor is
/// the window-B regime the controller is FOR (per-worker shares shrink
/// below adjudication value; dopcap scales caps toward the floor and the
/// α-gate decides which workers deserve their share). Sharing
/// [`agg_dopcap_anchor`] means a window-D re-anchoring moves the α-gate's
/// engagement band with it — one calibration channel, one knob. The
/// official mt16 channel is therefore byte-AND-time identical by
/// construction (the dopcap boarding precedent).
fn alpha_gate_floor(state_bytes: usize, cap: u32, dop: i32) -> Option<u32> {
    if !agg_alphagate_enabled()
        || dop <= agg_dopcap_anchor() as i32
        || sink_cap_override().is_some()
    {
        return None;
    }
    let entry = 16u64 + 8 + state_bytes as u64 + 16;
    let floor =
        (agg_dopcap_floor_bytes() / entry.max(1)).clamp(1 << 12, u32::MAX as u64) as u32;
    (floor < cap).then_some(floor)
}

// ---------------------------------------------------------------------------
// Per-socket SHARED aggregate table EXPERIMENT (cachebudget D2, hard
// default-OFF — docs/design/shared-agg-table-experiment.md §4 and the
// nodeagg::sink::SharedCountTable section doc). The mid-NDV architecture
// decider for the next 48xl window; NOT a product default candidate until
// the measurement spec's verdicts land (and the emit-order law is settled).
// ---------------------------------------------------------------------------

/// `PGRUST_RUNTIME_AGG_SHARED_TABLE=1` arms the experiment (default OFF).
fn agg_shared_table_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        matches!(std::env::var("PGRUST_RUNTIME_AGG_SHARED_TABLE").as_deref(), Ok("1"))
    })
}

/// Engagement band upper bound in estimated groups
/// (`PGRUST_RUNTIME_AGG_SHARED_MAX`, default 1M ≈ a socket-SLC share at the
/// 16B count-entry: 36MB × 0.5 / 16B — the design note's band arithmetic).
/// The band's LOWER bound is the engaged sink cap itself (below it the
/// per-worker Locals never fill and today's path is already optimal).
fn agg_shared_table_max_groups() -> u64 {
    static N: OnceLock<u64> = OnceLock::new();
    crate::once_val(&N, || {
        std::env::var("PGRUST_RUNTIME_AGG_SHARED_MAX")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(1 << 20)
    })
}

/// Band lower-bound OVERRIDE (`PGRUST_RUNTIME_AGG_SHARED_MIN`, experiment/
/// e2e forcing channel): None = the engaged sink cap (the default band
/// floor — below the cap Locals never fill and there is nothing to
/// absorb). The e2e forced legs set 1 so engagement does not hinge on the
/// corpus's planner group estimates.
fn agg_shared_table_min_groups() -> Option<u64> {
    static N: OnceLock<Option<u64>> = OnceLock::new();
    crate::once_val(&N, || {
        std::env::var("PGRUST_RUNTIME_AGG_SHARED_MIN")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
    })
}

/// The experiment face: one shared count table per socket half (the
/// numa-combine socket map; non-Linux / single-node folds to table 0).
/// Prototype scope (recorded in the design note): single-int-word K2 keys,
/// COUNT-only states.
struct SharedAggFace {
    tables: [::nodeagg::sink::SharedCountTable; 2],
    /// Observability: runs absorbed / runs kept on the incumbent path
    /// after a close (the SHAREDAGG marker line).
    merged_runs: AtomicU64,
    kept_runs: AtomicU64,
}

impl SharedAggFace {
    fn new(est_groups: u64) -> SharedAggFace {
        // Each socket table can in the worst case hold EVERY group (groups
        // are not socket-partitioned — both halves may touch a key), so
        // both size at the estimate; 25% headroom absorbs estimate error
        // before the close-fallback fires.
        let cap = (est_groups + est_groups / 4).max(64) as usize;
        SharedAggFace {
            tables: [
                ::nodeagg::sink::SharedCountTable::new(cap),
                ::nodeagg::sink::SharedCountTable::new(cap),
            ],
            merged_runs: AtomicU64::new(0),
            kept_runs: AtomicU64::new(0),
        }
    }

    /// Try to absorb a flushed run into the calling worker's socket table.
    /// `true` = absorbed (drop the run); `false` = keep it on the incumbent
    /// runs path (closed face / overflow — the spill fallback).
    fn absorb(&self, run: &::nodeagg::sink::SinkRun) -> bool {
        let half = numa_current_half().unwrap_or(0).min(1);
        if self.tables[half].merge_run(run) {
            self.merged_runs.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            self.kept_runs.fetch_add(1, Ordering::Relaxed);
            false
        }
    }

    /// SEAL-time drain (single-threaded by the seal contract): inject the
    /// socket tables' contents as runs on the first sealed Local — the
    /// combine consumes them exactly like flushed runs.
    fn drain_into(&self, locals: &mut [AggSinkLocal]) {
        let Some(l0) = locals.first_mut() else { return };
        for t in &self.tables {
            if let Some(run) = t.drain_to_run() {
                l0.run_bytes += run.bytes();
                l0.runs.push(run);
            }
        }
        if agg_markers_on() {
            eprintln!(
                "MORSEL|SHAREDAGG|members={},{}|closed={},{}|merged_runs={}|kept_runs={}",
                self.tables[0].reserved(),
                self.tables[1].reserved(),
                u8::from(self.tables[0].is_closed()),
                u8::from(self.tables[1].is_closed()),
                self.merged_runs.load(Ordering::Relaxed),
                self.kept_runs.load(Ordering::Relaxed),
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn sink_cap_engaged(
    state_bytes: usize,
    budget: usize,
    ngroups_limit: u64,
    dop: i32,
    word_keyed: bool,
    est_groups: u64,
    est_rows: u64,
) -> u32 {
    let base = sink_cap_for(state_bytes, budget, ngroups_limit);
    // An explicit fixed-cap override (PGRUST_RUNTIME_AGG_CAP) is the A/B
    // channel and stays authoritative — the locality bound never rewrites it.
    if sink_cap_override().is_some() || dop <= 1 || !word_keyed {
        return base;
    }
    match sink_locality_cap_for(est_groups, est_rows) {
        Some(l) => {
            let mut l = l;
            let anchor = agg_dopcap_anchor();
            if agg_dopcap_enabled() && dop as u32 > anchor {
                let scaled =
                    ((l as u64 * anchor as u64) / (dop as u64).max(1)) as u32;
                // Byte-denominated floor at THIS shape's entry estimate
                // (the same 16+8+state+16 arithmetic as sink_cap_for).
                let entry = 16u64 + 8 + state_bytes as u64 + 16;
                let floor = (agg_dopcap_floor_bytes() / entry.max(1))
                    .clamp(1 << 12, u32::MAX as u64) as u32;
                l = scaled.max(floor).min(l);
            }
            base.min(l)
        }
        None => base,
    }
}

/// Sink flush cap (worker table bound, entries) — BUDGET-DERIVED (dop1-tax
/// inc-3b). The fixed 64K exchange-class cap forced ~17 flush cycles on
/// the reduced-key shape @10M at DOP1 (~1.1M groups), keeping the single-Local pass-through
/// permanently dormant and re-inserting every group at combine. The cap is
/// now the entry count whose compact-table estimate fills HALF the
/// per-Local budget (the compact spill gate's own arithmetic:
/// 16+8+state+16 bytes/entry), floored at the 64K class — at default
/// work_mem it degenerates to ~the old cap (tranche behavior preserved);
/// under the matched-memory protocol (1GB) a reduced-key-class Local holds all its
/// groups, never flushes, and the pass-through fires. Width-INDEPENDENT:
/// each Local is budget-bounded exactly as before (runs held the same
/// bytes the larger live table now holds — the R3 envelope arithmetic is
/// unchanged, and the seal/accept budget checks still refuse crossings).
/// PGRUST_RUNTIME_AGG_CAP overrides to a fixed cap (the A/B arm; 65536 =
/// the old behavior).
fn sink_cap_for(state_bytes: usize, budget: usize, ngroups_limit: u64) -> u32 {
    if let Some(c) = sink_cap_override() {
        return c;
    }
    let entry = 16u64 + 8 + state_bytes as u64 + 16;
    // BOTH admission bounds (compact_admission / agg_hash_compact_sink_
    // admissible): capped-numgroups must satisfy est_bytes <= budget/2 AND
    // numgroups <= ngroups_limit/2 — a cap above either manufactures
    // refusals the fixed 64K cap never hit (round-3 battery: count-only
    // high-NDV shapes flipped admit->refuse because the mem-derived cap
    // 74898 crossed ngroups_limit/2 ~73.7k at default work_mem). The 64K
    // floor keeps heavy-state shapes exactly at the old cap (their old
    // verdict, admit or refuse, is reproduced verbatim).
    let mem_bound = (budget as u64 / 2) / entry.max(1);
    // TRIP GUARD (dop1-tax2): the drain's flush-if-due runs BEFORE each
    // batch and a batch can insert up to a full staged batch of NEW groups,
    // so the cap must sit a batch below the runtime backstop's ngroups trip
    // (hash_ngroups_limit/2) or the flush never fires first. The old 64K
    // floor could RAISE the cap ABOVE the trip on small-limit plans
    // (planner underestimates the Mk car now admits) — the worker backstop
    // then errored mid-build (battery legs 2d/2e parity FAIL @ 5451ddc9d:
    // "worker compact table crossed the hash memory limits under the sink
    // cap" on the 389k-group two-key corpus query). The floor is kept for
    // admission-verdict stability but NEVER above the trip.
    let trip = (ngroups_limit / 2)
        .saturating_sub(2 * ::exectuples::SOA_MAX_ROWS as u64)
        .max(1);
    let cap = mem_bound.min(trip);
    cap.clamp((1 << 16).min(trip), u32::MAX as u64 / 2) as u32
}

/// M3.5 spill arm kill switch: ON by default when the sink engages
/// (refusal→engagement is the charter); `PGRUST_RUNTIME_AGG_SPILL=0`
/// restores the phase-1 budget refusal exactly.
fn agg_spill_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || std::env::var("PGRUST_RUNTIME_AGG_SPILL").as_deref() != Ok("0"))
}

/// EPOCH SIZING A/B arm (spill-envelopes lane): `1` restores the
/// spill-on-every-pressure-trip behavior (one ~(half-limit − aggctx) run
/// per epoch); default OFF = pressure-flush runs accumulate to the R3
/// budget crossing before an epoch is written (fewer, bigger epochs).
fn agg_spill_eager() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || std::env::var("PGRUST_RUNTIME_AGG_SPILL_EAGER").as_deref() == Ok("1"))
}

/// `PGRUST_RUNTIME_AGG_TEXT` kill switch (default ON): the C2 text-key
/// admission classes — Intern (text) components merged on canonical raw
/// bytes, and Numeric components under the demote→refusal discipline. Off,
/// those shapes refuse exactly as before the car (attribution channel).
fn runtime_agg_text_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_RUNTIME_AGG_TEXT").as_deref(), Ok("0") | Ok("off"))
    })
}

/// `PGRUST_RUNTIME_AGG_TEXT2` kill switch (default ON): TWO-text canonical
/// shapes (the CaseDict class — canon-sink car 1: length-prefixed
/// canonical tails). Off, two-Intern shapes refuse the sink exactly as
/// before the car (serial CaseDict arm unchanged — the attribution channel).
fn runtime_agg_text2_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_RUNTIME_AGG_TEXT2").as_deref(), Ok("0") | Ok("off"))
    })
}

/// The sink's packed-shape component gates (leader admission): non-nullable
/// image; at most TWO Intern (text) components — ONE decodes as the raw
/// canonical tail (the historical image), TWO ride the length-prefixed
/// multi-tail encoding (canon-sink car 1, `PGRUST_RUNTIME_AGG_TEXT2`); any
/// non-Int component class (Intern/Numeric) rides the text-car kill switch.
fn mk_shape_sink_ok(shape: &::nodeagg::MkShape) -> bool {
    if shape.nullable {
        return false;
    }
    let n_intern = shape.n_intern();
    if n_intern > 2 || (n_intern == 2 && !runtime_agg_text2_enabled()) {
        return false;
    }
    let all_int = shape
        .comps
        .iter()
        .all(|c| matches!(c.kind, ::nodeagg::MkCompKind::Int { .. }));
    all_int || runtime_agg_text_enabled()
}

/// `PGRUST_RUNTIME_AGG_FREEZE` kill switch (default ON): the LIMIT-k-no-
/// ORDER group-admission freeze (band-2a freeze class). Off, bare-Limit bounds
/// are ignored and those engagements run the plain full drain exactly as
/// before the car.
fn agg_freeze_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_RUNTIME_AGG_FREEZE").as_deref(), Ok("0") | Ok("off"))
    })
}

/// Engagement floor (granules) — below it helper launches are pure overhead.
/// GL-STRMM-2 group-estimate ceiling, executor half — SAME env spelling and
/// default as the m5 probe's `strminmax_max_groups` (planner m5_suppress.rs;
/// the knob-coherence law: both seams move together or a suppressed shape
/// diverges from its engagement). Doc + calibration provenance live on the
/// probe half. Env: `PGRUST_LANE_V2_AGG_STRMINMAX_MAX_GROUPS` (> 0).
fn strminmax_max_groups() -> f64 {
    static CEIL: OnceLock<f64> = OnceLock::new();
    *CEIL.get_or_init(|| {
        std::env::var("PGRUST_LANE_V2_AGG_STRMINMAX_MAX_GROUPS")
            .ok()
            .and_then(|v| v.trim().parse::<f64>().ok())
            .filter(|v| *v > 0.0)
            .unwrap_or(30_000.0)
    })
}

fn min_granules() -> u64 {
    static N: OnceLock<u64> = OnceLock::new();
    crate::once_val(&N, || {
        std::env::var("PGRUST_RUNTIME_AGG_MIN_GRANULES")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(64)
    })
}

/// Find the Node of `agg.plan` inside the leader's plan tree (worker pstmts
/// root at the Agg subtree; the Agg need not be the leader plan's root).
pub(super) fn find_agg_node<'mcx>(
    root: Node<'mcx>,
    target: *const ::types_nodes::plannodes::Agg<'mcx>,
) -> Option<Node<'mcx>> {
    if let Some(a) = root.as_agg() {
        if core::ptr::eq(a, target) {
            return Some(root);
        }
    }
    let plan = root.as_plan()?;
    if let Some(l) = plan.lefttree {
        if let Some(n) = find_agg_node(l, target) {
            return Some(n);
        }
    }
    if let Some(r) = plan.righttree {
        if let Some(n) = find_agg_node(r, target) {
            return Some(n);
        }
    }
    None
}

/// The runtime aggregation-sink arm. `false` = not engaged (caller falls
/// through to the serial build, byte-identically — nothing was consumed).
/// `true` = the published parallel result was adopted; every retrieve path
/// drains it through `agg_hash_retrieve`'s sink branch.
pub(super) fn try_engage_hashagg_runtime<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    xk: Option<&super::ExprKeyState>,
    topn: Option<SinkTopnSpec>,
    freeze_bound: Option<u32>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // --- Arming + kill-switch layering (all cheap; absent = today's path).
    // M5-1: the router is the DOP source (bench GUC verbatim when set; else
    // engine=runtime arms at pgrust.runtime_dop; else 0 = today's path).
    let dop = router::arm_dop(ArmClass::Agg);
    if dop <= 0 || !runtime::runtime_enabled() {
        return Ok(false);
    }
    let Some(rt) = runtime::global() else { return Ok(false) };
    router::tick(ArmClass::Agg, ArmCounter::Offered);

    // EA-on-morsels (ea-morsels.md §5/§6): from here the session is ARMED —
    // under EXPLAIN ANALYZE every refusal records its first failing gate for
    // the transparency line.
    let ea = super::runtime_instr::ea_active(estate);
    let node_id = agg.plan.plan.plan_node_id;

    // --- Plan shape gates (fail-closed). Refusals trace AND (under EA)
    // record for the per-node EXPLAIN line.
    fn refuse(estate: &mut EStateData<'_>, ea: bool, node_id: i32, why: &'static str) {
        // M5-1: every agg-arm refusal feeds the router's consolidated
        // taxonomy alongside the trace / EA transparency line.
        router::tick_refused(ArmClass::Agg, why);
        lane_trace(&format!("runtime-agg: refused ({why})"));
        if ea {
            estate.runtime_ea_record_refusal(node_id, "agg", why);
        }
    }
    if !::nodeagg::sink::agg_sink_plan_shape_ok(agg) {
        refuse(estate, ea, node_id, "plan shape");
        return Ok(false);
    }
    if estate.es_epq_active {
        router::tick_refused(ArmClass::Agg, "epq");
        return Ok(false);
    }
    // Instrument MODE gate: INSTRUMENT_ROWS (TIMING OFF, inc-1) or
    // INSTRUMENT_TIMER (BUFFERS OFF, inc-3 — one clock pair per claim)
    // engage; BUFFERS/WAL combinations refuse until threaded.
    if ea && !super::runtime_instr::ea_mode_admissible(estate) {
        refuse(estate, true, node_id, super::runtime_instr::ea_mode_refuse_reason(estate));
        return Ok(false);
    }
    // Under EA the leader node carries an instr slot, which the serial-lane
    // fusibility memo rightly refuses — the sink's workers run
    // uninstrumented, so EA admission walks the same gates with only the
    // instrument check vacated (E4).
    let fusible = if ea {
        super::seq_scan_fusible_runtime_ea(ss, estate)?
    } else {
        seq_scan_fusible(ss, estate)?
    };
    if !fusible || !::nodeseqscan::seq_scan_is_pgrcolumnar(ss) {
        refuse(estate, ea, node_id, "scan not fusible cbstore");
        return Ok(false);
    }
    // Unprojected K2 class only in phase 1 (exprkey/Reduced/Multi are the
    // next cars); scan projection means the key is computed — refuse.
    // SE-T2AGG CAR B: vguard-only guarded plans (min/max(text) passengers)
    // admit knob-ON through `sink_vguard_plan_ok` — the drain proves each
    // batch inline (check_guards) and a demote REFUSES to the serial rerun.
    // GL-DICTDRAIN-1: the dict-class expr-key drain admits vguard-bearing
    // PROJECTED plans the same way (`sink_exprkey_dict_vguard_ok`); the
    // DictCoded kind arm below is the only consumer (belted per kind).
    let base_plan_ok = ::nodeagg::agg_lanefold_plan(agg)
        .is_some_and(|p| !p.guarded && p.vguards.is_empty() && p.resid.is_empty())
        || super::sink_vguard_plan_ok(agg, ss);
    let plan_ok = base_plan_ok || super::sink_exprkey_dict_vguard_ok(agg, ss);
    if !plan_ok || ::nodeagg::agg_lanefold_has_resid(agg) {
        refuse(estate, ea, node_id, "fold plan guarded/varlena/residual");
        return Ok(false);
    }
    // Budget triple (hoisted above the shape decide): the leader-side
    // cap-aware mk probe below and the sink construction must see the SAME
    // budget-derived cap (dop1-tax inc-3b — sink_cap_for replaces the fixed
    // 64K cap everywhere a cap is decided).
    let Some(state_bytes) = ::nodeagg::sink::agg_sink_state_bytes(agg) else {
        return Ok(false);
    };
    let Some(budget) = ::nodeagg::sink::agg_sink_hash_mem_limit(agg) else {
        return Ok(false);
    };
    let Some(ngroups_limit) = ::nodeagg::sink::agg_sink_ngroups_limit(agg) else {
        return Ok(false);
    };
    // Drain mode: projected scans take the expr-key feed (Arith/TsTrunc/
    // Reduced/Multi kinds — the lane's decide already ran and is memoized
    // in `xk`); unprojected scans take the K2 single-int-key batch probe,
    // the single-TEXT 1-component packed feed (C2 car), or the packed
    // multi-key composite feed (Mk car, int/numeric/one-text components).
    let (drain, red, mk, width);
    // K2 dict-code feed marker (set below): dict windows group on codes
    // through the worker table — a shape the scatter accept must refuse
    // (its fold-bypass has no dict leg; mixing paths would reorder
    // first-seen arrivals).
    let mut k2_dict_code = false;
    // DictCoded-kind marker (set in the expr-key arm below): consumed by
    // the strminmax group-estimate gate — hold disposition D2 exempts this
    // kind (lockstep with the dropped m5 mirror in the dict-key
    // classifier; the class letter carries the witnessed parity cell).
    let mut dict_coded_kind = false;
    if ss.ss.ps_ProjInfo.is_some() {
        let Some(xk) = xk else {
            refuse(estate, ea, node_id, "projected scan without an expr-key decide");
            return Ok(false);
        };
        if xk.sink_refused() {
            refuse(estate, ea, node_id, "expr-key decide refused");
            return Ok(false);
        }
        let Some(kind) = xk.sink_key_kind() else {
            refuse(estate, ea, node_id, "expr-key kind (dict/multi cars)");
            return Ok(false);
        };
        // Belt (GL-DICTDRAIN-1): the widened vguard plan admission serves
        // the DictCoded kind ONLY.
        if !base_plan_ok && !matches!(kind, super::exprkey::SinkXkKind::DictCoded) {
            refuse(estate, ea, node_id, "fold plan guarded/varlena/residual");
            return Ok(false);
        }
        drain = SinkDrain::ExprKey;
        match kind {
            super::exprkey::SinkXkKind::Single => {
                let Some(w) = ::nodeagg::sink::agg_sink_key_width(agg) else {
                    refuse(estate, ea, node_id, "key width");
                    return Ok(false);
                };
                red = None;
                mk = None;
                width = w;
            }
            super::exprkey::SinkXkKind::Reduced(shape) => {
                width = shape.width;
                red = Some(shape);
                mk = None;
            }
            super::exprkey::SinkXkKind::Multi { dict_input_att: _ } => {
                // ts-extract/CaseDict class: packed multi-key over the projected scan
                // (int/numeric components + one or two texts through the
                // canonical-bytes lane; CaseDict shapes carry TWO Intern
                // atts — the bare text Var and the computed key). Cap-aware
                // admission probe — no table armed on the leader (see the
                // Mk comment below). Spill-armed admission: mk_admit_n
                // vacates its estimate refusal for word-keyed shapes and —
                // under the canonical spill record's kill switch — for
                // Intern-bearing shapes too (canon-sink car 3).
                let (atts, n_atts) = xk
                    .sink_mk_intern_atts()
                    .expect("Multi kind carries intern atts");
                ::nodeagg::sink::agg_sink_set_cap_spill(
                    agg,
                    sink_cap_for(state_bytes, budget, ngroups_limit),
                    agg_spill_enabled(),
                );
                let admitted =
                    ::nodeagg::agg_hash_compact_mk_admit_multi(agg, false, &atts[..n_atts]);
                ::nodeagg::sink::agg_sink_clear_cap(agg);
                let Ok((shape, _numgroups)) = admitted else {
                    refuse(estate, ea, node_id, "expr-key mk admission");
                    return Ok(false);
                };
                if !mk_shape_sink_ok(&shape) {
                    refuse(estate, ea, node_id, "mk component kind (text car gate)");
                    return Ok(false);
                }
                red = None;
                mk = Some(shape);
                width = 8;
            }
            super::exprkey::SinkXkKind::DictCoded => {
                dict_coded_kind = true;
                // GL-DICTDRAIN-1: the Dict key class through the 1-Intern
                // compact spec (the C2 single-text shape, computed-key
                // fed). Cap-aware admission probe — no table armed on the
                // leader (the Mk comment above); the worker arm elects
                // intern-armed or DIRECT, both flushing identical
                // canonical-bytes runs (shape-only snapshot).
                let (atts, n_atts) =
                    xk.sink_mk_intern_atts().expect("Dict kind carries the key-out att");
                debug_assert_eq!(n_atts, 1, "the dict drain is the 1-Intern spec");
                ::nodeagg::sink::agg_sink_set_cap_spill(
                    agg,
                    sink_cap_for(state_bytes, budget, ngroups_limit),
                    agg_spill_enabled(),
                );
                let admitted = ::nodeagg::agg_hash_compact_mk_admit1(agg, Some(atts[0]));
                ::nodeagg::sink::agg_sink_clear_cap(agg);
                let Ok((shape, _numgroups)) = admitted else {
                    refuse(estate, ea, node_id, "expr-key dict-coded admission");
                    return Ok(false);
                };
                if !mk_shape_sink_ok(&shape) {
                    refuse(estate, ea, node_id, "mk component kind (text car gate)");
                    return Ok(false);
                }
                lane_trace("runtime-agg: dict-coded sink drain admitted (leader)");
                red = None;
                mk = Some(shape);
                width = 8;
            }
        }
    } else {
        // The staging arm (idempotent — the serial fold feed re-arms the
        // same shape on fallback) + the K2 single-int / single-text / Mk
        // packed decides.
        super::arm_scan_staging(ss, estate, ScanFeedShape::HashAggFold { agg })?;
        // SE-T2AGG CAR B: vguard plans read direct SoA indexes — re-arm the
        // columnar deform over any single-varlena remap staging (fn doc).
        if super::sink_vguard_plan_ok(agg, ss)
            && !super::sink_rearm_vguard_columnar(agg, ss, estate)
        {
            refuse(estate, ea, node_id, "vguard columnar staging");
            return Ok(false);
        }
        let k2_int = super::scan_k2_shape_sink(agg, ss, estate).is_some()
            && ::nodeagg::sink::agg_sink_key_width(agg).is_some();
        if k2_int {
            // Dict-group staging (the dict-int-key class: a single dict-encoded int
            // key whose fixed-width prefix deform is unarmable). CODE feed:
            // admit with the dict registration kept — the sink drain's
            // dict-window branch consumes the codes through the per-epoch
            // cache. RAW feed: dict-free columnar re-arm, plain K2 drain.
            // Off: the pre-lane refusal exactly.
            if ::nodeseqscan::seq_scan_batch_dictgroup_col(ss).is_some() {
                match dict_feed_mode() {
                    DictFeed::Code => {
                        k2_dict_code = true;
                        lane_trace("runtime-agg: K2 dict-code feed admitted");
                    }
                    DictFeed::Raw => {
                        if !sink_rearm_dictfree(agg, ss, estate) {
                            refuse(estate, ea, node_id, "dict-free columnar re-arm");
                            return Ok(false);
                        }
                        lane_trace("runtime-agg: K2 dict-free columnar re-arm");
                    }
                    DictFeed::Off => {
                        refuse(estate, ea, node_id, "dict-group staging");
                        return Ok(false);
                    }
                }
            }
            let Some(w) = ::nodeagg::sink::agg_sink_key_width(agg) else {
                refuse(estate, ea, node_id, "key width");
                return Ok(false);
            };
            drain = SinkDrain::K2;
            red = None;
            mk = None;
            width = w;
        } else if let Some(probe) = {
            // Cap-aware probes: the worker arms under the sink cap (bounded
            // table + flush discipline), so the leader's spill-estimate gate
            // must see the same capped group count — the K2 leader has no
            // estimate gate at all for exactly this reason. The cap is
            // cleared right after: the leader's own executor may still run
            // the SERIAL build (refusal / budget fallback / rescan), which
            // must never see sink mode.
            ::nodeagg::sink::agg_sink_set_cap_spill(
                agg,
                sink_cap_for(state_bytes, budget, ngroups_limit),
                agg_spill_enabled(),
            );
            // Single-text (C2) first — its shape class (one TEXT key) is
            // disjoint from the multi-key decide's (>= 2 keys).
            let probe = if runtime_agg_text_enabled() {
                super::scan_mk1_text_probe(agg, ss, estate)
            } else {
                None
            }
            .or_else(|| super::scan_mk_probe(agg, ss, estate));
            ::nodeagg::sink::agg_sink_clear_cap(agg);
            probe
        } {
            // Component gates: nullable images are heap-source-only; at most
            // TWO Intern (text) components — merged on CANONICAL RAW BYTES
            // (intern ids stay per-worker; two tails ride the canonical
            // multi-tail encoding — the unprojected two-text feed is the
            // SE-MKTEXT knob path, `PGRUST_LANE_V2_MULTIKEY_TEXT`); Numeric
            // packs are demote-SAFE (a mid-build pack failure maps to the
            // budget-refusal rerun); text/numeric classes ride the text-car
            // kill switches.
            if !mk_shape_sink_ok(&probe.shape) {
                refuse(estate, ea, node_id, "mk component kind (text car gate)");
                return Ok(false);
            }
            drain = SinkDrain::Mk;
            red = None;
            mk = Some(probe.shape);
            // Unused by the Mk drain: per-component widths ride the emit
            // plan's MultiComp columns.
            width = 8;
        } else {
            refuse(estate, ea, node_id, "K2/Mk shape");
            return Ok(false);
        }
    }
    // Combine + identity-emit qualification (fail-closed; catalog access).
    let Some(combines) = sink_resolve_combines(agg)? else {
        refuse(estate, ea, node_id, "combine whitelist");
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::ParallelGate);
        return Ok(false);
    };
    let key_spec = match (&red, &mk) {
        (Some(shape), _) => SinkKeySpec::Reduced(shape.clone()),
        (None, Some(shape)) => SinkKeySpec::Multi(shape.clone()),
        (None, None) => SinkKeySpec::Single { width },
    };
    let Some(emit) = sink_build_emit_plan(agg, &key_spec) else {
        refuse(estate, ea, node_id, "identity emit");
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::ParallelGate);
        return Ok(false);
    };
    // Post-aggregate filtered grouped shapes (stragg-coverage inc-1): a
    // filtered emit plan composes with the plain FULL drain only — winner
    // selection, the admission freeze, and table adopt all reason over
    // UNFILTERED group sets (a frozen/truncated admission could starve
    // groups that pass the filter; an adopted table's retrieve bypasses the
    // emit-buf row gate). Vacate them here; the serial Sort/Limit above the
    // engaged frame still consume the filtered grouped output.
    let (topn, freeze_bound) = if emit.has_filter() {
        if topn.is_some() || freeze_bound.is_some() {
            lane_trace("runtime-agg: topn/freeze vacated (post-aggregate emit filter)");
        }
        (None, None)
    } else {
        (topn, freeze_bound)
    };
    // F1 root cause (chaos-battery): the WORKER arm re-runs the compact
    // spill-eligibility gate under the sink cap with the leader's restored
    // work_mem — at small work_mem (<=256kB on 16k-group shapes) EVERY
    // worker refused ("worker compact arm refused under the sink cap"),
    // erroring pre-drive and stranding the pinned RG nobody would ever
    // drain. The leader runs the SAME numbers, so admission must refuse
    // here, fail-closed to the serial arm, before anything launches.
    // Spill-armed engagements vacate the estimate half of the gate on BOTH
    // sides (the workers arm under `sink_spill_ok`) — the predicate below
    // must match the worker flag exactly (F1 invariant). Canonical
    // (Intern-bearing) shapes spill through the C2 bytes record since
    // canon-sink car 3; its kill switch restores the historical exclusion.
    let spill_admission = agg_spill_enabled()
        && (!mk.as_ref().is_some_and(|s| s.intern_comp().is_some())
            || ::nodeagg::sink::sink_spill_canon_enabled());
    // Word-keyedness for the locality cap = the canonical predicate below
    // (Intern-bearing Mk shapes merge on canonical bytes; everything else —
    // Single/Reduced/K2/all-int Mk — merges on key words). Canon shapes
    // stayed excluded from the cap at train-19 (car3/car4 seam);
    // PGRUST_RUNTIME_AGG_LOCALITY_CANON=1 opts them in (the two-key/CaseDict probe
    // channel — see [`agg_locality_canon_enabled`]).
    let word_keyed = !mk.as_ref().is_some_and(|s| s.intern_comp().is_some());
    let cap_shape_ok = word_keyed || agg_locality_canon_enabled();
    // Plan-estimated group count for the NDV-adaptive locality rule (the
    // same leader estimate the compact layout law reads; the adaptive bands
    // in [`sink_locality_cap_for`] were calibrated on this figure).
    let est_groups = agg.plan.numGroups.max(1) as u64;
    // The scan's estimated rows — the cap-band v2 curve's collapse term
    // (α_est = est_rows / est_groups; [`agg_capband_v2_enabled`]). The
    // same planner figure the m5 FloorGuards read for this shape.
    let est_rows = agg
        .plan
        .plan
        .lefttree
        .and_then(Node::as_plan)
        .map(|p| p.plan_rows.max(0.0) as u64)
        .unwrap_or(0);
    // GL-STRMM-2 flip calibration, EXECUTOR half (knob-coherence law: same
    // spelling + same constant as the m5 probe's `strminmax_max_groups`):
    // string-min/max transvalues (byref text states, deep-copy emit) make
    // the sink measurably LOSE to the serial hash lane past the group-count
    // band the witnessed ladder banked — and the engine's OWN serial-plan
    // offer reaches here WITHOUT any suppression, so the probe-side ceiling
    // alone cannot close the band. Refuse fail-open to the serial arm (the
    // measured winner there). Leader-side pre-launch => workers never arm.
    // Hold disposition D2 (GL-HEAVYTIER-1, coordinator-approved): the
    // DictCoded kind is EXEMPT from this ceiling, because the class's engaged
    // sink is the WITNESSED WINNER for that kind with parity at production
    // scale far above the band (the class letter's cell). The m5 dict-key
    // classifier dropped its mirror in the SAME commit (knob-coherence
    // lockstep); every other kind keeps the ceiling byte-for-byte.
    //
    // GL-SINKCRASH-2 re-verified this exemption and CORRECTED its stated
    // grounds. D2 used to read as though the arena substrate were the
    // discriminator ("its byref-text combine/emit rides the allocator-exact
    // StrStateArena substrate"). Since every str-capable drain now arms that
    // store, the substrate is uniform and cannot discriminate anything — so
    // the only thing holding this exemption up is the measured cell, and the
    // `!dict_coded_kind` test must STAY. No ladder has been run for the K2 or
    // Mk kinds above the band; deleting the kind test because "the substrate
    // is the same now" would be an unpriced perf flip, and leaving the old
    // wording would be a comment naming a mechanism that no longer explains
    // the code — the tree's sharpest defect predictor.
    //
    // Note also that D2 is NOT what admits the shapes this class crashed on:
    // the ceiling is an ESTIMATE ceiling, and the crashing statement estimates
    // far below it, so it was always admitted by the ordinary path.
    if !dict_coded_kind
        && est_groups as f64 >= strminmax_max_groups()
        && combines
            .iter()
            .any(|c| matches!(c.kind, ::nodeagg::sink::SinkCombineKind::VarlenaMinMax { .. }))
    {
        refuse(estate, ea, node_id, "strminmax group-estimate ceiling");
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::ParallelGate);
        return Ok(false);
    }
    let sink_cap = sink_cap_engaged(
        state_bytes,
        budget,
        ngroups_limit,
        dop,
        cap_shape_ok,
        est_groups,
        est_rows,
    );
    if sink_cap < sink_cap_for(state_bytes, budget, ngroups_limit) {
        lane_trace(&format!("runtime-agg: locality cap engaged (cap={sink_cap})"));
    } else if agg_capband_v2_enabled()
        && cap_shape_ok
        && dop > 1
        && sink_cap_override().is_none()
    {
        // v2 high-α uncap witness (the band's engagement trace — the
        // GL-RADIX-2 ladder greps it; low-α / high-est shapes fall in the
        // branch above with their band cap in the line).
        lane_trace(&format!(
            "runtime-agg: cap-band v2 uncapped (est_groups={est_groups} est_rows={est_rows})"
        ));
    }
    if !::nodeagg::agg_hash_compact_sink_admissible(agg, sink_cap, spill_admission) {
        refuse(estate, ea, node_id, "worker compact arm would refuse under the sink cap/budget");
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::ParallelGate);
        return Ok(false);
    }
    // --- Session/binder gates (the M1 set, verbatim).
    if super::runtime_in_parallel_machinery(ss) {
        refuse(estate, ea, node_id, "in parallel mode");
        return Ok(false);
    }
    if estate.es_param_list_info.is_some_and(|p| !p.is_empty()) {
        refuse(estate, ea, node_id, "extern params");
        return Ok(false);
    }
    let Some(leader_pstmt) = estate.es_plannedstmt else {
        refuse(estate, ea, node_id, "no planned stmt");
        return Ok(false);
    };
    if leader_pstmt.paramExecTypes.iter().next().is_some() {
        refuse(estate, ea, node_id, "exec params");
        return Ok(false);
    }
    if !estate
        .es_snapshot
        .as_deref()
        .is_some_and(::types_snapshot::IsMVCCSnapshot)
    {
        refuse(estate, ea, node_id, "non-MVCC snapshot");
        return Ok(false);
    }
    let policy = parallel::query_task_policy_probe();
    if policy.has_params
        || policy.temp_state
        || policy.serializable
        || policy.pending_invalidations
    {
        refuse(estate, ea, node_id, "binder policy");
        return Ok(false);
    }
    // Worker plan root: the Agg subtree's Node in the leader plan tree.
    let Some(root) = leader_pstmt.planTree else {
        refuse(estate, ea, node_id, "no plan tree");
        return Ok(false);
    };
    let Some(agg_node) = find_agg_node(root, agg.plan) else {
        refuse(estate, ea, node_id, "agg node not in plan tree");
        return Ok(false);
    };
    // The Agg's scan child must be the SeqScan (no intermediate nodes).
    if agg.plan.plan.lefttree.map(Node::node_tag) != Some(NodeTag::T_SeqScan) {
        refuse(estate, ea, node_id, "scan child shape");
        return Ok(false);
    }

    // --- Geometry.
    let Some((total_granules, starts)) =
        ::nodeseqscan::seq_scan_cb_granule_geometry(ss, estate)?
    else {
        refuse(estate, ea, node_id, "granule geometry unavailable (no columnar part)");
        return Ok(false);
    };
    if total_granules < min_granules().max(2 * dop as u64) {
        refuse(estate, ea, node_id, "granule floor");
        return Ok(false);
    }
    // --- Engage.
    // Canonical (text-bearing) shapes merge on canonical key BYTES:
    // key_words 0 = the combine's bytes mode.
    let canon = mk.as_ref().is_some_and(|s| s.intern_comp().is_some());
    let key_words = if canon {
        0
    } else {
        mk.as_ref().map_or(1, |s| if s.two_words { 2 } else { 1 })
    };
    let byref_states = ::nodeagg::sink::sink_combines_byref(&combines);
    // DOP-elastic admission (tails192 #5): floors above ran against the
    // POOL dop; arm only what the work can feed (kill:
    // PGRUST_RUNTIME_ELASTIC_DOP=0). BYREF-FLOOR GUARD (cross-lane
    // constraint, proportionality-audit @ a39910a0b, two-int-key @dop2 172s
    // root-cause): byref state classes (AvgInt8/PolyInt128) accumulate an
    // UNSPILLABLE per-Local aggcontext floor ~= est_groups/W x per-group
    // bytes, while agg_sink_budget_pressure refuses at hash_mem_limit/2 —
    // a per-worker CONSTANT. Narrowing W raises the floor toward the trip
    // and a refusal costs a wasted drive + TRUE-serial rerun (~100x wall).
    // Never narrow below the floor-safe width:
    //   w_floor = ceil(est_groups x per_group_bytes / (budget/2))
    // per_group_bytes = state_bytes + key_words*8 + 48 (row + aggcontext
    // headroom — deliberately conservative: it only makes elastic narrow
    // LESS; the audit's root fix packs these classes inline and will
    // retire the term). est_groups = the same leader plan estimate the
    // locality law reads (coordinate: proportionality-audit).
    let dop = {
        let elastic = super::runtime_scan::elastic_dop(dop, total_granules);
        if elastic < dop && byref_states {
            let half_budget = (budget as u64 / 2).max(1);
            let per_group = state_bytes as u64 + (key_words as u64) * 8 + 48;
            let w_floor = est_groups
                .saturating_mul(per_group)
                .div_ceil(half_budget)
                .max(1)
                .min(dop as u64) as i32;
            let guarded = elastic.max(w_floor);
            if guarded > elastic {
                lane_trace(&format!(
                    "runtime-agg: elastic byref-floor guard {elastic}->{guarded} (est_groups={est_groups})"
                ));
            }
            guarded
        } else {
            elastic
        }
    };
    // TABLE-ADOPT shape gate: byval emit columns AND byval combine states —
    // the adopted table's rows must be self-contained past helper teardown
    // (a byref transvalue points into a worker aggcontext). Filtered emit
    // plans never adopt (the vacate comment at the emit-plan build).
    let adopt_shape =
        ::nodeagg::sink::sink_emit_plan_all_byval(&emit) && !byref_states && !emit.has_filter();
    // M3.5 spill arm: ON by default when the sink engages (this is the
    // refusal→engagement charter); PGRUST_RUNTIME_AGG_SPILL=0 restores the
    // phase-1 refusal exactly. SpillSet creation is leader-side (fd
    // substrate guaranteed); a creation failure fail-closes to refusal.
    // COMPOSITION GATE LIFTED (canon-sink car 3): canonical bytes-keyed
    // shapes (key_words == 0) spill through the C2 BYTES record
    // (variable-width, length-prefixed content, hash-carrying — the
    // distinct sink's record-v2 pattern on the AGG side), so text-bearing
    // engagements ride the same pressure/flush/split laws as word keys.
    // PGRUST_RUNTIME_AGG_SPILL_CANON=0 restores the train-13 exclusion
    // (canonical engagements keep the phase-1 budget refusal). The
    // predicate MUST stay equal to `spill_admission` above (F1 invariant).
    let spill_set = if agg_spill_enabled()
        && (!canon || ::nodeagg::sink::sink_spill_canon_enabled())
    {
        match ::spillset::SpillSet::create() {
            Ok(s) => Some(s),
            Err(_) => {
                // FAIL-CLOSED REFUSAL (not disarm): admission above may
                // have vacated the estimate gates under `spill_admission`;
                // launching spill-less workers would re-refuse under the
                // sink cap pre-drive and strand the pinned RG (the F1
                // class). Refuse the whole engagement — serial arm runs.
                lane_trace("runtime-agg: spill set creation failed — refused");
                refuse(estate, ea, node_id, "spill set creation failed");
                return Ok(false);
            }
        }
    } else {
        None
    };
    let ea_scan_node = if ea {
        agg.plan
            .plan
            .lefttree
            .and_then(Node::as_seq_scan)
            .map(|s| s.scan.plan.plan_node_id)
    } else {
        None
    };
    // LIMIT-k-no-ORDER group-admission freeze (band-2a): armed only on
    // the unprojected Mk drain — the one worker feed carrying the filter
    // hooks — with a small bound, no composed top-N (structurally exclusive:
    // topn derives from a Sort consumer, the bound from a bare Limit), a
    // non-nullable all-Int/one-Intern image (Numeric components demote
    // mid-build, which the membership filter must never race), and the kill
    // switch on. Declines keep the plain full drain, byte-identically.
    let freeze = match freeze_bound {
        Some(b)
            if drain == SinkDrain::Mk
                && topn.is_none()
                && agg_freeze_enabled()
                && b >= 1
                && b <= ::nodeagg::sink::SINK_FREEZE_MAX_BOUND
                && mk.as_ref().is_some_and(|s| {
                    !s.nullable
                        // Belt: the freeze snapshot parses a SINGLE raw text
                        // tail; two-intern shapes never reach the Mk drain
                        // (ExprKey-only), but fail closed if that changes.
                        && s.n_intern() <= 1
                        && s.comps.iter().all(|c| {
                            !matches!(c.kind, ::nodeagg::MkCompKind::Numeric { .. })
                        })
                }) =>
        {
            lane_trace(&format!("runtime-agg freeze: armed (bound={b})"));
            Some(Arc::new(::nodeagg::sink::SinkFreeze::new(b)))
        }
        Some(b) => {
            lane_trace(&format!(
                "runtime-agg freeze: declined (bound={b}; drain/shape/switch gate)"
            ));
            None
        }
        None => None,
    };
    // Shared-table EXPERIMENT engagement (D2, default OFF): K2 single-int-
    // word, COUNT-only states, no topn/freeze composition, est groups in
    // (engaged cap, band max] — below the band Locals never fill; above it
    // the table cannot be SLC-resident and the incumbent partitioned path
    // is the right answer (and remains the runtime fallback either way).
    let shared = if agg_shared_table_enabled()
        && !emit.has_filter()
        && drain == SinkDrain::K2
        && key_words == 1
        // ONE count transition: the state block is a single AggPerGroup
        // (16B — Datum + flag bytes), the layout the shared face folds
        // (wave-4 finding: the raw-transvalue 8B guess never engaged).
        && state_bytes == core::mem::size_of::<::execexpr::AggPerGroup>()
        && dop > 1
        && topn.is_none()
        && freeze.is_none()
        && ::nodeagg::sink::agg_sink_all_count(agg)
        && est_groups > agg_shared_table_min_groups().unwrap_or(sink_cap as u64)
        && est_groups <= agg_shared_table_max_groups()
    {
        lane_trace(&format!(
            "runtime-agg: SHARED-TABLE experiment engaged (est_groups={est_groups})"
        ));
        Some(SharedAggFace::new(est_groups))
    } else {
        None
    };
    // SEAL-FLUSH (radix seal) admission — [`agg_sealflush_enabled`]: the
    // high-NDV band only (the groupby-high class), multi-Local engagements
    // (DOP1 keeps the adopt/pass-through fast paths, which require zero
    // flushed runs), and no freeze/topn/shared composition in v1 — their
    // SEAL censuses read the remainder table; each composes later with its
    // own letter if the phase data asks for it.
    let seal_flush = agg_sealflush_enabled()
        && dop > 1
        && est_groups >= agg_sealflush_floor()
        && topn.is_none()
        && freeze.is_none()
        && shared.is_none()
        // Post-aggregate emit filters keep the plain full-drain composition
        // only in v1 (the vacate comment at the emit-plan build).
        && !emit.has_filter()
        // BYREF-STATE EXCLUSION (t40 composition red, assembler unload):
        // the flush bodies' caller contract is byval-POD state blocks
        // (sink_flush_table doc) — a byref transvalue (VarlenaMinMax /
        // unpacked PolyInt128) is a POINTER copied verbatim into the run,
        // and under rung-3 sticky retention the re-engaged worker's
        // aggcontext lifecycle no longer brackets the combine that reads
        // it (grouped-agg parity FAIL(15) at the t40 assembly tip:
        // min/max transvalues dropped; either side alone green). Byref
        // shapes keep the SEAL-index remainder path — which reads states
        // through the LIVE table — branch-for-branch. The GL-RADIX-1
        // ladder cells (byval count/sum) are untouched by this gate.
        && !byref_states;
    if seal_flush {
        lane_trace(&format!("runtime-agg: seal-flush armed (est_groups={est_groups})"));
    }
    // SCATTER ACCEPT admission (GL-RADIX-3, [`AggSink::scatter`]): kill
    // switch DEFAULT OFF; PLANNER-EST band gates (α_est ≤ ceiling AND
    // est_groups ≥ floor — the α≈1 high-NDV class where the accept is
    // probe-miss-dominated); the K2 single-int-key unprojected drain only,
    // dict-code feed excluded (no dict leg in the bypass); BYVAL-POD state
    // blocks only (single-row blocks are copied verbatim into runs — the
    // byref contract of the seal-flush exclusion above, same argument);
    // DOP>1 (the DOP1 adopt/pass-through fast paths keep the fold);
    // topn (reads the post-combine no_trans_value flag) / freeze / shared
    // (both census the worker table) excluded; fold-plan whitelist verdict
    // via `sink_scatter_admits` (the worker re-derives it — F1 law).
    // Spill composition: scatter buffers flush as ORDINARY runs under the
    // Local's R3 budget accounting, so the existing pressure→spill-epoch
    // law applies to them unchanged (nothing scatter-specific to compose).
    let scatter = agg_scatter_enabled()
        && drain == SinkDrain::K2
        && !k2_dict_code
        && dop > 1
        && !byref_states
        && topn.is_none()
        && freeze.is_none()
        && shared.is_none()
        && !emit.has_filter()
        && est_groups >= agg_scatter_floor()
        && est_rows / est_groups.max(1) <= agg_scatter_alpha()
        && ::nodeagg::sink::sink_scatter_admits(agg);
    if scatter {
        lane_trace(&format!(
            "runtime-agg: scatter accept armed (est_groups={est_groups} est_rows={est_rows})"
        ));
    }
    // VEC ACCEPT admission (GL-VECACCEPT-2 — the K2 substrate port of the
    // vecaccept lane; kill switch DEFAULT OFF): the K2 single-int-key
    // unprojected drain only, dict-code feed excluded (the direct lanes
    // serve no dict gather; int chunks never dict-encode, so this is
    // belt-and-braces); QUAL-FREE scan only (the direct granule walk
    // applies no filters — PREWHERE shapes keep the staged windows);
    // UNGUARDED, vguard-free, filter-free, residual-free fold plan (the
    // vguard proof and FILTER masks are window machinery); BYVAL-POD
    // states (the seal-flush/scatter byref exclusion's exact argument —
    // runs copy state blocks verbatim); DOP>1 (the DOP1 adopt/pass-through
    // fast paths key on zero flushed runs and the vec drive's coarser
    // flush grain could differ at the boundary); freeze/topn/shared/
    // scatter compositions excluded in v1 (each reads accept-side
    // censuses this drive does not produce); non-EA (the EA row funnel is
    // per-window machinery — named residual, not a law).
    let vec_accept = agg_vecaccept_k2_enabled()
        && drain == SinkDrain::K2
        && !k2_dict_code
        && dop > 1
        && !byref_states
        && topn.is_none()
        && freeze.is_none()
        && shared.is_none()
        && !scatter
        && !ea
        && ss.ss.qual.is_none()
        && ::nodeagg::agg_lanefold_plan(agg).is_some_and(|p| {
            !p.guarded && p.vguards.is_empty() && p.resid.is_empty() && p.filters.is_empty()
        });
    if vec_accept {
        lane_trace(&format!(
            "runtime-agg: vecaccept-k2 armed (est_groups={est_groups} est_rows={est_rows})"
        ));
        // GL-ALPHA1 batched-install arm witness (the knob lives in the
        // compact batch kernel; witnessed here once per engagement so
        // ladder legs can prove the arm — and its absence — per query).
        if ::nodeagg::compact_batch_install_enabled() {
            lane_trace("runtime-agg: batch-install armed");
        }
        // GL-ALPHA1 inc-2 route-latch arm witness (same per-engagement
        // channel; the mechanism proof rides the counter/profile legs).
        if super::agg_route_latch_enabled() {
            lane_trace("runtime-agg: route-latch armed");
        }
    }
    let sink = Arc::new(AggSink {
        drain,
        red,
        mk,
        cap: sink_cap,
        // Freeze-armed (LIMIT-k admission) shapes stay on the incumbent
        // cadence: their live group set is k-bounded post-freeze, so the
        // α-gate has no footprint to reclaim and the freeze-window byte
        // surface stays untouched.
        alpha_floor: if freeze.is_some() {
            None
        } else {
            alpha_gate_floor(state_bytes, sink_cap, dop)
        },
        shared,
        budget,
        key_words,
        state_bytes,
        byref_states,
        width,
        combines,
        emit,
        topn,
        topn_cands: (0..SINK_NBUCKETS).map(|_| UnsafeCell::new(Vec::new())).collect(),
        topn_degraded: AtomicBool::new(false),
        // §3.2 step 1 (meaningful only when `topn` armed): the kill switch
        // keeps decision-1 FullDrain; spill-armed engagements compose
        // (phase 2 split×selection) unless WINNERS_SPILL=0 restores the
        // phase-1 exclusion.
        topn_mode: AtomicU8::new(
            resolve_topn_mode_admission(
                spill_set.is_some(),
                topn_winners_enabled(),
                topn_winners_spill_enabled(),
            )
            .encode(),
        ),
        topn_refused: AtomicBool::new(false),
        topn_ctr: TopnCounters::default(),
        freeze,
        seal_flush,
        sealflush_rows: AtomicU64::new(0),
        scatter,
        vec_accept,
        vec_rows: AtomicU64::new(0),
        scatter_rows: AtomicU64::new(0),
        out_emit: (0..SINK_NBUCKETS).map(|_| UnsafeCell::new(SinkEmitBuf::default())).collect(),
        published: Mutex::new(None),
        adopt_shape,
        adopted: Mutex::new(None),
        adopted_flag: AtomicBool::new(false),
        forks: AtomicUsize::new(0),
        combine16_claims: AtomicU64::new(0),
        combine16_grows: AtomicU64::new(0),
        combine16_converts: AtomicU64::new(0),
        rg: OnceLock::new(),
        failed: AtomicBool::new(false),
        error: Mutex::new(None),
        budget_refused: AtomicBool::new(false),
        combined_bytes: AtomicUsize::new(0),
        spill_set,
        spill_epochs: AtomicU64::new(0),
        spilled_bytes: AtomicU64::new(0),
        combine_splits: AtomicU64::new(0),
        split_depth_max: AtomicU64::new(0),
        split_uniq: AtomicU64::new(0),
        ea_scan_node,
        ea_instr: Mutex::new(None),
        ea_timer: ea && super::runtime_instr::ea_timer(estate),
        ea_epoch: std::time::Instant::now(),
        // Two-level socket-local combine (numa-combine item 1): kill switch
        // + DOP threshold (default 96 — engages on the 48xl regime, never
        // at mt16 defaults) + the all-byval gate (partial-run state blocks
        // are copied verbatim across claims; byref blocks would dangle, and
        // the byval whitelist is what makes half-regrouping bit-exact).
        numa: (numa_combine_enabled() && dop >= numa_combine_dop_min() && !byref_states)
            .then(NumaCombine::new),
    });
    engage(agg, estate, rt, dop, total_granules, starts, agg_node, sink)
}

#[allow(clippy::too_many_arguments)]
fn engage<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    total_granules: u64,
    starts: Vec<u64>,
    agg_node: Node<'mcx>,
    sink: Arc<AggSink>,
) -> PgResult<bool> {
    ensure_hooks_registered();
    crate::execparallel::register_parallel_query_main();

    let pstmt = crate::execparallel::build_worker_pstmt(estate, agg_node)?;
    let payload = Arc::new(RuntimeAggShared {
        rt,
        rg: OnceLock::new(),
        pcxt_shared: OnceLock::new(),
        // SAFETY (lifetime erasure): leader executor arena, held across the
        // whole engagement; DestroyParallelContext joins helpers before this
        // frame returns on every path (runtime_scan precedent).
        pstmt: SendConstPstmt(unsafe {
            core::mem::transmute::<*const PlannedStmt<'mcx>, *const PlannedStmt<'static>>(
                pstmt as *const PlannedStmt<'mcx>,
            )
        }),
        query_text: estate.es_sourceText.unwrap_or("").to_string(),
        eflags: estate.es_top_eflags,
        refused: AtomicUsize::new(0),
        started: AtomicUsize::new(0),
        exited: AtomicUsize::new(0),
        sink: Arc::clone(&sink),
        query_id: AtomicU64::new(0),
        standing: Mutex::new(None),
    });

    // Arming-phase decomposition spans (tails192 #5): the agg arm had NO
    // l.* gtrace coverage while its submit->first-service window measures
    // 2.0-5.7ms at 16-core (vs the scan arm's 0.25ms standing channel) --
    // the launched-helper ceremony is the at-191 tiny-query tax suspect.
    // PGRUST_GATHER_TRACE-gated, free when off.
    parallel::gtrace("l.agg.engage.begin");
    xact::EnterParallelMode();
    // Router counter choke point (M5-1): Engaged = ceremony entered;
    // Completed = the runtime answered; Fallback = R5 serial rerun.
    router::tick(ArmClass::Agg, ArmCounter::Engaged);
    let engaged =
        engage_ceremony(agg, estate, rt, dop, total_granules, starts, &payload, &sink);
    xact::ExitParallelMode();
    parallel::gtrace("l.agg.engage.end");
    if let Ok(done) = &engaged {
        router::tick(
            ArmClass::Agg,
            if *done { ArmCounter::Completed } else { ArmCounter::Fallback },
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
        label: "runtime-agg",
        died: "runtime agg standing executors exited before completing the aggregation",
        sinks_gate: true,
    };

/// Shared post-outcome tail (standing and launched channels): worker-phase
/// errors rethrow PLAIN (the serial arm's surface, the parity oracle);
/// budget / topn-winners refusals take the R5 whole-attempt serial rerun;
/// an unexplained abort surfaces the pending interrupt or reports; a
/// completed-but-nobody-participated RG falls back serially.
fn finish_outcome(
    payload: &Arc<RuntimeAggShared>,
    sink: &Arc<AggSink>,
    outcome: runtime::RgOutcome,
) -> PgResult<EngageOutcome> {
    if let Some(e) = sink.take_error() {
        return Err(e);
    }
    if sink.budget_refused.load(Ordering::SeqCst) {
        // R5 degrade: whole-attempt rerun on the serial arm.
        lane_trace("runtime-agg: budget refusal — falling back to the serial arm");
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::ParallelGate);
        return Ok(EngageOutcome::Fallback);
    }
    if sink.topn_refused.load(Ordering::SeqCst) {
        // Winners-only refusal: same R5 whole-attempt serial rerun,
        // its own named trace reason (count-gated ≈0 by the e2e legs).
        lane_trace("runtime-agg: topn-winners refusal — falling back to the serial arm");
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::ParallelGate);
        return Ok(EngageOutcome::Fallback);
    }
    if outcome == runtime::RgOutcome::Aborted {
        ::postgres_seams::check_for_interrupts::call()?;
        return Err(Box::new(PgError::new(ERROR, "runtime agg pipeline aborted")));
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
    starts: Vec<u64>,
    payload: &Arc<RuntimeAggShared>,
    sink: &Arc<AggSink>,
) -> PgResult<bool> {
    let pcxt = parallel::CreateParallelContext("postgres", "pgrust_runtime_agg_main", dop)?;
    let mut submitted: Option<runtime::RgHandle> = None;
    // SinkProbe surface (M5-1, the §3.5 lane_trace remainder): captured out
    // of the ceremony body and reported at RG completion.
    let mut sink_probe: Option<Arc<runtime::SinkProbe>> = None;
    let probe_out = &mut sink_probe;

    let body = (|mut_submitted: &mut Option<runtime::RgHandle>| -> PgResult<EngageOutcome> {
        parallel::InitializeParallelDSM(pcxt)?;
        parallel::gtrace("l.agg.dsm.end");
        let nworkers = parallel::nworkers(pcxt);
        if nworkers <= 0 {
            return Ok(EngageOutcome::Fallback);
        }
        parallel::InstallQueryTaskBinding(pcxt, parallel::QueryTaskBindingPolicy::default())?;
        parallel::gtrace("l.agg.qtb.end");
        payload
            .pcxt_shared
            .set(parallel::shared_for(pcxt))
            .unwrap_or_else(|_| unreachable!("pcxt shared set once"));
        parallel::set_private(pcxt, Arc::clone(payload) as _);
        // Standing driver dispatch (M2 inc-1): deferred_bind false — this
        // arm's helper_drive binds EAGERLY (with_query_task_binding), so
        // the standing serve re-establishes visibility up front and evicts
        // any parked sticky retention.
        parallel::set_standing_driver(pcxt, parallel::standing::StandingDriver {
            drive: runtime_agg_standing_driver,
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


        // The sink's task sets over the pgrcolumnar granule geometry. Default
        // (combine-parallel lane): the 3-set sealed plumbing — ACCEPT →
        // FREEZE (per-Local SEAL partition, parallel across slots) →
        // COMBINE. PGRUST_RUNTIME_AGG_PARSEAL=0 restores the 2-set shape
        // (single-threaded SEAL in the accept set's finalize) exactly.
        let source = Arc::new(PgrcolumnarGranuleSource { starts });
        let tasksets = if parseal_enabled() {
            let runtime::SealedSinkTaskSets { accept, freeze, combine, probe } =
                runtime::sealed_sink_tasksets(
                    Arc::clone(sink),
                    source,
                    rt.nthreads() + runtime::MAX_EXTERNAL_LANES,
                    0,
                );
            *probe_out = Some(probe);
            vec![accept, freeze, combine]
        } else {
            let runtime::SinkTaskSets { accept, combine, probe } =
                runtime::sink_tasksets(Arc::clone(sink), source, rt.nthreads(), 0);
            *probe_out = Some(probe);
            vec![accept, combine]
        };
        static NEXT_QUERY_ID: AtomicUsize = AtomicUsize::new(1);
        let qid = NEXT_QUERY_ID.fetch_add(1, Ordering::SeqCst) as u64;
        payload.query_id.store(qid, Ordering::SeqCst);
        parallel::gtrace("l.agg.sink.end");
        let spec = runtime::QuerySpec { query_id: qid, tasksets };
        // rg-set-BEFORE-publish (M2 inc-3 rung 3): every serve-visible rg
        // cell is stored by on_rg before the bound submission can become
        // pool-visible — no "rg gone" refusal churn window. The unbound arm
        // has no pool pick; it stores post-submit as before.
        let set_rg = |rg: &runtime::RgHandle| {
            payload
                .rg
                .set(rg.downgrade())
                .unwrap_or_else(|_| unreachable!("rg set once"));
            sink.rg
                .set(rg.downgrade())
                .unwrap_or_else(|_| unreachable!("sink rg set once"));
        };
        let (rg, waiter) = match &pool {
            Some((_, descriptor)) => rt.submit_pinned_bound(
                spec,
                router::session_affinity_token(),
                descriptor.clone(),
                set_rg,
            ),
            None => {
                let (rg, waiter) =
                    rt.submit_pinned_with_affinity(spec, router::session_affinity_token());
                set_rg(&rg);
                (rg, waiter)
            }
        };
        parallel::gtrace("l.agg.submit.end");
        *mut_submitted = Some(rg.clone());

        // M2 inc-1: STANDING engagement first (the scan arm's channel,
        // extended to the sink arms) — no worker launch, no entry task,
        // one binder bind per participant. Fallback (kill switch / gang
        // busy / all-refused / claim deadline) leaves the RG untouched and
        // falls through to the serial arm below (rung 4).
        match super::standing_channel::standing_wait(
            &STANDING_ARM,
            super::standing_channel::StandingLeader {
                // M2 inc-2: the pool-db board attached at submit (None =
                // gang-first, inc-1 exactly).
                pool: pool.as_ref().map(|(entry, _)| Arc::clone(entry)),
                shared: payload
                    .pcxt_shared
                    .get()
                    .expect("pcxt shared set above"),
                slot: &payload.standing,
                started: &payload.started,
                refused: &payload.refused,
                take_error: &|| sink.take_error(),
                drain: &|rg| drain_rg(rt, rg),
                census: "",
            },
            dop,
            total_granules,
            &rg,
            &waiter,
        )? {
            super::standing_channel::StandingWait::Done(outcome) => {
                return finish_outcome(payload, sink, outcome);
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
        router::sink_probe_complete(ArmClass::Agg, probe);
    }

    match outcome {
        EngageOutcome::Fallback => {
            stats::tick_engaged(STANDING_ARM.label, stats::EngageChannel::Serial);
            lane_trace("runtime-agg: fallback to serial arm");
            Ok(false)
        }
        EngageOutcome::Completed => {
            let published = sink
                .published
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take()
                .ok_or_else(|| {
                    ::nodeagg::sink::sink_shape_error("completed sink published nothing")
                })?;
            let natts = sink.emit.cols.len();
            let spill_epochs = sink.spill_epochs.load(Ordering::Relaxed);
            if spill_epochs > 0 {
                // The R4 spill-rate observability line (e2e + gate records).
                lane_trace(&format!(
                    "runtime-agg: SPILLED epochs={spill_epochs} bytes={}",
                    sink.spilled_bytes.load(Ordering::Relaxed)
                ));
            }
            let splits = sink.combine_splits.load(Ordering::Relaxed);
            if splits > 0 {
                lane_trace(&format!(
                    "runtime-agg: COMBINE-SPLIT splits={splits} max_depth={}",
                    sink.split_depth_max.load(Ordering::Relaxed)
                ));
            }
            // combine16 evidence line (e2e-grepped): flat presized merged
            // tables must never grow or convert; the incumbent arm on a
            // large canonical shape shows the degenerate-top-byte growth.
            let c16_claims = sink.combine16_claims.load(Ordering::Relaxed);
            if c16_claims > 0 {
                lane_trace(&format!(
                    "runtime-agg: COMBINE16 flat={} claims={c16_claims} grows={} converts={}",
                    ::nodeagg::sink::sink_combine16_enabled() as u32,
                    sink.combine16_grows.load(Ordering::Relaxed),
                    sink.combine16_converts.load(Ordering::Relaxed)
                ));
            }
            // EA-on-morsels merge (clean Completed only): write the bypassed
            // scan node's rows/nfiltered/loops from the sealed accept-phase
            // merge (ea-morsels.md §3 — node-exact rows; the Agg root ticks
            // through its procnode wrapper as groups emit).
            if let Some(scan_node) = sink.ea_scan_node {
                if let Some(m) =
                    sink.ea_instr.lock().unwrap_or_else(|p| p.into_inner()).take()
                {
                    super::runtime_instr::ea_fill_scan_node(estate, scan_node, &m.rows);
                    // Pipeline report for the inc-2 EXPLAIN block (ACCEPT +
                    // COMBINE task sets on this arm; partials = workers).
                    estate.es_runtime_ea_pipelines.push(
                        super::runtime_instr::ea_pipeline_report(
                            "agg",
                            agg.plan.plan.plan_node_id,
                            scan_node,
                            -1,
                            // Task-set count: 3 under the sealed plumbing
                            // (accept/freeze/combine), 2 under PARSEAL=0.
                            if parseal_enabled() { 3 } else { 2 },
                            m.workers as u64,
                            &m,
                        ),
                    );
                    lane_trace(&format!(
                        "runtime-agg: EA merged workers={} claims={} granules={} \
                         scanned={} survived={}",
                        m.workers, m.claims, m.granules, m.rows.scanned, m.rows.survived
                    ));
                }
            }
            match published {
                SinkPublished::Emit(bufs, winners) => {
                    let rows = ::nodeagg::sink::sink_emit_rows(&bufs);
                    match (&winners, &sink.topn) {
                        // NOTE: "topn composed (winners=N)" is a load-bearing
                        // token (e2e leg-7 greps) — mode/materialized append
                        // AFTER the closing paren. Under winners-only,
                        // `groups=` counts MATERIALIZED rows (the compact
                        // candidate union), not the true group count.
                        (Some(w), _) => lane_trace(&format!(
                            "runtime-agg: complete, groups={rows}, topn composed (winners={}) mode={} materialized={rows}",
                            w.len(),
                            match sink.topn_mode() {
                                TopnMode::WinnersOnly => "winners-only",
                                TopnMode::FullDrain => "full",
                            },
                        )),
                        (None, Some(_)) => lane_trace(&format!(
                            "runtime-agg: complete, groups={rows}, topn degraded — full drain"
                        )),
                        (None, None) => {
                            lane_trace(&format!("runtime-agg: complete, groups={rows}"))
                        }
                    }
                    // Winners-only inc-1 evidence line (design §6): the
                    // combine phase's cost decomposition on topn-armed
                    // engagements. ns are worker-time sums across claims.
                    if sink.topn.is_some() {
                        let c = &sink.topn_ctr;
                        lane_trace(&format!(
                            "runtime-agg topn counters: mat_rows={} cand_rows={} \
                             build_us={} select_us={} emit_us={}",
                            c.mat_rows.load(Ordering::Relaxed),
                            c.cand_rows.load(Ordering::Relaxed),
                            c.build_ns.load(Ordering::Relaxed) / 1_000,
                            c.select_ns.load(Ordering::Relaxed) / 1_000,
                            c.emit_ns.load(Ordering::Relaxed) / 1_000,
                        ));
                    }
                    // spankey copy-tax decomposition (measurement only,
                    // PGRUST_SPANKEY_CTR=1): print-and-reset per engagement.
                    if let Some(s) = ::nodeagg::spankey::spankey_report_reset() {
                        lane_trace(&s);
                    }
                    // Freeze evidence line (e2e legs grep this): FROZEN
                    // engagements emit exactly `bound` member groups.
                    if let Some(fz) = &sink.freeze {
                        if fz.frozen() {
                            lane_trace(&format!(
                                "runtime-agg freeze: engaged bound={} dropped_rows={} \
                                 stragglers={}",
                                fz.bound(),
                                fz.dropped(),
                                fz.stragglers()
                            ));
                        } else {
                            lane_trace("runtime-agg freeze: armed, never froze (full drain)");
                        }
                    }
                    ::nodeagg::sink::agg_sink_adopt_emit(agg, bufs, natts, winners);
                }
                SinkPublished::Table(table) => {
                    let rows = table.table().nrows();
                    lane_trace(&format!(
                        "runtime-agg: complete (table adopt), groups={rows}"
                    ));
                    if let Some(s) = ::nodeagg::spankey::spankey_report_reset() {
                        lane_trace(&s);
                    }
                    ::nodeagg::sink::agg_sink_adopt_table(agg, table, sink.emit.clone());
                }
            }
            Ok(true)
        }
    }
}

/// Abort + BOUNDED drain of a pinned RG no helper will drive
/// (abort/fallback paths) — cleanup driving, not leader execution
/// (runtime_scan's hardened drain, verbatim; F1 port). True = the RG
/// completed. False = it could not be completed (a participant died holding
/// an unsettled pin): the RG and its slot are deliberately LEAKED and the
/// caller must surface an error rather than wait forever — the previous
/// unbounded `loop {{ acquire }} + drive_pinned` shape could itself wedge
/// on exactly the helper-death cases this lane fixes.
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
        lane_trace("runtime-agg: LEAKED pinned RG (no external lane for the drain)");
        return false;
    };
    let mut local = lane.local();
    let drained = rt.try_drain_pinned(&mut local, rg, 4000).is_some();
    if !drained {
        lane_trace("runtime-agg: LEAKED pinned RG (drain gave up — dead participant?)");
    }
    drained
}

/// Granule-addressed morsel source over one pgrcolumnar part's geometry
/// (runtime_scan's source, module-local copy — claims never cross a
/// row-group/dict-epoch edge).
struct PgrcolumnarGranuleSource {
    starts: Vec<u64>,
}

impl runtime::MorselSource for PgrcolumnarGranuleSource {
    fn total_granules(&self) -> u64 {
        self.starts.last().copied().unwrap_or(0)
    }

    fn next_boundary_after(&self, start: u64) -> u64 {
        match self.starts.binary_search(&start) {
            Ok(i) => self.starts.get(i + 1).copied().unwrap_or_else(|| self.total_granules()),
            Err(i) => self.starts.get(i).copied().unwrap_or_else(|| self.total_granules()),
        }
    }

    fn startup_c0(&self) -> u64 {
        2
    }
}

#[cfg(test)]
mod scratch_estate_tests {
    use super::{ScratchLedger, SinkDictScratch, SinkVecScratch};

    /// GL-CONCMEM-1: the delta-settled scratch ledger charges growth,
    /// uncharges shrink, and Drop unwinds the residue exactly. Delta-based
    /// against the process-global counter (other tests hold their own live
    /// charges) with the lanetable balance-test law: noise allowance on
    /// the held bound, retry on the final bound — a leaked charge is
    /// permanent and never converges.
    #[test]
    fn scratch_ledger_balances() {
        const NOISE: usize = 16 << 20;
        let base = ::mcx::global_footprint::bytes();
        {
            let mut l = ScratchLedger::default();
            l.settle(48 << 20);
            let held = ::mcx::global_footprint::bytes();
            assert!(
                held + NOISE >= base + (48 << 20),
                "settle did not charge the ledger (held {held}, base {base})"
            );
            // Shrink uncharges; regrow leaves a residue for Drop.
            l.settle(8 << 20);
            l.settle(24 << 20);
        }
        let mut ok = false;
        for _ in 0..50 {
            let after = ::mcx::global_footprint::bytes();
            if after <= base + NOISE && base <= after + NOISE {
                ok = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        let after = ::mcx::global_footprint::bytes();
        assert!(ok, "ledger did not balance after Drop: base {base}, after {after}");
    }

    /// The estate faces count backing-store CAPACITY — the whale lanes
    /// (gndv-sized per-epoch code caches / granule lane copies) dominate
    /// and must be visible to the ledger.
    #[test]
    fn scratch_estate_counts_capacity() {
        let mut dgs = SinkDictScratch::default();
        assert_eq!(dgs.estate_bytes(), 0);
        dgs.slots.resize(1 << 20, None);
        assert!(dgs.estate_bytes() >= (1 << 20) * 8, "dict slots lane uncounted");
        dgs.invalidate();
        // clear() keeps capacity — the allocator still holds the store and
        // the estate face must keep reporting it.
        assert!(dgs.estate_bytes() >= (1 << 20) * 8, "cleared capacity uncounted");

        let mut mks = super::super::MkScratch::default();
        mks.code_ids.resize(1 << 20, 0);
        mks.code_states.resize(1 << 20, core::ptr::null_mut());
        assert!(mks.estate_bytes() >= (1 << 20) * 12, "mk code caches uncounted");

        let mut vs = SinkVecScratch::default();
        vs.knull.resize(8192, false);
        vs.idxv.resize(8192, 0);
        assert!(vs.estate_bytes() >= 8192 * 5, "vec lanes uncounted");
    }
}

#[cfg(test)]
mod topn_mode_tests {
    use super::{resolve_topn_mode_admission, resolve_topn_mode_seal, TopnMode};

    /// §3.2 resolution ladder — the inc-2 mode-resolution matrix:
    /// spill armed/disarmed × kill switch × pass-through shape. (The adopt
    /// shape never consults the mode: finalize publishes Table and winners
    /// never ride it — covered by the e2e tranche's adopt legs.)
    #[test]
    fn mode_resolution_matrix() {
        // Kill switch off → FullDrain regardless of everything else.
        assert_eq!(resolve_topn_mode_admission(true, false, true), TopnMode::FullDrain);
        assert_eq!(resolve_topn_mode_admission(false, false, true), TopnMode::FullDrain);
        assert_eq!(resolve_topn_mode_admission(false, false, false), TopnMode::FullDrain);
        // Phase 2 (split×selection): spill-armed engagements compose.
        assert_eq!(resolve_topn_mode_admission(true, true, true), TopnMode::WinnersOnly);
        // WINNERS_SPILL=0 restores the ratified phase-1 exclusion exactly.
        assert_eq!(resolve_topn_mode_admission(true, true, false), TopnMode::FullDrain);
        assert_eq!(resolve_topn_mode_admission(false, true, false), TopnMode::WinnersOnly);
        // Product default: armed, spill-disarmed, switches on → WinnersOnly.
        assert_eq!(resolve_topn_mode_admission(false, true, true), TopnMode::WinnersOnly);
        // SEAL: the pass-through census (1 Local, no runs, no spill face)
        // forces FullDrain; a widened engagement keeps the admission mode.
        assert_eq!(
            resolve_topn_mode_seal(TopnMode::WinnersOnly, true),
            TopnMode::FullDrain
        );
        assert_eq!(
            resolve_topn_mode_seal(TopnMode::WinnersOnly, false),
            TopnMode::WinnersOnly
        );
        assert_eq!(resolve_topn_mode_seal(TopnMode::FullDrain, true), TopnMode::FullDrain);
        assert_eq!(resolve_topn_mode_seal(TopnMode::FullDrain, false), TopnMode::FullDrain);
    }

    #[test]
    fn mode_codec_roundtrip() {
        for m in [TopnMode::WinnersOnly, TopnMode::FullDrain] {
            assert_eq!(TopnMode::decode(m.encode()), m);
        }
        // Unknown encodings decode fail-closed to FullDrain.
        assert_eq!(TopnMode::decode(97), TopnMode::FullDrain);
    }
}

#[cfg(test)]
mod alpha_gate_tests {
    use super::AlphaGate;

    const FLOOR: Option<u32> = Some(16_384);
    const CAP: u32 = 65_536;
    const A0: u64 = 200; // α₀ = 2.0
    const RP: u64 = 10;

    fn fill_and_flush(g: &mut AlphaGate, rows: u64, entries: usize) {
        g.absorbed(rows as usize);
        g.adjudicate(entries, FLOOR, CAP, A0, RP);
    }

    /// Write-buffer window (α ≈ 1) demotes; a collapsing floor window
    /// (α ≥ α₀) restores immediately — the phase-change fast path.
    #[test]
    fn demote_then_collapse_restore() {
        let mut g = AlphaGate::default();
        assert!(!g.demoted);
        fill_and_flush(&mut g, CAP as u64, CAP as usize); // α = 1.0
        assert!(g.demoted);
        assert_eq!((g.demotes, g.restores, g.reprobes), (1, 0, 0));
        // Floor window with strong collapse: 3 rows/entry.
        fill_and_flush(&mut g, 3 * 16_384, 16_384);
        assert!(!g.demoted);
        assert_eq!((g.demotes, g.restores, g.reprobes), (1, 1, 0));
    }

    /// The α₀ boundary is INCLUSIVE-keep: exactly α₀ keeps the share
    /// (rows×100 ≥ entries×α₀×100), just under demotes.
    #[test]
    fn alpha0_boundary() {
        let mut g = AlphaGate::default();
        fill_and_flush(&mut g, 2 * CAP as u64, CAP as usize); // α = 2.0
        assert!(!g.demoted);
        fill_and_flush(&mut g, 2 * CAP as u64 - 1, CAP as usize); // just under
        assert!(g.demoted);
    }

    /// Müller hysteresis: a demoted Local with persistently low α re-probes
    /// (full threshold restored) only after reprobe_mult × full-cap rows,
    /// then re-demotes on the next low-α full window, restarting the clock.
    #[test]
    fn reprobe_after_ten_cache_volumes() {
        let mut g = AlphaGate::default();
        fill_and_flush(&mut g, CAP as u64, CAP as usize); // demote
        assert!(g.demoted);
        // Floor windows at α=1: 10×CAP rows = 40 floor fills before the
        // re-probe budget is met.
        let mut reprobed_at = None;
        for i in 0..50 {
            fill_and_flush(&mut g, 16_384, 16_384);
            if !g.demoted {
                reprobed_at = Some(i);
                break;
            }
        }
        // 10 × 65_536 rows / 16_384 rows-per-window = 40 windows.
        assert_eq!(reprobed_at, Some(39));
        assert_eq!((g.demotes, g.restores, g.reprobes), (1, 0, 1));
        // The probe window fails again → re-demote, clock restarted.
        fill_and_flush(&mut g, CAP as u64, CAP as usize);
        assert!(g.demoted);
        assert_eq!(g.demotes, 2);
        assert_eq!(g.rows_since_demote, 0);
    }

    /// reprobe_mult = 0 pins a low-α demote (sticky mode).
    #[test]
    fn reprobe_zero_is_sticky() {
        let mut g = AlphaGate::default();
        g.absorbed(CAP as usize);
        g.adjudicate(CAP as usize, FLOOR, CAP, A0, 0);
        assert!(g.demoted);
        for _ in 0..1000 {
            g.absorbed(16_384);
            g.adjudicate(16_384, FLOOR, CAP, A0, 0);
        }
        assert!(g.demoted);
        assert_eq!(g.reprobes, 0);
    }

    /// Unarmed controller (alpha_floor = None) never transitions and the
    /// effective threshold never leaves sink.cap — the kill-switch/serial/
    /// freeze identity. Pressure flushes reset the window without
    /// adjudication.
    #[test]
    fn unarmed_and_pressure_identity() {
        let mut g = AlphaGate::default();
        g.absorbed(CAP as usize);
        g.adjudicate(CAP as usize, None, CAP, A0, RP);
        assert!(!g.demoted);
        assert_eq!((g.demotes, g.restores, g.reprobes), (0, 0, 0));
        // Pressure flush: window resets, no verdict.
        g.absorbed(100);
        g.on_pressure_flush();
        assert_eq!(g.window_rows, 0);
        assert!(!g.demoted);
        // Empty run (entries = 0) never adjudicates.
        g.adjudicate(0, FLOOR, CAP, A0, RP);
        assert!(!g.demoted);
    }
}

#[cfg(test)]
mod numa_combine_tests {
    use super::{NumaCombine, SINK_NBUCKETS};
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    /// The credit/item law: 2×SINK_NBUCKETS pops (any mix of preferred
    /// halves) own each (half, bucket) item EXACTLY once; every later pop
    /// proves exhaustion (None). Serial form of the claims argument.
    #[test]
    fn pop_owns_every_item_exactly_once() {
        let nc = NumaCombine::new();
        let mut seen = vec![false; 2 * SINK_NBUCKETS];
        for credit in 0..2 * SINK_NBUCKETS {
            // Adversarial preference mix: alternate + skew.
            let my = usize::from(credit % 3 == 0);
            let (h, b) = nc.pop(my).expect("credits == items");
            let slot = h * SINK_NBUCKETS + b;
            assert!(!seen[slot], "item ({h},{b}) popped twice");
            seen[slot] = true;
        }
        assert!(seen.iter().all(|&s| s), "every item popped");
        assert_eq!(nc.pop(0), None, "exhaustion proven");
        assert_eq!(nc.pop(1), None);
    }

    /// Steering preference: with items available on both halves, a pop
    /// serves its own half first; once the own half drains, it steals.
    #[test]
    fn pop_prefers_own_half_then_steals() {
        let nc = NumaCombine::new();
        for _ in 0..SINK_NBUCKETS {
            let (h, _) = nc.pop(1).unwrap();
            assert_eq!(h, 1, "own half first");
        }
        assert_eq!(nc.steer_hit.load(Ordering::Relaxed), SINK_NBUCKETS as u64);
        // Half 1 drained: half-1 workers steal from half 0.
        let (h, _) = nc.pop(1).unwrap();
        assert_eq!(h, 0, "steal after own half drains");
        assert_eq!(nc.steer_miss.load(Ordering::Relaxed), 1);
    }

    /// The concurrent claims argument end-to-end: N threads race pops and
    /// the per-bucket 1→2 election; every item is owned once, every bucket
    /// elects exactly one FINAL, and no thread ever waits (a None pop is
    /// terminal for that credit).
    #[test]
    fn concurrent_pop_and_election() {
        let nc = Arc::new(NumaCombine::new());
        let nthreads = 8;
        let credits = 2 * SINK_NBUCKETS / nthreads;
        let mut handles = Vec::new();
        for t in 0..nthreads {
            let nc = Arc::clone(&nc);
            handles.push(std::thread::spawn(move || {
                let mut popped = Vec::new();
                let mut finals = Vec::new();
                for c in 0..credits {
                    let my = usize::from((t + c) % 2 == 1);
                    let (h, b) = nc.pop(my).expect("credits == items");
                    popped.push((h, b));
                    if nc.done[b].fetch_add(1, Ordering::AcqRel) + 1 == 2 {
                        finals.push(b);
                    }
                }
                (popped, finals)
            }));
        }
        let mut all_popped = vec![0u32; 2 * SINK_NBUCKETS];
        let mut all_finals = vec![0u32; SINK_NBUCKETS];
        for h in handles {
            let (popped, finals) = h.join().unwrap();
            for (hh, b) in popped {
                all_popped[hh * SINK_NBUCKETS + b] += 1;
            }
            for b in finals {
                all_finals[b] += 1;
            }
        }
        assert!(all_popped.iter().all(|&c| c == 1), "each item owned exactly once");
        assert!(all_finals.iter().all(|&c| c == 1), "each bucket elects exactly one final");
    }
}
