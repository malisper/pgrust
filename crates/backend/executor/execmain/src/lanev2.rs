//! Lane executor v2 — the operator→operator batched execution lane (production
//! rebuild). See `docs/design/lane-executor-v2.md`.
//!
//! Control model: **push** (Source → Operator → Sink), with a pull adapter at
//! the pipeline root because PostgreSQL's executor is Volcano/pull — the lane
//! is a push island that doles one tuple per `exec_proc_node` call out of the
//! root adapter's capacity-one buffer. The skeleton (traits + driver + root
//! adapter) lives in `lanev2/push.rs`; this file owns the per-scan refuse-sets
//! and the scan pipelines (source + scalar filter/project operator). The
//! conversion changes ONLY who calls whom: the batch staging primitives, the
//! one-row-at-a-time scalar emit, their order, and the refuse-sets are exactly
//! the Phase-1 pull drive's — byte-identical output.
//!
//! ALL substantive lane logic lives in this module, kept deliberately separate
//! from the byte-identical Volcano row-executor spine (`procnode.rs`,
//! `nodeseqscan`, `nodeagg`, …). The existing executor is touched in only a
//! handful of thin, mechanical spots:
//!   * `procnode::seq_scan_arm` — a 3-line dispatch hook (`if enabled() { if let
//!     Some(r) = try_own_seq_scan()? { return Ok(r) } }`) that falls through to
//!     the UNCHANGED per-tuple path on refuse;
//!   * `nodeseqscan::SeqScanState` — a two-`u32` page-batch cursor + accessors
//!     (the one-tuple-per-call drive needs its position to survive the Volcano
//!     call boundary, so this state must live on the node);
//!   * `executils::BatchSource` — the shared pull seam trait (it cannot live
//!     here: `nodeagg` re-exports it as `AggBatchSource`, and `nodeagg` cannot
//!     depend on `execmain` without a crate cycle, so the trait sits in the
//!     shared `executils` seam both crates already depend on).
//! Disabling or deleting the lane is therefore local: drop this module + the
//! thin hook, and the C-identical executor is exactly as before.
//!
//! Gated ON by default (as of 2026-07-14) via the `PGRUST_LANE_V2` env var;
//! `PGRUST_LANE_V2=0`/`off` is the explicit kill switch — the operational
//! escape hatch and the A/B lever. Env-var gating mirrors `jit_deform`'s
//! `PGRUST_JIT_DEFORM` switch and is byte-identity-safe (no `pg_settings` /
//! `SHOW ALL` row). Harness OFF arms must set `PGRUST_LANE_V2=0` explicitly.

mod batch_source;
mod census;
pub mod coverage;
mod dml;
mod express;
mod exprkey;
mod indexsource;
mod router;
mod runtime_agg;
mod runtime_agg_sorted;
mod runtime_bitmap;
mod runtime_distinct;
mod runtime_hashjoin;
mod runtime_instr;
pub(crate) mod runtime_nlindex;
mod runtime_partwise;
mod runtime_plaindistinct;
mod runtime_scan;
mod runtime_sort;
mod standing_channel;
mod push;
mod row_emit;
mod runtime_passthrough;
mod stmt_task; // GL-STMTTASK-1: serial statement as a dop-1 pool task
mod write_blockrun; // W2a inc-2 worker-direct block-run writes
mod write_funnel; // W0 funnel-into-writer admission (parallel-writes design §4)
pub(crate) use runtime_passthrough::try_passthrough_funnel;
pub use runtime_passthrough::funnel_engagements;
pub(crate) use stmt_task::{try_stmt_task, InlineRun as StmtInlineRun, StmtTaskVerdict};
pub use stmt_task::{stmt_task_engagements, stmt_task_inline_count};
pub use write_funnel::ctas_funnel_engagements;
mod rowmode;
mod rowmode_tail;
mod stats;
mod tail_source; // WS-Q wave-3: T3 source-form tail hosts (contract §3.1)
mod windows;

pub use exprkey::ExprKeyState;
/// GL-SLEASE-1 accounting-witness tick, re-exported for the execmain seam
/// (the serial-lease acquire lives outside the lane module tree).
pub(crate) use stats::{
    tick_serial_lease, tick_serial_lease_admitted, tick_serial_lease_donation,
    tick_serial_lease_floor_crossing,
};
pub(crate) use census::{census_armed, record_execution as census_record};
pub(crate) use dml::try_own_modify_table;
pub(crate) use indexsource::try_own_agg_over_index_only_source;
pub(crate) use router::engine_runtime_active;
pub(crate) use router::query_start as router_query_start;
pub(crate) use rowmode::merge_join_pull_verdict;
pub(crate) use rowmode::try_own_project_set;
pub(crate) use rowmode_tail::{cte_scan_pull_verdict, lock_rows_pull_verdict, material_pull_verdict, memoize_pull_verdict, merge_append_pull_verdict, recursive_union_pull_verdict, set_op_pull_verdict, try_own_function_scan, try_own_named_tuplestore_scan, try_own_sample_scan, try_own_table_func_scan, try_own_tid_range_scan, try_own_tid_scan, values_scan_pull_verdict, work_table_scan_pull_verdict};
pub(crate) use windows::try_own_window_agg;
pub(crate) use windows::try_own_window_agg_t2;
// --- WS-R T2-B (wave-3) ---
pub(crate) use windows::try_own_window_agg_t2b;
// --- end WS-R T2-B ---
#[cfg(test)]
pub(crate) use dml::{dml_set_for_tests, DML_OWNED_FOR_TESTS, DML_SHAPE_REFUSED_FOR_TESTS};
#[cfg(test)]
pub(crate) use express::{express_set_for_tests, EXPRESS_OFF, EXPRESS_OWNED_FOR_TESTS, EXPRESS_POINT, EXPRESS_STRUCTURED};
#[cfg(test)]
pub(crate) use indexsource::{indexsource_set_for_tests, INDEXSOURCE_OWNED_FOR_TESTS};
#[cfg(test)]
pub(crate) use rowmode::{mergejoin_set_for_tests, rowmode_set_for_tests, ROWMODE_MJ_OWNED_FOR_TESTS, ROWMODE_OWNED_FOR_TESTS};
#[cfg(test)]
pub(crate) use rowmode_tail::tail_owned_probe_for_tests;
#[cfg(test)]
pub(crate) use tail_source::{
    scans_t3_set_for_tests, scans_t3_shape_set_for_tests, t3_owned_probe_for_tests,
    t3_sort_child_probe_for_tests,
};
#[cfg(test)]
pub(crate) use windows::{windows_set_for_tests, WINDOWS_OWNED_FOR_TESTS};
#[cfg(test)]
pub(crate) use windows::{windows_t2_set_for_tests, WINDOWS_T2_OWNED_FOR_TESTS};
// --- WS-R T2-B (wave-3) ---
#[cfg(test)]
pub(crate) use windows::{windows_t2b_set_for_tests, WINDOWS_T2B_OWNED_FOR_TESTS};
// --- end WS-R T2-B ---
#[cfg(test)]
pub(crate) use census::{
    attribution_for_tests as census_attribution_for_tests,
    census_rows_for_tests,
    planstate_kind_name_for_tests as census_planstate_kind_name_for_tests,
};
// --- WS-T wave-3 (dml inc-2b/3a) ---
pub(crate) use dml::try_own_lock_rows_dml;
#[cfg(test)]
pub(crate) use dml::{
    dml_ud_set_for_tests, DML_LANEFED_FOR_TESTS, DML_LOCKROWS_OWNED_FOR_TESTS,
};
// --- end WS-T wave-3 ---

use std::sync::OnceLock;

use ::executils::{EStateData, ExecSlotId};
use ::types_error::PgResult;

use push::{
    drain_pipeline, drain_pipeline_chain, pull_step, pull_step_chain, Batch, BatchEmit,
    BatchSink, OpStatus, Operator, RootAdapter, Sink, SinkFeed, Source, TupleOp,
};
use stats::{RefuseReason, ShapeClass};

/// Master switch for lane-v2. Default ON since 2026-07-14 (evidence:
/// notes/lane-timed-regress-2026-07-14.md — byte-identical regress ×6,
/// timed-regress median 1.000, all floors green). The primary control is the
/// `pgrust.lane_executor` bool GUC (USERSET; its session TLS backing cell is
/// read here directly, so SET / SET LOCAL re-evaluates the gate on the next
/// query). The `PGRUST_LANE_V2` boot env var sets the GUC's startup default
/// (`=0`/`off` → default off, PGC_S_ENV_VAR) and remains the fleet-harness /
/// kill-switch path.
#[inline]
pub fn enabled() -> bool {
    ::guc_tables::backing::pgrust_lane_executor()
}

/// GL-Q4142 — the runtime morsel arms' parallel gate, fail-closed on the
/// SCAN rather than only on the process role.
///
/// Every runtime arm must refuse inside parallel machinery, because its
/// morsel source is a PRIVATE part-global granule map: engaging under a
/// classic Gather makes each participant walk the whole relation, so every
/// partial aggregate is the global answer and the result comes back inflated
/// by the participant count (see the hazard note at
/// `runtime_passthrough::…` — "every participant emits the complete result").
///
/// The historical gate was `IsParallelWorker() || IsInParallelMode()` — two
/// PROCESS-ROLE predicates. Neither is a property of the plan:
/// `ParallelWorkerNumber` is a bare thread-local with no adoption path (any
/// thread other than the one `ParallelWorkerMain` stamped reads -1), and a
/// Gather worker's serialized plan clears `parallelModeNeeded`, so
/// `IsInParallelMode()` is false there too. A single misread of either one
/// turns into a wrong answer.
///
/// `ss.is_parallel()` (`parallel_aware || pstate.is_some()`) is the
/// structural fact the arm actually needs: this scan divides its work
/// through the shared `phs_nallocated` cursor, so a private range drive over
/// it is never sound. It is the same posture the refsort gate
/// (`IsParallelWorker() || ss.is_parallel()`) and the bitmap gate
/// (`parallel_aware || pstate.is_some()`) already take. Widening a refusal
/// is always byte-safe: the classic parallel arm owns the shape.
#[inline]
pub(crate) fn runtime_in_parallel_machinery(ss: &::nodeseqscan::SeqScanState<'_>) -> bool {
    runtime_in_parallel_role() || ss.is_parallel()
}

/// The ROLE half of the gate above, on its own — for the runtime arms whose
/// input is not a `SeqScan` (bitmap heap scan, index feeds, partitionwise
/// children), so `runtime_in_parallel_machinery` has no node to read.
///
/// NAMED, not open-coded, precisely because it is NOT sufficient on its own
/// (GL-Q4142): both predicates are process-role facts, not plan facts —
/// `ParallelWorkerNumber` is a bare thread-local with no adoption path, and a
/// Gather worker's serialized plan clears `parallelModeNeeded`. Every caller
/// must ALSO carry a structural refusal on its own input:
///   * `runtime_bitmap` — `scan.parallel_aware || scan.pstate.is_some()`,
///     checked immediately before this gate;
///   * `runtime_nlindex` / the index feeds — `iss_ParallelAware` /
///     `ioss_ParallelAware` in the `index_*_refuse_reason_ex` ladders;
///   * every `SeqScan`-fed arm — `seq_scan_cb_granule_geometry` /
///     `seq_scan_heap_block_geometry` refuse a parallel scan outright, so a
///     private morsel map cannot be built for one at all.
#[inline]
pub(crate) fn runtime_in_parallel_role() -> bool {
    ::parallel::IsParallelWorker() || ::xact::IsInParallelMode()
}

/// Combined gate for the wave-2 row-mode TAIL dispatch hooks in procnode
/// (se2-cost-fix): the process-static, default-OFF `PGRUST_LANE_V2_ROWMODE`
/// knob FIRST — one relaxed byte load + compare that short-circuits at
/// default config before the lane-executor GUC read — then `enabled()`.
/// The order is semantics-free (both are pure reads and the tail try_own_*
/// bodies re-check the knob before any tick), but it is the difference
/// between ~2 and ~8 instructions per PULL on the per-row tail arms
/// (values_scan on a 100-row multi-VALUES INSERT was the se2-dmlcost batch
/// letter: +39 instr/row at knob-OFF).
#[inline]
pub(crate) fn rowmode_tail_active() -> bool {
    // se-delegtax SH-F: knob-only since the verdict form — the lane-executor
    // GUC gate rides the per-execution es_lane_leaf_fast byte (fast path)
    // and verdict_slow's enabled() head (slow path), so the knob-ON per-pull
    // path no longer pays the GUC TLS read. Knob-OFF stays one relaxed byte
    // load + compare (the se2-cost law this gate exists for).
    rowmode::rowmode_enabled()
}

/// WS-Q wave-3: dispatch-arm gate for the T3 source form
/// (`PGRUST_LANE_V2_SCANS_T3`), consulted ONLY by the six T3 shapes'
/// procnode arms (never the VALUES/Material/... arms — the m4 letter paths
/// stay byte-identical). Knob-OFF cost on those six arms: one relaxed byte
/// load + branch after `rowmode_tail_active()`'s short-circuit.
#[inline]
pub(crate) fn scans_t3_active() -> bool {
    tail_source::scans_t3_enabled() && enabled()
}

/// Combined gate for the WS-N modify_table dispatch hook — same shape and
/// rationale as `rowmode_tail_active` (per statement rather than per pull).
#[inline]
pub(crate) fn dml_active() -> bool {
    dml::dml_enabled() && enabled()
}

// ---------------------------------------------------------------------------
// Wave-4 TIER-B default resolution (flip manifest §2; branch
// se/wave4-flips-tierB). APPEND REGION — tierB owns this block (manifest §4
// file-ownership table); later Tier-B rows extend it BELOW, tierA never
// edits it.
// ---------------------------------------------------------------------------

/// Resolve a wave-4 TIER-B default-flipped knob (flip manifest §2). The
/// precedence table, unit-tested below (`tier_b_precedence`):
///
///   1. The per-shape EXPLICIT knob always wins, in both directions —
///      "flips never delete knobs": the `=1`/`on` and `=0`/`off` spellings
///      are permanent, and a SET-but-unrecognized spelling keeps its
///      pre-flip meaning (OFF — a typo fails safe to legacy behavior, the
///      same `matches!(Ok("1") | Ok("on"))` parse every lanev2 knob shipped
///      with).
///   2. Knob UNSET: the single escape hatch `PGRUST_LANE_V2_DEFAULTS=legacy`
///      reverts the default to its pre-wave-4 value (OFF) for EVERY Tier-B
///      row at once (the manifest's one-lever rollback).
///   3. Hatch absent / any other hatch value: the flipped default (ON).
///
/// Pure function so the precedence is unit-testable without process-global
/// env mutation; each caller is a Tier-B-flipped knob's one-shot `#[cold]`
/// resolve tail, which passes its own env read + `tier_b_defaults_env()` and
/// caches the verdict in its existing AtomicU8 (the hatch itself needs no
/// cache — it is only ever read inside those one-shot tails, never on a
/// fast path). No refusal-vocab or census-key involvement (manifest §2).
// The allow dies with the first Tier-B flip commit (its resolve tail is the
// first caller); this commit lands the resolver + tests only.
#[allow(dead_code)]
pub(super) fn tier_b_flip_default(explicit: Option<&str>, defaults_hatch: Option<&str>) -> bool {
    match explicit {
        Some("1") | Some("on") => true,
        Some(_) => false,
        None => !matches!(defaults_hatch, Some("legacy")),
    }
}

/// The one `PGRUST_LANE_V2_DEFAULTS` env read (flip manifest §2 — the single
/// rollback lever for all Tier-B rows). Called only from Tier-B knobs'
/// one-shot cold resolve tails.
// Same transient allow as above — dies with the first flip commit.
#[allow(dead_code)]
pub(super) fn tier_b_defaults_env() -> Option<String> {
    std::env::var("PGRUST_LANE_V2_DEFAULTS").ok()
}

#[cfg(test)]
mod tier_b_tests {
    use super::tier_b_flip_default;

    /// The manifest §2 precedence table: explicit knob > hatch > new default.
    #[test]
    fn tier_b_precedence() {
        // 3. Knob unset, hatch absent/other -> the flipped default (ON).
        assert!(tier_b_flip_default(None, None));
        assert!(tier_b_flip_default(None, Some("")));
        assert!(tier_b_flip_default(None, Some("new")));
        assert!(tier_b_flip_default(None, Some("Legacy"))); // exact spelling only
        // 2. Knob unset, hatch=legacy -> the pre-wave-4 default (OFF).
        assert!(!tier_b_flip_default(None, Some("legacy")));
        // 1a. Explicit ON wins over the hatch.
        assert!(tier_b_flip_default(Some("1"), Some("legacy")));
        assert!(tier_b_flip_default(Some("on"), Some("legacy")));
        // 1b. Explicit OFF wins over the flipped default (permanent spelling).
        assert!(!tier_b_flip_default(Some("0"), None));
        assert!(!tier_b_flip_default(Some("off"), Some("anything")));
        // 1c. Set-but-unrecognized keeps its pre-flip meaning (OFF), in both
        // hatch states — a typo never silently arms or re-arms a lane.
        assert!(!tier_b_flip_default(Some("2"), None));
        assert!(!tier_b_flip_default(Some("true"), Some("legacy")));
    }
}

/// Engagement trace (verification aid, no perf path): `PGRUST_LANE_V2_TRACE=1`
/// logs lane engagement events to stderr. Resolved once per process.
fn lane_trace(event: &str) {
    if lane_trace_enabled() {
        eprintln!("[lane-v2] {event}");
    }
}

/// Whether the engagement trace is armed — callers gating format! work
/// (router trace lines) check this before building the string.
fn lane_trace_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        matches!(std::env::var("PGRUST_LANE_V2_TRACE").as_deref(), Ok("1") | Ok("on"))
    })
}

/// GL-ROWMODE-1: emit a row-mode OWNED engagement trace at most ONCE per
/// (class × execution), deduped through the estate-resident
/// `es_lane_trace_owned` bitmask (bit = `ShapeClass` discriminant; workers
/// dedup on their own estates, so per-worker engagement stays visible).
///
/// The OWNED verdict/tick cadence is per pull (row-mode law §3.3) and is
/// NOT changed here — only the trace line's cadence is. A per-pull trace on
/// a delegation leaf that sits on a per-inner-row pull path is one
/// format! + locked stderr write syscall per inner row per worker: a merge
/// join's Materialize inner re-pulled across its mark/restore cycle emitted
/// ~1.2M lines per statement at w16 and turned a ~50ms statement into
/// ~10-17s (~200-330x) under a trace-armed boot — which read as a
/// legacy-engine collapse in the rowflip measurement vehicle (that vehicle
/// boots with the trace armed) and was UNDER-read by EXPLAIN ANALYZE (the
/// instrumented gate refuses before the trace). Engagement witnesses assert
/// line PRESENCE, never per-pull counts (lane-rowmode-tail-e2e asserts off
/// the stats TSV), so first-pull-only is witness-preserving.
///
/// Trace-disarmed cost is one cached-bool check — identical to a bare
/// `lane_trace` call; the line closure runs only on the first armed emit.
pub(self) fn lane_trace_owned_once<F: FnOnce() -> String>(
    class: ShapeClass,
    estate: &mut ::executils::EStateData<'_>,
    line: F,
) {
    if !lane_trace_enabled() {
        return;
    }
    let bit = 1u64 << (class as usize);
    if estate.es_lane_trace_owned & bit == 0 {
        estate.es_lane_trace_owned |= bit;
        lane_trace(&line());
    }
}

/// Process-static diagnostics mask for the row-mode LEAF drives' owned path
/// (se-delegtax SH-B): bit 0 = lane accounting armed (`stats::armed()` —
/// PGRUST_LANE_V2_STATS / PGRUST_LANE_V2_COVERAGE), bit 1 = engagement trace
/// armed (PGRUST_LANE_V2_TRACE). Both inputs are process-static envs; ONE
/// relaxed byte load + zero-test replaces two OnceLock fast paths per owned
/// pull. Load deletion, not branch reshaping — the se-express-adm INC-1
/// lesson: dist fat-LTO codegen already fuses branches but cannot fold two
/// distinct atomic loads into one. 0xFF = unresolved sentinel (the ROWMODE
/// AtomicU8 idiom); the resolved mask is 0..=3. Semantics are unchanged by
/// construction: mask==0 skips exactly the calls that would no-op anyway
/// (`tick_owned` under `!armed()`, `lane_trace` under `!enabled`).
static LEAF_DIAG: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0xFF);

#[inline]
pub(self) fn leaf_diag_mask() -> u8 {
    let v = LEAF_DIAG.load(std::sync::atomic::Ordering::Relaxed);
    if v != 0xFF {
        v
    } else {
        leaf_diag_resolve()
    }
}

/// se-delegtax SH-F: (re)compute the row-mode LEAF fast-admit byte
/// (`EStateData::es_lane_leaf_fast`). Called ONCE per execution at
/// standard_executor_start's InitPlan-end — every input is per-execution
/// static from that point: the lane master GUC (documented semantic:
/// SET takes effect on the NEXT query), es_instrument (set before
/// InitPlan; every es_instrumentation growth site is gated on it),
/// ENGINE capture (es_top_eflags, stored at entry), and the process-static
/// diag mask. The per-pull-dynamic gates (EPQ, scan direction) are checked
/// INLINE by the fast path itself, so the byte needs NO mid-query
/// maintenance. byte==true ⇒ the full verdict would admit and would tick
/// nothing (accounting disarmed) — every tick/capture-asserting channel
/// has diagnostics armed and therefore takes the slow path unchanged.
pub(crate) fn refresh_lane_leaf_fast(estate: &mut ::executils::EStateData<'_>) {
    estate.es_lane_leaf_fast = enabled()
        && estate.es_instrument == 0
        && !estate.engine_capture()
        && leaf_diag_mask() == 0;
    // GL-ROWMODE-1 known-divergence note (trace-armed only, once per
    // execution — bit 63 of the owned-trace dedup mask, no ShapeClass
    // conflict at N_CLASSES=41): an instrumented execution (EXPLAIN
    // ANALYZE / auto_explain) refuses every lane/runtime arm through the
    // Instrumented gates, so its timings measure the fallback executor —
    // any shape where an arm is slower OR faster than the fallback reads
    // differently under instrumentation than in plain execution. The gate
    // is load-bearing (the arms carry no per-node instrumentation
    // counters; owning an instrumented pull would fabricate zeroed
    // EXPLAIN ANALYZE node stats), so the honest posture is this
    // triage-visible marker rather than a silent divergence.
    if estate.es_instrument != 0 && enabled() && lane_trace_enabled() {
        const INSTR_NOTE: u64 = 1u64 << 63;
        if estate.es_lane_trace_owned & INSTR_NOTE == 0 {
            estate.es_lane_trace_owned |= INSTR_NOTE;
            lane_trace(
                "instrumented execution: lane arms refuse under instrumentation — \
                 instrumented timings measure the fallback executor, not the arms \
                 a plain execution elects",
            );
        }
    }
}

#[cold]
#[inline(never)]
fn leaf_diag_resolve() -> u8 {
    let mut m = 0u8;
    if stats::armed() {
        m |= 1;
    }
    if lane_trace_enabled() {
        m |= 2;
    }
    LEAF_DIAG.store(m, std::sync::atomic::Ordering::Relaxed);
    m
}

/// M5-2 liveness-battery fault injection (test-only, default-off — dead
/// unless the env var is set at server start): `PGRUST_TEST_HELPER_PANIC=
/// <arm>[,<arm>...]` (or `all`; arms: scan/agg/distinct/hashjoin/sort)
/// makes every helper of the named arm(s) panic BEFORE binding or driving.
/// This is the exact all-helpers-exit-without-driving wedge geometry of the
/// m35-spill inc-2c agg wedge (a pinned RG is invisible to pool workers, so
/// with every helper gone nobody steps it and the leader parks forever) —
/// the geometry the leader's ExitBump reap must convert into a prompt,
/// recoverable error. scripts/runtime-liveness-e2e.sh is the standing
/// battery over this knob. Resolved once per process.
fn test_helper_panic(arm: &str) {
    static KNOB: OnceLock<Option<String>> = OnceLock::new();
    let Some(v) = KNOB.get_or_init(|| std::env::var("PGRUST_TEST_HELPER_PANIC").ok()) else {
        return;
    };
    if v.split(',').any(|a| {
        let a = a.trim();
        a == "all" || a.eq_ignore_ascii_case(arm)
    }) {
        panic!("pgrust: test helper panic injection ({arm})");
    }
}

// ===========================================================================
// Phase-3 qual kernel: the vectorized selection-bitmap qual for lane-owned
// filtered scans. This restores the fast path the NON-lane `WithQual` drive
// already has (`scan_batch_probe` → `exec_seq_scan_batch`): a kernel-shaped
// `col CMP const` qual (`Kernel::QualScanVarCmpConst`) is evaluated over the
// whole staged page batch by `qual_bitmap_cmp_const` (execexpr/steps.rs —
// chunked so LLVM can vectorize the compare) into a selection bitmap, and the
// lane's filter/project segment iterates ONLY the survivors instead of
// running `exec_qual` scalar per staged row. All of the staging + bitmap +
// forced-fallback-bit machinery is the EXISTING `BatchSoa` flow in
// `nodeseqscan` (`seq_scan_batch_soa_prepare` / `seq_scan_next_pagebatch` /
// `seq_scan_batch_fetch`); the lane only arms it and consumes the bitmap.
// ===========================================================================

/// Arm the SoA deform + selection-bitmap qual for a lane-owned filtered
/// SeqScan pipeline. Admission generalizes `scan_batch_probe`'s to the
/// clause census: the qual must be an AND of scan-Var-CMP-Const clauses
/// (`scan_cmp_const_clauses` — non-erroring, non-volatile by construction,
/// which is why only these shapes are admitted; 1 clause = the fused
/// kernel), and `seq_scan_batch_soa_prepare` internally refuses a
/// non-fixed-width column prefix (the scalar per-row path then continues
/// unchanged). `qual_only`: single-clause staging deforms the qual column
/// only, multi-clause the clause-covering prefix; surviving rows deform
/// lazily per-row — identical to the non-lane `exec_seq_scan_batch` drive.
/// No-op (memo hit) when already armed, so per-pull callers pay one
/// load+test.
///
/// `stitch`: additionally arm the tier-2 stitched body for the qual — set
/// ONLY by drain-pipeline callers (feeds into breakers). Pull-one-tuple
/// pipelines keep the AOT bitmap tier (design rule: stitched segments exist
/// only on drain pipelines).
fn arm_seq_scan_qual_bitmap<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    ctx: &str,
    stitch: bool,
) {
    if !::nodeseqscan::seq_scan_batch_qual_bitmap_armed(ss) {
        let Some(q) = ss.ss.qual.as_deref() else { return };
        let Some(c) = q.scan_cmp_const_clauses() else {
            // strsearch contains-LIKE kernel (single `col LIKE '%lit%'`
            // clause census): varkey-staged text lane + one memmem bitmap
            // pass per batch; every other admission stays per-row. The
            // stitch arms below no-op on this shape (nquals = 0, unused
            // deform plan), so the registration is this one call.
            if q.scan_contains_clause().is_some()
                && ::nodeseqscan::seq_scan_batch_soa_prepare_contains(ss, estate)
            {
                lane_trace(&format!("seqscan contains qual bitmap armed ({ctx})"));
            }
            return;
        };
        let prefix = c.clauses[..c.n as usize]
            .iter()
            .map(|&(col, _, _)| col as i32 + 1)
            .max()
            .expect("census has at least one clause");
        // Phase-3 projection stitching (drain pipelines only, like every
        // stitched segment): when the scan's projection is census-covered,
        // widen the deform prefix to its read columns so the stitched
        // projection reads the SAME staged lanes as the qual bitmap (the
        // one-deform-two-consumers coupling; the bitmap + output lanes are
        // the only currency between the segments). If the wider prefix is
        // unarmable (a non-fixed-width column inside it), fall back to the
        // qual-only prefix — projection hosting refuses, current per-row
        // projection behavior untouched (fail closed).
        let proj_prefix = if stitch {
            ::nodeseqscan::seq_scan_proj_stitch_prefix(ss).unwrap_or(0)
        } else {
            0
        };
        if proj_prefix > prefix {
            ::nodeseqscan::seq_scan_batch_soa_prepare(ss, estate, proj_prefix, true, false, true);
        }
        if !::nodeseqscan::seq_scan_batch_qual_bitmap_armed(ss) {
            ::nodeseqscan::seq_scan_batch_soa_prepare(ss, estate, prefix, true, false, true);
        }
        if ::nodeseqscan::seq_scan_batch_qual_bitmap_armed(ss) {
            lane_trace(&format!("seqscan qual bitmap armed ({ctx})"));
        }
    }
    if stitch {
        ::nodeseqscan::seq_scan_stitch_arm(ss);
        ::nodeseqscan::seq_scan_proj_stitch_arm(ss, estate);
    }
}

/// The feed shapes that arm heap-scan staging (kernel-qual selection bitmap,
/// SoA prefix deform, stitched tiers, varlane staging) on a SeqScan before a
/// lane pipeline drives it. `arm_scan_staging` is the ONE seam owning the
/// arming decision + staging setup across every feed site (agg fold /
/// per-row build feeds, sort feed, join build and probe feeds), so a second
/// staging backend (pgrcolumnar column windows) plugs in by matching the scan's
/// source kind inside that helper — not by growing per-site variants.
enum ScanFeedShape<'a, 'mcx> {
    /// Row-emit feed with no SoA lane reader above the scan: arm the
    /// kernel-qual selection bitmap, and on drain pipelines (`stitch`) the
    /// tier-2 stitched body + projection stitching. `ctx` labels the
    /// lane-trace line.
    RowFeed { ctx: &'static str, stitch: bool },
    /// Hash-agg FOLD drain feed: varlane staging, or the fused full-prefix
    /// deform (forced when the fold reads lane columns or K2 wants the key;
    /// the kernel-qual bitmap is detected inside the prefix), falling back
    /// to the qual-only bitmap when the prefix is unarmable; stitched tiers
    /// armed (drain pipeline).
    HashAggFold { agg: &'a ::nodeagg::AggStateData<'mcx> },
    /// Hash-agg PER-ROW drain feed: unforced fused prefix (bitmap detected
    /// inside), qual-only bitmap fallback; stitched tiers armed.
    HashAggPerRow { agg: &'a ::nodeagg::AggStateData<'mcx> },
    /// Forced fold-prefix deform ONLY (no bitmap fallback, no stitch arm):
    /// the plain-agg fold feed, and the decide-phase admission probes (via
    /// `probe_arm_fold_prefix`). Reaches only unprojected scans (the fold
    /// deciders refuse projected ones before choosing Fold).
    FoldPrefix { agg: &'a ::nodeagg::AggStateData<'mcx> },
}

/// Arm the scan staging a lane feed consumes, per feed shape. Idempotent at
/// every site (re-preparing the same shape is a no-op; the bitmap arm
/// early-returns once armed). This is the single seam the pgrcolumnar staging
/// variant (PgrcolumnarSource tranche) plugs into: every arm below stages
/// through `nodeseqscan`'s SoA seam, whose batch primitives dispatch on the
/// scan's AM inside `tableam` — heap scans stage page batches
/// (`heap_getnextpagebatch` + `heap_batch_deform_soa`), pgrcolumnar scans stage
/// column windows (`next_window` + `batch_deform`, <= WINDOW_ROWS <=
/// SOA_MAX_ROWS, RG/granule/block pruning inside the staging call) — so the
/// feed sites stay untouched and one arm serves both source kinds. The
/// pgrcolumnar-only differences live below the seam: the fill honors
/// `lane_fill_wanted`/`dict_want` (dict-lane publication), the store slot is
/// the scan's virtual slot (`store_slot`, needed columns only), and the
/// prefix publish is a virtual-slot no-op.
fn arm_scan_staging<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    shape: ScanFeedShape<'_, 'mcx>,
) -> PgResult<()> {
    // PREWHERE v1 (pgrcolumnar scans with a qual, phase4 design §3): try the
    // lane-qual arm FIRST — it subsumes the kernel-bitmap arms (staged
    // clauses cheapest-first with zone folds + per-clause late
    // materialization, the dict text tier, hybrid requal tails) over the
    // same forced full-prefix deform, widened to the feed's own SoA ask.
    // Varlane fold feeds COEXIST (q22coexist): the fold's one varlena column
    // joins the lane's prefix ask — the pgrcolumnar (virtual-)prefix deform
    // hosts any column type, and the lane's completing deform fills it for
    // survivor windows, exactly the rows the fold touches (the fold drain
    // walks the selection bitmap and the guard proof restricts to it).
    // Refusal falls through to the heap-shaped arms below (the varkey
    // staging for varlane folds), byte-safely.
    if ::nodeseqscan::seq_scan_is_pgrcolumnar(ss) {
        let min_prefix = match &shape {
            ScanFeedShape::RowFeed { .. } => 0,
            ScanFeedShape::HashAggFold { agg } | ScanFeedShape::HashAggPerRow { agg } => {
                if ss.ss.ps_ProjInfo.is_none() {
                    fused_agg_soa_prefix(agg, ss).unwrap_or(0)
                } else {
                    0
                }
            }
            ScanFeedShape::FoldPrefix { agg } => fused_agg_soa_prefix(agg, ss).unwrap_or(0),
        };
        // Every varlena fold column joins the lane's prefix ask — the single
        // varkey-shaped column AND the multi-varlena set (lane-v2-
        // dictminmax): the pgrcolumnar (virtual-)prefix deform hosts any column
        // type, and vguard columns must be staged for the fold + guard proof.
        let vcol = match &shape {
            ScanFeedShape::HashAggFold { agg } => ::nodeagg::agg_lanefold_plan(agg)
                .and_then(|p| p.vguards.iter().copied().max()),
            _ => None,
        };
        let ask = match vcol {
            Some(c) => min_prefix.max(c as i32 + 1),
            None => min_prefix,
        };
        if ::nodeseqscan::seq_scan_cb_prewhere_arm(ss, estate, ask)? {
            if vcol.is_some() {
                lane_trace("cbstore prewhere+varlane dual arm engaged");
            }
            // Multi-key dict co-arm on the PREWHERE-owned batch (the two-key
            // dict-int + text grouped-count shape whose qual sits on the
            // text key itself): count(*)-only fold plans take decide's
            // `plan.cols.is_empty()` shortcut, so no decide-phase probe ever
            // registered the text component as a dict lane, and the early
            // return here used to skip the arm entirely — the multi-key feed
            // then refused (`MultiKeyShape`: no dict registration) down to
            // the per-row arrival probe. `seq_scan_cb_columnar_arm` registers
            // the consumer ON the live lane batch (its forced full prefix
            // covers the feed's ask by construction: `ask` >= the fused
            // prefix); the fill answers the component as codes+dict and the
            // pack pre-pass interns per (epoch, code). Refusal falls through
            // byte-identically. Measured (10M bank sorted-v2, serial hot):
            // this two-key shape 0.572 -> 0.375 s.
            //
            // DELIBERATELY NOT the single-key dict-group co-arm: for the
            // interned-int-key class (single dict key under a SELECTIVE qual) the
            // survivors-per-epoch ~ distinct-codes-per-epoch, so the lazy
            // per-(epoch, code) resolve dominates and dict-code grouping
            // measured SLOWER than the K2 staged text probe it would replace
            // (0.381 -> 0.413 s serial, +10% parallel; jobs
            // explain-channel 1783907728 / -1783908658). The K2
            // text probe stays the single-key path under PREWHERE; the
            // multi-key arm has no such fallback (its alternative is the
            // per-row arrival probe, far behind the packed feed even with
            // cold resolves).
            if let ScanFeedShape::HashAggFold { agg } = &shape {
                if try_arm_cb_multikey_dict(agg, ss, estate) {
                    lane_trace("cbstore multikey co-armed on prewhere lane");
                }
            }
            return Ok(());
        }
    }
    match shape {
        ScanFeedShape::RowFeed { ctx, stitch } => {
            arm_seq_scan_qual_bitmap(ss, estate, ctx, stitch);
        }
        ScanFeedShape::HashAggFold { agg } => {
            // Arm the SoA page-batch deform + kernel-qual bitmap for the
            // fused drive when the whole read prefix is knowable
            // (unprojected scans only: with a projection the agg reads
            // output columns, which are not commensurable with scan-column
            // prefixes). ONE deform serves both consumers:
            // `seq_scan_batch_soa_prepare` detects the kernel qual inside
            // the prefix and arms the selection bitmap on the same staged
            // SoA the fold lanes read. When no prefix is knowable
            // (projected / shape-unknown), fall back to the qual-only bitmap
            // arm so a kernel-shaped filter still vectorizes (survivors
            // deform lazily per-row). The fold feed FORCES the deform when
            // the fold reads lane columns (the <3-column break-even is a
            // deform+gather artifact; the fold consumes the columns
            // directly).
            let soa_prefix = if ss.ss.ps_ProjInfo.is_none() {
                fused_agg_soa_prefix(agg, ss).unwrap_or(0)
            } else {
                0
            };
            if let Some(vcol) =
                ::nodeagg::agg_lanefold_plan(agg).and_then(lanefold_varlane_col)
            {
                // Varlena lane: re-arm the varkey staging (idempotent; the
                // decide-phase probe already proved it arms).
                let armed = ::nodeseqscan::seq_scan_batch_soa_prepare_varlane(ss, estate, vcol);
                debug_assert!(armed, "varlane re-arm is idempotent");
            } else if ::nodeagg::agg_lanefold_plan(agg)
                .is_some_and(|p| !p.vguards.is_empty())
            {
                // Multi-varlena fold (2+ varlena lanes): re-arm the pgrcolumnar
                // virtual-prefix staging the decide-phase probe proved
                // (idempotent). A lost arm leaves the SoA unarmed and the
                // feed's (None, _) route asserts no lane reader — so a
                // failed re-arm here would be a bug, not a silent demote.
                let armed = try_arm_cb_multivar(agg, ss, estate)?;
                debug_assert!(armed, "multi-varlena re-arm is idempotent");
            } else if soa_prefix > 0 {
                // Force the SoA deform when the fold reads lane columns, OR when
                // the K2 deferred probe could host this shape (the K2 key lane
                // must be staged even for count(*)-only plans, whose fold reads
                // nothing — the prefix covers grouping columns, so arming stages
                // the key). A non-fixed-width prefix still refuses to arm, and
                // the feed then keeps the arrival probe — byte-safe either way.
                let force = ::nodeagg::agg_lanefold_plan(agg)
                    .is_some_and(|plan| !plan.cols.is_empty())
                    || scan_k2_wanted(agg)
                    || scan_mk_plan_wanted(agg);
                let was = ::nodeseqscan::seq_scan_batch_qual_bitmap_armed(ss);
                ::nodeseqscan::seq_scan_batch_soa_prepare(ss, estate, soa_prefix, false, force, true);
                if ::nodeseqscan::seq_scan_batch_soa(ss).is_none() {
                    // Full-prefix deform unarmable (non-fixed-width column in
                    // the prefix) or declined (break-even). Try the pgrcolumnar
                    // dict-group columnar arm (§2.1) first — count(*)-only
                    // plans reach here without decide's probe-arm (their fold
                    // reads no lane columns, but K2 wants the key staged).
                    // Otherwise: a column-reading fold plan cannot get here —
                    // `decide_agg_lane` probe-armed this prefix (or armed
                    // dict-group, which the re-prepare above keeps) before
                    // choosing Fold — so the SoA has no fold reader and the
                    // qual-only bitmap arm is safe.
                    if !try_arm_cb_dictgroup(agg, ss, estate)
                        && !try_arm_cb_multikey_dict(agg, ss, estate)
                    {
                        arm_seq_scan_qual_bitmap(ss, estate, "agg fold feed, qual-only", true);
                    }
                } else if !was && ::nodeseqscan::seq_scan_batch_qual_bitmap_armed(ss) {
                    lane_trace("seqscan qual bitmap armed (agg fold fused deform)");
                }
            } else {
                // Fold with no knowable prefix = a plan reading no lane
                // columns (count(*)-only); the bitmap is the only SoA user.
                arm_seq_scan_qual_bitmap(ss, estate, "agg fold feed", true);
            }
            // Tier-2 arm for the fused-deform-armed bitmap (drain feed);
            // idempotent, no-op when the bitmap is not armed.
            ::nodeseqscan::seq_scan_stitch_arm(ss);
        }
        ScanFeedShape::HashAggPerRow { agg } => {
            // Same prefix bound and fallbacks as the fold arm (comment
            // there), but the per-row feed reads no SoA columns, so the
            // deform is never forced.
            let soa_prefix = if ss.ss.ps_ProjInfo.is_none() {
                fused_agg_soa_prefix(agg, ss).unwrap_or(0)
            } else {
                0
            };
            if soa_prefix > 0 {
                let was = ::nodeseqscan::seq_scan_batch_qual_bitmap_armed(ss);
                ::nodeseqscan::seq_scan_batch_soa_prepare(ss, estate, soa_prefix, false, false, true);
                if ::nodeseqscan::seq_scan_batch_soa(ss).is_none() {
                    // Unarmable/declined full prefix; the per-row feed reads
                    // no SoA columns, so fall back to the qual-only bitmap.
                    arm_seq_scan_qual_bitmap(ss, estate, "agg per-row feed, qual-only", true);
                } else if !was && ::nodeseqscan::seq_scan_batch_qual_bitmap_armed(ss) {
                    lane_trace("seqscan qual bitmap armed (agg per-row fused deform)");
                }
            } else {
                arm_seq_scan_qual_bitmap(ss, estate, "agg per-row feed", true);
            }
            // Tier-2 arm for the fused-deform-armed bitmap (drain feed).
            ::nodeseqscan::seq_scan_stitch_arm(ss);
        }
        ScanFeedShape::FoldPrefix { agg } => {
            let prefix = fused_agg_soa_prefix(agg, ss).unwrap_or(0);
            ::nodeseqscan::seq_scan_batch_soa_prepare(ss, estate, prefix, false, true, true);
        }
    }
    Ok(())
}

/// Multi-varlena fold staging (lane-v2-dictminmax, the multi-varlena
/// `MIN(text1), MIN(text2)` shape): a plan whose lane set carries 2+ varlena
/// columns (or one varlena among fixed-width lanes) is unhostable by the
/// heap paths — the fixed-width prefix deform cannot stage `attlen == -1`
/// and the varkey pass stages exactly one column — but the pgrcolumnar
/// virtual-prefix staging hosts ANY column type. Arm it: PREWHERE for
/// qualled scans (it owns the staging + selection bitmap; ask widened to
/// every fold/vguard column), the offset-free columnar arm for bare scans.
/// False = not that shape, or the staging refused — the decider keeps the
/// per-row feed, byte-safely.
fn try_arm_cb_multivar<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if !::nodeseqscan::seq_scan_is_pgrcolumnar(ss) {
        return Ok(false);
    }
    let Some(plan) = ::nodeagg::agg_lanefold_plan(agg) else {
        return Ok(false);
    };
    if plan.vguards.is_empty() || lanefold_varlane_col(plan).is_some() {
        return Ok(false);
    }
    let Some(mut prefix) = fused_agg_soa_prefix(agg, ss) else {
        return Ok(false);
    };
    for &c in plan.cols.iter().chain(plan.vguards.iter()) {
        prefix = prefix.max(c as i32 + 1);
    }
    let armed = if ss.ss.qual.is_some() {
        ::nodeseqscan::seq_scan_cb_prewhere_arm(ss, estate, prefix)?
    } else {
        ::nodeseqscan::seq_scan_cb_columnar_arm(ss, estate, prefix, None)
    };
    Ok(armed && ::nodeseqscan::seq_scan_batch_soa(ss).is_some())
}

/// Decide-phase admission probe: arm the forced fold prefix NOW so an
/// unarmable prefix (non-fixed-width column) is known BEFORE committing to
/// ownership. Returns whether the SoA deform armed. Shared by the hashed and
/// plain fold deciders; the plain fold feed re-arms the identical shape (a
/// no-op) through `ScanFeedShape::FoldPrefix`.
fn probe_arm_fold_prefix<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    arm_scan_staging(ss, estate, ScanFeedShape::FoldPrefix { agg })?;
    Ok(::nodeseqscan::seq_scan_batch_soa(ss).is_some())
}

// ===========================================================================
// Standalone scan ownership: DELIBERATELY REFUSED (admission economics,
// design §4; measured on the integration bench 2026-07-11, narrow-sort class).
//
// The `try_own_*` scan entry points are reached only from the per-node
// dispatch arms — i.e. only when the PARENT is a per-tuple Volcano consumer
// (lane breakers drive their scan pipelines directly, never through these
// hooks). A lane-owned scan in that position emits one tuple per pull through
// the capacity-one adapter with NO batch consumer above and NO scan kernels
// wired yet — pure adapter overhead (narrow-sort: +3–9%), and for kernel-qual'd scans
// it PREEMPTS the row executor's own fused SoA-bitmap WithQual drive.
//
// Revisited with the Phase-3 qual kernel (2026-07-11): lane-owned filtered
// scans now carry the same selection bitmap, but for a STANDALONE scan the
// incumbent per-node drive is `exec_seq_scan_batch` — the identical bitmap
// over the identical staging, with NO pull-adapter round trip per surviving
// row on top. The lane can therefore only match-or-lose here (the narrow-sort-class
// adapter overhead stands), so the refuse stays. It shrinks when standalone
// scans gain a kernel the row drive lacks (dict/PREWHERE-class); the scan
// pipelines stay fully exercised via the agg/sort/join breaker feeds.
const STANDALONE_SCAN_NO_UPSIDE: bool = true;

/// Tiny-input row floor for standalone pgrcolumnar scan admission (the
/// `TinyInputFloor` refuse): relations below this never pay the qual-
/// translate/arm admission cascade. Default = one pgrcolumnar granule (8,192
/// rows — the store's decode/zone unit; a sub-granule scan is a handful of
/// staged windows either way, bench 2026-07-12: lane-ON == lane-OFF to noise
/// at this size, so the cascade is pure tax). `PGRUST_LANE_V2_TINY_FLOOR`
/// overrides for floor-calibration benches.
fn cb_tiny_floor() -> u64 {
    static FLOOR: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    crate::once_val(&FLOOR, || {
        std::env::var("PGRUST_LANE_V2_TINY_FLOOR")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8_192)
    })
}

// ===========================================================================
// EXPLAIN (ENGINE) capture (single-executor migration Phase 0.2, WS-C inc-1).
//
// Every helper below runs ONLY under `estate.engine_capture()` — i.e. inside
// an EXPLAIN (ENGINE, ANALYZE ...) execution (EXEC_FLAG_ENGINE_REPORT) — and
// records one EngineEvent per (node, class) into es_engine_events (dedup,
// first record wins; the emission-gate law of ea-morsels E1: no records on
// any default path, so default EXPLAIN output stays byte-identical).
//
// Verdict semantics (ea-morsels E4): the reported verdict is the PRODUCTION
// (uninstrumented) verdict where a proven ignore-instrument mirror exists
// (seqscan/cbscan via `seq_scan_refuse_reason_ex(.., true)`; the agg hashed
// route via the runtime-EA mirror; the sort feed via
// `sort_refuse_reason_runtime_ea`). Where no mirror exists the OBSERVED
// refusal is recorded and reason==Instrumented displays with the honest
// "production engine may differ" suffix (explain/src/node.rs).
// ===========================================================================

/// FusedArm attribution is DERIVED from the refusal reason (integration
/// contract, WS-C amendment 4): the fused drives record no ownership events.
fn engine_kind_for_refuse(r: RefuseReason) -> ::executils::EngineKind {
    if r == RefuseReason::AdmissionEconomicsFusedDrive {
        ::executils::EngineKind::FusedArm
    } else {
        ::executils::EngineKind::Spine
    }
}

/// Record one class verdict for a node (cold: ENGINE-capture paths only).
/// `None` = the lane owns the shape in production; `Some(r)` = the spine
/// (or, for the fused-drive economics reason, the legacy fused arm) owns it.
#[cold]
fn engine_record_verdict(
    estate: &mut EStateData<'_>,
    plan_node_id: i32,
    class: ShapeClass,
    refuse: Option<RefuseReason>,
) {
    match refuse {
        None => {
            estate.engine_record(plan_node_id, ::executils::EngineKind::Lane, class.name(), "")
        }
        Some(r) => estate.engine_record(
            plan_node_id,
            engine_kind_for_refuse(r),
            class.name(),
            r.name(),
        ),
    }
}

/// Refusal capture for the scan hooks whose reason precedes (and is
/// independent of) the instrument gate — the observed reason IS the
/// production verdict. `instr_idx == plan_node_id`
/// (procnode::instrument_node), always present under ANALYZE, which the
/// ENGINE option requires in inc-1.
#[cold]
fn engine_capture_scan_refused(
    estate: &mut EStateData<'_>,
    instr_idx: Option<u32>,
    class: ShapeClass,
    reason: RefuseReason,
) {
    if let Some(idx) = instr_idx {
        engine_record_verdict(estate, idx as i32, class, Some(reason));
    }
}

/// EXPLAIN (ENGINE) capture at the SeqScan/CbScan fusibility chokepoint: an
/// observed `Instrumented` refusal (ANALYZE wraps every node; ENGINE
/// requires ANALYZE) is re-evaluated with ONLY the instrument gate vacated —
/// the `seq_scan_fusible_runtime_ea` mechanism, E4 — so the recorded verdict
/// equals the production one. Every other observed reason is recorded
/// verbatim (those gates apply identically uninstrumented). Touches neither
/// the memoized serial verdict nor the stat counters.
#[cold]
fn engine_capture_seq_scan_verdict<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    class: ShapeClass,
    observed: Option<RefuseReason>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let Some(idx) = ss.ss.instr_idx else { return Ok(()) };
    let production = match observed {
        Some(RefuseReason::Instrumented) => seq_scan_refuse_reason_ex(ss, estate, true)?,
        other => other,
    };
    engine_record_verdict(estate, idx as i32, class, production);
    Ok(())
}

// ===========================================================================
// SeqScan ownership (Phase 1 first vertical slice, now push-driven). The
// pipeline is source → filter/project operator → root pull-adapter, over the
// same `BatchSource`-seam primitives the pull drive used
// (`seq_scan_next_pagebatch` / `seq_scan_batch_emit`).
// ===========================================================================

/// Try to let the lane *own* a `SeqScan` (scan→filter→project,
/// scalar-within-lane over row batches).
///
/// `Some(result)` = the lane drove this call (`result` is the tuple-or-end,
/// the ordinary `ExecProcNode` return); `None` = refused, and the caller must
/// run the unchanged `exec_seq_scan`. Refusing is always byte-safe.
#[inline]
pub fn try_own_seq_scan<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Standalone scan ownership: refused for heap (STANDALONE_SCAN_NO_UPSIDE
    // — the incumbent row drive carries the identical kernels), but ADMITTED
    // for pgrcolumnar scans WITH AN ARMED QUAL KERNEL: the documented exception
    // (phase4 design §7 / design-doc §4 "shrinks when standalone scans gain
    // a kernel the row drive lacks"). The incumbent pgrcolumnar per-row drive
    // (`getnextslot`) has NO SoA staging, NO kernel-qual bitmap and NO
    // dict/PREWHERE tier, so lane ownership of a QUAL'D scan is staged-
    // kernel upside. A kernel-less pgrcolumnar scan (no qual, or an unarmable
    // one) is the heap case exactly — per-pull capacity-one adapter overhead
    // with nothing to vectorize — and REFUSES: bench-gated 2026-07-12 on the
    // 2M-row pgrcolumnar microbench, where unconditional admission ran
    // count-star 1.33x, group-int (sorted-agg pull feed) 1.21x and
    // merge-join-agg 1.10x lane-ON vs lane-OFF while the qual'd shapes won
    // 0.45-0.84x; the qual-armed gate keeps the wins and returns the rest
    // to the per-row drive. Per-PULL tick cadence (once per call).
    let is_cb = ::nodeseqscan::seq_scan_is_pgrcolumnar(ss);
    if STANDALONE_SCAN_NO_UPSIDE && !is_cb {
        stats::tick_refused(ShapeClass::SeqScan, RefuseReason::AdmissionEconomicsNoConsumer);
        if estate.engine_capture() {
            engine_capture_scan_refused(
                estate,
                ss.ss.instr_idx,
                ShapeClass::SeqScan,
                RefuseReason::AdmissionEconomicsNoConsumer,
            );
        }
        return Ok(None);
    }
    if is_cb {
        // Memoized per node: the arm outcome is static, and a refused scan
        // must not re-run the fusibility + arm cascade per pulled tuple
        // (measured +20% on kernel-less count(*) shapes). A refusal is
        // byte-safe regardless of the dynamic gates, so the memoized-false
        // path is one branch; the admitted path still re-checks the
        // dynamic gates inside seq_scan_fusible every call.
        // Refusal split (refusal-audit rider, 2026-07-14): a QUAL'D scan
        // that failed to arm any staged kernel is "qual-not-vectorizable"
        // (the walker/translate residual — the countable survivor of the
        // dead fixed-width-prefix refusal); a kernel-less NO-QUAL scan is
        // the plain admission-economics refuse. Stateless per pull off the
        // memoized verdict.
        let refused_reason = if ss.ss.qual.is_some() {
            RefuseReason::QualNotVectorizable
        } else {
            RefuseReason::AdmissionEconomicsNoConsumer
        };
        match ss.cb_standalone_verdict() {
            Some(false) => {
                let r = if ss.cb_standalone_tiny() {
                    RefuseReason::TinyInputFloor
                } else {
                    refused_reason
                };
                stats::tick_refused(ShapeClass::CbScan, r);
                if estate.engine_capture() {
                    engine_capture_scan_refused(estate, ss.ss.instr_idx, ShapeClass::CbScan, r);
                }
                return Ok(None);
            }
            Some(true) => {
                if !seq_scan_fusible(ss, estate)? {
                    return Ok(None);
                }
            }
            None => {
                // Tiny-input floor (§4 endgame refuse-set, armed with the
                // noqualfeed tranche): below the floor the whole scan fits a
                // handful of windows, so lane ownership can never recover
                // even its own admission cascade (qual walk + translate +
                // arm). Checked BEFORE the cascade — the refuse costs one
                // footer metadata read, memoized. Floor = one granule
                // (8,192 rows, pgrcolumnar's zone/decode unit); PGRUST_LANE_V2_
                // TINY_FLOOR overrides for floor-calibration benches.
                if let Some(rows) = ::nodeseqscan::seq_scan_cb_total_rows(ss, estate)? {
                    if rows < cb_tiny_floor() {
                        ss.set_cb_standalone_tiny();
                        ss.set_cb_standalone_verdict(false);
                        stats::tick_refused(ShapeClass::CbScan, RefuseReason::TinyInputFloor);
                        if estate.engine_capture() {
                            engine_capture_scan_refused(
                                estate,
                                ss.ss.instr_idx,
                                ShapeClass::CbScan,
                                RefuseReason::TinyInputFloor,
                            );
                        }
                        return Ok(None);
                    }
                }
                // First call: never memoize on a dynamic-gate refusal.
                if !seq_scan_fusible(ss, estate)? {
                    return Ok(None);
                }
                // Arm the qual staging (PREWHERE lane or kernel bitmap).
                // Stitch stays off: tier-2 bodies are drain-pipeline-only,
                // and this is a per-pull feed.
                arm_scan_staging(ss, estate, ScanFeedShape::RowFeed { ctx: "standalone cbstore scan", stitch: false })?;
                let armed = ::nodeseqscan::seq_scan_batch_qual_bitmap_armed(ss);
                ss.set_cb_standalone_verdict(armed);
                if !armed {
                    stats::tick_refused(ShapeClass::CbScan, refused_reason);
                    if estate.engine_capture() {
                        engine_capture_scan_refused(
                            estate,
                            ss.ss.instr_idx,
                            ShapeClass::CbScan,
                            refused_reason,
                        );
                    }
                    return Ok(None);
                }
            }
        }
    } else if !seq_scan_fusible(ss, estate)? {
        return Ok(None);
    }
    debug_assert!(::types_scan::sdir::ScanDirectionIsForward(estate.es_direction));
    // Assemble the scan-only push pipeline. Stages are stateless unit structs
    // (cross-call position is node-resident), so per-call assembly is free.
    // End-of-stream mirrors ExecScanExtended's projected-slot clear (the
    // non-projected path returns end-of-scan without clearing).
    let clear_on_finish = ss.ss.ps_ProjInfo.as_ref().map(|p| p.pi_result_slot);
    let mut root = RootAdapter::new(clear_on_finish);
    Ok(Some(pull_step(ss, &mut SeqScanSource, &mut SeqScanFilterProject, &mut root, estate)?))
}

/// Refuse-set for the lane-v2 SeqScan pipeline (false → the caller falls
/// through to `exec_seq_scan`, byte-identically). Admits Plain / WithQual /
/// WithProject / WithQualProject over a page-batch-supporting AM.
/// Subplan- and param-bearing quals/projections are admitted (Phase 2):
/// `seq_scan_batch_emit` now runs `exec_scan_impl`'s exact arms for them —
/// pending-initplan param evaluation before the qual (and before the
/// projection, only on qual-passing rows), and the suspension-driven
/// `exec_qual_with_subplans` / `exec_project_with_subplans` drivers — so
/// initplan params demand-evaluate identically and correlated subplans run
/// scalar-per-batch-row through the same `nodesubplan` machinery.
///
/// Disarms on: EPQ, a backward/mark cursor (init eflags) or a non-forward
/// call, EXPLAIN ANALYZE (instrumented), the Bloom/EPQ variants, and AMs
/// without page-batch support. Parallel scans (leader or worker) are
/// admitted: the batched page feed acquires blocks through the shared DSM
/// block cursor (`parallel_next_block`), exactly as the per-tuple pagemode
/// walk does, so per-worker page batches partition the relation without
/// gaps or overlaps.
fn seq_scan_fusible<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // Engagement class: pgrcolumnar scans are counted apart (their admission
    // economics and corpus differ — see ShapeClass::CbScan).
    let class = if ::nodeseqscan::seq_scan_is_pgrcolumnar(ss) {
        ShapeClass::CbScan
    } else {
        ShapeClass::SeqScan
    };
    // Dynamic per-call gates: these may legitimately vary call to call.
    // (The backward gate retired with the backward-execution wave B11:
    // es_direction is forward-invariant below the run seam, deletion-prep B1.)
    if estate.es_epq_active {
        stats::tick_refused(class, RefuseReason::Epq);
        return Ok(false);
    }
    // Static verdict, memoized on the node at first evaluation: (a) stability
    // — a mid-scan REFUSE→OWN flip would silently skip the staged remainder
    // of the current page batch; (b) the fusibility cascade (expr walks + AM
    // probe) must not run once per pulled tuple on the Volcano hot path.
    // Engagement accounting ticks exactly here — once per memoized decision.
    if let Some(v) = ss.lane_verdict() {
        return Ok(v);
    }
    let refuse = seq_scan_refuse_reason(ss, estate)?;
    if estate.engine_capture() {
        engine_capture_seq_scan_verdict(ss, class, refuse, estate)?;
    }
    let v = match refuse {
        None => {
            stats::tick_owned(class);
            true
        }
        Some(r) => {
            stats::tick_refused(class, r);
            false
        }
    };
    ss.set_lane_verdict(v);
    Ok(v)
}

/// EA-on-morsels fusibility (docs/design/ea-morsels.md §6, E4): the RUNTIME
/// arms' admission proxy under EXPLAIN ANALYZE. The leader's node carries an
/// instr slot (so `seq_scan_fusible` memoizes REFUSE — correct for the
/// serial lane, whose batched drive would skip the per-node instrument
/// wrappers), but the runtime's WORKER executors are built uninstrumented,
/// so for runtime admission the Instrumented gate — and ONLY that gate — is
/// vacuous. Evaluates the same dynamic gates + the same call-invariant
/// refuse-set with the instrument check skipped; touches neither the
/// memoized serial verdict nor the engagement stat counters (the runtime
/// arm ticks its own). Cold: runs once per engagement attempt.
#[cold]
pub(super) fn seq_scan_fusible_runtime_ea<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if estate.es_epq_active {
        return Ok(false);
    }
    Ok(seq_scan_refuse_reason_ex(ss, estate, true)?.is_none())
}

/// The call-invariant half of the SeqScan refuse-set: plan shape, init-time
/// eflags, parallel wiring, instrumentation, and AM page-batch support.
/// `None` = admitted; `Some(reason)` = refused (the caller ticks accounting).
fn seq_scan_refuse_reason<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<RefuseReason>> {
    seq_scan_refuse_reason_ex(ss, estate, false)
}

/// `ignore_instrument`: the EA-on-morsels arm evaluates fusibility FOR ITS
/// UNINSTRUMENTED WORKERS — every other gate applies identically (the E4
/// rule: EA may never change the admission verdict except the instrument
/// gate itself).
fn seq_scan_refuse_reason_ex<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    ignore_instrument: bool,
) -> PgResult<Option<RefuseReason>> {
    if !ss.batch_allowed() {
        return Ok(Some(RefuseReason::ScrollMark));
    }
    if !ignore_instrument && ss.ss.instr_idx.is_some() {
        return Ok(Some(RefuseReason::Instrumented));
    }
    match ss.variant() {
        ::nodeseqscan::SeqScanVariant::Plain
        | ::nodeseqscan::SeqScanVariant::WithQual
        | ::nodeseqscan::SeqScanVariant::WithProject
        | ::nodeseqscan::SeqScanVariant::WithQualProject => {}
        ::nodeseqscan::SeqScanVariant::PlainBloom => {
            return Ok(Some(RefuseReason::BloomVariant))
        }
        ::nodeseqscan::SeqScanVariant::Epq => return Ok(Some(RefuseReason::Epq)),
    }
    // AM must support the page-batch primitives (opens the scan desc once).
    // The parallel-admitting variant: only this lane routes through it; the
    // fused agg/sort/hash drives keep `seq_scan_batch_supported`'s
    // serial-only gate.
    Ok(if ::nodeseqscan::seq_scan_batch_supported_parallel(ss, estate)? {
        None
    } else {
        Some(RefuseReason::NoPageBatch)
    })
}

/// Push source: stages heap page batches (`seq_scan_next_pagebatch` — the
/// same `BatchSource`-seam primitive `SeqScanBatchSource` wraps). Staging
/// resets the node-resident consume cursor: a fresh batch replaces the staged
/// rows.
struct SeqScanSource;

impl<'mcx> Source<'mcx> for SeqScanSource {
    type Node = ::nodeseqscan::SeqScanState<'mcx>;

    fn produce(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<Batch>> {
        // SE-R41 v2 (the page-remainder defect fix, notes/se-r41-v2.md §2):
        // a FRESH batch engagement over a scan the per-tuple row walk left
        // mid-page ADOPTS the current page's unconsumed remainder instead of
        // advancing past it (`heap_getnextpagebatch` advances pages — the
        // documented no-interleave invariant this probe discharges). The
        // probe is self-limiting: after any batch staging or adoption the
        // AM's per-tuple cursor parks at page end, so it answers None on
        // every in-fill page exhaustion.
        if let Some((start, n)) = ::nodeseqscan::seq_scan_adopt_midpage_batch(node) {
            node.set_lane_cursor(start, n);
            return Ok(Some(Batch { n }));
        }
        let n = ::nodeseqscan::seq_scan_next_pagebatch(node, estate)?;
        node.set_lane_cursor(0, n);
        if n == 0 {
            // End of scan: the per-tuple path's getnextslot clears the scan
            // slot on exhaustion (dropping its buffer pin); match it so a
            // lane-owned scan does not hold a pin until rescan/end.
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(node.ss.ss_ScanTupleSlot), mcx);
        }
        Ok((n > 0).then_some(Batch { n }))
    }
}

/// Push operator: the scan's filter→project segment. Consumes the staged
/// batch via `seq_scan_batch_emit` — `ExecScanExtended`'s body over a staged
/// batch row (reset per-tuple context, store + apply the scan qual via
/// `execexpr`, project) — pushing each surviving output slot into the sink.
/// Kernel-shaped quals (`QualScanVarCmpConst`, armed by
/// `arm_seq_scan_qual_bitmap` or the agg's fused full-prefix deform) run
/// vectorized: the staging computed a whole-batch selection bitmap
/// (`qual_bitmap_cmp_const`), and this operator walks only the survivors;
/// all other quals run scalar per-row. Filter and projection stay fused
/// within this one segment operator per the operator-model decision (design
/// §1): the push conversion inverts driver control, never the fused per-row
/// segment. Same tuples, same order, same qual/proj/NULL semantics as
/// `exec_seq_scan` → BYTE-IDENTICAL.
///
/// The consume position over the staged page batch lives on the node
/// (`SeqScanState::lane_cursor`), so a `Paused` pipeline survives the Volcano
/// per-call boundary.
struct SeqScanFilterProject;

impl<'mcx> Operator<'mcx> for SeqScanFilterProject {
    type Node = ::nodeseqscan::SeqScanState<'mcx>;

    fn pending(&self, node: &Self::Node) -> Option<Batch> {
        let (pos, n) = node.lane_cursor();
        (pos < n).then_some(Batch { n })
    }

    fn consume(
        &mut self,
        node: &mut Self::Node,
        batch: Batch,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        // Phase-3 qual kernel: when the kernel-shaped qual bitmap is staged
        // for this batch (`seq_scan_next_pagebatch` ran `qual_bitmap_cmp_const`
        // over the SoA qual column at staging), iterate ONLY the selection
        // survivors — bitmap hits plus forced fallback bits, which
        // `seq_scan_batch_fetch` re-checks per-row inside the emit — instead
        // of running the scalar qual on every staged row. Survivors come out
        // in ascending row order: same rows, same order, same per-row
        // emit/projection semantics as the scalar walk → byte-identical (the
        // kernel is non-erroring/non-volatile by admission, so skipped rows
        // have no observable evaluation). The bitmap cursor is node-resident,
        // so a `Paused` pipeline resumes exactly; `lane_cursor` is kept in
        // step for `pending`. Interrupt cadence: one check per survivor and
        // at least one per staged page (no coarser than the page-level check
        // in `heap_fetch_next_buffer` the incumbent batch drive relies on).
        if ::nodeseqscan::seq_scan_batch_qual_bitmap_ready(node) {
            loop {
                ::postgres_seams::check_for_interrupts::call()?;
                let Some(i) = ::nodeseqscan::seq_scan_batch_next_selected(node) else {
                    node.set_lane_cursor(batch.n, batch.n);
                    return Ok(OpStatus::NeedInput);
                };
                node.set_lane_cursor(i + 1, batch.n);
                if let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(node, estate, i)? {
                    if let SinkFeed::Full = out.accept(slot, estate)? {
                        return Ok(OpStatus::Paused);
                    }
                }
            }
        }
        loop {
            let (pos, n) = node.lane_cursor();
            debug_assert_eq!(n, batch.n);
            if pos >= n {
                return Ok(OpStatus::NeedInput);
            }
            // Match the per-tuple path's interrupt cadence: `exec_scan_fetch`
            // runs `check_for_interrupts` once per tuple attempt. Skipping it
            // in the batched drive would process pending interrupts / cache
            // invalidations at a different cadence than the code the lane
            // replaces; keep it identical.
            ::postgres_seams::check_for_interrupts::call()?;
            node.set_lane_cursor(pos + 1, n);
            if let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(node, estate, pos)? {
                if let SinkFeed::Full = out.accept(slot, estate)? {
                    return Ok(OpStatus::Paused);
                }
            }
        }
    }

    fn consume_batch<K: BatchSink<'mcx>>(
        &mut self,
        node: &mut Self::Node,
        batch: Batch,
        out: &mut K,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        let (pos, n) = node.lane_cursor();
        debug_assert_eq!(n, batch.n);
        out.accept_batch(&mut SeqScanBatchEmit { node }, pos, n, estate)?;
        // One cursor save per batch (not per row): breaker sinks never pause,
        // an error mid-batch aborts the query, and a rescan restages.
        node.set_lane_cursor(n, n);
        Ok(OpStatus::NeedInput)
    }

    fn arm_sort_key(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> bool {
        // The incumbent fused sort drive's matcher, shared: no qual, output
        // column 0 is exactly one scan Var the SoA plan covers.
        ::nodeseqscan::seq_scan_sortkey_direct(node, estate)
    }
}

/// `SeqScanFilterProject`'s per-row body as a `BatchEmit` face: identical
/// primitive (`seq_scan_batch_emit`) at the identical per-row interrupt
/// cadence (`consume` runs `check_for_interrupts` once per tuple attempt,
/// matching `exec_scan_fetch`).
struct SeqScanBatchEmit<'a, 'mcx> {
    node: &'a mut ::nodeseqscan::SeqScanState<'mcx>,
}

impl<'mcx> BatchEmit<'mcx> for SeqScanBatchEmit<'_, 'mcx> {
    #[inline]
    fn emit(
        &mut self,
        i: u32,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>> {
        ::postgres_seams::check_for_interrupts::call()?;
        ::nodeseqscan::seq_scan_batch_emit(self.node, estate, i)
    }

    /// Direct sort-key read (armed by `arm_sort_key`): value/null straight
    /// from the staged SoA key column — no per-row interrupt seam, exactly
    /// the incumbent `SeqScanSortSource::emit_key` cadence (page-level CFI
    /// inside the staging fetch covers the batch).
    #[inline(always)]
    fn emit_key(&mut self, i: u32) -> Option<(::datum::Datum, bool)> {
        ::nodeseqscan::seq_scan_batch_key(self.node, i)
    }

    #[inline]
    fn topk_key_lane(&self, n: u32) -> Option<(&[::datum::Datum], &[bool], &[u64])> {
        ::nodeseqscan::seq_scan_topk_key_lane(self.node, n)
    }

    #[inline]
    fn push_topk_bound(&mut self, key: ::datum::Datum) {
        ::nodeseqscan::seq_scan_adaptive_push_bound(self.node, key);
    }

    #[inline]
    fn key_dict_lane(&self) -> Option<::exectuples::SoaDictLane> {
        ::nodeseqscan::seq_scan_batch_key_dict_lane(self.node)
    }

    #[inline]
    fn window_ref(&self) -> Option<(u32, u32)> {
        ::nodeseqscan::seq_scan_batch_window_ref(self.node)
    }

    #[inline]
    fn refsort_key_batch(
        &self,
        col: u16,
        n: u32,
    ) -> Option<(&[::datum::Datum], &[bool], &[u64], Option<&[u64]>)> {
        ::nodeseqscan::seq_scan_refsort_key_batch(self.node, col, n)
    }

    #[inline]
    fn refsort_dictcode_batch(&mut self, col: u16) -> Option<::exectuples::SoaDictLane> {
        ::nodeseqscan::seq_scan_batch_dict_codes_global(self.node, col as usize)
    }

    #[inline]
    fn refsort_batch_masks(&self, n: u32) -> Option<(&[u64], Option<&[u64]>)> {
        ::nodeseqscan::seq_scan_refsort_batch_masks(self.node, n)
    }

    #[inline]
    fn rowref_base(&self) -> Option<u64> {
        ::nodeseqscan::seq_scan_batch_rowref_base(self.node)
    }

    #[inline]
    fn live_sel(&self) -> Option<[u64; ::exectuples::SOA_BM_WORDS]> {
        let sel = ::nodeseqscan::seq_scan_batch_skip_sel(self.node)?;
        let mut out = [0u64; ::exectuples::SOA_BM_WORDS];
        out[..sel.len()].copy_from_slice(sel);
        Some(out)
    }
}

// ===========================================================================
// IndexScan ownership (Phase 1 breadth, now push-driven). Same pipeline shape
// over the SAME batch primitives the fused-agg path uses
// (`index_scan_next_tidrun` / `index_scan_batch_fetch`). The admitted shape is
// deliberately narrow — no qual, no projection, no runtime keys, forward btree
// — so the node's output is exactly the stored scan tuple: `exec_index_scan`
// over that shape is `exec_scan_extended::<false,false>` (reset ctx, fetch,
// return the scan slot). Same visible tuples, same index order → BYTE-IDENTICAL.
// ===========================================================================

/// Try to let the lane own an `IndexScan`. `Some` = lane drove this call;
/// `None` = refused (caller runs the unchanged `exec_index_scan`).
#[inline]
pub fn try_own_index_scan<'mcx>(
    is: &mut ::nodeindexscan::IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // WS-J express-lane point experiment (default OFF behind
    // PGRUST_LANE_V2_EXPRESS; rowmode-operators.md §5): Some = express drove
    // this pull; None = off/refused — fall through UNCHANGED.
    if let Some(owned) = express::try_own_index_scan_express(is, estate)? {
        return Ok(Some(owned));
    }
    // Standalone scan ownership: refused, see STANDALONE_SCAN_NO_UPSIDE.
    // Per-PULL tick cadence (this hook runs once per exec_proc_node call).
    if STANDALONE_SCAN_NO_UPSIDE {
        stats::tick_refused(ShapeClass::IndexScan, RefuseReason::AdmissionEconomicsNoConsumer);
        if estate.engine_capture() {
            engine_capture_scan_refused(
                estate,
                is.ss.instr_idx,
                ShapeClass::IndexScan,
                RefuseReason::AdmissionEconomicsNoConsumer,
            );
        }
        return Ok(None);
    }
    if !index_scan_fusible(is, estate) {
        return Ok(None);
    }
    debug_assert!(::types_scan::sdir::ScanDirectionIsForward(estate.es_direction));
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step(is, &mut IndexScanSource, &mut IndexScanEmit, &mut root, estate)?))
}

/// Refuse-set for the lane-v2 IndexScan pipeline. Admits only the shape the
/// fused-agg index arm admits (no qual / no projection / no runtime keys /
/// forward index order / btree AM / MVCC), plus the lane-specific disarms:
/// EPQ, a non-forward call, a scrollable/backward or mergejoin-mark cursor
/// (`!batch_allowed` — mark/restore + backward desync the tidrun cursor),
/// parallel, EXPLAIN ANALYZE (instrumented), and any amcanorderbyop reorder
/// (`iss_OrderBy`) which the tidrun path does not reorder.
fn index_scan_fusible<'mcx>(
    is: &::nodeindexscan::IndexScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> bool {
    // This gate is per-call (not node-memoized), so accounting ticks are
    // per-pull decisions for this class — see `stats.rs` tick semantics.
    match index_scan_refuse_reason(is, estate) {
        None => {
            stats::tick_owned(ShapeClass::IndexScan);
            true
        }
        Some(r) => {
            stats::tick_refused(ShapeClass::IndexScan, r);
            false
        }
    }
}

/// `None` = admitted; `Some(reason)` = refused.
fn index_scan_refuse_reason<'mcx>(
    is: &::nodeindexscan::IndexScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> Option<RefuseReason> {
    index_scan_refuse_reason_ex(is, estate, false)
}

/// `allow_parallel`: the AGGIDX-PAR agg-over-index feed admits parallel-aware
/// scans (each worker's feed drives the SAME `index_scan_next_tidrun`
/// primitive over the shared parallel scan descriptor the DSM initializers
/// opened — page claims coordinate inside the AM, exactly the fused arm #2
/// drive, which never had a parallel check). Every OTHER gate applies
/// identically (the `seq_scan_refuse_reason_ex` rule: a widening flag may
/// never skip any row but its own). The tidrun pull path and the sorted-agg
/// hook keep `allow_parallel = false` — their drives were never priced under
/// workers.
fn index_scan_refuse_reason_ex<'mcx>(
    is: &::nodeindexscan::IndexScanState<'mcx>,
    estate: &EStateData<'mcx>,
    allow_parallel: bool,
) -> Option<RefuseReason> {
    if estate.es_epq_active {
        return Some(RefuseReason::Epq);
    }
    if !is.batch_allowed() {
        return Some(RefuseReason::ScrollMark);
    }
    if is.iss_ParallelAware && !allow_parallel {
        return Some(RefuseReason::ParallelGate);
    }
    if is.ss.instr_idx.is_some() {
        return Some(RefuseReason::Instrumented);
    }
    // Same-block tidrun batching is only sound under an MVCC snapshot (matches
    // the fused-agg gate; non-MVCC keeps the per-tuple path).
    if !estate
        .es_snapshot
        .as_deref()
        .is_some_and(::types_snapshot::IsMVCCSnapshot)
    {
        return Some(RefuseReason::NonMvccSnapshot);
    }
    if is.ss.qual.is_some() || is.ss.ps_ProjInfo.is_some() {
        return Some(RefuseReason::ShapeQualProj);
    }
    if is.iss_Runtime.is_some() {
        return Some(RefuseReason::RuntimeKeys);
    }
    if is.iss_OrderBy.is_some() {
        return Some(RefuseReason::OrderByReorder);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(is.iss_OrderDir) {
        // A DESC-ordered index scan PLAN (indexorderdir backward) - a live
        // planner shape, distinct from the retired runtime-direction row
        // (backward-execution wave B11 re-vocab).
        return Some(RefuseReason::DescOrder);
    }
    if !is
        .iss_RelationDesc
        .as_ref()
        .is_some_and(|r| r.rd_rel.relam == ::types_core::BTREE_AM_OID)
    {
        return Some(RefuseReason::NonBtree);
    }
    None
}

/// Push source: stages a same-block TID run (`index_scan_next_tidrun`, which
/// runs `check_for_interrupts` per run, matching the fused-agg drive this
/// reuses). Staging resets the node-resident consume cursor.
struct IndexScanSource;

impl<'mcx> Source<'mcx> for IndexScanSource {
    type Node = ::nodeindexscan::IndexScanState<'mcx>;

    fn produce(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<Batch>> {
        let n = ::nodeindexscan::index_scan_next_tidrun(node, estate)?;
        node.set_lane_cursor(0, n);
        if n == 0 {
            // End of scan: C's IndexNext clears the scan slot on exhaustion
            // (dropping its buffer pin); match it.
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(node.ss.ss_ScanTupleSlot), mcx);
        }
        Ok((n > 0).then_some(Batch { n }))
    }
}

/// Push operator: replays the staged TID run one visible tuple at a time
/// (`index_scan_batch_fetch`, sequential: entry `i>0` advances the AM cursor,
/// so the run is consumed 0,1,2,… without gaps). No qual/projection → the
/// pushed tuple is the stored scan slot. The run position lives on the node
/// (`IndexScanState::lane_cursor`) to survive the Volcano call boundary.
struct IndexScanEmit;

impl<'mcx> Operator<'mcx> for IndexScanEmit {
    type Node = ::nodeindexscan::IndexScanState<'mcx>;

    fn pending(&self, node: &Self::Node) -> Option<Batch> {
        let (pos, n) = node.lane_cursor();
        (pos < n).then_some(Batch { n })
    }

    fn consume(
        &mut self,
        node: &mut Self::Node,
        batch: Batch,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        let scan_id = node.ss.ss_ScanTupleSlot;
        loop {
            let (pos, n) = node.lane_cursor();
            debug_assert_eq!(n, batch.n);
            if pos >= n {
                return Ok(OpStatus::NeedInput);
            }
            node.set_lane_cursor(pos + 1, n);
            if ::nodeindexscan::index_scan_batch_fetch(node, estate, pos)? {
                if let SinkFeed::Full = out.accept(scan_id, estate)? {
                    return Ok(OpStatus::Paused);
                }
            }
        }
    }

    fn consume_batch<K: BatchSink<'mcx>>(
        &mut self,
        node: &mut Self::Node,
        batch: Batch,
        out: &mut K,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        let (pos, n) = node.lane_cursor();
        debug_assert_eq!(n, batch.n);
        out.accept_batch(&mut IndexScanBatchEmit { node }, pos, n, estate)?;
        node.set_lane_cursor(n, n);
        Ok(OpStatus::NeedInput)
    }
}

/// `IndexScanEmit`'s per-row body as a `BatchEmit` face (no per-row CFI —
/// `index_scan_next_tidrun` runs it per run, exactly as `consume`). The run
/// is consumed sequentially 0,1,2,… by construction (`pos..n`).
struct IndexScanBatchEmit<'a, 'mcx> {
    node: &'a mut ::nodeindexscan::IndexScanState<'mcx>,
}

impl<'mcx> BatchEmit<'mcx> for IndexScanBatchEmit<'_, 'mcx> {
    #[inline]
    fn emit(
        &mut self,
        i: u32,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>> {
        Ok(::nodeindexscan::index_scan_batch_fetch(self.node, estate, i)?
            .then_some(self.node.ss.ss_ScanTupleSlot))
    }
}

// ===========================================================================
// IndexOnlyScan ownership (push-driven). `index_only_scan_batch_next` advances
// to the next VISIBLE index tuple (VM probe / heap fallback / predicate lock —
// C's IndexOnlyNext order) and returns 0 or 1; `index_only_scan_batch_store`
// stages `xs_itup` into the scan slot. The source produces one-row batches, so
// a batch never outlives the driver round that produced it — no node-resident
// cursor. Narrow shape (no qual / no projection / no runtime keys / forward
// btree) → the output is the stored scan tuple, identical to
// `exec_index_only_scan`.
// ===========================================================================

/// Try to let the lane own an `IndexOnlyScan`.
#[inline]
pub fn try_own_index_only_scan<'mcx>(
    ios: &mut ::nodeindexonlyscan::IndexOnlyScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Standalone scan ownership: refused, see STANDALONE_SCAN_NO_UPSIDE.
    // Per-PULL tick cadence (this hook runs once per exec_proc_node call).
    if STANDALONE_SCAN_NO_UPSIDE {
        stats::tick_refused(
            ShapeClass::IndexOnlyScan,
            RefuseReason::AdmissionEconomicsNoConsumer,
        );
        if estate.engine_capture() {
            engine_capture_scan_refused(
                estate,
                ios.ss.instr_idx,
                ShapeClass::IndexOnlyScan,
                RefuseReason::AdmissionEconomicsNoConsumer,
            );
        }
        return Ok(None);
    }
    if !index_only_scan_fusible(ios, estate) {
        return Ok(None);
    }
    debug_assert!(::types_scan::sdir::ScanDirectionIsForward(estate.es_direction));
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step(ios, &mut IndexOnlyScanSource, &mut IndexOnlyScanEmit, &mut root, estate)?))
}

/// Refuse-set for the lane-v2 IndexOnlyScan pipeline (mirrors the fused-agg
/// IOS arm + the lane disarms). `!batch_allowed` refuses a scrollable/backward
/// or mergejoin-mark cursor; `ioss_OrderByKeys` non-empty refuses
/// amcanorderbyop (distance-ordered) scans.
fn index_only_scan_fusible<'mcx>(
    ios: &::nodeindexonlyscan::IndexOnlyScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> bool {
    // Per-call gate: accounting ticks are per-pull decisions for this class.
    match index_only_scan_refuse_reason(ios, estate) {
        None => {
            stats::tick_owned(ShapeClass::IndexOnlyScan);
            true
        }
        Some(r) => {
            stats::tick_refused(ShapeClass::IndexOnlyScan, r);
            false
        }
    }
}

/// `None` = admitted; `Some(reason)` = refused.
fn index_only_scan_refuse_reason<'mcx>(
    ios: &::nodeindexonlyscan::IndexOnlyScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> Option<RefuseReason> {
    index_only_scan_refuse_reason_ex(ios, estate, false)
}

/// `allow_parallel`: the SE-AGGIOS agg-over-IOS feed admits parallel-aware
/// scans (each worker's feed drives the SAME `index_only_scan_batch_next`
/// primitive over the shared parallel scan descriptor the DSM initializers
/// opened — page claims coordinate inside the AM, exactly the fused arm #3
/// drive, which never had a parallel check; the VM probe and heap fallback
/// run per TID against worker-local buffers either way). Every OTHER gate
/// applies identically (the `seq_scan_refuse_reason_ex` /
/// `index_scan_refuse_reason_ex` rule: a widening flag may never skip any
/// row but its own). The standalone pull path keeps `allow_parallel =
/// false` — its drive was never priced under workers.
fn index_only_scan_refuse_reason_ex<'mcx>(
    ios: &::nodeindexonlyscan::IndexOnlyScanState<'mcx>,
    estate: &EStateData<'mcx>,
    allow_parallel: bool,
) -> Option<RefuseReason> {
    if estate.es_epq_active {
        return Some(RefuseReason::Epq);
    }
    if !ios.batch_allowed() {
        return Some(RefuseReason::ScrollMark);
    }
    if ios.ioss_ParallelAware && !allow_parallel {
        return Some(RefuseReason::ParallelGate);
    }
    if ios.ss.instr_idx.is_some() {
        return Some(RefuseReason::Instrumented);
    }
    if !estate
        .es_snapshot
        .as_deref()
        .is_some_and(::types_snapshot::IsMVCCSnapshot)
    {
        return Some(RefuseReason::NonMvccSnapshot);
    }
    if ios.ss.qual.is_some() || ios.ss.ps_ProjInfo.is_some() {
        return Some(RefuseReason::ShapeQualProj);
    }
    if ios.ioss_Runtime.is_some() {
        return Some(RefuseReason::RuntimeKeys);
    }
    if !ios.ioss_OrderByKeys.is_empty() {
        return Some(RefuseReason::OrderByReorder);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(ios.ioss_OrderDir) {
        // DESC-ordered IOS plan (indexorderdir backward) - live planner
        // shape; B11 re-vocab (see index_scan_refuse_reason).
        return Some(RefuseReason::DescOrder);
    }
    if !ios
        .ioss_RelationDesc
        .as_ref()
        .is_some_and(|r| r.rd_rel.relam == ::types_core::BTREE_AM_OID)
    {
        return Some(RefuseReason::NonBtree);
    }
    None
}

/// Push source: one VISIBLE index tuple per batch (`index_only_scan_batch_next`
/// runs `check_for_interrupts` per tuple).
struct IndexOnlyScanSource;

impl<'mcx> Source<'mcx> for IndexOnlyScanSource {
    type Node = ::nodeindexonlyscan::IndexOnlyScanState<'mcx>;

    fn produce(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<Batch>> {
        let n = ::nodeindexonlyscan::index_only_scan_batch_next(node, estate)?;
        if n == 0 {
            // End of scan: C's IndexOnlyNext clears the scan slot on
            // exhaustion; match it.
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(node.ss.ss_ScanTupleSlot), mcx);
            return Ok(None);
        }
        debug_assert_eq!(n, 1);
        Ok(Some(Batch { n }))
    }
}

/// Push operator: stages `xs_itup` into the scan slot and pushes it. One-row
/// batches are always fully consumed within the producing driver round, so
/// `pending` is statically `None` (the drive is stateless across the Volcano
/// boundary — no cursor).
struct IndexOnlyScanEmit;

impl<'mcx> Operator<'mcx> for IndexOnlyScanEmit {
    type Node = ::nodeindexonlyscan::IndexOnlyScanState<'mcx>;

    fn pending(&self, _node: &Self::Node) -> Option<Batch> {
        None
    }

    fn consume(
        &mut self,
        node: &mut Self::Node,
        batch: Batch,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        debug_assert_eq!(batch.n, 1);
        ::nodeindexonlyscan::index_only_scan_batch_store(node, estate)?;
        Ok(match out.accept(node.ss.ss_ScanTupleSlot, estate)? {
            SinkFeed::Full => OpStatus::Paused,
            SinkFeed::NeedMore => OpStatus::NeedInput,
        })
    }
}

// ===========================================================================
// BitmapHeapScan ownership (push-driven). The bitmap must be built before the
// pipeline runs — the dispatch hook keeps the arm's existing
// `bitmap_table_scan_setup_dispatch` call, then offers the
// (already-initialized) scan to the lane. Same pipeline shape as the SeqScan
// lane over the page-batch primitives (`bitmap_scan_next_pagebatch` /
// `bitmap_scan_batch_fetch`, random-access by `i`); `bitmap_scan_batch_fetch`
// applies the page recheck (`bitmapqualorig`) internally on lossy/recheck
// pages, exactly as `BitmapHeapNext` does. Narrow shape (no scan qual / no
// projection) → the output is the stored scan tuple.
// ===========================================================================

/// Try to let the lane own a `BitmapHeapScan`. The caller must have already
/// run the bitmap setup (the arm does, unconditionally, before this).
#[inline]
/// bitmap-morsels lane: the runtime bitmap-heap arm (plain Agg root over a
/// serial-plan Bitmap Heap Scan, morselized claims over the frozen shared
/// bitmap). Refusal falls through to the UNCHANGED serial paths (including
/// the fused agg-over-bitmap batch drive) byte-identically; admission +
/// refuse-set live in `runtime_bitmap`. Dispatched from procnode's agg_arm
/// BEFORE the bitmap is built (the arm builds it once, leader-side).
pub fn try_own_agg_over_bitmap_heap_scan<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    b: &mut crate::procnode::BitmapHeapPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    runtime_bitmap::try_own_plain_agg_over_bitmap_runtime(agg, b, estate)
}

pub fn try_own_bitmap_heap_scan<'mcx>(
    bhs: &mut ::nodebitmapheapscan::BitmapHeapScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Standalone scan ownership: refused, see STANDALONE_SCAN_NO_UPSIDE.
    // Per-PULL tick cadence (this hook runs once per exec_proc_node call).
    if STANDALONE_SCAN_NO_UPSIDE {
        stats::tick_refused(
            ShapeClass::BitmapHeapScan,
            RefuseReason::AdmissionEconomicsNoConsumer,
        );
        return Ok(None);
    }
    if !bitmap_heap_scan_fusible(bhs, estate) {
        return Ok(None);
    }
    debug_assert!(::types_scan::sdir::ScanDirectionIsForward(estate.es_direction));
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step(bhs, &mut BitmapHeapScanSource, &mut BitmapHeapScanEmit, &mut root, estate)?))
}

/// Refuse-set for the lane-v2 BitmapHeapScan pipeline (mirrors the fused-agg
/// bitmap arm: no scan qual / no projection). Disarms EPQ, non-forward,
/// parallel (aware or a worker attached to shared state), and EXPLAIN ANALYZE.
/// Also refuses when the page recheck qual (`bitmapqualorig`) carries a subplan
/// or exec-param — the recheck runs a plain `exec_qual` that evaluates neither.
/// Bitmap scans are never scrollable/mark cursors (planner-guaranteed; a SCROLL
/// cursor gets a Material parent), so no eflags gate is needed. Bitmap init
/// asserts an MVCC snapshot, so that is implicit.
fn bitmap_heap_scan_fusible<'mcx>(
    bhs: &::nodebitmapheapscan::BitmapHeapScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> bool {
    // Per-call gate: accounting ticks are per-pull decisions for this class.
    match bitmap_heap_scan_refuse_reason(bhs, estate) {
        None => {
            stats::tick_owned(ShapeClass::BitmapHeapScan);
            true
        }
        Some(r) => {
            stats::tick_refused(ShapeClass::BitmapHeapScan, r);
            false
        }
    }
}

/// `None` = admitted; `Some(reason)` = refused.
fn bitmap_heap_scan_refuse_reason<'mcx>(
    bhs: &::nodebitmapheapscan::BitmapHeapScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> Option<RefuseReason> {
    if estate.es_epq_active {
        return Some(RefuseReason::Epq);
    }
    if bhs.parallel_aware || bhs.pstate.is_some() {
        return Some(RefuseReason::ParallelGate);
    }
    if bhs.ss.instr_idx.is_some() {
        return Some(RefuseReason::Instrumented);
    }
    if bhs
        .bitmapqualorig
        .as_deref()
        .is_some_and(|q| q.has_subplan() || !q.param_exec_deps().is_empty())
    {
        return Some(RefuseReason::SubplanParam);
    }
    if bhs.ss.qual.is_some() || bhs.ss.ps_ProjInfo.is_some() {
        return Some(RefuseReason::ShapeQualProj);
    }
    None
}

/// Push source: stages the next bitmap page's tuples
/// (`bitmap_scan_next_pagebatch` runs `check_for_interrupts` per page).
/// Staging resets the node-resident consume cursor.
struct BitmapHeapScanSource;

impl<'mcx> Source<'mcx> for BitmapHeapScanSource {
    type Node = ::nodebitmapheapscan::BitmapHeapScanState<'mcx>;

    fn produce(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<Batch>> {
        let n = ::nodebitmapheapscan::bitmap_scan_next_pagebatch(node, estate)?;
        node.set_lane_cursor(0, n);
        if n == 0 {
            // End of scan: C's BitmapHeapNext returns ExecClearTuple(slot) on
            // exhaustion (dropping its buffer pin); match it.
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(node.ss.ss_ScanTupleSlot), mcx);
        }
        Ok((n > 0).then_some(Batch { n }))
    }
}

/// Push operator: pushes each surviving row of the staged page
/// (`bitmap_scan_batch_fetch` applies the page recheck on lossy pages). The
/// page-batch position lives on the node (`BitmapHeapScanState::lane_cursor`).
struct BitmapHeapScanEmit;

impl<'mcx> Operator<'mcx> for BitmapHeapScanEmit {
    type Node = ::nodebitmapheapscan::BitmapHeapScanState<'mcx>;

    fn pending(&self, node: &Self::Node) -> Option<Batch> {
        let (pos, n) = node.lane_cursor();
        (pos < n).then_some(Batch { n })
    }

    fn consume(
        &mut self,
        node: &mut Self::Node,
        batch: Batch,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        let scan_id = node.ss.ss_ScanTupleSlot;
        loop {
            let (pos, n) = node.lane_cursor();
            debug_assert_eq!(n, batch.n);
            if pos >= n {
                return Ok(OpStatus::NeedInput);
            }
            node.set_lane_cursor(pos + 1, n);
            if ::nodebitmapheapscan::bitmap_scan_batch_fetch(node, estate, pos)? {
                if let SinkFeed::Full = out.accept(scan_id, estate)? {
                    return Ok(OpStatus::Paused);
                }
            }
        }
    }

    fn consume_batch<K: BatchSink<'mcx>>(
        &mut self,
        node: &mut Self::Node,
        batch: Batch,
        out: &mut K,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        let (pos, n) = node.lane_cursor();
        debug_assert_eq!(n, batch.n);
        out.accept_batch(&mut BitmapHeapScanBatchEmit { node }, pos, n, estate)?;
        node.set_lane_cursor(n, n);
        Ok(OpStatus::NeedInput)
    }
}

/// `BitmapHeapScanEmit`'s per-row body as a `BatchEmit` face (no per-row CFI
/// — `bitmap_scan_next_pagebatch` runs it per page, exactly as `consume`).
struct BitmapHeapScanBatchEmit<'a, 'mcx> {
    node: &'a mut ::nodebitmapheapscan::BitmapHeapScanState<'mcx>,
}

impl<'mcx> BatchEmit<'mcx> for BitmapHeapScanBatchEmit<'_, 'mcx> {
    #[inline]
    fn emit(
        &mut self,
        i: u32,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>> {
        Ok(::nodebitmapheapscan::bitmap_scan_batch_fetch(self.node, estate, i)?
            .then_some(self.node.ss.ss_ScanTupleSlot))
    }
}

// ===========================================================================
// Hash-agg pipeline breaker (Phase-2 vertical slice): the first
// operator→operator composition. Two chained pipelines on one Agg node:
//
//   pipeline N   : SeqScanSource → SeqScanFilterProject → HashAggBuildSink
//   pipeline N+1 : HashAggSource → HashAggEmit → RootAdapter
//
// The breaker node (the Agg) implements Sink for pipeline N (accept = the
// existing per-row transition path via `agg_hash_build_accept`; always
// `NeedMore`) and Source for pipeline N+1 (produce = the existing
// `agg_retrieve_hash_table` read-back — same table, same iteration → same
// output order as C, spill refill included). Chaining is the per-node
// Build→Probe phase flag (`table_filled` — C's own cross-call state), driven
// from the `agg_arm` dispatch hook: the build pipeline drains to completion
// before the first probe tuple, which is C's exact order for free
// (push-executor study, Pattern 3). Spill delegates wholesale to the row-path
// hashagg machinery (§8): `finish()` = spill finish + handoff install; the
// read-back's refill walks PG's spill partitions in PG's order.
// ===========================================================================

/// Memoized structural choice for an Agg-over-SeqScan node, decided at the
/// first call and stable thereafter (a mid-stream flip would desync the
/// build). Dynamic gates (EPQ, direction, the post-build merge handoff) stay
/// per-call in `agg_over_seq_scan_fusible`, evaluated BEFORE the memo.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggLaneChoice {
    /// Admission economics (design §4): no lanefold coverage AND the legacy
    /// fused `exec_agg_batched` arm would engage — the lane must not preempt
    /// the measured-faster fused batch drive (plain count/avg class, integration bench
    /// 2026-07-11). Re-measured with the Phase-3 qual bitmap (2026-07-12):
    /// the lane's per-row breaker feed is STILL slower than the fused arm at
    /// the qualed plain-avg shape's 50% selectivity (+2.5%; only ~-5% at 10% selectivity) — the
    /// dominant cost is the per-row `agg_hash_build_accept` vs the fused
    /// arm's batched drive, which carries the same bitmap. Deliberate
    /// refuse-set entry; shrinks as fold coverage widens.
    Refuse,
    /// Lane owns with the per-row breaker feed: no fold coverage, but no
    /// fused arm to preempt either (shapes the fused arm refuses — scalar
    /// quals, admitted projections).
    PerRow,
    /// Lane owns with the batched build feed: per-batch group probe + the
    /// lanefold whole-batch transition kernels (residual transitions
    /// per-row).
    Fold,
    /// Lane answers the whole AGG_PLAIN node from pgrcolumnar part metadata
    /// (footer row counts + zone maps + footer sums) — zero rows staged, end
    /// states finalized by the real finalfns (the metaagg arm; phase4 §7
    /// re-entry, armed 2026-07-14). Structural admission only: the per-call
    /// runtime gates (MVCC snapshot, AM answerability, guard-interval
    /// re-proof) fall back to the per-row drive byte-identically.
    Meta,
    /// sorted-arm lane: the ordered-grouped RUNTIME sink engaged and the
    /// stitched parallel result was adopted — every subsequent pull drains
    /// it (`agg_sorted_sink_emit_next`). Memoized like the serial choices:
    /// the engagement ran once, at the first pull, before anything was
    /// consumed.
    SortedSink,
}

::mcx::forget_safe_nodrop!(AggLaneChoice);

/// PARTWISE-MORSELS (night/partitionwise-morsels, knob-gated default OFF):
/// try to let the RUNTIME own a plain `Agg` over an `Append` of partition
/// SeqScans — partition-as-morsel on the scan arm's engagement
/// (runtime_partwise.rs module doc). `Some(result)` = the runtime drove the
/// node; `None` = refused (the caller falls through to the unchanged serial
/// Append per-tuple drive, byte-identically). Knob-OFF this is one memoized
/// bool read.
#[inline]
pub fn try_own_agg_over_append<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    apn: &mut crate::procnode::AppendNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if !runtime_partwise::partwise_enabled() {
        return Ok(None);
    }
    // Plain (ungrouped) fold shapes only — the same admissibility face the
    // seq-scan plain walk fronts with; grouped/sorted/distinct shapes keep
    // today's path untouched.
    if !::nodeagg::agg_plain_fold_admissible(agg) {
        return Ok(None);
    }
    runtime_partwise::try_own_plain_agg_partwise(agg, apn, estate)
}

/// Try to let the lane own an `Agg` over a `SeqScan` child — the fused
/// scan→filter→hash-agg push pipeline. `Some(result)` = the lane drove this
/// call; `None` = refused (the caller falls through to the existing fused /
/// per-tuple agg paths, byte-identically).
#[inline]
pub fn try_own_agg_over_seq_scan<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    choice: &mut Option<AggLaneChoice>,
    stage_slot: &mut Option<ExecSlotId>,
    xk: &mut Option<Box<ExprKeyState>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Route latch (GL-ALPHA1 inc-2, knob default OFF — doc at
    // `agg_route_latch_enabled`): a mid-emit sink has committed the hashed
    // route; dispatch straight to the drain (the SE-T2AGG emitting branch's
    // exact pair) instead of re-deriving the identical walk per pull.
    if agg_route_latch_enabled() && ::nodeagg::sink::agg_sink_emitting(agg) {
        if ::nodeagg::agg_is_done(agg) {
            return Ok(Some(None));
        }
        let mut root = RootAdapter::new(None);
        return Ok(Some(pull_step(agg, &mut HashAggSource, &mut HashAggEmit, &mut root, estate)?));
    }
    // AGG_PLAIN (ungrouped) routes to the plain drive: no breaker needed (a
    // single group has no per-group read-back — feed + finalize is the whole
    // node inside one call), but the same staged-batch fold applies, with
    // `lanefold::fold_batch` (the ungrouped kernel, CSE included) in place of
    // the grouped probe+fold. pgrcolumnar scans additionally route WITHOUT a
    // classified fold plan (lane-v2-noqualfeed): the plain decider can pick
    // the per-row drain feed there — batch window decode + the full per-row
    // transition program — because the pgrcolumnar incumbent is the per-pull
    // Volcano drive, not the fused batched arm the heap refusal defends.
    if ::nodeagg::agg_plain_fold_admissible(agg)
        || (::nodeagg::agg_plain_perrow_admissible(agg)
            && ::nodeseqscan::seq_scan_is_pgrcolumnar(ss))
        // SE-AGGPOLY (band 101001, knob-gated default OFF): heap plain
        // shapes with NO fold plan but a poly export manifest (numeric
        // states) must reach the plain walk so the RUNTIME scan arm can
        // offer (the serial heap decide still Refuses them — the fused
        // incumbent keeps the serial drive byte-identically; only the
        // runtime engagement path is new).
        || (agg_poly_enabled()
            && ::nodeagg::agg_plain_perrow_admissible(agg)
            && ::nodeagg::runtime_partial::agg_poly_partial_admissible(agg))
    {
        return try_own_plain_agg_over_seq_scan(agg, ss, choice, estate);
    }
    // AGG_SORTED (the sort-free GroupAggregate shape — clustered/footer-
    // sorted pgrcolumnar banks plan `Agg(AGG_SORTED) → SeqScan` with no Sort
    // node): the sorted-agg drive over the scan's staged batches, with
    // fold-admissible transitions run as vectorized per-group-run folds.
    // Section doc at `try_own_sorted_agg_over_seq_scan`. Non-admissible
    // sorted shapes fall through to the hashed gate below, which refuses
    // exactly as before (AggNotDrainable).
    if ::nodeagg::agg_sorted_lane_admissible(agg) {
        return try_own_sorted_agg_over_seq_scan(agg, ss, choice, estate);
    }
    // AGG_PLAIN exact-DISTINCT (count/sum/avg(DISTINCT x) — nodeagg's
    // set-mode admission): NOT batch-drainable (pertrans_sort non-empty), so
    // neither the fold drive above nor the legacy fused arm can host it —
    // the incumbent is the per-tuple pull with a per-group TUPLESORT. The
    // set drive replaces that sort with the exact-distinct hash set
    // (uniqExact analog, pgrcolumnar-v2 plan §2.3).
    if ::nodeagg::agg_plain_distinct_set_admissible(agg) {
        return try_own_plain_distinct_agg_over_seq_scan(agg, ss, estate);
    }
    // GL-DISTALPHA-2 (knob-gated, DEFAULT OFF): the PRESORTED-bare
    // exact-DISTINCT face — a clustered scan order serves the DISTINCT
    // aggregate presorted with no Sort node, so the entries are set-CAPABLE
    // but DORMANT (`set_active` honors C's adjacent-dedup contract) and the
    // set dispatch above refuses; the runtime sink was structurally
    // unreachable for the whole class. Probe the RUNTIME sink alone, with
    // the skip-sort drive's force_set arming (the identical pertrans state:
    // presorted entries armed into exact sets — the ratified
    // order-relaxation grant; engage() arms only on ownership). A refusal
    // falls through UNCHANGED to the per-tuple presorted drive — never the
    // serial set drive (hash inserts must not replace the incumbent's
    // adjacent-dedup on ordered input).
    // Conjunct ORDER is the per-pull cost discipline: this walk re-enters
    // per pull, and multi-row Agg shapes (hashed grouping) pull it millions
    // of times — the node-STRUCTURAL predicates short-circuit first (one
    // aggstrategy compare for every non-target node; the sibling
    // admissibility probes' cost class), the knob OnceLock and the router's
    // GUC-reading arm_dop run only on actual target shapes (measured: a
    // per-pull arm_dop cost a 2-col hash-distinct leg ~1.4x on the fleet).
    if ::nodeagg::agg_plain_distinct_set_only(agg)
        && !::nodeagg::agg_pertrans_all_distinct_set(agg)
        && distinct_presorted_probe_enabled()
        && router::arm_dop(router::ArmClass::Distinct) > 0
        && !estate.es_epq_active
    {
        if let Some(scan_node) = agg.plan.plan.lefttree {
            if scan_node.node_tag() == ::types_nodes::NodeTag::T_SeqScan {
                lane_trace("runtime-plaindistinct: presorted-bare probe");
                if let Some(r) = runtime_plaindistinct::try_own_plain_distinct_runtime(
                    agg, ss, scan_node, true, estate,
                )? {
                    return Ok(Some(r));
                }
            }
        }
    }
    if !agg_over_seq_scan_fusible(agg, ss, estate)? {
        // EXPLAIN (ENGINE) capture: the hashed route's production verdict
        // through the same E4 mirror the EA walk below uses (breaker
        // admissibility + child refuse-set with only the instrument gates
        // vacated + the memoized production lane choice).
        if estate.engine_capture() {
            engine_capture_agg_over_seq_scan(agg, ss, choice, xk, estate)?;
        }
        // EA-on-morsels (ea-morsels.md §5, inc-1b): under EXPLAIN ANALYZE
        // the scan-side fusibility memo refuses (the leader node carries an
        // instr slot), but the runtime agg sink's workers run uninstrumented
        // executors. Mirror the uninstrumented decision (E4: same lane
        // choice, same breaker admissibility, only the instrument gate
        // vacated) and give the sink its admission walk. Once a build was
        // adopted, keep draining through the lane's own retrieve — the sink
        // emit has no interpreter leg. A hash table built by the interpreter
        // (engagement refused earlier) means the interpreter owns the node:
        // never engage after that.
        if runtime_instr::ea_active(estate)
            && ::nodeagg::agg_hash_breaker_admissible(agg)
            && seq_scan_fusible_runtime_ea(ss, estate)?
        {
            let c = match *choice {
                Some(c) => c,
                None => {
                    let c = decide_agg_lane(agg, ss, xk, estate)?;
                    *choice = Some(c);
                    c
                }
            };
            if c == AggLaneChoice::Fold {
                if ::nodeagg::agg_is_done(agg) {
                    return Ok(Some(None));
                }
                let engaged = ::nodeagg::sink::agg_sink_emitting(agg)
                    || (!::nodeagg::agg_hash_table_filled(agg)
                        && runtime_agg::try_engage_hashagg_runtime(
                            agg,
                            ss,
                            xk.as_deref(),
                            None,
                            None,
                            estate,
                        )?);
                if engaged {
                    let mut root = RootAdapter::new(None);
                    return Ok(Some(pull_step(
                        agg,
                        &mut HashAggSource,
                        &mut HashAggEmit,
                        &mut root,
                        estate,
                    )?));
                }
            }
        }
        return Ok(None);
    }
    let c = match *choice {
        Some(c) => c,
        None => {
            let c = decide_agg_lane(agg, ss, xk, estate)?;
            *choice = Some(c);
            c
        }
    };
    if c == AggLaneChoice::Refuse {
        // SE-T2AGG CAR A (knob-gated, default OFF): the zero-aggregate
        // SELECT DISTINCT shape has no fold plan, so its memoized choice
        // may be Refuse (the legacy fused drive never hosts it — the
        // hash-agg breaker runs it serially) — probe the runtime
        // plain-distinct sub-arm HERE, before the refusal stands. Success
        // adopts the published emit; re-pulls resume through the
        // emitting marker and drain agg_retrieve_hash_table's sink branch.
        if ::nodeagg::sink::agg_sink_emitting(agg)
            || runtime_plaindistinct::try_own_plain_selectdistinct_runtime(agg, ss, estate)?
        {
            if ::nodeagg::agg_is_done(agg) {
                return Ok(Some(None));
            }
            let mut root = RootAdapter::new(None);
            return Ok(Some(pull_step(
                agg,
                &mut HashAggSource,
                &mut HashAggEmit,
                &mut root,
                estate,
            )?));
        }
        return Ok(None);
    }
    // exec_agg's top-of-call guard: a drained agg stays drained (the hash
    // iterator is spent; re-iterating would replay groups).
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    agg_seq_scan_build_if_needed(agg, ss, c, stage_slot, xk, None, None, estate)?;
    // Probe phase (every call): the breaker is now the source of pipeline
    // N+1. One qual-passing group per PG pull, in C's retrieve order.
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step(agg, &mut HashAggSource, &mut HashAggEmit, &mut root, estate)?))
}

/// GL-ALPHA1-EMIT-1 (knob `PGRUST_LANE_AGG_EMIT_BATCH`, default OFF — doc
/// at `agg_emit_batch_enabled`): let a plain (ungrouped) Agg consume its
/// hashed-Agg child's ADOPTED SINK EMIT in per-bucket blocks. The child's
/// build runs through the ONE shared build seam every consumer uses
/// (`agg_seq_scan_build_if_needed` — bare hook / Limit-over-agg / sort
/// feed), so engagement, refusal, and every side effect land exactly where
/// the child's own first pull would land them; ownership then covers ONLY
/// the adopted-emit outcome, from row 0. Anything else (serial/compact
/// build, mid-drain resume, winner-composed emit) refuses, and the caller
/// falls through to the unchanged per-tuple drive byte-identically — which
/// serves those shapes through the child's own dispatch as before.
///
/// Row identity: the drain walks buckets 0..SINK_NBUCKETS in order, rows in
/// insertion order within each bucket — the IDENTICAL sequence the per-pull
/// cursor (`agg_sink_emit_next`) produces — and each row runs the same
/// per-row transition program against the same child result slot
/// (`exec_agg_batched` = exec_agg minus the node recursion), so the
/// transition ORDER, not merely the row set, matches the incumbent and the
/// single output row is byte-identical by construction.
#[inline]
pub fn try_own_plain_agg_over_agg_emit<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    child: &mut crate::procnode::AggPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if !agg_emit_batch_enabled() {
        return Ok(None);
    }
    // Dynamic gates (the fused arms' posture): EPQ, backward pulls, and
    // instrumented trees keep the per-tuple drive C-exact. (Instrumented
    // children can't reach this concrete Agg match anyway — defensive.)
    if estate.es_epq_active
        || estate.es_instrument != 0
        || !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction)
    {
        return Ok(None);
    }
    // Outer-node admission: the per-row batched drive's own agg-side gate
    // (AGG_PLAIN, batch-drainable, initplan-param-free).
    if !::nodeagg::agg_plain_perrow_admissible(agg) {
        return Ok(None);
    }
    // exec_agg's top-of-call guard: a drained plain agg stays drained.
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    // Child admission: the bare agg hook's gates (breaker-admissible hashed
    // child over a fusible SeqScan feed, memoized lane choice not Refuse) —
    // `agg_child_fusible`'s SeqScan branch, inlined so the build reuses the
    // child's own memo slots.
    if !::nodeagg::agg_hash_breaker_admissible(&child.agg) {
        return Ok(None);
    }
    let crate::procnode::PlanStateNode::SeqScan(ss) = &mut child.outer else {
        return Ok(None);
    };
    if !seq_scan_fusible(ss, estate)? {
        return Ok(None);
    }
    let c = match child.lane_choice {
        Some(c) => c,
        None => {
            let c = decide_agg_lane(&child.agg, ss, &mut child.lane_exprkey, estate)?;
            child.lane_choice = Some(c);
            c
        }
    };
    if c == AggLaneChoice::Refuse {
        return Ok(None);
    }
    agg_seq_scan_build_if_needed(
        &mut child.agg,
        ss,
        c,
        &mut child.lane_stage_slot,
        &mut child.lane_exprkey,
        None,
        None,
        estate,
    )?;
    if !::nodeagg::sink::agg_sink_emitting(&child.agg)
        || !::nodeagg::sink::agg_sink_emit_unstarted(&child.agg)
        || ::nodeagg::sink::agg_sink_emit_has_winners(&child.agg)
    {
        return Ok(None);
    }
    lane_trace("runtime-agg: emit-batch armed");
    let r = ::nodeagg::exec_agg_batched(
        agg,
        estate,
        SinkEmitBatchSource { child: &mut child.agg, next_bucket: 0, cur_bucket: 0 },
    )?;
    // Spend the child's emit exactly as the cursor drain's EOF spends it
    // (cursor parked, agg_done set, state and its arenas KEPT until
    // rescan/teardown) — a stray later pull serves EOF, never a re-emit.
    ::nodeagg::sink::agg_sink_emit_consume_all(&mut child.agg);
    Ok(Some(r))
}

/// `BatchSource` face of a hashed child Agg's adopted sink emit: one batch
/// per non-empty emit bucket (buckets 0..SINK_NBUCKETS in order; row order
/// within a bucket = insertion order) — the identical row sequence the
/// per-pull cursor drain produces. `fetch_tuple` is the cursor drain's own
/// slot-store body (`agg_sink_emit_block_row`), so slot contents match
/// per-row exactly.
struct SinkEmitBatchSource<'a, 'mcx> {
    child: &'a mut ::nodeagg::AggStateData<'mcx>,
    /// Next bucket to probe; `cur_bucket` is the staged one.
    next_bucket: usize,
    cur_bucket: usize,
}

impl<'mcx> ::nodeagg::AggBatchSource<'mcx> for SinkEmitBatchSource<'_, 'mcx> {
    fn next_batch(&mut self, _estate: &mut EStateData<'mcx>) -> PgResult<u32> {
        while self.next_bucket < ::nodeagg::sink::SINK_NBUCKETS {
            let b = self.next_bucket;
            self.next_bucket += 1;
            let n = ::nodeagg::sink::agg_sink_emit_bucket_len(self.child, b)
                .expect("sink emit state adopted");
            if n == 0 {
                continue;
            }
            // The batched sink drains' interrupt cadence (one CFI per
            // bucket — `sort_feed_sink_batched`'s).
            ::postgres_seams::check_for_interrupts::call()?;
            self.cur_bucket = b;
            return Ok(n as u32);
        }
        Ok(0)
    }

    fn fetch_tuple(&mut self, i: u32, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        ::nodeagg::sink::agg_sink_emit_block_row(self.child, estate, self.cur_bucket, i as usize);
        Ok(true)
    }

    fn outer_slot(&self) -> ExecSlotId {
        self.child.ps_ResultTupleSlot
    }

    fn has_qual(&self) -> bool {
        false
    }
}

/// EXPLAIN (ENGINE) capture for the hashed Agg-over-SeqScan route: record
/// the PRODUCTION verdict for the AggBuild class (E4 — the same mirror walk
/// the runtime-EA path runs under instrumentation, ending in the memoized
/// `decide_agg_lane`, whose staging side effects the EA path already proved
/// benign under a per-tuple fallback). `Refuse` = the legacy fused batch
/// drive owns the shape → the derived FusedArm attribution.
#[cold]
fn engine_capture_agg_over_seq_scan<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    choice: &mut Option<AggLaneChoice>,
    xk: &mut Option<Box<ExprKeyState>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let id = agg.plan.plan.plan_node_id;
    if !::nodeagg::agg_hash_breaker_admissible(agg) {
        engine_record_verdict(
            estate,
            id,
            ShapeClass::AggBuild,
            Some(RefuseReason::AggNotDrainable),
        );
        return Ok(());
    }
    if !seq_scan_fusible_runtime_ea(ss, estate)? {
        engine_record_verdict(
            estate,
            id,
            ShapeClass::AggBuild,
            Some(RefuseReason::ChildScanRefused),
        );
        return Ok(());
    }
    let c = match *choice {
        Some(c) => c,
        None => {
            let c = decide_agg_lane(agg, ss, xk, estate)?;
            *choice = Some(c);
            c
        }
    };
    let refuse = match c {
        AggLaneChoice::Refuse => Some(RefuseReason::AdmissionEconomicsFusedDrive),
        _ => None,
    };
    engine_record_verdict(estate, id, ShapeClass::AggBuild, refuse);
    Ok(())
}

/// The structural lane choice (see `AggLaneChoice`), decided once at the
/// first (pre-build) call. Fold-readiness = a classified lanefold plan on an
/// unprojected scan, with the SoA deform armed whenever the plan reads lane
/// columns (a plan of pure `count(*)` transitions reads none).
fn decide_agg_lane<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    xk: &mut Option<Box<ExprKeyState>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<AggLaneChoice> {
    // Projected scans: the expression-group-key arm (exprkey module) — the
    // scan computes the (single) grouping key, everything else bare Vars.
    // Refusal (reason ticked there) keeps the per-row/refuse economics below.
    if ss.ss.ps_ProjInfo.is_some() && ::nodeagg::agg_lanefold_plan(agg).is_some() {
        *xk = exprkey::decide_exprkey(agg, ss, estate);
        if xk.is_some() {
            return Ok(AggLaneChoice::Fold);
        }
    }
    // Decide-phase skip traces (qualed text-min/max serial audit, 2026-07-14 follow-up): a
    // non-Fold decide that does NOT hit the economics refuse below is
    // otherwise invisible in trace capture (PerRow ticks nothing here), so
    // name the failed gate once per memoized decision.
    let fold_ready = match ::nodeagg::agg_lanefold_plan(agg) {
        None => {
            lane_trace("agg fold skipped: no lanefold plan (classify refused)");
            false
        }
        Some(_) if ss.ss.ps_ProjInfo.is_some() => {
            lane_trace("agg fold skipped: projected scan (exprkey refused)");
            false
        }
        Some(plan) => {
            if !plan.vguards.is_empty() {
                // Varlena (str MIN/MAX) lanes: feedable only when the plan
                // reads EXACTLY the one varlena column (the varkey pass
                // stages one column; the fixed-width prefix deform cannot
                // host attlen == -1). Mixed fixed+varlena lane sets refuse —
                // exactly the shapes the prefix probe below already refuses
                // today (the varlena read sits inside the prefix).
                match lanefold_varlane_col(plan) {
                    Some(vcol) => {
                        let armed = ::nodeseqscan::seq_scan_batch_soa_prepare_varlane(
                            ss, estate, vcol,
                        );
                        if !armed {
                            lane_trace("agg fold skipped: varlane staging unarmable");
                        }
                        armed
                    }
                    // Multi-varlena (2+ varlena lanes): pgrcolumnar's virtual-prefix
                    // staging hosts it (lane-v2-dictminmax); heap refuses.
                    None => {
                        let armed = try_arm_cb_multivar(agg, ss, estate)?;
                        if !armed {
                            lane_trace("agg fold skipped: mixed fixed+varlena lane set");
                        }
                        armed
                    }
                }
            } else if plan.cols.is_empty() {
                true
            } else {
                // Probe-arm the deform now so an unarmable prefix (non-fixed-
                // width column) is known BEFORE committing to ownership. A
                // pgrcolumnar scan whose prefix refuses only on varlena columns
                // gets the dict-group columnar arm (§2.1) — the text grouping
                // key stages as dict codes, everything else as decoded Datums.
                let armed = probe_arm_fold_prefix(agg, ss, estate)?
                    || try_arm_cb_dictgroup(agg, ss, estate)
                    || try_arm_cb_multikey_dict(agg, ss, estate);
                if !armed {
                    lane_trace("agg fold skipped: prefix/dictgroup/multikey probes refused");
                }
                armed
            }
        }
    };
    if fold_ready {
        return Ok(AggLaneChoice::Fold);
    }
    // Admission economics (design §4): without fold coverage the lane's
    // per-row breaker feed is strictly slower than the legacy fused batched
    // drive it would preempt (the agg hook runs first) — measured +5%
    // (plain count/avg class). Never preempt a measured-faster path.
    if crate::procnode::seq_agg_fusible(agg, ss, estate)
        && ::nodeseqscan::seq_scan_batch_supported(ss, estate)?
    {
        // One tick per memoized structural choice (the choice is decided once
        // per node and stable thereafter).
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AdmissionEconomicsFusedDrive);
        // Trace the structural refuse (qualed text-min/max serial-dispatch diagnosis,
        // 2026-07-14): the memoized Refuse routes the agg into the legacy
        // FUSED batched drive, which never passes through try_own_seq_scan —
        // so a refused chain shows ZERO lane markers (not even a PREWHERE
        // arm) and reads as "non-attempt" in trace capture. This line makes
        // the attempt+refusal observable.
        lane_trace("agg-over-scan refused (admission economics: legacy fused drive)");
        return Ok(AggLaneChoice::Refuse);
    }
    Ok(AggLaneChoice::PerRow)
}

/// SE-T2AGG CAR B (knob-gated, default OFF): a vguard-bearing fold plan the
/// RUNTIME AGG SINK can host — min/max(text) transitions (and their uguard
/// siblings) over the multivar/prewhere DIRECT-INDEX stagings. Conditions:
///   * the knob (`sink_strminmax_enabled`, same spelling as the m5 probe);
///   * vguards present, NO int-range guards (their demote leg is the
///     checked per-row program, which the sink lacks — a vguard/uguard
///     demote instead REFUSES to the serial rerun in the drain), no
///     residual transitions;
///   * unprojected scan (the expr-key decides never proved vguard plans).
///
/// The sink drains read DIRECT SoA indexes, so single-varlena shapes (whose
/// `arm_scan_staging` ladder arms the REMAPPED varkey staging) additionally
/// need the columnar re-arm — [`sink_rearm_vguard_columnar`], run by the
/// sink decide and the worker arm right after the staging ladder.
fn sink_vguard_plan_ok(
    agg: &::nodeagg::AggStateData<'_>,
    ss: &::nodeseqscan::SeqScanState<'_>,
) -> bool {
    ss.ss.ps_ProjInfo.is_none()
        && ::nodeagg::sink::sink_strminmax_enabled()
        && !::nodeagg::agg_lanefold_has_resid(agg)
        && ::nodeagg::agg_lanefold_plan(agg).is_some_and(|p| {
            !p.vguards.is_empty() && p.guards.is_empty() && p.resid.is_empty()
        })
}

/// GL-DICTDRAIN-1: [`sink_vguard_plan_ok`]'s PROJECTED-scan twin for the
/// dict-class expr-key drain — vguard-bearing fold plans (min/max(text) +
/// textlen-lane passengers) whose per-batch proof the expr-key drain runs
/// INLINE (`exprkey_batch`'s check_guards over the MapCols remap, the
/// serial feed's exact discipline; a demote REFUSES to the serial rerun —
/// no per-row leg exists). Both car knobs gate (the strminmax vocabulary
/// law + the dict-drain knob itself); the admission's kind belts keep
/// every NON-DictCoded kind on the base plan gate.
fn sink_exprkey_dict_vguard_ok(
    agg: &::nodeagg::AggStateData<'_>,
    ss: &::nodeseqscan::SeqScanState<'_>,
) -> bool {
    ss.ss.ps_ProjInfo.is_some()
        && exprkey::dictkey_sink_enabled()
        && ::nodeagg::sink::sink_strminmax_enabled()
        && !::nodeagg::agg_lanefold_has_resid(agg)
        && ::nodeagg::agg_lanefold_plan(agg).is_some_and(|p| {
            !p.vguards.is_empty() && p.guards.is_empty() && p.resid.is_empty()
        })
}

/// SE-T2AGG CAR B: ensure the armed SoA staging covers EVERY fold/vguard
/// column at its direct index. The prewhere lane already covers the ask
/// (arm_scan_staging widens its prefix onto vguard columns) and is left
/// untouched; the single-varlena REMAP staging (varkey pass — one column at
/// SoA index 0) does not, so re-arm the pgrcolumnar columnar deform over the
/// full prefix (the `sink_rearm_dictfree` precedent; a later serial
/// fallback re-arms its own shape through the staging ladder, idempotently).
fn sink_rearm_vguard_columnar<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> bool {
    let Some(plan) = ::nodeagg::agg_lanefold_plan(agg) else { return false };
    let mut need: i32 = fused_agg_soa_prefix(agg, ss).unwrap_or(0);
    for &c in plan.cols.iter().chain(plan.vguards.iter()) {
        need = need.max(c as i32 + 1);
    }
    if need <= 0 {
        return false;
    }
    if ::nodeseqscan::seq_scan_batch_soa(ss).is_some_and(|soa| soa.ncols() as i32 >= need) {
        return true;
    }
    // String-min/max engagement fix (suppress-then-unarmed bug class): on
    // QUAL-FREE single-varlena shapes the staging ladder armed the varkey
    // REMAP (one column at SoA 0, plan ncols == 0), which can never satisfy
    // `need` and which `seq_scan_cb_columnar_arm`'s foreign-consumer guard
    // rightly refuses to clobber — so every qual-free engagement refused
    // here ("vguard columnar staging") and the probe-suppressed plan landed
    // SERIAL. The remap is the fold feed's OWN staging (bare shape proven by
    // the shed's precondition, qual-free scans only — fn doc), so shed it
    // and arm the full columnar prefix — the direct-index staging the sink
    // drains require, the exact staging the QUALED path gets from the
    // PREWHERE dual arm. A later serial fallback re-arms the remap
    // idempotently through `arm_scan_staging`'s ladder.
    if let Some(vcol) = lanefold_varlane_col(plan) {
        let _ = ::nodeseqscan::seq_scan_cb_varlane_shed(ss, vcol);
    }
    ::nodeseqscan::seq_scan_cb_columnar_arm(ss, estate, need, None)
        && ::nodeseqscan::seq_scan_batch_soa(ss).is_some_and(|soa| soa.ncols() as i32 >= need)
}

/// The varlena-lane fold feed's single staged column, when the plan is that
/// shape: every lane read is the one varlena (str MIN/MAX) column. Any other
/// varlena-bearing plan (mixed fixed+varlena lane sets) returns None and the
/// fold refuses — the SoA prefix deform cannot stage an `attlen == -1` column
/// and the varkey pass stages exactly one.
fn lanefold_varlane_col(plan: &::lanefold::LanePlan<'_>) -> Option<u16> {
    match (&plan.vguards[..], &plan.cols[..]) {
        ([v], [c]) if v == c => Some(*v),
        _ => None,
    }
}

/// `LaneCols` remap for the varlena lane feed: the varkey pass stages the
/// single varlena column's per-row datum pointers into SoA column 0, while
/// the plan addresses that column by its scan attno.
struct VarLaneCols<'a, 'mcx> {
    soa: &'a ::exectuples::SoaBatch<'mcx>,
    col: u16,
}

impl ::lanefold::LaneCols for VarLaneCols<'_, '_> {
    fn col_values(&self, c: usize) -> &[::datum::Datum] {
        debug_assert_eq!(c, self.col as usize);
        self.soa.col_values(0)
    }

    fn col_isnull(&self, c: usize) -> &[bool] {
        debug_assert_eq!(c, self.col as usize);
        self.soa.col_isnull(0)
    }
}

/// `LaneCols` wrapper carrying the str MIN/MAX dict-code side channel
/// (lane-v2-dictminmax): delegates the lane reads to `inner` and answers
/// `col_codes` from the per-batch codes list the feed collected through
/// `seq_scan_batch_dict_codes` (which certifies the values-were-gathered
/// half of the contract; the sortedness half is the writer's
/// CHUNK_FLAG_DICT_SORTED, carried in the table). Keys are the PLAN's
/// column indexes (the inner wrapper owns any scan remap).
struct CodesCols<'a, C: ::lanefold::LaneCols> {
    inner: &'a C,
    codes: &'a [(u16, ::exectuples::SoaDictLane)],
}

impl<C: ::lanefold::LaneCols> ::lanefold::LaneCols for CodesCols<'_, C> {
    #[inline(always)]
    fn col_values(&self, c: usize) -> &[::datum::Datum] {
        self.inner.col_values(c)
    }

    #[inline(always)]
    fn col_isnull(&self, c: usize) -> &[bool] {
        self.inner.col_isnull(c)
    }

    #[inline(always)]
    fn col_len_staged(&self, c: usize) -> bool {
        self.inner.col_len_staged(c)
    }

    #[inline(always)]
    fn col_codes(&self, c: usize) -> Option<::exectuples::SoaDictLane> {
        self.codes.iter().find(|(pc, _)| *pc as usize == c).map(|(_, l)| *l)
    }
}

/// The plan's str MIN/MAX (text kinds only — bpchar never rides codes)
/// column list: (plan col, scan col) pairs, deduped. `map` translates plan
/// columns to scan columns (`None` entries never admit str transitions —
/// identity when absent).
fn mm_str_cols(
    plan: &::lanefold::LanePlan<'_>,
    map: impl Fn(u16) -> Option<u16>,
) -> Vec<(u16, u16)> {
    let mut out: Vec<(u16, u16)> = Vec::new();
    for t in plan.trans.iter() {
        if matches!(t.kind, ::lanefold::LaneKind::StrMin | ::lanefold::LaneKind::StrMax)
            && !out.iter().any(|&(pc, _)| pc == t.col)
        {
            if let Some(sc) = map(t.col) {
                out.push((t.col, sc));
            }
        }
    }
    out
}

/// Per-batch dict-code collection for the mm columns: `Some(lane)` per
/// column exactly when the CURRENT staged window certifies the `col_codes`
/// contract (dict window, values gathered — `seq_scan_batch_dict_codes`).
fn collect_mm_codes(
    ss: &::nodeseqscan::SeqScanState<'_>,
    mm_cols: &[(u16, u16)],
    out: &mut Vec<(u16, ::exectuples::SoaDictLane)>,
) {
    out.clear();
    for &(pc, sc) in mm_cols {
        if let Some(lane) = ::nodeseqscan::seq_scan_batch_dict_codes(ss, sc as usize) {
            out.push((pc, lane));
        }
    }
}

/// `LaneCols` for a fold plan that reads no lane columns (pure `count(*)`
/// transitions): the kernels never call these.
struct NoCols;

impl ::lanefold::LaneCols for NoCols {
    fn col_values(&self, _c: usize) -> &[::datum::Datum] {
        unreachable!("count(*)-only fold plans read no lane columns")
    }

    fn col_isnull(&self, _c: usize) -> &[bool] {
        unreachable!("count(*)-only fold plans read no lane columns")
    }
}

/// Build feed for the fold-armed breaker (`AggLaneChoice::Fold`): per staged
/// page batch, run the scan's per-row emit + the per-row group probe (with
/// the residual transitions inside the probe), snapshotting each row's
/// pergroup, then fold the admitted transitions whole-batch with
/// `lanefold::fold_rows_grouped`. One CHECK_FOR_INTERRUPTS per staged batch
/// (design §9 batch-operator cadence). Guarded plans re-prove every batch;
/// `Demote` runs the WHOLE batch through the checked per-row program (never
/// mixing a partial fold with per-row transitions — lanefold contract).
///
/// Byte-identity: the same rows flow through the same qual and the same
/// prepare/lookup/spill per-row machinery in the same order; only the
/// transition arithmetic is batched, and every fold kernel is either
/// commutative or (the str kinds) applied per row in row order, bit-for-bit
/// equal to C's transition semantics (lanefold's tested contract) — str
/// transvalue copies land in the agg context at exactly the per-row path's
/// datumCopy points, so hash-agg memory accounting and spill decisions are
/// unchanged too. Transvalues — and therefore output bytes — are identical.
fn agg_hash_build_fold_feed<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    stage_slot: &mut Option<ExecSlotId>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // K1 inc-1 source selection (the plain feed's Phase-1 pattern): heap
    // scans ride the dedicated HeapBatchSource iff PGRUST_LANE_V2_HEAPFEED
    // is on; everything else — and the whole knob-OFF world — constructs
    // SeqScanSource exactly as before (same monomorphized drain, same
    // machine code). Knob-ON, end-of-claim ownership sits on the source
    // (trait doc): the serial scan is ONE claim, settled right here after
    // the drain — on the ERROR path too (zero-pins-at-settle; the drain
    // error wins the report; strict on-error pin release is the
    // HeapBatchSource arm's — the below-floor SeqScanSource arm clears the
    // slot only, matching base knob-OFF, see SeqScanSource::end_claim).
    // Grouped consumers satisfy copy-at-the-
    // consumer under R3v pin-holding: key bytes copy at the
    // lookup_hash_entry insert / compact-table pack points and str
    // transvalues at C's datumCopy-into-aggcontext points, so no staged
    // pointer outlives its batch.
    //
    // GROUPED small-N floor (K1 inc-1's one new policy, heap_gagg_admits):
    // a heap grouped scan estimated under PGRUST_LANE_V2_HEAP_GAGG_FLOOR
    // keeps SeqScanSource — the heapfeed probe's plan-time >=1k-row
    // engagement rule (grouped crossover ~1k rows; plain stays ungated).
    use batch_source::BatchGranuleSource as _;
    if batch_source::heapfeed_v2_enabled() {
        if batch_source::heap_gagg_admits(ss) {
            // K1 inc-2 (wave-9 WS-AH): late materialization engages only on
            // this arm — HEAPFEED ∧ K1_LATEMAT ∧ gagg-admits (rail F: the
            // below-floor and knob-OFF worlds stay byte-untouched); the
            // per-build shape admission runs inside the drain.
            let latemat = batch_source::k1_latemat_enabled();
            let mut src = batch_source::HeapBatchSource::new(ss);
            let drove = agg_hash_build_fold_drain(agg, &mut src, stage_slot, latemat, estate);
            let settled = src.end_claim(estate);
            drove?;
            settled?;
        } else {
            let mut src = batch_source::SeqScanSource::new(ss);
            let drove = agg_hash_build_fold_drain(agg, &mut src, stage_slot, false, estate);
            let settled = src.end_claim(estate);
            drove?;
            settled?;
        }
    } else {
        agg_hash_build_fold_drain(
            agg,
            &mut batch_source::SeqScanSource::new(ss),
            stage_slot,
            false,
            estate,
        )?;
    }
    // Combine-before-finish (delegated; the Stage-4 seam): spill finish +
    // merge handoff, then the phase flip — AFTER the claim settle (the
    // zero-pins-at-settle law precedes the spill/merge work).
    ::nodeagg::agg_hash_build_combine(agg, estate)?;
    ::nodeagg::agg_hash_build_finish(agg, estate)
}

/// The hashed build feed's drain half (K1 inc-1 — the exact
/// `agg_plain_fold_drain_impl` treatment): generic over the storage seam's
/// batch source. Staged reads ride the trait's read face
/// (`batch_soa`/`skip_sel`/`lane_sel`/`emit`); the columnar-only branches
/// (dict-group col peek, str-mm dict-code memos) are caps-gated; branches
/// driving the shared kernel helpers (K2 probe, mk packed table, the
/// arrival-probe row loop) reach the hosted scan through the transitional
/// `seq_scan_bridge` — both scan implementors host a SeqScan, and WS-A
/// inc-2 deletes the bridge. Both instantiations monomorphize to #[inline]
/// delegation — the SeqScanSource instantiation is the pre-genericization
/// machine code (WS-A code-shape-neutral law).
fn agg_hash_build_fold_drain<'mcx, S: batch_source::BatchGranuleSource<'mcx>>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    src: &mut S,
    stage_slot: &mut Option<ExecSlotId>,
    latemat: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let caps = src.capabilities();
    // Scan-invariant end-of-scan clear ownership (process-static —
    // trait-doc single-owner rules): knob-OFF the drain clears inline
    // exactly as before; knob-ON the feed wrapper's end_claim owns it.
    let clear_inline = !batch_source::heapfeed_v2_enabled();
    // K2 admission for the scan feed, decided once per build (mirrors the
    // joined-row feed's `staged_feed_shape` mode choice): unguarded, fully
    // admitted (no residual transitions), single kernel-hostable grouping
    // key, with the key and every spill-replay column staged in the armed
    // SoA lanes. `None` = the per-row arrival probe (byte-identical).
    let k2 = scan_k2_shape(agg, batch_source::require_bridge(src)?, estate);
    // Stage-2.2 compact-table arming, per build, on top of the K2 shape
    // (nodeagg::compact module doc: int-width key kernel, AGGSPLIT_SIMPLE,
    // not spill-eligible by estimate; runtime backstop migrates to the C
    // table). Non-armed verdicts tick their observability reasons; the
    // build itself stays lane-owned either way.
    let compact = k2.is_some()
        && match ::nodeagg::agg_hash_compact_try_arm(agg) {
            ::nodeagg::CompactArm::Armed => true,
            ::nodeagg::CompactArm::KeyKind => {
                stats::tick_refused(ShapeClass::AggBuild, RefuseReason::CompactKeyKind);
                false
            }
            ::nodeagg::CompactArm::SpillRisk => {
                stats::tick_refused(ShapeClass::AggBuild, RefuseReason::CompactSpillRisk);
                false
            }
            ::nodeagg::CompactArm::Off => false,
        };
    // Stage-2.1 dict-group registration (per build): the K2 key column was
    // opted into dict lanes by `try_arm_cb_dictgroup`. Dict-answered windows
    // take the per-epoch code-grouping path inside `scan_k2_batch`; Raw
    // windows keep the Raw keys path — both through the same global table.
    let dictgroup = match &k2 {
        // Columnar-only peek (caps-gated bridge): heap sources never
        // publish a dict-group column.
        Some(s) if caps.dict_codes => {
            ::nodeseqscan::seq_scan_batch_dictgroup_col(batch_source::require_bridge(src)?)
                == Some(s.key_col)
        }
        _ => false,
    };
    let mut dgs = DictGroupScratch::default();
    // Packed multi-key admission + compact arm (multikey spike): only for
    // shapes the single-key K2 machinery does not own.
    let mk = if k2.is_none() {
        scan_mk_shape(agg, batch_source::require_bridge(src)?, estate)
    } else {
        None
    };
    let mut mks = MkScratch::default();
    trace_feed(if mk.is_some() {
        "agg-over-seqscan: staged fold feed engaged (multi-key packed)"
    } else if dictgroup {
        "agg-over-seqscan: staged fold feed engaged (dict-group armed)"
    } else if compact {
        "agg-over-seqscan: staged fold feed engaged (compact table)"
    } else if k2.is_some() {
        "agg-over-seqscan: staged fold feed engaged (k2 probe)"
    } else {
        "agg-over-seqscan: staged fold feed engaged"
    });
    let mut idxs: Vec<u32> = Vec::new();
    let mut groups: Vec<core::ptr::NonNull<::execexpr::AggPerGroup>> = Vec::new();
    // Varlena-lane plans read their one column through the varkey staging at
    // SoA column 0 (see lanefold_varlane_col / VarLaneCols). Multi-varlena
    // plans (lane-v2-dictminmax, multi-varlena class) admitted only over the pgrcolumnar
    // virtual-prefix staging, which stages every column at its NATURAL index
    // — no remap (vcol None).
    let vcol = {
        let plan = ::nodeagg::agg_lanefold_plan(agg).expect("fold feed without a plan");
        debug_assert!(
            plan.vguards.is_empty()
                || lanefold_varlane_col(plan).is_some()
                || src.batch_soa().is_some(),
            "multi-varlena fold without the cbstore staging armed"
        );
        lanefold_varlane_col(plan)
    };
    // Dual arm (q22coexist): when the PREWHERE lane owns the staging, the
    // varlena fold column sits at its NATURAL prefix index (the lane's
    // completing deform fills it for survivor windows) — no varkey remap.
    // The lane fills lazily, so the guard proof below must restrict itself
    // to the selection bitmap (unselected cells may be stale pointers).
    let lane_owned =
        ::nodeseqscan::seq_scan_batch_lane_armed(batch_source::require_bridge(src)?);
    let vremap = if lane_owned { None } else { vcol };
    // Str MIN/MAX dict-code memo (lane-v2-dictminmax): plan columns == scan
    // columns on this feed (identity map). Codes collect per batch; the
    // scratch invalidates whenever any row advanced str transitions through
    // the per-row program (demote / fallback / arrival-probe routes).
    let mm_cols = {
        let plan = ::nodeagg::agg_lanefold_plan(agg).expect("fold feed without a plan");
        mm_str_cols(plan, Some)
    };
    if !mm_cols.is_empty() && caps.dict_codes {
        trace_feed("fold str min/max dict-code memo armed");
    }
    let mut mm_scratch = ::lanefold::StrMmScratch::default();
    let mut mm_codes: Vec<(u16, ::exectuples::SoaDictLane)> = Vec::new();
    let mut k2s = ScanK2Scratch::default();
    // K1 inc-2 late-materialization arm (wave-9 WS-AH), decided once per
    // build AFTER the K2/mk shape census: staging narrows to {qual clause
    // cols ∪ the feed's key cols} and the deferred prefix columns complete
    // per batch for qual survivors only. Inc-3 splits the deferred set:
    // `now` (agg-needed columns — every whole-batch consumer's read set)
    // completes right after staging; `publish` (columns ONLY the per-row
    // emit's prefix publish reads) completes at the per-row fall-through
    // sites below, so kernel-leg batches never deform them at all. `None` =
    // today's full staging bytes (the knob-OFF world takes the `!latemat`
    // branch without touching the node). Guarded/vguard plans refuse NAMED
    // (rail G, `k1-latemat-guard-cols`); shapes without a stated key set
    // (per-row arrival builds) keep today's staging silently.
    let latemat_cols: Option<LatematCols> = if latemat {
        let ss = batch_source::require_bridge(src)?;
        // The mk needed-census natts check (scan_k2_shape's identity-map
        // precondition, computed here where estate is reachable); a missing
        // descriptor fails open inside the arm (publish set stays empty).
        let slot_id = ss.ss.ss_ScanTupleSlot;
        let natts = estate
            .slot(slot_id)
            .base()
            .tts_tupleDescriptor
            .as_ref()
            .map_or(usize::MAX, |d| d.attrs.len());
        scan_k1_latemat_arm(ss, agg, k2.as_ref(), mk.as_ref(), natts)
    } else {
        None
    };
    loop {
        let n = src.next_batch(estate)?;
        if n == 0 {
            if clear_inline {
                // End of scan: drop the scan slot's buffer pin
                // (SeqScanSource end-of-stream parity). Knob-ON this moves
                // to the source's end_claim.
                let ss = batch_source::require_bridge(src)?;
                let mcx = estate.es_query_cxt;
                ::exectuples::exec_clear_tuple(estate.slot_mut(ss.ss.ss_ScanTupleSlot), mcx);
            }
            break;
        }
        ::postgres_seams::check_for_interrupts::call()?;
        // K1 inc-2 completion (pass B): fill the agg-needed deferred prefix
        // columns for this batch's qual survivors off the still-pinned page
        // BEFORE any whole-batch consumer — probes, folds, and spill replays
        // read only agg-needed cells (`plan.cols ⊆ colnos_needed` is
        // `agg_fold_staged`'s documented contract; the K2 spill-miss replay
        // fills `shape.needed` cells only; the compact backstop migration
        // reads the compact table's own arena, never the SoA — rail S holds
        // by construction). Inc-3: the publish-only deferred columns (read
        // by NOTHING on the kernel legs) complete at the per-row
        // fall-through sites below instead. The whole-qual kernel bitmap IS
        // the survivor set on this admission (no requal tail); forced-
        // fallback bits OR'd into it are harmless (kind-0 rows only fill).
        if let Some(cols) = &latemat_cols {
            k1_latemat_complete(src, estate, &cols.now, n)?;
        }
        // Guarded plans (int2-Var OpExpr admissions): prove the batch before
        // any fold. The proof runs over every staged non-fallback row — a
        // superset of the rows the fold will touch — so a Pass is sound and a
        // Demote at worst conservative (the checked per-row program is always
        // correct; it raises C's error at C's row when a selected row really
        // overflows).
        let mut demote = false;
        {
            let plan = ::nodeagg::agg_lanefold_plan(agg).expect("fold feed without a plan");
            if plan.guarded {
                let soa = src.batch_soa().expect("guarded fold plans read lane columns");
                let nwords = (n as usize).div_ceil(64);
                let mut rows = [0u64; ::exectuples::SOA_BM_WORDS];
                // Proof domain: every staged non-fallback row — a superset of
                // the rows the fold will touch, so a Pass is sound and a
                // Demote at worst conservative. Under the PREWHERE lane the
                // staged columns fill lazily (survivor windows only), so the
                // domain must intersect the selection bitmap: unselected
                // cells may be stale pointers, and the fold touches only
                // selected rows anyway (requal survivors ⊆ selected bits).
                match src.lane_sel() {
                    Some(sel) if lane_owned => {
                        for ((r, fb), s) in
                            rows[..nwords].iter_mut().zip(soa.fallback_words()).zip(sel)
                        {
                            *r = s & !fb;
                        }
                    }
                    _ => {
                        for (r, fb) in rows[..nwords].iter_mut().zip(soa.fallback_words()) {
                            *r = !fb;
                        }
                    }
                }
                if n % 64 != 0 {
                    rows[nwords - 1] &= (1u64 << (n % 64)) - 1;
                }
                // Empty domain: nothing to prove and nothing will fold —
                // never probe lane cells (a survivor-less lane window ran no
                // completing deform; every cell is stale).
                if rows[..nwords].iter().any(|&w| w != 0) {
                    // SAFETY: proof rows are staged non-fallback rows — under
                    // a varkey/prefix staging every staged row's lane values
                    // are live page datum pointers (staging contract); under
                    // the PREWHERE lane the domain is selected rows of a
                    // survivor window, whose completing deform filled every
                    // prefix column with decoded datums — vguard columns
                    // readable at their varlena header byte either way.
                    demote = unsafe {
                        match vremap {
                            Some(c) => ::lanefold::check_guards(
                                plan,
                                &VarLaneCols { soa, col: c },
                                &rows[..nwords],
                                |_| None,
                            ),
                            None => {
                                ::lanefold::check_guards(plan, soa, &rows[..nwords], |_| None)
                            }
                        }
                    } == ::lanefold::GuardCheck::Demote;
                }
            }
        }
        if demote {
            // K1 inc-3: this batch leaves the kernel legs for the per-row
            // emit — complete the publish-only deferred columns first (the
            // emit's `soa_store_prefix` publishes every prefix cell of each
            // selected row; rail B: never a stale published cell). Demote is
            // unreachable while rail G refuses guarded plans, but the leg
            // stays safe on its own terms.
            if let Some(cols) = &latemat_cols {
                k1_latemat_complete(src, estate, &cols.publish, n)?;
            }
            // The per-row program advances the admitted str transitions
            // behind the memo's back — drop every memo (StrMmScratch doc).
            // Emit-dead word skip (skip-sel: cleared bits are definitive
            // rejections, even under requal) — same accepted rows/order.
            mm_scratch.invalidate();
            let skip = {
                let mut w = [0u64; ::exectuples::SOA_BM_WORDS];
                src.skip_sel().map(|s| {
                    w[..s.len()].copy_from_slice(s);
                    w
                })
            };
            ::exectuples::for_each_live(skip.as_ref().map(|w| &w[..]), 0, n, |i| -> PgResult<()> {
                if let Some(slot) = src.emit(estate, i)? {
                    ::nodeagg::agg_hash_build_accept(agg, estate, slot)?;
                }
                Ok(())
            })?;
            continue;
        }
        // K2 deferred batched probe, per batch: only when EVERY staged row
        // carries lane values — a fallback row has no staged key, and probing
        // it at arrival while deferring its neighbors would reorder
        // first-arrival insertions. Batches with any fallback row keep the
        // arrival probe wholesale (both modes probe in row order, so a
        // per-batch mode choice preserves the global insertion sequence).
        if let Some(shape) = &k2 {
            let all_lane = src
                .batch_soa()
                .is_some_and(|soa| soa.fallback_words().iter().all(|&w| w == 0));
            if all_lane {
                scan_k2_batch(
                    agg,
                    batch_source::require_bridge(src)?,
                    shape,
                    stage_slot,
                    &mut k2s,
                    dictgroup.then_some(&mut dgs),
                    &mut idxs,
                    &mut groups,
                    n,
                    estate,
                )?;
                // The K2 fold ran without the memo (str advances bypass it)
                // — keep the memo coherent for any later arrival batch.
                mm_scratch.invalidate();
                continue;
            }
            // A fallback-bearing batch routes through the arrival probe (the
            // C table): the compact table must hand its groups over FIRST so
            // every group lives in exactly one table (states carried over
            // byte-for-byte; no-op when not armed).
            ::nodeagg::agg_hash_compact_disarm(agg, estate)?;
        }
        // Packed multi-key batch (multikey spike): only while the compact
        // table stays armed — after a backstop migration (scan_mk_batch =
        // false) or a fallback-bearing batch, this and every later batch
        // route through the per-row arrival probe below (the C table now
        // holds every group; there is no multi-key staged C probe).
        if let Some(shape) = &mk {
            if ::nodeagg::agg_hash_compact_armed(agg) {
                let all_lane = src
                    .batch_soa()
                    .is_some_and(|soa| soa.fallback_words().iter().all(|&w| w == 0));
                if all_lane {
                    if scan_mk_batch(
                        agg,
                        batch_source::require_bridge(src)?,
                        shape,
                        &mut mks,
                        &mut idxs,
                        &mut groups,
                        n,
                        None,
                        estate,
                    )? {
                        // As the K2 arm: the mk fold bypassed the memo.
                        mm_scratch.invalidate();
                        continue;
                    }
                } else {
                    ::nodeagg::agg_hash_compact_disarm(agg, estate)?;
                }
            }
        }
        // K1 inc-3: every route from here is per-row — the batch left the
        // kernel legs (fallback-bearing K2/mk batches, an mk backstop
        // migration's fall-through, post-migration sticky batches). The
        // emit's `soa_store_prefix` publishes every prefix cell of each
        // selected row, so the publish-only deferred columns complete now
        // (rail B); the `now` columns completed after staging above.
        if let Some(cols) = &latemat_cols {
            k1_latemat_complete(src, estate, &cols.publish, n)?;
        }
        idxs.clear();
        groups.clear();
        // Phase-3 qual kernel: with the selection bitmap staged for this
        // batch, walk ONLY the survivors (bitmap hits + forced fallback bits,
        // re-checked per-row inside the emit) — same rows, same ascending
        // order as the full walk, whose emit would have bit-tested each row
        // anyway. Non-kernel quals keep the full per-row walk.
        {
            let ss = batch_source::require_bridge(src)?;
            if ::nodeseqscan::seq_scan_batch_qual_bitmap_ready(ss) {
                while let Some(i) = ::nodeseqscan::seq_scan_batch_next_selected(ss) {
                    agg_fold_feed_row(agg, ss, estate, &mut idxs, &mut groups, i)?;
                }
            } else {
                for i in 0..n {
                    agg_fold_feed_row(agg, ss, estate, &mut idxs, &mut groups, i)?;
                }
            }
        }
        // Fallback rows advanced str transitions through the full per-row
        // accept above — drop every memo before this batch's fold.
        if !mm_cols.is_empty()
            && src
                .batch_soa()
                .is_some_and(|soa| soa.fallback_words().iter().any(|&w| w != 0))
        {
            mm_scratch.invalidate();
        }
        // SAFETY: non-fallback rows carry valid deformed lane values for
        // every plan column (the SoA prefix covers the evaltrans fetch
        // bound; varlena lanes are page datum pointers from the varkey
        // staging, pinned for the staged batch); guarded plans passed
        // `check_guards` above; dict-code views satisfy the col_codes
        // contract (`seq_scan_batch_dict_codes`); the rest is
        // `agg_fold_staged`'s per-feed contract.
        if caps.dict_codes {
            collect_mm_codes(batch_source::require_bridge(src)?, &mm_cols, &mut mm_codes);
        } else {
            // Heap sources certify no dict windows; the memo list still
            // resets per batch (CodesCols reads it).
            mm_codes.clear();
        }
        match (src.batch_soa(), vremap) {
            (Some(soa), Some(cix)) => unsafe {
                agg_fold_staged_mm(
                    agg,
                    &CodesCols { inner: &VarLaneCols { soa, col: cix }, codes: &mm_codes },
                    &idxs,
                    &groups,
                    Some(&mut mm_scratch),
                )?
            },
            (Some(soa), None) => unsafe {
                agg_fold_staged_mm(
                    agg,
                    &CodesCols { inner: soa, codes: &mm_codes },
                    &idxs,
                    &groups,
                    Some(&mut mm_scratch),
                )?
            },
            (None, _) => {
                debug_assert!(
                    ::nodeagg::agg_lanefold_plan(agg).is_some_and(|p| p.cols.is_empty())
                );
                unsafe { agg_fold_staged(agg, &NoCols, &idxs, &groups)? }
            }
        }
    }
    Ok(())
}

/// One staged row of the fold build feed: the per-row emit (per-tuple ctx
/// reset, store, qual), then route — SoA fallback rows to the full per-row
/// transition program (they carry no lane values; the order split across
/// transitions is bit-invisible — commutative kernels), everything else
/// through the group probe with its pergroup snapshotted for the whole-batch
/// fold.
fn agg_fold_feed_row<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    idxs: &mut Vec<u32>,
    groups: &mut Vec<core::ptr::NonNull<::execexpr::AggPerGroup>>,
    i: u32,
) -> PgResult<()> {
    let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(ss, estate, i)? else {
        return Ok(());
    };
    if ::nodeseqscan::seq_scan_batch_soa(ss).is_some_and(|soa| soa.is_fallback(i)) {
        ::nodeagg::agg_hash_build_accept(agg, estate, slot)?;
    } else if let Some(pg) = ::nodeagg::agg_hash_build_probe_resid(agg, estate, slot)? {
        idxs.push(i);
        groups.push(pg);
    }
    Ok(())
}

// ===========================================================================
// Plain-agg (AGG_PLAIN, ungrouped) fold drive — the one-group
// `SELECT sum(a), avg(b), count(*) FROM t [WHERE ...]` shapes. SIMPLER than
// the hashed breaker: one group, no probe — each staged batch folds straight
// into the single pergroup array via `lanefold::fold_batch` (the ungrouped
// kernel, CSE schedule included), and the retrieve side is the delegated
// `plain_finish` (finalize + HAVING + project, one row, zero-row contract
// included). The whole node runs inside one `exec_proc_node` call, exactly
// like `exec_agg`'s single-group arm.
// ===========================================================================

/// Try to let the lane own an AGG_PLAIN `Agg` over a `SeqScan` child with the
/// batched fold. `Some(result)` = the lane drove this call; `None` = refused
/// (the caller falls through to the fused `exec_agg_batched` / per-tuple
/// paths, byte-identically).
fn try_own_plain_agg_over_seq_scan<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    choice: &mut Option<AggLaneChoice>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Scan-side refuse-set: the Phase-1 gate verbatim (dynamic EPQ/direction
    // gates re-checked per call; structural verdict memoized on the node).
    if !seq_scan_fusible(ss, estate)? {
        // EA-on-morsels (docs/design/ea-morsels.md §5): the serial lane
        // refuses instrumented scans (its batched drive bypasses the
        // per-node instrument wrappers), but the RUNTIME arm's workers run
        // uninstrumented executors — so under EXPLAIN ANALYZE the runtime
        // arm still gets its walk, gated on the SAME lane choice the
        // uninstrumented run computes (E4: EA may never change the
        // engagement decision except the instrument gate itself).
        if runtime_instr::ea_active(estate) {
            let c = match *choice {
                Some(c) => c,
                None => {
                    let c = decide_plain_agg_lane(agg, ss, estate)?;
                    *choice = Some(c);
                    c
                }
            };
            if matches!(c, AggLaneChoice::Fold | AggLaneChoice::Refuse) {
                if let Some(r) = runtime_scan::try_own_plain_agg_runtime(agg, ss, estate)? {
                    return Ok(Some(r));
                }
            }
        }
        return Ok(None);
    }
    let c = match *choice {
        Some(c) => c,
        None => {
            let c = decide_plain_agg_lane(agg, ss, estate)?;
            *choice = Some(c);
            c
        }
    };
    // Metadata-answer arm: the whole node from pgrcolumnar footers, zero rows
    // staged. Runtime gates (MVCC snapshot / AM answerability / guard
    // re-proof) are re-checked per call; a runtime refusal falls back to the
    // per-row Volcano drive byte-identically (it may raise C's overflow
    // error at C's row — exactly what the guard re-proof protects).
    if c == AggLaneChoice::Meta {
        // exec_agg's top-of-call guard (exec_agg_meta re-checks it too).
        if ::nodeagg::agg_is_done(agg) {
            return Ok(Some(None));
        }
        return try_meta_agg_answer(agg, ss, estate);
    }
    // M1 runtime scan arm (the Meta arm preempts above — footer answers
    // beat any parallel scan): FORCED engagement under PGRUST_RUNTIME=1 +
    // pgrust.runtime_scan_pool, serial plan surface unchanged; the morsel
    // source dispatches on the table AM (pgrcolumnar granules / heap block
    // ranges). Owns the Fold shapes AND the qualed count-only census shape
    // (serial-refused on pgrcolumnar because the footer is the serial lever,
    // and on heap for admission economics — but a parallel qual-bitmap
    // scan is exactly M1's LIKE-count target). Heap Refuse shapes that are
    // neither fold-prefix-armable nor census fall through inside the arm.
    // None = not engaged/refused — fall through byte-identically (nothing
    // was consumed).
    if matches!(c, AggLaneChoice::Fold | AggLaneChoice::Refuse) {
        if let Some(r) = runtime_scan::try_own_plain_agg_runtime(agg, ss, estate)? {
            return Ok(Some(r));
        }
    }
    if c == AggLaneChoice::Refuse {
        return Ok(None);
    }
    // exec_agg's top-of-call guard: the one result row is out; a drained agg
    // stays drained until rescan clears `agg_done`.
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    // One OWNED tick per lane-owned plain-agg build event (the gate's
    // aggbuild floor counts builds, not calls; a plain node builds once per
    // (re)scan — this drive runs the whole feed inside one call).
    stats::tick_owned(ShapeClass::AggBuild);
    if c == AggLaneChoice::Fold {
        agg_plain_fold_feed(agg, ss, estate)?;
    } else {
        agg_plain_perrow_feed(agg, ss, estate)?;
    }
    // Retrieve (delegated): finalize + HAVING + project — one row (or none,
    // when the var-free HAVING rejects it), setting `agg_done`.
    Ok(Some(::nodeagg::agg_plain_finish(agg, estate)?))
}

/// Build feed for the plain PER-ROW drive (`AggLaneChoice::PerRow` — pgrcolumnar
/// scans only, lane-v2-noqualfeed): drain the Phase-1 scan pipeline (batch
/// window decode; the PREWHERE/kernel-bitmap arms engage when the qual has a
/// kernel shape) into the FULL per-row transition program. This replaces the
/// per-pull Volcano chain (`exec_agg` → `exec_proc_node` → `getnextslot`)
/// with one drained loop over staged windows; no fold plan is required, so
/// arbitrary transition expressions (the arithmetic SUM(x op k) batteries)
/// are hosted.
///
/// Byte-identity: the same rows flow through the same qual (staged bitmap =
/// the kernel qual's verdict; other quals run scalar per row inside the
/// emit) and the same per-row transition program (`agg_plain_build_accept` =
/// `exec_agg`'s single-group loop body) in the same row order — only the
/// pull chain is elided. The transvalues, and therefore the one finalized
/// output row, are identical.
fn agg_plain_perrow_feed<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    debug_assert!(::nodeseqscan::seq_scan_is_pgrcolumnar(ss));
    // Row-emit staging (drain pipeline): PREWHERE v1 / kernel bitmap when a
    // qual kernel exists; a no-qual scan stages bare batch-decoded windows.
    arm_scan_staging(
        ss,
        estate,
        ScanFeedShape::RowFeed { ctx: "plain agg per-row feed", stitch: true },
    )?;
    // initialize_aggregates (delegated): fresh initval pergroups; a rescan
    // re-enters here with agg_done cleared.
    ::nodeagg::agg_plain_build_begin(agg, estate)?;
    let mut sink = PlainAggBuildSink { agg };
    drain_pipeline(ss, &mut SeqScanSource, &mut SeqScanFilterProject, &mut sink, estate)
}

/// The plain agg as breaker Sink: accept = the full per-row transition
/// program (`exec_agg`'s single-group loop body, delegated); finish = no-op
/// (finalize/HAVING/project is the caller's `agg_plain_finish`, exactly as
/// the fold drive sequences it). Always `NeedMore` — a breaker consumes its
/// whole input.
struct PlainAggBuildSink<'a, 'mcx> {
    agg: &'a mut ::nodeagg::AggStateData<'mcx>,
}

impl<'mcx> Sink<'mcx> for PlainAggBuildSink<'_, 'mcx> {
    fn accept(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<SinkFeed> {
        ::nodeagg::agg_plain_build_accept(self.agg, estate, tuple)?;
        Ok(SinkFeed::NeedMore)
    }

    fn finish(&mut self, _estate: &mut EStateData<'mcx>) -> PgResult<()> {
        Ok(())
    }
}

/// Batch-granular feed: the default loop, monomorphized (same rows, same
/// order; the per-row dyn dispatch elided) — mirrors `HashAggBuildSink`.
impl<'mcx> BatchSink<'mcx> for PlainAggBuildSink<'_, 'mcx> {}

/// The structural lane choice for an AGG_PLAIN Agg over a SeqScan, decided
/// once at the first call.
///
/// Heap scans: Fold or Refuse only — the lane never takes heap plain shapes
/// per-row: the incumbent legacy fused `exec_agg_batched` drive is already
/// batched with per-row transitions, so a per-row lane feed has nothing to
/// win (admission economics, design §4).
///
/// pgrcolumnar scans (lane-v2-noqualfeed, phase4 §7 re-entry): the incumbent
/// fused drive is gated OFF (`table_scan_supports_pagebatch` false — lane-OFF
/// stays the per-row Volcano oracle), so the heap Refuse arms take the
/// PER-ROW drain feed instead: batch window decode + the full per-row
/// transition program beats the per-pull Volcano chain regardless of quals
/// (the shape the old kernel-armed gate mis-scoped — its 1.21-1.33x evidence
/// measured the standalone capacity-one RowFeed adapter, not a drained
/// breaker feed). The one pgrcolumnar Refuse left is the count(*)-only census
/// shape: transitions reading NO input columns decode nothing on the per-row
/// drive (empty needed set) and are the MetaAggScan footer path's target —
/// a batch-decoded feed has nothing to win there (distinct reason so the
/// gate can watch it).
fn decide_plain_agg_lane<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<AggLaneChoice> {
    let is_cb = ::nodeseqscan::seq_scan_is_pgrcolumnar(ss);
    // Heap Refuse = admission economics (§4): the legacy fused
    // `exec_agg_batched` drive (or the per-tuple path) already owns the shape
    // at least as well as a lane feed could. One tick per memoized per-node
    // choice.
    let refuse = || {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AdmissionEconomicsFusedDrive);
        Ok(AggLaneChoice::Refuse)
    };
    // Metadata-answer arm first (phase4 §7 re-entry, armed 2026-07-14): a
    // bare pgrcolumnar scan under an all-footer-answerable transition set
    // answers from part metadata — strictly cheaper than any fold feed, so
    // it preempts the fold decision below wherever it admits.
    if meta_agg_admissible(agg, ss, estate)? {
        return Ok(AggLaneChoice::Meta);
    }
    // count(*)-only census shapes (the transition program reads no input
    // columns): heap's incumbent fused drive advances those per batch with
    // zero per-row work (the storeless advance / `qualifying_count` bitmap
    // census); pgrcolumnar's per-row drive decodes nothing (empty needed set)
    // and the footer answer (MetaAggScan) is the real lever — a bare-pgrcolumnar
    // count(*) is answered by the Meta arm above; one reaching here has a
    // qual/projection/uncovered transition. Deliberate refuse-set entries,
    // one tick per memoized choice.
    if ::nodeagg::agg_batch_outer_prefix(agg) == Some(0) {
        if is_cb {
            stats::tick_refused(ShapeClass::AggBuild, RefuseReason::CountOnlyCensus);
            return Ok(AggLaneChoice::Refuse);
        }
        return refuse();
    }
    // Fold-readiness: a classified fold plan reading lane columns on an
    // unprojected scan (projected scans read output columns, which are not
    // commensurable with scan-column prefixes — the hashed breaker's
    // scoping, verbatim), with the forced prefix deform probe-armed NOW so
    // an unarmable prefix (non-fixed-width column) is known BEFORE
    // committing.
    let fold_ready = match ::nodeagg::agg_lanefold_plan(agg) {
        Some(plan) if ss.ss.ps_ProjInfo.is_none() && !plan.cols.is_empty() => {
            probe_arm_fold_prefix(agg, ss, estate)?
        }
        _ => false,
    };
    if fold_ready {
        return Ok(AggLaneChoice::Fold);
    }
    if is_cb {
        return Ok(AggLaneChoice::PerRow);
    }
    refuse()
}

/// Metadata-answer arm kill switch: default ON when the lane is on;
/// `PGRUST_LANE_V2_METAAGG=0`/`off` disarms (A/B tooling — both sides are
/// value-identical by exec_agg_meta's end-state contract, so the switch is
/// byte-identity-safe like `PGRUST_LANE_V2_K2`).
fn metaagg_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_LANE_V2_METAAGG").as_deref(), Ok("0") | Ok("off"))
    })
}

/// Zero-count qual arm kill switch (v7 footer zero/empty counts): default ON
/// under the metaagg arm; `PGRUST_LANE_V2_ZEROCNT=0`/`off` disarms just the
/// qual extension (byte-identity-safe A/B — refused shapes fall to the scan
/// drive, which answers identically).
fn zerocnt_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_LANE_V2_ZEROCNT").as_deref(), Ok("0") | Ok("off"))
    })
}

/// Footer-stat fold-drive meta arm kill switch (whole-RG / whole-granule
/// aggregate answers under an all-rows-passing zone proof): default ON under
/// the lane; `PGRUST_LANE_V2_FOLDMETA=0`/`off` disarms (byte-identity-safe
/// A/B — declined units stage and fold from decoded lanes identically).
fn foldmeta_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_LANE_V2_FOLDMETA").as_deref(), Ok("0") | Ok("off"))
    })
}

/// Zero-count qual transition filter: under `col <> 0` / `col = 0` a
/// transition is footer-answerable iff it is a COUNT (rows arrives
/// qual-adjusted: N - zeros or zeros) or a SUM/AVG-family fold over the QUAL
/// column itself (excluded zero rows contribute exactly zero to the footer
/// sum S; the affine addend term scales by the qual-adjusted rows — the
/// exec_agg_meta derivation is unchanged). MIN/MAX refuse: the zone
/// extremes include the excluded rows.
fn meta_trans_zero_ok(
    metas: &[::lanefold::MetaTrans],
    zq: ::tableam::MetaZeroQual,
) -> bool {
    metas.iter().all(|t| match t.kind {
        ::lanefold::MetaKind::Count => true,
        ::lanefold::MetaKind::Min | ::lanefold::MetaKind::Max => false,
        k => k.needs_sum() && t.col == zq.col,
    })
}

/// Structural admission for the metadata-answer arm, evaluated once per
/// memoized per-node choice: a BARE pgrcolumnar scan (variant Plain — no qual,
/// no projection — and no zone quals; v1 requires literally no qual) under
/// an AGG_PLAIN node whose EVERY transition is footer-answerable
/// (`classify_meta`: count(*)/count(col)/min/max over bare int-family Vars,
/// sum/avg over affine divk==1 int transforms; FILTER/DISTINCT/ORDER BY and
/// the float/bool/bitwise/text tiers refuse). Ticks the metaagg class only
/// for pgrcolumnar-backed scans — heap plain aggs are out of the arm's scope
/// (heap has no part metadata) and fall through silently.
fn meta_agg_admissible<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if !::nodeseqscan::seq_scan_is_pgrcolumnar(ss) {
        return Ok(false);
    }
    if !metaagg_enabled() {
        stats::tick_refused(ShapeClass::MetaAgg, RefuseReason::EnvOff);
        return Ok(false);
    }
    let Some(metas) = ::nodeagg::agg_meta_plan(agg) else {
        stats::tick_refused(ShapeClass::MetaAgg, RefuseReason::MetaShape);
        return Ok(false);
    };
    // Bare arm (v1): no qual at all.
    if ::nodeseqscan::seq_scan_meta_agg_ok(ss, estate)? {
        return Ok(true);
    }
    // Zero-count qual arm (v7): the whole qual is one `col <> 0` / `col = 0`
    // conjunct and every transition is answerable under it.
    if zerocnt_enabled() {
        if let Some(zq) = ::nodeseqscan::seq_scan_meta_zero_qual(ss, estate)? {
            if meta_trans_zero_ok(metas, zq) {
                return Ok(true);
            }
        }
    }
    stats::tick_refused(ShapeClass::MetaAgg, RefuseReason::MetaShape);
    Ok(false)
}

/// Per-call runtime half of the metadata-answer arm. `Ok(Some(_))` = the
/// node was answered from footers (one finalized row or a drained None);
/// `Ok(None)` = runtime refusal — the caller falls through to the per-row
/// Volcano drive byte-identically.
fn try_meta_agg_answer<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // RG xmin visibility folds against the scan snapshot: MVCC only (the
    // same gate the fused metacount arm carried on the old branch).
    if !estate
        .es_snapshot
        .as_deref()
        .is_some_and(::types_snapshot::IsMVCCSnapshot)
    {
        stats::tick_refused(ShapeClass::MetaAgg, RefuseReason::NonMvccSnapshot);
        return Ok(None);
    }
    let metas = ::nodeagg::agg_meta_plan(agg).expect("Meta choice requires a meta plan");
    // Guarded sum cols join the minmax request: the guard re-proof below
    // needs the visible rows' exact (min, max).
    let cols: Vec<u16> = metas
        .iter()
        .filter(|t| {
            matches!(t.kind, ::lanefold::MetaKind::Min | ::lanefold::MetaKind::Max)
                || t.guard.is_some()
        })
        .map(|t| t.col)
        .collect();
    let mut sum_cols: Vec<u16> =
        metas.iter().filter(|t| t.kind.needs_sum()).map(|t| t.col).collect();
    sum_cols.sort_unstable();
    sum_cols.dedup();
    // Zero-count qual arm: recompute the (deterministic, plan-derived) qual
    // the admission site accepted; None on bare-admitted nodes. The
    // admission filter already restricted the transitions to the shapes the
    // qual-adjusted (rows, sums) answer exactly.
    let zq = if zerocnt_enabled() {
        ::nodeseqscan::seq_scan_meta_zero_qual(ss, estate)?
    } else {
        None
    };
    let Some(res) = ::nodeseqscan::seq_scan_meta_agg(ss, estate, &cols, &sum_cols, zq)? else {
        // AM declined: parallel scan desc, an uncovered column type, or a
        // zero-count qual over a part with v<=6-preserved RGs.
        stats::tick_refused(ShapeClass::MetaAgg, RefuseReason::MetaRuntime);
        return Ok(None);
    };
    // Data-level guard re-proof against the visible rows' footer min/max: a
    // failed interval falls through to the ordinary drives, whose per-row
    // program raises C's int4 overflow error at C's row. rows == 0 passes
    // vacuously (empty minmax stays (MAX, MIN)).
    let guards_ok = res.rows == 0
        || metas.iter().all(|t| match t.guard {
            None => true,
            Some((lo, hi)) => res
                .minmax
                .iter()
                .find(|e| e.0 == t.col)
                .is_some_and(|&(_, mn, mx)| lo <= mn && mx <= hi),
        });
    if !guards_ok {
        stats::tick_refused(ShapeClass::MetaAgg, RefuseReason::MetaRuntime);
        return Ok(None);
    }
    // One OWNED tick per metadata-answered execution event.
    stats::tick_owned(ShapeClass::MetaAgg);
    if zq.is_some() {
        lane_trace(&format!("metaagg: zerocnt footer answer, rows={}", res.rows));
    } else {
        lane_trace(&format!("metaagg: footer answer, rows={}", res.rows));
    }
    Ok(Some(::nodeagg::exec_agg_meta(agg, estate, res.rows, &res.minmax, &res.sums)?))
}

/// Footer-stat fold-drive meta arm (the footer-stat consumption lane): the
/// PLAIN fold drive's whole-RG / whole-granule metadata shortcut. Once per
/// serial drain, admission proves (a) every admitted transition is
/// footer-answerable over an all-rows-passing unit (`lanefold::
/// agg_meta_cols` — count / plain int min-max / affine divk==1 int sum-avg
/// via the v4 RG footer sums / bare int8 sum-avg / plain octet_length
/// sum-avg via the v7 length stats), and (b) the scan's zone quals ARE the
/// whole qual with the staged bitmap owning it (`seq_scan_agg_meta_qual_ok`)
/// — so a unit whose every zone verdict is AllPass folds from footer
/// metadata with NO decode. Guarded plans re-prove each unit against the
/// unit's exact footer (min, max) — a failed interval declines the unit,
/// which then stages and fold/demotes exactly as before (check_guards'
/// value domain over an all-passing unit IS the footer extreme pair).
struct FoldMetaArm {
    mm_cols: Vec<u16>,
    sum_cols: Vec<u16>,
    len_cols: Vec<u16>,
    // (col, lo, hi) integer guard intervals; col is always in mm_cols.
    guards: Vec<(u16, i64, i64)>,
    mm: Vec<(i64, i64)>,
    sums: Vec<i128>,
    lens: Vec<i64>,
    rg_units: u64,
    granule_units: u64,
}

fn plain_fold_meta_arm<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> Option<FoldMetaArm> {
    if !::nodeseqscan::seq_scan_is_pgrcolumnar(ss) || !foldmeta_enabled() {
        return None;
    }
    // RG xmin visibility folds against the scan snapshot: MVCC only (the
    // metaagg arm's own gate).
    if !estate
        .es_snapshot
        .as_deref()
        .is_some_and(::types_snapshot::IsMVCCSnapshot)
    {
        return None;
    }
    let plan = ::nodeagg::agg_lanefold_plan(agg)?;
    let cols = ::lanefold::agg_meta_cols(plan)?;
    if !::nodeseqscan::seq_scan_agg_meta_qual_ok(ss) {
        return None;
    }
    let guards: Vec<(u16, i64, i64)> =
        plan.guards.iter().map(|g| (g.col, g.lo, g.hi)).collect();
    let (nmm, nsum, nlen) = (cols.mm_cols.len(), cols.sum_cols.len(), cols.len_cols.len());
    Some(FoldMetaArm {
        mm_cols: cols.mm_cols,
        sum_cols: cols.sum_cols,
        len_cols: cols.len_cols,
        guards,
        mm: vec![(0, 0); nmm],
        sums: vec![0; nsum],
        lens: vec![0; nlen],
        rg_units: 0,
        granule_units: 0,
    })
}

/// Consume 0+ whole scan units (row groups / granules) from footer metadata
/// at the drain loop's head: peek; while the upcoming unit is wholly visible
/// and every zone qual is AllPass over its footer extremes (all rows pass
/// the whole qual — the arm's admission proved the zone quals mirror it),
/// re-prove any guard intervals, fold the transitions from (rows, footer
/// (min, max), footer sums, Σ octet_length) and skip the unit's decode
/// entirely. Stops at a mid-granule position, a non-meta unit, a failed
/// guard re-proof, or scan end. Byte-identity: the unit's rows are exactly
/// the rows next_window would stage, all selected (AllPass bitmap) and
/// non-fallback (nothing staged); `fold_agg_meta`'s state mutations are
/// `fold_batch`'s own over that selection (see its bit-equality contract).
fn agg_plain_fold_meta_units<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    arm: &mut FoldMetaArm,
) -> PgResult<()> {
    loop {
        let step = ::nodeseqscan::seq_scan_agg_meta_peek(
            ss,
            estate,
            &arm.mm_cols,
            &arm.sum_cols,
            &arm.len_cols,
            &mut arm.mm,
            &mut arm.sums,
            &mut arm.lens,
        )?;
        let rows = match step {
            ::tableam::CbAggMetaStep::MetaRg { rows } => rows as i64,
            ::tableam::CbAggMetaStep::MetaGranule { rows } => rows as i64,
            _ => return Ok(()),
        };
        // Data-level guard re-proof against the unit's exact footer
        // (min, max): a failed interval declines — the unit stages and the
        // per-batch check_guards/demote machinery owns it byte-identically.
        let guards_ok = arm.guards.iter().all(|&(col, lo, hi)| {
            let i = arm.mm_cols.iter().position(|&c| c == col).expect("guard col staged");
            lo <= arm.mm[i].0 && arm.mm[i].1 <= hi
        });
        if !guards_ok {
            return Ok(());
        }
        debug_assert!(rows > 0);
        let plan = ::nodeagg::agg_lanefold_plan(agg).expect("meta arm proved the plan");
        let aggcx = ::nodeagg::agg_aggcontext(agg);
        let pos = |cols: &[u16], c: u16| {
            cols.iter().position(|&x| x == c).expect("meta arm staged every column")
        };
        // SAFETY: pergroup contract identical to the drain's fold_batch call
        // (once-allocated single-group pergroup array, live AvgAccum
        // transarray, NULL-or-live Int128AggState with `aggcx` its
        // aggcontext); admissibility proven by agg_meta_cols in
        // plain_fold_meta_arm; guarded intervals re-proved above.
        unsafe {
            ::lanefold::fold_agg_meta(
                plan,
                rows,
                |c| arm.mm[pos(&arm.mm_cols, c)],
                |c| arm.sums[pos(&arm.sum_cols, c)],
                |c| arm.lens[pos(&arm.len_cols, c)],
                ::nodeagg::agg_plain_pergroup_base(agg),
                aggcx,
            )?;
        }
        match step {
            ::tableam::CbAggMetaStep::MetaRg { .. } => {
                ::nodeseqscan::seq_scan_agg_meta_consume_rg(ss);
                arm.rg_units += 1;
            }
            _ => {
                ::nodeseqscan::seq_scan_agg_meta_consume_granule(ss);
                arm.granule_units += 1;
            }
        }
        ::postgres_seams::check_for_interrupts::call()?;
    }
}

/// Feed for the plain fold drive: per staged page batch, compose the row
/// selection and fold the admitted transitions whole-batch with
/// `lanefold::fold_batch` into the single pergroup array. One
/// CHECK_FOR_INTERRUPTS per staged batch (design §9 batch-operator cadence).
/// Guarded plans re-prove every batch; `Demote` runs the WHOLE batch through
/// the checked per-row program (lanefold contract).
///
/// Two per-batch modes:
///   * bitmap: no residual transitions and the qual is absent or staged as
///     the kernel-qual bitmap — the selection is `sel & !fallback` (or
///     `!fallback` with no qual) with NO per-row work for deformed rows (the
///     fold reads the SoA lanes; a per-row emit would only store a slot
///     nothing reads). Forced fallback rows re-check the qual per-row and run
///     the full per-row program off the stored tuple.
///   * per-row emit: a scalar qual and/or residual transitions — the scan's
///     per-row emit applies the qual; surviving deformed rows join the fold
///     selection (+ the residual program per row), fallback rows run the
///     full per-row program.
///
/// Byte-identity: the same rows pass the same qual (the staged bitmap IS the
/// kernel qual's verdict; fallback rows re-run the per-row check), and every
/// fold kernel is commutative and bit-for-bit equal to C's transition
/// semantics on admitted/guard-proven data (lanefold's tested contract), so
/// the single group's transvalues — and the finalized output row — are
/// identical.
fn agg_plain_fold_feed<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Same one-deform staging as the hashed fold feed; the deform is FORCED
    // (count(*)-only plans were refused, so the fold always reads lane
    // columns). Re-preparing with the same shape is a no-op.
    arm_scan_staging(ss, estate, ScanFeedShape::FoldPrefix { agg })?;
    // Fold length lanes (no grouping key, no spill, no staged replay on the
    // plain feed — the staged lanes' only reader is the fold itself).
    arm_fold_len_lanes(agg, ss);
    // initialize_aggregates (delegated): fresh initval pergroups; a rescan
    // re-enters here with agg_done cleared.
    ::nodeagg::agg_plain_build_begin(agg, estate)?;
    // Phase-1 source selection (WS-K): heap scans ride the dedicated
    // HeapBatchSource iff PGRUST_LANE_V2_HEAPFEED is on; everything else —
    // and the whole knob-OFF world — constructs SeqScanSource exactly as
    // before (same monomorphized drain). Knob-ON, end-of-claim ownership
    // sits on the source (trait doc): the serial scan is ONE claim,
    // settled right here after the drain.
    use batch_source::BatchGranuleSource as _;
    if batch_source::heapfeed_v2_enabled() {
        // WS-O inc-2 claim-settle guard (the serial scan is ONE claim):
        // end_claim runs on the drain's ERROR path too — zero pins at
        // settle; the drain error wins the report.
        if ::nodeseqscan::seq_scan_is_heap(ss) {
            let mut src = batch_source::HeapBatchSource::new(ss);
            let drove = agg_plain_fold_drain(agg, &mut src, estate);
            let settled = src.end_claim(estate);
            drove?;
            return settled;
        }
        let mut src = batch_source::SeqScanSource::new(ss);
        let drove = agg_plain_fold_drain(agg, &mut src, estate);
        let settled = src.end_claim(estate);
        drove?;
        return settled;
    }
    agg_plain_fold_drain(agg, &mut batch_source::SeqScanSource::new(ss), estate)
}

/// The fold feed's drain half (split out for the runtime morsel drive,
/// which arms staging + build_begin ONCE per worker and then re-enters this
/// loop per claimed granule range): drives `seq_scan_next_pagebatch` to
/// exhaustion — the whole scan on the serial feed; exactly the positioned
/// claim on a granule-ranged scan — folding into the CURRENT pergroups.
/// Byte-path-identical to the pre-split body (pure extraction; the EA=false
/// instantiation compiles every tally out — the serial machine code is the
/// pre-split machine code).
fn agg_plain_fold_drain<'mcx, S: batch_source::BatchGranuleSource<'mcx>>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    src: &mut S,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    agg_plain_fold_drain_impl::<S, false>(agg, src, estate, &mut Default::default())
}

/// EA-on-morsels drain (docs/design/ea-morsels.md §2): identical fold, plus
/// the row-funnel tally — window-grain popcounts on the bitmap paths, a
/// per-survivor increment where a per-row emit already happened. Runtime
/// workers only; never on a serial path. (Private, not pub(super): the one
/// external caller is the child module runtime_scan, which sees the
/// parent's private items — and the trait bound is lanev2-private.)
fn agg_plain_fold_drain_ea<'mcx, S: batch_source::BatchGranuleSource<'mcx>>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    src: &mut S,
    estate: &mut EStateData<'mcx>,
    tally: &mut runtime_instr::EaRowTally,
) -> PgResult<()> {
    agg_plain_fold_drain_impl::<S, true>(agg, src, estate, tally)
}

/// Phase-1 (WS-K): generic over the storage seam's batch source — staged
/// reads ride the trait's read face; the columnar-only branches (str-mm
/// dict-code memos, the footer-meta arm) and the two remaining
/// scan-invariant peeks (qual presence, the knob-OFF inline clear) ride the
/// caps-gated `seq_scan_bridge`. Both instantiations monomorphize to
/// #[inline] delegation — the SeqScanSource instantiation is the
/// pre-genericization machine code (WS-A code-shape-neutral law).
fn agg_plain_fold_drain_impl<'mcx, S: batch_source::BatchGranuleSource<'mcx>, const EA: bool>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    src: &mut S,
    estate: &mut EStateData<'mcx>,
    tally: &mut runtime_instr::EaRowTally,
) -> PgResult<()> {
    let caps = src.capabilities();
    let has_resid =
        ::nodeagg::agg_lanefold_plan(agg).is_some_and(|plan| !plan.resid.is_empty());
    // Str MIN/MAX dict-code side channel (lane-v2-dictminmax; identity plan→
    // scan column map on this feed).
    let mm_cols = {
        let plan = ::nodeagg::agg_lanefold_plan(agg).expect("fold feed without a plan");
        mm_str_cols(plan, Some)
    };
    if !mm_cols.is_empty() && caps.dict_codes {
        trace_feed("fold str min/max dict-code memo armed");
    }
    let mut mm_codes: Vec<(u16, ::exectuples::SoaDictLane)> = Vec::new();
    // Scan-invariant qual presence (a plan-fixed field), hoisted once
    // through the bridge; the knob decides end-of-claim clear ownership
    // (process-static — trait-doc single-owner rules).
    let no_qual = batch_source::require_bridge(src)?.ss.qual.is_none();
    let clear_inline = !batch_source::heapfeed_v2_enabled();
    // Footer-stat meta arm (serial drains only; the EA tally is a runtime
    // channel and runtime claims are granule-ranged, which the scan-side
    // peek refuses anyway — the structural gate keeps the funnel exact).
    // Caps-gated: zone/footer peeks are a pgrcolumnar capability
    // (plain_fold_meta_arm's own is_pgrcolumnar gate, lifted to the seam).
    let mut meta_arm = if EA || !caps.zone_maps {
        None
    } else {
        plain_fold_meta_arm(agg, batch_source::require_bridge(src)?, estate)
    };
    loop {
        if let Some(arm) = meta_arm.as_mut() {
            agg_plain_fold_meta_units(agg, batch_source::require_bridge(src)?, estate, arm)?;
        }
        let n = src.next_batch(estate)?;
        if n == 0 {
            if clear_inline {
                // End of scan: drop the scan slot's buffer pin
                // (SeqScanSource end-of-stream parity). Knob-ON this moves
                // to the source's end_claim.
                let ss = batch_source::require_bridge(src)?;
                let mcx = estate.es_query_cxt;
                ::exectuples::exec_clear_tuple(estate.slot_mut(ss.ss.ss_ScanTupleSlot), mcx);
            }
            break;
        }
        ::postgres_seams::check_for_interrupts::call()?;
        if EA {
            tally.scanned += n as u64;
        }
        let nwords = (n as usize).div_ceil(64);
        // Guarded plans: prove the batch over every staged non-fallback row —
        // a superset of the rows the fold will touch — before any fold (same
        // soundness argument as the hashed fold feed).
        let mut demote = false;
        {
            let plan = ::nodeagg::agg_lanefold_plan(agg).expect("fold feed without a plan");
            if plan.guarded {
                let soa = src.batch_soa().expect("plain fold plans read lane columns");
                let mut rows = [0u64; ::exectuples::SOA_BM_WORDS];
                // Proof domain: under the PREWHERE lane the staged columns
                // fill lazily (survivor windows only), so intersect the
                // selection bitmap — the fold touches only selected rows
                // (requal survivors ⊆ selected bits); unselected cells may be
                // stale pointers (vguard columns via the virtual prefix).
                match src.lane_sel() {
                    Some(sel) => {
                        for ((r, fb), s) in
                            rows[..nwords].iter_mut().zip(soa.fallback_words()).zip(sel)
                        {
                            *r = s & !fb;
                        }
                    }
                    None => {
                        for (r, fb) in rows[..nwords].iter_mut().zip(soa.fallback_words()) {
                            *r = !fb;
                        }
                    }
                }
                if n % 64 != 0 {
                    rows[nwords - 1] &= (1u64 << (n % 64)) - 1;
                }
                // Empty domain: nothing will fold — never probe lane cells
                // (a survivor-less lane window ran no completing deform).
                if rows[..nwords].iter().any(|&w| w != 0) {
                    // SAFETY: proof rows are staged non-fallback rows with
                    // live deformed lane values (prefix deform contract;
                    // under the PREWHERE lane the domain is selected rows of
                    // a survivor window, whose completing deform filled every
                    // prefix column — vguard columns readable at their
                    // varlena header byte).
                    demote = unsafe {
                        ::lanefold::check_guards(plan, soa, &rows[..nwords], |_| None)
                            == ::lanefold::GuardCheck::Demote
                    };
                }
            }
        }
        if demote {
            // Emit-dead word skip (skip-sel: cleared bits are definitive
            // rejections, even under requal) — same accepted rows/order,
            // same survived tally (skipped rows emit None).
            let skip = {
                let mut w = [0u64; ::exectuples::SOA_BM_WORDS];
                src.skip_sel().map(|s| {
                    w[..s.len()].copy_from_slice(s);
                    w
                })
            };
            ::exectuples::for_each_live(skip.as_ref().map(|w| &w[..]), 0, n, |i| -> PgResult<()> {
                if let Some(slot) = src.emit(estate, i)? {
                    if EA {
                        tally.survived += 1;
                    }
                    ::nodeagg::agg_plain_build_accept(agg, estate, slot)?;
                }
                Ok(())
            })?;
            continue;
        }
        let mut rows = [0u64; ::exectuples::SOA_BM_WORDS];
        let bitmap_qual = src.qual_sel().is_some();
        if !has_resid && (bitmap_qual || no_qual) {
            let mut fallback = [0u64; ::exectuples::SOA_BM_WORDS];
            {
                let soa = src.batch_soa().expect("plain fold plans read lane columns");
                let fb = soa.fallback_words();
                let sel = src.qual_sel();
                for w in 0..nwords {
                    rows[w] = sel.map_or(!fb[w], |s| s[w] & !fb[w]);
                    fallback[w] = fb[w];
                }
                if n % 64 != 0 {
                    rows[nwords - 1] &= (1u64 << (n % 64)) - 1;
                    fallback[nwords - 1] &= (1u64 << (n % 64)) - 1;
                }
            }
            if EA {
                // Window-grain: the selected non-fallback rows all fold.
                tally.survived +=
                    rows[..nwords].iter().map(|w| w.count_ones() as u64).sum::<u64>();
            }
            for (w, mut bits) in fallback[..nwords].iter().copied().enumerate() {
                while bits != 0 {
                    let i = (w as u32) * 64 + bits.trailing_zeros();
                    bits &= bits - 1;
                    if let Some(slot) = src.emit(estate, i)? {
                        if EA {
                            tally.survived += 1;
                        }
                        ::nodeagg::agg_plain_build_accept(agg, estate, slot)?;
                    }
                }
            }
        } else {
            // Residual/requal per-row walk, with the emit-dead word skip
            // (skip-sel: cleared bits are definitive rejections even under
            // requal — the surviving emit stream and fold selection are
            // identical).
            let skip = {
                let mut w = [0u64; ::exectuples::SOA_BM_WORDS];
                src.skip_sel().map(|s| {
                    w[..s.len()].copy_from_slice(s);
                    w
                })
            };
            ::exectuples::for_each_live(skip.as_ref().map(|w| &w[..]), 0, n, |i| -> PgResult<()> {
                let Some(slot) = src.emit(estate, i)? else {
                    return Ok(());
                };
                if EA {
                    tally.survived += 1;
                }
                if src.batch_soa().is_some_and(|soa| soa.is_fallback(i)) {
                    ::nodeagg::agg_plain_build_accept(agg, estate, slot)?;
                } else {
                    rows[(i / 64) as usize] |= 1u64 << (i % 64);
                    if has_resid {
                        ::nodeagg::agg_plain_build_accept_resid(agg, estate, slot)?;
                    }
                }
                Ok(())
            })?;
        }
        if rows[..nwords].iter().any(|w| *w != 0) {
            let plan = ::nodeagg::agg_lanefold_plan(agg).expect("fold feed without a plan");
            let aggcx = ::nodeagg::agg_aggcontext(agg);
            // Str MIN/MAX dict-code views for this batch (lane-v2-
            // dictminmax): the ungrouped fold's batch winner becomes an
            // integer code scan — no scratch (fold_batch needs no memo).
            // Caps-gated (dict codes are a pgrcolumnar capability): heap
            // never publishes codes and `mm_codes`' only writer is this
            // call, so skipping keeps it empty — identical to today's
            // per-column None answers.
            if caps.dict_codes {
                collect_mm_codes(batch_source::require_bridge(src)?, &mm_cols, &mut mm_codes);
            }
            let soa = src.batch_soa().expect("plain fold plans read lane columns");
            // SAFETY: pergroup_base is the node's once-allocated single-group
            // pergroup array covering every transno (initialize_aggregates
            // just wrote it); selected rows are non-fallback, carrying valid
            // deformed lane values for every plan column (the SoA prefix
            // covers the evaltrans fetch bound); AvgAccum pergroups hold the
            // catalog's {0,0} int8[2] transarray, datum-copied at
            // initialize_aggregates; Int128AvgAccum pergroups are NULL or
            // hold the aggcontext state the fold/transfn chain installed, and
            // `aggcx` is that same aggcontext; guarded plans passed
            // `check_guards` above; dict-code views satisfy the col_codes
            // contract (`seq_scan_batch_dict_codes`).
            unsafe {
                ::lanefold::fold_batch(
                    plan,
                    &CodesCols { inner: soa, codes: &mm_codes },
                    &rows[..nwords],
                    n as usize,
                    ::nodeagg::agg_plain_pergroup_base(agg),
                    aggcx,
                )?;
            }
        }
    }
    if let Some(arm) = &meta_arm {
        if arm.rg_units + arm.granule_units > 0 {
            lane_trace(&format!(
                "plainagg footer meta-fold: {} rgs + {} granules",
                arm.rg_units, arm.granule_units
            ));
        }
    }
    Ok(())
}

// ===========================================================================
// Plain-agg exact-DISTINCT drive (the uniqExact analog — pgrcolumnar-v2 plan
// §2.3; nodeagg's distinctset module). Hosts AGG_PLAIN nodes whose every
// DISTINCT aggregate is a set-mode entry (count/sum/avg(DISTINCT x) over
// int2/4/8 or deterministic-collation text — `distinct_set_kind`'s matrix):
// the per-row feed runs the SAME evaltrans park + ordered-input collect the
// per-tuple pull runs (the collect inserts into the per-group set instead of
// a tuplesort), and the delegated finalize replays each distinct value once
// through the real transfn.
//
// Value identity (order-relaxation charter): the set changes only the
// transfn REPLAY ORDER over the identical distinct-value multiset, and the
// admitted transitions are order-insensitive (counting / exact integer /
// Int128 accumulation), so transvalues — and output bytes — match the C
// sort-based path on every input. Memory stays C-shaped: past the work_mem
// budget the group degrades to the very tuplesort it displaced (nodeagg
// `degrade_distinct_set`), whose own spill machinery then applies.
// ===========================================================================

/// The plain exact-DISTINCT build sink: accept = the delegated per-row
/// transition program (`agg_plain_build_accept`, set collect included);
/// finish = nothing (the drive runs the delegated `agg_plain_finish` after
/// the drain, mirroring the fold drive's retrieve step).
///
/// `key_direct` (v2, the batched-insert lever): when the node's one
/// transition is a set-mode integer DISTINCT over exactly outer column 0
/// (`agg_plain_distinct_direct_shape`) AND the scan armed the direct key
/// staging (`seq_scan_sortkey_direct` — the sort breaker's own matcher/
/// staging, shared), `accept_batch` serves each staged row's key straight
/// off the SoA column and hands the whole batch to one staged set insert
/// (batched hashing + row-order probes) — no per-row transition program, no
/// per-row collect scan. Narrow-tuple fallback rows keep the full per-row
/// path. Value identity: the per-row program's entire effect for the
/// admitted shape is "park outer col 0 + set-insert", and set insertion
/// order is replay-invisible (the admission's order-insensitivity grant).
struct PlainDistinctAggBuildSink<'a, 'mcx> {
    agg: &'a mut ::nodeagg::AggStateData<'mcx>,
    key_direct: bool,
    /// Direct key is text (`DistinctKeyKind::Bytes`): staged keys route
    /// through the bytes/dict batch inserts instead of the integer feed.
    key_bytes: bool,
    keys: Vec<::datum::Datum>,
    ints: Vec<i64>,
    hashes: Vec<u64>,
    /// Dict-code insert memo, IDENTITY-SCOPED (cleared whenever the memo
    /// identity changes): bit = this code's value was already fed under this
    /// identity. Without a v7 stitch the identity is `(false, epoch)` and
    /// bits are local codes — never carries across epochs (epoch-scoped ids
    /// are not stable value identities). With a stitch the identity is
    /// `(true, gepoch)` and bits are PART-GLOBAL codes (0..gndv) — the memo
    /// never resets at row-group rolls, deleting the per-epoch re-insert tax
    /// (the global-dict lane's distinct-set consumer, mirroring dict-group).
    /// Either way the set stores full bytes; the memo only filters repeat
    /// inserts, which every downstream consumer dedups anyway.
    dict_memo: Vec<u64>,
    dict_ident: Option<(bool, u64)>,
}

impl<'a, 'mcx> PlainDistinctAggBuildSink<'a, 'mcx> {
    fn new(
        agg: &'a mut ::nodeagg::AggStateData<'mcx>,
        key_direct: bool,
        key_bytes: bool,
    ) -> Self {
        PlainDistinctAggBuildSink {
            agg,
            key_direct,
            key_bytes,
            keys: Vec::new(),
            ints: Vec::new(),
            hashes: Vec::new(),
            dict_memo: Vec::new(),
            dict_ident: None,
        }
    }
}

impl<'mcx> Sink<'mcx> for PlainDistinctAggBuildSink<'_, 'mcx> {
    fn accept(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<SinkFeed> {
        ::nodeagg::agg_plain_build_accept(self.agg, estate, tuple)?;
        Ok(SinkFeed::NeedMore)
    }

    fn finish(&mut self, _estate: &mut EStateData<'mcx>) -> PgResult<()> {
        Ok(())
    }
}

impl<'mcx> BatchSink<'mcx> for PlainDistinctAggBuildSink<'_, 'mcx> {
    fn accept_batch<E: BatchEmit<'mcx>>(
        &mut self,
        emit: &mut E,
        pos: u32,
        n: u32,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()> {
        if !self.key_direct {
            // Default per-row delegation loop with the emit-dead word skip
            // (`live_sel`: a cleared bit answers `emit` with None and no
            // observable effect — the BatchSink default's contract).
            let live = emit.live_sel();
            let live = live.as_ref().map(|w| &w[..]);
            return ::exectuples::for_each_live(live, pos, n, |i| -> PgResult<()> {
                if let Some(slot) = emit.emit(i, estate)? {
                    ::nodeagg::agg_plain_build_accept(self.agg, estate, slot)?;
                }
                Ok(())
            });
        }
        // Dict-coded text window (the pgrcolumnar zero-decode lane): consume
        // codes+dict for the whole window — the key's datum cells are stale
        // while a lane is up, so `emit_key` must not run. The memo dedups
        // per epoch (row group); a repeat code's value was already fed and
        // every downstream consumer dedups exactly.
        if self.key_bytes {
            if let Some(lane) = emit.key_dict_lane() {
                let t = lane.table;
                // Memo identity roll: part-global (gepoch, never resets
                // across row groups) when the v7 stitch is published, else
                // per-epoch local codes (see the field doc).
                let global = t.has_stitch();
                let (ident, size) = if global {
                    ((true, t.gepoch), t.gndv as usize)
                } else {
                    ((false, t.epoch), t.ndict as usize)
                };
                if self.dict_ident != Some(ident) {
                    self.dict_memo.clear();
                    self.dict_memo.resize(size.div_ceil(64), 0);
                    self.dict_ident = Some(ident);
                    trace_feed(&format!(
                        "distinct-set dict memo {} {} (n={size})",
                        if global { "gepoch" } else { "epoch" },
                        ident.1
                    ));
                }
                // SAFETY: the lane covers the staged window's `n` rows and
                // `ndict` dict entries (the fill's contract; the stitch spans
                // `ndict` u32s per the Part::stitch length check); consumed
                // before the next window stages.
                // Lazy sub-framed dict: the insert reads entry bytes for
                // this window's novel codes through the raw `dict` slice —
                // ensure the window's codes up front (no-op once
                // materialized / on unframed dicts).
                if t.lazy_ensure.is_some() {
                    for i in pos as usize..n as usize {
                        // SAFETY: the lane covers the staged window's rows.
                        t.ensure_code(unsafe { *lane.codes.add(i) });
                    }
                }
                let (codes, dict, stitch) = unsafe {
                    (
                        core::slice::from_raw_parts(lane.codes, n as usize),
                        core::slice::from_raw_parts(t.dict, t.ndict as usize),
                        global
                            .then(|| core::slice::from_raw_parts(t.stitch, t.ndict as usize)),
                    )
                };
                return ::nodeagg::agg_plain_distinct_insert_dict_batch(
                    self.agg,
                    estate,
                    &codes[pos as usize..],
                    dict,
                    stitch,
                    &mut self.dict_memo,
                );
            }
        }
        // Lane-sliced int-key consume (hot-gap C2, the single-int-key count(DISTINCT) class): read the
        // staged key lane as WHOLE SLICES — no per-row emit_key call — and
        // let nodeagg run one null scan per window before the batched set
        // insert. Same rows, same order, same cells `emit_key` reads (the
        // loop below stays the authority for windows with fallback rows;
        // pgrcolumnar stages none).
        if !self.key_bytes {
            if let Some((vals, isnull, fb)) = emit.topk_key_lane(n) {
                if fb.iter().all(|&w| w == 0) {
                    return ::nodeagg::agg_plain_distinct_insert_lane_batch(
                        self.agg,
                        estate,
                        &vals[pos as usize..],
                        &isnull[pos as usize..],
                        &mut self.ints,
                        &mut self.hashes,
                    );
                }
            }
        }
        // Direct staged-key feed (page-level CFI in the staging fetch —
        // the sort breaker's emit_key cadence).
        self.keys.clear();
        let mut saw_null = false;
        for i in pos..n {
            match emit.emit_key(i) {
                Some((d, false)) => self.keys.push(d),
                Some((_, true)) => saw_null = true,
                None => {
                    // Narrow-tuple fallback row: the full per-row path.
                    if let Some(slot) = emit.emit(i, estate)? {
                        ::nodeagg::agg_plain_build_accept(self.agg, estate, slot)?;
                    }
                }
            }
        }
        if self.key_bytes {
            return ::nodeagg::agg_plain_distinct_insert_bytes_batch(
                self.agg,
                estate,
                &self.keys,
                saw_null,
            );
        }
        ::nodeagg::agg_plain_distinct_insert_batch(
            self.agg,
            estate,
            &self.keys,
            saw_null,
            &mut self.ints,
            &mut self.hashes,
        )
    }
}

/// Try to let the lane own an AGG_PLAIN exact-DISTINCT `Agg` over a
/// `SeqScan` (section doc above). `Some(result)` = the lane drove this call;
/// `None` = refused (the caller falls to the per-tuple pull — whose
/// collect/replay uses the SAME set state, so a per-call fallback is
/// value-safe in both directions).
fn try_own_plain_distinct_agg_over_seq_scan<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Scan-side refuse-set: the Phase-1 gate verbatim (dynamic EPQ/direction
    // gates re-checked per call; structural verdict memoized on the node).
    if !seq_scan_fusible(ss, estate)? {
        return Ok(None);
    }
    // exec_agg's top-of-call guard: the one result row is out; a drained agg
    // stays drained until rescan clears `agg_done`.
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    // Runtime plain-distinct sink probe (band-2b): DOP-parallel partitioned
    // distinct sets over the scan subtree. Refusal falls through to the
    // serial drive, value-identically.
    if let Some(scan_node) = agg.plan.plan.lefttree {
        if scan_node.node_tag() == ::types_nodes::NodeTag::T_SeqScan {
            if let Some(r) = runtime_plaindistinct::try_own_plain_distinct_runtime(
                agg, ss, scan_node, false, estate,
            )? {
                return Ok(Some(r));
            }
        }
    }
    // One OWNED tick per lane-owned plain-agg build event (the gate's
    // aggbuild floor counts builds; this drive runs the whole feed inside
    // one call).
    stats::tick_owned(ShapeClass::AggBuild);
    trace_feed("plain-agg distinct-set drive engaged");
    // v2 batched-insert arm: single set-mode DISTINCT over exactly outer
    // column 0 with the scan's direct key staging (no qual, covered column —
    // the sort breaker's own matcher; integer keys stage fixed-width, text
    // keys stage varlena pointers, dict-encoded pgrcolumnar text windows answer
    // codes+dict). Probed BEFORE the first produce, exactly as `sort_feed`
    // probes: arming decides staging.
    let key_direct = ::nodeagg::agg_plain_distinct_direct_shape(agg)
        && ::nodeseqscan::seq_scan_sortkey_direct(ss, estate);
    let key_bytes = key_direct && ::nodeagg::agg_plain_distinct_key_is_bytes(agg);
    if key_bytes && ::nodeseqscan::seq_scan_key_dict_arm(ss) {
        trace_feed("distinct-set direct text key feed armed (dict-capable)");
    } else if key_direct {
        trace_feed("distinct-set direct key feed armed");
    } else {
        // Kernel-shaped quals vectorize via the staged selection bitmap; the
        // set feed itself is per-row (the DISTINCT park is per-row).
        arm_seq_scan_qual_bitmap(ss, estate, "agg distinct-set feed", true);
        ::nodeseqscan::seq_scan_stitch_arm(ss);
    }
    // initialize_aggregates (delegated): fresh initval pergroups + cleared
    // sets; a rescan re-enters here with agg_done cleared.
    ::nodeagg::agg_plain_build_begin(agg, estate)?;
    let mut sink = PlainDistinctAggBuildSink::new(agg, key_direct, key_bytes);
    drain_pipeline(ss, &mut SeqScanSource, &mut SeqScanFilterProject, &mut sink, estate)?;
    // Retrieve (delegated): set replay + finalize + HAVING + project — one
    // row (or none, when the var-free HAVING rejects it), setting agg_done.
    Ok(Some(::nodeagg::agg_plain_finish(agg, estate)?))
}

/// Try to let the lane own `Agg(AGG_PLAIN, all-DISTINCT) → Sort → SeqScan`
/// by SKIPPING the Sort — the presorted-DISTINCT plan-shape family: the planner serves a
/// single DISTINCT aggregate by sorting the whole input and marking the
/// aggregate `aggpresorted` (adjacent-dedup). When EVERY transition of the
/// node is replayed from an exact-DISTINCT set
/// (`agg_plain_distinct_set_only` — presorted entries get force-armed into
/// set-mode), the Sort's ONLY observable effect is that dedup, so feeding
/// the UNSORTED scan into the sets produces identical values with the whole
/// O(n log n) sort deleted: the order-relaxation charter's headline grant.
/// `None` = refused; the caller falls to the per-tuple `exec_agg` over
/// `exec_sort` (which, if the drive armed set-mode on an earlier call,
/// still computes identical values — the arming doc in nodeagg).
#[inline]
pub fn try_own_plain_distinct_agg_over_sort<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    s: &mut crate::procnode::SortNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Refusals here are SILENT: every refused offer falls through to
    // `try_own_sorted_agg_over_sort`, whose gates tick the identical
    // accounting for this node (a tick here too would double-count the
    // (class, reason) cadence the gate files ratchet).
    if !::nodeagg::agg_plain_distinct_set_only(agg) {
        return Ok(None);
    }
    // Dynamic per-call gates (mirror the sorted-agg-over-sort arm).
    if estate.es_epq_active {
        return Ok(None);
    }
    // Sort-side structural verdict — the sort arms' shared memo (covers
    // random access + the child scan's own refuse-set, EXPLAIN ANALYZE
    // included).
    let fusible = match s.lane_fusible {
        Some(v) => v,
        None => {
            let refuse = sort_refuse_reason(s, estate)?;
            if let Some(r) = refuse {
                stats::tick_refused(ShapeClass::SortFeed, r);
            }
            let v = refuse.is_none();
            s.lane_fusible = Some(v);
            v
        }
    };
    if !fusible {
        return Ok(None);
    }
    // v1 scope: SeqScan child only (the presorted-DISTINCT shape; index/bitmap-fed
    // sorts under an all-DISTINCT plain agg keep the C drive). Silent for
    // the same fall-through reason as above.
    if !matches!(&*s.outer, crate::procnode::PlanStateNode::SeqScan(_)) {
        return Ok(None);
    }
    // exec_agg's top-of-call guard: a drained agg stays drained.
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    // C's CHECK_FOR_INTERRUPTS at the would-be ExecSort feed entry.
    ::postgres_seams::check_for_interrupts::call()?;
    // Runtime plain-distinct sink probe (band-2b): DOP-parallel partitioned
    // distinct sets over the scan subtree — the skip-sort law already ratified
    // above covers the parallel arrival-order relaxation too. Refusal falls
    // through to the serial skip-sort drive, value-identically.
    {
        let scan_node = s.state.plan.plan.lefttree;
        if let Some(scan_node) = scan_node {
            if scan_node.node_tag() == ::types_nodes::NodeTag::T_SeqScan {
                let crate::procnode::PlanStateNode::SeqScan(ss) = &mut *s.outer else {
                    unreachable!("matched SeqScan above")
                };
                if let Some(r) = runtime_plaindistinct::try_own_plain_distinct_runtime(
                    agg, ss, scan_node, true, estate,
                )? {
                    return Ok(Some(r));
                }
            }
        }
    }
    stats::tick_owned(ShapeClass::AggBuild);
    trace_feed("plain-agg distinct-set skip-sort drive engaged");
    // Arm set-mode for the presorted entries BEFORE any input (sticky;
    // value-safe on later fallbacks — nodeagg's arming doc).
    ::nodeagg::agg_force_distinct_set(agg);
    let crate::procnode::PlanStateNode::SeqScan(ss) = &mut *s.outer else {
        unreachable!("matched SeqScan above")
    };
    // v2 batched-insert arm (the over-SeqScan drive's twin; the Sort node's
    // tlist is its child's, so outer column 0 through the skipped Sort IS
    // scan output column 0 — the same column `seq_scan_sortkey_direct`
    // proves is one covered scan Var with no qual).
    let key_direct = ::nodeagg::agg_plain_distinct_direct_shape(agg)
        && ::nodeseqscan::seq_scan_sortkey_direct(ss, estate);
    let key_bytes = key_direct && ::nodeagg::agg_plain_distinct_key_is_bytes(agg);
    if key_bytes && ::nodeseqscan::seq_scan_key_dict_arm(ss) {
        trace_feed("distinct-set direct text key feed armed (dict-capable)");
    } else if key_direct {
        trace_feed("distinct-set direct key feed armed");
    } else {
        arm_seq_scan_qual_bitmap(ss, estate, "agg distinct-set skip-sort feed", true);
        ::nodeseqscan::seq_scan_stitch_arm(ss);
    }
    ::nodeagg::agg_plain_build_begin(agg, estate)?;
    let mut sink = PlainDistinctAggBuildSink::new(agg, key_direct, key_bytes);
    drain_pipeline(ss, &mut SeqScanSource, &mut SeqScanFilterProject, &mut sink, estate)?;
    Ok(Some(::nodeagg::agg_plain_finish(agg, estate)?))
}

/// Build phase of the hash-agg breaker over a SeqScan feed (once, lazily on
/// the first call), with the choice-dependent feed: drain the scan pipeline
/// into the breaker sink — the lanefold whole-batch feed for
/// `AggLaneChoice::Fold`, the per-row breaker feed otherwise — then finalize
/// (delegated). `table_filled` is the phase flag; a rescan rebuild clears it
/// and re-enters here. Shared by the bare agg hook above and the
/// Limit-over-agg chain (`try_own_limit`), so both drive the identical build.
fn agg_seq_scan_build_if_needed<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    c: AggLaneChoice,
    stage_slot: &mut Option<ExecSlotId>,
    xk: &mut Option<Box<ExprKeyState>>,
    sink_topn: Option<::nodeagg::sink::SinkTopnSpec>,
    sink_freeze: Option<u32>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    debug_assert_ne!(c, AggLaneChoice::Refuse);
    if ::nodeagg::agg_hash_table_filled(agg) || ::nodeagg::sink::agg_sink_emitting(agg) {
        return Ok(());
    }
    // SE-T2AGG CAR A (knob-gated, default OFF): the zero-aggregate SELECT
    // DISTINCT sub-arm, probed at the same shared build seam as the runtime
    // agg sink below (Fold/PerRow choices land here; the Refuse choice has
    // its own probe in try_own_agg_over_seq_scan). Success adopts the
    // published emit — the early return above catches re-pulls, and every
    // retrieve drains agg_hash_retrieve's sink branch. Refusal falls
    // through byte-identically.
    if runtime_plaindistinct::try_own_plain_selectdistinct_runtime(agg, ss, estate)? {
        return Ok(());
    }
    // M2 runtime aggregation sink (runtime_agg.rs): the forced/explicit
    // parallel engagement, tried at the ONE build seam every drive chain
    // shares (bare agg hook, Limit-over-agg, sort feed). Success adopts the
    // published emit; every retrieve path drains it through
    // agg_hash_retrieve's sink branch. Refusal falls through to the serial
    // build byte-identically. `sink_topn` (m3-sort-b car 1): the sort feed's
    // resolved combine-phase top-N spec — None from the other chains.
    // `sink_freeze` (band-2a): the Limit-over-agg chain's LIMIT-k-no-
    // ORDER bound (offset+count) — None from the other chains.
    if c == AggLaneChoice::Fold {
        if runtime_agg::try_engage_hashagg_runtime(
            agg,
            ss,
            xk.as_deref(),
            sink_topn,
            sink_freeze,
            estate,
        )? {
            return Ok(());
        }
    } else if sink_topn.is_some() {
        // Composition diagnosis channel: an armed topn spec never reaches
        // the sink when the agg's lane choice is the per-row feed.
        lane_trace("runtime-agg: not tried (lane choice != fold)");
    }
    // One OWNED tick per lane-owned hash-agg build event (the gate's
    // aggbuild floor counts builds, not calls) — fold-fed and per-row
    // feeds alike.
    stats::tick_owned(ShapeClass::AggBuild);
    // Staging arm per feed shape (see `arm_scan_staging` — the one seam for
    // deform + bitmap + stitched-tier setup across the feed sites).
    if c == AggLaneChoice::Fold {
        if let Some(xk) = xk.as_deref_mut() {
            // Expression-group-key feed (projected scans; exprkey module).
            // A staging rebuild that lost the arm falls back per-row inside
            // the feed's per-batch route — byte-safe either way.
            let _ = exprkey::exprkey_rearm(xk, ss, estate);
            return exprkey::exprkey_build_fold_feed(agg, ss, xk, stage_slot, estate);
        }
        arm_scan_staging(ss, estate, ScanFeedShape::HashAggFold { agg })?;
        agg_hash_build_fold_feed(agg, ss, stage_slot, estate)
    } else {
        arm_scan_staging(ss, estate, ScanFeedShape::HashAggPerRow { agg })?;
        let mut sink = HashAggBuildSink { agg };
        drain_pipeline(ss, &mut SeqScanSource, &mut SeqScanFilterProject, &mut sink, estate)
    }
}

/// Whether the K2 deferred probe could host this agg's build (the plan-level
/// half of the scan feed's admission — the SoA half needs the armed batch,
/// checked in `scan_k2_shape`). Used to force the SoA deform for shapes whose
/// fold reads no lane columns (count(*)-only plans) but whose key lane the
/// deferred probe wants staged.
fn scan_k2_wanted<'mcx>(agg: &::nodeagg::AggStateData<'mcx>) -> bool {
    k2_enabled()
        && ::nodeagg::agg_lanefold_plan(agg).is_some_and(|plan| !plan.guarded)
        && !::nodeagg::agg_lanefold_has_resid(agg)
        && ::nodeagg::agg_hash_staged_probe_col(agg).is_some()
}

// ===========================================================================
// Stage-2.1 dict-code grouping (pgrcolumnar-v2 plan §2.1 — the LowCardinality /
// DuckDB dict-grouping analog): when the K2 scan feed's single grouping key
// is a dict-encoded pgrcolumnar column, the feed opts the key into the dict lane
// (`dict_want`) and groups each dict-answered window ON THE u32 CODES — a
// per-epoch (row-group) DIRECT-INDEXED array maps code → the group's live
// pergroup state in the GLOBAL C-ported tuplehash, resolved LAZILY on the
// first surviving row of each (epoch, code): dict[code] is materialized ONCE
// per epoch and probed through the same `agg_hash_probe_staged` leg the Raw
// K2 path uses (same first-arrival insertion order — the resolve happens AT
// the first row that would have probed — same entry initialization, same
// spill decisions, same read-back). Per-row work drops from hash+probe to
// one array index; the k-per-epoch resolves are off the hot path, which is
// why the GLOBAL table stays the C tuplehash (full semantics/spill/retrieve
// delegation; the compact table's text-arena hosting is a non-blocking
// follow-up — its probe-speed edge is amortized away here).
//
// Rejected alternative (charter option A): per-epoch PARTIAL states merged
// at epoch boundaries — needs combine machinery per transtype and breaks
// first-arrival order; direct global pointers keep the C transition code
// running exactly once per row into the one true state.
//
// NULLs: dict lanes are NULL-free by the pgrcolumnar per-chunk proof (the store
// writes no NULLs today) — asserted per batch, never assumed structurally.
// Raw windows (non-dict-encoded key chunks) fall back to the Raw K2 keys
// path within the same build, byte-identically.
// ===========================================================================

/// `PGRUST_LANE_V2_DICTGROUP` kill switch (default ON inside the lane).
fn dictgroup_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_LANE_V2_DICTGROUP").as_deref(), Ok("0") | Ok("off"))
    })
}

/// Dict-group admission + columnar staging arm, tried when the standard
/// fixed-width-prefix arm refused (a varlena column — typically the text
/// grouping key itself — sits inside the fold prefix). Admission (§2.1):
///   * pgrcolumnar scan, unprojected (callers gate), lane fold plan classified;
///   * the K2 deferred probe wants the shape (`scan_k2_wanted`: unguarded,
///     no residual transitions, single kernel-hostable grouping key);
///   * no varlena-guard transitions (str MIN/MAX keeps the varkey staging /
///     per-row paths — mixed vguard+dict shapes are a follow-up);
///   * the columnar staging arms (`seq_scan_cb_dictgroup_arm`).
/// True = the SoA staging is armed with the key opted into dict lanes; the
/// fold feed's dict-group batch path consumes the codes. False = fail-open
/// (per-row / Raw paths, byte-identical), ticking the observability reason.
fn try_arm_cb_dictgroup<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> bool {
    if !dictgroup_enabled()
        || !::nodeseqscan::seq_scan_is_pgrcolumnar(ss)
        || !scan_k2_wanted(agg)
    {
        return false;
    }
    let refused = || {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::DictGroupShape);
        false
    };
    let Some(plan) = ::nodeagg::agg_lanefold_plan(agg) else { return refused() };
    if !plan.vguards.is_empty() {
        return refused();
    }
    let Some(key) = ::nodeagg::agg_hash_staged_probe_col(agg) else { return refused() };
    // The fold must not read the key column's SoA Datum cells: they are
    // STALE while a dict lane answers (e.g. `count(url) ... GROUP BY url`
    // reads url as a transition arg). Refuse — the Raw paths host it.
    if plan.cols.iter().any(|&c| c == key) {
        return refused();
    }
    let Some(prefix) = fused_agg_soa_prefix(agg, ss) else { return refused() };
    if !::nodeseqscan::seq_scan_cb_dictgroup_arm(ss, estate, prefix, key) {
        return refused();
    }
    true
}

/// Per-build dict-group state: the direct-indexed code → global pergroup
/// map (`slots`) plus the one-element hash scratch for the lazy per-code
/// resolve. Two keying modes (the identity tuple is (is_global, id)):
/// per-epoch (`ndict`-sized, cleared at every RG roll) or — when the part
/// carries v7 stitch tables — part-global (`gndv`-sized, keyed on the
/// scan-stable `gepoch`, cleared NEVER within one scan: every distinct
/// string resolves through the tuplehash exactly once per query).
#[derive(Default)]
struct DictGroupScratch {
    ident: Option<(bool, u64)>,
    slots: Vec<Option<core::ptr::NonNull<::execexpr::AggPerGroup>>>,
    hash1: Vec<u32>,
}

/// K2 admission inputs for the scan-fed fold feed. `needed` is the spill
/// replay's column set (`colnos_needed` — exactly what the hashagg spill
/// projection keeps); all of it must lie inside the armed SoA prefix so a
/// spill-mode miss can be replayed from the staged lanes.
struct ScanK2 {
    key_col: u16,
    needed: Vec<u16>,
    natts: usize,
}

/// Reusable per-build scratch for the K2 batch loop (qual-surviving row
/// indices, their gathered key lane, and the batched hashes).
#[derive(Default)]
struct ScanK2Scratch {
    rows: Vec<u32>,
    keys: Vec<::datum::Datum>,
    knull: Vec<bool>,
    hashes: Vec<u32>,
}

/// One Vec's backing-store bytes (CAPACITY, not len) — the estate-ledger
/// grain for the drive-scratch charges (GL-CONCMEM-1): the process ledger
/// mirrors what the allocator actually holds for the store, settled at
/// claim boundaries, never per row.
pub(crate) fn vec_estate_bytes<T>(v: &Vec<T>) -> usize {
    v.capacity() * core::mem::size_of::<T>()
}

impl ScanK2Scratch {
    /// Backing-store bytes for the process estate ledger (GL-CONCMEM-1).
    fn estate_bytes(&self) -> usize {
        vec_estate_bytes(&self.rows)
            + vec_estate_bytes(&self.keys)
            + vec_estate_bytes(&self.knull)
            + vec_estate_bytes(&self.hashes)
    }
}

/// The scan feed's K2 admission (mirrors the joined-row feed's K2 arm in
/// `staged_feed_shape`): unguarded, no residual transitions, a single
/// kernel-hostable (int4/int8/text) grouping key — plus the scan-side
/// requirement that the key and every needed column are armed SoA lanes
/// (fixed-width prefix; a text key never arms, so this class is int-keyed).
/// `None` = keep the per-row arrival probe.
fn scan_k2_shape<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> Option<ScanK2> {
    if !scan_k2_wanted(agg) {
        return None;
    }
    scan_k2_shape_body(agg, ss, estate)
}

/// SE-T2AGG CAR B: the K2 shape probe for the RUNTIME AGG SINK's decide —
/// `scan_k2_shape` widened onto vguard-only guarded plans
/// (`sink_vguard_plan_ok`; knob-gated, default OFF — identical to
/// `scan_k2_shape` otherwise, so the serial feed's own probe is untouched).
fn scan_k2_shape_sink<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> Option<ScanK2> {
    let vguard_wanted = k2_enabled()
        && sink_vguard_plan_ok(agg, ss)
        && ::nodeagg::agg_hash_staged_probe_col(agg).is_some();
    if !scan_k2_wanted(agg) && !vguard_wanted {
        return None;
    }
    scan_k2_shape_body(agg, ss, estate)
}

fn scan_k2_shape_body<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> Option<ScanK2> {
    let key_col = ::nodeagg::agg_hash_staged_probe_col(agg)?;
    let soa = ::nodeseqscan::seq_scan_batch_soa(ss)?;
    let (colnos_needed, max_colno) = ::nodeagg::agg_hash_needed_cols(agg);
    let natts = estate
        .slot(ss.ss.ss_ScanTupleSlot)
        .base()
        .tts_tupleDescriptor
        .as_ref()?
        .attrs
        .len();
    if colnos_needed.len() != natts
        || (key_col as usize) >= soa.ncols() as usize
        || max_colno > soa.ncols() as i32
        || !colnos_needed[key_col as usize]
    {
        return None;
    }
    let needed: Vec<u16> = colnos_needed
        .iter()
        .enumerate()
        .filter(|&(_, &b)| b)
        .map(|(c, _)| c as u16)
        .collect();
    Some(ScanK2 { key_col, needed, natts })
}

/// K1 inc-3 completion sets for one armed grouped-heap build (WS-AH
/// lineage): `now` = the agg-needed deferred columns (`deferred ∩
/// colnos_needed`) — completed right after staging, before every
/// whole-batch consumer; `publish` = the rest of the deferred prefix —
/// columns ONLY the per-row emit's prefix publish (`soa_store_prefix`)
/// reads, completed at the per-row fall-through sites. Split by
/// `batch_source::k1_latemat_split`; an unavailable needed census fails
/// open to `now` = the whole deferred set (the landed inc-2 bytes).
struct LatematCols {
    now: Vec<u16>,
    publish: Vec<u16>,
}

/// One late-mat completion call over THIS batch's whole-qual survivor
/// bitmap (kind-0 rows only fill; forced-fallback bits are harmless). The
/// empty set never touches the node — kernel-leg batches with an empty
/// `now` (needed ⊆ staged) and per-row batches with an empty `publish`
/// (needed covers the prefix) pay nothing.
fn k1_latemat_complete<'mcx, S: batch_source::BatchGranuleSource<'mcx>>(
    src: &mut S,
    estate: &mut EStateData<'mcx>,
    cols: &[u16],
    n: u32,
) -> PgResult<()> {
    if cols.is_empty() {
        return Ok(());
    }
    // WS-AH review F3 hardening: this completion trusts the staged
    // whole-qual bitmap as THIS batch's survivor set. Pin the arm invariant
    // (an armed drive recomputes the bitmap on every staged batch —
    // qual_armed + nwords > 0) against future feed re-plumbing: a batch
    // staged without recomputing the bitmap would expose stale selection
    // words here (silent stale cells, not a crash).
    #[cfg(debug_assertions)]
    {
        let ss = batch_source::require_bridge(src)?;
        debug_assert!(
            ::nodeseqscan::seq_scan_batch_qual_bitmap_ready(ss),
            "k1-latemat completion without THIS batch's whole-qual bitmap"
        );
    }
    let nwords = (n as usize).div_ceil(64);
    let mut sel = [0u64; ::exectuples::SOA_BM_WORDS];
    match src.qual_sel() {
        Some(s) => sel[..nwords].copy_from_slice(&s[..nwords]),
        // Belt: an armed narrowing without a staged verdict fails open to
        // completing every row (never a stale cell).
        None => sel[..nwords].fill(u64::MAX),
    }
    src.complete_deform(estate, cols, &sel[..nwords])
}

/// K1 inc-2 late-materialization admission for the grouped heap drain
/// (wave-9 WS-AH, contract §2), decided once per build after the K2/mk
/// shape census. `Some` = the staging narrowed to {qual clause cols ∪
/// key cols} and the deferred completion sets split per inc-3
/// (`LatematCols`); `None` = today's full staging bytes.
///
/// Rails (each refusal NAMED through the laneexec funnel):
/// - G: guarded / vguard-bearing fold plans refuse (`k1-latemat-guard-cols`)
///   — their whole-batch proof reads every staged non-fallback row's cells;
/// - J + shape: the nodeseqscan arm refuses no-qual/requal/qual-col-only
///   stagings (`k1-latemat-no-qual` — those shapes keep today's single JIT
///   full deform) and every non-plain-kernel staging (`k1-latemat-shape`);
/// - key set: K2 single key or the mk packed component atts — the mk
///   numeric packability pre-check reads its component lanes WHOLE-batch,
///   so component atts must stage eagerly. Builds with neither shape (the
///   per-row arrival probe) keep today's staging silently (no profitable
///   narrowing exists: the emit publishes every prefix column per row).
fn scan_k1_latemat_arm<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    agg: &::nodeagg::AggStateData<'mcx>,
    k2: Option<&ScanK2>,
    mk: Option<&ScanMk>,
    natts: usize,
) -> Option<LatematCols> {
    // Per-build re-decision: never inherit a previous build's narrowing.
    ::nodeseqscan::seq_scan_k1_latemat_disarm(ss);
    let plan = ::nodeagg::agg_lanefold_plan(agg)?;
    if plan.guarded || !plan.vguards.is_empty() {
        ::laneexec::log_refused("k1-latemat-guard-cols");
        return None;
    }
    let mut keys: Vec<u16> = Vec::new();
    if let Some(s) = k2 {
        keys.push(s.key_col);
    } else if let Some(m) = mk {
        keys.extend(m.shape.comps.iter().map(|c| c.att));
    } else {
        return None;
    }
    // K1-F2 selectivity gate (SE9-GATES item 2): admit late-mat only where
    // the plan-time qual-selectivity estimate sits in the letter's win
    // envelope; above the threshold the completion round-trip is pure cost
    // and full staging wins. One estimate per BUILD (never per-row).
    // N1 (inc-3): parallel-aware builds refuse inside the gate — every
    // admitted estimate is serial, so the per-worker numerator and the
    // whole-scan denominator agree in denomination.
    if let Err(reason) = batch_source::k1_latemat_sel_admits(ss) {
        ::laneexec::log_refused(reason);
        return None;
    }
    match ::nodeseqscan::seq_scan_k1_latemat_arm(ss, &keys) {
        Ok(cols) => {
            trace_feed("k1 late-mat staging engaged (deform narrowed to qual+key cols)");
            // Inc-3 needed-set split: the agg's colnos_needed census (the
            // hashagg spill projection's set — every whole-batch consumer's
            // read bound) decides which deferred columns complete after
            // staging vs only at a per-row publish leg. K2 builds reuse the
            // shape's natts-validated set; mk builds take the census here
            // under the same identity-map precondition (len == scan natts);
            // an unavailable census fails open (publish stays empty — the
            // landed inc-2 completion bytes).
            let mk_needed: Option<Vec<u16>> = if k2.is_none() {
                let (nd, _max) = ::nodeagg::agg_hash_needed_cols(agg);
                (nd.len() == natts).then(|| {
                    nd.iter()
                        .enumerate()
                        .filter(|&(_, &b)| b)
                        .map(|(c, _)| c as u16)
                        .collect()
                })
            } else {
                None
            };
            let needed: Option<&[u16]> = match k2 {
                Some(s) => Some(&s.needed),
                None => mk_needed.as_deref(),
            };
            let (now, publish) = batch_source::k1_latemat_split(cols, needed);
            if !publish.is_empty() {
                trace_feed("k1 late-mat needed-set split engaged (publish-only cols defer to per-row legs)");
            }
            Some(LatematCols { now, publish })
        }
        Err(reason) => {
            ::laneexec::log_refused(reason);
            None
        }
    }
}

/// Survivor collection for the deferred-probe batch arms (K2 / dict-group /
/// multi-key), colagg: when the staged batch is slot-free decidable
/// (`seq_scan_batch_slotfree_filter` — no projection; no qual, or the armed
/// bitmap IS the whole qual with no requal tail), read the verdicts straight
/// off the batch state instead of running the per-row emit — the emit would
/// materialize every row into the scan slot only for the arm to discard it
/// (keys and transition inputs both read the staged SoA lanes). One
/// per-batch ExprContext reset stands in for the emit's per-row reset
/// cadence (nothing on these arms allocates per-tuple memory per row).
/// Same rows, same ascending order as the emit loop by construction; every
/// other batch keeps the per-row emit sequence, byte-identically.
///
/// Callers admit ALL-LANE batches only (no fallback rows), so the bitmap's
/// forced-fallback re-check discipline is vacuous here.
fn scan_collect_survivors<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    n: u32,
    rows: &mut Vec<u32>,
) -> PgResult<()> {
    rows.clear();
    match ::nodeseqscan::seq_scan_batch_slotfree_filter(ss) {
        Some(::nodeseqscan::SlotFreeFilter::All) => {
            estate.ecxt_mut(ss.ss.ps_ExprContext).reset();
            rows.extend(0..n);
        }
        Some(::nodeseqscan::SlotFreeFilter::Bitmap) => {
            debug_assert!(::nodeseqscan::seq_scan_batch_soa(ss)
                .is_some_and(|soa| soa.fallback_words().iter().all(|&w| w == 0)));
            estate.ecxt_mut(ss.ss.ps_ExprContext).reset();
            let sel = ::nodeseqscan::seq_scan_batch_qual_sel(ss)
                .expect("Bitmap filter implies an armed whole-qual sel");
            let nwords = (n as usize).div_ceil(64);
            let tail_mask =
                if n % 64 == 0 { u64::MAX } else { (1u64 << (n % 64)) - 1 };
            for w in 0..nwords {
                let mut bits = sel[w];
                if w == nwords - 1 {
                    bits &= tail_mask;
                }
                while bits != 0 {
                    rows.push(w as u32 * 64 + bits.trailing_zeros());
                    bits &= bits - 1;
                }
            }
        }
        None => {
            // Per-row emit collection, word-skipping emit-dead rows
            // (skip-sel cleared bits are definitive rejections even under
            // requal — the collected survivor set/order is identical).
            let skip = {
                let mut w = [0u64; ::exectuples::SOA_BM_WORDS];
                ::nodeseqscan::seq_scan_batch_skip_sel(ss).map(|s| {
                    w[..s.len()].copy_from_slice(s);
                    w
                })
            };
            ::exectuples::for_each_live(skip.as_ref().map(|w| &w[..]), 0, n, |i| -> PgResult<()> {
                if ::nodeseqscan::seq_scan_batch_emit(ss, estate, i)?.is_some() {
                    rows.push(i);
                }
                Ok(())
            })?;
        }
    }
    Ok(())
}

/// One page batch through the scan feed's K2 deferred probe: (1) survivor
/// collection (`scan_collect_survivors` — slot-free off the batch state when
/// decidable, the arrival loop's exact per-row emit sequence otherwise);
/// (2) one tight batched-hash loop over the survivors' staged key
/// lane (bit-identical per element to the per-row `hash_slot`, by the probe-
/// kernel contract); (3) the IN-ORDER probe of every survivor through the
/// same C-ported tuplehash lookup (kernel `find_staged` fast path for the
/// dominant found-existing case; misses take the full insert/spill leg) — so
/// first-arrival insertion order, entry initialization, memory-limit checks,
/// and spill decisions are exactly the arrival path's; spill-mode misses
/// replay the row from the SoA lanes (needed columns populated, unneeded NULL
/// — the spill projection's own treatment) and spill byte-identically;
/// (4) the whole-batch fold over the resolved pergroups. The batch's CFI ran
/// in the caller (one per staged batch — design §9 cadence).
#[allow(clippy::too_many_arguments)]
fn scan_k2_batch<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    shape: &ScanK2,
    stage_slot: &mut Option<ExecSlotId>,
    k2s: &mut ScanK2Scratch,
    dgs: Option<&mut DictGroupScratch>,
    idxs: &mut Vec<u32>,
    groups: &mut Vec<core::ptr::NonNull<::execexpr::AggPerGroup>>,
    n: u32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let ScanK2Scratch { rows, keys, knull, hashes } = k2s;
    scan_collect_survivors(ss, estate, n, rows)?;
    // Stage-2.1 dict-group window (registered key + a dict-answered window):
    // group on the u32 codes through the per-epoch direct-indexed map — no
    // per-row hashing/probing at all. A Raw-answered window (non-dict key
    // chunk) falls through to the Raw keys path below; both paths resolve
    // into the same global table in the same row order.
    if let Some(dgs) = dgs {
        let lane = ::nodeseqscan::seq_scan_batch_soa(ss)
            .and_then(|soa| soa.dict_lane(shape.key_col as usize));
        if let Some(lane) = lane {
            return scan_dictgroup_batch(
                agg, ss, shape, stage_slot, dgs, idxs, groups, rows, lane, estate,
            );
        }
    }
    keys.clear();
    knull.clear();
    {
        let soa =
            ::nodeseqscan::seq_scan_batch_soa(ss).expect("K2 scan feed requires the armed SoA");
        let kc = shape.key_col as usize;
        let (kv, kn) = (soa.col_values(kc), soa.col_isnull(kc));
        for &i in rows.iter() {
            keys.push(kv[i as usize]);
            knull.push(kn[i as usize]);
        }
    }
    // Stage-2.2 compact-table batch (nodeagg::compact): probe + new-group
    // init inside the compact table — PG hashing bypassed entirely — and the
    // usual whole-batch fold over the returned pergroups. `false` = the
    // runtime backstop migrated the table into the C tuplehash BEFORE this
    // batch; fall through to the staged probe below (same rows, same order,
    // the migrated groups' states carried over byte-for-byte).
    if ::nodeagg::agg_hash_compact_armed(agg)
        && ::nodeagg::agg_hash_compact_batch(agg, estate, keys, knull, groups)?
    {
        idxs.clear();
        idxs.extend_from_slice(rows);
        let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
            .expect("K2 scan feed requires the armed SoA");
        // SAFETY: as the staged-probe fold below — every probed row is
        // non-fallback with valid lane values for every plan column; each
        // pergroup was installed by the compact probe within this batch.
        return unsafe { agg_fold_staged(agg, soa, idxs, groups) };
    }
    ::nodeagg::agg_hash_hash_staged(agg, keys, knull, hashes)?;
    idxs.clear();
    groups.clear();
    for (k, &i) in rows.iter().enumerate() {
        match ::nodeagg::agg_hash_probe_staged(agg, estate, keys[k], knull[k], hashes[k])? {
            Some(pg) => {
                idxs.push(i);
                groups.push(pg);
            }
            None => {
                // Spill-mode miss: replay the row off the SoA lanes and spill
                // it; no transition runs (the per-row path's exact
                // treatment). The replay slot is memoized across rescan
                // rebuilds and allocated only if a build ever spills.
                let slot_id = match *stage_slot {
                    Some(s) => s,
                    None => {
                        let desc = estate
                            .slot(ss.ss.ss_ScanTupleSlot)
                            .base()
                            .tts_tupleDescriptor
                            .clone();
                        let s = estate
                            .exec_init_extra_tuple_slot(desc, ::types_slot::TupleSlotKind::Virtual);
                        *stage_slot = Some(s);
                        s
                    }
                };
                {
                    let mcx = estate.es_query_cxt;
                    let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                        .expect("K2 scan feed requires the armed SoA");
                    let slot = estate.slot_mut(slot_id);
                    ::exectuples::exec_clear_tuple(slot, mcx);
                    let base = slot.base_mut();
                    for c in 0..shape.natts {
                        base.tts_values[c] = ::datum::Datum::null();
                        base.tts_isnull[c] = true;
                    }
                    for &c in &shape.needed {
                        let c = c as usize;
                        base.tts_values[c] = soa.col_values(c)[i as usize];
                        base.tts_isnull[c] = soa.col_isnull(c)[i as usize];
                    }
                    ::exectuples::exec_store_virtual_tuple(slot);
                }
                ::nodeagg::agg_hash_spill_staged(agg, estate, slot_id, hashes[k])?;
            }
        }
    }
    let soa =
        ::nodeseqscan::seq_scan_batch_soa(ss).expect("K2 scan feed requires the armed SoA");
    // SAFETY: every probed row is non-fallback (the caller admits only
    // all-lane batches), so the SoA lanes carry valid deformed values for
    // every plan column (`plan.cols ⊆ colnos_needed ⊆` the armed prefix);
    // the plan is unguarded (K2 admission); each pergroup was installed by
    // the probe within this batch; the rest is agg_fold_staged's contract.
    unsafe { agg_fold_staged(agg, soa, idxs, groups) }
}

/// One dict-answered page batch through the dict-group path (§2.1 header
/// above `dictgroup_enabled`): per surviving row, one direct index into the
/// per-epoch code→pergroup map; unseen codes resolve lazily — dict[code]
/// materialized once per epoch and probed through the SAME staged-probe leg
/// as the Raw K2 path, at exactly the row the Raw path would have probed
/// (first-arrival insertion order, entry initialization, memory limits and
/// spill decisions all identical). Spill-mode misses replay off the SoA
/// lanes with the key materialized from the dictionary (its SoA cells are
/// stale under a dict lane) and are deliberately NOT cached: every later row
/// of that code must also spill, exactly as the per-row path would.
///
/// NULL discipline: dict codes have no NULL representation and pgrcolumnar
/// stores no NULLs (per-chunk proof, phase4 §8.3) — every dict-window row
/// probes with `isnull = false`, which is what the Raw fill would have
/// published (`isnull.fill(false)`).
#[allow(clippy::too_many_arguments)]
fn scan_dictgroup_batch<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    shape: &ScanK2,
    stage_slot: &mut Option<ExecSlotId>,
    dgs: &mut DictGroupScratch,
    idxs: &mut Vec<u32>,
    groups: &mut Vec<core::ptr::NonNull<::execexpr::AggPerGroup>>,
    rows: &[u32],
    lane: ::exectuples::SoaDictLane,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Scratch identity roll. Per-RG dictionaries are dense 0..ndict, so the
    // map is a flat array — k entries per epoch, cleared once per RG change.
    // With a v7 stitch the map is indexed by PART-GLOBAL codes (dense
    // 0..gndv over the part's union dict) and keyed on the scan-stable
    // gepoch: it never clears across RGs, deleting the per-epoch
    // re-resolution tax (the global-dict lane's grouping consumer).
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
        trace_feed(&format!(
            "dict-group {} {} (n={size})",
            if global { "gepoch" } else { "epoch" },
            ident.1
        ));
    }
    debug_assert!(dgs.slots.len() >= size, "dict size is fixed per identity");
    idxs.clear();
    groups.clear();
    for &i in rows {
        let local = lane.code(i as usize);
        debug_assert!((local as usize) < ndict, "filler contract: code < ndict");
        let code = if global { lane.table.global_code(local) as usize } else { local as usize };
        debug_assert!(code < size, "stitch contract: global code < gndv");
        let pg = match dgs.slots[code] {
            Some(pg) => pg,
            None => {
                // First surviving row of (identity, code): materialize +
                // probe once. The hash rides the same probe-kernel leg as
                // the Raw path (bit-identical per the kernel contract).
                let key = lane.table.datum(local);
                ::nodeagg::agg_hash_hash_staged(agg, &[key], &[false], &mut dgs.hash1)?;
                let hash = dgs.hash1[0];
                match ::nodeagg::agg_hash_probe_staged(agg, estate, key, false, hash)? {
                    Some(pg) => {
                        dgs.slots[code] = Some(pg);
                        pg
                    }
                    None => {
                        scan_dictgroup_spill(agg, ss, shape, stage_slot, i, key, hash, estate)?;
                        continue;
                    }
                }
            }
        };
        idxs.push(i);
        groups.push(pg);
    }
    let soa =
        ::nodeseqscan::seq_scan_batch_soa(ss).expect("dict-group feed requires the armed SoA");
    // SAFETY: as the Raw K2 fold — every probed row is non-fallback (pgrcolumnar
    // stages none) with valid lane values for every plan column (the
    // columnar fill stages decoded Datums; the key column is NOT in
    // `plan.cols` — grouping keys are not transition args in this shape, and
    // vguard plans refuse dict-group); the plan is unguarded (K2 admission);
    // each pergroup is a live global-table state block (allocation-stable
    // for the table's lifetime — the per-epoch cache only ever holds
    // pointers the probe installed).
    unsafe { agg_fold_staged(agg, soa, idxs, groups) }
}

/// Dict-group spill-mode miss: the Raw K2 path's replay verbatim, except the
/// grouping key cell takes the materialized dictionary datum (the key's SoA
/// cells are stale under a dict lane). `hashagg_spill_tuple` materializes the
/// slot into the spill tape, so the dict-borrowed datum's scan lifetime is
/// long enough by construction.
#[cold]
#[allow(clippy::too_many_arguments)]
fn scan_dictgroup_spill<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    shape: &ScanK2,
    stage_slot: &mut Option<ExecSlotId>,
    i: u32,
    key: ::datum::Datum,
    hash: u32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let slot_id = match *stage_slot {
        Some(s) => s,
        None => {
            let desc = estate.slot(ss.ss.ss_ScanTupleSlot).base().tts_tupleDescriptor.clone();
            let s = estate.exec_init_extra_tuple_slot(desc, ::types_slot::TupleSlotKind::Virtual);
            *stage_slot = Some(s);
            s
        }
    };
    {
        let mcx = estate.es_query_cxt;
        let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
            .expect("dict-group feed requires the armed SoA");
        let slot = estate.slot_mut(slot_id);
        ::exectuples::exec_clear_tuple(slot, mcx);
        let base = slot.base_mut();
        for c in 0..shape.natts {
            base.tts_values[c] = ::datum::Datum::null();
            base.tts_isnull[c] = true;
        }
        for &c in &shape.needed {
            let c = c as usize;
            base.tts_values[c] = soa.col_values(c)[i as usize];
            base.tts_isnull[c] = soa.col_isnull(c)[i as usize];
        }
        base.tts_values[shape.key_col as usize] = key;
        base.tts_isnull[shape.key_col as usize] = false;
        ::exectuples::exec_store_virtual_tuple(slot);
    }
    ::nodeagg::agg_hash_spill_staged(agg, estate, slot_id, hash)
}

// ===========================================================================
// Packed multi-key GROUP BY (multikey spike 2026-07-14 — the two-key
// int+text `GROUP BY` grouped-count shapes): a batch pre-pass packs the N
// fixed-width key components of a staged window into ONE synthetic u64/u128
// key lane (REUSED per-batch scratch — the spike's 5.5ms-vs-45.5ms verdict),
// then ALL single-key compact-table machinery runs unchanged through
// `KeyRepr::Int`/`Int128` + CRC32C. A dict-coded text component is made
// packable by the per-epoch code → scan-lifetime intern-id resolve
// (dictgroup's lazy resolve retargeted from pergroup pointers to stable u32
// ids, spike §2.3); the intern table's reverse map materializes the text at
// read-back/migrate. NULL components (heap) fold into a null-bitmap byte in
// the key image (CH `nullable_keys128`); pgrcolumnar rides the no-NULLs proof.
//
// Fallback discipline: multi-key has NO C staged-probe leg (the tuplehash
// kernel is Expr) — the compact table is the ONLY packed host. The runtime
// backstop check runs BEFORE each batch's per-row emit (qual evaluated
// exactly once per row either way); after a migration the feed falls to the
// per-row arrival probe for the batch and the rest of the scan, with every
// group already in the C table (compact_migrate reconstructs component
// datums and inserts through the unmodified `lookup`, C-exact hashes).
// ===========================================================================

/// `PGRUST_LANE_V2_MULTIKEY` kill switch (default ON inside the lane).
fn multikey_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_LANE_V2_MULTIKEY").as_deref(), Ok("0") | Ok("off"))
    })
}

/// SE-MKTEXT (Lane-3 two-key text car): `PGRUST_LANE_V2_MULTIKEY_TEXT`,
/// **DEFAULT ON** since t35 routing-flips (GL-MKTEXT-1 FLIP-RECOMMENDED:
/// the two-key int+text shape 0.861 -> 0.061 hot unforced, 14.1x, == forced ref; zero
/// regressions); `=0|off` is the kill switch — every other spelling stays
/// ON (the flipped-kill idiom). Gates the UNPROJECTED scan feed's SECOND
/// TextRaw key component (the analytics charter's text+text `GROUP BY` census): the
/// primary text rides the dict-group lane exactly as today, the second is
/// opted in as an EXTRA dict-want column (`seq_scan_cb_dict_want_extra`,
/// the band-2a CaseDict mechanism) and packs through the SAME per-(epoch,
/// code) intern resolve — or the raw-answered-window fallback — into a
/// two-Intern MkShape (the canonical multi-tail encoding, canon-sink car
/// 1). Killed keeps the one-text census byte-for-byte. Same spelling as
/// the planner probe's keying (m5_suppress.rs — the AGG_POLY/GROUPSINK
/// knob-coherence law: BOTH read sites flip together).
fn multikey_text2_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_LANE_V2_MULTIKEY_TEXT").as_deref(), Ok("0") | Ok("off"))
    })
}

/// The plan-level half of the multi-key admission (the SoA/compact halves
/// need the armed batch — `scan_mk_shape`): unguarded, fully lanefold-
/// admitted, 2..N grouping keys (the single-key kernels own num_cols == 1).
/// Mirrors `scan_k2_wanted`'s role, including forcing the SoA deform so the
/// key lanes stage even for count(*)-only fold plans.
fn scan_mk_plan_wanted<'mcx>(agg: &::nodeagg::AggStateData<'mcx>) -> bool {
    multikey_enabled()
        && ::nodeagg::agg_lanefold_plan(agg).is_some_and(|plan| !plan.guarded)
        && !::nodeagg::agg_lanefold_has_resid(agg)
        && ::nodeagg::agg_hash_staged_probe_col(agg).is_none()
        && ::nodeagg::agg_hash_key_cols(agg).len() >= 2
}

/// The multi-key shapes' raw-bytes TEXT key components among Int-class
/// keys: `Some((atts, n))` with n ∈ 0..=2 (input colnos, group-clause
/// order). Historically capped at ONE (the dict-group lane); SE-MKTEXT
/// admits a SECOND text component behind `PGRUST_LANE_V2_MULTIKEY_TEXT`
/// (the caller gates — this census only counts). `None` = a third text or
/// an unpackable Other component (the compact arm refuses the same shapes
/// with the same taxonomy).
fn scan_mk_text_atts<'mcx>(agg: &::nodeagg::AggStateData<'mcx>) -> Option<([u16; 2], usize)> {
    let mut texts = [0u16; 2];
    let mut n = 0usize;
    for (att, kind) in ::nodeagg::agg_hash_key_cols(agg) {
        match kind {
            ::nodeagg::GroupKeyKind::Int { .. } | ::nodeagg::GroupKeyKind::Numeric => {}
            ::nodeagg::GroupKeyKind::TextRaw => {
                if n == 2 {
                    return None;
                }
                texts[n] = att;
                n += 1;
            }
            ::nodeagg::GroupKeyKind::Other => return None,
        }
    }
    Some((texts, n))
}

/// Multi-key dict-component columnar arm, tried when the fixed-width-prefix
/// arm refused (the text key component sits inside the prefix) — the
/// multi-key twin of `try_arm_cb_dictgroup`: arm the pgrcolumnar SoA staging
/// with the text component opted into dict lanes; the packing pre-pass
/// consumes the codes through the per-epoch intern resolve. False =
/// fail-open (per-row paths, byte-identical).
fn try_arm_cb_multikey_dict<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> bool {
    if !multikey_enabled()
        || !::nodeseqscan::seq_scan_is_pgrcolumnar(ss)
        || !scan_mk_plan_wanted(agg)
    {
        return false;
    }
    let refused = || {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::MultiKeyShape);
        false
    };
    let Some(plan) = ::nodeagg::agg_lanefold_plan(agg) else { return refused() };
    if !plan.vguards.is_empty() {
        return refused();
    }
    // Pure-int multi-key shapes need no dict lane — but a varlena column
    // INSIDE the fixed-width prefix (the reason the standard arm refused)
    // still blocks the staging. The offset-free columnar arm hosts those
    // (the high-cardinality two-int-key `GROUP BY` on pgrcolumnar): every staged
    // column fills as decoded Datums, no dict registration.
    let Some((texts, n_texts)) = scan_mk_text_atts(agg) else {
        // A third text / Other component: the compact arm would refuse
        // anyway — don't arm for nothing.
        return refused();
    };
    if n_texts == 0 {
        let Some(prefix) = fused_agg_soa_prefix(agg, ss) else { return refused() };
        if !::nodeseqscan::seq_scan_cb_columnar_arm(ss, estate, prefix, None) {
            return refused();
        }
        return true;
    }
    // SE-MKTEXT: the second text component is knob-gated (default OFF keeps
    // the one-text census byte-for-byte).
    if n_texts == 2 && !multikey_text2_enabled() {
        return refused();
    }
    // The fold must not read any dict component's SoA Datum cells: they are
    // STALE while a dict lane answers (the dictgroup rule, unchanged; the
    // extra dict-want column rides the same fill).
    if plan.cols.iter().any(|&c| texts[..n_texts].contains(&c)) {
        return refused();
    }
    let Some(prefix) = fused_agg_soa_prefix(agg, ss) else { return refused() };
    if !::nodeseqscan::seq_scan_cb_dictgroup_arm(ss, estate, prefix, texts[0]) {
        return refused();
    }
    // SE-MKTEXT second text: deliberately NOT dict-opted — it stages as
    // decoded raw Datums and the pack pre-pass interns it per row through
    // the raw-answered Intern branch ("correct, colder"). The dict-code
    // fast path's per-(epoch, code) id cache is SINGLE-LANE (one cache per
    // scratch), so a second dict-coded component would read the first's
    // ids — the cross-component crosstalk the mktext e2e caught live. A
    // per-component cache is the named follow-up (GL-MKTEXT-2).
    true
}

/// Multi-key admission inputs for the scan feed, decided once per build
/// (the compact table is ARMED as a side effect — mirrors the K2 +
/// compact-arm sequence).
struct ScanMk {
    /// The armed packed-key layout (component input colnos + offsets).
    shape: ::nodeagg::MkShape,
    /// The dict/intern text component's input colno, when one exists.
    dict_att: Option<u16>,
}

impl ScanMk {
    /// Heap backing-store bytes for the process estate ledger
    /// (GL-CONCMEM-1; the inline struct rides its owner's storage).
    fn estate_bytes(&self) -> usize {
        vec_estate_bytes(&self.shape.comps)
    }
}

/// Reusable per-build scratch for the multi-key batch loop: survivors, the
/// u128 pack accumulator, the packed key lanes, and the per-epoch
/// code → intern-id cache (dictgroup's `slots` pattern, retargeted).
#[derive(Default)]
struct MkScratch {
    rows: Vec<u32>,
    packbuf: Vec<u128>,
    keys1: Vec<i64>,
    keys2: Vec<[u64; 2]>,
    // Identity of the code -> intern-id cache: (is_global, id). Per-epoch
    // (RG index, cleared per roll) or — under a v7 stitch — part-global
    // (scan-stable gepoch, never cleared within one scan). Entry encoding:
    // 0 = unset, `id + 1` otherwise (exprkey::reset_code_id_cache — the
    // zero-page allocation is the vecstate CaseDict-shape fix: the gndv-sized
    // eager None-fill was 38% of that shape's cycles).
    epoch: Option<(bool, u64)>,
    code_ids: Vec<u32>,
    /// DIRECT single-text arm (arena-strings inc-3): per-(identity, code)
    /// GROUP STATE pointer cache — dict[code]'s live row in the DIRECT
    /// compact table (null = unset). THE 830320fed LAW: any code→X cache is
    /// (build, epoch, table-generation)-scoped — this one shares `epoch`'s
    /// identity roll, and because for direct tables every FLUSH resets the
    /// table itself (the table IS the vocabulary), the flush's
    /// `intern_reset` signal is unconditionally true and the drain clears
    /// `epoch`/this vec (runtime_agg's flush sites) — a cached pointer
    /// would otherwise dangle into the reset RowStore. FAIL-CLOSED: cleared
    /// on every identity mismatch and every flush.
    code_states: Vec<*mut u8>,
    /// LIMIT-k-no-ORDER freeze filter scratch (band-2a): the worker's
    /// parsed snapshot of the frozen set + its per-epoch code -> member-mask
    /// cache. `None` until this worker observes FROZEN.
    fz: Option<MkFreezeSnap>,
    /// Per-survivor candidate-mask scratch (reused across batches).
    fz_mask: Vec<u64>,
}

impl MkScratch {
    /// Backing-store bytes for the process estate ledger (GL-CONCMEM-1).
    /// The per-epoch code caches (`code_ids`/`code_states`, and the freeze
    /// snapshot's `code_mask`) are gndv-sized — the family's whale lanes at
    /// high-NDV dict shapes; everything else is batch-bounded.
    fn estate_bytes(&self) -> usize {
        let fz = self.fz.as_ref().map_or(0, |s| {
            core::mem::size_of::<MkFreezeSnap>()
                + vec_estate_bytes(&s.comp_vals)
                + s.comp_vals
                    .iter()
                    .flatten()
                    .map(vec_estate_bytes)
                    .sum::<usize>()
                + vec_estate_bytes(&s.texts)
                + s.texts.iter().map(vec_estate_bytes).sum::<usize>()
                + vec_estate_bytes(&s.code_mask)
        });
        vec_estate_bytes(&self.rows)
            + vec_estate_bytes(&self.packbuf)
            + vec_estate_bytes(&self.keys1)
            + vec_estate_bytes(&self.keys2)
            + vec_estate_bytes(&self.code_ids)
            + vec_estate_bytes(&self.code_states)
            + vec_estate_bytes(&self.fz_mask)
            + fz
    }
}

/// One worker's parsed view of the frozen canonical set ([`MkScratch::fz`]).
/// Entry order matches the shared set; masks are bit-per-entry (bound <= 64
/// by [`::nodeagg::sink::SINK_FREEZE_MAX_BOUND`]).
struct MkFreezeSnap {
    /// All-entries mask: (1 << k) - 1 (k = 64 => u64::MAX).
    full: u64,
    /// Per Int component (aligned with `shape.comps`; None for the Intern
    /// component): each entry's canonical (sign-extended) value.
    comp_vals: Vec<Option<Vec<i64>>>,
    /// The Intern component's text payload per entry (empty when the shape
    /// has no Intern component — word-keyed shapes).
    texts: Vec<Vec<u8>>,
    has_intern: bool,
    /// Per-epoch dict code -> member mask (the code_ids identity discipline,
    /// retargeted to the text bytes — intern-table resets do NOT invalidate
    /// it, so it rolls on its own identity).
    code_epoch: Option<(bool, u64)>,
    code_mask: Vec<Option<u64>>,
}

impl MkFreezeSnap {
    /// Parse the shared canonical entries against the armed shape. Entry
    /// encoding = the seal/flush canonical bytes: `packed_bytes` LE image
    /// bytes (Intern id bytes zeroed) + the text tail (Intern shapes only).
    fn build(shape: &::nodeagg::MkShape, entries: &[Vec<u8>]) -> MkFreezeSnap {
        let k = entries.len();
        debug_assert!(k >= 1 && k <= 64);
        // The snapshot parses a SINGLE raw text tail (`e[pb..]`); multi-tail
        // canonical images (two-Intern shapes, canon-sink car 1) are
        // length-prefixed and never reach the freeze (arming excludes them
        // — the Mk drain cannot produce a two-Intern shape).
        debug_assert!(shape.n_intern() <= 1, "freeze snapshot is single-tail");
        let full = if k == 64 { u64::MAX } else { (1u64 << k) - 1 };
        let pb = shape.packed_bytes as usize;
        let mut comp_vals: Vec<Option<Vec<i64>>> = Vec::with_capacity(shape.comps.len());
        for comp in &shape.comps {
            match comp.kind {
                ::nodeagg::MkCompKind::Int { width } => {
                    let mut vals = Vec::with_capacity(k);
                    for e in entries {
                        debug_assert!(e.len() >= pb);
                        let mut w = [0u8; 8];
                        let off = comp.off as usize;
                        w[..width as usize].copy_from_slice(&e[off..off + width as usize]);
                        let raw = u64::from_le_bytes(w);
                        // Sign-extend at the component width (canonical i64).
                        let shift = 64 - (width as u32) * 8;
                        vals.push(((raw << shift) as i64) >> shift);
                    }
                    comp_vals.push(Some(vals));
                }
                _ => comp_vals.push(None),
            }
        }
        let has_intern = shape.intern_comp().is_some();
        let texts = entries.iter().map(|e| e[pb..].to_vec()).collect();
        MkFreezeSnap {
            full,
            comp_vals,
            texts,
            has_intern,
            code_epoch: None,
            code_mask: Vec::new(),
        }
    }

    /// The member mask for one text payload (linear over <= 64 entries;
    /// called once per (epoch, code) through the cache, or per row on raw
    /// windows).
    fn text_mask(&self, bytes: &[u8]) -> u64 {
        let mut m = 0u64;
        for (e, t) in self.texts.iter().enumerate() {
            if t.as_slice() == bytes {
                m |= 1u64 << e;
            }
        }
        m
    }
}

/// SINGLE-TEXT sink admission (M2 C2 car, sink-only — the serial lane's
/// single-text builds keep their own K2-staged-text / dictgroup paths): the
/// one grouping key is a text/varchar column probing through the TEXT
/// kernel (deterministic collation proved at kernel selection), hosted as a
/// 1-component Intern MkShape over the packed machinery — the intern id IS
/// the packed image; canonical raw bytes are the cross-worker merge key.
/// Works under BOTH stagings: dictgroup-armed (codes through the per-epoch
/// intern memo) and prewhere/raw staged text (the Intern arm's raw branch).
/// `arm` mirrors `scan_mk_admit`'s halves (leader probe vs worker arm).
fn scan_mk1_text_admit<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
    estate: &EStateData<'mcx>,
    arm: bool,
) -> Option<ScanMk> {
    if !multikey_enabled() || !::nodeseqscan::seq_scan_is_pgrcolumnar(ss) {
        return None;
    }
    // SE-T2AGG CAR B: vguard-only guarded plans (min/max(text) passengers)
    // admit knob-ON through `sink_vguard_plan_ok` — the grouped-min/max single-
    // text-key shape. The proof obligation moves to the sink drain's
    // per-batch check_guards (demote = refusal to the serial rerun).
    let vguard_ok = sink_vguard_plan_ok(agg, ss);
    let plan_ok = (::nodeagg::agg_lanefold_plan(agg).is_some_and(|plan| !plan.guarded)
        || vguard_ok)
        && !::nodeagg::agg_lanefold_has_resid(agg);
    if !plan_ok {
        return None;
    }
    let refused = |r: RefuseReason| {
        stats::tick_refused(ShapeClass::AggBuild, r);
        None
    };
    let key = ::nodeagg::agg_hash_staged_probe_col(agg)?;
    if !::nodeagg::agg_hash_staged_probe_is_text(agg) {
        return None;
    }
    {
        let plan = ::nodeagg::agg_lanefold_plan(agg)?;
        if !plan.vguards.is_empty() && !vguard_ok {
            return refused(RefuseReason::MultiKeyShape);
        }
        // Stale-cell rule: while a dict lane answers the key column, its
        // SoA Datum cells are stale — the fold must not read them. (Raw
        // staging has no dict lane, but the worker's staging arm may
        // dict-arm where the leader's did — gate on the plan either way.)
        // The vguard widening keeps the rule verbatim: a min/max(text)
        // reading the KEY column itself stays refused (its lane is the
        // dict/intern side channel, not a foldable Datum lane).
        if plan.cols.iter().any(|&c| c == key) {
            return refused(RefuseReason::MultiKeyShape);
        }
    }
    // Every needed column must be a staged SoA lane (the mk feed's own
    // SoA-half checks, verbatim).
    {
        let soa = ::nodeseqscan::seq_scan_batch_soa(ss)?;
        let (colnos_needed, max_colno) = ::nodeagg::agg_hash_needed_cols(agg);
        let natts = estate
            .slot(ss.ss.ss_ScanTupleSlot)
            .base()
            .tts_tupleDescriptor
            .as_ref()?
            .attrs
            .len();
        if colnos_needed.len() != natts || max_colno > soa.ncols() as i32 {
            return refused(RefuseReason::MultiKeyShape);
        }
        if key as usize >= soa.ncols() as usize || !colnos_needed[key as usize] {
            return refused(RefuseReason::MultiKeyShape);
        }
    }
    let dict = Some(key);
    let verdict = if arm {
        match ::nodeagg::agg_hash_compact_try_arm_mk1(agg, dict) {
            ::nodeagg::CompactArm::Armed => {
                let shape =
                    ::nodeagg::agg_hash_compact_mk_shape(agg).expect("armed single-text table");
                if ::nodeagg::agg_hash_compact_text_direct(agg) {
                    // arena-strings inc-3 witness (e2e/rig greppable).
                    lane_trace("runtime-agg: text-direct accept table armed");
                }
                return Some(ScanMk { shape, dict_att: dict });
            }
            v => v,
        }
    } else {
        match ::nodeagg::agg_hash_compact_mk_admit1(agg, dict) {
            Ok((shape, _numgroups)) => return Some(ScanMk { shape, dict_att: dict }),
            Err(v) => v,
        }
    };
    match verdict {
        ::nodeagg::CompactArm::Armed => unreachable!("armed verdicts returned above"),
        ::nodeagg::CompactArm::KeyKind => refused(RefuseReason::MultiKeyShape),
        ::nodeagg::CompactArm::SpillRisk => refused(RefuseReason::CompactSpillRisk),
        ::nodeagg::CompactArm::Off => None,
    }
}

/// The probe half of the single-text admission (M2 sink leader — no table
/// armed; the serial fallback re-runs its own arm at its own build).
fn scan_mk1_text_probe<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> Option<ScanMk> {
    scan_mk1_text_admit(agg, ss, estate, false)
}

/// The arm half of the single-text admission (M2 sink worker builds).
fn scan_mk1_text_shape<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> Option<ScanMk> {
    scan_mk1_text_admit(agg, ss, estate, true)
}

/// The scan feed's multi-key admission + compact arm, decided once per
/// build: plan-level gates (`scan_mk_plan_wanted`), the dict component's
/// lane registration (when one exists), key lanes staged in the armed SoA,
/// then the packing admission + table arm in `agg_hash_compact_try_arm_mk`.
/// `None` = keep the per-row arrival probe (byte-identical), refuse reasons
/// ticked per taxonomy.
fn scan_mk_shape<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> Option<ScanMk> {
    scan_mk_admit(agg, ss, estate, true)
}

/// The probe half of [`scan_mk_shape`] for the M2 sink leader: identical
/// gates and shape, but the compact table is NOT armed — the leader's
/// executor only ever adopts the published parallel emit, so arming would
/// buy a dead group-estimate-sized prealloc (the serial fallback re-runs
/// the real arm at its own build).
fn scan_mk_probe<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> Option<ScanMk> {
    scan_mk_admit(agg, ss, estate, false)
}

fn scan_mk_admit<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
    estate: &EStateData<'mcx>,
    arm: bool,
) -> Option<ScanMk> {
    if !scan_mk_plan_wanted(agg) {
        return None;
    }
    let refused = |r: RefuseReason| {
        stats::tick_refused(ShapeClass::AggBuild, r);
        None
    };
    {
        let plan = ::nodeagg::agg_lanefold_plan(agg)?;
        if !plan.vguards.is_empty() {
            return refused(RefuseReason::MultiKeyShape);
        }
    }
    let is_cb = ::nodeseqscan::seq_scan_is_pgrcolumnar(ss);
    // Text components need the dict-lane registration (pgrcolumnar only) and
    // must stay out of the fold's lane reads (stale SoA cells). SE-MKTEXT:
    // a SECOND text component is knob-gated (`PGRUST_LANE_V2_MULTIKEY_TEXT`,
    // default OFF = the historical one-text census); the primary text is
    // the dict-group column, the second rides the extra dict-want lane the
    // staging arm registered (or the raw-answered Intern fallback).
    let text_atts = scan_mk_text_atts(agg);
    let has_text = ::nodeagg::agg_hash_key_cols(agg)
        .iter()
        .any(|&(_, k)| k == ::nodeagg::GroupKeyKind::TextRaw);
    if has_text {
        let Some((texts, n_texts)) = text_atts else {
            return refused(RefuseReason::MultiKeyShape);
        };
        debug_assert!(n_texts >= 1, "has_text census saw a TextRaw component");
        if n_texts == 2 && !multikey_text2_enabled() {
            return refused(RefuseReason::MultiKeyShape);
        }
        if !is_cb
            || ::nodeseqscan::seq_scan_batch_dictgroup_col(ss) != Some(texts[0])
            || ::nodeagg::agg_lanefold_plan(agg)
                .is_some_and(|plan| plan.cols.iter().any(|&c| texts[..n_texts].contains(&c)))
        {
            return refused(RefuseReason::MultiKeyShape);
        }
    }
    // Every key column must be a staged SoA lane the spillless packed feed
    // can read (colnos_needed always covers grouping columns).
    {
        let soa = ::nodeseqscan::seq_scan_batch_soa(ss)?;
        let (colnos_needed, max_colno) = ::nodeagg::agg_hash_needed_cols(agg);
        let natts = estate
            .slot(ss.ss.ss_ScanTupleSlot)
            .base()
            .tts_tupleDescriptor
            .as_ref()?
            .attrs
            .len();
        if colnos_needed.len() != natts || max_colno > soa.ncols() as i32 {
            return refused(RefuseReason::MultiKeyShape);
        }
        for (att, _) in ::nodeagg::agg_hash_key_cols(agg) {
            if att as usize >= soa.ncols() as usize || !colnos_needed[att as usize] {
                return refused(RefuseReason::MultiKeyShape);
            }
        }
    }
    // Packing admission + table arm (nullable = heap; pgrcolumnar rides the
    // no-NULLs per-chunk proof and packs no null byte). Text components
    // pass as the Intern att set (heap sources reach here textless — the
    // has_text block above requires cbstore): one att = the historical
    // dict-component admission verbatim; two atts = the SE-MKTEXT knob path
    // (mk_admit_n packs both through the shared intern pool).
    let interns_buf;
    let interns: &[u16] = match text_atts {
        Some((texts, n_texts)) if is_cb => {
            interns_buf = texts;
            &interns_buf[..n_texts]
        }
        _ => &[],
    };
    let dict = interns.first().copied();
    let verdict = if arm {
        match ::nodeagg::agg_hash_compact_try_arm_mk_multi(agg, !is_cb, interns) {
            ::nodeagg::CompactArm::Armed => {
                let shape =
                    ::nodeagg::agg_hash_compact_mk_shape(agg).expect("armed multi-key table");
                return Some(ScanMk { shape, dict_att: dict });
            }
            v => v,
        }
    } else {
        match ::nodeagg::agg_hash_compact_mk_admit_multi(agg, !is_cb, interns) {
            Ok((shape, _numgroups)) => return Some(ScanMk { shape, dict_att: dict }),
            Err(v) => v,
        }
    };
    match verdict {
        ::nodeagg::CompactArm::Armed => unreachable!("armed verdicts returned above"),
        ::nodeagg::CompactArm::KeyKind => refused(RefuseReason::MultiKeyShape),
        ::nodeagg::CompactArm::SpillRisk => refused(RefuseReason::CompactSpillRisk),
        ::nodeagg::CompactArm::Off => None,
    }
}

/// One page batch through the packed multi-key feed. Sequence per the
/// section header: (1) backstop check BEFORE any per-row work — a migration
/// returns `false` and the caller runs the WHOLE batch (emit included)
/// through the arrival leg, so the qual runs exactly once per row; (2)
/// survivor collection (`scan_collect_survivors` — slot-free when decidable,
/// the arrival loop's exact per-row emit sequence otherwise); (3) the pack
/// pre-pass over the survivors' staged component
/// lanes into the reused packed-key scratch (dict components through the
/// per-epoch intern resolve); (4) the compact-table batch probe + new-group
/// seeding; (5) the whole-batch fold.
#[allow(clippy::too_many_arguments)]
fn scan_mk_batch<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    mk: &ScanMk,
    mks: &mut MkScratch,
    idxs: &mut Vec<u32>,
    groups: &mut Vec<core::ptr::NonNull<::execexpr::AggPerGroup>>,
    n: u32,
    freeze: Option<&::nodeagg::sink::SinkFreeze>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if !::nodeagg::agg_hash_compact_backstop(agg, estate)? {
        return Ok(false);
    }
    // SURVIVOR-LESS PREWHERE-lane window: skip BEFORE the packability
    // pre-check below reads every staged row's cells (the near-unique-text-key codedgroup
    // precedent — condcache-census lane). A condition-cache hit whose
    // cached verdicts are all-fail legitimately skips the survivor deform
    // (nodeseqscan's cond_hit arm; multi-clause all-fail miss windows too),
    // so NO cell of the window is live — the pre-check would read stale
    // datums and could spuriously DISARM the compact table for the whole
    // remaining build over a window that has nothing to pack or fold on any
    // path. The conservative lane selection is exactly the completing
    // deform's own trigger domain (requal bits included), so "no bits" ==
    // "no deform ran" == "nothing survives". Non-lane stagings (varkey /
    // kernel prefix) deform every staged row up front and skip nothing.
    if let Some(lsel) = ::nodeseqscan::seq_scan_batch_lane_sel(ss) {
        if lsel.iter().all(|&w| w == 0) {
            return Ok(true);
        }
    }
    // Numeric components: per-VALUE packability over the WHOLE batch BEFORE
    // the per-row emit — an unpackable value (range / non-minimal display
    // scale, keypack module doc) migrates to the C table and the caller
    // routes this batch through the arrival leg, so the qual still runs
    // exactly once per row. Checking a superset of the survivors is sound
    // (pack legality is per-value, effect-free; survivor lane windows
    // complete the deform for every staged row, so the cells are live — the
    // survivor-less lane window was skipped above).
    let numeric_packable = {
        let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
            .expect("multi-key feed requires the armed SoA");
        mk.shape.comps.iter().all(|comp| {
            let ::nodeagg::MkCompKind::Numeric { width } = comp.kind else { return true };
            let att = comp.att as usize;
            let (values, isnull) = (soa.col_values(att), soa.col_isnull(att));
            (0..n as usize).all(|i| {
                if isnull[i] {
                    // Heap NULLs pack via the null-bitmap byte; a NULL under
                    // the pgrcolumnar no-NULLs proof is a staging surprise —
                    // demote instead of asserting in release.
                    return mk.shape.nullable;
                }
                ::nodeagg::mk_numeric_datum_bits(values[i], width).is_some()
            })
        })
    };
    if !numeric_packable {
        ::nodeagg::agg_hash_compact_disarm(agg, estate)?;
        return Ok(false);
    }
    let MkScratch { rows, packbuf, keys1, keys2, epoch, code_ids, code_states, fz, fz_mask } =
        mks;
    scan_collect_survivors(ss, estate, n, rows)?;
    // FREEZE FILTER (band-2a, LIMIT-k-no-ORDER): once FROZEN, drop
    // survivors whose key is not in the frozen set BEFORE any interning or
    // packing — post-freeze per-row work collapses to a per-(epoch, code)
    // mask lookup + tiny int compares. Component-major like the pack loop;
    // a row's final nonzero mask == full key equality with some entry.
    if let Some(fzc) = freeze {
        debug_assert!(!mk.shape.nullable, "freeze arming excludes nullable shapes");
        if fz.is_none() {
            if let Some(entries) = fzc.entries() {
                *fz = Some(MkFreezeSnap::build(&mk.shape, entries));
            }
        }
        if let Some(snap) = fz.as_mut() {
            let n_before = rows.len();
            fz_mask.clear();
            fz_mask.resize(rows.len(), snap.full);
            for (j, comp) in mk.shape.comps.iter().enumerate() {
                let att = comp.att as usize;
                match comp.kind {
                    ::nodeagg::MkCompKind::Int { width } => {
                        let vals =
                            snap.comp_vals[j].as_ref().expect("Int comp carries entry values");
                        let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                            .expect("multi-key feed requires the armed SoA");
                        let values = soa.col_values(att);
                        for (k, &i) in rows.iter().enumerate() {
                            let m = fz_mask[k];
                            if m == 0 {
                                continue;
                            }
                            let v = match width {
                                2 => values[i as usize].as_i16() as i64,
                                4 => values[i as usize].as_i32() as i64,
                                _ => values[i as usize].as_i64(),
                            };
                            let mut keep = 0u64;
                            let mut mm = m;
                            while mm != 0 {
                                let e = mm.trailing_zeros() as usize;
                                if vals[e] == v {
                                    keep |= 1u64 << e;
                                }
                                mm &= mm - 1;
                            }
                            fz_mask[k] = keep;
                        }
                    }
                    ::nodeagg::MkCompKind::Intern => {
                        let mcx = estate.es_query_cxt;
                        // Dict-code fast path ONLY for the feed's registered
                        // dict component: the snap's per-(identity, code)
                        // member-mask cache is single-lane, and another
                        // consumer (the PREWHERE dict tier; SE-MKTEXT's
                        // second text) may have dict lanes of its own whose
                        // code space would collide in it. Everything else
                        // takes the raw per-row compare below.
                        let lane = if Some(att as u16) == mk.dict_att {
                            ::nodeseqscan::seq_scan_batch_soa(ss)
                                .and_then(|soa| soa.dict_lane(att))
                        } else {
                            None
                        };
                        match lane {
                            Some(lane) => {
                                // The code_ids identity discipline retargeted
                                // to member masks (text-derived — intern
                                // resets never invalidate it).
                                let ndict = lane.table.ndict as usize;
                                let global = lane.table.has_stitch();
                                let (ident, size) = if global {
                                    ((true, lane.table.gepoch), lane.table.gndv as usize)
                                } else {
                                    ((false, lane.table.epoch), ndict)
                                };
                                if snap.code_epoch != Some(ident) {
                                    snap.code_epoch = Some(ident);
                                    snap.code_mask.clear();
                                    snap.code_mask.resize(size, None);
                                }
                                for (k, &i) in rows.iter().enumerate() {
                                    if fz_mask[k] == 0 {
                                        continue;
                                    }
                                    let local = lane.code(i as usize);
                                    let code = if global {
                                        lane.table.global_code(local) as usize
                                    } else {
                                        local as usize
                                    };
                                    let cm = match snap.code_mask[code] {
                                        Some(m) => m,
                                        None => {
                                            let d = lane.table.datum(local);
                                            // SAFETY: as the pack loop's dict
                                            // branch — live non-null text
                                            // varlena for the staged window.
                                            let v = unsafe {
                                                ::types_fmgr::datum_varlena_packed(d, mcx)
                                            }?;
                                            let m = snap.text_mask(v.data());
                                            snap.code_mask[code] = Some(m);
                                            m
                                        }
                                    };
                                    fz_mask[k] &= cm;
                                }
                            }
                            None => {
                                // Raw-answered window: per-row text compare
                                // (correct, colder — the dict path owns the
                                // hot shape).
                                let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                                    .expect("multi-key feed requires the armed SoA");
                                let values = soa.col_values(att);
                                for (k, &i) in rows.iter().enumerate() {
                                    if fz_mask[k] == 0 {
                                        continue;
                                    }
                                    let d = values[i as usize];
                                    // SAFETY: staged non-null live text
                                    // varlena (as the pack loop's raw branch).
                                    let v =
                                        unsafe { ::types_fmgr::datum_varlena_packed(d, mcx) }?;
                                    fz_mask[k] &= snap.text_mask(v.data());
                                }
                            }
                        }
                    }
                    ::nodeagg::MkCompKind::Numeric { .. } => {
                        unreachable!("freeze arming excludes Numeric components")
                    }
                }
            }
            // Compact the survivor list to members (order-preserving).
            let mut w = 0usize;
            for k in 0..rows.len() {
                if fz_mask[k] != 0 {
                    rows[w] = rows[k];
                    w += 1;
                }
            }
            rows.truncate(w);
            fzc.note_dropped((n_before - w) as u64);
        }
    }
    // DIRECT single-text accept (arena-strings inc-3, design §4.2): the
    // armed table keys on the canonical image itself — no interning, no
    // packing, no packed-word probe. Resolve survivors to live group states
    // (dict windows through the per-(identity, code) state cache, raw
    // windows per row), then the packed arm's exact fold + election tail.
    if ::nodeagg::agg_hash_compact_text_direct(agg) {
        let mcx = estate.es_query_cxt;
        scan_mk1_text_direct_batch(agg, ss, mk, rows, epoch, code_states, groups, mcx)?;
        idxs.clear();
        idxs.extend_from_slice(rows);
        let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
            .expect("multi-key feed requires the armed SoA");
        // SAFETY: as the packed arm's fold below — every probed row is
        // non-fallback (the caller admits only all-lane batches) with valid
        // lane values for every plan column (the key column is never in
        // `plan.cols` — admission); the plan is unguarded; each pergroup is
        // a live direct-table row installed by a probe since the last flush
        // (every direct flush clears the code→state cache through the
        // intern_reset channel before the next batch).
        let spk_t0 = ::nodeagg::spankey::spankey_t0();
        unsafe { agg_fold_staged(agg, soa, idxs, groups)? };
        ::nodeagg::spankey::spankey_lap(&::nodeagg::spankey::SPANKEY_CTRS.fold_ns, spk_t0);
        scan_mk_freeze_election(agg, freeze);
        return Ok(true);
    }
    // Pack pre-pass, component-major over the survivors (each component
    // lane streams once), into the REUSED u128 accumulator.
    packbuf.clear();
    packbuf.resize(rows.len(), 0u128);
    let shape = &mk.shape;
    for (j, comp) in shape.comps.iter().enumerate() {
        let att = comp.att as usize;
        let off_bits = comp.off as u32 * 8;
        // spankey copy-tax band timer (measurement only): Intern components
        // (datum views + code_ids + DictLazy ensures + intern resolves) vs
        // word components, per batch.
        let spk_t0 = ::nodeagg::spankey::spankey_t0();
        match comp.kind {
            ::nodeagg::MkCompKind::Int { width } => {
                let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                    .expect("multi-key feed requires the armed SoA");
                let (values, isnull) = (soa.col_values(att), soa.col_isnull(att));
                let mask = if width == 8 { u64::MAX } else { (1u64 << (width * 8)) - 1 };
                for (k, &i) in rows.iter().enumerate() {
                    let i = i as usize;
                    if shape.nullable && isnull[i] {
                        // CH nullable_keys128: bit j set, value bits zero —
                        // NOT-DISTINCT composite NULL semantics hold.
                        packbuf[k] |= 1u128 << (shape.null_off() as u32 * 8 + j as u32);
                        continue;
                    }
                    debug_assert!(
                        shape.nullable || !isnull[i],
                        "cbstore no-NULLs proof violated in a multi-key window"
                    );
                    let v = match width {
                        2 => values[i].as_i16() as i64,
                        4 => values[i].as_i32() as i64,
                        _ => values[i].as_i64(),
                    };
                    packbuf[k] |= (((v as u64) & mask) as u128) << off_bits;
                }
            }
            ::nodeagg::MkCompKind::Numeric { width } => {
                let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                    .expect("multi-key feed requires the armed SoA");
                let (values, isnull) = (soa.col_values(att), soa.col_isnull(att));
                for (k, &i) in rows.iter().enumerate() {
                    let i = i as usize;
                    if shape.nullable && isnull[i] {
                        packbuf[k] |= 1u128 << (shape.null_off() as u32 * 8 + j as u32);
                        continue;
                    }
                    let bits = ::nodeagg::mk_numeric_datum_bits(values[i], width)
                        .expect("numeric packability proven by the batch pre-check");
                    packbuf[k] |= (bits as u128) << off_bits;
                }
            }
            ::nodeagg::MkCompKind::Intern => {
                let mcx = estate.es_query_cxt;
                // Dict-code fast path ONLY for the feed's registered dict
                // component: the per-(identity, code) → intern-id cache
                // below is SINGLE-LANE (`epoch` + `code_ids`, one per
                // scratch), so a second dict-coded Intern component — the
                // SE-MKTEXT second text, or a PREWHERE-dict-tier column —
                // would read the FIRST component's ids for its own codes
                // (cross-component crosstalk, caught live by the mktext
                // e2e: t2 emitted t1's dict values). Non-dict components
                // take the raw-answered branch (per-row intern — correct,
                // colder); the per-component cache is GL-MKTEXT-2.
                let lane = if Some(att as u16) == mk.dict_att {
                    ::nodeseqscan::seq_scan_batch_soa(ss)
                        .and_then(|soa| soa.dict_lane(att))
                } else {
                    None
                };
                match lane {
                    Some(lane) => {
                        // Cache identity roll (dictgroup's per-RG cache,
                        // retargeted to intern ids — scan-stable, so the
                        // PACKED key is epoch-free). Under a v7 stitch the
                        // cache is indexed by PART-GLOBAL codes and keyed on
                        // the scan-stable gepoch — no per-RG re-intern of
                        // the dict entries.
                        let ndict = lane.table.ndict as usize;
                        let global = lane.table.has_stitch();
                        let (ident, size) = if global {
                            ((true, lane.table.gepoch), lane.table.gndv as usize)
                        } else {
                            ((false, lane.table.epoch), ndict)
                        };
                        if *epoch != Some(ident) {
                            *epoch = Some(ident);
                            exprkey::reset_code_id_cache(code_ids, size);
                        }
                        debug_assert!(code_ids.len() >= size);
                        for (k, &i) in rows.iter().enumerate() {
                            let local = lane.code(i as usize);
                            debug_assert!((local as usize) < ndict, "filler contract: code < ndict");
                            let code = if global {
                                lane.table.global_code(local) as usize
                            } else {
                                local as usize
                            };
                            debug_assert!(code < size, "stitch contract: global code < gndv");
                            let id = match code_ids[code] {
                                c if c != 0 => c - 1,
                                _ => {
                                    // First surviving row of (identity,
                                    // code): materialize dict[code] once,
                                    // intern.
                                    let d = lane.table.datum(local);
                                    // SAFETY: dict entries are live non-null
                                    // text varlenas for the staged window
                                    // (dict lane contract; kernel selection
                                    // proved the column type).
                                    let v = unsafe {
                                        ::types_fmgr::datum_varlena_packed(d, mcx)
                                    }?;
                                    let id =
                                        ::nodeagg::agg_hash_compact_intern(agg, v.data());
                                    debug_assert!(id != u32::MAX, "id+1 encoding");
                                    code_ids[code] = id + 1;
                                    id
                                }
                            };
                            packbuf[k] |= (id as u128) << off_bits;
                        }
                    }
                    None => {
                        // Raw-answered window (non-dict key chunk): intern
                        // the staged text datum per row — the dictgroup Raw
                        // fallback's multi-key analog (correct, colder).
                        let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                            .expect("multi-key feed requires the armed SoA");
                        let values = soa.col_values(att);
                        debug_assert!(
                            rows.iter().all(|&i| !soa.col_isnull(att)[i as usize]),
                            "cbstore no-NULLs proof violated in a multi-key window"
                        );
                        for (k, &i) in rows.iter().enumerate() {
                            let d = values[i as usize];
                            // SAFETY: staged non-null live text varlena (the
                            // columnar fill stages decoded Datums; kernel
                            // selection proved the column type).
                            let v = unsafe { ::types_fmgr::datum_varlena_packed(d, mcx) }?;
                            let id = ::nodeagg::agg_hash_compact_intern(agg, v.data());
                            packbuf[k] |= (id as u128) << off_bits;
                        }
                    }
                }
            }
        }
        {
            use ::nodeagg::spankey::{spankey_lap, SPANKEY_CTRS as S};
            let ctr = if matches!(comp.kind, ::nodeagg::MkCompKind::Intern) {
                &S.pack_intern_ns
            } else {
                &S.pack_word_ns
            };
            spankey_lap(ctr, spk_t0);
        }
    }
    // Split the accumulator into the packed key lane and probe (two-word
    // shapes view the accumulator in place — mkaccept inc-1).
    if shape.two_words {
        let lane = ::nodeagg::mk_keys2_lane(packbuf, keys2);
        ::nodeagg::agg_hash_compact_batch_mk2(agg, lane, groups)?;
    } else {
        keys1.clear();
        keys1.extend(packbuf.iter().map(|&w| w as u64 as i64));
        ::nodeagg::agg_hash_compact_batch_mk1(agg, keys1, groups)?;
    }
    idxs.clear();
    idxs.extend_from_slice(rows);
    let soa =
        ::nodeseqscan::seq_scan_batch_soa(ss).expect("multi-key feed requires the armed SoA");
    // SAFETY: as the K2 compact fold — every probed row is non-fallback (the
    // caller admits only all-lane batches) with valid lane values for every
    // plan column (a dict component is never in `plan.cols` — admission);
    // the plan is unguarded; each pergroup was installed by the compact
    // probe within this batch.
    let spk_t0 = ::nodeagg::spankey::spankey_t0();
    unsafe { agg_fold_staged(agg, soa, idxs, groups)? };
    ::nodeagg::spankey::spankey_lap(&::nodeagg::spankey::SPANKEY_CTRS.fold_ns, spk_t0);
    scan_mk_freeze_election(agg, freeze);
    Ok(true)
}

/// FREEZE INSTALL ELECTION (band-2a LIMIT-k-no-ORDER), shared by the packed and DIRECT
/// accept arms: the first worker whose live table reaches the bound wins the
/// CAS and publishes its first `bound` groups' canonical keys. Correct from
/// ANY table state — nothing was dropped anywhere before FROZEN, so every
/// present group is exact-so-far and set members keep counting everywhere
/// after.
fn scan_mk_freeze_election(
    agg: &::nodeagg::AggStateData<'_>,
    freeze: Option<&::nodeagg::sink::SinkFreeze>,
) {
    if let Some(fzc) = freeze {
        if !fzc.frozen()
            && ::nodeagg::agg_hash_compact_ngroups(agg)
                .is_some_and(|ng| ng >= fzc.bound() as usize)
            && fzc.try_begin_install()
        {
            match ::nodeagg::sink::sink_freeze_extract(agg, fzc.bound()) {
                Some(entries) => {
                    lane_trace(&format!(
                        "runtime-agg freeze: installed (bound={})",
                        fzc.bound()
                    ));
                    fzc.publish(entries);
                }
                None => {
                    // Fail OPEN: no drops ever happen; the engagement drains
                    // fully (correct, just unoptimized).
                    lane_trace("runtime-agg freeze: install extraction failed — disabled");
                    fzc.disable();
                }
            }
        }
    }
}

/// [`scan_mk_batch`]'s DIRECT single-text accept arm (arena-strings inc-3):
/// resolve each survivor row's text to its live group state by probing the
/// DIRECT compact table on the canonical image
/// ([`::nodeagg::agg_hash_compact_probe_text_direct`] — sink-hash probed, so
/// the table's saved hash word IS the sink hash). DICT windows resolve once
/// per (identity, code) through `code_states` — the Intern arm's `code_ids`
/// cache discipline with the value type swapped from intern id to live
/// group-state pointer (THE 830320fed LAW: (build, epoch, table-generation)
/// scoped; for direct tables every flush resets the TABLE, so the flush's
/// unconditional `intern_reset` signal clears this cache in the drain). RAW
/// windows probe the staged text per row (no cache — the dictgroup Raw
/// fallback's analog, correct and colder).
#[allow(clippy::too_many_arguments)]
fn scan_mk1_text_direct_batch<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
    mk: &ScanMk,
    rows: &[u32],
    epoch: &mut Option<(bool, u64)>,
    code_states: &mut Vec<*mut u8>,
    groups: &mut Vec<core::ptr::NonNull<::execexpr::AggPerGroup>>,
    mcx: ::mcx::Mcx<'mcx>,
) -> PgResult<()> {
    let shape = &mk.shape;
    debug_assert!(
        shape.comps.len() == 1
            && shape.comps[0].kind == ::nodeagg::MkCompKind::Intern
            && !shape.nullable,
        "direct accept requires the mk1 single-Intern shape"
    );
    let att = shape.comps[0].att as usize;
    groups.clear();
    let spk_t0 = ::nodeagg::spankey::spankey_t0();
    // Dict-code fast path only for the feed's registered dict component —
    // the single-lane cache discipline of the Intern arm, verbatim.
    let lane = if Some(att as u16) == mk.dict_att {
        ::nodeseqscan::seq_scan_batch_soa(ss).and_then(|soa| soa.dict_lane(att))
    } else {
        None
    };
    match lane {
        Some(lane) => {
            // Cache identity roll (the Intern arm's per-RG/gepoch cache,
            // retargeted to group-state pointers).
            let ndict = lane.table.ndict as usize;
            let global = lane.table.has_stitch();
            let (ident, size) = if global {
                ((true, lane.table.gepoch), lane.table.gndv as usize)
            } else {
                ((false, lane.table.epoch), ndict)
            };
            if *epoch != Some(ident) {
                *epoch = Some(ident);
                code_states.clear();
                code_states.resize(size, core::ptr::null_mut());
            }
            debug_assert!(code_states.len() >= size);
            for &i in rows.iter() {
                let local = lane.code(i as usize);
                debug_assert!((local as usize) < ndict, "filler contract: code < ndict");
                let code = if global {
                    lane.table.global_code(local) as usize
                } else {
                    local as usize
                };
                debug_assert!(code < size, "stitch contract: global code < gndv");
                let pg = match code_states[code] {
                    p if !p.is_null() => p,
                    _ => {
                        // First surviving row of (identity, code):
                        // materialize dict[code] once, probe DIRECT.
                        let d = lane.table.datum(local);
                        // SAFETY: dict entries are live non-null text
                        // varlenas for the staged window (dict lane
                        // contract; kernel selection proved the column
                        // type).
                        let v = unsafe { ::types_fmgr::datum_varlena_packed(d, mcx) }?;
                        let p = ::nodeagg::agg_hash_compact_probe_text_direct(agg, v.data())?
                            .as_ptr()
                            .cast::<u8>();
                        code_states[code] = p;
                        p
                    }
                };
                // SAFETY: probes never return null state pointers; a cached
                // pointer was installed by a probe since the last flush
                // (the flush clears this cache — the intern_reset channel).
                groups.push(unsafe {
                    core::ptr::NonNull::new_unchecked(pg.cast::<::execexpr::AggPerGroup>())
                });
            }
        }
        None => {
            // Raw-answered window (non-dict key chunk): probe the staged
            // text per row.
            let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                .expect("multi-key feed requires the armed SoA");
            let values = soa.col_values(att);
            debug_assert!(
                rows.iter().all(|&i| !soa.col_isnull(att)[i as usize]),
                "cbstore no-NULLs proof violated in a single-text window"
            );
            for &i in rows.iter() {
                let d = values[i as usize];
                // SAFETY: staged non-null live text varlena (the columnar
                // fill stages decoded Datums; kernel selection proved the
                // column type).
                let v = unsafe { ::types_fmgr::datum_varlena_packed(d, mcx) }?;
                groups.push(::nodeagg::agg_hash_compact_probe_text_direct(agg, v.data())?);
            }
        }
    }
    {
        use ::nodeagg::spankey::{spankey_lap, SPANKEY_CTRS as S};
        spankey_lap(&S.pack_intern_ns, spk_t0);
    }
    Ok(())
}

/// Shared fold tail for the staged fold feeds (seqscan page batches and the
/// joined-row staging buffer): the admitted transitions run whole-batch over
/// the probed rows' pergroup snapshots via `lanefold::fold_rows_grouped`,
/// generic over the staged-lanes source (`LaneCols`).
///
/// # Safety
/// `groups[k]` is the live pergroup array the probe just installed for staged
/// row `idxs[k]` (hash entries and their additional blocks are
/// allocation-stable for the table's lifetime; spill mode only redirects NEW
/// groups to the tapes — spilled rows never reach `groups`); `cols` covers
/// every staged row for every plan column; AvgAccum pergroups hold the
/// catalog's `{0,0}` int8[2] transarray, datum-copied per group at entry
/// initialization; Int128AvgAccum pergroups are NULL or hold the aggcontext
/// state the transfn chain installed, and `agg_aggcontext` is that same
/// aggcontext; guarded plans passed `check_guards` on this batch.
unsafe fn agg_fold_staged<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    cols: &impl ::lanefold::LaneCols,
    idxs: &[u32],
    groups: &[core::ptr::NonNull<::execexpr::AggPerGroup>],
) -> PgResult<()> {
    // SAFETY: forwarded caller contract.
    unsafe { agg_fold_staged_mm(agg, cols, idxs, groups, None) }
}

/// `agg_fold_staged` with the str MIN/MAX dict-code memo (lane-v2-
/// dictminmax): a `Some(scratch)` routes str advances whose column carries a
/// sorted dict-code view (`LaneCols::col_codes`) through integer code
/// compares — transvalue bytes and datumCopy sequence provably unchanged
/// (`lanefold::str_advance_coded`). The FEED owns the scratch's
/// invalidation: any row of the build that advances an admitted str
/// transition outside this call (demote, fallback, arrival-probe accept)
/// must `invalidate()` before the next fold.
///
/// # Safety
/// As `agg_fold_staged`, plus the `col_codes` contract for every answered
/// column when `mm` is `Some`.
unsafe fn agg_fold_staged_mm<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    cols: &impl ::lanefold::LaneCols,
    idxs: &[u32],
    groups: &[core::ptr::NonNull<::execexpr::AggPerGroup>],
    mm: Option<&mut ::lanefold::StrMmScratch>,
) -> PgResult<()> {
    if idxs.is_empty() {
        return Ok(());
    }
    let plan = ::nodeagg::agg_lanefold_plan(agg).expect("fold feed without a plan");
    let aggcx = ::nodeagg::agg_aggcontext(agg);
    // avgpack: packed inline AvgAccum slots — nonzero only on sink worker
    // builds (the armed table's creation-time mask; representation state
    // travels WITH the table that holds the states).
    let avgpack_mask = ::nodeagg::sink::agg_sink_avgpack_mask(agg);
    // Byref str transvalue copies: the TABLE-OWNED state store on a SINK build
    // (the table migrates across pool threads with the morsel lend/reclaim, so
    // the store must travel with it — `StrStateArena` doc), else the bump
    // aggcontext. Borrowed for exactly this fold call — mutation is
    // morsel-serialized and the combine phase never reaches here.
    //
    // The aggcontext arm is correct ONLY for a build whose table and context
    // die together, i.e. a serial (non-sink) build — which is C's own
    // one-allocator-per-table invariant. Sink builds MUST have the store.
    let mut sa = ::nodeagg::sink::agg_sink_str_arena(agg).map(|c| c.borrow_mut());
    // GL-SINKCRASH-2 — the fail-closed half of the class fix. `arm_sink_build`
    // arms the store for every drain whose plan carries a by-ref str
    // transvalue; this refuses if a sink build ever reaches a str advance
    // without one, instead of silently copying into the serving thread's
    // aggcontext. That silent fall-back is the whole defect: it cost this class
    // five incidents because nothing ever made an unarmed sink build LOUD.
    // A shape error aborts the RG and the serial arm reruns the statement, so
    // the failure mode of a future missed arming is a slow correct answer, not
    // a freed pointer in a live pergroup.
    if sa.is_none()
        && ::nodeagg::sink::agg_sink_state_bytes(agg).is_some()
        && ::lanefold::plan_has_str_trans(plan)
    {
        return Err(::nodeagg::sink::sink_shape_error(
            "byref str transvalue folded on a sink build with no table-owned state store",
        ));
    }
    // SAFETY: caller contract (above) is exactly fold_rows_grouped_mm's.
    unsafe {
        ::lanefold::fold_rows_grouped_mm(
            plan,
            cols,
            idxs,
            groups,
            aggcx,
            mm,
            avgpack_mask,
            sa.as_deref_mut(),
        )
    }
}

/// Refuse-set for the lane-v2 hash-agg pipeline. Two halves:
///   * scan side: the Phase-1 `seq_scan_fusible` gate verbatim (page-batch AM,
///     uninstrumented, forward, non-parallel, non-EPQ, non-Bloom; subplan- and
///     param-bearing quals/projections run scalar-within-lane via
///     `seq_scan_batch_emit`'s hosted arms) — WIDER than the legacy fused arm's
///     `seq_agg_fusible` (any scalar qual and any admitted projection run
///     scalar-within-lane, not just kernel quals / outer-read-free tlists);
///   * agg side: `agg_hash_breaker_admissible` (batch-drainable — no grouping
///     sets / DISTINCT-or-ordered-input / merge phase / subplan transitions —
///     AGG_HASHED, initplan-param-free). AGG_PLAIN routes to the fold drive
///     above (`try_own_plain_agg_over_seq_scan`) before this gate runs.
/// A post-build merge handoff flips `agg_batch_drainable` false, so later
/// calls refuse here and fall to `exec_agg`'s merged retrieve — exactly the
/// existing `exec_agg_batched` arm's cross-call behavior.
fn agg_over_seq_scan_fusible<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if !::nodeagg::agg_hash_breaker_admissible(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AggNotDrainable);
        return Ok(false);
    }
    // A scan-side refusal ticks under the SeqScan class inside
    // `seq_scan_fusible` (memoized), so it is counted once, not re-attributed.
    seq_scan_fusible(ss, estate)
}

/// Deform prefix for the SoA page-batch deform under the fused agg drive:
/// everything the per-row consumers read from the scan slot — the agg's
/// outer-column bound (transition args + grouping columns; outer slot == scan
/// slot for unprojected scans) and the scan qual's fetch bound. None = a
/// consumer's shape is unknown; the SoA deform stays disarmed (per-row lazy
/// deform, still correct).
fn fused_agg_soa_prefix<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
) -> Option<i32> {
    let mut p = ::nodeagg::agg_batch_outer_prefix(agg)?;
    if let Some(q) = ss.ss.qual.as_deref() {
        p = p.max(q.max_fetch(::execexpr::SlotSrc::Scan)?);
    }
    Some(p)
}

/// The breaker as Sink of pipeline N: accept = the existing hashagg per-row
/// build (prepare/lookup + transition program, spill-mode spilling included);
/// finish = the existing finalize tail (spill finish, handoff install, phase
/// flip). Always `NeedMore` — a breaker consumes its whole input.
struct HashAggBuildSink<'a, 'mcx> {
    agg: &'a mut ::nodeagg::AggStateData<'mcx>,
}

impl<'mcx> Sink<'mcx> for HashAggBuildSink<'_, 'mcx> {
    fn accept(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<SinkFeed> {
        ::nodeagg::agg_hash_build_accept(self.agg, estate, tuple)?;
        Ok(SinkFeed::NeedMore)
    }

    // Stage-4 combine seam: a parallel worker's partial build hands its
    // whole table to the leader here (merge handoff); idempotent under the
    // following finish (nodeagg's combined flag).
    fn combine(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        ::nodeagg::agg_hash_build_combine(self.agg, estate)
    }

    fn finish(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        ::nodeagg::agg_hash_build_finish(self.agg, estate)
    }
}

/// Batch-granular feed: the default loop, monomorphized — each staged row
/// runs the same `agg_hash_build_accept` in the same order, with the per-row
/// dyn dispatch, `SinkFeed` matching, and consume-cursor saves elided.
impl<'mcx> BatchSink<'mcx> for HashAggBuildSink<'_, 'mcx> {}

/// The breaker as Source of pipeline N+1: produce = the existing
/// `agg_retrieve_hash_table` read-back, one final projected group row per
/// batch (the row lives in the agg's result slot — node-side, per the `Batch`
/// contract). Delegation preserves C's group output order exactly (§7's
/// pragmatic rule for this slice: same table, same iteration, same spill
/// refill → same order, so regress stays byte-comparable without the
/// annotated comparator).
struct HashAggSource;

impl<'mcx> Source<'mcx> for HashAggSource {
    type Node = ::nodeagg::AggStateData<'mcx>;

    fn produce(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<Batch>> {
        Ok(::nodeagg::agg_hash_retrieve(node, estate)?.map(|_| Batch { n: 1 }))
    }
}

/// Pass-through operator for the probe pipeline: pushes the produced group
/// row (already finalized + projected into the result slot) to the root.
/// One-row batches never outlive the producing driver round → no cursor.
struct HashAggEmit;

impl<'mcx> Operator<'mcx> for HashAggEmit {
    type Node = ::nodeagg::AggStateData<'mcx>;

    fn pending(&self, _node: &Self::Node) -> Option<Batch> {
        None
    }

    fn consume(
        &mut self,
        node: &mut Self::Node,
        batch: Batch,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        debug_assert_eq!(batch.n, 1);
        Ok(match out.accept(node.ps_ResultTupleSlot, estate)? {
            SinkFeed::Full => OpStatus::Paused,
            SinkFeed::NeedMore => OpStatus::NeedInput,
        })
    }
}

// ===========================================================================
// Sort pipeline-breaker (Phase 2 operator→operator seam). ONE node
// implementing `Sink` for pipeline N (the feed: scan source → scalar
// filter/project → sort sink) and `Source` for pipeline N+1 (the read-back:
// sort source → RootAdapter), chained by a per-node Feed→Emit phase flag —
// which is exactly the row path's `sort_Done`, so `exec_rescan_sort` resets
// the phase (and delegates tuplesort rescan semantics) unchanged, and falling
// back to `exec_sort` at any call boundary is byte-safe (same node state).
//
// Everything delegates to the row-path `Tuplesort` (design §8: default =
// delegate finalize/read-back to the row-path state): `Sink::accept` =
// `tuplesort_puttupleslot`/`putdatum`, `Sink::finish` =
// `tuplesort_performsort`, `Source::produce` = `tuplesort_gettupleslot`/
// `getdatum` — via `nodesort`'s lane seam, over the SAME `SortState` the
// per-tuple `exec_sort` / fused `exec_sort_batched` use. Output order is
// therefore C's exactly, by construction. The feed is the Phase-1 scan
// pipeline (same sources, same per-row scalar emit) with the breaker as its
// sink instead of the root adapter, so the put sequence equals the per-tuple
// feed's — byte-identical.
// ===========================================================================

/// Try to let the lane own a `Sort` over a lane-fusible scan child. `Some` =
/// the lane drove this call; `None` = refused (caller runs the unchanged
/// `exec_sort`/`exec_sort_batched` paths — byte-safe even mid-stream, since
/// both drive the same node state).
#[inline]
pub fn try_own_sort<'mcx>(
    s: &mut crate::procnode::SortNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic gates, every call (cheap): EPQ can engage between calls on the
    // same node tree. (The backward gate retired with the backward-execution
    // wave B11: pulls are forward-invariant below the run seam, B1.)
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::SortFeed, RefuseReason::Epq);
        return Ok(None);
    }
    if !sort_lane_fusible_memo(s, estate)? {
        // SORTFEED-RA shave (the AD2 flip letter's documented follow-up —
        // notes/sortfeed-diffprof-lane.md "Named follow-up shave"): once
        // the sort is DONE, every path in this refused-memo branch returns
        // `Ok(None)` with no side effects — an RA-admitted node's feed
        // already happened and its read-back is the caller's bare drain
        // leg (the FEED-ONLY contract below); a refused node returns
        // `None` regardless. So exit on one `sort_Done` load BEFORE the
        // knob OnceLock + `sort_randomaccess_memo` probe: that pair was
        // the ledgered ~29 Ir/pull post-done exit ceremony behind the
        // AD2 letter's +0.72% residual (pgrust-corpus-pairs-1784356345).
        // The ONLY skipped side effect is first-time RA side-memo
        // computation on a node that reached `sort_Done` via the row/
        // fused path before its first forward lane pull (e.g. a
        // backward-first scroll cursor) — a stats-tick/trace delta only,
        // never a behavior one; the chain-shared memo above still
        // computes (and ticks) exactly as before. Unit pin:
        // `sortfeed_ra_postdone_pull_exits_before_ra_memo`.
        if s.state.sort_done() {
            return Ok(None);
        }
        // WS-AD wave-8: the chain-shared memo refuses ALL randomAccess
        // sorts (the policy line). The bare hook alone re-checks knob-ON:
        // an admitted randomAccess sort runs the RA-vanilla feed and
        // delegates every random-access read to the row-path Tuplesort
        // over the SAME node state (region doc at
        // `sort_randomaccess_memo`). Knob-OFF this is one field load on
        // the already-refused path — zero cost on owned paths.
        if !(s.state.randomAccess
            && sort_randomaccess_enabled()
            && sort_randomaccess_memo(s, estate)?)
        {
            return Ok(None);
        }
        // SORTFEED-DIFFPROF increment (wave-8 item-3 handback): the lane's
        // randomAccess ownership is the FEED ONLY. The callgrind pair on
        // corpus-p1-sortfeed (pgrust-cgpairs-1784351907, dist-prof, A/B
        // control-dump windows) split the re-earn letter's +2.70% residual
        // exactly: batch feed vs fused row feed = PAR (feed-side net
        // −2M Ir/replay, B slightly ahead), read-back per-pull ceremony =
        // the WHOLE regression (~118M Ir/replay = ~109 Ir per drained row:
        // `pull_step` + `RootAdapter::accept` + the `sort_feed_if_needed`
        // early-out + the `sort_randomaccess_memo` probe + a second
        // check_for_interrupts, on EVERY pull of a full-drain scroll
        // cursor). So feed once through the breaker sink (batched puts —
        // the owned tick fires at the feed event inside
        // `sort_feed_if_needed`, so D4 accounting is unchanged), then
        // REFUSE every call: the caller's `exec_sort`/`exec_sort_batched`
        // drain leg serves ALL read-back from the SAME node state (the
        // contract line above — byte-safe even mid-stream). The RA-vanilla
        // feed law (region doc) guarantees the tuplesort here is the row
        // path's own (no refsort, no runtime-sink adoption, no top-N cut
        // under randomAccess), so the row drain serves it verbatim; the
        // lane-served drain modes (refsort/runtime_full) exist only on
        // non-RA nodes, which keep the pull_step emit below. Post-done
        // pulls never reach here — the `sort_Done` head check on this
        // branch exits them in a handful of loads, the same cost class
        // as the knob-OFF refusal they replace (the SORTFEED-RA shave).
        debug_assert!(!s.state.sort_done());
        // C's CHECK_FOR_INTERRUPTS at ExecSort entry (the feed call).
        ::postgres_seams::check_for_interrupts::call()?;
        let crate::procnode::SortNode { state, outer, outer_desc, .. } = s;
        // A feed-time refuse (Ok(false)) needs no distinct arm:
        // ownership is refused either way, before any sort-side effect.
        let _ = sort_feed_if_needed(state, &mut **outer, outer_desc, None, estate)?;
        return Ok(None);
    }
    // C's CHECK_FOR_INTERRUPTS at ExecSort entry.
    ::postgres_seams::check_for_interrupts::call()?;

    let crate::procnode::SortNode { state, outer, outer_desc, rd_shape_refused, .. } = s;
    if !sort_feed_if_needed(state, &mut **outer, outer_desc, None, estate)? {
        // Feed-time refuse (agg-over-join multi-batch spill), before any
        // sort-side effect: the Volcano fallback resumes byte-identically.
        return Ok(None);
    }
    // Emit phase (pipeline N+1): the breaker's Source face streams the
    // tuplesort read-back through the root pull-adapter, one tuple per call.
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step(state, &mut SortEmitSource, &mut SortEmit, &mut root, estate)?))
}

/// Structural sort-breaker verdict, memoized at first call: the fusibility
/// cascade must not run once per pulled tuple, and a mid-stream verdict flip
/// would desync the staged-batch cursors. Shared by the bare sort hook and
/// the Limit/Unique-over-sort chains and the wave-4 chains over the sort
/// breaker (Group / Result / SubqueryScan) — all of which admit exactly the
/// sort shapes the breaker admits.
fn sort_lane_fusible_memo<'mcx>(
    s: &mut crate::procnode::SortNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    match s.lane_fusible {
        Some(v) => Ok(v),
        None => {
            // Refusal accounting ticks exactly here — once per memoized
            // structural verdict (a child-scan refusal's specific reason is
            // ticked under the child's class inside its fusible gate).
            let refuse = sort_refuse_reason(s, estate)?;
            if estate.engine_capture() {
                engine_capture_sort_verdict(s, refuse, estate)?;
            }
            if let Some(r) = refuse {
                stats::tick_refused(ShapeClass::SortFeed, r);
            }
            let v = refuse.is_none();
            s.lane_fusible = Some(v);
            Ok(v)
        }
    }
}

// ===========================================================================
// WS-AD wave-8 region: sort-breaker randomAccess admission (bare hook only).
// Contract §2 AD0 — the SE-LETTERS diagnosis verbatim: the breaker "already
// delegates finalize/read-back to the row-path Tuplesort, so the refusal is
// a policy line, not an architecture gap". Knob-ON, the bare sort hook
// admits randomAccess sorts (scroll cursors, nestloop-inner REWIND,
// mergejoin mark/restore): `sort_lane_begin` already builds the tuplesort
// with TUPLESORT_RANDOMACCESS off the node flag, the lane serves ONLY
// forward pulls (`sort_lane_next` — the same cursor advance `exec_sort`'s
// forward drain performs), and every random-access read rides the row-path
// fallbacks over the SAME node state: backward pulls refuse at
// `try_own_sort`'s direction gate and fall to `exec_sort`'s
// direction-aware drain; rewind rides `exec_rescan_sort`'s
// tuplesort_rescan arm; mark/restore ride `exec_sort_mark_pos`/
// `exec_sort_restr_pos` on the tuplesort directly.
//
// Scope fences (each recorded in notes/se-wave8-sortfeed.md):
//   * CHAIN hosts keep refusing randomAccess — the shared
//     `sort_refuse_reason` policy line is unchanged, so every chain memo
//     verdict is byte-identical to wave-7's. (A refused chain still lands
//     on the bare hook underneath when Volcano pulls the Sort node, so the
//     feed win materializes for chain shapes too.)
//   * The RA-VANILLA FEED LAW: an admitted randomAccess feed is
//     `exec_sort`'s construction verbatim — no runtime-sink adoption
//     (self-refused, runtime_sort.rs randomAccess gate), no zone-adaptive
//     order, no top-k cut, no refsort, no narrowed comparator, no agg
//     top-N specs (`sort_feed_if_needed` gates below). Every one of those
//     arms either replaces the tuplesort read-back face or reorders/
//     prunes arrival — sound for forward LIMIT reads, unproven for
//     random-access replay. The batched drains stay (put-order-identical
//     hoists into the real tuplesort).
//   * EXPLAIN (ENGINE) capture keeps reporting the chain-scope verdict
//     (RandomAccess) for these nodes knob-ON — ledgered inc-1 limitation;
//     production ownership shows in the SortFeed owned ticks.
// ===========================================================================

/// `PGRUST_LANE_V2_SORT_RANDOMACCESS` — wave-8 WS-AD knob, **default ON
/// since the SE9-GATES AD2 flip** (explicit `=0`/`off` = the permanent
/// kill switch restoring the pre-flip refusal stream; law 4 posture
/// preserved: either state is one branch on this cached bool, reached
/// only on already-refused nodes).
///
/// AD2 FLIP evidence (the diffprof package,
/// notes/sortfeed-diffprof-lane.md): the SE8 refusal at +2.705% was
/// 100% removable read-back dispatch (attribution
/// pgrust-cgpairs-1784351907-21c3: feed = PAR with the fused arm); the
/// FEED-ONLY fix (0d4bf241c — RA-admitted ownership feeds once, the
/// bare exec_sort drain serves all read-back) closed the letter to
/// B/A = 1.0072 PASS (pgrust-corpus-pairs-1784356345-4d78, bar <=1.02,
/// the same spelling SE8 refused at 1.027). The remaining +0.72% (the
/// RA-branch post-done per-pull exit ceremony, ~29 Ir/pull) is now
/// shaved by the SORTFEED-RA increment: `try_own_sort`'s refused-memo
/// branch checks `sort_Done` before the knob OnceLock + RA memo probe
/// (letter re-read in notes/sortfeed-ra-lane.md). Flips never
/// delete knobs (rowmode FLIP idiom; the AE2/K2 precedents).
fn sort_randomaccess_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(
            std::env::var("PGRUST_LANE_V2_SORT_RANDOMACCESS").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// Bare-hook randomAccess verdict, memoized on `SortState` (nodesort's
/// `lane_ra_fusible` — a SIDE memo: the chain-shared `lane_fusible` stays
/// `false` for randomAccess nodes, keeping every chain host on wave-7
/// behavior). The child cascade is `sort_refuse_reason`'s, verbatim
/// (`sort_child_refuse_reason`); refusals tick under SortFeed with the
/// child's reason (knob-ON only — the shared memo already ticked
/// RandomAccess once for the node, a documented knob-ON double-count on
/// refused nodes).
fn sort_randomaccess_memo<'mcx>(
    s: &mut crate::procnode::SortNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    debug_assert!(s.state.randomAccess && sort_randomaccess_enabled());
    if let Some(v) = ::nodesort::sort_lane_ra_fusible(&s.state) {
        return Ok(v);
    }
    let refuse = sort_child_refuse_reason(s, estate)?;
    if let Some(r) = refuse {
        stats::tick_refused(ShapeClass::SortFeed, r);
    }
    let v = refuse.is_none();
    ::nodesort::sort_lane_ra_fusible_set(&mut s.state, v);
    if v {
        lane_trace("sort randomAccess admitted (bare hook; read-back delegated)");
    }
    Ok(v)
}

// ===== end WS-AD wave-8 region (randomAccess admission) ====================

/// Feed phase of the sort breaker (pipeline N), once, lazily: drive the scan
/// pipeline to exhaustion into the breaker sink, then finalize (performsort)
/// — all inside one call, exactly like `exec_sort`'s build leg. `sort_Done`
/// is the phase flag; a rescan clears it and re-enters here. Shared by the
/// bare sort hook, the Limit/Unique-over-sort chains, and the wave-4 chains
/// over the sort breaker.
///
/// `Ok(false)` = feed-time refuse (only the agg-over-hash-join arm's
/// multi-batch spill, BEFORE the sort was touched or any owned tick fired):
/// the caller must refuse ownership; no lane tuple has been emitted and the
/// completed join build is byte-identical to the row path's, so the Volcano
/// fallback (`exec_sort` over the per-tuple agg over `exec_hash_join`)
/// resumes exactly.
fn sort_feed_if_needed<'mcx>(
    state: &mut ::nodesort::SortState<'mcx>,
    outer: &mut crate::procnode::PlanStateNode<'mcx>,
    outer_desc: &Option<std::rc::Rc<::types_tuple::TupleDescData<'static>>>,
    narrow: Option<usize>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if state.sort_done() {
        return Ok(true);
    }
    // WS-AD wave-8: narrowed comparators never pair with randomAccess (the
    // `sort_lane_begin_narrowed` invariant). Unreachable by construction —
    // `narrow` flows only from the sorted-agg chain hosts, whose shared
    // verdict refuses randomAccess wholesale.
    debug_assert!(narrow.is_none() || !state.randomAccess);
    // Hash-agg breaker child: build the agg FIRST (its own build-event tick
    // cadence), refusing before any sort-side effect on a multi-batch join
    // spill; then the agg's emit face feeds the breaker sink one finalized
    // group row per produce — exactly the row stream `exec_sort`'s feed loop
    // pulls from `exec_agg`, in C's retrieve order (per-row, matching the
    // per-tuple pull cadence: no staged batch exists over agg output).
    //
    // The vectorized topk_cut pre-filter never applies here (it runs over a
    // staged SoA key lane, which only scan feeds stage) — but the EMIT-side
    // boundary cut does: on the admitted `GROUP BY … ORDER BY count-agg
    // LIMIT k` shape, `topn_emit_arm` hoists the bounded sort's
    // compare-and-discard in front of each group's key reconstruction,
    // finalize, projection and tuple-form (see `sort_feed_agg_topn`).
    if let crate::procnode::PlanStateNode::Agg(aps) = outer {
        let aps = &mut **aps;
        // exec_agg's top-of-call guard: a drained agg stays drained (its
        // retrieve below yields EOF immediately — the empty feed C's
        // `exec_sort` would build from a drained child).
        if !::nodeagg::agg_is_done(&aps.agg) {
            let built = match &mut aps.outer {
                crate::procnode::PlanStateNode::SeqScan(ss) => {
                    let c = aps.lane_choice.expect("admission decided the agg lane choice");
                    // m3-sort-b car 1: the sort feed is the one chain that
                    // knows the bounded-sort consumer — resolve the runtime
                    // sink's combine-phase top-N spec pre-build (plan-shape
                    // reads only; declines arm nothing). WS-AD RA-vanilla
                    // feed law: never under randomAccess (a combine-phase
                    // top-N prunes rows a random-access replay must serve
                    // identically; the plain build + bounded tuplesort is
                    // `exec_sort`'s construction verbatim).
                    let sink_topn = if state.randomAccess {
                        None
                    } else {
                        sink_topn_arm(state, &aps.agg)
                    };
                    agg_seq_scan_build_if_needed(
                        &mut aps.agg,
                        ss,
                        c,
                        &mut aps.lane_stage_slot,
                        &mut aps.lane_exprkey,
                        sink_topn,
                        None,
                        estate,
                    )?;
                    true
                }
                crate::procnode::PlanStateNode::HashJoin(hj) => {
                    // SE-DECOROOT (CAR 1): the decorated grouped-join
                    // shape — offer the runtime grouped sink a FILL-ONLY
                    // engagement first (knob-gated OFF by default; the
                    // filled table drains through the identical emit paths
                    // below); a refusal falls to the serial join build
                    // byte-identically.
                    if runtime_hashjoin::try_fill_grouped_agg_over_join_runtime(
                        &mut aps.agg,
                        &mut **hj,
                        estate,
                    )? {
                        true
                    } else {
                        agg_hash_join_build_if_needed(
                            &mut aps.agg,
                            &mut **hj,
                            &mut aps.lane_stage_slot,
                            estate,
                        )?
                    }
                }
                crate::procnode::PlanStateNode::Gather(g) => {
                    agg_gather_build_if_needed(
                        &mut aps.agg,
                        &mut **g,
                        &mut aps.lane_stage_slot,
                        estate,
                    )?;
                    true
                }
                _ => unreachable!("agg_child_fusible admitted a non-lane agg feed"),
            };
            if !built {
                return Ok(false);
            }
        }
        stats::tick_owned(ShapeClass::SortFeed);
        let outer_desc = outer_desc.as_ref().expect("Sort already ended").clone();
        // Runtime-sink adopted emit (dop1-tax fix 4): the agg's build was
        // the parallel sink and it now holds published per-bucket EmitBufs
        // of finalized byval datums — drain them into the sort in
        // per-bucket BLOCKS (sort_lane_put_batch) instead of the per-row
        // cursor pull below. Same rows, same order, same slot contents;
        // only pull ceremony is hoisted (the DOP-1 tax ledger's ~35ms
        // EmitBuf drain line — the serial arm's own batchemit shortcut is
        // what the sink was forfeiting here). Kill switch: the same
        // PGRUST_LANE_V2_BATCHEMIT=0 layer as the compact batchemit.
        if batch_emit_enabled()
            && ::nodeagg::sink::agg_sink_emitting(&aps.agg)
            && ::nodeagg::sink::agg_sink_emit_unstarted(&aps.agg)
        {
            lane_trace("sink emit batched drain armed");
            sort_feed_sink_batched(state, &mut aps.agg, outer_desc, estate)?;
            return Ok(true);
        }
        // Batched finalize+emit off the compact table (lane-v2 batchemit):
        // resolved AFTER the build (the compact table must exist), composes
        // with the emit-side top-N boundary cut when that also arms. Non-
        // admission falls through to the per-row paths unchanged. WS-AD
        // RA-vanilla feed law: no emit-side top-N cut (and thereby no
        // topkfin selection — it keys off `spec`) under randomAccess; the
        // batched drains themselves stay (put-order-identical hoists into
        // the real tuplesort).
        let spec = if state.randomAccess { None } else { topn_emit_arm(state, &aps.agg) };
        let bplan = if batch_emit_enabled() {
            ::nodeagg::batch_emit_resolve(&aps.agg)
        } else {
            None
        };
        match (bplan, spec) {
            (Some(mut plan), spec) => {
                lane_trace("batch emit armed (compact finalize kernels)");
                // Top-k group selection before finalize/emit (lane-v2
                // topkfin, hot-c1): on the single-key bounded shape, pick the
                // k surviving groups on the RAW states and finalize/form only
                // those. Declines (Ok(false)) fall through to the batched
                // feed with nothing mutated.
                let owned = match spec {
                    Some(spec) if topkfin_admits(state) => sort_feed_agg_topk_finalize(
                        state,
                        &mut aps.agg,
                        outer_desc.clone(),
                        spec,
                        &mut plan,
                        estate,
                    )?,
                    _ => false,
                };
                if !owned {
                    sort_feed_agg_batched(state, &mut aps.agg, outer_desc, spec, plan, estate)?
                }
            }
            (None, Some(spec)) => {
                sort_feed_agg_topn(state, &mut aps.agg, outer_desc, spec, estate)?
            }
            (None, None) => sort_feed(
                state,
                &mut aps.agg,
                HashAggSource,
                HashAggEmit,
                outer_desc,
                None,
                estate,
                None,
                TieMode::Off,
            )?,
        }
        return Ok(true);
    }
    // One OWNED tick per lane-owned sort feed event (the gate's sortfeed
    // floor counts feeds, not calls).
    stats::tick_owned(ShapeClass::SortFeed);
    let outer_desc = outer_desc.as_ref().expect("Sort already ended").clone();
    match outer {
        crate::procnode::PlanStateNode::SeqScan(ss) => {
            arm_scan_staging(ss, estate, ScanFeedShape::RowFeed { ctx: "sort feed", stitch: true })?;
            // M3 runtime top-N sink (docs/design/m3-sort.md; PGRUST_RUNTIME=1
            // + pgrust.runtime_sort_pool + PGRUST_RUNTIME_SORT layering —
            // absent, one GUC read and today's serial path byte-identically).
            // Probed BEFORE the serial arms arm anything: true = the winners
            // are gathered and buffered, the refsort emit face is live;
            // false = refused or fell back with nothing consumed and no sort
            // state touched.
            if runtime_sort::try_own_sort(state, ss, &outer_desc, estate)? {
                return Ok(true);
            }
            // Zone-adaptive top-N granule order (pgrcolumnar bounded sorts; None
            // = physical order, exactly as before). Armed BEFORE topk_cut_arm
            // so both read the staged qual state the staging arm left.
            // WS-AD RA-vanilla feed law: never under randomAccess (adaptive
            // reorders arrival; its tie machinery is ratified for forward
            // LIMIT reads only) — physical order, exactly `exec_sort`'s feed.
            let adaptive = if state.randomAccess {
                None
            } else {
                adaptive_topk_arm(state, &outer_desc, ss)?
            };
            let tracked = adaptive.is_some_and(|a| a.tracked);
            // Payload-visible adaptive feeds, relaxed default: rule-2 rowref
            // selection (docs/conformance/tie-ordering.md) — the bounded
            // heap runs the (key, rowref) total order, pinning survivor
            // selection to the physical feed's first-arrived set by
            // construction, so no boundary tie can demote (the 100M sorted-limit-walk
            // cliff: at high per-key densities the LIMIT boundary always
            // ties and the old cut-selection demote re-fed the whole scan).
            // `=tracked` keeps the tie-tracking demote ladder (byte-exact
            // A/B channel).
            let mut tie = if !tracked {
                TieMode::Off
            } else if adaptive_topk_mode() == AdaptiveTopkMode::Relaxed {
                TieMode::Rowref
            } else {
                TieMode::Track
            };
            // Streaming top-k cutoff (bounded sorts over an admitted
            // qual-less seqscan; None = feed unfiltered, exactly as before).
            // Composes with the direct-key put: the keep-mask filters first,
            // then the direct-key arm reads only surviving rows. WS-AD
            // RA-vanilla feed law: never under randomAccess (inc-1
            // conservatism — the cut is content-exact for the bounded heap,
            // but the vanilla feed keeps the RA byte-identity proof
            // construction-verbatim).
            let topk = if state.randomAccess { None } else { topk_cut_arm(state, ss, estate) };
            // Refsort (late-materialization top-N): narrow (key, ref) feed +
            // winner-only gather; `None`/demote = the legacy wide feed below,
            // unchanged. The narrowed-comparator arm never composes (it
            // refused bounded sorts; refsort requires one).
            //
            // Composition ruling (train-10): rule-2 rowref selection owns the
            // relaxed default — its (key, rowref) bounded heap is selection-
            // exact with NO demote, while the refsort narrow sort resolves
            // full-key ties heap-arbitrarily and only has the tracked demote
            // ladder (a dense boundary tie would re-feed the whole scan: the
            // sorted-limit-walk@100M cliff class fix-100m-engagement retired). Refsort still
            // owns adaptive-off and tracked feeds (its ratified e2e arms).
            // LAZYTOPN (the chartered follow-up, delivered): under
            // `topn_lazyfetch_enabled` the narrow comparator EXTENDS with the
            // ref column (rule-2 (key, ref) total order — selection-exact,
            // demote-free, byte-identical to the wide rowref arm by
            // construction), reclaiming refsort under the relaxed default.
            // WS-AD RA-vanilla feed law: refsort never arms under
            // randomAccess (its winner buffer REPLACES the tuplesort
            // read-back face — `sort_lane_begin_refsort` asserts the
            // invariant; random-access reads must land on the tuplesort).
            let refsort = if !state.randomAccess
                && narrow.is_none()
                && (tie != TieMode::Rowref || topn_lazyfetch_enabled())
            {
                refsort_arm(state, ss, &outer_desc)
            } else {
                None
            };
            let mut fed = false;
            if let Some(spec) = &refsort {
                lane_trace("refsort armed (bounded sort late materialization)");
                if sort_feed_refsort(state, ss, &outer_desc, spec, topk, tie, estate)? {
                    fed = true;
                } else {
                    // Demoted before any output escaped: sticky-refuse the
                    // node, drop the narrow sort + winner buffer, disarm any
                    // adaptive order and rescan the child — then the legacy
                    // physical-order feed below reproduces the never-armed
                    // feed byte-for-byte (the adaptive demote pattern).
                    ::nodesort::sort_lane_refsort_refuse(state);
                    ::nodesort::sort_lane_reset_for_refeed(state);
                    ::nodeseqscan::seq_scan_adaptive_disarm_rescan(ss, estate)?;
                    tie = TieMode::Off;
                }
            }
            if !fed {
                let topk = if refsort.is_some() {
                    // The demote rescan restaged the scan; re-arm the cutoff
                    // against the fresh staging (the adaptive demote pattern).
                    topk_cut_arm(state, ss, estate)
                } else {
                    topk
                };
                sort_feed(
                    state,
                    ss,
                    SeqScanSource,
                    SeqScanFilterProject,
                    outer_desc.clone(),
                    narrow,
                    estate,
                    topk,
                    tie,
                )?;
            }
            // Tracked adaptive feed: an arrival-order-sensitive tie at the
            // LIMIT cut demotes — fresh tuplesort, adaptive disarmed, full
            // physical-order re-feed, reproducing the never-adaptive feed
            // byte-for-byte. Under the rowref mode only a rowref contract
            // break (reported as CutSelection) demotes; retained-tie emit
            // order is physical there and the ratified relaxation surface
            // (rule 3) covers it.
            let ambiguity = if tie != TieMode::Off {
                ::nodesort::sort_lane_topk_tie_ambiguity(state)
            } else {
                None
            };
            let ambiguity = match ambiguity {
                Some(::tuplesort::TopkTieAmbiguity::RetainedOrder)
                    if adaptive_topk_mode() == AdaptiveTopkMode::Relaxed =>
                {
                    stats::tick_adaptive_topk_tie_relaxed();
                    lane_trace("adaptive topk retained-tie order relaxed (rule 3)");
                    None
                }
                other => other,
            };
            if tie == TieMode::Rowref && ambiguity.is_none() {
                stats::tick_adaptive_topk_rowref_exact();
                lane_trace("adaptive topk rowref selection exact (rule 2)");
            }
            if let Some(kind) = ambiguity {
                stats::tick_adaptive_topk_demoted();
                lane_trace(match kind {
                    ::tuplesort::TopkTieAmbiguity::CutSelection => {
                        "adaptive topk demoted (cut-selection tie): physical re-feed"
                    }
                    ::tuplesort::TopkTieAmbiguity::RetainedOrder => {
                        "adaptive topk demoted (retained-tie order): physical re-feed"
                    }
                });
                ::nodesort::sort_lane_reset_for_refeed(state);
                ::nodeseqscan::seq_scan_adaptive_disarm_rescan(ss, estate)?;
                let topk = topk_cut_arm(state, ss, estate);
                sort_feed(
                    state,
                    ss,
                    SeqScanSource,
                    SeqScanFilterProject,
                    outer_desc,
                    narrow,
                    estate,
                    topk,
                    TieMode::Off,
                )?;
            }
        }
        crate::procnode::PlanStateNode::IndexScan(is) => sort_feed(
            state,
            is,
            IndexScanSource,
            IndexScanEmit,
            outer_desc,
            narrow,
            estate,
            None,
            TieMode::Off,
        )?,
        crate::procnode::PlanStateNode::IndexOnlyScan(ios) => sort_feed(
            state,
            &mut **ios,
            IndexOnlyScanSource,
            IndexOnlyScanEmit,
            outer_desc,
            narrow,
            estate,
            None,
            TieMode::Off,
        )?,
        crate::procnode::PlanStateNode::BitmapHeapScan(b) => {
            let b = &mut **b;
            // The bitmap must be built before the heap drive — the same
            // setup the bitmap arm runs before offering the scan.
            if !b.scan.initialized {
                crate::procnode::bitmap_table_scan_setup_dispatch(b, estate)?;
            }
            sort_feed(
                state,
                &mut b.scan,
                BitmapHeapScanSource,
                BitmapHeapScanEmit,
                outer_desc,
                narrow,
                estate,
                None,
                TieMode::Off,
            )?
        }
        // --- WS-Q wave-3 (contract §6.Q inc-final): T3 source-form feed
        // arms — the batch-size-1 pipeline over the delegated exec body,
        // drained into the breaker sink by the shared `sort_feed` (the
        // IndexScan-arm posture: no staging, no topk cut, no tie modes).
        crate::procnode::PlanStateNode::FunctionScan(fs) => tail_source::t3_sort_feed(
            state,
            &mut **fs,
            rowmode_tail::FunctionScanSource,
            outer_desc,
            narrow,
            estate,
        )?,
        crate::procnode::PlanStateNode::TableFuncScan(ts) => tail_source::t3_sort_feed(
            state,
            &mut **ts,
            rowmode_tail::TableFuncScanSource,
            outer_desc,
            narrow,
            estate,
        )?,
        crate::procnode::PlanStateNode::SampleScan(ss) => tail_source::t3_sort_feed(
            state,
            &mut **ss,
            rowmode_tail::SampleScanSource,
            outer_desc,
            narrow,
            estate,
        )?,
        crate::procnode::PlanStateNode::TidScan(ts) => tail_source::t3_sort_feed(
            state,
            ts,
            rowmode_tail::TidScanSource,
            outer_desc,
            narrow,
            estate,
        )?,
        crate::procnode::PlanStateNode::TidRangeScan(ts) => tail_source::t3_sort_feed(
            state,
            ts,
            rowmode_tail::TidRangeScanSource,
            outer_desc,
            narrow,
            estate,
        )?,
        crate::procnode::PlanStateNode::NamedTuplestoreScan(nts) => tail_source::t3_sort_feed(
            state,
            &mut **nts,
            rowmode_tail::NamedTuplestoreScanSource,
            outer_desc,
            narrow,
            estate,
        )?,
        _ => unreachable!("memoized sort verdict admitted a non-scan child"),
    }
    Ok(true)
}

/// Structural refuse-set for the sort breaker. Sort-side: refuse
/// `randomAccess` (EXEC_FLAG_REWIND/BACKWARD/MARK at init — scrollable and
/// backward cursors plus the mergejoin-outer mark/restore protocol need
/// tuplesort random access the forward-only emit pipeline doesn't drive);
/// bounded (top-N) IS admitted — `sort_lane_begin` applies
/// ALLOWBOUNDED/set_bound exactly as `exec_sort`. Child-side: the Phase-1
/// scan refuse-sets, verbatim (the feed is the Phase-1 scan pipeline with the
/// breaker as its sink) — these also cover EXPLAIN ANALYZE, since an
/// instrumented tree wraps every node in the `Instrumented` variant, which
/// matches no scan arm. The admitted checks are all init-stable, so the
/// verdict is memoizable; the caller re-checks the dynamic EPQ/direction
/// gates per call.
/// EA-on-morsels sort-side verdict (ea-morsels.md §5, E4): the serial
/// verdict with ONLY the instrument gates vacated — under EXPLAIN ANALYZE
/// the child is an `Instrumented` wrapper (peeled here for the check) and
/// the SeqScan refuse-set runs through `seq_scan_fusible_runtime_ea`. Every
/// other child kind refuses (the distinct runtime admits SeqScan children
/// only, and the serial arms that would own the rest cannot run under EA).
/// Touches neither the serial `lane_fusible` memo nor the stat counters.
#[cold]
fn sort_refuse_reason_runtime_ea<'mcx>(
    s: &mut crate::procnode::SortNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<RefuseReason>> {
    if s.state.randomAccess {
        return Ok(Some(RefuseReason::RandomAccess));
    }
    let child = match &mut *s.outer {
        crate::procnode::PlanStateNode::Instrumented(w) => &mut w.inner,
        o => o,
    };
    match child {
        crate::procnode::PlanStateNode::SeqScan(ss) => {
            Ok(if seq_scan_fusible_runtime_ea(ss, estate)? {
                None
            } else {
                Some(RefuseReason::ChildScanRefused)
            })
        }
        crate::procnode::PlanStateNode::Agg(_) => Ok(Some(RefuseReason::ChildNotLaneOwned)),
        _ => Ok(Some(RefuseReason::NonScanChild)),
    }
}

/// EXPLAIN (ENGINE) capture at the sort-breaker verdict: under ANALYZE the
/// child is an `Instrumented` wrapper, so the observed serial verdict is a
/// wrapper artifact (NonScanChild / ChildScanRefused via the child's
/// instrument gate). Report the production verdict through
/// `sort_refuse_reason_runtime_ea` instead (the E4 mirror; SeqScan children
/// get the full ignore-instrument refuse-set — non-SeqScan children keep a
/// conservative spine verdict in inc-1, ledgered).
#[cold]
fn engine_capture_sort_verdict<'mcx>(
    s: &mut crate::procnode::SortNode<'mcx>,
    observed: Option<RefuseReason>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let id = s.state.plan.plan.plan_node_id;
    let production = match observed {
        Some(RefuseReason::NonScanChild)
        | Some(RefuseReason::ChildScanRefused)
        | Some(RefuseReason::ChildNotLaneOwned)
        | Some(RefuseReason::Instrumented) => sort_refuse_reason_runtime_ea(s, estate)?,
        other => other,
    };
    engine_record_verdict(estate, id, ShapeClass::SortFeed, production);
    Ok(())
}

fn sort_refuse_reason<'mcx>(
    s: &mut crate::procnode::SortNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<RefuseReason>> {
    // The randomAccess POLICY line (SE-LETTERS §1/§4; wave-8 WS-AD): this
    // verdict is the one every chain host shares (Limit/Unique/Group/
    // Result/SubqueryScan/WindowAgg/sorted-agg over the breaker), and it
    // KEEPS refusing randomAccess wholesale — the chains' re-drive
    // disciplines over a rescan-preserved tuplesort are unaudited this
    // increment. The BARE sort hook alone re-checks under
    // `PGRUST_LANE_V2_SORT_RANDOMACCESS` (`sort_randomaccess_memo`): its
    // read-back delegates wholesale to the row-path Tuplesort, so
    // backward/rewind/mark-restore consumers are sound there by
    // construction.
    if s.state.randomAccess {
        return Ok(Some(RefuseReason::RandomAccess));
    }
    sort_child_refuse_reason(s, estate)
}

/// Child-side refuse-set of the sort breaker (`sort_refuse_reason` minus
/// the randomAccess policy line — split so the WS-AD bare-hook randomAccess
/// verdict runs the IDENTICAL child cascade). Behavior verbatim.
fn sort_child_refuse_reason<'mcx>(
    s: &mut crate::procnode::SortNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<RefuseReason>> {
    // Hash-agg BREAKER child (the final `ORDER BY agg ... LIMIT k` tail over
    // aggregate output): the agg's Source face (its retrieve/emit) feeds the
    // sort breaker exactly as a scan source would — breaker-composes-breaker,
    // the `try_own_agg_over_hash_join` precedent. Admission is the Limit
    // chain's exact agg-child gate (`agg_child_fusible`: the agg-side breaker
    // gate × the admitted feed children × the economics memo), so the sort
    // admits precisely where a Limit-over-agg chain would. All the admitted
    // checks are init-stable or child-memoized, keeping this verdict
    // memoizable like the scan arms'.
    if let crate::procnode::PlanStateNode::Agg(aps) = &mut *s.outer {
        return Ok(if agg_child_fusible(aps, estate)? {
            None
        } else {
            Some(RefuseReason::ChildNotLaneOwned)
        });
    }
    // --- WS-Q wave-3 (contract §6.Q inc-final): T3 source-form children.
    // The six tail leaf shapes admit as sort children when
    // `PGRUST_LANE_V2_SCANS_T3` arms them (init-stable: node type +
    // process-static knobs, so the caller's memo is sound; the child's
    // OWNED tick fires inside the admit at this verdict chokepoint).
    // Because every host composing over the sort breaker consumes THIS
    // memoized verdict (bare Sort, Limit/Unique-over-sort, the wave-4
    // Group/Result/SubqueryScan glue), this one arm retires their
    // `child-not-lane-owned` cascades for T3 shapes knob-ON. Knob-OFF: one
    // cached byte load, then the unchanged `scan_child_fusible` verdict.
    if tail_source::t3_sort_child_admit(&s.outer) {
        return Ok(None);
    }
    scan_child_fusible(&mut s.outer, estate)
}

/// Shared child-side gate for breakers fed by a Phase-1 scan pipeline (sort
/// and hash-join build/probe feeds): the Phase-1 scan refuse-sets, verbatim.
/// `None` = admitted; `Some(NonScanChild)` = not a lane-fusible scan node
/// type; `Some(ChildScanRefused)` = the child scan's own refuse-set refused
/// (the specific reason is ticked under the child's class inside its fusible
/// gate). These also cover EXPLAIN ANALYZE (an instrumented tree wraps every
/// node in the `Instrumented` variant, which matches no scan arm).
fn scan_child_fusible<'mcx>(
    child: &mut crate::procnode::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<RefuseReason>> {
    let child_ok = match child {
        crate::procnode::PlanStateNode::SeqScan(ss) => seq_scan_fusible(ss, estate)?,
        crate::procnode::PlanStateNode::IndexScan(is) => index_scan_fusible(is, estate),
        crate::procnode::PlanStateNode::IndexOnlyScan(ios) => {
            index_only_scan_fusible(ios, estate)
        }
        crate::procnode::PlanStateNode::BitmapHeapScan(b) => {
            bitmap_heap_scan_fusible(&b.scan, estate)
        }
        _ => return Ok(Some(RefuseReason::NonScanChild)),
    };
    Ok(if child_ok {
        None
    } else {
        Some(RefuseReason::ChildScanRefused)
    })
}

/// Feed phase driver: build the tuplesort (`sort_lane_begin` — `exec_sort`'s
/// construction verbatim), then run pipeline N to exhaustion into the breaker
/// sink. Mirrors `exec_sort`'s build leg in forcing a forward child read for
/// the feed's duration (restored on success; an error aborts the query).
/// `narrow_keys`: `Some(k)` = the grouped exact-DISTINCT order-relaxation
/// arm — begin the tuplesort with only the first `k` sort keys
/// (`sort_lane_begin_narrowed`; the caller proved the dropped suffix is
/// observation-free). `None` = `exec_sort`'s construction verbatim.
fn sort_feed<'mcx, S, O>(
    sort: &mut ::nodesort::SortState<'mcx>,
    scan: &mut S::Node,
    mut src: S,
    mut op: O,
    outer_desc: std::rc::Rc<::types_tuple::TupleDescData<'static>>,
    narrow_keys: Option<usize>,
    estate: &mut EStateData<'mcx>,
    topk: Option<TopkCut>,
    tie: TieMode,
) -> PgResult<()>
where
    S: Source<'mcx>,
    O: Operator<'mcx, Node = S::Node>,
{
    match narrow_keys {
        Some(k) => ::nodesort::sort_lane_begin_narrowed(sort, outer_desc, k)?,
        None => ::nodesort::sort_lane_begin(sort, outer_desc)?,
    }
    match tie {
        // Zone-adaptive tracked mode: record boundary-tie events so the
        // caller can demote before any output escapes (see the adaptive
        // block above).
        TieMode::Track => ::nodesort::sort_lane_topk_tie_track_arm(sort),
        // Rule-2 rowref selection: (key, rowref) bounded-heap total order;
        // the feed threads per-row physical rowrefs to the puts below.
        TieMode::Rowref => ::nodesort::sort_lane_topk_rowref_arm(sort),
        TieMode::Off => {}
    }
    // Direct sort-key feed (`exec_sort_batched`'s `key_direct` probe,
    // mirrored): probed once per feed, BEFORE the first `produce` (arming
    // decides what the staging pass stages), datum sorts only — exactly the
    // incumbent's probe placement inside its `node.datumSort` arm.
    let key_direct = ::nodesort::sort_lane_is_datum(sort) && op.arm_sort_key(scan, estate);
    let dir = estate.es_direction;
    estate.es_direction = ::types_scan::sdir::ForwardScanDirection;
    let mut sink = SortBreakerSink {
        sort,
        key_direct,
        topk: topk.map(TopkCutState::new),
        rowref: tie == TieMode::Rowref,
    };
    drain_pipeline(scan, &mut src, &mut op, &mut sink, estate)?;
    estate.es_direction = dir;
    Ok(())
}

/// Boundary-tie handling for the lane's bounded sort feed (zone-adaptive
/// arrival orders only; `Off` for physical-order and ties-invisible feeds).
#[derive(Clone, Copy, PartialEq, Eq)]
enum TieMode {
    /// Nothing to arm: physical-order feed, or every visible byte is a
    /// byte-equality sort key (ties invisible).
    Off,
    /// Boundary-tie tracking; the caller demotes on an observed trigger
    /// (the byte-exact `tracked` channel).
    Track,
    /// Rule-2 rowref selection (docs/conformance/tie-ordering.md): the
    /// bounded heap runs the (key, rowref) total order, so survivor
    /// selection equals the physical feed's by construction and retained
    /// ties emit in physical order; only a rowref contract break demotes.
    Rowref,
}

// ===========================================================================
// Streaming top-k cutoff on the sort-breaker feed (pgrcolumnar-v2 plan §2.8;
// ClickHouse PartialSortingTransform's threshold filter, our shape). For a
// bounded (top-N) sort, once the tuplesort's bounded heap is FULL every
// further put is compare-against-the-k-th-boundary-and-usually-discard. The
// pre-filter hoists that discard in front of the breaker: each staged batch
// is compared VECTORIZED (the existing `qual_bitmap_cmp_const` kernel, with
// the tuplesort's live k-th boundary datum as the "const") against the
// staged leading-key lane, and rows that cannot make the top k are skipped
// without an emit or a tuplesort put.
//
// CORRECTNESS INVARIANT (the proof the admission rules exist to keep): the
// pre-filter may skip EXACTLY rows the tuplesort itself would discard with
// no observable side effect. Piecewise:
//   * A bounded tuplesort in TSS_BOUNDED discards an incoming tuple iff
//     full_cmp(tuple, root) >= 0, where `root` (the bounded heap's top under
//     the reversed comparator) is the current WORST surviving member and
//     `full_cmp` is the full multi-key comparator (tuplesort.rs
//     `puttuple_bounded`).
//   * The pre-filter discards row R iff R's LEADING key is STRICTLY worse
//     than the boundary's leading key: `keep = R.k1 <op-order> boundary.k1
//     OR ties` — implemented as the non-strict keep compare (ASC keeps
//     `k1 <= b`, DESC keeps `k1 >= b`). Strictly-worse on the leading key
//     forces full_cmp(R, root) > 0 regardless of later keys — the multi-key
//     comparator is lexicographic — so every skipped row is a row tuplesort
//     would discard. Leading-key TIES ALWAYS PASS (they can still win on
//     later keys); equal-or-better rows always pass. Datum-sort ties also
//     pass and the tuplesort re-judges them — a pure subset, never a
//     different verdict.
//   * NULL leading keys are never pre-filtered (the keep mask ORs the
//     lane's null bits): a NULL's rank depends on NULLS FIRST/LAST, and the
//     tuplesort's own comparator is the authority. A NULL boundary disables
//     the batch's pre-filter entirely (nothing compares strictly-worse
//     against NULL through the kernel; conservative pass-through).
//   * Deform-fallback rows (no staged lane value) always pass.
//   * The boundary only TIGHTENS as puts replace the root (the reversed
//     heap's top is monotonically non-worsening in forward order), so the
//     once-per-batch boundary snapshot is stale-but-conservative: it only
//     lets through rows the tuplesort then judges itself.
//   * Skipping a row skips its emit body, so admission requires the emit to
//     be observation-free per row: NO scan qual (a qual evaluation C would
//     have run — including its possible error — must not be elided) and
//     only pure-Var projections (the single Var-copy kernel or the all-Var
//     census list — never a computing column). Under that shape a
//     skipped row's only C-side effects were the tuplesort compare+discard
//     (and its per-row CHECK_FOR_INTERRUPTS; the filtered path keeps one
//     CFI per staged batch, the lane's page-level cadence floor).
//   * By-value leading keys only (the CmpOp kernel families): the boundary
//     datum read from the heap root must not dangle when a later put in the
//     same batch evicts the root's tuple.
// Net: the same rows reach the tuplesort as would have survived its own
// bounded discards, in the same order, and the sorted output is
// byte-identical. This is a pure skip optimization with zero refusal
// surface — non-admission simply feeds the sort unfiltered, exactly as
// before.
// ===========================================================================

/// Armed pre-filter spec: the vectorized KEEP comparison (`key <= boundary`
/// for ASC / `key >= boundary` for DESC, in the leading key's kernel
/// family). Rows failing it (non-null, staged) are strictly worse than the
/// k-th boundary on the leading key and are skipped.
#[derive(Clone, Copy)]
struct TopkCut {
    keep: ::execexpr::CmpOp,
}

/// Map the sort's leading-key ORDER operator (the `<` or `>` operator's
/// kernel image) to the non-strict KEEP compare of the same family. `None`
/// refuses: cross-width families never appear as sort operators (both sides
/// are the key's own type) and everything else is outside the kernel
/// vocabulary.
fn topk_keep_op(cmp: ::execexpr::CmpOp) -> Option<::execexpr::CmpOp> {
    use ::execexpr::CmpOp::*;
    Some(match cmp {
        Int2Lt => Int2Le,
        Int2Gt => Int2Ge,
        Int4Lt => Int4Le,
        Int4Gt => Int4Ge,
        Int8Lt => Int8Le,
        Int8Gt => Int8Ge,
        OidLt => OidLe,
        OidGt => OidGe,
        Float4Lt => Float4Le,
        Float4Gt => Float4Ge,
        Float8Lt => Float8Le,
        Float8Gt => Float8Ge,
        _ => return None,
    })
}

/// Resolve the sort's leading INPUT column (1-based over the scan's output)
/// to a scan attnum (0-based), through an observation-free projection only:
/// no projection, the lone `JustAssignVar` Var-copy, or an all-Var census
/// projection with no arith columns. `None` = not resolvable under those
/// shapes (computing projections are refused: a row skipped by a top-k
/// pre-filter or a zone-adaptive granule skip elides its projection, and
/// only Var passthroughs are guaranteed observation-free — an elided arith
/// evaluation could elide C's error).
fn sort_leading_key_scan_attnum<'mcx>(
    state: &::nodesort::SortState<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
) -> Option<u16> {
    let plan = state.plan;
    if plan.numCols < 1 || plan.sortColIdx.is_empty() {
        return None;
    }
    let oc = plan.sortColIdx[0];
    if oc < 1 {
        return None;
    }
    match ss.ss.ps_ProjInfo.as_ref() {
        None => Some((oc - 1) as u16),
        Some(p) => match p.pi_state.kernel() {
            ::execexpr::Kernel::JustAssignVar {
                src: ::execexpr::SlotSrc::Scan,
                attnum,
                resultnum: 0,
            } if oc == 1 => Some(attnum),
            _ => {
                // Multi-column projections admit only the pure Var-copy list
                // (the ready-time scan-projection census, subplan/param-free
                // by construction, with NO arith columns). The sort's leading
                // input column then maps through the census to its scan
                // attnum.
                let cols = p.pi_state.scan_proj_cols()?;
                if cols.any_arith() || (oc as usize) > cols.n as usize {
                    return None;
                }
                match cols.cols[(oc - 1) as usize] {
                    ::execexpr::ScanProjCol::Var { attnum } => Some(attnum),
                    _ => None,
                }
            }
        },
    }
}

/// Admission + arming for the top-k cutoff over a seqscan-fed bounded sort.
/// `None` = not admitted; the feed runs unfiltered (never a lane refusal).
/// Admits: bounded sort; leading sort key resolvable to a scan column (no
/// projection, the lone `JustAssignVar` Var-copy, or an all-Var census
/// projection); NO scan qual (skipped
/// rows must have no observable per-row evaluation — see the invariant
/// block); leading-key order operator inside the by-value kernel compare
/// vocabulary (int2/4/8, oid, float4/8; ASC and DESC, any NULLS placement);
/// and the key column stageable by the fixed-width SoA prefix deform.
fn topk_cut_arm<'mcx>(
    state: &::nodesort::SortState<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> Option<TopkCut> {
    if !state.bounded {
        return None;
    }
    let attnum = sort_leading_key_scan_attnum(state, ss)?;
    // Kernel admission: order operator -> its comparison-function kernel ->
    // the keep compare. `get_opcode` is one syscache read per feed.
    let opfn = ::lsyscache::get_opcode(state.plan.sortOperators[0]).ok()?;
    let keep = topk_keep_op(::execexpr::CmpOp::for_fn_oid(opfn)?)?;
    // Key-lane staging (refuses qual-bearing scans and foreign SoA arming).
    if !::nodeseqscan::seq_scan_topk_key_arm(ss, estate, attnum) {
        return None;
    }
    stats::tick_owned(ShapeClass::TopkCut);
    lane_trace("topk cutoff armed (sort feed)");
    Some(TopkCut { keep })
}

// ===========================================================================
// Zone-ordered adaptive top-N traversal (pgrcolumnar; pgrcolumnar-v2 plan, design in
// docs/design/pgrcolumnar-zone-adaptive.md). For `ORDER BY x LIMIT k` over a
// pgrcolumnar scan, the granule directory's footer min/max gives a partial order
// on x: visiting granules best-first (zone min ascending for ASC / max
// descending for DESC) and feeding the bounded sort's k-th boundary back to
// the scan lets the scan STOP at the first granule whose bound the boundary
// strictly dominates — every remaining granule is at least as dominated
// (ClickHouse-style read-in-order early termination, but zone-map-driven, so
// it works on non-cluster-key columns).
//
// CORRECTNESS: a bound-skipped granule contains only rows STRICTLY worse
// than the current boundary on the LEADING key — rows the bounded tuplesort
// would discard with no observable side effect (topkcut's invariant, granule-
// granular; equality never skips, `strict=false` at the AM). Observation-
// freedom of the elided per-row work is the arm's admission: pure-Var
// projections (the shared resolution) and no qual / whole-qual staged
// kernels (non-erroring vocabularies; `seq_scan_adaptive_topk_arm`).
//
// TIE EXACTNESS is the residual risk: the adaptive order changes ARRIVAL
// order at the bounded heap, and both the survivor selection at a boundary
// full-key tie and the emit order among retained full-key ties are arrival-
// dependent (heap-shape effects). Shape/mode ladder:
//   * ties-invisible (every non-junk output column IS a sort key column of
//     a byte-equality type): any legal tie selection/order prints identical
//     bytes — nothing to track.
//   * payload-visible, relaxed (DEFAULT; ratified tie-ordering rule 3): the
//     tuplesort's tie tracking (armed via `sort_lane_topk_tie_track_arm`)
//     still runs, and a CUT-SELECTION trigger (which rows made the LIMIT
//     cut is arrival-dependent) DEMOTES — fresh tuplesort, adaptive
//     disarmed, full physical-order re-feed — so the SELECTED SET is always
//     the physical-order feed's. A RETAINED-ORDER trigger (same rows,
//     arrival-dependent order within equal-full-key groups) is accepted:
//     within-tie-group order is not a compatibility surface.
//   * payload-visible, `=tracked`: EITHER trigger demotes — byte-identical
//     to lane-off, the experiment/A-B channel.
// Net: lane-on output is byte-identical to lane-off except, in relaxed
// mode, for the order within equal-full-key tie groups of the emitted rows
// (tie-normalizing gates cover that channel).
// ===========================================================================

/// Armed adaptive top-N traversal for the current sort feed. `tracked` =
/// boundary-tie tracking armed (some visible output byte is not determined
/// by the sort keys; the feed demotes on an observed ambiguous tie).
#[derive(Clone, Copy)]
struct AdaptiveTopk {
    tracked: bool,
}

/// Adaptive top-N modes for `PGRUST_LANE_ADAPTIVE_TOPK` (resolved once).
#[derive(Clone, Copy, PartialEq, Eq)]
enum AdaptiveTopkMode {
    /// `=0|off`: never arm (byte-identical A/B gate channel).
    Off,
    /// `=invisible`: arm only ties-invisible shapes (every non-junk output
    /// column is a byte-equality sort key — any tie handling prints
    /// identical bytes, so the walk can never demote and never loses).
    /// The pre-relaxation default, kept as an A/B channel.
    InvisibleOnly,
    /// Default (ratified 2026-07-12, tie-ordering rule 3): additionally arm
    /// payload-visible shapes with tie tracking, but demote ONLY on
    /// cut-selection ambiguity — retained-tie emit order is accepted as-is
    /// (the ratified relaxation surface: same selected rows, possibly
    /// different order within equal-full-key groups). Sorted-limit-walk shapes stop
    /// demoting (their boundary tie was pure retained order); the exactness
    /// backstop for WHICH rows are returned stays. The AM-side probe budget
    /// covers the take-k-sorted-class sparse-qual degeneration.
    Relaxed,
    /// `=tracked`: payload-visible shapes demote on EITHER trigger
    /// (retained-tie order included) — the byte-exact experiment channel.
    Tracked,
}

fn adaptive_topk_mode() -> AdaptiveTopkMode {
    static MODE: std::sync::OnceLock<AdaptiveTopkMode> = std::sync::OnceLock::new();
    crate::once_val(&MODE, || match std::env::var("PGRUST_LANE_ADAPTIVE_TOPK") {
        Ok(v) if v == "0" || v.eq_ignore_ascii_case("off") => AdaptiveTopkMode::Off,
        Ok(v) if v.eq_ignore_ascii_case("tracked") => AdaptiveTopkMode::Tracked,
        Ok(v) if v.eq_ignore_ascii_case("invisible") => AdaptiveTopkMode::InvisibleOnly,
        _ => AdaptiveTopkMode::Relaxed,
    })
}

/// Bound cap: beyond this a top-N is scan-shaped anyway (the early-stop
/// upside shrinks as k grows) and the demotion re-feed risk isn't worth it.
const ADAPTIVE_TOPK_MAX_BOUND: i64 = 1 << 16;

/// Admission + arming for the zone-adaptive traversal over a seqscan-fed
/// bounded sort. `None` = not armed (never a lane refusal — the feed runs in
/// physical order exactly as before). Admits: bounded sort with a sane
/// bound; leading sort key resolvable to a scan column through an
/// observation-free projection (the shared topk-cut resolution); leading
/// order operator in the int-family kernel vocabulary (maps ASC/DESC; float
/// and cross-width never appear as sort operators on admitted columns);
/// scan-side qual observation-freedom plus the AM's own gates (pgrcolumnar,
/// serial, exact int-family zone entries) inside
/// `seq_scan_adaptive_topk_arm`.
fn adaptive_topk_arm<'mcx>(
    state: &::nodesort::SortState<'mcx>,
    outer_desc: &::types_tuple::TupleDescData<'static>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
) -> PgResult<Option<AdaptiveTopk>> {
    let mode = adaptive_topk_mode();
    if mode == AdaptiveTopkMode::Off {
        return Ok(None);
    }
    if !state.bounded || state.bound <= 0 || state.bound > ADAPTIVE_TOPK_MAX_BOUND {
        return Ok(None);
    }
    let Some(attnum) = sort_leading_key_scan_attnum(state, ss) else {
        return Ok(None);
    };
    let Ok(opfn) = ::lsyscache::get_opcode(state.plan.sortOperators[0]) else {
        return Ok(None);
    };
    use ::execexpr::CmpOp::*;
    let desc = match ::execexpr::CmpOp::for_fn_oid(opfn) {
        Some(Int2Lt | Int4Lt | Int8Lt | OidLt) => false,
        Some(Int2Gt | Int4Gt | Int8Gt | OidGt) => true,
        _ => return Ok(None),
    };
    let tracked = !sort_topk_ties_invisible(state, outer_desc);
    if tracked && mode == AdaptiveTopkMode::InvisibleOnly {
        return Ok(None);
    }
    if !::nodeseqscan::seq_scan_adaptive_topk_arm(ss, attnum, desc)? {
        return Ok(None);
    }
    stats::tick_owned(ShapeClass::AdaptiveTopk);
    lane_trace(match (tracked, mode) {
        (false, _) => "adaptive topk armed (sort feed, ties invisible)",
        (true, AdaptiveTopkMode::Relaxed) => {
            "adaptive topk armed (sort feed, rowref selection + relaxed tie order)"
        }
        (true, _) => "adaptive topk armed (sort feed, tie-tracked)",
    });
    Ok(Some(AdaptiveTopk { tracked }))
}

/// True when every visible output byte is determined by the sort keys:
/// every NON-JUNK targetlist column is itself a sort key column, and every
/// sort key's comparator equality implies byte equality of the keyed column
/// (by-value int-family types; text/varchar only under the C collation,
/// where the comparator is memcmp+len). Under that shape any legal
/// selection/order of a full-key tie group prints identical bytes, so the
/// adaptive feed needs no tie tracking.
fn sort_topk_ties_invisible(
    state: &::nodesort::SortState<'_>,
    outer_desc: &::types_tuple::TupleDescData<'static>,
) -> bool {
    let plan = state.plan;
    let nkeys = plan.numCols as usize;
    if plan.sortColIdx.len() < nkeys || plan.collations.len() < nkeys {
        return false;
    }
    let keys = &plan.sortColIdx[..nkeys];
    for tle in plan.plan.targetlist.iter() {
        let Some(te) = tle.as_target_entry() else {
            return false;
        };
        if !te.resjunk && !keys.contains(&te.resno) {
            return false;
        }
    }
    for (i, &k) in keys.iter().enumerate() {
        if k < 1 || k as usize > outer_desc.natts as usize {
            return false;
        }
        use ::types_core::catalog::{
            BOOLOID, DATEOID, INT2OID, INT4OID, INT8OID, OIDOID, TEXTOID, TIMESTAMPOID,
            TIMESTAMPTZOID, VARCHAROID,
        };
        let byte_eq = match outer_desc.attr((k - 1) as usize).atttypid {
            INT2OID | INT4OID | INT8OID | OIDOID | BOOLOID | DATEOID | TIMESTAMPOID
            | TIMESTAMPTZOID => true,
            // Deterministic collations only compare equal on identical bytes
            // (C's varstr_cmp strcmp tiebreak, kept by the ported comparator
            // and the varstr_cmp_locale seam); this resolves the DEFAULT
            // collation through the database locale (the analytics banks are
            // initdb'd --no-locale with no per-column COLLATE).
            TEXTOID | VARCHAROID => {
                ::varlena::text_collation_is_raw_bytes(plan.collations[i]).unwrap_or(false)
            }
            _ => false,
        };
        if !byte_eq {
            return false;
        }
    }
    true
}

/// Compute the keep mask for one staged batch, or `None` when the pre-filter
/// is not engaged for this batch (heap not yet full, NULL boundary, or no
/// staged key lane). Bits: `keep = (!isnull && key KEEP-cmp boundary) ||
/// isnull || fallback` over staged rows `0..n`; bits at `n..` are garbage
/// and never consulted.
fn topk_keep_mask<'mcx, E: BatchEmit<'mcx>>(
    cut: TopkCut,
    sort: &::nodesort::SortState<'mcx>,
    emit: &E,
    n: u32,
) -> Option<[u64; ::exectuples::SOA_BM_WORDS]> {
    let (boundary, bnull) = ::nodesort::sort_lane_topk_boundary(sort)?;
    if bnull {
        return None;
    }
    let (values, isnull, fallback) = emit.topk_key_lane(n)?;
    debug_assert!(values.len() == n as usize && isnull.len() == n as usize);
    let mut sel = [0u64; ::exectuples::SOA_BM_WORDS];
    ::execexpr::qual_bitmap_cmp_const(cut.keep, boundary, values, isnull, &mut sel);
    // NULL keys and deform-fallback rows always pass through to the
    // tuplesort's own comparator.
    for (w, (nch, fb)) in isnull.chunks(64).zip(fallback).enumerate() {
        let mut nulls = 0u64;
        for (j, &isn) in nch.iter().enumerate() {
            nulls |= (isn as u64) << j;
        }
        sel[w] |= nulls | fb;
    }
    Some(sel)
}

// ===========================================================================
// Emit-side top-N boundary cut on the hash-agg-fed sort breaker (lane-v2
// topnemit; the emit-side complement of the scan-level topk_cut above). The
// `GROUP BY keys ORDER BY count-agg DESC LIMIT k` tail (the grouped-count
// top-n family class) today EMITS EVERY GROUP — key reconstruction, finalize,
// projection, minimal-tuple form, sort put — into a bounded sort that keeps
// k. Once the bounded heap is full, each further put is compare-against-the-
// k-th-boundary-and-usually-discard; this arm hoists that compare all the
// way into the agg retrieve, in front of the WHOLE per-group emit body.
//
// CORRECTNESS INVARIANT (same family as topk_cut's, tie-relaxation-free):
//   * The retrieve skips group G iff G's leading-key value is STRICTLY worse
//     than the bounded heap root's leading key (heap FULL — `topk_boundary`
//     returns None otherwise, disabling the cut). A strictly-worse leading
//     key forces full_cmp(G, root) > 0 (lexicographic), so the tuplesort
//     would discard G with NO state change (`puttuple_bounded`'s compare<=0
//     arm frees the tuple and returns). Removing exactly no-state-change
//     puts leaves every heap transition, every tie selection, and the
//     surviving arrival ORDER identical — the sorted output is
//     byte-identical BY CONSTRUCTION, with no reliance on the ratified
//     tie-order relaxation. Leading-key ties and better keys always pass;
//     NULL/pending transvalues always pass (rank depends on NULLS placement;
//     the tuplesort's comparator stays the authority).
//   * The compared value is the group's RAW int8 transvalue; admission
//     (`topn_emit_resolve`) proves it IS the finalized, projected sort-key
//     datum: finalfn-none int8-byval aggregate (count(*)/count(x)/sum-int
//     family) projected as a bare tlist Aggref. The boundary datum is the
//     same column's datum1 in the heap root — an i64/i64 compare in the
//     leading order operator's own direction (Int8Lt/Int8Gt kernel families
//     only), matching btint8cmp exactly.
//   * Skipping a group elides its whole emit body, so admission requires it
//     observation-free: no HAVING qual, every other tlist entry a bare
//     Var/Const/Aggref, and every skipped finalfn in the pure-arithmetic
//     allowlist (`TOPN_SKIPPABLE_FINALFNS`) — nothing C could observably do
//     (no reachable error, no side effect) is elided. Skipped groups keep a
//     per-group CHECK_FOR_INTERRUPTS (the elided sort put's cadence).
//   * The boundary only TIGHTENS as puts replace the root, and it is
//     re-read from the live heap before every retrieve call; within one
//     call's skip run no puts happen, so the held boundary is
//     stale-but-conservative — it only lets through groups the tuplesort
//     then judges itself.
// Net: a pure skip optimization with zero refusal surface — non-admission
// feeds the sort through the unfiltered breaker path, exactly as before.
// Kill switch: PGRUST_LANE_V2_TOPNEMIT=0.
// ===========================================================================

/// `PGRUST_LANE_V2_TOPNEMIT` kill switch (default ON inside the lane).
fn topn_emit_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_LANE_V2_TOPNEMIT").as_deref(), Ok("0") | Ok("off"))
    })
}

/// `PGRUST_RUNTIME_AGG_TOPN` kill switch (default ON — the composed
/// combine-phase top-N only ever engages inside an engaged runtime agg
/// sink, which has its own arming ladder).
fn sink_topn_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_RUNTIME_AGG_TOPN").as_deref(), Ok("0") | Ok("off"))
    })
}

/// m3-sort-b car 1 — arm the RUNTIME agg sink's combine-phase top-N: the
/// bounded-sort consumer's shape, resolved PRE-BUILD at the sort feed's
/// engagement seam (plan reads only; `topn_emit_resolve` never touches
/// build state). Same admission vocabulary as `topn_emit_arm` (single
/// int8-kernel order column over a raw finalfn-none int8 Aggref) plus the
/// bound cap (`SINK_TOPN_MAX_BOUND` — the serial lanes' cap agreement).
/// `None` = not admitted: the sink runs with the plain full drain — never
/// a refusal, never a plan change.
fn sink_topn_arm<'mcx>(
    state: &::nodesort::SortState<'mcx>,
    agg: &::nodeagg::AggStateData<'mcx>,
) -> Option<::nodeagg::sink::SinkTopnSpec> {
    let decline = |why: &str| {
        lane_trace(&format!("runtime-agg topn: declined ({why})"));
    };
    if !sink_topn_enabled() {
        return None;
    }
    if !state.bounded {
        decline("unbounded");
        return None;
    }
    let Some(bound) = u32::try_from(state.bound).ok().filter(|&b| b > 0) else {
        decline("bound range");
        return None;
    };
    if bound > ::nodeagg::sink::SINK_TOPN_MAX_BOUND {
        decline("bound cap");
        return None;
    }
    let plan = state.plan;
    // Single-column ORDER BY only: the boundary's tie-break must not need
    // secondary keys (the topkfin admission, verbatim).
    if plan.numCols != 1 || plan.sortColIdx.is_empty() {
        decline("multi-column order");
        return None;
    }
    let oc = plan.sortColIdx[0];
    if oc < 1 {
        decline("order column resno");
        return None;
    }
    let Some(opfn) = ::lsyscache::get_opcode(plan.sortOperators[0]).ok() else {
        decline("order operator opcode");
        return None;
    };
    let desc = match ::execexpr::CmpOp::for_fn_oid(opfn) {
        Some(::execexpr::CmpOp::Int8Gt) => true,
        Some(::execexpr::CmpOp::Int8Lt) => false,
        _ => {
            decline("order operator kernel");
            return None;
        }
    };
    let Some(transno) = ::nodeagg::topn_emit_resolve(agg, oc) else {
        decline("order column resolve");
        return None;
    };
    lane_trace(&format!("runtime-agg topn: armed (bound={bound})"));
    Some(::nodeagg::sink::SinkTopnSpec { transno, desc, bound })
}

/// Admission + arming for the emit-side top-N boundary cut (invariant block
/// above). `None` = not admitted; the feed runs through the unchanged
/// breaker path (never a lane refusal). Sort side: bounded, leading order
/// operator in the int8 kernel family. Agg side: `topn_emit_resolve` (bare
/// finalfn-none int8 Aggref sort key; whole emit body observation-free).
fn topn_emit_arm<'mcx>(
    state: &::nodesort::SortState<'mcx>,
    agg: &::nodeagg::AggStateData<'mcx>,
) -> Option<::nodeagg::TopnEmitSpec> {
    if !topn_emit_enabled() || !state.bounded {
        return None;
    }
    let plan = state.plan;
    if plan.numCols < 1 || plan.sortColIdx.is_empty() {
        return None;
    }
    let oc = plan.sortColIdx[0];
    if oc < 1 {
        return None;
    }
    // The leading order operator's compare kernel fixes both the key type
    // (int8 — the resolve below re-proves it on the agg) and the direction.
    let opfn = ::lsyscache::get_opcode(plan.sortOperators[0]).ok()?;
    let desc = match ::execexpr::CmpOp::for_fn_oid(opfn)? {
        ::execexpr::CmpOp::Int8Gt => true,
        ::execexpr::CmpOp::Int8Lt => false,
        _ => return None,
    };
    let transno = ::nodeagg::topn_emit_resolve(agg, oc)?;
    stats::tick_owned(ShapeClass::TopnEmit);
    lane_trace("topn emit boundary armed (agg sort feed)");
    Some(::nodeagg::TopnEmitSpec { transno, desc })
}

/// The armed agg→sort feed: `sort_feed`'s begin/finish frame around a
/// per-group pull loop that re-reads the tuplesort's live k-th boundary
/// before every retrieve (`sort_lane_topk_boundary` — None until the bounded
/// heap fills, disabling the cut) and hands it to the agg's retrieve, which
/// skips boundary-rejected groups ahead of their whole emit body. Surviving
/// groups take the exact `HashAggSource`/`HashAggEmit`/`SortBreakerSink`
/// row path: retrieve → result slot → `sort_lane_put`.
fn sort_feed_agg_topn<'mcx>(
    sort: &mut ::nodesort::SortState<'mcx>,
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    outer_desc: std::rc::Rc<::types_tuple::TupleDescData<'static>>,
    spec: ::nodeagg::TopnEmitSpec,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    ::nodesort::sort_lane_begin(sort, outer_desc)?;
    let dir = estate.es_direction;
    estate.es_direction = ::types_scan::sdir::ForwardScanDirection;
    let mut emitted: u64 = 0;
    let mut skipped: u64 = 0;
    loop {
        let cut = match ::nodesort::sort_lane_topk_boundary(sort) {
            Some((b, false)) => {
                Some(::nodeagg::TopnEmitCut { spec, bound: b.as_i64(), skipped: &mut skipped })
            }
            // Heap not yet full, or a NULL boundary (a NULL's rank depends
            // on NULLS placement): no cut, the retrieve emits unfiltered.
            _ => None,
        };
        let Some(slot) = ::nodeagg::agg_hash_retrieve_topn(agg, estate, cut)? else {
            break;
        };
        emitted += 1;
        ::nodesort::sort_lane_put(sort, estate, slot)?;
    }
    if stats::armed() {
        stats::tick_topnemit_groups(emitted + skipped, skipped);
    }
    ::nodesort::sort_lane_finish(sort, estate)?;
    estate.es_direction = dir;
    Ok(())
}

// ===========================================================================
// Batched finalize+emit from the compact agg table (lane-v2 batchemit; the
// datekey lane's finalize-bucket charter). The per-row agg→sort feed pays,
// PER GROUP: an ExprContext reset, the compact key scatter into first_slot,
// a full fmgr finalize round-trip per aggregate (fcinfo frame + transarray
// re-parse for avg), an interpreted projection into the result slot, and a
// per-put tuplesort handle resolve. The batched feed walks the compact table
// in blocks (`nodeagg::batch_emit_scan_block`) and builds each surviving
// group's OUTPUT ROW directly (`nodeagg::batch_emit_row`) — admission
// (`nodeagg::batch_emit_resolve`) proves every column byte-identical by
// construction (raw byval transvalues; avg/sum images through the SAME
// test-pinned kernels the finalfns call — see the invariant block there).
//
// Composes with the emit-side top-N boundary cut (topnemit): the boundary is
// re-read once per BLOCK instead of per group. Boundaries only TIGHTEN as
// puts land, so the hoisted (staler, looser) boundary can only UNDER-skip —
// and every under-skipped group is one the downstream bounded tuplesort
// compares and discards with no state change. Output bytes are unchanged;
// only no-observable-effect work moves.
//
// Non-admission falls through to the per-row paths unchanged (never a lane
// refusal). Kill switch: PGRUST_LANE_V2_BATCHEMIT=0.
// ===========================================================================

/// `PGRUST_LANE_V2_BATCHEMIT` kill switch (default ON inside the lane).
fn batch_emit_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_LANE_V2_BATCHEMIT").as_deref(), Ok("0") | Ok("off"))
    })
}

// ===========================================================================
// Lane-v2 topkfin (hot-c1-topk-finalize): top-k group selection BEFORE
// finalize/emit. The batched feed above still finalizes and forms a tuple for
// EVERY group the topnemit boundary cut can't skip — and the streaming cut
// only skips groups STRICTLY worse than the live k-th boundary, so on flat
// order-key distributions (two-int-key high-card: ~10M groups, boundary count ~1) it skips
// nothing and ~10M numeric-avg divisions + tuple forms feed a sort that
// keeps 10. This pass selects the k surviving groups FIRST, on the raw int8
// order-key states (`nodeagg::topk_finalize_select` — a bounded (key, row)
// heap in first-arrival total order), then runs the batched finalize+emit
// for those k rows only.
//
// Admission (fail-closed, on top of the batchemit + topnemit admissions the
// caller already proved):
//   * single-column ORDER BY (the k-th boundary's tie-break must not need
//     secondary keys);
//   * bounded sort with a sane positive bound;
//   * relaxed adaptive-topk tie mode (the selected members of the boundary
//     tie group are the first-arrived, deterministic, C-LEGAL set — rule 2
//     of docs/conformance/tie-ordering.md applied to agg groups; `tracked`
//     and `0` stay byte-exact channels and never arm this);
//   * every group's order-key transvalue non-NULL, checked row-by-row inside
//     the selection scan (a NULL bails the whole pass before any side
//     effect — NULLS placement stays the tuplesort's authority).
// Kill switch: PGRUST_LANE_V2_TOPKFIN=0. Declines are never lane refusals —
// the batched (or per-row) feed runs unchanged.
// ===========================================================================

/// `PGRUST_LANE_V2_TOPKFIN` kill switch (default ON inside the lane).
fn topkfin_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_LANE_V2_TOPKFIN").as_deref(), Ok("0") | Ok("off"))
    })
}

/// Plan-shape admission for the topkfin pass (invariant block above). The
/// state-level conditions (compact table present, non-NULL order keys) are
/// checked by the selection itself.
fn topkfin_admits(state: &::nodesort::SortState<'_>) -> bool {
    topkfin_enabled()
        && adaptive_topk_mode() == AdaptiveTopkMode::Relaxed
        && state.bounded
        && state.bound > 0
        && state.plan.numCols == 1
}

/// The topkfin agg→sort feed: select the top-k groups on raw states, then
/// `sort_feed_agg_batched`'s begin/put/finish frame over exactly those rows.
/// Ok(false) = the selection declined (no compact table or a NULL order key)
/// — nothing was mutated and no sort side effect happened; the caller runs
/// the pre-existing feed.
fn sort_feed_agg_topk_finalize<'mcx>(
    sort: &mut ::nodesort::SortState<'mcx>,
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    outer_desc: std::rc::Rc<::types_tuple::TupleDescData<'static>>,
    spec: ::nodeagg::TopnEmitSpec,
    plan: &mut ::nodeagg::BatchEmitPlan,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let k = usize::try_from(sort.bound).expect("admission checked bound > 0");
    let Some((rows, groups)) = ::nodeagg::topk_finalize_select(agg, spec, k)? else {
        stats::tick_topkfin_demoted();
        return Ok(false);
    };
    lane_trace("topk finalize armed (group selection before finalize/emit)");
    ::nodesort::sort_lane_begin(sort, outer_desc)?;
    let dir = estate.es_direction;
    estate.es_direction = ::types_scan::sdir::ForwardScanDirection;
    for chunk in rows.chunks(::nodeagg::BATCH_EMIT_BLOCK) {
        // Block-granular ExprContext reset — the previous block's finalized
        // images were copied by its sort puts (batchemit's residency rule).
        ::nodeagg::batch_emit_set_block(agg, estate, plan, chunk);
        ::nodesort::sort_lane_put_batch(
            sort,
            estate,
            0,
            chunk.len() as u32,
            false,
            &mut BatchEmitFeed { agg, plan },
        )?;
    }
    ::nodeagg::agg_emit_mark_drained(agg);
    if stats::armed() {
        stats::tick_topkfin_groups(groups, rows.len() as u64);
        stats::tick_batchemit_groups(rows.len() as u64, 1);
    }
    ::nodesort::sort_lane_finish(sort, estate)?;
    estate.es_direction = dir;
    Ok(true)
}

/// `SortLaneBatchFeed` face of the batched compact emit: staged position `i`
/// is the i-th surviving row of the current block (`plan.idx`), built
/// directly into the agg's result slot.
struct BatchEmitFeed<'a, 'mcx> {
    agg: &'a mut ::nodeagg::AggStateData<'mcx>,
    plan: &'a mut ::nodeagg::BatchEmitPlan,
}

impl<'mcx> ::nodesort::SortLaneBatchFeed<'mcx> for BatchEmitFeed<'_, 'mcx> {
    fn emit(
        &mut self,
        i: u32,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<::executils::ExecSlotId>> {
        ::nodeagg::batch_emit_row(self.agg, estate, self.plan, i).map(Some)
    }
}

/// The sink-emit batched drain (dop1-tax fix 4): the agg adopted the
/// runtime sink's published per-bucket EmitBufs (finalized, identity-
/// projected byval datums). Feed them into the just-begun tuplesort in
/// per-bucket blocks — row-for-row the same stream, order and slot
/// contents as the per-row cursor drain (`agg_sink_emit_next` under
/// `sort_feed`), with the per-produce pull ceremony hoisted — COMPOSED
/// with the emit-side top-N boundary cut (`topn_emit_arm`): on the
/// admitted `GROUP BY … ORDER BY count-agg LIMIT k` shape the sort key is
/// output column `sortColIdx[0]`, a bare int8 Aggref whose EMITTED datum
/// equals the raw transvalue (`topn_emit_resolve`'s proof), so the
/// boundary compare runs straight off the EmitBuf datum and a
/// boundary-rejected row skips its whole slot build + tuplesort put —
/// exactly the rows the bounded heap would compare-and-discard (the
/// `sort_feed_agg_topn` invariant, verbatim; ties and NULLs always pass).
/// This is what the SERIAL arm's batchemit shortcut does for its compact
/// table — the sink was forfeiting both the batching AND the cut (the
/// DOP-1 tax ledger's ~35ms EmitBuf drain line: 16.4ms of that was
/// heap_form_minimal_tuple over EVERY group; the cut is what makes it
/// ~k tuples).
fn sort_feed_sink_batched<'mcx>(
    sort: &mut ::nodesort::SortState<'mcx>,
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    outer_desc: std::rc::Rc<::types_tuple::TupleDescData<'static>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    ::nodesort::sort_lane_begin(sort, outer_desc)?;
    let dir = estate.es_direction;
    estate.es_direction = ::types_scan::sdir::ForwardScanDirection;
    let spec = topn_emit_arm(sort, agg);
    // COMPOSED top-N winner list (topn-winners-only amendment): when the
    // sink published winners, the winner list IS the drain — put exactly
    // the ≤bound winner rows in the selection total order (rule 5), in
    // BOTH selection modes. This restores the winners-vs-FullDrain
    // byte-identity oracle on tie-bearing shapes: the landed bucket walk
    // fed ALL rows and let the bounded heap re-pick tie members
    // (arrival-order-arbitrary — the ratified count-gated class); the
    // winner-directed put pins tie members to the deterministic selection
    // order. Winner sets are a valid top-bound under the sort's count
    // order by the refinement argument (selection order = count badness
    // first), so the tuplesort's bounded result stays correct by
    // construction. Degraded/uncomposed sinks take `None` and walk the
    // buckets exactly as before.
    if let Some(winners) = ::nodeagg::sink::agg_sink_emit_take_winners(agg) {
        for &(b, r) in &winners {
            ::postgres_seams::check_for_interrupts::call()?;
            let slot =
                ::nodeagg::sink::agg_sink_emit_block_row(agg, estate, b as usize, r as usize);
            ::nodesort::sort_lane_put(sort, estate, slot)?;
        }
        if stats::armed() && spec.is_some() {
            stats::tick_topnemit_groups(winners.len() as u64, 0);
        }
        ::nodeagg::sink::agg_sink_emit_drained(agg);
        ::nodesort::sort_lane_finish(sort, estate)?;
        estate.es_direction = dir;
        return Ok(());
    }
    // The armed spec's sort key as an EMIT column: sortColIdx is 1-based
    // over the agg's output tlist = the EmitBuf's column order.
    let key_col = spec.map(|_| (sort.plan.sortColIdx[0] - 1) as usize);
    let mut emitted: u64 = 0;
    let mut skipped: u64 = 0;
    for b in 0..::nodeagg::sink::SINK_NBUCKETS {
        let n = ::nodeagg::sink::agg_sink_emit_bucket_len(agg, b)
            .expect("sink emit state adopted");
        if n == 0 {
            continue;
        }
        ::postgres_seams::check_for_interrupts::call()?;
        match key_col {
            None => {
                // Unbounded (or unadmitted) sort: the plain batched put.
                ::nodesort::sort_lane_put_batch(
                    sort,
                    estate,
                    0,
                    n as u32,
                    false,
                    &mut SinkEmitFeed { agg, bucket: b },
                )?;
                emitted += n as u64;
            }
            Some(kc) => {
                // Chunk-hoisted boundary (under-skips only: the boundary
                // tightens monotonically, so a stale snapshot only lets
                // through rows the tuplesort re-judges itself).
                let spec = spec.expect("key_col implies spec");
                let mut row = 0usize;
                while row < n {
                    let chunk_end = (row + 1024).min(n);
                    let cut = match ::nodesort::sort_lane_topk_boundary(sort) {
                        Some((bnd, false)) => Some(bnd.as_i64()),
                        // Heap not yet full, or a NULL boundary (rank
                        // depends on NULLS placement): unfiltered.
                        _ => None,
                    };
                    for r in row..chunk_end {
                        if let Some(bound) = cut {
                            let (v, isnull) =
                                ::nodeagg::sink::agg_sink_emit_datum(agg, b, r, kc);
                            // Strictly worse on the leading key = a row the
                            // full bounded heap discards regardless of later
                            // keys; ties/NULLs pass (comparator authority).
                            if !isnull
                                && (if spec.desc {
                                    v.as_i64() < bound
                                } else {
                                    v.as_i64() > bound
                                })
                            {
                                skipped += 1;
                                continue;
                            }
                        }
                        let slot = ::nodeagg::sink::agg_sink_emit_block_row(
                            agg, estate, b, r,
                        );
                        ::nodesort::sort_lane_put(sort, estate, slot)?;
                        emitted += 1;
                    }
                    row = chunk_end;
                }
            }
        }
    }
    if stats::armed() {
        if spec.is_some() {
            stats::tick_topnemit_groups(emitted + skipped, skipped);
        }
    }
    ::nodeagg::sink::agg_sink_emit_drained(agg);
    ::nodesort::sort_lane_finish(sort, estate)?;
    estate.es_direction = dir;
    Ok(())
}

/// `SortLaneBatchFeed` face of the sink-emit drain: position `i` is row `i`
/// of the current bucket's EmitBuf, built into the agg's result slot.
struct SinkEmitFeed<'a, 'mcx> {
    agg: &'a mut ::nodeagg::AggStateData<'mcx>,
    bucket: usize,
}

impl<'mcx> ::nodesort::SortLaneBatchFeed<'mcx> for SinkEmitFeed<'_, 'mcx> {
    fn emit(
        &mut self,
        i: u32,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<::executils::ExecSlotId>> {
        Ok(Some(::nodeagg::sink::agg_sink_emit_block_row(
            self.agg,
            estate,
            self.bucket,
            i as usize,
        )))
    }
}

/// The batched agg→sort feed: `sort_feed_agg_topn`'s begin/finish frame
/// around a block loop — scan a block of surviving compact rows (boundary
/// cut applied row-wise against the block-hoisted k-th boundary), then put
/// the block through `sort_lane_put_batch` (tuplesort handle hoisted; each
/// row built by `batch_emit_row`). `spec` None = no top-N cut (unbounded or
/// unresolvable sort key) — the walk emits every group, still batched.
fn sort_feed_agg_batched<'mcx>(
    sort: &mut ::nodesort::SortState<'mcx>,
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    outer_desc: std::rc::Rc<::types_tuple::TupleDescData<'static>>,
    spec: Option<::nodeagg::TopnEmitSpec>,
    mut plan: ::nodeagg::BatchEmitPlan,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    ::nodesort::sort_lane_begin(sort, outer_desc)?;
    let dir = estate.es_direction;
    estate.es_direction = ::types_scan::sdir::ForwardScanDirection;
    let mut emitted: u64 = 0;
    let mut skipped: u64 = 0;
    loop {
        // Block-hoisted boundary read (under-skips only; see the invariant
        // block above). None until the bounded heap fills, or on a NULL
        // boundary (a NULL's rank depends on NULLS placement).
        let cut = spec.and_then(|spec| match ::nodesort::sort_lane_topk_boundary(sort) {
            Some((b, false)) => Some(::nodeagg::TopnEmitCut {
                spec,
                bound: b.as_i64(),
                skipped: &mut skipped,
            }),
            _ => None,
        });
        let (n, drained) = ::nodeagg::batch_emit_scan_block(agg, estate, &mut plan, cut)?;
        if n > 0 {
            emitted += u64::from(n);
            ::nodesort::sort_lane_put_batch(
                sort,
                estate,
                0,
                n,
                false,
                &mut BatchEmitFeed { agg, plan: &mut plan },
            )?;
        }
        if drained {
            break;
        }
    }
    if stats::armed() {
        if spec.is_some() {
            stats::tick_topnemit_groups(emitted + skipped, skipped);
        }
        stats::tick_batchemit_groups(emitted, 1);
    }
    ::nodesort::sort_lane_finish(sort, estate)?;
    estate.es_direction = dir;
    Ok(())
}

/// The breaker's `Sink` face (pipeline N endpoint). Holds the sort node by
/// `&mut` — the driver threads the SCAN node, so a breaker spanning two nodes
/// needs no driver rework: pipeline N's threaded node is the scan, and the
/// sort node rides in its sink.
struct SortBreakerSink<'a, 'mcx> {
    sort: &'a mut ::nodesort::SortState<'mcx>,
    /// Direct sort-key feed armed for this feed (datum sort whose key the
    /// leaf serves straight from its staged column — `sort_feed`'s probe).
    key_direct: bool,
    /// Armed streaming top-k cutoff (see the invariant block above); `None`
    /// = feed unfiltered.
    topk: Option<TopkCutState>,
    /// Rule-2 rowref selection armed (`TieMode::Rowref`): each batch fetches
    /// the emit face's rowref base and threads per-row physical rowrefs to
    /// the tuplesort puts. Off = never consult the base (no stamping cost).
    rowref: bool,
}

/// Per-feed pre-filter state: the armed spec + the zero-cut back-off. On the
/// adversarial shape (e.g. descending input under an ASC top-k, where every
/// row beats the boundary) the filter can never cut anything and its
/// per-batch mask would be pure overhead; consecutive zero-cut batches back
/// the filter off exponentially (skip 2, 4, … up to 256 batches between
/// attempts), and any batch that cuts a row resets it. A skipped batch takes
/// the exact unfiltered path — correctness is untouched either way (pure
/// skip optimization), this only bounds the overhead of never-winning feeds.
struct TopkCutState {
    cut: TopkCut,
    /// Batches to feed unfiltered before the next filter attempt.
    skip: u32,
    /// Consecutive zero-cut filter attempts (drives the back-off width).
    fails: u32,
}

impl TopkCutState {
    const MAX_BACKOFF_SHIFT: u32 = 8; // cap: retry every 256 batches

    fn new(cut: TopkCut) -> TopkCutState {
        TopkCutState { cut, skip: 0, fails: 0 }
    }

    /// Compute this batch's keep mask (rows `pos..n`), honoring the zero-cut
    /// exponential back-off. `None` = feed the batch unfiltered (not engaged,
    /// backed off, boundary unavailable, or nothing cuttable at the current
    /// boundary). Shared by the wide sort-breaker sink and the refsort sink —
    /// identical engagement, accounting, and interrupt cadence (the skipped
    /// rows' per-row CFIs are elided; one check per engaged staged batch is
    /// the lane's page-batch cadence floor).
    fn batch_mask<'mcx, E: BatchEmit<'mcx>>(
        &mut self,
        sort: &::nodesort::SortState<'mcx>,
        emit: &E,
        pos: u32,
        n: u32,
    ) -> PgResult<Option<[u64; ::exectuples::SOA_BM_WORDS]>> {
        if self.skip > 0 {
            self.skip -= 1;
            return Ok(None);
        }
        let Some(sel) = topk_keep_mask(self.cut, sort, emit, n) else {
            return Ok(None);
        };
        ::postgres_seams::check_for_interrupts::call()?;
        let kept: u32 =
            (pos..n).map(|i| ((sel[(i / 64) as usize] >> (i % 64)) & 1) as u32).sum();
        let cut_rows = n - pos - kept;
        if cut_rows == 0 {
            // Nothing cuttable at the current boundary: back off. The caller
            // feeds unfiltered — an all-set mask would be a dead bit test.
            self.fails = (self.fails + 1).min(TopkCutState::MAX_BACKOFF_SHIFT);
            self.skip = 1u32 << self.fails;
            return Ok(None);
        }
        self.fails = 0;
        if stats::armed() {
            stats::tick_topkcut_rows((n - pos) as u64, cut_rows as u64);
        }
        Ok(Some(sel))
    }
}

impl<'mcx> Sink<'mcx> for SortBreakerSink<'_, 'mcx> {
    fn accept(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<SinkFeed> {
        ::nodesort::sort_lane_put(self.sort, estate, tuple)?;
        Ok(SinkFeed::NeedMore)
    }

    fn finish(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        ::nodesort::sort_lane_finish(self.sort, estate)
    }
}

/// Batch-granular feed: `sort_lane_put_batch` — row-for-row `sort_lane_put`
/// over the same emit stream in the same order, with the tuplesort handle
/// hoisted per batch and the by-val datum batch putter held open across the
/// batch (the `exec_sort`/`exec_sort_batched` feed arms; identical put
/// accounting, see the seam's doc).
impl<'mcx> BatchSink<'mcx> for SortBreakerSink<'_, 'mcx> {
    fn accept_batch<E: BatchEmit<'mcx>>(
        &mut self,
        emit: &mut E,
        pos: u32,
        n: u32,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()> {
        struct Feed<'e, E> {
            emit: &'e mut E,
            /// Streaming top-k keep mask (None = unfiltered feed): cut rows
            /// answer `emit` with None (the qual-filtered path) and `emit_key`
            /// with None (which routes them through `emit`, filtering them) —
            /// mask first, then the direct-key put on survivors.
            sel: Option<&'e [u64; ::exectuples::SOA_BM_WORDS]>,
            /// Rule-2 rowref base for this staged batch (rowref-armed feeds
            /// only): staged row `i`'s rowref is `base + i`. `None` = bare
            /// puts (a rowref-armed tuplesort then records the contract
            /// break and the caller demotes).
            rrbase: Option<u64>,
        }
        impl<'e, E> Feed<'e, E> {
            #[inline(always)]
            fn cut(&self, i: u32) -> bool {
                match self.sel {
                    Some(sel) => sel[(i / 64) as usize] & (1u64 << (i % 64)) == 0,
                    None => false,
                }
            }
        }
        impl<'mcx, E: BatchEmit<'mcx>> ::nodesort::SortLaneBatchFeed<'mcx> for Feed<'_, E> {
            #[inline]
            fn emit(
                &mut self,
                i: u32,
                estate: &mut EStateData<'mcx>,
            ) -> PgResult<Option<ExecSlotId>> {
                if self.cut(i) {
                    return Ok(None);
                }
                self.emit.emit(i, estate)
            }
            #[inline(always)]
            fn emit_key(&mut self, i: u32) -> Option<(::datum::Datum, bool)> {
                if self.cut(i) {
                    // Cut row: fall back to `emit`, which filters it.
                    return None;
                }
                self.emit.emit_key(i)
            }
            #[inline(always)]
            fn emit_rowref(&self, i: u32) -> Option<u64> {
                self.rrbase.map(|b| b + i as u64)
            }
            #[inline(always)]
            fn live_words(&self) -> Option<[u64; ::exectuples::SOA_BM_WORDS]> {
                // The cut mask is exactly this feed's skip contract: a
                // cleared bit answers `emit`/`emit_key` with None above.
                self.sel.copied()
            }
        }
        // Streaming top-k cutoff: once the bounded heap is full, discard the
        // batch's cannot-make-top-k rows (strictly worse than the k-th
        // boundary on the leading key) before any emit or tuplesort put.
        // The mask computation completes before the put loop (the lane
        // borrow ends), and survivors take the EXACT unfiltered put path
        // (including the direct-key arm when q9triage's probe armed it).
        // Rule-2 rowref base, fetched once per staged batch (armed feeds
        // only — unarmed feeds never consult the seam).
        let rrbase = if self.rowref { emit.rowref_base() } else { None };
        let mut filtered = None;
        if let Some(tk) = self.topk.as_mut() {
            filtered = tk.batch_mask(self.sort, &*emit, pos, n)?;
        }
        // Fold the feed's qual-survivor snapshot into the skip mask: a
        // qual-cleared bit is a row whose `emit` returns None with no
        // observable effect (`BatchEmit::live_sel`), so cutting it is
        // put-stream-identical — and lets the put loop skip the whole
        // per-row emit ceremony word-wise for selective quals.
        let filtered = match (filtered, emit.live_sel()) {
            (Some(mut t), Some(q)) => {
                for (w, qw) in t.iter_mut().zip(q.iter()) {
                    *w &= *qw;
                }
                Some(t)
            }
            (Some(t), None) => Some(t),
            (None, q) => q,
        };
        match filtered {
            Some(sel) => ::nodesort::sort_lane_put_batch(
                self.sort,
                estate,
                pos,
                n,
                self.key_direct,
                &mut Feed { emit, sel: Some(&sel), rrbase },
            )?,
            None => ::nodesort::sort_lane_put_batch(
                self.sort,
                estate,
                pos,
                n,
                self.key_direct,
                &mut Feed { emit, sel: None, rrbase },
            )?,
        }
        // Zone-adaptive bound feedback: hand the (possibly just-tightened)
        // k-th boundary leading-key datum to the scan before it stages the
        // next window. No-op unless the scan armed the adaptive order (the
        // emit face's default and the AM's unarmed path both drop it); a
        // NULL boundary never feeds (pgrcolumnar stores no NULLs — conservative
        // guard only).
        if let Some((bkey, false)) = ::nodesort::sort_lane_topk_boundary(self.sort) {
            emit.push_topk_bound(bkey);
        }
        Ok(())
    }
}

// ===========================================================================
// Refsort — late-materialization top-N on the pgrcolumnar-fed bounded sort
// breaker (notes/latemat-lane.md, Phase B conversion 1). For `... WHERE
// <qual> ORDER BY key LIMIT n` over a lane-armed pgrcolumnar scan with a
// Var-only child tlist, the legacy feed projects EVERY survivor to a full
// slot and puts a full minimal tuple into the bounded tuplesort — of which
// only <= n winners ever emerge. The refsort feed instead puts NARROW
// (key, ref) rows (ref = the survivor's pgrcolumnar (rg, row) address) into a
// synthetic 2-col tuplesort built with the plan's leading-key comparator,
// then AFTER performsort gathers only the <= bound winners' full rows via
// `gather_row` and buffers the projected outer tuples on the node
// (`refsort_out`), which the emit face serves in sorted order.
//
// CORRECTNESS (the design note's invariants):
//   * Winner selection/order: same comparator (plan key-0 operator/
//     collation/nullsFirst over the same key datums), same put order
//     (ascending staged rows, survivors only), same ALLOWBOUNDED discard
//     and tie retention => the winner SET and their output ORDER are
//     byte-identical to the legacy feed's, by construction.
//   * Key currency: clean exact-bitmap rows read the key from the staged
//     SoA column — the same cells the legacy emit's projection copies.
//     Requal/fallback rows (and batches without an exact bitmap or a ready
//     key lane) run `seq_scan_batch_emit` per row — the exact per-row qual
//     (C detoast semantics, C's errors on C's row) — and read the key from
//     the projected slot cell. Same rows survive, same order.
//   * Deferred projection is Var-only by admission (physical tlist or the
//     all-Var census): a Var copy cannot err, so eliding per-survivor
//     projection and materializing only winners elides nothing C could
//     observe. gather_row re-decodes from the pinned Part under the CURRENT
//     needed set (tlist Vars are in the scan's needed set by construction;
//     the gather-time null guard demotes if not — before any output).
//   * Demote discipline: any batch without a window ref, and any gather
//     failure, demotes to the legacy feed BEFORE any output escapes (the
//     winners buffer is node-internal until the emit face pops it):
//     fresh tuplesort + child rescan + physical re-feed, the adaptive-topk
//     demote pattern. Demotion is sticky per node.
//   * Views/refs never cross a rescan: every rescan/reset path clears the
//     narrow tuplesort, the marker, and the winner buffer.
// Composes with topk_cut (the keep mask filters the same rows the bounded
// heap would discard — shared `TopkCutState::batch_mask`) and with the
// zone-adaptive traversal (refs are rg-global, order-independent; the tie
// demote machinery runs on the narrow sort's identical leading-key ties).
// Kill switch: PGRUST_LANE_REFSORT=0|off. Never a lane refusal —
// non-admission runs the legacy feed unchanged.
// ===========================================================================

/// `PGRUST_LANE_REFSORT` kill switch (default ON inside the lane).
fn refsort_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_LANE_REFSORT").as_deref(), Ok("0") | Ok("off"))
    })
}

/// `PGRUST_LANE_TOPN_LAZYFETCH` kill switch (default ON; lazytopn lane).
/// Governs the two lazytopn increments: (a) refsort arming under the
/// relaxed rowref default (the rule-2 (key, ref) narrow comparator — the
/// train-10 landing follow-up), and (b) the refsort accept-side needed-set
/// narrowing (key ∪ qual during the narrow feed; the full set restores
/// before the winner gather). `0|off` restores the pre-lane behavior
/// exactly: refsort only on Off/Track tie modes, full-needed accept.
fn topn_lazyfetch_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_LANE_TOPN_LAZYFETCH").as_deref(), Ok("0") | Ok("off"))
    })
}

/// Bound cap (matches the adaptive walk's): past this the top-N is
/// scan-shaped anyway and the winner gather stops being "a handful of rows".
const REFSORT_MAX_BOUND: i64 = 1 << 16;

/// Armed refsort feed spec (all columns 0-based).
struct RefSortSpec {
    /// The leading sort key's scan column (the SoA fast-leg read).
    key_attno_scan: u16,
    /// The key's position in the outer (child output) desc — the fallback
    /// leg reads the key from this projected slot cell.
    key_resno_outer: usize,
    /// Outer resno -> scan attno (the deferred Var-only winner projection).
    tlist_map: Vec<u16>,
}

/// Admission for the refsort feed (never a lane refusal — `None` runs the
/// legacy feed). Bounded sort with a sane bound; heap (multi-column) sort
/// with exactly ONE plan sort key (v1); pgrcolumnar child scan (window refs
/// exist for staged batches); EVERY outer tlist entry a plain Var of the
/// scan relation (no projection = the physical tlist, or the all-Var
/// scan-projection census); sticky-refused nodes never re-arm.
fn refsort_arm<'mcx>(
    state: &::nodesort::SortState<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
    outer_desc: &::types_tuple::TupleDescData<'static>,
) -> Option<RefSortSpec> {
    if !refsort_enabled() || ::nodesort::sort_lane_refsort_refused(state) {
        return None;
    }
    // Parallel-worker refusal (measured 2026-07-13, take-k-sorted shape on the
    // sorted-v2-10m bank, explain-channel warm x3: parallel lane-on 0.068s refsort-off vs 0.097s
    // refsort-on, ~+45%): under Gather Merge every worker gathers its own
    // `bound` winners — N_workers x bound full-granule decodes through
    // `gather_row` for the ~bound rows that survive the leader's merge, while
    // the per-worker survivor stream (the narrow-put saving) is ~1/N of the
    // serial one. Serial measured -20% pod-normalized on the same shape.
    // Leader-side gather across the Gather Merge (workers emit (key, ref),
    // the leader gathers the final bound) is the v2 design — a plan-visible
    // tuple-format change, not an admission tweak.
    //
    // v1.2: the gate is the SCAN's parallelism, not the process role — with
    // leader participation the leader runs the same partial Sort with
    // IsParallelWorker()==false, and its bound-winner gathers alone kept the
    // take-k-sorted parallel leg at +50% (v1.1 A/B: lane-on 0.106s vs 0.070s with only
    // the worker-side refusal). A parallel-aware scan means this Sort is a
    // per-participant partial sort whose winners mostly die at the merge.
    if ::parallel::IsParallelWorker() || ss.is_parallel() {
        return None;
    }
    if !state.bounded || state.bound <= 0 || state.bound > REFSORT_MAX_BOUND {
        return None;
    }
    // Single-column output sorts bare datums already — nothing to narrow.
    if ::nodesort::sort_lane_is_datum(state) {
        return None;
    }
    let plan = state.plan;
    if plan.numCols != 1 || plan.sortColIdx.is_empty() {
        return None;
    }
    // Window refs only exist for pgrcolumnar staged batches (the heap AM answers
    // `window_ref` with None — every batch would demote).
    if !::nodeseqscan::seq_scan_is_pgrcolumnar(ss) {
        return None;
    }
    let natts = outer_desc.natts as usize;
    let tlist_map: Vec<u16> = match ss.ss.ps_ProjInfo.as_ref() {
        // No projection: the scan's output IS the scan tuple (physical
        // tlist) — outer resno j is scan attno j.
        None => (0..natts as u16).collect(),
        // Projected scans admit only the pure Var-copy census (no arith —
        // a computing column deferred to winners could elide C's error).
        Some(p) => {
            let cols = p.pi_state.scan_proj_cols()?;
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
    };
    let oc = plan.sortColIdx[0];
    if oc < 1 || oc as usize > natts {
        return None;
    }
    let key_resno_outer = (oc - 1) as usize;
    Some(RefSortSpec {
        key_attno_scan: tlist_map[key_resno_outer],
        key_resno_outer,
        tlist_map,
    })
}

/// Build (or reuse) the synthetic 2-col (key, ref) desc: col 1 copies the
/// outer key attribute verbatim (type/typmod/collation/len/byval/align —
/// the comparator identity), col 2 is a plain int8. Memoized on the node —
/// one desc-context allocation per node, reused across rescan re-feeds.
fn refsort_key_desc<'mcx>(
    state: &mut ::nodesort::SortState<'mcx>,
    outer_desc: &::types_tuple::TupleDescData<'static>,
    key_resno_outer: usize,
) -> std::rc::Rc<::types_tuple::TupleDescData<'static>> {
    if let Some(d) = ::nodesort::sort_lane_refsort_key_desc(state) {
        return d;
    }
    use ::types_tuple::{CompactAttribute, FormData_pg_attribute};
    let mcx = crate::desc_mcx();
    let mut key = *outer_desc.attr(key_resno_outer);
    key.attnum = 1;
    let refatt = FormData_pg_attribute {
        attnum: 2,
        atttypid: ::types_core::catalog::INT8OID,
        atttypmod: -1,
        attlen: 8,
        attbyval: true,
        attalign: ::types_tuple::TYPALIGN_DOUBLE,
        attstorage: ::types_tuple::TYPSTORAGE_PLAIN,
        ..Default::default()
    };
    let mut attrs = ::mcx::PgVec::new_in(mcx);
    let mut compact = ::mcx::PgVec::new_in(mcx);
    for att in [key, refatt] {
        compact.push(CompactAttribute::populate_from(&att));
        attrs.push(att);
    }
    std::rc::Rc::new(::types_tuple::TupleDescData {
        natts: 2,
        tdtypeid: 2249, // RECORDOID
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

/// The refsort feed's `Sink`/`BatchSink` face: narrow (key, ref) puts in the
/// legacy feed's exact survivor order. `demoted` = a batch arrived without a
/// window ref (or a row arrived at the per-row face) — the remaining feed is
/// drained without effect and the caller runs the byte-safe legacy re-feed.
struct RefSortSink<'a, 'mcx> {
    sort: &'a mut ::nodesort::SortState<'mcx>,
    key_col: u16,
    key_resno: usize,
    /// Armed streaming top-k cutoff (shared spec/back-off with the wide
    /// breaker sink); `None` = feed unfiltered.
    topk: Option<TopkCutState>,
    demoted: bool,
}

impl<'mcx> Sink<'mcx> for RefSortSink<'_, 'mcx> {
    fn accept(
        &mut self,
        _tuple: ExecSlotId,
        _estate: &mut EStateData<'mcx>,
    ) -> PgResult<SinkFeed> {
        // Row-granular arrival = no staged window ref to pair the row with.
        // Never reached from the seqscan drain (its operator overrides
        // `consume_batch`); defensive demote, byte-safe (nothing was put for
        // this row and the caller re-feeds from a rescan).
        self.demoted = true;
        Ok(SinkFeed::NeedMore)
    }

    fn finish(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        if self.demoted {
            // The caller resets the narrow tuplesort wholesale; skipping
            // performsort keeps `sort_Done` false for the re-feed.
            return Ok(());
        }
        ::nodesort::sort_lane_finish(self.sort, estate)
    }
}

impl<'mcx> BatchSink<'mcx> for RefSortSink<'_, 'mcx> {
    fn accept_batch<E: BatchEmit<'mcx>>(
        &mut self,
        emit: &mut E,
        pos: u32,
        n: u32,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()> {
        if self.demoted {
            // Already demoting: drain the rest of the feed without effect
            // (the legacy re-feed rescans from row zero).
            return Ok(());
        }
        let Some((rg, row0)) = emit.window_ref() else {
            stats::tick_refsort_demoted();
            lane_trace("refsort demoted: staged batch without a window ref");
            self.demoted = true;
            return Ok(());
        };
        // Interrupt cadence floor: one check per staged batch (the fast
        // leg's rows have no per-row seam call, like the direct-key feed;
        // emit-path rows keep their per-row check inside `emit`).
        ::postgres_seams::check_for_interrupts::call()?;
        // Streaming top-k cutoff (shared mask + back-off — see the wide
        // sink): cut rows are exactly rows the bounded heap would discard.
        let mut cutmask = None;
        if let Some(tk) = self.topk.as_mut() {
            cutmask = tk.batch_mask(self.sort, &*emit, pos, n)?;
        }
        // Fast leg availability for THIS batch: exact whole-qual selection
        // (or no qual) + the key column's staged datum cells ready. The
        // bitmap words are snapshotted locally (they are per-batch-stable;
        // the key slices are re-borrowed per fast row) so the per-row emit
        // can take its `&mut` on the fallback rows in between.
        let fast = emit.refsort_key_batch(self.key_col, n).map(|(_, _, fallback, sel)| {
            let mut fb = [0u64; ::exectuples::SOA_BM_WORDS];
            fb[..fallback.len()].copy_from_slice(fallback);
            let selw = sel.map(|s| {
                let mut w = [0u64; ::exectuples::SOA_BM_WORDS];
                w[..s.len()].copy_from_slice(s);
                w
            });
            (fb, selw)
        });
        // Composed skip mask, word-skipped by `for_each_live`: top-k cut
        // rows, the fast leg's exact qual-cleared rows, and the feed's
        // qual-survivor snapshot (`live_sel` — cleared bits answer `emit`
        // with None; covers the no-fast/requal legs) each produce nothing,
        // so skipping them keeps the surviving put stream identical.
        let skip = {
            let mut skip: Option<[u64; ::exectuples::SOA_BM_WORDS]> = None;
            let selw = fast.as_ref().and_then(|(_, s)| *s);
            for m in [cutmask, emit.live_sel(), selw].into_iter().flatten() {
                match &mut skip {
                    None => skip = Some(m),
                    Some(acc) => {
                        for (a, b) in acc.iter_mut().zip(m.iter()) {
                            *a &= *b;
                        }
                    }
                }
            }
            skip
        };
        ::exectuples::for_each_live(skip.as_ref().map(|w| &w[..]), pos, n, |i| -> PgResult<()> {
            if let Some((fb, _)) = &fast {
                let w = (i / 64) as usize;
                let bit = 1u64 << (i % 64);
                if fb[w] & bit == 0 {
                    // Clean staged row: key straight from the SoA column —
                    // value/null identical to the emit + slot read below.
                    let (key, isnull) = {
                        let (kvals, knulls, _, _) = emit
                            .refsort_key_batch(self.key_col, n)
                            .expect("refsort key batch stable within a staged batch");
                        (kvals[i as usize], knulls[i as usize])
                    };
                    return ::nodesort::sort_lane_put_refsort(
                        self.sort,
                        key,
                        isnull,
                        ::nodesort::refsort_encode(rg, row0 + i),
                    );
                }
                // Forced-fallback row (stale cells / kernel-undecidable):
                // fall through to the exact per-row emit.
            }
            // Per-row emit leg: the exact qual (C detoast semantics, C's
            // errors on C's row) + the Var projection; key from the
            // projected output slot cell.
            let Some(id) = emit.emit(i, estate)? else { return Ok(()) };
            let (key, isnull) = {
                let slot = estate.slot_mut(id);
                ::exectuples::slot_getsomeattrs(slot, self.key_resno as i32 + 1);
                let base = slot.base();
                (base.tts_values[self.key_resno], base.tts_isnull[self.key_resno])
            };
            ::nodesort::sort_lane_put_refsort(
                self.sort,
                key,
                isnull,
                ::nodesort::refsort_encode(rg, row0 + i),
            )
        })?;
        // Zone-adaptive bound feedback (identical to the wide sink's tail).
        if let Some((bkey, false)) = ::nodesort::sort_lane_topk_boundary(self.sort) {
            emit.push_topk_bound(bkey);
        }
        Ok(())
    }
}

/// The refsort feed: `sort_feed`'s frame (begin / forward-forced drain /
/// finish) over the narrow sink, then the winner gather while the scan is
/// still borrowed. `Ok(false)` = demoted (mid-feed or at gather) — NO output
/// escaped and the tuplesort/refsort state is the caller's to reset before
/// the byte-safe legacy re-feed.
fn sort_feed_refsort<'mcx>(
    sort: &mut ::nodesort::SortState<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    outer_desc: &::types_tuple::TupleDescData<'static>,
    spec: &RefSortSpec,
    topk: Option<TopkCut>,
    tie: TieMode,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let key_desc = refsort_key_desc(sort, outer_desc, spec.key_resno_outer);
    // Rowref (relaxed default, lazytopn): the ref column joins the
    // comparator — rule-2 (key, ref) total order, selection-exact, no tie
    // machinery. Track keeps the byte-exact demote ladder; Off keeps the
    // plain leading-key comparator (physical-order feed, ties invisible to
    // selection by arrival order).
    ::nodesort::sort_lane_begin_refsort(sort, key_desc, tie == TieMode::Rowref)?;
    if tie == TieMode::Track {
        ::nodesort::sort_lane_topk_tie_track_arm(sort);
    } else if tie == TieMode::Rowref {
        lane_trace("refsort rule-2 comparator armed (rowref tie-break)");
    }
    // Accept-side needed-set narrowing (lazytopn): the narrow feed reads
    // only the key (fast leg) and the qual columns (staging + per-row
    // requal); every other tlist column decodes ONLY at the winner gather.
    // Restored unconditionally after the drain — the gather's needed-set
    // guard demotes on any cell outside the CURRENT set, and the demote
    // re-feed (legacy wide feed) reads every tlist column.
    let narrowed = topn_lazyfetch_enabled()
        && ::nodeseqscan::seq_scan_cb_narrow_needed(ss, &[spec.key_attno_scan]);
    if narrowed {
        lane_trace("refsort accept narrowed (key+qual needed set)");
    }
    let dir = estate.es_direction;
    estate.es_direction = ::types_scan::sdir::ForwardScanDirection;
    let mut sink = RefSortSink {
        sort,
        key_col: spec.key_attno_scan,
        key_resno: spec.key_resno_outer,
        topk: topk.map(TopkCutState::new),
        demoted: false,
    };
    let drained =
        drain_pipeline(ss, &mut SeqScanSource, &mut SeqScanFilterProject, &mut sink, estate);
    if narrowed {
        ::nodeseqscan::seq_scan_cb_restore_needed(ss);
    }
    drained?;
    let demoted = sink.demoted;
    estate.es_direction = dir;
    if demoted {
        return Ok(false);
    }
    // Winner gather (the late materialization): read the <= bound narrow
    // tuples in sorted order, materialize each ref's full row under the
    // scan's needed set, project Var-only into outer format, and buffer an
    // owned minimal tuple. The winners' by-ref payloads are copied by the
    // minimal-tuple form BEFORE the next gather reuses the scratch.
    let natts = outer_desc.natts as usize;
    let mut values = vec![::datum::Datum::null(); natts];
    let mut isnull = vec![true; natts];
    let mcx = estate.es_query_cxt;
    // Cap the read-back at `bound`: a bounded tuplesort ERRORS past bound
    // ("retrieved too many tuples in a bounded sort"), and C's puller (the
    // Limit that installed the bound) never reads past it either — the
    // buffered winners are exactly the rows any legal pull sequence sees.
    let mut left = sort.bound;
    loop {
        if left == 0 {
            break;
        }
        left -= 1;
        let Some((rg, row)) = ::nodesort::sort_lane_refsort_next_ref(sort)? else {
            break;
        };
        if !::nodeseqscan::seq_scan_gather_row(ss, estate, rg, row) {
            stats::tick_refsort_demoted();
            lane_trace("refsort demoted: winner gather failed");
            return Ok(false);
        }
        {
            let slot = estate.slot_mut(ss.ss.ss_ScanTupleSlot);
            let base = slot.base();
            for (j, &c) in spec.tlist_map.iter().enumerate() {
                values[j] = base.tts_values[c as usize];
                isnull[j] = base.tts_isnull[c as usize];
                // Needed-set guard: gather_row nulls only unneeded cells
                // (pgrcolumnar stores no NULLs), so a null projected cell means
                // the column was not in the scan's needed set — demote
                // before any output escapes (spurious-demote-safe: the
                // legacy re-feed is byte-identical regardless).
                if isnull[j] {
                    stats::tick_refsort_demoted();
                    lane_trace("refsort demoted: gathered cell outside the needed set");
                    return Ok(false);
                }
            }
        }
        ::nodesort::sort_lane_refsort_push_winner(sort, mcx, &values, &isnull)?;
    }
    stats::tick_refsort_owned();
    lane_trace(&format!(
        "refsort feed done: {} winner(s) gathered (bound {})",
        ::nodesort::sort_lane_refsort_winners(sort),
        sort.bound
    ));
    Ok(true)
}

/// The breaker's `Source` face (pipeline N+1): each produce streams the next
/// tuple of the tuplesort read-back into `ps_ResultTupleSlot` (one-row
/// batches, like the IndexOnlyScan source — always consumed within the
/// producing driver round, so no node-resident cursor is needed; the
/// tuplesort's own read cursor is the cross-call position).
struct SortEmitSource;

impl<'mcx> Source<'mcx> for SortEmitSource {
    type Node = ::nodesort::SortState<'mcx>;

    fn produce(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<Batch>> {
        // C's per-ExecSort-call CHECK_FOR_INTERRUPTS: when a chained consumer
        // (Unique dedup, Limit's offset skip, Group's duplicate skip,
        // Result's stream) drains several sorted tuples in one PG pull, C
        // would enter ExecSort once per tuple — keep that cadence here
        // rather than once per pull (§9). Pending-gated, exactly C's
        // CHECK_FOR_INTERRUPTS macro: the unconditional seam call per
        // produced tuple measured +17% on the distinct-count shape (narrow-sort),
        // where the agg parent pulls this source once per input row.
        if ::init_small::globals::InterruptPending() {
            ::postgres_seams::check_for_interrupts::call()?;
        }
        Ok(::nodesort::sort_lane_next(node, estate)?.map(|_| Batch { n: 1 }))
    }
}

/// Push operator for the emit pipeline: pushes the staged result slot.
struct SortEmit;

impl<'mcx> Operator<'mcx> for SortEmit {
    type Node = ::nodesort::SortState<'mcx>;

    fn pending(&self, _node: &Self::Node) -> Option<Batch> {
        None
    }

    fn consume(
        &mut self,
        node: &mut Self::Node,
        batch: Batch,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        debug_assert_eq!(batch.n, 1);
        Ok(match out.accept(node.ps_ResultTupleSlot, estate)? {
            SinkFeed::Full => OpStatus::Paused,
            SinkFeed::NeedMore => OpStatus::NeedInput,
        })
    }
}

// ===========================================================================
// Sorted-agg (AGG_SORTED) streaming operator (Phase-2 breadth). NOT a
// breaker: input arrives sorted on the grouping keys, so the node emits a
// finalized group at each key boundary and never needs the whole input — it
// sits as a mid-pipeline `TupleOp` between a lane-owned ordered feed and the
// root pull-adapter:
//
//   Agg over Sort (two chained pipelines on the sort breaker):
//     pipeline N   : scan source → filter/project → SortBreakerSink
//     pipeline N+1 : SortEmitSourceCfi → SortEmit → SortedAggOp → RootAdapter
//   Agg over IndexScan / IndexOnlyScan (order from the index, one pipeline):
//     IndexScanSource → IndexScanEmit → SortedAggOp → RootAdapter
//
// The lane owns ONLY control flow; ALL semantics delegate to the row-path
// nodeagg seam (`agg_sorted_*`): the group-boundary comparison is the ported
// grouping-equality ExprState (NULL keys group together through it), the
// per-row transition program, and the finalize/HAVING/project tail are
// `agg_retrieve_sorted`'s own pieces over the node's own persort state
// (first/pending slots + `have_pending`). Because the seam maintains exactly
// the pull loop's node state — every call boundary has the current group
// closed and at most a pending boundary tuple saved — a per-call fallback to
// `exec_agg` (dynamic gates) is byte-safe in both directions.
//
// Per-tuple laziness holds: the capacity-one root buffers the boundary
// group's row, pausing the pipeline (the child feed advances only to the
// boundary tuple, which is saved in the pending slot before the pause).
// End-of-stream uses the driver's `TupleOp::source_exhausted` hook (the
// Finished-vs-more-phases seam) to finalize the last
// open group; `agg_done` (set exactly where the pull loop sets it) makes the
// drained node stay drained.
//
// v1 is deliberately per-row (correctness first): the ordered feeds emit
// one-row batches (sort read-back) or short TID runs, so there is no clean
// whole-batch group-run fold seam here yet; the lanefold `fold_rows_grouped`
// batching over contiguous group runs is a later, measured step.
// ===========================================================================

/// The sorted-agg streaming operator. `group_open` is call-local by
/// construction: the only pauses are group-row emissions (capacity-one root),
/// after which the group is already closed — so at every PG call boundary the
/// open-group flag is false and the cross-call resume state is entirely the
/// node's own `have_pending`/`agg_done`.
struct SortedAggOp<'a, 'mcx> {
    agg: &'a mut ::nodeagg::AggStateData<'mcx>,
    group_open: bool,
}

impl<'mcx> SortedAggOp<'_, 'mcx> {
    /// Start the next group from the saved pending boundary tuple.
    fn begin_from_pending(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        ::nodeagg::agg_sorted_group_begin(self.agg, estate, None)?;
        self.group_open = true;
        Ok(())
    }
}

impl<'mcx> TupleOp<'mcx> for SortedAggOp<'_, 'mcx> {
    fn pending(&self) -> bool {
        // A saved boundary tuple whose group has not started: the resume
        // after the pause that delivered the previous group's row.
        !self.group_open && ::nodeagg::agg_sorted_have_pending(self.agg)
    }

    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        if !self.group_open {
            // First row of the stream (or after a HAVING-rejected tail): the
            // group prologue — copy, initialize, first transition.
            debug_assert!(!::nodeagg::agg_sorted_have_pending(self.agg));
            ::nodeagg::agg_sorted_group_begin(self.agg, estate, Some(tuple))?;
            self.group_open = true;
            return Ok(OpStatus::NeedInput);
        }
        if ::nodeagg::agg_sorted_same_group(self.agg, estate, tuple)? {
            ::nodeagg::agg_sorted_accept(self.agg, estate, tuple)?;
            return Ok(OpStatus::NeedInput);
        }
        // Group boundary: save the boundary row first (the pull loop's
        // order), then finalize + HAVING + project the completed group.
        ::nodeagg::agg_sorted_save_pending(self.agg, estate, tuple)?;
        self.group_open = false;
        match ::nodeagg::agg_sorted_emit(self.agg, estate)? {
            Some(row) => match out.accept(row, estate)? {
                SinkFeed::Full => Ok(OpStatus::Paused),
                // Non-root sinks (none wired today): start the next group
                // immediately, as the pull loop's next iteration would.
                SinkFeed::NeedMore => {
                    self.begin_from_pending(estate)?;
                    Ok(OpStatus::NeedInput)
                }
            },
            // HAVING rejected the group: no output row; start the next group
            // from the pending boundary tuple (the pull loop's `continue`).
            None => {
                self.begin_from_pending(estate)?;
                Ok(OpStatus::NeedInput)
            }
        }
    }

    fn resume(
        &mut self,
        _out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        // The paused emit already delivered its row; resuming means starting
        // the next group from the saved boundary tuple, then asking for more
        // input.
        debug_assert!(self.pending());
        self.begin_from_pending(estate)?;
        Ok(OpStatus::NeedInput)
    }

    fn source_exhausted(
        &mut self,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        // Input exhausted: agg_done first (the pull loop's fetch-None arms),
        // then finalize the last open group, if any. Zero input rows emit
        // nothing (C's sorted-agg contract — unlike AGG_PLAIN).
        ::nodeagg::agg_sorted_input_done(self.agg);
        if !self.group_open {
            return Ok(OpStatus::Finished);
        }
        self.group_open = false;
        match ::nodeagg::agg_sorted_emit(self.agg, estate)? {
            Some(row) => match out.accept(row, estate)? {
                SinkFeed::Full => Ok(OpStatus::Paused),
                SinkFeed::NeedMore => Ok(OpStatus::Finished),
            },
            None => Ok(OpStatus::Finished),
        }
    }
}

/// Sort read-back source for the sorted-agg emit chain: `SortEmitSource`
/// plus C's per-fetch CHECK_FOR_INTERRUPTS — each row the agg pulls from the
/// sort is one `ExecSort` call in the per-tuple path, which checks at entry
/// (the bare-sort pipeline's equivalent check lives at `try_own_sort`'s
/// entry, once per pull).
struct SortEmitSourceCfi;

impl<'mcx> Source<'mcx> for SortEmitSourceCfi {
    type Node = ::nodesort::SortState<'mcx>;

    fn produce(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<Batch>> {
        ::postgres_seams::check_for_interrupts::call()?;
        Ok(::nodesort::sort_lane_next(node, estate)?.map(|_| Batch { n: 1 }))
    }
}

// ===========================================================================
// Hash-grouped exact-DISTINCT arm (lane-v2-distincthash; nodeagg
// hashgrouped.rs holds the state machine + the byte-identity argument). For
// the narrow-sort shape (sorted grouped exact-DISTINCT) the group-prefix SORT itself is the
// remaining dominant cost: this arm bypasses the plan's Sort node entirely —
// the scan pipeline drains into a group hash table whose entries own the
// order-insensitive transition state and the per-aggregate exact-DISTINCT
// sets, the groups order by the prefix (groups, not rows — the cheap sort),
// and the unchanged finalize/HAVING/project tail emits one group per pull.
//
// Admission tiers (demote-within-lane): the arm engages only where the
// narrow-sort admission ALREADY holds, plus all-integer group keys +
// integer set kinds + a SeqScan feed + planner-estimate economics
// (`agg_hashgroup_economical`). Anything else falls to the narrow-sort arm
// unchanged. At runtime, crossing the arm's memory budget mid-build
// DEGRADES to the narrow-sort arm exactly once: the narrowed tuplesort is
// begun late, the deferred group representatives + all remaining rows feed
// it, and the narrow emit chain resumes with per-group residual-state
// preload (nodeagg's `initialize_aggregates` hook) — so the arm is
// spill-safe wherever the narrow-sort arm is.
//
// Fallback safety: once the build consumed the scan, the plan's Sort node
// must never be fed again (it would rebuild empty from the exhausted scan).
// The no-degrade emit therefore resumes BEFORE the per-call dynamic gates —
// sound because the emit touches no scan, backward fetches imply
// randomAccess (refused at admission, so the executor never runs this node
// backward), and es_epq_active is constant for the estate this node was
// built in (EPQ rechecks run their own estate). The degraded path needs no
// such care: the sort IS built there, so the existing per-call narrow-arm
// resume (and even the per-tuple C fallback) is byte-safe.
// ===========================================================================

/// `PGRUST_LANE_V2_DISTINCTHASH` kill switch (default ON inside the lane).
fn distincthash_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_LANE_V2_DISTINCTHASH").as_deref(), Ok("0") | Ok("off"))
    })
}

/// `PGRUST_LANE_V2_DISTINCTHASH_TEXT` kill switch (default ON): text group
/// keys for the hash-grouped arm (the text-keyed grouped-distinct shape). Off, text-keyed
/// nodes fall to the narrow-sort arm exactly as before this lane — the A/B
/// attribution channel for the text-key delta.
fn distincthash_text_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(
            std::env::var("PGRUST_LANE_V2_DISTINCTHASH_TEXT").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// `PGRUST_LANE_V2_DISTINCTHASH_BATCH` kill switch (default ON): the
/// batched fast leg of the hash-grouped distinct feed (staged-cell key
/// probe + direct set insert, skipping per-row slot projection and the
/// transition program for the all-set-mode bare-Var shape — narrow-sort class).
/// Off, the sink feeds per-row exactly as before — the A/B attribution
/// channel for the batch delta.
fn distincthash_batch_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(
            std::env::var("PGRUST_LANE_V2_DISTINCTHASH_BATCH").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// `PGRUST_LANE_V2_DISTINCTHASH_FOLD` kill switch (default ON): mixed-shape
/// admission into the batched fast leg — plain exact-integer vocabulary
/// transitions (count(*)/count(x)/sum(int2/4)/avg(int2/4), the pardistinct
/// vocab) fold in sidecar words beside the DISTINCT set parks (the companion-agg class:
/// sums alongside the COUNT DISTINCT). Off, mixed shapes keep the per-row
/// transition program exactly as before (the historical all-set-mode batch
/// gate) — the A/B attribution channel for the fold delta.
fn distincthash_fold_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(
            std::env::var("PGRUST_LANE_V2_DISTINCTHASH_FOLD").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// `PGRUST_LANE_V2_DISTINCTHASH_SPAN` kill switch (default ON): the span
/// form of the batched fast leg — one nodeagg call per staged run with the
/// loop-invariant state hoisted and no per-row group switch
/// (`agg_hashgroup_accept_batch_span`). Off, the per-row
/// `agg_hashgroup_accept_batch_row` loop stands — the A/B attribution
/// channel for the span delta; results are byte-identical either way.
fn distincthash_span_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(
            std::env::var("PGRUST_LANE_V2_DISTINCTHASH_SPAN").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// `PGRUST_LANE_V2_DISTINCTHASH_TEXTBATCH` kill switch (default ON): admit
/// TEXT/VARCHAR grouping keys to the batched fast leg (named-kernels-
/// distinct kernel 1 — the filtered grouped-distinct batch feed, the
/// text-arg distinct-count top-n class). Off = the historical int-keys-only batch gate; the text-key
/// arm then rides the per-row path exactly as before this lane (the A/B
/// attribution channel; results byte-identical either way).
fn distincthash_textbatch_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(
            std::env::var("PGRUST_LANE_V2_DISTINCTHASH_TEXTBATCH").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// `PGRUST_LANE_V2_AGG_POLY` (SE-AGGPOLY, band 101001; DEFAULT ON since t35
/// routing-flips, `=0|off` kills): admit the poly export manifest —
/// plain-agg shapes whose transitions the fold plan does not fully cover
/// but whose remainder is exactly the sum/avg(numeric) NumericAggState
/// family — to the runtime scan arm's per-row drive (DriveMode::PerRowPoly:
/// the real checked transition program per row; numeric end states
/// relocated as self-contained exact digit snapshots for the cross-worker
/// export/combine/absorb). The m5 suppression probe keys the matching plan
/// shapes under the SAME env spelling (knob coherence — a
/// keyed-but-disarmed shape would land on serial; BOTH read sites flip
/// together, the GL letter's duty). FLIP EVIDENCE (GL letter 2026-07-21 @
/// 67a99589d): official suite geomean 0.9278 vs knob-OFF (noise floor 0.9889);
/// the mixed narrow-sort shape 1.861 -> 0.066 hot (28x, == forced ref); GL-AGGPOLY-1 SE16 filtered-plain-agg
/// heap −12.4%; zero regressions, parity class set unchanged. Killed =
/// pre-flip refusals, byte-identical.
pub(crate) fn agg_poly_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(
            std::env::var("PGRUST_LANE_V2_AGG_POLY").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// GL-DISTALPHA-2 measurement arm (`PGRUST_LANE_V2_DISTINCT_PRESORTED=1`,
/// DEFAULT OFF): probe the runtime plain-distinct sink for the
/// PRESORTED-bare exact-DISTINCT face (clustered scan order serves the
/// DISTINCT aggregate with no Sort node; the set entries are dormant by
/// the `set_active` contract, so every set drive and sink probe was
/// structurally unreachable). OFF = today's paths byte-identically; the
/// letter's ladder is the flip evidence channel.
pub(crate) fn distinct_presorted_probe_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        matches!(std::env::var("PGRUST_LANE_V2_DISTINCT_PRESORTED").as_deref(), Ok("1"))
    })
}

/// `PGRUST_LANE_AGG_ROUTE_LATCH` (GL-ALPHA1-COUNTERS-1 Phase B inc-2,
/// default OFF; `1`/`on` arms): a hashed-agg node whose SINK EMIT is in
/// progress (`sink_emit.is_some()` — one Option load) has already committed
/// its route for this execution, yet the ownership walk re-runs its whole
/// admissibility ladder (plain/sorted/distinct-set predicates, scan
/// fusibility, build_if_needed) on EVERY emitted row — per-pull ceremony
/// measured at ~13% of process CPU on the all-distinct-keys grouped-agg
/// cell, where output cardinality equals input. The latch dispatches those
/// pulls straight to the drain. Decision parity is structural: the walk's
/// verdict is a pure function of state the emit marker already witnesses,
/// and a rescan drops `sink_emit` (exec_rescan_agg) so the latch
/// self-invalidates and the full walk re-runs. EXPLAIN(ENGINE) capture
/// cadence during an armed emit moves from per-pull to none (diagnostic
/// channel; documented, never measured). OFF keeps the incumbent walk
/// branch-for-branch.
pub(crate) fn agg_route_latch_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        matches!(
            std::env::var("PGRUST_LANE_AGG_ROUTE_LATCH").as_deref(),
            Ok("1") | Ok("on")
        )
    })
}

/// `PGRUST_LANE_AGG_EMIT_BATCH` (GL-ALPHA1-EMIT-1, default OFF; `1`/`on`
/// arms): a plain (ungrouped) Agg over a hashed-Agg child whose build the
/// runtime sink served drains the child's ADOPTED EMIT (the 256 per-bucket
/// EmitBufs, already pre-materialized IN PARALLEL by the combine claims)
/// through the `BatchSource` seam in per-bucket blocks —
/// `try_own_plain_agg_over_agg_emit` — instead of the per-EMITTED-row pull
/// chain (procnode dispatch → ownership walk / route latch → pull_step
/// driver → emit cursor → per-row slot ceremony), which dominates the
/// leader's serial tail where output cardinality approaches input (the
/// all-distinct-keys grouped-agg cell). Same rows, same (bucket 0..255,
/// insertion) order, same slot contents, same per-row transition program
/// (`exec_agg_batched` = exec_agg minus the node recursion) — result bytes
/// identical by construction. OFF keeps the incumbent per-pull chain
/// branch-for-branch.
pub(crate) fn agg_emit_batch_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        matches!(
            std::env::var("PGRUST_LANE_AGG_EMIT_BATCH").as_deref(),
            Ok("1") | Ok("on")
        )
    })
}

/// `PGRUST_LANE_V2_DISTINCTHASH_FORCE=1`: skip the planner-estimate
/// economics (e2e harness lever — small tables would otherwise refuse and
/// never exercise the arm; the runtime degrade still bounds memory).
fn distincthash_force() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        matches!(
            std::env::var("PGRUST_LANE_V2_DISTINCTHASH_FORCE").as_deref(),
            Ok("1") | Ok("on")
        )
    })
}

/// Map the plan Sort's k-key prefix onto the grouping columns as the arm's
/// group-emit order. `None` refuses (an operator outside the integer/text
/// asc/desc vocabulary — bool group keys keep the narrow-sort arm). Text
/// keys carry the plan Sort's collation for the group-order comparator
/// (`varstr_cmp`'s authority) and require it valid + DETERMINISTIC (the
/// no-ties total-order invariant; nondeterministic collations keep the C
/// sort path per the textsets rule — the equality-side admission refuses
/// them independently). The multiset equality of prefix and group columns
/// was already proven by the narrow admission; `used` disambiguates
/// duplicated columns.
fn hashgroup_order_spec(
    agg: &::nodeagg::AggStateData<'_>,
    sp: &::types_nodes::plannodes::Sort<'_>,
    k: usize,
) -> Option<Vec<::nodeagg::HashGroupOrderKey>> {
    use ::execexpr::CmpOp;
    /// pg_proc text_lt / text_gt — the btree text opclass's `<` / `>`
    /// support (varchar sorts through the same text operators).
    const F_TEXT_LT: ::types_core::Oid = 740;
    const F_TEXT_GT: ::types_core::Oid = 742;
    let group_cols = ::nodeagg::agg_plan_group_cols(agg);
    debug_assert_eq!(group_cols.len(), k);
    let mut used = vec![false; k];
    let mut out = Vec::with_capacity(k);
    for i in 0..k {
        let col = sp.sortColIdx[i];
        let j = (0..k).find(|&j| !used[j] && group_cols[j] == col)?;
        used[j] = true;
        // Sort operator -> its comparison-kernel image -> ASC/DESC (the
        // top-k cutoff's resolution path), or the text operator pair.
        let opfn = ::lsyscache::get_opcode(sp.sortOperators[i]).ok()?;
        let (desc, collation) = match opfn {
            F_TEXT_LT | F_TEXT_GT => {
                let coll = sp.collations[i];
                if coll == 0 || !::lsyscache::get_collation_isdeterministic(coll).ok()? {
                    return None;
                }
                (opfn == F_TEXT_GT, coll)
            }
            _ => {
                let desc = match CmpOp::for_fn_oid(opfn)? {
                    CmpOp::Int2Lt | CmpOp::Int4Lt | CmpOp::Int8Lt => false,
                    CmpOp::Int2Gt | CmpOp::Int4Gt | CmpOp::Int8Gt => true,
                    _ => return None,
                };
                (desc, 0)
            }
        };
        out.push(::nodeagg::HashGroupOrderKey {
            key_idx: j,
            desc,
            nulls_first: sp.nullsFirst[i],
            collation,
        });
    }
    Some(out)
}

/// The hash-grouped build sink: rows feed the group table until the shared
/// budget crosses, then the sink degrades IN PLACE — the narrowed tuplesort
/// begins late, the deferred representatives dump into it, and every
/// further row goes straight to the sort (the narrow-sort arm's feed,
/// resumed mid-stream; section doc).
/// Scan-column map for the batched fast leg (`agg_hashgroup_batch_shape`
/// order): the grouping keys', the per-pertrans DISTINCT args', and the
/// arg-bearing fold-vocab entries' 0-based SCAN columns — every mapped
/// outer column proved a bare Var, so the staged scan cell IS the projected
/// outer cell.
struct HgBatchCols {
    key_cols: Vec<u16>,
    arg_cols: Vec<u16>,
    fold_cols: Vec<u16>,
}

struct HashGroupDistinctSink<'a, 'mcx> {
    agg: &'a mut ::nodeagg::AggStateData<'mcx>,
    sort: &'a mut ::nodesort::SortState<'mcx>,
    outer_desc: std::rc::Rc<::types_tuple::TupleDescData<'static>>,
    nkeys: usize,
    degraded: bool,
    /// `Some` = the batched fast leg is admitted (shape + Var map +
    /// `distincthash_batch_enabled`); per-batch availability still gates
    /// each staged window (`refsort_key_batch`'s soundness contract).
    batch: Option<HgBatchCols>,
    /// Engagement counters (trace observability): rows absorbed by the
    /// fast leg vs rows routed through the per-row emit path.
    fast_rows: u64,
    slow_rows: u64,
}

impl<'mcx> Sink<'mcx> for HashGroupDistinctSink<'_, 'mcx> {
    fn accept(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<SinkFeed> {
        if self.degraded {
            ::nodesort::sort_lane_put(self.sort, estate, tuple)?;
        } else if !::nodeagg::agg_hashgroup_accept(self.agg, estate, tuple)? {
            self.degrade_impl(estate)?;
        }
        Ok(SinkFeed::NeedMore)
    }

    fn finish(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        if self.batch.is_some() {
            trace_feed(&format!(
                "hashgroup batch feed: fast={} perrow={}",
                self.fast_rows, self.slow_rows
            ));
        }
        if self.degraded {
            ::nodesort::sort_lane_finish(self.sort, estate)
        } else {
            Ok(())
        }
    }
}

/// Batch-granular feed. Default: the per-row delegation loop (group probe +
/// transition program per row). With `batch` armed (all-int keys, every
/// transition a set-mode bare-Var pertrans) the FAST LEG reads the keys and
/// DISTINCT args straight from the staged scan cells and absorbs found-group
/// rows with zero slot work — no `emit` projection, no `slot_getsomeattrs`,
/// no transition program (`agg_hashgroup_accept_batch_row` reproduces
/// `run_row`'s set collect verbatim). Rows the fast leg cannot host go
/// through the exact per-row path in ROW ORDER (single pass): qual-dead rows
/// skip (the whole-qual bitmap verdict — the emit's own verdict), forced-
/// fallback rows and probe misses (group creation defers the row as the
/// group's rep, which needs a materialized slot) take `emit` + `accept`.
/// Byte identity: same rows, same order, same group-creation order, same
/// rep bytes, same degrade point (`Absorbed(false)` mirrors `accept`'s
/// `Ok(false)` — the row is absorbed first, then the one-shot degrade).
impl<'mcx> BatchSink<'mcx> for HashGroupDistinctSink<'_, 'mcx> {
    fn accept_batch<E: BatchEmit<'mcx>>(
        &mut self,
        emit: &mut E,
        pos: u32,
        n: u32,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()> {
        'fast: {
            if self.degraded || self.batch.is_none() {
                break 'fast;
            }
            let cols = self.batch.as_ref().expect("checked");
            // Snapshot the staged views: value/isnull cell pointers per
            // needed column + one copy of the shared fallback/sel words.
            // Any column unavailable this batch -> the per-row loop.
            // SAFETY of the raw pointers: the staged window (SoA cells,
            // bitmap words) is stable for the whole batch — `emit.emit`
            // projects into a slot and never restages (the dict-lane feed
            // relies on the same window-stability contract); the pointers
            // are consumed before this call returns.
            let nc = cols.key_cols.len() + cols.arg_cols.len() + cols.fold_cols.len();
            let mut views: Vec<(*const ::datum::Datum, *const bool)> = Vec::with_capacity(nc);
            let mut fb = [0u64; ::exectuples::SOA_BM_WORDS];
            let mut selw: Option<[u64; ::exectuples::SOA_BM_WORDS]> = None;
            for (ci, &col) in cols
                .key_cols
                .iter()
                .chain(cols.arg_cols.iter())
                .chain(cols.fold_cols.iter())
                .enumerate()
            {
                let Some((vals, nulls, fallback, sel)) = emit.refsort_key_batch(col, n)
                else {
                    break 'fast;
                };
                if ci == 0 {
                    fb[..fallback.len()].copy_from_slice(fallback);
                    selw = sel.map(|s| {
                        let mut w = [0u64; ::exectuples::SOA_BM_WORDS];
                        w[..s.len()].copy_from_slice(s);
                        w
                    });
                }
                views.push((vals.as_ptr(), nulls.as_ptr()));
            }
            // Interrupt cadence floor: one check per staged batch (the
            // refsort fast leg's cadence; per-row-path rows keep their
            // per-row check inside `emit`).
            ::postgres_seams::check_for_interrupts::call()?;
            let nk = cols.key_cols.len();
            let na = cols.arg_cols.len();
            // Span form (default): one nodeagg call per run of fast-hosted
            // rows — hoisted state, no per-row switch, no marshaling. The
            // per-row loop below is the SPAN=0 control (byte-identical).
            if distincthash_span_enabled() {
                let mut i = pos;
                while i < n && !self.degraded {
                    // SAFETY: per the snapshot contract above — every view
                    // spans `n` staged rows and the window is stable for
                    // the whole batch; fb/selw cover bit n-1.
                    let stop = unsafe {
                        ::nodeagg::agg_hashgroup_accept_batch_span(
                            self.agg,
                            &views,
                            na,
                            selw.as_ref().map(|s| &s[..]),
                            &fb,
                            i,
                            n,
                        )
                    };
                    match stop {
                        ::nodeagg::HgSpanStop::Done { absorbed } => {
                            self.fast_rows += absorbed as u64;
                            i = n;
                        }
                        ::nodeagg::HgSpanStop::NeedSlot { at, absorbed } => {
                            self.fast_rows += absorbed as u64;
                            self.slow_rows += 1;
                            // Probe miss / forced fallback: materialize the
                            // row and run the per-row accept (byte-identical
                            // creation order + rep bytes).
                            if let Some(slot) = emit.emit(at, estate)? {
                                self.accept(slot, estate)?;
                            }
                            i = at + 1;
                        }
                        ::nodeagg::HgSpanStop::Budget { at, absorbed } => {
                            self.fast_rows += absorbed as u64;
                            self.degrade_impl(estate)?;
                            i = at + 1;
                        }
                    }
                }
                // Post-degrade remainder: the exact per-row path (word-skip
                // over the same qual-filtered continues, stream-identical).
                ::exectuples::for_each_live(
                    selw.as_ref().map(|w| &w[..]),
                    i,
                    n,
                    |j| -> PgResult<()> {
                        self.slow_rows += 1;
                        if let Some(slot) = emit.emit(j, estate)? {
                            self.accept(slot, estate)?;
                        }
                        Ok(())
                    },
                )?;
                return Ok(());
            }
            let mut keyd = [::datum::Datum::null(); 32];
            let mut keyn = [false; 32];
            let mut args: Vec<(::datum::Datum, bool)> = vec![(::datum::Datum::null(), false); cols.arg_cols.len()];
            let mut folds: Vec<(::datum::Datum, bool)> = vec![(::datum::Datum::null(), false); cols.fold_cols.len()];
            // Word-skip the qual-filtered continues (exact whole-qual
            // verdict; same surviving rows, same order — the SPAN control
            // stays byte-identical to the span form).
            ::exectuples::for_each_live(selw.as_ref().map(|w| &w[..]), pos, n, |i| -> PgResult<()> {
                let w = (i / 64) as usize;
                let bit = 1u64 << (i % 64);
                if self.degraded || fb[w] & bit != 0 {
                    // Post-degrade remainder / forced-fallback row: the
                    // exact per-row path (emit re-checks + C detoast).
                    self.slow_rows += 1;
                    if let Some(slot) = emit.emit(i, estate)? {
                        self.accept(slot, estate)?;
                    }
                    return Ok(());
                }
                // SAFETY: per the snapshot contract above; `i < n` and every
                // view spans `n` staged rows.
                unsafe {
                    for (j, &(v, nl)) in views[..nk].iter().enumerate() {
                        keyd[j] = *v.add(i as usize);
                        keyn[j] = *nl.add(i as usize);
                    }
                    for (j, &(v, nl)) in views[nk..nk + na].iter().enumerate() {
                        args[j] = (*v.add(i as usize), *nl.add(i as usize));
                    }
                    for (j, &(v, nl)) in views[nk + na..].iter().enumerate() {
                        folds[j] = (*v.add(i as usize), *nl.add(i as usize));
                    }
                }
                match ::nodeagg::agg_hashgroup_accept_batch_row(
                    self.agg,
                    &keyd[..nk],
                    &keyn[..nk],
                    &args,
                    &folds,
                ) {
                    ::nodeagg::HgBatchRow::Absorbed(true) => self.fast_rows += 1,
                    ::nodeagg::HgBatchRow::Absorbed(false) => {
                        self.fast_rows += 1;
                        self.degrade_impl(estate)?;
                    }
                    ::nodeagg::HgBatchRow::NeedSlot => {
                        self.slow_rows += 1;
                        // Probe miss: group creation defers this row as the
                        // rep — materialize it and run the per-row accept
                        // (byte-identical creation order + rep bytes).
                        if let Some(slot) = emit.emit(i, estate)? {
                            self.accept(slot, estate)?;
                        }
                    }
                }
                Ok(())
            })?;
            return Ok(());
        }
        // Per-row delegation loop (the default impl, incl. its emit-dead
        // word skip; slow_rows stays window-grain, unchanged by skips).
        if self.batch.is_some() {
            self.slow_rows += (n - pos) as u64;
        }
        let live = emit.live_sel();
        ::exectuples::for_each_live(live.as_ref().map(|w| &w[..]), pos, n, |i| -> PgResult<()> {
            if let Some(slot) = emit.emit(i, estate)? {
                match self.accept(slot, estate)? {
                    SinkFeed::NeedMore => {}
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

impl<'mcx> HashGroupDistinctSink<'_, 'mcx> {
    /// The one-shot degrade (section doc): begin the narrowed sort, dump
    /// every deferred representative, flip the table to residual mode.
    #[cold]
    #[inline(never)]
    fn degrade_impl(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        trace_feed("hash-grouped distinct arm degrading to narrowed sort");
        ::nodesort::sort_lane_begin_narrowed(self.sort, self.outer_desc.clone(), self.nkeys)?;
        let mcx = estate.es_query_cxt;
        while let Some(slot) = ::nodeagg::agg_hashgroup_next_rep(self.agg) {
            ::nodesort::sort_lane_put_slot(self.sort, mcx, slot)?;
        }
        ::nodeagg::agg_hashgroup_set_residual(self.agg)?;
        self.degraded = true;
        Ok(())
    }
}

/// Build outcome of the hash-grouped arm's probe.
enum HgBuild {
    /// Table built; the arm owns the emit (groups in prefix order).
    Emit,
    /// Budget crossed mid-build: the narrowed sort is fed and finished; the
    /// narrow-sort emit chain resumes over it (residual preload installed).
    Degraded,
    /// Arm not admitted (admission/economics/child shape): the caller runs
    /// the narrow-sort feed exactly as before.
    Refused,
}

/// Probe + build for the hash-grouped arm (called with the narrow admission
/// already proven and `force_distinct_set` armed, BEFORE the sort exists).
fn try_hashgroup_build<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    sort: &mut ::nodesort::SortState<'mcx>,
    outer: &mut crate::procnode::PlanStateNode<'mcx>,
    outer_desc: &Option<std::rc::Rc<::types_tuple::TupleDescData<'static>>>,
    k: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<HgBuild> {
    if !distincthash_enabled() {
        return Ok(HgBuild::Refused);
    }
    // v1 feed scope: SeqScan child only (the narrow-sort shape; index/bitmap-fed
    // sorts keep the narrow-sort arm).
    let crate::procnode::PlanStateNode::SeqScan(ss) = outer else {
        return Ok(HgBuild::Refused);
    };
    if !::nodeagg::agg_hashgroup_admissible(agg)
        // Density/memory economics: the Sort's row estimate is the arm's
        // input cardinality (the sort passes every input row through).
        || !::nodeagg::agg_hashgroup_economical(
            agg,
            distincthash_force(),
            sort.plan.plan.plan_rows,
        )
    {
        return Ok(HgBuild::Refused);
    }
    let text_keys = ::nodeagg::agg_hashgroup_text_key_count(agg);
    if text_keys > 0 && !distincthash_text_enabled() {
        return Ok(HgBuild::Refused);
    }
    let Some(order) = hashgroup_order_spec(agg, sort.plan, k) else {
        return Ok(HgBuild::Refused);
    };
    ::nodeagg::agg_hashgroup_begin(agg, estate, order)?;
    trace_feed("sorted-agg hash-grouped distinct drive engaged");
    if text_keys > 0 {
        trace_feed("hash-grouped distinct arm: text group keys armed");
    }
    // Filtered grouped-distinct batch feed (named-kernels-distinct): the
    // batched fast leg's staged views need (a) the PREWHERE lane's forced
    // prefix to COVER the key/arg/fold columns — the lane arms first-wins
    // and never widens (`seq_scan_cb_prewhere_arm` keeps an armed lane), so
    // the ask must ride the FIRST arm — and (b) the lane fill to
    // materialize those columns (`lane_fill_wanted` masks non-lane-read
    // columns once a lane program arms). Resolve the batch column map
    // BEFORE the staging arm, pre-arm PREWHERE with the covering ask on
    // qual-bearing pgrcolumnar scans, and register the feed's columns as lane
    // reads. Every refusal leaves the per-row path byte-identically (the
    // views refuse per batch).
    let batch_cols = if distincthash_batch_enabled() {
        ::nodeagg::agg_hashgroup_batch_shape(
            agg,
            distincthash_fold_enabled(),
            distincthash_textbatch_enabled(),
        )
        .and_then(|shape| {
                let map = |att: u16| -> Option<u16> {
                    match ss.ss.ps_ProjInfo.as_ref() {
                        None => Some(att),
                        Some(p) => {
                            let cols = p.pi_state.scan_proj_cols()?;
                            if (att as usize) >= cols.n as usize {
                                return None;
                            }
                            match cols.cols[att as usize] {
                                ::execexpr::ScanProjCol::Var { attnum } => Some(attnum),
                                _ => None,
                            }
                        }
                    }
                };
                let key_cols: Option<Vec<u16>> =
                    shape.key_atts.iter().map(|&a| map(a)).collect();
                let arg_cols: Option<Vec<u16>> =
                    shape.set_args.iter().map(|&a| map(a)).collect();
                let fold_cols: Option<Vec<u16>> =
                    shape.fold_atts.iter().map(|&a| map(a)).collect();
                let cols = HgBatchCols {
                    key_cols: key_cols?,
                    arg_cols: arg_cols?,
                    fold_cols: fold_cols?,
                };
                // Arm the sidecar vocab only once the whole column map held
                // (a fold-armed build whose batch never engages would waste
                // the per-group sidecar words for nothing).
                if !shape.vocab.is_empty() {
                    trace_feed("hash-grouped distinct fold vocab armed");
                    ::nodeagg::agg_hashgroup_arm_fold(agg, shape.vocab);
                }
                Some(cols)
        })
    } else {
        None
    };
    // Pre-arm PREWHERE with the feed's covering column ask (qual-bearing
    // pgrcolumnar scans; a no-qual/refused arm returns false and the ordinary
    // staging below proceeds). Idempotent with `arm_scan_staging`'s own
    // PREWHERE arm — an armed lane is kept there, and the ask=0 re-arm
    // early-returns.
    if let Some(b) = &batch_cols {
        let ask = b
            .key_cols
            .iter()
            .chain(b.arg_cols.iter())
            .chain(b.fold_cols.iter())
            .map(|&c| c as i32 + 1)
            .max()
            .unwrap_or(0);
        if ::nodeseqscan::seq_scan_is_pgrcolumnar(ss)
            && ::nodeseqscan::seq_scan_cb_prewhere_arm(ss, estate, ask)?
        {
            trace_feed("hash-grouped distinct batch feed: prewhere pre-armed");
        }
    }
    arm_scan_staging(
        ss,
        estate,
        ScanFeedShape::RowFeed { ctx: "hashgroup distinct feed", stitch: true },
    )?;
    let outer_desc = outer_desc.as_ref().expect("Sort already ended").clone();
    let batch = match batch_cols {
        Some(b) => {
            trace_feed("hash-grouped distinct batch accept armed");
            let ask = b
                .key_cols
                .iter()
                .chain(b.arg_cols.iter())
                .chain(b.fold_cols.iter())
                .map(|&c| c as i32 + 1)
                .max()
                .unwrap_or(0);
            // A live PREWHERE lane whose forced prefix covers the feed's
            // ask already stages everything the views read (survivor
            // completing deform fills every prefix column; the qual's
            // dict lanes gather back to Raw post-qual). Otherwise arm the
            // offset-free columnar staging covering the key+arg columns
            // (codedgroup's arm precedent; idempotent/co-arm-aware, false
            // on heap). Failure just leaves the per-row path (the views
            // refuse per batch).
            if !::nodeseqscan::seq_scan_cb_lane_covers(ss, ask) {
                let _ = ::nodeseqscan::seq_scan_cb_columnar_arm(ss, estate, ask, None);
            }
            Some(b)
        }
        None => None,
    };
    // Force a forward child read for the feed's duration (`sort_feed`'s
    // discipline — this drain replaces the sort's own feed).
    let dir = estate.es_direction;
    estate.es_direction = ::types_scan::sdir::ForwardScanDirection;
    let mut sink = HashGroupDistinctSink {
        agg,
        sort,
        outer_desc,
        nkeys: k,
        degraded: false,
        batch,
        fast_rows: 0,
        slow_rows: 0,
    };
    let fed = drain_pipeline(ss, &mut SeqScanSource, &mut SeqScanFilterProject, &mut sink, estate);
    let degraded = sink.degraded;
    estate.es_direction = dir;
    fed?;
    if degraded {
        return Ok(HgBuild::Degraded);
    }
    ::nodeagg::agg_hashgroup_finish_build(agg, estate)?;
    Ok(HgBuild::Emit)
}

/// Emit loop over the hash-grouped table: one HAVING-passing group per PG
/// pull, in the plan Sort's prefix order (C's group order). CFI per group —
/// the pull loop's per-ExecSort-fetch cadence.
fn hashgroup_emit<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    loop {
        ::postgres_seams::check_for_interrupts::call()?;
        match ::nodeagg::agg_hashgroup_emit_next(agg, estate)? {
            None => return Ok(None),
            Some(None) => continue,
            Some(Some(id)) => return Ok(Some(id)),
        }
    }
}

/// Emit loop over the runtime distinct sink's PAREMIT buckets (pardistinct
/// paremit section doc): pre-formed rows in the plan Sort's prefix order,
/// one row per pull — no HAVING on admitted shapes, so there is no
/// group-rejected continue arm. CFI per group, the pull loop's cadence.
fn pdemit_emit<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    ::postgres_seams::check_for_interrupts::call()?;
    ::nodeagg::agg_pdemit_emit_next(agg, estate)
}

// ===========================================================================
// Dict-code batched exact-DISTINCT grouping (lane-v2-q14feed; nodeagg
// codedgroup.rs holds the state machine + the byte-identity argument). The
// textgroup lane's deferred lever for the near-unique single-text-key shape
// BOTH incumbent arms price per survivor string (the near-unique shape: the hash arm's
// density tier refuses at 1.95 rows/group and the narrow sort pays sort 33%
// + memcmp 21%): group on the (epoch, dict code) INTEGER domain batch-wise —
// per surviving row one direct-map index + one chain append of the DISTINCT
// arg — and resolve code→string once per distinct (epoch, code); the emit
// k-way-merges the per-epoch byte-sorted runs (sorted dicts) into the plan
// Sort's ASC memcmp-tier group order, unioning equal-content states' chains
// into the pertrans exact set through the unchanged finalize tail.
//
// Interned-int-key negative-precedent boundary (dictgroupwire, feded70a7): for a K2 FOLD
// under a selective qual, per-(epoch, code) lazy STRING RESOLVES INTO THE
// HASH TABLE lost to hashing survivor strings directly (survivors-per-epoch
// ≈ codes-per-epoch). This arm's alternative is NOT a hash probe — it is the
// narrowed SORT — and its per-code cost is one image memcpy (no hash, no
// table), with the cross-epoch identity resolved once at emit by the merge
// instead of per (epoch, code) at build. The gepoch upgrade (globaldict
// stitch tables, train-9 format group) deletes even the merge's string
// compares: part-stable byte-rank codes make the merge an integer compare
// and equal-gcode states merge with no memcmp at all — the documented
// increment-2 consumer.
//
// Admission tier: engages ONLY where the hash arm's density tier refuses
// (the two arms partition the density axis at MIN_ROWS_PER_GROUP), pgrcolumnar
// scans only, dict-answered sorted windows only, bare-Var projections, the
// near-unique-key transition shape (one set-mode COUNT(DISTINCT <bare int Var>)).
// Everything else falls through to the hash arm / narrow sort exactly as
// before. Runtime degrade replays every absorbed (key, arg) row into the
// narrowed sort — no residual state, the narrow emit chain runs unchanged.
// ===========================================================================

/// `PGRUST_LANE_V2_CODEFEED` kill switch (default ON inside the lane) — the
/// A/B attribution channel; OFF keeps near-unique-text-key shapes on the narrowed text
/// sort exactly as before this lane.
fn codefeed_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_LANE_V2_CODEFEED").as_deref(), Ok("0") | Ok("off"))
    })
}

/// `PGRUST_LANE_V2_CODEFEED_FORCE=1`: skip the density/memory economics (e2e
/// harness lever — small tables never look near-unique; the runtime degrade
/// still bounds memory).
fn codefeed_force() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        matches!(std::env::var("PGRUST_LANE_V2_CODEFEED_FORCE").as_deref(), Ok("1") | Ok("on"))
    })
}

/// Emit loop over the coded-group merge: one HAVING-passing group per PG
/// pull, in the plan Sort's prefix order. CFI per group (the pull loop's
/// per-ExecSort-fetch cadence).
fn codedgroup_emit<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    loop {
        ::postgres_seams::check_for_interrupts::call()?;
        match ::nodeagg::agg_codedgroup_emit_next(agg, estate)? {
            None => return Ok(None),
            Some(None) => continue,
            Some(Some(id)) => return Ok(Some(id)),
        }
    }
}

/// The one-shot coded-arm degrade: begin the narrowed sort and replay every
/// absorbed (key, arg) row into it (the exact multiset of absorbed survivor
/// rows — codedgroup.rs module doc), then drop the arm's state. The caller
/// feeds all remaining input per-row and the narrow-sort emit chain resumes
/// with NO residual preload.
#[cold]
#[inline(never)]
fn degrade_codedgroup<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    sort: &mut ::nodesort::SortState<'mcx>,
    outer_desc: &std::rc::Rc<::types_tuple::TupleDescData<'static>>,
    k: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    trace_feed("dict-code distinct feed degrading to narrowed sort");
    ::nodesort::sort_lane_begin_narrowed(sort, outer_desc.clone(), k)?;
    let mcx = estate.es_query_cxt;
    while let Some(slot) = ::nodeagg::agg_codedgroup_next_replay(agg) {
        ::nodesort::sort_lane_put_slot(sort, mcx, slot)?;
    }
    ::nodeagg::agg_codedgroup_reset(agg);
    Ok(())
}

/// Probe + build for the dict-code distinct arm (called with the narrow
/// admission proven and `force_distinct_set` armed, BEFORE the sort exists —
/// the hash arm's calling contract). Refused = the caller tries the hash
/// arm, then the narrow-sort feed, exactly as before this lane.
fn try_codedgroup_build<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    sort: &mut ::nodesort::SortState<'mcx>,
    outer: &mut crate::procnode::PlanStateNode<'mcx>,
    outer_desc: &Option<std::rc::Rc<::types_tuple::TupleDescData<'static>>>,
    k: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<HgBuild> {
    /// pg_proc text_lt — the btree text opclass's `<` (ASC; DESC keeps the
    /// incumbent arms: the merge streams ascending byte order only).
    const F_TEXT_LT: ::types_core::Oid = 740;
    if !codefeed_enabled() || k != 1 {
        return Ok(HgBuild::Refused);
    }
    let crate::procnode::PlanStateNode::SeqScan(ss) = outer else {
        return Ok(HgBuild::Refused);
    };
    // Dict lanes exist only on pgrcolumnar scans; heap keeps the incumbents.
    if !::nodeseqscan::seq_scan_is_pgrcolumnar(ss) {
        return Ok(HgBuild::Refused);
    }
    if !::nodeagg::agg_codedgroup_admissible(agg)
        || !::nodeagg::agg_codedgroup_economical(
            agg,
            codefeed_force(),
            sort.plan.plan.plan_rows,
        )
    {
        return Ok(HgBuild::Refused);
    }
    // Emit order admission: the single prefix key is the grouping column,
    // ASC text order under a memcmp-tier collation (varstr_cmp there IS
    // memcmp + length tiebreak — `lanefold::str_collation_safe`, the
    // dictminmax gate; determinism included). NULLS placement is never
    // observed: NULL group keys cannot reach the arm (dict windows carry no
    // NULLs; anything else degrades before being absorbed).
    let sp = sort.plan;
    if sp.sortColIdx.is_empty() || sp.sortColIdx[0] != ::nodeagg::agg_plan_group_cols(agg)[0] {
        return Ok(HgBuild::Refused);
    }
    let Ok(opfn) = ::lsyscache::get_opcode(sp.sortOperators[0]) else {
        return Ok(HgBuild::Refused);
    };
    if opfn != F_TEXT_LT {
        return Ok(HgBuild::Refused);
    }
    let coll = sp.collations[0];
    if coll == 0 || !::lanefold::str_collation_safe(coll) {
        return Ok(HgBuild::Refused);
    }
    // Outer → scan column map: identity for bare scans; through the
    // projection's per-column classification otherwise, with EVERY output
    // column a bare Var (the arm never runs the projection, so computed —
    // possibly volatile — columns must not exist; NULL reps are sound
    // because an Agg output references only grouping columns + aggregates).
    let (key_att, arg_att) = ::nodeagg::agg_codedgroup_key_arg_atts(agg);
    let (key_col, arg_col) = match ss.ss.ps_ProjInfo.as_ref() {
        None => (key_att, arg_att),
        Some(p) => {
            let Some(cols) = p.pi_state.scan_proj_cols() else {
                return Ok(HgBuild::Refused);
            };
            let n = cols.n as usize;
            if (key_att as usize) >= n || (arg_att as usize) >= n {
                return Ok(HgBuild::Refused);
            }
            let var_of = |j: usize| match cols.cols[j] {
                ::execexpr::ScanProjCol::Var { attnum } => Some(attnum),
                _ => None,
            };
            if (0..n).any(|j| var_of(j).is_none()) {
                return Ok(HgBuild::Refused);
            }
            (
                var_of(key_att as usize).expect("checked above"),
                var_of(arg_att as usize).expect("checked above"),
            )
        }
    };
    // Arm the staging: PREWHERE owns qualled scans (dict text tier included
    // — this shape's qual is on the key column itself); the columnar arm then
    // registers the dict-group consumer on the live batch (the multi-key co-arm
    // seam) or arms fresh offset-free staging for bare scans. The ask covers
    // both consumed columns.
    let ask = (key_col.max(arg_col) as i32) + 1;
    if ss.ss.qual.is_some() && !::nodeseqscan::seq_scan_cb_prewhere_arm(ss, estate, ask)? {
        return Ok(HgBuild::Refused);
    }
    if !::nodeseqscan::seq_scan_cb_columnar_arm(ss, estate, ask, Some(key_col)) {
        return Ok(HgBuild::Refused);
    }
    ::nodeagg::agg_codedgroup_begin(agg, estate)?;
    trace_feed("sorted-agg dict-code distinct feed engaged");
    let outer_desc = outer_desc.as_ref().expect("Sort already ended").clone();
    // Force a forward child read for the feed's duration (`sort_feed`'s
    // discipline — this drain replaces the sort's own feed).
    let dir = estate.es_direction;
    estate.es_direction = ::types_scan::sdir::ForwardScanDirection;
    let res = codedgroup_drive(agg, sort, ss, &outer_desc, k, key_col, arg_col, estate);
    estate.es_direction = dir;
    res
}

/// The coded arm's batch drive (the fold feeds' loop shape): one staged
/// window at a time, survivors decided slot-free off the whole-qual bitmap,
/// codes + the DISTINCT-arg lane fed batch-wise into the state machine. Any
/// window the arm cannot host — non-dict / unsorted-dict key chunk,
/// fallback-bearing batch, no slot-free qual verdicts, NULL arg, budget
/// crossing — degrades to the narrowed sort exactly once, at a row-exact
/// boundary (absorbed rows replay; unconsumed rows feed per-row).
#[allow(clippy::too_many_arguments)]
fn codedgroup_drive<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    sort: &mut ::nodesort::SortState<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    outer_desc: &std::rc::Rc<::types_tuple::TupleDescData<'static>>,
    k: usize,
    key_col: u16,
    arg_col: u16,
    estate: &mut EStateData<'mcx>,
) -> PgResult<HgBuild> {
    let mut rows: Vec<u32> = Vec::new();
    let mut degraded = false;
    let mut mode_traced = false;
    loop {
        let n = ::nodeseqscan::seq_scan_next_pagebatch(ss, estate)?;
        if n == 0 {
            // End of scan: drop the scan slot's buffer pin (SeqScanSource
            // end-of-stream parity).
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(ss.ss.ss_ScanTupleSlot), mcx);
            break;
        }
        ::postgres_seams::check_for_interrupts::call()?;
        if degraded {
            // Post-degrade remainder: the narrow-sort arm's per-row feed
            // (emit = qual verdicts + projection, one evaluation per row),
            // word-skipping emit-dead rows (skip-sel cleared bits).
            let skip = {
                let mut w = [0u64; ::exectuples::SOA_BM_WORDS];
                ::nodeseqscan::seq_scan_batch_skip_sel(ss).map(|s| {
                    w[..s.len()].copy_from_slice(s);
                    w
                })
            };
            ::exectuples::for_each_live(skip.as_ref().map(|w| &w[..]), 0, n, |i| -> PgResult<()> {
                if let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(ss, estate, i)? {
                    ::nodesort::sort_lane_put(sort, estate, slot)?;
                }
                Ok(())
            })?;
            continue;
        }
        // Slot-free survivor collection FIRST (`scan_collect_survivors`'
        // Bitmap/All arms, minus the projection gate the admission made
        // moot); one per-batch ExprContext reset stands in for the emit's
        // per-row cadence (the coded absorb allocates no per-tuple memory).
        // A ZERO-SURVIVOR staged window skips BEFORE the lane admission
        // below: a condition-cache hit whose cached verdicts are all-fail
        // legitimately skips the survivor deform (no dict lane is ever
        // published for the window — nodeseqscan's cond_hit arm), and the
        // window carries nothing to absorb either way. Requiring the lane
        // there demoted the WHOLE remaining scan to the narrow sort on the
        // first all-fail hit (the official condcache arm's near-unique shape 1.19 s vs
        // 0.55 s). Zero survivors also implies zero fallback bits (fallback
        // rows OR their bits into the selection), so the fallback-free
        // admission is vacuously met.
        let survivors_decidable = ss.ss.qual.is_none()
            || ::nodeseqscan::seq_scan_batch_whole_qual_sel(ss).is_some();
        if survivors_decidable {
            rows.clear();
            estate.ecxt_mut(ss.ss.ps_ExprContext).reset();
            match ::nodeseqscan::seq_scan_batch_whole_qual_sel(ss) {
                None => rows.extend(0..n),
                Some(sel) => {
                    let nwords = (n as usize).div_ceil(64);
                    let tail_mask = if n % 64 == 0 { u64::MAX } else { (1u64 << (n % 64)) - 1 };
                    for w in 0..nwords {
                        let mut bits = sel[w];
                        if w == nwords - 1 {
                            bits &= tail_mask;
                        }
                        while bits != 0 {
                            rows.push(w as u32 * 64 + bits.trailing_zeros());
                            bits &= bits - 1;
                        }
                    }
                }
            }
            if rows.is_empty() {
                continue;
            }
        }
        // Window admission: a dict-answered SORTED key lane (byte order ==
        // code order — the merge's foundation) over a fallback-free batch,
        // with the whole qual decided by the staged bitmap (per-row emit
        // survivors would re-evaluate rows on the degrade seam; refuse
        // instead — the sort arm evaluates each row exactly once).
        let (lane, all_lane) = {
            let soa = ::nodeseqscan::seq_scan_batch_soa(ss);
            (
                soa.and_then(|s| s.dict_lane(key_col as usize)).filter(|l| l.table.sorted),
                soa.is_some_and(|s| s.fallback_words().iter().all(|&w| w == 0)),
            )
        };
        let Some(lane) = lane.filter(|_| all_lane && survivors_decidable) else {
            degrade_codedgroup(agg, sort, outer_desc, k, estate)?;
            degraded = true;
            // This batch is wholly unconsumed: feed it per-row, word-
            // skipping emit-dead rows (skip-sel cleared bits).
            let skip = {
                let mut w = [0u64; ::exectuples::SOA_BM_WORDS];
                ::nodeseqscan::seq_scan_batch_skip_sel(ss).map(|s| {
                    w[..s.len()].copy_from_slice(s);
                    w
                })
            };
            ::exectuples::for_each_live(skip.as_ref().map(|w| &w[..]), 0, n, |i| -> PgResult<()> {
                if let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(ss, estate, i)? {
                    ::nodesort::sort_lane_put(sort, estate, slot)?;
                }
                Ok(())
            })?;
            continue;
        };
        let (consumed, keep) = {
            let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                .expect("dict window implies the armed SoA");
            // The DISTINCT-arg lane: valid at every selected row (PREWHERE's
            // completing deform fills survivor windows; bare columnar arms
            // fill every row).
            let argv = soa.col_values(arg_col as usize);
            let argn = soa.col_isnull(arg_col as usize);
            let r = ::nodeagg::agg_codedgroup_accept_batch(agg, lane, &rows, argv, argn);
            (r.consumed, r.keep)
        };
        // One-shot code-domain trace (engagement observability): global =
        // part-global stitch codes, local = per-epoch codes + k-way merge.
        if !mode_traced {
            if let Some(g) = ::nodeagg::agg_codedgroup_mode_global(agg) {
                trace_feed(if g { "codedgroup global codes" } else { "codedgroup local codes" });
                mode_traced = true;
            }
        }
        if !keep {
            degrade_codedgroup(agg, sort, outer_desc, k, estate)?;
            degraded = true;
            // Row-exact boundary: absorbed survivors replayed above; the
            // unconsumed survivors of THIS batch feed per-row (the emit's
            // whole-qual kernel verdict is stable — non-volatile by the
            // bitmap translation admission).
            for &i in &rows[consumed..] {
                if let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(ss, estate, i)? {
                    ::nodesort::sort_lane_put(sort, estate, slot)?;
                }
            }
        }
    }
    if degraded {
        ::nodesort::sort_lane_finish(sort, estate)?;
        return Ok(HgBuild::Degraded);
    }
    ::nodeagg::agg_codedgroup_finish_build(agg);
    Ok(HgBuild::Emit)
}

/// Try to let the lane own `Agg(AGG_SORTED) → Sort → scan`: the sort breaker
/// feeds once (pipeline N), then the sorted-agg operator streams the
/// read-back into one group row per PG pull. `None` = refused (caller falls
/// to the per-tuple `exec_agg` over `exec_sort`, byte-safely — see the
/// section doc on call-boundary state compatibility).
#[inline]
/// EA-on-morsels entry for the DISTINCT sink arm (ea-morsels.md §5,
/// inc-1b): under EXPLAIN ANALYZE the ordinary dispatch cannot reach the
/// runtime distinct probe (Instrumented wrappers + the sort fusibility
/// memo), so procnode's EA hook calls this dedicated walk. It mirrors ONLY
/// the gates the uninstrumented run evaluates on the
/// try_own_sorted_agg_over_sort path down to the probe (E4: no gate
/// differs except instrument checks), with zero side effects unless the
/// session is ARMED (an unarmed EA session must not even arm set-mode).
/// The sticky set-mode force is value-safe on the per-tuple interpreter
/// fallback by the arming contract (nodeagg force_distinct_set doc).
pub fn try_own_sorted_distinct_runtime_ea<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    s: &mut crate::procnode::SortNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Armed-only, before any side effect (M5-1: the router's arming read —
    // bench GUC verbatim, else engine=runtime at pgrust.runtime_dop).
    if router::arm_dop(router::ArmClass::Distinct) <= 0
        || !runtime::runtime_enabled()
    {
        return Ok(None);
    }
    // Mid-emit resume of a prior EA-engaged build (the serial top's
    // hashgroup resume, verbatim — the sink adopts through hashgroup emit
    // or the paremit bucket merge; the bypassed Sort must never be fed
    // from the consumed scan).
    if ::nodeagg::agg_pdemit_emitting(agg) {
        return Ok(Some(pdemit_emit(agg, estate)?));
    }
    if ::nodeagg::agg_hashgroup_emitting(agg) {
        return Ok(Some(hashgroup_emit(agg, estate)?));
    }
    // Dynamic per-call gates (serial arm verbatim; no stat ticks — the EA
    // walk never perturbs the serial cadence).
    if estate.es_epq_active {
        return Ok(None);
    }
    // The grouped narrow-distinct decision (serial arm verbatim).
    let plain_admissible = ::nodeagg::agg_sorted_lane_admissible(agg);
    let mut narrow: Option<usize> = None;
    if !plain_admissible || ::nodeagg::agg_distinct_set_forced(agg) {
        let sp = s.state.plan;
        let k = ::nodeagg::agg_plan_group_cols(agg).len();
        let ok = ::nodeagg::agg_sorted_distinct_narrow_admissible(agg)
            && !s.state.sort_done()
            && !s.state.bounded
            && k >= 1
            && (sp.numCols as usize) > k
            && sp.sortColIdx.len() >= k
            && {
                let mut a: Vec<i16> = sp.sortColIdx[..k].to_vec();
                let mut b: Vec<i16> = ::nodeagg::agg_plan_group_cols(agg).to_vec();
                a.sort_unstable();
                b.sort_unstable();
                a == b
            };
        if ok {
            narrow = Some(k);
        }
    }
    let Some(k) = narrow else { return Ok(None) };
    // Sort-side verdict, instrument gates vacated (serial memo untouched).
    if sort_refuse_reason_runtime_ea(s, estate)?.is_some() {
        return Ok(None);
    }
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    let crate::procnode::SortNode { state, outer, outer_desc, rd_shape_refused, .. } = s;
    if state.sort_done() {
        // The interpreter (or a serial arm) already consumed the feed on an
        // earlier call — it owns the node; never engage after (E4/agg-arm
        // discipline).
        return Ok(None);
    }
    ::postgres_seams::check_for_interrupts::call()?;
    // Arm set-mode BEFORE any input (serial site verbatim; sticky, and
    // value-safe on the per-tuple fallback per the arming doc).
    ::nodeagg::agg_sorted_force_distinct_set(agg);
    // Peel the wrapper for the probe: the runtime's workers run
    // uninstrumented; the bypassed nodes' Instrumentation is written from
    // the merged partials on clean completion.
    let outer: &mut crate::procnode::PlanStateNode<'mcx> = match &mut **outer {
        crate::procnode::PlanStateNode::Instrumented(w) => &mut w.inner,
        o => o,
    };
    runtime_distinct::try_own_sorted_distinct_runtime(
        agg,
        state,
        outer,
        outer_desc,
        rd_shape_refused,
        k,
        estate,
    )
}

pub fn try_own_sorted_agg_over_sort<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    s: &mut crate::procnode::SortNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Hash-grouped arm mid-emit resume — BEFORE the dynamic gates (the arm's
    // section doc: the plan's Sort was bypassed and must never be fed from
    // the now-exhausted scan; the gates cannot flip mid-node here).
    if ::nodeagg::agg_pdemit_emitting(agg) {
        return Ok(Some(pdemit_emit(agg, estate)?));
    }
    if ::nodeagg::agg_hashgroup_emitting(agg) {
        return Ok(Some(hashgroup_emit(agg, estate)?));
    }
    // Dict-code distinct arm mid-emit resume — same contract (its build
    // also consumed the scan; the plan's Sort must never be fed).
    if ::nodeagg::agg_codedgroup_emitting(agg) {
        return Ok(Some(codedgroup_emit(agg, estate)?));
    }
    // Dynamic per-call gates (mirror the bare-sort breaker).
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::SortFeed, RefuseReason::Epq);
        return Ok(None);
    }
    // Agg-side admission (static shape; ticked per offered call, the hashed
    // breaker's AggNotDrainable cadence).
    //
    // Grouped narrow-sort arm (v2, the sorted grouped exact-DISTINCT shape): an
    // AGG_SORTED node whose DISTINCT aggregates ride the plan Sort's
    // distinct-arg SUFFIX keys (aggpresorted adjacent-dedup) fails the plain
    // admission — but when every internal-sort entry is set-CAPABLE, every
    // transition is order-insensitive-exact, and the Sort's key prefix is
    // exactly the grouping columns, the suffix's only observable effect is
    // intra-group row order, which nothing observes once the drive arms
    // set-mode. The drive then feeds the sort with the comparator NARROWED
    // to the group prefix (`sort_lane_begin_narrowed`) and the exact sets
    // replace the dedup: byte-identical output (same groups, same group
    // order, same exact values), with the suffix compares and the per-row
    // dedup calls deleted. Armed only BEFORE the sort is built (arming
    // decides the feed's construction); the sticky force keeps the plain
    // admission true on later calls and any per-tuple fallback value-safe.
    let mut narrow: Option<usize> = None;
    let plain_admissible = ::nodeagg::agg_sorted_lane_admissible(agg);
    // Probe the narrow shape when the plain admission failed (the arm's
    // first engagement) OR when a prior call armed it (a rescan-rebuilt sort
    // must narrow again — the sticky force makes the plain admission true).
    if !plain_admissible || ::nodeagg::agg_distinct_set_forced(agg) {
        let sp = s.state.plan;
        let k = ::nodeagg::agg_plan_group_cols(agg).len();
        let ok = ::nodeagg::agg_sorted_distinct_narrow_admissible(agg)
            && !s.state.sort_done()
            && !s.state.bounded
            && k >= 1
            && (sp.numCols as usize) > k
            && sp.sortColIdx.len() >= k
            && {
                // Prefix == group columns as a MULTISET (order within the
                // prefix is free: grouping adjacency only needs the rows
                // prefix-sorted, whichever prefix order).
                let mut a: Vec<i16> = sp.sortColIdx[..k].to_vec();
                let mut b: Vec<i16> = ::nodeagg::agg_plan_group_cols(agg).to_vec();
                a.sort_unstable();
                b.sort_unstable();
                a == b
            };
        if !plain_admissible && !ok {
            stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AggNotDrainable);
            return Ok(None);
        }
        if ok {
            narrow = Some(k);
        }
    }
    // Sort-side structural verdict — the bare-sort arm's memo, shared (the
    // refusal ticks once per node whichever arm probes first).
    let fusible = match s.lane_fusible {
        Some(v) => v,
        None => {
            let refuse = sort_refuse_reason(s, estate)?;
            if let Some(r) = refuse {
                stats::tick_refused(ShapeClass::SortFeed, r);
            }
            let v = refuse.is_none();
            s.lane_fusible = Some(v);
            v
        }
    };
    if !fusible {
        return Ok(None);
    }
    // exec_agg's top-of-call guard: a drained agg stays drained.
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    let crate::procnode::SortNode { state, outer, outer_desc, rd_shape_refused, .. } = s;
    if !state.sort_done() {
        // C's CHECK_FOR_INTERRUPTS at the feed call's ExecSort entry (the
        // emit chain's source checks per subsequent fetch).
        ::postgres_seams::check_for_interrupts::call()?;
        if let Some(k) = narrow {
            // Arm set-mode BEFORE any input (sticky; the arming doc).
            ::nodeagg::agg_sorted_force_distinct_set(agg);
            // M2 runtime DISTINCT sink first (armed only under
            // PGRUST_RUNTIME=1 + pgrust.runtime_distinct_pool > 0, falling
            // back to pgrust.runtime_scan_pool — absent, two placeholder GUC
            // lookups (alloc-free when never SET) and fall through). Owns
            // the node on engagement; refusal/fallback keeps every serial
            // arm below byte-identical.
            match runtime_distinct::try_own_sorted_distinct_runtime(
                agg, state, &mut **outer, outer_desc, rd_shape_refused, k, estate,
            )? {
                Some(row) => return Ok(Some(row)),
                None => {}
            }
            // Dict-code distinct arm first: it owns EXACTLY the density band
            // the hash arm's tier refuses (the near-unique-text-key class;
            // the two admissions partition the density axis), so ordering
            // between the two arms is arbitrary — coded-first keeps the
            // engaged arm's traces unambiguous. Refused falls to the hash
            // arm, then the narrow-sort feed, exactly as before.
            match try_codedgroup_build(agg, state, &mut **outer, outer_desc, k, estate)? {
                HgBuild::Emit => {
                    // One OWNED tick per lane-owned build event; the emit
                    // owns the node from here (no sort exists).
                    stats::tick_owned(ShapeClass::AggBuild);
                    return Ok(Some(codedgroup_emit(agg, estate)?));
                }
                HgBuild::Degraded => {
                    // The narrowed sort was fed and finished inside the
                    // degrade (a real sort-feed event); the narrow-sort emit
                    // chain below resumes over it — NO residual preload (the
                    // coded degrade replayed every absorbed row).
                    stats::tick_owned(ShapeClass::SortFeed);
                    stats::tick_owned(ShapeClass::AggBuild);
                }
                // Hash-grouped arm next (its own admission tier; Refused
                // keeps the narrow-sort feed exactly as before).
                HgBuild::Refused => {
                    match try_hashgroup_build(agg, state, &mut **outer, outer_desc, k, estate)? {
                        HgBuild::Emit => {
                            // One OWNED tick per lane-owned build event; the
                            // emit owns the node from here (no sort exists).
                            stats::tick_owned(ShapeClass::AggBuild);
                            return Ok(Some(hashgroup_emit(agg, estate)?));
                        }
                        HgBuild::Degraded => {
                            // The narrowed sort was fed and finished inside
                            // the degrade (a real sort-feed event); the
                            // narrow-sort emit chain below resumes over it,
                            // preloading residual group state at each group
                            // begin (nodeagg's initialize_aggregates hook).
                            stats::tick_owned(ShapeClass::SortFeed);
                            stats::tick_owned(ShapeClass::AggBuild);
                        }
                        HgBuild::Refused => {
                            trace_feed("sorted-agg distinct-set narrowed sort feed armed");
                            // The shared feed threads the narrow-key count in.
                            if !sort_feed_if_needed(
                                state,
                                &mut **outer,
                                outer_desc,
                                narrow,
                                estate,
                            )? {
                                return Ok(None);
                            }
                            stats::tick_owned(ShapeClass::AggBuild);
                        }
                    }
                }
            }
        } else {
            // The shared feed (a sort under a sorted agg is never bounded —
            // no LIMIT pushdown crosses the agg — so its seqscan arm's top-k
            // probe no-ops; false = the agg-child arm's spill refuse,
            // byte-safe).
            if !sort_feed_if_needed(state, &mut **outer, outer_desc, narrow, estate)? {
                return Ok(None);
            }
            // One OWNED tick per lane-owned sorted-agg stream start
            // (feed/build EVENTS, once per (re)scan; the sort feed ticked
            // its own class).
            stats::tick_owned(ShapeClass::AggBuild);
        }
    }
    // Emit phase (every call): sort read-back → sorted-agg operator → root,
    // one qual-passing group row per PG pull.
    let mut op = SortedAggOp { agg, group_open: false };
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step_chain(
        state,
        &mut SortEmitSourceCfi,
        &mut SortEmit,
        &mut op,
        &mut root,
        estate,
    )?))
}

/// Try to let the lane own `Agg(AGG_SORTED) → IndexScan` (index order feeds
/// the grouping directly — no Sort node). Engagement accounting: the
/// per-pull indexscan class ticks (owned per admitted feed decision, the
/// class's documented cadence); agg-side refusals tick AggNotDrainable per
/// offered call.
#[inline]
pub fn try_own_sorted_agg_over_index_scan<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    is: &mut ::nodeindexscan::IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if !::nodeagg::agg_sorted_lane_admissible(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AggNotDrainable);
        return Ok(None);
    }
    // Child refuse-set verbatim (dynamic EPQ/direction gates included; ticks
    // under the indexscan class, per call).
    if !index_scan_fusible(is, estate) {
        return Ok(None);
    }
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    let mut op = SortedAggOp { agg, group_open: false };
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step_chain(
        is,
        &mut IndexScanSource,
        &mut IndexScanEmit,
        &mut op,
        &mut root,
        estate,
    )?))
}

/// Try to let the lane own `Agg(AGG_SORTED) → IndexOnlyScan`. Accounting as
/// the IndexScan arm (per-pull indexonlyscan class).
#[inline]
pub fn try_own_sorted_agg_over_index_only_scan<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ios: &mut ::nodeindexonlyscan::IndexOnlyScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if !::nodeagg::agg_sorted_lane_admissible(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AggNotDrainable);
        return Ok(None);
    }
    if !index_only_scan_fusible(ios, estate) {
        return Ok(None);
    }
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    let mut op = SortedAggOp { agg, group_open: false };
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step_chain(
        ios,
        &mut IndexOnlyScanSource,
        &mut IndexOnlyScanEmit,
        &mut op,
        &mut root,
        estate,
    )?))
}

// ===========================================================================
// Sorted-agg over SeqScan (lane-v2-sortedfold): the sort-free GroupAggregate
// shape — clustered/footer-sorted pgrcolumnar banks plan `Agg(AGG_SORTED) →
// SeqScan` with NO Sort node (the pathkeys come from the store order), so
// neither the hashed fold breaker (AGG_HASHED only) nor the sorted-agg-over-
// Sort arm can host it. Two drives, chosen once per node:
//
//   * Fold (`sorted_fold_step`): per staged column window, detect the group
//     boundaries over the staged SoA key lanes (width-masked raw-datum
//     compare — exactly the ported grouping-equality program's verdict under
//     the node's representational-equality grant, NULL keys grouping
//     together via the null-pair compare), then run the admitted PLAIN
//     transitions as ONE `lanefold::fold_batch` per group run — the hashed
//     feed's kernels (strlenfold's charlen included), fed per group run
//     instead of per table. Group prologue (first row), boundary emit
//     (finalize + HAVING + project), residual transitions and fallback rows
//     all delegate per row to the same `agg_sorted_*` seams the per-row
//     operator uses.
//   * PerRow (`sorted_perrow_step`): the SortedAggOp chain over the scan's
//     staged batches (SeqScanSource → SeqScanFilterProject → SortedAggOp).
//     The pgrcolumnar incumbent is the per-pull Volcano drive, so the staged
//     window decode alone wins (the noqualfeed economics); hosts every
//     `agg_sorted_lane_admissible` shape incl. exact-DISTINCT set entries.
//
// pgrcolumnar scans only: the planner produces this shape from pgrcolumnar footer
// pathkeys; heap SeqScans are never ordered, and heap's incumbent drives own
// heap agg shapes anyway (admission economics §4).
//
// Byte-identity: same rows through the same qual in the same order (the
// staged bitmap IS the kernel/PREWHERE verdict; per-row emits re-check
// exactly as the per-row drive; requal/resid shapes take the per-row-emit
// fold mode); group boundaries are the grouping-equality verdicts
// (representational grant); every fold kernel is bit-for-bit equal to C's
// transition semantics on admitted/guard-proven data (lanefold contract) and
// guarded batches that fail re-proof demote WHOLESALE to the checked per-row
// program; finalize/HAVING/project is `agg_sorted_emit` per group in input
// order — group emit order = input order = C's order. Cross-call state is
// node-resident (scan lane cursor + persort pending slot): every mid-stream
// pause happens exactly at the pull loop's call boundary (group closed,
// boundary tuple saved in the pending slot), so a per-call fallback to
// `exec_agg` is byte-safe in both directions, and the resume re-derives the
// open group's key from the group's first tuple (`agg_sorted_group_key`).
// ===========================================================================

/// Group-key columns the sorted-fold boundary compare can host: at most this
/// many by-value fixed-width grouping columns (the analytics-bank shapes carry 1-2).
const SORTED_FOLD_MAX_KEYS: usize = 4;

/// The staged group-key column set for the sorted-fold arm: 0-based scan
/// column + attlen per grouping column, in grpColIdx order. None = a
/// grouping column is by-ref / dropped / out of range — the per-row drive
/// keeps the shape.
#[derive(Clone, Copy)]
struct SortedFoldKeys {
    n: usize,
    cols: [(u16, i16); SORTED_FOLD_MAX_KEYS],
}

fn sorted_fold_key_cols<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
) -> Option<SortedFoldKeys> {
    let group = ::nodeagg::agg_plan_group_cols(agg);
    if group.is_empty() || group.len() > SORTED_FOLD_MAX_KEYS {
        return None;
    }
    let rel = ss.ss.ss_currentRelation.as_ref()?;
    let atts: &[_] = &rel.rd_att.compact_attrs;
    let mut cols = [(0u16, 0i16); SORTED_FOLD_MAX_KEYS];
    for (k, &attno) in group.iter().enumerate() {
        if attno < 1 {
            return None;
        }
        let c = (attno - 1) as usize;
        let att = atts.get(c)?;
        // By-value fixed-width only: the raw-datum compare's domain (by-ref
        // keys would need byte-image walks; representational TEXTEQ shapes
        // stay per-row in v1).
        if !att.attbyval || !matches!(att.attlen, 1 | 2 | 4 | 8) || att.attisdropped {
            return None;
        }
        cols[k] = (c as u16, att.attlen);
    }
    Some(SortedFoldKeys { n: group.len(), cols })
}

/// Width-masked by-value datum equality: exactly the representational
/// grouping-equality verdict for the admitted key widths (bool/int2/int4/
/// int8/date/timestamp — `group_eq_representational`'s operator set), with
/// any sign-extension convention differences between producers masked off.
#[inline(always)]
fn sorted_key_datum_eq(a: ::datum::Datum, b: ::datum::Datum, attlen: i16) -> bool {
    let mask = match attlen {
        1 => 0xffu64,
        2 => 0xffffu64,
        4 => 0xffff_ffffu64,
        _ => u64::MAX,
    };
    (a.as_u64() ^ b.as_u64()) & mask == 0
}

/// Arm fold LENGTH lanes (lane-v2-asciilen) on the staged batch: for every
/// fold plan column whose transitions are ALL one length kind (VarLenBytes
/// xor VarLenChars; CountAny rides along — it reads only isnull) and which
/// is not a grouping column, ask the staging to answer the column as i64
/// lengths (`seq_scan_batch_len_want` audits the datum-reading
/// co-consumers). On dict-encoded pgrcolumnar chunks the fill then reads ONE
/// per-code length table entry per row (string bytes touched once per
/// distinct value per row group) — the fold never materializes or
/// dereferences a per-row varlena datum; Raw chunks read the varlena header
/// (bytes) or run C's exact mb walk (chars) at the fill. A refused arm
/// keeps the datum-lane kernels byte-identically.
fn arm_fold_len_lanes<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
) {
    let Some(plan) = ::nodeagg::agg_lanefold_plan(agg) else { return };
    let group = ::nodeagg::agg_plan_group_cols(agg);
    for &c in plan.cols.iter() {
        let (mut bytes, mut chars, mut other) = (false, false, false);
        for t in plan.trans.iter() {
            if t.col != c
                || matches!(t.kind, ::lanefold::LaneKind::CountAny | ::lanefold::LaneKind::CountStar)
            {
                continue;
            }
            match t.width {
                ::lanefold::LaneWidth::VarLenBytes => bytes = true,
                ::lanefold::LaneWidth::VarLenChars => chars = true,
                _ => other = true,
            }
        }
        if other || bytes == chars {
            continue; // not a length column, or mixed kinds share the lane
        }
        if group.iter().any(|&a| a >= 1 && (a - 1) as u16 == c) {
            continue;
        }
        if ::nodeseqscan::seq_scan_batch_len_want(ss, c, chars) {
            lane_trace(&format!(
                "fold length lane armed (col {c}, {})",
                if chars { "chars" } else { "bytes" }
            ));
        }
    }
}

/// The structural lane choice for the sorted-agg-over-SeqScan drive, decided
/// once per node: Fold when the node passes the sorted-fold admission AND
/// the group keys are lane-comparable AND the staging (PREWHERE for qualled
/// scans, the offset-free columnar arm otherwise) covers every fold + key
/// column; PerRow otherwise (always available — the pgrcolumnar incumbent is the
/// per-pull Volcano drive).
fn decide_sorted_agg_lane<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<AggLaneChoice> {
    if ss.ss.ps_ProjInfo.is_none() && ::nodeagg::agg_sorted_fold_admissible(agg) {
        if let Some(keys) = sorted_fold_key_cols(agg, ss) {
            let plan =
                ::nodeagg::agg_lanefold_plan(agg).expect("fold admission implies a plan");
            let mut maxcol = 0i32;
            for &c in plan.cols.iter().chain(plan.vguards.iter()) {
                maxcol = maxcol.max(c as i32);
            }
            for &(c, _) in &keys.cols[..keys.n] {
                maxcol = maxcol.max(c as i32);
            }
            let prefix = maxcol + 1;
            // Qualled scans require the PREWHERE lane (it owns the staging
            // and the selection bitmap; its forced prefix is widened to our
            // ask). Bare scans arm the offset-free columnar staging. A
            // refusal keeps the per-row drive — byte-safe either way.
            let armed = if ss.ss.qual.is_some() {
                ::nodeseqscan::seq_scan_cb_prewhere_arm(ss, estate, prefix)?
            } else {
                ::nodeseqscan::seq_scan_cb_columnar_arm(ss, estate, prefix, None)
            };
            if armed && ::nodeseqscan::seq_scan_batch_soa(ss).is_some() {
                arm_fold_len_lanes(agg, ss);
                trace_feed("sorted-agg fold drive armed (seqscan)");
                return Ok(AggLaneChoice::Fold);
            }
        }
    }
    // Per-row drive staging: PREWHERE/kernel-bitmap qual vectorization on
    // the staged windows (the drained per-row feed's own arm shape).
    arm_scan_staging(
        ss,
        estate,
        ScanFeedShape::RowFeed { ctx: "sorted agg per-row feed", stitch: true },
    )?;
    trace_feed("sorted-agg per-row drive armed (seqscan)");
    Ok(AggLaneChoice::PerRow)
}

/// Try to let the lane own `Agg(AGG_SORTED) → SeqScan` (section doc above).
/// `None` = refused — the caller falls to the per-tuple `exec_agg` over
/// `exec_seq_scan`, byte-safely (call-boundary state compatibility, section
/// doc).
#[inline]
pub fn try_own_sorted_agg_over_seq_scan<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    choice: &mut Option<AggLaneChoice>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // pgrcolumnar scans only (section doc): heap falls through silently — the
    // shape does not arise there and the fused drives own heap aggs.
    if !::nodeseqscan::seq_scan_is_pgrcolumnar(ss) {
        return Ok(None);
    }
    // Scan-side refuse-set: dynamic EPQ/direction gates re-checked per call;
    // structural verdict memoized on the node (ticks under the CbScan class).
    if !seq_scan_fusible(ss, estate)? {
        return Ok(None);
    }
    // Rescan re-decide: the engagement is per-execution — a rescan dropped
    // the adopted emit state (exec_rescan_agg) and reset agg_done, so a
    // memoized SortedSink with neither is a FRESH build: re-probe the
    // engagement (or fall to a serial decide) instead of draining a state
    // that no longer exists.
    if *choice == Some(AggLaneChoice::SortedSink)
        && !::nodeagg::sortedsink::agg_sorted_sink_emitting(agg)
        && !::nodeagg::agg_is_done(agg)
    {
        *choice = None;
    }
    let c = match *choice {
        Some(c) => c,
        None => {
            // M2 runtime ORDERED-GROUPED arm first (the sorted-arm lane; armed
            // only under PGRUST_RUNTIME=1 + pgrust.runtime_agg_pool > 0 —
            // absent, two cheap gate reads and fall through). Engagement
            // runs the whole parallel attempt here, before anything is
            // consumed; every refusal/fallback keeps the serial decide
            // below byte-identical.
            let c = if runtime_agg_sorted::try_engage_sortedagg_runtime(agg, ss, estate)? {
                AggLaneChoice::SortedSink
            } else {
                let c = decide_sorted_agg_lane(agg, ss, estate)?;
                // One OWNED tick per memoized ownership decision (the sorted
                // stream's build event; the per-group pulls all ride it).
                stats::tick_owned(ShapeClass::AggBuild);
                c
            };
            *choice = Some(c);
            c
        }
    };
    if c == AggLaneChoice::Refuse {
        return Ok(None);
    }
    // exec_agg's top-of-call guard: a drained agg stays drained.
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    Ok(Some(match c {
        AggLaneChoice::SortedSink => runtime_agg_sorted::sorted_sink_emit_step(agg, estate)?,
        AggLaneChoice::Fold => sorted_fold_step(agg, ss, estate)?,
        _ => sorted_perrow_step(agg, ss, estate)?,
    }))
}

/// The per-row sorted drive: one PG pull's worth of the SortedAggOp chain
/// over the scan's staged batches — the sorted-agg-over-IndexScan pipeline
/// with the SeqScan source/emit pair (both proven pieces, composed).
fn sorted_perrow_step<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    let mut op = SortedAggOp { agg, group_open: false };
    let mut root = RootAdapter::new(None);
    pull_step_chain(ss, &mut SeqScanSource, &mut SeqScanFilterProject, &mut op, &mut root, estate)
}

/// Granule length-stats meta-fold context (lane-v2-lenfooter): the sorted
/// fold drive may consume whole INTERIOR granules of the open group from v7
/// footer metadata — no decode, no staging — when every admitted transition
/// is answerable from (passing rows, Σ octet_length) and the qual is absent
/// or exactly `col <> ''` on the length column itself (the arithmetic:
/// empty strings are precisely the rejected rows AND contribute zero to the
/// byte-length sum, and pgrcolumnar stores no NULLs, so filtered count =
/// rows − empties and the filtered sum IS the footer sum).
struct SortedMetaFold {
    // Columns whose footer stats the peek fetches: the plan's VarLenBytes
    // length columns plus (if any) the qual column.
    peek_cols: [u16; 4],
    npeek: usize,
    // Index into peek_cols of the `<> ''` qual column; None = no qual.
    qual_idx: Option<usize>,
}

fn sorted_fold_meta_ctx<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
    keys: &SortedFoldKeys,
) -> Option<SortedMetaFold> {
    if !metaagg_enabled() {
        return None;
    }
    // The key compare below widens by attlen: only 2/4/8-byte keys arise on
    // pgrcolumnar (i16/i32/date/i64/timestamp), but stay fail-closed.
    for &(_, attlen) in &keys.cols[..keys.n] {
        if !matches!(attlen, 2 | 4 | 8) {
            return None;
        }
    }
    let plan = ::nodeagg::agg_lanefold_plan(agg)?;
    let cols = ::lanefold::granule_meta_len_cols(plan)?;
    let qual = ::nodeseqscan::seq_scan_meta_qual_shape(ss)?;
    if let Some(qc) = qual {
        // Under `qc <> ''` only length sums over qc itself stay exact:
        // empty-string rows of qc contribute zero to qc's sum but arbitrary
        // bytes to any OTHER length column's.
        if !cols.iter().all(|&c| c == qc) {
            return None;
        }
    }
    let mut peek_cols = [0u16; 4];
    let mut npeek = 0usize;
    for &c in cols.iter() {
        if npeek == peek_cols.len() {
            return None;
        }
        peek_cols[npeek] = c;
        npeek += 1;
    }
    let qual_idx = match qual {
        None => None,
        Some(qc) => Some(match peek_cols[..npeek].iter().position(|&c| c == qc) {
            Some(i) => i,
            None => {
                if npeek == peek_cols.len() {
                    return None;
                }
                peek_cols[npeek] = qc;
                npeek += 1;
                npeek - 1
            }
        }),
    };
    Some(SortedMetaFold { peek_cols, npeek, qual_idx })
}

/// Widen a by-value key datum to the decode-value domain granule zone
/// entries live in (sign-extension per attlen — exactly how the pgrcolumnar
/// decode widens i16/i32/date columns).
#[inline]
fn sorted_key_widen(d: ::datum::Datum, attlen: i16) -> i64 {
    match attlen {
        2 => d.as_i16() as i64,
        4 => d.as_i32() as i64,
        _ => d.as_i64(),
    }
}

/// Consume 0+ whole granules of the OPEN group from footer metadata:
/// peek each upcoming fresh granule; while it is key-constant AT the open
/// group's key, fold its transitions from (rows, Σ octet_length, empties)
/// and skip its decode entirely. Stops at a mid-granule position, a
/// non-meta granule, a key change (boundary granules stage and fold/emit
/// normally), or scan end. Byte-identity: the granule's rows are exactly
/// the rows next_window would stage, all same-key (zone min == max) and
/// non-fallback (nothing staged); the qual bitmap over them is the length
/// predicate the footer arithmetic derives; fold_granule_meta's state
/// mutations are fold_batch's own on that selection.
fn sorted_fold_meta_granules<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    keys: &SortedFoldKeys,
    cur_key: &[(::datum::Datum, bool); SORTED_FOLD_MAX_KEYS],
    mf: &SortedMetaFold,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let nkeys = keys.n;
    let mut key_cols = [0u16; SORTED_FOLD_MAX_KEYS];
    let mut want_key = [0i64; SORTED_FOLD_MAX_KEYS];
    for k in 0..nkeys {
        // NULL keys never arise from a pgrcolumnar scan (no NULLs stored), but a
        // null open-group key would not be zone-comparable — refuse.
        if cur_key[k].1 {
            return Ok(());
        }
        key_cols[k] = keys.cols[k].0;
        want_key[k] = sorted_key_widen(cur_key[k].0, keys.cols[k].1);
    }
    let mut key_mm = [(0i64, 0i64); SORTED_FOLD_MAX_KEYS];
    let mut stats = [(0u64, 0u32, 0u32); 4];
    let mut consumed = 0u64;
    loop {
        match ::nodeseqscan::seq_scan_granule_meta_peek(
            ss,
            estate,
            &key_cols[..nkeys],
            &mf.peek_cols[..mf.npeek],
            &mut key_mm[..nkeys],
            &mut stats[..mf.npeek],
        )? {
            ::tableam::CbGranuleMetaStep::Meta { rows } => {
                let same = (0..nkeys)
                    .all(|k| key_mm[k].0 == key_mm[k].1 && key_mm[k].0 == want_key[k]);
                if !same {
                    break;
                }
                let passing = rows as i64
                    - mf.qual_idx.map_or(0, |i| stats[i].2 as i64);
                if passing > 0 {
                    let plan =
                        ::nodeagg::agg_lanefold_plan(agg).expect("meta ctx proved the plan");
                    // SAFETY: pergroup contract identical to flush_run's
                    // fold_batch call (once-allocated pergroup array, live
                    // AvgAccum transarray); admissibility proven by
                    // granule_meta_len_cols in sorted_fold_meta_ctx; every
                    // sum_of column is in peek_cols by construction.
                    unsafe {
                        ::lanefold::fold_granule_meta(
                            plan,
                            passing,
                            |c| {
                                let i = mf.peek_cols[..mf.npeek]
                                    .iter()
                                    .position(|&pc| pc == c)
                                    .expect("meta ctx staged every length column");
                                stats[i].0 as i64
                            },
                            ::nodeagg::agg_sorted_pergroup_base(agg),
                        );
                    }
                }
                ::nodeseqscan::seq_scan_granule_meta_consume(ss);
                consumed += 1;
                ::postgres_seams::check_for_interrupts::call()?;
            }
            _ => break,
        }
    }
    if consumed > 0 {
        lane_trace(&format!("sorted-agg granule meta-fold: {consumed} granules"));
    }
    Ok(())
}

/// One PG pull's worth of the sorted FOLD drive: walk the staged window from
/// the node-resident cursor, folding each group run whole-batch and emitting
/// one qual-passing group row per pull (pausing with the boundary tuple
/// saved pending — the pull loop's own call-boundary state). See the section
/// doc for the mode split and the byte-identity argument.
fn sorted_fold_step<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    // C's per-pull interrupt check (the pull loop's child-fetch entry).
    ::postgres_seams::check_for_interrupts::call()?;
    let keys = sorted_fold_key_cols(agg, ss).expect("Fold choice proved the key shape");
    let nkeys = keys.n;
    let mut cur_key = [(::datum::Datum::null(), false); SORTED_FOLD_MAX_KEYS];
    // Resume: a saved boundary tuple means the previous pull paused right
    // after emitting a group; start the next group from it (the pull loop's
    // next iteration) and re-derive its key from the group's first tuple.
    let mut group_open = false;
    if ::nodeagg::agg_sorted_have_pending(agg) {
        ::nodeagg::agg_sorted_group_begin(agg, estate, None)?;
        ::nodeagg::agg_sorted_group_key(agg, &mut cur_key[..nkeys]);
        group_open = true;
    }
    // Granule length-stats meta-fold admission (once per pull; the walk is a
    // handful of transition matches). None = every granule stages normally.
    let meta = sorted_fold_meta_ctx(agg, ss, &keys);
    loop {
        // The staged window (node-resident cursor) or the next one.
        let (pos, n) = {
            let (pos, n) = ss.lane_cursor();
            if pos < n {
                (pos, n)
            } else {
                // Between windows: consume whole interior granules of the
                // open group from footer metadata (no decode) first.
                if let Some(mf) = &meta {
                    if group_open {
                        sorted_fold_meta_granules(agg, ss, &keys, &cur_key, mf, estate)?;
                    }
                }
                let n = ::nodeseqscan::seq_scan_next_pagebatch(ss, estate)?;
                ss.set_lane_cursor(0, n);
                if n == 0 {
                    // End of scan: drop the scan slot's pin (source parity),
                    // agg_done BEFORE the last group finalizes (the pull
                    // loop's fetch-None arms), then flush the open group.
                    let mcx = estate.es_query_cxt;
                    ::exectuples::exec_clear_tuple(
                        estate.slot_mut(ss.ss.ss_ScanTupleSlot),
                        mcx,
                    );
                    ::nodeagg::agg_sorted_input_done(agg);
                    if !group_open {
                        return Ok(None);
                    }
                    return ::nodeagg::agg_sorted_emit(agg, estate);
                }
                ::postgres_seams::check_for_interrupts::call()?;
                (0, n)
            }
        };
        match sorted_fold_window(agg, ss, &keys, &mut cur_key, &mut group_open, pos, n, estate)?
        {
            Some(row) => return Ok(Some(row)),
            None => {
                // Window consumed; produce the next one.
                debug_assert_eq!(ss.lane_cursor().0, n);
            }
        }
    }
}

/// Process staged rows `pos..n` of the current window. `Some(row)` = a group
/// row was emitted (the caller returns it to PG; the cursor already points
/// at the first unconsumed row and the boundary tuple is saved pending);
/// `None` = window fully consumed.
#[allow(clippy::too_many_arguments)]
fn sorted_fold_window<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    keys: &SortedFoldKeys,
    cur_key: &mut [(::datum::Datum, bool); SORTED_FOLD_MAX_KEYS],
    group_open: &mut bool,
    pos: u32,
    n: u32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    let nkeys = keys.n;
    let nwords = (n as usize).div_ceil(64);
    let has_resid;
    let guarded;
    {
        let plan = ::nodeagg::agg_lanefold_plan(agg).expect("fold drive without a plan");
        has_resid = !plan.resid.is_empty();
        guarded = plan.guarded;
    }
    // Selection words for this window: the PREWHERE/kernel bitmap when one
    // owns the qual (final verdicts), all-ones on bare scans. `bitmap` mode
    // additionally requires no residual transitions and no requal tail — the
    // fold then touches selected non-fallback rows with NO per-row emits
    // except group prologues/boundaries; otherwise every row goes through
    // the per-row emit (which applies the full qual) and survivors join the
    // fold selection.
    let mut sel = [u64::MAX; ::exectuples::SOA_BM_WORDS];
    let bitmap_qual = match ::nodeseqscan::seq_scan_batch_qual_sel(ss) {
        Some(s) => {
            sel[..nwords].copy_from_slice(&s[..nwords]);
            true
        }
        None => false,
    };
    if n % 64 != 0 {
        sel[nwords - 1] &= (1u64 << (n % 64)) - 1;
    }
    let bitmap_mode = !has_resid && (bitmap_qual || ss.ss.qual.is_none());
    // Per-window demote verdict (recomputed on every resume of the same
    // window — the inputs are staged and deterministic): guard re-proof over
    // a superset of the rows the fold will touch, key lanes ready (a dict-
    // answered or fill-skipped key lane cannot serve the compare), and in
    // bitmap mode the staged SoA present. Demote = the WHOLE window runs the
    // checked per-row program (never a partial fold — lanefold contract).
    let mut demote = false;
    {
        let soa = ::nodeseqscan::seq_scan_batch_soa(ss).expect("fold drive staged the SoA");
        for &(c, _) in &keys.cols[..nkeys] {
            if !soa.col_datum_ready(c as usize) {
                demote = true;
            }
        }
    }
    if !demote && guarded {
        // Zone answers first (whole-window value intervals from the granule
        // footer), prefetched before the SoA borrow.
        let mut zmm = [(0u16, (0i64, 0i64)); 8];
        let mut nz = 0usize;
        {
            let plan = ::nodeagg::agg_lanefold_plan(agg).unwrap();
            for g in plan.guards.iter() {
                if nz == zmm.len() {
                    break;
                }
                if let Some(mm) = ::nodeseqscan::seq_scan_window_value_minmax(ss, g.col as usize)
                {
                    zmm[nz] = (g.col, mm);
                    nz += 1;
                }
            }
        }
        let plan = ::nodeagg::agg_lanefold_plan(agg).unwrap();
        let soa = ::nodeseqscan::seq_scan_batch_soa(ss).expect("fold drive staged the SoA");
        // Proof domain: staged non-fallback rows of the conservative
        // selection (lane sel under PREWHERE includes requal-pending rows —
        // a superset of everything the fold touches; unselected cells may be
        // stale under the lazy fill).
        let mut rows = [0u64; ::exectuples::SOA_BM_WORDS];
        match ::nodeseqscan::seq_scan_batch_lane_sel(ss) {
            Some(ls) => {
                for ((r, fb), s) in
                    rows[..nwords].iter_mut().zip(soa.fallback_words()).zip(ls)
                {
                    *r = s & !fb;
                }
            }
            None => {
                for ((r, fb), s) in
                    rows[..nwords].iter_mut().zip(soa.fallback_words()).zip(&sel[..nwords])
                {
                    *r = s & !fb;
                }
            }
        }
        if n % 64 != 0 {
            rows[nwords - 1] &= (1u64 << (n % 64)) - 1;
        }
        if rows[..nwords].iter().any(|&w| w != 0) {
            // SAFETY: proof rows are staged non-fallback selected rows with
            // live deformed lane values (the completing deform filled every
            // prefix column for survivor windows; vguard columns readable at
            // their varlena header byte).
            demote = unsafe {
                ::lanefold::check_guards(plan, soa, &rows[..nwords], |c| {
                    zmm[..nz].iter().find(|e| e.0 == c).map(|e| e.1)
                }) == ::lanefold::GuardCheck::Demote
            };
        }
    }
    // Copy the fallback words out so the walk below can interleave emits.
    let mut fb = [0u64; ::exectuples::SOA_BM_WORDS];
    {
        let soa = ::nodeseqscan::seq_scan_batch_soa(ss).expect("fold drive staged the SoA");
        fb[..nwords].copy_from_slice(&soa.fallback_words()[..nwords]);
    }
    // The open group's pending fold selection (contiguous same-key run being
    // accumulated); flushed before every per-row event and at window end.
    let mut run = [0u64; ::exectuples::SOA_BM_WORDS];
    let mut run_any = false;
    macro_rules! flush_run {
        () => {
            if run_any {
                let plan = ::nodeagg::agg_lanefold_plan(agg).unwrap();
                let aggcx = ::nodeagg::agg_aggcontext(agg);
                // Str MIN/MAX dict-code views for this window (lane-v2-
                // dictminmax; identity plan→scan map, no scratch —
                // fold_batch's batch winner is codes-only).
                let mm_cols = mm_str_cols(plan, Some);
                let mut mm_codes: Vec<(u16, ::exectuples::SoaDictLane)> = Vec::new();
                collect_mm_codes(ss, &mm_cols, &mut mm_codes);
                let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                    .expect("fold drive staged the SoA");
                // SAFETY: pergroup_base is the node's once-allocated current-
                // group pergroup array covering every transno
                // (initialize_aggregates re-wrote it at group begin); run
                // rows are selected non-fallback rows carrying valid
                // deformed lane values for every plan column; AvgAccum /
                // Int128AvgAccum pergroup states follow the same
                // initialize/fold/transfn chain contract as the plain feed;
                // guarded plans passed check_guards above (a demoted window
                // never reaches here).
                unsafe {
                    ::lanefold::fold_batch(
                        plan,
                        &CodesCols { inner: soa, codes: &mm_codes },
                        &run[..nwords],
                        n as usize,
                        ::nodeagg::agg_sorted_pergroup_base(agg),
                        aggcx,
                    )?;
                }
                run[..nwords].fill(0);
                run_any = false;
            }
        };
    }
    // One same-key row's full per-row delegation (fallback rows, demoted
    // windows, group prologues and boundaries): exactly the SortedAggOp
    // body. Returns Some(row) on a paused boundary emit.
    macro_rules! per_row {
        ($i:expr, $slot:expr) => {{
            let slot = $slot;
            if *group_open && ::nodeagg::agg_sorted_same_group(agg, estate, slot)? {
                ::nodeagg::agg_sorted_accept(agg, estate, slot)?;
                None
            } else if !*group_open {
                ::nodeagg::agg_sorted_group_begin(agg, estate, Some(slot))?;
                ::nodeagg::agg_sorted_group_key(agg, &mut cur_key[..nkeys]);
                *group_open = true;
                None
            } else {
                // Boundary: save the boundary row first (the pull loop's
                // order), then finalize + HAVING + project the group.
                ::nodeagg::agg_sorted_save_pending(agg, estate, slot)?;
                *group_open = false;
                match ::nodeagg::agg_sorted_emit(agg, estate)? {
                    Some(row) => Some(row),
                    None => {
                        // HAVING rejected: start the next group from the
                        // pending boundary tuple (the pull loop's continue).
                        ::nodeagg::agg_sorted_group_begin(agg, estate, None)?;
                        ::nodeagg::agg_sorted_group_key(agg, &mut cur_key[..nkeys]);
                        *group_open = true;
                        None
                    }
                }
            }
        }};
    }
    if demote {
        // Whole-window per-row program (checked transitions, C's detoast/
        // overflow behavior at C's row).
        for i in pos..n {
            ss.set_lane_cursor(i + 1, n);
            if let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(ss, estate, i)? {
                if let Some(row) = per_row!(i, slot) {
                    return Ok(Some(row));
                }
            }
        }
        ss.set_lane_cursor(n, n);
        return Ok(None);
    }
    let mut i = pos;
    while i < n {
        if bitmap_mode {
            // Phase A (staged reads only): extend the open group's run to
            // the next event — a group boundary, a fallback row, or window
            // end. Skipped rows are qual rejections (the bitmap IS the
            // verdict).
            enum Ev {
                Boundary(u32),
                Fallback(u32),
                End,
            }
            let ev = {
                let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                    .expect("fold drive staged the SoA");
                let mut key_vals: [&[::datum::Datum]; SORTED_FOLD_MAX_KEYS] =
                    [&[]; SORTED_FOLD_MAX_KEYS];
                let mut key_nulls: [&[bool]; SORTED_FOLD_MAX_KEYS] =
                    [&[]; SORTED_FOLD_MAX_KEYS];
                for k in 0..nkeys {
                    key_vals[k] = soa.col_values(keys.cols[k].0 as usize);
                    key_nulls[k] = soa.col_isnull(keys.cols[k].0 as usize);
                }
                // Word-skip the qual-rejected positions (the bitmap IS the
                // verdict here): an all-clear selection word advances 64
                // rows in one compare — the run-extension events fire at
                // exactly the same survivor positions in the same order.
                let walk = ::exectuples::for_each_live(
                    Some(&sel[..nwords]),
                    i,
                    n,
                    |j| -> Result<(), Ev> {
                    if fb[(j / 64) as usize] & (1u64 << (j % 64)) != 0 {
                        return Err(Ev::Fallback(j));
                    }
                    if !*group_open {
                        return Err(Ev::Boundary(j));
                    }
                    let same = (0..nkeys).all(|k| {
                        let (cv, cn) = cur_key[k];
                        let jn = key_nulls[k][j as usize];
                        if cn || jn {
                            cn && jn
                        } else {
                            sorted_key_datum_eq(key_vals[k][j as usize], cv, keys.cols[k].1)
                        }
                    });
                    if !same {
                        return Err(Ev::Boundary(j));
                    }
                    run[(j / 64) as usize] |= 1u64 << (j % 64);
                    run_any = true;
                    Ok(())
                    },
                );
                match walk {
                    Err(ev) => ev,
                    Ok(()) => Ev::End,
                }
            };
            // Phase B: fold the accumulated run, then the per-row event.
            flush_run!();
            match ev {
                Ev::End => {
                    i = n;
                }
                Ev::Fallback(j) | Ev::Boundary(j) => {
                    ss.set_lane_cursor(j + 1, n);
                    if let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(ss, estate, j)? {
                        if let Some(row) = per_row!(j, slot) {
                            return Ok(Some(row));
                        }
                    }
                    i = j + 1;
                }
            }
        } else {
            // Per-row-emit mode (residual transitions and/or a requal/
            // scalar-checked qual): every row goes through the scan's
            // per-row emit — the full qual at the per-row path's cadence —
            // and surviving deformed rows join the fold run (residuals per
            // row, the fold-feed discipline); fallback survivors run the
            // full per-row program.
            let j = i;
            ss.set_lane_cursor(j + 1, n);
            let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(ss, estate, j)? else {
                i = j + 1;
                continue;
            };
            let is_fb = fb[(j / 64) as usize] & (1u64 << (j % 64)) != 0;
            if is_fb {
                flush_run!();
                if let Some(row) = per_row!(j, slot) {
                    return Ok(Some(row));
                }
                i = j + 1;
                continue;
            }
            // Deformed survivor: same-group rows fold; boundaries and group
            // prologues delegate per row.
            let same = *group_open && {
                let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                    .expect("fold drive staged the SoA");
                (0..nkeys).all(|k| {
                    let (cv, cn) = cur_key[k];
                    let jn = soa.col_isnull(keys.cols[k].0 as usize)[j as usize];
                    if cn || jn {
                        cn && jn
                    } else {
                        sorted_key_datum_eq(
                            soa.col_values(keys.cols[k].0 as usize)[j as usize],
                            cv,
                            keys.cols[k].1,
                        )
                    }
                })
            };
            if same {
                run[(j / 64) as usize] |= 1u64 << (j % 64);
                run_any = true;
                if has_resid {
                    ::nodeagg::agg_sorted_accept_resid(agg, estate, slot)?;
                }
            } else {
                flush_run!();
                if let Some(row) = per_row!(j, slot) {
                    return Ok(Some(row));
                }
            }
            i = j + 1;
        }
    }
    flush_run!();
    let _ = run_any;
    ss.set_lane_cursor(n, n);
    Ok(None)
}

// ===========================================================================
// Hash-join pipeline breaker (Phase 2). The join spans two pipelines plus a
// mid-pipeline streaming stage:
//
//   pipeline N   (build): inner scan source → scalar filter/project →
//                         HashJoinBuildSink   (breaker Sink face)
//   pipeline N+1 (probe): outer scan source → scalar filter/project →
//                         JoinProbe (TupleOp) → sink
//
// The build side is the breaker: `accept` = the row-path per-row hash +
// `ExecHashTableInsert` (`nodehash::lane_build_accept` — spill/growth arms
// included), `finish` = the delegated build tail (`finish_build`,
// empty-build early return, `nbatch_outstart`/`dense_on`, phase flip). The
// probe side is NOT a breaker — it streams: one outer row in, 0..K joined
// rows out, with the intra-row expansion position node-resident on the
// HashJoinState (`hj_CurTuple`/`hj_CurDense` — C's own cross-call state), so
// a mid-expansion pause resumes exactly. The phase flag is `hj_JoinState`
// itself (HJ_BUILD_HASHTABLE → HJ_NEED_NEW_OUTER — C's own state machine).
//
// Spill (§8): the build delegates wholesale to the row-path table, so nbatch
// growth happens exactly as the row path's; the lane then checks the FINAL
// nbatch after the completed build and REFUSES the probe when nbatch > 1 —
// before any lane tuple is emitted, so the fallback `exec_hash_join` resumes
// from HJ_NEED_NEW_OUTER over the identical table (postponing outer tuples
// to batch files exactly as if the row path had built it). Refusing on the
// planner's initial estimate alone would be insufficient: the row path grows
// nbatch mid-build (`ExecHashIncreaseNumBatches`), so only the post-build
// value is authoritative — and checking after a fully delegated build is
// byte-safe precisely because the build is bit-equal to the row path's.
//
// Admitted join types: all eight — INNER, LEFT, SEMI, ANTI plus the
// right-fill family RIGHT, FULL, RIGHT_SEMI, RIGHT_ANTI — with
// joinqual/otherqual residuals evaluated scalar-within-lane through the
// row path's exact `eval_probe_qual` (LEFT/FULL/ANTI null-fill emits happen
// inside `lane_probe_next`'s HJ_FILL_OUTER_TUPLE arm, exactly where C emits
// them). The right-fill types (`hj_fill_inner` — RIGHT/FULL/RIGHT_ANTI) add
// a post-exhaustion phase: when the outer source ends, the probe TupleOp
// becomes a SOURCE of never-matched build tuples (C's HJ_FILL_INNER_TUPLES
// via the driver's `source_exhausted` seam; the walk delegates to the
// row path's exact `ExecScanHashTableForUnmatched` port, so the fill
// emission order is C's bucket order for free; the cursor is C's own
// node-resident `hj_CurBucketNo`/`hj_CurTuple`, so a LIMIT pause mid-fill
// resumes exactly). RIGHT_SEMI needs no fill phase — only the has-match
// skip in the probe arm. Refused join shapes (assert-refuse set):
// multi-batch (above), parallel hash, instrumented, subplan/param-bearing
// hash, residual-qual or projection exprs, non-lane-fusible scan children
// on either side.
// ===========================================================================

/// The breaker's `Sink` face (build pipeline endpoint). Holds the join +
/// hash nodes by `&mut` — the driver threads the inner SCAN node, so the
/// breaker spanning other nodes needs no driver rework (sort-breaker shape).
struct HashJoinBuildSink<'a, 'mcx> {
    hj: &'a mut ::nodehashjoin::HashJoinState<'mcx>,
    hs: &'a mut ::nodehash::HashState<'mcx>,
    done: Option<::nodehashjoin::LaneBuildDone>,
}

impl<'mcx> Sink<'mcx> for HashJoinBuildSink<'_, 'mcx> {
    fn accept(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<SinkFeed> {
        ::nodehash::lane_build_accept(self.hs, estate, tuple)?;
        Ok(SinkFeed::NeedMore)
    }

    fn finish(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        self.done = Some(::nodehashjoin::lane_build_finish(self.hj, self.hs, estate)?);
        Ok(())
    }
}

/// Batch-granular feed: the default loop, monomorphized — each staged row
/// runs the same `lane_build_accept` in the same order, with the per-row dyn
/// dispatch, `SinkFeed` matching, and consume-cursor saves elided.
impl<'mcx> BatchSink<'mcx> for HashJoinBuildSink<'_, 'mcx> {}

/// The join probe as a mid-pipeline `TupleOp`: accept stages one outer row
/// (`lane_probe_accept` — ecxt reset + hash/dense key, C's per-outer-row
/// prologue), then the expansion streams each bucket/dense-chain match
/// through the row-path recheck + projection (`lane_probe_next`) into the
/// downstream sink. Expansion position is node-resident on the join state.
struct JoinProbe<'a, 'mcx> {
    hj: &'a mut ::nodehashjoin::HashJoinState<'mcx>,
    hs: &'a mut ::nodehash::HashState<'mcx>,
}

impl<'mcx> JoinProbe<'_, 'mcx> {
    fn emit(
        &mut self,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        while let Some(j) = ::nodehashjoin::lane_probe_next(self.hj, self.hs, estate)? {
            if let SinkFeed::Full = out.accept(j, estate)? {
                return Ok(OpStatus::Paused);
            }
        }
        Ok(OpStatus::NeedInput)
    }
}

impl<'mcx> TupleOp<'mcx> for JoinProbe<'_, 'mcx> {
    fn pending(&self) -> bool {
        ::nodehashjoin::lane_probe_pending(self.hj)
    }

    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        ::nodehashjoin::lane_probe_accept(self.hj, self.hs, estate, tuple)?;
        self.emit(out, estate)
    }

    fn resume(
        &mut self,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        let s = self.emit(out, estate)?;
        // A resumed fill scan that just drained is terminal: the driver must
        // not fall through to another source produce — a pulled-past-end
        // heap scan RESTARTS (C never re-pulls a child after NULL).
        if s == OpStatus::NeedInput && ::nodehashjoin::lane_join_finished(self.hj) {
            return Ok(OpStatus::Finished);
        }
        Ok(s)
    }

    /// Outer exhausted: the right-fill types (`hj_fill_inner` —
    /// RIGHT/FULL/RIGHT_ANTI) flip into the unmatched-BUILD fill scan
    /// (C's HJ_FILL_INNER_TUPLES, sequenced exactly where C enters it:
    /// after the probe fully ends) and become a source of null-extended
    /// unmatched inner tuples into the same sink. The prep is idempotent
    /// (no-op unless the join sits at HJ_NEED_NEW_OUTER), the fill cursor
    /// is C's own node-resident `hj_CurBucketNo`/`hj_CurTuple`, and a
    /// mid-fill pause (`Paused`) resumes through the ordinary
    /// `pending()`/`resume()` protocol. Non-fill types emit nothing here.
    fn source_exhausted(
        &mut self,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        ::nodehashjoin::lane_fill_inner_prep(self.hj);
        Ok(match self.emit(out, estate)? {
            // The fill scan is drained (or there never was one): nothing
            // further will ever be produced.
            OpStatus::NeedInput => OpStatus::Finished,
            s => s,
        })
    }
}

/// Build-pipeline driver, generic over the inner scan: table create
/// (delegated, bit-equal to the row path's), drain the scan pipeline into
/// the breaker sink, delegated finish. Returns the post-build verdict inputs
/// (empty / final nbatch).
fn join_build_feed<'mcx, S, O>(
    hj: &mut ::nodehashjoin::HashJoinState<'mcx>,
    hs: &mut ::nodehash::HashState<'mcx>,
    scan: &mut S::Node,
    mut src: S,
    mut op: O,
    estate: &mut EStateData<'mcx>,
) -> PgResult<::nodehashjoin::LaneBuildDone>
where
    S: Source<'mcx>,
    O: Operator<'mcx, Node = S::Node>,
{
    ::nodehashjoin::lane_build_begin(hj, hs, estate)?;
    let mut sink = HashJoinBuildSink { hj, hs, done: None };
    drain_pipeline(scan, &mut src, &mut op, &mut sink, estate)?;
    Ok(sink.done.expect("build sink finished"))
}

/// Dispatch the build feed over the admitted inner-scan child types.
fn join_build_dispatch<'mcx>(
    hj: &mut ::nodehashjoin::HashJoinState<'mcx>,
    hs: &mut ::nodehash::HashState<'mcx>,
    child: &mut crate::procnode::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<::nodehashjoin::LaneBuildDone> {
    // One OWNED tick per lane-owned join build event (the gate's join floor
    // counts builds, not calls) — bare joins and agg-over-join compositions
    // alike.
    stats::tick_owned(ShapeClass::Join);
    match child {
        crate::procnode::PlanStateNode::SeqScan(ss) => {
            arm_scan_staging(
                ss,
                estate,
                ScanFeedShape::RowFeed { ctx: "join build feed", stitch: true },
            )?;
            join_build_feed(hj, hs, ss, SeqScanSource, SeqScanFilterProject, estate)
        }
        crate::procnode::PlanStateNode::IndexScan(is) => {
            join_build_feed(hj, hs, is, IndexScanSource, IndexScanEmit, estate)
        }
        crate::procnode::PlanStateNode::IndexOnlyScan(ios) => {
            join_build_feed(hj, hs, &mut **ios, IndexOnlyScanSource, IndexOnlyScanEmit, estate)
        }
        crate::procnode::PlanStateNode::BitmapHeapScan(b) => {
            let b = &mut **b;
            if !b.scan.initialized {
                crate::procnode::bitmap_table_scan_setup_dispatch(b, estate)?;
            }
            join_build_feed(hj, hs, &mut b.scan, BitmapHeapScanSource, BitmapHeapScanEmit, estate)
        }
        _ => unreachable!("memoized join verdict admitted a non-scan build child"),
    }
}

/// Probe-pipeline drain (composition): outer scan → filter/project →
/// JoinProbe → the downstream breaker sink (the agg build), to exhaustion.
fn join_probe_drain_dispatch<'mcx>(
    hj: &mut ::nodehashjoin::HashJoinState<'mcx>,
    hs: &mut ::nodehash::HashState<'mcx>,
    outer: &mut crate::procnode::PlanStateNode<'mcx>,
    sink: &mut dyn Sink<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mut probe = JoinProbe { hj, hs };
    match outer {
        crate::procnode::PlanStateNode::SeqScan(ss) => {
            arm_scan_staging(
                ss,
                estate,
                ScanFeedShape::RowFeed { ctx: "join probe drain", stitch: true },
            )?;
            drain_pipeline_chain(
                ss,
                &mut SeqScanSource,
                &mut SeqScanFilterProject,
                &mut probe,
                sink,
                estate,
            )
        }
        crate::procnode::PlanStateNode::IndexScan(is) => drain_pipeline_chain(
            is,
            &mut IndexScanSource,
            &mut IndexScanEmit,
            &mut probe,
            sink,
            estate,
        ),
        crate::procnode::PlanStateNode::IndexOnlyScan(ios) => drain_pipeline_chain(
            &mut **ios,
            &mut IndexOnlyScanSource,
            &mut IndexOnlyScanEmit,
            &mut probe,
            sink,
            estate,
        ),
        crate::procnode::PlanStateNode::BitmapHeapScan(b) => {
            let b = &mut **b;
            if !b.scan.initialized {
                crate::procnode::bitmap_table_scan_setup_dispatch(b, estate)?;
            }
            drain_pipeline_chain(
                &mut b.scan,
                &mut BitmapHeapScanSource,
                &mut BitmapHeapScanEmit,
                &mut probe,
                sink,
                estate,
            )
        }
        _ => unreachable!("memoized join verdict admitted a non-scan outer child"),
    }
}

/// Probe-pipeline pull (bare join): one PG pull's worth through the chain
/// into the root adapter — exercising the mid-expansion pause/resume.
fn join_probe_pull_dispatch<'mcx>(
    hj: &mut ::nodehashjoin::HashJoinState<'mcx>,
    hs: &mut ::nodehash::HashState<'mcx>,
    outer: &mut crate::procnode::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    let mut probe = JoinProbe { hj, hs };
    let mut root = RootAdapter::new(None);
    match outer {
        crate::procnode::PlanStateNode::SeqScan(ss) => {
            // Per-pull entry: the bitmap arm early-returns once armed (one
            // load+test), and the first pull arms BEFORE any batch is
            // staged, so a staged batch always matches its bitmap. No
            // stitch: pull-one-tuple pipelines keep the AOT bitmap tier
            // (stitched segments exist only on drain pipelines).
            arm_scan_staging(
                ss,
                estate,
                ScanFeedShape::RowFeed { ctx: "join probe pull", stitch: false },
            )?;
            pull_step_chain(
                ss,
                &mut SeqScanSource,
                &mut SeqScanFilterProject,
                &mut probe,
                &mut root,
                estate,
            )
        }
        crate::procnode::PlanStateNode::IndexScan(is) => pull_step_chain(
            is,
            &mut IndexScanSource,
            &mut IndexScanEmit,
            &mut probe,
            &mut root,
            estate,
        ),
        crate::procnode::PlanStateNode::IndexOnlyScan(ios) => pull_step_chain(
            &mut **ios,
            &mut IndexOnlyScanSource,
            &mut IndexOnlyScanEmit,
            &mut probe,
            &mut root,
            estate,
        ),
        crate::procnode::PlanStateNode::BitmapHeapScan(b) => {
            let b = &mut **b;
            if !b.scan.initialized {
                crate::procnode::bitmap_table_scan_setup_dispatch(b, estate)?;
            }
            pull_step_chain(
                &mut b.scan,
                &mut BitmapHeapScanSource,
                &mut BitmapHeapScanEmit,
                &mut probe,
                &mut root,
                estate,
            )
        }
        _ => unreachable!("memoized join verdict admitted a non-scan outer child"),
    }
}

/// Structural refuse-set for the lane hash join, memoized on the node at
/// first evaluation (verdict stability: a lane-owned join must stay
/// lane-owned — `lane_join_untouched` in the verdict guarantees the row path
/// never drove this node before the lane, and memoization guarantees the
/// lane drives it ever after). Join side: `lane_join_admissible`
/// (all eight join types, subplan/param-free residual quals admitted,
/// uninstrumented, subplan/param-free hash + projection exprs) + serial hash
/// + subplan/param-free build hash. Child side: the Phase-1 scan refuse-sets
/// on BOTH children. The caller re-checks the dynamic EPQ/direction gates
/// per call.
fn hash_join_lane_fusible<'mcx>(
    hj: &mut crate::procnode::HashJoinNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if let Some(v) = hj.lane_fusible {
        return Ok(v);
    }
    // Engagement accounting for the structural verdict ticks exactly here —
    // once per memoized decision (a child-scan refusal's specific reason is
    // ticked under the child's class inside its fusible gate). OWNED ticks
    // for the join class count build EVENTS, in `join_build_dispatch`.
    let refuse = hash_join_refuse_reason(hj, estate)?;
    if let Some(r) = refuse {
        stats::tick_refused(ShapeClass::Join, r);
    }
    let v = refuse.is_none();
    hj.lane_fusible = Some(v);
    Ok(v)
}

/// `None` = admitted; `Some(reason)` = refused.
fn hash_join_refuse_reason<'mcx>(
    hj: &mut crate::procnode::HashJoinNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<RefuseReason>> {
    let crate::procnode::HashJoinNode { state, outer, hash, .. } = hj;
    let crate::procnode::HashSubNode { state: hstate, child } = &mut **hash;
    // Instrumented, subplan/param-bearing join exprs or projection (all
    // eight join types + residuals are admitted since lane-v2-jointypes /
    // lane-v2-rightjoin) — plus a node the row path already drove (verdict
    // stability demands whole-life ownership).
    if !::nodehashjoin::lane_join_admissible(state)
        || !::nodehashjoin::lane_join_untouched(state, hstate)
    {
        return Ok(Some(RefuseReason::JoinShape));
    }
    if hstate.parallel_state().is_some() || hstate.is_parallel_aware() {
        return Ok(Some(RefuseReason::ParallelGate));
    }
    if !::nodehash::lane_build_hash_admissible(hstate) {
        return Ok(Some(RefuseReason::SubplanParam));
    }
    if let Some(r) = scan_child_fusible(outer, estate)? {
        return Ok(Some(r));
    }
    scan_child_fusible(child, estate)
}

/// Try to let the lane own a bare `HashJoin` (no lane consumer above): build
/// pipeline once (lazily, phase = the node's own HJ_BUILD_HASHTABLE), then
/// one joined tuple per PG pull through the probe chain. `None` = refused
/// (caller runs the unchanged `exec_hash_join` — byte-safe even after a
/// lane-delegated build, which leaves exactly the row path's post-build node
/// state). The dispatch hook gates this on the legacy fused probe drive NOT
/// engaging (admission economics: never preempt the faster existing path).
#[inline]
pub fn try_own_hash_join<'mcx>(
    hj: &mut crate::procnode::HashJoinNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Admission economics (design §4): the legacy fused probe drive already
    // owns this shape better than the v2 pipeline — never preempt the
    // measured-faster path. Per-PULL tick cadence (the dispatch arm resolves
    // the probe mode before offering the join to the lane). Parallel Hash
    // ticks its own gate.
    match hj.probe_batch.mode() {
        crate::procnode::ProbeBatchMode::Off => {}
        crate::procnode::ProbeBatchMode::Parallel => {
            stats::tick_refused(ShapeClass::Join, RefuseReason::ParallelGate);
            return Ok(None);
        }
        crate::procnode::ProbeBatchMode::Unknown | crate::procnode::ProbeBatchMode::On => {
            stats::tick_refused(ShapeClass::Join, RefuseReason::AdmissionEconomicsFusedDrive);
            return Ok(None);
        }
    }
    // Dynamic per-call gates (mirrors the sort breaker).
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::Join, RefuseReason::Epq);
        return Ok(None);
    }
    if !hash_join_lane_fusible(hj, estate)? {
        return Ok(None);
    }
    // C's CHECK_FOR_INTERRUPTS at ExecHashJoin entry.
    ::postgres_seams::check_for_interrupts::call()?;
    let crate::procnode::HashJoinNode { state, outer, hash, .. } = hj;
    let crate::procnode::HashSubNode { state: hstate, child } = &mut **hash;
    if ::nodehashjoin::lane_join_phase(state, hstate) == ::nodehashjoin::LaneJoinPhase::Build {
        let done = join_build_dispatch(state, hstate, child, estate)?;
        if done.empty {
            // C's empty-build early return: no output, outer never pulled.
            return Ok(Some(None));
        }
        if done.nbatch > 1 {
            // Spill refuse, before any lane tuple is emitted: the fallback
            // row path resumes from HJ_NEED_NEW_OUTER over the same table.
            stats::tick_refused(ShapeClass::Join, RefuseReason::MultiBatch);
            return Ok(None);
        }
        // Bloom pushdown reclaim: arm the lane probe's prefilter, only
        // where the legacy path's own push seats would (SeqScan outer
        // drives — the fused probe drive and the bare `seq_scan_set_bloom`
        // seat are both SeqScan-only), so lane-vs-legacy comparisons stay
        // apples-to-apples. The arm re-applies the row path's exact push
        // gate (never fill_outer, never dense, hash cover, single batch,
        // density <= 0.25).
        if let crate::procnode::PlanStateNode::SeqScan(_) = &**outer {
            ::nodehashjoin::lane_probe_filter_arm(state, hstate);
        }
    } else {
        match ::nodehashjoin::lane_join_phase(state, hstate) {
            ::nodehashjoin::LaneJoinPhase::EmptyDone => return Ok(Some(None)),
            ::nodehashjoin::LaneJoinPhase::Probe => {
                if hstate.table.as_ref().expect("probe phase has a table").nbatch > 1 {
                    stats::tick_refused(ShapeClass::Join, RefuseReason::MultiBatch);
                    return Ok(None);
                }
            }
            ::nodehashjoin::LaneJoinPhase::Build => unreachable!("handled above"),
        }
    }
    Ok(Some(join_probe_pull_dispatch(state, hstate, outer, estate)?))
}

// ===========================================================================
// NestLoop hosting (the deferred §4 bundle). The join is a mid-pipeline
// streaming `TupleOp` — NOT a breaker: one outer row in, 0..K joined rows
// out.
//
//   pipeline: outer scan source → scalar filter/project → NestLoopProbe
//             (TupleOp) → sink (RootAdapter, or the hash-agg breaker)
//
// Per accepted outer row the op runs C's need-new-outer arm
// (`nodenestloop::lane_accept_outer`): bind the outer tuple, assign the
// join's exec params (nestParams → PARAM_EXEC slots), and RESCAN the inner
// child; the expansion then streams each inner row through the joinqual /
// otherqual / projection (`lane_probe_next` = `exec_nest_loop`'s own loop
// body, LEFT/SEMI/ANTI arms included). The INNER child stays a Volcano
// child, driven per-row through the same `NestLoopChild` calls the row path
// uses (scalar-within-lane, per the design's allowance) — so exec-param-
// driven runtime keys on an inner index scan are evaluated in
// `exec_rescan_index_scan`'s preamble exactly as C's ExecReScan path does,
// AUTOMATICALLY. The Phase-1 lane scan gates therefore KEEP refusing runtime
// keys (`iss_Runtime`/`ioss_Runtime`) for LANE-OWNED scans: that relaxation
// belongs to the inner-as-lane-pipeline follow-up, where the lane would have
// to drive the rescan preamble itself. Expansion position across the Volcano
// pull boundary is the node's own `nl_NeedNewOuter`/`nl_MatchedOuter` — C's
// cross-call state; no new fields.
//
// Admission economics (design §4): NestLoop per-tuple in Volcano is already
// cheap; the lane's value is OWNERSHIP CONTINUITY — the outer side stays a
// lane pipeline feeding breakers above. The hooks engage (a) under the
// hash-agg breaker (`try_own_agg_over_nest_loop` — a lane consumer above,
// no fused competitor exists for this shape) and (b) bare
// (`try_own_nest_loop`) where the outer is a lane-fusible scan the join
// pipeline then owns — the bare hash-join precedent; there is no legacy
// fused NestLoop drive to preempt. Refused (assert-refuse set):
// instrumented, subplan/param-bearing joinqual/otherqual/projection,
// row-path-touched nodes (verdict stability), non-lane-fusible outer
// children, EPQ, non-forward. The inner child is unconstrained — it runs
// the identical Volcano calls at the identical points either way.
// ===========================================================================

/// The NestLoop join as a mid-pipeline `TupleOp`: accept stages one outer
/// row (param assignment + inner rescan — C's per-outer-row prologue), then
/// the expansion streams the inner drain through the row-path joinqual /
/// projection arms into the downstream sink. Expansion position is
/// node-resident on the join state (`nl_NeedNewOuter`).
struct NestLoopProbe<'a, 'mcx> {
    nl: &'a mut ::nodenestloop::NestLoopState<'mcx>,
    inner: &'a mut crate::procnode::PlanStateNode<'mcx>,
}

impl<'mcx> NestLoopProbe<'_, 'mcx> {
    fn emit(
        &mut self,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        while let Some(j) = ::nodenestloop::lane_probe_next(self.nl, self.inner, estate)? {
            if let SinkFeed::Full = out.accept(j, estate)? {
                return Ok(OpStatus::Paused);
            }
        }
        Ok(OpStatus::NeedInput)
    }
}

impl<'mcx> TupleOp<'mcx> for NestLoopProbe<'_, 'mcx> {
    fn pending(&self) -> bool {
        ::nodenestloop::lane_probe_pending(self.nl)
    }

    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        // One OWNED tick per accepted outer row — the unit the lane owns
        // (bind params -> rescan the inner -> drain the expansion).
        stats::tick_owned(ShapeClass::NestLoop);
        ::nodenestloop::lane_accept_outer(self.nl, self.inner, estate, tuple)?;
        self.emit(out, estate)
    }

    fn resume(
        &mut self,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        self.emit(out, estate)
    }
}

/// One PG pull through outer scan → filter/project → `top` → root adapter,
/// dispatched over the admitted lane-scan child types (join_probe dispatch
/// shape, generic over the mid-pipeline op).
fn scan_chain_pull_dispatch<'mcx>(
    outer: &mut crate::procnode::PlanStateNode<'mcx>,
    top: &mut dyn TupleOp<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    let mut root = RootAdapter::new(None);
    match outer {
        crate::procnode::PlanStateNode::SeqScan(ss) => pull_step_chain(
            ss,
            &mut SeqScanSource,
            &mut SeqScanFilterProject,
            top,
            &mut root,
            estate,
        ),
        crate::procnode::PlanStateNode::IndexScan(is) => pull_step_chain(
            is,
            &mut IndexScanSource,
            &mut IndexScanEmit,
            top,
            &mut root,
            estate,
        ),
        crate::procnode::PlanStateNode::IndexOnlyScan(ios) => pull_step_chain(
            &mut **ios,
            &mut IndexOnlyScanSource,
            &mut IndexOnlyScanEmit,
            top,
            &mut root,
            estate,
        ),
        crate::procnode::PlanStateNode::BitmapHeapScan(b) => {
            let b = &mut **b;
            if !b.scan.initialized {
                crate::procnode::bitmap_table_scan_setup_dispatch(b, estate)?;
            }
            pull_step_chain(
                &mut b.scan,
                &mut BitmapHeapScanSource,
                &mut BitmapHeapScanEmit,
                top,
                &mut root,
                estate,
            )
        }
        _ => unreachable!("memoized lane verdict admitted a non-scan outer child"),
    }
}

/// Full drain of outer scan → filter/project → `top` → breaker sink, same
/// dispatch as `scan_chain_pull_dispatch`.
fn scan_chain_drain_dispatch<'mcx>(
    outer: &mut crate::procnode::PlanStateNode<'mcx>,
    top: &mut dyn TupleOp<'mcx>,
    sink: &mut dyn Sink<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    match outer {
        crate::procnode::PlanStateNode::SeqScan(ss) => drain_pipeline_chain(
            ss,
            &mut SeqScanSource,
            &mut SeqScanFilterProject,
            top,
            sink,
            estate,
        ),
        crate::procnode::PlanStateNode::IndexScan(is) => drain_pipeline_chain(
            is,
            &mut IndexScanSource,
            &mut IndexScanEmit,
            top,
            sink,
            estate,
        ),
        crate::procnode::PlanStateNode::IndexOnlyScan(ios) => drain_pipeline_chain(
            &mut **ios,
            &mut IndexOnlyScanSource,
            &mut IndexOnlyScanEmit,
            top,
            sink,
            estate,
        ),
        crate::procnode::PlanStateNode::BitmapHeapScan(b) => {
            let b = &mut **b;
            if !b.scan.initialized {
                crate::procnode::bitmap_table_scan_setup_dispatch(b, estate)?;
            }
            drain_pipeline_chain(
                &mut b.scan,
                &mut BitmapHeapScanSource,
                &mut BitmapHeapScanEmit,
                top,
                sink,
                estate,
            )
        }
        _ => unreachable!("memoized lane verdict admitted a non-scan outer child"),
    }
}

/// Structural refuse-set for the lane NestLoop, memoized on the node at
/// first evaluation (verdict stability: a lane-owned join must stay
/// lane-owned — `lane_nest_loop_untouched` in the verdict guarantees the row
/// path never drove this node before the lane, and memoization guarantees
/// the lane drives it ever after). Join side: `lane_nest_loop_admissible`
/// (all four ported join types; uninstrumented; subplan/param-free quals +
/// projection). Outer side: the Phase-1 scan refuse-sets. The INNER side is
/// deliberately unconstrained — it stays a Volcano child driven through the
/// identical `NestLoopChild` calls. The caller re-checks the dynamic
/// EPQ/direction gates per call.
fn nest_loop_lane_fusible<'mcx>(
    nl: &mut crate::procnode::NestLoopNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if let Some(v) = nl.lane_fusible {
        return Ok(v);
    }
    // Engagement accounting for the structural verdict ticks exactly here —
    // once per memoized decision (a child-scan refusal's specific reason is
    // ticked under the child's class inside its fusible gate). OWNED ticks
    // for the nestloop class count accepted OUTER ROWS, in
    // `NestLoopProbe::accept`.
    let refuse = nest_loop_refuse_reason(nl, estate)?;
    if let Some(r) = refuse {
        stats::tick_refused(ShapeClass::NestLoop, r);
    }
    let v = refuse.is_none();
    nl.lane_fusible = Some(v);
    Ok(v)
}

/// `None` = admitted; `Some(reason)` = refused.
fn nest_loop_refuse_reason<'mcx>(
    nl: &mut crate::procnode::NestLoopNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<RefuseReason>> {
    let crate::procnode::NestLoopNode { state, outer, .. } = nl;
    // Instrumented, subplan/param-bearing joinqual/otherqual/projection —
    // plus a node the row path already drove (verdict stability demands
    // whole-life ownership).
    if !::nodenestloop::lane_nest_loop_admissible(state)
        || !::nodenestloop::lane_nest_loop_untouched(state, estate)
    {
        return Ok(Some(RefuseReason::JoinShape));
    }
    scan_child_fusible(outer, estate)
}

/// Try to let the lane own a bare `NestLoop` (no lane consumer above): one
/// joined tuple per PG pull through the chain, the mid-inner-drain position
/// riding C's own `nl_NeedNewOuter` across the pull boundary. `None` =
/// refused (caller runs the unchanged `exec_nest_loop` — byte-safe: an
/// untouched-only verdict means the row path owns the node's whole life).
#[inline]
pub fn try_own_nest_loop<'mcx>(
    nl: &mut crate::procnode::NestLoopNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic per-call gates (mirrors the sort/hash-join breakers).
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::NestLoop, RefuseReason::Epq);
        return Ok(None);
    }
    if !nest_loop_lane_fusible(nl, estate)? {
        return Ok(None);
    }
    // C's CHECK_FOR_INTERRUPTS at ExecNestLoop entry.
    ::postgres_seams::check_for_interrupts::call()?;
    let crate::procnode::NestLoopNode { state, outer, inner, .. } = nl;
    let mut probe = NestLoopProbe { nl: state, inner };
    Ok(Some(scan_chain_pull_dispatch(outer, &mut probe, estate)?))
}

/// Try to let the lane own `Agg(hashed) → NestLoop → lane outer scan` (the
/// inner stays Volcano): two pipelines on one breaker node —
///
///   1. build: outer scan → filter/project → NestLoopProbe → HashAggBuildSink
///   2. emit:  HashAggSource → HashAggEmit → RootAdapter (one group per pull)
///
/// `None` = refused (caller falls to the per-tuple `exec_agg` over
/// `exec_nest_loop`, byte-identically).
#[inline]
pub fn try_own_agg_over_nest_loop<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    nl: &mut crate::procnode::NestLoopNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic per-call gates, ticked under the nestloop class (the
    // composition's feed pipeline hangs off the join's drive).
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::NestLoop, RefuseReason::Epq);
        return Ok(None);
    }
    if !::nodeagg::agg_hash_breaker_admissible(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AggNotDrainable);
        return Ok(None);
    }
    if !nest_loop_lane_fusible(nl, estate)? {
        return Ok(None);
    }
    // exec_agg's top-of-call guard: a drained agg stays drained.
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    // Build phase (once, lazily; a rescan rebuild clears `table_filled` and
    // re-enters — the whole-NestLoop rescan resets `nl_NeedNewOuter` and the
    // outer scan's staged cursor, so the feed restarts coherently).
    if !::nodeagg::agg_hash_table_filled(agg) {
        // One OWNED tick per lane-owned agg build event (here the build is
        // fed by the NestLoop expansion drain).
        stats::tick_owned(ShapeClass::AggBuild);
        let crate::procnode::NestLoopNode { state, outer, inner, .. } = nl;
        let mut probe = NestLoopProbe { nl: state, inner };
        let mut sink = HashAggBuildSink { agg: &mut *agg };
        scan_chain_drain_dispatch(outer, &mut probe, &mut sink, estate)?;
    }
    // Emit phase (every call): one qual-passing group per PG pull, in C's
    // retrieve order.
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step(agg, &mut HashAggSource, &mut HashAggEmit, &mut root, estate)?))
}

// Staged joined-row fold feed (the agg-over-join composition's batched build
// feed). The join probe streams one joined row at a time — there is no page
// batch and no SoA deform on the agg's outer side — so the composition's agg
// breaker previously fed per-row (`HashAggBuildSink`), leaving the lanefold
// kernels disengaged. This sink stages joined rows into `LaneCols`-compatible
// arrays and folds the admitted transitions per staged batch (~the page-batch
// row cap) via the shared fold tail (`agg_fold_staged`), in two modes:
//
// * UNGUARDED plans (no data-level Guard — the common case): the group probe
//   + residual transitions run per row AT ARRIVAL against the incoming joined
//   slot (exactly the per-row sink's own call), snapshotting the pergroup;
//   only the fold lanes (`plan.cols` — always byval int-family by classify
//   construction) are staged, and the flush is just the whole-batch fold.
//   No replay slot, no datum copies: strictly the per-row sink minus the
//   admitted transitions' interpreted per-row steps. This mirrors the seqscan
//   fold feed's probe-then-fold split (residuals per-row inside the batch,
//   commutative fold after) — bit-identical by the lanefold contract.
//
// * GUARDED plans (int2/int4 OpExpr admissions carrying a Guard interval):
//   the guard must be re-proven BEFORE any probe/transition runs, and a
//   Demote must run the WHOLE batch through the checked per-row program — so
//   nothing may run at arrival. The sink stages every build-relevant column
//   (fold lanes + group keys + residual inputs — exactly the `colnos_needed`
//   set the hashagg spill projection keeps), and per batch: `check_guards`
//   (data-scan tier — join output has no zone map), Demote → replay every
//   staged row through `agg_hash_build_accept` (raises C's error at C's
//   row), else replay → probe/residual per row → fold. The replay slot
//   presents the same needed-column values in the same row order the per-row
//   sink would (unneeded columns NULL — the spill projection's own
//   treatment), so probe sequence, spill decisions, residual transitions,
//   and error rows are identical.
//
// Memory: the lanes are fixed-capacity (STAGE_ROWS), reused across batches;
// by-ref staged values on the guarded path (e.g. text group keys — they may
// point into per-tuple memory the probe resets row to row) are datum-copied
// into a dedicated bump context that is reset after every staged batch —
// per-batch, fixed-size, no unbounded growth.
// ===========================================================================

/// Staging window for the joined-row fold feed: the page-batch row cap, so a
/// staged join batch matches the seqscan fold feed's batch magnitude (and the
/// guard bitmask reuses `SOA_BM_WORDS`).
const STAGE_ROWS: usize = ::exectuples::SOA_MAX_ROWS;

/// `LaneCols` over the staged joined-row window: per-column value/isnull
/// lanes indexed by the join output's 0-based attno. Only the needed columns
/// are populated; the fold reads only `plan.cols`, a subset.
struct StagedLanes {
    values: Vec<Vec<::datum::Datum>>,
    isnull: Vec<Vec<bool>>,
}

impl ::lanefold::LaneCols for StagedLanes {
    fn col_values(&self, c: usize) -> &[::datum::Datum] {
        &self.values[c]
    }

    fn col_isnull(&self, c: usize) -> &[bool] {
        &self.isnull[c]
    }
}

/// Staged-feed mode (see the section comment and `staged_feed_shape`).
#[derive(Clone, Copy, PartialEq, Eq)]
enum StagedMode {
    /// Unguarded, arrival probe: only the fold lanes stage; the group probe +
    /// residual transitions run per row at accept.
    Arrival,
    /// Guarded: full needed-column staging with the per-batch guard proof and
    /// the Demote whole-batch per-row replay.
    Guarded,
    /// K2 deferred batched probe (design §3a): full needed-column staging;
    /// per batch — one tight batched-hash loop over the staged grouping-key
    /// lane, then the in-order probe through the same C-ported tuplehash
    /// lookup (bit-identical hashes → identical table layout / iteration /
    /// output order), then the whole-batch fold. Replaces the per-row
    /// expr-program hash+eq walk and per-row slot/context churn. Admitted for
    /// unguarded plans with NO residual transitions over a single
    /// kernel-hostable (int4/int8/text) grouping key.
    K2 {
        /// The grouping key's 0-based colno in the join output.
        key_col: u16,
    },
    /// Packed multi-key deferred batched probe (the scan multikey feed's
    /// slot-stream analog): full needed-column staging; per batch — pack the
    /// staged grouping-key lanes into the armed compact table's ≤16-byte key
    /// image (Int shift/mask, numeric keypack, raw-bytes text through the
    /// build-lifetime intern table), one batched compact-table probe, then
    /// the whole-batch fold. Admitted for unguarded plans with NO residual
    /// transitions over 2..N packable keys (`staged_mk_admit` — the compact
    /// table is ARMED as a side effect). Inadmissible values demote at
    /// runtime (NULL keys on a non-nullable image, unpackable numerics,
    /// backstop migration): the compact groups migrate into the C tuplehash
    /// and the batch (and every later one) replays per-row — byte-safe.
    Mk,
}

/// The composition breaker's fold-armed `Sink` face (three modes, see
/// `StagedMode`). `finish` flushes the tail window and runs the delegated
/// build finalize.
struct StagedFoldAggSink<'a, 'mcx> {
    agg: &'a mut ::nodeagg::AggStateData<'mcx>,
    mode: StagedMode,
    /// Fold lanes (`plan.cols`) + their arrival deform bound — the unguarded
    /// mode's whole staging set (byval by classify construction).
    fold_cols: Vec<u16>,
    fold_bound: i32,
    /// Replay slot (virtual, the join output's tupledesc): guarded mode
    /// re-presents each staged row here for the probe/residual/demote
    /// machinery. Unset in unguarded mode.
    stage_slot: Option<ExecSlotId>,
    natts: usize,
    /// Guarded-mode deform bound for the incoming joined slot
    /// (`max_colno_needed`).
    max_colno: i32,
    /// Guarded mode: 0-based attnos of the needed columns (`colnos_needed`),
    /// with each column's attlen for the by-ref datum copy (attbyval columns
    /// skip the copy). Empty in unguarded mode (only fold lanes stage).
    needed: Vec<(u16, i16, bool)>,
    lanes: StagedLanes,
    nstaged: usize,
    /// Per-batch arena for by-ref staged values (guarded/K2 modes); reset
    /// after every flush.
    stage_cxt: Option<::mcx::MemoryContext>,
    idxs: Vec<u32>,
    groups: Vec<core::ptr::NonNull<::execexpr::AggPerGroup>>,
    /// K2 scratch: the batch's grouping-key hashes (batched hash pre-pass).
    hashes: Vec<u32>,
    /// Mk mode's armed shape + scratch (`None` in every other mode).
    mk: Option<StagedMk>,
}

/// Mk-mode state: the armed packed-key layout plus the reused packing
/// scratch, and the one-way demote flag (after a runtime demote the compact
/// table has migrated into the C tuplehash — every later batch replays
/// per-row through the arrival probe, byte-identically).
struct StagedMk {
    shape: ::nodeagg::MkShape,
    demoted: bool,
    packbuf: Vec<u128>,
    keys1: Vec<i64>,
    keys2: Vec<[u64; 2]>,
}

/// Staged-feed admission inputs for the composition. `None` = the composition
/// keeps the per-row sink (no fold plan, or the join output does not line up
/// with the agg's outer shape — defensive, they are the same tlist by
/// construction).
struct StagedFeedShape {
    mode: StagedMode,
    fold_cols: Vec<u16>,
    fold_bound: i32,
    /// Guarded/K2/Mk modes only (empty in arrival mode): each needed
    /// column's 0-based attno, attlen, attbyval.
    needed: Vec<(u16, i16, bool)>,
    max_colno: i32,
    natts: usize,
    /// Mk mode's armed packed-key layout (`None` in every other mode).
    mk: Option<::nodeagg::MkShape>,
}

/// K2 deferred-probe kill-switch: on by default under the lane;
/// `PGRUST_LANE_V2_K2=0`/`off` forces the arrival probe (A/B tooling — both
/// modes are byte-identical).
fn k2_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_LANE_V2_K2").as_deref(), Ok("0") | Ok("off"))
    })
}

/// Slot-stream multi-key kill switch: on by default under the lane;
/// `PGRUST_LANE_V2_MKSTREAM=0`/`off` forces the arrival probe for the staged
/// join/gather feeds' multi-key shapes (A/B tooling — byte-identical up to
/// the group-order relaxation). The scan-feed switch
/// (`PGRUST_LANE_V2_MULTIKEY`) gates this arm too (shared machinery).
fn mkstream_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_LANE_V2_MKSTREAM").as_deref(), Ok("0") | Ok("off"))
    })
}

/// The slot-stream multi-key admission (`scan_mk_shape`'s analog for the
/// staged join/gather feeds), decided once per build — the compact table is
/// ARMED as a side effect. Caller checked: unguarded plan, no residual
/// transitions, no single-key kernel probe. This adds:
///   * 2..N grouping keys, every one a staged needed column;
///   * packable kinds — Int class / numeric (keypack canonical form, gated
///     per value at flush) / at most ONE raw-bytes text component, hosted
///     through the compact table's build-lifetime intern table (slot streams
///     carry raw varlenas — no dict codes — so the feed interns per row; ids
///     are stable for the whole stream and bounded by the backstop's memory
///     check, which counts the intern arena);
///   * the packing admission + table arm (`agg_hash_compact_try_arm_mk`) —
///     first WITH the null-bitmap byte (slot streams carry no no-NULLs
///     proof), and when that busts the 16-byte image budget (the ts-extract-keyed shape's
///     int8+numeric4+intern4 = 16), WITHOUT it plus `flush_mk`'s runtime
///     NULL-demote pre-check (a NULL grouping key migrates to the C table —
///     byte-safe, never packed wrong).
/// `None` = keep the arrival probe (byte-identical); refuse reasons ticked
/// per the scan feed's taxonomy.
fn staged_mk_admit<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    natts: usize,
    needed: &[(u16, i16, bool)],
) -> Option<::nodeagg::MkShape> {
    if !multikey_enabled() || !mkstream_enabled() {
        return None;
    }
    let key_cols = ::nodeagg::agg_hash_key_cols(agg);
    if key_cols.len() < 2 {
        return None;
    }
    let refused = |r: RefuseReason| {
        stats::tick_refused(ShapeClass::AggBuild, r);
        None
    };
    // Mirror `scan_mk_shape`'s vguard belt (the staged feeds admit no
    // varlena fold lanes, so vguards should be empty on unguarded plans).
    if ::nodeagg::agg_lanefold_plan(agg).is_none_or(|plan| !plan.vguards.is_empty()) {
        return refused(RefuseReason::MultiKeyShape);
    }
    // Every key must be a staged needed column (it always is — the spill
    // projection keeps grouping columns); structural gate.
    for &(att, _) in &key_cols {
        if att as usize >= natts || !needed.iter().any(|&(c, _, _)| c == att) {
            return refused(RefuseReason::MultiKeyShape);
        }
    }
    // Kind census: at most one raw-bytes text component (one intern table).
    let mut dict_att = None;
    for &(att, kind) in &key_cols {
        match kind {
            ::nodeagg::GroupKeyKind::Int { .. } | ::nodeagg::GroupKeyKind::Numeric => {}
            ::nodeagg::GroupKeyKind::TextRaw => {
                if dict_att.is_some() {
                    return refused(RefuseReason::MultiKeyShape);
                }
                dict_att = Some(att);
            }
            ::nodeagg::GroupKeyKind::Other => return refused(RefuseReason::MultiKeyShape),
        }
    }
    // Arm: nullable first (NULL keys ride the bitmap — no demote); a budget
    // refusal retries without the null byte, taking the runtime NULL-demote
    // pre-check instead.
    for nullable in [true, false] {
        match ::nodeagg::agg_hash_compact_try_arm_mk(agg, nullable, dict_att) {
            ::nodeagg::CompactArm::Armed => {
                return Some(
                    ::nodeagg::agg_hash_compact_mk_shape(agg).expect("armed multi-key table"),
                );
            }
            ::nodeagg::CompactArm::KeyKind => continue,
            ::nodeagg::CompactArm::SpillRisk => {
                return refused(RefuseReason::CompactSpillRisk)
            }
            ::nodeagg::CompactArm::Off => return None,
        }
    }
    refused(RefuseReason::MultiKeyShape)
}

fn staged_feed_shape<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    join_result_slot: ExecSlotId,
    estate: &EStateData<'mcx>,
) -> Option<StagedFeedShape> {
    let (guarded, fold_cols): (bool, Vec<u16>) = {
        let plan = ::nodeagg::agg_lanefold_plan(agg)?;
        (plan.guarded, plan.cols.iter().copied().collect())
    };
    let has_resid = ::nodeagg::agg_lanefold_has_resid(agg);
    // Full needed-column census up front (the borrow of `agg` must end
    // before the multi-key arm takes `&mut agg` to arm the compact table).
    let (natts, max_colno, needed_all): (usize, i32, Vec<(u16, i16, bool)>) = {
        let desc = estate.slot(join_result_slot).base().tts_tupleDescriptor.as_ref()?;
        let natts = desc.attrs.len();
        let (colnos_needed, max_colno) = ::nodeagg::agg_hash_needed_cols(agg);
        if colnos_needed.len() != natts {
            return None;
        }
        debug_assert!(fold_cols.iter().all(|&c| colnos_needed[c as usize]));
        let needed_all = colnos_needed
            .iter()
            .enumerate()
            .filter(|&(_, &n)| n)
            .map(|(c, _)| (c as u16, desc.attrs[c].attlen, desc.attrs[c].attbyval))
            .collect();
        (natts, max_colno, needed_all)
    };
    let fold_bound = fold_cols.iter().map(|&c| c as i32 + 1).max().unwrap_or(0);
    // Mode choice: guarded plans keep the proof/Demote staging; unguarded
    // plans with fully-admitted transitions (no residuals — they need the
    // live row at probe time) take the K2 deferred batched probe when the
    // grouping key is a single kernel-hostable column, the packed multi-key
    // deferred probe when 2..N keys pack into the compact table
    // (`staged_mk_admit` — armed as a side effect); otherwise the arrival
    // probe. `PGRUST_LANE_V2_K2=0` / `PGRUST_LANE_V2_MKSTREAM=0` force
    // arrival mode per arm (A/B kill-switches; byte-identical either way up
    // to the compact table's group-order relaxation).
    let mut mk = None;
    let mode = if guarded {
        StagedMode::Guarded
    } else if has_resid {
        StagedMode::Arrival
    } else {
        match ::nodeagg::agg_hash_staged_probe_col(agg).filter(|_| k2_enabled()) {
            // The key must be in the staged needed set (it always is — the
            // spill projection keeps grouping columns); structural gate.
            Some(key_col)
                if (key_col as usize) < natts
                    && needed_all.iter().any(|&(c, _, _)| c == key_col) =>
            {
                StagedMode::K2 { key_col }
            }
            Some(_) => StagedMode::Arrival,
            None => match staged_mk_admit(agg, natts, &needed_all) {
                Some(shape) => {
                    mk = Some(shape);
                    StagedMode::Mk
                }
                None => StagedMode::Arrival,
            },
        }
    };
    let needed: Vec<(u16, i16, bool)> =
        if mode == StagedMode::Arrival { Vec::new() } else { needed_all };
    Some(StagedFeedShape { mode, fold_cols, fold_bound, needed, max_colno, natts, mk })
}

impl<'a, 'mcx> StagedFoldAggSink<'a, 'mcx> {
    /// Construction for an admitted shape (`staged_feed_shape` returned the
    /// inputs). The guarded replay slot is memoized across rescan rebuilds (a
    /// fresh extra slot per rebuild would grow es_tupleTable per rescan).
    fn new(
        agg: &'a mut ::nodeagg::AggStateData<'mcx>,
        join_result_slot: ExecSlotId,
        stage_slot_memo: &mut Option<ExecSlotId>,
        shape: StagedFeedShape,
        estate: &mut EStateData<'mcx>,
    ) -> Self {
        let StagedFeedShape { mode, fold_cols, fold_bound, needed, max_colno, natts, mk } =
            shape;
        // Guarded, K2 and Mk modes stage every needed column (guarded for
        // the Demote replay, K2/Mk for the deferred probe + spill/demote
        // replay) and need the replay slot + by-ref arena; arrival mode
        // stages only the (byval) fold lanes.
        let (stage_slot, stage_cxt) = if mode == StagedMode::Arrival {
            (None, None)
        } else {
            let slot = match *stage_slot_memo {
                Some(s) => s,
                None => {
                    let desc =
                        estate.slot(join_result_slot).base().tts_tupleDescriptor.clone();
                    let s = estate
                        .exec_init_extra_tuple_slot(desc, ::types_slot::TupleSlotKind::Virtual);
                    *stage_slot_memo = Some(s);
                    s
                }
            };
            let cxt = estate
                .es_query_cxt
                .context()
                .new_child_bump("lane-v2 staged join feed");
            (Some(slot), Some(cxt))
        };
        let mut lanes = StagedLanes {
            values: vec![Vec::new(); natts],
            isnull: vec![Vec::new(); natts],
        };
        let staged: Vec<u16> = if mode == StagedMode::Arrival {
            fold_cols.clone()
        } else {
            needed.iter().map(|&(c, _, _)| c).collect()
        };
        for &c in &staged {
            lanes.values[c as usize].reserve_exact(STAGE_ROWS);
            lanes.isnull[c as usize].reserve_exact(STAGE_ROWS);
        }
        StagedFoldAggSink {
            agg,
            mode,
            fold_cols,
            fold_bound,
            stage_slot,
            natts,
            max_colno,
            needed,
            lanes,
            nstaged: 0,
            stage_cxt,
            idxs: Vec::new(),
            groups: Vec::new(),
            hashes: Vec::new(),
            mk: mk.map(|shape| StagedMk {
                shape,
                demoted: false,
                packbuf: Vec::new(),
                keys1: Vec::new(),
                keys2: Vec::new(),
            }),
        }
    }

    /// Re-present staged row `k` in the replay slot: needed columns carry the
    /// staged values, unneeded columns are NULL (the spill projection's own
    /// treatment, so a spilled staged row is byte-identical).
    fn replay_row(&self, k: usize, estate: &mut EStateData<'mcx>) {
        let mcx = estate.es_query_cxt;
        let slot = estate.slot_mut(self.stage_slot.expect("staging mode has a replay slot"));
        ::exectuples::exec_clear_tuple(slot, mcx);
        {
            let base = slot.base_mut();
            for c in 0..self.natts {
                base.tts_values[c] = ::datum::Datum::null();
                base.tts_isnull[c] = true;
            }
            for &(c, _, _) in &self.needed {
                let c = c as usize;
                base.tts_values[c] = self.lanes.values[c][k];
                base.tts_isnull[c] = self.lanes.isnull[c][k];
            }
        }
        ::exectuples::exec_store_virtual_tuple(slot);
    }

    /// Unguarded accept: stage the fold lanes (byval — plain datum copies),
    /// then run the group probe + residual transitions NOW against the
    /// incoming joined slot — exactly the per-row sink's call — snapshotting
    /// the pergroup for the batch fold.
    fn accept_unguarded(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()> {
        {
            let slot = estate.slot_mut(tuple);
            ::exectuples::slot_getsomeattrs(slot, self.fold_bound);
            let base = slot.base();
            for &c in &self.fold_cols {
                let c = c as usize;
                self.lanes.values[c].push(base.tts_values[c]);
                self.lanes.isnull[c].push(base.tts_isnull[c]);
            }
        }
        let k = self.nstaged;
        self.nstaged += 1;
        if let Some(pg) = ::nodeagg::agg_hash_build_probe_resid(self.agg, estate, tuple)? {
            self.idxs.push(k as u32);
            self.groups.push(pg);
        }
        if self.nstaged == STAGE_ROWS {
            self.flush_unguarded(estate)?;
        }
        Ok(())
    }

    /// Unguarded flush: just the whole-batch fold over the snapshotted
    /// pergroups (the probe/residuals already ran at arrival).
    fn flush_unguarded(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        let _ = estate;
        // SAFETY: staged fold lanes cover every plan column for all staged
        // rows (`idxs` indexes this window); the plan is unguarded, so no
        // guard proof is required; the rest is agg_fold_staged's contract
        // (the probe just installed each snapshot within this batch).
        unsafe { agg_fold_staged(self.agg, &self.lanes, &self.idxs, &self.groups)? }
        for &c in &self.fold_cols {
            let c = c as usize;
            self.lanes.values[c].clear();
            self.lanes.isnull[c].clear();
        }
        self.idxs.clear();
        self.groups.clear();
        self.nstaged = 0;
        Ok(())
    }

    /// Stage every needed column of the incoming joined row (guarded and K2
    /// modes — nothing runs at arrival in either).
    fn stage_needed_row(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()> {
        {
            let slot = estate.slot_mut(tuple);
            ::exectuples::slot_getsomeattrs(slot, self.max_colno);
        }
        let StagedFoldAggSink { needed, lanes, stage_cxt, .. } = &mut *self;
        let base = estate.slot(tuple).base();
        for &(c, attlen, byval) in needed.iter() {
            let c = c as usize;
            let (v, isnull) = (base.tts_values[c], base.tts_isnull[c]);
            // By-ref values may point into per-tuple memory the probe resets
            // row to row (and heap pages the outer scan unpins): copy into
            // the per-batch arena so the staged window is self-contained.
            let v = if isnull || byval {
                v
            } else {
                let cxt = stage_cxt.as_ref().expect("staging mode has a stage cxt");
                crate::nodesubplan::datum_copy_in(cxt.mcx(), v, attlen)?
            };
            lanes.values[c].push(v);
            lanes.isnull[c].push(isnull);
        }
        self.nstaged += 1;
        Ok(())
    }

    /// Guarded accept: stage every needed column (nothing may run before the
    /// batch guard proof — a Demote must replay the WHOLE batch through the
    /// checked per-row program).
    fn accept_guarded(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()> {
        self.stage_needed_row(tuple, estate)?;
        if self.nstaged == STAGE_ROWS {
            self.flush_guarded(estate)?;
        }
        Ok(())
    }

    /// K2 accept: stage every needed column; the group probe is deferred to
    /// the batched flush.
    fn accept_k2(&mut self, tuple: ExecSlotId, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        self.stage_needed_row(tuple, estate)?;
        if self.nstaged == STAGE_ROWS {
            self.flush_k2(estate)?;
        }
        Ok(())
    }

    /// K2 flush — the batched group-probe pre-pass: (1) one CFI per batch
    /// (design §9 cadence); (2) the batched hash loop over the staged
    /// grouping-key lane (bit-identical per element to the per-row
    /// `TupleHashTableHash`, by the probe-kernel contract); (3) the in-order
    /// probe of every staged row through the same C-ported tuplehash lookup
    /// (same first-arrival insertion, same entry init, same spill-mode gate —
    /// identical table layout / iteration order / output bytes); spill-mode
    /// misses replay the row (needed cols, unneeded NULL — the spill
    /// projection's own treatment) and spill it byte-identically; (4) the
    /// whole-batch fold over the resolved pergroups.
    fn flush_k2(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        let n = self.nstaged;
        if n == 0 {
            return Ok(());
        }
        ::postgres_seams::check_for_interrupts::call()?;
        let StagedMode::K2 { key_col } = self.mode else {
            unreachable!("flush_k2 outside K2 mode")
        };
        let kc = key_col as usize;
        {
            let StagedFoldAggSink { agg, lanes, hashes, .. } = &mut *self;
            ::nodeagg::agg_hash_hash_staged(
                agg,
                &lanes.values[kc],
                &lanes.isnull[kc],
                hashes,
            )?;
        }
        self.idxs.clear();
        self.groups.clear();
        for k in 0..n {
            let probed = ::nodeagg::agg_hash_probe_staged(
                self.agg,
                estate,
                self.lanes.values[kc][k],
                self.lanes.isnull[kc][k],
                self.hashes[k],
            )?;
            match probed {
                Some(pg) => {
                    self.idxs.push(k as u32);
                    self.groups.push(pg);
                }
                None => {
                    // Spill-mode miss: replay + spill; no transition runs
                    // for the row (the per-row path's exact treatment).
                    let stage_slot =
                        self.stage_slot.expect("staging mode has a replay slot");
                    self.replay_row(k, estate);
                    ::nodeagg::agg_hash_spill_staged(
                        self.agg, estate, stage_slot, self.hashes[k],
                    )?;
                }
            }
        }
        // SAFETY: staged lanes cover every plan column for all staged rows
        // (plan.cols ⊆ colnos_needed); the plan is unguarded (K2 admission);
        // each pergroup was installed by the probe within this batch; the
        // rest is agg_fold_staged's contract.
        unsafe { agg_fold_staged(self.agg, &self.lanes, &self.idxs, &self.groups)? }
        for &(c, _, _) in &self.needed {
            let c = c as usize;
            self.lanes.values[c].clear();
            self.lanes.isnull[c].clear();
        }
        self.nstaged = 0;
        self.stage_cxt.as_mut().expect("staging mode has a stage cxt").reset();
        Ok(())
    }

    /// Mk accept: stage every needed column; the packed multi-key probe is
    /// deferred to the batched flush.
    fn accept_mk(&mut self, tuple: ExecSlotId, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        self.stage_needed_row(tuple, estate)?;
        if self.nstaged == STAGE_ROWS {
            self.flush_mk(estate)?;
        }
        Ok(())
    }

    /// Clear the staged window (all needed lanes + the by-ref arena) after a
    /// staging-mode flush.
    fn clear_staged_window(&mut self) {
        for &(c, _, _) in &self.needed {
            let c = c as usize;
            self.lanes.values[c].clear();
            self.lanes.isnull[c].clear();
        }
        self.nstaged = 0;
        self.stage_cxt.as_mut().expect("staging mode has a stage cxt").reset();
    }

    /// Mk flush — the packed multi-key deferred probe (`scan_mk_batch`'s
    /// slot-stream analog): (1) one CFI per batch (design §9 cadence);
    /// (2) demote decision BEFORE any packing — the runtime backstop
    /// (memory migration), then per-value packability over the staged key
    /// lanes (NULL keys on a non-nullable image; unpackable numerics —
    /// range / non-minimal display scale). A demote migrates the compact
    /// groups into the C tuplehash ONCE; this batch and every later one
    /// replay per-row through the arrival probe (byte-identical, spill
    /// machinery intact). (3) the component-major pack of the staged key
    /// lanes into the reused u128 accumulator — Int shift/mask, numeric
    /// keypack, raw-bytes text through the build-lifetime intern table
    /// (slot streams carry raw varlenas, no dict codes: intern per row;
    /// NULLs on nullable images set the bitmap bit, value bits zero);
    /// (4) the compact-table batched probe + new-group seeding; (5) the
    /// whole-batch fold. Every staged row is a survivor (the feed stages
    /// only emitted rows), so the fold covers `0..n`.
    fn flush_mk(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        let n = self.nstaged;
        if n == 0 {
            return Ok(());
        }
        ::postgres_seams::check_for_interrupts::call()?;
        debug_assert!(self.mode == StagedMode::Mk, "flush_mk outside Mk mode");
        let live = !self.mk.as_ref().expect("Mk mode carries its state").demoted
            && ::nodeagg::agg_hash_compact_backstop(self.agg, estate)?;
        let packable = live && {
            let StagedFoldAggSink { lanes, mk, .. } = &*self;
            let shape = &mk.as_ref().expect("Mk mode carries its state").shape;
            shape.comps.iter().all(|comp| {
                let att = comp.att as usize;
                let (values, isnull) = (&lanes.values[att], &lanes.isnull[att]);
                match comp.kind {
                    ::nodeagg::MkCompKind::Numeric { width } => (0..n).all(|k| {
                        if isnull[k] {
                            shape.nullable
                        } else {
                            ::nodeagg::mk_numeric_datum_bits(values[k], width).is_some()
                        }
                    }),
                    // Int/Intern values always pack; a NULL key needs the
                    // bitmap byte — without it, demote.
                    ::nodeagg::MkCompKind::Int { .. } | ::nodeagg::MkCompKind::Intern => {
                        shape.nullable || (0..n).all(|k| !isnull[k])
                    }
                }
            })
        };
        if live && !packable {
            ::nodeagg::agg_hash_compact_disarm(self.agg, estate)?;
        }
        if !live || !packable {
            self.mk.as_mut().expect("Mk mode carries its state").demoted = true;
            return self.flush_mk_demoted(estate);
        }
        {
            let StagedFoldAggSink { agg, lanes, mk, stage_cxt, idxs, groups, .. } = &mut *self;
            let StagedMk { shape, packbuf, keys1, keys2, .. } =
                mk.as_mut().expect("Mk mode carries its state");
            packbuf.clear();
            packbuf.resize(n, 0u128);
            for (j, comp) in shape.comps.iter().enumerate() {
                let att = comp.att as usize;
                let off_bits = comp.off as u32 * 8;
                let (values, isnull) = (&lanes.values[att], &lanes.isnull[att]);
                // Only read when `shape.nullable` (guarded per row below).
                let null_bit = if shape.nullable {
                    1u128 << (shape.null_off() as u32 * 8 + j as u32)
                } else {
                    0
                };
                match comp.kind {
                    ::nodeagg::MkCompKind::Int { width } => {
                        let mask =
                            if width == 8 { u64::MAX } else { (1u64 << (width * 8)) - 1 };
                        for (k, pb) in packbuf.iter_mut().enumerate() {
                            if shape.nullable && isnull[k] {
                                // CH nullable_keys128: bit j set, value bits
                                // zero — NOT-DISTINCT composite NULLs hold.
                                *pb |= null_bit;
                                continue;
                            }
                            debug_assert!(!isnull[k], "NULL keys demote before packing");
                            let v = match width {
                                2 => values[k].as_i16() as i64,
                                4 => values[k].as_i32() as i64,
                                _ => values[k].as_i64(),
                            };
                            *pb |= (((v as u64) & mask) as u128) << off_bits;
                        }
                    }
                    ::nodeagg::MkCompKind::Numeric { width } => {
                        for (k, pb) in packbuf.iter_mut().enumerate() {
                            if shape.nullable && isnull[k] {
                                *pb |= null_bit;
                                continue;
                            }
                            let bits = ::nodeagg::mk_numeric_datum_bits(values[k], width)
                                .expect("numeric packability proven by the batch pre-check");
                            *pb |= (bits as u128) << off_bits;
                        }
                    }
                    ::nodeagg::MkCompKind::Intern => {
                        let cxt = stage_cxt.as_ref().expect("staging mode has a stage cxt");
                        for (k, pb) in packbuf.iter_mut().enumerate() {
                            if shape.nullable && isnull[k] {
                                *pb |= null_bit;
                                continue;
                            }
                            debug_assert!(!isnull[k], "NULL keys demote before packing");
                            // SAFETY: staged non-null live text varlena,
                            // datum-copied into the batch arena at accept
                            // (kernel selection proved the column type). A
                            // detoast copy lands in the batch arena too —
                            // reset after this flush; the intern table owns
                            // its own copy of the bytes.
                            let v = unsafe {
                                ::types_fmgr::datum_varlena_packed(values[k], cxt.mcx())
                            }?;
                            let id = ::nodeagg::agg_hash_compact_intern(agg, v.data());
                            *pb |= (id as u128) << off_bits;
                        }
                    }
                }
            }
            // Split the accumulator into the packed key lane and probe
            // (two-word shapes view the accumulator in place — mkaccept
            // inc-1).
            if shape.two_words {
                let lane = ::nodeagg::mk_keys2_lane(packbuf, keys2);
                ::nodeagg::agg_hash_compact_batch_mk2(agg, lane, groups)?;
            } else {
                keys1.clear();
                keys1.extend(packbuf.iter().map(|&w| w as u64 as i64));
                ::nodeagg::agg_hash_compact_batch_mk1(agg, keys1, groups)?;
            }
            idxs.clear();
            idxs.extend(0..n as u32);
            // SAFETY: staged lanes cover every plan column for all staged
            // rows (plan.cols ⊆ colnos_needed); the plan is unguarded (Mk
            // admission); each pergroup was installed by the compact probe
            // within this batch; the rest is agg_fold_staged's contract.
            unsafe { agg_fold_staged(agg, &*lanes, idxs, groups)? }
        }
        self.clear_staged_window();
        Ok(())
    }

    /// Mk demote leg: the staged window replays per-row through the arrival
    /// probe against the C tuplehash (the compact groups migrated at the
    /// demote) — `flush_guarded`'s replay loop without the guard proof (the
    /// plan is unguarded by Mk admission) — then the whole-batch fold.
    /// Spill-mode misses return no pergroup and run no transition, exactly
    /// the per-row build's treatment.
    fn flush_mk_demoted(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        let n = self.nstaged;
        let stage_slot = self.stage_slot.expect("staging mode has a replay slot");
        self.idxs.clear();
        self.groups.clear();
        for k in 0..n {
            self.replay_row(k, estate);
            if let Some(pg) =
                ::nodeagg::agg_hash_build_probe_resid(self.agg, estate, stage_slot)?
            {
                self.idxs.push(k as u32);
                self.groups.push(pg);
            }
        }
        // SAFETY: staged lanes cover every plan column for all staged rows
        // (plan.cols ⊆ colnos_needed); the plan is unguarded (Mk admission);
        // each pergroup was installed by the probe within this batch; the
        // rest is agg_fold_staged's contract.
        unsafe { agg_fold_staged(self.agg, &self.lanes, &self.idxs, &self.groups)? }
        self.clear_staged_window();
        Ok(())
    }

    /// Guarded flush: one CHECK_FOR_INTERRUPTS per batch (design §9
    /// batch-operator cadence), the guard proof re-run per batch, then the
    /// replayed probe/residual + fold — or the whole batch through the
    /// checked per-row program on Demote.
    fn flush_guarded(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        let n = self.nstaged;
        if n == 0 {
            return Ok(());
        }
        ::postgres_seams::check_for_interrupts::call()?;
        let stage_slot = self.stage_slot.expect("guarded mode has a replay slot");
        // Re-prove per staged batch. Join output has no zone map, so the
        // proof always runs the exact data-scan tier over the staged lanes.
        // Every staged row is selected (the join emits only qual-passing
        // rows).
        let demote = {
            let plan =
                ::nodeagg::agg_lanefold_plan(self.agg).expect("staged feed without a plan");
            let nwords = n.div_ceil(64);
            let mut rows = [u64::MAX; ::exectuples::SOA_BM_WORDS];
            if n % 64 != 0 {
                rows[nwords - 1] = (1u64 << (n % 64)) - 1;
            }
            // SAFETY: every staged lane value is a live datum copied from
            // the joined row at accept time (StagedLanes contract); the
            // staged join feed admits no varlena lanes, so no vguard column
            // is ever probed here.
            unsafe {
                ::lanefold::check_guards(plan, &self.lanes, &rows[..nwords], |_| None)
                    == ::lanefold::GuardCheck::Demote
            }
        };
        if demote {
            // The WHOLE batch runs the checked per-row program (never mixing
            // a partial fold with per-row transitions — lanefold contract);
            // it raises C's error at C's row.
            for k in 0..n {
                self.replay_row(k, estate);
                ::nodeagg::agg_hash_build_accept(self.agg, estate, stage_slot)?;
            }
        } else {
            self.idxs.clear();
            self.groups.clear();
            for k in 0..n {
                self.replay_row(k, estate);
                if let Some(pg) =
                    ::nodeagg::agg_hash_build_probe_resid(self.agg, estate, stage_slot)?
                {
                    self.idxs.push(k as u32);
                    self.groups.push(pg);
                }
            }
            // SAFETY: staged lanes cover every plan column for all staged
            // rows (plan.cols ⊆ colnos_needed); the guard proof passed on
            // this batch; the rest is agg_fold_staged's contract.
            unsafe { agg_fold_staged(self.agg, &self.lanes, &self.idxs, &self.groups)? }
        }
        for &(c, _, _) in &self.needed {
            let c = c as usize;
            self.lanes.values[c].clear();
            self.lanes.isnull[c].clear();
        }
        self.nstaged = 0;
        self.stage_cxt.as_mut().expect("guarded mode has a stage cxt").reset();
        Ok(())
    }
}

impl<'mcx> Sink<'mcx> for StagedFoldAggSink<'_, 'mcx> {
    fn accept(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<SinkFeed> {
        match self.mode {
            StagedMode::Guarded => self.accept_guarded(tuple, estate)?,
            StagedMode::K2 { .. } => self.accept_k2(tuple, estate)?,
            StagedMode::Mk => self.accept_mk(tuple, estate)?,
            StagedMode::Arrival => self.accept_unguarded(tuple, estate)?,
        }
        Ok(SinkFeed::NeedMore)
    }

    // Stage-4 combine seam (see HashAggBuildSink::combine): flush the staged
    // tail first so the handed table is complete, then hand off; the
    // following finish re-flushes nothing (flushes drain their staging) and
    // skips the install (combined flag).
    fn combine(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        match self.mode {
            StagedMode::Guarded => self.flush_guarded(estate)?,
            StagedMode::K2 { .. } => self.flush_k2(estate)?,
            StagedMode::Mk => self.flush_mk(estate)?,
            StagedMode::Arrival => self.flush_unguarded(estate)?,
        }
        ::nodeagg::agg_hash_build_combine(self.agg, estate)
    }

    fn finish(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        match self.mode {
            StagedMode::Guarded => self.flush_guarded(estate)?,
            StagedMode::K2 { .. } => self.flush_k2(estate)?,
            StagedMode::Mk => self.flush_mk(estate)?,
            StagedMode::Arrival => self.flush_unguarded(estate)?,
        }
        ::nodeagg::agg_hash_build_finish(self.agg, estate)
    }
}

/// Engagement trace for the composition feeds, env-gated
/// (`PGRUST_LANE_V2_TRACE=1`): one line per build-feed engagement on stderr.
/// Diagnostics only — never affects execution.
/// GL-VECACCEPT lane posture (knob unification, the flip's coherent
/// surface): `PGRUST_RUNTIME_AGG_VECACCEPT` governs the WHOLE vectorized-
/// accept lane — the distinct sink's whole-granule accept (GL-VECACCEPT-1)
/// AND the K2 agg drain's (GL-VECACCEPT-2). DEFAULT ON (both flips'
/// evidence: GL-VECACCEPT-1 §5b — 0.64-0.72 vec/base everywhere incl. the
/// shipped binary; GL-VECACCEPT-2 §4 — never loses a cell, flips the
/// 1e6-group band); t35 flipped-kill: `0|off` restores BOTH incumbent
/// accepts byte-identically. Per-sink sub-kill: the K2 side additionally
/// honors `PGRUST_RUNTIME_AGG_VECACCEPT_K2=0|off` (adjudication
/// granularity — kill one sink without the other).
pub(super) fn vecaccept_lane_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(
            std::env::var("PGRUST_RUNTIME_AGG_VECACCEPT").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

fn trace_feed(msg: &str) {
    static ON: OnceLock<bool> = OnceLock::new();
    if crate::once_val(&ON, || {
        matches!(std::env::var("PGRUST_LANE_V2_TRACE").as_deref(), Ok("1") | Ok("on"))
    }) {
        eprintln!("[lanev2] {msg}");
    }
}

/// Try to let the lane own `Agg(hashed) → HashJoin(admitted type) → scans`
/// — the first breaker-to-breaker composition. Three pipelines on two breaker
/// nodes, all phase flags node-resident row-path state:
///
///   1. build:  inner scan → filter/project → HashJoinBuildSink
///   2. probe:  outer scan → filter/project → JoinProbe → agg build sink
///   3. emit:   HashAggSource → HashAggEmit → RootAdapter (one group per pull)
///
/// The probe-pipeline sink is the staged fold feed (`StagedFoldAggSink`) when
/// the agg carries a lanefold plan — the batched joined-row feed — and the
/// per-row `HashAggBuildSink` otherwise. `stage_slot` memoizes the staged
/// feed's replay slot across rescan rebuilds.
///
/// `None` = refused (caller falls to the per-tuple `exec_agg` over
/// `exec_hash_join`, byte-identically — including after a lane-delegated
/// join build that then spill-refused).
#[inline]
pub fn try_own_agg_over_hash_join<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    hj: &mut crate::procnode::HashJoinNode<'mcx>,
    stage_slot: &mut Option<ExecSlotId>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic per-call gates, ticked under the join class (the composition's
    // pipelines all hang off the join's drive).
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::Join, RefuseReason::Epq);
        return Ok(None);
    }
    // M3 runtime hash-join arm (FORCED engagement under PGRUST_RUNTIME=1 +
    // pgrust.runtime_hashjoin_pool; serial plan surface unchanged). Owns the
    // plain-agg-over-join probe tails; None = not engaged/refused — fall
    // through byte-identically (nothing was consumed).
    if let Some(r) = runtime_hashjoin::try_own_agg_over_hash_join_runtime(agg, hj, estate)? {
        return Ok(Some(r));
    }
    if !::nodeagg::agg_hash_breaker_admissible(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AggNotDrainable);
        return Ok(None);
    }
    if !hash_join_lane_fusible(hj, estate)? {
        return Ok(None);
    }
    // exec_agg's top-of-call guard: a drained agg stays drained.
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    if !agg_hash_join_build_if_needed(agg, hj, stage_slot, estate)? {
        return Ok(None);
    }
    // Agg emit phase (every call): one qual-passing group per PG pull, in
    // C's retrieve order.
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step(agg, &mut HashAggSource, &mut HashAggEmit, &mut root, estate)?))
}

/// Build phases of the agg-over-join composition (join build, then the probe
/// drain into the agg breaker sink), once, lazily. `Ok(false)` = multi-batch
/// spill refuse — the caller must refuse ownership; no lane tuple has been
/// emitted, so the fallback per-tuple agg over `exec_hash_join` resumes from
/// HJ_NEED_NEW_OUTER over the identical table. Shared by the bare
/// composition hook above and the Limit-over-agg chain (`try_own_limit`).
fn agg_hash_join_build_if_needed<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    hj: &mut crate::procnode::HashJoinNode<'mcx>,
    stage_slot: &mut Option<ExecSlotId>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if ::nodeagg::agg_hash_table_filled(agg) {
        return Ok(true);
    }
    let crate::procnode::HashJoinNode { state, outer, hash, .. } = hj;
    let crate::procnode::HashSubNode { state: hstate, child } = &mut **hash;
    // Join build phase (once, lazily; a rescan that rebuilt the inner
    // side re-enters here via the node's own HJ_BUILD_HASHTABLE).
    if ::nodehashjoin::lane_join_phase(state, hstate)
        == ::nodehashjoin::LaneJoinPhase::Build
    {
        let done = join_build_dispatch(state, hstate, child, estate)?;
        if !done.empty && done.nbatch > 1 {
            // Spill refuse before any lane tuple is emitted; the
            // fallback per-tuple agg over exec_hash_join resumes from
            // HJ_NEED_NEW_OUTER over the identical table.
            stats::tick_refused(ShapeClass::Join, RefuseReason::MultiBatch);
            return Ok(false);
        }
        // Bloom pushdown reclaim (see try_own_hash_join): legacy-seat
        // parity — SeqScan outer drives only; the arm re-applies the
        // row path's exact push gate.
        if !done.empty {
            if let crate::procnode::PlanStateNode::SeqScan(_) = &**outer {
                ::nodehashjoin::lane_probe_filter_arm(state, hstate);
            }
        }
    }
    match ::nodehashjoin::lane_join_phase(state, hstate) {
        ::nodehashjoin::LaneJoinPhase::EmptyDone => {
            // A non-fill-outer join (INNER/SEMI/RIGHT/RIGHT_SEMI/
            // RIGHT_ANTI) over an empty build: emits nothing — an empty
            // build has no unmatched inner tuples to fill either — and
            // the outer child is never pulled (C's early return;
            // LEFT/FULL/ANTI never take this phase — their empty build
            // proceeds to the probe and null-fills). The agg finalizes
            // over an empty input.
            stats::tick_owned(ShapeClass::AggBuild);
            let mut sink = HashAggBuildSink { agg };
            sink.finish(estate)?;
        }
        ::nodehashjoin::LaneJoinPhase::Probe => {
            if hstate.table.as_ref().expect("probe phase has a table").nbatch > 1 {
                stats::tick_refused(ShapeClass::Join, RefuseReason::MultiBatch);
                return Ok(false);
            }
            // One OWNED tick per lane-owned agg build event (here the
            // build is fed by the join probe drain).
            stats::tick_owned(ShapeClass::AggBuild);
            // Batched joined-row feed when the agg carries a fold plan;
            // the per-row breaker sink otherwise.
            match staged_feed_shape(agg, state.ps_ResultTupleSlot, estate) {
                Some(shape) => {
                    trace_feed(match shape.mode {
                        StagedMode::Guarded => {
                            "agg-over-join: staged fold feed engaged (guarded)"
                        }
                        StagedMode::K2 { .. } => {
                            "agg-over-join: staged fold feed engaged (k2 probe)"
                        }
                        StagedMode::Mk => {
                            "agg-over-join: staged fold feed engaged (mk probe)"
                        }
                        StagedMode::Arrival => "agg-over-join: staged fold feed engaged",
                    });
                    let mut sink = StagedFoldAggSink::new(
                        agg,
                        state.ps_ResultTupleSlot,
                        stage_slot,
                        shape,
                        estate,
                    );
                    join_probe_drain_dispatch(state, hstate, outer, &mut sink, estate)?;
                }
                None => {
                    trace_feed("agg-over-join: per-row sink (no fold plan)");
                    let mut sink = HashAggBuildSink { agg: &mut *agg };
                    join_probe_drain_dispatch(state, hstate, outer, &mut sink, estate)?;
                }
            }
        }
        ::nodehashjoin::LaneJoinPhase::Build => unreachable!("build ran above"),
    }
    Ok(true)
}

// ===========================================================================
// Streaming Limit + Unique (Phase-2 breadth): mid-pipeline `TupleOp`s at the
// TOP of an already-lane-owned chain, engaged ONLY where the lane owns the
// child pipeline (admission economics, design §4): a Volcano Limit/Unique is
// already cheap per-tuple and PG's pull already stops a lane pipeline lazily,
// so ownership here buys chain continuity (no per-tuple root adapter between
// the breaker emit and the limit/dedup — and future within-pipeline fusion),
// never a new layer over a refused child.
//
//   Limit  (Pattern 2, DuckDB streaming limit): counts in the node's own
//          LimitState (lstate/position — C's cross-call state, so a Volcano
//          fallback at any call boundary is byte-safe), delivers the boundary
//          tuple via `Paused`, reports `Finished` on the next driver round —
//          the source is never pulled past the boundary tuple's batch, and
//          quals/projections are never evaluated past the limit (C's LIMIT
//          stops calling its child). OFFSET tuples are pulled + discarded,
//          exactly as C's LIMIT_RESCAN skip loop pulls them.
//   Unique (over the sort breaker): adjacent-dedup streaming op — one sorted
//          tuple in, 0..1 group heads out, via `nodeunique::lane_unique_feed`
//          (the SAME grouping-equality program + prev-slot copy exec_unique
//          runs — reused, not reimplemented).
//
// Row-identity note (LIMIT without ORDER BY): C returns whichever rows its
// plan yields first. The lane's owned pipelines emit C's rows in C's order BY
// CONSTRUCTION — scan pipelines walk the same pages/TID runs in the same
// order, and breaker read-backs delegate to the same tuplesort / hash-table
// retrieves — so the lane's first k tuples are C's first k tuples,
// byte-identically (verified by the full regress off/on comparison).
//
// Refused shapes (each byte-safe on the Volcano fallback):
//   * LIMIT ... WITH TIES — needs boundary-tuple retention + the sort-peer
//     equality walk (LIMIT_WINDOWEND_TIES); staged later. (PG's Limit node
//     has no percent-limit form — nothing to gate.)
//   * Limit/Unique over a BARE scan — the scan hooks themselves refuse
//     standalone ownership (per-tuple emission through the pull adapter with
//     no batch consumer above = pure adapter overhead); a Volcano
//     Limit/Unique over the refused scan IS C's shape, so taking ownership
//     adds a layer with no consumer benefit.
//   * Limit over a bare HashJoin — needs a two-TupleOp chain driver
//     (JoinProbe → LimitOp); staged with the next chain generalization.
//   * Backward/scrollable cursors — a scrollable/backward cursor forces
//     randomAccess on the Sort child (refused by `sort_fusible`), Limit
//     never sees EXEC_FLAG_MARK (init assert), Unique never sees
//     BACKWARD/MARK (init assert); the dynamic direction gate refuses any
//     non-forward pull.
//   * Hashed DISTINCT is NOT here: the planner emits Agg (AGG_HASHED, zero
//     aggregates), which the hash-agg breaker already admits
//     (`agg_hash_breaker_admissible` — evaltrans is an empty transition
//     program, subplan- and param-free trivially).
// ===========================================================================

/// The Limit node as a mid-pipeline streaming operator. All window
/// arithmetic delegates to `nodelimit`'s lane seam, which mirrors
/// `exec_limit`'s forward COUNT arms verbatim over the same node state.
struct LimitOp<'a, 'mcx> {
    limit: &'a mut ::nodelimit::LimitState<'mcx>,
}

impl<'mcx> TupleOp<'mcx> for LimitOp<'_, 'mcx> {
    fn pending(&self) -> bool {
        // Window complete (the boundary tuple was already delivered via
        // `Paused`): the next driver round must resume() → `Finished`
        // BEFORE the source is pulled again — the Paused-then-Finished rule.
        ::nodelimit::lane_limit_window_done(self.limit)
    }

    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        match ::nodelimit::lane_limit_feed(self.limit, tuple) {
            ::nodelimit::LaneLimitFeed::Skip => Ok(OpStatus::NeedInput),
            ::nodelimit::LaneLimitFeed::Emit => Ok(match out.accept(tuple, estate)? {
                SinkFeed::Full => OpStatus::Paused,
                SinkFeed::NeedMore => OpStatus::NeedInput,
            }),
            ::nodelimit::LaneLimitFeed::EmitBoundary => {
                // Paused-then-Finished (`OpStatus::Finished` contract):
                // deliver the boundary tuple now; pending()/resume() report
                // Finished on the next driver round. The downstream sink is
                // always the capacity-one root here (LimitOp tops the chain),
                // so accept necessarily returns Full.
                let fed = out.accept(tuple, estate)?;
                debug_assert_eq!(fed, SinkFeed::Full, "limit chain must end at the root adapter");
                let _ = fed;
                Ok(OpStatus::Paused)
            }
        }
    }

    fn resume(
        &mut self,
        _out: &mut dyn Sink<'mcx>,
        _estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        // Only reachable via pending() = window done: flip to LIMIT_WINDOWEND
        // (what C's next ExecLimit call would do) and end the stream.
        ::nodelimit::lane_limit_end_window(self.limit);
        Ok(OpStatus::Finished)
    }
}

/// The Unique node as a mid-pipeline streaming operator: never pends (no
/// intra-tuple expansion) and never finishes early.
struct UniqueOp<'a, 'mcx> {
    unique: &'a mut ::nodeunique::UniqueState<'mcx>,
}

impl<'mcx> TupleOp<'mcx> for UniqueOp<'_, 'mcx> {
    fn pending(&self) -> bool {
        false
    }

    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        match ::nodeunique::lane_unique_feed(self.unique, estate, tuple)? {
            None => Ok(OpStatus::NeedInput),
            Some(result) => Ok(match out.accept(result, estate)? {
                SinkFeed::Full => OpStatus::Paused,
                SinkFeed::NeedMore => OpStatus::NeedInput,
            }),
        }
    }

    fn resume(
        &mut self,
        _out: &mut dyn Sink<'mcx>,
        _estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        unreachable!("UniqueOp never pends")
    }
}

/// Admission for a hash-agg breaker child under a lane Limit or under the
/// sort breaker (`sort_refuse_reason`'s Agg arm — the `ORDER BY agg` tail):
/// the agg-side breaker gate × the child gates × (for the SeqScan feed) the
/// memoized `AggLaneChoice` — exactly the bare `agg_arm` hooks' admission
/// (`try_own_agg_over_seq_scan` / `try_own_agg_over_hash_join`), including
/// the economics `Refuse` arm, so a Limit- or Sort-owned agg chain admits
/// precisely where the agg hook would.
fn agg_child_fusible<'mcx>(
    aps: &mut crate::procnode::AggPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if !::nodeagg::agg_hash_breaker_admissible(&aps.agg) {
        return Ok(false);
    }
    match &mut aps.outer {
        crate::procnode::PlanStateNode::SeqScan(ss) => {
            if !seq_scan_fusible(ss, estate)? {
                return Ok(false);
            }
            let c = match aps.lane_choice {
                Some(c) => c,
                None => {
                    let c = decide_agg_lane(&aps.agg, ss, &mut aps.lane_exprkey, estate)?;
                    aps.lane_choice = Some(c);
                    c
                }
            };
            Ok(c != AggLaneChoice::Refuse)
        }
        crate::procnode::PlanStateNode::HashJoin(hj) => hash_join_lane_fusible(hj, estate),
        // Agg-over-gather: no child-side structural gate — the build reuses
        // `exec_gather` verbatim (section header), so every gather shape the
        // breaker-admissible agg sits on is drivable.
        crate::procnode::PlanStateNode::Gather(_) => Ok(agg_gather_enabled()),
        _ => Ok(false),
    }
}

/// Try to let the lane own a `Limit` over a lane-owned chain — the streaming
/// limit (see the section header above for the protocol, the row-identity
/// argument, and the documented refusals). Admitted children: the sort
/// breaker, and the hash-agg breaker over its admitted feeds (SeqScan, or
/// the hash-join composition). `None` = refused; falling to `exec_limit` is
/// byte-safe at any boundary because the lane drives the SAME LimitState
/// machine C does (including after the prologue below ran — C's own INITIAL
/// arm would have run the same recompute once).
#[inline]
pub fn try_own_limit<'mcx>(
    l: &mut crate::procnode::LimitNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    use ::nodelimit::LimitStateCond::*;
    // Dynamic per-call gates + the limit-side shape gate (COUNT only; the
    // option is init-stable so this refuse is stable too).
    if estate.es_epq_active || !::nodelimit::lane_limit_admissible(&l.state) {
        return Ok(None);
    }
    // Child admission BEFORE any state effect (a refuse must leave the node
    // untouched). Child verdicts are memoized on the child nodes.
    let child_ok = match &mut *l.outer {
        crate::procnode::PlanStateNode::Sort(s) => sort_lane_fusible_memo(s, estate)?,
        crate::procnode::PlanStateNode::Agg(aps) => agg_child_fusible(&mut **aps, estate)?,
        _ => false,
    };
    if !child_ok {
        return Ok(None);
    }
    // C's exec_limit entry: CFI, then the LIMIT_INITIAL recompute (evaluates
    // OFFSET/LIMIT — same negative-value errors — and pushes the tuple bound
    // to the child: the Sort's top-N bound; a no-op for Agg).
    ::nodelimit::lane_limit_prologue(&mut l.state, &mut *l.outer, estate)?;
    match l.state.lstate {
        // Terminal forward states: nothing more to return (C's arms).
        LIMIT_EMPTY | LIMIT_WINDOWEND | LIMIT_SUBPLANEOF => return Ok(Some(None)),
        LIMIT_RESCAN => {
            // LIMIT 0: the window is empty and the child is NEVER pulled
            // (C's `count <= 0 && !noCount` arm) — no feed, no build.
            if ::nodelimit::lane_limit_empty_window(&mut l.state) {
                return Ok(Some(None));
            }
        }
        LIMIT_INWINDOW => {}
        LIMIT_INITIAL => unreachable!("prologue recomputed"),
        // WITH TIES boundary walk in progress — the lane's COUNT-only shape
        // gate refuses TIES plans, so refuse defensively. (LIMIT_WINDOWSTART
        // deleted with the backward-execution wave B4 — it was backward-only.)
        LIMIT_WINDOWEND_TIES => return Ok(None),
    }
    // Run the owned chain: child pipeline → LimitOp → root adapter.
    let r = match &mut *l.outer {
        crate::procnode::PlanStateNode::Sort(s) => {
            // C's first child pull enters ExecSort: entry CFI, then the feed
            // (the tuplesort bound set by the prologue makes it top-N,
            // exactly as C's bounded sort under Limit).
            ::postgres_seams::check_for_interrupts::call()?;
            let crate::procnode::SortNode { state, outer, outer_desc, rd_shape_refused, .. } = s;
            if !sort_feed_if_needed(state, &mut **outer, outer_desc, None, estate)? {
                // Agg-over-join multi-batch spill refuse, before any lane
                // tuple or sort-side effect: exec_limit over the per-tuple
                // sort/agg/join resumes byte-identically (the recompute above
                // ran once, as C's INITIAL arm would have).
                return Ok(None);
            }
            let mut op = LimitOp { limit: &mut l.state };
            let mut root = RootAdapter::new(None);
            pull_step_chain(state, &mut SortEmitSource, &mut SortEmit, &mut op, &mut root, estate)?
        }
        crate::procnode::PlanStateNode::Agg(aps) => {
            let aps = &mut **aps;
            // exec_agg's top-of-call guard: a drained agg stays drained (the
            // hash iterator is spent) — treat as source EOF.
            if ::nodeagg::agg_is_done(&aps.agg) {
                None
            } else {
                let built = match &mut aps.outer {
                    crate::procnode::PlanStateNode::SeqScan(ss) => {
                        let c = aps.lane_choice.expect("admission decided the agg lane choice");
                        // band-2a: the Limit-over-Agg chain is the one
                        // chain that knows the bare-LIMIT bound — derive the
                        // group-admission freeze bound (offset + count,
                        // recomputed by the prologue above). There is no
                        // Sort between this Limit and the Agg by
                        // construction, so ANY `bound` groups with exact
                        // aggregates satisfy the query (the ratified LIMIT-k-no-ORDER
                        // membership class); the sink's arming gates
                        // (Mk-drain shape, bound ceiling, kill switch)
                        // decline everything else, and a decline keeps
                        // today's full drain byte-identically.
                        let sink_freeze = ::nodelimit::lane_limit_total_bound(&l.state)
                            .and_then(|b| u32::try_from(b).ok());
                        agg_seq_scan_build_if_needed(
                            &mut aps.agg,
                            ss,
                            c,
                            &mut aps.lane_stage_slot,
                            &mut aps.lane_exprkey,
                            None,
                            sink_freeze,
                            estate,
                        )?;
                        true
                    }
                    crate::procnode::PlanStateNode::HashJoin(hj) => {
                        agg_hash_join_build_if_needed(
                            &mut aps.agg,
                            &mut **hj,
                            &mut aps.lane_stage_slot,
                            estate,
                        )?
                    }
                    crate::procnode::PlanStateNode::Gather(g) => {
                        agg_gather_build_if_needed(
                            &mut aps.agg,
                            &mut **g,
                            &mut aps.lane_stage_slot,
                            estate,
                        )?;
                        true
                    }
                    _ => unreachable!("agg_child_fusible admitted a non-lane agg feed"),
                };
                if !built {
                    // Join multi-batch spill refuse, before any lane tuple:
                    // exec_limit over the per-tuple agg/join resumes
                    // byte-identically (the recompute above ran once, as C's
                    // INITIAL arm would have).
                    return Ok(None);
                }
                let mut op = LimitOp { limit: &mut l.state };
                let mut root = RootAdapter::new(None);
                pull_step_chain(
                    &mut aps.agg,
                    &mut HashAggSource,
                    &mut HashAggEmit,
                    &mut op,
                    &mut root,
                    estate,
                )?
            }
        }
        _ => unreachable!("admitted a non-lane limit child"),
    };
    if r.is_none() && matches!(l.state.lstate, LIMIT_RESCAN | LIMIT_INWINDOW) {
        // Source exhausted before the window filled — C's subplan-EOF arms.
        ::nodelimit::lane_limit_eof(&mut l.state);
    }
    Ok(Some(r))
}

/// Try to let the lane own a `Unique` over the sort breaker — streaming
/// adjacent-dedup on the sorted emit (see the section header for economics +
/// refusals; hashed DISTINCT plans an Agg and is owned by the agg breaker).
/// `None` = refused; `exec_unique` drives the same UniqueState byte-safely.
#[inline]
pub fn try_own_unique<'mcx>(
    u: &mut crate::procnode::UniqueNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if let Some(r) = try_own_unique_streaming(u, estate)? {
        return Ok(Some(r));
    }
    // Wave-2 row-mode tail fallback, SH-E verdict form (knob-gated inside;
    // the streaming glue above keeps priority per the composition rule).
    rowmode_tail::unique_tail_verdict(u, estate);
    Ok(None)
}

/// The Phase-2 streaming unique over the sort breaker. `None` = refused.
#[inline]
fn try_own_unique_streaming<'mcx>(
    u: &mut crate::procnode::UniqueNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic per-call gates (Unique init asserts !BACKWARD && !MARK, so a
    // non-forward pull should be impossible — gate anyway, like the sort).
    if estate.es_epq_active
        || !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction)
    {
        return Ok(None);
    }
    let crate::procnode::UniqueNode { state, outer } = u;
    let crate::procnode::PlanStateNode::Sort(s) = outer else {
        return Ok(None);
    };
    if !sort_lane_fusible_memo(s, estate)? {
        return Ok(None);
    }
    // C's ExecUnique entry interrupt check (conditional, exactly the
    // Volcano entry's), then the first child pull's ExecSort entry CFI.
    ::nodeunique::lane_unique_cfi()?;
    ::postgres_seams::check_for_interrupts::call()?;
    let crate::procnode::SortNode { state: sstate, outer: souter, outer_desc, .. } = s;
    if !sort_feed_if_needed(sstate, &mut **souter, outer_desc, None, estate)? {
        return Ok(None);
    }
    let mut op = UniqueOp { unique: state };
    let mut root = RootAdapter::new(None);
    let r = pull_step_chain(sstate, &mut SortEmitSource, &mut SortEmit, &mut op, &mut root, estate)?;
    if r.is_none() {
        // exec_unique's end-of-stream arm: drop the retained previous tuple
        // and clear both slots.
        ::nodeunique::lane_unique_eof(state, estate);
    }
    Ok(Some(r))
}

// ===========================================================================
// Wave-4 streaming glue (Volcano-tail triage, 2026-07-12): three small
// streaming operators hosted where the lane already owns the neighboring
// pipeline — never a new layer over a refused child (admission economics,
// design §4; the Limit/Unique precedent):
//
//   Group        adjacent-row grouping over the SORT breaker's emit — a
//                mid-pipeline `TupleOp` running `exec_group`'s own per-tuple
//                body (`nodegroup::lane_group_feed`: the same
//                grouping-equality program, first-tuple copy, HAVING qual and
//                projection — reused, not reimplemented); state = the
//                node-resident first-tuple slot + have-first/grp_done flags,
//                so a Volcano fallback at any call boundary is byte-safe.
//                NOTE: Group the NODE only — AGG_SORTED / the agg breaker
//                admission are owned elsewhere (wave-4 charter split).
//   Result       the gating/projection node: `resconstantqual` evaluated
//                once (C's rs_checkqual arm, via `noderesult`'s seams), then
//                either the degenerate no-child pipeline (the single no-FROM
//                row) or the child stream projected row-by-row through a
//                `TupleOp` over the sort breaker's emit.
//   SubqueryScan a pass-through filter/project `TupleOp` over the child
//                pipeline (`execscan::lane_scan_accept` — `exec_scan_impl`'s
//                per-tuple qual/proj body, subplan/param arms included):
//                bare over the sort breaker, and spliced mid-pipeline in the
//                agg-over-subquery-over-scan composition so lane pipelines
//                chain through subquery boundaries end to end.
//
// Refused shapes (each byte-safe on the Volcano fallback): EPQ and
// non-forward pulls (dynamic gates, ticked per offered call); instrumented
// nodes (EXPLAIN ANALYZE keeps per-node counters — for the chained shapes an
// instrumented tree wraps every node so the child never matches the Sort/scan
// arms, and the Result no-FROM arm gates on the estate's instrumentation
// explicitly); any child that is not a lane-owned pipeline
// (`child-not-lane-owned`; the child's own refusal reason ticks under the
// child's class). Group/Result/SubqueryScan quals and projections run the
// nodes' OWN evaluation arms (subplan-aware where the node's Volcano body is
// — noderesult and execscan host subplans/params; nodegroup's body is reused
// verbatim), so no subplan-param refusal is needed at this layer.
// ===========================================================================

/// The Group node as a mid-pipeline streaming operator: one sorted tuple in,
/// 0..1 projected group heads out, never pends (no intra-tuple expansion) and
/// never finishes early.
struct GroupOp<'a, 'mcx> {
    group: &'a mut ::nodegroup::GroupState<'mcx>,
}

impl<'mcx> TupleOp<'mcx> for GroupOp<'_, 'mcx> {
    fn pending(&self) -> bool {
        false
    }

    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        match ::nodegroup::lane_group_feed(self.group, estate, tuple)? {
            None => Ok(OpStatus::NeedInput),
            Some(result) => Ok(match out.accept(result, estate)? {
                SinkFeed::Full => OpStatus::Paused,
                SinkFeed::NeedMore => OpStatus::NeedInput,
            }),
        }
    }

    fn resume(
        &mut self,
        _out: &mut dyn Sink<'mcx>,
        _estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        unreachable!("GroupOp never pends")
    }
}

/// Try to let the lane own a `Group` over the sort breaker — streaming
/// adjacent-row grouping on the sorted emit. `None` = refused; `exec_group`
/// drives the same GroupState byte-safely at any call boundary.
#[inline]
pub fn try_own_group<'mcx>(
    g: &mut ::mcx::PgBox<'mcx, crate::procnode::GroupNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic per-call gates. (The defensive backward gate retired with the
    // backward-execution wave B11 — pulls are forward-invariant below the
    // run seam, B1; Group init already asserts !BACKWARD && !MARK.)
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::Group, RefuseReason::Epq);
        return Ok(None);
    }
    let g = &mut **g;
    // Sorted grouping's input comes from a Sort in the lane-ownable plans
    // (a presorted index path arrives as a standalone scan, which refuses
    // ownership — C's shape there IS the Volcano Group). Instrumented trees
    // wrap every node, so EXPLAIN ANALYZE never matches the Sort arm.
    let crate::procnode::PlanStateNode::Sort(s) = &mut g.outer else {
        stats::tick_refused(ShapeClass::Group, RefuseReason::ChildNotLaneOwned);
        return Ok(None);
    };
    if !sort_lane_fusible_memo(s, estate)? {
        stats::tick_refused(ShapeClass::Group, RefuseReason::ChildNotLaneOwned);
        return Ok(None);
    }
    // C's ExecGroup entry interrupt check (conditional, exactly the Volcano
    // entry's), then the drained guard, then the first child pull's ExecSort
    // entry CFI.
    ::nodegroup::lane_group_cfi()?;
    if ::nodegroup::lane_group_done(&g.state) {
        return Ok(Some(None));
    }
    ::postgres_seams::check_for_interrupts::call()?;
    let crate::procnode::SortNode { state: sstate, outer: souter, outer_desc, .. } = s;
    // One OWNED tick per lane-owned group drive start (= the underlying sort
    // feed event; rescan re-feeds and re-ticks, like the sortfeed class) —
    // after the feed, so a feed-time refuse never ticks owned.
    let feeding = !sstate.sort_done();
    if !sort_feed_if_needed(sstate, &mut **souter, outer_desc, None, estate)? {
        return Ok(None);
    }
    if feeding {
        stats::tick_owned(ShapeClass::Group);
    }
    let mut op = GroupOp { group: &mut g.state };
    let mut root = RootAdapter::new(None);
    let r =
        pull_step_chain(sstate, &mut SortEmitSource, &mut SortEmit, &mut op, &mut root, estate)?;
    if r.is_none() {
        // exec_group's child-exhausted arm: the node stays drained.
        ::nodegroup::lane_group_eof(&mut g.state);
    }
    Ok(Some(r))
}

/// The Result node's per-row projection as a mid-pipeline streaming operator:
/// one child row in, exactly one projected row out (Result has no per-row
/// qual — C's ExecResult projects every child row). Never pends, never
/// finishes early.
struct ResultOp<'a, 'mcx> {
    ps: &'a mut crate::procnode::PlanStateBase<'mcx>,
}

impl<'mcx> TupleOp<'mcx> for ResultOp<'_, 'mcx> {
    fn pending(&self) -> bool {
        false
    }

    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        // exec_result's per-call body over one pushed child row: per-tuple
        // context reset, stage the outer tuple, project (param hoist +
        // subplan-aware arm inside the seam).
        let ecxt = self.ps.ps_ExprContext.expect("ResultState without ExprContext");
        estate.reset_expr_context(ecxt);
        estate.ecxt_mut(ecxt).ecxt_outertuple = Some(tuple);
        let result = crate::noderesult::lane_result_project(self.ps, estate)?;
        Ok(match out.accept(result, estate)? {
            SinkFeed::Full => OpStatus::Paused,
            SinkFeed::NeedMore => OpStatus::NeedInput,
        })
    }

    fn resume(
        &mut self,
        _out: &mut dyn Sink<'mcx>,
        _estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        unreachable!("ResultOp never pends")
    }
}

/// Try to let the lane own a `Result`: the no-FROM single-row arm (degenerate
/// no-child pipeline), or the projection stream over the sort breaker. The
/// one-time `resconstantqual` gate runs BEFORE the child is ever fed, via
/// `noderesult::lane_result_gate` — C's rs_checkqual arm verbatim, so a
/// refusal after the gate ran is still byte-safe (`exec_result` sees the
/// same consumed rs_checkqual / rs_done state its own first call would have
/// left).
#[inline]
pub fn try_own_result<'mcx>(
    rs: &mut crate::noderesult::ResultState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic per-call gates.
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::ResultNode, RefuseReason::Epq);
        return Ok(None);
    }
    // EXPLAIN ANALYZE refuses by policy (§4). The no-FROM arm has no child
    // whose Instrumented wrapper would break the match, so gate on the
    // estate's instrumentation table directly (non-empty exactly when the
    // plan is instrumented).
    if !estate.es_instrumentation.is_empty() {
        stats::tick_refused(ShapeClass::ResultNode, RefuseReason::Instrumented);
        return Ok(None);
    }
    match rs.outer.as_deref_mut() {
        None => {
            // The no-FROM row: exec_result's childless body, statement for
            // statement (entry CFI → one-time gate → per-call ctx reset →
            // drained guard → mark done + project the single row). INLINE by
            // contract: this is the select1 hot path, and the WS-E code move
            // to `noderesult::lane_result_childless_next` cost it entry
            // instructions (se-entrycost) — the integration contract
            // pre-approves keeping this arm as an inline duplicate of that
            // seam (the row-mode `ResultRowSource` face keeps the outlined
            // copy; the two bodies MUST stay statement-identical —
            // rowmode_ab::childless_result_seam_knob_positions pins both).
            crate::cfi()?;
            // One OWNED tick per lane-owned Result execution: the call that
            // consumes the gate and/or emits; the drained tail calls after it
            // don't re-tick.
            if rs.rs_checkqual || !rs.rs_done {
                stats::tick_owned(ShapeClass::ResultNode);
            }
            if rs.rs_checkqual && !crate::noderesult::lane_result_gate(rs, estate)? {
                return Ok(Some(None));
            }
            let ecxt = rs.ps.ps_ExprContext.expect("ResultState without ExprContext");
            estate.reset_expr_context(ecxt);
            if rs.rs_done {
                return Ok(Some(None));
            }
            rs.rs_done = true;
            Ok(Some(Some(crate::noderesult::lane_result_project(&mut rs.ps, estate)?)))
        }
        Some(crate::procnode::PlanStateNode::Sort(_)) => {
            // Child admission BEFORE any state effect. (Instrumented trees
            // wrap every node, so an instrumented Sort never matches — the
            // estate gate above already refused anyway.)
            {
                let Some(crate::procnode::PlanStateNode::Sort(s)) = rs.outer.as_deref_mut()
                else {
                    unreachable!("matched above")
                };
                if !sort_lane_fusible_memo(s, estate)? {
                    stats::tick_refused(ShapeClass::ResultNode, RefuseReason::ChildNotLaneOwned);
                    return Ok(None);
                }
            }
            // exec_result entry: CFI, then the one-time gate (C evaluates it
            // before the child is ever pulled; false = the sort is never fed).
            crate::cfi()?;
            if rs.rs_checkqual && !crate::noderesult::lane_result_gate(rs, estate)? {
                return Ok(Some(None));
            }
            if rs.rs_done {
                return Ok(Some(None));
            }
            let crate::noderesult::ResultState { ps, outer, .. } = rs;
            let Some(crate::procnode::PlanStateNode::Sort(s)) = outer.as_deref_mut() else {
                unreachable!("matched above")
            };
            // C's first child pull enters ExecSort: entry CFI, then the feed.
            ::postgres_seams::check_for_interrupts::call()?;
            let crate::procnode::SortNode { state: sstate, outer: souter, outer_desc, .. } = s;
            // One OWNED tick per lane-owned Result child-feed event — after
            // the feed, so a feed-time refuse never ticks owned.
            let feeding = !sstate.sort_done();
            if !sort_feed_if_needed(sstate, &mut **souter, outer_desc, None, estate)? {
                return Ok(None);
            }
            if feeding {
                stats::tick_owned(ShapeClass::ResultNode);
            }
            let mut op = ResultOp { ps };
            let mut root = RootAdapter::new(None);
            Ok(Some(pull_step_chain(
                sstate,
                &mut SortEmitSource,
                &mut SortEmit,
                &mut op,
                &mut root,
                estate,
            )?))
        }
        Some(_) => {
            stats::tick_refused(ShapeClass::ResultNode, RefuseReason::ChildNotLaneOwned);
            Ok(None)
        }
    }
}

/// The SubqueryScan node as a mid-pipeline streaming operator: one subplan
/// row in, 0..1 filtered/projected rows out, via `execscan::lane_scan_accept`
/// — `exec_scan_impl`'s per-tuple qual/projection body (subplan/param arms
/// included), over the same node state (`ss_ScanTupleSlot` repointed at the
/// subplan's slot exactly as `SubqueryNext` does). Never pends, never
/// finishes early.
struct SubqueryScanOp<'a, 'mcx> {
    ss: &'a mut ::execscan::ScanState<'mcx>,
}

impl<'mcx> TupleOp<'mcx> for SubqueryScanOp<'_, 'mcx> {
    fn pending(&self) -> bool {
        false
    }

    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        // exec_scan_fetch's conditional per-tuple interrupt check (§9: same
        // cadence as the per-tuple driver this replaces).
        if ::init_small::globals::InterruptPending() {
            ::postgres_seams::check_for_interrupts::call()?;
        }
        // SubqueryNext: the subplan's slot goes to the driver uncopied.
        self.ss.ss_ScanTupleSlot = tuple;
        match ::execscan::lane_scan_accept(self.ss, estate, tuple)? {
            None => Ok(OpStatus::NeedInput),
            Some(result) => Ok(match out.accept(result, estate)? {
                SinkFeed::Full => OpStatus::Paused,
                SinkFeed::NeedMore => OpStatus::NeedInput,
            }),
        }
    }

    fn resume(
        &mut self,
        _out: &mut dyn Sink<'mcx>,
        _estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        unreachable!("SubqueryScanOp never pends")
    }
}

/// Try to let the lane own a `SubqueryScan`: FIRST the wave-4 streaming
/// glue over the sort breaker (the batch pipeline — priority per the wave-2
/// contract composition rule), THEN the wave-2 row-mode tail delegation
/// (`rowmode_tail::try_own_subquery_scan_tail`, knob-gated) on glue refuse.
/// `None` = both refused; `exec_scan` drives the same ScanState byte-safely.
/// Class-10 accounting: the glue's per-call refusal ticks fire first; the
/// tail's gates may tick the same reason again knob-ON (two mechanisms, two
/// offers — see the allowlist block comment).
#[inline]
pub fn try_own_subquery_scan<'mcx>(
    s: &mut ::mcx::PgBox<'mcx, crate::procnode::SubqueryScanNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if let Some(r) = try_own_subquery_scan_glue(s, estate)? {
        return Ok(Some(r));
    }
    // Wave-2 row-mode tail fallback, SH-E verdict form (knob-gated inside):
    // accounting only — the arm's fall-through exec_scan IS the delegated
    // body, so refusal and admission run the same bytes.
    rowmode_tail::subquery_scan_tail_verdict(estate);
    Ok(None)
}

/// The wave-4 streaming glue: a bare `SubqueryScan` over the sort breaker —
/// the pass-through filter/project stream on the sorted emit. `None` =
/// refused.
#[inline]
fn try_own_subquery_scan_glue<'mcx>(
    s: &mut ::mcx::PgBox<'mcx, crate::procnode::SubqueryScanNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic per-call gates. EPQ substitutes test tuples in the fetch
    // (exec_scan_epq); the lane refuses it wholesale (§4).
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::SubqueryScan, RefuseReason::Epq);
        return Ok(None);
    }
    let s = &mut **s;
    if s.ss.instr_idx.is_some() {
        stats::tick_refused(ShapeClass::SubqueryScan, RefuseReason::Instrumented);
        return Ok(None);
    }
    let crate::procnode::PlanStateNode::Sort(sort) = &mut *s.subplan else {
        stats::tick_refused(ShapeClass::SubqueryScan, RefuseReason::ChildNotLaneOwned);
        return Ok(None);
    };
    if !sort_lane_fusible_memo(sort, estate)? {
        stats::tick_refused(ShapeClass::SubqueryScan, RefuseReason::ChildNotLaneOwned);
        return Ok(None);
    }
    // C's first fetch: exec_scan_fetch's conditional CFI runs per tuple in
    // the TupleOp; the subplan pull enters ExecSort — entry CFI here.
    ::postgres_seams::check_for_interrupts::call()?;
    let crate::procnode::SortNode { state: sstate, outer: souter, outer_desc, .. } = sort;
    // One OWNED tick per lane-owned feed event (the child sort feed) — after
    // the feed, so a feed-time refuse never ticks owned.
    let feeding = !sstate.sort_done();
    if !sort_feed_if_needed(sstate, &mut **souter, outer_desc, None, estate)? {
        return Ok(None);
    }
    if feeding {
        stats::tick_owned(ShapeClass::SubqueryScan);
    }
    // End-of-stream mirrors exec_scan_impl's projected-slot clear.
    let clear_on_finish = s.ss.ps_ProjInfo.as_ref().map(|p| p.pi_result_slot);
    let mut op = SubqueryScanOp { ss: &mut s.ss };
    let mut root = RootAdapter::new(clear_on_finish);
    Ok(Some(pull_step_chain(
        sstate,
        &mut SortEmitSource,
        &mut SortEmit,
        &mut op,
        &mut root,
        estate,
    )?))
}

/// Feed pipeline for the agg-over-subquery composition: lane scan source →
/// scalar filter/project → SubqueryScanOp (pass-through filter/project) →
/// the hash-agg breaker sink, to exhaustion — dispatched over the admitted
/// scan child types (join-probe-drain shape).
fn subquery_feed_drain_dispatch<'mcx>(
    sqs: &mut crate::procnode::SubqueryScanNode<'mcx>,
    sink: &mut dyn Sink<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let crate::procnode::SubqueryScanNode { ss, subplan } = sqs;
    let mut op = SubqueryScanOp { ss };
    match &mut **subplan {
        crate::procnode::PlanStateNode::SeqScan(ss2) => drain_pipeline_chain(
            ss2,
            &mut SeqScanSource,
            &mut SeqScanFilterProject,
            &mut op,
            sink,
            estate,
        ),
        crate::procnode::PlanStateNode::IndexScan(is) => {
            drain_pipeline_chain(is, &mut IndexScanSource, &mut IndexScanEmit, &mut op, sink, estate)
        }
        crate::procnode::PlanStateNode::IndexOnlyScan(ios) => drain_pipeline_chain(
            &mut **ios,
            &mut IndexOnlyScanSource,
            &mut IndexOnlyScanEmit,
            &mut op,
            sink,
            estate,
        ),
        crate::procnode::PlanStateNode::BitmapHeapScan(b) => {
            let b = &mut **b;
            if !b.scan.initialized {
                crate::procnode::bitmap_table_scan_setup_dispatch(b, estate)?;
            }
            drain_pipeline_chain(
                &mut b.scan,
                &mut BitmapHeapScanSource,
                &mut BitmapHeapScanEmit,
                &mut op,
                sink,
                estate,
            )
        }
        _ => unreachable!("composition admitted a non-scan subquery child"),
    }
}

/// Try to let the lane own `Agg(hashed) → SubqueryScan → scan` — lane
/// pipelines chaining through a subquery boundary. Two pipelines on one
/// breaker node:
///
///   1. build: scan source → filter/project → SubqueryScanOp → HashAggBuildSink
///   2. emit:  HashAggSource → HashAggEmit → RootAdapter (one group per pull)
///
/// The agg reads the SUBQUERY's output slot (not the scan slot), so the
/// lanefold/SoA fold feed does not apply — the build is the per-row breaker
/// feed. No admission-economics refuse is needed: the legacy fused
/// `exec_agg_batched` arms never match a SubqueryScan outer, so there is no
/// faster drive to preempt. `None` = refused (the caller falls to the
/// per-tuple `exec_agg` over `exec_scan`, byte-identically).
#[inline]
pub fn try_own_agg_over_subquery_scan<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    sqs: &mut ::mcx::PgBox<'mcx, crate::procnode::SubqueryScanNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic per-call gates, ticked under the subqueryscan class (the
    // composition's feed hangs off the subquery's drive).
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::SubqueryScan, RefuseReason::Epq);
        return Ok(None);
    }
    if !::nodeagg::agg_hash_breaker_admissible(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AggNotDrainable);
        return Ok(None);
    }
    let sqs = &mut **sqs;
    if sqs.ss.instr_idx.is_some() {
        stats::tick_refused(ShapeClass::SubqueryScan, RefuseReason::Instrumented);
        return Ok(None);
    }
    // The subquery's child must be a lane-fusible scan (the Phase-1 refuse-
    // sets, verbatim; the specific child reason ticks under its class).
    if let Some(r) = scan_child_fusible(&mut sqs.subplan, estate)? {
        stats::tick_refused(ShapeClass::SubqueryScan, r);
        return Ok(None);
    }
    // exec_agg's top-of-call guard: a drained agg stays drained.
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    // Build phase (once, lazily): drain the scan → subquery chain into the
    // breaker sink, then finalize (delegated). `table_filled` is the phase
    // flag; a rescan rebuild clears it and re-enters here.
    if !::nodeagg::agg_hash_table_filled(agg) {
        // One OWNED tick per lane-owned build event, on both classes the
        // event engages (aggbuild counts builds; subqueryscan counts feeds).
        stats::tick_owned(ShapeClass::AggBuild);
        stats::tick_owned(ShapeClass::SubqueryScan);
        {
            let mut agg_sink = HashAggBuildSink { agg: &mut *agg };
            subquery_feed_drain_dispatch(sqs, &mut agg_sink, estate)?;
        }
        // End-of-scan parity with exec_scan_impl: the projected slot is
        // cleared when the subquery's stream ends (byte-invisible; keeps the
        // node state identical to the per-tuple driver's).
        if let Some(p) = sqs.ss.ps_ProjInfo.as_ref() {
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(p.pi_result_slot), mcx);
        }
    }
    // Emit phase (every call): one qual-passing group per PG pull, in C's
    // retrieve order.
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step(agg, &mut HashAggSource, &mut HashAggEmit, &mut root, estate)?))
}

// ===========================================================================
// Agg-over-Gather hosting (lane-v2-aggovergather): the leader-side
// HashAggregate above a Gather — the plan shape the planner picks when
// partial-aggregation costing does not win (common at 10M+: many-group
// GROUP BYs) — as a lane breaker build fed by the GATHER MACHINERY AS A
// SOURCE. The workers stay row-path (they only scan/filter/project into the
// shm_mq); the leader's half becomes lane pipelines on the one breaker node:
//
//   1. build: exec_gather (REUSED VERBATIM, per pull: worker launch,
//      round-robin nowait queue reads, leader participation, projection) →
//      staged fold sink / per-row breaker sink
//   2. emit:  HashAggSource → HashAggEmit → RootAdapter (one group per pull)
//
// The Append house rule applies: the node's OWN drive body is reused, not
// reimplemented — worker launch/teardown, tqueue reads, latch waits,
// leader-participation pulls (`exec_proc_node` on the partial plan — the
// leader's local child stays row-path; parallel-aware scans refuse the lane
// via the parallel gate), deferred-rescan chgParam, and the per-pull CFI are
// all `exec_gather`'s. Only the consumer changes: each returned slot feeds
// the breaker sink instead of returning through the Volcano boundary, so the
// agg consumes C's rows in C's arrival order and the built table is
// byte-identical to `exec_agg` over `exec_gather`'s.
//
// Feed choice mirrors the agg-over-join composition: the staged fold feed
// (`StagedFoldAggSink` — batched transition folds; K2's deferred batched
// probe when the grouping key is single and kernel-hostable) when the agg
// carries a lanefold plan, the per-row `HashAggBuildSink` otherwise. Staged
// by-ref values are copied into the per-batch arena at accept, so the
// funnel slot's transport-lifetime tuple (live only until the next queue
// receive) is never held across rows.
//
// Refuse-set (each byte-safe on the Volcano fallback):
//   * agg-side: `agg_hash_breaker_admissible`, verbatim (grouping sets /
//     DISTINCT / ordered-set / merge-phase — notably the parallel FINALIZE
//     half of a partial-agg plan, whose AGGSPLIT deserialization the breaker
//     does not own; ticked under aggbuild per offered call).
//   * dynamic EPQ / non-forward pulls (§4 model-incompatible; per call).
//   * GatherMerge stays Volcano: the planner puts a hash agg above
//     GatherMerge only when the merge order is useful elsewhere — no such
//     analytics-charter shape exists; a sorted GroupAggregate over GatherMerge is a
//     different (sorted-agg) breaker and refuses via the dispatch match.
//   * kill switch `PGRUST_LANE_V2_AGGGATHER=0` (A/B tooling; default ON).
//
// EXPLAIN is unchanged (no planner surface); EXPLAIN ANALYZE trees wrap
// every node in the `Instrumented` variant and never reach the hook.
// ===========================================================================

/// Agg-over-Gather kill switch: on by default under the lane;
/// `PGRUST_LANE_V2_AGGGATHER=0`/`off` forces the Volcano fallback.
fn agg_gather_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_LANE_V2_AGGGATHER").as_deref(), Ok("0") | Ok("off"))
    })
}

/// Drain the gather stream to exhaustion into a breaker sink — pipeline 1's
/// driver with `exec_gather` as the source (the node's own drive, reused
/// verbatim; its per-pull CFI is the loop's interrupt cadence). `finish`
/// runs the sink's finalize tail (staged flush + build finalize).
fn gather_drain<'mcx>(
    g: &mut crate::procnode::GatherNode<'mcx>,
    sink: &mut dyn Sink<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    while let Some(slot) = crate::nodegather::exec_gather(&mut g.state, &mut g.outer, estate)? {
        let fed = sink.accept(slot, estate)?;
        debug_assert_eq!(fed, SinkFeed::NeedMore, "a breaker sink consumes its whole input");
        let _ = fed;
    }
    sink.finish(estate)
}

/// Build phase of the agg-over-gather composition, once, lazily: drain the
/// gather stream into the breaker sink (staged fold feed when the agg
/// carries a fold plan; the per-row sink otherwise), then finalize
/// (delegated). `table_filled` is the phase flag; a rescan rebuild clears it
/// (`exec_rescan_gather` reset the gather side, workers relaunch on the
/// first pull) and re-enters here. Shared by the bare composition hook and
/// the Sort-/Limit-over-agg chains. Unlike the join composition there is no
/// feed-time refuse: the gather stream has no spill analog, so the build
/// always completes.
fn agg_gather_build_if_needed<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    g: &mut crate::procnode::GatherNode<'mcx>,
    stage_slot: &mut Option<ExecSlotId>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if ::nodeagg::agg_hash_table_filled(agg) {
        return Ok(());
    }
    // One OWNED tick per lane-owned build event, on both classes the event
    // engages (aggbuild counts builds; gather counts feeds).
    stats::tick_owned(ShapeClass::AggBuild);
    stats::tick_owned(ShapeClass::Gather);
    // The gather's output slot: the projected result slot when the Gather
    // carries a projection, else the funnel slot (worker rows; leader-local
    // rows arrive in the leader plan's own slot with the same descriptor —
    // the sinks deform from the slot each accept).
    let out_slot = g.state.ps.ps_ResultTupleSlot.unwrap_or(g.state.funnel_slot);
    match staged_feed_shape(agg, out_slot, estate) {
        Some(shape) => {
            trace_feed(match shape.mode {
                StagedMode::Guarded => "agg-over-gather: staged fold feed engaged (guarded)",
                StagedMode::K2 { .. } => "agg-over-gather: staged fold feed engaged (k2 probe)",
                StagedMode::Mk => "agg-over-gather: staged fold feed engaged (mk probe)",
                StagedMode::Arrival => "agg-over-gather: staged fold feed engaged",
            });
            let mut sink = StagedFoldAggSink::new(agg, out_slot, stage_slot, shape, estate);
            gather_drain(g, &mut sink, estate)
        }
        None => {
            trace_feed("agg-over-gather: per-row sink (no fold plan)");
            let mut sink = HashAggBuildSink { agg };
            gather_drain(g, &mut sink, estate)
        }
    }
}

/// Try to let the lane own `Agg(hashed) → Gather → (row-path parallel
/// workers)` — the leader-side aggregation shape (see the section header for
/// the model, the reuse rule, and the refuse-set). `None` = refused (the
/// caller falls to the per-tuple `exec_agg` over `exec_gather`,
/// byte-identically).
#[inline]
pub fn try_own_agg_over_gather<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    g: &mut ::mcx::PgBox<'mcx, crate::procnode::GatherNode<'mcx>>,
    stage_slot: &mut Option<ExecSlotId>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if !agg_gather_enabled() {
        return Ok(None);
    }
    // Dynamic per-call gates, ticked under the gather class (the
    // composition's feed hangs off the gather's drive).
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::Gather, RefuseReason::Epq);
        return Ok(None);
    }
    if !::nodeagg::agg_hash_breaker_admissible(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AggNotDrainable);
        return Ok(None);
    }
    // exec_agg's top-of-call guard: a drained agg stays drained.
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    agg_gather_build_if_needed(agg, &mut **g, stage_slot, estate)?;
    // Emit phase (every call): one qual-passing group per PG pull, in C's
    // retrieve order.
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step(agg, &mut HashAggSource, &mut HashAggEmit, &mut root, estate)?))
}

// ===========================================================================
// Append hosting (wave 5, 2026-07-12): the serial Append as a lane
// concatenation point — the node's OWN `exec_append` body drives, verbatim
// (subplan choice, `as_begun`, runtime pruning via `choose_next_subplan_
// locally`/`identify_valid_subplans`, and the conditional per-fetch CFI are
// all C's, reused not reimplemented — the wave-4 house rule); only the
// `fetch_subplan` closure changes, pulling one tuple per fetch from the
// CHILD's lane pipeline (`pull_step` over the Phase-1 scan stages) instead of
// `exec_proc_node`. Child N's pipeline exhausting returns `None` to
// `exec_append`, which advances to child N+1 — C's exact
// child-EOF-then-advance order for free. Each child's cross-call position
// (staged page batch + cursor) is node-resident, so the one-tuple-per-pull
// Volcano boundary is safe, and each child's output slot goes to the parent
// exactly as `exec_append` would hand it (Append projects nothing; children
// with differing physical descs already carry their own planner-installed
// projections, which run inside the child pipelines — byte-identical).
//
// Refuse-set (each byte-safe on the Volcano fallback):
//   * parallel Append (Leader/Worker choosers over the shared DSM claim
//     table) — non-serial subplan order; the lane refuses anything not
//     provably ordering-identical serially (`lane_choose_local`). Ticked per
//     offered call (the mode is worker/DSM-init-assigned).
//   * async-capable subplans — unported (`exec_init_append` panics), so no
//     gate is needed; recorded here for the C-diff reader.
//   * dynamic EPQ / non-forward pulls (§4 model-incompatible; per call).
//   * any child that is not a lane-fusible Phase-1 scan
//     (`scan_child_fusible`, verbatim — the child's specific refusal reason
//     ticks under the child's class). v1 policy: MIXED children refuse the
//     WHOLE Append — a per-child owned/Volcano split would need per-child
//     verdict pinning across the shared `exec_append` drive for no measured
//     upside; future work when a real mixed shape shows up.
//
// Runtime partition pruning is ADMITTED: the pruning arms run inside the
// reused `exec_append`/`choose_next_subplan_locally` body itself, so the
// subplan order is C's by construction. The structural verdict conservatively
// probes ALL initialized children (a superset of what pruning may run) —
// probing opens each child scan's descriptor once, which C's lazy first-pull
// open would skip for pruned/LIMIT-cut children: pgstat-only divergence,
// same accepted class as the hash-join build-side probe (design §9 F5).
// ===========================================================================

/// One PG pull's worth from a lane-owned scan child pipeline — the
/// `fetch_subplan` face of the hosted Append (join_probe_pull_dispatch's
/// shape, without a mid-pipeline op). The child's staged batch + cursor are
/// node-resident, so consecutive fetches resume exactly.
fn lane_scan_pull_dispatch<'mcx>(
    child: &mut crate::procnode::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    match child {
        crate::procnode::PlanStateNode::SeqScan(ss) => {
            // End-of-stream mirrors ExecScanExtended's projected-slot clear
            // (try_own_seq_scan's shape).
            let clear_on_finish = ss.ss.ps_ProjInfo.as_ref().map(|p| p.pi_result_slot);
            let mut root = RootAdapter::new(clear_on_finish);
            pull_step(ss, &mut SeqScanSource, &mut SeqScanFilterProject, &mut root, estate)
        }
        crate::procnode::PlanStateNode::IndexScan(is) => {
            let mut root = RootAdapter::new(None);
            pull_step(is, &mut IndexScanSource, &mut IndexScanEmit, &mut root, estate)
        }
        crate::procnode::PlanStateNode::IndexOnlyScan(ios) => {
            let mut root = RootAdapter::new(None);
            pull_step(&mut **ios, &mut IndexOnlyScanSource, &mut IndexOnlyScanEmit, &mut root, estate)
        }
        crate::procnode::PlanStateNode::BitmapHeapScan(b) => {
            let b = &mut **b;
            if !b.scan.initialized {
                crate::procnode::bitmap_table_scan_setup_dispatch(b, estate)?;
            }
            let mut root = RootAdapter::new(None);
            pull_step(&mut b.scan, &mut BitmapHeapScanSource, &mut BitmapHeapScanEmit, &mut root, estate)
        }
        _ => unreachable!("memoized append verdict admitted a non-scan child"),
    }
}

/// Structural Append verdict, memoized on the node at first offer (verdict
/// stability: a lane-driven child carries a staged-batch cursor across the
/// Volcano boundary, so ownership must not flip mid-stream; the child scan
/// verdicts are themselves memoized). ALL children must pass the Phase-1
/// scan refuse-sets — mixed children refuse the whole Append (v1 policy,
/// module doc). Owned accounting ticks exactly here — once per memoized
/// admission (per Append node per (re)init, the seqscan class cadence).
fn append_lane_fusible_memo<'mcx>(
    a: &mut crate::procnode::AppendNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if let Some(v) = a.lane_fusible {
        return Ok(v);
    }
    let mut refuse: Option<RefuseReason> = None;
    for child in a.substates.iter_mut() {
        if let Some(r) = scan_child_fusible(child, estate)? {
            refuse = Some(r);
            break;
        }
    }
    match refuse {
        None => stats::tick_owned(ShapeClass::Append),
        Some(r) => stats::tick_refused(ShapeClass::Append, r),
    }
    let v = refuse.is_none();
    a.lane_fusible = Some(v);
    Ok(v)
}

/// Try to let the lane own a serial `Append` over lane-fusible scan children.
/// `Some` = the lane drove this call (via the node's own `exec_append` body
/// over lane child pipelines); `None` = refused (caller runs the unchanged
/// `exec_append` over `exec_proc_node` children, byte-identically).
#[inline]
pub fn try_own_append<'mcx>(
    a: &mut ::mcx::PgBox<'mcx, crate::procnode::AppendNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic per-call gates (mirror the sort/join breakers). (The backward
    // gate retired with the backward-execution wave B11: pulls are forward-
    // invariant below the run seam, B1, and B2 deleted the BACKWARD-eflags
    // producer C's backward Append pulls rode.)
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::Append, RefuseReason::Epq);
        return Ok(None);
    }
    let a = &mut **a;
    // Parallel Append: the Leader/Worker choosers claim subplans through the
    // shared DSM table in a non-serial order — Volcano keeps it. The mode is
    // assigned at DSM/worker init (before the node's first pull), but gate
    // per call (one flag load) rather than memoize an init-order assumption.
    if !::nodeappend::lane_choose_local(&a.state) {
        stats::tick_refused(ShapeClass::Append, RefuseReason::ParallelGate);
        return Ok(None);
    }
    if !append_lane_fusible_memo(a, estate)? {
        return Ok(None);
    }
    let crate::procnode::AppendNode { state, substates, .. } = a;
    Ok(Some(::nodeappend::exec_append(state, estate, |e, i| {
        lane_scan_pull_dispatch(&mut substates[i], e)
    })?))
}

// ===========================================================================
// ProjectSet: wholesale refuse BY DEFAULT (wave-5 evaluation, 2026-07-12),
// with the row-mode facility's default-OFF unlock behind it (Phase 0 of
// docs/design/single-executor-migration.md, item 0.5; rowmode.rs).
//
// The wave-5 verdict stands for the DEFAULT config: the SRF tlist expansion
// is per-tuple stateful in three ways the batched lane would have to carry,
// for zero engagement:
//   * the multi-call protocol itself — `pending_srf_tuples` resumes a
//     half-emitted expansion across `exec_proc_node` calls, `args_valid`
//     pins evaluated arg datums across those calls (query-context armed),
//     and `elemdone` tracks per-element ExprMultipleResult state;
//   * SFRM_Materialize mode parks the whole set in a tuplestore read back
//     one row per call — a second, per-element cross-call cursor;
//   * `ExecProjectSRF` interleaves per-tuple context resets between (not
//     within) expansions — a batched drive would need the exact reset
//     points replayed to keep by-ref datum lifetimes identical.
// The "SRFs = expanding operator" phase item now EXISTS as
// `rowmode::ProjectSetOp` — an expanding `TupleOp` whose pause/resume IS
// `pending_srf_tuples` and whose bodies are `exec_project_set`'s own seams
// (reset points replayed exactly; nodeprojectset.rs) — over the one child
// shape with a lane-ownable row face today, the childless Result
// (`SELECT generate_series(...)`). It is engagement-coverage work, not a
// perf lever (migration doc: the facility's value is the contract), so it
// stays behind the default-OFF `PGRUST_LANE_V2_ROWMODE` knob: knob OFF,
// `rowmode::try_own_project_set` ticks the wholesale `srf-set-expansion`
// refuse exactly as the pre-rowmode hook did and `project_set_arm` falls
// through to the unchanged `exec_project_set`.
// ===========================================================================

// ===========================================================================
// Lane-v2 parallel exact-DISTINCT partials (lane-v2-pardistinct) — DELETED
// at Phase-5 D1 (the distinct-hybrid deletion increment; drill letter
// GL-D1-DRILL-1). The GatherMerge-hybrid drives
// (try_own_sorted_distinct_agg_over_gather_merge /
// try_own_plain_distinct_agg_over_gather_merge, the worker-fragment
// partial build try_pardistinct_worker_sort + PdWorkerSink, the shared
// pd_leader_drive, and the Sort-keyed handoff registry) are gone, along
// with their `PGRUST_LANE_V2_PARDISTINCT` kill (now inert). GatherMerge
// distinct plans that still arise (the LOWWIDTH kill posture, the dop-1
// keep-Gather band, faces the parse-altitude probes cannot key) execute on
// the UNCHANGED per-tuple exec_agg over exec_gather_merge fall-through,
// which stays until Phase-5 D5. The nodeagg pardistinct machinery
// (PdSpec/pd_derive_spec, PdBuilder, PdHandedTable, the bucket merger, the
// paremit face) REMAINS: it is the runtime distinct sink's substrate
// (lanev2/runtime_distinct.rs, lanev2/runtime_plaindistinct.rs).
// ===========================================================================

/// `PGRUST_LANE_V2_PARDISTINCT_FORCE=1`: skip the planner-estimate
/// economics (e2e harness lever; the runtime freeze/evict still bounds
/// memory). Consumed by the runtime distinct sink's economics gates
/// (`runtime_distinct.rs`); the Gather-era hybrid drives that shared it
/// were deleted at Phase-5 D1.
fn pardistinct_force() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        matches!(
            std::env::var("PGRUST_LANE_V2_PARDISTINCT_FORCE").as_deref(),
            Ok("1") | Ok("on")
        )
    })
}

// --- WS-S wave-3 (caller C2) — REGION RETIRED at M2 inc-3 rung 4 ---
// The C1/C2 caller-drive modes and their gang-death classifier deleted
// with the launched path (Michael ruling D-1, notes/m2-inc3-rung4.md §4a);
// the PGRUST_TEST_HELPER_VANISH injection (C2's own fault class) retired
// with its battery. C3 (board-shaped seam) lives in runtime_scan.rs.

// ===== WAVE-5 APPEND REGION — do not edit above =====
// Sub-regions in fixed order U, V, W, X (wave-5 contract §2). lanev2.rs
// appends are the FALLBACK placement (knob-resolve/admission shims whose
// vocabulary is lanev2-private); module-local placement is preferred.

// --- WS-U wave-5 (EPQ inc-1: PGRUST_LANE_V2_EPQ, refuse-all admission) --------

use std::sync::atomic::{AtomicU8, Ordering::Relaxed};

/// `PGRUST_LANE_V2_EPQ` (default OFF; wave-5 contract §3 + §6.3): EPQ
/// inc-1's structure-first knob. ON runs the recheck admission WALK below,
/// which REFUSES every shape through the existing `epq` refusal carrier —
/// zero ownership, zero behavior delta (the recheck stays the Volcano
/// drive at both arms; admission widening is inc-5's, gated on WS-P's
/// 100% read-side coverage census). NEVER default during migration.
/// Same AtomicU8 idiom as `dml.rs`'s knobs (OFF-first relaxed byte load,
/// `#[cold]`-outlined resolve, same-process test lever) — placed here and
/// not in epq.rs because the tick vocabulary (`ShapeClass`/`RefuseReason`)
/// is lanev2-private (§2's shim fallback).
static EPQ_LANE: AtomicU8 = AtomicU8::new(0);

/// One relaxed byte load + compare on the OFF arm; called ONLY at the
/// recheck-initiation chokepoint in `crate::epq::eval_plan_qual` (never
/// per row, never per batch — SE2-COST §0.6 idiom).
#[inline]
pub(crate) fn epq_lane_enabled() -> bool {
    match EPQ_LANE.load(Relaxed) {
        1 => false,
        2 => true,
        _ => epq_lane_resolve(),
    }
}

#[cold]
#[inline(never)]
fn epq_lane_resolve() -> bool {
    let on = matches!(
        std::env::var("PGRUST_LANE_V2_EPQ").as_deref(),
        Ok("1") | Ok("on")
    );
    EPQ_LANE.store(if on { 2 } else { 1 }, Relaxed);
    on
}

/// Same-process A/B lever for the unit corpus (`crate::tests`).
#[cfg(test)]
pub(crate) fn epq_lane_set_for_tests(on: bool) {
    EPQ_LANE.store(if on { 2 } else { 1 }, Relaxed);
}

/// Test-only refusal probe: recheck admission-walk refusals ticked by the
/// admission chokepoint (wave-5's `epq_recheck_refuse_all`, widened at
/// wave-7 into `epq::epq_recheck_admission` — the unit corpus proves ON
/// ticks and OFF ticks NOTHING without a stats-env dump).
#[cfg(test)]
pub(crate) static EPQ_ADMISSION_REFUSED_FOR_TESTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

// Wave-5's `epq_recheck_refuse_all` (the unconditional refuse-all walk) and
// its `epq_recheck_shape_class` map were WIDENED at wave-7 into the
// per-node, per-plan-memoized verdict machinery in `lanev2/epq.rs`
// (`epq::epq_recheck_admission` — WS-Y wave-7 rung Y1). Same chokepoint,
// same `epq` refusal carrier, same tick-per-initiation census semantics;
// the classification walk now runs ONCE per recheck plan (wave-5 review
// finding 5). See the module doc there.
// --- end WS-U wave-5 ----------------------------------------------------------

// --- WS-V wave-5 sub-region (reserved) ----------------------------------------
// --- end WS-V wave-5 ----------------------------------------------------------

// --- WS-W (wave-5): OC admission test lever (the admission entry itself is
// dml.rs-local per §2's preference; this is the unit-corpus re-export only).
#[cfg(test)]
pub(crate) use dml::dml_oc_set_for_tests;
// --- end WS-W (wave-5) ---

// --- WS-X wave-5 sub-region (reserved) ----------------------------------------
// --- end WS-X wave-5 ----------------------------------------------------------

// --- WS-Y wave-7 (EPQ inc-5 rungs Y0-Y2; contract §1) ---------------------------
// The lane-side EPQ module: Y0 captured-singleton source (dark), Y1 per-node
// verdicts memoized per recheck plan, chokepoint entry `epq_recheck_admission`.
// Mounted here because the vocabulary it reuses (ShapeClass/RefuseReason and
// the BatchGranuleSource seam) is lanev2-private (§2 shim-fallback precedent,
// wave-5). Y3 (the es_epq_active lift at the try_own_* sites) did NOT land
// this wave — census gate not met; see notes/se-wave7-epq.md (Y3 CARRIED).
pub(crate) mod epq;
// --- end WS-Y wave-7 ------------------------------------------------------------
// --- WS-AA (wave-7): rowchain admission test levers — DELETED at RB-R1
// (SE18) with the stitched trigger-INSERT chain (dml.rs region tombstone).
// --- end WS-AA (wave-7) ---
// --- WS-AE (wave-8): agg-over-IndexScan feed re-exports (AGG_INDEX arm
// re-earn, contract §3 AE0). The feed itself is indexsource.rs-local (the
// WS-AE FREE zone); this EOF append is the module-scope re-export only and
// touches no existing code line (the WS-AA wave-7 EOF-append precedent —
// the wave-7 WS-AA EOF region above is preserved byte-verbatim).
pub(crate) use indexsource::try_own_agg_over_index_source;
#[cfg(test)]
pub(crate) use indexsource::{agg_indexfeed_set_for_tests, AGG_INDEXFEED_OWNED_FOR_TESTS};
// --- end WS-AE (wave-8) ---
// ============================================================================
// ===== WAVE-9 SHARED EOF REGION (contract §7) — sub-regions in AG, AH, AI,
// AJ order; each WS fills ONLY its own block; integration splices verbatim.
// ============================================================================
// --- WS-AG (wave-9): per-mask chain-program test re-export — DELETED at
// RB-R1 (SE18) with the stitched trigger-INSERT chain.
// --- end WS-AG (wave-9) ---
// --- WS-AH (wave-9): reserved ---
// --- WS-AI wave-9 (forward-pull cursors inc-1; contract §3, band 92001+) -------
// The budget substrate is push.rs-local (the §6 freeze-lift grant surface);
// this EOF append is the module-scope re-export only — the WS-AA wave-7 /
// WS-AE wave-8 EOF-append precedent (touches no existing code line). The
// run seam (`execmain.rs::execute_plan`) installs the per-run emission
// budget through this export; the inc-1b park walker will consume
// `push::cursor_run_budget` lanev2-locally (no re-export until it exists).
pub(crate) use push::cursor_run_budget_install;
#[cfg(test)]
pub(crate) use push::{cursor_run_budget, cursors_set_for_tests};
// inc-1b (se/wave95-cursors-1b, this same WS-AI sub-region — append-only
// growth): the §2 park-shape walkers, consumed by the `execute_plan` run
// seam (settle below the drive loop, resume at entry), plus the
// admission-classifier test face (the NAMED refusal-taxonomy strings).
pub(crate) use push::{cursor_park_resume, cursor_run_park};
#[cfg(test)]
pub(crate) use push::cursor_admission_refusal_name;
// --- end WS-AI wave-9 -----------------------------------------------------------
// --- WS-AJ (wave-9): reserved ---
// WS-AJ wave-9.5 (SPI Stage-A seam, `se/spi-stage-a`; lane-spi.md §1/§3 —
// filling the reserved sub-region above, EOF-append only): the count-seam
// halves consumed by the `execute_plan` run seam (install at entry; the
// settle below the drive loop, whose parked result arms the shared WS-AI
// resume signal — notes/se-spi-stage-a.md §8), plus the
// admission-classifier and knob test faces. Substrate lives in push.rs
// (WS-AJ region).
pub(crate) use push::{spi_run_budget_install, spi_run_settle};
#[cfg(test)]
pub(crate) use push::{spi_admission_refusal_name, spi_set_for_tests};
// --- end WS-AJ (wave-9.5) ---------------------------------------------------------
// ============================================================================
// ===== WAVE-10 SHARED EOF REGION (cursors inc-2 contract §8) — sub-regions
// in CA, CB, CC order; each WS fills ONLY its own block; integration splices
// verbatim.
// ============================================================================
// --- WS-CA (wave-10): reserved ---
// --- WS-CB wave-10 (cursors inc-2: batch store fill; contract §2.1, band 95001+) ---
// EOF append only (the WS-AI precedent above; zero code lines touched).
// The pub faces are the CA-facing seam (knob gate for store arming, the §6
// assert arming note, the §3.3 tick face), re-exported at the crate root —
// worklog notes/se-wave10-cb.md EX-CB-1. The pub(crate) faces are the run
// seam's (execute_plan wave-10 CB sub-region) and the band-95001 units'.
pub use push::{cursor_store_armed_note, cursor_store_fill_enabled};
// SEAM-WIRING (SE10-GATES item 1): the portal-layer unit-test lever for THE
// single knob cell (replaces the retired portalmem duplicate's
// `cursor_store_set_for_tests`; pquery/portalcmds batteries reach it through
// the execmain crate-root re-export).
pub use push::cursor_store_fill_set_for_tests;
pub(crate) use push::{
    cursor_store_batch_fill, run_seam_backward_evidence, run_seam_backward_evidence_count,
};

// SE-HASHOFF census face (deletion-prep arms #6/#7; notes/
// se-hashoff-letters.md): procnode's fused hash-build chokepoint ticks the
// stats-armed census counters through these crate-visible wrappers — dump
// rows `fused-hash-build-*` in the PGRUST_LANE_V2_STATS TSV. Measurement
// accounting only; no admission decision reads them.
#[inline]
pub(crate) fn fused_hash_build_census_seq(engaged: bool, proj: bool) {
    stats::tick_fused_hash_build_seq(engaged, proj);
}
#[inline]
pub(crate) fn fused_hash_build_census_other() {
    stats::tick_fused_hash_build_other();
}
#[cfg(test)]
pub(crate) use push::{cursor_fill_step_seqscan_for_tests, cursor_store_ever_armed};
// --- end WS-CB wave-10 ------------------------------------------------------------
// --- WS-CC (wave-10): reserved ---
// ============================================================================
// ===== WS-MJ (LANE-MERGEJOIN inc-1) shared EOF sub-region — contract §6.1:
// the WS-MJ named dispatch region (mergejoin arm surface + module mount +
// unit-corpus re-exports). Append-only; other workstreams splice below.
// ============================================================================
// The lane-native MergeJoin engine composition (knob, feed adapters, drive)
// is lane_mergejoin.rs-local; this region holds the census-counted surface
// (`fn try_own_merge_join` must live in lanev2.rs proper — the wave-7 census
// derivation greps `fn try_own` rows HERE; contract §1.1/§3.1) and the
// module-scope re-exports (the WS-AA/WS-AE EOF-append precedent).
mod lane_mergejoin;

/// Try to let the lane own one pull of a MergeJoin node — the lane-native
/// merge join over sorted inputs (LANE-MERGEJOIN contract §1; K4 option-a,
/// notes/se-wave8-epq.md §4). inc-1 envelope: JOIN_INNER only, Sort inner
/// only; the mergejoin family is SEVEN join types (nodeMergejoin.c
/// ExecInitMergeJoin's jointype switch — JOIN_RIGHT_SEMI has no case and
/// falls to its `elog(ERROR)` default; the "8 join types" framing is the
/// hashjoin lane's envelope), covered by inc-2/inc-3. Refusals are NAMED
/// and ride existing carriers (mint zero, contract §4.1):
///   * `epq`            — es_epq_active HARD LAW (§1.4): refuse inside every
///                        driven recheck, fall through byte-identically; the
///                        lift is Y3's, one step, census-gated — NOT here.
///   * `backward`       — mergejoin-backward: C asserts !(BACKWARD|MARK) at
///                        ExecInitMergeJoin; the node never supports either.
///   * `instrumented`   — EXPLAIN ANALYZE keeps the Volcano drive + counters
///                        (the mj_verdict_slow cadence).
///   * `join-shape`     — mergejoin-jointype: non-INNER face at inc-1.
///   * `child-not-lane-owned` — mergejoin-inner-feed: non-Sort inner at
///                        inc-1 (Material = inc-2, index scans = inc-3; C's
///                        admissible inner set is ExecSupportsMarkRestore,
///                        execAmi.c:411).
/// `None` = refused: the caller falls through to the SH-E hosting verdict +
/// the Volcano `exec_merge_join`, byte-identically. Knob-OFF (the default)
/// is one relaxed cached-bool load + compare and ticks NOTHING (§4.1
/// knob-OFF-zero: base ticks no refusal from this surface, so OFF must not
/// either). Mixed-drive coherence: lane and Volcano share one
/// `MergeJoinState` (lane_mergejoin.rs module doc), so per-pull handover is
/// byte-safe in both directions.
#[inline]
pub fn try_own_merge_join<'mcx>(
    mj: &mut crate::procnode::MergeJoinNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // The §4.1 OFF-arm byte: knob FIRST, before any other read or tick.
    if !lane_mergejoin::mergejoin_native_enabled() {
        return Ok(None);
    }
    // Master facility switch: checked before lane logic, never ticks (the
    // EnvOff doc-contract in stats.rs; the mj_verdict_slow head cadence).
    if !enabled() {
        return Ok(None);
    }
    // Dynamic per-call gates, the mj_verdict_slow priority order
    // (EPQ -> instrumented), each a NAMED refusal per pull. (The backward
    // gate - probe id 1 - retired with the backward-execution wave B11;
    // probe ids keep their historical numbering.)
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::MergeJoin, RefuseReason::Epq);
        #[cfg(test)]
        lane_mergejoin::mj_native_refusal_probe(0);
        return Ok(None);
    }
    if !estate.es_instrumentation.is_empty() {
        stats::tick_refused(ShapeClass::MergeJoin, RefuseReason::Instrumented);
        #[cfg(test)]
        lane_mergejoin::mj_native_refusal_probe(2);
        return Ok(None);
    }
    // inc-1 envelope: INNER only (mergejoin-jointype; inc-2/inc-3 widen).
    if mj.state.plan.join.jointype != ::types_nodes::JoinType::JOIN_INNER {
        stats::tick_refused(ShapeClass::MergeJoin, RefuseReason::JoinShape);
        #[cfg(test)]
        lane_mergejoin::mj_native_refusal_probe(3);
        return Ok(None);
    }
    // inc-1 inner feed: Sort only (mergejoin-inner-feed) — the exact RA
    // Tuplesort read-back family AD0 admits, byte-proven by the sortra-e2e
    // "mergejoin mark/restore Sort inner" cell (contract §0/§1.3).
    if !matches!(&*mj.inner, crate::procnode::PlanStateNode::Sort(_)) {
        stats::tick_refused(ShapeClass::MergeJoin, RefuseReason::ChildNotLaneOwned);
        #[cfg(test)]
        lane_mergejoin::mj_native_refusal_probe(4);
        return Ok(None);
    }
    stats::tick_owned(ShapeClass::MergeJoin);
    lane_mergejoin::lane_merge_join_drive(mj, estate).map(Some)
}

#[cfg(test)]
pub(crate) use lane_mergejoin::{
    mergejoin_native_set_for_tests, MJ_NATIVE_MARKS_FOR_TESTS, MJ_NATIVE_OWNED_FOR_TESTS,
    MJ_NATIVE_REFUSED_FOR_TESTS, MJ_NATIVE_RESTORES_FOR_TESTS,
};
// --- end WS-MJ (LANE-MERGEJOIN inc-1) -------------------------------------------
// --- SE-AGGBITMAP (deletion-prep arm #4 re-host): the lane batch feed for
// aggregation over BitmapHeapScan, behind PGRUST_LANE_V2_AGG_BITMAP
// (default OFF). Feed logic + refuse-set are agg_bitmap.rs-local; this EOF
// append is the module mount + re-export only and touches no existing code
// line (the WS-AE wave-8 EOF-append precedent). Mounted here because the
// vocabulary it reuses (ShapeClass/RefuseReason, lane_trace, the engine
// mirror) is lanev2-private.
mod agg_bitmap;
pub(crate) use agg_bitmap::try_own_agg_over_bitmap_feed;
#[cfg(test)]
pub(crate) use agg_bitmap::{agg_bitmap_set_for_tests, AGG_BITMAP_OWNED_FOR_TESTS};
// --- end SE-AGGBITMAP -----------------------------------------------------------
// --- MJSORT (the "merge join after sort" runtime car, m5-coverage row
// merge-join-parallel; PGRUST_RUNTIME_MJSORT, default OFF): the arm rides
// the shape-(b) full-sort machinery in PUBLISH mode on both children and
// merges aligned key ranges as pure-compute morsels (nodesort mjmerge
// kernels). Logic + refuse-set live in runtime_mergejoin.rs; this EOF
// append is the module mount + re-exports only (the SE-AGGBITMAP
// precedent). NOT a BOOTSTRAP_MATRIX class — the coverage row keeps
// route_to=legacy until the GL-MJSORT fleet letter flips it.
mod runtime_mergejoin;
pub(crate) use runtime_mergejoin::{
    try_own_agg_over_merge_join, try_own_merge_join_mjsort, MjSortAdopted,
};
// --- end MJSORT -------------------------------------------------------------------
