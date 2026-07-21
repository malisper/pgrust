//! Row-mode read-side TAIL — wave-2 WS-L (the wave-2 integration contract
//! §1/§3/§4; design + WS-N seam spec in docs/design/rowmode-tail.md).
//!
//! Hosts the remaining read-side plan shapes as pure DELEGATION LEAVES.
//! Evolution of the hosting form (each step statement-identity-preserving):
//! wave-2 drove a per-shape `RowSource` through `pull_step_rows` over
//! `PassthroughOp` + `RootAdapter::new(None)`; se-delegtax SH-A collapsed
//! that pipeline to the degenerate `pull_step_point` driver (≡ a bare
//! `next_row`; proof in push.rs); se-delegtax SH-E then took the final
//! step: since the delegated `next_row` ran the IDENTICAL statements the
//! `procnode` arm's own fallback runs, ownership of a delegation leaf is
//! purely an ACCOUNTING fact — the arm now executes its single fallback
//! body on every pull (original Volcano tail-call shape), and this module
//! contributes the per-pull ownership VERDICT (gates + ticks + G7 capture)
//! as a side effect (`verdict` below). The `RowSource` bodies remain ONLY
//! for the six T3 shapes, whose tail_source.rs source form reuses them.
//! docs/design/rowmode-tail.md restates the byte-identity argument (one
//! `next_row` per owned pull ≡ one Volcano call per pull; zero lane-held
//! cross-call state; mark/restore and rescan enter through `execami`,
//! which the hosting never intercepted).
//!
//! Shapes (vocabulary in stats.rs, the one wave-2 vocab commit):
//! SubqueryScan (REUSES class 10), FunctionScan, TableFuncScan, ValuesScan,
//! SampleScan, TidScan, TidRangeScan, NamedTuplestoreScan, Material (inc-1);
//! CteScan, RecursiveUnion + WorkTableScan, Memoize (inc-2); SetOp,
//! MergeAppend, Unique, LockRows-without-EPQ (inc-3). ForeignScan is OUT of
//! wave 2 (Phase 3.4 ledger); Gather/GatherMerge are parallel-dispatch
//! nodes, excluded from the coverage denominator (contract §6-WS-L(2)).
//!
//! Knob: the existing `PGRUST_LANE_V2_ROWMODE` facility gate (default OFF;
//! contract §2 — NO per-shape sub-knobs; per-shape bisect is test-side only
//! via `ROWMODE_TAIL_OWNED_FOR_TESTS`). Knob-OFF ticks NOTHING for every
//! tail class: none of these shapes has a pre-existing wholesale refuse, so
//! default-config accounting stays byte-identical by construction (§2d).
//! MergeJoin is deliberately NOT here — it sits behind its own
//! `PGRUST_LANE_V2_MERGEJOIN` since the wave-2 knob-split commit
//! (rowmode.rs).
//!
//! Gate order (contract §3.2, exactly WS-G): knob (OFF = `Ok(None)`, ticks
//! nothing) → `es_epq_active` → Epq → `!forward` → Backward → instrumented →
//! Instrumented → shape gates (none: delegation is shape-agnostic) →
//! `tick_owned` ONCE → `pull_step_point`. OWNED cadence = once per drive
//! start = once per owned PG pull (§3.3; pull ≡ drive for row-mode). The
//! dynamic gates are OR-folded with the reason re-derived on a `#[cold]`
//! tail in the same priority order (se-delegtax SH-D) — set + cadence
//! identical.
//!
//! Shared-slot law (contract §3.8, binding here): no `RowSource` below
//! caches a shared-slot handle or read position across `next_row` calls.
//! Trivially satisfied: every delegation body re-enters the ported exec
//! function, which itself does the `es_worktable_shared` / CTE-shared
//! take-use-put-back per call (`exec_recursive_union`'s take/put around
//! every child call; `exec_work_table_scan` / `exec_cte_scan` resolving
//! their shared state per call).
//!
//! SubqueryScan + Unique COMPOSITION: those two arms already carry lane
//! hooks (the wave-4 streaming glue over the sort breaker). The glue keeps
//! priority — `lanev2.rs`'s `try_own_subquery_scan` / `try_own_unique` fall
//! through to `try_own_subquery_scan_tail` / `try_own_unique_tail` here when
//! the glue refuses, so the procnode arms stay single-hook and default
//! accounting is untouched (knob OFF the tail returns before any tick).
//! Knob-ON, an EPQ/backward offer may tick a class-10 refusal from the glue
//! AND one from the tail (two mechanisms, two offers) — documented in the
//! lane-gates.allowlist block.

#[cfg(test)]
use std::sync::atomic::Ordering::Relaxed;

use ::executils::{EStateData, ExecSlotId};
use ::types_error::PgResult;

use super::push::RowSource;
use super::rowmode::rowmode_enabled;
use super::stats::{self, RefuseReason, ShapeClass};

/// Test-only per-class engagement probes: owned row-mode tail drives, per
/// pull, indexed by `ShapeClass` discriminant (the ratified replacement for
/// per-shape probe statics — contract §3.4). The unit corpus asserts the ON
/// arm engaged THE shape under test, not some other tail hook on the same
/// knob.
#[cfg(test)]
static ROWMODE_TAIL_OWNED_FOR_TESTS: [std::sync::atomic::AtomicU64; stats::n_classes()] =
    [const { std::sync::atomic::AtomicU64::new(0) }; stats::n_classes()];

/// Test-side probe read, keyed by the class display name (`ShapeClass` is
/// vocabulary-private to `lanev2`; the corpus asserts engagement by name —
/// "material", "ctescan", ... — through this accessor).
#[cfg(test)]
pub(crate) fn tail_owned_probe_for_tests(name: &str) -> u64 {
    let class = ShapeClass::ALL
        .iter()
        .find(|c| c.name() == name)
        .unwrap_or_else(|| panic!("unknown lane shape class name: {name}"));
    ROWMODE_TAIL_OWNED_FOR_TESTS[*class as usize].load(Relaxed)
}

/// The dynamic per-call gates of the host template, in contract §3.2 order
/// (the knob is checked by each `try_own_*` BEFORE this so knob-OFF ticks
/// nothing). `None` = admitted; `Some(reason)` = refused (already ticked —
/// the reason is returned so the wave-4 G7 capture below can record the
/// verdict without re-deriving it).
///
/// se-delegtax SH-D (the express-adm INC-1 shape): the hot path is one
/// OR-combined test; the reason derivation + refused tick live on a
/// `#[cold]` outlined tail that re-derives the FIRST failing gate in the
/// original §3.2 priority order — refusal set and tick cadence identical.
#[inline]
fn tail_gates(class: ShapeClass, estate: &EStateData<'_>) -> Option<RefuseReason> {
    // (The backward compare retired with the backward-execution wave B11:
    // pulls are forward-invariant below the run seam, deletion-prep B1.)
    if estate.es_epq_active || !estate.es_instrumentation.is_empty() {
        return Some(tail_gate_refused(class, estate));
    }
    None
}

/// Cold refuse tail: re-derive the first failing gate in §3.2 priority
/// order (EPQ → instrumented; the backward arm retired with the
/// backward-execution wave B11), tick it, return it. Reached only when
/// `tail_gates`'s OR-fold fired, so one of the two holds.
#[cold]
#[inline(never)]
fn tail_gate_refused(class: ShapeClass, estate: &EStateData<'_>) -> RefuseReason {
    let r = if estate.es_epq_active {
        RefuseReason::Epq
    } else {
        RefuseReason::Instrumented
    };
    stats::tick_refused(class, r);
    r
}

/// Cold owned-path diagnostics tail (se-delegtax SH-B): reached only when
/// `super::leaf_diag_mask()` is nonzero (accounting or trace armed — never
/// at default config). Tick cadence unchanged: OWNED once per drive start.
/// GL-ROWMODE-1: the TRACE line (bit 1) is deduped to the first owned pull
/// per class per execution — per-pull tracing on these delegation leaves is
/// a per-inner-row stderr write on merge-join-over-Materialize shapes (the
/// witnessed ~200-330x trace-armed collapse; rationale at
/// `lane_trace_owned_once`).
#[cold]
#[inline(never)]
fn tail_diag_owned(class: ShapeClass, diag: u8, estate: &mut EStateData<'_>) {
    if diag & 1 != 0 {
        stats::tick_owned(class);
    }
    if diag & 2 != 0 {
        super::lane_trace_owned_once(class, estate, || {
            format!("rowmode-tail: {} drive owned", class.name())
        });
    }
}

/// The per-pull OWNERSHIP VERDICT, shared by every tail shape (se-delegtax
/// SH-E). After SH-A made delegation hosting statement-identical to the
/// arm's own fallback call, "the lane owns this pull" is PURELY an
/// accounting fact for a delegation leaf: the arm runs ONE body either way
/// (its original Volcano tail-call shape — no `Option<Option<..>>` sret
/// round trip, no second knob read, no PgBox re-deref), and this function
/// contributes the decision side effects only: the §3.2 dynamic gates
/// (refusal ticks on the cold tail), the G7 EngineEvent capture, and the
/// OWNED tick (once per pull, behind the SH-B diag mask). Returns whether
/// the pull was ADMITTED (lockrows_arm uses it to keep the
/// rowmode-before-DML hook priority; other arms discard it).
///
/// Callers reached from procnode arms are pre-gated on
/// `rowmode_tail_active()`; the two lanev2-glue fallbacks (SubqueryScan /
/// Unique) gate on `rowmode_enabled()` in their own wrappers below, so
/// knob-OFF still ticks NOTHING for every tail class (contract §2d).
///
/// `capture_id` (SH-C): the Plan-id chase runs ONLY under
/// `estate.engine_capture()` — `|| None` for the ScanState-shaped leaves
/// (the NAMED G7 residual on the WS-C D3 ledger, not a silent hole).
#[inline]
fn verdict<F: FnOnce() -> Option<i32>>(
    class: ShapeClass,
    capture_id: F,
    estate: &mut EStateData<'_>,
) -> bool {
    // SH-F fast path: the per-execution-static byte (GUC on, no
    // instrumentation, no capture, diag disarmed) + the per-pull-dynamic
    // EPQ gate inline (es_epq_active shares the byte's cache line).
    // byte==true means the slow path below would admit AND tick nothing,
    // so the fast admit is decision- and accounting-identical. (The
    // backward compare retired with the backward-execution wave B11.)
    if estate.es_lane_leaf_fast && !estate.es_epq_active {
        #[cfg(test)]
        ROWMODE_TAIL_OWNED_FOR_TESTS[class as usize].fetch_add(1, Relaxed);
        return true;
    }
    verdict_slow(class, capture_id, estate)
}

/// The full verdict (outlined): every diagnostics-armed, instrumented,
/// EPQ, or GUC-off pull lands here — the exact pre-SH-F body,
/// with the lane-executor GUC gate at its head (it rode the arms'
/// `rowmode_tail_active` before SH-F made that knob-only). GUC-off admits
/// nothing and ticks nothing, matching the retired arm-gate short-circuit.
#[inline(never)]
fn verdict_slow<F: FnOnce() -> Option<i32>>(
    class: ShapeClass,
    capture_id: F,
    estate: &mut EStateData<'_>,
) -> bool {
    if !super::enabled() {
        return false;
    }
    let refuse = tail_gates(class, estate);
    if estate.engine_capture() {
        if let Some(id) = capture_id() {
            super::engine_record_verdict(estate, id, class, refuse);
        }
    }
    if refuse.is_some() {
        return false;
    }
    let diag = super::leaf_diag_mask();
    if diag != 0 {
        tail_diag_owned(class, diag, estate);
    }
    #[cfg(test)]
    ROWMODE_TAIL_OWNED_FOR_TESTS[class as usize].fetch_add(1, Relaxed);
    true
}

// ===========================================================================
// Increment 1 — the 9 delegation shapes (contract §5 Stage 1).
// ===========================================================================

/// SubqueryScan tail fallback verdict (class 10 REUSE, §1) — called ONLY
/// from `try_own_subquery_scan` (lanev2.rs) after the wave-4 streaming glue
/// refused; never hooked from procnode directly. Knob-gated here (the glue
/// is master-gated only).
#[inline]
pub(super) fn subquery_scan_tail_verdict(estate: &mut EStateData<'_>) {
    if rowmode_enabled() {
        // No reachable Plan id (ScanState-shaped; G7 residual — see `verdict`).
        verdict(ShapeClass::SubqueryScan, || None, estate);
    }
}

/// FunctionScan as a delegation leaf: `exec_function_scan` (SRF
/// materialize/value-per-call protocols run inside it, state node-resident).
/// `pub(super)`: the wave-3 WS-Q source form (tail_source.rs) reuses THIS
/// body — statement-identity between the two hosting forms by construction
/// (same for the five other T3 shapes below).
pub(super) struct FunctionScanSource;

impl<'mcx> RowSource<'mcx> for FunctionScanSource {
    type Node = ::nodefunctionscan::FunctionScanState<'mcx>;

    fn next_row(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>> {
        ::nodefunctionscan::exec_function_scan(node, estate)
    }
}

#[inline]
pub fn try_own_function_scan<'mcx>(
    fs: &mut ::mcx::PgBox<'mcx, ::nodefunctionscan::FunctionScanState<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Wave-3 WS-Q: source form FIRST (the upgrade, `PGRUST_LANE_V2_SCANS_T3`);
    // delegation under `PGRUST_LANE_V2_ROWMODE` is the rollback semantics.
    if let Some(r) = super::tail_source::try_own_function_scan_t3(&mut **fs, estate)? {
        return Ok(Some(r));
    }
    // T3 refused: the delegation VERDICT (SH-E — accounting only; the
    // arm's fall-through IS the delegated body). Knob-gated: the arm may
    // be reached via scans_t3_active() alone.
    if rowmode_enabled() {
        // No reachable Plan id (ScanState-shaped; G7 residual — see `verdict`).
        verdict(ShapeClass::FunctionScan, || None, estate);
    }
    Ok(None)
}

/// TableFuncScan (XMLTABLE/JSON_TABLE) as a delegation leaf.
pub(super) struct TableFuncScanSource;

impl<'mcx> RowSource<'mcx> for TableFuncScanSource {
    type Node = ::nodetablefuncscan::TableFuncScanState<'mcx>;

    fn next_row(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>> {
        ::nodetablefuncscan::exec_table_func_scan(node, estate)
    }
}

#[inline]
pub fn try_own_table_func_scan<'mcx>(
    ts: &mut ::mcx::PgBox<'mcx, ::nodetablefuncscan::TableFuncScanState<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if let Some(r) = super::tail_source::try_own_table_func_scan_t3(&mut **ts, estate)? {
        return Ok(Some(r));
    }
    // T3 refused: the delegation VERDICT (SH-E — accounting only; the
    // arm's fall-through IS the delegated body). Knob-gated: the arm may
    // be reached via scans_t3_active() alone.
    if rowmode_enabled() {
        // No reachable Plan id (ScanState-shaped; G7 residual — see `verdict`).
        verdict(ShapeClass::TableFuncScan, || None, estate);
    }
    Ok(None)
}

/// ValuesScan pull verdict (SH-E; the m4 batch-INSERT feed shape). No
/// reachable Plan id (ScanState-shaped; G7 residual — see `verdict`).
#[inline]
pub fn values_scan_pull_verdict(estate: &mut EStateData<'_>) {
    verdict(ShapeClass::ValuesScan, || None, estate);
}

/// SampleScan as a delegation leaf (TSM method calls stay inside the ported
/// body; the EPQ arm inside `exec_sample_scan` is unreachable through this
/// hosting — the Epq gate refused first — and delegated verbatim anyway).
pub(super) struct SampleScanSource;

impl<'mcx> RowSource<'mcx> for SampleScanSource {
    type Node = ::nodesamplescan::SampleScanState<'mcx>;

    fn next_row(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>> {
        ::nodesamplescan::exec_sample_scan(node, estate)
    }
}

#[inline]
pub fn try_own_sample_scan<'mcx>(
    ss: &mut ::mcx::PgBox<'mcx, ::nodesamplescan::SampleScanState<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if let Some(r) = super::tail_source::try_own_sample_scan_t3(&mut **ss, estate)? {
        return Ok(Some(r));
    }
    // T3 refused: the delegation VERDICT (SH-E — accounting only; the
    // arm's fall-through IS the delegated body). Knob-gated: the arm may
    // be reached via scans_t3_active() alone.
    if rowmode_enabled() {
        // No reachable Plan id (ScanState-shaped; G7 residual — see `verdict`).
        verdict(ShapeClass::SampleScan, || None, estate);
    }
    Ok(None)
}

/// TidScan as a delegation leaf (`WHERE ctid = ...` / `= ANY(...)` /
/// CURRENT OF; the tid-list build + heap fetches stay in the ported body).
pub(super) struct TidScanSource;

impl<'mcx> RowSource<'mcx> for TidScanSource {
    type Node = ::nodetidscan::TidScanState<'mcx>;

    fn next_row(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>> {
        ::nodetidscan::exec_tid_scan(node, estate)
    }
}

#[inline]
pub fn try_own_tid_scan<'mcx>(
    ts: &mut ::nodetidscan::TidScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if let Some(r) = super::tail_source::try_own_tid_scan_t3(ts, estate)? {
        return Ok(Some(r));
    }
    // T3 refused: the delegation VERDICT (SH-E — accounting only; the
    // arm's fall-through IS the delegated body). Knob-gated: the arm may
    // be reached via scans_t3_active() alone.
    if rowmode_enabled() {
        // No reachable Plan id (ScanState-shaped; G7 residual — see `verdict`).
        verdict(ShapeClass::TidScan, || None, estate);
    }
    Ok(None)
}

/// TidRangeScan as a delegation leaf (ctid range bounds inside the body).
pub(super) struct TidRangeScanSource;

impl<'mcx> RowSource<'mcx> for TidRangeScanSource {
    type Node = ::nodetidrangescan::TidRangeScanState<'mcx>;

    fn next_row(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>> {
        ::nodetidrangescan::exec_tid_range_scan(node, estate)
    }
}

#[inline]
pub fn try_own_tid_range_scan<'mcx>(
    ts: &mut ::nodetidrangescan::TidRangeScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if let Some(r) = super::tail_source::try_own_tid_range_scan_t3(ts, estate)? {
        return Ok(Some(r));
    }
    // T3 refused: the delegation VERDICT (SH-E — accounting only; the
    // arm's fall-through IS the delegated body). Knob-gated: the arm may
    // be reached via scans_t3_active() alone.
    if rowmode_enabled() {
        // No reachable Plan id (ScanState-shaped; G7 residual — see `verdict`).
        verdict(ShapeClass::TidRangeScan, || None, estate);
    }
    Ok(None)
}

/// NamedTuplestoreScan (AFTER-trigger transition tables) as a delegation
/// leaf. The mutation that PRODUCES the named store is out of lane scope
/// (dualexec-strict cannot dual-execute it); the read leg proves via the
/// serial e2e (contract cross-cutting law: honest-gap flag at boards).
pub(super) struct NamedTuplestoreScanSource;

impl<'mcx> RowSource<'mcx> for NamedTuplestoreScanSource {
    type Node = ::nodenamedtuplestorescan::NamedTuplestoreScanState<'mcx>;

    fn next_row(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>> {
        ::nodenamedtuplestorescan::exec_named_tuplestore_scan(node, estate)
    }
}

#[inline]
pub fn try_own_named_tuplestore_scan<'mcx>(
    nts: &mut ::mcx::PgBox<'mcx, ::nodenamedtuplestorescan::NamedTuplestoreScanState<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if let Some(r) = super::tail_source::try_own_named_tuplestore_scan_t3(&mut **nts, estate)? {
        return Ok(Some(r));
    }
    // T3 refused: the delegation VERDICT (SH-E — accounting only; the
    // arm's fall-through IS the delegated body). Knob-gated: the arm may
    // be reached via scans_t3_active() alone.
    if rowmode_enabled() {
        // No reachable Plan id (ScanState-shaped; G7 residual — see `verdict`).
        verdict(ShapeClass::NamedTuplestoreScan, || None, estate);
    }
    Ok(None)
}

/// Material pull verdict (SH-E; tail-1's dominant puller). The mark/restore
/// protocol (`exec_material_mark_pos` / `exec_material_restr_pos` — the
/// MergeJoin ExtraMarks cadence) enters through `execami` DIRECTLY on the
/// node and never crossed the retired hosting either.
#[inline]
pub fn material_pull_verdict<'mcx>(
    m: &crate::procnode::MaterialNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) {
    verdict(ShapeClass::Material, || Some(m.state.plan.plan.plan_node_id), estate);
}

// ===========================================================================
// Increment 2 — the recursive-CTE machinery + Memoize (contract §5 Stage 2;
// iteration protocol + shared-slot law: docs/design/rowmode-tail.md §3).
// ===========================================================================

/// CteScan pull verdict (SH-E). The CTE-shared tuplestore take-use-put-back
/// stays inside the ported body (shared-slot law trivially preserved — the
/// lane holds NOTHING). No reachable Plan id (G7 residual).
#[inline]
pub fn cte_scan_pull_verdict(estate: &mut EStateData<'_>) {
    verdict(ShapeClass::CteScan, || None, estate);
}

/// RecursiveUnion pull verdict (SH-E): the whole iteration protocol
/// (worktable swap, dedup hash, WorkTableShared TAKE/PUT) is
/// `exec_recursive_union`'s own body — the arm's single fall-through call.
#[inline]
pub fn recursive_union_pull_verdict<'mcx>(
    ru: &crate::procnode::RecursiveUnionNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) {
    verdict(ShapeClass::RecursiveUnion, || Some(ru.state.plan.plan.plan_node_id), estate);
}

/// WorkTableScan pull verdict (SH-E): `exec_work_table_scan` resolves its
/// rustate from `estate.worktable_shared_slot(wtParam)` per call (shared-slot
/// law). No reachable Plan id (G7 residual).
#[inline]
pub fn work_table_scan_pull_verdict(estate: &mut EStateData<'_>) {
    verdict(ShapeClass::WorkTableScan, || None, estate);
}

/// Memoize pull verdict (SH-E; the WS-L OQ delegation ruling stands — the
/// arm's fall-through rebuilds the MemoizeOuter view + `exec_memoize`, the
/// exact statements the retired MemoizeSource replayed).
#[inline]
pub fn memoize_pull_verdict<'mcx>(
    m: &crate::procnode::MemoizeNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) {
    verdict(ShapeClass::Memoize, || Some(m.state.plan.plan.plan_node_id), estate);
}

// ===========================================================================
// Increment 3 — SetOp / MergeAppend / Unique / LockRows-without-EPQ
// (contract §5 Stage 3; the LockRows RowSource closure boundary is THE
// pinned WS-N inc-2b seam — docs/design/rowmode-tail.md §4).
// ===========================================================================

/// SetOp pull verdict (SH-E).
#[inline]
pub fn set_op_pull_verdict<'mcx>(
    so: &crate::procnode::SetOpNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) {
    verdict(ShapeClass::SetOp, || Some(so.state.plan.plan.plan_node_id), estate);
}

/// MergeAppend pull verdict (SH-E).
#[inline]
pub fn merge_append_pull_verdict<'mcx>(
    m: &crate::procnode::MergeAppendNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) {
    verdict(ShapeClass::MergeAppend, || Some(m.state.plan.plan.plan_node_id), estate);
}

/// Unique tail fallback verdict (SH-E) — called ONLY from `try_own_unique`
/// (lanev2.rs) after the streaming glue refused; never hooked from procnode
/// directly. Knob-gated here (the glue is master-gated only).
#[inline]
pub(super) fn unique_tail_verdict<'mcx>(
    u: &crate::procnode::UniqueNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) {
    if rowmode_enabled() {
        verdict(ShapeClass::Unique, || Some(u.state.plan.plan.plan_node_id), estate);
    }
}

/// LockRows pull verdict (SH-E), meaningful ONLY outside an active EPQ
/// recheck (the `tail_gates` Epq refuse; EPQ law §3.5). Locking (and any
/// EPQ recheck it initiates) happens inside the arm's single fall-through
/// body, so lock-before-emit order is Volcano's own — as it was under the
/// retired LockRowsSource (whose `epq_eval` closure boundary WAS the pinned
/// WS-N inc-2b seam; the seam now lives solely in the arm's fall-through
/// call, same spelling). Returns ADMITTED so lockrows_arm preserves the
/// rowmode-before-DML hook priority exactly (the DML TupleOp is offered
/// only on a rowmode non-admit, as before).
#[inline]
pub fn lock_rows_pull_verdict<'mcx>(
    l: &crate::procnode::LockRowsNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> bool {
    // SH-F exemption: LockRows admission gates REAL behavior (the DML
    // TupleOp hook is offered only on non-admit), so it never rides the
    // fast-admit byte — the full-gate slow path is its permanent spelling
    // (defensive: a stale byte must never flip hook priority).
    verdict_slow(ShapeClass::LockRows, || Some(l.state.plan.plan.plan_node_id), estate)
}
