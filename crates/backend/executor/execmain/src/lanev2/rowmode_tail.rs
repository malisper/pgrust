//! Row-mode read-side TAIL — wave-2 WS-L (the wave-2 integration contract
//! §1/§3/§4; design + WS-N seam spec in docs/design/rowmode-tail.md).
//!
//! Hosts the remaining read-side plan shapes through the row-mode host
//! template (contract §3.2): each shape is a pure DELEGATION LEAF — a
//! `RowSource` whose `next_row` runs the identical statements the node's
//! `procnode` arm fallback runs (a call into the ported per-node exec body,
//! children driven Volcano inside it), stepped by `pull_step_rows` through
//! `PassthroughOp` into a `RootAdapter`. The WS-G MergeJoin hosting is the
//! template's precedent; docs/design/rowmode-tail.md restates the argument
//! for why delegation is byte-identical BY CONSTRUCTION (one `next_row` per
//! owned pull ≡ one Volcano call per pull; zero lane-held cross-call state;
//! mark/restore and rescan enter through `execami`, which this hosting never
//! intercepts).
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
//! `tick_owned` ONCE → `pull_step_rows`. OWNED cadence = once per drive
//! start = once per owned PG pull (§3.3; pull ≡ drive for row-mode).
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

use std::sync::atomic::Ordering::Relaxed;

use ::executils::{EStateData, ExecSlotId};
use ::types_error::PgResult;

use super::push::{pull_step_rows, PassthroughOp, RootAdapter, RowSource};
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
/// nothing). `true` = admitted.
#[inline]
fn tail_gates(class: ShapeClass, estate: &EStateData<'_>) -> bool {
    if estate.es_epq_active {
        stats::tick_refused(class, RefuseReason::Epq);
        return false;
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(class, RefuseReason::Backward);
        return false;
    }
    if !estate.es_instrumentation.is_empty() {
        stats::tick_refused(class, RefuseReason::Instrumented);
        return false;
    }
    true
}

/// The host-template drive, shared by every tail shape: gates (in the §3.2
/// order), OWNED tick ONCE, then one `pull_step_rows` step over the
/// per-call-reassembled `RowSource → PassthroughOp → RootAdapter` pipeline.
/// `RootAdapter::new(None)` for all tail shapes: every delegated exec body
/// runs its own end-of-stream slot handling (`exec_scan_impl`'s projected
/// clear included), so the wrapper adds no clear the Volcano arm would not
/// perform.
#[inline]
fn drive<'mcx, S>(
    class: ShapeClass,
    node: &mut S::Node,
    src: &mut S,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>>
where
    S: RowSource<'mcx>,
{
    if !rowmode_enabled() {
        return Ok(None);
    }
    if !tail_gates(class, estate) {
        return Ok(None);
    }
    stats::tick_owned(class);
    if super::lane_trace_enabled() {
        super::lane_trace(&format!("rowmode-tail: {} drive owned", class.name()));
    }
    #[cfg(test)]
    ROWMODE_TAIL_OWNED_FOR_TESTS[class as usize].fetch_add(1, Relaxed);
    let mut op = PassthroughOp;
    let mut root = RootAdapter::new(None);
    pull_step_rows(node, src, &mut op, &mut root, estate).map(Some)
}

// ===========================================================================
// Increment 1 — the 9 delegation shapes (contract §5 Stage 1).
// ===========================================================================

/// SubqueryScan as a delegation leaf (class 10 REUSE, §1): `next_row` runs
/// the arm fallback's exact statement — `execscan::exec_scan` over the same
/// `SubqueryScanNode` (the subplan pulled Volcano inside `scan_next`).
/// Mechanism attribution vs the wave-4 streaming glue goes in the
/// EngineEvent detail string when WS-C's capture reaches this chokepoint.
struct SubqueryScanTailSource;

impl<'mcx> RowSource<'mcx> for SubqueryScanTailSource {
    type Node = crate::procnode::SubqueryScanNode<'mcx>;

    fn next_row(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>> {
        ::execscan::exec_scan(node, estate)
    }
}

/// Tail fallback for `try_own_subquery_scan` (lanev2.rs) — called ONLY after
/// the wave-4 streaming glue refused; never hooked from procnode directly.
#[inline]
pub(super) fn try_own_subquery_scan_tail<'mcx>(
    s: &mut crate::procnode::SubqueryScanNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    drive(ShapeClass::SubqueryScan, s, &mut SubqueryScanTailSource, estate)
}

/// FunctionScan as a delegation leaf: `exec_function_scan` (SRF
/// materialize/value-per-call protocols run inside it, state node-resident).
struct FunctionScanSource;

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
    drive(ShapeClass::FunctionScan, &mut **fs, &mut FunctionScanSource, estate)
}

/// TableFuncScan (XMLTABLE/JSON_TABLE) as a delegation leaf.
struct TableFuncScanSource;

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
    drive(ShapeClass::TableFuncScan, &mut **ts, &mut TableFuncScanSource, estate)
}

/// ValuesScan as a delegation leaf (per-row expression-list evaluation in
/// its own per-value context, all inside the ported body).
struct ValuesScanSource;

impl<'mcx> RowSource<'mcx> for ValuesScanSource {
    type Node = ::nodevaluesscan::ValuesScanState<'mcx>;

    fn next_row(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>> {
        ::nodevaluesscan::exec_values_scan(node, estate)
    }
}

#[inline]
pub fn try_own_values_scan<'mcx>(
    vs: &mut ::mcx::PgBox<'mcx, ::nodevaluesscan::ValuesScanState<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    drive(ShapeClass::ValuesScan, &mut **vs, &mut ValuesScanSource, estate)
}

/// SampleScan as a delegation leaf (TSM method calls stay inside the ported
/// body; the EPQ arm inside `exec_sample_scan` is unreachable through this
/// hosting — the Epq gate refused first — and delegated verbatim anyway).
struct SampleScanSource;

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
    drive(ShapeClass::SampleScan, &mut **ss, &mut SampleScanSource, estate)
}

/// TidScan as a delegation leaf (`WHERE ctid = ...` / `= ANY(...)` /
/// CURRENT OF; the tid-list build + heap fetches stay in the ported body).
struct TidScanSource;

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
    drive(ShapeClass::TidScan, ts, &mut TidScanSource, estate)
}

/// TidRangeScan as a delegation leaf (ctid range bounds inside the body).
struct TidRangeScanSource;

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
    drive(ShapeClass::TidRangeScan, ts, &mut TidRangeScanSource, estate)
}

/// NamedTuplestoreScan (AFTER-trigger transition tables) as a delegation
/// leaf. The mutation that PRODUCES the named store is out of lane scope
/// (dualexec-strict cannot dual-execute it); the read leg proves via the
/// serial e2e (contract cross-cutting law: honest-gap flag at boards).
struct NamedTuplestoreScanSource;

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
    drive(ShapeClass::NamedTuplestoreScan, &mut **nts, &mut NamedTuplestoreScanSource, estate)
}

/// Material as a delegation leaf: `next_row` runs the arm fallback's exact
/// statements (`exec_material` over the node's own tuplestore + Volcano
/// child). The mark/restore protocol (`exec_material_mark_pos` /
/// `exec_material_restr_pos` — the MergeJoin ExtraMarks cadence) enters
/// through `execami` DIRECTLY on the node and never crosses this hosting;
/// the mergejoin-over-material corpus leg (BLOCKING for inc-1 boards,
/// contract §6-WS-L(4)) proves the composition with both knobs on.
struct MaterialSource;

impl<'mcx> RowSource<'mcx> for MaterialSource {
    type Node = crate::procnode::MaterialNode<'mcx>;

    fn next_row(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>> {
        ::nodematerial::exec_material(&mut node.state, &mut *node.outer, estate)
    }
}

#[inline]
pub fn try_own_material<'mcx>(
    m: &mut ::mcx::PgBox<'mcx, crate::procnode::MaterialNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    drive(ShapeClass::Material, &mut **m, &mut MaterialSource, estate)
}

// ===========================================================================
// Increment 2 — the recursive-CTE machinery + Memoize (contract §5 Stage 2;
// iteration protocol + shared-slot law: docs/design/rowmode-tail.md §3).
// ===========================================================================

/// CteScan as a delegation leaf: `exec_cte_scan` resolves the CTE-shared
/// tuplestore state per call (take-use-put-back inside the ported body —
/// the shared-slot law holds because this source holds NOTHING).
struct CteScanSource;

impl<'mcx> RowSource<'mcx> for CteScanSource {
    type Node = ::nodectescan::CteScanState<'mcx>;

    fn next_row(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>> {
        ::nodectescan::exec_cte_scan(node, estate)
    }
}

#[inline]
pub fn try_own_cte_scan<'mcx>(
    cs: &mut ::mcx::PgBox<'mcx, ::nodectescan::CteScanState<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    drive(ShapeClass::CteScan, &mut **cs, &mut CteScanSource, estate)
}

/// RecursiveUnion as a delegation leaf. The whole iteration protocol —
/// working/intermediate table swap, dedup hash, and the `WorkTableShared`
/// TAKE/PUT around every child call (so descendant WorkTableScans reach it)
/// — is `exec_recursive_union`'s own body; both children stay Volcano
/// inside it. A tail-owned WorkTableScan in the recursive term is an
/// INDEPENDENT nested single-pull drive (no shared driver state — the
/// nested-drive validation of the inc-2 corpus).
struct RecursiveUnionSource;

impl<'mcx> RowSource<'mcx> for RecursiveUnionSource {
    type Node = crate::procnode::RecursiveUnionNode<'mcx>;

    fn next_row(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>> {
        let crate::procnode::RecursiveUnionNode { state, outer, inner } = node;
        ::noderecursiveunion::exec_recursive_union(state, outer, inner, estate)
    }
}

#[inline]
pub fn try_own_recursive_union<'mcx>(
    ru: &mut ::mcx::PgBox<'mcx, crate::procnode::RecursiveUnionNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    drive(ShapeClass::RecursiveUnion, &mut **ru, &mut RecursiveUnionSource, estate)
}

/// WorkTableScan as a delegation leaf: `exec_work_table_scan` resolves its
/// `rustate` from `estate.worktable_shared_slot(wtParam)` per call (the
/// entry its RecursiveUnion put back before pulling — shared-slot law).
struct WorkTableScanSource;

impl<'mcx> RowSource<'mcx> for WorkTableScanSource {
    type Node = ::nodeworktablescan::WorkTableScanState<'mcx>;

    fn next_row(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>> {
        ::nodeworktablescan::exec_work_table_scan(node, estate)
    }
}

#[inline]
pub fn try_own_work_table_scan<'mcx>(
    wts: &mut ::mcx::PgBox<'mcx, ::nodeworktablescan::WorkTableScanState<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    drive(ShapeClass::WorkTableScan, &mut **wts, &mut WorkTableScanSource, estate)
}

/// Memoize as a delegation leaf (the WS-L OQ ruling: the delegation leaf
/// satisfies the wave-2 charter; lane-owned-child composition is a ledgered
/// later increment). `next_row` replays `memoize_arm`'s fallback statements
/// exactly: rebuild the `MemoizeOuter` view (deferred child-rescan replay —
/// C's outerPlan->chgParam cadence) fresh per call, then `exec_memoize`.
struct MemoizeSource;

impl<'mcx> RowSource<'mcx> for MemoizeSource {
    type Node = crate::procnode::MemoizeNode<'mcx>;

    fn next_row(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>> {
        let plan = node.state.plan.plan.lefttree.expect("Memoize outer plan");
        let mut outer = crate::procnode::MemoizeOuter {
            node: &mut node.outer,
            plan,
            chg: &mut node.outer_chg,
        };
        ::nodememoize::exec_memoize(&mut node.state, &mut outer, estate)
    }
}

#[inline]
pub fn try_own_memoize<'mcx>(
    m: &mut ::mcx::PgBox<'mcx, crate::procnode::MemoizeNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    drive(ShapeClass::Memoize, &mut **m, &mut MemoizeSource, estate)
}
