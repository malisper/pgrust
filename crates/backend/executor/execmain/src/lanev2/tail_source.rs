//! Tier-3 SOURCE-FORM hosting of the tail leaf shapes — wave-3 WS-Q
//! (`se/wave3-refusals`; wave-3 contract §6.Q; worklog
//! notes/se-ws-q-refusals.md).
//!
//! Where wave-2's rowmode_tail.rs hosts these shapes as pull-DELEGATION
//! leaves (`RowSource → PassthroughOp → RootAdapter` under
//! `pull_step_rows`), this module hosts the six Tier-2 tail LEAF shapes —
//! FunctionScan, TableFuncScan, SampleScan, TidScan, TidRangeScan,
//! NamedTuplestoreScan — as row-mode SOURCES: a `push::Source`/`Operator`
//! pair producing BATCH-SIZE-1 batches whose one staged row is the ported
//! exec body's own emitted slot. Exact PG per-row semantics BY CONSTRUCTION:
//! `produce` runs the identical `next_row` statement the delegation leaf
//! runs (one ported exec-body call per row — same qual/projection/interrupt
//! cadence, same errors at the same rows, zero lane-held cross-call state),
//! and the one-row batch is fully consumed within the producing driver
//! round (the IndexOnlyScan one-row-batch precedent), so nothing survives a
//! Volcano call boundary that the node's own C state does not.
//!
//! WHY a source form when delegation already exists (contract §6.Q):
//! source-form hosts (a) attribute as lane-DISPATCHED drives in the census
//! and (b) count as lane-owned CHILDREN — `t3_sort_child_admit` below lets
//! the sort breaker's memoized child verdict admit these shapes, which
//! transitively unlocks the pervasive `child-not-lane-owned` refusals in
//! every host that composes over the sort breaker (bare Sort,
//! Limit/Unique-over-sort, and the wave-4 Group / Result / SubqueryScan
//! streaming glue — all of them consume `sort_lane_fusible_memo`, so ONE
//! child-verdict extension unlocks the whole family; that IS the §6.Q
//! inc-final composition).
//!
//! **SubqueryScan stays delegation** (contract §6.Q: its win is pure
//! composition over now-lane-owned children, not a new source). ValuesScan
//! and the other wave-2 tail shapes stay delegation-only — out of the T3
//! candidate set, and keeping them off this path keeps the m4 insert/batch
//! instruction letters byte-identical (the VALUES arm never reads the T3
//! knob).
//!
//! Knobs (contract §2.1, all default OFF; registered in
//! notes/se-phase0-integration.md):
//!   * `PGRUST_LANE_V2_SCANS_T3` — the facility gate. OFF-first LAW: the
//!     cached-bool check runs BEFORE any other work on every path into this
//!     module; knob-OFF ticks NOTHING and adds one relaxed byte load +
//!     branch to the six shapes' dispatch arms only.
//!   * `PGRUST_LANE_V2_SCANS_T3_<SHAPE>=0` — per-shape force-off spellings
//!     (`FUNCTIONSCAN`, `TABLEFUNCSCAN`, `SAMPLESCAN`, `TIDSCAN`,
//!     `TIDRANGESCAN`, `NAMEDTUPLESTORESCAN`); the wave-2 P1-analog RULE:
//!     these exist in code BEFORE any per-shape fleet A/B is run. A
//!     forced-off shape under an armed facility knob ticks `env-off` (the
//!     metaagg `=0`-disarm precedent) and falls through to the delegation
//!     form / Volcano unchanged.
//!
//! Delegation-form hosting under `PGRUST_LANE_V2_ROWMODE` is NOT re-gated
//! (contract §2.1): both knobs are independent; when both arm, the source
//! form takes priority (rowmode_tail.rs try_own_* order) and delegation is
//! the rollback semantics.
//!
//! Gate order (host template, contract §4.4): knob (OFF = `Ok(None)`, ticks
//! nothing) → per-shape arm (force-off ticks `env-off` through the
//! `#[cold]` tail) → `es_epq_active` → backward → instrumented (each
//! refusal through the `#[cold]` tail) → `tick_owned` ONCE at the verdict
//! chokepoint → the one-row-batch drive. EPQ law §4.2: `es_epq_active` is
//! the first dynamic refusal after the knob gates.
//!
//! Accounting: the six classes reuse their frozen wave-2 `ShapeClass` rows
//! (contract §1.2: ZERO new classes) and the existing epq/backward/
//! instrumented/env-off reasons (+ detail strings) — the WS-Q inc-0
//! decision is **NO `SourceShape` mint**; `N_REASONS` stays 36 and stats.rs
//! is untouched (the §1.2 carrier duty is vacuous). Knob-ON, a dynamically
//! refused offer may tick the same (class, reason) twice per pull — once
//! here, once in the delegation tail (two mechanisms, two offers; the
//! SubqueryScan glue+tail precedent, documented in lane-gates.allowlist).
//! Mechanism attribution is the lane trace detail string ("scans-t3: ..."),
//! never a second class.

use std::sync::atomic::{AtomicU8, Ordering::Relaxed};

use ::executils::{EStateData, ExecSlotId};
use ::types_error::PgResult;

use super::push::{
    Batch, Operator, OpStatus, RootAdapter, RowSource, Sink, SinkFeed, Source, pull_step,
};
use super::stats::{self, RefuseReason, ShapeClass};

// ===========================================================================
// Knobs.
// ===========================================================================

/// `PGRUST_LANE_V2_SCANS_T3` (default OFF): the wave-3 source-form facility
/// gate. 0 = unresolved, 1 = OFF, 2 = ON — the rowmode.rs AtomicU8 idiom so
/// the unit corpus can A/B both arms in one process; env var (not GUC) per
/// the standing `pg_settings` byte-identity discipline (lanev2 module doc).
/// OFF-first: one relaxed byte load + compare on the fast path; the env
/// resolve is the `#[cold]` outlined tail (se2-cost lesson, LAW).
static SCANS_T3: AtomicU8 = AtomicU8::new(0);

#[inline]
pub(super) fn scans_t3_enabled() -> bool {
    match SCANS_T3.load(Relaxed) {
        1 => false,
        2 => true,
        _ => scans_t3_resolve(),
    }
}

#[cold]
#[inline(never)]
fn scans_t3_resolve() -> bool {
    let on = matches!(
        std::env::var("PGRUST_LANE_V2_SCANS_T3").as_deref(),
        Ok("1") | Ok("on")
    );
    SCANS_T3.store(if on { 2 } else { 1 }, Relaxed);
    on
}

/// Same-process A/B lever for the unit corpus (`crate::tests::scans_t3_ab`).
#[cfg(test)]
pub(crate) fn scans_t3_set_for_tests(on: bool) {
    SCANS_T3.store(if on { 2 } else { 1 }, Relaxed);
}

/// The six T3-hostable tail leaf shapes, in inc-0 population-rank order
/// (worklog: regress+dualexec grep census; the fleet coverage-counter
/// re-rank is a ledgered board-time rider). Discriminant = the per-shape
/// arm bit in `SCANS_T3_SHAPES`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum T3Shape {
    FunctionScan = 0,
    TableFuncScan = 1,
    SampleScan = 2,
    TidScan = 3,
    NamedTuplestoreScan = 4,
    TidRangeScan = 5,
}

impl T3Shape {
    const ALL: [T3Shape; 6] = [
        T3Shape::FunctionScan,
        T3Shape::TableFuncScan,
        T3Shape::SampleScan,
        T3Shape::TidScan,
        T3Shape::NamedTuplestoreScan,
        T3Shape::TidRangeScan,
    ];

    /// The frozen wave-2 vocabulary class this shape ticks (REUSE, never a
    /// new class — contract §1.2).
    fn class(self) -> ShapeClass {
        match self {
            T3Shape::FunctionScan => ShapeClass::FunctionScan,
            T3Shape::TableFuncScan => ShapeClass::TableFuncScan,
            T3Shape::SampleScan => ShapeClass::SampleScan,
            T3Shape::TidScan => ShapeClass::TidScan,
            T3Shape::NamedTuplestoreScan => ShapeClass::NamedTuplestoreScan,
            T3Shape::TidRangeScan => ShapeClass::TidRangeScan,
        }
    }

    /// The per-shape force-off env suffix (`PGRUST_LANE_V2_SCANS_T3_<THIS>`).
    fn env_suffix(self) -> &'static str {
        match self {
            T3Shape::FunctionScan => "FUNCTIONSCAN",
            T3Shape::TableFuncScan => "TABLEFUNCSCAN",
            T3Shape::SampleScan => "SAMPLESCAN",
            T3Shape::TidScan => "TIDSCAN",
            T3Shape::NamedTuplestoreScan => "NAMEDTUPLESTORESCAN",
            T3Shape::TidRangeScan => "TIDRANGESCAN",
        }
    }
}

/// Per-shape arm mask: bit `shape as u8` SET = the shape is armed (default;
/// cleared only by the `=0` force-off spelling); bit 7 = resolved marker.
/// One relaxed byte load on the armed path (the facility knob already
/// admitted), resolve `#[cold]`-outlined.
static SCANS_T3_SHAPES: AtomicU8 = AtomicU8::new(0);
const T3_RESOLVED: u8 = 0x80;

#[inline]
fn shape_armed(shape: T3Shape) -> bool {
    let m = SCANS_T3_SHAPES.load(Relaxed);
    if m & T3_RESOLVED != 0 {
        m & (1 << shape as u8) != 0
    } else {
        shapes_resolve() & (1 << shape as u8) != 0
    }
}

#[cold]
#[inline(never)]
fn shapes_resolve() -> u8 {
    let mut m = T3_RESOLVED;
    for s in T3Shape::ALL {
        let forced_off = matches!(
            std::env::var(format!("PGRUST_LANE_V2_SCANS_T3_{}", s.env_suffix())).as_deref(),
            Ok("0") | Ok("off")
        );
        if !forced_off {
            m |= 1 << s as u8;
        }
    }
    SCANS_T3_SHAPES.store(m, Relaxed);
    m
}

/// Same-process per-shape force-off lever for the unit corpus.
#[cfg(test)]
pub(crate) fn scans_t3_shape_set_for_tests(class_name: &str, armed: bool) {
    let shape = *T3Shape::ALL
        .iter()
        .find(|s| s.class().name() == class_name)
        .unwrap_or_else(|| panic!("not a T3 shape class name: {class_name}"));
    let mut m = SCANS_T3_SHAPES.load(Relaxed);
    if m & T3_RESOLVED == 0 {
        m = shapes_resolve();
    }
    if armed {
        m |= 1 << shape as u8;
    } else {
        m &= !(1 << shape as u8);
    }
    SCANS_T3_SHAPES.store(m | T3_RESOLVED, Relaxed);
}

// ===========================================================================
// Test-only engagement probes (the wave-2 probe-array pattern, contract
// §5.1): per-class counts of owned T3 SOURCE drives (standalone) and of T3
// sort-child admissions (composition), so the unit corpus asserts which
// MECHANISM engaged — the delegation tail's probe array cannot move when
// the source form owned the pull.
// ===========================================================================

#[cfg(test)]
static T3_OWNED_FOR_TESTS: [std::sync::atomic::AtomicU64; stats::n_classes()] =
    [const { std::sync::atomic::AtomicU64::new(0) }; stats::n_classes()];

#[cfg(test)]
static T3_SORT_CHILD_FOR_TESTS: [std::sync::atomic::AtomicU64; stats::n_classes()] =
    [const { std::sync::atomic::AtomicU64::new(0) }; stats::n_classes()];

#[cfg(test)]
pub(crate) fn t3_owned_probe_for_tests(name: &str) -> u64 {
    let class = ShapeClass::ALL
        .iter()
        .find(|c| c.name() == name)
        .unwrap_or_else(|| panic!("unknown lane shape class name: {name}"));
    T3_OWNED_FOR_TESTS[*class as usize].load(Relaxed)
}

#[cfg(test)]
pub(crate) fn t3_sort_child_probe_for_tests(name: &str) -> u64 {
    let class = ShapeClass::ALL
        .iter()
        .find(|c| c.name() == name)
        .unwrap_or_else(|| panic!("unknown lane shape class name: {name}"));
    T3_SORT_CHILD_FOR_TESTS[*class as usize].load(Relaxed)
}

// ===========================================================================
// The batch-size-1 source pair: `T3Feed` (the pipeline NODE — the wrapper
// holding the borrowed exec node, the delegated `RowSource` body and the
// one staged slot; cross-STAGE state is node-resident per the push.rs
// doctrine, and nothing in it survives a PG pull: `staged` is always
// consumed within the producing driver round) + `T3Stage`/`T3Emit` (unit
// stage markers, reassembled per call — free).
// ===========================================================================

pub(super) struct T3Feed<'a, 'mcx, S: RowSource<'mcx>> {
    node: &'a mut S::Node,
    src: S,
    staged: Option<ExecSlotId>,
}

impl<'a, 'mcx, S: RowSource<'mcx>> T3Feed<'a, 'mcx, S> {
    #[inline]
    fn new(node: &'a mut S::Node, src: S) -> Self {
        T3Feed { node, src, staged: None }
    }
}

pub(super) struct T3Stage<'a, 'mcx, S: RowSource<'mcx>> {
    _p: std::marker::PhantomData<fn(&'a mut S::Node, &'mcx ())>,
}

impl<'a, 'mcx, S: RowSource<'mcx>> T3Stage<'a, 'mcx, S> {
    #[inline]
    fn new() -> Self {
        T3Stage { _p: std::marker::PhantomData }
    }
}

impl<'a, 'mcx, S: RowSource<'mcx>> Source<'mcx> for T3Stage<'a, 'mcx, S> {
    type Node = T3Feed<'a, 'mcx, S>;

    /// One ported exec-body call = one staged row (batch size 1). The body
    /// runs the delegation leaf's exact statement, so per-row semantics —
    /// qual/projection arms, interrupt cadence, mid-stream errors — are the
    /// Volcano arm's by construction.
    #[inline(always)]
    fn produce(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<Batch>> {
        debug_assert!(node.staged.is_none(), "T3 batch-size-1: prior row not consumed");
        match node.src.next_row(node.node, estate)? {
            Some(slot) => {
                node.staged = Some(slot);
                Ok(Some(Batch { n: 1 }))
            }
            None => Ok(None),
        }
    }
}

pub(super) struct T3Emit<'a, 'mcx, S: RowSource<'mcx>> {
    _p: std::marker::PhantomData<fn(&'a mut S::Node, &'mcx ())>,
}

impl<'a, 'mcx, S: RowSource<'mcx>> T3Emit<'a, 'mcx, S> {
    #[inline]
    fn new() -> Self {
        T3Emit { _p: std::marker::PhantomData }
    }
}

impl<'a, 'mcx, S: RowSource<'mcx>> Operator<'mcx> for T3Emit<'a, 'mcx, S> {
    type Node = T3Feed<'a, 'mcx, S>;

    /// One-row batches are always fully consumed within the producing
    /// driver round (`consume` takes the staged slot before the sink call),
    /// so this is honestly derived rather than statically `None` — it can
    /// only ever observe `None` at a driver round boundary.
    #[inline]
    fn pending(&self, node: &Self::Node) -> Option<Batch> {
        node.staged.is_some().then_some(Batch { n: 1 })
    }

    #[inline(always)]
    fn consume(
        &mut self,
        node: &mut Self::Node,
        batch: Batch,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        debug_assert_eq!(batch.n, 1);
        let Some(slot) = node.staged.take() else {
            // Loud fail-closed PgError, never a panic (panicfix discipline):
            // reachable only through a driver-contract bug.
            return Err(Box::new(::types_error::PgError::error(
                "scans-t3 emit consumed a batch with no staged row".to_string(),
            )));
        };
        Ok(match out.accept(slot, estate)? {
            SinkFeed::Full => OpStatus::Paused,
            SinkFeed::NeedMore => OpStatus::NeedInput,
        })
    }
}

// ===========================================================================
// Standalone source-form drives (the per-shape try_own_*_t3 hooks, called
// FIRST by rowmode_tail.rs's try_own_* wrappers — source form is the
// upgrade; delegation under ROWMODE is the rollback semantics).
// ===========================================================================

/// Dynamic per-call gates in the fixed §4.4 order; refusal ticks go through
/// the `#[cold]` tail (WS-P Layer-A pattern) so the fall-through adds no
/// work beyond the compares.
#[inline]
fn t3_gates(class: ShapeClass, estate: &EStateData<'_>) -> bool {
    if estate.es_epq_active {
        t3_refuse(class, RefuseReason::Epq);
        return false;
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        t3_refuse(class, RefuseReason::Backward);
        return false;
    }
    if !estate.es_instrumentation.is_empty() {
        t3_refuse(class, RefuseReason::Instrumented);
        return false;
    }
    true
}

#[cold]
#[inline(never)]
fn t3_refuse(class: ShapeClass, reason: RefuseReason) {
    stats::tick_refused(class, reason);
}

/// The host-template drive: knob gates → dynamic gates → OWNED tick ONCE →
/// one `pull_step` round over the per-call-reassembled batch-size-1
/// pipeline. `RootAdapter::new(None)`: every delegated exec body runs its
/// own end-of-stream slot handling (the rowmode_tail `drive` argument,
/// verbatim).
#[inline]
fn t3_drive<'mcx, S: RowSource<'mcx>>(
    shape: T3Shape,
    node: &mut S::Node,
    src: S,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if !scans_t3_enabled() {
        return Ok(None);
    }
    let class = shape.class();
    if !shape_armed(shape) {
        // Per-shape kill switch under an armed facility knob: the metaagg
        // `=0`-disarm precedent — tick env-off (knob-ON-only accounting;
        // per-offered-pull cadence) and fall through unchanged.
        t3_refuse(class, RefuseReason::EnvOff);
        return Ok(None);
    }
    if !t3_gates(class, estate) {
        return Ok(None);
    }
    stats::tick_owned(class);
    if super::lane_trace_enabled() {
        super::lane_trace(&format!("scans-t3: {} source drive owned", class.name()));
    }
    #[cfg(test)]
    T3_OWNED_FOR_TESTS[class as usize].fetch_add(1, Relaxed);
    let mut feed = T3Feed::new(node, src);
    let mut stage = T3Stage::new();
    let mut emit = T3Emit::new();
    let mut root = RootAdapter::new(None);
    pull_step(&mut feed, &mut stage, &mut emit, &mut root, estate).map(Some)
}

#[inline]
pub(super) fn try_own_function_scan_t3<'mcx>(
    fs: &mut ::nodefunctionscan::FunctionScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    t3_drive(T3Shape::FunctionScan, fs, super::rowmode_tail::FunctionScanSource, estate)
}

#[inline]
pub(super) fn try_own_table_func_scan_t3<'mcx>(
    ts: &mut ::nodetablefuncscan::TableFuncScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    t3_drive(T3Shape::TableFuncScan, ts, super::rowmode_tail::TableFuncScanSource, estate)
}

#[inline]
pub(super) fn try_own_sample_scan_t3<'mcx>(
    ss: &mut ::nodesamplescan::SampleScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    t3_drive(T3Shape::SampleScan, ss, super::rowmode_tail::SampleScanSource, estate)
}

#[inline]
pub(super) fn try_own_tid_scan_t3<'mcx>(
    ts: &mut ::nodetidscan::TidScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    t3_drive(T3Shape::TidScan, ts, super::rowmode_tail::TidScanSource, estate)
}

#[inline]
pub(super) fn try_own_tid_range_scan_t3<'mcx>(
    ts: &mut ::nodetidrangescan::TidRangeScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    t3_drive(T3Shape::TidRangeScan, ts, super::rowmode_tail::TidRangeScanSource, estate)
}

#[inline]
pub(super) fn try_own_named_tuplestore_scan_t3<'mcx>(
    nts: &mut ::nodenamedtuplestorescan::NamedTuplestoreScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    t3_drive(
        T3Shape::NamedTuplestoreScan,
        nts,
        super::rowmode_tail::NamedTuplestoreScanSource,
        estate,
    )
}

// ===========================================================================
// inc-final COMPOSITION: T3 sources as lane-owned SORT children. One
// verdict extension (consumed by `sort_refuse_reason`) + one feed helper
// (consumed by the `sort_feed_if_needed` dispatch) unlock every host that
// composes over the sort breaker — bare Sort, Limit/Unique-over-sort, and
// the wave-4 Group / Result / SubqueryScan glue all share
// `sort_lane_fusible_memo`, whose `child-not-lane-owned` cascades this
// retires knob-ON.
// ===========================================================================

/// Structural sort-child verdict for the T3 shapes: `true` = this child is
/// a T3-armed tail leaf the sort breaker may treat as a lane-owned source.
/// INIT-STABLE by construction (node type + process-static knobs), so the
/// caller's memoization is sound; the dynamic EPQ/backward/instrumented
/// gates stay per-call at the driving hosts (an instrumented tree wraps
/// every node in the `Instrumented` variant, which matches no arm here).
/// Knob-OFF: one cached byte load, then `false` — the caller's verdict
/// (`NonScanChild` for these shapes) is byte-identical to wave-2.
///
/// OWNED tick: ONCE here, at the memoized-verdict chokepoint (§4.4; the
/// SeqScan memoized-verdict cadence) — the feed drive below never re-ticks,
/// and a forced-off or unknown shape ticks nothing (the caller's sortfeed
/// refusal accounting is unchanged).
pub(super) fn t3_sort_child_admit(child: &crate::procnode::PlanStateNode<'_>) -> bool {
    if !scans_t3_enabled() {
        return false;
    }
    let shape = match child {
        crate::procnode::PlanStateNode::FunctionScan(_) => T3Shape::FunctionScan,
        crate::procnode::PlanStateNode::TableFuncScan(_) => T3Shape::TableFuncScan,
        crate::procnode::PlanStateNode::SampleScan(_) => T3Shape::SampleScan,
        crate::procnode::PlanStateNode::TidScan(_) => T3Shape::TidScan,
        crate::procnode::PlanStateNode::TidRangeScan(_) => T3Shape::TidRangeScan,
        crate::procnode::PlanStateNode::NamedTuplestoreScan(_) => T3Shape::NamedTuplestoreScan,
        _ => return false,
    };
    if !shape_armed(shape) {
        return false;
    }
    let class = shape.class();
    stats::tick_owned(class);
    if super::lane_trace_enabled() {
        super::lane_trace(&format!("scans-t3: sort child admitted ({})", class.name()));
    }
    #[cfg(test)]
    T3_SORT_CHILD_FOR_TESTS[class as usize].fetch_add(1, Relaxed);
    true
}

/// Sort-breaker feed over a T3 source: the batch-size-1 pipeline drained
/// into the breaker sink by the shared `sort_feed` (same puts, same order,
/// same slot contents as C's `exec_sort` feed loop over this child — one
/// exec-body call per row, forward direction pinned by `sort_feed` exactly
/// as C's feed loop pins it). No topk cut / tie tracking / direct-key arm
/// (those read staged SoA lanes only scan feeds stage — the IndexScan-arm
/// posture, verbatim).
pub(super) fn t3_sort_feed<'mcx, S: RowSource<'mcx>>(
    sort: &mut ::nodesort::SortState<'mcx>,
    node: &mut S::Node,
    src: S,
    outer_desc: std::rc::Rc<::types_tuple::TupleDescData<'static>>,
    narrow_keys: Option<usize>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if super::lane_trace_enabled() {
        super::lane_trace("scans-t3: sort feed over T3 source");
    }
    let mut feed = T3Feed::new(node, src);
    super::sort_feed(
        sort,
        &mut feed,
        T3Stage::new(),
        T3Emit::new(),
        outer_desc,
        narrow_keys,
        estate,
        None,
        super::TieMode::Off,
    )
}
