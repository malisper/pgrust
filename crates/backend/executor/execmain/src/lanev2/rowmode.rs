//! Row-mode operator facility — single-executor migration Phase 0, item 0.5
//! (docs/design/single-executor-migration.md; contract + express-lane design
//! in docs/design/rowmode-operators.md).
//!
//! THE CONTRACT (ratified here, no trait change): `push::TupleOp` IS the
//! row-mode operator contract — accept one tuple, push 0..K; node-resident
//! `pending()`/`resume()` = suspend/resume across the PG pull boundary;
//! `source_exhausted()` = the post-input phase seam. What Phase 0 adds is the
//! missing row-mode LEAF (`push::RowSource`, singleton-batch producer) and
//! its driver (`push::pull_step_rows`), then proves the contract on the
//! smallest real pair the migration doc names: `ProjectSet ← childless
//! Result` — the no-FROM SRF-in-tlist shape (`SELECT generate_series(...)`).
//!
//! Byte-identity: every face below is a code move, not a rewrite. The
//! childless Result body is `noderesult::lane_result_childless_next` (the
//! same statements `exec_result` runs for an outer-less node); the ProjectSet
//! expansion is `nodeprojectset::LaneProjectSet`'s seams (the same
//! `exec_project_srf` body `exec_project_set` drives, reset cadence
//! replayed exactly — see the seam module doc there). All cross-call state
//! (`rs_checkqual`/`rs_done`, `pending_srf_tuples`/`args_valid`/`elemdone`/
//! `result_store`) is the node's own C state and the lane holds zero shadow
//! state, so a Volcano fallback at ANY PG call boundary (knob flipped
//! mid-query is impossible — the knob is process-static — but EPQ/backward
//! gates re-check per pull) resumes byte-safely.
//!
//! Default OFF behind `PGRUST_LANE_V2_ROWMODE` (contract R-KNOBS): the OFF
//! path ticks today's documented ProjectSet wholesale refuse
//! (`srf-set-expansion`) exactly as before — lane-gates accounting at default
//! config is unchanged — and `exec_project_set` runs as today. No floor file
//! moves while the knob is OFF; a default flip must reseed the ProjectSet /
//! Result floors (see notes/ws-e-rowmode-ledger.md).
//!
//! Phase 1 (WS-G) adds the second hosted shape: MergeJoin as a bare row-mode
//! LEAF (`MergeJoinRowSource` → `PassthroughOp` → `RootAdapter` under
//! `pull_step_rows`) — a pure delegation to the ported
//! `::nodemergejoin::exec_merge_join` FSM with both children Volcano-driven
//! inside it (see docs/design/mergejoin-decision.md and
//! notes/se-ws-g-mergejoin.md). Unlike ProjectSet, MergeJoin has no
//! pre-existing wholesale refuse, so the knob-OFF path here ticks NOTHING
//! (default accounting byte-identical by construction, `mergejoin` class
//! silent at default config per §2d).
//!
//! Wave 2 (WS-L, the knob-split commit ruled in the wave-2 integration
//! contract §2): MergeJoin hosting moves OUT of `PGRUST_LANE_V2_ROWMODE`
//! behind its own `PGRUST_LANE_V2_MERGEJOIN` gate. This adjudicates WS-P OQ5
//! and unblocks flip-ladder rung 3 (the 2026-07-12 microbench measured a
//! 1.10x kernel-less-admission tax on merge-join-agg, the WS-G L1
//! flip-blocker — the split lets the tail flip without shipping that tax).
//! `PGRUST_LANE_V2_ROWMODE` keeps ProjectSet + the wave-2 16-shape
//! delegation tail (rowmode_tail.rs); no per-shape sub-knobs within the
//! tail (per-shape bisect is test-side only, the
//! `ROWMODE_TAIL_OWNED_FOR_TESTS` probe array).

use std::sync::atomic::{AtomicU8, Ordering::Relaxed};

use ::executils::{EStateData, ExecSlotId};
use ::types_error::PgResult;

use super::push::{
    pull_step_rows, OpStatus, PassthroughOp, RootAdapter, RowSource, Sink, SinkFeed, TupleOp,
};
use super::stats::{self, RefuseReason, ShapeClass};

/// `PGRUST_LANE_V2_ROWMODE` (default ON since wave-4 FLIP-1 — flip-ladder
/// rung 1; explicit `=0`/`off` is the permanent kill switch): the Phase-0
/// row-mode facility gate. 0 = unresolved (read env on first use), 1 = OFF,
/// 2 = ON. An AtomicU8 rather than the OnceLock idiom so the seams unit
/// tests can A/B both paths in one process (`rowmode_set_for_tests`);
/// env-var (not GUC) per the standing `pg_settings` byte-identity
/// discipline (lanev2 module doc).
static ROWMODE: AtomicU8 = AtomicU8::new(0);

/// `pub(super)` per the wave-2 contract §3.4 (visibility change owned by
/// WS-L): rowmode_tail.rs gates its 16 delegation shapes on the same knob.
///
/// se2-cost-fix: `#[inline]` + `#[cold]`-outlined resolve — the tail arms
/// call this once per PULL (values_scan_arm on a 100-row multi-VALUES
/// INSERT calls it 100x per statement), and the outlined call was +13
/// instr/row of the se2-dmlcost batch regression on the m4 pair. The fast
/// path must fold into the arm as one relaxed byte load + compare.
#[inline]
pub(super) fn rowmode_enabled() -> bool {
    match ROWMODE.load(Relaxed) {
        1 => false,
        2 => true,
        _ => rowmode_resolve(),
    }
}

#[cold]
#[inline(never)]
fn rowmode_resolve() -> bool {
    // WAVE-4 FLIP-1 (flip-ladder rung 1; wave4-flip-manifest A1): default ON.
    // The flip changes ONLY this default read — the explicit-OFF spelling
    // (`=0`/`off`) is permanent and restores today's bytes AND ticks (G4);
    // the knob itself never dies (flips never delete knobs).
    let on = !matches!(
        std::env::var("PGRUST_LANE_V2_ROWMODE").as_deref(),
        Ok("0") | Ok("off")
    );
    ROWMODE.store(if on { 2 } else { 1 }, Relaxed);
    on
}

/// `PGRUST_LANE_V2_MERGEJOIN` (default OFF): the wave-2 knob-split gate for
/// the WS-G MergeJoin row-mode hosting (contract §2 — facility-level knob,
/// granted; same AtomicU8 idiom as `ROWMODE` for the same test-lever
/// reason).
static MERGEJOIN: AtomicU8 = AtomicU8::new(0);

#[inline] // per-pull gate on the merge_join arm — same shape as rowmode_enabled
fn mergejoin_enabled() -> bool {
    match MERGEJOIN.load(Relaxed) {
        1 => false,
        2 => true,
        _ => mergejoin_resolve(),
    }
}

#[cold]
#[inline(never)]
fn mergejoin_resolve() -> bool {
    let on = matches!(
        std::env::var("PGRUST_LANE_V2_MERGEJOIN").as_deref(),
        Ok("1") | Ok("on")
    );
    MERGEJOIN.store(if on { 2 } else { 1 }, Relaxed);
    on
}

/// Same-process A/B lever for the unit corpus (`crate::tests`).
#[cfg(test)]
pub(crate) fn rowmode_set_for_tests(on: bool) {
    ROWMODE.store(if on { 2 } else { 1 }, Relaxed);
}

/// Same-process A/B lever for the mergejoin unit corpus (`crate::tests`).
#[cfg(test)]
pub(crate) fn mergejoin_set_for_tests(on: bool) {
    MERGEJOIN.store(if on { 2 } else { 1 }, Relaxed);
}

/// Test-only engagement probe: owned row-mode drives, per pull (the unit
/// corpus asserts the ON arm actually engaged — stats.rs ticks arm only via
/// the process-global `PGRUST_LANE_V2_STATS` env, unusable per-test).
#[cfg(test)]
pub(crate) static ROWMODE_OWNED_FOR_TESTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Test-only engagement probe for the MergeJoin row-mode hosting (WS-G):
/// owned drives, per pull. Separate from `ROWMODE_OWNED_FOR_TESTS` so the
/// mergejoin A/B corpus proves ITS shape engaged (not some other row-mode
/// hook on the same knob).
#[cfg(test)]
pub(crate) static ROWMODE_MJ_OWNED_FOR_TESTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// The childless Result plan as a row-mode source: delegates to
/// `noderesult::lane_result_childless_next`. `try_own_result`'s childless
/// arm carries an inline duplicate of the same statement stream (the
/// contract's pre-approved entry-cost fallback, se-entrycost) — the two
/// bodies must stay statement-identical.
struct ResultRowSource;

impl<'mcx> RowSource<'mcx> for ResultRowSource {
    type Node = crate::noderesult::ResultState<'mcx>;

    fn next_row(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>> {
        crate::noderesult::lane_result_childless_next(node, estate)
    }
}

/// ProjectSet as a row-mode expanding operator (the design doc's "SRFs = an
/// expanding operator" item): `pending` = C's own `pending_srf_tuples`; the
/// accept/resume bodies are `exec_project_set`'s own loop-body/continuing
/// arms behind the `LaneProjectSet` seams.
struct ProjectSetOp<'a, 'mcx> {
    ps: crate::nodeprojectset::LaneProjectSet<'a, 'mcx>,
}

impl<'mcx> TupleOp<'mcx> for ProjectSetOp<'_, 'mcx> {
    fn pending(&self) -> bool {
        self.ps.pending()
    }

    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        match self.ps.accept(estate, tuple)? {
            // Non-producing child row (all SRFs empty): the loop-bottom reset
            // ran inside the seam; feed the next child row.
            None => Ok(OpStatus::NeedInput),
            Some(row) => Ok(match out.accept(row, estate)? {
                SinkFeed::Full => OpStatus::Paused,
                SinkFeed::NeedMore => OpStatus::NeedInput,
            }),
        }
    }

    fn resume(
        &mut self,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        match self.ps.resume_expansion(estate)? {
            // Expansion done: C falls straight into its child-pull loop with
            // no intervening reset; NeedInput makes the driver do the same.
            None => Ok(OpStatus::NeedInput),
            Some(row) => Ok(match out.accept(row, estate)? {
                SinkFeed::Full => OpStatus::Paused,
                SinkFeed::NeedMore => OpStatus::NeedInput,
            }),
        }
    }
}

/// Try to let the row-mode lane own `ProjectSet ← Result(no child)` — the
/// no-FROM SRF-in-tlist shape. `None` = refused (the unchanged
/// `exec_project_set` drives the same node state byte-safely). Gates, in
/// order: the rowmode knob (OFF ticks today's wholesale `srf-set-expansion`
/// refuse unchanged), EPQ, backward, instrumented (the `try_own_result`
/// estate-gate pattern — the no-FROM composition has no scan child whose
/// Instrumented wrapper would break the match), child shape. Entry then
/// replays `exec_project_set`'s per-call prologue (CFI + entry per-tuple
/// reset — the latter is also C's continuing-call entry reset) before
/// driving `pull_step_rows`.
///
/// OWNED tick cadence: once per offered pull the row-mode drive owns (the
/// per-pull decision cadence of the index classes; stats.rs ProjectSet doc).
#[inline]
pub fn try_own_project_set<'mcx>(
    ps: &mut ::mcx::PgBox<'mcx, crate::nodeprojectset::ProjectSetState<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Wave-4 pre-flip G7 capture (flip-ladder §2): record the verdict at
    // this chokepoint under `estate.engine_capture()` only (the emission-
    // gate law — zero records on any default path; dedup first-wins keeps
    // the per-pull cadence honest).
    let capture_id =
        if estate.engine_capture() { Some(ps.ps.plan.plan_node_id) } else { None };
    let refuse = |estate: &mut EStateData<'mcx>, r: RefuseReason| {
        stats::tick_refused(ShapeClass::ProjectSet, r);
        if let Some(id) = capture_id {
            super::engine_record_verdict(estate, id, ShapeClass::ProjectSet, Some(r));
        }
    };
    if !rowmode_enabled() {
        // Knob OFF: the documented wholesale refuse (lanev2.rs ProjectSet
        // section), tick-for-tick as before the rowmode facility landed.
        refuse(estate, RefuseReason::SrfSetExpansion);
        return Ok(None);
    }
    // Dynamic per-call gates (the try_own_result cadence).
    if estate.es_epq_active {
        refuse(estate, RefuseReason::Epq);
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        refuse(estate, RefuseReason::Backward);
        return Ok(None);
    }
    if !estate.es_instrumentation.is_empty() {
        refuse(estate, RefuseReason::Instrumented);
        return Ok(None);
    }
    let ps = &mut **ps;
    // Increment-1 admits ONLY the childless-Result child (SELECT srf(...)
    // with no FROM). Everything else — scans, Sort, a Result over a child —
    // stays on the Volcano body (ledger: ProjectSet-over-Sort).
    {
        let crate::procnode::PlanStateNode::Result(r) = &mut *ps.outer else {
            refuse(estate, RefuseReason::ChildNotLaneOwned);
            return Ok(None);
        };
        if r.outer.is_some() {
            refuse(estate, RefuseReason::ChildNotLaneOwned);
            return Ok(None);
        }
    }
    stats::tick_owned(ShapeClass::ProjectSet);
    if let Some(id) = capture_id {
        super::engine_record_verdict(estate, id, ShapeClass::ProjectSet, None);
    }
    #[cfg(test)]
    ROWMODE_OWNED_FOR_TESTS.fetch_add(1, Relaxed);
    // exec_project_set's per-call prologue: entry CFI + entry per-tuple
    // reset. The reset frees the PREVIOUS pull's emitted-row by-ref datums
    // (the parent consumed them before pulling again) and doubles as C's
    // continuing-call entry reset; resume_expansion therefore does not reset.
    crate::cfi()?;
    let ecxt = ps.ps.ps_ExprContext.expect("ProjectSetState without ExprContext");
    estate.reset_expr_context(ecxt);
    let (view, outer) = crate::nodeprojectset::lane_project_set_split(ps);
    let crate::procnode::PlanStateNode::Result(rs) = &mut **outer else {
        unreachable!("matched above")
    };
    let mut op = ProjectSetOp { ps: view };
    // No clear-on-finish: exec_project_set returns end-of-set without
    // clearing the result slot.
    let mut root = RootAdapter::new(None);
    pull_step_rows(rs, &mut ResultRowSource, &mut op, &mut root, estate).map(Some)
}

/// MergeJoin as a row-mode LEAF (Phase-1 WS-G): one joined row per step;
/// both children stay Volcano-driven INSIDE the ported FSM — `next_row` runs
/// the identical statements `merge_join_arm`'s fallback runs (a pure
/// delegation to `::nodemergejoin::exec_merge_join`, zero changes to that
/// crate). All cross-call state is the FSM's own node-resident
/// `mj_JoinState`/slots (nodemergejoin lib.rs), including the mark/restore
/// protocol on the inner child (EXEC_MJ_SKIP_TEST marks, EXEC_MJ_TESTOUTER
/// restores — delegated to the child's `mark_pos`/`restr_pos` exactly as the
/// Volcano drive does), so a Volcano fallback at ANY pull boundary is
/// byte-safe by construction.
struct MergeJoinRowSource;

impl<'mcx> RowSource<'mcx> for MergeJoinRowSource {
    type Node = crate::procnode::MergeJoinNode<'mcx>;

    fn next_row(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>> {
        let crate::procnode::MergeJoinNode { state, outer, inner } = node;
        ::nodemergejoin::exec_merge_join(state, &mut **outer, &mut **inner, estate)
    }
}

/// Try to let the row-mode lane host a MergeJoin (both children Volcano).
/// `None` = refused; the caller runs the unchanged `exec_merge_join`.
///
/// Gates, in the `try_own_project_set` order: the `PGRUST_LANE_V2_MERGEJOIN`
/// knob FIRST (the wave-2 knob-split — see the module doc; before the split
/// this read the facility-wide rowmode knob) — and unlike ProjectSet,
/// knob-OFF ticks NOTHING (there is no pre-existing MergeJoin wholesale
/// refuse, so default-config accounting stays byte-identical trivially;
/// integration contract §2d) — then the dynamic per-call EPQ / backward /
/// instrumented gates. No shape gate: increment-1 admits every plan the FSM
/// itself admits (the hosting is jointype-agnostic delegation). No extra
/// prologue either: `exec_merge_join` runs its own entry CFI + per-tuple
/// reset as the FSM body's first statements, so the wrapper adds no calls
/// the Volcano drive would not make.
///
/// OWNED tick cadence: once per drive start (each owned PG pull starts one
/// `pull_step_rows` drive over the per-call-reassembled pipeline; the
/// stats.rs class doc restates this).
#[inline]
pub fn try_own_merge_join<'mcx>(
    mj: &mut crate::procnode::MergeJoinNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if !mergejoin_enabled() {
        return Ok(None);
    }
    // Wave-4 pre-flip G7 capture (flip-ladder §2 rung-2 gate): the MJ ENGINE
    // capture at THE verdict chokepoint — `estate.engine_capture()` gated
    // (emission-gate law, zero records on default paths), dedup first-wins.
    let capture_id =
        if estate.engine_capture() { Some(mj.state.plan.join.plan.plan_node_id) } else { None };
    let refuse = |estate: &mut EStateData<'mcx>, r: RefuseReason| {
        stats::tick_refused(ShapeClass::MergeJoin, r);
        if let Some(id) = capture_id {
            super::engine_record_verdict(estate, id, ShapeClass::MergeJoin, Some(r));
        }
    };
    // Dynamic per-call gates (the try_own_result cadence).
    if estate.es_epq_active {
        refuse(estate, RefuseReason::Epq);
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        refuse(estate, RefuseReason::Backward);
        return Ok(None);
    }
    if !estate.es_instrumentation.is_empty() {
        refuse(estate, RefuseReason::Instrumented);
        return Ok(None);
    }
    stats::tick_owned(ShapeClass::MergeJoin);
    if let Some(id) = capture_id {
        super::engine_record_verdict(estate, id, ShapeClass::MergeJoin, None);
    }
    super::lane_trace("mergejoin: row-mode drive owned");
    #[cfg(test)]
    ROWMODE_MJ_OWNED_FOR_TESTS.fetch_add(1, Relaxed);
    // No clear-on-finish: exec_merge_join returns end-of-join without
    // clearing the result slot.
    let mut op = PassthroughOp;
    let mut root = RootAdapter::new(None);
    pull_step_rows(mj, &mut MergeJoinRowSource, &mut op, &mut root, estate).map(Some)
}
