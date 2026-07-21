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

use super::push::{pull_step_rows, OpStatus, RootAdapter, RowSource, Sink, SinkFeed, TupleOp};
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

/// `PGRUST_LANE_V2_MERGEJOIN` (default ON since wave-4 FLIP-2 — flip-ladder
/// rung 2; explicit `=0`/`off` is the permanent kill switch): the wave-2
/// knob-split gate for the WS-G MergeJoin row-mode hosting (contract §2 —
/// facility-level knob, granted; same AtomicU8 idiom as `ROWMODE` for the
/// same test-lever reason).
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
    // WAVE-4 FLIP-2 (flip-ladder rung 2; wave4-flip-manifest A2): default ON.
    // Only the default read changes; `=0`/`off` stays the permanent kill
    // switch (G4 restores today's bytes and ticks).
    let on = !matches!(
        std::env::var("PGRUST_LANE_V2_MERGEJOIN").as_deref(),
        Ok("0") | Ok("off")
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
    // Dynamic per-call gates (the try_own_result cadence). (The backward
    // gate retired with the backward-execution wave B11.)
    if estate.es_epq_active {
        refuse(estate, RefuseReason::Epq);
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

/// MergeJoin per-pull ownership VERDICT (se-delegtax SH-E; supersedes the
/// SH-A hosted drive, which was already statement-identical to the arm's
/// own fallback — `exec_merge_join` over the same FSM, both children
/// Volcano inside it, all cross-call state node-resident). Ownership of a
/// pure delegation leaf is an ACCOUNTING fact: the arm runs its single
/// fallback body either way (original Volcano tail-call shape, no
/// `Option<Option<..>>` sret round trip), and this function contributes the
/// decision side effects only — the dynamic gates (OR-folded, reason
/// re-derived on the cold tail; refusal set + cadence identical), the G7
/// EngineEvent capture, and the OWNED tick (once per pull, SH-B diag mask).
///
/// Knob order unchanged: `PGRUST_LANE_V2_MERGEJOIN` FIRST — knob-OFF ticks
/// NOTHING (no pre-existing MergeJoin wholesale refuse; contract §2d).
#[inline]
pub fn merge_join_pull_verdict<'mcx>(
    mj: &crate::procnode::MergeJoinNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) {
    if !mergejoin_enabled() {
        return;
    }
    // SH-F fast path (see rowmode_tail::verdict): the per-execution-static
    // byte + the per-pull-dynamic EPQ gate inline; byte==true means the
    // slow path would admit and tick nothing. (The backward compare retired
    // with the backward-execution wave B11.)
    if estate.es_lane_leaf_fast && !estate.es_epq_active {
        #[cfg(test)]
        ROWMODE_MJ_OWNED_FOR_TESTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return;
    }
    mj_verdict_slow(mj, estate);
}

/// The full MJ verdict (outlined; diagnostics/EPQ/backward/instrumented/
/// GUC-off pulls): the exact pre-SH-F body with the lane master GUC gate
/// at its head (it rode merge_join_arm's `enabled()` gate before SH-F).
#[inline(never)]
fn mj_verdict_slow<'mcx>(
    mj: &crate::procnode::MergeJoinNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) {
    if !super::enabled() {
        return;
    }
    // Dynamic per-call gates (the try_own_result cadence), OR-folded. (The
    // backward compare retired with the backward-execution wave B11.)
    if estate.es_epq_active || !estate.es_instrumentation.is_empty() {
        mj_gate_refused(mj, estate);
        return;
    }
    // Wave-4 pre-flip G7 capture (flip-ladder §2 rung-2 gate): the MJ ENGINE
    // capture at THE verdict chokepoint — `estate.engine_capture()` gated
    // (emission-gate law, zero records on default paths), dedup first-wins.
    if estate.engine_capture() {
        let id = mj.state.plan.join.plan.plan_node_id;
        super::engine_record_verdict(estate, id, ShapeClass::MergeJoin, None);
    }
    let diag = super::leaf_diag_mask();
    if diag != 0 {
        mj_diag_owned(diag, estate);
    }
    #[cfg(test)]
    ROWMODE_MJ_OWNED_FOR_TESTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

/// Cold refuse tail (SH-D): re-derive the first failing gate in the
/// original priority order (EPQ → instrumented; the backward arm retired
/// with the backward-execution wave B11), tick it, and record the G7
/// verdict under capture. Reached only when the OR-fold fired, so one of
/// the two holds.
#[cold]
#[inline(never)]
fn mj_gate_refused<'mcx>(
    mj: &crate::procnode::MergeJoinNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) {
    let r = if estate.es_epq_active {
        RefuseReason::Epq
    } else {
        RefuseReason::Instrumented
    };
    stats::tick_refused(ShapeClass::MergeJoin, r);
    if estate.engine_capture() {
        let id = mj.state.plan.join.plan.plan_node_id;
        super::engine_record_verdict(estate, id, ShapeClass::MergeJoin, Some(r));
    }
}

/// Cold owned-path diagnostics tail (SH-B): reached only when the diag mask
/// is nonzero (accounting or trace armed — never at default config).
/// GL-ROWMODE-1: the TRACE line (bit 1) is deduped to the first owned pull
/// per execution (tick cadence unchanged; rationale at
/// `lane_trace_owned_once` — the MergeJoin verdict is per output-row pull,
/// the same unbounded cadence as the tail leaves).
#[cold]
#[inline(never)]
fn mj_diag_owned(diag: u8, estate: &mut EStateData<'_>) {
    if diag & 1 != 0 {
        stats::tick_owned(ShapeClass::MergeJoin);
    }
    if diag & 2 != 0 {
        super::lane_trace_owned_once(ShapeClass::MergeJoin, estate, || {
            "mergejoin: row-mode drive owned".to_owned()
        });
    }
}
