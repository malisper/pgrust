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

use std::sync::atomic::{AtomicU8, Ordering::Relaxed};

use ::executils::{EStateData, ExecSlotId};
use ::types_error::PgResult;

use super::push::{pull_step_rows, OpStatus, RootAdapter, RowSource, Sink, SinkFeed, TupleOp};
use super::stats::{self, RefuseReason, ShapeClass};

/// `PGRUST_LANE_V2_ROWMODE` (default OFF): the Phase-0 row-mode facility
/// gate. 0 = unresolved (read env on first use), 1 = OFF, 2 = ON. An
/// AtomicU8 rather than the OnceLock idiom so the seams unit tests can A/B
/// both paths in one process (`rowmode_set_for_tests`); env-var (not GUC)
/// per the standing `pg_settings` byte-identity discipline (lanev2 module
/// doc).
static ROWMODE: AtomicU8 = AtomicU8::new(0);

fn rowmode_enabled() -> bool {
    match ROWMODE.load(Relaxed) {
        1 => false,
        2 => true,
        _ => {
            let on = matches!(
                std::env::var("PGRUST_LANE_V2_ROWMODE").as_deref(),
                Ok("1") | Ok("on")
            );
            ROWMODE.store(if on { 2 } else { 1 }, Relaxed);
            on
        }
    }
}

/// Same-process A/B lever for the unit corpus (`crate::tests`).
#[cfg(test)]
pub(crate) fn rowmode_set_for_tests(on: bool) {
    ROWMODE.store(if on { 2 } else { 1 }, Relaxed);
}

/// Test-only engagement probe: owned row-mode drives, per pull (the unit
/// corpus asserts the ON arm actually engaged — stats.rs ticks arm only via
/// the process-global `PGRUST_LANE_V2_STATS` env, unusable per-test).
#[cfg(test)]
pub(crate) static ROWMODE_OWNED_FOR_TESTS: std::sync::atomic::AtomicU64 =
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
    if !rowmode_enabled() {
        // Knob OFF: the documented wholesale refuse (lanev2.rs ProjectSet
        // section), tick-for-tick as before the rowmode facility landed.
        stats::tick_refused(ShapeClass::ProjectSet, RefuseReason::SrfSetExpansion);
        return Ok(None);
    }
    // Dynamic per-call gates (the try_own_result cadence).
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::ProjectSet, RefuseReason::Epq);
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(ShapeClass::ProjectSet, RefuseReason::Backward);
        return Ok(None);
    }
    if !estate.es_instrumentation.is_empty() {
        stats::tick_refused(ShapeClass::ProjectSet, RefuseReason::Instrumented);
        return Ok(None);
    }
    let ps = &mut **ps;
    // Increment-1 admits ONLY the childless-Result child (SELECT srf(...)
    // with no FROM). Everything else — scans, Sort, a Result over a child —
    // stays on the Volcano body (ledger: ProjectSet-over-Sort).
    {
        let crate::procnode::PlanStateNode::Result(r) = &mut *ps.outer else {
            stats::tick_refused(ShapeClass::ProjectSet, RefuseReason::ChildNotLaneOwned);
            return Ok(None);
        };
        if r.outer.is_some() {
            stats::tick_refused(ShapeClass::ProjectSet, RefuseReason::ChildNotLaneOwned);
            return Ok(None);
        }
    }
    stats::tick_owned(ShapeClass::ProjectSet);
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
