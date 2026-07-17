//! WindowAgg lane hosting over the sort breaker — single-executor Phase 1,
//! WS-H inc-1 (contract §2c/§5; design worklog notes/se-ws-h-windows.md).
//!
//! Increment-1 admits the W1 class only: `WindowAgg(frameOptions ==
//! FRAMEOPTION_DEFAULTS)` over `Sort` over a lane-fusible child (the shared
//! `sort_lane_fusible_memo` verdict), every window function in {row_number,
//! rank, dense_rank, plain default-frame aggregates on the node's compiled
//! evaltrans}, no runCondition, no qual, non-EPQ, forward, no row marks,
//! first-pull-fresh node. Everything else refuses to the byte-identical
//! Volcano `exec_window_agg` path.
//!
//! Control shape: `pull_step_chain(sort, SortEmitSourceCfi, SortEmit,
//! WindowOp, RootAdapter)` — the try_own_group wiring over the sort
//! read-back, with `SortEmitSourceCfi` replaying the row engine's per-fetch
//! ExecSort entry CHECK_FOR_INTERRUPTS (exec_window_agg's spool loop enters
//! the child once per row). ALL window semantics live in the node crate's
//! seams (`nodewindowagg::lane`); the lane owns only control flow.
//!
//! STICKY OWNERSHIP: a partition-buffered lane drive holds cross-call state
//! `exec_window_agg` cannot resume (unlike Group/SortedAgg, whose node state
//! IS the Volcano state), so ownership is all-or-nothing per (re)scan: once
//! `w.lane` exists the lane drives unconditionally, and a dynamic-gate flip
//! (EPQ engaging mid-stream, a backward pull) raises a LOUD PgError — never
//! a silent wrong-results fallback. The flip is made unreachable by
//! construction through the STRUCTURAL row-marks gate (es_rowmarks non-empty
//! refuses admission — EPQ's substrate; ruled in the Phase-1 contract, WS-H
//! amendment 4); the loud tripwire stays as defense-in-depth, and a fired
//! tripwire in any test is a release blocker for the knob.
//!
//! Default OFF behind `PGRUST_LANE_V2_WINDOWS` (contract R-KNOBS §1): the
//! OFF path runs ZERO lane code and ticks NOTHING (no pre-existing WindowAgg
//! wholesale refuse exists, so default-config lane-gates accounting is
//! byte-identical by construction; floor seeding is flip-time work).

use std::sync::atomic::{AtomicU8, Ordering::Relaxed};

use ::executils::{EStateData, ExecSlotId};
use ::nodewindowagg::lane as wlane;
use ::types_error::{PgError, PgResult};

use super::push::{pull_step_chain, OpStatus, RootAdapter, Sink, SinkFeed, TupleOp};
use super::stats::{self, RefuseReason, ShapeClass};

/// `PGRUST_LANE_V2_WINDOWS` (default OFF): 0 = unresolved (read env on first
/// use), 1 = OFF, 2 = ON. AtomicU8 + set_for_tests so the unit corpus can
/// A/B both paths in one process (the rowmode idiom); env-var, not GUC, per
/// the standing `pg_settings` byte-identity discipline.
static WINDOWS: AtomicU8 = AtomicU8::new(0);

fn windows_enabled() -> bool {
    match WINDOWS.load(Relaxed) {
        1 => false,
        2 => true,
        _ => {
            let on = matches!(
                std::env::var("PGRUST_LANE_V2_WINDOWS").as_deref(),
                Ok("1") | Ok("on")
            );
            WINDOWS.store(if on { 2 } else { 1 }, Relaxed);
            on
        }
    }
}

/// Same-process A/B lever for the unit corpus (`crate::tests`).
#[cfg(test)]
pub(crate) fn windows_set_for_tests(on: bool) {
    WINDOWS.store(if on { 2 } else { 1 }, Relaxed);
}

/// Test-only engagement probe: owned window-lane pulls (stats ticks arm only
/// via the process-global `PGRUST_LANE_V2_STATS` env, unusable per-test).
#[cfg(test)]
pub(crate) static WINDOWS_OWNED_FOR_TESTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cold]
#[inline(never)]
fn sticky_tripwire(what: &str) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "lane-v2 windows: {what} flipped mid-stream on a lane-owned WindowAgg \
         (sticky-ownership tripwire; structurally unreachable — row-marks \
         plans refuse admission)"
    )))
}

/// The WindowAgg node as a mid-pipeline streaming operator over the sorted
/// emit: rows in, finalized-peer-group rows out. All semantics delegate to
/// the `nodewindowagg::lane` seams.
struct WindowOp<'a, 'mcx> {
    state: &'a mut ::nodewindowagg::WindowAggStateData<'mcx>,
    drive: &'a mut wlane::LaneWindowDrive,
}

impl<'mcx> WindowOp<'_, 'mcx> {
    /// Emit the finalized region into `out` until it drains (NeedInput) or
    /// the capacity-one root pauses the pipeline (Paused).
    fn emit_into(
        &mut self,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        while let Some(row) = wlane::lane_window_emit_next(self.state, self.drive, estate)? {
            if out.accept(row, estate)? == SinkFeed::Full {
                return Ok(OpStatus::Paused);
            }
        }
        Ok(OpStatus::NeedInput)
    }
}

impl<'mcx> TupleOp<'mcx> for WindowOp<'_, 'mcx> {
    fn pending(&self) -> bool {
        wlane::lane_window_emit_pending(self.drive)
    }

    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        match wlane::lane_window_accept(self.state, self.drive, estate, tuple)? {
            wlane::LaneAccept::NeedMore => Ok(OpStatus::NeedInput),
            // A finalized peer group awaits: emit its first row (the root
            // pauses per emitted row; the rest stream through resume).
            wlane::LaneAccept::GroupReady | wlane::LaneAccept::PartitionBoundary => {
                self.emit_into(out, estate)
            }
        }
    }

    fn resume(
        &mut self,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        debug_assert!(self.pending());
        self.emit_into(out, estate)
    }

    fn source_exhausted(
        &mut self,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        // Close the open group / partition, emit, then begin the parked
        // partition (if any) and repeat. Idempotent once drained: the seam
        // marks the node Done and keeps answering false.
        loop {
            if wlane::lane_window_emit_pending(self.drive)
                && self.emit_into(out, estate)? == OpStatus::Paused
            {
                return Ok(OpStatus::Paused);
            }
            if !wlane::lane_window_input_done(self.state, self.drive, estate)? {
                return Ok(OpStatus::Finished);
            }
        }
    }
}

/// EXPLAIN (ENGINE) capture at the memoized admission chokepoint (the
/// sort-verdict precedent): under ANALYZE the child is an `Instrumented`
/// wrapper, so an observed child refusal is a wrapper artifact — peel it and
/// report the production verdict (the E4 sort mirror for the child + the
/// init-stable window shape census). Touches neither the memo nor the stat
/// counters.
#[cold]
fn engine_capture_window_verdict<'mcx>(
    w: &mut crate::procnode::WindowAggNode<'mcx>,
    observed: Option<RefuseReason>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let id = wlane::lane_plan_node_id(&w.state);
    let production = match observed {
        Some(RefuseReason::ChildNotLaneOwned) => {
            let child = match &mut w.outer {
                crate::procnode::PlanStateNode::Instrumented(iw) => &mut iw.inner,
                o => o,
            };
            match child {
                crate::procnode::PlanStateNode::Sort(s) => {
                    match super::sort_refuse_reason_runtime_ea(s, estate)? {
                        Some(_) => Some(RefuseReason::ChildNotLaneOwned),
                        None if wlane::lane_window_shape_admissible(&w.state) => None,
                        None => Some(RefuseReason::ShapeQualProj),
                    }
                }
                _ => Some(RefuseReason::ChildNotLaneOwned),
            }
        }
        other => other,
    };
    super::engine_record_verdict(estate, id, ShapeClass::WindowAgg, production);
    Ok(())
}

/// Try to let the lane own a `WindowAgg` over the sort breaker. `Some` = the
/// lane drove this call; `None` = refused (the unchanged `exec_window_agg`
/// owns the node — and, because admission is decided before the row engine
/// ever runs the node, refusal is for the node's whole (re)scan life just
/// as ownership is).
#[inline]
pub fn try_own_window_agg<'mcx>(
    w: &mut ::mcx::PgBox<'mcx, crate::procnode::WindowAggNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if !windows_enabled() {
        // Knob OFF: zero lane code, zero ticks (no pre-existing WindowAgg
        // refuse class — default accounting stays byte-identical).
        return Ok(None);
    }
    let w = &mut **w;
    if w.lane.is_some() {
        // STICKY: the lane owns this node's whole (re)scan life. A dynamic
        // gate flipping here is structurally unreachable (row-marks gate) —
        // fail LOUD, never silently wrong (module doc).
        if estate.es_epq_active {
            return Err(sticky_tripwire("es_epq_active"));
        }
        if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
            return Err(sticky_tripwire("scan direction"));
        }
        return drive(w, estate).map(Some);
    }
    // Dynamic per-call gates, pre-ownership (the Group hook's cadence).
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::WindowAgg, RefuseReason::Epq);
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(ShapeClass::WindowAgg, RefuseReason::Backward);
        return Ok(None);
    }
    // Structural admission, memoized on the node; refusal accounting ticks
    // exactly here — once per memoized verdict (the sortfeed precedent).
    // Either verdict is final: fresh + admitted ⇒ the lane owns from THIS
    // pull (sticky); anything else ⇒ the row engine has (or will have)
    // driven this node, and a mid-life switch is unsound.
    let admit = match w.lane_admit {
        Some(v) => v,
        None => {
            let refuse = window_refuse_reason(w, estate)?;
            if estate.engine_capture() {
                engine_capture_window_verdict(w, refuse, estate)?;
            }
            if let Some(r) = refuse {
                stats::tick_refused(ShapeClass::WindowAgg, r);
            }
            let v = refuse.is_none();
            w.lane_admit = Some(v);
            v
        }
    };
    if !admit {
        return Ok(None);
    }
    // Feed-time dynamic refuse (the agg-over-join multi-batch spill arm of
    // `sort_feed_if_needed`) happens inside drive() BEFORE the drive is
    // created — see there.
    drive_first(w, estate)
}

/// Structural refuse-set for the W1 admission (init-stable + first-pull
/// freshness; reasons restricted to the frozen vocabulary — no new
/// RefuseReason, contract §2d). Row marks tick `Epq` (they are EPQ's
/// substrate — the structural gate that makes the sticky tripwire
/// unreachable); a non-fresh node also ticks `Epq` (the only way to observe
/// one is a prior EPQ/backward-refused pull that let the row engine drive).
fn window_refuse_reason<'mcx>(
    w: &mut crate::procnode::WindowAggNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<RefuseReason>> {
    if !estate.es_rowmarks.is_empty() {
        return Ok(Some(RefuseReason::Epq));
    }
    let crate::procnode::PlanStateNode::Sort(s) = &mut w.outer else {
        // Presorted-index window plans (no Sort) and instrumented trees
        // (EXPLAIN ANALYZE wraps every node) land here, like the Group hook.
        return Ok(Some(RefuseReason::ChildNotLaneOwned));
    };
    if !super::sort_lane_fusible_memo(s, estate)? {
        return Ok(Some(RefuseReason::ChildNotLaneOwned));
    }
    if !wlane::lane_window_shape_admissible(&w.state) {
        return Ok(Some(RefuseReason::ShapeQualProj));
    }
    if !wlane::lane_window_fresh(&w.state) {
        return Ok(Some(RefuseReason::Epq));
    }
    Ok(None)
}

/// First owned pull: run the sort feed (refusing ownership on its dynamic
/// feed-time refuse, before any window-side effect beyond the byte-inert
/// `all_first` flip), then create the sticky drive and stream.
fn drive_first<'mcx>(
    w: &mut crate::procnode::WindowAggNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // exec_window_agg's entry interrupt gate (conditional, C's macro).
    if ::init_small::globals::InterruptPending() {
        ::postgres_seams::check_for_interrupts::call()?;
    }
    // The all_first arm: for FRAMEOPTION_DEFAULTS this evaluates nothing
    // (no offsets) — flag flip + ecxt reset only, so a feed-time refuse
    // below still falls back byte-identically.
    wlane::lane_window_begin(&mut w.state, estate)?;
    {
        let crate::procnode::PlanStateNode::Sort(s) = &mut w.outer else {
            unreachable!("memoized window admission requires a Sort child")
        };
        let crate::procnode::SortNode { state: sstate, outer: souter, outer_desc, .. } = s;
        debug_assert!(!sstate.sort_done(), "fresh window node over a fed sort");
        if !super::sort_feed_if_needed(sstate, &mut **souter, outer_desc, None, estate)? {
            // Feed-time refuse before any lane tuple: the Volcano fallback
            // resumes byte-identically (sort_feed_if_needed's contract).
            return Ok(None);
        }
    }
    // One OWNED tick per drive start (the Group cadence: the underlying
    // sort-feed event; a rescan re-feeds and re-ticks).
    stats::tick_owned(ShapeClass::WindowAgg);
    super::lane_trace("windows drive armed (W1 over sort breaker)");
    w.lane = Some(wlane::LaneWindowDrive::new(::init_small::globals::work_mem()));
    drive(w, estate).map(Some)
}

/// One owned pull: resume/stream through the chain driver.
fn drive<'mcx>(
    w: &mut crate::procnode::WindowAggNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    #[cfg(test)]
    WINDOWS_OWNED_FOR_TESTS.fetch_add(1, Relaxed);
    // exec_window_agg's entry interrupt gate + drained guard.
    if ::init_small::globals::InterruptPending() {
        ::postgres_seams::check_for_interrupts::call()?;
    }
    if wlane::lane_window_done(&w.state) {
        return Ok(None);
    }
    // Rescan re-entry: re-run the all_first arm and re-feed the sort (the
    // Group hook's shape; the WindowAgg rescan reset both flags).
    wlane::lane_window_begin(&mut w.state, estate)?;
    let crate::procnode::PlanStateNode::Sort(s) = &mut w.outer else {
        unreachable!("lane-owned WindowAgg lost its Sort child")
    };
    let crate::procnode::SortNode { state: sstate, outer: souter, outer_desc, .. } = s;
    if !sstate.sort_done() {
        // Post-rescan re-feed. A feed-time refuse here would strand the
        // sticky drive — but the refuse arm is the agg-over-join spill,
        // whose completed build the fallback cannot resume EITHER; fail
        // loud (it cannot fire: the feed refused or succeeded identically
        // on the first pull, and spill multiplicity is input-determined).
        if !super::sort_feed_if_needed(sstate, &mut **souter, outer_desc, None, estate)? {
            return Err(sticky_tripwire("sort feed verdict"));
        }
        stats::tick_owned(ShapeClass::WindowAgg);
    }
    let mut op = WindowOp {
        state: &mut w.state,
        drive: w.lane.as_mut().expect("sticky drive exists"),
    };
    let mut root = RootAdapter::new(None);
    pull_step_chain(
        sstate,
        &mut super::SortEmitSourceCfi,
        &mut super::SortEmit,
        &mut op,
        &mut root,
        estate,
    )
}
