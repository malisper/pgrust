//! WindowAgg lane hosting over the sort breaker — single-executor Phase 1,
//! WS-H inc-1 (contract §2c/§5; design worklog notes/se-ws-h-windows.md).
//!
//! Increment-1 admits the W1 class only: `WindowAgg(frameOptions ==
//! FRAMEOPTION_DEFAULTS)` over `Sort` over a lane-fusible SCAN child (the
//! shared `sort_lane_fusible_memo` verdict, further restricted to scan
//! feeds — Agg-fed sorts refuse structurally because their feed carries
//! the lane's ONE dynamic feed-time refuse, which sticky ownership cannot
//! host; see `window_refuse_reason`), every window function in {row_number,
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
//!
//! WAVE 2 (WS-M): tier-2 windows behind the separate `PGRUST_LANE_V2_
//! WINDOWS_T2` knob (wave-2 contract §2 — a distinct flip rung from W1 with
//! independent gates). Increment-1 is **T2-A**: the WindowAgg node as a
//! row-mode delegation LEAF (`WindowAggRowSource` → `PassthroughOp` →
//! `RootAdapter` under `pull_step_rows` — the WS-G MergeJoin template),
//! `next_row` a pure delegation to the ported `exec_window_agg` with the
//! child Volcano-driven inside it. T2-A therefore admits EVERY WindowAgg
//! shape the row engine itself runs — explicit ROWS/RANGE/GROUPS frames
//! with all bound types, EXCLUDE, lead/lag/first/last/nth_value, FILTER,
//! runCondition/pass-through, multiple window defs (stacked WindowAgg
//! nodes host independently), inverse transitions — and inherits the row
//! engine's posture BY CONSTRUCTION (contract §6 WS-M amendment 3): all
//! cross-call state is the node's own `WindowAggStateData`, the lane holds
//! ZERO shadow state, so a Volcano fallback at ANY pull boundary (EPQ /
//! backward / instrumented per-pull gates) resumes byte-identically.
//! Unlike W1 there is NO sticky ownership and NO structural admission —
//! ownership is decided per pull (pull ≡ drive for row-mode hosts).
//!
//! T2 tick cadence (contract §3.3, adjudicating WS-M OQ5): the row-mode
//! law — OWNED once per owned PULL (the WS-G cadence). W1 keeps its sticky
//! batch-drive cadence — once per drive start. Both mechanisms REUSE
//! `ShapeClass::WindowAgg` (= 20; wave-2 vocab commit) — mechanism
//! attribution lives in the EngineEvent detail string ("w1-batch" vs
//! "t2-rowmode"), never a second class (contract §1). Corollary,
//! documented for the gate harnesses: with BOTH knobs ON, a pull refused
//! by both hooks (EPQ/backward) ticks the shared class ONCE PER HOOK; at
//! default config both knobs are OFF and the class stays silent.
//!
//! Hook order in `procnode::window_agg_arm`: W1 first (sticky owner wins
//! its admitted shapes), T2-A second on W1 refuse — so with both knobs ON,
//! W1 drives the default-frame batch lane and T2 hosts the refused
//! remainder; with only T2 ON the delegation hosts everything.
//!
//! WAVE 3 (WS-R): **T2-B** — the sealed FRAMED batch drive behind the
//! separate `PGRUST_LANE_V2_WINDOWS_T2B` knob (wave-3 contract §2.1/§6.R; an
//! independent flip rung; the T2-A delegation knob's behavior is unchanged
//! at both of its arms). T2-B generalizes the W1 machine to explicit frames
//! by driving the NODE'S OWN framed machinery over a lane-buffered partition
//! (`nodewindowagg::lane` T2-B section): same control shape as W1
//! (`pull_step_chain(sort, SortEmitSourceCfi, SortEmit, FramedWindowOp,
//! RootAdapter)`, sort-over-scan feeds only), STICKY ownership with the same
//! loud tripwire, structural admission = W1's child gates + the framed shape
//! seal (no runCondition, no qual — the pass-through family stays on T2-A).
//! Hook order becomes W1 → T2-B → T2-A: W1 keeps its default-frame shapes,
//! T2-B batch-hosts the framed (and W1-refused default-frame) remainder over
//! admitted sort feeds, T2-A delegation hosts everything else. Tick cadence:
//! T2-B is a sticky batch drive — OWNED once per drive start (the W1
//! cadence), refusals once per memoized verdict; the shared class stays
//! `ShapeClass::WindowAgg` with mechanism detail "t2b-framed" (contract §1.2:
//! zero new vocabulary).

use std::sync::atomic::{AtomicU8, Ordering::Relaxed};

use ::executils::{EStateData, ExecSlotId};
use ::nodewindowagg::lane as wlane;
use ::types_error::{PgError, PgResult};

use super::push::{
    pull_step_chain, pull_step_rows, OpStatus, PassthroughOp, RootAdapter, RowSource, Sink,
    SinkFeed, TupleOp,
};
use super::stats::{self, RefuseReason, ShapeClass};

/// `PGRUST_LANE_V2_WINDOWS` (default ON since wave-4 FLIP-3 — flip-ladder
/// rung 3; explicit `=0`/`off` is the permanent kill switch; `_T2`/`_T2B`
/// defaults unchanged): 0 = unresolved (read env on first use), 1 = OFF,
/// 2 = ON. AtomicU8 + set_for_tests so the unit corpus can A/B both paths
/// in one process (the rowmode idiom); env-var, not GUC, per the standing
/// `pg_settings` byte-identity discipline.
static WINDOWS: AtomicU8 = AtomicU8::new(0);

fn windows_enabled() -> bool {
    match WINDOWS.load(Relaxed) {
        1 => false,
        2 => true,
        _ => {
            // WAVE-4 FLIP-3 (flip-ladder rung 3; wave4-flip-manifest A3):
            // default ON — W1 only. `_T2`/`_T2B` defaults UNCHANGED (T2-A is
            // Tier-B, T2-B is Tier-C). `=0`/`off` stays the permanent kill
            // switch (G4 restores today's bytes and ticks).
            let on = !matches!(
                std::env::var("PGRUST_LANE_V2_WINDOWS").as_deref(),
                Ok("0") | Ok("off")
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

/// `PGRUST_LANE_V2_WINDOWS_T2` (default OFF): the wave-2 tier-2 windows
/// gate (contract §2 — WS-M's granted facility knob, independent flip rung
/// from W1). Same AtomicU8 + set_for_tests idiom for the same test-lever
/// reason; env-var, not GUC, per the standing `pg_settings` byte-identity
/// discipline.
static WINDOWS_T2: AtomicU8 = AtomicU8::new(0);

fn windows_t2_enabled() -> bool {
    match WINDOWS_T2.load(Relaxed) {
        1 => false,
        2 => true,
        _ => {
            let on = matches!(
                std::env::var("PGRUST_LANE_V2_WINDOWS_T2").as_deref(),
                Ok("1") | Ok("on")
            );
            WINDOWS_T2.store(if on { 2 } else { 1 }, Relaxed);
            on
        }
    }
}

/// Same-process A/B lever for the T2 unit corpus (`crate::tests`).
#[cfg(test)]
pub(crate) fn windows_t2_set_for_tests(on: bool) {
    WINDOWS_T2.store(if on { 2 } else { 1 }, Relaxed);
}

/// Test-only engagement probe for T2-A: owned row-mode window drives, per
/// pull. Separate from `WINDOWS_OWNED_FOR_TESTS` so the T2 corpus proves
/// ITS mechanism engaged (and the W1 corpus proves T2 did NOT hijack
/// W1-admitted shapes).
#[cfg(test)]
pub(crate) static WINDOWS_T2_OWNED_FOR_TESTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// `PGRUST_LANE_V2_WINDOWS_T2B` (default OFF): the wave-3 WS-R sealed framed
/// drive (contract §2.1 — its own flip rung; the T2-A delegation knob is
/// untouched at both arms). Same AtomicU8 + set_for_tests idiom; env-var,
/// not GUC, per the standing `pg_settings` byte-identity discipline.
/// OFF-first law: this single cached bool is read BEFORE any other work on
/// the hook path; the env resolve tail is one-shot.
static WINDOWS_T2B: AtomicU8 = AtomicU8::new(0);

fn windows_t2b_enabled() -> bool {
    match WINDOWS_T2B.load(Relaxed) {
        1 => false,
        2 => true,
        _ => windows_t2b_resolve(),
    }
}

#[cold]
#[inline(never)]
fn windows_t2b_resolve() -> bool {
    let on = matches!(
        std::env::var("PGRUST_LANE_V2_WINDOWS_T2B").as_deref(),
        Ok("1") | Ok("on")
    );
    WINDOWS_T2B.store(if on { 2 } else { 1 }, Relaxed);
    on
}

/// Same-process A/B lever for the T2-B unit corpus (`crate::tests`).
#[cfg(test)]
pub(crate) fn windows_t2b_set_for_tests(on: bool) {
    WINDOWS_T2B.store(if on { 2 } else { 1 }, Relaxed);
}

/// Test-only engagement probe for T2-B: owned framed batch drives, per
/// drive start (the sticky W1 cadence). Separate from both other probes so
/// the corpus proves mechanism attribution (and non-hijack) on all sides.
#[cfg(test)]
pub(crate) static WINDOWS_T2B_OWNED_FOR_TESTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[track_caller]
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
///
/// Wave-2 mechanism attribution (contract §6 WS-M amendment 2): the window
/// class is SHARED by the W1 batch drive and the T2-A row-mode delegation,
/// so the Lane verdict carries a detail string — "w1-batch" when W1's
/// production admission holds; "t2-rowmode" when W1 refuses but the T2 knob
/// is ON (T2-A admits every shape, so a W1 production refuse + T2-enabled ⇒
/// the lane still owns the node, via delegation). A refuse reason records
/// only when NO enabled window mechanism owns in production. Both hooks'
/// records dedup per (node, class), first wins — this composed chokepoint
/// runs first (W1 before T2 in the arm), so the composed verdict is the one
/// displayed.
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
    match production {
        None => estate.engine_record(
            id,
            ::executils::EngineKind::Lane,
            ShapeClass::WindowAgg.name(),
            "w1-batch",
        ),
        // WS-R T2-B (wave-3): a W1 SHAPE-census refuse over an admitted
        // (fusible-Sort) child is production-owned by the framed batch
        // drive when its knob is on and the framed seal admits — the hook
        // order (W1 → T2-B → T2-A) makes this the next verdict in line.
        // ChildNotLaneOwned refusals fall past T2-B too (same child gates).
        Some(RefuseReason::ShapeQualProj)
            if windows_t2b_enabled() && wlane::lane_framed_shape_admissible(&w.state) =>
        {
            engine_record_t2b_owned(estate, id)
        }
        Some(_) if windows_t2_enabled() => engine_record_t2_owned(estate, id),
        Some(r) => super::engine_record_verdict(estate, id, ShapeClass::WindowAgg, Some(r)),
    }
    Ok(())
}

/// The T2-B Lane record with its mechanism detail (shared by the composed
/// W1 chokepoint above and the T2-B hook's own memoized-verdict capture).
#[cold]
fn engine_record_t2b_owned(estate: &mut EStateData<'_>, plan_node_id: i32) {
    estate.engine_record(
        plan_node_id,
        ::executils::EngineKind::Lane,
        ShapeClass::WindowAgg.name(),
        "t2b-framed",
    );
}

/// The T2-A Lane record with its mechanism detail (shared by the composed
/// W1 chokepoint above and the T2 hook's own instrumented-refuse mirror).
#[cold]
fn engine_record_t2_owned(estate: &mut EStateData<'_>, plan_node_id: i32) {
    estate.engine_record(
        plan_node_id,
        ::executils::EngineKind::Lane,
        ShapeClass::WindowAgg.name(),
        "t2-rowmode",
    );
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
    // No feed-time dynamic refuse can follow: the admitted feeds are scan
    // feeds only (the dynamically-refusing agg-over-join family refuses
    // structurally above). drive_first still guards the refuse arm — and
    // flips the memo — as defense-in-depth; see there.
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
    // SCAN-FED SORTS ONLY (inc-1): an Agg-fed sort (the hash-agg breaker
    // composing under the sort) is the ONE feed family whose
    // `sort_feed_if_needed` can refuse DYNAMICALLY (the agg-over-join
    // multi-batch spill arm — and a chgParam rescan can flip a rebuilt
    // join's nbatch 1→N mid-life). Sticky ownership cannot host a dynamic
    // feed verdict: a feed-time refuse after admission would either strand
    // the memoized admit (the fixed inc-1 blocker: the next pull re-entered
    // drive_first over a row-engine-fed sort) or fire the rescan tripwire
    // on a query that succeeds knob-OFF. Refuse the whole family
    // structurally (plan-shape, init-stable ⇒ memoizable); every admitted
    // feed below is a scan feed, whose sort_feed_if_needed has NO refuse
    // arm — making the feed-refuse paths unreachable by construction.
    // Re-admission is ledgered (notes/se-ws-h-windows.md TODO 16). The
    // inner sort/agg lanes still engage on their own under the refused
    // window via the row engine's child pulls — nothing is forfeited but
    // the window computation itself.
    if matches!(&*s.outer, crate::procnode::PlanStateNode::Agg(_)) {
        return Ok(Some(RefuseReason::ChildNotLaneOwned));
    }
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
            // UNREACHABLE for the inc-1 admission (scan-fed sorts only —
            // no scan feed refuses; the dynamically-refusing agg-over-join
            // family is refused structurally in window_refuse_reason), but
            // kept live as defense-in-depth for future feed admissions:
            // the refusal MUST also flip the memo — a stranded admit=true
            // would re-enter this function on the next pull, over a sort
            // the row engine has by then fed, and hijack the mid-stream
            // WindowAgg (the fixed inc-1 blocker). Either verdict is final
            // (module doc): once the row engine drives, it drives for the
            // node's whole (re)scan life.
            w.lane_admit = Some(false);
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
        // sticky drive; it is unreachable BY CONSTRUCTION because inc-1
        // admission is scan-fed sorts only (window_refuse_reason refuses
        // the agg-fed family structurally) and no scan feed has a refuse
        // arm in sort_feed_if_needed. The one dynamic refuse — the
        // agg-over-join multi-batch spill, which a chgParam rescan CAN
        // flip 1→N on a rebuilt join — can therefore never reach a
        // lane-owned window. The loud tripwire stays as defense-in-depth
        // (a future feed admission that reintroduces a dynamic refuse must
        // ship a byte-safe rescan fallback design first).
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

// ===========================================================================
// Tier-2 (wave-2 WS-M inc-1): T2-A — WindowAgg as a row-mode delegation
// LEAF behind PGRUST_LANE_V2_WINDOWS_T2 (module doc).
// ===========================================================================

/// WindowAgg as a row-mode LEAF (the WS-G MergeJoin template): one window
/// output row per step; the child stays Volcano-driven INSIDE the ported
/// node body — `next_row` runs the identical statements `window_agg_arm`'s
/// fallback runs (a pure delegation to `::nodewindowagg::exec_window_agg`,
/// zero changes to that crate). ALL cross-call state — spool position,
/// partition buffer tuplestore, per-agg carriers, frame heads/tails,
/// runCondition pass-through modes — is the node's own
/// `WindowAggStateData`, so a Volcano fallback at ANY pull boundary is
/// byte-safe by construction.
struct WindowAggRowSource;

impl<'mcx> RowSource<'mcx> for WindowAggRowSource {
    type Node = crate::procnode::WindowAggNode<'mcx>;

    fn next_row(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>> {
        let crate::procnode::WindowAggNode { state, outer, .. } = node;
        ::nodewindowagg::exec_window_agg(state, estate, |e| {
            crate::procnode::exec_proc_node(outer, e)
        })
    }
}

/// Try to let the T2-A row-mode lane host a WindowAgg (child Volcano).
/// `None` = refused; the caller falls through (to the unchanged
/// `exec_window_agg` — `window_agg_arm` runs this hook SECOND, after W1, so
/// a W1-owned pull never reaches here).
///
/// Gates, exactly the wave-2 row-mode host template (contract §3.2, the
/// try_own_merge_join order): the `PGRUST_LANE_V2_WINDOWS_T2` knob FIRST —
/// knob-OFF runs zero lane code and ticks NOTHING (no pre-existing
/// wholesale refuse; default accounting byte-identical by construction,
/// §2d) — then the dynamic per-call EPQ / backward / instrumented gates.
/// NO shape gate: T2-A admits every plan the ported node body itself
/// admits (the hosting is frame-shape-agnostic delegation — that is the
/// tier-2 coverage claim), and NO stickiness: the delegation holds zero
/// shadow state, so refusal on one pull and ownership on the next compose
/// byte-identically. No extra prologue either: `exec_window_agg` runs its
/// own entry CFI + Done-guard as its first statements, so the wrapper adds
/// no calls the Volcano drive would not make.
///
/// OWNED tick cadence: once per owned pull (§3.3 row-mode law — pull ≡
/// drive; each owned PG pull starts one `pull_step_rows` drive over the
/// per-call-reassembled pipeline).
#[inline]
pub fn try_own_window_agg_t2<'mcx>(
    w: &mut ::mcx::PgBox<'mcx, crate::procnode::WindowAggNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if !windows_t2_enabled() {
        return Ok(None);
    }
    // Dynamic per-call gates (the row-mode template order). NB: with the W1
    // knob ALSO on, W1's hook has already ticked these reasons this pull —
    // the shared class counts once per HOOK by design (module doc; the
    // detail-string law keeps mechanism attribution out of the counters).
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::WindowAgg, RefuseReason::Epq);
        return Ok(None);
    }
    if !estate.es_instrumentation.is_empty() {
        stats::tick_refused(ShapeClass::WindowAgg, RefuseReason::Instrumented);
        // EXPLAIN (ENGINE) mirror (E4): ENGINE requires ANALYZE, so every
        // capture-run pull lands on this arm; with ONLY the instrument gate
        // vacated T2-A owns unconditionally — record the production Lane
        // verdict. Dedup (first record wins) defers to the composed W1
        // chokepoint when that knob is also on.
        if estate.engine_capture() {
            let id = wlane::lane_plan_node_id(&w.state);
            engine_record_t2_owned(estate, id);
        }
        return Ok(None);
    }
    stats::tick_owned(ShapeClass::WindowAgg);
    // GL-ROWMODE-1: owned-trace deduped to the first owned pull per
    // execution (this verdict is per output-row pull; rationale at
    // `lane_trace_owned_once`).
    super::lane_trace_owned_once(ShapeClass::WindowAgg, estate, || {
        "windows-t2 row-mode drive owned (T2-A delegation)".to_owned()
    });
    #[cfg(test)]
    WINDOWS_T2_OWNED_FOR_TESTS.fetch_add(1, Relaxed);
    // No clear-on-finish: exec_window_agg returns end-of-set without
    // clearing the result slot.
    let mut op = PassthroughOp;
    let mut root = RootAdapter::new(None);
    pull_step_rows(&mut **w, &mut WindowAggRowSource, &mut op, &mut root, estate).map(Some)
}

// ===========================================================================
// WS-R T2-B (wave-3 inc-2): the sealed FRAMED batch drive behind
// PGRUST_LANE_V2_WINDOWS_T2B (module doc WAVE 3 paragraph). The machine
// lives in `nodewindowagg::lane` (T2-B section); this host owns only the
// W1-template control flow: knob gate, dynamic gates, memoized structural
// admission, sticky drive + tripwires, the pull_step_chain over the sort
// read-back, and the EXPLAIN (ENGINE) chokepoint capture.
// ===========================================================================

/// The framed WindowAgg as a mid-pipeline batch operator over the sorted
/// emit: rows spool into the NODE's own partition buffer; a complete
/// partition emits through the node's own framed evaluation (all semantics
/// in `nodewindowagg`; the lane owns control flow only).
struct FramedWindowOp<'a, 'mcx> {
    state: &'a mut ::nodewindowagg::WindowAggStateData<'mcx>,
    drive: &'a mut wlane::LaneFramedDrive,
}

impl<'mcx> FramedWindowOp<'_, 'mcx> {
    /// Emit the spooled partition into `out` until it drains (NeedInput) or
    /// the capacity-one root pauses the pipeline (Paused).
    fn emit_into(
        &mut self,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        while let Some(row) = wlane::lane_framed_emit_next(self.state, self.drive, estate)? {
            if out.accept(row, estate)? == SinkFeed::Full {
                return Ok(OpStatus::Paused);
            }
        }
        Ok(OpStatus::NeedInput)
    }
}

impl<'mcx> TupleOp<'mcx> for FramedWindowOp<'_, 'mcx> {
    fn pending(&self) -> bool {
        wlane::lane_framed_emit_pending(self.drive)
    }

    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        match wlane::lane_framed_accept(self.state, self.drive, estate, tuple)? {
            wlane::LaneFramedAccept::NeedMore => Ok(OpStatus::NeedInput),
            // A complete partition awaits: emit its first row (the root
            // pauses per emitted row; the rest stream through resume).
            wlane::LaneFramedAccept::PartitionReady => self.emit_into(out, estate),
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
        // Close the open partition, emit, then begin the parked final
        // partition (if any) and repeat. Idempotent once drained: the seam
        // marks the node Done and keeps answering false.
        loop {
            if wlane::lane_framed_emit_pending(self.drive)
                && self.emit_into(out, estate)? == OpStatus::Paused
            {
                return Ok(OpStatus::Paused);
            }
            if !wlane::lane_framed_input_done(self.state, self.drive, estate)? {
                return Ok(OpStatus::Finished);
            }
        }
    }
}

/// EXPLAIN (ENGINE) capture at T2-B's memoized admission chokepoint — runs
/// only when the W1 hook's composed chokepoint did not (W1 knob off; dedup is
/// first-record-wins anyway). Same peel as the W1 mirror: under ANALYZE the
/// child is an `Instrumented` wrapper, so an observed child refusal is a
/// wrapper artifact — peel it and report the production verdict.
#[cold]
fn engine_capture_window_framed_verdict<'mcx>(
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
                    if matches!(&*s.outer, crate::procnode::PlanStateNode::Agg(_)) {
                        Some(RefuseReason::ChildNotLaneOwned)
                    } else {
                        match super::sort_refuse_reason_runtime_ea(s, estate)? {
                            Some(_) => Some(RefuseReason::ChildNotLaneOwned),
                            None if wlane::lane_framed_shape_admissible(&w.state) => None,
                            None => Some(RefuseReason::ShapeQualProj),
                        }
                    }
                }
                _ => Some(RefuseReason::ChildNotLaneOwned),
            }
        }
        other => other,
    };
    match production {
        None => engine_record_t2b_owned(estate, id),
        Some(_) if windows_t2_enabled() => engine_record_t2_owned(estate, id),
        Some(r) => super::engine_record_verdict(estate, id, ShapeClass::WindowAgg, Some(r)),
    }
    Ok(())
}

/// Try to let the T2-B framed batch lane own a `WindowAgg` over the sort
/// breaker. `Some` = the lane drove this call; `None` = refused (the caller
/// falls through — to T2-A when that knob is on, ultimately to the unchanged
/// `exec_window_agg`). `window_agg_arm` runs this hook SECOND, after W1 and
/// before T2-A, so a W1-owned pull never reaches here and a T2-B-owned pull
/// never reaches the delegation.
///
/// Template = `try_own_window_agg` verbatim (the sticky W1 law): knob FIRST
/// (OFF = zero lane code, zero ticks), sticky-drive tripwires, dynamic
/// per-call EPQ/backward gates, memoized structural admission with capture
/// + refusal accounting at the verdict chokepoint, then the sticky drive.
#[inline]
pub fn try_own_window_agg_t2b<'mcx>(
    w: &mut ::mcx::PgBox<'mcx, crate::procnode::WindowAggNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if !windows_t2b_enabled() {
        // Knob OFF: zero lane code, zero ticks (no pre-existing WindowAgg
        // refuse class — default accounting stays byte-identical).
        return Ok(None);
    }
    let w = &mut **w;
    if w.lane_framed.is_some() {
        // STICKY: the lane owns this node's whole (re)scan life. A dynamic
        // gate flipping here is structurally unreachable (row-marks gate) —
        // fail LOUD, never silently wrong (module doc).
        if estate.es_epq_active {
            return Err(sticky_tripwire("es_epq_active"));
        }
        if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
            return Err(sticky_tripwire("scan direction"));
        }
        return framed_drive(w, estate).map(Some);
    }
    // Dynamic per-call gates, pre-ownership. NB: with the W1 knob ALSO on,
    // W1's hook has already ticked these reasons this pull — the shared
    // class counts once per HOOK by design (module doc; mechanism
    // attribution rides the detail string, never the counters).
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::WindowAgg, RefuseReason::Epq);
        return Ok(None);
    }
    // Structural admission, memoized on the node (T2-B's own memo — W1's
    // verdict is a different census); refusal accounting ticks exactly here,
    // once per memoized verdict. Either verdict is final (the sticky law).
    let admit = match w.lane_framed_admit {
        Some(v) => v,
        None => {
            let refuse = window_framed_refuse_reason(w, estate)?;
            if estate.engine_capture() {
                engine_capture_window_framed_verdict(w, refuse, estate)?;
            }
            if let Some(r) = refuse {
                stats::tick_refused(ShapeClass::WindowAgg, r);
            }
            let v = refuse.is_none();
            w.lane_framed_admit = Some(v);
            v
        }
    };
    if !admit {
        return Ok(None);
    }
    framed_drive_first(w, estate)
}

/// Structural refuse-set for the T2-B framed admission: W1's child-side
/// gates verbatim (row marks = EPQ's substrate; Sort child; the agg-fed
/// dynamic-feed family refused structurally; the shared fusible-sort memo)
/// with the W1 function/frame census replaced by the framed seal (no
/// runCondition, no qual — pass-through family stays on T2-A delegation).
/// Reasons restricted to the frozen vocabulary (contract §1.2: no new
/// RefuseReason; ShapeQualProj carries the seal refusals).
fn window_framed_refuse_reason<'mcx>(
    w: &mut crate::procnode::WindowAggNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<RefuseReason>> {
    if !estate.es_rowmarks.is_empty() {
        return Ok(Some(RefuseReason::Epq));
    }
    let crate::procnode::PlanStateNode::Sort(s) = &mut w.outer else {
        // Presorted-index window plans (no Sort) and instrumented trees
        // (EXPLAIN ANALYZE wraps every node) land here, like W1.
        return Ok(Some(RefuseReason::ChildNotLaneOwned));
    };
    // SCAN-FED SORTS ONLY, exactly W1's law (see window_refuse_reason): the
    // agg-fed family carries the lane's ONE dynamic feed-time refuse, which
    // sticky ownership cannot host. Same structural refuse, same ledger
    // (notes/se-ws-h-windows.md TODO 16 covers re-admission for both).
    if matches!(&*s.outer, crate::procnode::PlanStateNode::Agg(_)) {
        return Ok(Some(RefuseReason::ChildNotLaneOwned));
    }
    if !super::sort_lane_fusible_memo(s, estate)? {
        return Ok(Some(RefuseReason::ChildNotLaneOwned));
    }
    if !wlane::lane_framed_shape_admissible(&w.state) {
        return Ok(Some(RefuseReason::ShapeQualProj));
    }
    if !wlane::lane_window_fresh(&w.state) {
        return Ok(Some(RefuseReason::Epq));
    }
    Ok(None)
}

/// First owned pull: run the sort feed (refusing ownership on its dynamic
/// feed-time refuse, before any window-side effect beyond the byte-inert
/// `all_first` arm), then create the sticky framed drive and stream.
fn framed_drive_first<'mcx>(
    w: &mut crate::procnode::WindowAggNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // exec_window_agg's entry interrupt gate (conditional, C's macro).
    if ::init_small::globals::InterruptPending() {
        ::postgres_seams::check_for_interrupts::call()?;
    }
    // The all_first arm: calculate_frame_offsets — for framed shapes this
    // EVALUATES the start/end offset expressions (unlike W1's no-offset
    // shapes), exactly exec_window_agg's entry order; a feed-time refuse
    // below still falls back byte-identically (the row engine's own entry
    // re-runs nothing: all_first flipped, offsets computed once either way).
    wlane::lane_window_begin(&mut w.state, estate)?;
    {
        let crate::procnode::PlanStateNode::Sort(s) = &mut w.outer else {
            unreachable!("memoized framed admission requires a Sort child")
        };
        let crate::procnode::SortNode { state: sstate, outer: souter, outer_desc, .. } = s;
        debug_assert!(!sstate.sort_done(), "fresh window node over a fed sort");
        if !super::sort_feed_if_needed(sstate, souter, outer_desc, None, estate)? {
            // Feed-time refuse before any lane tuple: the Volcano fallback
            // resumes byte-identically. UNREACHABLE for this admission
            // (scan-fed sorts only), kept live as defense-in-depth exactly
            // like W1's drive_first — and the refusal MUST flip the memo
            // (a stranded admit=true would hijack a row-engine-fed node on
            // the next pull; the fixed W1 inc-1 blocker).
            w.lane_framed_admit = Some(false);
            return Ok(None);
        }
    }
    // One OWNED tick per drive start (the sticky batch cadence: the
    // underlying sort-feed event; a rescan re-feeds and re-ticks).
    stats::tick_owned(ShapeClass::WindowAgg);
    super::lane_trace("windows drive armed (T2-B framed over sort breaker)");
    w.lane_framed = Some(wlane::LaneFramedDrive::new());
    framed_drive(w, estate).map(Some)
}

/// One owned pull: resume/stream through the chain driver (the W1 drive
/// shape with the framed op).
fn framed_drive<'mcx>(
    w: &mut crate::procnode::WindowAggNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    #[cfg(test)]
    WINDOWS_T2B_OWNED_FOR_TESTS.fetch_add(1, Relaxed);
    // exec_window_agg's entry interrupt gate + drained guard.
    if ::init_small::globals::InterruptPending() {
        ::postgres_seams::check_for_interrupts::call()?;
    }
    if wlane::lane_window_done(&w.state) {
        return Ok(None);
    }
    // Rescan re-entry: re-run the all_first arm (offset re-evaluation — a
    // chgParam rescan can change offset Params) and re-feed the sort.
    wlane::lane_window_begin(&mut w.state, estate)?;
    let crate::procnode::PlanStateNode::Sort(s) = &mut w.outer else {
        unreachable!("lane-owned WindowAgg lost its Sort child")
    };
    let crate::procnode::SortNode { state: sstate, outer: souter, outer_desc, .. } = s;
    if !sstate.sort_done() {
        // Post-rescan re-feed; a feed-time refuse is unreachable BY
        // CONSTRUCTION (scan-fed sorts only — W1's drive() argument holds
        // verbatim). The loud tripwire stays as defense-in-depth.
        if !super::sort_feed_if_needed(sstate, souter, outer_desc, None, estate)? {
            return Err(sticky_tripwire("sort feed verdict"));
        }
        stats::tick_owned(ShapeClass::WindowAgg);
    }
    let mut op = FramedWindowOp {
        state: &mut w.state,
        drive: w.lane_framed.as_mut().expect("sticky framed drive exists"),
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
