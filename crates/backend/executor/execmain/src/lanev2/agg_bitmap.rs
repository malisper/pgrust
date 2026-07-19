//! SE-AGGBITMAP — the lane batch feed for aggregation over BitmapHeapScan
//! (single-executor deletion-prep, fused arm #4 `AGG_BITMAP` re-host).
//!
//! Plain English: today, a query shaped `Agg over Bitmap Heap Scan` (with no
//! extra scan filter and no projection) is driven by a "fused arm" — a
//! special-case batched loop living in procnode.rs behind the default-ON
//! `PGRUST_FUSED_ARM_AGG_BITMAP` kill knob. The row-executor deletion plan
//! wants that arm physically removed, but the SE-DELETION-PREP audit
//! (notes/se-deletion-prep.md arm #4) found the only lane hook for this
//! shape is the morsel-runtime plain-root arm (`runtime_bitmap`, needs
//! explicit arming) — so at stock serial defaults the fused arm OWNS the
//! shape and deleting it would drop the whole family to the slow per-tuple
//! path. This module is the lane replacement: the SAME drive, hosted in the
//! lane layer, so the procnode arm becomes deletable once this feed is
//! lettered and flipped default-ON (the WS-F/WS-AE `indexsource` playbook,
//! arms #2/#3 — this is the designed "inc-4" of that module doc, built as
//! its own estate).
//!
//! # What the drive is (byte-identity by construction)
//!
//! Knob ON, admitted shapes run `::nodeagg::exec_agg_batched` — the exact
//! kernel the fused arm calls — over the exact node primitives the fused
//! arm's `BitmapScanBatchSource` wraps:
//!   * `bitmap_scan_next_pagebatch` — the bitmap iterator advances one page
//!     (exact page or lossy chunk) and stages its visible tuples,
//!   * `bitmap_scan_batch_fetch` — stores staged tuple `i` into the scan
//!     slot and, on recheck pages, runs the page recheck qual
//!     (`bitmapqualorig`) PER ROW — C's `BitmapHeapNext` semantics exactly.
//! Same rows, same order, same per-row recheck program, same transition
//! order, same hash-agg spill machinery (`lookup_hash_entry` spill-mode +
//! `hashagg_finish_initial_spills` + the canonical retrieve refill), same
//! output bytes. The only new code on the hot path is this chokepoint's
//! admission walk and accounting.
//!
//! # Exact-vs-lossy TID semantics (the charter's hard rule)
//!
//! The batch shape honors C's semantics BECAUSE the recheck decision stays
//! below this seam: `table_scan_bitmap_next_pagebatch` sets the node's
//! per-page `recheck` flag from the iterator entry (exact page => the TIDs
//! are trusted, no recheck; lossy chunk / candidate match => every fetched
//! row runs `bitmapqualorig`), and `bitmap_scan_batch_fetch` applies it per
//! row. `storeless_ok` is FALSE (the fused source's stated override): a
//! storeless count(*) advance would count rows the recheck must reject on
//! lossy pages — visibility and recheck both resolve at fetch time.
//!
//! # Serial + parallel-aware coverage (shared iterator state)
//!
//! The fused arm has no parallel gate: a `Partial Aggregate <- Parallel
//! Bitmap Heap Scan` worker fragment reaches the same dispatch arm inside
//! each worker and is fused-arm-driven today (the arm #2 AGG_INDEX audit
//! found the identical situation — an unpriced parallel surface a serial
//! lane feed would strand). This feed therefore covers it: the build/attach
//! ceremony is procnode's `bitmap_table_scan_setup_dispatch` VERBATIM
//! (serial builds; parallel, only the `BM_INITIAL` winner builds and
//! everyone else attaches the shared iterator), and page claims below the
//! seam come off the shared iterator state (`TbmSharedIterator`) page by
//! page — each claimed page is staged, fetched, and rechecked wholly inside
//! the claiming worker, so per-worker byte-identity holds by the same
//! argument as serial.
//!
//! # Refuse-set (every refusal falls through byte-identically)
//!
//! Named, in order, after the silent knob/agg gates:
//!   * `epq` / `backward` / `non-mvcc-snapshot` — the fused gate's estate
//!     legs (`agg_fusible_common`), re-stated per pull;
//!   * `subplan-param` — the page recheck qual (`bitmapqualorig`) carries a
//!     SubPlan or exec-param. The per-row recheck is a plain `exec_qual`
//!     program on ALL paths (per-tuple, fused, this feed), but this feed
//!     refuses the shape fail-closed rather than own a recheck program
//!     whose parameter environment it cannot prove — the standalone bitmap
//!     lane's posture (`bitmap_heap_scan_refuse_reason`), kept verbatim.
//!     NAMED RESIDUAL for the deletion re-trace: these stay arm-owned.
//!   * `shape-qual-proj` — a scan qual or projection under the Agg. The
//!     fused arm refuses these too (procnode gate), so nothing is stranded;
//!     the per-tuple path is the incumbent and stays it.
//! The agg-side gate (`agg_batch_drainable`: AGG_PLAIN/AGG_HASHED, no
//! grouping sets / merge / sorted-input transitions / subplan transitions)
//! ticks `AggBuild`/`AggNotDrainable` per offered pull — there is no hook
//! ahead of this one that ticks for the bitmap arm (unlike the index arms,
//! where the sorted-agg hook does), so the tick is this chokepoint's own.
//! NOT refused: parallel (covered above); instrumentation is structurally
//! unreachable (an EXPLAIN ANALYZE child is a `PlanStateNode::Instrumented`
//! wrapper, so procnode's concrete BitmapHeapScan agg-arm match — where
//! this hook lives — never sees it; the fused arm relies on the same fact).
//!
//! # Interplay with the runtime bitmap arm (`runtime_bitmap`)
//!
//! The morsel-runtime hook dispatches BEFORE this feed. If it engages, this
//! feed never runs. If it built the bitmap and then refused (geometry
//! floor / engage fallback), the node is left `initialized` with the classic
//! iterator attached and NOTHING consumed — this feed (like the fused arm
//! in that situation) skips its own setup and drains byte-identically.
//!
//! # Accounting (knob-OFF = today's accounting, contract §7)
//!
//! Knob OFF this module ticks NOTHING — the fused arm owns the shape
//! exactly as today, and any OFF-path tick would drift the default floors.
//! Knob ON: one OWNED tick under `ShapeClass::BitmapHeapScan` per
//! lane-owned feed event (retrieve-phase pulls of a filled hash agg run
//! zero feed ceremony and tick nothing); child refusals per offered pull
//! under `BitmapHeapScan`; the agg-side gate under `AggBuild`. R-VOCAB
//! untouched: no new ShapeClass, no new RefuseReason.
//!
//! # Knob
//!
//! `PGRUST_LANE_V2_AGG_BITMAP` (default OFF; R-KNOBS registry spelling),
//! layered UNDER the master `pgrust.lane_executor` gate (the procnode hook
//! sits inside `crate::lanev2::enabled()`). OFF cost at the dispatch site =
//! one cached-bool test. Env-var (not GUC) per the standing `pg_settings`
//! byte-identity discipline (lanev2 module doc). The flip to default-ON is
//! the SE drivers' call, gated on the A/B letters — never this lane's.

use std::sync::atomic::{AtomicU8, Ordering::Relaxed};

use ::executils::{EStateData, ExecSlotId};
use ::types_error::PgResult;

use super::stats::{self, RefuseReason, ShapeClass};

/// `PGRUST_LANE_V2_AGG_BITMAP` (default OFF): the SE-AGGBITMAP feed gate.
/// 0 = unresolved (read env on first use), 1 = OFF, 2 = ON. AtomicU8 +
/// `_set_for_tests` per the contract R-KNOBS idiom (rowmode.rs precedent)
/// so the unit corpus can A/B both paths in one process.
static AGG_BITMAP: AtomicU8 = AtomicU8::new(0);

fn agg_bitmap_enabled() -> bool {
    match AGG_BITMAP.load(Relaxed) {
        1 => false,
        2 => true,
        _ => {
            let on = matches!(
                std::env::var("PGRUST_LANE_V2_AGG_BITMAP").as_deref(),
                Ok("1") | Ok("on")
            );
            AGG_BITMAP.store(if on { 2 } else { 1 }, Relaxed);
            on
        }
    }
}

/// Same-process A/B lever for the unit corpus (`crate::tests`).
#[cfg(test)]
pub(crate) fn agg_bitmap_set_for_tests(on: bool) {
    AGG_BITMAP.store(if on { 2 } else { 1 }, Relaxed);
}

/// Test-only engagement probe: lane-owned agg-over-bitmap feed events (the
/// stats ticks arm only via process-global envs, unusable per-test).
#[cfg(test)]
pub(crate) static AGG_BITMAP_OWNED_FOR_TESTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// The feed's child refuse-set: `None` = admitted; `Some(reason)` = refused
/// (ticked under `ShapeClass::BitmapHeapScan`, per offered pull). Mirrors
/// the fused gate's estate legs (`agg_fusible_common`) plus the recheck
/// subplan/exec-param leg from the standalone bitmap lane's refuse-set.
/// Deliberately ABSENT (module doc): a parallel gate (parallel-aware is
/// covered — the shared-iterator setup and page claims live below the
/// seam) and an instrumentation gate (structurally unreachable here).
fn agg_bitmap_refuse_reason<'mcx>(
    bhs: &::nodebitmapheapscan::BitmapHeapScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> Option<RefuseReason> {
    if estate.es_epq_active {
        return Some(RefuseReason::Epq);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        return Some(RefuseReason::Backward);
    }
    if !estate
        .es_snapshot
        .as_deref()
        .is_some_and(::types_snapshot::IsMVCCSnapshot)
    {
        return Some(RefuseReason::NonMvccSnapshot);
    }
    // NAMED RESIDUAL (deletion re-trace row): subplan/exec-param-bearing
    // page recheck quals stay fused-arm-owned; this feed will not own a
    // per-row recheck program whose parameter environment it cannot prove.
    if bhs
        .bitmapqualorig
        .as_deref()
        .is_some_and(|q| q.has_subplan() || !q.param_exec_deps().is_empty())
    {
        return Some(RefuseReason::SubplanParam);
    }
    // Mirror of the fused gate's shape legs: qual/projection-bearing scans
    // are refused BY THE ARM TOO — the per-tuple path is the incumbent
    // there and keeps the shape either way.
    if bhs.ss.qual.is_some() || bhs.ss.ps_ProjInfo.is_some() {
        return Some(RefuseReason::ShapeQualProj);
    }
    None
}

/// Operator-pull adapter over the bitmap page-batch primitives —
/// call-for-call identical to procnode's fused `BitmapScanBatchSource`:
/// `next_batch` = `bitmap_scan_next_pagebatch` (interrupt check per page,
/// per-page recheck flag set below the seam), `fetch_tuple` =
/// `bitmap_scan_batch_fetch` (store + per-row recheck on recheck pages),
/// no qual, and `storeless_ok` FALSE — recheck and visibility both resolve
/// at fetch time (a storeless count(*) advance over a lossy page would be
/// a wrong answer; the fused source states the same override).
struct BitmapFeedAggSource<'a, 'mcx> {
    bhs: &'a mut ::nodebitmapheapscan::BitmapHeapScanState<'mcx>,
    outer_slot: ExecSlotId,
}

impl<'mcx> ::nodeagg::AggBatchSource<'mcx> for BitmapFeedAggSource<'_, 'mcx> {
    #[inline]
    fn next_batch(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<u32> {
        ::nodebitmapheapscan::bitmap_scan_next_pagebatch(self.bhs, estate)
    }

    #[inline]
    fn fetch_tuple(&mut self, i: u32, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        ::nodebitmapheapscan::bitmap_scan_batch_fetch(self.bhs, estate, i)
    }

    #[inline]
    fn outer_slot(&self) -> ExecSlotId {
        self.outer_slot
    }

    #[inline]
    fn has_qual(&self) -> bool {
        false
    }

    // Lossy/recheck pages apply bitmapqualorig in fetch_tuple; visibility
    // resolves there too. Never storeless (module doc, the fused source's
    // stated override).
    #[inline]
    fn storeless_ok(&self) -> bool {
        false
    }
}

/// Try to let the lane host the fused agg-over-BitmapHeapScan drive
/// (`SELECT agg(..) FROM t WHERE bitmap-qual` PLAIN drains and the HASHED
/// drainable GROUP BY shapes fused arm #4 owns — exactly
/// `agg_batch_drainable`'s set under `agg_fusible_common`'s estate legs,
/// serial AND parallel-aware).
///
/// `Some(result)` = the lane drove this call; `None` = refused, the caller
/// falls through to the UNCHANGED fused/per-tuple paths (always byte-safe:
/// the drive here is `exec_agg_batched` over the same primitives in the
/// same order, so knob-ON output is byte-identical by construction).
///
/// Refuse-set, in order: the knob (silent — accounting doc above); the
/// fused arm's agg gate (`agg_batch_drainable`, ticked under `AggBuild` —
/// no hook ahead of this one ticks for the bitmap arm); the child
/// refuse-set [`agg_bitmap_refuse_reason`] (EPQ / backward / non-MVCC /
/// recheck subplan-param / qual-proj), per offered pull.
#[inline]
pub fn try_own_agg_over_bitmap_feed<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    b: &mut crate::procnode::BitmapHeapPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if !agg_bitmap_enabled() {
        // Knob OFF: silent fall-through — today's bytes AND today's
        // accounting (the fused arm below owns the shape exactly as before).
        return Ok(None);
    }
    // Agg-side gate, per offered pull (module accounting doc: this
    // chokepoint's own tick — no earlier hook covers the bitmap arm).
    if !::nodeagg::agg_batch_drainable(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AggNotDrainable);
        return Ok(None);
    }
    // Child refuse-set, per offered pull, NAMED under the class.
    if let Some(r) = agg_bitmap_refuse_reason(&b.scan, estate) {
        stats::tick_refused(ShapeClass::BitmapHeapScan, r);
        engine_mirror(&b.scan, estate, Some(r));
        return Ok(None);
    }
    // Emit phase of a filled hash agg (and the done plain agg's final
    // pull): the fused drive runs no source work there, so feed ceremony
    // would be NEW per-pull work — skip it and stay work-identical (the
    // indexsource posture). The done case returns end-of-set exactly as
    // `exec_agg_batched` would (`agg_done` short-circuit); the filled-hash
    // case delegates to the retrieve path with the source untouched.
    let hashed_emit = ::nodeagg::agg_hash_table_filled(agg);
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    let outer_slot = b.scan.ss.ss_ScanTupleSlot;
    if !hashed_emit {
        // The feed event. Build/attach ceremony = the fused arm's line,
        // verbatim: serial builds the bitmap here; parallel builds only in
        // the BM_INITIAL winner and attaches the shared iterator elsewhere;
        // a runtime_bitmap floor-refusal already left the node initialized
        // and this (like the fused arm) skips its own setup.
        if !b.scan.initialized {
            crate::procnode::bitmap_table_scan_setup_dispatch(b, estate)?;
        }
        stats::tick_owned(ShapeClass::BitmapHeapScan);
        engine_mirror(&b.scan, estate, None);
        #[cfg(test)]
        AGG_BITMAP_OWNED_FOR_TESTS.fetch_add(1, Relaxed);
        if super::lane_trace_enabled() {
            super::lane_trace("agg-bitmap: owned agg-over-bitmap-heap feed");
        }
    }
    let result = ::nodeagg::exec_agg_batched(
        agg,
        estate,
        BitmapFeedAggSource { bhs: &mut b.scan, outer_slot },
    )?;
    if !hashed_emit {
        // Scan-slot hygiene at drain settle (the indexsource end_claim
        // posture): the slot may hold the last fetched tuple (buffer pin);
        // the clear is idempotent and runs AFTER the agg result is
        // computed, so output bytes cannot depend on it. C's BitmapHeapNext
        // performs the same clear at exhaustion on the per-tuple path.
        let mcx = estate.es_query_cxt;
        ::exectuples::exec_clear_tuple(estate.slot_mut(outer_slot), mcx);
    }
    Ok(Some(result))
}

/// EXPLAIN (ENGINE) production-verdict mirror at this chokepoint (WS-C
/// `engine_record_verdict` conventions; the indexsource `engine_mirror`
/// twin). Same ledgered reachability gap as the index feeds: under
/// EXPLAIN (ANALYZE, ENGINE) the child is an `Instrumented` wrapper and
/// this arm is not reached — when WS-C's breadth pass offers the wrapped
/// shape, the mirror here is already live.
#[inline]
fn engine_mirror(
    bhs: &::nodebitmapheapscan::BitmapHeapScanState<'_>,
    estate: &mut EStateData<'_>,
    refuse: Option<RefuseReason>,
) {
    if estate.engine_capture() {
        if let Some(idx) = bhs.ss.instr_idx {
            super::engine_record_verdict(estate, idx as i32, ShapeClass::BitmapHeapScan, refuse);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `PGRUST_LANE_V2_AGG_BITMAP` A/B lever (AtomicU8 idiom): both states
    /// resolvable in one process; restored to OFF (the default the rest of
    /// the suite assumes — knob-OFF = today's bytes).
    #[test]
    fn agg_bitmap_knob_ab() {
        agg_bitmap_set_for_tests(true);
        assert!(agg_bitmap_enabled());
        agg_bitmap_set_for_tests(false);
        assert!(!agg_bitmap_enabled());
    }
}
