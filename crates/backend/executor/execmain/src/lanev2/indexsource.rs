//! Index scans as lane sources — single-executor migration Phase 1, WS-F
//! increment 1: the IndexOnlyScan hosted behind the storage seam
//! ([`super::batch_source::BatchGranuleSource`]), SERIAL, behind
//! `PGRUST_LANE_V2_INDEXSOURCE` (default OFF). Everything here is
//! fail-closed to today's byte-identical paths: knob OFF, the procnode
//! `agg_arm` hook falls straight through to the UNCHANGED fused
//! `exec_agg_batched` drive; knob ON, the drive is the SAME
//! `exec_agg_batched` loop over the SAME node primitives
//! (`index_only_scan_batch_next` / `index_only_scan_batch_store` — the
//! VM-probe / heap-fallback / predicate-lock order preserved verbatim), now
//! routed through the seam with ownership ticks, trace lines and the
//! EXPLAIN (ENGINE) production-verdict mirror at this one chokepoint.
//!
//! # The index-source posture law (integration contract §2a(iii))
//!
//! Index sources are POSITIONAL (mode B): `position()` is a real seek by
//! key-range subrange (`bt_partition_scan_range` on the index-morsels
//! branch is the substrate), and the sequential seize-cursor posture is
//! confined to the runtime arm's private drive. Increment 1 is dop-1: the
//! two postures degenerate identically to ONE whole-range claim + drain,
//! and [`IndexOnlyScanSource::position`] therefore accepts exactly one
//! whole-range claim per scan and ERRORS on any other range — fail-closed
//! until a width posture is ratified for a later increment.
//!
//! # Width posture: IOS is serial BY EVIDENCE, not by omission
//!
//! The index-morsels lane's MODE RACE (notes/index-morsels-lane.md,
//! CONFIRM-green 2026-07-15) measured index-ONLY-scan morselization at
//! 0.42–0.51x (a REGRESSION) at 1M/10M in every mode, hot and cold — the
//! serial leaf walk is fast and morsel ceremony dominates — while
//! heap-fetch IndexScan fold shapes won 4.6–6.1x at dop8. Boarding action
//! ratified by the reconciler (contract Q1): IOS parallel width is REFUSED
//! by default; the 6.1x heap-fetch class arrives by ABSORBING the
//! index-morsels branch (inc-3), never by re-implementing it here.
//!
//! # Accounting (knob-OFF = today's accounting, contract §7)
//!
//! Knob OFF this module ticks NOTHING (no EnvOff: the fused arm owns the
//! shape exactly as today and today's accounting at this arm is exactly the
//! sorted-agg hook's — any OFF-path tick would drift the default floors).
//! Knob ON: one OWNED tick under `ShapeClass::IndexOnlyScan` per lane-owned
//! feed event (the granule-map + position + drive ceremony; retrieve-phase
//! pulls of a filled hash agg run zero seam code and tick nothing), child
//! refusals per offered pull under `IndexOnlyScan` (the class's per-pull
//! cadence), the admission floor under `TinyInputFloor`. The agg-side gate
//! falls through SILENTLY: the sorted-agg hook ahead of this one already
//! ticks `AggBuild`/`AggNotDrainable` per offered pull at this same arm, so
//! ticking it again here would double-count. R-VOCAB untouched: no new
//! ShapeClass, no new RefuseReason.
//!
//! # EXPLAIN (ENGINE) mirror (contract §2e) and its inc-1 reachability gap
//!
//! `engine_record_verdict` is wired at this chokepoint (owned and refused
//! verdicts both). KNOWN GAP, ledgered (notes/se-ws-f-indexsource.md):
//! under EXPLAIN (ANALYZE, ENGINE) every child is an `Instrumented`
//! wrapper, so procnode's concrete `IndexOnlyScan` agg-arm match — where
//! this hook lives — is not reached; the recorded IOS verdict today comes
//! from the standalone `try_own_index_only_scan` hook. Capture breadth for
//! the agg-over-IOS composition is WS-C inc-2's D3 ledger item 2 (the
//! reconciler's Q5 ruling: coordinate, don't duplicate) — when WS-C's
//! breadth pass offers the wrapped shape, the mirror here is already live.
//!
//! # Later increments (designed, NOT built — see the WS-F ledger)
//!
//! inc-2: page-batch leaf staging (`index_only_scan_stage_leaf_run`) and
//! push-pipeline hosting; inc-3: absorption of the index-morsels branch
//! (heap-fetch IndexScan fold shapes, the race-proven 6.1x class) with the
//! tail-drain-to-Done completion law as an invariant + churn e2e leg and an
//! explicit SAOP width refusal; inc-4: BitmapHeapSource over the frozen
//! shared bitmap. Runtime keys stay refused until the increments rebase
//! onto the t25 subplankey fix (`exec_eval_expr_with_subplans` routing;
//! SubPlan-bearing keys never parallel).

use std::sync::atomic::{AtomicU8, Ordering::Relaxed};
use std::sync::OnceLock;

use ::executils::{EStateData, ExecSlotId};
use ::types_error::{PgError, PgResult, ERROR};

use super::batch_source::{BatchGranuleSource, SourceCaps};
use super::stats::{self, RefuseReason, ShapeClass};

/// `PGRUST_LANE_V2_INDEXSOURCE` (default OFF): the WS-F index-source gate,
/// layered UNDER the master `pgrust.lane_executor` gate (the procnode hook
/// is inside `crate::lanev2::enabled()`). 0 = unresolved (read env on first
/// use), 1 = OFF, 2 = ON. AtomicU8 + `_set_for_tests` per the contract
/// R-KNOBS idiom (rowmode.rs precedent) so the unit corpus can A/B both
/// paths in one process; env-var (not GUC) per the standing `pg_settings`
/// byte-identity discipline (lanev2 module doc).
static INDEXSOURCE: AtomicU8 = AtomicU8::new(0);

fn indexsource_enabled() -> bool {
    match INDEXSOURCE.load(Relaxed) {
        1 => false,
        2 => true,
        _ => {
            let on = matches!(
                std::env::var("PGRUST_LANE_V2_INDEXSOURCE").as_deref(),
                Ok("1") | Ok("on")
            );
            INDEXSOURCE.store(if on { 2 } else { 1 }, Relaxed);
            on
        }
    }
}

/// Same-process A/B lever for the unit corpus (`crate::tests`).
#[cfg(test)]
pub(crate) fn indexsource_set_for_tests(on: bool) {
    INDEXSOURCE.store(if on { 2 } else { 1 }, Relaxed);
}

/// Test-only engagement probe: lane-owned index-source feed events (the
/// stats ticks arm only via process-global envs, unusable per-test).
#[cfg(test)]
pub(crate) static INDEXSOURCE_OWNED_FOR_TESTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Planner-estimate admission floor (rows): `PGRUST_LANE_V2_INDEXSOURCE_MIN_ROWS`,
/// default 0 in inc-1 — the serial drive is work-identical to the fused arm,
/// so there is no cost to recover yet; the floor becomes load-bearing when
/// staging/width costs appear (inc-2+). This is the router's LOUD refusal of
/// below-crossover index lookups: a configured floor ticks
/// `RefuseReason::TinyInputFloor` per refused offer and traces the estimate.
fn indexsource_min_rows() -> f64 {
    static FLOOR: OnceLock<f64> = OnceLock::new();
    *FLOOR.get_or_init(|| {
        std::env::var("PGRUST_LANE_V2_INDEXSOURCE_MIN_ROWS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.0)
    })
}

/// Below-floor verdict, factored pure for the unit corpus: refuse when the
/// planner estimate is strictly under the configured floor (floor 0 — the
/// inc-1 default — never refuses).
#[inline]
fn below_floor(plan_rows: f64, floor: f64) -> bool {
    plan_rows < floor
}

/// Startup-ramp seed for index-leaf pacing maps: one btree leaf stages a few
/// hundred keys — heap-block-class granularity, so the heap seed's class
/// (16, batch_source.rs `HEAP_STARTUP_C0`). Inert at dop-1 (one whole-range
/// claim); kept so the map is honestly formed for the width increments.
const IOS_STARTUP_C0: u64 = 16;

/// Serial-claim discipline (inc-1, dop-1): exactly ONE whole-range claim per
/// scan, validated against the published granule total. Factored pure for
/// the unit corpus; [`IndexOnlyScanSource::position`] maps `Err` to a loud
/// `PgError` (fail-closed — never a silent truncation, the completion-law
/// hazard the WS-F design names).
fn validate_serial_claim(
    positioned: bool,
    total: Option<u64>,
    seg: &runtime::MorselRange,
) -> Result<(), &'static str> {
    let Some(total) = total else {
        return Err("position before granule_map");
    };
    if positioned {
        return Err("second claim on a serial index source");
    }
    if seg.start != 0 || seg.end != total {
        return Err("partial claim on a serial index source");
    }
    Ok(())
}

/// `BatchGranuleSource` implementor for a FORWARD BTREE IndexOnlyScan.
/// Inc-1 semantics (single-claimer serial): granule = leaf-page pacing
/// quantum; `granule_map` publishes an UPPER-BOUND estimate (index-relation
/// block count — pacing only, correctness never rides on it: the drain runs
/// to the AM's own exhaustion, the tail-drain-to-Done law's dop-1
/// degenerate); `position` accepts exactly one whole-range claim per scan
/// and errors on any other range; `next_batch` is
/// `index_only_scan_batch_next` verbatim (0/1 visible tuple; VM-probe /
/// heap-fallback / predicate-lock order preserved); `end_claim` clears the
/// scan + table slots (ownership ABI R3 zero-pins-at-settle; the VM buffer
/// pin is node-lifetime, like the seq scan's cached pins).
pub(super) struct IndexOnlyScanSource<'a, 'mcx> {
    ios: &'a mut ::nodeindexonlyscan::IndexOnlyScanState<'mcx>,
    /// Granule total published by `granule_map`; `position` validates the
    /// whole-range claim against it. `None` until `granule_map` ran.
    total: Option<u64>,
    positioned: bool,
}

impl<'a, 'mcx> IndexOnlyScanSource<'a, 'mcx> {
    #[inline]
    pub(super) fn new(ios: &'a mut ::nodeindexonlyscan::IndexOnlyScanState<'mcx>) -> Self {
        IndexOnlyScanSource { ios, total: None, positioned: false }
    }

    /// Inc-1 bridge for the seam→operator-pull adapter: the staged tuple is
    /// consumed through the node fn (`index_only_scan_batch_store`) on
    /// `&mut IndexOnlyScanState`, exactly like `SeqScanSource::scan_mut`
    /// (removed when the trait read face lands — WS-K inc-2).
    #[inline]
    fn ios_mut(&mut self) -> &mut ::nodeindexonlyscan::IndexOnlyScanState<'mcx> {
        self.ios
    }
}

impl<'mcx> BatchGranuleSource<'mcx> for IndexOnlyScanSource<'_, 'mcx> {
    fn granule_map(
        &mut self,
        _estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<runtime::GranuleMap>> {
        let Some(nblocks) = ::nodeindexonlyscan::index_only_scan_leaf_estimate(self.ios)? else {
            return Ok(None); // no open index relation — the caller refuses
        };
        self.total = Some(nblocks);
        Ok(Some(runtime::GranuleMap::unbounded(nblocks, IOS_STARTUP_C0)))
    }

    fn position(
        &mut self,
        _estate: &mut EStateData<'mcx>,
        seg: runtime::MorselRange,
    ) -> PgResult<()> {
        validate_serial_claim(self.positioned, self.total, &seg)
            .map_err(|why| indexsource_misuse(why))?;
        // dop-1 whole-range claim: the node's natural full leaf walk IS the
        // positioned segment — no AM seek to perform. Real subrange seeks
        // (reposition_morsel + bt_partition_scan_range) arrive with the
        // index-morsels absorption (inc-3, mode-B posture).
        self.positioned = true;
        Ok(())
    }

    #[inline]
    fn next_batch(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<u32> {
        if !self.positioned {
            return Err(indexsource_misuse("next_batch before position"));
        }
        ::nodeindexonlyscan::index_only_scan_batch_next(self.ios, estate)
    }

    fn end_claim(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        ::nodeindexonlyscan::index_only_scan_end_claim(self.ios, estate);
        self.positioned = false; // claim settled; a NEW claim may position
        Ok(())
    }

    fn capabilities(&self) -> SourceCaps {
        // `index_leaf: true` arrives with WS-K's SourceCaps growth (contract
        // §2a ships the field on WS-F's behalf; WS-K merges first) — the
        // missing-field compile error at that rebase is the deliberate
        // fail-closed reminder to flip it here.
        SourceCaps { columnar: false, heap_pages: false }
    }
}

/// Seam→operator-pull adapter (the two seams composed per the R-NAME
/// ruling: [`BatchGranuleSource`] below, `executils::BatchSource` — nodeagg's
/// `AggBatchSource` — above). Call-for-call identical to procnode's fused
/// `IndexOnlyScanBatchSource`: `next_batch` = `index_only_scan_batch_next`
/// (through the seam), `fetch_tuple` = `index_only_scan_batch_store`, no
/// qual, and NO `storeless_ok` override (next_batch counts visible rows
/// only, so the default `!has_qual()` storeless drain stays sound — exactly
/// the fused source's contract).
struct SeamAggSource<'s, 'a, 'mcx> {
    src: &'s mut IndexOnlyScanSource<'a, 'mcx>,
    outer_slot: ExecSlotId,
}

impl<'mcx> ::nodeagg::AggBatchSource<'mcx> for SeamAggSource<'_, '_, 'mcx> {
    #[inline]
    fn next_batch(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<u32> {
        BatchGranuleSource::next_batch(self.src, estate)
    }

    #[inline]
    fn fetch_tuple(&mut self, _i: u32, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        ::nodeindexonlyscan::index_only_scan_batch_store(self.src.ios_mut(), estate)
    }

    #[inline]
    fn outer_slot(&self) -> ExecSlotId {
        self.outer_slot
    }

    #[inline]
    fn has_qual(&self) -> bool {
        false
    }
}

/// Try to let the lane host the fused agg-over-IndexOnlyScan drive through
/// the storage seam (`SELECT agg(..) FROM t WHERE indexed-range` fold shapes
/// and the hashed drainable shapes the fused arm owns — exactly
/// `agg_batch_drainable`'s set, procnode `agg_fusible_common`'s agg leg).
///
/// `Some(result)` = the lane drove this call; `None` = refused, the caller
/// falls through to the UNCHANGED fused/per-tuple paths (always byte-safe:
/// the drive here is `exec_agg_batched` over the same primitives in the
/// same order, so knob-ON output is byte-identical by construction).
///
/// Refuse-set, in order: the knob (silent — see the module accounting doc);
/// the fused arm's agg gate (`agg_batch_drainable`; its estate legs are
/// re-checked in the child refuse-set); the child refuse-set
/// `index_only_scan_refuse_reason` VERBATIM (EPQ / backward / scroll-mark /
/// parallel-aware / instrumented / non-MVCC / qual-proj / runtime-keys /
/// order-by-reorder / non-btree); the planner-estimate admission floor; the
/// seam geometry probe (`granule_map` `None` = fail-closed fall-through).
#[inline]
pub fn try_own_agg_over_index_only_source<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ios: &mut ::nodeindexonlyscan::IndexOnlyScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if !indexsource_enabled() {
        // Knob OFF: silent fall-through — today's bytes AND today's
        // accounting (the fused arm below owns the shape exactly as before).
        return Ok(None);
    }
    // Agg-side gate, silent (the sorted-agg hook ahead of this one already
    // ticked AggBuild/AggNotDrainable for this offer — module doc).
    if !::nodeagg::agg_batch_drainable(agg) {
        return Ok(None);
    }
    // Child refuse-set VERBATIM (per-pull cadence, the IOS class's
    // documented tick semantics).
    if let Some(r) = super::index_only_scan_refuse_reason(ios, estate) {
        stats::tick_refused(ShapeClass::IndexOnlyScan, r);
        engine_mirror(ios, estate, Some(r));
        return Ok(None);
    }
    // Planner-estimate admission floor (the LOUD below-crossover refusal:
    // point lookups under a configured floor never enter the seam).
    let floor = indexsource_min_rows();
    let plan_rows = ::nodeindexonlyscan::index_only_scan_plan_rows(ios);
    if below_floor(plan_rows, floor) {
        stats::tick_refused(ShapeClass::IndexOnlyScan, RefuseReason::TinyInputFloor);
        engine_mirror(ios, estate, Some(RefuseReason::TinyInputFloor));
        if super::lane_trace_enabled() {
            super::lane_trace(&format!(
                "indexsource: refused tiny-input-floor (plan_rows={plan_rows}, floor={floor})"
            ));
        }
        return Ok(None);
    }
    // Emit phase of a filled hash agg (and the done plain agg's final pull):
    // the fused drive runs no source work there, so the seam ceremony below
    // would be NEW per-pull work — skip it and stay work-identical. The
    // plain-agg done case returns end-of-set exactly as `exec_agg_batched`
    // would (`agg_done` short-circuit); the filled-hash case delegates to
    // the retrieve path with the source untouched (it is never called).
    let hashed_emit = ::nodeagg::agg_hash_table_filled(agg);
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    let outer_slot = ios.ss.ss_ScanTupleSlot;
    let mut src = IndexOnlyScanSource::new(ios);
    if !hashed_emit {
        // The feed event: geometry + one whole-range claim (dop-1).
        let Some(map) = src.granule_map(estate)? else {
            // Cannot express granules (no open index relation): fail closed
            // to the fused arm — byte-identical drive, nothing lost.
            super::lane_trace("indexsource: refused (no granule geometry)");
            return Ok(None);
        };
        src.position(estate, 0..map.total())?;
        // OWNED: one tick per lane-owned feed event (module accounting doc).
        stats::tick_owned(ShapeClass::IndexOnlyScan);
        engine_mirror(src.ios, estate, None);
        #[cfg(test)]
        INDEXSOURCE_OWNED_FOR_TESTS.fetch_add(1, Relaxed);
        if super::lane_trace_enabled() {
            super::lane_trace(&format!(
                "indexsource: owned agg-over-ios feed (granules={}, plan_rows={plan_rows})",
                map.total()
            ));
        }
    }
    let result =
        ::nodeagg::exec_agg_batched(agg, estate, SeamAggSource { src: &mut src, outer_slot })?;
    if !hashed_emit {
        // Claim settle (ownership ABI R3: slot hygiene; zero pins held).
        src.end_claim(estate)?;
    }
    Ok(Some(result))
}

/// EXPLAIN (ENGINE) production-verdict mirror at this chokepoint (contract
/// §2e, WS-C's `engine_record_verdict` conventions). Records the OBSERVED
/// verdict; an observed `Instrumented` displays with WS-C's honest
/// "production engine may differ" suffix (no ignore-instrument re-proof
/// exists for this composition yet — that sharpening is WS-C inc-2's D3
/// item 2, the agg-over-IOS capture; see the module doc's reachability gap).
#[inline]
fn engine_mirror(
    ios: &::nodeindexonlyscan::IndexOnlyScanState<'_>,
    estate: &mut EStateData<'_>,
    refuse: Option<RefuseReason>,
) {
    if estate.engine_capture() {
        if let Some(idx) = ios.ss.instr_idx {
            super::engine_record_verdict(estate, idx as i32, ShapeClass::IndexOnlyScan, refuse);
        }
    }
}

#[cold]
#[inline(never)]
fn indexsource_misuse(why: &str) -> Box<PgError> {
    Box::new(PgError::new(
        ERROR,
        format!("lane index source claim-discipline violation: {why}"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// dop-1 claim discipline: one whole-range claim, everything else loud.
    #[test]
    fn serial_claim_discipline() {
        // No granule_map yet: every claim refused.
        assert_eq!(
            validate_serial_claim(false, None, &(0..10)),
            Err("position before granule_map")
        );
        // The one legal claim: whole range, first claim.
        assert_eq!(validate_serial_claim(false, Some(10), &(0..10)), Ok(()));
        // Second claim on the same scan: refused.
        assert_eq!(
            validate_serial_claim(true, Some(10), &(0..10)),
            Err("second claim on a serial index source")
        );
        // Partial / offset / oversized claims: refused (fail-closed, never a
        // silent truncation — the completion-law hazard).
        for seg in [0..5, 1..10, 0..11, 3..7] {
            assert_eq!(
                validate_serial_claim(false, Some(10), &seg),
                Err("partial claim on a serial index source")
            );
        }
        // Degenerate empty map: only the empty whole-range claim passes.
        assert_eq!(validate_serial_claim(false, Some(0), &(0..0)), Ok(()));
        assert_eq!(
            validate_serial_claim(false, Some(0), &(0..1)),
            Err("partial claim on a serial index source")
        );
    }

    /// The floor verdict: default 0 never refuses; a configured floor
    /// refuses strictly-below estimates (the loud point-path refusal).
    #[test]
    fn floor_verdict() {
        assert!(!below_floor(0.0, 0.0));
        assert!(!below_floor(1.0, 0.0));
        assert!(!below_floor(1000.0, 1000.0));
        assert!(below_floor(999.0, 1000.0));
        assert!(below_floor(1.0, 1000.0));
    }

    /// Knob A/B lever + resolution states (the rowmode idiom).
    #[test]
    fn knob_set_for_tests_flips() {
        indexsource_set_for_tests(false);
        assert!(!indexsource_enabled());
        indexsource_set_for_tests(true);
        assert!(indexsource_enabled());
        indexsource_set_for_tests(false);
        assert!(!indexsource_enabled());
    }
}
