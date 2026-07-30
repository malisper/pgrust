//! Index scans as lane sources — single-executor migration Phase 1, WS-F
//! increment 1: the IndexOnlyScan hosted behind the storage seam
//! ([`super::batch_source::BatchGranuleSource`]), SERIAL, behind
//! `PGRUST_LANE_V2_INDEXSOURCE` (default ON since the SE17-GATES flip; explicit
//! `=0`/`off` = permanent kill restoring the WS-F inc-1 arm-owned world).
//! Everything here is byte-identical between the arms (the lane's 5-arm ISO
//! effigy + 6-cell dualexec + C-oracle proof): knob OFF, the procnode
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
//! SE-AGGIOS clarification: admitting PLANNER-parallel worker fragments
//! ([`INDEXSOURCE_PAR`] point 1) does NOT contradict this evidence. The
//! mode race priced LANE-initiated morsel width (the lane splitting one
//! serial scan). A stock `Partial Aggregate <- Parallel Index Only Scan`
//! fragment is the PLANNER's width: each worker's feed stays dop-1 over
//! the AM's shared page cursor — exactly the drive the fused arm #3 runs
//! on those fragments today (its gate never had a parallel check).
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

/// `PGRUST_LANE_V2_INDEXSOURCE` (default ON since SE17-GATES; `=0`/`off` =
/// permanent kill): the WS-F index-source gate, layered UNDER the master
/// `pgrust.lane_executor` gate (the procnode hook is inside
/// `crate::lanev2::enabled()`). 0 = unresolved (read env on first
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
            // SE17-GATES flip (default OFF -> ON): the AE2 agg_indexfeed_resolve
            // idiom — bare/unset/other reads ON, explicit `=0`/`off` = permanent
            // kill (restores the WS-F inc-1 arm-owned world). Flip evidence:
            // se-aggios worklog §6 + the composed-tip letter (flip surface
            // B/A 0.9998 flat-or-better; IndexScan control exact-flat). The
            // AGG_IOS census clock starts at the first train carrying this flip.
            let on = !matches!(
                std::env::var("PGRUST_LANE_V2_INDEXSOURCE").as_deref(),
                Ok("0") | Ok("off")
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

/// `PGRUST_LANE_V2_INDEXSOURCE_PAR` (default ON; explicit `=0`/`off` is the
/// increment kill switch): the SE-AGGIOS increment gate, layered UNDER
/// [`INDEXSOURCE`] (a dead read while the feed itself is off — the feed
/// knob is still default OFF at this tip; the SE17 flip proposal carries
/// both). Gates the two admission widenings this increment adds to the
/// WS-F inc-1 feed — BOTH restore the exact inc-1 admission set when
/// killed (the AGGIDX-PAR [`AGG_INDEXFEED_PAR`] pattern verbatim):
///
/// 1. **Parallel-aware scans** (`ioss_ParallelAware`): the fused arm #3
///    gate never had a parallel check, so a stock `Partial Aggregate <-
///    Parallel Index Only Scan` worker fragment is FUSED-ARM-driven at
///    knob-ON-without-this-increment — the same deletion-blocker class the
///    SE-DELETION-PREP audit named for arm #2. Coverage is byte-identical
///    by construction: each worker's feed drives the SAME
///    `index_only_scan_batch_next` over the shared parallel scan
///    descriptor the DSM initializers opened
///    (`index_beginscan_parallel`) — page claims coordinate inside the AM;
///    the VM probe / heap-fallback / page-level predicate lock run per TID
///    against worker-local state either way (visibility semantics move
///    zero bytes). The seam ceremony stays dop-1-per-worker (one
///    whole-range claim, drain to the AM's own exhaustion — the
///    tail-drain-to-Done law; the granule map is a pacing upper bound and
///    correctness never rides on it).
/// 2. **Zero-block index geometry**: the inc-1 feed refused `nblocks == 0`
///    (fail-closed `granule_map` None via the old leaf-estimate gate)
///    while the fused gate admits it. Covered by publishing the degenerate
///    empty map ([`index_feed_geometry`]; `validate_serial_claim` accepts
///    exactly the empty whole-range claim) and driving to the AM's own
///    exhaustion — the same primitives the fused arm would run,
///    byte-identical including any AM error surface. (Defensive: a built
///    btree always has a metapage, so this row is SQL-unreachable today —
///    covered so the AGG_IOS deletion re-trace closes on ADMISSION, not on
///    a reachability argument.)
static INDEXSOURCE_PAR: AtomicU8 = AtomicU8::new(0);

fn indexsource_par_enabled() -> bool {
    match INDEXSOURCE_PAR.load(Relaxed) {
        1 => false,
        2 => true,
        _ => indexsource_par_resolve(),
    }
}

#[cold]
#[inline(never)]
fn indexsource_par_resolve() -> bool {
    // Default ON (the AGGIDX-PAR charter rule: increments default-match
    // their feed's own default read — a dead read while INDEXSOURCE is
    // OFF; the SE-AGGIOS letters price the widened surface for the SE17
    // flip board). The explicit `=0`/`off` spelling is the increment kill
    // switch and restores the WS-F inc-1 feed's exact admission set
    // (rowmode FLIP-1/FLIP-2 idiom; knobs never delete).
    let on = !matches!(
        std::env::var("PGRUST_LANE_V2_INDEXSOURCE_PAR").as_deref(),
        Ok("0") | Ok("off")
    );
    INDEXSOURCE_PAR.store(if on { 2 } else { 1 }, Relaxed);
    on
}

/// Same-process A/B lever for the unit corpus (`crate::tests`).
#[cfg(test)]
pub(crate) fn indexsource_par_set_for_tests(on: bool) {
    INDEXSOURCE_PAR.store(if on { 2 } else { 1 }, Relaxed);
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
    crate::once_val(&FLOOR, || {
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
        // SE-AGGIOS increment point 2: zero-block geometry publishes the
        // degenerate empty map (one empty whole-range claim; drain runs to
        // the AM's own exhaustion — byte-identical to the fused drive on
        // this shape, error surface included). Increment killed = the WS-F
        // inc-1 fail-closed refusal.
        let Some(total) = index_feed_geometry(nblocks, indexsource_par_enabled()) else {
            return Ok(None);
        };
        self.total = Some(total);
        Ok(Some(runtime::GranuleMap::unbounded(total, IOS_STARTUP_C0)))
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
        // WS-K's full Phase-1 SourceCaps (contract §2a shipped `index_leaf`
        // on WS-F's behalf; flipped true here at integration exactly as the
        // fail-closed reminder in the pre-merge draft demanded). Granule =
        // one index leaf page, positional posture; everything else is a
        // heap/columnar capability this source does not have.
        SourceCaps {
            columnar: false,
            heap_pages: false,
            dict_codes: false,
            zone_maps: false,
            all_visible_batches: false,
            index_leaf: true,
        }
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
/// `index_only_scan_refuse_reason_ex` (EPQ / backward / scroll-mark /
/// instrumented / non-MVCC / qual-proj / runtime-keys / order-by-reorder /
/// non-btree, with the parallel-aware row admitted under the SE-AGGIOS
/// increment — see [`INDEXSOURCE_PAR`] point 1; increment killed, the WS-F
/// inc-1-verbatim set returns); the planner-estimate admission floor; the
/// seam geometry probe (`granule_map` `None` = fail-closed fall-through;
/// zero-block geometry covered under the increment, point 2).
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
    // Child refuse-set (per-pull cadence, the IOS class's documented tick
    // semantics). SE-AGGIOS: parallel-aware admitted when the increment is
    // armed — every other row applies identically.
    if let Some(r) =
        super::index_only_scan_refuse_reason_ex(ios, estate, indexsource_par_enabled())
    {
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
    let parallel_aware = ios.ioss_ParallelAware;
    let mut src = IndexOnlyScanSource::new(ios);
    if !hashed_emit {
        // The feed event: geometry + one whole-range claim. Under a
        // parallel-aware scan this runs ONCE PER WORKER (each worker's node
        // is its own feed; the claim is worker-local bookkeeping — the
        // shared page cursor lives inside the AM's parallel scan state).
        let Some(map) = src.granule_map(estate)? else {
            // Cannot express granules (no open index relation): fail closed
            // to the fused arm — byte-identical drive, nothing lost.
            super::lane_trace("indexsource: refused (no granule geometry)");
            return Ok(None);
        };
        src.position(estate, 0..map.total())?;
        // OWNED: one tick per lane-owned feed event (module accounting doc;
        // under parallel that is one tick per worker feed — N feed events).
        stats::tick_owned(ShapeClass::IndexOnlyScan);
        engine_mirror(src.ios, estate, None);
        #[cfg(test)]
        INDEXSOURCE_OWNED_FOR_TESTS.fetch_add(1, Relaxed);
        if super::lane_trace_enabled() {
            super::lane_trace(&format!(
                "indexsource: owned agg-over-ios feed (granules={}, plan_rows={plan_rows}, parallel={parallel_aware})",
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

#[track_caller]
#[cold]
#[inline(never)]
fn indexsource_misuse(why: &str) -> Box<PgError> {
    Box::new(PgError::new(
        ERROR,
        format!("lane index source claim-discipline violation: {why}"),
    ))
}

// --- WS-AE (wave-8): agg-over-IndexScan feed (AGG_INDEX arm re-earn) ----------
//
// Lane PLAIN + HASHED agg feeds over IndexScan order — the fused arm #2
// (`PGRUST_FUSED_ARM_AGG_INDEX`, procnode.rs) surface, hosted behind the
// storage seam exactly as WS-F hosted arm #3 (agg-over-IndexOnlyScan) above.
// Charter: SE-LETTERS banked arm #2 as the WIDEST measured OFF gap
// (+10.20% instr / +7.21% wall, NOT CLOCK-STARTABLE) because the lane
// sorted-agg hook admits only `agg_sorted_lane_admissible` (AGG_SORTED)
// shapes — PLAIN/HASHED refuse (AggNotDrainable) and NO lane replacement
// exists. This feed is the replacement: knob ON, the drive is the SAME
// `exec_agg_batched` loop over the SAME node primitives as the fused arm
// (`index_scan_next_tidrun` / `index_scan_batch_fetch` — same-block TID
// runs; per-TID heap fetch resolves visibility, so `storeless_ok` is FALSE,
// the HOT-chain/dead-row law the fused `IndexScanBatchSource` states), now
// routed through [`BatchGranuleSource`] with ownership ticks and traces at
// this one chokepoint. Knob `PGRUST_LANE_V2_AGG_INDEXFEED`, default ON
// since the SE8-GATES AE2 flip (re-earn letter flat at +0.047%; explicit
// `=0`/`off` = permanent kill switch restoring the fused arm); OFF path =
// one cached-bool test (knob-OFF-zero-cost idiom, wave-8 law 4).
//
// Posture: identical to [`IndexOnlyScanSource`] — POSITIONAL (mode B),
// dop-1, exactly one whole-range claim per scan (`validate_serial_claim`
// reused verbatim); granule = index-leaf pacing quantum, published as an
// UPPER-BOUND block-count estimate (pacing only; the drain runs to the AM's
// own exhaustion). IOS parallel-width evidence (module doc above) carries:
// no width posture here until ratified.
//
// Accounting (mirrors the WS-F contract §7 posture): knob OFF this feed
// ticks NOTHING (the fused arm owns the shape exactly as today; an OFF tick
// would drift the default floors). Knob ON: one OWNED tick under
// `ShapeClass::IndexScan` per lane-owned feed event (under a parallel-aware
// scan: one per WORKER feed — N feed events exist); child refusals per
// offered pull under `IndexScan` (`index_scan_refuse_reason_ex` — the AE2
// set with the parallel-aware row admitted under the AGGIDX-PAR increment;
// the class's per-pull cadence). The agg-side gate falls through SILENTLY:
// the sorted-agg hook ahead of this one already ticked
// AggBuild/AggNotDrainable for this offer. R-VOCAB untouched: no new
// ShapeClass, no new RefuseReason.
//
// No admission floor in inc-1, RECORDED: `IndexScanState` retains no
// planner row estimate (the IOS floor reads `plan_rows`, a field
// nodeindexonlyscan stored for WS-F; nodeindexscan is outside WS-AE's
// ownership rows this wave), and the WS-F floor's inc-1 default is 0 =
// never refuses — so shipping floorless is behaviorally identical to the
// precedent. The floor becomes load-bearing only when staging/width costs
// appear; adding the `plan_rows` mirror is a one-line init-site escalation
// for that increment (notes/se-wave8-aggindex.md).

/// `PGRUST_LANE_V2_AGG_INDEXFEED` (default ON since the SE8-GATES AE2
/// flip; explicit `=0`/`off` is the permanent kill switch): the WS-AE
/// agg-over-IndexScan feed gate, layered UNDER the master
/// `pgrust.lane_executor` gate (the procnode hook is inside
/// `crate::lanev2::enabled()`). Same AtomicU8 + `_set_for_tests` idiom as
/// [`INDEXSOURCE`] above.
static AGG_INDEXFEED: AtomicU8 = AtomicU8::new(0);

fn agg_indexfeed_enabled() -> bool {
    match AGG_INDEXFEED.load(Relaxed) {
        1 => false,
        2 => true,
        _ => agg_indexfeed_resolve(),
    }
}

#[cold]
#[inline(never)]
fn agg_indexfeed_resolve() -> bool {
    // SE8-GATES AE2 FLIP (flip-ladder arm #2 board; notes/se-wave8-gates.md
    // §AE2): default ON — the re-earn letter closed the +10.20%/+7.21% gap
    // to +0.047% instr / -0.3% wall (pgrust-corpus-pairs-1784346407-6704,
    // medians of 3 same-pod rounds; bar flat-or-better MET). Only this
    // default read changes — the explicit `=0`/`off` spelling is the
    // permanent kill switch and restores the fused-arm bytes AND ticks
    // (rowmode FLIP-1/FLIP-2 idiom verbatim; flips never delete knobs).
    let on = !matches!(
        std::env::var("PGRUST_LANE_V2_AGG_INDEXFEED").as_deref(),
        Ok("0") | Ok("off")
    );
    AGG_INDEXFEED.store(if on { 2 } else { 1 }, Relaxed);
    on
}

/// Same-process A/B lever for the unit corpus (`crate::tests`).
#[cfg(test)]
pub(crate) fn agg_indexfeed_set_for_tests(on: bool) {
    AGG_INDEXFEED.store(if on { 2 } else { 1 }, Relaxed);
}

/// `PGRUST_LANE_V2_AGG_INDEXFEED_PAR` (default ON, matching the feed's
/// AE2-flipped default; explicit `=0`/`off` is the increment kill switch):
/// the AGGIDX-PAR increment gate, layered UNDER [`AGG_INDEXFEED`] (a dead
/// read when the feed itself is off). Gates the two admission widenings this
/// increment adds — BOTH restore the exact AE2 feed surface when killed:
///
/// 1. **Parallel-aware scans** (`iss_ParallelAware`): the fused arm #2 gate
///    never had a parallel check, so a stock `Partial Aggregate ← Parallel
///    Index Scan` worker fragment was FUSED-ARM-driven at defaults — the
///    SE-DELETION-PREP audit's loudest blocker row (the AE2 deletion clock
///    was running toward a deletion whose caller re-trace fails). Coverage
///    is byte-identical by construction: each worker's feed drives the SAME
///    `index_scan_next_tidrun` over the shared parallel scan descriptor the
///    DSM initializers opened (`index_beginscan_parallel`) — page claims
///    coordinate inside the AM; the seam ceremony stays dop-1-per-worker
///    (one whole-range claim, drain to the AM's own exhaustion — the
///    tail-drain-to-Done law; the granule map is a pacing upper bound and
///    correctness never rides on it).
/// 2. **Zero-block index geometry**: the AE2 feed refused `nblocks == 0`
///    (fail-closed `granule_map` None) while the fused gate admits it —
///    audit residual row 4. Covered by publishing the degenerate empty map
///    (total 0; `validate_serial_claim` accepts exactly the empty
///    whole-range claim) and driving to the AM's own exhaustion — the same
///    primitives the fused arm would run, byte-identical including any
///    AM error surface. (Defensive: a built btree always has a metapage,
///    so this row is SQL-unreachable today — covered so the deletion
///    re-trace closes on admission, not on a reachability argument.)
static AGG_INDEXFEED_PAR: AtomicU8 = AtomicU8::new(0);

fn agg_indexfeed_par_enabled() -> bool {
    match AGG_INDEXFEED_PAR.load(Relaxed) {
        1 => false,
        2 => true,
        _ => agg_indexfeed_par_resolve(),
    }
}

#[cold]
#[inline(never)]
fn agg_indexfeed_par_resolve() -> bool {
    // Default ON (the charter: increments default-match the feed's AE2
    // default; the AGGIDX-PAR letter prices the widened surface before the
    // SE16 board). The explicit `=0`/`off` spelling is the increment kill
    // switch and restores the AE2 feed's exact admission set (rowmode
    // FLIP-1/FLIP-2 idiom; knobs never delete).
    let on = !matches!(
        std::env::var("PGRUST_LANE_V2_AGG_INDEXFEED_PAR").as_deref(),
        Ok("0") | Ok("off")
    );
    AGG_INDEXFEED_PAR.store(if on { 2 } else { 1 }, Relaxed);
    on
}

/// Same-process A/B lever for the unit corpus (`crate::tests`).
#[cfg(test)]
pub(crate) fn agg_indexfeed_par_set_for_tests(on: bool) {
    AGG_INDEXFEED_PAR.store(if on { 2 } else { 1 }, Relaxed);
}

/// Geometry verdict for BOTH index feeds (IndexScan under
/// [`AGG_INDEXFEED_PAR`] point 2, IndexOnlyScan under [`INDEXSOURCE_PAR`]
/// point 2), factored pure for the unit corpus: `None` = refuse fail-closed
/// (the fused arm owns the shape), `Some(total)` = publish a granule map
/// with this total. The zero-block row is covered only under the feed's own
/// increment knob; killed, the pre-increment refusal returns.
#[inline]
fn index_feed_geometry(nblocks: u64, empty_covered: bool) -> Option<u64> {
    if nblocks == 0 && !empty_covered {
        return None;
    }
    Some(nblocks)
}

/// Test-only engagement probe: lane-owned agg-over-IndexScan feed events.
#[cfg(test)]
pub(crate) static AGG_INDEXFEED_OWNED_FOR_TESTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// `BatchGranuleSource` implementor for a FORWARD BTREE IndexScan (the
/// fused arm #2 shape). Inc-1 semantics mirror [`IndexOnlyScanSource`]
/// field-for-field: granule = leaf-page pacing quantum; `granule_map`
/// publishes the index relation's MAIN-fork block count (UPPER BOUND,
/// pacing only — correctness rides the AM's own exhaustion, the
/// tail-drain-to-Done dop-1 degenerate); `position` accepts exactly one
/// whole-range claim; `next_batch` is `index_scan_next_tidrun` verbatim
/// (stages the next same-block TID run; interrupt check per run preserved);
/// `end_claim` clears the scan slot (ownership ABI R3 zero-pins-at-settle —
/// the slot may hold the last fetched heap tuple's buffer pin; the clear is
/// idempotent and runs AFTER the agg result is computed, so output bytes
/// cannot depend on it).
pub(super) struct IndexScanFeedSource<'a, 'mcx> {
    is: &'a mut ::nodeindexscan::IndexScanState<'mcx>,
    /// Granule total published by `granule_map`; `position` validates the
    /// whole-range claim against it. `None` until `granule_map` ran.
    total: Option<u64>,
    positioned: bool,
}

impl<'a, 'mcx> IndexScanFeedSource<'a, 'mcx> {
    #[inline]
    pub(super) fn new(is: &'a mut ::nodeindexscan::IndexScanState<'mcx>) -> Self {
        IndexScanFeedSource { is, total: None, positioned: false }
    }

    /// Inc-1 bridge for the seam→operator-pull adapter (the
    /// [`IndexOnlyScanSource::ios_mut`] precedent, removed when the trait
    /// read face lands): the staged run is consumed through the node fn
    /// (`index_scan_batch_fetch`) on `&mut IndexScanState`.
    #[inline]
    fn is_mut(&mut self) -> &mut ::nodeindexscan::IndexScanState<'mcx> {
        self.is
    }
}

impl<'mcx> BatchGranuleSource<'mcx> for IndexScanFeedSource<'_, 'mcx> {
    fn granule_map(
        &mut self,
        _estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<runtime::GranuleMap>> {
        // Leaf-page UPPER-BOUND estimate: total blocks of the INDEX relation
        // (metapage + internal pages counted too — pacing only, the
        // IOS leaf-estimate posture verbatim). `None` = no open index
        // relation (parked skeleton) — the caller refuses, fail-closed.
        let Some(index_rel) = self.is.iss_RelationDesc.as_ref() else {
            return Ok(None);
        };
        let nblocks = ::bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
            index_rel,
            ::types_core::ForkNumber::MAIN_FORKNUM,
        )?;
        // AGGIDX-PAR increment point 2: zero-block geometry publishes the
        // degenerate empty map (one empty whole-range claim; drain runs to
        // the AM's own exhaustion — byte-identical to the fused drive on
        // this shape, error surface included). Increment killed = the AE2
        // fail-closed refusal.
        let Some(total) = index_feed_geometry(nblocks as u64, agg_indexfeed_par_enabled()) else {
            return Ok(None);
        };
        self.total = Some(total);
        Ok(Some(runtime::GranuleMap::unbounded(total, IOS_STARTUP_C0)))
    }

    fn position(
        &mut self,
        _estate: &mut EStateData<'mcx>,
        seg: runtime::MorselRange,
    ) -> PgResult<()> {
        validate_serial_claim(self.positioned, self.total, &seg)
            .map_err(|why| indexsource_misuse(why))?;
        // dop-1 whole-range claim: the node's natural TID-run walk IS the
        // positioned segment — no AM seek to perform (the IOS posture).
        self.positioned = true;
        Ok(())
    }

    #[inline]
    fn next_batch(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<u32> {
        if !self.positioned {
            return Err(indexsource_misuse("next_batch before position"));
        }
        ::nodeindexscan::index_scan_next_tidrun(self.is, estate)
    }

    fn end_claim(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        // Scan-slot hygiene at settle (R3): the slot may hold the last
        // fetched heap tuple (buffer pin); clearing is idempotent and
        // matches C's IndexNext exhaustion clear. IndexScan has no separate
        // table slot (heap tuples land in the scan slot itself).
        let mcx = estate.es_query_cxt;
        ::exectuples::exec_clear_tuple(estate.slot_mut(self.is.ss.ss_ScanTupleSlot), mcx);
        self.positioned = false; // claim settled; a NEW claim may position
        Ok(())
    }

    fn capabilities(&self) -> SourceCaps {
        // Granule = one index leaf page, positional posture; heap tuples
        // are fetched per TID (never staged as pages), so every other
        // capability is honestly false.
        SourceCaps {
            columnar: false,
            heap_pages: false,
            dict_codes: false,
            zone_maps: false,
            all_visible_batches: false,
            index_leaf: true,
        }
    }
}

/// Seam→operator-pull adapter for the IndexScan feed (the [`SeamAggSource`]
/// twin). Call-for-call identical to procnode's fused `IndexScanBatchSource`:
/// `next_batch` = `index_scan_next_tidrun` (through the seam), `fetch_tuple`
/// = `index_scan_batch_fetch` (entry `i>0` advances the AM cursor — the run
/// is consumed 0,1,2,… without gaps), no qual, and `storeless_ok` = FALSE:
/// visibility resolves per TID in `fetch_tuple` (the fused source's stated
/// override — a storeless drain would count dead rows on churned heaps).
struct SeamIndexAggSource<'s, 'a, 'mcx> {
    src: &'s mut IndexScanFeedSource<'a, 'mcx>,
    outer_slot: ExecSlotId,
}

impl<'mcx> ::nodeagg::AggBatchSource<'mcx> for SeamIndexAggSource<'_, '_, 'mcx> {
    #[inline]
    fn next_batch(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<u32> {
        BatchGranuleSource::next_batch(self.src, estate)
    }

    #[inline]
    fn fetch_tuple(&mut self, i: u32, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        ::nodeindexscan::index_scan_batch_fetch(self.src.is_mut(), estate, i)
    }

    #[inline]
    fn outer_slot(&self) -> ExecSlotId {
        self.outer_slot
    }

    #[inline]
    fn has_qual(&self) -> bool {
        false
    }

    // Visibility resolves in fetch_tuple (the fused IndexScanBatchSource
    // override, load-bearing on HOT-churned/dead-row heaps).
    #[inline]
    fn storeless_ok(&self) -> bool {
        false
    }
}

/// Try to let the lane host the fused agg-over-IndexScan drive through the
/// storage seam (`SELECT agg(..) FROM t WHERE indexed-range` PLAIN drains
/// and the HASHED drainable GROUP BY shapes the fused arm #2 owns — exactly
/// `agg_batch_drainable`'s set under procnode `agg_fusible_common`'s legs).
///
/// `Some(result)` = the lane drove this call; `None` = refused, the caller
/// falls through to the UNCHANGED fused/per-tuple paths (always byte-safe:
/// the drive here is `exec_agg_batched` over the same primitives in the
/// same order, so knob-ON output is byte-identical by construction).
///
/// Refuse-set, in order: the knob (silent — accounting doc above); the
/// fused arm's agg gate (`agg_batch_drainable`, silent — its estate legs
/// are re-checked in the child refuse-set); the child refuse-set
/// `index_scan_refuse_reason_ex` (EPQ / backward / scroll-mark /
/// instrumented / non-MVCC / qual-proj / runtime-keys / order-by-reorder /
/// non-btree, with the parallel-aware row admitted under the AGGIDX-PAR
/// increment — see [`AGG_INDEXFEED_PAR`] point 1; increment killed, the
/// AE2-verbatim set returns); the seam geometry probe (`granule_map`
/// `None` = fail-closed fall-through; zero-block geometry covered under
/// the increment, point 2).
#[inline]
pub fn try_own_agg_over_index_source<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    is: &mut ::nodeindexscan::IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if !agg_indexfeed_enabled() {
        // Knob OFF: silent fall-through — today's bytes AND today's
        // accounting (the fused arm below owns the shape exactly as before).
        return Ok(None);
    }
    // Agg-side gate, silent (the sorted-agg hook ahead of this one already
    // ticked AggBuild/AggNotDrainable for this offer — accounting doc).
    if !::nodeagg::agg_batch_drainable(agg) {
        return Ok(None);
    }
    // Child refuse-set (per-pull cadence, the IndexScan class's documented
    // tick semantics). AGGIDX-PAR: parallel-aware admitted when the
    // increment is armed — every other row applies identically.
    if let Some(r) = super::index_scan_refuse_reason_ex(is, estate, agg_indexfeed_par_enabled()) {
        stats::tick_refused(ShapeClass::IndexScan, r);
        engine_mirror_is(is, estate, Some(r));
        return Ok(None);
    }
    // Emit phase of a filled hash agg (and the done plain agg's final
    // pull): the fused drive runs no source work there, so the seam
    // ceremony below would be NEW per-pull work — skip it and stay
    // work-identical (the WS-F posture verbatim).
    let hashed_emit = ::nodeagg::agg_hash_table_filled(agg);
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    let outer_slot = is.ss.ss_ScanTupleSlot;
    let parallel_aware = is.iss_ParallelAware;
    let mut src = IndexScanFeedSource::new(is);
    if !hashed_emit {
        // The feed event: geometry + one whole-range claim. Under a
        // parallel-aware scan this runs ONCE PER WORKER (each worker's node
        // is its own feed; the claim is worker-local bookkeeping — the
        // shared page cursor lives inside the AM's parallel scan state).
        let Some(map) = src.granule_map(estate)? else {
            // Cannot express granules (no open index relation): fail closed
            // to the fused arm — byte-identical drive, nothing lost.
            super::lane_trace("aggindexfeed: refused (no granule geometry)");
            return Ok(None);
        };
        src.position(estate, 0..map.total())?;
        // OWNED: one tick per lane-owned feed event (accounting doc; under
        // parallel that is one tick per worker feed — N feed events exist).
        stats::tick_owned(ShapeClass::IndexScan);
        engine_mirror_is(src.is, estate, None);
        #[cfg(test)]
        AGG_INDEXFEED_OWNED_FOR_TESTS.fetch_add(1, Relaxed);
        if super::lane_trace_enabled() {
            super::lane_trace(&format!(
                "aggindexfeed: owned agg-over-indexscan feed (granules={}, parallel={})",
                map.total(),
                parallel_aware
            ));
        }
    }
    let result = ::nodeagg::exec_agg_batched(
        agg,
        estate,
        SeamIndexAggSource { src: &mut src, outer_slot },
    )?;
    if !hashed_emit {
        // Claim settle (ownership ABI R3: slot hygiene; zero pins held).
        src.end_claim(estate)?;
    }
    Ok(Some(result))
}

/// EXPLAIN (ENGINE) production-verdict mirror for the IndexScan feed
/// chokepoint (the [`engine_mirror`] twin; same WS-C reachability gap —
/// wrapped `Instrumented` children never reach procnode's concrete
/// `IndexScan` agg-arm match, so agg-over-IndexScan capture breadth rides
/// WS-C's D3 item 2 exactly as the IOS composition does).
#[inline]
fn engine_mirror_is(
    is: &::nodeindexscan::IndexScanState<'_>,
    estate: &mut EStateData<'_>,
    refuse: Option<RefuseReason>,
) {
    if estate.engine_capture() {
        if let Some(idx) = is.ss.instr_idx {
            super::engine_record_verdict(estate, idx as i32, ShapeClass::IndexScan, refuse);
        }
    }
}

// --- end WS-AE (wave-8) --------------------------------------------------------

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

    // --- WS-AE (wave-8) ---

    /// The agg-indexfeed knob lever (independent cell from the WS-F knob:
    /// flipping one must never arm the other).
    #[test]
    fn aggindexfeed_knob_set_for_tests_flips() {
        agg_indexfeed_set_for_tests(false);
        assert!(!agg_indexfeed_enabled());
        agg_indexfeed_set_for_tests(true);
        assert!(agg_indexfeed_enabled());
        // Independence: the WS-AE knob ON leaves the WS-F knob untouched.
        indexsource_set_for_tests(false);
        assert!(agg_indexfeed_enabled());
        assert!(!indexsource_enabled());
        agg_indexfeed_set_for_tests(false);
        assert!(!agg_indexfeed_enabled());
    }

    // --- end WS-AE (wave-8) ---

    // --- AGGIDX-PAR increment ---

    /// The increment knob lever, independent from BOTH parent knobs
    /// (flipping the increment must never arm/disarm the feed or the WS-F
    /// cell — the kill switch restores the AE2 admission set only).
    #[test]
    fn aggindexfeed_par_knob_set_for_tests_flips() {
        agg_indexfeed_par_set_for_tests(false);
        assert!(!agg_indexfeed_par_enabled());
        agg_indexfeed_par_set_for_tests(true);
        assert!(agg_indexfeed_par_enabled());
        // Independence both ways.
        agg_indexfeed_set_for_tests(false);
        assert!(agg_indexfeed_par_enabled());
        assert!(!agg_indexfeed_enabled());
        indexsource_set_for_tests(false);
        assert!(agg_indexfeed_par_enabled());
        agg_indexfeed_par_set_for_tests(false);
        assert!(!agg_indexfeed_par_enabled());
    }

    /// The geometry verdict: the zero-block row is covered only under the
    /// increment (killed = the AE2 fail-closed refusal); non-zero geometry
    /// is unaffected by the lever. The degenerate empty map composes with
    /// the claim discipline (`serial_claim_discipline`'s Some(0)/0..0 row).
    /// SE-AGGIOS: the same pure verdict now also serves the IOS feed's
    /// geometry (under its own [`INDEXSOURCE_PAR`] lever) — these rows bind
    /// both feeds.
    #[test]
    fn index_feed_geometry_verdicts() {
        // AE2 behavior (increment killed): zero blocks refuse fail-closed.
        assert_eq!(index_feed_geometry(0, false), None);
        // Increment armed: the empty map publishes (total 0 — exactly one
        // empty whole-range claim is legal, validated by the shared
        // discipline unit).
        assert_eq!(index_feed_geometry(0, true), Some(0));
        // Non-degenerate geometry: lever-invariant.
        assert_eq!(index_feed_geometry(1, false), Some(1));
        assert_eq!(index_feed_geometry(1, true), Some(1));
        assert_eq!(index_feed_geometry(4096, false), Some(4096));
        assert_eq!(index_feed_geometry(4096, true), Some(4096));
    }

    // --- end AGGIDX-PAR increment ---

    // --- SE-AGGIOS increment ---

    /// The IOS increment knob lever, independent from EVERY other knob in
    /// this module (flipping it must never arm/disarm the IOS feed, the
    /// IndexScan feed, or that feed's own increment — the kill switch
    /// restores the WS-F inc-1 admission set only).
    #[test]
    fn indexsource_par_knob_set_for_tests_flips() {
        indexsource_par_set_for_tests(false);
        assert!(!indexsource_par_enabled());
        indexsource_par_set_for_tests(true);
        assert!(indexsource_par_enabled());
        // Independence: the IOS increment lever leaves the other three
        // cells untouched (and vice versa).
        indexsource_set_for_tests(false);
        assert!(indexsource_par_enabled());
        assert!(!indexsource_enabled());
        agg_indexfeed_set_for_tests(false);
        assert!(indexsource_par_enabled());
        assert!(!agg_indexfeed_enabled());
        agg_indexfeed_par_set_for_tests(false);
        assert!(indexsource_par_enabled());
        assert!(!agg_indexfeed_par_enabled());
        indexsource_par_set_for_tests(false);
        assert!(!indexsource_par_enabled());
        agg_indexfeed_par_set_for_tests(true);
        assert!(!indexsource_par_enabled());
        assert!(agg_indexfeed_par_enabled());
    }

    // --- end SE-AGGIOS increment ---
}
