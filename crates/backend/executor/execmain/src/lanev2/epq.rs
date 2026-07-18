//! Lane-side EPQ — WS-Y wave-7, the ladder's inc-5 rungs Y0-Y2
//! (docs/design/lane-epq.md §2/§6; wave-7 contract §1).
//!
//! This module is the lane home of EPQ-capture work. It holds:
//!
//! * **Y0 — the capture substrate**: [`EpqCapturedSource`], the
//!   captured-singleton [`BatchGranuleSource`] flavor of lane-epq.md §2 —
//!   a one-row source fed from the owner's swapped-in `EpqSubs`
//!   (`relsubs_slot` test tuple or `origslot` rowmark row) whose
//!   exhaustion state IS the `relsubs_done`/`relsubs_blocked` latches.
//!   DARK CODE this wave: constructible only under `PGRUST_LANE_V2_EPQ`
//!   inside an active recheck (`es_epq_active`), and nothing drives it in
//!   production until rung Y3 (the census-gated es_epq_active lift)
//!   lands. The child-EState port is REJECTED PERMANENTLY (lane-epq.md
//!   §4): this source reads the ONE parent estate's swapped-in subs —
//!   any drift back toward a private recheck estate is a contract
//!   violation, not a judgment call.
//!
//! * **Y1 — per-node verdicts, memoized per plan**: the wave-5 refuse-all
//!   chokepoint (`lanev2::epq_recheck_refuse_all`) widened into
//!   [`epq_recheck_admission`] — one [`EpqNodeVerdict`] per mappable
//!   recheck-plan node, reusing the EXISTING engagement classes (vocab
//!   mint count: zero) and MEMOIZED per (plan node, recheck plan) in
//!   [`EpqPlanVerdicts`] (wave-5 review finding 5, ledgered in
//!   lane-epq.md §6 + notes/se-ws-u-epq-inc1.md: classification is a
//!   plan-shape function — paid once per plan, never once per recheck
//!   row). The REFUSAL accounting keeps wave-5 semantics: while the
//!   es_epq_active HARD LAW stands (it lifts at Y3 only, in one step,
//!   under the 100% census gate), every classified node still refuses
//!   through the existing `epq` carrier at every recheck initiation —
//!   the tick re-fires from the memo, the walk does not re-run.
//!
//! `check_epq_plan` (crate::epq) stays THE loud admission list (Y2): this
//! module's verdict map covers exactly the tags that list admits, and the
//! two walks are unit-pinned against each other.

use ::executils::{EStateData, ExecSlotId};
use ::mcx::{Mcx, PgVec};
use ::types_error::{PgError, PgResult, ERROR};
use ::types_nodes::Node;

use super::batch_source::{BatchGranuleSource, SourceCaps};
use super::stats::{self, RefuseReason, ShapeClass};

// ---------------------------------------------------------------------------
// Y1 — per-node verdicts, memoized per recheck plan
// ---------------------------------------------------------------------------

/// The per-node admission verdict for one recheck-plan node. STRUCTURAL
/// (plan-shape) facts only — never census statistics: the verdict says what
/// surface exists at this head, and the Y3 gate separately requires every
/// admitted shape census-green before the lift.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum EpqNodeVerdict {
    /// A scan shape the Y0 captured-singleton substrate can feed when its
    /// rel is captured (test slot parked / rowmark present), AND whose
    /// plain-rel fallthrough (a real rescan inside the recheck — the
    /// join-source case) has a `try_own_*` lane surface today.
    CaptureScan,
    /// A non-scan shape recomposed over its children inside the recheck; a
    /// `try_own_*` lane surface exists for the class at this head.
    RescanComposed,
    /// No unconditional lane-ownership surface exists for the shape at this
    /// head — census-short BY CONSTRUCTION: a Y3 gate-delta row (the shape
    /// is admitted by `check_epq_plan` but cannot be census-green until an
    /// owner lands its lane surface).
    Short,
}

::mcx::forget_safe_nodrop!(EpqNodeVerdict);

/// The memoized per-plan verdict list (Y1's memoization law): one entry per
/// mappable node of ONE recheck plan, in `check_epq_plan` walk order.
/// Owner-held in `crate::epq::EpqState::lane_verdicts` — the recheck plan
/// is fixed at plan init (procnode.rs builds `EpqState` once; there is no
/// dynamic SetPlan reset today), so the cache's lifetime IS the plan's.
pub(crate) struct EpqPlanVerdicts<'mcx> {
    entries: PgVec<'mcx, (ShapeClass, EpqNodeVerdict)>,
}

::mcx::forget_safe_struct!(EpqPlanVerdicts<'_> { entries });

// The tuple element type of the memo must be ForgetSafe; ShapeClass is a
// no-drop leaf vocabulary enum (stats.rs) with no impl of its own yet.
::mcx::forget_safe_nodrop!(ShapeClass);

/// Test-only probe: classification WALKS (not ticks) — the memoization
/// unit proves one walk per plan however many rechecks re-initiate.
#[cfg(test)]
pub(crate) static EPQ_CLASSIFY_WALKS_FOR_TESTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Wave-7 Y1 chokepoint (widens wave-5's `epq_recheck_refuse_all`; the ONE
/// call site is `crate::epq::eval_plan_qual`, knob-ON at recheck initiation
/// only — never per row, never per batch). First initiation classifies the
/// plan into `cache` (ONE walk per recheck plan — the memoization law);
/// every initiation re-ticks the refusals from the memo: while the
/// es_epq_active HARD LAW stands, every mappable node refuses through the
/// EXISTING `epq` carrier, exactly as wave-5 (census semantics preserved:
/// refusal counts still scale with recheck initiations). `#[cold]`:
/// reachable only knob-ON on the TM_Updated conflict path.
#[cold]
#[inline(never)]
pub(crate) fn epq_recheck_admission<'mcx>(
    plan: Option<Node<'_>>,
    cache: &mut Option<EpqPlanVerdicts<'mcx>>,
    mcx: Mcx<'mcx>,
) {
    if cache.is_none() {
        let mut entries = PgVec::new_in(mcx);
        classify_into(plan, &mut entries);
        #[cfg(test)]
        EPQ_CLASSIFY_WALKS_FOR_TESTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        *cache = Some(EpqPlanVerdicts { entries });
    }
    for &(class, _verdict) in cache.as_ref().expect("just ensured").entries.iter() {
        stats::tick_refused(class, RefuseReason::Epq);
        #[cfg(test)]
        super::EPQ_ADMISSION_REFUSED_FOR_TESTS
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// The classification walk: EXACTLY `check_epq_plan`'s edges (Append member
/// list, lefttree/righttree, SubqueryScan.subplan) — the two walks are
/// unit-pinned to visit the same nodes so the loud admission list and the
/// verdict map can never drift apart silently.
fn classify_into<'mcx>(
    plan: Option<Node<'_>>,
    entries: &mut PgVec<'mcx, (ShapeClass, EpqNodeVerdict)>,
) {
    let Some(plan) = plan else { return };
    if let Some(row) = epq_recheck_verdict(plan) {
        entries.push(row);
    }
    if let Some(ap) = plan.as_append() {
        for child in ap.appendplans.iter() {
            classify_into(Some(child), entries);
        }
    }
    if let Some(sq) = plan.as_subquery_scan() {
        classify_into(sq.subplan, entries);
    }
    if let Some(p) = plan.as_plan() {
        if let Some(l) = p.lefttree {
            classify_into(Some(l), entries);
        }
        if let Some(r) = p.righttree {
            classify_into(Some(r), entries);
        }
    }
}

/// The `check_epq_plan` whitelist tags -> (EXISTING engagement class,
/// wave-7 verdict). Reuse only — the unmappable glue tags (Limit / Hash /
/// BitmapIndexScan) return None and tick nothing (vocab law §0.7: no new
/// classes, no new reasons; identical to the wave-5 map's glue handling).
///
/// Verdicts are structural facts at this head, mechanically derived from
/// the `try_own_*` inventory in lanev2.rs (the census delta of
/// notes/se-wave7-epq.md re-derives this table; the Short rows ARE the Y3
/// gate delta):
///   * CaptureScan      — Y0 substrate + a try_own_* fallthrough surface
///   * RescanComposed   — try_own_* surface for the composed shape
///   * Short            — NO try_own_* surface exists (TidScan,
///                        TidRangeScan, MergeJoin, Material, ValuesScan,
///                        CteScan, FunctionScan, LockRows)
fn epq_recheck_verdict(plan: Node<'_>) -> Option<(ShapeClass, EpqNodeVerdict)> {
    use ::types_nodes::NodeTag as T;
    use EpqNodeVerdict as V;
    Some(match plan.node_tag() {
        T::T_SeqScan => (ShapeClass::SeqScan, V::CaptureScan),
        T::T_IndexScan => (ShapeClass::IndexScan, V::CaptureScan),
        T::T_IndexOnlyScan => (ShapeClass::IndexOnlyScan, V::CaptureScan),
        T::T_BitmapHeapScan => (ShapeClass::BitmapHeapScan, V::CaptureScan),
        // Tid scans: the Y0 substrate can feed their CAPTURED case, but no
        // try_own_tid_scan/_tid_range_scan surface exists for the plain-rel
        // fallthrough (and the AM recheckMtd divergence — TidRecheck's
        // bsearch vs TidRangeRecheck's range re-compare, pinned by
        // epq-storm-tid — is unwired on lanes): structurally Short.
        T::T_TidScan => (ShapeClass::TidScan, V::Short),
        T::T_TidRangeScan => (ShapeClass::TidRangeScan, V::Short),
        T::T_NestLoop => (ShapeClass::NestLoop, V::RescanComposed),
        T::T_MergeJoin => (ShapeClass::MergeJoin, V::Short),
        T::T_HashJoin => (ShapeClass::Join, V::RescanComposed),
        T::T_Sort => (ShapeClass::SortFeed, V::RescanComposed),
        T::T_Material => (ShapeClass::Material, V::Short),
        T::T_Result => (ShapeClass::ResultNode, V::RescanComposed),
        T::T_ValuesScan => (ShapeClass::ValuesScan, V::Short),
        T::T_CteScan => (ShapeClass::CteScan, V::Short),
        T::T_SubqueryScan => (ShapeClass::SubqueryScan, V::RescanComposed),
        T::T_FunctionScan => (ShapeClass::FunctionScan, V::Short),
        T::T_Append => (ShapeClass::Append, V::RescanComposed),
        T::T_LockRows => (ShapeClass::LockRows, V::Short),
        _ => return None,
    })
}

// ---------------------------------------------------------------------------
// Y0 — the captured-singleton BatchGranuleSource flavor (dark this wave)
// ---------------------------------------------------------------------------

/// Which `EpqSubs` cell feeds the captured row (lane-epq.md §2's two
/// captured-source flavors).
// DARK CODE (wave-7 Y0): no production caller exists until rung Y3 (the
// census-gated es_epq_active lift) wires recheck source selection; the
// unit corpus (tests.rs epq_capture_w7, band 83001+) is the only driver.
#[allow(dead_code)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum EpqCaptureFeed {
    /// `relsubs_slot[scanrelid-1]` — the parked EPQ test tuple (C
    /// ExecScanFetch's test-slot arm; returned exactly once via the
    /// `relsubs_done` latch).
    TestSlot,
    /// `EpqSubs::origslot` — the row under recheck, the feed the rowmark
    /// arm materializes from (C EvalPlanQualFetchRowMark; the
    /// ROW_MARK_REFERENCE ctid re-fetch / ROW_MARK_COPY wholerow
    /// materialization COMPOSITION stays with `execscan::
    /// epq_fetch_row_mark` and is Y3 wiring — this source feeds the
    /// origslot row, per the wave-7 contract's Y0 scope).
    OrigSlot,
}

/// Y0: the captured-singleton source — a capacity-1 [`BatchGranuleSource`]
/// whose one "granule" is the captured row and whose exhaustion state IS
/// the owner's `relsubs_done` latch (lane-epq.md §2: "a one-element
/// captured batch, done = source exhausted"). Reads the ONE parent
/// estate's swapped-in `EpqSubs` (capture model, §4 — no child estate,
/// ever). Per-row emit face only (`batch_soa` = None): the captured row is
/// already a slot; there is nothing to stage columnar-wise.
// DARK CODE (wave-7 Y0): constructed only by the unit corpus until Y3 —
// see `EpqCaptureFeed`'s note.
#[allow(dead_code)]
pub(super) struct EpqCapturedSource {
    /// `scanrelid - 1`, indexing the swapped-in relsubs arrays.
    idx: usize,
    feed: EpqCaptureFeed,
    /// `position` accepted the singleton claim window.
    positioned: bool,
    /// The staged one-row batch (valid until the next `&mut` call — ABI R1).
    staged: Option<ExecSlotId>,
}

impl EpqCapturedSource {
    /// DARK-CODE constructor (wave-7 contract Y0): refuses — returns None —
    /// unless `PGRUST_LANE_V2_EPQ` is armed AND the estate is inside an
    /// active recheck with the owner's subs swapped in, AND the requested
    /// feed cell is populated for the rel. Fail-closed: a None here means
    /// the caller keeps the Volcano recheck drive (which is the ONLY drive
    /// until Y3 lands).
    #[allow(dead_code)] // dark until Y3 wires recheck source selection
    pub(super) fn for_recheck(
        estate: &EStateData<'_>,
        scanrelid: u32,
        feed: EpqCaptureFeed,
    ) -> Option<EpqCapturedSource> {
        if !super::epq_lane_enabled() || !estate.es_epq_active {
            return None;
        }
        let subs = estate.es_epq.as_ref()?;
        let idx = (scanrelid.checked_sub(1)?) as usize;
        if idx >= subs.relsubs_slot.len() {
            return None;
        }
        let available = match feed {
            EpqCaptureFeed::TestSlot => subs.relsubs_slot[idx].is_some(),
            EpqCaptureFeed::OrigSlot => subs.origslot.is_some(),
        };
        if !available {
            // No captured cell: the rel is the plain-rescannable case
            // (join-source), which is NOT this source's shape.
            return None;
        }
        Some(EpqCapturedSource { idx, feed, positioned: false, staged: None })
    }
}

fn epq_capture_misuse(what: &str) -> Box<PgError> {
    Box::new(PgError::new(
        ERROR,
        format!("EPQ captured source misuse (lane-epq.md §2): {what}"),
    ))
}

impl<'mcx> BatchGranuleSource<'mcx> for EpqCapturedSource {
    fn granule_map(
        &mut self,
        _estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<runtime::GranuleMap>> {
        // ONE granule: the captured row. No interior boundaries, seed 1.
        Ok(Some(runtime::GranuleMap::unbounded(1, 1)))
    }

    fn position(
        &mut self,
        _estate: &mut EStateData<'mcx>,
        seg: runtime::MorselRange,
    ) -> PgResult<()> {
        // EXACTLY the singleton window: empty claims (0..0) refuse too —
        // an accepted empty window would still hand out the captured row
        // from next_batch against a zero-width claim (wave-7 review
        // finding 3; fail-closed like the constructor arms).
        if seg != (0..1) {
            return Err(epq_capture_misuse("claim != the singleton granule window"));
        }
        self.positioned = true;
        self.staged = None;
        Ok(())
    }

    fn next_batch(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<u32> {
        if !self.positioned {
            return Err(epq_capture_misuse("next_batch before position"));
        }
        let subs = estate
            .es_epq
            .as_mut()
            .ok_or_else(|| epq_capture_misuse("subs swapped out mid-claim"))?;
        // relsubs_done is the exactly-once latch; relsubs_blocked was
        // reloaded into done by EvalPlanQualBegin's rescan arm, so a
        // blocked rel reads done here (the writep4a/writep4b inheritance
        // class: every sibling result rel stays blocked+done except the
        // one under test).
        if subs.relsubs_done[self.idx] {
            self.staged = None;
            return Ok(0);
        }
        let slot = match self.feed {
            EpqCaptureFeed::TestSlot => subs.relsubs_slot[self.idx],
            EpqCaptureFeed::OrigSlot => subs.origslot,
        };
        let Some(slot) = slot else {
            // Constructor checked availability; a raced clear is a
            // fail-closed empty source, never a panic.
            self.staged = None;
            return Ok(0);
        };
        // Latch BEFORE handing out the row (C ExecScanFetch: mark
        // relsubs_done when the test tuple is returned, so the next pull
        // of this scan inside the same recheck sees the cleared slot).
        subs.relsubs_done[self.idx] = true;
        self.staged = Some(slot);
        Ok(1)
    }

    fn end_claim(&mut self, _estate: &mut EStateData<'mcx>) -> PgResult<()> {
        // Zero pins by construction: the captured row lives in the parent
        // estate's tuple table (relsubs slots / origslot), never a page
        // image — nothing to release (ABI R3's zero-pins-at-settle law
        // holds vacuously).
        self.staged = None;
        self.positioned = false;
        Ok(())
    }

    fn capabilities(&self) -> SourceCaps {
        SourceCaps {
            columnar: false,
            heap_pages: false,
            dict_codes: false,
            zone_maps: false,
            all_visible_batches: false,
            index_leaf: false,
        }
    }

    fn emit(
        &mut self,
        _estate: &mut EStateData<'mcx>,
        i: u32,
    ) -> PgResult<Option<ExecSlotId>> {
        if i != 0 {
            return Err(epq_capture_misuse("emit past the singleton row"));
        }
        match self.staged {
            Some(slot) => Ok(Some(slot)),
            None => Err(epq_capture_misuse("emit with no staged batch")),
        }
    }
}

// ---------------------------------------------------------------------------
// Test probes (the unit corpus lives in crate::tests with the exec fixtures;
// the trait and vocabulary stay lanev2-private, so the probes speak slots,
// names and counts only).
// ---------------------------------------------------------------------------

/// One full captured-source ladder observation for the unit corpus.
#[cfg(test)]
pub(crate) struct EpqCaptureProbe {
    pub granule_total: u64,
    pub first_batch: u32,
    pub emitted: Option<ExecSlotId>,
    pub second_batch: u32,
    /// `relsubs_done[idx]` observed AFTER the ladder (the exactly-once latch).
    pub done_latched: bool,
    /// emit after `end_claim` refused with a loud PgError (never a panic).
    pub reemit_refused: bool,
    /// an EMPTY claim window (0..0) refused with a loud PgError (wave-7
    /// review finding 3: only the exact singleton window positions).
    pub empty_claim_refused: bool,
}

/// Drive the Y0 source through its whole ladder (construct -> granule_map ->
/// position -> next_batch -> emit -> next_batch -> end_claim -> emit).
/// `Ok(None)` = the dark-code constructor refused (knob off / not in a
/// recheck / feed cell empty) — the fail-closed arm the units pin.
#[cfg(test)]
pub(crate) fn epq_captured_probe_for_tests<'mcx>(
    estate: &mut EStateData<'mcx>,
    scanrelid: u32,
    feed: EpqCaptureFeed,
) -> PgResult<Option<EpqCaptureProbe>> {
    let Some(mut src) = EpqCapturedSource::for_recheck(estate, scanrelid, feed) else {
        return Ok(None);
    };
    let map = src.granule_map(estate)?.expect("captured source has geometry");
    let empty_claim_refused = src.position(estate, 0..0).is_err();
    src.position(estate, 0..1)?;
    let first_batch = src.next_batch(estate)?;
    let emitted = if first_batch > 0 { src.emit(estate, 0)? } else { None };
    let second_batch = src.next_batch(estate)?;
    src.end_claim(estate)?;
    let reemit_refused = src.emit(estate, 0).is_err();
    let done_latched = estate
        .es_epq
        .as_ref()
        .map(|s| s.relsubs_done[(scanrelid - 1) as usize])
        .unwrap_or(false);
    Ok(Some(EpqCaptureProbe {
        granule_total: map.total(),
        first_batch,
        emitted,
        second_batch,
        done_latched,
        reemit_refused,
        empty_claim_refused,
    }))
}

/// Classification probe for the unit corpus: (class name, verdict) rows for
/// one plan, WITHOUT touching any cache or counter.
#[cfg(test)]
pub(crate) fn epq_classify_for_tests(
    plan: Option<Node<'_>>,
) -> Vec<(&'static str, EpqNodeVerdict)> {
    let mcx_owned = ::mcx::MemoryContext::new("epq-classify-probe");
    let mut entries = PgVec::new_in(mcx_owned.mcx());
    classify_into(plan, &mut entries);
    entries.iter().map(|&(c, v)| (c.name(), v)).collect()
}
