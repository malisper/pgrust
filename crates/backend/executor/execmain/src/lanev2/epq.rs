//! Lane-side EPQ — WS-Y wave-7, the ladder's inc-5 rungs
//! (docs/design/lane-epq.md §2/§6; wave-7 contract §1).
//!
//! This module is the lane home of EPQ-capture work. This commit lands
//! **Y0 — the capture substrate**: [`EpqCapturedSource`], the
//! captured-singleton [`BatchGranuleSource`] flavor of lane-epq.md §2 —
//! a one-row source fed from the owner's swapped-in `EpqSubs`
//! (`relsubs_slot` test tuple or `origslot` rowmark row) whose
//! exhaustion state IS the `relsubs_done`/`relsubs_blocked` latches.
//! DARK CODE this wave: constructible only under `PGRUST_LANE_V2_EPQ`
//! inside an active recheck (`es_epq_active`), and nothing drives it in
//! production until rung Y3 (the census-gated es_epq_active lift)
//! lands. The child-EState port is REJECTED PERMANENTLY (lane-epq.md
//! §4): this source reads the ONE parent estate's swapped-in subs —
//! any drift back toward a private recheck estate is a contract
//! violation, not a judgment call.

use ::executils::{EStateData, ExecSlotId};
use ::types_error::{PgError, PgResult, ERROR};

use super::batch_source::{BatchGranuleSource, SourceCaps};

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
        if seg.start != 0 || seg.end > 1 {
            return Err(epq_capture_misuse("claim outside the singleton granule"));
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
    }))
}
