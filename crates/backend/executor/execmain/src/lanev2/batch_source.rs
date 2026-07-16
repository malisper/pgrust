//! THE STORAGE SEAM of the runtime scan drive (single-executor migration
//! §0.1: open / granules / read / capabilities) — [`BatchGranuleSource`],
//! the granule-addressed batch source the drive positions and reads through.
//! One instance per WORKER per scan; heap and pgrcolumnar dispatch live
//! BELOW this seam (nodeseqscan → tableam), where they already lived.
//!
//! Naming (integration contract R-NAME, permanent): the storage seam is
//! `BatchGranuleSource`; `executils::BatchSource` is the operator→operator
//! pull seam (re-exported as `AggBatchSource`) and is never renamed.
//!
//! Increment 1 carries the geometry + positioning faces; the staged-batch
//! READ face (batch_soa / qual_sel / skip_sel / emit accessors) stays on the
//! nodeseqscan fns until the drains go generic (inc-2, routed through
//! `arm_scan_staging` — the one staging seam). Claim-time readahead needs
//! no seam surface: it is already BELOW `position()` (the AM's
//! `set_granule_range` claim-window advise), and passes through untouched.
//!
//! # Batch ownership / pin-lifetime ABI (settled here, per the migration doc)
//!
//! - **R1 CLAIM-SCOPED BATCHES**: a staged batch is valid from
//!   `next_batch() > 0` until the next `next_batch`/`position`/`end_claim`
//!   on the SAME source instance; never retained past that. Today this is
//!   enforced by convention (drains are synchronous inside `run_morsel`);
//!   when the read face lands (inc-2), accessors borrowing `&self` between
//!   `&mut` calls make it borrow-checked.
//! - **R2 WORKER-PRIVATE**: batches never cross threads. Each worker builds
//!   its OWN executor + scan descriptor (`build_worker_exec`); the only
//!   cross-thread currency is (a) `Arc<GranuleMap>` (immutable geometry),
//!   (b) `Arc<Part>`/mmap + SegMap below the seam (immutable storage),
//!   (c) exported partials — deep copies under per-ordinal mutexes.
//! - **R3 HEAP PINS**: the staged heap batch IS one pinned page (`rs_cbuf`);
//!   SoA byref cells and emitted tuples alias the pinned image
//!   (`heap_batch_deform_soa`: "pinned by rs_cbuf for the whole batch").
//!   Pins release on batch advance (n == 0 → end of scan), on reposition
//!   (`heap_set_block_range`'s defensive release), and the scan SLOT's pin
//!   on the drain's end-of-claim `exec_clear_tuple`. LAW: at claim settle
//!   the worker holds zero pins from that claim.
//! - **R4 COLUMNAR SCRATCH**: staged cells alias per-scan decode scratch
//!   (ColDecode datums/dict/arena) rebuilt at granule/RG grain — NOT the
//!   mmap. Validity therefore requires epoch-integral claims: enforced at
//!   DEFINITION (`GranuleMap` boundaries = `Part::granule_starts`) and
//!   re-checked at position (`set_granule_range`'s cross-RG error).
//!   Coalesced multi-epoch claims are legal ONLY for consumers that
//!   subdivide via `GranuleMap::segments` (the scan drive's `morsel_body`;
//!   sink drains feed claims straight to `set_granule_range` and must not
//!   coalesce).
//! - **R5 CARRY-OVER MEMOS**: worker-private reader memoization across
//!   claims (same-RG `rg_checked` carry + dict scratch reuse) is permitted
//!   only for pure per-RG predicates under the engagement snapshot.
//! - **R6 STEALING (the morsel-runtime-v2 stealing/NUMA law)**: by R1+R2 a
//!   stolen or re-split granule has no batch state — stealing/shedding is
//!   claim-level only (today's shed happens between morsels; a claim is
//!   executed whole by its claimer). A batch NEVER outlives its claim; if a
//!   scheduler ever wants mid-claim handoff, the unconsumed REMAINDER of
//!   the claim (a granule range) is what changes hands, never staged state.
//!   Sources wanting cross-worker decoded reuse must publish source-level
//!   immutable shared state (`Arc<Part>`-class), never staged batches.
//!   NUMA-affine claiming moves the CLAIM, never the batch.

use std::sync::Arc;

use ::executils::EStateData;
use ::types_error::{PgError, PgResult, ERROR};

/// Capabilities of a granule-addressed batch source (migration-doc
/// "capabilities" face; grows honestly per increment — no speculative
/// flags).
#[derive(Clone, Copy)]
#[allow(dead_code)] // read by the inc-2 read-face consumers (tests read them today)
pub(super) struct SourceCaps {
    /// Columnar staging: granule = the store's 8,192-row unit, hard
    /// boundaries = dictionary epochs, staged cells alias per-scan decode
    /// scratch (ownership ABI R4).
    pub columnar: bool,
    /// Heap page staging: granule = one block, staged batch pins its page
    /// (ownership ABI R3).
    pub heap_pages: bool,
}

/// The storage seam trait; the batch ownership / pin-lifetime ABI (module
/// doc, R1–R6) is this trait's contract.
///
/// Inherited preconditions (stated so a second caller cannot violate them
/// silently): `position` is single-claimer per instance — the heap AM
/// errors on parallel-scan descriptors and the columnar AM debug-asserts
/// its adaptive drive is unarmed; both refuse ranges that cross a hard
/// boundary (`GranuleMap::segments` upholds that above the seam).
#[allow(dead_code)] // read faces (next_batch/end_claim/capabilities) wire in inc-2
pub(super) trait BatchGranuleSource<'mcx> {
    /// OPEN + GRANULES: open the underlying scan exactly as the drive
    /// would (the same `ensure_scandesc` open the geometry probes perform
    /// today) and publish its granule geometry. `None` = the source cannot
    /// express granules (heap 0 blocks / empty part / foreign AM) — the
    /// caller refuses engagement, fail-closed.
    fn granule_map(
        &mut self,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<runtime::GranuleMap>>;

    /// Position the reader on the epoch-integral claim segment
    /// `[seg.start, seg.end)`: whole granules, never crossing a
    /// `GranuleMap` boundary. Claim-time readahead is the source's own
    /// policy, below this call.
    fn position(
        &mut self,
        estate: &mut EStateData<'mcx>,
        seg: runtime::MorselRange,
    ) -> PgResult<()>;

    /// READ (staging half): stage the next batch of the positioned
    /// segment; 0 = segment drained. The staged batch is claim-scoped
    /// (ABI R1) and readable through the node seam (inc-1) / the trait's
    /// read face (inc-2) until the next call on this source.
    fn next_batch(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<u32>;

    /// Release end-of-claim resources (heap: the scan slot's page pin —
    /// ABI R3's zero-pins-at-settle law). Inc-1: the drains still own this
    /// themselves; the method lands with the read face so ownership moves
    /// atomically (never double-owned).
    fn end_claim(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()>;

    fn capabilities(&self) -> SourceCaps;
}

/// The increment-1 implementor: a SeqScan over heap or pgrcolumnar, driven
/// through the existing nodeseqscan/tableam AM dispatch (heap and columnar
/// behavior both live BELOW this seam already — delegation only, so the
/// extraction is code-shape-neutral on the claim hot path).
pub(super) struct SeqScanSource<'a, 'mcx> {
    ss: &'a mut ::nodeseqscan::SeqScanState<'mcx>,
}

impl<'a, 'mcx> SeqScanSource<'a, 'mcx> {
    #[inline]
    pub(super) fn new(ss: &'a mut ::nodeseqscan::SeqScanState<'mcx>) -> Self {
        SeqScanSource { ss }
    }

    /// Inc-1 bridge: the drains keep consuming the staged batch through
    /// the nodeseqscan fns on `&mut SeqScanState` (re-borrow between trait
    /// calls); removed when the read face lands (inc-2).
    #[inline]
    pub(super) fn scan_mut(&mut self) -> &mut ::nodeseqscan::SeqScanState<'mcx> {
        self.ss
    }
}

/// Startup-ramp seed for pgrcolumnar maps: granules are 8,192 rows — large
/// against Umbra's 16-tuple C0; one 2-granule probe morsel (~16K rows, tens
/// of µs on fold shapes) sizes the pipeline without a giant first claim on
/// tiny scans. (Inert under whole-boundary claims; kept for the kill
/// switch.)
const CB_STARTUP_C0: u64 = 2;
/// Startup-ramp seed for heap maps: a block stages ~50-250 tuples — seed 16
/// blocks (128KB, a few thousand rows). Same probe-morsel intent as
/// pgrcolumnar's C0=2.
const HEAP_STARTUP_C0: u64 = 16;

impl<'mcx> BatchGranuleSource<'mcx> for SeqScanSource<'_, 'mcx> {
    fn granule_map(
        &mut self,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<runtime::GranuleMap>> {
        if ::nodeseqscan::seq_scan_is_pgrcolumnar(self.ss) {
            let Some((_, starts)) =
                ::nodeseqscan::seq_scan_cb_granule_geometry(self.ss, estate)?
            else {
                return Ok(None); // empty part
            };
            return Ok(Some(runtime::GranuleMap::with_boundaries(
                Arc::new(starts),
                CB_STARTUP_C0,
            )));
        }
        if ::nodeseqscan::seq_scan_is_heap(self.ss) {
            let Some(nblocks) = ::nodeseqscan::seq_scan_heap_block_geometry(self.ss, estate)?
            else {
                return Ok(None); // empty relation
            };
            return Ok(Some(runtime::GranuleMap::unbounded(nblocks, HEAP_STARTUP_C0)));
        }
        Ok(None)
    }

    #[inline]
    fn position(
        &mut self,
        estate: &mut EStateData<'mcx>,
        seg: runtime::MorselRange,
    ) -> PgResult<()> {
        ::nodeseqscan::seq_scan_set_morsel_range(self.ss, estate, seg.start, seg.end)
    }

    #[inline]
    fn next_batch(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<u32> {
        ::nodeseqscan::seq_scan_next_pagebatch(self.ss, estate)
    }

    fn end_claim(&mut self, _estate: &mut EStateData<'mcx>) -> PgResult<()> {
        // Inc-1: end-of-claim slot-clear is still OWNED by the drains
        // (never double-owned — see the trait doc); real body lands with
        // the read face (inc-2).
        Err(seam_not_wired("end_claim"))
    }

    fn capabilities(&self) -> SourceCaps {
        SourceCaps {
            columnar: ::nodeseqscan::seq_scan_is_pgrcolumnar(self.ss),
            heap_pages: ::nodeseqscan::seq_scan_is_heap(self.ss),
        }
    }
}

/// M1 heap-source skeleton (typed, NEVER constructed in Phase 0): the
/// dedicated heap implementor for when the staged-batch read face moves
/// onto the trait. Today heap flows through [`SeqScanSource`]'s AM dispatch
/// below the seam; this skeleton exists to prove the trait admits a direct
/// heap source (granule = one block, staged batch pins its page — ABI R3,
/// boundary-free `GranuleMap::unbounded` geometry) with no shape changes.
#[allow(dead_code)]
pub(super) struct HeapPageSource<'a, 'mcx> {
    ss: &'a mut ::nodeseqscan::SeqScanState<'mcx>,
}

impl<'mcx> BatchGranuleSource<'mcx> for HeapPageSource<'_, 'mcx> {
    fn granule_map(
        &mut self,
        _estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<runtime::GranuleMap>> {
        Err(seam_not_wired("HeapPageSource"))
    }

    fn position(
        &mut self,
        _estate: &mut EStateData<'mcx>,
        _seg: runtime::MorselRange,
    ) -> PgResult<()> {
        Err(seam_not_wired("HeapPageSource"))
    }

    fn next_batch(&mut self, _estate: &mut EStateData<'mcx>) -> PgResult<u32> {
        Err(seam_not_wired("HeapPageSource"))
    }

    fn end_claim(&mut self, _estate: &mut EStateData<'mcx>) -> PgResult<()> {
        Err(seam_not_wired("HeapPageSource"))
    }

    fn capabilities(&self) -> SourceCaps {
        SourceCaps { columnar: false, heap_pages: true }
    }
}

#[cold]
#[inline(never)]
fn seam_not_wired(what: &str) -> Box<PgError> {
    Box::new(PgError::new(
        ERROR,
        format!("batch source face not wired in this increment: {what}"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Trait-level capability-flags test: the trait is implementable by a
    /// plain stub and the caps flags read back exactly as constructed
    /// (no inference anywhere in the face).
    struct StubSource(SourceCaps);

    impl<'mcx> BatchGranuleSource<'mcx> for StubSource {
        fn granule_map(
            &mut self,
            _estate: &mut EStateData<'mcx>,
        ) -> PgResult<Option<runtime::GranuleMap>> {
            unimplemented!("stub")
        }

        fn position(
            &mut self,
            _estate: &mut EStateData<'mcx>,
            _seg: runtime::MorselRange,
        ) -> PgResult<()> {
            unimplemented!("stub")
        }

        fn next_batch(&mut self, _estate: &mut EStateData<'mcx>) -> PgResult<u32> {
            unimplemented!("stub")
        }

        fn end_claim(&mut self, _estate: &mut EStateData<'mcx>) -> PgResult<()> {
            unimplemented!("stub")
        }

        fn capabilities(&self) -> SourceCaps {
            self.0
        }
    }

    #[test]
    fn capability_flags_read_back() {
        for (columnar, heap_pages) in [(false, false), (true, false), (false, true)] {
            let src = StubSource(SourceCaps { columnar, heap_pages });
            let caps = src.capabilities();
            assert_eq!(caps.columnar, columnar);
            assert_eq!(caps.heap_pages, heap_pages);
        }
    }
}
