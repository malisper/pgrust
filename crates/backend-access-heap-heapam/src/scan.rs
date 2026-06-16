//! The heap access method's SEQUENTIAL-SCAN core (`access/heap/heapam.c`):
//! `initscan` / `heap_setscanlimits` / `heap_prepare_pagescan` (with the
//! `page_collect_tuples` visibility loop), the block-advance helpers
//! `heapgettup_initial_block` / `heapgettup_advance_block` /
//! `heapgettup_start_page` / `heapgettup_continue_page` / `heap_fetch_next_buffer`,
//! the forward/backward `heapgettup` and `heapgettup_pagemode` loops, and the
//! public entry points `heap_beginscan` / `heap_rescan` / `heap_endscan` /
//! `heap_getnext` / `heap_getnextslot` / `heap_set_tidrange` /
//! `heap_getnextslot_tidrange`.
//!
//! ## Owned scan descriptor (convention A)
//!
//! C's `HeapScanDescData` embeds `TableScanDescData rs_base` as its first member
//! and the AM casts the generic `TableScanDesc` back to `HeapScanDesc`. The owned
//! model keeps the generic [`TableScanDescData`] as the value the dispatch layer
//! threads, and the heap-private tail lives in [`HeapScanDescData`], an
//! [`AmOpaque`] payload riding `rs_base.am_private` (the C `void *` made
//! `'mcx`-safe with a tag-checked downcast). The scan functions take a `&mut
//! TableScanDescData<'mcx>` (the C `TableScanDesc sscan`) and downcast
//! `am_private` to `&mut HeapScanDescData` for the heap-private fields.
//!
//! ## Read-stream collapse (SANCTIONED)
//!
//! C prefetches blocks through a `ReadStream` whose callback
//! (`heap_scan_stream_read_next_serial` / `_parallel`) is pure in-crate block
//! arithmetic over the scan it drives — a self-borrow that cannot be modeled as
//! an owned `ReadStream` field. [`heap_fetch_next_buffer`] therefore computes the
//! next block INLINE (the block the read stream would have returned) and pins it
//! with [`bufmgr_seam::read_buffer`], matching the C semantics. `read_stream_reset`
//! on a direction change collapses to resetting `rs_prefetch_block` to the
//! current block.
//!
//! ## Scan keys (SANCTIONED)
//!
//! The trimmed [`ScanKeyData`] carries no `sk_func`/`sk_argument`, so
//! `HeapKeyTest` for `nkeys > 0` cannot run until the scan-key carrier keystone
//! (task #281). It is declared as the [`heapam_seam::heap_key_test`] seam and
//! panics until then; `nkeys == 0` (the executor seqscan path) is fully live.

use mcx::{Mcx, PgBox, PgVec};
use types_core::primitive::{
    BlockNumber, InvalidBlockNumber, OffsetNumber, Oid,
};
use types_error::{PgResult, ERROR, ERRCODE_FEATURE_NOT_SUPPORTED};
use backend_utils_error::ereport;
use types_rel::Relation;
use types_scan::sdir::{
    ScanDirection, ScanDirectionIsBackward, ScanDirectionIsForward,
};
use types_snapshot::snapshot::IsMVCCSnapshot;
use types_snapshot::SnapshotData;
use types_storage::bufpage::{MaxHeapTuplesPerPage, MaxOffsetNumber};
use types_storage::buf::{BufferAccessStrategy, BufferAccessStrategyType};
use types_storage::{Buffer, InvalidBuffer};
use types_tableam::amopaque::{tags, AmOpaque, AmOpaqueTag, AmOpaqueType};
use types_tableam::relscan::{
    ParallelBlockTableScanWorkerData, ParallelTableScanDescData, TableScanDescData,
    SO_ALLOW_PAGEMODE, SO_ALLOW_STRAT, SO_ALLOW_SYNC, SO_TEMP_SNAPSHOT, SO_TYPE_BITMAPSCAN,
    SO_TYPE_SAMPLESCAN, SO_TYPE_SEQSCAN,
};
use types_tableam::scankey::ScanKeyData;
use types_tuple::backend_access_common_heaptuple::FormedTuple;
use types_tuple::heaptuple::{FIRST_OFFSET_NUMBER as FirstOffsetNumber, ItemPointerData};

use backend_storage_page::{
    ItemPointerCompare, ItemPointerGetBlockNumberNoCheck,
    PageGetItem, PageGetItemId, PageGetMaxOffsetNumber, PageIsAllVisible, PageRef,
    ItemIdIsNormal, ItemIdGetLength,
};

use backend_access_heap_heapam_seams as heapam_seam;
use backend_storage_buffer_bufmgr_seams as bufmgr_seam;
use backend_storage_lmgr_predicate_seams as predicate_seam;
use backend_access_common_syncscan_seams as syncscan_seam;
use backend_access_heap_pruneheap_seams as prune_seam;
use backend_utils_cache_relcache_seams as relcache_seam;
use backend_utils_activity_pgstat_seams as pgstat_seam;
use backend_utils_init_small_seams as initsmall_seam;
use backend_access_heap_vacuumlazy_seams as vacuumlazy_seam;
use backend_access_transam_xact_seams as xact_seam;
use backend_utils_time_snapmgr_seams as snapmgr_seam;
use backend_access_table_tableam as tableam;

use backend_access_heap_heapam_visibility::HeapTupleSatisfiesVisibility;

/// `OffsetNumberNext(offsetNumber)` (`storage/off.h`).
#[inline]
fn OffsetNumberNext(offset: OffsetNumber) -> OffsetNumber {
    offset + 1
}

/// `OffsetNumberPrev(offsetNumber)` (`storage/off.h`).
#[inline]
fn OffsetNumberPrev(offset: OffsetNumber) -> OffsetNumber {
    offset - 1
}

/// `BufferIsValid(buffer)` (bufmgr.h).
#[inline]
fn buffer_is_valid(buffer: Buffer) -> bool {
    buffer != InvalidBuffer
}

// ===========================================================================
// HeapScanDescData — the heap-private scan tail (access/heapam.h), riding
// `TableScanDescData.am_private` as a tag-checked AmOpaque payload.
// ===========================================================================

/// `HeapScanDescData` (`access/heapam.h`) minus its embedded `rs_base`
/// `TableScanDescData` (which lives in the generic descriptor; see the module
/// docs). The shared parallel descriptor lives in `rs_base.rs_parallel`; this
/// tail holds the per-backend scan working state.
pub struct HeapScanDescData<'mcx> {
    /// `rs_nblocks` — total number of blocks in the relation.
    pub rs_nblocks: BlockNumber,
    /// `rs_startblock` — block # to start at.
    pub rs_startblock: BlockNumber,
    /// `rs_numblocks` — max number of blocks to scan
    /// (`InvalidBlockNumber` means "all").
    pub rs_numblocks: BlockNumber,

    /* scan current state */
    /// `rs_inited` — false = scan not init'd yet.
    pub rs_inited: bool,
    /// `rs_coffset` — offset of last returned tuple (non-pagemode).
    pub rs_coffset: OffsetNumber,
    /// `rs_cblock` — current block # in scan, if any.
    pub rs_cblock: BlockNumber,
    /// `rs_cbuf` — current buffer in scan, if any (held in pin).
    pub rs_cbuf: Buffer,

    /// `rs_strategy` — access strategy for reads (`BAS_NORMAL`/`NONE` = NULL).
    pub rs_strategy: BufferAccessStrategy,

    /// `rs_ctup` — current tuple in scan, if any. `None` is C's
    /// `rs_ctup.t_data == NULL` "no tuple" sentinel. Carries the full on-page
    /// tuple image (header + user-data area) so the slot store and the
    /// visibility test both have what they need.
    pub rs_ctup: Option<FormedTuple<'mcx>>,

    /// `rs_dir` — direction of the last call to `heapgettup`/`pagemode`.
    pub rs_dir: ScanDirection,
    /// `rs_prefetch_block` — used by the inline read-stream collapse to track
    /// the block to fetch next.
    pub rs_prefetch_block: BlockNumber,

    /// `rs_parallelworkerdata` — per-worker page-allocation state for parallel
    /// scans (`None` for non-parallel scans).
    pub rs_parallelworkerdata: Option<PgBox<'mcx, ParallelBlockTableScanWorkerData>>,

    /* these fields only used in page-at-a-time mode and for bitmap scans */
    /// `rs_cindex` — current tuple's index in `rs_vistuples`.
    pub rs_cindex: u32,
    /// `rs_ntuples` — number of visible tuples on this page.
    pub rs_ntuples: u32,
    /// `rs_vistuples` — their offsets.
    pub rs_vistuples: [OffsetNumber; MaxHeapTuplesPerPage],

    /// TID-range scan bounds (`sscan->st.tidrange` in C; the only `st` union
    /// member a sequential/tidrange heap scan uses). Set by `heap_set_tidrange`.
    pub rs_mintid: ItemPointerData,
    pub rs_maxtid: ItemPointerData,
}

impl<'mcx> AmOpaqueType<'mcx> for HeapScanDescData<'mcx> {
    const TAG: AmOpaqueTag = tags::HEAP_SCAN;
}

/// Borrow the heap-private scan tail out of the generic descriptor's
/// `am_private` (C's `(HeapScanDesc) sscan`). A missing or mistyped payload is a
/// wiring error (the heap AM always installs a `HeapScanDescData`).
#[inline]
fn heap_scan<'a, 'mcx>(sscan: &'a mut TableScanDescData<'mcx>) -> &'a mut HeapScanDescData<'mcx> {
    let am = sscan
        .am_private
        .as_deref_mut()
        .expect("heap scan: TableScanDescData.am_private is empty");
    am.downcast_mut::<HeapScanDescData<'mcx>>()
        .expect("heap scan: am_private is not a HeapScanDescData")
}

/// Provider-facing accessor for the heap-private scan tail (`(HeapScanDesc)
/// scan` in C). The table-AM provider (heapam_handler.c
/// `heapam_index_build_range_scan` / `heapam_scan_get_blocks_done`) needs the
/// heap-private fields (`rs_cbuf`, `rs_cblock`, `rs_nblocks`, `rs_startblock`)
/// of a scan it owns; this exposes them the same way the in-crate `heap_scan`
/// helper does.
#[inline]
pub fn heap_scan_state<'a, 'mcx>(
    sscan: &'a mut TableScanDescData<'mcx>,
) -> &'a mut HeapScanDescData<'mcx> {
    heap_scan(sscan)
}

// ===========================================================================
// initscan - scan code common to heap_beginscan and heap_rescan.
// ===========================================================================

fn initscan(
    mcx: Mcx<'_>,
    sscan: &mut TableScanDescData<'_>,
    keep_startblock: bool,
) -> PgResult<()> {
    let allow_strat: bool;
    let allow_sync: bool;

    let relid = sscan.rs_rd.rd_id;
    let flags = sscan.rs_flags;
    let parallel = sscan.rs_parallel.clone();

    // Determine the number of blocks we have to scan. Sufficient to do once at
    // scan start, since tuples added during the scan are invisible to my
    // snapshot anyway.
    let nblocks = if let Some(p) = parallel.as_ref() {
        // bpscan = (ParallelBlockTableScanDesc) rs_parallel; scan->rs_nblocks =
        // bpscan->phs_nblocks;
        p.block
            .as_ref()
            .map(|b| b.phs_nblocks)
            .expect("initscan: parallel scan descriptor not block-initialized")
    } else {
        relation_get_number_of_blocks(&sscan.rs_rd)?
    };

    let uses_local = relation_uses_local_buffers(relid)?;
    let nbuffers = initsmall_seam::nbuffers::call();

    let scan = heap_scan(sscan);
    scan.rs_nblocks = nblocks;

    // If the table is large relative to NBuffers, use a bulk-read access
    // strategy and enable synchronized scanning.
    if !uses_local && scan.rs_nblocks > (nbuffers as u32) / 4 {
        allow_strat = (flags & SO_ALLOW_STRAT) != 0;
        allow_sync = (flags & SO_ALLOW_SYNC) != 0;
    } else {
        allow_strat = false;
        allow_sync = false;
    }

    if allow_strat {
        // During a rescan, keep the previous strategy object.
        if scan.rs_strategy.is_none() {
            scan.rs_strategy =
                bufmgr_seam::get_access_strategy::call(BufferAccessStrategyType::BasBulkread)?;
        }
    } else {
        if scan.rs_strategy.is_some() {
            // Hand the ring to FreeAccessStrategy (drops it); `.take()` resets
            // the field to the C `NULL` strategy.
            bufmgr_seam::free_access_strategy::call(scan.rs_strategy.take());
        }
        scan.rs_strategy = None;
    }

    if let Some(p) = parallel.as_ref() {
        // For parallel scan, believe whatever ParallelTableScanDesc says.
        if p.phs_syncscan {
            sscan.rs_flags |= SO_ALLOW_SYNC;
        } else {
            sscan.rs_flags &= !SO_ALLOW_SYNC;
        }
    } else if keep_startblock {
        // When rescanning, keep the previous startblock so rewinding a cursor
        // doesn't surprise; reset the active syncscan setting, though.
        if allow_sync && tableam::synchronize_seqscans() {
            sscan.rs_flags |= SO_ALLOW_SYNC;
        } else {
            sscan.rs_flags &= !SO_ALLOW_SYNC;
        }
    } else if allow_sync && tableam::synchronize_seqscans() {
        sscan.rs_flags |= SO_ALLOW_SYNC;
        let startblock = syncscan_seam::ss_get_location::call(relid, nblocks)?;
        heap_scan(sscan).rs_startblock = startblock;
    } else {
        sscan.rs_flags &= !SO_ALLOW_SYNC;
        heap_scan(sscan).rs_startblock = 0;
    }

    let scan = heap_scan(sscan);
    scan.rs_numblocks = InvalidBlockNumber;
    scan.rs_inited = false;
    scan.rs_ctup = None;
    scan.rs_cbuf = InvalidBuffer;
    scan.rs_cblock = InvalidBlockNumber;
    scan.rs_ntuples = 0;
    scan.rs_cindex = 0;

    // Initialize to ForwardScanDirection because it is most common and because
    // heap scans go forward before going backward (e.g. CURSORs).
    scan.rs_dir = ScanDirection::ForwardScanDirection;
    scan.rs_prefetch_block = InvalidBlockNumber;

    // page-at-a-time fields are always invalid when not rs_inited

    // (The scan key copy C does here happened in heap_beginscan/heap_rescan via
    // the owned `rs_key` PgVec; nothing to do here.)

    // Currently, we only have a stats counter for sequential heap scans.
    if (sscan.rs_flags & SO_TYPE_SEQSCAN) != 0 {
        pgstat_seam::pgstat_count_heap_scan::call(relid);
    }
    let _ = mcx;
    Ok(())
}

/// `RelationGetNumberOfBlocks(rel)` (bufmgr.h macro) — the MAIN_FORKNUM block
/// count, reached through the bufmgr seam (which calls `smgrnblocks`).
fn relation_get_number_of_blocks(rel: &Relation<'_>) -> PgResult<BlockNumber> {
    bufmgr_seam::relation_get_number_of_blocks_in_fork::call(
        rel,
        types_core::primitive::ForkNumber::MAIN_FORKNUM,
    )
}

/// `RelationUsesLocalBuffers(rel)` — reached through the seam keyed by OID.
fn relation_uses_local_buffers(relid: Oid) -> PgResult<bool> {
    vacuumlazy_seam::relation_uses_local_buffers::call(relid)
}

// ===========================================================================
// heap_setscanlimits - restrict range of a heapscan.
// ===========================================================================

/// `heap_setscanlimits(sscan, startBlk, numBlks)` — `startBlk` is the page to
/// start at; `numBlks` is the number of pages to scan (`InvalidBlockNumber`
/// means "all").
pub fn heap_setscanlimits(
    sscan: &mut TableScanDescData<'_>,
    start_blk: BlockNumber,
    num_blks: BlockNumber,
) {
    let allow_sync = (sscan.rs_flags & SO_ALLOW_SYNC) != 0;
    let scan = heap_scan(sscan);
    debug_assert!(!scan.rs_inited); // else too late to change
    debug_assert!(!allow_sync); // else rs_startblock is significant
    debug_assert!(start_blk == 0 || start_blk < scan.rs_nblocks);

    scan.rs_startblock = start_blk;
    scan.rs_numblocks = num_blks;
}

// ===========================================================================
// heap_prepare_pagescan - prune the page and fill rs_vistuples[].
// ===========================================================================

/// Per-tuple loop for [`heap_prepare_pagescan`]. Mirrors C's manual
/// constant-folding of `all_visible` / `check_serializable`. Operates on the
/// page bytes already obtained under the buffer content lock.
#[allow(clippy::too_many_arguments)]
fn page_collect_tuples(
    mcx: Mcx<'_>,
    scan: &mut HeapScanDescData<'_>,
    snapshot: Option<&mut SnapshotData>,
    page: &PageRef<'_>,
    buffer: Buffer,
    block: BlockNumber,
    relid: Oid,
    lines: u16,
    all_visible: bool,
    check_serializable: bool,
) -> PgResult<u32> {
    let mut ntup: u32 = 0;

    // We thread the optional snapshot through the loop. The C dereferences
    // `snapshot` only on the !all_visible / serializable paths, where the
    // snapshot is guaranteed non-NULL (heap_prepare_pagescan asserts pagemode,
    // which implies an MVCC snapshot).
    let mut snap_holder = snapshot;

    let mut lineoff: OffsetNumber = FirstOffsetNumber;
    while lineoff <= lines {
        let lpp = PageGetItemId(page, lineoff)?;
        if !ItemIdIsNormal(&lpp) {
            lineoff += 1;
            continue;
        }

        let item = PageGetItem(page, &lpp)?;
        // loctup carries the full on-page tuple image (C's loctup.t_data points
        // into the page; loctup.t_len = ItemIdGetLength). For the visibility
        // test only the header is consulted; for a visible tuple the offset is
        // recorded and the materialized image is dropped (re-read in
        // heapgettup_pagemode).
        let mut loctup =
            FormedTuple::read_on_page_full(mcx, &item[..ItemIdGetLength(&lpp) as usize], block, lineoff, relid)?;

        let valid = if all_visible {
            true
        } else {
            let snap = snap_holder
                .as_deref_mut()
                .expect("heap scan: page-at-a-time visibility test requires a snapshot");
            HeapTupleSatisfiesVisibility(&mut loctup.tuple, snap, buffer)?
        };

        if check_serializable {
            let snap = snap_holder
                .as_deref_mut()
                .expect("heap scan: serializable conflict-out check requires a snapshot");
            predicate_seam::heap_check_for_serializable_conflict_out::call(
                valid,
                relid,
                &loctup.tuple,
                buffer,
                snap,
            )?;
        }

        if valid {
            scan.rs_vistuples[ntup as usize] = lineoff;
            ntup += 1;
        }

        lineoff += 1;
    }

    debug_assert!(ntup <= MaxHeapTuplesPerPage as u32);
    Ok(ntup)
}

/// `heap_prepare_pagescan(sscan)` - prune the scan's `rs_cbuf` page and fill the
/// `rs_vistuples[]` array with the OffsetNumbers of visible tuples.
pub fn heap_prepare_pagescan(mcx: Mcx<'_>, sscan: &mut TableScanDescData<'_>) -> PgResult<()> {
    let relid = sscan.rs_rd.rd_id;
    let buffer = heap_scan(sscan).rs_cbuf;
    let block = heap_scan(sscan).rs_cblock;

    debug_assert!(bufmgr_seam::buffer_get_block_number::call(buffer) == block);
    debug_assert!((sscan.rs_flags & SO_ALLOW_PAGEMODE) != 0);

    // Prune and repair fragmentation for the whole page, if possible.
    prune_seam::heap_page_prune_opt::call(mcx, &sscan.rs_rd, buffer)?;

    // We must hold share lock on the buffer content while examining tuple
    // visibility. Afterwards, found-visible tuples are good while we hold the
    // pin.
    bufmgr_seam::lock_buffer::call(buffer, BUFFER_LOCK_SHARE)?;

    // Whether the page is all-visible / serializable-conflict-checked is decided
    // off the page header and the snapshot; both are read inside the page
    // callback so the page bytes are accessed only through `with_buffer_page`.
    let ntuples = {
        // We snapshot the relevant snapshot scalars before entering the page
        // closure (which borrows the scan mutably). The MVCC snapshot itself is
        // needed inside the per-tuple test, so it travels as an owned clone for
        // the duration of the page scan (C aliases scan->rs_base.rs_snapshot;
        // the owned model clones the value-typed snapshot).
        let mut snapshot = sscan.rs_snapshot.clone();
        let taken_during_recovery = snapshot.as_ref().map(|s| s.takenDuringRecovery);
        let check_serializable = match snapshot.as_ref() {
            Some(s) => predicate_seam::check_for_serializable_conflict_out_needed::call(relid, s),
            None => false,
        };

        let mut ntuples: u32 = 0;
        bufmgr_seam::with_buffer_page::call(buffer, &mut |page_bytes| {
            let page = PageRef::new(page_bytes)?;
            let lines = PageGetMaxOffsetNumber(&page);

            // If the all-visible flag says all tuples are visible to everyone,
            // skip the per-tuple visibility tests.
            let all_visible = PageIsAllVisible(&page)
                && !taken_during_recovery.unwrap_or(false);

            let scan = heap_scan(sscan);
            ntuples = if all_visible {
                if !check_serializable {
                    page_collect_tuples(
                        mcx, scan, None, &page, buffer, block, relid, lines, true, false,
                    )?
                } else {
                    page_collect_tuples(
                        mcx, scan, snapshot.as_mut(), &page, buffer, block, relid, lines, true, true,
                    )?
                }
            } else if !check_serializable {
                page_collect_tuples(
                    mcx, scan, snapshot.as_mut(), &page, buffer, block, relid, lines, false, false,
                )?
            } else {
                page_collect_tuples(
                    mcx, scan, snapshot.as_mut(), &page, buffer, block, relid, lines, false, true,
                )?
            };
            Ok(())
        })?;
        ntuples
    };
    heap_scan(sscan).rs_ntuples = ntuples;

    bufmgr_seam::lock_buffer::call(buffer, BUFFER_LOCK_UNLOCK)?;
    Ok(())
}

/// `BUFFER_LOCK_UNLOCK` (bufmgr.h).
const BUFFER_LOCK_UNLOCK: i32 = 0;
/// `BUFFER_LOCK_SHARE` (bufmgr.h).
const BUFFER_LOCK_SHARE: i32 = 1;

// ===========================================================================
// heap_fetch_next_buffer + block-advance helpers.
// ===========================================================================

/// `heap_fetch_next_buffer(scan, dir)` - read and pin the next block from
/// MAIN_FORKNUM. The read-stream collapse computes the next block inline (the
/// block the read stream would have prefetched) and pins it with `read_buffer`.
fn heap_fetch_next_buffer(sscan: &mut TableScanDescData<'_>, dir: ScanDirection) -> PgResult<()> {
    let is_parallel = sscan.rs_parallel.is_some();

    // release previous scan buffer, if any
    {
        let scan = heap_scan(sscan);
        if buffer_is_valid(scan.rs_cbuf) {
            bufmgr_seam::release_buffer::call(scan.rs_cbuf);
            scan.rs_cbuf = InvalidBuffer;
        }
    }

    // Be sure to check for interrupts at least once per page.
    vacuumlazy_seam::check_for_interrupts::call()?;

    // If the scan direction is changing, reset the prefetch block to the current
    // block (the read_stream_reset semantics).
    {
        let scan = heap_scan(sscan);
        if scan.rs_dir != dir {
            scan.rs_prefetch_block = scan.rs_cblock;
        }
        scan.rs_dir = dir;
    }

    // Compute the next block to fetch (the read-stream callback equivalent).
    let next_block = {
        let inited = heap_scan(sscan).rs_inited;
        if !inited {
            let b = if is_parallel {
                parallel_next_block(sscan)?
            } else {
                heapgettup_initial_block(sscan, dir)?
            };
            heap_scan(sscan).rs_inited = true;
            b
        } else if is_parallel {
            parallel_next_block(sscan)?
        } else {
            let prefetch = heap_scan(sscan).rs_prefetch_block;
            heapgettup_advance_block(sscan, prefetch, dir)?
        }
    };
    heap_scan(sscan).rs_prefetch_block = next_block;

    if next_block == InvalidBlockNumber {
        heap_scan(sscan).rs_cbuf = InvalidBuffer;
        return Ok(());
    }

    let cbuf = bufmgr_seam::read_buffer::call(&sscan.rs_rd, next_block)?;
    let scan = heap_scan(sscan);
    scan.rs_cbuf = cbuf;
    if buffer_is_valid(cbuf) {
        scan.rs_cblock = bufmgr_seam::buffer_get_block_number::call(cbuf);
    }
    Ok(())
}

/// The parallel read-stream callback equivalent
/// (`heap_scan_stream_read_next_parallel`): claim the next block from the shared
/// parallel scan descriptor and store back the per-worker chunk state.
fn parallel_next_block(sscan: &mut TableScanDescData<'_>) -> PgResult<BlockNumber> {
    debug_assert!(ScanDirectionIsForward(heap_scan(sscan).rs_dir));
    let pscan: std::sync::Arc<ParallelTableScanDescData> = sscan
        .rs_parallel
        .clone()
        .expect("parallel_next_block: no parallel scan descriptor");

    let inited = heap_scan(sscan).rs_inited;
    if !inited {
        let mut worker = heap_scan(sscan)
            .rs_parallelworkerdata
            .as_deref()
            .copied()
            .unwrap_or_default();
        tableam::table_block_parallelscan_startblock_init(&sscan.rs_rd, &mut worker, &pscan)?;
        store_worker(heap_scan(sscan), worker);
    }
    let mut worker = heap_scan(sscan)
        .rs_parallelworkerdata
        .as_deref()
        .copied()
        .unwrap_or_default();
    let next = tableam::table_block_parallelscan_nextpage(&sscan.rs_rd, &mut worker, &pscan)?;
    store_worker(heap_scan(sscan), worker);
    Ok(next)
}

#[inline]
fn store_worker(scan: &mut HeapScanDescData<'_>, worker: ParallelBlockTableScanWorkerData) {
    match scan.rs_parallelworkerdata.as_deref_mut() {
        Some(w) => *w = worker,
        None => unreachable!("parallel scan must have allocated rs_parallelworkerdata"),
    }
}

/// `heapgettup_initial_block(scan, dir)` - the first BlockNumber to scan, or
/// `InvalidBlockNumber` when there are no blocks.
pub fn heapgettup_initial_block(
    sscan: &mut TableScanDescData<'_>,
    dir: ScanDirection,
) -> PgResult<BlockNumber> {
    debug_assert!(!heap_scan(sscan).rs_inited);
    debug_assert!(sscan.rs_parallel.is_none());

    let scan = heap_scan(sscan);
    // When there are no pages to scan, return InvalidBlockNumber.
    if scan.rs_nblocks == 0 || scan.rs_numblocks == 0 {
        return Ok(InvalidBlockNumber);
    }

    if ScanDirectionIsForward(dir) {
        Ok(scan.rs_startblock)
    } else {
        // Disable reporting to syncscan logic in a backwards scan.
        sscan.rs_flags &= !SO_ALLOW_SYNC;
        let scan = heap_scan(sscan);

        // Start from the last page of the scan, honoring rs_numblocks if set by
        // heap_setscanlimits().
        if scan.rs_numblocks != InvalidBlockNumber {
            return Ok((scan.rs_startblock + scan.rs_numblocks - 1) % scan.rs_nblocks);
        }
        if scan.rs_startblock > 0 {
            return Ok(scan.rs_startblock - 1);
        }
        Ok(scan.rs_nblocks - 1)
    }
}

/// `heapgettup_advance_block(scan, block, dir)` - the BlockNumber to scan next,
/// or `InvalidBlockNumber`. Adjusts `rs_numblocks` when a setscanlimits limit is
/// imposed. Not for the initial block.
pub fn heapgettup_advance_block(
    sscan: &mut TableScanDescData<'_>,
    mut block: BlockNumber,
    dir: ScanDirection,
) -> PgResult<BlockNumber> {
    debug_assert!(sscan.rs_parallel.is_none());

    if ScanDirectionIsForward(dir) {
        block += 1;

        // wrap back to the start of the heap
        if block >= heap_scan(sscan).rs_nblocks {
            block = 0;
        }

        // Report our new scan position for synchronization (not when backwards).
        if (sscan.rs_flags & SO_ALLOW_SYNC) != 0 {
            syncscan_seam::ss_report_location::call(sscan.rs_rd.rd_id, block)?;
        }

        let scan = heap_scan(sscan);
        // we're done if we're back at where we started
        if block == scan.rs_startblock {
            return Ok(InvalidBlockNumber);
        }

        // check if the heap_setscanlimits() limit is met
        if scan.rs_numblocks != InvalidBlockNumber {
            scan.rs_numblocks -= 1;
            if scan.rs_numblocks == 0 {
                return Ok(InvalidBlockNumber);
            }
        }

        Ok(block)
    } else {
        let scan = heap_scan(sscan);
        // we're done if the last block is the start position
        if block == scan.rs_startblock {
            return Ok(InvalidBlockNumber);
        }

        // check if the heap_setscanlimits() limit is met
        if scan.rs_numblocks != InvalidBlockNumber {
            scan.rs_numblocks -= 1;
            if scan.rs_numblocks == 0 {
                return Ok(InvalidBlockNumber);
            }
        }

        // wrap to the end of the heap when the last page was page 0
        if block == 0 {
            block = scan.rs_nblocks;
        }
        block -= 1;
        Ok(block)
    }
}

/// `heapgettup_start_page(scan, dir, &linesleft, &lineoff)` — set `*linesleft`
/// to the number of tuples on `rs_cbuf` and `*lineoff` to the first offset.
fn heapgettup_start_page(
    page: &PageRef<'_>,
    dir: ScanDirection,
    linesleft: &mut i32,
    lineoff: &mut OffsetNumber,
) {
    let max = PageGetMaxOffsetNumber(page);
    *linesleft = max as i32 - FirstOffsetNumber as i32 + 1;

    if ScanDirectionIsForward(dir) {
        *lineoff = FirstOffsetNumber;
    } else {
        *lineoff = *linesleft as OffsetNumber;
    }
}

/// `heapgettup_continue_page(scan, dir, &linesleft, &lineoff)` — set
/// `*linesleft`/`*lineoff` to the next offset to scan according to `dir`.
fn heapgettup_continue_page(
    page: &PageRef<'_>,
    coffset: OffsetNumber,
    dir: ScanDirection,
    linesleft: &mut i32,
    lineoff: &mut OffsetNumber,
) {
    let max = PageGetMaxOffsetNumber(page);
    if ScanDirectionIsForward(dir) {
        *lineoff = OffsetNumberNext(coffset);
        *linesleft = max as i32 - (*lineoff as i32) + 1;
    } else {
        // Re-establish the lineoff <= PageGetMaxOffsetNumber(page) invariant (the
        // previously returned tuple may have been vacuumed).
        *lineoff = core::cmp::min(max, OffsetNumberPrev(coffset));
        *linesleft = *lineoff as i32;
    }
}

// ===========================================================================
// heapgettup / heapgettup_pagemode.
// ===========================================================================

/// `heapgettup(scan, dir, nkeys, key)` - fetch the next heap tuple into
/// `scan->rs_ctup`, or set it to `None` if no more tuples.
fn heapgettup<'mcx>(
    mcx: Mcx<'mcx>,
    sscan: &mut TableScanDescData<'mcx>,
    dir: ScanDirection,
) -> PgResult<()> {
    let relid = sscan.rs_rd.rd_id;
    let nkeys = sscan.rs_nkeys;
    let mut lineoff: OffsetNumber = 0;
    let mut linesleft: i32 = 0;
    // Tracks whether the loop body should jump into `continue_page` (the C
    // `goto continue_page`) on its first iteration.
    let mut into_continue_page;

    if heap_scan(sscan).rs_inited {
        // continue from previously returned page/tuple
        let cbuf = heap_scan(sscan).rs_cbuf;
        bufmgr_seam::lock_buffer::call(cbuf, BUFFER_LOCK_SHARE)?;
        let coffset = heap_scan(sscan).rs_coffset;
        bufmgr_seam::with_buffer_page::call(cbuf, &mut |page_bytes| {
            let page = PageRef::new(page_bytes)?;
            heapgettup_continue_page(&page, coffset, dir, &mut linesleft, &mut lineoff);
            Ok(())
        })?;
        into_continue_page = true;
    } else {
        into_continue_page = false;
    }

    // advance the scan until we find a qualifying tuple or run out
    loop {
        if !into_continue_page {
            heap_fetch_next_buffer(sscan, dir)?;

            // did we run out of blocks to scan?
            if !buffer_is_valid(heap_scan(sscan).rs_cbuf) {
                break;
            }

            let cbuf = heap_scan(sscan).rs_cbuf;
            debug_assert!(
                bufmgr_seam::buffer_get_block_number::call(cbuf) == heap_scan(sscan).rs_cblock
            );

            bufmgr_seam::lock_buffer::call(cbuf, BUFFER_LOCK_SHARE)?;
            bufmgr_seam::with_buffer_page::call(cbuf, &mut |page_bytes| {
                let page = PageRef::new(page_bytes)?;
                heapgettup_start_page(&page, dir, &mut linesleft, &mut lineoff);
                Ok(())
            })?;
        }
        into_continue_page = false;
        // `continue_page:` label.

        let cbuf = heap_scan(sscan).rs_cbuf;
        let cblock = heap_scan(sscan).rs_cblock;
        let snapshot = sscan.rs_snapshot.clone();

        // Only continue scanning while we have lines left (protects against
        // accessing line pointers past PageGetMaxOffsetNumber()).
        let mut found: Option<(OffsetNumber, FormedTuple<'_>)> = None;
        while linesleft > 0 {
            let mut produced: Option<FormedTuple<'_>> = None;
            let off = lineoff;
            bufmgr_seam::with_buffer_page::call(cbuf, &mut |page_bytes| {
                let page = PageRef::new(page_bytes)?;
                let lpp = PageGetItemId(&page, off)?;
                if !ItemIdIsNormal(&lpp) {
                    return Ok(());
                }
                let item = PageGetItem(&page, &lpp)?;
                produced = Some(FormedTuple::read_on_page_full(
                    mcx,
                    &item[..ItemIdGetLength(&lpp) as usize],
                    cblock,
                    off,
                    relid,
                )?);
                Ok(())
            })?;

            if let Some(mut tuple) = produced {
                let mut snap = snapshot.clone();
                let visible = {
                    let s = snap
                        .as_mut()
                        .expect("heap scan: visibility test requires a snapshot");
                    HeapTupleSatisfiesVisibility(&mut tuple.tuple, s, cbuf)?
                };

                {
                    let s = snap
                        .as_mut()
                        .expect("heap scan: serializable conflict-out check requires a snapshot");
                    predicate_seam::heap_check_for_serializable_conflict_out::call(
                        visible, relid, &tuple.tuple, cbuf, s,
                    )?;
                }

                // skip tuples not visible to this snapshot
                if visible {
                    // skip any tuples that don't match the scan key
                    let matches = if nkeys > 0 {
                        heap_key_test(&tuple, &sscan.rs_rd, &sscan.rs_key)?
                    } else {
                        true
                    };
                    if matches {
                        found = Some((off, tuple));
                        break;
                    }
                }
            }

            linesleft -= 1;
            lineoff = (lineoff as i32 + dir as i32) as OffsetNumber;
        }

        if let Some((off, tuple)) = found {
            bufmgr_seam::lock_buffer::call(cbuf, BUFFER_LOCK_UNLOCK)?;
            let scan = heap_scan(sscan);
            scan.rs_coffset = off;
            scan.rs_ctup = Some(tuple);
            return Ok(());
        }

        // exhausted the items on this page; move to the next
        bufmgr_seam::lock_buffer::call(cbuf, BUFFER_LOCK_UNLOCK)?;
    }

    // end of scan
    end_of_scan(sscan);
    Ok(())
}

/// `heapgettup_pagemode(scan, dir, nkeys, key)` - fetch the next heap tuple in
/// page-at-a-time mode (no buffer content lock; iterate the `rs_vistuples[]`
/// offsets). `lineindex` is 0-based.
fn heapgettup_pagemode<'mcx>(
    mcx: Mcx<'mcx>,
    sscan: &mut TableScanDescData<'mcx>,
    dir: ScanDirection,
) -> PgResult<()> {
    let relid = sscan.rs_rd.rd_id;
    let nkeys = sscan.rs_nkeys;
    // signed so the backward `lineindex += dir` can go to -1
    let mut lineindex: i32 = 0;
    let mut linesleft: u32 = 0;
    let mut into_continue_page;

    if heap_scan(sscan).rs_inited {
        // continue from previously returned page/tuple
        let scan = heap_scan(sscan);
        lineindex = scan.rs_cindex as i32 + dir as i32;
        if ScanDirectionIsForward(dir) {
            linesleft = scan.rs_ntuples - lineindex as u32;
        } else {
            linesleft = scan.rs_cindex;
        }
        into_continue_page = true;
    } else {
        into_continue_page = false;
    }

    loop {
        if !into_continue_page {
            heap_fetch_next_buffer(sscan, dir)?;

            // did we run out of blocks to scan?
            if !buffer_is_valid(heap_scan(sscan).rs_cbuf) {
                break;
            }

            debug_assert!(
                bufmgr_seam::buffer_get_block_number::call(heap_scan(sscan).rs_cbuf)
                    == heap_scan(sscan).rs_cblock
            );

            // prune the page and determine visible tuple offsets
            heap_prepare_pagescan(mcx, sscan)?;
            let scan = heap_scan(sscan);
            linesleft = scan.rs_ntuples;
            lineindex = if ScanDirectionIsForward(dir) {
                0
            } else {
                linesleft as i32 - 1
            };
        }
        into_continue_page = false;
        // `continue_page:` label.

        let cbuf = heap_scan(sscan).rs_cbuf;
        let cblock = heap_scan(sscan).rs_cblock;
        let ntuples = heap_scan(sscan).rs_ntuples;

        let mut found: Option<(u32, FormedTuple<'_>)> = None;
        while linesleft > 0 {
            debug_assert!((lineindex as u32) < ntuples);
            let lineoff = heap_scan(sscan).rs_vistuples[lineindex as usize];

            let mut produced: Option<FormedTuple<'_>> = None;
            bufmgr_seam::with_buffer_page::call(cbuf, &mut |page_bytes| {
                let page = PageRef::new(page_bytes)?;
                let lpp = PageGetItemId(&page, lineoff)?;
                debug_assert!(ItemIdIsNormal(&lpp));
                let item = PageGetItem(&page, &lpp)?;
                produced = Some(FormedTuple::read_on_page_full(
                    mcx,
                    &item[..ItemIdGetLength(&lpp) as usize],
                    cblock,
                    lineoff,
                    relid,
                )?);
                Ok(())
            })?;
            let tuple = produced.expect("heapgettup_pagemode: rs_vistuples offset not normal");

            // skip any tuples that don't match the scan key
            let matches = if nkeys > 0 {
                heap_key_test(&tuple, &sscan.rs_rd, &sscan.rs_key)?
            } else {
                true
            };

            if matches {
                found = Some((lineindex as u32, tuple));
                break;
            }

            linesleft -= 1;
            lineindex += dir as i32;
        }

        if let Some((idx, tuple)) = found {
            let scan = heap_scan(sscan);
            scan.rs_cindex = idx;
            scan.rs_ctup = Some(tuple);
            return Ok(());
        }
    }

    // end of scan
    end_of_scan(sscan);
    Ok(())
}

/// The shared end-of-scan teardown of `heapgettup`/`heapgettup_pagemode`.
fn end_of_scan(sscan: &mut TableScanDescData<'_>) {
    let scan = heap_scan(sscan);
    if buffer_is_valid(scan.rs_cbuf) {
        bufmgr_seam::release_buffer::call(scan.rs_cbuf);
    }
    scan.rs_cbuf = InvalidBuffer;
    scan.rs_cblock = InvalidBlockNumber;
    scan.rs_prefetch_block = InvalidBlockNumber;
    scan.rs_ctup = None;
    scan.rs_inited = false;
}

/// `HeapKeyTest(tuple, RelationGetDescr(rel), nkeys, key)` (`access/valid.h`).
/// SANCTIONED panic-until-keystone: the trimmed [`ScanKeyData`] has no
/// `sk_func`/`sk_argument`, so the comparison cannot run until the scan-key
/// carrier keystone (task #281). The seam panics; `nkeys == 0` never reaches it.
fn heap_key_test<'mcx>(
    tuple: &FormedTuple<'mcx>,
    rel: &Relation<'mcx>,
    keys: &PgVec<'mcx, ScanKeyData>,
) -> PgResult<bool> {
    heapam_seam::heap_key_test::call(tuple, rel, keys)
}

// ===========================================================================
// heap access method interface
// ===========================================================================

/// `heap_beginscan(relation, snapshot, nkeys, key, parallel_scan, flags)` -
/// begin a relation scan. Returns the owned generic scan descriptor with the
/// heap-private tail in `am_private` (convention A).
pub fn heap_beginscan<'mcx>(
    mcx: Mcx<'mcx>,
    relation: Relation<'mcx>,
    snapshot: Option<SnapshotData>,
    nkeys: i32,
    key: PgVec<'mcx, ScanKeyData>,
    parallel_scan: Option<std::sync::Arc<ParallelTableScanDescData>>,
    flags: u32,
) -> PgResult<std::boxed::Box<TableScanDescData<'mcx>>> {
    let relid = relation.rd_id;

    // increment relation ref count while scanning relation
    relcache_seam::relation_increment_reference_count::call(relid)?;

    // allocate and initialize scan descriptor. A bitmap heap scan has no extra
    // fields versus a normal heap scan; this sequential-scan core does not set
    // up the bitmap read stream.
    let heap = HeapScanDescData {
        rs_nblocks: 0,
        rs_startblock: 0,
        rs_numblocks: InvalidBlockNumber,
        rs_inited: false,
        rs_coffset: 0,
        rs_cblock: InvalidBlockNumber,
        rs_cbuf: InvalidBuffer,
        rs_strategy: None, // set in initscan
        rs_ctup: None,
        rs_dir: ScanDirection::ForwardScanDirection,
        rs_prefetch_block: InvalidBlockNumber,
        rs_parallelworkerdata: None,
        rs_cindex: 0,
        rs_ntuples: 0,
        rs_vistuples: [0; MaxHeapTuplesPerPage],
        rs_mintid: ItemPointerData::default(),
        rs_maxtid: ItemPointerData::default(),
    };
    let am: PgBox<'mcx, dyn AmOpaque<'mcx> + 'mcx> = erase_heap_scan(mcx, heap)?;

    let mut sscan = std::boxed::Box::new(TableScanDescData {
        rs_rd: relation,
        rs_snapshot: snapshot,
        rs_nkeys: nkeys,
        rs_key: key,
        rs_flags: flags,
        rs_parallel: parallel_scan,
        rs_tbmiterator: types_tidbitmap::TBMIterator::default(),
        am_private: Some(am),
    });

    // Disable page-at-a-time mode if it's not an MVCC-safe snapshot. A NULL
    // snapshot short-circuits (IsMVCCSnapshot is never consulted).
    let mvcc = matches!(sscan.rs_snapshot.as_ref(), Some(s) if IsMVCCSnapshot(s));
    if !mvcc {
        sscan.rs_flags &= !SO_ALLOW_PAGEMODE;
    }

    // For seqscan and sample scans in a serializable transaction, acquire a
    // predicate lock on the entire relation.
    if (flags & (SO_TYPE_SEQSCAN | SO_TYPE_SAMPLESCAN)) != 0 {
        // Ensure a missing snapshot is noticed reliably.
        let sn = sscan
            .rs_snapshot
            .as_ref()
            .expect("heap_beginscan: seq/sample scan requires a snapshot");
        predicate_seam::predicate_lock_relation::call(relid, sn)?;
    }

    // Allocate per-worker page-allocation state for a parallel scan.
    if sscan.rs_parallel.is_some() {
        heap_scan(&mut sscan).rs_parallelworkerdata =
            Some(mcx::alloc_in(mcx, ParallelBlockTableScanWorkerData::default())?);
    } else {
        heap_scan(&mut sscan).rs_parallelworkerdata = None;
    }

    initscan(mcx, &mut sscan, false)?;

    // C sets up a ReadStream here for seqscan / tidrangescan; the inline
    // read-stream collapse needs no separate stream object. A bitmap-scan stream
    // is not part of this sequential-scan core.
    if (sscan.rs_flags & SO_TYPE_BITMAPSCAN) != 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg_internal("bitmap heap scan is not part of the heap sequential-scan core")
            .into_error());
    }

    Ok(sscan)
}

/// Erase a `HeapScanDescData` into the `dyn AmOpaque` carrier the generic
/// descriptor's `am_private` holds (the same unsize-through-raw-pointer pattern
/// the carrier's own tests document; no `CoerceUnsized` on stable).
fn erase_heap_scan<'mcx>(
    mcx: Mcx<'mcx>,
    heap: HeapScanDescData<'mcx>,
) -> PgResult<PgBox<'mcx, dyn AmOpaque<'mcx> + 'mcx>> {
    let boxed: PgBox<'mcx, HeapScanDescData<'mcx>> = mcx::alloc_in(mcx, heap)?;
    let (ptr, alloc) = PgBox::into_raw_with_allocator(boxed);
    // SAFETY: `ptr`/`alloc` came from `into_raw_with_allocator`; the cast only
    // attaches the `dyn AmOpaque` vtable.
    Ok(unsafe { PgBox::from_raw_in(ptr as *mut (dyn AmOpaque<'mcx> + 'mcx), alloc) })
}

/// `heap_rescan(sscan, key, set_params, allow_strat, allow_sync, allow_pagemode)`
/// - restart a relation scan. (The scan-key array C `memcpy`s into `rs_key`
/// happened at `heap_beginscan`; `key` here re-supplies the same array, which
/// the caller already stored. The sequential-scan core's `rs_key` is unchanged.)
pub fn heap_rescan(
    mcx: Mcx<'_>,
    sscan: &mut TableScanDescData<'_>,
    set_params: bool,
    allow_strat: bool,
    allow_sync: bool,
    allow_pagemode: bool,
) -> PgResult<()> {
    if set_params {
        if allow_strat {
            sscan.rs_flags |= SO_ALLOW_STRAT;
        } else {
            sscan.rs_flags &= !SO_ALLOW_STRAT;
        }

        if allow_sync {
            sscan.rs_flags |= SO_ALLOW_SYNC;
        } else {
            sscan.rs_flags &= !SO_ALLOW_SYNC;
        }

        let mvcc = matches!(sscan.rs_snapshot.as_ref(), Some(s) if IsMVCCSnapshot(s));
        if allow_pagemode && mvcc {
            sscan.rs_flags |= SO_ALLOW_PAGEMODE;
        } else {
            sscan.rs_flags &= !SO_ALLOW_PAGEMODE;
        }
    }

    // unpin scan buffers
    {
        let scan = heap_scan(sscan);
        if buffer_is_valid(scan.rs_cbuf) {
            bufmgr_seam::release_buffer::call(scan.rs_cbuf);
            scan.rs_cbuf = InvalidBuffer;
        }
    }

    // (No separate read stream to reset in the inline collapse.)

    // reinitialize scan descriptor
    initscan(mcx, sscan, true)
}

/// `heap_endscan(sscan)` - end a relation scan. Consumes the owned descriptor.
pub fn heap_endscan(mut sscan: std::boxed::Box<TableScanDescData<'_>>) -> PgResult<()> {
    let relid = sscan.rs_rd.rd_id;

    // unpin scan buffers
    {
        let scan = heap_scan(&mut sscan);
        if buffer_is_valid(scan.rs_cbuf) {
            bufmgr_seam::release_buffer::call(scan.rs_cbuf);
        }
    }

    // (No separate read stream to free in the inline collapse.)

    // decrement relation reference count
    relcache_seam::relation_decrement_reference_count::call(relid)?;

    // (The owned rs_key PgVec is freed when the descriptor drops; C's
    // pfree(rs_key) is implicit.)

    // Take the ring handle out of the descriptor (the descriptor is about to be
    // dropped) and hand it to FreeAccessStrategy, which drops it. A `None`
    // (default) strategy is a no-op in C.
    let strategy = heap_scan(&mut sscan).rs_strategy.take();
    if strategy.is_some() {
        bufmgr_seam::free_access_strategy::call(strategy);
    }

    // (The owned rs_parallelworkerdata box is freed on drop.)

    if (sscan.rs_flags & SO_TEMP_SNAPSHOT) != 0 {
        if let Some(sn) = sscan.rs_snapshot.clone() {
            snapmgr_seam::unregister_snapshot::call(sn);
        }
    }

    // Drop the owned descriptor (the C `pfree(scan)`).
    drop(sscan);
    Ok(())
}

/// `heapam_tuple_tid_valid(scan, tid)` (heapam_handler.c): is `tid` potentially
/// valid (within the relation's current size)? `ItemPointerIsValid(tid) &&
/// ItemPointerGetBlockNumber(tid) < hscan->rs_nblocks`. The `HeapScanDescData`
/// rides in `am_private`, so the table-AM provider crate reaches it through this
/// accessor rather than re-implementing the (crate-private) downcast.
pub fn heapam_tuple_tid_valid(
    sscan: &mut TableScanDescData<'_>,
    tid: &ItemPointerData,
) -> bool {
    let nblocks = heap_scan(sscan).rs_nblocks;
    backend_storage_page::ItemPointerIsValid(Some(tid))
        && ItemPointerGetBlockNumberNoCheck(tid) < nblocks
}

/// `heap_getnext(sscan, direction)` - retrieve the next tuple in scan; returns
/// a reference to the scan's `rs_ctup` (`None` at end of scan).
pub fn heap_getnext<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    sscan: &'a mut TableScanDescData<'mcx>,
    direction: ScanDirection,
) -> PgResult<Option<&'a FormedTuple<'mcx>>> {
    // This is still widely used directly, so add a safety check: only the heap
    // AM is supported, and not during logical decoding.
    if !relation_is_heapam(&sscan.rs_rd) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg_internal("only heap AM is supported")
            .into_error());
    }

    if check_xid_alive_during_decoding() {
        return Err(ereport(ERROR)
            .errmsg_internal("unexpected heap_getnext call during logical decoding")
            .into_error());
    }

    if (sscan.rs_flags & SO_ALLOW_PAGEMODE) != 0 {
        heapgettup_pagemode(mcx, sscan, direction)?;
    } else {
        heapgettup(mcx, sscan, direction)?;
    }

    if heap_scan(sscan).rs_ctup.is_none() {
        return Ok(None);
    }

    pgstat_seam::pgstat_count_heap_getnext::call(sscan.rs_rd.rd_id);
    Ok(heap_scan(sscan).rs_ctup.as_ref())
}

/// `heap_getnextslot(sscan, direction, slot)` - retrieve the next tuple into an
/// executor slot. Returns whether a tuple was found.
pub fn heap_getnextslot<'mcx>(
    mcx: Mcx<'mcx>,
    sscan: &mut TableScanDescData<'mcx>,
    direction: ScanDirection,
    slot: &mut types_nodes::tuptable::SlotData<'mcx>,
) -> PgResult<bool> {
    if (sscan.rs_flags & SO_ALLOW_PAGEMODE) != 0 {
        heapgettup_pagemode(mcx, sscan, direction)?;
    } else {
        heapgettup(mcx, sscan, direction)?;
    }

    if heap_scan(sscan).rs_ctup.is_none() {
        exec_clear_tuple(slot)?;
        return Ok(false);
    }

    pgstat_seam::pgstat_count_heap_getnext::call(sscan.rs_rd.rd_id);
    let cbuf = heap_scan(sscan).rs_cbuf;
    let tuple = heap_scan(sscan)
        .rs_ctup
        .as_ref()
        .expect("heap_getnextslot: rs_ctup just checked non-None")
        .clone_in(mcx)?;
    exec_store_buffer_heap_tuple(tuple, slot, cbuf)?;
    Ok(true)
}

/// `ExecClearTuple(slot)` over the payload-bearing slot (the heap-scan vtable
/// holds the slot directly, not as a pool `SlotId`).
fn exec_clear_tuple(slot: &mut types_nodes::tuptable::SlotData<'_>) -> PgResult<()> {
    backend_executor_execTuples_seams::exec_clear_tuple_payload::call(slot)
}

/// `ExecStoreBufferHeapTuple(tuple, slot, buffer)` (#283 keystone seam).
fn exec_store_buffer_heap_tuple<'mcx>(
    tuple: FormedTuple<'mcx>,
    slot: &mut types_nodes::tuptable::SlotData<'mcx>,
    buffer: Buffer,
) -> PgResult<()> {
    backend_executor_execTuples_seams::exec_store_buffer_heap_tuple::call(tuple, slot, buffer)
}

// ===========================================================================
// heap_set_tidrange / heap_getnextslot_tidrange.
// ===========================================================================

/// `heap_set_tidrange(sscan, mintid, maxtid)` - bound a scan to a range of TIDs.
pub fn heap_set_tidrange(
    sscan: &mut TableScanDescData<'_>,
    mintid: &ItemPointerData,
    maxtid: &ItemPointerData,
) {
    // For relations without any pages, leave the TID range unset.
    if heap_scan(sscan).rs_nblocks == 0 {
        return;
    }

    let nblocks = heap_scan(sscan).rs_nblocks;
    // ItemPointers for the first and last possible tuples in the heap.
    let mut highest_item = ItemPointerData::new(nblocks - 1, MaxOffsetNumber);
    let mut lowest_item = ItemPointerData::new(0, FirstOffsetNumber);

    // Restrict the range to the given max/min if tighter than the relation.
    if ItemPointerCompare(maxtid, &highest_item) < 0 {
        highest_item = *maxtid;
    }
    if ItemPointerCompare(mintid, &lowest_item) > 0 {
        lowest_item = *mintid;
    }

    // Check for an empty range (protect the numBlks calc below).
    if ItemPointerCompare(&highest_item, &lowest_item) < 0 {
        heap_setscanlimits(sscan, 0, 0);
        return;
    }

    // Calculate the first block and the number of blocks we must scan.
    let start_blk = ItemPointerGetBlockNumberNoCheck(&lowest_item);
    let num_blks = ItemPointerGetBlockNumberNoCheck(&highest_item)
        - ItemPointerGetBlockNumberNoCheck(&lowest_item)
        + 1;

    heap_setscanlimits(sscan, start_blk, num_blks);

    // Finally, set the TID range in sscan.
    let scan = heap_scan(sscan);
    scan.rs_mintid = lowest_item;
    scan.rs_maxtid = highest_item;
}

/// `heap_getnextslot_tidrange(sscan, direction, slot)` - retrieve the next tuple
/// within the configured TID range into an executor slot.
pub fn heap_getnextslot_tidrange<'mcx>(
    mcx: Mcx<'mcx>,
    sscan: &mut TableScanDescData<'mcx>,
    direction: ScanDirection,
    slot: &mut types_nodes::tuptable::SlotData<'mcx>,
) -> PgResult<bool> {
    let mintid = heap_scan(sscan).rs_mintid;
    let maxtid = heap_scan(sscan).rs_maxtid;

    loop {
        if (sscan.rs_flags & SO_ALLOW_PAGEMODE) != 0 {
            heapgettup_pagemode(mcx, sscan, direction)?;
        } else {
            heapgettup(mcx, sscan, direction)?;
        }

        if heap_scan(sscan).rs_ctup.is_none() {
            exec_clear_tuple(slot)?;
            return Ok(false);
        }

        let tid = heap_scan(sscan)
            .rs_ctup
            .as_ref()
            .expect("heap_getnextslot_tidrange: rs_ctup just checked non-None")
            .tuple
            .t_self;

        // Filter out tuples outside the TID range. heap_set_tidrange limited the
        // page range; here we filter the tuples on the boundary pages.
        if ItemPointerCompare(&tid, &mintid) < 0 {
            exec_clear_tuple(slot)?;
            // Backward: TIDs descend, so we can stop.
            if ScanDirectionIsBackward(direction) {
                return Ok(false);
            }
            continue;
        }
        if ItemPointerCompare(&tid, &maxtid) > 0 {
            exec_clear_tuple(slot)?;
            // Forward: TIDs ascend, so we can stop.
            if ScanDirectionIsForward(direction) {
                return Ok(false);
            }
            continue;
        }

        break;
    }

    pgstat_seam::pgstat_count_heap_getnext::call(sscan.rs_rd.rd_id);
    let cbuf = heap_scan(sscan).rs_cbuf;
    let tuple = heap_scan(sscan)
        .rs_ctup
        .as_ref()
        .expect("heap_getnextslot_tidrange: rs_ctup non-None")
        .clone_in(mcx)?;
    exec_store_buffer_heap_tuple(tuple, slot, cbuf)?;
    Ok(true)
}

/// `sscan->rs_rd->rd_tableam == GetHeapamTableAmRoutine()` — the heap_getnext
/// safety check. The owned model has no function-pointer identity to compare; a
/// scan that reached this crate is by construction a heap scan, so it is always
/// the heap AM here.
#[inline]
fn relation_is_heapam(_rel: &Relation<'_>) -> bool {
    true
}

/// `TransactionIdIsValid(CheckXidAlive) && !bsysscan` — the logical-decoding
/// guard.
fn check_xid_alive_during_decoding() -> bool {
    let check_xid_alive = xact_seam::check_xid_alive::call();
    transaction_id_is_valid(check_xid_alive) && !xact_seam::bsysscan::call()
}

/// `TransactionIdIsValid(xid)` — non-`InvalidTransactionId`.
#[inline]
fn transaction_id_is_valid(xid: types_core::primitive::TransactionId) -> bool {
    xid != 0
}
