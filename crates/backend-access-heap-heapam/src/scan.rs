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
//! ## Scan keys
//!
//! [`ScanKeyData`] carries the full `access/skey.h` key (`sk_func` +
//! `sk_argument`), so [`heap_key_test`] (`HeapKeyTest`, `access/valid.h`)
//! evaluates each key against a fetched tuple via `FunctionCall2Coll`. Installed
//! as the [`heapam_seam::heap_key_test`] seam from this crate's `init_seams()`.

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

use backend_storage_buffer_bufmgr_seams as bufmgr_seam;
use backend_storage_lmgr_predicate_seams as predicate_seam;
use backend_access_common_syncscan_seams as syncscan_seam;
use backend_utils_fmgr_fmgr_seams as fmgr_seam;
use types_tableam::scankey::SK_ISNULL;
use backend_access_heap_pruneheap_seams as prune_seam;
use backend_utils_cache_relcache_seams as relcache_seam;
use backend_utils_activity_pgstat_seams as pgstat_seam;
use backend_utils_init_small_seams as initsmall_seam;
use backend_access_transam_xact_seams as xact_seam;
use backend_utils_time_snapmgr_seams as snapmgr_seam;
use backend_access_table_tableam as tableam;

use backend_access_heap_heapam_visibility::HeapTupleSatisfiesVisibility;
use backend_nodes_core_tidbitmap_seams as tidbitmap_seam;
use crate::fetch;

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
    /// Whether the current bitmap page requires per-tuple qual recheck. In C
    /// this is the caller's `node->recheck` field, which `BitmapHeapScanNextBlock`
    /// writes once per page and `heapam_scan_bitmap_next_tuple` reads for every
    /// tuple on that page. The owned port persists it here so the 2nd..Nth tuple
    /// of a lossy page still reports recheck=true (otherwise a fresh per-call
    /// local defaulted to false, skipping the recheck and over-returning rows).
    pub rs_recheck: bool,

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

    // RelationUsesLocalBuffers(scan->rs_base.rs_rd) — the rel.h macro
    // `rd_rel->relpersistence == RELPERSISTENCE_TEMP`, read directly off the
    // in-hand Relation.
    let uses_local = sscan.rs_rd.uses_local_buffers();
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
        pgstat_seam::pgstat_count_heap_scan::call(
            relid,
            sscan.rs_rd.rd_rel.relisshared,
            sscan.rs_rd.pgstat_enabled,
        );
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
    // CHECK_FOR_INTERRUPTS() (miscadmin.h) — owned by tcop/postgres.c.
    backend_tcop_postgres_seams::check_for_interrupts::call()?;

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
                // A `None` snapshot is SnapshotAny: every tuple satisfies it
                // (HeapTupleSatisfiesAny) and SerializationNeededForRead is
                // false for a non-MVCC snapshot, so the serializable
                // conflict-out check is a no-op.
                let visible = match snap.as_mut() {
                    Some(s) => HeapTupleSatisfiesVisibility(&mut tuple.tuple, s, cbuf)?,
                    None => true,
                };

                if let Some(s) = snap.as_mut() {
                    predicate_seam::heap_check_for_serializable_conflict_out::call(
                        visible, relid, &tuple.tuple, cbuf, s,
                    )?;
                }

                // skip tuples not visible to this snapshot
                if visible {
                    // skip any tuples that don't match the scan key
                    let matches = if nkeys > 0 {
                        heap_key_test(mcx, &tuple, &sscan.rs_rd, &sscan.rs_key)?
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
                heap_key_test(mcx, &tuple, &sscan.rs_rd, &sscan.rs_key)?
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

/// `HeapKeyTest(tuple, RelationGetDescr(rel), nkeys, keys)` (`access/valid.h`)
/// — test a heap tuple against an array of scan keys (implicitly ANDed),
/// returning whether the tuple satisfies all of them. For each key: a
/// `SK_ISNULL` key never matches; the keyed attribute is fetched with
/// `heap_getattr` (a NULL attribute never matches); otherwise the comparison
/// operator `FunctionCall2Coll(&sk_func, sk_collation, atp, sk_argument)` is
/// invoked and its boolean result decides the match.
pub(crate) fn heap_key_test<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: &FormedTuple<'mcx>,
    rel: &types_rel::RelationData<'mcx>,
    keys: &[ScanKeyData<'mcx>],
) -> PgResult<bool> {
    // RelationGetDescr(rel).
    let tupdesc = &rel.rd_att;
    // Deform the tuple once and read each keyed attribute from the result
    // (heap_getattr per key). heap_deform_tuple fills trailing columns beyond
    // the stored natts with NULL, matching heap_getattr's missing-column path.
    let deformed = backend_access_common_heaptuple::heap_deform_tuple(
        mcx,
        &tuple.tuple,
        tupdesc,
        &tuple.data,
    )?;

    for cur_key in keys {
        if (cur_key.sk_flags & SK_ISNULL) != 0 {
            return Ok(false);
        }

        // atp = heap_getattr(tuple, cur_key->sk_attno, tupdesc, &isnull);
        // Heap scan keys reference user columns (sk_attno > 0). A column beyond
        // the deformed array reads as NULL.
        let idx = (cur_key.sk_attno - 1) as usize;
        let (atp, isnull) = match deformed.get(idx) {
            Some((v, n)) => (v.clone(), *n),
            None => (types_tuple::backend_access_common_heaptuple::Datum::null(), true),
        };

        if isnull {
            return Ok(false);
        }

        // test = FunctionCall2Coll(&cur_key->sk_func, cur_key->sk_collation,
        //                          atp, cur_key->sk_argument);
        let test = fmgr_seam::function_call2_coll_datum::call(
            mcx,
            cur_key.sk_func.fn_oid,
            cur_key.sk_collation,
            atp,
            cur_key.sk_argument.clone(),
        )?;

        if !datum_get_bool(&test) {
            return Ok(false);
        }
    }

    Ok(true)
}

/// `DatumGetBool(d)` — the low bit of the by-value word.
#[inline]
fn datum_get_bool(d: &types_tuple::backend_access_common_heaptuple::Datum<'_>) -> bool {
    use types_tuple::backend_access_common_heaptuple::Datum;
    match d {
        Datum::ByVal(w) => (*w & 1) != 0,
        // A boolean comparison result is always by-value; any other arm is a
        // wiring error in the comparison function.
        _ => panic!("HeapKeyTest: comparison function returned a non-by-value Datum"),
    }
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
    key: PgVec<'mcx, ScanKeyData<'mcx>>,
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
        rs_recheck: false,
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
    //
    // C calls `PredicateLockRelation(relation, snapshot)` unconditionally here;
    // that function is a no-op for a NULL snapshot (`SerializationNeededForRead`
    // returns false when `snapshot == NULL`). A NULL `rs_snapshot` ==
    // `SnapshotAny` is the normal serial CREATE INDEX heap-scan case, so skip
    // the predicate-lock seam (whose owner keys on a live snapshot) when there
    // is no snapshot, matching C's early no-op return.
    if (flags & (SO_TYPE_SEQSCAN | SO_TYPE_SAMPLESCAN)) != 0 {
        if let Some(sn) = sscan.rs_snapshot.as_ref() {
            predicate_seam::predicate_lock_relation::call(relid, sn)?;
        }
    }

    // Allocate per-worker page-allocation state for a parallel scan.
    if sscan.rs_parallel.is_some() {
        heap_scan(&mut sscan).rs_parallelworkerdata =
            Some(mcx::alloc_in(mcx, ParallelBlockTableScanWorkerData::default())?);
    } else {
        heap_scan(&mut sscan).rs_parallelworkerdata = None;
    }

    initscan(mcx, &mut sscan, false)?;

    // C sets up a ReadStream here for seqscan / tidrangescan / bitmap heap
    // scans; the inline read-stream collapse needs no separate stream object —
    // the bitmap scan pulls its blocks directly off `rs_base.rs_tbmiterator` in
    // BitmapHeapScanNextBlock, exactly as the seqscan core reads blocks inline.

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

    pgstat_seam::pgstat_count_heap_getnext::call(
        sscan.rs_rd.rd_id,
        sscan.rs_rd.rd_rel.relisshared,
        sscan.rs_rd.pgstat_enabled,
    );
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

    pgstat_seam::pgstat_count_heap_getnext::call(
        sscan.rs_rd.rd_id,
        sscan.rs_rd.rd_rel.relisshared,
        sscan.rs_rd.pgstat_enabled,
    );
    let cbuf = heap_scan(sscan).rs_cbuf;
    let tuple = heap_scan(sscan)
        .rs_ctup
        .as_ref()
        .expect("heap_getnextslot: rs_ctup just checked non-None")
        .clone_in(mcx)?;
    exec_store_buffer_heap_tuple(tuple, slot, cbuf)?;
    Ok(true)
}

// ===========================================================================
// Bitmap heap scan (heapam_handler.c): BitmapHeapScanNextBlock +
// heapam_scan_bitmap_next_tuple.
// ===========================================================================

/// `BitmapHeapScanNextBlock(scan, &recheck, &lossy_pages, &exact_pages)`
/// (heapam_handler.c) — pull the next block off the scan's `rs_tbmiterator`,
/// fill `rs_vistuples[]` with the visible candidate offsets on that page, and
/// leave it the scan's current page. Returns `true` when a block was found (the
/// page may yield zero visible tuples; the caller loops back), `false` when the
/// bitmap and relation are exhausted.
///
/// C drives block selection through `read_stream_next_buffer` over a read
/// stream whose callback (`bitmapheap_stream_read_next`) pulls from
/// `rs_tbmiterator`; the inline read-stream collapse here calls `tbm_iterate`
/// directly and `read_buffer`s the block, exactly as the seqscan core reads its
/// blocks inline (see the module docs).
fn BitmapHeapScanNextBlock<'mcx>(
    mcx: Mcx<'mcx>,
    sscan: &mut TableScanDescData<'mcx>,
    recheck: &mut bool,
    lossy_pages: &mut u64,
    exact_pages: &mut u64,
) -> PgResult<bool> {
    debug_assert!((sscan.rs_flags & SO_TYPE_BITMAPSCAN) != 0);

    {
        let scan = heap_scan(sscan);
        scan.rs_cindex = 0;
        scan.rs_ntuples = 0;

        // Release buffer containing previous block.
        if buffer_is_valid(scan.rs_cbuf) {
            bufmgr_seam::release_buffer::call(scan.rs_cbuf);
            scan.rs_cbuf = InvalidBuffer;
        }
    }

    // hscan->rs_cbuf = read_stream_next_buffer(...): pull the next TBM page and
    // pin its buffer. `tbm_iterate` returns None when the bitmap is exhausted.
    let outcome = tidbitmap_seam::tbm_iterate::call(&mut sscan.rs_tbmiterator)?;
    let tbmres = match outcome {
        Some(o) => o,
        None => {
            // the bitmap is exhausted
            return Ok(false);
        }
    };

    debug_assert!(tbmres.blockno != InvalidBlockNumber);

    let block = tbmres.blockno;
    let buffer = bufmgr_seam::read_buffer::call(&sscan.rs_rd, block)?;
    {
        let scan = heap_scan(sscan);
        scan.rs_cbuf = buffer;
        scan.rs_cblock = block;
    }
    debug_assert!(bufmgr_seam::buffer_get_block_number::call(buffer) == block);

    *recheck = tbmres.recheck;
    // Persist on the scan state so per-tuple fetches on this page (which do not
    // re-enter this block-advance path) still report the page's recheck flag.
    heap_scan(sscan).rs_recheck = tbmres.recheck;

    let relid = sscan.rs_rd.rd_id;

    // Prune and repair fragmentation for the whole page, if possible.
    prune_seam::heap_page_prune_opt::call(mcx, &sscan.rs_rd, buffer)?;

    // We must hold share lock on the buffer content while examining tuple
    // visibility. Afterwards, the tuples found visible are good as long as we
    // hold the buffer pin.
    bufmgr_seam::lock_buffer::call(buffer, BUFFER_LOCK_SHARE)?;

    let mut ntup: u32 = 0;

    if !tbmres.lossy {
        // Non-lossy: walk the offsets listed in tbmres, following any HOT chain
        // starting at each. `heap_hot_search_buffer` returns the live chain
        // member's tid; record its offset.
        let mut snapshot = sscan
            .rs_snapshot
            .clone()
            .expect("bitmap heap scan requires an MVCC snapshot");
        for &offnum in &tbmres.offsets {
            let mut tid = ItemPointerData::default();
            backend_storage_page::ItemPointerSet(&mut tid, block, offnum);
            let res = fetch::heap_hot_search_buffer(
                mcx,
                tid,
                &sscan.rs_rd,
                buffer,
                &mut snapshot,
                false,
                true,
            )?;
            if res.found {
                let off = backend_storage_page::ItemPointerGetOffsetNumber(&res.tid);
                heap_scan(sscan).rs_vistuples[ntup as usize] = off;
                ntup += 1;
            }
        }
    } else {
        // Lossy: examine each line pointer on the page. We can ignore HOT
        // chains since we recheck each tuple anyway. The page-bytes closure does
        // the visibility test (and the serializable conflict-out check, which
        // only needs the tuple + snapshot); the predicate-lock TID calls for
        // visible tuples are deferred to after the closure (they need the live
        // snapshot but no page access).
        let mut snapshot = sscan
            .rs_snapshot
            .clone()
            .expect("bitmap heap scan requires an MVCC snapshot");
        let mut visible: std::vec::Vec<(
            OffsetNumber,
            ItemPointerData,
            types_core::primitive::TransactionId,
        )> = std::vec::Vec::new();
        bufmgr_seam::with_buffer_page::call(buffer, &mut |page_bytes| {
            let page = PageRef::new(page_bytes)?;
            let maxoff = PageGetMaxOffsetNumber(&page);
            let mut offnum: OffsetNumber = FirstOffsetNumber;
            while offnum <= maxoff {
                let lp = PageGetItemId(&page, offnum)?;
                if !ItemIdIsNormal(&lp) {
                    offnum = OffsetNumberNext(offnum);
                    continue;
                }
                let item = PageGetItem(&page, &lp)?;
                let mut loctup = FormedTuple::read_on_page_full(
                    mcx,
                    &item[..ItemIdGetLength(&lp) as usize],
                    block,
                    offnum,
                    relid,
                )?;
                let valid = HeapTupleSatisfiesVisibility(&mut loctup.tuple, &mut snapshot, buffer)?;
                if valid {
                    let header = loctup
                        .tuple
                        .t_data
                        .as_ref()
                        .expect("bitmap lossy scan: normal line-pointer tuple has no t_data");
                    let xmin = types_tuple::heaptuple::HeapTupleHeaderGetXmin(header);
                    visible.push((offnum, loctup.tuple.t_self, xmin));
                }
                predicate_seam::heap_check_for_serializable_conflict_out::call(
                    valid, relid, &loctup.tuple, buffer, &mut snapshot,
                )?;
                offnum = OffsetNumberNext(offnum);
            }
            Ok(())
        })?;
        for (offnum, tid, xmin) in visible {
            heap_scan(sscan).rs_vistuples[ntup as usize] = offnum;
            ntup += 1;
            predicate_seam::predicate_lock_tid::call(relid, tid, &snapshot, xmin)?;
        }
    }

    bufmgr_seam::lock_buffer::call(buffer, BUFFER_LOCK_UNLOCK)?;

    debug_assert!(ntup <= MaxHeapTuplesPerPage as u32);
    heap_scan(sscan).rs_ntuples = ntup;

    if tbmres.lossy {
        *lossy_pages += 1;
    } else {
        *exact_pages += 1;
    }

    // Return true: a valid block was found and the bitmap is not exhausted. If
    // there are no visible tuples on this page, rs_ntuples == 0 and
    // heapam_scan_bitmap_next_tuple loops back here to advance to the next block.
    Ok(true)
}

/// `heapam_scan_bitmap_next_tuple(scan, slot, &recheck, &lossy_pages,
/// &exact_pages)` (heapam_handler.c) — store the next visible tuple of a bitmap
/// heap scan into `slot`. Advances over the current page's `rs_vistuples[]`,
/// calling [`BitmapHeapScanNextBlock`] when the page is exhausted. Returns
/// `Ok(true)` when a tuple was stored, `Ok(false)` at end of scan.
///
/// `recheck`/`lossy_pages`/`exact_pages` are caller-owned out-params; they are
/// written ONLY by [`BitmapHeapScanNextBlock`] (when a new block is loaded), so
/// the per-block `recheck` flag persists across the multiple per-tuple calls on
/// the same block. This routine must NOT reset them (faithful to C, where they
/// are `bool *`/`uint64 *` out-params the AM never re-initializes).
pub fn heapam_scan_bitmap_next_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    sscan: &mut TableScanDescData<'mcx>,
    slot: &mut types_nodes::tuptable::SlotData<'mcx>,
    recheck: &mut bool,
    lossy_pages: &mut u64,
    exact_pages: &mut u64,
) -> PgResult<bool> {
    // Out of range? If so, advance to the next block (or exhaust the bitmap).
    while heap_scan(sscan).rs_cindex >= heap_scan(sscan).rs_ntuples {
        if !BitmapHeapScanNextBlock(mcx, sscan, recheck, lossy_pages, exact_pages)? {
            return Ok(false);
        }
    }

    let relid = sscan.rs_rd.rd_id;
    let (targoffset, block, buffer) = {
        let scan = heap_scan(sscan);
        let targoffset = scan.rs_vistuples[scan.rs_cindex as usize];
        (targoffset, scan.rs_cblock, scan.rs_cbuf)
    };

    // Materialize the on-page tuple at `targoffset` (C aliases t_data into the
    // pinned page; the owned model reads the full image under the held pin).
    let mut tuple_holder: Option<FormedTuple<'mcx>> = None;
    bufmgr_seam::with_buffer_page::call(buffer, &mut |page_bytes| {
        let page = PageRef::new(page_bytes)?;
        let lp = PageGetItemId(&page, targoffset)?;
        debug_assert!(ItemIdIsNormal(&lp));
        let item = PageGetItem(&page, &lp)?;
        tuple_holder = Some(FormedTuple::read_on_page_full(
            mcx,
            &item[..ItemIdGetLength(&lp) as usize],
            block,
            targoffset,
            relid,
        )?);
        Ok(())
    })?;
    let tuple = tuple_holder.expect("bitmap scan: tuple materialization closure did not run");

    pgstat_seam::pgstat_count_heap_fetch::call(
        relid,
        sscan.rs_rd.rd_rel.relisshared,
        sscan.rs_rd.pgstat_enabled,
    );

    // Set up the result slot to point to this tuple (acquires a pin on buffer).
    exec_store_buffer_heap_tuple(tuple, slot, buffer)?;

    let recheck = heap_scan(sscan).rs_recheck;
    heap_scan(sscan).rs_cindex += 1;

    Ok(true)
}

// ===========================================================================
// Sample scan (heapam_handler.c): heapam_scan_sample_next_block /
// heapam_scan_sample_next_tuple / SampleHeapTupleVisible.
// ===========================================================================

/// `InvalidOffsetNumber` (`storage/off.h`).
const InvalidOffsetNumber: OffsetNumber = 0;

/// `OffsetNumberIsValid(offsetNumber)` (`storage/off.h`).
#[inline]
fn offset_number_is_valid(offset_number: OffsetNumber) -> bool {
    offset_number != InvalidOffsetNumber
}

/// `BlockNumberIsValid(blockNumber)` (`storage/block.h`).
#[inline]
fn block_number_is_valid(block_number: BlockNumber) -> bool {
    block_number != InvalidBlockNumber
}

/// `heapam_scan_sample_next_block(scan, scanstate)` (heapam_handler.c) — select
/// the next block to sample (via the tablesample method's `NextSampleBlock`
/// callback or a sequential scan over the relation), pin it, and (in pagemode)
/// prune it and collect its visible offsets. Returns `true` when a block was
/// selected, `false` when the sample scan is finished.
pub fn heapam_scan_sample_next_block<'mcx>(
    mcx: Mcx<'mcx>,
    sscan: &mut TableScanDescData<'mcx>,
    scanstate: &mut dyn types_tableam::tableam::SampleScanDriver,
) -> PgResult<bool> {
    let blockno: BlockNumber;

    // return false immediately if relation is empty
    if heap_scan(sscan).rs_nblocks == 0 {
        return Ok(false);
    }

    // release previous scan buffer, if any
    {
        let scan = heap_scan(sscan);
        if buffer_is_valid(scan.rs_cbuf) {
            bufmgr_seam::release_buffer::call(scan.rs_cbuf);
            scan.rs_cbuf = InvalidBuffer;
        }
    }

    if scanstate.has_next_sample_block() {
        blockno = scanstate.next_sample_block(heap_scan(sscan).rs_nblocks);
    } else {
        // scanning table sequentially

        if heap_scan(sscan).rs_cblock == InvalidBlockNumber {
            debug_assert!(!heap_scan(sscan).rs_inited);
            blockno = heap_scan(sscan).rs_startblock;
        } else {
            debug_assert!(heap_scan(sscan).rs_inited);

            let mut next = heap_scan(sscan).rs_cblock + 1;

            if next >= heap_scan(sscan).rs_nblocks {
                // wrap to beginning of rel, might not have started at 0
                next = 0;
            }

            // Report our new scan position for synchronization purposes.
            //
            // Note: we do this before checking for end of scan so that the
            // final state of the position hint is back at the start of the rel.
            if (sscan.rs_flags & SO_ALLOW_SYNC) != 0 {
                syncscan_seam::ss_report_location::call(sscan.rs_rd.rd_id, next)?;
            }

            if next == heap_scan(sscan).rs_startblock {
                next = InvalidBlockNumber;
            }

            blockno = next;
        }
    }

    heap_scan(sscan).rs_cblock = blockno;

    if !block_number_is_valid(blockno) {
        heap_scan(sscan).rs_inited = false;
        return Ok(false);
    }

    debug_assert!(blockno < heap_scan(sscan).rs_nblocks);

    // Be sure to check for interrupts at least once per page. Checks at higher
    // code levels won't be able to stop a sample scan that encounters many
    // pages' worth of consecutive dead tuples.
    backend_tcop_postgres_seams::check_for_interrupts::call()?;

    // Read page using selected strategy.
    let strategy = heap_scan(sscan).rs_strategy.clone();
    let cbuf = bufmgr_seam::read_buffer_with_strategy::call(&sscan.rs_rd, blockno, strategy)?;
    heap_scan(sscan).rs_cbuf = cbuf;

    // in pagemode, prune the page and determine visible tuple offsets
    if (sscan.rs_flags & SO_ALLOW_PAGEMODE) != 0 {
        heap_prepare_pagescan(mcx, sscan)?;
    }

    heap_scan(sscan).rs_inited = true;
    Ok(true)
}

/// `heapam_scan_sample_next_tuple(scan, scanstate, slot)` (heapam_handler.c) —
/// fetch the next sample tuple of the current block (selected by
/// [`heapam_scan_sample_next_block`]) into `slot`. Asks the tablesample method
/// which offsets to check (`scanstate.next_sample_tuple`), tests each for
/// visibility, and stores the first visible one. Returns `false` at end of
/// block.
pub fn heapam_scan_sample_next_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    sscan: &mut TableScanDescData<'mcx>,
    scanstate: &mut dyn types_tableam::tableam::SampleScanDriver,
    slot: &mut types_nodes::tuptable::SlotData<'mcx>,
) -> PgResult<bool> {
    let blockno = heap_scan(sscan).rs_cblock;
    let pagemode = (sscan.rs_flags & SO_ALLOW_PAGEMODE) != 0;
    let cbuf = heap_scan(sscan).rs_cbuf;
    let relid = sscan.rs_rd.rd_id;

    // When not using pagemode, we must lock the buffer during tuple visibility
    // checks.
    if !pagemode {
        bufmgr_seam::lock_buffer::call(cbuf, BUFFER_LOCK_SHARE)?;
    }

    // page = (Page) BufferGetPage(hscan->rs_cbuf);
    // all_visible = PageIsAllVisible(page) && !rs_snapshot->takenDuringRecovery;
    // maxoffset = PageGetMaxOffsetNumber(page);
    let taken_during_recovery = sscan
        .rs_snapshot
        .as_ref()
        .map(|s| s.takenDuringRecovery)
        .unwrap_or(false);
    let mut all_visible = false;
    let mut maxoffset: OffsetNumber = 0;
    bufmgr_seam::with_buffer_page::call(cbuf, &mut |page_bytes| {
        let page = PageRef::new(page_bytes)?;
        all_visible = PageIsAllVisible(&page) && !taken_during_recovery;
        maxoffset = PageGetMaxOffsetNumber(&page);
        Ok(())
    })?;

    loop {
        backend_tcop_postgres_seams::check_for_interrupts::call()?;

        // Ask the tablesample method which tuples to check on this page.
        let tupoffset = scanstate.next_sample_tuple(blockno, maxoffset);

        if offset_number_is_valid(tupoffset) {
            // Materialize the on-page tuple at `tupoffset`. C aliases t_data into
            // the pinned page (`tuple = &hscan->rs_ctup`); the owned model reads
            // the full image under the held pin. A non-normal line pointer is
            // skipped (`continue`), exactly as in C.
            let mut produced: Option<FormedTuple<'mcx>> = None;
            bufmgr_seam::with_buffer_page::call(cbuf, &mut |page_bytes| {
                let page = PageRef::new(page_bytes)?;
                let itemid = PageGetItemId(&page, tupoffset)?;
                if !ItemIdIsNormal(&itemid) {
                    return Ok(());
                }
                let item = PageGetItem(&page, &itemid)?;
                produced = Some(FormedTuple::read_on_page_full(
                    mcx,
                    &item[..ItemIdGetLength(&itemid) as usize],
                    blockno,
                    tupoffset,
                    relid,
                )?);
                Ok(())
            })?;

            // Skip invalid tuple pointers.
            let mut tuple = match produced {
                Some(t) => t,
                None => continue,
            };

            heap_scan(sscan).rs_ctup = Some(tuple.clone_in(mcx)?);

            let visible = if all_visible {
                true
            } else {
                SampleHeapTupleVisible(mcx, sscan, cbuf, &mut tuple, tupoffset)?
            };

            // in pagemode, heap_prepare_pagescan did this for us
            if !pagemode {
                if let Some(s) = sscan.rs_snapshot.as_ref() {
                    predicate_seam::heap_check_for_serializable_conflict_out::call(
                        visible,
                        relid,
                        &tuple.tuple,
                        cbuf,
                        s,
                    )?;
                }
            }

            // Try next tuple from same page.
            if !visible {
                continue;
            }

            // Found visible tuple, return it.
            if !pagemode {
                bufmgr_seam::lock_buffer::call(cbuf, BUFFER_LOCK_UNLOCK)?;
            }

            exec_store_buffer_heap_tuple(tuple, slot, cbuf)?;

            // Count successfully-fetched tuples as heap fetches.
            pgstat_seam::pgstat_count_heap_getnext::call(
                relid,
                sscan.rs_rd.rd_rel.relisshared,
                sscan.rs_rd.pgstat_enabled,
            );

            return Ok(true);
        } else {
            // We've exhausted the items on this page; move to the next.
            if !pagemode {
                bufmgr_seam::lock_buffer::call(cbuf, BUFFER_LOCK_UNLOCK)?;
            }

            exec_clear_tuple(slot)?;
            return Ok(false);
        }
    }
}

/// `SampleHeapTupleVisible(scan, buffer, tuple, tupoffset)` (heapam_handler.c) —
/// check visibility of the sample tuple. In pagemode, `heap_prepare_pagescan`
/// already did the visibility checks, so a binary search over the known-sorted
/// `rs_vistuples[]` array answers; otherwise the tuple is tested individually.
fn SampleHeapTupleVisible<'mcx>(
    _mcx: Mcx<'mcx>,
    sscan: &mut TableScanDescData<'mcx>,
    buffer: Buffer,
    tuple: &mut FormedTuple<'mcx>,
    tupoffset: OffsetNumber,
) -> PgResult<bool> {
    if (sscan.rs_flags & SO_ALLOW_PAGEMODE) != 0 {
        // In pageatatime mode, heap_prepare_pagescan() already did visibility
        // checks, so just look at the info it left in rs_vistuples[].
        //
        // Binary search over the known-sorted array.
        let scan = heap_scan(sscan);
        let mut start: u32 = 0;
        let mut end: u32 = scan.rs_ntuples;

        while start < end {
            let mid = start + (end - start) / 2;
            let curoffset = scan.rs_vistuples[mid as usize];

            if tupoffset == curoffset {
                return Ok(true);
            } else if tupoffset < curoffset {
                end = mid;
            } else {
                start = mid + 1;
            }
        }

        Ok(false)
    } else {
        // Otherwise, we have to check the tuple individually. A `None` snapshot
        // is SnapshotAny: every tuple satisfies it (HeapTupleSatisfiesAny).
        match sscan.rs_snapshot.as_mut() {
            Some(s) => HeapTupleSatisfiesVisibility(&mut tuple.tuple, s, buffer),
            None => Ok(true),
        }
    }
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

    pgstat_seam::pgstat_count_heap_getnext::call(
        sscan.rs_rd.rd_id,
        sscan.rs_rd.rd_rel.relisshared,
        sscan.rs_rd.pgstat_enabled,
    );
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
