//! heapam.c SCAN/FETCH read lane; DML (insert/update/delete/lock) is phase 2.
//! C divergence: the ReadStream prefetcher is collapsed — `rs_prefetch_block`
//! tracks the block the stream callback would return, computed inline (same
//! block order, no readahead until bufmgr lands).

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

use core::ptr::NonNull;

use ::bufmgr_seams::BufferPin;
use ::mcx::{Mcx, PgVec};
use ::tableam_vocab::{
    ParallelBlockTableScanDescData, Snapshot, TableAm, TableScanDescData, SO_ALLOW_PAGEMODE,
    SO_ALLOW_STRAT, SO_ALLOW_SYNC, SO_TEMP_SNAPSHOT, SO_TYPE_SAMPLESCAN, SO_TYPE_SEQSCAN,
};
use ::types_core::xact::TransactionIdIsValid;
use ::types_core::{
    BlockNumber, Buffer, ForkNumber, InvalidBlockNumber, MultiXactId, OffsetNumber, TransactionId,
};
use ::types_core::xact::{InvalidTransactionId, TransactionIdPrecedes};
use ::types_error::{PgError, PgResult};
use ::types_fmgr::LocalFcinfo;
use ::types_rel::{Relation, RelationData};
use ::types_scan::scankey::{ScanKeyData, SK_ISNULL};
use ::types_scan::sdir::{ScanDirection, ScanDirectionIsBackward, ScanDirectionIsForward};
use ::types_slot::SlotData;
use ::types_snapshot::{HTSV_Result, IsMVCCSnapshot, SnapshotData};
use ::types_storage::bufpage::{MaxHeapTuplesPerPage, MaxOffsetNumber, PageRef};
use ::types_storage::buf::{BufferAccessStrategy, BufferAccessStrategyType};
use ::types_storage::multixact::ISUPDATE_from_mxstatus;
use ::types_tuple::{
    heap_getattr, FirstOffsetNumber, HeapTupleData, HeapTupleHeaderData, ItemPointerCompare,
    ItemPointerData, ItemPointerGetBlockNumberNoCheck, HEAP_XMAX_INVALID, HEAP_XMAX_IS_MULTI,
    HEAP_XMAX_IS_LOCKED_ONLY,
};

use heapam_visibility_seams as hv_seam;

pub mod bitmap;
pub mod dml;
pub mod fetch;
pub mod freeze;
pub mod hio;
pub mod index_delete;
pub mod inplace;
pub use fetch::{heap_fetch, heap_fetch_dirty, heap_get_latest_tid, heap_hot_search_buffer};
pub use index_delete::heap_index_delete_tuples;
pub use dml::{heap_abort_speculative, heap_delete, heap_finish_speculative, heap_insert, heap_lock_tuple, heap_multi_insert, heap_update, simple_heap_delete, simple_heap_insert, simple_heap_update};
pub use hio::{GetBulkInsertState, RelationGetBufferForTuple, RelationPutHeapTuple, ReleaseBulkInsertStatePin};
pub use inplace::{heap_inplace_lock, heap_inplace_unlock, heap_inplace_update_and_unlock};
#[cfg(test)]
mod tests;

// HeapScanDescData with C's rs_base embedding; bitmap tail lands with its unit.
pub struct HeapScanDescData<'mcx> {
    pub rs_base: TableScanDescData<'mcx>,
    pub rs_nblocks: BlockNumber,
    pub rs_startblock: BlockNumber,
    pub rs_numblocks: BlockNumber,
    pub rs_inited: bool,
    pub rs_coffset: OffsetNumber,
    pub rs_cblock: BlockNumber,
    pub rs_cbuf: Option<BufferPin>,
    pub rs_strategy: BufferAccessStrategy,
    // INVARIANT: when Some, the image lies in the page pinned by rs_cbuf ('mcx erased).
    rs_ctup: Option<HeapTupleData<'mcx>>,
    pub rs_dir: ScanDirection,
    pub rs_prefetch_block: BlockNumber,
    pub rs_parallelworkerdata: Option<::tableam_vocab::ParallelBlockTableScanWorkerData>,
    pub rs_cindex: u32,
    pub rs_ntuples: u32,
    // Page image of rs_cbuf, cached by pagemode_next_page; null whenever the
    // pin moves. Keeps the per-tuple walk free of the seam-derive call edge.
    rs_cpage: *mut u8,
    pub rs_vistuples: [OffsetNumber; MaxHeapTuplesPerPage],
    // One-probe pgstat accumulators (indexam precedent); pgstat_relation flushes.
    pub rs_pgstat_numscans: u64,
    pub rs_pgstat_getnext: u64,
}

impl<'mcx> HeapScanDescData<'mcx> {
    /// C's `&scan->rs_ctup` after a fetching call: valid while `rs_cbuf`
    /// stays pinned (enforced here by the `&self` borrow).
    #[inline]
    pub fn rs_ctup(&self) -> Option<&HeapTupleData<'mcx>> {
        self.rs_ctup.as_ref()
    }
}

#[cold]
#[inline(never)]
fn unported(unit: &'static str) -> ! {
    panic!("backend-access-heap-heapam reached unported unit: {unit}")
}

fn elog_error(message: impl Into<std::string::String>) -> Box<PgError> {
    Box::new(PgError::error(message))
}

// CheckXidAlive (logical decoding only, unported): const-false like C's unlikely().
fn unexpected_during_logical_decoding() -> bool {
    const CHECK_XID_ALIVE: TransactionId = InvalidTransactionId;
    TransactionIdIsValid(CHECK_XID_ALIVE)
}

#[cold]
#[inline(never)]
fn process_interrupts() -> PgResult<()> {
    postgres_seams::check_for_interrupts::call()
}

#[inline(always)]
fn check_for_interrupts() -> PgResult<()> {
    if init_small::globals::InterruptPending() {
        return process_interrupts();
    }
    Ok(())
}

#[inline]
fn pgstat_count_heap_scan(scan: &mut HeapScanDescData<'_>) {
    if scan.rs_base.rs_rd.pgstat_enabled.get() {
        scan.rs_pgstat_numscans += 1;
    }
}

#[inline]
fn pgstat_count_heap_getnext(scan: &mut HeapScanDescData<'_>) {
    if scan.rs_base.rs_rd.pgstat_enabled.get() {
        scan.rs_pgstat_getnext += 1;
    }
}

fn MultiXactIdGetUpdateXid(xmax: MultiXactId, t_infomask: u16) -> PgResult<TransactionId> {
    debug_assert!(!HEAP_XMAX_IS_LOCKED_ONLY(t_infomask));
    debug_assert!((t_infomask & HEAP_XMAX_IS_MULTI) != 0);

    let mut update_xact = InvalidTransactionId;
    multixact_seams::get_multi_xact_id_members::call(xmax, false, false, &mut |members| {
        for m in members {
            if !ISUPDATE_from_mxstatus(m.status) {
                continue;
            }
            debug_assert!(update_xact == InvalidTransactionId);
            update_xact = m.xid;
            if !cfg!(debug_assertions) {
                break;
            }
        }
    })?;
    Ok(update_xact)
}

pub fn HeapTupleGetUpdateXid(hdr: &HeapTupleHeaderData) -> PgResult<TransactionId> {
    MultiXactIdGetUpdateXid(hdr.xmax_raw(), hdr.t_infomask)
}

pub fn HeapTupleHeaderGetUpdateXid(hdr: &HeapTupleHeaderData) -> PgResult<TransactionId> {
    let infomask = hdr.t_infomask;
    if (infomask & HEAP_XMAX_INVALID) == 0
        && (infomask & HEAP_XMAX_IS_MULTI) != 0
        && !HEAP_XMAX_IS_LOCKED_ONLY(infomask)
    {
        MultiXactIdGetUpdateXid(hdr.xmax_raw(), infomask)
    } else {
        Ok(hdr.xmax_raw())
    }
}

/// `HeapTupleHeaderAdvanceConflictHorizon` (heapam.c): maintain the
/// snapshotConflictHorizon while removing tuples.
pub fn HeapTupleHeaderAdvanceConflictHorizon(
    tuple: &HeapTupleHeaderData,
    snapshot_conflict_horizon: &mut TransactionId,
) -> PgResult<()> {
    use ::types_core::xact::TransactionIdFollows;
    let xmin = tuple.xmin();
    let xmax = HeapTupleHeaderGetUpdateXid(tuple)?;
    let xvac = tuple.xvac();

    if (tuple.t_infomask & ::types_tuple::HEAP_MOVED) != 0
        && TransactionIdPrecedes(*snapshot_conflict_horizon, xvac)
    {
        *snapshot_conflict_horizon = xvac;
    }

    // Ignore tuples inserted by an aborted transaction or updated/deleted by
    // the inserting transaction itself.
    if tuple.xmin_committed()
        || (!tuple.xmin_invalid() && transam_seams::transaction_id_did_commit::call(xmin)?)
    {
        if xmax != xmin && TransactionIdFollows(xmax, *snapshot_conflict_horizon) {
            *snapshot_conflict_horizon = xmax;
        }
    }
    Ok(())
}

pub fn heap_tuple_needs_eventual_freeze(tuple: &HeapTupleHeaderData) -> bool {
    use ::types_core::xact::TransactionIdIsNormal;
    if TransactionIdIsNormal(tuple.xmin()) {
        return true;
    }
    if (tuple.t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        if TransactionIdIsValid(tuple.xmax_raw()) {
            return true;
        }
    } else if TransactionIdIsNormal(tuple.xmax_raw()) {
        return true;
    }
    if (tuple.t_infomask & ::types_tuple::HEAP_MOVED) != 0
        && TransactionIdIsNormal(tuple.xvac())
    {
        return true;
    }
    false
}

pub fn HeapCheckForSerializableConflictOut(
    visible: bool,
    relation: &RelationData<'_>,
    tuple: &mut HeapTupleData<'_>,
    buffer: Buffer,
    snapshot: &SnapshotData<'_>,
) -> PgResult<()> {
    if !predicate_seams::check_for_serializable_conflict_out_needed::call(relation, snapshot)? {
        return Ok(());
    }

    let transaction_xmin = snapmgr_seams::transaction_xmin::call();
    let htsv = hv_seam::heap_tuple_satisfies_vacuum::call(tuple, transaction_xmin, buffer)?;
    let hdr = tuple.t_data();

    // Visible-but-updated checks the updater's xid (the write-skew edge), else xmin.
    let xid = match htsv {
        HTSV_Result::HEAPTUPLE_LIVE => {
            if visible {
                return Ok(());
            }
            hdr.xmin()
        }
        HTSV_Result::HEAPTUPLE_RECENTLY_DEAD | HTSV_Result::HEAPTUPLE_DELETE_IN_PROGRESS => {
            let x = if visible {
                HeapTupleHeaderGetUpdateXid(hdr)?
            } else {
                hdr.xmin()
            };
            if TransactionIdPrecedes(x, transaction_xmin) {
                debug_assert!(!visible);
                return Ok(());
            }
            x
        }
        HTSV_Result::HEAPTUPLE_INSERT_IN_PROGRESS => hdr.xmin(),
        HTSV_Result::HEAPTUPLE_DEAD => {
            debug_assert!(!visible);
            return Ok(());
        }
    };
    debug_assert!(TransactionIdIsValid(xid));

    if xid == xact_seams::get_top_transaction_id_if_any::call() {
        return Ok(());
    }
    let xid = subtrans_seams::sub_trans_get_topmost_transaction::call(xid)?;
    if TransactionIdPrecedes(xid, transaction_xmin) {
        return Ok(());
    }

    predicate_seams::check_for_serializable_conflict_out::call(relation, xid, snapshot)
}

fn initscan(
    scan: &mut HeapScanDescData<'_>,
    key: Option<&[ScanKeyData]>,
    keep_startblock: bool,
) -> PgResult<()> {
    scan.rs_nblocks = if let Some(p) = scan.rs_base.rs_parallel {
        // SAFETY: the shared parallel descriptor outlives every worker scan
        // (parallel-context contract carried by rs_parallel).
        unsafe { p.as_ref() }.phs_nblocks
    } else {
        bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
            &scan.rs_base.rs_rd,
            ForkNumber::MAIN_FORKNUM,
        )?
    };

    let allow_strat: bool;
    let allow_sync: bool;
    if !scan.rs_base.rs_rd.uses_local_buffers()
        && scan.rs_nblocks > (init_small::globals::NBuffers() as BlockNumber) / 4
    {
        allow_strat = (scan.rs_base.rs_flags & SO_ALLOW_STRAT) != 0;
        allow_sync = (scan.rs_base.rs_flags & SO_ALLOW_SYNC) != 0;
    } else {
        allow_strat = false;
        allow_sync = false;
    }

    if allow_strat {
        if scan.rs_strategy.is_none() {
            scan.rs_strategy =
                bufmgr_seams::get_access_strategy::call(BufferAccessStrategyType::BasBulkread);
        }
    } else if scan.rs_strategy.is_some() {
        bufmgr_seams::free_access_strategy::call(scan.rs_strategy.take());
    }

    if let Some(p) = scan.rs_base.rs_parallel {
        // SAFETY: as above.
        if unsafe { p.as_ref() }.phs_syncscan {
            scan.rs_base.rs_flags |= SO_ALLOW_SYNC;
        } else {
            scan.rs_base.rs_flags &= !SO_ALLOW_SYNC;
        }
    } else if keep_startblock {
        if allow_sync && ::tableam_vocab::synchronize_seqscans() {
            scan.rs_base.rs_flags |= SO_ALLOW_SYNC;
        } else {
            scan.rs_base.rs_flags &= !SO_ALLOW_SYNC;
        }
    } else if allow_sync && ::tableam_vocab::synchronize_seqscans() {
        scan.rs_base.rs_flags |= SO_ALLOW_SYNC;
        scan.rs_startblock =
            syncscan_seams::ss_get_location::call(&scan.rs_base.rs_rd, scan.rs_nblocks)?;
    } else {
        scan.rs_base.rs_flags &= !SO_ALLOW_SYNC;
        scan.rs_startblock = 0;
    }

    scan.rs_numblocks = InvalidBlockNumber;
    scan.rs_inited = false;
    scan.rs_ctup = None;
    scan.rs_cbuf = None;
    scan.rs_cblock = InvalidBlockNumber;
    scan.rs_ntuples = 0;
    scan.rs_cindex = 0;
    scan.rs_dir = ScanDirection::ForwardScanDirection;
    scan.rs_prefetch_block = InvalidBlockNumber;

    if let Some(key) = key {
        if scan.rs_base.rs_nkeys > 0 {
            scan.rs_base.rs_key.clone_from_slice(key);
        }
    }

    if (scan.rs_base.rs_flags & SO_TYPE_SEQSCAN) != 0 {
        pgstat_count_heap_scan(scan);
    }
    Ok(())
}

pub fn heap_setscanlimits(
    scan: &mut HeapScanDescData<'_>,
    start_blk: BlockNumber,
    num_blks: BlockNumber,
) {
    debug_assert!(!scan.rs_inited);
    debug_assert!((scan.rs_base.rs_flags & SO_ALLOW_SYNC) == 0);
    debug_assert!(start_blk == 0 || start_blk < scan.rs_nblocks);

    scan.rs_startblock = start_blk;
    scan.rs_numblocks = num_blks;
}

// Const generics stand in for C's four constant-folded call sites.
//
// # Safety
// `lines <= MaxHeapTuplesPerPage` was checked by the caller for THIS page
// (heap_prepare_pagescan's one-per-page bound): that proves every
// `item_id_unchecked(lineoff)` for `lineoff <= lines` is in the image and
// every `vistuples[ntup]` store (`ntup < lineoff <= lines`) is in bounds.
#[allow(clippy::too_many_arguments)]
#[inline(always)]
unsafe fn page_collect_tuples<const ALL_VISIBLE: bool, const CHECK_SERIALIZABLE: bool>(
    vistuples: &mut [OffsetNumber; MaxHeapTuplesPerPage],
    relation: &RelationData<'_>,
    snapshot: &SnapshotData<'_>,
    page: &PageRef<'_>,
    buffer: Buffer,
    block: BlockNumber,
    lines: OffsetNumber,
) -> PgResult<u32> {
    let mut ntup: u32 = 0;

    // C's `for (lineoff = FirstOffsetNumber; lineoff <= lines; lineoff++)`:
    // a manual while — RangeInclusive drags an exhausted-flag (cset/cinc)
    // through the per-tuple loop control.
    let mut lineoff = FirstOffsetNumber;
    while lineoff <= lines {
        // SAFETY: lineoff <= lines <= MaxHeapTuplesPerPage (fn contract).
        let lpp = unsafe { page.item_id_unchecked(lineoff) };
        if !lpp.is_normal() {
            lineoff += 1;
            continue;
        }

        let valid = if ALL_VISIBLE && !CHECK_SERIALIZABLE {
            // Vacuumed-table fast path: the tuple header is never consulted.
            true
        } else {
            // SAFETY: normal line pointer on a pinned + share-locked heap
            // page (page invariant, item_raw_unchecked contract).
            let (ptr, len) = unsafe { page.item_raw_unchecked(lpp) };
            // SAFETY: pinned + share-locked page; a normal line pointer carries a full tuple image.
            let mut loctup = unsafe {
                HeapTupleData::from_raw_parts(
                    ptr,
                    len,
                    ItemPointerData::new(block, lineoff),
                    relation.rd_id,
                )
            };
            let valid = if ALL_VISIBLE {
                true
            } else {
                hv_seam::heap_tuple_satisfies_visibility::call(&mut loctup, snapshot, buffer)?
            };
            if CHECK_SERIALIZABLE {
                HeapCheckForSerializableConflictOut(
                    valid, relation, &mut loctup, buffer, snapshot,
                )?;
            }
            valid
        };

        if valid {
            // SAFETY: ntup < lineoff <= lines <= MaxHeapTuplesPerPage
            // (fn contract), matching C's unchecked rs_vistuples store.
            unsafe { *vistuples.get_unchecked_mut(ntup as usize) = lineoff };
            ntup += 1;
        }
        lineoff += 1;
    }

    debug_assert!(ntup as usize <= MaxHeapTuplesPerPage);
    Ok(ntup)
}

pub fn heap_prepare_pagescan(scan: &mut HeapScanDescData<'_>) -> PgResult<()> {
    debug_assert!((scan.rs_base.rs_flags & SO_ALLOW_PAGEMODE) != 0);
    let block = scan.rs_cblock;

    pruneheap_seams::heap_page_prune_opt::call(
        &scan.rs_base.rs_rd,
        scan.rs_cbuf.as_ref().expect("pagescan without buffer").buffer(),
    )?;

    let relation = &scan.rs_base.rs_rd;
    let snapshot = scan
        .rs_base
        .rs_snapshot
        .as_deref()
        .expect("page-at-a-time mode requires an MVCC snapshot");
    let check_serializable =
        predicate_seams::check_for_serializable_conflict_out_needed::call(relation, snapshot)?;

    let pin = scan.rs_cbuf.as_ref().expect("pagescan without buffer");
    debug_assert!(pin.block_number() == block);
    let buffer = pin.buffer();

    // Found-visible tuples stay good under the pin alone after unlock.
    let lock = pin.lock_share()?;
    let page = pin.page();
    let lines = page.max_offset_number();
    // ONE bounds check per page — the proof obligation of every _unchecked
    // line-pointer access below AND of the pagemode walk over rs_vistuples
    // (rs_ntuples <= lines). C trusts this implicitly (its rs_vistuples array
    // would overflow on the same corruption); the hard check is per page, not
    // per tuple, per heapam's hoisting model.
    assert!(
        lines as usize <= MaxHeapTuplesPerPage,
        "corrupt heap page: pd_lower implies {lines} line pointers"
    );
    let all_visible = page.is_all_visible() && !snapshot.takenDuringRecovery;

    let vist = &mut scan.rs_vistuples;
    // SAFETY: lines bound checked above (page_collect_tuples contract).
    let ntuples = unsafe {
        match (all_visible, check_serializable) {
            (true, false) => page_collect_tuples::<true, false>(
                vist, relation, snapshot, &page, buffer, block, lines,
            )?,
            (true, true) => page_collect_tuples::<true, true>(
                vist, relation, snapshot, &page, buffer, block, lines,
            )?,
            (false, false) => page_collect_tuples::<false, false>(
                vist, relation, snapshot, &page, buffer, block, lines,
            )?,
            (false, true) => page_collect_tuples::<false, true>(
                vist, relation, snapshot, &page, buffer, block, lines,
            )?,
        }
    };
    drop(lock);

    scan.rs_ntuples = ntuples;
    Ok(())
}

pub fn heapgettup_initial_block(
    scan: &mut HeapScanDescData<'_>,
    dir: ScanDirection,
) -> BlockNumber {
    debug_assert!(!scan.rs_inited);
    debug_assert!(scan.rs_base.rs_parallel.is_none());

    if scan.rs_nblocks == 0 || scan.rs_numblocks == 0 {
        return InvalidBlockNumber;
    }

    if ScanDirectionIsForward(dir) {
        scan.rs_startblock
    } else {
        // Backwards scans don't report to syncscan.
        scan.rs_base.rs_flags &= !SO_ALLOW_SYNC;

        if scan.rs_numblocks != InvalidBlockNumber {
            return (scan.rs_startblock + scan.rs_numblocks - 1) % scan.rs_nblocks;
        }
        if scan.rs_startblock > 0 {
            return scan.rs_startblock - 1;
        }
        scan.rs_nblocks - 1
    }
}

pub fn heapgettup_advance_block(
    scan: &mut HeapScanDescData<'_>,
    mut block: BlockNumber,
    dir: ScanDirection,
) -> PgResult<BlockNumber> {
    debug_assert!(scan.rs_base.rs_parallel.is_none());

    if ScanDirectionIsForward(dir) {
        block += 1;
        if block >= scan.rs_nblocks {
            block = 0;
        }

        // Report before the end-of-scan check: the hint parks at the scan's start.
        if (scan.rs_base.rs_flags & SO_ALLOW_SYNC) != 0 {
            syncscan_seams::ss_report_location::call(&scan.rs_base.rs_rd, block)?;
        }

        if block == scan.rs_startblock {
            return Ok(InvalidBlockNumber);
        }
        if scan.rs_numblocks != InvalidBlockNumber {
            scan.rs_numblocks -= 1;
            if scan.rs_numblocks == 0 {
                return Ok(InvalidBlockNumber);
            }
        }
        Ok(block)
    } else {
        if block == scan.rs_startblock {
            return Ok(InvalidBlockNumber);
        }
        if scan.rs_numblocks != InvalidBlockNumber {
            scan.rs_numblocks -= 1;
            if scan.rs_numblocks == 0 {
                return Ok(InvalidBlockNumber);
            }
        }
        if block == 0 {
            block = scan.rs_nblocks;
        }
        Ok(block - 1)
    }
}

// heap_scan_stream_read_next_parallel's block arithmetic, inline.
fn parallel_next_block(scan: &mut HeapScanDescData<'_>, first: bool) -> PgResult<BlockNumber> {
    debug_assert!(ScanDirectionIsForward(scan.rs_dir));
    let pscan = scan
        .rs_base
        .rs_parallel
        .expect("parallel_next_block without parallel descriptor");
    // SAFETY: shared descriptor outlives the scan (parallel-context contract).
    let pbscan: &ParallelBlockTableScanDescData = unsafe { pscan.as_ref() };
    let worker = scan
        .rs_parallelworkerdata
        .as_mut()
        .expect("parallel scan without rs_parallelworkerdata");

    if first {
        ::tableam_vocab::table_block_parallelscan_startblock_init(&scan.rs_base.rs_rd, worker, pbscan)?;
    }
    ::tableam_vocab::table_block_parallelscan_nextpage(&scan.rs_base.rs_rd, worker, pbscan)
}

fn heap_fetch_next_buffer(scan: &mut HeapScanDescData<'_>, dir: ScanDirection) -> PgResult<()> {
    scan.rs_cpage = core::ptr::null_mut();
    if let Some(pin) = scan.rs_cbuf.take() {
        pin.release();
    }

    check_for_interrupts()?;

    // Direction change = read_stream_reset: prefetch restarts at the current block.
    if scan.rs_dir != dir {
        scan.rs_prefetch_block = scan.rs_cblock;
    }
    scan.rs_dir = dir;

    let next = if scan.rs_base.rs_parallel.is_some() {
        let first = !scan.rs_inited;
        scan.rs_inited = true;
        parallel_next_block(scan, first)?
    } else if !scan.rs_inited {
        let b = heapgettup_initial_block(scan, dir);
        scan.rs_inited = true;
        b
    } else {
        heapgettup_advance_block(scan, scan.rs_prefetch_block, dir)?
    };
    scan.rs_prefetch_block = next;

    if next == InvalidBlockNumber {
        return Ok(());
    }

    let buf = bufmgr_seams::read_buffer_strategy::call(
        &scan.rs_base.rs_rd,
        next,
        scan.rs_strategy.clone(),
    )?;
    scan.rs_cbuf = BufferPin::adopt(buf);
    if let Some(pin) = scan.rs_cbuf.as_ref() {
        scan.rs_cblock = pin.block_number();
    }
    Ok(())
}

fn heapgettup_start_page(
    page: &PageRef<'_>,
    dir: ScanDirection,
    linesleft: &mut i32,
    lineoff: &mut OffsetNumber,
) {
    *linesleft = page.max_offset_number() as i32 - FirstOffsetNumber as i32 + 1;
    if ScanDirectionIsForward(dir) {
        *lineoff = FirstOffsetNumber;
    } else {
        *lineoff = *linesleft as OffsetNumber;
    }
}

fn heapgettup_continue_page(
    page: &PageRef<'_>,
    coffset: OffsetNumber,
    dir: ScanDirection,
    linesleft: &mut i32,
    lineoff: &mut OffsetNumber,
) {
    let max = page.max_offset_number();
    if ScanDirectionIsForward(dir) {
        *lineoff = coffset + 1;
        *linesleft = max as i32 - *lineoff as i32 + 1;
    } else {
        // Re-establish lineoff <= max (non-MVCC snapshot: last tuple may be vacuumed).
        *lineoff = core::cmp::min(max, coffset - 1);
        *linesleft = *lineoff as i32;
    }
}

fn end_of_scan(scan: &mut HeapScanDescData<'_>) {
    scan.rs_ctup = None;
    scan.rs_cpage = core::ptr::null_mut();
    if let Some(pin) = scan.rs_cbuf.take() {
        pin.release();
    }
    scan.rs_cblock = InvalidBlockNumber;
    scan.rs_prefetch_block = InvalidBlockNumber;
    scan.rs_inited = false;
}

fn heap_key_test(
    tuple: &HeapTupleData<'_>,
    tupdesc: &::types_tuple::TupleDescData<'_>,
    keys: &mut [ScanKeyData],
) -> PgResult<bool> {
    for cur_key in keys {
        if (cur_key.sk_flags & SK_ISNULL) != 0 {
            return Ok(false);
        }

        let attno = cur_key.sk_attno as i32;
        assert!(attno > 0 && attno <= tupdesc.natts);
        let mut isnull = false;
        // SAFETY: attno in 1..=natts (checked); the image is live under the
        // caller's pin.
        let atp = unsafe { heap_getattr(tuple, attno, tupdesc, &mut isnull) };
        if isnull {
            return Ok(false);
        }

        let mut fcinfo = LocalFcinfo::<2>::new(cur_key.sk_collation);
        fcinfo.set_arg(0, atp);
        fcinfo.set_arg(1, cur_key.sk_argument);
        let test = cur_key.sk_func.invoke(&mut fcinfo)?;
        if fcinfo.isnull || !test.as_bool() {
            return Ok(false);
        }
    }
    Ok(true)
}

// C TU shape: heapgettup/heapgettup_pagemode are standalone functions, not
// inlined into every heap_getnext* entry — fusing them there drags the cold
// arms' register pressure onto the per-tuple prologue (8 callee-save pairs
// observed in the composed lane).
#[inline(never)]
fn heapgettup<'mcx>(scan: &mut HeapScanDescData<'mcx>, dir: ScanDirection) -> PgResult<()> {
    let nkeys = scan.rs_base.rs_nkeys;
    let mut linesleft: i32 = 0;
    let mut lineoff: OffsetNumber = 0;
    // C's `goto continue_page` from the rs_inited entry.
    let mut continue_page = scan.rs_inited;

    loop {
        if !continue_page {
            heap_fetch_next_buffer(scan, dir)?;
            if scan.rs_cbuf.is_none() {
                break;
            }
        }

        // Raw image parts cross out of the pin borrow; rs_ctup is set after it ends.
        let mut found: Option<(OffsetNumber, *const u8, u32)> = None;
        {
            let pin = scan.rs_cbuf.as_ref().expect("scan lost its buffer");
            debug_assert!(pin.block_number() == scan.rs_cblock);
            let _lock = pin.lock_share()?;
            let page = pin.page();
            // ONE bounds check per page (see heap_prepare_pagescan): proves
            // every lineoff <= max_offset_number() below is in the image.
            assert!(
                page.max_offset_number() as usize <= MaxHeapTuplesPerPage,
                "corrupt heap page: pd_lower overflows the line-pointer bound"
            );
            if continue_page {
                heapgettup_continue_page(&page, scan.rs_coffset, dir, &mut linesleft, &mut lineoff);
            } else {
                heapgettup_start_page(&page, dir, &mut linesleft, &mut lineoff);
            }

            while linesleft > 0 {
                // SAFETY: lineoff stays in 1..=max_offset_number() (start/
                // continue_page establish it, the walk steps by ±1 within
                // linesleft), bounded per the page check above.
                let lpp = unsafe { page.item_id_unchecked(lineoff) };
                if lpp.is_normal() {
                    // SAFETY: normal line pointer on a pinned + share-locked
                    // heap page (page invariant, item_raw_unchecked contract).
                    let (ptr, len) = unsafe { page.item_raw_unchecked(lpp) };
                    // SAFETY: pinned + share-locked page, normal line pointer.
                    let mut tuple = unsafe {
                        HeapTupleData::from_raw_parts(
                            ptr,
                            len,
                            ItemPointerData::new(scan.rs_cblock, lineoff),
                            scan.rs_base.rs_rd.rd_id,
                        )
                    };

                    // None is SnapshotAny: all qualify; conflict-out gate is false.
                    let visible = match scan.rs_base.rs_snapshot.as_deref() {
                        Some(snap) => hv_seam::heap_tuple_satisfies_visibility::call(
                            &mut tuple,
                            snap,
                            pin.buffer(),
                        )?,
                        None => true,
                    };
                    if let Some(snap) = scan.rs_base.rs_snapshot.as_deref() {
                        HeapCheckForSerializableConflictOut(
                            visible,
                            &scan.rs_base.rs_rd,
                            &mut tuple,
                            pin.buffer(),
                            snap,
                        )?;
                    }

                    if visible
                        && (nkeys == 0
                            || heap_key_test(
                                &tuple,
                                &scan.rs_base.rs_rd.rd_att,
                                &mut scan.rs_base.rs_key,
                            )?)
                    {
                        found = Some((lineoff, ptr, len));
                        break;
                    }
                }
                linesleft -= 1;
                lineoff = (lineoff as i32 + dir as i32) as OffsetNumber;
            }
        }
        continue_page = false;

        if let Some((off, ptr, len)) = found {
            scan.rs_coffset = off;
            // SAFETY: image on the page pinned by rs_cbuf (struct invariant).
            scan.rs_ctup = Some(unsafe {
                HeapTupleData::from_raw_parts(
                    ptr,
                    len,
                    ItemPointerData::new(scan.rs_cblock, off),
                    scan.rs_base.rs_rd.rd_id,
                )
            });
            return Ok(());
        }
    }

    end_of_scan(scan);
    Ok(())
}

// Advance the pagemode scan to its next page (read + collect); false = scan
// exhausted. Out of line: keeps the page-advance arm's register pressure off
// the per-tuple walk's frame.
#[inline(never)]
fn pagemode_next_page(scan: &mut HeapScanDescData<'_>, dir: ScanDirection) -> PgResult<bool> {
    heap_fetch_next_buffer(scan, dir)?;
    if scan.rs_cbuf.is_none() {
        return Ok(false);
    }
    debug_assert!(scan.rs_cbuf.as_ref().unwrap().block_number() == scan.rs_cblock);
    heap_prepare_pagescan(scan)?;
    scan.rs_cpage = scan
        .rs_cbuf
        .as_ref()
        .expect("pagescan without buffer")
        .page()
        .as_ptr()
        .cast_mut();
    Ok(true)
}

// Also #[inline(never)]: the call boundary between the rs_ctup narrow stores
// here and heap_getnextslot's wide reload for the slot store lets the stores
// retire — fusing them puts a failed store-to-load forward (strh trio → ldr d)
// on every returned tuple (measured 2x ns for -12 instr). C gets the same
// separation from its noinline tts_buffer_heap_store_tuple.
#[inline(never)]
fn heapgettup_pagemode<'mcx>(scan: &mut HeapScanDescData<'mcx>, dir: ScanDirection) -> PgResult<()> {
    let nkeys = scan.rs_base.rs_nkeys;
    let relid = scan.rs_base.rs_rd.rd_id;
    // Signed: the backward `+= dir` walk ends at -1.
    let mut lineindex: i32 = 0;
    let mut linesleft: i32 = 0;
    let mut continue_page = scan.rs_inited;

    if scan.rs_inited {
        lineindex = scan.rs_cindex as i32 + dir as i32;
        linesleft = if ScanDirectionIsForward(dir) {
            scan.rs_ntuples as i32 - lineindex
        } else {
            scan.rs_cindex as i32
        };
    }

    loop {
        if !continue_page {
            if !pagemode_next_page(scan, dir)? {
                break;
            }
            linesleft = scan.rs_ntuples as i32;
            lineindex = if ScanDirectionIsForward(dir) { 0 } else { linesleft - 1 };
        }
        continue_page = false;

        debug_assert!(!scan.rs_cpage.is_null() && scan.rs_cbuf.is_some());
        // SAFETY: rs_cpage is the image of the page pinned by rs_cbuf
        // (pagemode_next_page set it; every pin move nulls it), so it stays
        // valid across this walk. No call edge on the per-tuple path.
        let page: PageRef<'_> =
            unsafe { PageRef::from_raw(NonNull::new_unchecked(scan.rs_cpage)) };

        // No content lock: rs_vistuples entries stay good under the pin.
        while linesleft > 0 {
            debug_assert!((lineindex as u32) < scan.rs_ntuples);
            // SAFETY: 0 <= lineindex < rs_ntuples (linesleft counts it down)
            // and rs_ntuples <= MaxHeapTuplesPerPage (heap_prepare_pagescan's
            // per-page bound).
            let lineoff = unsafe { *scan.rs_vistuples.get_unchecked(lineindex as usize) };
            // SAFETY: lineoff came from page_collect_tuples on this pinned
            // page under the per-page line-pointer bound; it was is_normal at
            // collect time and normal items satisfy the page invariant
            // (item_raw_unchecked contract). Both stay good under the pin.
            let (ptr, len) = unsafe {
                let lpp = page.item_id_unchecked(lineoff);
                debug_assert!(lpp.is_normal());
                page.item_raw_unchecked(lpp)
            };

            let matches = if nkeys == 0 {
                true
            } else {
                // SAFETY: pinned page, offset from rs_vistuples.
                let tuple = unsafe {
                    HeapTupleData::from_raw_parts(
                        ptr,
                        len,
                        ItemPointerData::new(scan.rs_cblock, lineoff),
                        relid,
                    )
                };
                heap_key_test(&tuple, &scan.rs_base.rs_rd.rd_att, &mut scan.rs_base.rs_key)?
            };

            if matches {
                scan.rs_cindex = lineindex as u32;
                // SAFETY: image on the page pinned by rs_cbuf (struct invariant).
                scan.rs_ctup = Some(unsafe {
                    HeapTupleData::from_raw_parts(
                        ptr,
                        len,
                        ItemPointerData::new(scan.rs_cblock, lineoff),
                        relid,
                    )
                });
                return Ok(());
            }
            linesleft -= 1;
            lineindex += dir as i32;
        }
    }

    end_of_scan(scan);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn heap_beginscan<'mcx>(
    _mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    snapshot: Snapshot<'mcx>,
    nkeys: i32,
    key: PgVec<'mcx, ScanKeyData>,
    parallel_scan: Option<NonNull<ParallelBlockTableScanDescData>>,
    mut flags: u32,
) -> PgResult<HeapScanDescData<'mcx>> {
    // rs_rd alias = RelationIncrementReferenceCount (Rc strong count).
    debug_assert!(nkeys <= 0 || key.len() == nkeys as usize);

    if !snapshot.as_deref().is_some_and(IsMVCCSnapshot) {
        flags &= !SO_ALLOW_PAGEMODE;
    }

    if (flags & (SO_TYPE_SEQSCAN | SO_TYPE_SAMPLESCAN)) != 0 {
        // None (SnapshotAny) never needs serialization (C
        // SerializationNeededForRead requires an MVCC snapshot).
        if let Some(snap) = snapshot.as_deref() {
            predicate_seams::predicate_lock_relation::call(relation, snap)?;
        }
    }

    let mut scan = HeapScanDescData {
        rs_base: TableScanDescData {
            rs_rd: relation.alias(),
            rs_snapshot: snapshot,
            rs_nkeys: nkeys,
            rs_key: key,
            rs_mintid: ItemPointerData::invalid(),
            rs_maxtid: ItemPointerData::invalid(),
            rs_flags: flags,
            rs_parallel: parallel_scan,
            rs_am: TableAm::Heap,
        },
        rs_nblocks: 0,
        rs_startblock: 0,
        rs_numblocks: InvalidBlockNumber,
        rs_inited: false,
        rs_coffset: 0,
        rs_cblock: InvalidBlockNumber,
        rs_cbuf: None,
        rs_strategy: None,
        rs_ctup: None,
        rs_dir: ScanDirection::ForwardScanDirection,
        rs_prefetch_block: InvalidBlockNumber,
        rs_parallelworkerdata: parallel_scan.map(|_| Default::default()),
        rs_cindex: 0,
        rs_ntuples: 0,
        rs_cpage: core::ptr::null_mut(),
        rs_vistuples: [0; MaxHeapTuplesPerPage],
        rs_pgstat_numscans: 0,
        rs_pgstat_getnext: 0,
    };

    initscan(&mut scan, None, false)?;
    Ok(scan)
}

pub fn heap_rescan(
    scan: &mut HeapScanDescData<'_>,
    key: Option<&[ScanKeyData]>,
    set_params: bool,
    allow_strat: bool,
    allow_sync: bool,
    allow_pagemode: bool,
) -> PgResult<()> {
    if set_params {
        if allow_strat {
            scan.rs_base.rs_flags |= SO_ALLOW_STRAT;
        } else {
            scan.rs_base.rs_flags &= !SO_ALLOW_STRAT;
        }
        if allow_sync {
            scan.rs_base.rs_flags |= SO_ALLOW_SYNC;
        } else {
            scan.rs_base.rs_flags &= !SO_ALLOW_SYNC;
        }
        if allow_pagemode && scan.rs_base.rs_snapshot.as_deref().is_some_and(IsMVCCSnapshot) {
            scan.rs_base.rs_flags |= SO_ALLOW_PAGEMODE;
        } else {
            scan.rs_base.rs_flags &= !SO_ALLOW_PAGEMODE;
        }
    }

    scan.rs_ctup = None;
    scan.rs_cpage = core::ptr::null_mut();
    if let Some(pin) = scan.rs_cbuf.take() {
        pin.release();
    }

    initscan(scan, key, true)
}

pub fn heap_endscan(mut scan: HeapScanDescData<'_>) -> PgResult<()> {
    scan.rs_ctup = None;
    pgstat::relation::pgstat_count_heap_scan_batched(
        scan.rs_base.rs_rd.rd_id,
        scan.rs_base.rs_rd.rd_rel.relisshared,
        scan.rs_pgstat_numscans,
        scan.rs_pgstat_getnext,
    );
    if let Some(pin) = scan.rs_cbuf.take() {
        pin.release();
    }

    if scan.rs_strategy.is_some() {
        bufmgr_seams::free_access_strategy::call(scan.rs_strategy.take());
    }

    if (scan.rs_base.rs_flags & SO_TEMP_SNAPSHOT) != 0 {
        unported("backend-utils-time-snapmgr (UnregisterSnapshot)");
    }

    // rs_rd alias drop = RelationDecrementReferenceCount; rs_key drops.
    Ok(())
}

pub fn heap_getnext<'a, 'mcx>(
    scan: &'a mut HeapScanDescData<'mcx>,
    direction: ScanDirection,
) -> PgResult<Option<&'a HeapTupleData<'mcx>>> {
    // C's "only heap AM" ereport is subsumed by the closed TableAm carrier.
    match scan.rs_base.rs_am {
        TableAm::Heap => {}
    }
    if unexpected_during_logical_decoding() {
        return Err(elog_error(
            "unexpected heap_getnext call during logical decoding",
        ));
    }

    if (scan.rs_base.rs_flags & SO_ALLOW_PAGEMODE) != 0 {
        heapgettup_pagemode(scan, direction)?;
    } else {
        heapgettup(scan, direction)?;
    }

    if scan.rs_ctup.is_none() {
        return Ok(None);
    }
    pgstat_count_heap_getnext(scan);
    Ok(scan.rs_ctup.as_ref())
}

pub fn heap_getnextslot<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut HeapScanDescData<'mcx>,
    direction: ScanDirection,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    if (scan.rs_base.rs_flags & SO_ALLOW_PAGEMODE) != 0 {
        heapgettup_pagemode(scan, direction)?;
    } else {
        heapgettup(scan, direction)?;
    }

    if scan.rs_ctup.is_none() {
        exectuples::exec_clear_tuple(slot, mcx);
        return Ok(false);
    }

    pgstat_count_heap_getnext(scan);
    store_ctup_into_slot(mcx, scan, slot);
    Ok(true)
}

#[inline]
fn store_ctup_into_slot<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut HeapScanDescData<'mcx>,
    slot: &mut SlotData<'mcx>,
) {
    debug_assert!(scan.rs_ctup.is_some() && scan.rs_cbuf.is_some());
    // SAFETY: caller checked rs_ctup is Some; the struct invariant ties a
    // Some rs_ctup to a pinned rs_cbuf (C: rs_ctup.t_data != NULL implies
    // rs_cbuf is valid).
    let (t, pin) = unsafe {
        (
            scan.rs_ctup.as_ref().unwrap_unchecked(),
            scan.rs_cbuf.as_ref().unwrap_unchecked(),
        )
    };
    // SAFETY: same pinned image as rs_ctup; ExecStoreBufferHeapTuple takes its own pin (C contract).
    let tuple = unsafe {
        HeapTupleData::from_raw_parts(t.header_ptr(), t.t_len, t.t_self, t.t_tableOid)
    };
    exectuples::exec_store_buffer_heap_tuple(slot, mcx, tuple, pin.buffer());
}

pub fn heap_set_tidrange(
    scan: &mut HeapScanDescData<'_>,
    mintid: &ItemPointerData,
    maxtid: &ItemPointerData,
) {
    if scan.rs_nblocks == 0 {
        return;
    }

    let mut highest_item = ItemPointerData::new(scan.rs_nblocks - 1, MaxOffsetNumber);
    let mut lowest_item = ItemPointerData::new(0, FirstOffsetNumber);

    if ItemPointerCompare(maxtid, &highest_item) < 0 {
        highest_item = *maxtid;
    }
    if ItemPointerCompare(mintid, &lowest_item) > 0 {
        lowest_item = *mintid;
    }

    if ItemPointerCompare(&highest_item, &lowest_item) < 0 {
        heap_setscanlimits(scan, 0, 0);
        return;
    }

    let start_blk = ItemPointerGetBlockNumberNoCheck(&lowest_item);
    let num_blks = ItemPointerGetBlockNumberNoCheck(&highest_item)
        - ItemPointerGetBlockNumberNoCheck(&lowest_item)
        + 1;

    heap_setscanlimits(scan, start_blk, num_blks);
    scan.rs_base.rs_mintid = lowest_item;
    scan.rs_base.rs_maxtid = highest_item;
}

pub fn heap_getnextslot_tidrange<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut HeapScanDescData<'mcx>,
    direction: ScanDirection,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    let mintid = scan.rs_base.rs_mintid;
    let maxtid = scan.rs_base.rs_maxtid;

    loop {
        if (scan.rs_base.rs_flags & SO_ALLOW_PAGEMODE) != 0 {
            heapgettup_pagemode(scan, direction)?;
        } else {
            heapgettup(scan, direction)?;
        }

        let Some(t) = scan.rs_ctup.as_ref() else {
            exectuples::exec_clear_tuple(slot, mcx);
            return Ok(false);
        };

        // setscanlimits bounded the pages; boundary-page TIDs still need filtering.
        if ItemPointerCompare(&t.t_self, &mintid) < 0 {
            exectuples::exec_clear_tuple(slot, mcx);
            if ScanDirectionIsBackward(direction) {
                return Ok(false);
            }
            continue;
        }
        if ItemPointerCompare(&t.t_self, &maxtid) > 0 {
            exectuples::exec_clear_tuple(slot, mcx);
            if ScanDirectionIsForward(direction) {
                return Ok(false);
            }
            continue;
        }
        break;
    }

    pgstat_count_heap_getnext(scan);
    store_ctup_into_slot(mcx, scan, slot);
    Ok(true)
}
