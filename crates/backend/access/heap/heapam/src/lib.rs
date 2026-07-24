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
use ::types_core::xact::{InvalidTransactionId, TransactionIdPrecedes};
use ::types_core::{
    BlockNumber, Buffer, ForkNumber, InvalidBlockNumber, MultiXactId, OffsetNumber, TransactionId,
};
use ::types_error::{PgError, PgResult};
use ::types_fmgr::LocalFcinfo;
use ::types_rel::{Relation, RelationData};
use ::types_scan::scankey::{ScanKeyData, SK_ISNULL};
use ::types_scan::sdir::{ScanDirection, ScanDirectionIsForward};
use ::types_slot::SlotData;
use ::types_snapshot::{HTSV_Result, IsMVCCSnapshot, SnapshotData, SnapshotType, XidVisMemo};
use ::types_storage::buf::{BufferAccessStrategy, BufferAccessStrategyType};
use ::types_storage::bufpage::{MaxHeapTuplesPerPage, MaxOffsetNumber, PageRef};
use ::types_storage::multixact::ISUPDATE_from_mxstatus;
use ::types_tuple::{
    heap_getattr, FirstOffsetNumber, HeapTupleData, HeapTupleHeaderData, ItemPointerCompare,
    ItemPointerData, ItemPointerGetBlockNumberNoCheck, HEAP_XMAX_INVALID, HEAP_XMAX_IS_LOCKED_ONLY,
    HEAP_XMAX_IS_MULTI,
};

use heapam_visibility_seams as hv_seam;

pub mod bitmap;
pub mod dml;
pub mod fetch;
pub mod freeze;
pub mod hio;
pub mod index_delete;
pub mod inplace;
pub mod sample;
pub(crate) mod wal;
pub use dml::{
    heap_abort_speculative, heap_delete, heap_finish_speculative, heap_insert, heap_lock_tuple,
    heap_multi_insert, heap_update, relation_is_accessible_in_logical_decoding,
    relation_is_logically_logged, relation_needs_wal, simple_heap_delete, simple_heap_insert,
    simple_heap_update, ParallelWriteGuard,
};
pub use fetch::{heap_fetch, heap_fetch_dirty, heap_get_latest_tid, heap_hot_search_buffer};
pub use hio::{
    GetBulkInsertState, RelationGetBufferForTuple, RelationPutHeapTuple, ReleaseBulkInsertStatePin,
};
pub use index_delete::heap_index_delete_tuples;
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
    // rs_dir deleted (backward-execution wave B7): heap scans step forward
    // only; the direction-change prefetch reset it powered is dead.
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
    // The registered handle behind SO_TEMP_SNAPSHOT: rs_snapshot's lifetime is
    // 'mcx-erased, so the 'static Rc UnregisterSnapshot needs is kept here.
    pub rs_temp_snapshot: Option<std::rc::Rc<SnapshotData<'static>>>,
}

impl<'mcx> HeapScanDescData<'mcx> {
    /// C's `&scan->rs_ctup` after a fetching call: valid while `rs_cbuf`
    /// stays pinned (enforced here by the `&self` borrow).
    #[inline]
    pub fn rs_ctup(&self) -> Option<&HeapTupleData<'mcx>> {
        self.rs_ctup.as_ref()
    }
}

fn elog_error(message: impl Into<std::string::String>) -> Box<PgError> {
    Box::new(PgError::error(message))
}

// CheckXidAlive (xact.c) is set while logical decoding replays a prepared
// (or, phase-2, streamed) transaction; direct heap access outside the
// systable_* wrappers (which set bsysscan) is a decode-path bug. Mirrors
// heapam.c:1361's `unlikely(TransactionIdIsValid(CheckXidAlive) && !bsysscan)`.
#[inline]
fn unexpected_during_logical_decoding() -> bool {
    TransactionIdIsValid(xact::CheckXidAlive()) && !xact::bsysscan()
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
    if (tuple.t_infomask & ::types_tuple::HEAP_MOVED) != 0 && TransactionIdIsNormal(tuple.xvac()) {
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

/// Position the scan on the block-range morsel claim `[b0, b1)` (M1 heap
/// morsel source): the heap analog of pgrcolumnar's `set_granule_range`, and a
/// repositionable `heap_setscanlimits` — callable between claims (the
/// previous claim drained to `end_of_scan`, or never started). The claim
/// unit is one block, so the range maps directly onto the
/// `rs_startblock`/`rs_numblocks` limit walk C's TID-range scans use:
/// forward iteration visits exactly blocks `b0..b1`, no wrap.
///
/// Morsel-positioned scans never follow or report the syncscan hint (their
/// start position is the runtime's claim, not a scan-order heuristic), so
/// `SO_ALLOW_SYNC` is cleared — which also satisfies the
/// `heap_setscanlimits` invariant this function inherits.
pub fn heap_set_block_range(scan: &mut HeapScanDescData<'_>, b0: u64, b1: u64) -> PgResult<()> {
    if scan.rs_base.rs_parallel.is_some() {
        return Err(elog_error(
            "heap: block-range positioning on a parallel scan".to_string(),
        ));
    }
    if b0 >= b1 || b1 > scan.rs_nblocks as u64 {
        return Err(elog_error(format!(
            "heap: invalid block range [{b0}, {b1}) of {}",
            scan.rs_nblocks
        )));
    }
    scan.rs_base.rs_flags &= !SO_ALLOW_SYNC;
    // Unconditional reset to the un-inited state (end_of_scan shape): a
    // drained previous claim already looks like this; a defensive caller
    // repositioning mid-claim releases the pin here.
    scan.rs_ctup = None;
    scan.rs_cpage = core::ptr::null_mut();
    if let Some(pin) = scan.rs_cbuf.take() {
        pin.release();
    }
    scan.rs_cblock = InvalidBlockNumber;
    scan.rs_prefetch_block = InvalidBlockNumber;
    scan.rs_inited = false;
    scan.rs_ntuples = 0;
    scan.rs_cindex = 0;
    scan.rs_startblock = b0 as BlockNumber;
    scan.rs_numblocks = (b1 - b0) as BlockNumber;
    Ok(())
}

/// End-of-claim pin release (single-executor wave 2, WS-O inc-2 —
/// append-only seam; the R3 "zero pins at claim settle" tightening the
/// Phase-1 contract deferred at WS-K Q3): reset the scan to the drained
/// (un-inited) state, releasing the current page pin. A claim that ended
/// EARLY (error mid-batch, abort between segments, shed) may still hold
/// `rs_cbuf`; a normally-drained claim is already in this state and every
/// store below is idempotent. The body mirrors `heap_set_block_range`'s
/// reset half exactly (deliberately duplicated, not extracted — the
/// positioning path's code shape is untouched); the next
/// `heap_set_block_range` positions the following claim as before.
pub fn heap_end_claim_release(scan: &mut HeapScanDescData<'_>) {
    scan.rs_ctup = None;
    scan.rs_cpage = core::ptr::null_mut();
    if let Some(pin) = scan.rs_cbuf.take() {
        pin.release();
    }
    scan.rs_cblock = InvalidBlockNumber;
    scan.rs_prefetch_block = InvalidBlockNumber;
    scan.rs_inited = false;
    scan.rs_ntuples = 0;
    scan.rs_cindex = 0;
}

/// Cursor-suspension park point (WS-AI wave-9.5, lane-cursors.md §2,
/// append-only next to `heap_end_claim_release`, its record-then-release
/// companion): the reposition window `(b0, b1)` of a mid-claim forward
/// scan whose staged (pinned) page is `b0` and whose unvisited remainder
/// is exactly `[b0, b1)` under a linear no-wrap walk — the shape
/// `heap_set_block_range(b0, b1)` restores after the release. None = not
/// settleable:
/// * nothing staged/pinned (`!rs_inited` / no `rs_cbuf`) — a drained or
///   never-started claim holds nothing;
/// * parallel scans (block order owned by the shared DSM cursor);
/// * non-forward walks (the lane drive is forward-only; belt for the
///   probe's remainder arithmetic);
/// * wrap-capable walks: `SO_ALLOW_SYNC` set, or an unlimited walk
///   (`rs_numblocks == InvalidBlockNumber`) that started mid-relation —
///   their remainder is not a contiguous `[b0, end)` range, and the
///   C-parity pin-held posture stands for them.
///
/// Remainder arithmetic (heapgettup_advance_block): on a limited walk
/// `rs_numblocks` counts the blocks left INCLUDING the currently-staged
/// one, so `b1 = b0 + rs_numblocks`; on an unlimited 0-started walk,
/// `b1 = rs_nblocks`. Idempotent across settle/resume cycles: resume sets
/// `[b0, b1)` via `heap_set_block_range`, whose walk re-reports the same
/// end at the next suspension.
pub fn heap_cursor_park_point(scan: &HeapScanDescData<'_>) -> Option<(u64, u64)> {
    if scan.rs_base.rs_parallel.is_some()
        || !scan.rs_inited
        || scan.rs_cbuf.is_none()
        || (scan.rs_base.rs_flags & SO_ALLOW_SYNC) != 0
    {
        return None;
    }
    let b0 = scan.rs_cblock as u64;
    let b1 = if scan.rs_numblocks == InvalidBlockNumber {
        if scan.rs_startblock != 0 {
            // Mid-relation start on an unlimited walk wraps past the end;
            // [b0, rs_nblocks) is not the remainder. Not settleable.
            return None;
        }
        scan.rs_nblocks as u64
    } else {
        b0 + scan.rs_numblocks as u64
    };
    debug_assert!(b0 < b1 && b1 <= scan.rs_nblocks as u64);
    Some((b0, b1))
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
    // Page-batch visibility: resolve each distinct xid's status once per page.
    let mvcc = snapshot.snapshot_type == SnapshotType::SNAPSHOT_MVCC;
    let mut memo = XidVisMemo::new();

    // All-visible pages consult lp_flags alone, so an all-normal page (the
    // vacuumed common case) selects exactly 1..=lines: one vectorizable
    // flag reduction + a sequential fill replace the item-at-a-time walk.
    // Any non-normal line falls to the exact walk below.
    if ALL_VISIBLE && !CHECK_SERIALIZABLE {
        let mut all_normal = true;
        let mut off = FirstOffsetNumber;
        while off <= lines {
            // SAFETY: off <= lines <= MaxHeapTuplesPerPage (fn contract).
            all_normal &= unsafe { page.item_id_unchecked(off) }.is_normal();
            off += 1;
        }
        if all_normal {
            let mut i: u32 = 0;
            while i < lines as u32 {
                // SAFETY: i < lines <= MaxHeapTuplesPerPage (fn contract).
                unsafe { *vistuples.get_unchecked_mut(i as usize) = (i + 1) as OffsetNumber };
                i += 1;
            }
            return Ok(lines as u32);
        }
    }

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
            } else if mvcc {
                hv_seam::heap_tuple_satisfies_mvcc_page::call(
                    &mut loctup,
                    snapshot,
                    buffer,
                    &mut memo,
                )?
            } else {
                hv_seam::heap_tuple_satisfies_visibility::call(&mut loctup, snapshot, buffer)?
            };
            if CHECK_SERIALIZABLE {
                HeapCheckForSerializableConflictOut(
                    valid,
                    relation,
                    &mut loctup,
                    buffer,
                    snapshot,
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
        scan.rs_cbuf
            .as_ref()
            .expect("pagescan without buffer")
            .buffer(),
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

// Forward-only (backward-execution wave B7): C heapam.c's backward stepping
// arms - heapgettup_initial_block's last-block start + syncscan disarm,
// heapgettup_advance_block's decrement-with-wrap, the start/continue-page
// last-line walks, and the `+= dir` line stepping - are deleted. The only
// runtime-direction callers (nodeseqscan / nodetidrangescan es_direction)
// are forward-invariant below the run seam (deletion-prep B1); every other
// caller in the tree (index builds/validation, CLUSTER copy, COPY TO, FK
// validation, typecmds/tablecmds rewrites, partbounds probes, genam) passes
// ForwardScanDirection literally. C RETAINS backward heap scans; ratified
// strategy divergence (Michael's 2026-07-17 SCROLL/WITH-HOLD decision).
pub fn heapgettup_initial_block(scan: &mut HeapScanDescData<'_>) -> BlockNumber {
    debug_assert!(!scan.rs_inited);
    debug_assert!(scan.rs_base.rs_parallel.is_none());

    if scan.rs_nblocks == 0 || scan.rs_numblocks == 0 {
        return InvalidBlockNumber;
    }

    scan.rs_startblock
}

pub fn heapgettup_advance_block(
    scan: &mut HeapScanDescData<'_>,
    mut block: BlockNumber,
) -> PgResult<BlockNumber> {
    debug_assert!(scan.rs_base.rs_parallel.is_none());

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
}

// heap_scan_stream_read_next_parallel's block arithmetic, inline.
fn parallel_next_block(scan: &mut HeapScanDescData<'_>, first: bool) -> PgResult<BlockNumber> {
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
        ::tableam_vocab::table_block_parallelscan_startblock_init(
            &scan.rs_base.rs_rd,
            worker,
            pbscan,
        )?;
    }
    ::tableam_vocab::table_block_parallelscan_nextpage(&scan.rs_base.rs_rd, worker, pbscan)
}

fn heap_fetch_next_buffer(scan: &mut HeapScanDescData<'_>) -> PgResult<()> {
    scan.rs_cpage = core::ptr::null_mut();
    if let Some(pin) = scan.rs_cbuf.take() {
        pin.release();
    }

    check_for_interrupts()?;

    let next = if scan.rs_base.rs_parallel.is_some() {
        let first = !scan.rs_inited;
        scan.rs_inited = true;
        parallel_next_block(scan, first)?
    } else if !scan.rs_inited {
        let b = heapgettup_initial_block(scan);
        scan.rs_inited = true;
        b
    } else {
        heapgettup_advance_block(scan, scan.rs_prefetch_block)?
    };
    scan.rs_prefetch_block = next;

    if next == InvalidBlockNumber {
        return Ok(());
    }

    // Forward scans read the miss run ahead in one combined smgrreadv
    // (read_stream's combining without its pin handoff); the hint never runs
    // past relation end or the wrapped serial scan's logical end
    // (rs_startblock), and bufmgr caps it by io_combine_limit and the md
    // segment boundary. Parallel chunk boundaries are not capped: an over-read
    // lands valid in the pool and becomes another worker's hit.
    let nblocks_ahead = if next < scan.rs_nblocks {
        if scan.rs_base.rs_parallel.is_some() || next >= scan.rs_startblock {
            scan.rs_nblocks - next
        } else {
            scan.rs_startblock - next
        }
    } else {
        1
    };
    let buf = if nblocks_ahead > 1 && bufmgr_seams::read_buffer_batched::is_installed() {
        bufmgr_seams::read_buffer_batched::call(
            &scan.rs_base.rs_rd,
            next,
            nblocks_ahead,
            scan.rs_strategy.clone(),
        )?
    } else {
        bufmgr_seams::read_buffer_strategy::call(
            &scan.rs_base.rs_rd,
            next,
            scan.rs_strategy.clone(),
        )?
    };
    scan.rs_cbuf = BufferPin::adopt(buf);
    if let Some(pin) = scan.rs_cbuf.as_ref() {
        scan.rs_cblock = pin.block_number();
    }
    Ok(())
}

fn heapgettup_start_page(page: &PageRef<'_>, linesleft: &mut i32, lineoff: &mut OffsetNumber) {
    *linesleft = page.max_offset_number() as i32 - FirstOffsetNumber as i32 + 1;
    *lineoff = FirstOffsetNumber;
}

fn heapgettup_continue_page(
    page: &PageRef<'_>,
    coffset: OffsetNumber,
    linesleft: &mut i32,
    lineoff: &mut OffsetNumber,
) {
    let max = page.max_offset_number();
    *lineoff = coffset + 1;
    *linesleft = max as i32 - *lineoff as i32 + 1;
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

// C HeapKeyTest: sk_func runs via FunctionCall2Coll, pallocing (e.g. a
// detoasted by-ref key column) in the caller's context. There's no per-tuple
// mcx reachable this deep in heapgettup, so a reset-per-call scratch context
// stands in for it (the hash-support-proc precedent in
// access/hash/hash/src/util.rs's HASH_PROC_SCRATCH).
std::thread_local! {
    static KEY_TEST_SCRATCH: core::cell::RefCell<::mcx::MemoryContext> =
        core::cell::RefCell::new(::mcx::MemoryContext::new_bump("heap key test scratch"));
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
        let test = KEY_TEST_SCRATCH.with(|cell| -> PgResult<::datum::Datum> {
            match cell.try_borrow_mut() {
                Ok(mut ctx) => {
                    ctx.reset();
                    // SAFETY: reset-only context, outlives this call.
                    unsafe { fcinfo.set_result_mcx(ctx.mcx()) };
                    fcinfo.set_arg(0, atp);
                    fcinfo.set_arg(1, cur_key.sk_argument);
                    cur_key.sk_func.invoke(&mut fcinfo)
                }
                Err(_) => {
                    let ctx = ::mcx::MemoryContext::new_bump("heap key test scratch (reentrant)");
                    // SAFETY: as above.
                    unsafe { fcinfo.set_result_mcx(ctx.mcx()) };
                    fcinfo.set_arg(0, atp);
                    fcinfo.set_arg(1, cur_key.sk_argument);
                    cur_key.sk_func.invoke(&mut fcinfo)
                }
            }
        })?;
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
fn heapgettup<'mcx>(scan: &mut HeapScanDescData<'mcx>) -> PgResult<()> {
    let nkeys = scan.rs_base.rs_nkeys;
    let mut linesleft: i32 = 0;
    let mut lineoff: OffsetNumber = 0;
    // C's `goto continue_page` from the rs_inited entry.
    let mut continue_page = scan.rs_inited;

    loop {
        if !continue_page {
            heap_fetch_next_buffer(scan)?;
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
                heapgettup_continue_page(&page, scan.rs_coffset, &mut linesleft, &mut lineoff);
            } else {
                heapgettup_start_page(&page, &mut linesleft, &mut lineoff);
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
                lineoff += 1;
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
fn pagemode_next_page(scan: &mut HeapScanDescData<'_>) -> PgResult<bool> {
    heap_fetch_next_buffer(scan)?;
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
fn heapgettup_pagemode<'mcx>(scan: &mut HeapScanDescData<'mcx>) -> PgResult<()> {
    let nkeys = scan.rs_base.rs_nkeys;
    let relid = scan.rs_base.rs_rd.rd_id;
    let mut lineindex: i32 = 0;
    let mut linesleft: i32 = 0;
    let mut continue_page = scan.rs_inited;

    if scan.rs_inited {
        lineindex = scan.rs_cindex as i32 + 1;
        linesleft = scan.rs_ntuples as i32 - lineindex;
    }

    loop {
        if !continue_page {
            if !pagemode_next_page(scan)? {
                break;
            }
            linesleft = scan.rs_ntuples as i32;
            lineindex = 0;
        }
        continue_page = false;

        debug_assert!(!scan.rs_cpage.is_null() && scan.rs_cbuf.is_some());
        // SAFETY: rs_cpage is the image of the page pinned by rs_cbuf
        // (pagemode_next_page set it; every pin move nulls it), so it stays
        // valid across this walk. No call edge on the per-tuple path.
        let page: PageRef<'_> = unsafe { PageRef::from_raw(NonNull::new_unchecked(scan.rs_cpage)) };

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
            lineindex += 1;
        }
    }

    end_of_scan(scan);
    Ok(())
}

// Page-batch scan feed (upstream batch table-AM scan design, CF 6176):
// forward pagemode only, whole pages consumed by the fused executor drive.
// INVARIANT: no interleaving with per-tuple getnext calls on the same scan
// (rs_cindex parks at the page end so a stray continue-walk advances pages).
pub fn heap_getnextpagebatch(scan: &mut HeapScanDescData<'_>) -> PgResult<u32> {
    debug_assert!((scan.rs_base.rs_flags & SO_ALLOW_PAGEMODE) != 0);
    debug_assert!(scan.rs_base.rs_nkeys == 0);
    loop {
        if !pagemode_next_page(scan)? {
            end_of_scan(scan);
            return Ok(0);
        }
        if scan.rs_ntuples > 0 {
            scan.rs_cindex = scan.rs_ntuples - 1;
            if scan.rs_base.rs_rd.pgstat_enabled.get() {
                scan.rs_pgstat_getnext += scan.rs_ntuples as u64;
            }
            return Ok(scan.rs_ntuples);
        }
    }
}

/// Mid-page adoption for a FRESH batch engagement over a scan the PER-TUPLE
/// pagemode walk already advanced (SE-R41 v2, the page-remainder defect fix;
/// notes/se-r41-v2.md §2): `heap_getnextpagebatch`'s documented invariant is
/// "no interleaving with per-tuple getnext calls" because it ADVANCES pages —
/// a batch drive that freshly engages while the row walk sits mid-page would
/// silently skip the current page's unconsumed remainder (the SE12
/// budget-floor probe's es_processed 8-vs-16 loss). This probe ADOPTS that
/// remainder instead: if the scan holds a per-tuple-walked page with
/// unreturned visible tuples, park `rs_cindex` at the page end (the batch
/// consumption convention — a stray per-tuple continue advances pages) and
/// hand the batch drive the window `[start, n)` over the ALREADY-COLLECTED
/// `rs_vistuples` of the pinned page. `rs_cindex` is the index of the tuple
/// the per-tuple walk last RETURNED (`lineindex = rs_cindex + dir`), so the
/// remainder starts at `rs_cindex + 1`; rows `<= rs_cindex` were already
/// delivered to the caller's qual by the row walk. None = nothing to adopt
/// (fresh/drained scan, or the page is fully consumed): the caller stages
/// the NEXT page via `heap_getnextpagebatch` as before. Forward pagemode
/// only (the batch drive's admission gates).
pub fn heap_adopt_midpage_batch(scan: &mut HeapScanDescData<'_>) -> Option<(u32, u32)> {
    debug_assert!((scan.rs_base.rs_flags & SO_ALLOW_PAGEMODE) != 0);
    debug_assert!(scan.rs_base.rs_nkeys == 0);
    if !scan.rs_inited || scan.rs_cbuf.is_none() || scan.rs_ntuples == 0 {
        return None;
    }
    let start = scan.rs_cindex + 1;
    if start >= scan.rs_ntuples {
        return None;
    }
    scan.rs_cindex = scan.rs_ntuples - 1;
    Some((start, scan.rs_ntuples))
}

pub fn heap_batch_deform_soa<'mcx>(
    scan: &mut HeapScanDescData<'mcx>,
    plan: &exectuples::SoaDeformPlan<'_>,
    soa: &mut exectuples::SoaBatch<'_>,
    qual_col_only: Option<u16>,
) {
    let n = scan.rs_ntuples;
    debug_assert!(!scan.rs_cpage.is_null() || n == 0);
    soa.begin(n);
    let relid = scan.rs_base.rs_rd.rd_id;
    let atts: &[_] = &scan.rs_base.rs_rd.rd_att.compact_attrs;
    if n == 0 {
        return;
    }
    // SAFETY: as heap_batch_store_slot — pinned page, offsets from
    // page_collect_tuples under the per-page bound.
    let page: PageRef<'_> = unsafe { PageRef::from_raw(NonNull::new_unchecked(scan.rs_cpage)) };
    // Descending i = ascending tuple addresses (pages fill from pd_upper down;
    // scanqual -8.3% wall); SoA writes are positional, so output is order-free.
    let mut i = n;
    while i != 0 {
        i -= 1;
        let (ptr, len, lineoff) = unsafe {
            let lineoff = *scan.rs_vistuples.get_unchecked(i as usize);
            let lpp = page.item_id_unchecked(lineoff);
            debug_assert!(lpp.is_normal());
            let (ptr, len) = page.item_raw_unchecked(lpp);
            (ptr, len, lineoff)
        };
        // SAFETY: image on the page pinned by rs_cbuf for the whole batch.
        let tuple = unsafe {
            HeapTupleData::from_raw_parts(
                ptr,
                len,
                ItemPointerData::new(scan.rs_cblock, lineoff),
                relid,
            )
        };
        exectuples::soa_classify_row(soa, plan, atts, i, &tuple);
    }
    exectuples::soa_deform_columns(soa, plan, atts, qual_col_only);
}

/// `heap_batch_deform_soa` with the kind-0 column pass narrowed to an
/// explicit column SET (K1 inc-2 late materialization, wave-9 WS-AH:
/// {qual clause cols ∪ the grouped feed's key cols}). Classification is
/// IDENTICAL — kind-1 hasnulls rows still deform fully at classify (a
/// harmless superset), kind-2 narrow rows carry the fallback bit — only
/// the column-major pass narrows. Survivors complete later through
/// `heap_batch_complete_deform_soa` (value movement only: same rows,
/// same survivor set/order, same errors as the full staging deform).
pub fn heap_batch_deform_soa_cols<'mcx>(
    scan: &mut HeapScanDescData<'mcx>,
    plan: &exectuples::SoaDeformPlan<'_>,
    soa: &mut exectuples::SoaBatch<'_>,
    cols: &[u16],
) {
    let n = scan.rs_ntuples;
    debug_assert!(!scan.rs_cpage.is_null() || n == 0);
    soa.begin(n);
    let relid = scan.rs_base.rs_rd.rd_id;
    let atts: &[_] = &scan.rs_base.rs_rd.rd_att.compact_attrs;
    if n == 0 {
        return;
    }
    // SAFETY: as heap_batch_deform_soa — pinned page, offsets from
    // page_collect_tuples under the per-page bound.
    let page: PageRef<'_> = unsafe { PageRef::from_raw(NonNull::new_unchecked(scan.rs_cpage)) };
    let mut i = n;
    while i != 0 {
        i -= 1;
        let (ptr, len, lineoff) = unsafe {
            let lineoff = *scan.rs_vistuples.get_unchecked(i as usize);
            let lpp = page.item_id_unchecked(lineoff);
            debug_assert!(lpp.is_normal());
            let (ptr, len) = page.item_raw_unchecked(lpp);
            (ptr, len, lineoff)
        };
        // SAFETY: image on the page pinned by rs_cbuf for the whole batch.
        let tuple = unsafe {
            HeapTupleData::from_raw_parts(
                ptr,
                len,
                ItemPointerData::new(scan.rs_cblock, lineoff),
                relid,
            )
        };
        exectuples::soa_classify_row(soa, plan, atts, i, &tuple);
    }
    exectuples::soa_deform_columns_set(soa, plan, atts, cols, None);
}

/// Completion half of the K1 inc-2 deform split: fill `cols` for
/// `sel`-selected kind-0 rows of the ALREADY-staged batch — no re-begin,
/// no re-classify (the staged batch's kinds/fallback state is live and
/// the page is still pinned by rs_cbuf, ownership ABI R3: valid from the
/// staging `next_batch` until the next batch advance/reposition/settle).
/// Idempotent per (column, row).
pub fn heap_batch_complete_deform_soa<'mcx>(
    scan: &HeapScanDescData<'mcx>,
    plan: &exectuples::SoaDeformPlan<'_>,
    soa: &mut exectuples::SoaBatch<'_>,
    cols: &[u16],
    sel: &[u64],
) {
    debug_assert!(!scan.rs_cpage.is_null() || soa.nrows() == 0);
    debug_assert!(
        soa.nrows() <= scan.rs_ntuples,
        "completion outside the staged batch"
    );
    let atts: &[_] = &scan.rs_base.rs_rd.rd_att.compact_attrs;
    exectuples::soa_deform_columns_set(soa, plan, atts, cols, Some(sel));
}

pub fn heap_batch_stage_varkey<'mcx>(
    scan: &mut HeapScanDescData<'mcx>,
    plan: &exectuples::SoaVarKeyPlan,
    soa: &mut exectuples::SoaBatch<'_>,
) {
    let n = scan.rs_ntuples;
    debug_assert!(!scan.rs_cpage.is_null() || n == 0);
    soa.begin(n);
    let relid = scan.rs_base.rs_rd.rd_id;
    let atts: &[_] = &scan.rs_base.rs_rd.rd_att.compact_attrs;
    if n == 0 {
        return;
    }
    // SAFETY: as heap_batch_deform_soa — pinned page, offsets from
    // page_collect_tuples under the per-page bound.
    let page: PageRef<'_> = unsafe { PageRef::from_raw(NonNull::new_unchecked(scan.rs_cpage)) };
    let mut i = n;
    while i != 0 {
        i -= 1;
        let (ptr, len, lineoff) = unsafe {
            let lineoff = *scan.rs_vistuples.get_unchecked(i as usize);
            let lpp = page.item_id_unchecked(lineoff);
            debug_assert!(lpp.is_normal());
            let (ptr, len) = page.item_raw_unchecked(lpp);
            (ptr, len, lineoff)
        };
        // SAFETY: image on the page pinned by rs_cbuf for the whole batch.
        let tuple = unsafe {
            HeapTupleData::from_raw_parts(
                ptr,
                len,
                ItemPointerData::new(scan.rs_cblock, lineoff),
                relid,
            )
        };
        exectuples::soa_stage_varkey(soa, plan, atts, i, &tuple);
    }
}

#[inline(always)]
pub fn heap_batch_store_slot<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut HeapScanDescData<'mcx>,
    i: u32,
    slot: &mut SlotData<'mcx>,
) {
    debug_assert!(i < scan.rs_ntuples && !scan.rs_cpage.is_null());
    // SAFETY: the heapgettup_pagemode walk verbatim — i < rs_ntuples <=
    // MaxHeapTuplesPerPage (heap_prepare_pagescan's per-page bound), lineoff
    // from page_collect_tuples on the page pinned by rs_cbuf.
    let (ptr, len, lineoff) = unsafe {
        let lineoff = *scan.rs_vistuples.get_unchecked(i as usize);
        let page: PageRef<'_> = PageRef::from_raw(NonNull::new_unchecked(scan.rs_cpage));
        let lpp = page.item_id_unchecked(lineoff);
        debug_assert!(lpp.is_normal());
        let (ptr, len) = page.item_raw_unchecked(lpp);
        (ptr, len, lineoff)
    };
    let pin = scan.rs_cbuf.as_ref().expect("batch store without buffer");
    // SAFETY: image on the page pinned by rs_cbuf; the buffer-slot store
    // takes its own pin (C contract).
    let tuple = unsafe {
        HeapTupleData::from_raw_parts(
            ptr,
            len,
            ItemPointerData::new(scan.rs_cblock, lineoff),
            scan.rs_base.rs_rd.rd_id,
        )
    };
    exectuples::exec_store_buffer_heap_tuple(slot, mcx, tuple, pin.buffer());
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
        rs_prefetch_block: InvalidBlockNumber,
        rs_parallelworkerdata: parallel_scan.map(|_| Default::default()),
        rs_cindex: 0,
        rs_ntuples: 0,
        rs_cpage: core::ptr::null_mut(),
        rs_vistuples: [0; MaxHeapTuplesPerPage],
        rs_pgstat_numscans: 0,
        rs_pgstat_getnext: 0,
        rs_temp_snapshot: None,
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
        if allow_pagemode
            && scan
                .rs_base
                .rs_snapshot
                .as_deref()
                .is_some_and(IsMVCCSnapshot)
        {
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
        let snap = scan
            .rs_temp_snapshot
            .take()
            .expect("SO_TEMP_SNAPSHOT scan carries its registered snapshot");
        snapmgr_seams::unregister_snapshot::call(snap);
    }

    // rs_rd alias drop = RelationDecrementReferenceCount; rs_key drops.
    Ok(())
}

/// The `direction` parameter stays for table-AM API parity (C's
/// heap_getnext face); backward stepping is DELETED (backward-execution
/// wave B7) - a backward direction is asserted away, forward-invariant
/// below the run seam (deletion-prep B1).
pub fn heap_getnext<'a, 'mcx>(
    scan: &'a mut HeapScanDescData<'mcx>,
    direction: ScanDirection,
) -> PgResult<Option<&'a HeapTupleData<'mcx>>> {
    debug_assert!(
        ScanDirectionIsForward(direction),
        "backward heap scan below the forward-only run seam (deletion-prep B1)"
    );
    // C's "only heap AM" ereport is subsumed by the closed TableAm carrier.
    match scan.rs_base.rs_am {
        TableAm::Heap => {}
        other => panic!("only heap AM is supported in heap_getnext: {other:?}"),
    }
    if unexpected_during_logical_decoding() {
        return Err(elog_error(
            "unexpected heap_getnext call during logical decoding",
        ));
    }

    if (scan.rs_base.rs_flags & SO_ALLOW_PAGEMODE) != 0 {
        heapgettup_pagemode(scan)?;
    } else {
        heapgettup(scan)?;
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
    debug_assert!(
        ScanDirectionIsForward(direction),
        "backward heap scan below the forward-only run seam (deletion-prep B1)"
    );
    if (scan.rs_base.rs_flags & SO_ALLOW_PAGEMODE) != 0 {
        heapgettup_pagemode(scan)?;
    } else {
        heapgettup(scan)?;
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
    let tuple =
        unsafe { HeapTupleData::from_raw_parts(t.header_ptr(), t.t_len, t.t_self, t.t_tableOid) };
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
    debug_assert!(
        ScanDirectionIsForward(direction),
        "backward heap scan below the forward-only run seam (deletion-prep B1)"
    );
    let mintid = scan.rs_base.rs_mintid;
    let maxtid = scan.rs_base.rs_maxtid;

    loop {
        if (scan.rs_base.rs_flags & SO_ALLOW_PAGEMODE) != 0 {
            heapgettup_pagemode(scan)?;
        } else {
            heapgettup(scan)?;
        }

        let Some(t) = scan.rs_ctup.as_ref() else {
            exectuples::exec_clear_tuple(slot, mcx);
            return Ok(false);
        };

        // setscanlimits bounded the pages; boundary-page TIDs still need
        // filtering. (B7: the backward boundary arms - stop-below-min /
        // skip-above-max - are deleted with the backward stepping.)
        if ItemPointerCompare(&t.t_self, &mintid) < 0 {
            exectuples::exec_clear_tuple(slot, mcx);
            continue;
        }
        if ItemPointerCompare(&t.t_self, &maxtid) > 0 {
            exectuples::exec_clear_tuple(slot, mcx);
            return Ok(false);
        }
        break;
    }

    pgstat_count_heap_getnext(scan);
    store_ctup_into_slot(mcx, scan, slot);
    Ok(true)
}
