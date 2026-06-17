//! `backend-access-heap-pruneheap` — heap page pruning and HOT-chain management
//! (`access/heap/pruneheap.c`).
//!
//! This is the combined prune + freeze engine that VACUUM's first heap pass and
//! on-access scans drive. It scans a page's line pointers, classifies each tuple
//! with `HeapTupleSatisfiesVacuumHorizon`, walks each HOT chain to decide which
//! items become LP_REDIRECT / LP_DEAD / LP_UNUSED, optionally prepares freeze
//! plans for the surviving tuples, then — in a critical section — applies the
//! planned line-pointer changes (`heap_page_prune_execute` +
//! `PageRepairFragmentation`), executes the freeze plans, and emits a single
//! combined `XLOG_HEAP2_PRUNE_FREEZE` record (`log_heap_prune_and_freeze`).
//!
//! Page access follows the repo's `Buffer`-id-through-seams model
//! (freespace.c / visibilitymap precedent): the buffer manager owns the shared
//! page; this crate crosses the boundary by `Buffer` id and the
//! `bufmgr-seams::with_buffer_page` callback (one mutable `&mut [u8]` page image
//! for the whole prune+apply, mirroring C's direct `Page` pointer). The
//! src-idiomatic crate's bare-`&[u8]` page model is a logic reference only.
//!
//! The freeze machinery (`heap_prepare_freeze_tuple`,
//! `heap_freeze_prepared_tuples`, `heap_pre_freeze_checks`,
//! `HeapTupleHeaderAdvanceConflictHorizon`) lives in the heap-AM owner and is a
//! direct dependency. Cross-cycle callees (the global-visibility test, the WAL
//! emitter primitives, pgstat) go through their owners' seam crates.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use mcx::Mcx;
use types_core::primitive::{BlockNumber, OffsetNumber, TransactionId};
use types_error::{PgError, PgResult};
use types_rel::{Relation, RelationData};
use types_snapshot::snapshot::{GlobalVisStateHandle, HTSV_Result};
use types_storage::bufpage::MaxHeapTuplesPerPage;
use types_storage::Buffer;
use types_tuple::access::{RELKIND_MATVIEW, RELKIND_RELATION};
use types_tuple::heaptuple::{
    HeapTupleData, HeapTupleHeaderData, ItemPointerData, FIRST_OFFSET_NUMBER,
    HEAP_HOT_UPDATED, HEAP_ONLY_TUPLE, HEAP_XMAX_INVALID,
};
use types_vacuum::vacuum::{HeapPageFreeze, HeapTupleFreeze, PruneFreezeResult, VacuumCutoffs};

use backend_storage_page::{
    ItemIdGetRedirect, ItemIdIsDead, ItemIdIsNormal,
    ItemIdIsRedirected, ItemIdIsUsed, ItemIdSetDead, ItemIdSetRedirect, ItemIdSetUnused,
    ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber, ItemPointerSet, PageClearFull,
    PageGetItem, PageGetItemId, PageGetMaxOffsetNumber, PageIsFull, PageMut, PageRef,
    PageRepairFragmentation, PageTruncateLinePointerArray,
};

use backend_access_heap_heapam::freeze::{
    heap_freeze_prepared_tuples, heap_pre_freeze_checks, heap_prepare_freeze_tuple,
    HeapTupleHeaderAdvanceConflictHorizon,
};
use backend_access_heap_heapam_visibility::htup::HeapTupleHeaderGetXmin;
use backend_access_heap_heapam_visibility::{
    HeapTupleHeaderGetUpdateXid, HeapTupleSatisfiesVacuumHorizon,
};

use backend_access_heap_vacuumlazy_seams as vacuumlazy_seam;
use backend_access_transam_xlog_seams as xlog_seam;
use backend_access_transam_xloginsert_seams as xloginsert_seam;
use backend_storage_buffer_bufmgr_seams as bufmgr_seam;
use backend_storage_ipc_procarray_seams as procarray_seam;
use backend_utils_activity_pgstat_seams as pgstat_seam;
use backend_utils_cache_relcache_seams as relcache_seam;
use backend_catalog_catalog_seams as catalog_seam;
use backend_access_heap_hio_seams as hio_seam;

use backend_rmgrdesc_next::heapdesc::{
    XLHP_CLEANUP_LOCK, XLHP_HAS_CONFLICT_HORIZON, XLHP_HAS_DEAD_ITEMS,
    XLHP_HAS_FREEZE_PLANS, XLHP_HAS_NOW_UNUSED_ITEMS, XLHP_HAS_REDIRECTIONS,
    XLHP_IS_CATALOG_REL, XLOG_HEAP2_PRUNE_ON_ACCESS, XLOG_HEAP2_PRUNE_VACUUM_CLEANUP,
    XLOG_HEAP2_PRUNE_VACUUM_SCAN,
};
use types_wal::wal::RM_HEAP2_ID;
use types_xlog_records::heapam_xlog::{SIZEOF_XLHP_FREEZE_PLAN, SIZE_OF_HEAP_PRUNE};

pub mod init;
pub use init::init_seams;

// ===========================================================================
// Constants and macro-equivalents mirrored from the headers.
// ===========================================================================

/// `InvalidTransactionId`.
const InvalidTransactionId: TransactionId = 0;
/// `InvalidOffsetNumber`.
const InvalidOffsetNumber: OffsetNumber = 0;
/// `InvalidMultiXactId` (multixact.h).
const InvalidMultiXactId: u32 = 0;
/// `FirstOffsetNumber`.
const FirstOffsetNumber: OffsetNumber = FIRST_OFFSET_NUMBER;
/// `BLCKSZ` (pg_config.h default).
const BLCKSZ: usize = 8192;
/// `HEAP_DEFAULT_FILLFACTOR` (rel.h).
const HEAP_DEFAULT_FILLFACTOR: i32 = 100;

/// `HEAP_PAGE_PRUNE_MARK_UNUSED_NOW` (heapam.h, `1 << 0`).
pub const HEAP_PAGE_PRUNE_MARK_UNUSED_NOW: i32 = 1 << 0;
/// `HEAP_PAGE_PRUNE_FREEZE` (heapam.h, `1 << 1`).
pub const HEAP_PAGE_PRUNE_FREEZE: i32 = 1 << 1;

/// `PRUNE_ON_ACCESS` reason.
pub const PRUNE_ON_ACCESS: i32 = 0;
/// `PRUNE_VACUUM_SCAN` reason.
pub const PRUNE_VACUUM_SCAN: i32 = 1;
/// `PRUNE_VACUUM_CLEANUP` reason.
pub const PRUNE_VACUUM_CLEANUP: i32 = 2;

/// `TransactionIdIsValid(xid)` (transam.h).
#[inline]
fn TransactionIdIsValid(xid: TransactionId) -> bool {
    xid != InvalidTransactionId
}

/// `TransactionIdIsNormal(xid)` (transam.h).
#[inline]
fn TransactionIdIsNormal(xid: TransactionId) -> bool {
    xid >= 3 // FirstNormalTransactionId
}

/// `TransactionIdEquals(id1, id2)` (transam.h).
#[inline]
fn TransactionIdEquals(id1: TransactionId, id2: TransactionId) -> bool {
    id1 == id2
}

/// `TransactionIdPrecedes(id1, id2)` (transam.c) — circular comparison.
#[inline]
fn TransactionIdPrecedes(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 < id2;
    }
    (id1.wrapping_sub(id2) as i32) < 0
}

/// `TransactionIdFollows(id1, id2)` (transam.c).
#[inline]
fn TransactionIdFollows(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 > id2;
    }
    (id1.wrapping_sub(id2) as i32) > 0
}

/// `NormalTransactionIdPrecedes(id1, id2)` (transam.h) — both args are known
/// normal, so the comparison is the plain signed-difference form.
#[inline]
fn NormalTransactionIdPrecedes(id1: TransactionId, id2: TransactionId) -> bool {
    debug_assert!(TransactionIdIsNormal(id1) && TransactionIdIsNormal(id2));
    (id1.wrapping_sub(id2) as i32) < 0
}

/// `TransactionIdRetreat(dest)` (transam.h) — step `dest` back one XID,
/// skipping the special low values.
#[inline]
fn TransactionIdRetreat(dest: &mut TransactionId) {
    *dest = dest.wrapping_sub(1);
    while *dest < 3 {
        *dest = dest.wrapping_sub(1);
    }
}

/// `OffsetNumberPrev(offsetNumber)` (off.h).
#[inline]
fn OffsetNumberPrev(offset: OffsetNumber) -> OffsetNumber {
    offset.wrapping_sub(1)
}

/// `HeapTupleHeaderIsHotUpdated(tup)` (htup_details.h).
#[inline]
fn HeapTupleHeaderIsHotUpdated(tup: &HeapTupleHeaderData) -> bool {
    use backend_access_heap_heapam_visibility::htup::HeapTupleHeaderXminInvalid;
    (tup.t_infomask2 & HEAP_HOT_UPDATED) != 0
        && (tup.t_infomask & HEAP_XMAX_INVALID) == 0
        && !HeapTupleHeaderXminInvalid(tup)
}

/// `HeapTupleHeaderIsHeapOnly(tup)` (htup_details.h).
#[inline]
fn HeapTupleHeaderIsHeapOnly(tup: &HeapTupleHeaderData) -> bool {
    (tup.t_infomask2 & HEAP_ONLY_TUPLE) != 0
}

/// `HeapTupleHeaderIndicatesMovedPartitions(tup)` (htup_details.h).
#[inline]
fn HeapTupleHeaderIndicatesMovedPartitions(tup: &HeapTupleHeaderData) -> bool {
    // ItemPointerGetOffsetNumber(&tup->t_ctid) == MovedPartitionsOffsetNumber
    // && ItemPointerGetBlockNumberNoCheck(&tup->t_ctid) == MovedPartitionsBlockNumber
    ItemPointerGetOffsetNumber(&tup.t_ctid) == 0xfffd
        && ItemPointerGetBlockNumber(&tup.t_ctid) == 0xffffffff
}

// ===========================================================================
// PruneState — working data for heap_page_prune_and_freeze() and subroutines
// (pruneheap.c struct). Carried as one owned value through the page closure.
// ===========================================================================

struct PruneState<'a> {
    // ---- arguments ----
    vistest: GlobalVisStateHandle,
    mark_unused_now: bool,
    freeze: bool,
    cutoffs: Option<&'a VacuumCutoffs>,

    // ---- what to do to the page ----
    new_prune_xid: TransactionId,
    latest_xid_removed: TransactionId,
    nredirected: usize,
    ndead: usize,
    nunused: usize,
    nfrozen: usize,
    /// `redirected[MaxHeapTuplesPerPage * 2]` — from/to pairs.
    redirected: Vec<OffsetNumber>,
    nowdead: Vec<OffsetNumber>,
    nowunused: Vec<OffsetNumber>,
    frozen: Vec<HeapTupleFreeze>,

    // ---- HOT chain processing state ----
    nroot_items: usize,
    root_items: Vec<OffsetNumber>,
    nheaponly_items: usize,
    heaponly_items: Vec<OffsetNumber>,

    /// `processed[MaxHeapTuplesPerPage + 1]`.
    processed: Vec<bool>,
    /// `htsv[MaxHeapTuplesPerPage + 1]`, int8 with -1 = not computed.
    htsv: Vec<i8>,

    pagefrz: HeapPageFreeze,

    // ---- information about what was done ----
    ndeleted: i32,
    live_tuples: i32,
    recently_dead_tuples: i32,
    hastup: bool,
    lpdead_items: usize,
    /// Mirrors C's `deadoffsets` (points into the caller's result array). We
    /// accumulate here and copy into `presult.deadoffsets` at the end.
    deadoffsets: Vec<OffsetNumber>,

    all_visible: bool,
    all_frozen: bool,
    visibility_cutoff_xid: TransactionId,
}

impl<'a> PruneState<'a> {
    fn new(vistest: GlobalVisStateHandle) -> Self {
        PruneState {
            vistest,
            mark_unused_now: false,
            freeze: false,
            cutoffs: None,
            new_prune_xid: InvalidTransactionId,
            latest_xid_removed: InvalidTransactionId,
            nredirected: 0,
            ndead: 0,
            nunused: 0,
            nfrozen: 0,
            redirected: alloc::vec![0; MaxHeapTuplesPerPage * 2],
            nowdead: alloc::vec![0; MaxHeapTuplesPerPage],
            nowunused: alloc::vec![0; MaxHeapTuplesPerPage],
            frozen: alloc::vec![HeapTupleFreeze::default(); MaxHeapTuplesPerPage],
            nroot_items: 0,
            root_items: alloc::vec![0; MaxHeapTuplesPerPage],
            nheaponly_items: 0,
            heaponly_items: alloc::vec![0; MaxHeapTuplesPerPage],
            processed: alloc::vec![false; MaxHeapTuplesPerPage + 1],
            htsv: alloc::vec![-1; MaxHeapTuplesPerPage + 1],
            pagefrz: HeapPageFreeze::default(),
            ndeleted: 0,
            live_tuples: 0,
            recently_dead_tuples: 0,
            hastup: false,
            lpdead_items: 0,
            deadoffsets: alloc::vec![0; MaxHeapTuplesPerPage],
            all_visible: false,
            all_frozen: false,
            visibility_cutoff_xid: InvalidTransactionId,
        }
    }
}

// ===========================================================================
// heap_page_prune_opt — opportunistic on-access pruning (pruneheap.c).
// ===========================================================================

/// `heap_page_prune_opt(relation, buffer)` — optionally prune and repair
/// fragmentation on `buffer`'s page. Caller has a pin on the buffer and *no*
/// lock; this only runs if the page heuristically looks worth pruning and a
/// cleanup lock can be had without blocking.
pub fn heap_page_prune_opt<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    buffer: Buffer,
) -> PgResult<()> {
    // We can't write WAL in recovery mode, so there's no point trying to clean
    // the page.
    if xlog_seam::recovery_in_progress::call() {
        return Ok(());
    }

    // First check whether there's any chance there's something to prune.
    let prune_xid = {
        let bytes = bufmgr_seam::buffer_get_page::call(mcx, buffer)?;
        let page = PageRef::new(bytes.as_slice())?;
        page.pd_prune_xid()
    };
    if !TransactionIdIsValid(prune_xid) {
        return Ok(());
    }

    // Check whether prune_xid indicates that there may be dead rows that can be
    // cleaned up.
    let vistest = vacuumlazy_seam::global_vis_test_for::call(relation.rd_id)?;

    if !procarray_seam::global_vis_test_is_removable_xid::call(vistest, prune_xid)? {
        return Ok(());
    }

    // We prune when a previous UPDATE failed to find enough space on the page
    // for a new tuple version, or when free space falls below the relation's
    // fill-factor target (but not less than 10%).
    let mut minfree =
        hio_seam::relation_get_target_page_free_space::call(relation.rd_id, HEAP_DEFAULT_FILLFACTOR)?;
    minfree = core::cmp::max(minfree, BLCKSZ / 10);

    let (is_full, free) = {
        let bytes = bufmgr_seam::buffer_get_page::call(mcx, buffer)?;
        let page = PageRef::new(bytes.as_slice())?;
        (PageIsFull(&page), backend_storage_page::PageGetHeapFreeSpace(&page))
    };

    if is_full || free < minfree {
        // OK, try to get exclusive buffer cleanup lock.
        if !bufmgr_seam::conditional_lock_buffer_for_cleanup::call(buffer)? {
            return Ok(());
        }

        // Recheck the heuristic now that we have the lock and accurate info.
        let (is_full, free) = {
            let bytes = bufmgr_seam::buffer_get_page::call(mcx, buffer)?;
            let page = PageRef::new(bytes.as_slice())?;
            (PageIsFull(&page), backend_storage_page::PageGetHeapFreeSpace(&page))
        };

        if is_full || free < minfree {
            // For now, pass mark_unused_now as false regardless of whether or
            // not the relation has indexes, since we cannot safely determine
            // that during on-access pruning with the current implementation.
            let mut off_loc = InvalidOffsetNumber;
            let presult = heap_page_prune_and_freeze(
                mcx,
                relation,
                buffer,
                vistest,
                0,
                None,
                PRUNE_ON_ACCESS,
                &mut off_loc,
                None,
                None,
            )?;

            // Report the number of tuples reclaimed to pgstats. This is
            // presult.ndeleted minus the number of newly-LP_DEAD-set items.
            if presult.ndeleted > presult.nnewlpdead {
                pgstat_seam::pgstat_update_heap_dead_tuples::call(
                    relation.rd_id,
                    relation.rd_rel.relisshared,
                    relation.pgstat_enabled,
                    presult.ndeleted - presult.nnewlpdead,
                );
            }
        }

        // And release buffer lock.
        bufmgr_seam::lock_buffer::call(buffer, BUFFER_LOCK_UNLOCK)?;

        // We avoid reuse of any free space created on the page by unrelated
        // UPDATEs/INSERTs by opting to not update the FSM at this point.
    }

    Ok(())
}

/// `BUFFER_LOCK_UNLOCK` (bufmgr.h).
const BUFFER_LOCK_UNLOCK: i32 = 0;

// ===========================================================================
// heap_page_prune_and_freeze — the combined prune + freeze engine.
// ===========================================================================

/// `heap_page_prune_and_freeze(relation, buffer, vistest, options, cutoffs,
/// presult, reason, off_loc, new_relfrozen_xid, new_relmin_mxid)`.
///
/// Returns the per-page [`PruneFreezeResult`]; `off_loc` is updated like C's
/// out-param. When `new_relfrozen_xid` / `new_relmin_mxid` are supplied (the
/// FREEZE option requires them), they are updated with the values present on the
/// page after pruning and returned through their `&mut`.
#[allow(clippy::too_many_arguments)]
pub fn heap_page_prune_and_freeze<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &RelationData<'mcx>,
    buffer: Buffer,
    vistest: GlobalVisStateHandle,
    options: i32,
    cutoffs: Option<&VacuumCutoffs>,
    reason: i32,
    off_loc: &mut OffsetNumber,
    new_relfrozen_xid: Option<&mut TransactionId>,
    new_relmin_mxid: Option<&mut u32>,
) -> PgResult<PruneFreezeResult> {
    let blockno = bufmgr_seam::buffer_get_block_number::call(buffer);

    let mut prstate = PruneState::new(vistest);
    let mut presult = PruneFreezeResult::default();

    // Copy parameters to prstate.
    prstate.mark_unused_now = (options & HEAP_PAGE_PRUNE_MARK_UNUSED_NOW) != 0;
    prstate.freeze = (options & HEAP_PAGE_PRUNE_FREEZE) != 0;
    prstate.cutoffs = cutoffs;

    // Snapshot the new-relfrozenxid / new-relminmxid in-values (C threads them
    // by reference; we read them at the start and write back at the end).
    let in_relfrozen_xid = new_relfrozen_xid.as_ref().map(|r| **r);
    let in_relmin_mxid = new_relmin_mxid.as_ref().map(|r| **r);

    // Initialize page freezing working state.
    prstate.pagefrz.freeze_required = false;
    if prstate.freeze {
        debug_assert!(in_relfrozen_xid.is_some() && in_relmin_mxid.is_some());
        prstate.pagefrz.FreezePageRelfrozenXid = in_relfrozen_xid.unwrap();
        prstate.pagefrz.NoFreezePageRelfrozenXid = in_relfrozen_xid.unwrap();
        prstate.pagefrz.FreezePageRelminMxid = in_relmin_mxid.unwrap();
        prstate.pagefrz.NoFreezePageRelminMxid = in_relmin_mxid.unwrap();
    } else {
        debug_assert!(in_relfrozen_xid.is_none() && in_relmin_mxid.is_none());
        prstate.pagefrz.FreezePageRelminMxid = InvalidMultiXactId;
        prstate.pagefrz.NoFreezePageRelminMxid = InvalidMultiXactId;
        prstate.pagefrz.FreezePageRelfrozenXid = InvalidTransactionId;
        prstate.pagefrz.NoFreezePageRelfrozenXid = InvalidTransactionId;
    }

    // Caller may update the VM after we're done.
    if prstate.freeze {
        prstate.all_visible = true;
        prstate.all_frozen = true;
    } else {
        prstate.all_visible = false;
        prstate.all_frozen = false;
    }
    prstate.visibility_cutoff_xid = InvalidTransactionId;

    let reltableoid = relation.rd_id;

    // ---- Phase 1: scan the page, compute HTSV, queue HOT-chain roots and
    // heap-only items. This reads the page; the freeze/scan plan is built off a
    // snapshot copy of the page bytes (the caller holds the cleanup lock). ----
    let page_bytes = bufmgr_seam::buffer_get_page::call(mcx, buffer)?;
    let do_hint_existing_prune_xid;
    let page_is_full_pre;
    {
        let page = PageRef::new(page_bytes.as_slice())?;
        let maxoff = PageGetMaxOffsetNumber(&page);
        do_hint_existing_prune_xid = page.pd_prune_xid();
        page_is_full_pre = PageIsFull(&page);

        // Determine HTSV for all tuples in reverse offset order.
        let mut offnum = maxoff;
        while offnum >= FirstOffsetNumber {
            let itemid = PageGetItemId(&page, offnum)?;

            *off_loc = offnum;
            prstate.processed[offnum as usize] = false;
            prstate.htsv[offnum as usize] = -1;

            if !ItemIdIsUsed(&itemid) {
                heap_prune_record_unchanged_lp_unused(&mut prstate, offnum);
                offnum = OffsetNumberPrev(offnum);
                continue;
            }

            if ItemIdIsDead(&itemid) {
                if prstate.mark_unused_now {
                    heap_prune_record_unused(&mut prstate, offnum, false);
                } else {
                    heap_prune_record_unchanged_lp_dead(&mut prstate, offnum);
                }
                offnum = OffsetNumberPrev(offnum);
                continue;
            }

            if ItemIdIsRedirected(&itemid) {
                // Start of a HOT chain.
                prstate.root_items[prstate.nroot_items] = offnum;
                prstate.nroot_items += 1;
                offnum = OffsetNumberPrev(offnum);
                continue;
            }

            debug_assert!(ItemIdIsNormal(&itemid));

            // Visibility status + queue.
            let item = PageGetItem(&page, &itemid)?;
            let htup = HeapTupleHeaderData::read_on_page(mcx, item)?;
            let is_heap_only = HeapTupleHeaderIsHeapOnly(&htup);

            let mut tup = HeapTupleData {
                t_len: backend_storage_page::ItemIdGetLength(&itemid) as u32,
                t_self: ItemPointerData::default(),
                t_tableOid: reltableoid,
                t_data: Some(mcx::alloc_in(mcx, htup)?),
            };
            ItemPointerSet(&mut tup.t_self, blockno, offnum);

            let res = heap_prune_satisfies_vacuum(&prstate, &mut tup, buffer)?;
            prstate.htsv[offnum as usize] = res as i8;

            if !is_heap_only {
                prstate.root_items[prstate.nroot_items] = offnum;
                prstate.nroot_items += 1;
            } else {
                prstate.heaponly_items[prstate.nheaponly_items] = offnum;
                prstate.nheaponly_items += 1;
            }

            offnum = OffsetNumberPrev(offnum);
        }

        // ---- Process HOT chains in ascending offset order. ----
        for i in (0..prstate.nroot_items).rev() {
            let offnum = prstate.root_items[i];
            if prstate.processed[offnum as usize] {
                continue;
            }
            *off_loc = offnum;
            heap_prune_chain(mcx, &page, blockno, maxoff, offnum, &mut prstate)?;
        }

        // ---- Process leftover heap-only tuples. ----
        for i in (0..prstate.nheaponly_items).rev() {
            let offnum = prstate.heaponly_items[i];
            if prstate.processed[offnum as usize] {
                continue;
            }
            *off_loc = offnum;

            if prstate.htsv[offnum as usize] == HTSV_Result::HEAPTUPLE_DEAD as i8 {
                let itemid = PageGetItemId(&page, offnum)?;
                let item = PageGetItem(&page, &itemid)?;
                let htup = HeapTupleHeaderData::read_on_page(mcx, item)?;

                if !HeapTupleHeaderIsHotUpdated(&htup) {
                    HeapTupleHeaderAdvanceConflictHorizon(
                        &htup,
                        &mut prstate.latest_xid_removed,
                    )?;
                    heap_prune_record_unused(&mut prstate, offnum, true);
                } else {
                    // This tuple should've been processed and removed as part of
                    // a HOT chain, so something's wrong.
                    return Err(PgError::error(fmt2(
                        "dead heap-only tuple (",
                        &blockno.to_string(),
                        ", ",
                        &offnum.to_string(),
                        ") is not linked to from any HOT chain",
                    )));
                }
            } else {
                heap_prune_record_unchanged_lp_normal(mcx, &page, &mut prstate, offnum)?;
            }
        }

        // We should now have processed every tuple exactly once.
        #[cfg(debug_assertions)]
        {
            let mut offnum = FirstOffsetNumber;
            while offnum <= maxoff {
                *off_loc = offnum;
                debug_assert!(prstate.processed[offnum as usize]);
                offnum += 1;
            }
        }
    }

    // Clear the offset information once we have processed the given page.
    *off_loc = InvalidOffsetNumber;

    let do_prune = prstate.nredirected > 0 || prstate.ndead > 0 || prstate.nunused > 0;

    // Even if we don't prune anything, if we found a new pd_prune_xid value or
    // the page was marked full, we will update the hint bit.
    let do_hint =
        do_hint_existing_prune_xid != prstate.new_prune_xid || page_is_full_pre;

    // Decide whether to freeze.
    let mut do_freeze = false;
    if prstate.freeze {
        if prstate.pagefrz.freeze_required {
            // At least one XID/MXID from before FreezeLimit/MultiXactCutoff is
            // present. Must freeze to advance relfrozenxid/relminmxid.
            do_freeze = true;
        } else if prstate.all_visible && prstate.all_frozen && prstate.nfrozen > 0 {
            // Opportunistically freeze if doing so makes the page all-frozen and
            // an FPI is being emitted anyway.
            //
            // NB: the C heuristic also checks `hint_bit_fpi` (whether a hint-bit
            // FPI was emitted while computing visibility above). The repo's
            // visibility path does not WAL-log hint bits inside
            // heap_prune_satisfies_vacuum (no FPI is emitted there in this
            // model), so `hint_bit_fpi` is always false; we therefore only take
            // the do_prune / do_hint XLogCheckBufferNeedsBackup branches.
            if relcache_seam::relation_needs_wal::call(relation) {
                if do_prune {
                    if xlog_seam::xlog_check_buffer_needs_backup::call(buffer)? {
                        do_freeze = true;
                    }
                } else if do_hint
                    && xlog_seam::xlog_hint_bit_is_needed::call()
                    && xlog_seam::xlog_check_buffer_needs_backup::call(buffer)?
                {
                    do_freeze = true;
                }
            }
        }
    }

    if do_freeze {
        // Validate the tuples we will be freezing before the critical section.
        heap_pre_freeze_checks(mcx, buffer, &prstate.frozen[..prstate.nfrozen])?;
    } else if prstate.nfrozen > 0 {
        // We chose not to freeze; the page won't be all-frozen then.
        debug_assert!(!prstate.pagefrz.freeze_required);
        prstate.all_frozen = false;
        prstate.nfrozen = 0; // avoid miscounts in instrumentation
    }

    // ---- Critical section: apply the planned changes. The whole apply runs in
    // one `with_buffer_page` mutable-page closure (C's direct Page writes inside
    // START/END_CRIT_SECTION). ----
    if do_hint {
        bufmgr_seam::with_buffer_page::call(buffer, &mut |bytes| {
            let mut page = PageMut::new(bytes)?;
            // Update pd_prune_xid to zero or the lowest soon-prunable XID.
            page.set_pd_prune_xid(prstate.new_prune_xid);
            // Clear the "page is full" flag.
            PageClearFull(&mut page);
            Ok(())
        })?;

        if !do_freeze && !do_prune {
            // Non-WAL-logged hint.
            bufmgr_seam::mark_buffer_dirty_hint::call(buffer, true);
        }
    }

    if do_prune || do_freeze {
        if do_prune {
            heap_page_prune_execute(
                buffer,
                false,
                prstate.redirected[..prstate.nredirected * 2].to_vec(),
                prstate.nowdead[..prstate.ndead].to_vec(),
                prstate.nowunused[..prstate.nunused].to_vec(),
            )?;
        }

        if do_freeze {
            heap_freeze_prepared_tuples(mcx, buffer, &prstate.frozen[..prstate.nfrozen])?;
        }

        bufmgr_seam::mark_buffer_dirty::call(buffer);

        // Emit a WAL XLOG_HEAP2_PRUNE_FREEZE record showing what we did.
        if relcache_seam::relation_needs_wal::call(relation) {
            let mut frz_conflict_horizon = InvalidTransactionId;
            if do_freeze {
                if prstate.all_visible && prstate.all_frozen {
                    frz_conflict_horizon = prstate.visibility_cutoff_xid;
                } else {
                    // Avoids false conflicts when hot_standby_feedback in use.
                    frz_conflict_horizon = prstate.cutoffs.unwrap().OldestXmin;
                    TransactionIdRetreat(&mut frz_conflict_horizon);
                }
            }

            let conflict_xid =
                if TransactionIdFollows(frz_conflict_horizon, prstate.latest_xid_removed) {
                    frz_conflict_horizon
                } else {
                    prstate.latest_xid_removed
                };

            log_heap_prune_and_freeze(
                relation,
                buffer,
                conflict_xid,
                true,
                reason,
                &mut prstate.frozen[..prstate.nfrozen].to_vec(),
                &prstate.redirected[..prstate.nredirected * 2],
                &prstate.nowdead[..prstate.ndead],
                &prstate.nowunused[..prstate.nunused],
            )?;
        }
    }

    // ---- Copy information back for caller. ----
    presult.ndeleted = prstate.ndeleted;
    presult.nnewlpdead = prstate.ndead as i32;
    presult.nfrozen = prstate.nfrozen as i32;
    presult.live_tuples = prstate.live_tuples;
    presult.recently_dead_tuples = prstate.recently_dead_tuples;

    if prstate.all_visible && prstate.lpdead_items == 0 {
        presult.all_visible = prstate.all_visible;
        presult.all_frozen = prstate.all_frozen;
    } else {
        presult.all_visible = false;
        presult.all_frozen = false;
    }

    presult.hastup = prstate.hastup;

    if presult.all_frozen {
        presult.vm_conflict_horizon = InvalidTransactionId;
    } else {
        presult.vm_conflict_horizon = prstate.visibility_cutoff_xid;
    }

    presult.lpdead_items = prstate.lpdead_items as i32;
    for i in 0..prstate.lpdead_items {
        presult.deadoffsets[i] = prstate.deadoffsets[i];
    }

    if prstate.freeze {
        if presult.nfrozen > 0 {
            if let Some(r) = new_relfrozen_xid {
                *r = prstate.pagefrz.FreezePageRelfrozenXid;
            }
            if let Some(r) = new_relmin_mxid {
                *r = prstate.pagefrz.FreezePageRelminMxid;
            }
        } else {
            if let Some(r) = new_relfrozen_xid {
                *r = prstate.pagefrz.NoFreezePageRelfrozenXid;
            }
            if let Some(r) = new_relmin_mxid {
                *r = prstate.pagefrz.NoFreezePageRelminMxid;
            }
        }
    }

    Ok(presult)
}

// ===========================================================================
// heap_prune_satisfies_vacuum — per-tuple visibility for pruning.
// ===========================================================================

fn heap_prune_satisfies_vacuum<'mcx>(
    prstate: &PruneState,
    tup: &mut HeapTupleData<'mcx>,
    buffer: Buffer,
) -> PgResult<HTSV_Result> {
    let mut dead_after = InvalidTransactionId;
    let res = HeapTupleSatisfiesVacuumHorizon(tup, buffer, &mut dead_after)?;

    if res != HTSV_Result::HEAPTUPLE_RECENTLY_DEAD {
        return Ok(res);
    }

    // For VACUUM, prune tuples with xmax older than OldestXmin.
    if let Some(cutoffs) = prstate.cutoffs {
        if TransactionIdIsValid(cutoffs.OldestXmin)
            && NormalTransactionIdPrecedes(dead_after, cutoffs.OldestXmin)
        {
            return Ok(HTSV_Result::HEAPTUPLE_DEAD);
        }
    }

    // GlobalVisState could find the row dead even if xmax isn't older than
    // OldestXmin, if the horizon has advanced.
    if procarray_seam::global_vis_test_is_removable_xid::call(prstate.vistest, dead_after)? {
        return Ok(HTSV_Result::HEAPTUPLE_DEAD);
    }

    Ok(res)
}

/// `htsv_get_valid_status(status)` (pruneheap.c) — guard against examining an
/// uncomputed htsv slot.
#[inline]
fn htsv_get_valid_status(status: i8) -> HTSV_Result {
    debug_assert!(
        status >= HTSV_Result::HEAPTUPLE_DEAD as i8
            && status <= HTSV_Result::HEAPTUPLE_DELETE_IN_PROGRESS as i8
    );
    match status {
        0 => HTSV_Result::HEAPTUPLE_DEAD,
        1 => HTSV_Result::HEAPTUPLE_LIVE,
        2 => HTSV_Result::HEAPTUPLE_RECENTLY_DEAD,
        3 => HTSV_Result::HEAPTUPLE_INSERT_IN_PROGRESS,
        _ => HTSV_Result::HEAPTUPLE_DELETE_IN_PROGRESS,
    }
}

// ===========================================================================
// heap_prune_chain — prune a line pointer or HOT chain (pruneheap.c).
// ===========================================================================

fn heap_prune_chain<'mcx>(
    mcx: Mcx<'mcx>,
    page: &PageRef<'_>,
    blockno: BlockNumber,
    maxoff: OffsetNumber,
    rootoffnum: OffsetNumber,
    prstate: &mut PruneState,
) -> PgResult<()> {
    let mut prior_xmax = InvalidTransactionId;
    let rootlp = PageGetItemId(page, rootoffnum)?;
    let mut chainitems: Vec<OffsetNumber> = alloc::vec![0; MaxHeapTuplesPerPage];

    let mut ndeadchain: usize = 0;
    let mut nchain: usize = 0;

    let mut offnum = rootoffnum;
    let mut reached_process_chain = false;

    // while not end of the chain
    loop {
        if offnum < FirstOffsetNumber {
            break;
        }
        // An offset past the end of the line pointer array is possible when the
        // array was truncated (original item must have been unused).
        if offnum > maxoff {
            break;
        }
        if prstate.processed[offnum as usize] {
            break;
        }

        let lp = PageGetItemId(page, offnum)?;

        debug_assert!(ItemIdIsUsed(&lp));
        debug_assert!(!ItemIdIsDead(&lp));

        if ItemIdIsRedirected(&lp) {
            if nchain > 0 {
                break; // not at start of chain
            }
            chainitems[nchain] = offnum;
            nchain += 1;
            offnum = ItemIdGetRedirect(&rootlp);
            continue;
        }

        debug_assert!(ItemIdIsNormal(&lp));

        let item = PageGetItem(page, &lp)?;
        let htup = HeapTupleHeaderData::read_on_page(mcx, item)?;

        // Check the tuple XMIN against prior XMAX, if any.
        if TransactionIdIsValid(prior_xmax)
            && !TransactionIdEquals(HeapTupleHeaderGetXmin(&htup), prior_xmax)
        {
            break;
        }

        // OK, this tuple is a member of the chain.
        chainitems[nchain] = offnum;
        nchain += 1;

        match htsv_get_valid_status(prstate.htsv[offnum as usize]) {
            HTSV_Result::HEAPTUPLE_DEAD => {
                ndeadchain = nchain;
                HeapTupleHeaderAdvanceConflictHorizon(
                    &htup,
                    &mut prstate.latest_xid_removed,
                )?;
                // advance to next chain member
            }
            HTSV_Result::HEAPTUPLE_RECENTLY_DEAD => {
                // advance past RECENTLY_DEAD just in case there's a DEAD after
            }
            HTSV_Result::HEAPTUPLE_DELETE_IN_PROGRESS
            | HTSV_Result::HEAPTUPLE_LIVE
            | HTSV_Result::HEAPTUPLE_INSERT_IN_PROGRESS => {
                reached_process_chain = true;
                break;
            }
        }

        // If the tuple is not HOT-updated, we are at the end of the chain.
        if !HeapTupleHeaderIsHotUpdated(&htup) {
            reached_process_chain = true;
            break;
        }

        // HOT implies it can't have moved to a different partition.
        debug_assert!(!HeapTupleHeaderIndicatesMovedPartitions(&htup));

        // Advance to next chain member.
        debug_assert!(ItemPointerGetBlockNumber(&htup.t_ctid) == blockno);
        offnum = ItemPointerGetOffsetNumber(&htup.t_ctid);
        prior_xmax = HeapTupleHeaderGetUpdateXid(&htup)?;
    }

    if !reached_process_chain && ItemIdIsRedirected(&rootlp) && nchain < 2 {
        // Found a redirect item that doesn't point to a valid follow-on item.
        heap_prune_record_dead_or_unused(prstate, rootoffnum, false);
        return Ok(());
    }

    // process_chain:
    if ndeadchain == 0 {
        // No DEAD tuple found; chain is entirely normal, unchanged tuples.
        let mut i = 0;
        if ItemIdIsRedirected(&rootlp) {
            heap_prune_record_unchanged_lp_redirect(prstate, rootoffnum);
            i += 1;
        }
        while i < nchain {
            heap_prune_record_unchanged_lp_normal(mcx, page, prstate, chainitems[i])?;
            i += 1;
        }
    } else if ndeadchain == nchain {
        // The entire chain is dead.
        heap_prune_record_dead_or_unused(prstate, rootoffnum, ItemIdIsNormal(&rootlp));
        for i in 1..nchain {
            heap_prune_record_unused(prstate, chainitems[i], true);
        }
    } else {
        // Found a DEAD tuple in the chain. Redirect the root to the first
        // non-DEAD tuple, mark intermediate items unused.
        heap_prune_record_redirect(
            prstate,
            rootoffnum,
            chainitems[ndeadchain],
            ItemIdIsNormal(&rootlp),
        );
        for i in 1..ndeadchain {
            heap_prune_record_unused(prstate, chainitems[i], true);
        }
        for i in ndeadchain..nchain {
            heap_prune_record_unchanged_lp_normal(mcx, page, prstate, chainitems[i])?;
        }
    }

    Ok(())
}

// ===========================================================================
// heap_prune_record_* — PruneState bookkeeping subroutines (pruneheap.c).
// ===========================================================================

fn heap_prune_record_prunable(prstate: &mut PruneState, xid: TransactionId) {
    debug_assert!(TransactionIdIsNormal(xid));
    if !TransactionIdIsValid(prstate.new_prune_xid)
        || TransactionIdPrecedes(xid, prstate.new_prune_xid)
    {
        prstate.new_prune_xid = xid;
    }
}

fn heap_prune_record_redirect(
    prstate: &mut PruneState,
    offnum: OffsetNumber,
    rdoffnum: OffsetNumber,
    was_normal: bool,
) {
    debug_assert!(!prstate.processed[offnum as usize]);
    prstate.processed[offnum as usize] = true;

    debug_assert!(prstate.nredirected < MaxHeapTuplesPerPage);
    prstate.redirected[prstate.nredirected * 2] = offnum;
    prstate.redirected[prstate.nredirected * 2 + 1] = rdoffnum;
    prstate.nredirected += 1;

    if was_normal {
        prstate.ndeleted += 1;
    }
    prstate.hastup = true;
}

fn heap_prune_record_dead(prstate: &mut PruneState, offnum: OffsetNumber, was_normal: bool) {
    debug_assert!(!prstate.processed[offnum as usize]);
    prstate.processed[offnum as usize] = true;

    debug_assert!(prstate.ndead < MaxHeapTuplesPerPage);
    prstate.nowdead[prstate.ndead] = offnum;
    prstate.ndead += 1;

    // Record the dead offset for vacuum.
    prstate.deadoffsets[prstate.lpdead_items] = offnum;
    prstate.lpdead_items += 1;

    if was_normal {
        prstate.ndeleted += 1;
    }
}

fn heap_prune_record_dead_or_unused(
    prstate: &mut PruneState,
    offnum: OffsetNumber,
    was_normal: bool,
) {
    if prstate.mark_unused_now {
        heap_prune_record_unused(prstate, offnum, was_normal);
    } else {
        heap_prune_record_dead(prstate, offnum, was_normal);
    }
}

fn heap_prune_record_unused(prstate: &mut PruneState, offnum: OffsetNumber, was_normal: bool) {
    debug_assert!(!prstate.processed[offnum as usize]);
    prstate.processed[offnum as usize] = true;

    debug_assert!(prstate.nunused < MaxHeapTuplesPerPage);
    prstate.nowunused[prstate.nunused] = offnum;
    prstate.nunused += 1;

    if was_normal {
        prstate.ndeleted += 1;
    }
}

fn heap_prune_record_unchanged_lp_unused(prstate: &mut PruneState, offnum: OffsetNumber) {
    debug_assert!(!prstate.processed[offnum as usize]);
    prstate.processed[offnum as usize] = true;
}

fn heap_prune_record_unchanged_lp_normal<'mcx>(
    mcx: Mcx<'mcx>,
    page: &PageRef<'_>,
    prstate: &mut PruneState,
    offnum: OffsetNumber,
) -> PgResult<()> {
    use backend_access_heap_heapam_visibility::htup::HeapTupleHeaderXminFrozen;
    use types_tuple::heaptuple::HeapTupleHeaderXminCommitted;

    debug_assert!(!prstate.processed[offnum as usize]);
    prstate.processed[offnum as usize] = true;

    prstate.hastup = true; // the page is not empty

    let itemid = PageGetItemId(page, offnum)?;
    let item = PageGetItem(page, &itemid)?;
    let htup = HeapTupleHeaderData::read_on_page(mcx, item)?;
    let _ = HeapTupleHeaderXminFrozen; // (only the committed hint is consulted)

    match htsv_get_valid_status(prstate.htsv[offnum as usize]) {
        HTSV_Result::HEAPTUPLE_LIVE => {
            prstate.live_tuples += 1;

            if prstate.all_visible {
                if !HeapTupleHeaderXminCommitted(&htup) {
                    prstate.all_visible = false;
                } else {
                    let xmin = HeapTupleHeaderGetXmin(&htup);
                    debug_assert!(prstate.cutoffs.is_some());
                    if !TransactionIdPrecedes(xmin, prstate.cutoffs.unwrap().OldestXmin) {
                        prstate.all_visible = false;
                    } else {
                        // Track newest xmin on page.
                        if TransactionIdFollows(xmin, prstate.visibility_cutoff_xid)
                            && TransactionIdIsNormal(xmin)
                        {
                            prstate.visibility_cutoff_xid = xmin;
                        }
                    }
                }
            }
        }
        HTSV_Result::HEAPTUPLE_RECENTLY_DEAD => {
            prstate.recently_dead_tuples += 1;
            prstate.all_visible = false;
            heap_prune_record_prunable(prstate, HeapTupleHeaderGetUpdateXid(&htup)?);
        }
        HTSV_Result::HEAPTUPLE_INSERT_IN_PROGRESS => {
            prstate.all_visible = false;
        }
        HTSV_Result::HEAPTUPLE_DELETE_IN_PROGRESS => {
            prstate.live_tuples += 1;
            prstate.all_visible = false;
            heap_prune_record_prunable(prstate, HeapTupleHeaderGetUpdateXid(&htup)?);
        }
        HTSV_Result::HEAPTUPLE_DEAD => {
            // DEAD tuples should've gone to record_dead/record_unused instead.
            return Err(PgError::error(fmt1(
                "unexpected HeapTupleSatisfiesVacuum result ",
                &prstate.htsv[offnum as usize].to_string(),
            )));
        }
    }

    // Consider freezing any normal tuples which will not be removed.
    if prstate.freeze {
        let nfrozen = prstate.nfrozen;
        let mut frz = HeapTupleFreeze::default();
        let (do_freeze_tuple, totally_frozen) = heap_prepare_freeze_tuple(
            mcx,
            &htup,
            prstate.cutoffs.unwrap(),
            &mut prstate.pagefrz,
            &mut frz,
        )?;

        if do_freeze_tuple {
            frz.offset = offnum;
            prstate.frozen[nfrozen] = frz;
            prstate.nfrozen += 1;
        }

        if !totally_frozen {
            prstate.all_frozen = false;
        }
    }

    Ok(())
}

fn heap_prune_record_unchanged_lp_dead(prstate: &mut PruneState, offnum: OffsetNumber) {
    debug_assert!(!prstate.processed[offnum as usize]);
    prstate.processed[offnum as usize] = true;

    // Deliberately don't set hastup for LP_DEAD items; don't unset all_visible
    // until the end. Record the dead offset for vacuum.
    prstate.deadoffsets[prstate.lpdead_items] = offnum;
    prstate.lpdead_items += 1;
}

fn heap_prune_record_unchanged_lp_redirect(prstate: &mut PruneState, offnum: OffsetNumber) {
    debug_assert!(!prstate.processed[offnum as usize]);
    prstate.processed[offnum as usize] = true;
}

// ===========================================================================
// heap_page_prune_execute — apply planned line-pointer changes (pruneheap.c).
// ===========================================================================

/// `heap_page_prune_execute(buffer, lp_truncate_only, redirected, nowdead,
/// nowunused)` — apply the planned item changes and repair page fragmentation.
/// `redirected` is the flat from/to pair array.
///
/// If `lp_truncate_only`, only already-dead line pointers are set unused and
/// `PageRepairFragmentation` is skipped (an ordinary exclusive lock suffices);
/// otherwise the caller must hold a cleanup lock.
pub fn heap_page_prune_execute(
    buffer: Buffer,
    lp_truncate_only: bool,
    redirected: Vec<OffsetNumber>,
    nowdead: Vec<OffsetNumber>,
    nowunused: Vec<OffsetNumber>,
) -> PgResult<()> {
    let nredirected = redirected.len() / 2;
    let ndead = nowdead.len();
    let nunused = nowunused.len();

    debug_assert!(nredirected > 0 || ndead > 0 || nunused > 0);
    debug_assert!(!lp_truncate_only || (nredirected == 0 && ndead == 0));

    bufmgr_seam::with_buffer_page::call(buffer, &mut |bytes| {
        let mut page = PageMut::new(bytes)?;

        // Update all redirected line pointers.
        for i in 0..nredirected {
            let fromoff = redirected[i * 2];
            let tooff = redirected[i * 2 + 1];
            let mut fromlp = PageGetItemId(&page.as_ref(), fromoff)?;
            ItemIdSetRedirect(&mut fromlp, tooff);
            page.set_item_id(fromoff, fromlp)?;
        }

        // Update all now-dead line pointers.
        for &off in nowdead.iter() {
            let mut lp = PageGetItemId(&page.as_ref(), off)?;
            ItemIdSetDead(&mut lp);
            page.set_item_id(off, lp)?;
        }

        // Update all now-unused line pointers.
        for &off in nowunused.iter() {
            let mut lp = PageGetItemId(&page.as_ref(), off)?;
            ItemIdSetUnused(&mut lp);
            page.set_item_id(off, lp)?;
        }

        if lp_truncate_only {
            PageTruncateLinePointerArray(&mut page);
        } else {
            // Repair any fragmentation and update the free-pointer hint bit.
            PageRepairFragmentation(&mut page)?;
            // (page_verify_redirects is an assertion-only check; the
            // PageRepairFragmentation post-invariants cover the same ground.)
        }
        Ok(())
    })
}

// ===========================================================================
// heap_get_root_tuples — map each item to its HOT-chain root (pruneheap.c).
// ===========================================================================

/// `heap_get_root_tuples(page, root_offsets)` — for all items on the page, find
/// their HOT-chain root line pointers. Returns the `MaxHeapTuplesPerPage`-long
/// array (`InvalidOffsetNumber` for unmapped slots). Caller holds at least a
/// share lock and a pin.
pub fn heap_get_root_tuples<'mcx>(mcx: Mcx<'mcx>, buffer: Buffer) -> PgResult<Vec<OffsetNumber>> {
    let mut root_offsets: Vec<OffsetNumber> =
        alloc::vec![InvalidOffsetNumber; MaxHeapTuplesPerPage];

    let bytes = bufmgr_seam::buffer_get_page::call(mcx, buffer)?;
    let page = PageRef::new(bytes.as_slice())?;
    let maxoff = PageGetMaxOffsetNumber(&page);

    let mut offnum = FirstOffsetNumber;
    while offnum <= maxoff {
        let lp = PageGetItemId(&page, offnum)?;

        // skip unused and dead items
        if !ItemIdIsUsed(&lp) || ItemIdIsDead(&lp) {
            offnum += 1;
            continue;
        }

        let mut nextoffnum: OffsetNumber;
        let mut prior_xmax: TransactionId;

        if ItemIdIsNormal(&lp) {
            let item = PageGetItem(&page, &lp)?;
            let htup = HeapTupleHeaderData::read_on_page(mcx, item)?;

            // If this tuple is part of a HOT-chain rooted elsewhere, skip it.
            if HeapTupleHeaderIsHeapOnly(&htup) {
                offnum += 1;
                continue;
            }

            // Plain tuple or root of a HOT-chain.
            root_offsets[(offnum - 1) as usize] = offnum;

            if !HeapTupleHeaderIsHotUpdated(&htup) {
                offnum += 1;
                continue;
            }

            nextoffnum = ItemPointerGetOffsetNumber(&htup.t_ctid);
            prior_xmax = HeapTupleHeaderGetUpdateXid(&htup)?;
        } else {
            debug_assert!(ItemIdIsRedirected(&lp));
            nextoffnum = ItemIdGetRedirect(&lp);
            prior_xmax = InvalidTransactionId;
        }

        // Follow the HOT-chain.
        loop {
            if offnum < FirstOffsetNumber {
                break;
            }
            if offnum > maxoff {
                break;
            }

            let lp = PageGetItemId(&page, nextoffnum)?;

            // Check for broken chains.
            if !ItemIdIsNormal(&lp) {
                break;
            }

            let item = PageGetItem(&page, &lp)?;
            let htup = HeapTupleHeaderData::read_on_page(mcx, item)?;

            if TransactionIdIsValid(prior_xmax)
                && !TransactionIdEquals(prior_xmax, HeapTupleHeaderGetXmin(&htup))
            {
                break;
            }

            root_offsets[(nextoffnum - 1) as usize] = offnum;

            if !HeapTupleHeaderIsHotUpdated(&htup) {
                break;
            }

            debug_assert!(!HeapTupleHeaderIndicatesMovedPartitions(&htup));

            nextoffnum = ItemPointerGetOffsetNumber(&htup.t_ctid);
            prior_xmax = HeapTupleHeaderGetUpdateXid(&htup)?;
        }

        offnum += 1;
    }

    Ok(root_offsets)
}

// ===========================================================================
// log_heap_prune_and_freeze — emit the combined XLOG_HEAP2_PRUNE_FREEZE record.
// ===========================================================================

/// `xlhp_freeze_plan` (heapam_xlog.h) — open canonical plan during dedup.
#[derive(Clone, Copy, Default)]
struct XlhpFreezePlan {
    xmax: TransactionId,
    t_infomask2: u16,
    t_infomask: u16,
    frzflags: u8,
    ntuples: u16,
}

/// `heap_log_freeze_eq(plan, frz)` (pruneheap.c).
#[inline]
fn heap_log_freeze_eq(plan: &XlhpFreezePlan, frz: &HeapTupleFreeze) -> bool {
    plan.xmax == frz.xmax
        && plan.t_infomask2 == frz.t_infomask2
        && plan.t_infomask == frz.t_infomask
        && plan.frzflags == frz.frzflags
}

/// `heap_log_freeze_cmp(frz1, frz2)` (pruneheap.c) — dedup sort comparator.
fn heap_log_freeze_cmp(frz1: &HeapTupleFreeze, frz2: &HeapTupleFreeze) -> core::cmp::Ordering {
    use core::cmp::Ordering;
    match frz1.xmax.cmp(&frz2.xmax) {
        Ordering::Equal => {}
        o => return o,
    }
    match frz1.t_infomask2.cmp(&frz2.t_infomask2) {
        Ordering::Equal => {}
        o => return o,
    }
    match frz1.t_infomask.cmp(&frz2.t_infomask) {
        Ordering::Equal => {}
        o => return o,
    }
    match frz1.frzflags.cmp(&frz2.frzflags) {
        Ordering::Equal => {}
        o => return o,
    }
    // Tiebreak on page offset number.
    frz1.offset.cmp(&frz2.offset)
}

/// `heap_log_freeze_new_plan(plan, frz)` (pruneheap.c).
#[inline]
fn heap_log_freeze_new_plan(plan: &mut XlhpFreezePlan, frz: &HeapTupleFreeze) {
    plan.xmax = frz.xmax;
    plan.t_infomask2 = frz.t_infomask2;
    plan.t_infomask = frz.t_infomask;
    plan.frzflags = frz.frzflags;
    plan.ntuples = 1;
}

/// `heap_log_freeze_plan(tuples, ntuples, plans_out, offsets_out)`
/// (pruneheap.c) — deduplicate tuple-based freeze plans. Destructively sorts
/// `tuples` in place. Returns the number of canonical plans and fills
/// `plans_out` and the per-tuple `offsets_out`.
fn heap_log_freeze_plan(
    tuples: &mut [HeapTupleFreeze],
    plans_out: &mut [XlhpFreezePlan],
    offsets_out: &mut [OffsetNumber],
) -> usize {
    let ntuples = tuples.len();
    tuples.sort_by(heap_log_freeze_cmp);

    let mut nplans = 0usize;
    let mut cur = 0usize; // index of the open canonical plan in plans_out

    for i in 0..ntuples {
        let frz = tuples[i];

        if i == 0 {
            heap_log_freeze_new_plan(&mut plans_out[cur], &frz);
            nplans += 1;
        } else if heap_log_freeze_eq(&plans_out[cur], &frz) {
            debug_assert!(offsets_out[i - 1] < frz.offset);
            plans_out[cur].ntuples += 1;
        } else {
            cur += 1;
            heap_log_freeze_new_plan(&mut plans_out[cur], &frz);
            nplans += 1;
        }

        offsets_out[i] = frz.offset;
    }

    debug_assert!(nplans > 0 && nplans <= ntuples);
    nplans
}

/// `log_heap_prune_and_freeze(relation, buffer, conflict_xid, cleanup_lock,
/// reason, frozen, nfrozen, redirected, nredirected, dead, ndead, unused,
/// nunused)` (pruneheap.c). Scribbles on the `frozen` array (sorts it).
///
/// Called in a critical section.
#[allow(clippy::too_many_arguments)]
pub fn log_heap_prune_and_freeze(
    relation: &RelationData<'_>,
    buffer: Buffer,
    conflict_xid: TransactionId,
    cleanup_lock: bool,
    reason: i32,
    frozen: &mut [HeapTupleFreeze],
    redirected: &[OffsetNumber],
    dead: &[OffsetNumber],
    unused: &[OffsetNumber],
) -> PgResult<()> {
    let nfrozen = frozen.len();
    let nredirected = redirected.len() / 2;
    let ndead = dead.len();
    let nunused = unused.len();

    let mut flags: u8 = 0;

    // Prepare data for the buffer.
    xloginsert_seam::xlog_begin_insert::call()?;
    xloginsert_seam::xlog_register_buffer::call(0, buffer, REGBUF_STANDARD)?;

    let mut plans: Vec<XlhpFreezePlan> =
        alloc::vec![XlhpFreezePlan::default(); MaxHeapTuplesPerPage];
    let mut frz_offsets: Vec<OffsetNumber> = alloc::vec![0; MaxHeapTuplesPerPage];

    if nfrozen > 0 {
        flags |= XLHP_HAS_FREEZE_PLANS;

        let nplans = heap_log_freeze_plan(
            &mut frozen[..nfrozen],
            &mut plans[..],
            &mut frz_offsets[..nfrozen],
        );

        // xlhp_freeze_plans { uint16 nplans; xlhp_freeze_plan plans[]; },
        // registered as the offsetof-plans header then the plans array.
        let mut hdr = Vec::with_capacity(4);
        hdr.extend_from_slice(&(nplans as u16).to_ne_bytes());
        hdr.extend_from_slice(&[0u8, 0u8]); // pad to offsetof(plans) == 4
        xloginsert_seam::xlog_register_buf_data::call(0, &hdr)?;

        let mut plan_bytes = Vec::with_capacity(nplans * SIZEOF_XLHP_FREEZE_PLAN);
        for plan in plans.iter().take(nplans) {
            plan_bytes.extend_from_slice(&plan.xmax.to_ne_bytes());
            plan_bytes.extend_from_slice(&plan.t_infomask2.to_ne_bytes());
            plan_bytes.extend_from_slice(&plan.t_infomask.to_ne_bytes());
            plan_bytes.push(plan.frzflags);
            plan_bytes.push(0u8); // pad before ntuples
            plan_bytes.extend_from_slice(&plan.ntuples.to_ne_bytes());
        }
        xloginsert_seam::xlog_register_buf_data::call(0, &plan_bytes)?;
    }

    if nredirected > 0 {
        flags |= XLHP_HAS_REDIRECTIONS;
        xloginsert_seam::xlog_register_buf_data::call(0, &prune_items_header(nredirected))?;
        xloginsert_seam::xlog_register_buf_data::call(
            0,
            &offsets_to_bytes(&redirected[..nredirected * 2]),
        )?;
    }
    if ndead > 0 {
        flags |= XLHP_HAS_DEAD_ITEMS;
        xloginsert_seam::xlog_register_buf_data::call(0, &prune_items_header(ndead))?;
        xloginsert_seam::xlog_register_buf_data::call(0, &offsets_to_bytes(&dead[..ndead]))?;
    }
    if nunused > 0 {
        flags |= XLHP_HAS_NOW_UNUSED_ITEMS;
        xloginsert_seam::xlog_register_buf_data::call(0, &prune_items_header(nunused))?;
        xloginsert_seam::xlog_register_buf_data::call(0, &offsets_to_bytes(&unused[..nunused]))?;
    }
    if nfrozen > 0 {
        xloginsert_seam::xlog_register_buf_data::call(0, &offsets_to_bytes(&frz_offsets[..nfrozen]))?;
    }

    // Prepare the main xl_heap_prune record.
    if relation_is_accessible_in_logical_decoding(relation) {
        flags |= XLHP_IS_CATALOG_REL;
    }
    if TransactionIdIsValid(conflict_xid) {
        flags |= XLHP_HAS_CONFLICT_HORIZON;
    }
    if cleanup_lock {
        flags |= XLHP_CLEANUP_LOCK;
    } else {
        debug_assert!(nredirected == 0 && ndead == 0);
    }

    // xl_heap_prune { uint8 reason; uint8 flags; }
    let reason_info = prune_reason_info(reason)?;
    let mut xlrec = Vec::with_capacity(SIZE_OF_HEAP_PRUNE);
    xlrec.push(reason_info.0); // reason byte (record's `reason` field)
    xlrec.push(flags);
    xloginsert_seam::xlog_register_data::call(&xlrec)?;
    if TransactionIdIsValid(conflict_xid) {
        xloginsert_seam::xlog_register_data::call(&conflict_xid.to_ne_bytes())?;
    }

    let recptr = xloginsert_seam::xlog_insert_record::call(RM_HEAP2_ID, reason_info.1)?;

    // PageSetLSN(BufferGetPage(buffer), recptr).
    bufmgr_seam::page_set_lsn::call(buffer, recptr)?;

    Ok(())
}

/// `xlhp_prune_items { uint16 ntargets; OffsetNumber data[]; }` header
/// (offsetof(data) == 2, so no padding).
fn prune_items_header(ntargets: usize) -> Vec<u8> {
    (ntargets as u16).to_ne_bytes().to_vec()
}

fn offsets_to_bytes(offs: &[OffsetNumber]) -> Vec<u8> {
    let mut v = Vec::with_capacity(offs.len() * 2);
    for &o in offs {
        v.extend_from_slice(&o.to_ne_bytes());
    }
    v
}

/// Map the `PruneReason` to `(record-reason-byte, xlog-info-byte)`. In C the
/// `xl_heap_prune.reason` field stores the `PruneReason` enum value while the
/// record's `info` byte carries the matching `XLOG_HEAP2_PRUNE_*` opcode.
fn prune_reason_info(reason: i32) -> PgResult<(u8, u8)> {
    match reason {
        PRUNE_ON_ACCESS => Ok((reason as u8, XLOG_HEAP2_PRUNE_ON_ACCESS)),
        PRUNE_VACUUM_SCAN => Ok((reason as u8, XLOG_HEAP2_PRUNE_VACUUM_SCAN)),
        PRUNE_VACUUM_CLEANUP => Ok((reason as u8, XLOG_HEAP2_PRUNE_VACUUM_CLEANUP)),
        _ => Err(PgError::error(fmt1(
            "unrecognized prune reason: ",
            &reason.to_string(),
        ))),
    }
}

/// `REGBUF_STANDARD` (xloginsert.h).
const REGBUF_STANDARD: u8 = 0x04;

// ===========================================================================
// RelationIsAccessibleInLogicalDecoding (utils/rel.h) — expanded inline as in
// the heap-AM delete/insert families.
// ===========================================================================

fn relation_is_accessible_in_logical_decoding(relation: &RelationData<'_>) -> bool {
    let wal = xlog_seam::wal_level::call();
    let xlog_logical_info_active = wal >= types_wal::WalLevel::Logical;
    xlog_logical_info_active
        && relcache_seam::relation_needs_wal::call(relation)
        && (catalog_seam::is_catalog_relation::call(relation)
            || relation_is_used_as_catalog_table(relation))
}

fn relation_is_used_as_catalog_table(relation: &RelationData<'_>) -> bool {
    let relkind = relation.rd_rel.relkind;
    (relkind == RELKIND_RELATION || relkind == RELKIND_MATVIEW)
        && relation
            .rd_options
            .as_ref()
            .is_some_and(|o| o.user_catalog_table)
}

// ===========================================================================
// no_std-friendly error-message formatting helpers.
// ===========================================================================

fn fmt1(a: &str, b: &str) -> String {
    let mut s = String::with_capacity(a.len() + b.len());
    s.push_str(a);
    s.push_str(b);
    s
}

fn fmt2(a: &str, b: &str, c: &str, d: &str, e: &str) -> String {
    let mut s = String::with_capacity(a.len() + b.len() + c.len() + d.len() + e.len());
    s.push_str(a);
    s.push_str(b);
    s.push_str(c);
    s.push_str(d);
    s.push_str(e);
    s
}
