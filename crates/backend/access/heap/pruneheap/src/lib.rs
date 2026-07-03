//! pruneheap.c: on-access prune lane live (prune loop, HOT-chain walk,
//! execute, XLOG_HEAP2_PRUNE_ON_ACCESS). Freeze arms and vacuum-only paths
//! (HEAP_PAGE_PRUNE_FREEZE, PageTruncateLinePointerArray) are loud named
//! panics for the vacuum lane.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use ::types_core::xact::{
    InvalidTransactionId, TransactionIdFollows, TransactionIdIsNormal, TransactionIdPrecedes,
};
use ::types_core::{
    Buffer, GlobalVisStateHandle, OffsetNumber, TransactionId, TransactionIdIsValid, XLogRecPtr,
    BLCKSZ,
};
use ::types_error::PgResult;
use ::types_rel::RelationData;
use ::types_snapshot::HTSV_Result;
use ::types_storage::bufpage::{ItemIdData, MaxHeapTuplesPerPage, PageMut, PageRef};
use ::types_tuple::{
    FirstOffsetNumber, HeapTupleData, HeapTupleHeaderData, ItemPointerData,
    ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber,
};

use ::heapam::{HeapTupleHeaderAdvanceConflictHorizon, HeapTupleHeaderGetUpdateXid};
use ::heapam_visibility::HeapTupleSatisfiesVacuumHorizon;

const HEAP_DEFAULT_FILLFACTOR: i32 = 100;

pub const HEAP_PAGE_PRUNE_MARK_UNUSED_NOW: i32 = 1 << 0;
pub const HEAP_PAGE_PRUNE_FREEZE: i32 = 1 << 1;

// PruneReason (heapam.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PruneReason {
    PruneOnAccess = 0,
    PruneVacuumScan = 1,
    PruneVacuumCleanup = 2,
}

pub const XLOG_HEAP2_PRUNE_ON_ACCESS: u8 = 0x10;
pub const XLOG_HEAP2_PRUNE_VACUUM_SCAN: u8 = 0x20;
pub const XLOG_HEAP2_PRUNE_VACUUM_CLEANUP: u8 = 0x30;

// xl_heap_prune flags (heapam_xlog.h).
pub const XLHP_IS_CATALOG_REL: u8 = 1 << 1;
pub const XLHP_CLEANUP_LOCK: u8 = 1 << 2;
pub const XLHP_HAS_CONFLICT_HORIZON: u8 = 1 << 3;
pub const XLHP_HAS_FREEZE_PLANS: u8 = 1 << 4;
pub const XLHP_HAS_REDIRECTIONS: u8 = 1 << 5;
pub const XLHP_HAS_DEAD_ITEMS: u8 = 1 << 6;
pub const XLHP_HAS_NOW_UNUSED_ITEMS: u8 = 1 << 7;

const RM_HEAP2_ID: u8 = rmgr::RmgrIds::RM_HEAP2_ID as u8;
const InvalidOffsetNumber: OffsetNumber = 0;

/// `PruneFreezeResult` (heapam.h).
pub struct PruneFreezeResult {
    pub ndeleted: i32,
    pub nnewlpdead: i32,
    pub nfrozen: i32,
    pub live_tuples: i32,
    pub recently_dead_tuples: i32,
    pub all_visible: bool,
    pub all_frozen: bool,
    pub vm_conflict_horizon: TransactionId,
    pub hastup: bool,
    pub lpdead_items: i32,
    pub deadoffsets: [OffsetNumber; MaxHeapTuplesPerPage],
}

impl Default for PruneFreezeResult {
    fn default() -> Self {
        PruneFreezeResult {
            ndeleted: 0,
            nnewlpdead: 0,
            nfrozen: 0,
            live_tuples: 0,
            recently_dead_tuples: 0,
            all_visible: false,
            all_frozen: false,
            vm_conflict_horizon: InvalidTransactionId,
            hastup: false,
            lpdead_items: 0,
            deadoffsets: [0; MaxHeapTuplesPerPage],
        }
    }
}

// PruneState (pruneheap.c); one stack value per prune, as C. Freeze fields
// (pagefrz, frozen[]) live in the vacuum lane.
struct PruneState {
    vistest: GlobalVisStateHandle,
    mark_unused_now: bool,

    new_prune_xid: TransactionId,
    latest_xid_removed: TransactionId,
    nredirected: usize,
    ndead: usize,
    nunused: usize,
    redirected: [OffsetNumber; MaxHeapTuplesPerPage * 2],
    nowdead: [OffsetNumber; MaxHeapTuplesPerPage],
    nowunused: [OffsetNumber; MaxHeapTuplesPerPage],

    nroot_items: usize,
    root_items: [OffsetNumber; MaxHeapTuplesPerPage],
    nheaponly_items: usize,
    heaponly_items: [OffsetNumber; MaxHeapTuplesPerPage],

    processed: [bool; MaxHeapTuplesPerPage + 1],
    // -1 = not computed (LP_DEAD/unused slots), else HTSV_Result.
    htsv: [i8; MaxHeapTuplesPerPage + 1],

    ndeleted: i32,
    live_tuples: i32,
    recently_dead_tuples: i32,
    hastup: bool,
    lpdead_items: usize,

    all_visible: bool,
    all_frozen: bool,
    visibility_cutoff_xid: TransactionId,
}

/// `heap_page_prune_opt`: opportunistic prune; caller holds a pin and no lock.
pub fn heap_page_prune_opt(rel: &RelationData<'_>, buffer: Buffer) -> PgResult<()> {
    if transam_xlog_seams::recovery_in_progress::call() {
        return Ok(());
    }

    // SAFETY: caller holds a pin on `buffer` (heap_prepare_pagescan contract); pages are BLCKSZ, MAXALIGNed.
    let page = unsafe { PageRef::from_raw(bufmgr_seams::buffer_get_page::call(buffer)) };
    let prune_xid = page.prune_xid();
    if !TransactionIdIsValid(prune_xid) {
        return Ok(());
    }

    let vistest = procarray_seams::global_vis_test_for::call(rel);
    if !procarray_seams::global_vis_test_is_removable_xid::call(vistest, prune_xid)? {
        return Ok(());
    }

    let minfree = rel
        .get_target_page_free_space(HEAP_DEFAULT_FILLFACTOR)
        .max(BLCKSZ / 10);

    if page.is_full() || page.heap_free_space() < minfree {
        if !bufmgr_seams::conditional_lock_buffer_for_cleanup::call(buffer)? {
            return Ok(());
        }

        if page.is_full() || page.heap_free_space() < minfree {
            let mut presult = PruneFreezeResult::default();
            let mut dummy_off_loc = InvalidOffsetNumber;
            heap_page_prune_and_freeze(
                rel,
                buffer,
                vistest,
                0,
                &mut presult,
                PruneReason::PruneOnAccess,
                &mut dummy_off_loc,
            )?;

            if presult.ndeleted > presult.nnewlpdead && rel.pgstat_enabled.get() {
                pgstat::relation::pgstat_update_heap_dead_tuples(
                    rel.rd_id,
                    rel.rd_rel.relisshared,
                    presult.ndeleted - presult.nnewlpdead,
                );
            }
        }

        bufmgr_seams::lock_buffer::call(buffer, bufmgr_seams::BUFFER_LOCK_UNLOCK)?;
    }
    Ok(())
}

/// `heap_page_prune_and_freeze` with `options` restricted to the no-freeze
/// lane (`HEAP_PAGE_PRUNE_FREEZE` panics; cutoffs/new_relfrozen_xid/
/// new_relmin_mxid are vacuum-lane parameters and travel with it).
pub fn heap_page_prune_and_freeze(
    relation: &RelationData<'_>,
    buffer: Buffer,
    vistest: GlobalVisStateHandle,
    options: i32,
    presult: &mut PruneFreezeResult,
    reason: PruneReason,
    off_loc: &mut OffsetNumber,
) -> PgResult<()> {
    if (options & HEAP_PAGE_PRUNE_FREEZE) != 0 {
        unported("heap_prepare_freeze_tuple/heap_pre_freeze_checks (freeze lane; VacuumCutoffs)");
    }

    let page_ptr = bufmgr_seams::buffer_get_page::call(buffer);
    // SAFETY: caller holds a pin + the buffer cleanup lock for the whole call.
    let page = unsafe { PageRef::from_raw(page_ptr) };
    let blockno = bufmgr_seams::buffer_get_block_number::call(buffer);

    let mut prstate = PruneState {
        vistest,
        mark_unused_now: (options & HEAP_PAGE_PRUNE_MARK_UNUSED_NOW) != 0,
        new_prune_xid: InvalidTransactionId,
        latest_xid_removed: InvalidTransactionId,
        nredirected: 0,
        ndead: 0,
        nunused: 0,
        redirected: [0; MaxHeapTuplesPerPage * 2],
        nowdead: [0; MaxHeapTuplesPerPage],
        nowunused: [0; MaxHeapTuplesPerPage],
        nroot_items: 0,
        root_items: [0; MaxHeapTuplesPerPage],
        nheaponly_items: 0,
        heaponly_items: [0; MaxHeapTuplesPerPage],
        processed: [false; MaxHeapTuplesPerPage + 1],
        htsv: [-1; MaxHeapTuplesPerPage + 1],
        ndeleted: 0,
        live_tuples: 0,
        recently_dead_tuples: 0,
        hastup: false,
        lpdead_items: 0,
        all_visible: false,
        all_frozen: false,
        visibility_cutoff_xid: InvalidTransactionId,
    };

    let maxoff = page.max_offset_number();

    // HTSV once per tuple (a second call could answer differently), in
    // reverse offset order: tuples then read at increasing page offsets,
    // which the prefetcher likes.
    let mut offnum = maxoff;
    while offnum >= FirstOffsetNumber {
        // SAFETY: offnum <= maxoff.
        let itemid = unsafe { page.item_id_unchecked(offnum) };
        *off_loc = offnum;
        prstate.processed[offnum as usize] = false;
        prstate.htsv[offnum as usize] = -1;

        if !itemid.is_used() {
            heap_prune_record_unchanged_lp_unused(&mut prstate, offnum);
            offnum -= 1;
            continue;
        }
        if itemid.is_dead() {
            if prstate.mark_unused_now {
                heap_prune_record_unused(&mut prstate, offnum, false);
            } else {
                heap_prune_record_unchanged_lp_dead(presult, &mut prstate, offnum);
            }
            offnum -= 1;
            continue;
        }
        if itemid.is_redirected() {
            prstate.root_items[prstate.nroot_items] = offnum;
            prstate.nroot_items += 1;
            offnum -= 1;
            continue;
        }

        debug_assert!(itemid.is_normal());
        // SAFETY: LP_NORMAL item within the page image (page invariant).
        let (ptr, len) = unsafe { page.item_raw_unchecked(itemid) };
        // SAFETY: in-page tuple image, exclusively held; HTSV hint-bit stores land in the page, as C.
        let mut tup = unsafe {
            HeapTupleData::from_raw_parts(
                ptr,
                len,
                ItemPointerData::new(blockno, offnum),
                relation.rd_id,
            )
        };
        let is_heap_only = tup.is_heap_only();
        let res = heap_prune_satisfies_vacuum(&prstate, &mut tup, buffer)?;
        prstate.htsv[offnum as usize] = res as i8;

        if !is_heap_only {
            prstate.root_items[prstate.nroot_items] = offnum;
            prstate.nroot_items += 1;
        } else {
            prstate.heaponly_items[prstate.nheaponly_items] = offnum;
            prstate.nheaponly_items += 1;
        }
        offnum -= 1;
    }

    for i in (0..prstate.nroot_items).rev() {
        let offnum = prstate.root_items[i];
        if prstate.processed[offnum as usize] {
            continue;
        }
        *off_loc = offnum;
        heap_prune_chain(page, blockno, maxoff, offnum, &mut prstate, presult)?;
    }

    for i in (0..prstate.nheaponly_items).rev() {
        let offnum = prstate.heaponly_items[i];
        if prstate.processed[offnum as usize] {
            continue;
        }
        *off_loc = offnum;

        if prstate.htsv[offnum as usize] == HTSV_Result::HEAPTUPLE_DEAD as i8 {
            // SAFETY: queued as LP_NORMAL above; page exclusively held.
            let itemid = unsafe { page.item_id_unchecked(offnum) };
            // SAFETY: LP_NORMAL item.
            let htup = unsafe { header_at(page, itemid) };
            if !htup.is_hot_updated() {
                HeapTupleHeaderAdvanceConflictHorizon(htup, &mut prstate.latest_xid_removed)?;
                heap_prune_record_unused(&mut prstate, offnum, true);
            } else {
                return Err(Box::new(::types_error::PgError::new(
                    ::types_error::ERROR,
                    format!(
                        "dead heap-only tuple ({blockno}, {offnum}) is not linked to from any HOT chain"
                    ),
                )));
            }
        } else {
            heap_prune_record_unchanged_lp_normal(page, &mut prstate, offnum);
        }
    }

    #[cfg(debug_assertions)]
    for offnum in FirstOffsetNumber..=maxoff {
        *off_loc = offnum;
        debug_assert!(prstate.processed[offnum as usize]);
    }
    *off_loc = InvalidOffsetNumber;

    let do_prune = prstate.nredirected > 0 || prstate.ndead > 0 || prstate.nunused > 0;
    let do_hint = page.prune_xid() != prstate.new_prune_xid || page.is_full();

    init_small::globals::StartCriticalSection();
    let res = prune_apply(relation, buffer, page_ptr, &prstate, reason, do_prune, do_hint);
    init_small::globals::EndCriticalSection();
    res?;

    presult.ndeleted = prstate.ndeleted;
    presult.nnewlpdead = prstate.ndead as i32;
    presult.nfrozen = 0;
    presult.live_tuples = prstate.live_tuples;
    presult.recently_dead_tuples = prstate.recently_dead_tuples;
    presult.all_visible = prstate.all_visible && prstate.lpdead_items == 0;
    presult.all_frozen = prstate.all_frozen && prstate.lpdead_items == 0;
    presult.hastup = prstate.hastup;
    presult.vm_conflict_horizon = if presult.all_frozen {
        InvalidTransactionId
    } else {
        prstate.visibility_cutoff_xid
    };
    presult.lpdead_items = prstate.lpdead_items as i32;
    Ok(())
}

fn prune_apply(
    relation: &RelationData<'_>,
    buffer: Buffer,
    page_ptr: core::ptr::NonNull<u8>,
    prstate: &PruneState,
    reason: PruneReason,
    do_prune: bool,
    do_hint: bool,
) -> PgResult<()> {
    // SAFETY: cleanup lock held by the caller for the whole prune.
    let mut pm = unsafe { PageMut::from_raw(page_ptr) };

    if do_hint {
        pm.set_prune_xid(prstate.new_prune_xid);
        pm.clear_full();
        if !do_prune {
            bufmgr_seams::mark_buffer_dirty_hint::call(buffer, true)?;
        }
    }

    if do_prune {
        heap_page_prune_execute(
            buffer,
            false,
            &prstate.redirected[..prstate.nredirected * 2],
            &prstate.nowdead[..prstate.ndead],
            &prstate.nowunused[..prstate.nunused],
        );

        bufmgr_seams::mark_buffer_dirty::call(buffer)?;

        if relation_needs_wal(relation) {
            log_heap_prune_and_freeze(
                relation,
                buffer,
                prstate.latest_xid_removed,
                true,
                reason,
                &prstate.redirected[..prstate.nredirected * 2],
                &prstate.nowdead[..prstate.ndead],
                &prstate.nowunused[..prstate.nunused],
            )?;
        }
    }
    Ok(())
}

fn relation_needs_wal(rel: &RelationData<'_>) -> bool {
    rel.is_permanent()
}

// heap_prune_satisfies_vacuum, no-cutoffs form (the OldestXmin arm is the vacuum lane's).
fn heap_prune_satisfies_vacuum(
    prstate: &PruneState,
    tup: &mut HeapTupleData<'_>,
    buffer: Buffer,
) -> PgResult<HTSV_Result> {
    let mut dead_after = InvalidTransactionId;
    let res = HeapTupleSatisfiesVacuumHorizon(tup, buffer, &mut dead_after)?;
    if res != HTSV_Result::HEAPTUPLE_RECENTLY_DEAD {
        return Ok(res);
    }
    if procarray_seams::global_vis_test_is_removable_xid::call(prstate.vistest, dead_after)? {
        return Ok(HTSV_Result::HEAPTUPLE_DEAD);
    }
    Ok(res)
}

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

/// # Safety
/// `itemid` is an LP_NORMAL item of `page`, whose image is exclusively held.
unsafe fn header_at<'a>(page: PageRef<'a>, itemid: ItemIdData) -> &'a HeapTupleHeaderData {
    // SAFETY: caller contract.
    let (ptr, _len) = unsafe { page.item_raw_unchecked(itemid) };
    // SAFETY: heap tuples start with a full HeapTupleHeaderData.
    unsafe { &*ptr.cast::<HeapTupleHeaderData>() }
}

// heap_prune_chain: walk one HOT chain (or standalone item) and record the
// fate of each member.
fn heap_prune_chain(
    page: PageRef<'_>,
    blockno: u32,
    maxoff: OffsetNumber,
    rootoffnum: OffsetNumber,
    prstate: &mut PruneState,
    presult: &mut PruneFreezeResult,
) -> PgResult<()> {
    let mut priorXmax = InvalidTransactionId;
    let mut chainitems = [0 as OffsetNumber; MaxHeapTuplesPerPage];
    // Index in chainitems of the first live successor after the last dead item.
    let mut ndeadchain = 0usize;
    let mut nchain = 0usize;

    // SAFETY: rootoffnum <= maxoff (root_items filled from the line array).
    let rootlp = unsafe { page.item_id_unchecked(rootoffnum) };
    let mut offnum = rootoffnum;

    let mut reached_live = false;
    loop {
        if offnum < FirstOffsetNumber || offnum > maxoff {
            break; // past the truncated end of the line array
        }
        if prstate.processed[offnum as usize] {
            break; // must not be the same chain
        }
        // SAFETY: FirstOffsetNumber <= offnum <= maxoff, checked above.
        let lp = unsafe { page.item_id_unchecked(offnum) };
        debug_assert!(lp.is_used());
        debug_assert!(!lp.is_dead());

        if lp.is_redirected() {
            if nchain > 0 {
                break; // not at start of chain
            }
            chainitems[nchain] = offnum;
            nchain += 1;
            offnum = rootlp.lp_off(); // ItemIdGetRedirect
            continue;
        }

        debug_assert!(lp.is_normal());
        // SAFETY: LP_NORMAL item; page exclusively held.
        let htup = unsafe { header_at(page, lp) };

        if TransactionIdIsValid(priorXmax) && htup.xmin() != priorXmax {
            break;
        }

        chainitems[nchain] = offnum;
        nchain += 1;

        match htsv_get_valid_status(prstate.htsv[offnum as usize]) {
            HTSV_Result::HEAPTUPLE_DEAD => {
                ndeadchain = nchain;
                HeapTupleHeaderAdvanceConflictHorizon(htup, &mut prstate.latest_xid_removed)?;
            }
            HTSV_Result::HEAPTUPLE_RECENTLY_DEAD => {
                // Advance past RECENTLY_DEAD: a DEAD member may follow, and
                // its conflict horizon covers this one.
            }
            _ => {
                reached_live = true;
            }
        }
        if reached_live {
            break;
        }

        if !htup.is_hot_updated() {
            reached_live = true; // end of chain: process it
            break;
        }
        debug_assert!(!htup.indicates_moved_partitions());
        debug_assert!(ItemPointerGetBlockNumber(&htup.t_ctid) == blockno);
        offnum = ItemPointerGetOffsetNumber(&htup.t_ctid);
        priorXmax = HeapTupleHeaderGetUpdateXid(htup)?;
    }

    if !reached_live && rootlp.is_redirected() && nchain < 2 {
        heap_prune_record_dead_or_unused(presult, prstate, rootoffnum, false);
        return Ok(());
    }

    if ndeadchain == 0 {
        let mut i = 0;
        if rootlp.is_redirected() {
            heap_prune_record_unchanged_lp_redirect(prstate, rootoffnum);
            i = 1;
        }
        for &item in &chainitems[i..nchain] {
            heap_prune_record_unchanged_lp_normal(page, prstate, item);
        }
    } else if ndeadchain == nchain {
        heap_prune_record_dead_or_unused(presult, prstate, rootoffnum, rootlp.is_normal());
        for &item in &chainitems[1..nchain] {
            heap_prune_record_unused(prstate, item, true);
        }
    } else {
        heap_prune_record_redirect(prstate, rootoffnum, chainitems[ndeadchain], rootlp.is_normal());
        for &item in &chainitems[1..ndeadchain] {
            heap_prune_record_unused(prstate, item, true);
        }
        for &item in &chainitems[ndeadchain..nchain] {
            heap_prune_record_unchanged_lp_normal(page, prstate, item);
        }
    }
    Ok(())
}

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

fn heap_prune_record_dead(
    presult: &mut PruneFreezeResult,
    prstate: &mut PruneState,
    offnum: OffsetNumber,
    was_normal: bool,
) {
    debug_assert!(!prstate.processed[offnum as usize]);
    prstate.processed[offnum as usize] = true;
    debug_assert!(prstate.ndead < MaxHeapTuplesPerPage);
    prstate.nowdead[prstate.ndead] = offnum;
    prstate.ndead += 1;
    // all_visible stays set: removable dead tuples must not preclude freezing.
    presult.deadoffsets[prstate.lpdead_items] = offnum;
    prstate.lpdead_items += 1;
    if was_normal {
        prstate.ndeleted += 1;
    }
}

fn heap_prune_record_dead_or_unused(
    presult: &mut PruneFreezeResult,
    prstate: &mut PruneState,
    offnum: OffsetNumber,
    was_normal: bool,
) {
    if prstate.mark_unused_now {
        heap_prune_record_unused(prstate, offnum, was_normal);
    } else {
        heap_prune_record_dead(presult, prstate, offnum, was_normal);
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

// The freeze-plan arm is the vacuum lane's; all_visible starts false without
// FREEZE, so the all-visible tracking body is dead here, as in C.
fn heap_prune_record_unchanged_lp_normal(
    page: PageRef<'_>,
    prstate: &mut PruneState,
    offnum: OffsetNumber,
) {
    debug_assert!(!prstate.processed[offnum as usize]);
    prstate.processed[offnum as usize] = true;
    prstate.hastup = true;

    // SAFETY: recorded as LP_NORMAL during the scan; page exclusively held.
    let itemid = unsafe { page.item_id_unchecked(offnum) };
    // SAFETY: LP_NORMAL item.
    let htup = unsafe { header_at(page, itemid) };

    match htsv_get_valid_status(prstate.htsv[offnum as usize]) {
        HTSV_Result::HEAPTUPLE_LIVE => {
            prstate.live_tuples += 1;
            if prstate.all_visible {
                unported("all-visible tracking (freeze lane; VacuumCutoffs->OldestXmin)");
            }
        }
        HTSV_Result::HEAPTUPLE_RECENTLY_DEAD => {
            prstate.recently_dead_tuples += 1;
            prstate.all_visible = false;
            let xid = HeapTupleHeaderGetUpdateXid(htup)
                .expect("multixact update xid resolvable during prune");
            heap_prune_record_prunable(prstate, xid);
        }
        HTSV_Result::HEAPTUPLE_INSERT_IN_PROGRESS => {
            // Not counted live (acquire_sample_rows parity).
            prstate.all_visible = false;
        }
        HTSV_Result::HEAPTUPLE_DELETE_IN_PROGRESS => {
            prstate.live_tuples += 1;
            prstate.all_visible = false;
            let xid = HeapTupleHeaderGetUpdateXid(htup)
                .expect("multixact update xid resolvable during prune");
            heap_prune_record_prunable(prstate, xid);
        }
        HTSV_Result::HEAPTUPLE_DEAD => {
            panic!("unexpected HeapTupleSatisfiesVacuum result");
        }
    }
}

fn heap_prune_record_unchanged_lp_dead(
    presult: &mut PruneFreezeResult,
    prstate: &mut PruneState,
    offnum: OffsetNumber,
) {
    debug_assert!(!prstate.processed[offnum as usize]);
    prstate.processed[offnum as usize] = true;
    // No hastup: LP_DEAD items are assumed LP_UNUSED-to-be for rel truncation.
    presult.deadoffsets[prstate.lpdead_items] = offnum;
    prstate.lpdead_items += 1;
}

fn heap_prune_record_unchanged_lp_redirect(prstate: &mut PruneState, offnum: OffsetNumber) {
    debug_assert!(!prstate.processed[offnum as usize]);
    prstate.processed[offnum as usize] = true;
}

/// `heap_page_prune_execute`: apply the planned line-pointer changes.
/// `redirected` carries from/to pairs (2 entries per redirect). Requires the
/// cleanup lock unless `lp_truncate_only` (vacuum's 2nd pass).
pub fn heap_page_prune_execute(
    buffer: Buffer,
    lp_truncate_only: bool,
    redirected: &[OffsetNumber],
    nowdead: &[OffsetNumber],
    nowunused: &[OffsetNumber],
) {
    debug_assert!(!redirected.is_empty() || !nowdead.is_empty() || !nowunused.is_empty());
    debug_assert!(!lp_truncate_only || (redirected.is_empty() && nowdead.is_empty()));

    let page_ptr = bufmgr_seams::buffer_get_page::call(buffer);
    // SAFETY: cleanup lock (exclusive lock if lp_truncate_only) held by the caller.
    let mut pm = unsafe { PageMut::from_raw(page_ptr) };
    let page = unsafe { PageRef::from_raw(page_ptr) };

    for pair in redirected.chunks_exact(2) {
        let (fromoff, tooff) = (pair[0], pair[1]);
        let mut fromlp = page.item_id(fromoff);
        #[cfg(debug_assertions)]
        {
            // A new LP_REDIRECT must be a HOT-chain root or a re-aimed redirect.
            if !fromlp.is_redirected() {
                debug_assert!(fromlp.has_storage() && fromlp.is_normal());
                // SAFETY: LP_NORMAL item, page held.
                let htup = unsafe { header_at(page, fromlp) };
                debug_assert!(!htup.is_heap_only());
            } else {
                debug_assert!(fromlp.lp_off() != tooff);
            }
            // The target must be a live heap-only tuple (page_verify_redirects).
            let tolp = page.item_id(tooff);
            debug_assert!(tolp.has_storage() && tolp.is_normal());
            // SAFETY: as above.
            let htup = unsafe { header_at(page, tolp) };
            debug_assert!(htup.is_heap_only());
        }
        fromlp.set_redirect(tooff);
        pm.set_item_id(fromoff, fromlp);
    }

    for &off in nowdead {
        let mut lp = page.item_id(off);
        #[cfg(debug_assertions)]
        {
            // LP_DEAD keeps a TID indexes may reference: never heap-only.
            if lp.has_storage() {
                debug_assert!(lp.is_normal());
                // SAFETY: as above.
                let htup = unsafe { header_at(page, lp) };
                debug_assert!(!htup.is_heap_only());
            } else {
                debug_assert!(lp.is_redirected());
            }
        }
        lp.set_dead();
        pm.set_item_id(off, lp);
    }

    for &off in nowunused {
        let mut lp = page.item_id(off);
        #[cfg(debug_assertions)]
        {
            if lp_truncate_only {
                debug_assert!(lp.is_dead() && !lp.has_storage());
            } else if !nowdead.is_empty() {
                // mark_unused_now was false: unused items are heap-only chain members.
                debug_assert!(lp.has_storage() && lp.is_normal());
                // SAFETY: as above.
                let htup = unsafe { header_at(page, lp) };
                debug_assert!(htup.is_heap_only());
            } else {
                debug_assert!(lp.is_used());
            }
        }
        lp.set_unused();
        pm.set_item_id(off, lp);
    }

    if lp_truncate_only {
        pm.truncate_line_pointer_array();
    } else {
        pm.repair_fragmentation();
        page_verify_redirects(page);
    }
}

fn page_verify_redirects(page: PageRef<'_>) {
    #[cfg(debug_assertions)]
    {
        let maxoff = page.max_offset_number();
        for offnum in FirstOffsetNumber..=maxoff {
            // SAFETY: offnum <= maxoff.
            let itemid = unsafe { page.item_id_unchecked(offnum) };
            if !itemid.is_redirected() {
                continue;
            }
            let targitem = page.item_id(itemid.lp_off());
            debug_assert!(targitem.is_used());
            debug_assert!(targitem.is_normal());
            debug_assert!(targitem.has_storage());
            // SAFETY: LP_NORMAL item, page held.
            let htup = unsafe { header_at(page, targitem) };
            debug_assert!(htup.is_heap_only());
        }
    }
    #[cfg(not(debug_assertions))]
    let _ = page;
}

/// `heap_get_root_tuples`: root line pointer for every HOT-chain member;
/// unused entries are InvalidOffsetNumber. Caller holds at least share lock.
pub fn heap_get_root_tuples(
    page: PageRef<'_>,
    root_offsets: &mut [OffsetNumber; MaxHeapTuplesPerPage],
) -> PgResult<()> {
    root_offsets.fill(InvalidOffsetNumber);
    let maxoff = page.max_offset_number();
    for offnum in FirstOffsetNumber..=maxoff {
        // SAFETY: offnum <= maxoff.
        let lp = unsafe { page.item_id_unchecked(offnum) };
        if !lp.is_used() || lp.is_dead() {
            continue;
        }

        let mut nextoffnum;
        let mut priorXmax;
        if lp.is_normal() {
            // SAFETY: LP_NORMAL item; share lock held per contract.
            let htup = unsafe { header_at(page, lp) };
            if htup.is_heap_only() {
                continue; // reached via its root
            }
            root_offsets[offnum as usize - 1] = offnum;
            if !htup.is_hot_updated() {
                continue;
            }
            nextoffnum = ItemPointerGetOffsetNumber(&htup.t_ctid);
            priorXmax = HeapTupleHeaderGetUpdateXid(htup)?;
        } else {
            debug_assert!(lp.is_redirected());
            nextoffnum = lp.lp_off();
            priorXmax = InvalidTransactionId;
        }

        loop {
            if nextoffnum < FirstOffsetNumber || nextoffnum > maxoff {
                break;
            }
            // SAFETY: bounds checked above.
            let lp = unsafe { page.item_id_unchecked(nextoffnum) };
            if !lp.is_normal() {
                break;
            }
            // SAFETY: LP_NORMAL item.
            let htup = unsafe { header_at(page, lp) };
            if TransactionIdIsValid(priorXmax) && priorXmax != htup.xmin() {
                break;
            }
            root_offsets[nextoffnum as usize - 1] = offnum;
            if !htup.is_hot_updated() {
                break;
            }
            debug_assert!(!htup.indicates_moved_partitions());
            nextoffnum = ItemPointerGetOffsetNumber(&htup.t_ctid);
            priorXmax = HeapTupleHeaderGetUpdateXid(htup)?;
        }
    }
    Ok(())
}

/// `log_heap_prune_and_freeze`, freeze plans excluded (vacuum lane): emits
/// XLOG_HEAP2_PRUNE_* with the redirect/dead/unused sub-records as block 0
/// data and the conflict horizon unaligned after the 2-byte xl_heap_prune.
#[allow(clippy::too_many_arguments)]
pub fn log_heap_prune_and_freeze(
    _relation: &RelationData<'_>,
    buffer: Buffer,
    conflict_xid: TransactionId,
    cleanup_lock: bool,
    reason: PruneReason,
    redirected: &[OffsetNumber],
    dead: &[OffsetNumber],
    unused: &[OffsetNumber],
) -> PgResult<XLogRecPtr> {
    let mut flags: u8 = 0;

    // xlhp_prune_items { uint16 ntargets; data[] } per group; offset arrays cross as raw byte views.
    let redirect_hdr = ((redirected.len() / 2) as u16).to_ne_bytes();
    let dead_hdr = (dead.len() as u16).to_ne_bytes();
    let unused_hdr = (unused.len() as u16).to_ne_bytes();

    let mut bufdata: [&[u8]; 6] = [&[]; 6];
    let mut n = 0;
    if !redirected.is_empty() {
        flags |= XLHP_HAS_REDIRECTIONS;
        bufdata[n] = &redirect_hdr;
        bufdata[n + 1] = offsets_bytes(redirected);
        n += 2;
    }
    if !dead.is_empty() {
        flags |= XLHP_HAS_DEAD_ITEMS;
        bufdata[n] = &dead_hdr;
        bufdata[n + 1] = offsets_bytes(dead);
        n += 2;
    }
    if !unused.is_empty() {
        flags |= XLHP_HAS_NOW_UNUSED_ITEMS;
        bufdata[n] = &unused_hdr;
        bufdata[n + 1] = offsets_bytes(unused);
        n += 2;
    }

    // RelationIsAccessibleInLogicalDecoding const-false (heapam DML divergence): no XLHP_IS_CATALOG_REL.
    if TransactionIdIsValid(conflict_xid) {
        flags |= XLHP_HAS_CONFLICT_HORIZON;
    }
    if cleanup_lock {
        flags |= XLHP_CLEANUP_LOCK;
    } else {
        debug_assert!(redirected.is_empty() && dead.is_empty());
    }

    let info = match reason {
        PruneReason::PruneOnAccess => XLOG_HEAP2_PRUNE_ON_ACCESS,
        PruneReason::PruneVacuumScan => XLOG_HEAP2_PRUNE_VACUUM_SCAN,
        PruneReason::PruneVacuumCleanup => XLOG_HEAP2_PRUNE_VACUUM_CLEANUP,
    };

    // C divergence: C ships an uninitialized stack byte for xl_heap_prune.reason
    // (redo derives it from `info`); we stamp the enum value.
    let xlrec = [reason as u8, flags];
    let conflict = conflict_xid.to_ne_bytes();
    let main_data: [&[u8]; 2] = [
        &xlrec,
        if TransactionIdIsValid(conflict_xid) { &conflict } else { &[] },
    ];

    let recptr = xloginsert_seams::xlog_insert_record::call(
        RM_HEAP2_ID,
        info,
        0,
        &main_data,
        &[xloginsert_seams::XLogRegBuf {
            block_id: 0,
            buffer,
            flags: xloginsert_seams::REGBUF_STANDARD,
            bufdata: &bufdata[..n],
        }],
    )?;

    // SAFETY: caller holds the buffer exclusively (critical section).
    let mut pm = unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(buffer)) };
    pm.set_lsn(recptr);
    Ok(recptr)
}

fn offsets_bytes(offs: &[OffsetNumber]) -> &[u8] {
    // SAFETY: OffsetNumber is u16 POD; same allocation, len*2 bytes.
    unsafe { core::slice::from_raw_parts(offs.as_ptr().cast::<u8>(), offs.len() * 2) }
}

#[cold]
#[inline(never)]
fn unported(unit: &'static str) -> ! {
    panic!("unported callee reached from pruneheap.c: {unit}");
}

pub fn init_seams() {
    pruneheap_seams::heap_page_prune_opt::set(heap_page_prune_opt);
    pruneheap_seams::heap_page_prune_execute::set(heap_page_prune_execute);
}

#[cfg(test)]
mod tests;
