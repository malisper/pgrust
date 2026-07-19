//! pruneheap.c: prune + freeze lanes live (prune loop, HOT-chain walk,
//! execute, freeze plans, XLOG_HEAP2_PRUNE_*).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use ::tableam_vocab::VacuumCutoffs;
use ::types_core::xact::{
    InvalidTransactionId, TransactionIdFollows, TransactionIdIsNormal, TransactionIdPrecedes,
};
use ::types_core::{
    Buffer, GlobalVisStateHandle, MultiXactId, OffsetNumber, TransactionId, TransactionIdIsValid,
    XLogRecPtr, BLCKSZ,
};
use ::types_error::PgResult;
use ::types_rel::RelationData;
use ::types_snapshot::HTSV_Result;
use ::types_storage::bufpage::{ItemIdData, MaxHeapTuplesPerPage, PageMut, PageRef};
use ::types_tuple::{
    FirstOffsetNumber, HeapTupleData, HeapTupleHeaderData, ItemPointerData,
    ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber,
};

use ::heapam::freeze::{
    heap_freeze_prepared_tuples, heap_pre_freeze_checks, heap_prepare_freeze_tuple,
    HeapPageFreeze, HeapTupleFreeze,
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

// PruneState (pruneheap.c); one stack value per prune, as C.
struct PruneState<'a> {
    vistest: GlobalVisStateHandle,
    mark_unused_now: bool,
    freeze: bool,
    cutoffs: Option<&'a VacuumCutoffs>,

    pagefrz: HeapPageFreeze,
    nfrozen: usize,
    frozen: [HeapTupleFreeze; MaxHeapTuplesPerPage],

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
                None,
                &mut presult,
                PruneReason::PruneOnAccess,
                &mut dummy_off_loc,
                None,
                None,
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

/// `heap_page_prune_and_freeze`: `cutoffs`, `new_relfrozen_xid` and
/// `new_relmin_mxid` are required iff `HEAP_PAGE_PRUNE_FREEZE` is set.
#[allow(clippy::too_many_arguments)]
pub fn heap_page_prune_and_freeze(
    relation: &RelationData<'_>,
    buffer: Buffer,
    vistest: GlobalVisStateHandle,
    options: i32,
    cutoffs: Option<&VacuumCutoffs>,
    presult: &mut PruneFreezeResult,
    reason: PruneReason,
    off_loc: &mut OffsetNumber,
    mut new_relfrozen_xid: Option<&mut TransactionId>,
    mut new_relmin_mxid: Option<&mut MultiXactId>,
) -> PgResult<()> {
    let page_ptr = bufmgr_seams::buffer_get_page::call(buffer);
    // SAFETY: caller holds a pin + the buffer cleanup lock for the whole call.
    let page = unsafe { PageRef::from_raw(page_ptr) };
    let blockno = bufmgr_seams::buffer_get_block_number::call(buffer);
    let freeze = (options & HEAP_PAGE_PRUNE_FREEZE) != 0;
    let fpi_before = transam_xlog_seams::wal_usage_fpi::call();

    let pagefrz = if freeze {
        let new_relfrozen_xid = new_relfrozen_xid.as_deref_mut().expect("FREEZE needs trackers");
        let new_relmin_mxid = new_relmin_mxid.as_deref_mut().expect("FREEZE needs trackers");
        HeapPageFreeze {
            freeze_required: false,
            FreezePageRelfrozenXid: *new_relfrozen_xid,
            NoFreezePageRelfrozenXid: *new_relfrozen_xid,
            FreezePageRelminMxid: *new_relmin_mxid,
            NoFreezePageRelminMxid: *new_relmin_mxid,
        }
    } else {
        debug_assert!(new_relfrozen_xid.is_none() && new_relmin_mxid.is_none());
        HeapPageFreeze {
            freeze_required: false,
            FreezePageRelfrozenXid: InvalidTransactionId,
            NoFreezePageRelfrozenXid: InvalidTransactionId,
            FreezePageRelminMxid: 0,
            NoFreezePageRelminMxid: 0,
        }
    };

    let mut prstate = PruneState {
        vistest,
        mark_unused_now: (options & HEAP_PAGE_PRUNE_MARK_UNUSED_NOW) != 0,
        freeze,
        cutoffs,
        pagefrz,
        nfrozen: 0,
        frozen: [HeapTupleFreeze::default(); MaxHeapTuplesPerPage],
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
        // With FREEZE, all_visible/all_frozen start true and are cleared by
        // any tuple that precludes them; without it they start false so the
        // tracking body stays dead.
        all_visible: freeze,
        all_frozen: freeze,
        visibility_cutoff_xid: InvalidTransactionId,
    };
    debug_assert!(!freeze || cutoffs.is_some());

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

    // heap_prune_satisfies_vacuum may have emitted a hint-bit FPI (checksums).
    let hint_bit_fpi = fpi_before != transam_xlog_seams::wal_usage_fpi::call();

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
            heap_prune_record_unchanged_lp_normal(page, &mut prstate, offnum)?;
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

    let mut do_freeze = false;
    if prstate.freeze {
        if prstate.pagefrz.freeze_required {
            do_freeze = true;
        } else if prstate.all_visible && prstate.all_frozen && prstate.nfrozen > 0 {
            // Opportunistic: freeze if the page would become all-frozen and
            // an FPI was (or will be) emitted anyway.
            if relation_needs_wal(relation) {
                if hint_bit_fpi {
                    do_freeze = true;
                } else if do_prune {
                    if xloginsert_seams::xlog_check_buffer_needs_backup::call(buffer) {
                        do_freeze = true;
                    }
                } else if do_hint
                    && xlog_hint_bit_is_needed()
                    && xloginsert_seams::xlog_check_buffer_needs_backup::call(buffer)
                {
                    do_freeze = true;
                }
            }
        }
    }

    if do_freeze {
        heap_pre_freeze_checks(buffer, &prstate.frozen[..prstate.nfrozen])?;
    } else if prstate.nfrozen > 0 {
        // Chose not to freeze prepared plans; the page won't be all-frozen.
        debug_assert!(!prstate.pagefrz.freeze_required);
        prstate.all_frozen = false;
        prstate.nfrozen = 0;
    }

    init_small::globals::StartCriticalSection();
    let res = prune_apply(
        relation, buffer, page_ptr, &mut prstate, reason, do_prune, do_hint, do_freeze,
    );
    init_small::globals::EndCriticalSection();
    res?;

    presult.ndeleted = prstate.ndeleted;
    presult.nnewlpdead = prstate.ndead as i32;
    presult.nfrozen = prstate.nfrozen as i32;
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

    if prstate.freeze {
        let new_relfrozen_xid = new_relfrozen_xid.expect("FREEZE needs trackers");
        let new_relmin_mxid = new_relmin_mxid.expect("FREEZE needs trackers");
        if presult.nfrozen > 0 {
            *new_relfrozen_xid = prstate.pagefrz.FreezePageRelfrozenXid;
            *new_relmin_mxid = prstate.pagefrz.FreezePageRelminMxid;
        } else {
            *new_relfrozen_xid = prstate.pagefrz.NoFreezePageRelfrozenXid;
            *new_relmin_mxid = prstate.pagefrz.NoFreezePageRelminMxid;
        }
    }
    Ok(())
}

// XLogHintBitIsNeeded() (xlog.h); uninstalled slots read as boot defaults.
fn xlog_hint_bit_is_needed() -> bool {
    (transam_xlog_seams::data_checksums_enabled::is_installed()
        && transam_xlog_seams::data_checksums_enabled::call())
        || (guc_tables::vars::wal_log_hints.installed() && guc_tables::vars::wal_log_hints.read())
}

#[allow(clippy::too_many_arguments)]
fn prune_apply(
    relation: &RelationData<'_>,
    buffer: Buffer,
    page_ptr: core::ptr::NonNull<u8>,
    prstate: &mut PruneState<'_>,
    reason: PruneReason,
    do_prune: bool,
    do_hint: bool,
    do_freeze: bool,
) -> PgResult<()> {
    // SAFETY: cleanup lock held by the caller for the whole prune.
    let mut pm = unsafe { PageMut::from_raw(page_ptr) };

    if do_hint {
        pm.set_prune_xid(prstate.new_prune_xid);
        pm.clear_full();
        if !do_freeze && !do_prune {
            bufmgr_seams::mark_buffer_dirty_hint::call(buffer, true)?;
        }
    }

    if do_prune || do_freeze {
        if do_prune {
            heap_page_prune_execute(
                buffer,
                false,
                &prstate.redirected[..prstate.nredirected * 2],
                &prstate.nowdead[..prstate.ndead],
                &prstate.nowunused[..prstate.nunused],
            );
        }

        if do_freeze {
            heap_freeze_prepared_tuples(buffer, &prstate.frozen[..prstate.nfrozen]);
        }

        bufmgr_seams::mark_buffer_dirty::call(buffer)?;

        if relation_needs_wal(relation) {
            // The record's conflict horizon is the most conservative of the
            // prune and freeze horizons.
            let mut frz_conflict_horizon = InvalidTransactionId;
            if do_freeze {
                if prstate.all_visible && prstate.all_frozen {
                    frz_conflict_horizon = prstate.visibility_cutoff_xid;
                } else {
                    // Avoids false conflicts when hot_standby_feedback in use.
                    frz_conflict_horizon =
                        prstate.cutoffs.expect("freeze lane has cutoffs").OldestXmin;
                    frz_conflict_horizon = frz_conflict_horizon.wrapping_sub(1);
                    if !TransactionIdIsNormal(frz_conflict_horizon) {
                        frz_conflict_horizon = ::types_core::xact::MaxTransactionId;
                    }
                }
            }
            let conflict_xid =
                if TransactionIdFollows(frz_conflict_horizon, prstate.latest_xid_removed) {
                    frz_conflict_horizon
                } else {
                    prstate.latest_xid_removed
                };

            // log_heap_prune_and_freeze sorts the frozen array in place (C
            // scribbles it too).
            let PruneState {
                frozen,
                nfrozen,
                redirected,
                nredirected,
                nowdead,
                ndead,
                nowunused,
                nunused,
                ..
            } = prstate;
            log_heap_prune_and_freeze(
                relation,
                buffer,
                conflict_xid,
                true,
                reason,
                &mut frozen[..*nfrozen],
                &redirected[..*nredirected * 2],
                &nowdead[..*ndead],
                &nowunused[..*nunused],
            )?;
        }
    }
    Ok(())
}

// RelationNeedsWAL (rel.h), including the wal_level=minimal skip-WAL clause.
fn relation_needs_wal(rel: &RelationData<'_>) -> bool {
    rel.is_permanent()
        && (transam_xlog_seams::xlog_standby_info_active::call()
            || (rel.rd_createSubid.get() == types_core::InvalidSubTransactionId
                && rel.rd_firstRelfilelocatorSubid.get() == types_core::InvalidSubTransactionId))
}

fn heap_prune_satisfies_vacuum(
    prstate: &PruneState<'_>,
    tup: &mut HeapTupleData<'_>,
    buffer: Buffer,
) -> PgResult<HTSV_Result> {
    let mut dead_after = InvalidTransactionId;
    let res = HeapTupleSatisfiesVacuumHorizon(tup, buffer, &mut dead_after)?;
    if res != HTSV_Result::HEAPTUPLE_RECENTLY_DEAD {
        return Ok(res);
    }
    // VACUUM must prune xmaxes older than OldestXmin: freezing determination
    // uses it and dead tuples' xmaxes cannot be frozen.
    if let Some(cutoffs) = prstate.cutoffs {
        if TransactionIdIsValid(cutoffs.OldestXmin)
            && TransactionIdPrecedes(dead_after, cutoffs.OldestXmin)
        {
            return Ok(HTSV_Result::HEAPTUPLE_DEAD);
        }
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
    prstate: &mut PruneState<'_>,
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
            heap_prune_record_unchanged_lp_normal(page, prstate, item)?;
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
            heap_prune_record_unchanged_lp_normal(page, prstate, item)?;
        }
    }
    Ok(())
}

fn heap_prune_record_prunable(prstate: &mut PruneState<'_>, xid: TransactionId) {
    debug_assert!(TransactionIdIsNormal(xid));
    if !TransactionIdIsValid(prstate.new_prune_xid)
        || TransactionIdPrecedes(xid, prstate.new_prune_xid)
    {
        prstate.new_prune_xid = xid;
    }
}

fn heap_prune_record_redirect(
    prstate: &mut PruneState<'_>,
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
    prstate: &mut PruneState<'_>,
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
    prstate: &mut PruneState<'_>,
    offnum: OffsetNumber,
    was_normal: bool,
) {
    if prstate.mark_unused_now {
        heap_prune_record_unused(prstate, offnum, was_normal);
    } else {
        heap_prune_record_dead(presult, prstate, offnum, was_normal);
    }
}

fn heap_prune_record_unused(prstate: &mut PruneState<'_>, offnum: OffsetNumber, was_normal: bool) {
    debug_assert!(!prstate.processed[offnum as usize]);
    prstate.processed[offnum as usize] = true;
    debug_assert!(prstate.nunused < MaxHeapTuplesPerPage);
    prstate.nowunused[prstate.nunused] = offnum;
    prstate.nunused += 1;
    if was_normal {
        prstate.ndeleted += 1;
    }
}

fn heap_prune_record_unchanged_lp_unused(prstate: &mut PruneState<'_>, offnum: OffsetNumber) {
    debug_assert!(!prstate.processed[offnum as usize]);
    prstate.processed[offnum as usize] = true;
}

fn heap_prune_record_unchanged_lp_normal(
    page: PageRef<'_>,
    prstate: &mut PruneState<'_>,
    offnum: OffsetNumber,
) -> PgResult<()> {
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
                // As with hint bits, PD_ALL_VISIBLE must not be set off an
                // async-committed inserter; require the hinted bit.
                if !htup.xmin_committed() {
                    prstate.all_visible = false;
                } else {
                    let xmin = htup.xmin();
                    let cutoffs = prstate.cutoffs.expect("all_visible only tracked with FREEZE");
                    if !TransactionIdPrecedes(xmin, cutoffs.OldestXmin) {
                        prstate.all_visible = false;
                    } else if TransactionIdFollows(xmin, prstate.visibility_cutoff_xid)
                        && TransactionIdIsNormal(xmin)
                    {
                        prstate.visibility_cutoff_xid = xmin;
                    }
                }
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

    if prstate.freeze {
        let cutoffs = prstate.cutoffs.expect("freeze lane has cutoffs");
        let (has_plan, totally_frozen) = heap_prepare_freeze_tuple(
            htup,
            cutoffs,
            &mut prstate.pagefrz,
            &mut prstate.frozen[prstate.nfrozen],
        )?;
        if has_plan {
            prstate.frozen[prstate.nfrozen].offset = offnum;
            prstate.nfrozen += 1;
        }
        // A tuple neither totally frozen nor freezable-to-frozen keeps the
        // page off the all-frozen path.
        if !totally_frozen {
            prstate.all_frozen = false;
        }
    }
    Ok(())
}

fn heap_prune_record_unchanged_lp_dead(
    presult: &mut PruneFreezeResult,
    prstate: &mut PruneState<'_>,
    offnum: OffsetNumber,
) {
    debug_assert!(!prstate.processed[offnum as usize]);
    prstate.processed[offnum as usize] = true;
    // No hastup: LP_DEAD items are assumed LP_UNUSED-to-be for rel truncation.
    presult.deadoffsets[prstate.lpdead_items] = offnum;
    prstate.lpdead_items += 1;
}

fn heap_prune_record_unchanged_lp_redirect(prstate: &mut PruneState<'_>, offnum: OffsetNumber) {
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

fn heap_log_freeze_eq(plan: &[u8; 12], frz: &HeapTupleFreeze) -> bool {
    u32::from_ne_bytes(plan[0..4].try_into().unwrap()) == frz.xmax
        && u16::from_ne_bytes(plan[4..6].try_into().unwrap()) == frz.t_infomask2
        && u16::from_ne_bytes(plan[6..8].try_into().unwrap()) == frz.t_infomask
        && plan[8] == frz.frzflags
}

// xlhp_freeze_plan: xmax, t_infomask2, t_infomask, frzflags, pad, ntuples.
fn heap_log_freeze_new_plan(plan: &mut [u8; 12], frz: &HeapTupleFreeze) {
    plan[0..4].copy_from_slice(&frz.xmax.to_ne_bytes());
    plan[4..6].copy_from_slice(&frz.t_infomask2.to_ne_bytes());
    plan[6..8].copy_from_slice(&frz.t_infomask.to_ne_bytes());
    plan[8] = frz.frzflags;
    plan[9] = 0;
    plan[10..12].copy_from_slice(&1u16.to_ne_bytes());
}

fn heap_log_freeze_cmp(a: &HeapTupleFreeze, b: &HeapTupleFreeze) -> core::cmp::Ordering {
    (a.xmax, a.t_infomask2, a.t_infomask, a.frzflags, a.offset)
        .cmp(&(b.xmax, b.t_infomask2, b.t_infomask, b.frzflags, b.offset))
}

// heap_log_freeze_plan: dedup tuple freeze plans (sorts `tuples` in place);
// offsets_out is grouped by plan, ascending within each group.
fn heap_log_freeze_plan(
    tuples: &mut [HeapTupleFreeze],
    plans_out: &mut [[u8; 12]],
    offsets_out: &mut [OffsetNumber],
) -> usize {
    tuples.sort_unstable_by(heap_log_freeze_cmp);
    let mut nplans = 0usize;
    for (i, frz) in tuples.iter().enumerate() {
        if i == 0 || !heap_log_freeze_eq(&plans_out[nplans - 1], frz) {
            heap_log_freeze_new_plan(&mut plans_out[nplans], frz);
            nplans += 1;
        } else {
            let p = &mut plans_out[nplans - 1];
            let nt = u16::from_ne_bytes(p[10..12].try_into().unwrap()) + 1;
            p[10..12].copy_from_slice(&nt.to_ne_bytes());
        }
        offsets_out[i] = frz.offset;
    }
    debug_assert!(nplans > 0 && nplans <= tuples.len());
    nplans
}

/// `log_heap_prune_and_freeze`: emits XLOG_HEAP2_PRUNE_* with freeze-plan,
/// redirect/dead/unused sub-records and trailing frz offsets as block 0 data;
/// the conflict horizon rides unaligned after the 2-byte xl_heap_prune.
/// Scribbles on `frozen` (C contract).
#[allow(clippy::too_many_arguments)]
pub fn log_heap_prune_and_freeze(
    relation: &RelationData<'_>,
    buffer: Buffer,
    conflict_xid: TransactionId,
    cleanup_lock: bool,
    reason: PruneReason,
    frozen: &mut [HeapTupleFreeze],
    redirected: &[OffsetNumber],
    dead: &[OffsetNumber],
    unused: &[OffsetNumber],
) -> PgResult<XLogRecPtr> {
    let mut flags: u8 = 0;

    // xlhp_prune_items { uint16 ntargets; data[] } per group; offset arrays cross as raw byte views.
    let redirect_hdr = ((redirected.len() / 2) as u16).to_ne_bytes();
    let dead_hdr = (dead.len() as u16).to_ne_bytes();
    let unused_hdr = (unused.len() as u16).to_ne_bytes();

    // xlhp_freeze_plans { uint16 nplans; pad[2]; plans[] }.
    let mut plans = [[0u8; 12]; MaxHeapTuplesPerPage];
    let mut frz_offsets = [0 as OffsetNumber; MaxHeapTuplesPerPage];
    let mut freeze_hdr = [0u8; 4];
    let mut nplans = 0usize;
    if !frozen.is_empty() {
        nplans = heap_log_freeze_plan(frozen, &mut plans, &mut frz_offsets);
        freeze_hdr[0..2].copy_from_slice(&(nplans as u16).to_ne_bytes());
    }
    let nfrozen = frozen.len();

    let mut bufdata: [&[u8]; 9] = [&[]; 9];
    let mut n = 0;
    if nfrozen > 0 {
        flags |= XLHP_HAS_FREEZE_PLANS;
        bufdata[n] = &freeze_hdr;
        bufdata[n + 1] = plans_bytes(&plans[..nplans]);
        n += 2;
    }
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
    if nfrozen > 0 {
        bufdata[n] = offsets_bytes(&frz_offsets[..nfrozen]);
        n += 1;
    }

    // XLHP_IS_CATALOG_REL: a standby uses it to invalidate logical slots
    // whose catalog_xmin the pruning overtook (pruneheap.c:2139).
    if ::heapam::relation_is_accessible_in_logical_decoding(relation) {
        flags |= XLHP_IS_CATALOG_REL;
    }
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

fn plans_bytes(plans: &[[u8; 12]]) -> &[u8] {
    // SAFETY: [u8; 12] arrays are contiguous; same allocation, len*12 bytes.
    unsafe { core::slice::from_raw_parts(plans.as_ptr().cast::<u8>(), plans.len() * 12) }
}

pub fn init_seams() {
    pruneheap_seams::heap_page_prune_opt::set(heap_page_prune_opt);
    pruneheap_seams::heap_page_prune_execute::set(heap_page_prune_execute);
}

#[cfg(test)]
mod tests;
